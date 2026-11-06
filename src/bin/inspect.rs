//! Inspect SBE files written by the collector.
//!
//! `inspect <files...> [--head N] [--symbol S] [--check]`
//!
//! Prints message counts, symbol coverage, the time range and timestamp
//! monotonicity. `--check` replays the stream into books and reports
//! anything a downstream consumer would trip over.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use clap::Parser;
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Parser, Debug)]
#[command(name = "inspect", about = "Inspect collector SBE output files")]
struct Args {
    /// SBE files to read, in order.
    #[arg(required = true)]
    files: Vec<PathBuf>,

    /// Print the first N messages, one per line.
    #[arg(long)]
    head: Option<usize>,

    /// Print every message for this symbol.
    #[arg(long)]
    symbol: Option<String>,

    /// Rebuild books from the stream and report inconsistencies.
    #[arg(long)]
    check: bool,
}

#[derive(Default)]
struct SymbolBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    saw_snapshot: bool,
}

#[derive(Default)]
struct Stats {
    total: u64,
    snapshots: u64,
    updates: u64,
    per_symbol: HashMap<String, u64>,
    first_ts: Option<DateTime<Utc>>,
    last_ts: Option<DateTime<Utc>>,
    prev_ts: Option<DateTime<Utc>>,
    non_monotonic: u64,
    bytes: u64,
    // --check
    books: HashMap<String, SymbolBook>,
    updates_before_snapshot: HashSet<String>,
    negative_quantities: u64,
}

fn fmt_fixed(v: i64) -> String {
    let sign = if v < 0 { "-" } else { "" };
    let m = v.unsigned_abs();
    format!("{sign}{}.{:0>6}", m / 1_000_000, m % 1_000_000)
}

fn describe_snapshot(s: &SnapshotDomain) -> String {
    let best_bid = s
        .bids
        .first()
        .map(|l| format!("{}x{}", fmt_fixed(l.price_level), fmt_fixed(l.quantity)))
        .unwrap_or_else(|| "-".into());
    let best_ask = s
        .asks
        .first()
        .map(|l| format!("{}x{}", fmt_fixed(l.price_level), fmt_fixed(l.quantity)))
        .unwrap_or_else(|| "-".into());
    format!(
        "{} SNAPSHOT {} exch={} bids={} asks={} best_bid={} best_ask={}",
        s.received_at.format("%Y-%m-%dT%H:%M:%S%.6fZ"),
        s.symbol,
        s.exchange_timestamp.format("%H:%M:%S%.6f"),
        s.bids.len(),
        s.asks.len(),
        best_bid,
        best_ask,
    )
}

fn describe_update(u: &PriceLevelUpdateDomain) -> String {
    format!(
        "{} UPDATE   {} exch={} {} {} -> {}",
        u.backtest_timestamp.format("%Y-%m-%dT%H:%M:%S%.6fZ"),
        u.symbol,
        u.exchange_timestamp.format("%H:%M:%S%.6f"),
        match u.side {
            Side::Buy => "bid",
            Side::Sell => "ask",
        },
        fmt_fixed(u.price_level),
        fmt_fixed(u.quantity),
    )
}

impl Stats {
    fn observe_ts(&mut self, ts: DateTime<Utc>) {
        if self.first_ts.is_none() {
            self.first_ts = Some(ts);
        }
        self.last_ts = Some(ts);
        if let Some(prev) = self.prev_ts
            && ts < prev
        {
            self.non_monotonic += 1;
        }
        self.prev_ts = Some(ts);
    }

    fn check_snapshot(&mut self, s: &SnapshotDomain) {
        let book = self.books.entry(s.symbol.clone()).or_default();
        book.saw_snapshot = true;
        book.bids = s
            .bids
            .iter()
            .filter(|l| l.quantity != 0)
            .map(|l| (l.price_level, l.quantity))
            .collect();
        book.asks = s
            .asks
            .iter()
            .filter(|l| l.quantity != 0)
            .map(|l| (l.price_level, l.quantity))
            .collect();
        let negative_bids = s.bids.iter().filter(|l| l.quantity < 0).count();
        let negative_asks = s.asks.iter().filter(|l| l.quantity < 0).count();
        self.negative_quantities += (negative_bids + negative_asks) as u64;
    }

    fn check_update(&mut self, u: &PriceLevelUpdateDomain) {
        let book = self.books.entry(u.symbol.clone()).or_default();
        if !book.saw_snapshot {
            self.updates_before_snapshot.insert(u.symbol.clone());
        }
        if u.quantity < 0 {
            self.negative_quantities += 1;
        }
        let levels = match u.side {
            Side::Buy => &mut book.bids,
            Side::Sell => &mut book.asks,
        };
        if u.quantity == 0 {
            levels.remove(&u.price_level);
        } else {
            levels.insert(u.price_level, u.quantity);
        }
    }

    fn crossed_books(&self) -> Vec<(&String, i64, i64)> {
        let mut crossed: Vec<_> = self
            .books
            .iter()
            .filter_map(|(sym, b)| {
                let best_bid = *b.bids.keys().next_back()?;
                let best_ask = *b.asks.keys().next()?;
                (best_bid >= best_ask).then_some((sym, best_bid, best_ask))
            })
            .collect();
        crossed.sort();
        crossed
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut stats = Stats::default();
    let mut printed_head = 0usize;

    for path in &args.files {
        let bytes = std::fs::metadata(path)?.len();
        stats.bytes += bytes;
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)
            .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let mut file_msgs = 0u64;

        loop {
            let msg = match reader.read_msg() {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    eprintln!(
                        "{}: decode error after {file_msgs} messages: {e}",
                        path.display()
                    );
                    break;
                }
            };
            file_msgs += 1;
            stats.total += 1;

            let (symbol, ts, line) = match msg {
                AnyMessage::Snapshot(d) => {
                    let s = SnapshotDomain::try_from_decoder(d)?;
                    stats.snapshots += 1;
                    if args.check {
                        stats.check_snapshot(&s);
                    }
                    let line = describe_snapshot(&s);
                    (s.symbol, s.received_at, line)
                }
                AnyMessage::PriceLevelUpdate(d) => {
                    let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                    stats.updates += 1;
                    if args.check {
                        stats.check_update(&u);
                    }
                    let line = describe_update(&u);
                    (u.symbol, u.backtest_timestamp, line)
                }
                _ => anyhow::bail!("{}: unknown message type", path.display()),
            };

            stats.observe_ts(ts);
            *stats.per_symbol.entry(symbol.clone()).or_insert(0) += 1;

            let want_head = args.head.is_some_and(|n| printed_head < n);
            let want_symbol = args.symbol.as_deref() == Some(symbol.as_str());
            if want_head || want_symbol {
                println!("{line}");
                if want_head {
                    printed_head += 1;
                }
            }
        }
        println!("file {}: {bytes} bytes, {file_msgs} messages", path.display());
    }

    println!();
    println!("messages:          {}", stats.total);
    println!("  snapshots:       {}", stats.snapshots);
    println!("  updates:         {}", stats.updates);
    println!("distinct symbols:  {}", stats.per_symbol.len());
    println!("bytes (all files): {}", stats.bytes);
    match (stats.first_ts, stats.last_ts) {
        (Some(a), Some(b)) => {
            println!("first timestamp:   {}", a.format("%Y-%m-%dT%H:%M:%S%.6fZ"));
            println!("last timestamp:    {}", b.format("%Y-%m-%dT%H:%M:%S%.6fZ"));
            println!("span:              {:.3}s", (b - a).num_microseconds().unwrap_or(0) as f64 / 1e6);
        }
        _ => println!("no messages"),
    }
    println!("non-monotonic received_at steps: {}", stats.non_monotonic);

    if !stats.per_symbol.is_empty() {
        let mut top: Vec<(&String, &u64)> = stats.per_symbol.iter().collect();
        top.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        println!();
        println!("top symbols by message count:");
        for (sym, n) in top.iter().take(20) {
            println!("  {n:>8}  {sym}");
        }
    }

    if args.check {
        let crossed = stats.crossed_books();
        println!();
        println!("check:");
        println!(
            "  symbols with updates before any snapshot: {}",
            stats.updates_before_snapshot.len()
        );
        println!("  negative quantities:                      {}", stats.negative_quantities);
        println!("  crossed books at end (bid >= ask):        {}", crossed.len());
        for (sym, bid, ask) in crossed.iter().take(20) {
            println!("    {sym}: best_bid={} best_ask={}", fmt_fixed(*bid), fmt_fixed(*ask));
        }
        let live_books = stats.books.values().filter(|b| b.saw_snapshot).count();
        println!("  books rebuilt from snapshots:             {live_books}");
    }

    Ok(())
}
