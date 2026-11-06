//! Process-wide counters and gauges, logged every few seconds.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.add(1);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

#[derive(Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn inc(&self) {
        self.0.fetch_add(1, Relaxed);
    }
    pub fn dec(&self) {
        self.0.fetch_sub(1, Relaxed);
    }
    pub fn add(&self, n: i64) {
        self.0.fetch_add(n, Relaxed);
    }
    pub fn set(&self, v: i64) {
        self.0.store(v, Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.0.load(Relaxed)
    }
}

/// One shard's view of its symbols' update rates (extension 2, metric only).
/// `buckets`: symbols with no applied update in the last 10 min, under 1/min,
/// 1 to 10/min, over 10/min. `top`: its busiest symbols, updates per minute.
#[derive(Clone, Debug, Default)]
pub struct RateReport {
    pub buckets: [u64; 4],
    pub top: Vec<(String, f64)>,
}

#[derive(Default)]
pub struct Metrics {
    pub frames: Counter,
    pub bytes_in: Counter,
    pub snapshots_rx: Counter,
    pub deltas_rx: Counter,
    pub deltas_applied: Counter,
    pub dropped_syncing: Counter,
    pub gaps: Counter,
    pub resyncs: Counter,
    pub reconnects: Counter,
    pub failovers: Counter,
    pub ws_errors: Counter,
    pub decode_errors: Counter,
    pub exchange_errors: Counter,
    /// Books whose clock lagged the standby source's for 2 ticks (ext. 2).
    pub stale_books: Counter,
    pub msgs_written: Counter,
    pub bytes_written: Counter,
    pub journal_lines: Counter,
    pub symbols: Gauge,
    pub books_live: Gauge,
    pub books_syncing: Gauge,
    pub sockets_connected: Gauge,
    pub writer_queue: Gauge,
    pub journal_queue: Gauge,
    /// received_at - sending_ts_ms (exchange queue time to our read), micros.
    lat_send: Mutex<Vec<i64>>,
    /// received_at - ts_ms (exchange change time to our read), micros.
    lat_exch: Mutex<Vec<i64>>,
    /// Latest per-shard rate report, merged by the logger.
    rates: Mutex<HashMap<usize, RateReport>>,
}

const MAX_SAMPLES: usize = 400_000;

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_send_latency(&self, micros: i64) {
        let mut v = self.lat_send.lock().unwrap();
        if v.len() < MAX_SAMPLES {
            v.push(micros);
        }
    }

    pub fn record_exchange_latency(&self, micros: i64) {
        let mut v = self.lat_exch.lock().unwrap();
        if v.len() < MAX_SAMPLES {
            v.push(micros);
        }
    }

    pub fn report_rates(&self, shard: usize, report: RateReport) {
        self.rates.lock().unwrap().insert(shard, report);
    }

    /// Sum of all shards' buckets and the five busiest symbols overall.
    pub fn merged_rates(&self) -> RateReport {
        let shards = self.rates.lock().unwrap();
        let mut merged = RateReport::default();
        for r in shards.values() {
            for (m, b) in merged.buckets.iter_mut().zip(r.buckets.iter()) {
                *m += b;
            }
            merged.top.extend(r.top.iter().cloned());
        }
        merged.top.sort_by(|a, b| b.1.total_cmp(&a.1));
        merged.top.truncate(5);
        merged
    }

    pub fn take_latencies(&self) -> (Vec<i64>, Vec<i64>) {
        (
            std::mem::take(&mut *self.lat_send.lock().unwrap()),
            std::mem::take(&mut *self.lat_exch.lock().unwrap()),
        )
    }

    pub async fn run_logger(self: Arc<Self>, every: Duration, cancel: CancellationToken) {
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let mut prev = self.counters();
        let mut rising_for = 0u32;
        // Rolling 60 s window of send-latency samples, so a run summary can
        // quote one number per run instead of a chain of 10 s intervals.
        let window_len = ((60.0 / every.as_secs_f64()).ceil() as usize).max(1);
        let mut recent: std::collections::VecDeque<Vec<i64>> = std::collections::VecDeque::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            let cur = self.counters();
            let secs = every.as_secs_f64();
            let rate = |i: usize| (cur[i].saturating_sub(prev[i])) as f64 / secs;
            let (mut ls, mut le) = self.take_latencies();
            ls.sort_unstable();
            le.sort_unstable();
            recent.push_back(ls.clone());
            while recent.len() > window_len {
                recent.pop_front();
            }
            let mut win: Vec<i64> = recent.iter().flatten().copied().collect();
            win.sort_unstable();
            let w = Percentiles::of(&win);
            // Inbound latency that keeps growing means the link cannot carry
            // the subscribed stream. Nothing in this process fixes that, so
            // say so plainly instead of letting the queue grow quietly.
            let p50_ms = Percentiles::of(&ls).p50 / 1000;
            if !ls.is_empty() && p50_ms > 5_000 {
                rising_for += 1;
                if rising_for >= 3 {
                    tracing::warn!(
                        p50_s = p50_ms / 1000,
                        kib_s = format_args!("{:.0}", rate(1) / 1024.0),
                        "inbound latency above 5 s for {}s: the network link is not keeping up with the subscribed markets; expect exchange-side resets until bandwidth is added or the universe is split across hosts",
                        rising_for as f64 * secs
                    );
                }
            } else {
                rising_for = 0;
            }
            let rates = self.merged_rates();
            let rss = rss_mib().map_or_else(|| "n/a".to_owned(), |m| format!("{m:.1}"));
            let top5 = rates
                .top
                .iter()
                .map(|(sym, per_min)| format!("{sym}:{per_min:.1}/min"))
                .collect::<Vec<_>>()
                .join(",");
            tracing::info!(
                target: "metrics",
                frames_s = format_args!("{:.0}", rate(0)),
                kib_s = format_args!("{:.0}", rate(1) / 1024.0),
                snaps_s = format_args!("{:.1}", rate(2)),
                deltas_s = format_args!("{:.0}", rate(3)),
                applied_s = format_args!("{:.0}", rate(4)),
                written_s = format_args!("{:.0}", rate(13)),
                dropped = cur[5],
                gaps = cur[6],
                resyncs = cur[7],
                reconnects = cur[8],
                failovers = cur[9],
                ws_err = cur[10],
                decode_err = cur[11],
                exch_err = cur[12],
                stale = cur[15],
                mib_written = format_args!("{:.1}", cur[14] as f64 / 1048576.0),
                symbols = self.symbols.get(),
                live = self.books_live.get(),
                syncing = self.books_syncing.get(),
                sockets = self.sockets_connected.get(),
                wq = self.writer_queue.get(),
                jq = self.journal_queue.get(),
                lat_send_ms = %Percentiles::of(&ls),
                lat_exch_ms = %Percentiles::of(&le),
                rate_0 = rates.buckets[0],
                rate_lt1 = rates.buckets[1],
                rate_1to10 = rates.buckets[2],
                rate_gt10 = rates.buckets[3],
                top5 = %top5,
                rss_mib = %rss,
                lat60_n = w.n,
                lat60_p50_ms = format_args!("{:.1}", w.p50 as f64 / 1000.0),
                lat60_p90_ms = format_args!("{:.1}", w.p90 as f64 / 1000.0),
                lat60_p99_ms = format_args!("{:.1}", w.p99 as f64 / 1000.0),
                "metrics"
            );
            prev = cur;
        }
    }

    fn counters(&self) -> [u64; 16] {
        [
            self.frames.get(),
            self.bytes_in.get(),
            self.snapshots_rx.get(),
            self.deltas_rx.get(),
            self.deltas_applied.get(),
            self.dropped_syncing.get(),
            self.gaps.get(),
            self.resyncs.get(),
            self.reconnects.get(),
            self.failovers.get(),
            self.ws_errors.get(),
            self.decode_errors.get(),
            self.exchange_errors.get(),
            self.msgs_written.get(),
            self.bytes_written.get(),
            self.stale_books.get(),
        ]
    }
}

/// Resident set size in MiB. Linux reads `VmRSS` from `/proc/self/status`
/// (the same figure as `/proc/self/statm` resident pages times the page size,
/// without assuming a page size). Elsewhere there is no portable source
/// without another dependency, so the metrics line says `n/a`.
pub fn rss_mib() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
        let kib: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
        return Some(kib / 1024.0);
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// p50/p90/p99/max of a sorted sample, rendered in milliseconds.
pub struct Percentiles {
    pub n: usize,
    pub p50: i64,
    pub p90: i64,
    pub p99: i64,
    pub max: i64,
}

impl Percentiles {
    pub fn of(sorted: &[i64]) -> Self {
        let at = |p: f64| {
            if sorted.is_empty() {
                0
            } else {
                let i = ((sorted.len() - 1) as f64 * p).round() as usize;
                sorted[i.min(sorted.len() - 1)]
            }
        };
        Self {
            n: sorted.len(),
            p50: at(0.5),
            p90: at(0.9),
            p99: at(0.99),
            max: sorted.last().copied().unwrap_or(0),
        }
    }
}

impl std::fmt::Display for Percentiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.n == 0 {
            return f.write_str("n=0");
        }
        write!(
            f,
            "n={} p50={:.1} p90={:.1} p99={:.1} max={:.1}",
            self.n,
            self.p50 as f64 / 1000.0,
            self.p90 as f64 / 1000.0,
            self.p99 as f64 / 1000.0,
            self.max as f64 / 1000.0
        )
    }
}
