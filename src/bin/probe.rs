//! Hour-one probe: how many markets, how many fit on one subscription, how
//! fast they talk, and how far behind the exchange we are. Writes a JSON
//! summary plus the first raw snapshot/delta frames for fixtures.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use engineering_challenge::auth::Auth;
use engineering_challenge::exchange::kalshi::{DEFAULT_REST_URL, DEFAULT_WS_URL, Kalshi};
use engineering_challenge::exchange::{Event, Exchange};
use engineering_challenge::metrics::Percentiles;
use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "KALSHI_KEY_ID")]
    key_id: String,
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    private_key: PathBuf,
    #[arg(long, default_value = DEFAULT_WS_URL)]
    ws_url: String,
    #[arg(long, default_value = DEFAULT_REST_URL)]
    rest_url: String,
    /// Seconds to measure the subscribed feed.
    #[arg(long, default_value_t = 60)]
    measure_secs: u64,
    /// Skip the per-subscription limit search and use this many markets.
    #[arg(long)]
    markets_per_socket: Option<usize>,
    #[arg(long, default_value = "probe")]
    out: PathBuf,
}

enum SubscribeOutcome {
    Subscribed(u64),
    Error(u32, String),
    Timeout,
}

async fn try_subscribe(ex: &Kalshi, symbols: &[String]) -> anyhow::Result<SubscribeOutcome> {
    let ws = ex.connect().await?;
    let (mut sink, mut stream) = ws.split();
    sink.send(Message::Text(ex.subscribe_books(1, symbols).into()))
        .await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let outcome = loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break SubscribeOutcome::Timeout;
        }
        match tokio::time::timeout(left, stream.next()).await {
            Err(_) => break SubscribeOutcome::Timeout,
            Ok(None) => anyhow::bail!("stream ended during subscribe"),
            Ok(Some(Err(e))) => anyhow::bail!("read: {e}"),
            Ok(Some(Ok(Message::Text(t)))) => match ex.decode(t.as_str())?.event {
                Event::Subscribed { sid, .. } => break SubscribeOutcome::Subscribed(sid),
                Event::Error { code, msg, .. } => break SubscribeOutcome::Error(code, msg),
                _ => {}
            },
            Ok(Some(Ok(Message::Ping(p)))) => sink.send(Message::Pong(p)).await?,
            Ok(Some(Ok(Message::Close(_)))) => anyhow::bail!("closed during subscribe"),
            Ok(Some(Ok(_))) => {}
        }
    };
    let _ = sink.send(Message::Close(None)).await;
    Ok(outcome)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    std::fs::create_dir_all(&args.out)?;
    let started_at = Utc::now();
    println!("probe start {}", started_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    let auth = Arc::new(Auth::load(&args.key_id, &args.private_key)?);
    println!("auth: key {} ({})", auth.key_id(), auth.algorithm());
    let ex = Kalshi::new(auth, &args.ws_url, &args.rest_url);

    let t0 = Instant::now();
    let symbols = ex.discover().await.context("discovery")?;
    println!(
        "discovery: {} open non-MVE markets in {:.1}s",
        symbols.len(),
        t0.elapsed().as_secs_f64()
    );
    std::fs::write(args.out.join("symbols.txt"), symbols.join("\n"))?;

    let mut summary = BTreeMap::new();
    summary.insert("started_at".to_owned(), serde_json::json!(started_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)));
    summary.insert("markets_open".to_owned(), serde_json::json!(symbols.len()));
    summary.insert(
        "discovery_secs".to_owned(),
        serde_json::json!(t0.elapsed().as_secs_f64()),
    );

    let k = match args.markets_per_socket {
        Some(k) => k.min(symbols.len()),
        None => {
            let mut n = symbols.len();
            println!("limit search: trying {n} markets on one subscription");
            let mut lo = 0usize;
            let mut hi;
            match try_subscribe(&ex, &symbols[..n]).await? {
                SubscribeOutcome::Subscribed(sid) => {
                    println!("limit search: all {n} accepted on one subscription (sid {sid})");
                    hi = n;
                    lo = n;
                }
                SubscribeOutcome::Error(code, msg) => {
                    println!("limit search: {n} -> error {code} {msg}");
                    summary.insert("limit_error".to_owned(), serde_json::json!({"code": code, "msg": msg}));
                    hi = n;
                }
                SubscribeOutcome::Timeout => {
                    println!("limit search: {n} -> timeout (no ack in 20s)");
                    hi = n;
                }
            }
            let mut tries = 0;
            while hi - lo > 1 && tries < 16 {
                tries += 1;
                n = (lo + hi) / 2;
                tokio::time::sleep(Duration::from_secs(1)).await;
                match try_subscribe(&ex, &symbols[..n]).await? {
                    SubscribeOutcome::Subscribed(_) => {
                        println!("limit search: {n} ok");
                        lo = n;
                    }
                    SubscribeOutcome::Error(code, msg) => {
                        println!("limit search: {n} -> error {code} {msg}");
                        hi = n;
                    }
                    SubscribeOutcome::Timeout => {
                        println!("limit search: {n} -> timeout");
                        hi = n;
                    }
                }
            }
            lo
        }
    };
    println!("markets per subscription: {k}");
    summary.insert("markets_per_subscription".to_owned(), serde_json::json!(k));

    // Measurement run on the first k symbols.
    let subset = &symbols[..k];
    let ws = ex.connect().await?;
    let (mut sink, mut stream) = ws.split();
    let measure_started_at = Utc::now();
    println!("measure start {}", measure_started_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    let t_sub = Instant::now();
    sink.send(Message::Text(ex.subscribe_books(1, subset).into()))
        .await?;
    let end = t_sub + Duration::from_secs(args.measure_secs);
    let mut frames = 0u64;
    let mut bytes = 0u64;
    let mut snaps = 0u64;
    let mut deltas = 0u64;
    let mut errors = Vec::new();
    let mut per_sec: BTreeMap<u64, u64> = BTreeMap::new();
    let mut lat_send = Vec::new();
    let mut lat_exch = Vec::new();
    let mut active: HashSet<String> = HashSet::new();
    let mut snap_done_at: Option<f64> = None;
    let mut first_snapshot_saved = false;
    let mut first_delta_saved = false;
    let mut last_seq: Option<u64> = None;
    let mut seq_gaps = 0u64;
    let mut sid: Option<u64> = None;
    loop {
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match tokio::time::timeout(left, stream.next()).await {
            Err(_) => break,
            Ok(None) => {
                errors.push("stream ended".to_owned());
                break;
            }
            Ok(Some(Err(e))) => {
                errors.push(format!("read: {e}"));
                break;
            }
            Ok(Some(Ok(Message::Ping(p)))) => sink.send(Message::Pong(p)).await?,
            Ok(Some(Ok(Message::Close(f)))) => {
                errors.push(format!("close: {f:?}"));
                break;
            }
            Ok(Some(Ok(Message::Text(t)))) => {
                let now = Utc::now();
                frames += 1;
                bytes += t.len() as u64;
                *per_sec.entry(t_sub.elapsed().as_secs()).or_default() += 1;
                let d = match ex.decode(t.as_str()) {
                    Ok(d) => d,
                    Err(e) => {
                        errors.push(format!("decode: {e}: {:.200}", t.as_str()));
                        continue;
                    }
                };
                if let (Some(s), Some(q)) = (d.sid, d.seq)
                    && sid == Some(s) {
                        if let Some(prev) = last_seq
                            && q != prev + 1 {
                                seq_gaps += 1;
                            }
                        last_seq = Some(q);
                    }
                if let Some(st) = d.sending_ts {
                    lat_send.push((now - st).num_microseconds().unwrap_or(0));
                }
                match d.event {
                    Event::Subscribed { sid: s, .. } => sid = Some(s),
                    Event::Snapshot { .. } => {
                        snaps += 1;
                        if !first_snapshot_saved {
                            std::fs::write(args.out.join("raw_snapshot.json"), t.as_str())?;
                            first_snapshot_saved = true;
                        }
                        if snaps as usize == k && snap_done_at.is_none() {
                            snap_done_at = Some(t_sub.elapsed().as_secs_f64());
                        }
                    }
                    Event::Delta { symbol, exchange_ts, .. } => {
                        deltas += 1;
                        active.insert(symbol);
                        if let Some(et) = exchange_ts {
                            lat_exch.push((now - et).num_microseconds().unwrap_or(0));
                        }
                        if !first_delta_saved {
                            std::fs::write(args.out.join("raw_delta.json"), t.as_str())?;
                            first_delta_saved = true;
                        }
                    }
                    Event::Error { code, msg, .. } => errors.push(format!("error {code}: {msg}")),
                    _ => {}
                }
            }
            Ok(Some(Ok(_))) => {}
        }
    }
    let _ = sink.send(Message::Close(None)).await;
    let measure_ended_at = Utc::now();
    let secs = t_sub.elapsed().as_secs_f64();
    lat_send.sort_unstable();
    lat_exch.sort_unstable();
    let peak = per_sec.values().copied().max().unwrap_or(0);
    // Steady state: skip the snapshot burst window.
    let steady: Vec<u64> = per_sec
        .iter()
        .filter(|(s, _)| **s as f64 > snap_done_at.unwrap_or(5.0) + 1.0)
        .map(|(_, v)| *v)
        .collect();
    let steady_avg = if steady.is_empty() {
        0.0
    } else {
        steady.iter().sum::<u64>() as f64 / steady.len() as f64
    };
    let steady_peak = steady.iter().copied().max().unwrap_or(0);
    println!("measure: {secs:.0}s on {k} markets");
    println!("  frames {frames} ({:.0}/s avg, peak {peak}/s incl. snapshot burst)", frames as f64 / secs);
    println!("  steady state after snapshots: avg {steady_avg:.1}/s, peak {steady_peak}/s");
    println!("  bytes {} ({:.1} KiB/s)", bytes, bytes as f64 / secs / 1024.0);
    println!("  snapshots {snaps} (all {k} received after {:?}s), deltas {deltas}, active markets {}", snap_done_at, active.len());
    println!("  seq gaps {seq_gaps}");
    println!("  latency recv - sending_ts_ms: {}", Percentiles::of(&lat_send));
    println!("  latency recv - delta ts_ms:   {}", Percentiles::of(&lat_exch));
    if !errors.is_empty() {
        println!("  errors ({}): {:?}", errors.len(), &errors[..errors.len().min(10)]);
    }
    summary.insert("measure_started_at".to_owned(), serde_json::json!(measure_started_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)));
    summary.insert("measure_ended_at".to_owned(), serde_json::json!(measure_ended_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)));
    summary.insert("measure_secs".to_owned(), serde_json::json!(secs));
    summary.insert("frames".to_owned(), serde_json::json!(frames));
    summary.insert("frames_per_sec_avg".to_owned(), serde_json::json!(frames as f64 / secs));
    summary.insert("frames_per_sec_peak".to_owned(), serde_json::json!(peak));
    summary.insert("steady_frames_per_sec_avg".to_owned(), serde_json::json!(steady_avg));
    summary.insert("steady_frames_per_sec_peak".to_owned(), serde_json::json!(steady_peak));
    summary.insert("bytes".to_owned(), serde_json::json!(bytes));
    summary.insert("snapshots".to_owned(), serde_json::json!(snaps));
    summary.insert("snapshots_complete_after_secs".to_owned(), serde_json::json!(snap_done_at));
    summary.insert("deltas".to_owned(), serde_json::json!(deltas));
    summary.insert("active_markets".to_owned(), serde_json::json!(active.len()));
    summary.insert("seq_gaps".to_owned(), serde_json::json!(seq_gaps));
    let p = |v: &[i64]| {
        let p = Percentiles::of(v);
        serde_json::json!({"n": p.n, "p50_ms": p.p50 as f64 / 1000.0, "p90_ms": p.p90 as f64 / 1000.0, "p99_ms": p.p99 as f64 / 1000.0, "max_ms": p.max as f64 / 1000.0})
    };
    summary.insert("latency_recv_minus_sending_ts".to_owned(), p(&lat_send));
    summary.insert("latency_recv_minus_delta_ts".to_owned(), p(&lat_exch));
    summary.insert("errors".to_owned(), serde_json::json!(errors));
    let ended_at = Utc::now();
    summary.insert("ended_at".to_owned(), serde_json::json!(ended_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)));
    std::fs::write(
        args.out.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    println!("wrote {}", args.out.join("summary.json").display());
    println!("probe end {}", ended_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    Ok(())
}
