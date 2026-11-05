//! Process-wide counters and gauges, logged every few seconds.

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
    pub ws_errors: Counter,
    pub decode_errors: Counter,
    pub exchange_errors: Counter,
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
            tracing::info!(
                target: "metrics",
                frames_s = format_args!("{:.0}", rate(0)),
                kib_s = format_args!("{:.0}", rate(1) / 1024.0),
                snaps_s = format_args!("{:.1}", rate(2)),
                deltas_s = format_args!("{:.0}", rate(3)),
                applied_s = format_args!("{:.0}", rate(4)),
                written_s = format_args!("{:.0}", rate(12)),
                dropped = cur[5],
                gaps = cur[6],
                resyncs = cur[7],
                reconnects = cur[8],
                ws_err = cur[9],
                decode_err = cur[10],
                exch_err = cur[11],
                mib_written = format_args!("{:.1}", cur[13] as f64 / 1048576.0),
                symbols = self.symbols.get(),
                live = self.books_live.get(),
                syncing = self.books_syncing.get(),
                sockets = self.sockets_connected.get(),
                wq = self.writer_queue.get(),
                jq = self.journal_queue.get(),
                lat_send_ms = %Percentiles::of(&ls),
                lat_exch_ms = %Percentiles::of(&le),
                "metrics"
            );
            prev = cur;
        }
    }

    fn counters(&self) -> [u64; 14] {
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
            self.ws_errors.get(),
            self.decode_errors.get(),
            self.exchange_errors.get(),
            self.msgs_written.get(),
            self.bytes_written.get(),
        ]
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
