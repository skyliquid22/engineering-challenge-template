//! Single owner of the `AlignedSbeWriter`: hourly UTC rotation, stdout mode,
//! optional periodic flush, drain-then-flush on shutdown. Runs on its own OS
//! thread because the template writer is synchronous.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::{DateTime, Timelike as _, Utc};

use crate::metrics::Metrics;
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// The reader hard-codes 64 MiB chunks, so the writer must too.
pub const CHUNK_SIZE: usize = 64 << 20;

pub enum Out {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

#[derive(Clone, Debug)]
pub enum Sink {
    /// `<dir>/<prefix>-<YYYYMMDD>T<HH>Z.sbe`, rotated on the UTC hour.
    Dir { dir: PathBuf, prefix: String },
    /// A single file (or `/dev/stdout`), never rotated.
    File(PathBuf),
}

#[derive(Clone, Debug)]
pub struct WriterConfig {
    pub sink: Sink,
    /// Flush (= pad the chunk and write it) at this interval. `None` means
    /// only on rotation and shutdown; see README for the trade-off.
    pub flush_every: Option<Duration>,
}

pub type OutSender = mpsc::Sender<Out>;

pub fn spawn_writer(
    cfg: WriterConfig,
    rx: mpsc::Receiver<Out>,
    metrics: Arc<Metrics>,
) -> std::thread::JoinHandle<anyhow::Result<()>> {
    std::thread::Builder::new()
        .name("sbe-writer".into())
        .spawn(move || run(cfg, rx, metrics))
        .expect("spawn writer thread")
}

fn hour_path(dir: &Path, prefix: &str, at: DateTime<Utc>) -> PathBuf {
    let stamp = at.format("%Y%m%dT%HZ");
    let mut path = dir.join(format!("{prefix}-{stamp}.sbe"));
    // Never truncate an earlier run's file for the same hour.
    let mut n = 1;
    while path.exists() {
        n += 1;
        path = dir.join(format!("{prefix}-{stamp}-{n}.sbe"));
    }
    path
}

struct Current {
    writer: AlignedSbeWriter,
    hour: Option<DateTime<Utc>>,
    path: PathBuf,
}

fn open(cfg: &WriterConfig, now: DateTime<Utc>) -> anyhow::Result<Current> {
    let (path, hour) = match &cfg.sink {
        Sink::Dir { dir, prefix } => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating {}", dir.display()))?;
            let hour = now
                .with_minute(0)
                .and_then(|t| t.with_second(0))
                .and_then(|t| t.with_nanosecond(0))
                .unwrap_or(now);
            (hour_path(dir, prefix, now), Some(hour))
        }
        Sink::File(p) => (p.clone(), None),
    };
    let writer = AlignedSbeWriter::create(&path, CHUNK_SIZE)
        .with_context(|| format!("opening {}", path.display()))?;
    tracing::info!(path = %path.display(), "writing");
    Ok(Current { writer, hour, path })
}

fn run(cfg: WriterConfig, rx: mpsc::Receiver<Out>, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let mut cur = open(&cfg, Utc::now())?;
    let mut last_flush = Instant::now();
    loop {
        let msg = match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(m) => Some(m),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let now = Utc::now();
        if let Some(hour) = cur.hour
            && now.signed_duration_since(hour) >= chrono::Duration::hours(1) {
                cur.writer.flush()?;
                tracing::info!(path = %cur.path.display(), "rotating on the hour");
                cur = open(&cfg, now)?;
                last_flush = Instant::now();
            }
        if let Some(every) = cfg.flush_every
            && last_flush.elapsed() >= every {
                cur.writer.flush()?;
                last_flush = Instant::now();
            }
        let Some(msg) = msg else { continue };
        metrics.writer_queue.dec();
        let len = match &msg {
            Out::Snapshot(s) => {
                let len = s.encoded_length_with_header()?;
                cur.writer.write_msg(s)?;
                len
            }
            Out::Update(u) => {
                let len = u.encoded_length_with_header()?;
                cur.writer.write_msg(u)?;
                len
            }
        };
        metrics.msgs_written.inc();
        metrics.bytes_written.add(len as u64);
    }
    cur.writer.flush().context("final flush")?;
    tracing::info!(path = %cur.path.display(), msgs = metrics.msgs_written.get(), "writer drained and flushed");
    Ok(())
}
