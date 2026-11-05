//! Raw-frame journal: every frame the active source delivered, with its
//! receive timestamp, as one JSON line. Bounds crash loss to about a second
//! (the SBE writer buffers a whole 64 MiB chunk) and makes the SBE output
//! reproducible offline.

use std::io::{BufWriter, Write as _};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::metrics::Metrics;

pub struct JournalLine {
    pub received_at: DateTime<Utc>,
    pub source: String,
    pub text: String,
}

pub type JournalSender = mpsc::Sender<JournalLine>;

#[derive(Clone, Debug)]
pub struct JournalConfig {
    pub dir: PathBuf,
    pub prefix: String,
}

pub fn spawn_journal(
    cfg: JournalConfig,
    rx: mpsc::Receiver<JournalLine>,
    metrics: Arc<Metrics>,
) -> std::thread::JoinHandle<anyhow::Result<()>> {
    std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || run(cfg, rx, metrics))
        .expect("spawn journal thread")
}

fn run(cfg: JournalConfig, rx: mpsc::Receiver<JournalLine>, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.dir)?;
    let mut current_hour = String::new();
    let mut out: Option<BufWriter<std::fs::File>> = None;
    let mut last_flush = Instant::now();
    loop {
        let line = match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(l) => Some(l),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        if let Some(line) = line {
            metrics.journal_queue.dec();
            let hour = line.received_at.format("%Y%m%dT%HZ").to_string();
            if hour != current_hour || out.is_none() {
                if let Some(mut w) = out.take() {
                    w.flush()?;
                }
                let path = cfg.dir.join(format!("{}-{hour}.jsonl", cfg.prefix));
                let file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
                out = Some(BufWriter::with_capacity(1 << 20, file));
                current_hour = hour;
                tracing::info!(path = %path.display(), "journal file");
            }
            let w = out.as_mut().expect("journal writer");
            // `text` is the exchange's own JSON object, embedded as-is.
            writeln!(
                w,
                "{{\"rx\":{},\"src\":{},\"f\":{}}}",
                line.received_at.timestamp_micros(),
                serde_json::to_string(&line.source)?,
                line.text
            )?;
            metrics.journal_lines.inc();
        }
        if last_flush.elapsed() >= Duration::from_secs(1) {
            if let Some(w) = out.as_mut() {
                w.flush()?;
            }
            last_flush = Instant::now();
        }
    }
    if let Some(mut w) = out.take() {
        w.flush()?;
    }
    Ok(())
}
