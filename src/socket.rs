//! One websocket connection, kept alive forever: connect with backoff, stamp
//! every frame the instant it is read, answer pings, reconnect on silence,
//! error, close, or a scheduled rotation (extension 2: a planned full
//! reconnect clears whatever the exchange side has accumulated).
//!
//! The worker never blocks on anything but the socket itself: frames go to the
//! engine over an unbounded channel so a slow consumer can never stall reads
//! (a stalled read starves pongs and trips Kalshi's error 25).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::exchange::kalshi::Kalshi;
use crate::metrics::Metrics;

#[derive(Debug)]
pub enum SocketCmd {
    /// Send this text frame.
    Send(String),
    /// Close and reconnect (resubscription is the engine's job on `Connected`).
    Reconnect,
}

#[derive(Debug)]
pub enum SocketMsg {
    Connected {
        source: usize,
        conn: u64,
    },
    Frame {
        source: usize,
        received_at: DateTime<Utc>,
        text: String,
    },
    Disconnected {
        source: usize,
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct SocketConfig {
    /// No frame of any kind (pings included) for this long means the
    /// connection is dead even if TCP says otherwise.
    pub silence_timeout: Duration,
    /// Planned full reconnect (extension 2). Jittered +-10%.
    pub reconnect_every: Option<Duration>,
    /// Added to the first connection's rotation deadline so redundant
    /// sockets of one shard never rotate together.
    pub initial_offset: Duration,
    /// Wait this long before the very first connect, so dozens of shards do
    /// not all take their initial snapshot burst through one downlink at once.
    pub initial_connect_delay: Duration,
    pub max_backoff: Duration,
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self {
            silence_timeout: Duration::from_secs(30),
            reconnect_every: None,
            initial_offset: Duration::ZERO,
            initial_connect_delay: Duration::ZERO,
            max_backoff: Duration::from_secs(30),
        }
    }
}

fn jitter(d: Duration) -> Duration {
    use rand::Rng as _;
    let f = rand::thread_rng().gen_range(0.9..1.1);
    d.mul_f64(f)
}

#[allow(clippy::too_many_arguments)]
pub async fn run_socket(
    ex: Arc<Kalshi>,
    source: usize,
    label: String,
    mut cmds: mpsc::UnboundedReceiver<SocketCmd>,
    out: mpsc::UnboundedSender<SocketMsg>,
    cancel: CancellationToken,
    cfg: SocketConfig,
    metrics: Arc<Metrics>,
) {
    let mut backoff = Duration::from_secs(1);
    let mut conn: u64 = 0;
    if !cfg.initial_connect_delay.is_zero() {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(cfg.initial_connect_delay) => {}
        }
    }
    while !cancel.is_cancelled() {
        let ws = tokio::select! {
            _ = cancel.cancelled() => return,
            r = ex.connect() => r,
        };
        let ws = match ws {
            Ok(ws) => ws,
            Err(e) => {
                metrics.ws_errors.inc();
                tracing::warn!(%label, error = %e, ?backoff, "connect failed");
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(jitter(backoff)) => {}
                }
                backoff = (backoff * 2).min(cfg.max_backoff);
                continue;
            }
        };
        backoff = Duration::from_secs(1);
        conn += 1;
        if conn > 1 {
            metrics.reconnects.inc();
        }
        metrics.sockets_connected.inc();
        // Commands queued for the previous connection are meaningless now.
        while cmds.try_recv().is_ok() {}
        let (mut sink, mut stream) = ws.split();
        if out.send(SocketMsg::Connected { source, conn }).is_err() {
            return;
        }
        let deadline = cfg.reconnect_every.map(|d| {
            let offset = if conn == 1 { cfg.initial_offset } else { Duration::ZERO };
            Instant::now() + offset + jitter(d)
        });
        let reason: String = loop {
            let rotation = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let _ = sink.send(Message::Close(None)).await;
                    break "shutdown".to_owned();
                }
                cmd = cmds.recv() => match cmd {
                    None => break "engine dropped".to_owned(),
                    Some(SocketCmd::Send(text)) => {
                        if let Err(e) = sink.send(Message::Text(text.into())).await {
                            break format!("send: {e}");
                        }
                    }
                    Some(SocketCmd::Reconnect) => {
                        let _ = sink.send(Message::Close(None)).await;
                        break "reconnect requested".to_owned();
                    }
                },
                item = tokio::time::timeout(cfg.silence_timeout, stream.next()) => match item {
                    Err(_) => break format!("no frames for {:?}", cfg.silence_timeout),
                    Ok(None) => break "stream ended".to_owned(),
                    Ok(Some(Err(e))) => break format!("read: {e}"),
                    Ok(Some(Ok(msg))) => {
                        // Stamp before anything else touches the frame.
                        let received_at = Utc::now();
                        match msg {
                            Message::Text(t) => {
                                metrics.frames.inc();
                                metrics.bytes_in.add(t.len() as u64);
                                let text = t.as_str().to_owned();
                                if out.send(SocketMsg::Frame { source, received_at, text }).is_err() {
                                    return;
                                }
                            }
                            Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                                Ok(text) => {
                                    metrics.frames.inc();
                                    metrics.bytes_in.add(text.len() as u64);
                                    if out.send(SocketMsg::Frame { source, received_at, text }).is_err() {
                                        return;
                                    }
                                }
                                Err(_) => tracing::warn!(%label, "non-utf8 binary frame ignored"),
                            },
                            Message::Ping(payload) => {
                                if let Err(e) = sink.send(Message::Pong(payload)).await {
                                    break format!("pong: {e}");
                                }
                            }
                            Message::Pong(_) | Message::Frame(_) => {}
                            Message::Close(frame) => break format!("server close: {frame:?}"),
                        }
                    }
                },
                _ = rotation => {
                    let _ = sink.send(Message::Close(None)).await;
                    break "scheduled reconnect".to_owned();
                }
            }
        };
        metrics.sockets_connected.dec();
        tracing::info!(%label, conn, %reason, "socket closed");
        if out.send(SocketMsg::Disconnected { source, reason }).is_err() {
            return;
        }
    }
}
