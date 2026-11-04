//! Exchange-agnostic event model. Everything above this module (books,
//! engine, writer) speaks `Event`; only the exchange adapters know wire formats.

pub mod kalshi;

use chrono::{DateTime, Utc};

use crate::Side;

pub type Symbol = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleKind {
    /// Market is open for trading: subscribe.
    Activated,
    /// Market stopped trading (deactivated, determined, settled): unsubscribe.
    Deactivated,
    /// Informational; no action.
    Other,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Full book for one symbol. Prices/quantities already in fixed point,
    /// asks already in yes terms.
    Snapshot {
        symbol: Symbol,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        clock: u64,
        exchange_ts: Option<DateTime<Utc>>,
    },
    /// One level change. `delta` is signed; the engine computes the absolute size.
    Delta {
        symbol: Symbol,
        side: Side,
        price: i64,
        delta: i64,
        clock: u64,
        exchange_ts: Option<DateTime<Utc>>,
    },
    Subscribed { channel: String, sid: u64 },
    Unsubscribed { sid: u64 },
    Ok { id: Option<u64> },
    Error { id: Option<u64>, code: u32, msg: String },
    Lifecycle { symbol: Symbol, kind: LifecycleKind },
    /// Anything we do not model (keep-alive text, unknown types).
    Other,
}

/// A decoded frame: the per-subscription ordering fields plus the event.
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    /// When the exchange queued the frame, if it says.
    pub sending_ts: Option<DateTime<Utc>>,
    pub event: Event,
}

pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
