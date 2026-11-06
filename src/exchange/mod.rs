//! Exchange-agnostic event model and the adapter trait (extension 3).
//!
//! Everything above this module (books, engine, allocator, writer) speaks
//! `Event`. Only `kalshi` (and a future `polymarket`) know wire formats.

pub mod kalshi;
pub mod polymarket;

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

/// The adapter boundary. The collector is generic over this; see `kalshi`.
pub trait Exchange: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// Every currently tradable symbol (REST).
    fn discover(&self) -> impl std::future::Future<Output = anyhow::Result<Vec<Symbol>>> + Send;

    /// Open an authenticated websocket.
    fn connect(&self) -> impl std::future::Future<Output = anyhow::Result<WsStream>> + Send;

    /// Command text to subscribe to order book updates for `symbols`.
    fn subscribe_books(&self, id: u64, symbols: &[Symbol]) -> String;
    fn add_markets(&self, id: u64, sid: u64, symbols: &[Symbol]) -> String;
    fn delete_markets(&self, id: u64, sid: u64, symbols: &[Symbol]) -> String;
    fn get_snapshot(&self, id: u64, sid: u64, symbols: &[Symbol]) -> String;
    /// Command text to subscribe to market lifecycle events (all markets).
    fn subscribe_lifecycle(&self, id: u64) -> String;

    /// Decode one text frame.
    fn decode(&self, text: &str) -> anyhow::Result<Decoded>;
}
