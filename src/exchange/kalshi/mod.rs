//! Kalshi adapter: REST discovery, authenticated websocket, command
//! encoders and the JSON decoder. The only place that knows "yes"/"no".

mod decode;
mod rest;
mod ws;

use std::sync::Arc;

use serde_json::json;

use super::{Decoded, Exchange, Symbol, WsStream};
use crate::auth::Auth;

pub const DEFAULT_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const DEFAULT_REST_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
pub const ORDERBOOK_CHANNEL: &str = "orderbook_delta";
pub const LIFECYCLE_CHANNEL: &str = "market_lifecycle_v2";

pub struct Kalshi {
    auth: Arc<Auth>,
    ws_url: String,
    rest_url: String,
    http: reqwest::Client,
}

impl Kalshi {
    pub fn new(auth: Arc<Auth>, ws_url: impl Into<String>, rest_url: impl Into<String>) -> Self {
        Self {
            auth,
            ws_url: ws_url.into(),
            rest_url: rest_url.into(),
            http: reqwest::Client::builder()
                .user_agent("voluntas-challenge-collector/0.1")
                .timeout(std::time::Duration::from_secs(90))
                .build()
                .expect("reqwest client"),
        }
    }

    fn update_subscription(&self, id: u64, sid: u64, action: &str, symbols: &[Symbol]) -> String {
        json!({
            "id": id,
            "cmd": "update_subscription",
            "params": { "sids": [sid], "market_tickers": symbols, "action": action }
        })
        .to_string()
    }
}

impl Exchange for Kalshi {
    fn name(&self) -> &'static str {
        "kalshi"
    }

    async fn discover(&self) -> anyhow::Result<Vec<Symbol>> {
        rest::discover(self).await
    }

    async fn connect(&self) -> anyhow::Result<WsStream> {
        ws::connect(self).await
    }

    fn subscribe_books(&self, id: u64, symbols: &[Symbol]) -> String {
        json!({
            "id": id,
            "cmd": "subscribe",
            "params": { "channels": [ORDERBOOK_CHANNEL], "market_tickers": symbols }
        })
        .to_string()
    }

    fn add_markets(&self, id: u64, sid: u64, symbols: &[Symbol]) -> String {
        self.update_subscription(id, sid, "add_markets", symbols)
    }

    fn delete_markets(&self, id: u64, sid: u64, symbols: &[Symbol]) -> String {
        self.update_subscription(id, sid, "delete_markets", symbols)
    }

    fn get_snapshot(&self, id: u64, sid: u64, symbols: &[Symbol]) -> String {
        self.update_subscription(id, sid, "get_snapshot", symbols)
    }

    fn subscribe_lifecycle(&self, id: u64) -> String {
        json!({
            "id": id,
            "cmd": "subscribe",
            "params": { "channels": [LIFECYCLE_CHANNEL] }
        })
        .to_string()
    }

    fn decode(&self, text: &str) -> anyhow::Result<Decoded> {
        decode::decode(text)
    }
}
