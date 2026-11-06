//! Polymarket adapter: shape only. Shows that nothing outside `exchange` has
//! to change to add an exchange; the bodies are deliberately unimplemented.

use super::{Decoded, Exchange, Symbol, WsStream};

pub struct Polymarket;

impl Exchange for Polymarket {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    async fn discover(&self) -> anyhow::Result<Vec<Symbol>> {
        // GET https://clob.polymarket.com/markets, collect active token ids.
        todo!("polymarket discovery")
    }

    async fn connect(&self) -> anyhow::Result<WsStream> {
        // wss://ws-subscriptions-clob.polymarket.com/ws/market, no auth.
        todo!("polymarket connect")
    }

    fn subscribe_books(&self, _id: u64, _symbols: &[Symbol]) -> String {
        // {"assets_ids": [...], "type": "market"}
        todo!("polymarket subscribe")
    }

    fn add_markets(&self, _id: u64, _sid: u64, _symbols: &[Symbol]) -> String {
        todo!("polymarket add_markets")
    }

    fn delete_markets(&self, _id: u64, _sid: u64, _symbols: &[Symbol]) -> String {
        todo!("polymarket delete_markets")
    }

    fn get_snapshot(&self, _id: u64, _sid: u64, _symbols: &[Symbol]) -> String {
        todo!("polymarket get_snapshot")
    }

    fn subscribe_lifecycle(&self, _id: u64) -> String {
        todo!("polymarket lifecycle")
    }

    fn decode(&self, _text: &str) -> anyhow::Result<Decoded> {
        // "book" -> Event::Snapshot, "price_change" -> Event::Delta (absolute
        // size, so delta = new - old must be computed against our book).
        todo!("polymarket decode")
    }
}
