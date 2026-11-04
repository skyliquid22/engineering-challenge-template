//! Kalshi adapter. The only place that knows "yes" and "no".

mod decode;

use super::Decoded;

/// Decode one text frame from the Kalshi websocket.
pub fn decode(text: &str) -> anyhow::Result<Decoded> {
    decode::decode(text)
}
