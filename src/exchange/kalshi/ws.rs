//! Authenticated websocket handshake.

use anyhow::Context;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use super::Kalshi;
use crate::exchange::WsStream;

pub(super) async fn connect(k: &Kalshi) -> anyhow::Result<WsStream> {
    let mut request = k
        .ws_url
        .as_str()
        .into_client_request()
        .context("building websocket request")?;
    let path = request.uri().path().to_owned();
    for (h, v) in k.auth.headers("GET", &path) {
        request
            .headers_mut()
            .insert(h, v.parse().context("header value")?);
    }
    let config = WebSocketConfig::default().max_message_size(Some(256 << 20));
    let (stream, response) =
        tokio_tungstenite::connect_async_with_config(request, Some(config), true)
            .await
            .with_context(|| format!("websocket handshake to {}", k.ws_url))?;
    tracing::debug!(status = %response.status(), "websocket connected");
    Ok(stream)
}
