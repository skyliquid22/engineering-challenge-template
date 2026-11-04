//! Paginated market discovery over REST.

use anyhow::Context;
use serde::Deserialize;

use super::Kalshi;
use crate::exchange::Symbol;

#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: String,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
}

/// Every open, non-multivariate market ticker. `mve_filter=exclude` does the
/// filtering server side; the `mve_collection_ticker` check is belt and
/// braces. The field is set on MVE markets and absent on regular ones, see
/// probe/rest_market_samples.json for captured examples of both.
pub(super) async fn discover(k: &Kalshi) -> anyhow::Result<Vec<Symbol>> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    let path = "/trade-api/v2/markets";
    loop {
        let mut url = format!(
            "{}/markets?status=open&mve_filter=exclude&limit=1000",
            k.rest_url
        );
        if !cursor.is_empty() {
            url.push_str("&cursor=");
            url.push_str(&cursor);
        }
        let page: MarketsPage = get_with_retry(k, &url, path).await?;
        for m in page.markets {
            let is_mve = m
                .mve_collection_ticker
                .as_deref()
                .is_some_and(|s| !s.is_empty());
            if is_mve || m.ticker.is_empty() {
                continue;
            }
            // `status=open` returns `active`; tolerate either spelling.
            if !m.status.is_empty() && m.status != "active" && m.status != "open" {
                continue;
            }
            out.push(m.ticker);
        }
        if page.cursor.is_empty() {
            break;
        }
        cursor = page.cursor;
    }
    out.sort();
    out.dedup();
    Ok(out)
}

async fn get_with_retry<T: serde::de::DeserializeOwned>(
    k: &Kalshi,
    url: &str,
    sign_path: &str,
) -> anyhow::Result<T> {
    let mut delay = std::time::Duration::from_millis(250);
    let mut last_err = None;
    for _ in 0..5 {
        let mut req = k.http.get(url);
        for (h, v) in k.auth.headers("GET", sign_path) {
            req = req.header(h, v);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                return resp.json::<T>().await.context("decoding markets page");
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                last_err = Some(anyhow::anyhow!("GET {url}: {status} {body:.200}"));
                if status.is_client_error() && status.as_u16() != 429 {
                    break;
                }
            }
            Err(e) => last_err = Some(anyhow::Error::from(e).context(format!("GET {url}"))),
        }
        tokio::time::sleep(delay).await;
        delay *= 2;
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("GET {url} failed")))
}
