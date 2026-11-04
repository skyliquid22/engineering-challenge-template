//! JSON envelope -> `Decoded`. Converts `no` levels to yes-denominated asks.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::Side;
use crate::exchange::{Decoded, Event, LifecycleKind};
use crate::fixed::{no_to_yes_price, parse_fixed};

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    msg: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct SnapshotMsg {
    market_ticker: String,
    #[serde(default)]
    clock: u64,
    #[serde(default)]
    yes_dollars_fp: Vec<(String, String)>,
    #[serde(default)]
    no_dollars_fp: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct DeltaMsg {
    market_ticker: String,
    #[serde(default)]
    clock: u64,
    price_dollars: String,
    delta_fp: String,
    side: String,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct SubscribedMsg {
    channel: String,
    sid: u64,
}

#[derive(Deserialize)]
struct ErrorMsg {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    msg: String,
}

#[derive(Deserialize)]
struct LifecycleMsg {
    #[serde(default)]
    market_ticker: String,
    #[serde(default)]
    event_type: String,
}

fn ms_to_dt(ms: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
}

fn parse_msg<'a, T: Deserialize<'a>>(msg: Option<&'a RawValue>, ty: &str) -> anyhow::Result<T> {
    let raw = msg.ok_or_else(|| anyhow::anyhow!("{ty}: missing msg"))?;
    serde_json::from_str(raw.get()).map_err(|e| anyhow::anyhow!("{ty}: bad msg: {e}"))
}

type Levels = Vec<(i64, i64)>;

fn levels(yes: &[(String, String)], no: &[(String, String)]) -> anyhow::Result<(Levels, Levels)> {
    let mut bids = Vec::with_capacity(yes.len());
    for (p, q) in yes {
        bids.push((parse_fixed(p)?, parse_fixed(q)?));
    }
    let mut asks = Vec::with_capacity(no.len());
    for (p, q) in no {
        asks.push((no_to_yes_price(parse_fixed(p)?)?, parse_fixed(q)?));
    }
    Ok((bids, asks))
}

pub(super) fn decode(text: &str) -> anyhow::Result<Decoded> {
    let env: Envelope = serde_json::from_str(text).map_err(|e| anyhow::anyhow!("envelope: {e}"))?;
    let sending_ts = env.sending_ts_ms.and_then(ms_to_dt);
    let event = match env.ty {
        "orderbook_snapshot" => {
            let m: SnapshotMsg = parse_msg(env.msg, env.ty)?;
            let (bids, asks) = levels(&m.yes_dollars_fp, &m.no_dollars_fp)?;
            Event::Snapshot {
                symbol: m.market_ticker,
                bids,
                asks,
                clock: m.clock,
                exchange_ts: sending_ts,
            }
        }
        "orderbook_delta" => {
            let m: DeltaMsg = parse_msg(env.msg, env.ty)?;
            let price = parse_fixed(&m.price_dollars)?;
            let (side, price) = match m.side.as_str() {
                "yes" => (Side::Buy, price),
                "no" => (Side::Sell, no_to_yes_price(price)?),
                other => anyhow::bail!("orderbook_delta: unknown side {other:?}"),
            };
            Event::Delta {
                symbol: m.market_ticker,
                side,
                price,
                delta: parse_fixed(&m.delta_fp)?,
                clock: m.clock,
                exchange_ts: m.ts_ms.and_then(ms_to_dt).or(sending_ts),
            }
        }
        "subscribed" => {
            let m: SubscribedMsg = parse_msg(env.msg, env.ty)?;
            Event::Subscribed {
                channel: m.channel,
                sid: m.sid,
            }
        }
        "unsubscribed" => Event::Unsubscribed {
            sid: env.sid.unwrap_or_default(),
        },
        "ok" => Event::Ok { id: env.id },
        "error" => {
            let m: ErrorMsg = parse_msg(env.msg, env.ty).unwrap_or(ErrorMsg {
                code: 0,
                msg: text.chars().take(200).collect(),
            });
            Event::Error {
                id: env.id,
                code: m.code,
                msg: m.msg,
            }
        }
        "market_lifecycle_v2" => {
            let m: LifecycleMsg = parse_msg(env.msg, env.ty)?;
            let kind = match m.event_type.as_str() {
                "activated" => LifecycleKind::Activated,
                "deactivated" | "determined" | "settled" => LifecycleKind::Deactivated,
                _ => LifecycleKind::Other,
            };
            Event::Lifecycle {
                symbol: m.market_ticker,
                kind,
            }
        }
        _ => Event::Other,
    };
    Ok(Decoded {
        sid: env.sid,
        seq: env.seq,
        sending_ts,
        event,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SNAPSHOT: &str = r#"{"type":"orderbook_snapshot","sending_ts_ms":1669149841234,"sid":2,"seq":2,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"9b0f","clock":4182057,"yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],"no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}"#;
    const DELTA: &str = r#"{"type":"orderbook_delta","sending_ts_ms":1669149841123,"sid":2,"seq":3,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"9b0f","clock":4182061,"price_dollars":"0.9600","delta_fp":"-54.00","side":"no","ts":"2022-11-22T20:44:01Z","ts_ms":1669149841000}}"#;

    #[test]
    fn snapshot_converts_no_to_yes_asks() {
        let d = decode(SNAPSHOT).unwrap();
        assert_eq!((d.sid, d.seq), (Some(2), Some(2)));
        let Event::Snapshot { symbol, bids, asks, clock, exchange_ts } = d.event else {
            panic!("not a snapshot");
        };
        assert_eq!(symbol, "FED-23DEC-T3.00");
        assert_eq!(bids, vec![(80_000, 300_000_000), (220_000, 333_000_000)]);
        assert_eq!(asks, vec![(460_000, 20_000_000), (440_000, 146_000_000)]);
        assert_eq!(clock, 4182057);
        assert_eq!(exchange_ts.unwrap().timestamp_millis(), 1669149841234);
    }

    #[test]
    fn delta_on_no_side_is_a_yes_ask() {
        let d = decode(DELTA).unwrap();
        let Event::Delta { side, price, delta, exchange_ts, .. } = d.event else {
            panic!("not a delta");
        };
        assert_eq!(side, Side::Sell);
        assert_eq!(price, 40_000);
        assert_eq!(delta, -54_000_000);
        assert_eq!(exchange_ts.unwrap().timestamp_millis(), 1669149841000);
    }

    #[test]
    fn empty_sides_are_allowed() {
        let d = decode(r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"X","clock":0}}"#).unwrap();
        assert!(matches!(d.event, Event::Snapshot { ref bids, ref asks, .. } if bids.is_empty() && asks.is_empty()));
    }

    #[test]
    fn control_frames() {
        let d = decode(r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}"#).unwrap();
        assert_eq!(d.event, Event::Subscribed { channel: "orderbook_delta".into(), sid: 7 });
        let d = decode(r#"{"id":4,"type":"error","msg":{"code":26,"msg":"limit"}}"#).unwrap();
        assert_eq!(d.event, Event::Error { id: Some(4), code: 26, msg: "limit".into() });
        let d = decode(r#"{"type":"market_lifecycle_v2","sid":3,"seq":9,"msg":{"market_ticker":"M","event_type":"activated"}}"#).unwrap();
        assert_eq!(d.event, Event::Lifecycle { symbol: "M".into(), kind: LifecycleKind::Activated });
        assert_eq!(decode(r#"{"type":"whatever"}"#).unwrap().event, Event::Other);
    }
}
