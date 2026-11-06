This is the plan I wrote before the probe, kept unchanged. README.md describes what I actually built, and its section "What the probe changed" lists every place where the build diverged from this plan and why.

# Kalshi order book collector — architecture plan

Status: hypothesis. The hour-one smoke tests at the bottom decide the numbers (shard size, socket count, channel depth) and settle three open questions. Everything else is fixed by the challenge, the template, or the Kalshi API.

## 1. What is fixed

### By the template (read the code, not the page)

- Output is SBE through `AlignedSbeWriter`. Two message types: `Snapshot { received_at, exchange_timestamp, bids[], asks[], symbol }` and `PriceLevelUpdate { backtest_timestamp, exchange_timestamp, price_level, quantity, side, symbol }`.
- Fixed point: `1_000_000` = 1 USDC and 1 contract. `price_level` and `quantity` are `i64`.
- `Side::Buy` = bid, `Side::Sell` = ask. Discriminants are wire values.
- `quantity` on a `PriceLevelUpdate` is the absolute size at that level after the update, not the delta. Zero means the level is gone.
- The reader reads 64 MiB chunks and the chunk size is not configurable from outside (`open_with_chunk_size` is private). The writer must use `64 << 20`.
- `flush()` pads the current chunk to 64 MiB with zeros. Flushing is therefore a rotation event, not a durability tool.
- `write_msg` rejects any message larger than one chunk minus 8 bytes. A snapshot of one market is far below that.

### By the challenge page

- Subscribe to every live regular market. Multivariate event (MVE) markets are out of scope.
- Maintain a book per symbol. Emit deltas plus a full snapshot of every book once a minute.
- Watch for new symbols and subscribe; unsubscribe symbols that stop trading.
- Spread symbols across several websockets. One socket for everything is expected to fail.
- On SIGINT: close the websockets and close the file being written.

### By the Kalshi API (docs as of Nov 2026)

- Websocket: `wss://external-api-ws.kalshi.com/trade-api/ws/v2` (the older `wss://api.elections.kalshi.com/trade-api/ws/v2` still works).
- Auth on the handshake: headers `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (unix ms), `KALSHI-ACCESS-SIGNATURE` = base64(RSA-PSS-SHA256(ts + "GET" + "/trade-api/ws/v2")), MGF1 SHA-256, salt length = digest length.
- Discovery: `GET https://api.elections.kalshi.com/trade-api/v2/markets?status=open&mve_filter=exclude&limit=1000&cursor=...`. Basic tier read limit is 20 req/s. Belt and braces: also drop any ticker starting with `KXMVE`.
- Channel `orderbook_delta`. Subscribe with `market_tickers: [...]`. The server sends `orderbook_snapshot` for each market, then `orderbook_delta`. A repeated `subscribe` on the same channel adds to the existing `sid`. `update_subscription` on a `sid` supports `add_markets`, `delete_markets`, `get_snapshot`. `update_interval_ms: 0` for real time.
- Envelope: `{ type, sid, seq, msg, sending_ts_ms? }`. `seq` is contiguous per `sid`. `msg.clock` is the exchange clock and is the same on every connection.
- Snapshot `msg`: `market_ticker`, `clock`, `yes_dollars_fp: [[price, size], ...]`, `no_dollars_fp: [[price, size], ...]` with prices as 4-decimal strings and sizes as 2-decimal strings.
- Delta `msg`: `market_ticker`, `clock`, `price_dollars` (string), `delta_fp` (signed string), `side: "yes" | "no"`, `ts`, `ts_ms`.
- Semantics: `yes` is the bid side. `no` is the ask side, and a `no` level at price p is a yes ask at 1.0000 - p.
- Errors that drive the design: 25 subscription buffer overflow (terminal, resubscribe), 26 per-subscription market limit (number undocumented), 27 command rate limit.
- Heartbeat: the server sends Ping every 10 s. The client must Pong or it gets dropped.
- Lifecycle: channel `market_lifecycle_v2`, no market filter, all non-MVE markets. Event types: `created`, `activated`, `deactivated`, `close_date_updated`, `determined`, `settled`.

## 2. Data model

```
Symbol        = String (Kalshi market ticker)
Price, Qty    = i64, fixed point 1e6

Event (exchange-agnostic, produced by the adapter's decoder)
  BookSnapshot { symbol, bids: Vec<(Price, Qty)>, asks: Vec<(Price, Qty)>, exchange_ts, clock, received_at }
  LevelDelta   { symbol, side: Side, price: Price, delta: i64, exchange_ts, clock, received_at }
  Control      { Subscribed { sid }, Error { code, msg }, Gap { sid, expected, got } }
  Lifecycle    { symbol, Added | Removed }

Book
  bids: BTreeMap<Price, Qty>       // yes side, as received
  asks: BTreeMap<Price, Qty>       // no side converted: price = 1_000_000 - p_no
  last_clock: u64
  last_exchange_ts: Timestamp
  state: Syncing | Live
```

The yes/no to bid/ask conversion lives only in the Kalshi decoder. Books, engine, allocator, writer never see "yes" or "no". That is what makes the exchange adapter (extension 3) a boundary that already exists rather than a refactor.

## 3. Tasks

Tokio, multi-thread runtime. One task per concern, channels between them, no shared mutable state.

### universe

- Startup: paginate `GET /markets?status=open&mve_filter=exclude` into the initial symbol set.
- Subscribes `market_lifecycle_v2` on a dedicated control websocket. `activated` adds, `determined`/`settled`/`deactivated` removes. (`created` markets are not yet open; wait for `activated`.)
- Every 5 minutes, re-run discovery and diff against the live set. This catches lifecycle events missed across reconnects. Hour-one check decides whether lifecycle is reliable enough to make this interval longer.
- Emits `Add(symbol)` / `Remove(symbol)` to the allocator.

### allocator (lives in the universe task)

- Owns `symbol -> shard_id`. Assigns new symbols round-robin across N shards so hot markets spread rather than cluster; refuses to exceed K per shard and spawns a new shard when all are full.
- Sends `AddMarkets(Vec<Symbol>)` / `RemoveMarkets(Vec<Symbol>)` to shards, batched, so one new sports event with 40 markets is one `update_subscription`, not 40.

### shard (×N)

Each shard is an engine plus one or more socket workers.

- Socket worker: owns the websocket. Reads frames, stamps `received_at` immediately after the read and before any parsing, forwards `(received_at, raw_frame)` to the engine over an internal channel. Handles Pong. On disconnect: exponential backoff 1 s to 30 s, reconnect, resubscribe the shard's full symbol list.
- Engine: owns the books for the shard's symbols and the `seq` tracker for its `sid`. Loop is a `select!` over socket frames, allocator commands, a 60 s tick, and the cancellation token.
  - Frame: decode via the adapter into `Event`s. `BookSnapshot` replaces the book, sets `Live`, emits a `Snapshot`. `LevelDelta` applies to the book and emits a `PriceLevelUpdate` with the absolute quantity. A delta for a `Syncing` book is dropped (the snapshot that is coming supersedes it).
  - `seq` gap: mark every book on that `sid` as `Syncing`, send `get_snapshot`. Log it with a counter.
  - A delta that would drive a level negative is treated as a gap for that symbol (resnapshot), not clamped silently.
  - 60 s tick: emit a `Snapshot` for every `Live` book from local state, with `exchange_timestamp = last_exchange_ts`.
  - Reconnect: all books `Syncing`; the resubscribe snapshots bring them back and are emitted, which is the "snapshot at each switch" the extensions ask for.
- Why the books live in the shard and not in a central store: single owner, no locks, in-order processing of one socket's stream, and the minute snapshot is a local iteration. The cost is that a symbol's book is tied to the shard that holds its subscription, which is what we want anyway.

### writer

- Single consumer of a bounded `mpsc<DomainMsg>`. Owns the `AlignedSbeWriter`.
- File rotation hourly on the UTC boundary: flush, close, open `out/kalshi/<YYYYMMDDTHH>Z.sbe`. This bounds the zero padding to at most 64 MiB per hour and bounds crash loss to the current chunk.
- Backpressure: bounded channel. If the writer falls behind, shards block on send rather than drop. `received_at` is already stamped, so time accuracy survives a stall; only latency to disk suffers. Channel depth comes from hour-one rates.
- Shutdown: when every sender is dropped the channel closes; drain, flush, exit.

### shutdown

- `tokio::signal::ctrl_c` -> `CancellationToken`. Shards send a Close frame, drop their writer senders, exit. Writer drains and flushes. `main` joins everything with a 10 s timeout and exits non-zero if the writer did not finish cleanly.

### metrics

- Per shard: messages/s, deltas applied, snapshots received, gaps, reconnects, books live vs syncing. Global: writer queue depth, bytes written. Logged every 10 s through `tracing`. These numbers go in the README and are the basis of extension 2.

## 4. Conversions and invariants (unit tested)

- Decimal strings to fixed point without floats: `"0.0800" -> 80_000`, `"300.00" -> 300_000_000`, `"-54.00" -> -54_000_000`. Reject more than 6 decimals.
- Ask price: `1_000_000 - p_no`. Quantity unchanged.
- `apply_delta`: `new = old + delta`. `new == 0` removes the level and still emits an update with quantity 0. `new < 0` is an error that triggers resnapshot.
- `apply_snapshot` replaces both sides entirely. A reader that sees a `Snapshot` resets its view; levels that vanished between the old book and the new snapshot are not emitted as zero updates.
- Timestamps: `received_at` and `backtest_timestamp` are wall clock microseconds captured at frame receipt. `exchange_timestamp` is `ts_ms * 1000` for deltas; for exchange snapshots it is whatever the snapshot carries (hour-one check), falling back to `sending_ts_ms`, then `received_at`.
- Round trip: an integration test replays a recorded JSON stream through decode -> book -> writer, then reads the file back with `AlignedSbeReader` and asserts the decoded sequence. The template's current `main.rs` becomes `tests/roundtrip.rs` rather than being deleted.

## 5. Sharding policy

- K = markets per subscription, found by probing (subscribe with 1000 tickers; if error 26, binary search down). Also watch whether error 25 appears on a full shard during a busy period, which would mean K is bounded by throughput, not count.
- N = ceil(open_markets / K) + 1 at startup, re-evaluated as the universe grows.
- Round-robin assignment, sticky for the life of the symbol.

## 6. Extensions, in the order they pay off

1. Exchange adapter trait (cheap, already implied by the data model): `discover()`, `connect(symbols)`, `decode(raw, received_at) -> Vec<Event>`, `subscribe/add/remove/get_snapshot` encoders. Engine, allocator, books, writer are generic over it. A Polymarket stub with `todo!()` bodies shows the shape; do not implement it.
2. Redundant sockets: a second socket worker per shard with the same subscription feeding the same engine. The engine deduplicates on `(symbol, clock)` and keeps `max_clock_applied` per symbol. On primary silence longer than T with the standby still flowing, promote the standby and request `get_snapshot` so the switch is marked in the output. Depends on the hour-one check that `clock` is per-market monotonic and identical across connections.
3. Subscription health: socket-level silence (no frames, including pings, for 30 s) triggers reconnect. Symbol-level silence is only meaningful relative to the symbol's own recent rate, since most Kalshi markets are quiet for hours; use a per-symbol EMA of inter-arrival time and flag only symbols that were active and went silent while `clock` kept advancing on their siblings. Staggered full reconnect of each shard every 6 hours.
4. Latency write-up: distribution of `received_at - ts_ms` per shard; compare `update_interval_ms` 0 vs 100, permessage-deflate on vs off, TCP_NODELAY, and a run from us-east-1 against one from the Bay Area. Only if everything above is done.

## 7. Module layout

```
src/
  main.rs            wiring only: config, spawn tasks, await shutdown
  config.rs          CLI/env: key id, private key path, out dir, N, K, channel depth, reconcile interval
  auth.rs            signature headers
  fixed.rs           decimal string <-> i64 fixed point
  book.rs            Book, apply_snapshot, apply_delta, to_snapshot
  exchange/
    mod.rs           Exchange trait, Event, Side mapping helpers
    kalshi/
      rest.rs        discovery (paginated, mve_filter=exclude)
      ws.rs          connect with headers, command encoders, Pong
      decode.rs      JSON envelope -> Vec<Event>, yes/no -> bid/ask
  universe.rs        discovery + lifecycle + reconcile + allocator
  shard.rs           engine + socket worker
  writer.rs          AlignedSbeWriter owner, rotation, drain on shutdown
  metrics.rs         counters and the 10 s log line
  shutdown.rs        CancellationToken plumbing
tests/
  roundtrip.rs       recorded JSON -> file -> AlignedSbeReader
```

Crates beyond the template: `tokio`, `tokio-tungstenite` (rustls), `reqwest` (rustls, json), `serde`, `serde_json`, `rsa`, `sha2`, `base64`, `pkcs8`, `tracing`, `tracing-subscriber`, `tokio-util`, `clap`.

## 8. Hour-one smoke tests (these set the numbers)

1. Handshake works against `external-api-ws`; if not, the `api.elections` host.
2. Capture one raw `orderbook_snapshot` and one `orderbook_delta` verbatim. Confirm field names, whether the snapshot carries any timestamp, and that `clock` is present.
3. Open two connections to the same market. Confirm `clock` values match and are monotonic per market.
4. Subscribe with 1000 tickers. Record whether you get 26, 25, or success. Find K.
5. Count open non-MVE markets. Measure aggregate messages/s for 10 minutes and the peak on the busiest shard. Measure SBE bytes/minute to size the writer channel and confirm hourly rotation is sane.
6. After `add_markets`, confirm the snapshot arrives before any delta for the new market.
7. Watch `market_lifecycle_v2` for 10 minutes against REST polling. Measure `activated` lag.
8. Leave a connection idle for 90 s. Confirm Pong is being sent and the connection survives.
9. Send `get_snapshot` on a `sid`. Confirm whether it resends every market or needs tickers.

## 9. Decisions to state in the README

- Asks are stored and emitted in yes-price terms (1 - no price) so the output is one yes-denominated book per market.
- Chunk size is 64 MiB because the template reader requires it; files rotate hourly; crash loss is bounded by one chunk.
- Books are owned by shards, not a central store.
- Backpressure blocks producers rather than dropping, because completeness is the stated priority.
- A `Snapshot` resets the reader's view; vanished levels are not emitted as zero updates.
- How the submission was built: plan and module boundaries by hand, implementation by agents under these conventions, every line and every test reviewed.
