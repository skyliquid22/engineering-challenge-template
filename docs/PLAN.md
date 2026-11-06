> Written before implementation (2026-11-04). Phase 0 numbers are in probe/ and in the README.

# Voluntas challenge — execution plan

Companion to ARCHITECTURE.md, which stays the design reference with the four
changes listed under "Design changes" below. This file is the schedule and the
acceptance criteria. Metrics from Phase 0 fill in the PRD numbers.

## Verified facts (docs.kalshi.com, 2026-11-04; template at ./template)

- Template builds and runs on rustc 1.99.1 (edition 2024). `AlignedSbeWriter`
  buffers a whole 64 MiB chunk in memory and writes only on chunk-full or
  `flush()`. Reader chunk size is hard-wired to 64 MiB. Output is a file path,
  not stdout, despite the page's wording; SIGINT clause says "close the file".
- WS: `wss://external-api-ws.kalshi.com/trade-api/ws/v2`. Auth on the handshake:
  `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (ms), `KALSHI-ACCESS-SIGNATURE`
  = base64(sign(ts + "GET" + "/trade-api/ws/v2")). Ed25519 (recommended) or
  RSA-PSS-SHA256 (MGF1 SHA-256, salt = 32). Pick by parsed key type.
- Kalshi pings every 10 s with body `heartbeat`; we must Pong.
- `orderbook_delta` requires `market_ticker(s)`; no subscribe-all. One
  orderbook_delta subscription per connection; repeated `subscribe` adds markets.
  `update_subscription` actions: `add_markets`, `delete_markets`, `get_snapshot`
  (get_snapshot requires `market_tickers`; it does not resend everything).
- `seq` contiguous per sid. On gap: `get_snapshot` for all our tickers or
  unsubscribe+resubscribe. `clock` identical across connections, per-market
  monotonic in real-time mode but skips values, not comparable across markets.
  Snapshot includes all changes with clock <= its own.
- Snapshot sides are optional (absent when empty). Prices 4-dec strings,
  sizes 2-dec strings. Deltas: `price_dollars`, `delta_fp`, `side` yes|no,
  `ts_ms` optional, `sending_ts_ms` optional on envelope.
- Errors: 25 buffer overflow (terminal, resubscribe), 26 per-sub market limit
  (number undocumented), 27 command rate limit, 11 interval mismatch.
- `shard_factor`/`shard_key` are for the `communications` channel only. Not
  usable for orderbook sharding.
- `market_lifecycle_v2`: all markets, no filter. Events: created, activated,
  deactivated, close_date_updated, determined, settled.
- REST `GET /trade-api/v2/markets?status=open&mve_filter=exclude&limit=1000`.
  Basic tier: 200 read tokens/s, 10 per call => 20 req/s. WS connections per
  user on Basic: 200.

## Design changes vs ARCHITECTURE.md

1. Socket reader never blocks. Unbounded in-process queue to the engine and to
   the writer, with depth gauges and alarms. Blocking the reader starves Pongs
   and trips error 25 server-side, which is worse than local memory growth.
2. Raw-frame journal: every frame appended with `received_at` to
   `out/journal/<hour>.jsonl` (buffered, flushed every 1 s). Bounds crash loss
   to ~1 s instead of a 64 MiB chunk, feeds the round-trip test, and allows
   regenerating SBE output if a decode bug is found.
3. Every hourly SBE file opens with a full snapshot of all live books so each
   file is self-contained.
4. Lifecycle socket opens before REST discovery; its events are buffered and
   applied after the initial universe is built. No startup gap.

## Phases

### Phase 0 — probe
`cargo run --bin probe` answers, and writes to `probe/`:
- handshake OK; raw snapshot + delta frames captured verbatim
- K: subscribe with all open tickers, back off on error 26 (binary search with
  1 s spacing to avoid 27)
- open non-MVE market count
- two connections on the same shard: clock equality, repeated clocks per delta
- 5 min aggregate msg/s, peak shard msg/s, SBE bytes/min
- lifecycle lag vs REST (activated events observed vs polling)
- idle 90 s survives (Pong works)
These numbers go into the PRD and README.

### Phase 1 — base collector
Modules per ARCHITECTURE-2 §7. Deliverables:
- `collector` binary: discovery, allocator, N shards, engine, writer, journal,
  SIGINT/SIGTERM shutdown, metrics log every 10 s
- unit tests: fixed-point parsing, yes/no -> bid/ask, apply_delta (incl. zero
  and negative), apply_snapshot, seq-gap handling
- integration test: recorded JSON -> decode -> books -> SBE file ->
  AlignedSbeReader -> assert sequence
- exchange trait boundary with a Polymarket module containing `todo!()` bodies

### Phase 2 — live run and write-up 
- 10 min live run, inspect output with a small `inspect` binary that reads the
  SBE file and prints counts per symbol and timestamp monotonicity
- README: how to run, decisions (ARCHITECTURE-2 §9), measured numbers, what is
  not done and why

### Extensions 
- Ext 1 redundant sockets: +30 min build, +20 min soak to show a failover with
  snapshot-on-switch in the output. Depends on Phase 0 clock results.
- Ext 2 subscription health: +15 min; per-symbol EMA silence detection,
  staggered 6 h reconnects.
- Ext 4 latency write-up: needs hours of data and ideally a second vantage
  point (us-east-1). Overnight job, not an hour job.

## Acceptance
- `cargo test` green; `cargo clippy` clean
- 10 min live run: zero gaps unresolved, every book Live, minute snapshots for
  all symbols, file reads back fully with AlignedSbeReader
- SIGINT: Close frames sent, writer drains, final chunk flushed, exit 0
