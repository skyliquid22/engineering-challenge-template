# Kalshi Order Book Collector
**Complete, time-accurate order book capture for every live Kalshi market**

*A Rust service that subscribes to all non-MVE Kalshi markets over many
redundant websockets, maintains a yes-denominated book per market, and writes
the Voluntas challenge's SBE snapshots and price-level updates with
microsecond receive stamps.*

---

## Scope

- **In scope:** the challenge core (all live non-MVE markets, a book per
  market, deltas plus a snapshot a minute, new and dying markets, many
  sockets, SIGINT), extensions 1 to 3 in full, extension 4 as measurement and
  a written plan.
- **Out of scope:** a working Polymarket adapter (the stub shows the shape),
  trading, and latency optimization beyond what the measurements justify.

---
## Coverage

- Core (all live non-MVE markets, a book per market, deltas plus a snapshot a minute, new and dying markets, many sockets, SIGINT): done, measured; see Quickstart, Output Semantics, and the runs table under Measurements.
- Extension 1, redundant websockets with a snapshot on switch: done, measured on 6,000 markets with a forced failover; see System Architecture (shard) and run 3 in docs/MEASUREMENTS.md.
- Extension 2, subscription monitoring: done (socket, source and per-market checks, planned reconnects, rate tracking as a metric), measured in runs 2 and 3; see Extensions.
- Extension 3, exchange-generic core: done for Kalshi, Polymarket is a shape-only stub, not measured; see System Architecture (exchange).
- Extension 4, latency: measured, not optimized, experiments still TBD(run); see LATENCY.md.

---

## Core Capabilities

### Discovery and universe
- Paginated, gzip-compressed REST discovery of every open non-MVE market
- Lifecycle channel opened before discovery and buffered, so no market
  activates unseen; periodic REST reconcile as the safety net
- Least-loaded allocation across shards with a per-shard cap

### Books and output
- One yes-denominated book per market; `no` levels become asks at `1 - p`
- Template SBE `Snapshot` and `PriceLevelUpdate` on stdout or in hourly files
- Snapshots on subscribe, resync, failover, and once a minute for every live book

### Resilience
- Two sockets per shard with independent books; failover writes a snapshot first
- Sequence-gap detection with in-place resync; negative levels resync one book
- Flow-controlled, adaptive subscription requests sized to what the link delivers
- Raw-frame journal that bounds crash loss to about a second

### Monitoring
- Socket, source and per-market silence and lag detection
- Planned, staggered full reconnects
- Metrics line every 10 s: rates, latency percentiles, 60 s window, RSS

### Measurement
- `probe` for exchange limits and throughput, `inspect` to verify any output
  file, `scripts/measure.sh` and `scripts/summarize.py` for repeatable runs

---

## System Architecture

```
universe ──(Add/Remove symbol batches)──▶ shard 0 ──▶ engine ──▶ writer thread ──▶ hourly .sbe
 │  REST discovery (gzip, paginated)        │  socket 0 (active)      └▶ journal thread ──▶ hourly .jsonl
 │  lifecycle websocket (all markets)       │  socket 1 (standby)
 │  periodic REST reconcile (own task)     shard 1 ...
 └  least-loaded allocator, cap per shard  shard N
```

- **universe** (`universe.rs`): opens the `market_lifecycle_v2` socket *first*
  and buffers its events, runs REST discovery (`status=open&mve_filter=exclude`,
  `mve_collection_ticker` double-checked: the field is set on MVE markets and
  absent on regular ones, see `probe/rest_market_samples.json`), then applies
  the buffer. No market
  can activate unseen in between. `activated` subscribes;
  `deactivated`/`determined`/`settled` unsubscribes. A REST pass every 15 min
  reconciles anything missed across lifecycle reconnects.
- **allocator** (in `universe.rs`): least-loaded shard first, hard cap per
  shard, spawns a new shard when all are full. Commands are batched per shard.
- **shard** (`shard.rs`): one engine plus `redundancy` socket workers all
  subscribed to the same symbols. Each *source* keeps its own books, driven by
  its own contiguous `seq` stream. Only the **active** source's changes are
  written. On the active source failing (disconnect, `seq` gap, error 25,
  silence while the standby flows) the engine switches to a healthy standby
  and first emits a snapshot of every live book it holds (extension 1's
  requirement). The failed source resyncs in the background and becomes the
  new standby.
  Subscriptions are **flow-controlled**: each source keeps about one request
  of tickers in flight and sends the next only when those snapshots have
  arrived (or stalled for 10 s). The request size adapts to the link: it
  halves when the previous request took over a second to arrive and doubles
  when it took under half a second (10 to 400 tickers), so the exchange-side
  queue stays near a second of data whatever the bandwidth. Kalshi's
  per-subscription buffer never overflows and deltas for books that are
  already live are not stuck behind a snapshot burst. Standbys wait for the
  active source to finish its initial sync before subscribing. Shards open
  their sockets 500 ms apart.
  **Per-symbol monitoring** rides on the redundancy: Kalshi's `clock` is per
  market and identical on every connection, so a live book whose clock is
  behind the standby's copy for two consecutive seconds missed an update on
  the active connection. A few such books get a `get_snapshot` each; many at
  once means the connection itself is lagging and the shard fails over. The
  check pauses while either connection's own latency runs above 2 s: then
  the lag is the link, and re-snapshots would only add to the queue.
- **socket** (`socket.rs`): exchange-agnostic. Connect with backoff,
  stamp-on-read, answer pings, reconnect on silence (30 s, pings count),
  error, close or the planned rotation (6 h, jittered, offset between the two
  sockets of a shard so they never rotate together). It never blocks: frames
  go to the engine over an unbounded channel. A blocked reader would stop
  pongs and overflow Kalshi's per-subscription buffer (error 25), turning a
  local slowdown into exchange-side data loss.
- **book** (`book.rs`): `BTreeMap` per side, `Syncing`/`Live` state. A delta
  for a syncing book is dropped (the snapshot in flight supersedes it). A delta
  that would make a level negative is treated as a desync of that one book:
  `get_snapshot` for that ticker, not a silent clamp.
- **writer** (`writer.rs`): single owner of `AlignedSbeWriter` on its own OS
  thread, hourly rotation, drain-then-flush on shutdown.
- **journal** (`journal.rs`): every frame the active source delivered, with its
  receive timestamp, one JSON line per frame, flushed every second. The SBE
  writer holds a whole 64 MiB chunk in memory until it is full, so a crash
  would otherwise lose up to a chunk; the journal bounds that to about a
  second and lets the SBE output be regenerated offline if a decode bug is
  ever found.
- **exchange** (`exchange/`): the `Exchange` trait (discover, connect, command
  encoders, decode) and the `Event` model everything else speaks. `kalshi/`
  implements it; `polymarket.rs` is a shape-only stub with `todo!()` bodies to
  show that adding an exchange touches nothing outside this directory.

Tokio multi-thread runtime, one task per concern, channels between them, no
shared mutable state except metrics counters.

---

## What the probe changed

`docs/ARCHITECTURE.md` is the plan I wrote before the probe, unchanged. Where
the build diverged from it, and why:

- Least-loaded allocation with a per-shard cap instead of round-robin: one
  rule covers growth and shard spawning, and round-robin's "spread the hot
  markets" argument did not survive a universe of 176k mostly quiet markets.
- Separate books per redundant source instead of merging two streams by
  `clock`: a failover is then trivially correct (the new source's book is
  exactly what its own stream says) and no dedup sits in the hot path. The
  clocks are still used, for the per-market stale check.
- No per-subscription market limit was hit (all 176k tickers were accepted on
  one subscription), so sharding became a throughput question: 4,000 markets
  per shard, 91 sockets, sized against the ~2,000 frames/s one connection
  drains and the Basic tier connection limit.
- Flow-controlled, adaptive subscription requests were not in the plan: the
  probe's error 25 and run 1's queueing showed that the number of snapshots in
  flight, not the number of markets, is what overflows Kalshi's buffer.
- Socket readers never block on the writer. The plan had a bounded channel
  with backpressure; a blocked reader stops pongs and fills Kalshi's buffer,
  so the queues are unbounded and their depth is a metric instead.
- A raw-frame journal, not in the plan: the template writer holds a whole
  64 MiB chunk in memory, so without it a crash loses up to a chunk.
- The lifecycle socket opens before discovery and buffers its events, instead
  of after: this closes the startup gap in which a market could activate
  unseen.
- REST reconcile every 15 minutes in its own task, not every 5 minutes
  inline: a full pass is 177 pages and about 80 s, and lifecycle events must
  keep flowing while it runs.
- Discovery pages are requested gzip-compressed: a 1000-market page is 2.2 MB
  uncompressed and took about 13 s each on this link, 0.6 s compressed.
- `seq` gaps resync in place with paced `get_snapshot` chunks over the
  subscribed set, or reconnect when there is no subscription yet; the plan
  asked for all markets in one request, which is the same burst that
  overflows.
- The MVE double-check is on `mve_collection_ticker`, the field the API sets
  on MVE markets, rather than on a `KXMVE` ticker prefix.
- Ed25519 keys are the primary signing path, RSA is still supported; the plan
  listed only RSA crates.
- SIGTERM is handled like SIGINT; the plan only mentioned SIGINT.
- Module layout: no `shutdown.rs` (signals live in `main.rs`); `socket.rs`,
  `journal.rs` and the `probe` and `inspect` binaries were added.
- Of the plan's hour-one smoke tests, 1, 2, 4, 5 and 8 were answered by the
  probe and the runs; 9 was answered by the docs (`get_snapshot` takes an
  explicit ticker list); 3 (clock equality across connections) is exercised
  by the stale-book check rather than tested in isolation; 6 and 7 were not
  run.

---

## Output Semantics

- One book per market, **in yes terms**. Kalshi's `yes` levels are bids
  (`Side::Buy`); each `no` level at price `p` is a yes ask (`Side::Sell`) at
  `1.000000 - p`, same quantity. The conversion happens only in
  `exchange/kalshi/decode.rs`; nothing else knows "yes" or "no".
- Fixed point `1_000_000 = 1 USDC = 1 contract`, parsed from Kalshi's decimal
  strings with integer arithmetic, no floats.
- `PriceLevelUpdate.quantity` is the absolute size at that level after the
  update; `0` means the level is gone.
- `Snapshot` replaces the reader's view of that symbol. Levels that vanished
  between an old book and a new exchange snapshot are *not* emitted as zero
  updates.
- A `Snapshot` is written when the exchange sends one (initial subscribe, a
  market added later, any resync), once a minute for every live book (aligned
  to the wall-clock minute across all shards), and on a failover for every
  live book of the new source.
- `received_at` / `backtest_timestamp`: wall clock, microseconds, captured in
  the socket task the instant the frame is read, before parsing.
  `exchange_timestamp`: the delta's `ts_ms`, else the envelope's
  `sending_ts_ms`, else `received_at`. Minute snapshots carry the book's last
  exchange timestamp.
- Chunk size is 64 MiB because the template reader hard-codes it. With
  `--out <dir>`, files rotate on the UTC hour and a restart in the same hour
  gets a `-2` suffix rather than truncating the earlier file.
- Messages are written in processing order across ~45 shards. `received_at`
  is monotonic per symbol, not across the whole file (neighbouring messages
  from different shards can be a few milliseconds out of order). `inspect`
  reports the count of backward steps; readers that need global order sort by
  `received_at`.

---

## Quickstart

```sh
export KALSHI_KEY_ID=...                      # API key id
export KALSHI_PRIVATE_KEY_PATH=/path/key.pem  # Ed25519 or RSA, PKCS#8 or PKCS#1
cargo run --release -- > books.sbe            # SBE to stdout (the default), logs to stderr
cargo run --release -- --out out              # hourly files out/kalshi-orderbook-<YYYYMMDD>T<HH>Z.sbe
cargo run --release -- --symbols-file my.txt  # a fixed list instead of discovery
cargo run --release --bin inspect -- out/*.sbe --check   # read back and verify
cargo run --release --bin probe               # the measurements below
```

`Ctrl-C` (SIGINT) or SIGTERM, in either output mode: every socket sends a
Close frame, the engines drop their senders, the writer drains its queue, pads
and writes the last 64 MiB chunk, and the process exits 0. All logging goes to
stderr, so stdout carries nothing but SBE.

### Measuring a run

```sh
scripts/measure.sh <label> <minutes> [collector flags...]   # e.g. scripts/measure.sh full-universe 20
scripts/summarize.py runs/*.log                            # markdown rows, one per log
```

`measure.sh` runs the release binary for that many minutes with the flags
given, keeps its stderr as `runs/<UTC start>-<label>.log` and its stdout (the
SBE stream) as `runs/<start>-<label>.sbe`, sends SIGINT at the end so the
drain path is exercised, and prints the summary row for the log.
`summarize.py` (standard library only) reads any set of run logs and prints
one markdown row each: label, UTC start and end (first and last timestamp in
the log), sockets, books live at the end, frames/s average and peak,
`received_at - sending_ts_ms` p50/p90/p99 over the last 60 s (the
`lat60_*` fields the collector logs every 10 s), `seq` gaps, exchange errors,
resyncs, failovers and peak RSS. Every value is read from the log's metrics
lines; a field a log does not carry prints as `TBD(run)`, never an estimate.
The probe prints its own UTC start and end and stores them in
`probe/summary.json`.

Key flags (`--help` has them all): `--markets-per-socket` (default 4000),
`--redundancy` (sockets per shard, default 2; 1 turns redundancy off),
`--max-shards` (95, so 2x95 stays under the Basic tier connection limit,
200 at the time of writing per the "WebSocket connection limits" section of
Kalshi's rate limits page, docs.kalshi.com/getting_started/rate_limits),
`--subscribe-chunk` (largest request, 400 tickers; adaptive, see below),
`--shard-stagger-ms` (500), `--standby-defer-secs` (600),
`--snapshot-every-secs` (60), `--reconcile-every-secs` (900),
`--silence-timeout-secs` (30), `--silence-window-secs` (20),
`--reconnect-every-secs` (21600; 0 disables), `--flush-every-secs` (0),
`--journal off|active|all` (active), `--chaos-reconnect-secs` (0; test hook
that force-reconnects one socket of shard 0 on a timer to show failover),
`--update-interval-ms` (0; 100 or 1000 asks Kalshi to conflate deltas per
window, which also turns the per-market clock check off because conflated
clocks are not comparable across connections).

---

## Completeness and Time Accuracy

| Threat | Handling |
|---|---|
| Missed message on a subscription | `seq` must be contiguous per `sid`; a gap marks every book on that source `Syncing` and requests `get_snapshot` for all its tickers (Kalshi's documented recovery), or reconnects if that fails |
| Corrupt local book | A negative level resyncs that one book |
| Socket dies silently | No frame of any kind for 30 s triggers reconnect; pings arrive every 10 s so a healthy socket never trips this |
| Subscription goes quiet without a disconnect | Active source silent for 20 s while its standby received frames in the last 2 s = resync + failover (only once both sources have nothing in flight) |
| One market's updates stop arriving on a connection | Its `clock` falls behind the standby's copy for 2 s: `get_snapshot` that ticker; many at once: fail over |
| Exchange-side buffer overflow (error 25) | Never request more snapshots than the pipe has delivered: one request in flight per socket, sized to arrive in about a second, standbys defer to actives, shards staggered |
| Disconnect | Standby takes over immediately with a snapshot of every live book; the reconnecting socket resubscribes everything and resyncs in the background |
| Exchange-side cumulative bugs | Every socket does a planned reconnect every 6 h, staggered |
| Local backpressure | Readers never block; queue depths are logged; completeness beats latency to disk |
| Process crash | Raw-frame journal, flushed every second |
| New / dying markets | Lifecycle channel (opened before discovery) plus periodic REST reconcile |
| Timestamp fidelity | `received_at` is taken in the socket task right after the read, before any parsing or queueing. Residual error is tokio scheduling jitter, typically tens of microseconds |

---

## Measurements

All numbers are from my machine on 2026-11-04, in the UTC windows given per
run below (late US afternoon, European evening), Basic tier key, release
build. The run logs
are in `runs/`, named by UTC start. **The machine's downlink
measured ~245 KB/s** (a 10 MB CDN download could not finish in 40 s). That is
not Kalshi's limit, it is this network's, and it is the binding constraint on
everything below. On a normal link the sync phase is seconds, not minutes.

### Runs at a glance

Generated by `scripts/summarize.py runs/*.log` from the logs in `runs/`. The
narrative of the probe and of each 2026-11-04 run is in
[docs/MEASUREMENTS.md](docs/MEASUREMENTS.md). The two `rebuild-*` rows are runs
of this repository on 40 markets: `c11-smoke` at its first end-to-end commit,
and `final-smoke` at the finished code in its default stdout mode with a
forced reconnect of the active socket every 15 s, which failed over three
times with a snapshot of every live book written first (0 gaps, 0 anomalies
in `inspect --check`).

| label | UTC start | UTC end | sockets | books live at end | frames/s avg | frames/s peak | `received_at - sending_ts_ms` p50/p90/p99 ms, last 60 s | seq gaps | exchange errors | resyncs | failovers | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| smoke-100-markets | 2026-11-04 22:19:00Z | 2026-11-04 22:19:45Z | 6 | 100 | 22 | 43 | TBD(run) | 0 | 0 | 0 | 0 | TBD(run) |
| run1-full-universe-no-flow-control | 2026-11-04 22:25:50Z | 2026-11-04 22:30:50Z | 85 | 36880 | 355 | 661 | TBD(run) | 0 | 0 | 39 | 47 | TBD(run) |
| run2-full-universe-flow-control | 2026-11-04 22:34:38Z | 2026-11-04 22:40:37Z | 91 | 65740 | 590 | 1162 | TBD(run) | 0 | 0 | 1 | 1 | TBD(run) |
| run3-6000-markets-adaptive | 2026-11-04 22:41:09Z | 2026-11-04 22:45:09Z | 4 | 5999 | 943 | 1745 | TBD(run) | 0 | 0 | 0 | 1 | TBD(run) |
| rebuild-c11-smoke-40-markets | 2026-11-06 18:42:01Z | 2026-11-06 18:42:31Z | 2 | 40 | 2 | 5 | TBD(run) | 0 | 0 | 0 | TBD(run) | TBD(run) |
| rebuild-final-smoke-40-markets-failover | 2026-11-06 18:55:28Z | 2026-11-06 18:56:18Z | 4 | 40 | 4 | 9 | 3.4 / 76.7 / 213.7 | 0 | 0 | 0 | 2 | n/a |

Note: smoke-100-markets: no 60 s window in this log; last 10 s interval had n=24, p50/p90/p99 = 16.5 / 91.0 / 106.9 ms

Note: run1-full-universe-no-flow-control: no 60 s window in this log; last 10 s interval had n=293, p50/p90/p99 = 158864.1 / 194244.6 / 194433.5 ms

Note: run2-full-universe-flow-control: no 60 s window in this log; last 10 s interval had n=5344, p50/p90/p99 = 123064.4 / 168310.2 / 222366.4 ms

Note: run3-6000-markets-adaptive: no 60 s window in this log; last 10 s interval had n=6315, p50/p90/p99 = 19121.3 / 21801.3 / 22443.5 ms

### Open measurements

Every `TBD(run)` in this repo, and the command that fills it:

- The probe's own UTC window: `cargo run --release --bin probe`. The probe now
  prints and stores its start and end.
- The last-60 s latency column and peak RSS for the full universe:
  `scripts/measure.sh full-universe 20` on a link with headroom; the logs in
  `runs/` predate the `lat60_*` and `rss_mib` fields.
- The same for a 6,000-market subset: `head -6000 probe/symbols.txt >
  runs/subset-6000.txt` after a probe run, then
  `scripts/measure.sh subset-6000 5 --symbols-file runs/subset-6000.txt`.
- The `update_interval_ms` 100 row in `LATENCY.md`:
  `scripts/measure.sh interval-100 5 --symbols-file runs/subset-6000.txt --update-interval-ms 100`.
- Memory in the sizing table: the peak `rss_mib` of any of the above on Linux.

### Steady-state numbers to size a deployment

| | |
|---|---|
| Open non-MVE markets | ~176k; a REST discovery pass is 177 gzip pages, ~80 s |
| Snapshot | ~600 bytes of JSON in, ~100 to 200 bytes of SBE out |
| Full universe, 22:34Z to 22:40Z window (late US afternoon, European evening) | ~2,000 frames/s, ~1 MB/s inbound (extrapolated from 65k live books at 600 to 840 deltas/s) |
| Minute snapshots | 176k `Snapshot` messages per minute, ~20 to 35 MB/min of SBE |
| Memory | TBD(run): peak `rss_mib` from the metrics line on Linux (`n/a` on macOS) |
| Connections | 1 lifecycle + 45 shards x 2 = 91, against the Basic tier connection limit (200 at the time of writing, per Kalshi's rate limits page) |

---

## Extensions

1. **Redundant websockets**: done (`--redundancy`, default 2). See the shard
   description; the failover snapshot is emitted before any further update.
2. **Subscription monitoring**: done at three levels. Socket: no frame of any
   kind for 30 s reconnects. Source: the active socket silent while its
   standby flows fails over. Symbol: a book whose per-market `clock` lags the
   standby's copy for two consecutive seconds is re-snapshotted (many at once
   fails the whole source). Plus the planned, staggered full reconnect of
   every socket every 6 h. Per-symbol update rates are tracked (an EMA of the
   inter-arrival time of applied updates per symbol) and logged every 10 s as
   a histogram (`rate_0`, `rate_lt1`, `rate_1to10`, `rate_gt10`: symbols with
   no update in 10 min, under 1/min, 1 to 10/min, over 10/min) plus the five
   busiest symbols (`top5`). I deliberately do not use a rate threshold as a
   resubscribe trigger: most of Kalshi's 176k markets are quiet for hours, so
   "rate dropped" is noise, while "the other connection saw a change this one
   did not" is signal, and that is what the `clock` check acts on.
3. **Generic encode/decode**: done. `Exchange` trait, `Event` model, Kalshi
   adapter, Polymarket stub.
4. **Latency**: measured, not optimized. See Measurements for the current
   distribution of `received_at - sending_ts_ms` (exchange queue to our read)
   and `received_at - ts_ms` (exchange change to our read), both logged every
   10 s by the running collector. Ideas worth trying next, in order:
   run in AWS us-east-1 (Kalshi's region) to take the WAN out; compare
   `update_interval_ms` 0 vs 100 (conflation trades latency for throughput);
   many small shards vs few large ones (per-subscription buffering); connect
   several sockets and keep the one with the lowest median delta latency,
   which the per-source latency samples already allow; TCP_NODELAY is set
   but only affects our sends.

---

## Design Principles (decisions worth arguing about)

- Asks in yes terms, so the output is one yes-denominated book per market.
  The alternative (two one-sided books) makes every consumer do the
  conversion.
- Books live in the shard that holds the subscription, not a central store:
  single owner, no locks, in-order processing of one stream, and the minute
  snapshot is a local iteration.
- Each redundant source keeps its own books instead of merging two streams by
  `clock`. It costs memory (books are tiny) and buys a failover that is
  trivially correct: the new source's book is exactly what its own stream
  says, and the snapshot we emit on the switch tells the reader so.
- Blocking is never pushed back to the socket. Memory is cheaper than
  exchange-side data loss.
- Flushing is only on rotation and shutdown by default. Each flush writes a
  64 MiB zero-padded chunk, so periodic flushing is 64 MiB per flush of
  padding. The journal covers crash loss instead; `--flush-every-secs` exists
  for anyone who prefers padding.
- Transient crossed books are left alone. A `yes` delta and its matching `no`
  delta are separate messages; the book between them can cross for a moment.
  Fixing it locally would be inventing data. `inspect --check` evaluates
  crossed books once, on each book's final state at the end of the file, so
  its "0 crossed books" says the books ended uncrossed, not that no cross
  occurred along the way.
- stdout is the default output, as the challenge text says, written through
  the template writer via `/dev/stdout` since it takes a path. Hourly files
  are one flag away (`--out <dir>`) for anyone who wants rotation and the
  restart-safe naming. SIGINT behaves identically in both modes: drain, pad
  the last chunk, flush, exit 0.

---

## Testing and Quality

- `cargo test`: fixed-point parsing (including the boundary cases
  `"0.0800" -> 80_000`, `"300.00" -> 300_000_000`, `"-54.00" -> -54_000_000`,
  7-decimal rejection), book semantics (zero removes and still emits 0,
  negative leaves the book unchanged, snapshot replaces vanished levels,
  ordering), Kalshi decode on the documented frames (yes/no to bid/ask, empty
  sides, control frames), request signing (headers verify against the key),
  and a round trip through `AlignedSbeWriter` and `AlignedSbeReader` across a
  chunk boundary.
- `inspect` reads any output file back through the template reader and
  rebuilds the books: counts, symbols, time range, non-monotonic timestamps,
  updates before any snapshot and negative quantities (both checked on every
  message), and crossed books (best bid >= best ask, checked once per book on
  its final state at the end of the file, not after every message).
- Live run evidence is in Measurements.

---

## Project Status: Not Done

- Polymarket adapter bodies (stub only).
- Per-symbol rate thresholds as a resubscribe trigger: deliberately not used;
  rates are tracked and logged instead (see Extension 2).
- Latency experiments beyond measurement (see Extension 4).
- Backfill after an outage: there is no REST order book replay on Kalshi, so a
  gap longer than both sources' buffers is a gap; it is logged, counted and
  followed by a snapshot.

