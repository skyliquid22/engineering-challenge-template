# Latency notes (extension 4)

Correctness and completeness come first; this file records what was measured
about how far behind the exchange this collector runs, and what to try next.

## What is measured

Two distributions, logged every 10 s by the running collector
(`lat_send_ms`, `lat_exch_ms`) and sampled only from the active source:

- `received_at - sending_ts_ms`: from Kalshi queueing the frame to our socket
  task reading it. This is network plus any exchange-side buffering of the
  connection.
- `received_at - ts_ms` (deltas only): from the order book change at the
  exchange to our read. This additionally includes Kalshi's own publish delay.

`received_at` is wall clock, taken in the socket task immediately after the
read, before parsing or queueing. Both Kalshi timestamps have millisecond
resolution, so sub-millisecond differences are noise. The machine's clock
offset (NTP) adds a constant to every number here; the shape of the
distribution and changes under different configurations are what matter.

## Results

Measured on 2026-11-04 from my machine, whose downlink is ~245 KB/s (CDN
test). UTC windows come from the run logs in `runs/`. Milliseconds.

| Setup | UTC window | `received_at - sending_ts_ms` | Notes |
|---|---|---|---|
| Probe: 176k markets on one socket | TBD(run) for the whole probe; measurement phase from 22:20:37Z for 60.0 s (`probe/raw_snapshot.json`, `probe/summary.json`) | p50 16,097 / p90 33,924 / p99 37,386 / max 38,265 | queue behind the snapshot burst; error 25 within the minute |
| Run 1: 45 shards x 2 sockets, no flow control | 22:25:50Z to 22:30:50Z | 4,817 at t+10 s rising to p50 158,864 / p99 194,434 at t+200 s | the link, not Kalshi, is saturated; 34 server resets |
| Run 2: same, flow-controlled (fixed 200-ticker requests) | 22:34:38Z to 22:40:37Z | 1,634 at 40 sockets, 17,229 at 90 sockets, then p50 123,064 / p99 222,366 at t+270 s | deltas alone (600 to 840/s, 300 to 460 KB/s) exceed the 245 KB/s link once ~65k books are live |
| Interval 100 ms (`--update-interval-ms 100`), 6,000-market subset | TBD(run) | TBD(run) | TBD(run) |
| Run 3: 6,000 markets, adaptive requests | 22:41:09Z to 22:45:09Z | sync at p50 1,047 to 2,358 / p99 2,485 to 4,215; steady p50 191 to 450 / p90 845 to 1,073 / p99 1,256 to 1,494; 5,000 to 19,000 once two live events pushed the subset to 1,000 to 1,700 deltas/s | the steady-state rows are the floor on this link; the last is the link saturating again |

`received_at - ts_ms` (change at the exchange to our read) tracked
`received_at - sending_ts_ms` within ~10 ms in every run, i.e. Kalshi's
own publish delay is small compared with transport on this link.

## Observations

0. **Bandwidth first.** Kalshi's full non-MVE universe (176k markets) runs
   at roughly 2,000 frames/s, about 1 MB/s, in the 22:34Z to 22:40Z window
   (late US afternoon, European evening). Both figures are extrapolated from
   run 2, where 65k live books produced 600 to 840 deltas/s; they are not a
   full-universe measurement. Run 2 showed deltas alone exceeding this
   machine's 245 KB/s link once 65k books were live; from there latency grows without bound no matter what the
   client does, and Kalshi eventually resets the connection it cannot drain.
   The collector now warns when inbound latency keeps rising for 30 s. The
   only fix is a link with headroom (any cloud instance).
1. **Per-connection throughput is the next latency factor on Kalshi.**
   One subscription carrying all 176k markets drained at ~2000 frames/s
   (~240 KiB/s) and queued up 16 s to 38 s of latency within a minute before
   the exchange gave up with error 25 (buffer overflow). Spreading the same
   markets over ~90 sockets removes that queue entirely. This is the same
   mechanism as the challenge's "unintentional buffering to one connection":
   the fix is more, smaller subscriptions, and the per-source latency samples
   are how you notice it happening.
2. **Snapshot bursts are the other queue.** A fresh subscription receives one
   snapshot per market before any delta. Asking for 4000 at once is a 4000
   frame burst at ~2000 frames/s; asking for 176k is a dead subscription.
   Subscriptions are therefore paced (400 tickers every 250 ms per socket,
   shards staggered 500 ms apart) so the exchange-side queue never grows past
   a chunk.
3. **The redundant socket is also a latency monitor.** Kalshi's `clock` is per
   market and identical on every connection. When the standby's copy of a
   book is ahead of the active's for two consecutive seconds, the active
   connection is late on that market; a few such books get re-snapshotted,
   many at once means the connection is lagging and the collector fails over
   to the standby. That turns "buffered connection" from an invisible latency
   problem into a logged, counted event (`stale`, `failovers`).

## Worth trying next, in order

1. Run from a cloud region close to Kalshi's servers to take the WAN out of
   both distributions. A cloud box with a link that has headroom should
   bring the steady-state p50 down to tens of milliseconds; the numbers here
   are from a ~245 KB/s link.
2. Keep the faster of the two sockets per shard active. Both sources already
   receive everything; sampling `received_at - sending_ts_ms` per source and
   preferring the lower median (with hysteresis) is a small change to
   `maybe_failover`.
3. `update_interval_ms` 0 vs 100 (`--update-interval-ms 100`): conflation
   lowers frame count at the cost of up to 100 ms added latency; only
   interesting if a shard approaches the ~2000 frames/s drain rate. The row
   for it in the table above is still TBD(run).
4. More, smaller shards. The Basic tier connection limit (200 at the time of
   writing, per the "WebSocket connection limits" section of Kalshi's rate
   limits page) with redundancy 2 caps shards near 95. Measure whether 2000-market shards
   beat 4000-market shards on p99 in a busy window (late US afternoon,
   European evening).
5. Connect several candidate sockets at start and keep the lowest-latency
   ones ("spam connections until you find a fast one"), and reconnect right
   after Kalshi's scheduled maintenance windows. Both need a longer run than
   this submission had to show an effect.
6. TCP_NODELAY is set on our side but only affects our (rare) sends. Nothing
   on the receive path is tunable from the client beyond socket buffer sizes.
