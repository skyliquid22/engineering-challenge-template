# Measurements

The probe and the three live runs of 2026-11-04, moved here unchanged from
README.md, which keeps the summary table and the sizing numbers. Logs are in
`runs/`, probe artifacts in `probe/`.

### Probe (`probe/summary.json`, `probe/probe.log`)

Window: TBD(run) for the probe as a whole; `probe/probe.log` carries no
wall-clock timestamps. The 60 s measurement phase started at
2026-11-04T22:20:37Z (the `sending_ts_ms` of its first captured frame,
`probe/raw_snapshot.json`) and lasted 60.0 s (`measure_secs` in
`probe/summary.json`).

| | |
|---|---|
| Open non-MVE markets | 176,165 (177 REST pages; 77 s with gzip, ~13 s per 2.2 MB page without) |
| Per-subscription market limit | none hit: all 176,165 tickers accepted on one subscription |
| What happened next | error 25 (buffer overflow) after 28,067 snapshots; one socket drained 482 frames/s avg, 1,982 peak, 242 KiB/s; latency 16 s to 38 s |
| Deltas, 60 s, 28k subscribed markets | 852 deltas on 126 markets. Most Kalshi markets are quiet |
| `seq` gaps | 0 |

One socket cannot carry the universe, as the challenge says; the probe shows
exactly how it fails.

### Run 1: full universe, before flow control (45 shards x 2 sockets)

Window: 2026-11-04T22:25:50Z to 22:30:50Z
(`runs/20261104T222550Z-run1-full-universe-no-flow-control.log`).

Every socket requested its 4,000 snapshots within 2.5 s of connecting; the
link delivered ~500 frames/s in total, so queues built on Kalshi's side.
`received_at - sending_ts_ms` climbed from 4.8 s to 190 s over the run,
Kalshi reset 34 connections, and the silence heuristic misfired 51 times
under saturation. The output stayed correct: 154,361 messages (149,293
snapshots, 5,068 updates) for 48,574 books; `inspect --check` found 0
updates before a snapshot, 0 negative quantities, 0 crossed books; shutdown
drained and flushed cleanly. This run produced the flow control described
above.

### Run 2: full universe, with flow control

Window: 2026-11-04T22:34:38Z to 22:40:37Z
(`runs/20261104T223438Z-run2-full-universe-flow-control.log`).

Fixed 200-ticker requests, one in flight per socket, standbys deferred.
Kalshi's buffer no longer overflowed (0 exchange errors, 0 `seq` gaps, 1
resync, 1 failover, 23 stale books re-snapshotted) and the first minutes were
far better than run 1 (1.6 s at 40 sockets, 17 s at 90 sockets versus 28 s).
But latency still climbed, to p50 120 s / p99 220 s after 4.5 minutes, and
the metrics say why: with 65,000 books live the **deltas alone** ran 600 to
840 frames/s at 300 to 460 KB/s, more than this link carries. Pacing
snapshots cannot help when the unrequestable part of the stream exceeds the
pipe. Extrapolated to all 176k books, Kalshi's non-MVE universe in this window
(22:34Z to 22:40Z, late US afternoon, European evening) is roughly 2,000
frames/s, call it 1 MB/s with headroom; any cloud instance has that, this
laptop link does not. The output stayed exact:
372,236 messages (242,556 snapshots, 129,680 updates) for 71,201 books, 0
updates before a snapshot, 0 negative quantities, 0 crossed books, clean
drain and flush. Two changes came out of this run: the request size now
adapts to delivery time instead of being fixed, and the collector warns
loudly when inbound latency keeps rising (the link, not the exchange, is the
bottleneck and nothing but bandwidth fixes it).

### Run 3: 6,000 markets, steady state and failover

Window: 2026-11-04T22:41:09Z to 22:45:09Z
(`runs/20261104T224109Z-run3-6000-markets-adaptive.log`).

6,000 markets, 2 shards x 2 sockets, adaptive requests, a forced reconnect
of one shard-0 socket every 60 s.

- **Sync**: the active sources had all 6,000 books live after ~85 s while
  delta latency stayed at 1 s to 2.5 s (the adaptive requests held the
  exchange-side queue near a second; the same phase in run 2 with fixed
  200-ticker requests ran 17 s to 27 s). Standbys then synced; at t+90 s both
  sources of both shards were healthy.
- **Steady state** at ~600 frames/s: `received_at - sending_ts_ms` p50 190 to
  450 ms, p90 0.85 to 1.1 s, p99 1.2 to 1.5 s. That is the floor on this link.
- **Saturation again**: two live tennis and soccer events in the subset then
  pushed it to 1,000 to 1,700 deltas/s (330 to 590 KB/s) and latency rose to
  5 s to 19 s, with 223 books flagged as lagging the standby (the lagging
  connection is real; the cause is the pipe).
- **Failover**: the third forced reconnect hit the active socket while its
  standby was healthy. The shard switched within the second and wrote a
  snapshot of every live book before any further update (the metrics
  interval shows the burst of 100 snapshots/s). 3 reconnects, 1 failover,
  0 `seq` gaps, 0 exchange errors.
- **Output**: 154,063 messages (34,548 snapshots including three minute
  rounds of 6,000, 119,515 updates), 6,000 books; `inspect --check`: 0 updates
  before a snapshot, 0 negative quantities, 0 crossed books, 69 backward
  `received_at` steps across 154k messages from two shards. Clean drain and
  flush on SIGINT.
