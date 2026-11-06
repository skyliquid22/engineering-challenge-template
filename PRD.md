# PRD: Kalshi order book collector

## Goal

Capture complete, time-accurate order book data for every live Kalshi market
in the Voluntas challenge's SBE format, robustly enough to trust in a backtest.

## Requirements

Core
1. Discover every open market that is not a multivariate event (MVE) and
   subscribe to all of them; subscribe to new markets and unsubscribe from
   markets that stop trading.
2. Maintain an order book per market and write every price-level change plus
   a full snapshot of every book once a minute, using the template's
   `AlignedSbeWriter` and message types unchanged.
3. Spread markets over several websockets; one socket cannot carry them all.
4. On SIGINT, close the websockets and the file being written, cleanly.
5. Output goes to stdout by default; hourly files are an option.

Extension 1, redundant websockets
6. Survive disconnects, silent subscriptions, rate limiting and process
   faults with redundant sockets; when the active socket changes, write a
   fresh snapshot before any further update.

Extension 2, subscription monitoring
7. Track update rates per symbol; detect subscriptions that went quiet;
   periodically reconnect whole sockets to clear exchange-side state.

Extension 3, generic core
8. Keep everything but websocket encoding and decoding exchange-agnostic.

Extension 4, latency
9. Measure receive latency against the exchange's timestamps and document
   what was learned and what to try next.

## Acceptance criteria

- `cargo test` and `cargo clippy --all-targets` clean at every commit.
- A live run writes a file that `inspect --check` rebuilds with zero updates
  before a snapshot, zero negative quantities and zero crossed books at the
  end, and SIGINT produces a clean drain and flush.
- A forced reconnect of the active socket fails over with a snapshot of every
  live book written first.
- Every number in the documentation comes from a log or artifact in the repo.

## Constraints discovered during the probe

- Kalshi has roughly 176k open non-MVE markets and no per-subscription market
  limit; one subscription for all of them overflows the exchange's buffer
  (error 25) and drains at about 2,000 frames/s per connection.
- A REST discovery page is 2.2 MB uncompressed; gzip brings it from about
  13 s to 0.6 s on the test link.
- The test machine's downlink is about 245 KB/s, below what the full universe
  produces in a busy window; the collector must stay correct and say so
  rather than pretend to keep up.

## Non-goals

- A working Polymarket adapter (shape only).
- Trading, risk, or execution of any kind.
- Latency optimization beyond measurement in this version.
