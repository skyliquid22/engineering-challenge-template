//! Command line / environment configuration.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};

use crate::exchange::kalshi::{DEFAULT_REST_URL, DEFAULT_WS_URL};
use crate::journal::JournalConfig;
use crate::shard::{JournalMode, ShardConfig};
use crate::socket::SocketConfig;
use crate::universe::UniverseConfig;
use crate::writer::{Sink, WriterConfig};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ExchangeKind {
    Kalshi,
    Polymarket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum JournalArg {
    Off,
    Active,
    All,
}

/// Collects every live Kalshi order book into aligned SBE files.
#[derive(Parser, Debug, Clone)]
#[command(name = "collector", version, about)]
pub struct Config {
    #[arg(long, value_enum, default_value_t = ExchangeKind::Kalshi)]
    pub exchange: ExchangeKind,

    #[arg(long, env = "KALSHI_KEY_ID")]
    pub key_id: String,

    /// PEM private key (Ed25519 or RSA) matching --key-id.
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    pub private_key: PathBuf,

    #[arg(long, env = "KALSHI_WS_URL", default_value = DEFAULT_WS_URL)]
    pub ws_url: String,

    #[arg(long, env = "KALSHI_REST_URL", default_value = DEFAULT_REST_URL)]
    pub rest_url: String,

    /// `-` (the default) writes the SBE stream to stdout; a directory path
    /// writes hourly files `<dir>/<prefix>-<YYYYMMDD>T<HH>Z.sbe` instead.
    /// SIGINT drains and flushes the same way in both modes.
    #[arg(long, default_value = "-")]
    pub out: PathBuf,

    /// File name prefix inside --out.
    #[arg(long, default_value = "kalshi-orderbook")]
    pub prefix: String,

    /// Raw frame journal: off, active (frames that produced the output), all
    /// (every socket). Bounds crash loss to about a second, since the SBE
    /// writer holds a whole 64 MiB chunk in memory.
    #[arg(long, value_enum, default_value_t = JournalArg::Active)]
    pub journal: JournalArg,

    /// Journal directory (default: `journal/` next to the process when
    /// writing to stdout, `<out>/journal` when writing files).
    #[arg(long)]
    pub journal_dir: Option<PathBuf>,

    /// Markets per subscription (= per socket). Kalshi accepted all 176k open
    /// markets on one subscription in the probe, so this is a throughput
    /// knob: 4000 gives ~45 shards, 90 sockets with redundancy 2, well under
    /// the Basic tier connection limit (200 at the time of writing, per the "WebSocket connection
    /// limits" section of https://docs.kalshi.com/getting_started/rate_limits).
    #[arg(long, default_value_t = 4000)]
    pub markets_per_socket: usize,

    /// Upper bound on shards. The Basic tier connection limit is 200 per user
    /// at the time of writing ("WebSocket connection limits",
    /// https://docs.kalshi.com/getting_started/rate_limits).
    #[arg(long, default_value_t = 95)]
    pub max_shards: usize,

    /// Redundant sockets per shard (extension 1). 1 disables redundancy.
    #[arg(long, default_value_t = 2)]
    pub redundancy: usize,

    #[arg(long, default_value_t = 60)]
    pub snapshot_every_secs: u64,

    /// REST re-discovery interval, the safety net under the lifecycle channel.
    /// A full pass costs ~10 s per 1000 markets on Kalshi's side.
    #[arg(long, default_value_t = 900)]
    pub reconcile_every_secs: u64,

    /// A socket with no frames (pings included) for this long is reconnected.
    #[arg(long, default_value_t = 30)]
    pub silence_timeout_secs: u64,

    /// Active source quiet this long while its standby flows = failover.
    #[arg(long, default_value_t = 20)]
    pub silence_window_secs: u64,

    /// Planned full reconnect of every socket (extension 2); 0 disables.
    #[arg(long, default_value_t = 21600)]
    pub reconnect_every_secs: u64,

    /// Flush (pad + write a 64 MiB chunk) this often; 0 = only on rotation/exit.
    #[arg(long, default_value_t = 0)]
    pub flush_every_secs: u64,

    #[arg(long, default_value_t = 10)]
    pub metrics_every_secs: u64,

    /// Collect only these tickers (one per line) instead of discovering.
    #[arg(long)]
    pub symbols_file: Option<PathBuf>,

    /// Largest subscribe/add/get_snapshot request, in tickers. Each socket
    /// keeps about one request in flight and adapts its size to how fast the
    /// previous one arrived (halving above 1 s, doubling below 0.5 s), so the
    /// exchange-side queue stays near a second whatever the link delivers.
    #[arg(long, default_value_t = 400)]
    pub subscribe_chunk: usize,

    #[arg(long, default_value_t = 250)]
    pub pace_ms: u64,

    /// Shard i opens its sockets i * this after spawn, spreading the initial
    /// snapshot bursts of ~90 sockets over the downlink.
    #[arg(long, default_value_t = 500)]
    pub shard_stagger_ms: u64,

    /// A standby waits up to this long for the active source to finish its
    /// initial sync before subscribing itself.
    #[arg(long, default_value_t = 600)]
    pub standby_defer_secs: u64,

    /// Test hook: shard 0 force-reconnects one of its sockets this often
    /// (alternating), to exercise failover. 0 disables.
    #[arg(long, default_value_t = 0)]
    pub chaos_reconnect_secs: u64,

    /// Kalshi `update_interval_ms` on the orderbook subscription: 0 is one
    /// delta per change; 100 or 1000 conflates each window into one delta
    /// per changed level. Conflated clocks are not comparable across
    /// connections, so the per-market stale check is off when this is not 0.
    #[arg(long, default_value_t = 0)]
    pub update_interval_ms: u64,
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(self.update_interval_ms, 0 | 100 | 1000),
            "--update-interval-ms must be 0, 100 or 1000"
        );
        Ok(())
    }
}

impl Config {
    pub fn socket(&self) -> SocketConfig {
        SocketConfig {
            silence_timeout: Duration::from_secs(self.silence_timeout_secs.max(5)),
            reconnect_every: (self.reconnect_every_secs > 0)
                .then(|| Duration::from_secs(self.reconnect_every_secs)),
            initial_offset: Duration::ZERO,
            initial_connect_delay: Duration::ZERO,
            max_backoff: Duration::from_secs(30),
        }
    }

    pub fn shard(&self) -> ShardConfig {
        ShardConfig {
            redundancy: self.redundancy.max(1),
            socket: self.socket(),
            snapshot_every: Duration::from_secs(self.snapshot_every_secs.max(1)),
            silence_window: Duration::from_secs(self.silence_window_secs.max(2)),
            journal: match self.journal {
                JournalArg::Off => JournalMode::Off,
                JournalArg::Active => JournalMode::Active,
                JournalArg::All => JournalMode::All,
            },
            subscribe_chunk: self.subscribe_chunk,
            pace: Duration::from_millis(self.pace_ms),
            stagger: Duration::from_millis(self.shard_stagger_ms),
            standby_defer: Duration::from_secs(self.standby_defer_secs),
            chaos_reconnect: (self.chaos_reconnect_secs > 0)
                .then(|| Duration::from_secs(self.chaos_reconnect_secs)),
            conflated: self.update_interval_ms != 0,
        }
    }

    pub fn universe(&self) -> anyhow::Result<UniverseConfig> {
        let fixed_symbols = match &self.symbols_file {
            Some(p) => Some(
                std::fs::read_to_string(p)?
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty() && !l.starts_with('#'))
                    .map(str::to_owned)
                    .collect(),
            ),
            None => None,
        };
        Ok(UniverseConfig {
            markets_per_socket: self.markets_per_socket,
            max_shards: self.max_shards,
            reconcile_every: Duration::from_secs(self.reconcile_every_secs.max(30)),
            fixed_symbols,
            socket: self.socket(),
        })
    }

    pub fn journal(&self) -> Option<JournalConfig> {
        if self.journal == JournalArg::Off {
            return None;
        }
        let dir = self.journal_dir.clone().unwrap_or_else(|| {
            if self.out.as_os_str() == "-" {
                PathBuf::from("journal")
            } else {
                self.out.join("journal")
            }
        });
        Some(JournalConfig {
            dir,
            prefix: self.prefix.clone(),
        })
    }

    pub fn writer(&self) -> WriterConfig {
        let sink = if self.out.as_os_str() == "-" {
            Sink::File(PathBuf::from("/dev/stdout"))
        } else {
            Sink::Dir {
                dir: self.out.clone(),
                prefix: self.prefix.clone(),
            }
        };
        WriterConfig {
            sink,
            flush_every: (self.flush_every_secs > 0).then(|| Duration::from_secs(self.flush_every_secs)),
        }
    }
}
