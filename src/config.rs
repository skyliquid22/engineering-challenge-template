//! Command line / environment configuration.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::exchange::kalshi::{DEFAULT_REST_URL, DEFAULT_WS_URL};
use crate::shard::ShardConfig;
use crate::socket::SocketConfig;
use crate::universe::UniverseConfig;
use crate::writer::{Sink, WriterConfig};

/// Collects every live Kalshi order book into aligned SBE files.
#[derive(Parser, Debug, Clone)]
#[command(name = "collector", version, about)]
pub struct Config {
    #[arg(long, env = "KALSHI_KEY_ID")]
    pub key_id: String,

    /// PEM private key (Ed25519 or RSA) matching --key-id.
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    pub private_key: PathBuf,

    #[arg(long, env = "KALSHI_WS_URL", default_value = DEFAULT_WS_URL)]
    pub ws_url: String,

    #[arg(long, env = "KALSHI_REST_URL", default_value = DEFAULT_REST_URL)]
    pub rest_url: String,

    /// Output directory for hourly SBE files, or `-` for stdout.
    #[arg(long, default_value = "out")]
    pub out: PathBuf,

    /// File name prefix inside --out.
    #[arg(long, default_value = "kalshi-orderbook")]
    pub prefix: String,

    /// Markets per subscription (= per socket). Kalshi accepted all 176k open
    /// markets on one subscription in the probe, so this is a throughput
    /// knob: 4000 gives ~45 shards, well under the Basic tier connection
    /// limit (200 at the time of writing, per the "WebSocket connection
    /// limits" section of https://docs.kalshi.com/getting_started/rate_limits).
    #[arg(long, default_value_t = 4000)]
    pub markets_per_socket: usize,

    /// Upper bound on shards. The Basic tier connection limit is 200 per user
    /// at the time of writing ("WebSocket connection limits",
    /// https://docs.kalshi.com/getting_started/rate_limits).
    #[arg(long, default_value_t = 95)]
    pub max_shards: usize,

    #[arg(long, default_value_t = 60)]
    pub snapshot_every_secs: u64,

    /// REST re-discovery interval, the safety net under the lifecycle channel.
    /// A full pass costs ~10 s per 1000 markets on Kalshi's side.
    #[arg(long, default_value_t = 900)]
    pub reconcile_every_secs: u64,

    /// A socket with no frames (pings included) for this long is reconnected.
    #[arg(long, default_value_t = 30)]
    pub silence_timeout_secs: u64,

    /// Flush (pad + write a 64 MiB chunk) this often; 0 = only on rotation/exit.
    #[arg(long, default_value_t = 0)]
    pub flush_every_secs: u64,

    #[arg(long, default_value_t = 10)]
    pub metrics_every_secs: u64,

    /// Collect only these tickers (one per line) instead of discovering.
    #[arg(long)]
    pub symbols_file: Option<PathBuf>,
}

impl Config {
    pub fn socket(&self) -> SocketConfig {
        SocketConfig {
            silence_timeout: Duration::from_secs(self.silence_timeout_secs.max(5)),
            max_backoff: Duration::from_secs(30),
        }
    }

    pub fn shard(&self) -> ShardConfig {
        ShardConfig {
            socket: self.socket(),
            snapshot_every: Duration::from_secs(self.snapshot_every_secs.max(1)),
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
