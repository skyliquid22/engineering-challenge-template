//! Wiring only: config, threads, tasks, signals, orderly shutdown.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use clap::Parser as _;
use engineering_challenge::auth::Auth;
use engineering_challenge::config::Config;
use engineering_challenge::exchange::kalshi::Kalshi;
use engineering_challenge::metrics::Metrics;
use engineering_challenge::shard::spawn_shard;
use engineering_challenge::universe::run_universe;
use engineering_challenge::writer::spawn_writer;
use tokio_util::sync::CancellationToken;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cfg = Config::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("collector")
        .build()?;
    runtime.block_on(run(cfg))
}

async fn run(cfg: Config) -> anyhow::Result<()> {
    let auth = Arc::new(Auth::load(&cfg.key_id, &cfg.private_key)?);
    tracing::info!(key = auth.key_id(), algorithm = auth.algorithm(), "authenticating");
    let ex = Arc::new(Kalshi::new(auth, &cfg.ws_url, &cfg.rest_url));
    let metrics = Metrics::new();
    let cancel = CancellationToken::new();

    // Signals: SIGINT (ctrl-c) and SIGTERM both start an orderly shutdown.
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT: shutting down"),
                _ = term.recv() => tracing::info!("SIGTERM: shutting down"),
            }
            cancel.cancel();
        });
    }

    let (out_tx, out_rx) = mpsc::channel();
    let writer = spawn_writer(cfg.writer(), out_rx, metrics.clone());

    tokio::spawn(
        metrics
            .clone()
            .run_logger(Duration::from_secs(cfg.metrics_every_secs.max(1)), cancel.clone()),
    );

    let shard_cfg = cfg.shard();
    let spawner = {
        let ex = ex.clone();
        let out_tx = out_tx.clone();
        let metrics = metrics.clone();
        let cancel = cancel.clone();
        move |id: usize| {
            spawn_shard(id, ex.clone(), shard_cfg.clone(), out_tx.clone(), metrics.clone(), cancel.child_token())
        }
    };
    let universe = tokio::spawn(run_universe(ex.clone(), cfg.universe()?, spawner, metrics.clone(), cancel.clone()));

    // Runs until a signal cancels, or the universe task fails.
    let shard_tasks = match universe.await {
        Ok(Ok(tasks)) => tasks,
        Ok(Err(e)) => {
            tracing::error!(error = %e, "universe task failed; shutting down");
            cancel.cancel();
            Vec::new()
        }
        Err(e) => {
            tracing::error!(error = %e, "universe task panicked; shutting down");
            cancel.cancel();
            Vec::new()
        }
    };
    cancel.cancel();

    // Shards close their sockets and drop their writer senders.
    let join_all = async {
        for t in shard_tasks {
            let _ = t.await;
        }
    };
    if tokio::time::timeout(Duration::from_secs(10), join_all).await.is_err() {
        tracing::warn!("shards did not stop within 10s");
    }
    // Our own copy must go so the writer sees the channel close.
    drop(out_tx);

    tokio::task::spawn_blocking(move || writer.join())
        .await?
        .map_err(|_| anyhow::anyhow!("writer thread panicked"))??;
    tracing::info!(
        written = metrics.msgs_written.get(),
        bytes = metrics.bytes_written.get(),
        reconnects = metrics.reconnects.get(),
        gaps = metrics.gaps.get(),
        "clean shutdown"
    );
    Ok(())
}
