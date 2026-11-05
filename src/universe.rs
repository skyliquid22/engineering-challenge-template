//! Which symbols exist, and which shard holds each one.
//!
//! Startup order matters: the lifecycle socket opens first and its events
//! are buffered, then REST discovery runs, then the buffer is applied. No
//! market can activate unseen in between. A periodic REST reconcile catches
//! anything the lifecycle channel missed across its own reconnects.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::exchange::kalshi::Kalshi;
use crate::exchange::{Event, LifecycleKind, Symbol};
use crate::metrics::Metrics;
use crate::shard::{ShardCmd, ShardHandle};
use crate::socket::{SocketCmd, SocketConfig, SocketMsg, run_socket};

#[derive(Clone, Debug)]
pub struct UniverseConfig {
    pub markets_per_socket: usize,
    pub max_shards: usize,
    pub reconcile_every: Duration,
    /// Skip discovery and lifecycle; collect exactly these.
    pub fixed_symbols: Option<Vec<Symbol>>,
    pub socket: SocketConfig,
}

/// Least-loaded placement with a per-shard cap; spawns shards on demand.
struct Allocator<F: FnMut(usize) -> (ShardHandle, tokio::task::JoinHandle<()>)> {
    cap: usize,
    max_shards: usize,
    spawn: F,
    shards: Vec<ShardHandle>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    counts: Vec<usize>,
    assigned: HashMap<Symbol, usize>,
}

impl<F: FnMut(usize) -> (ShardHandle, tokio::task::JoinHandle<()>)> Allocator<F> {
    fn pick(&mut self) -> usize {
        let best = (0..self.shards.len()).min_by_key(|&i| self.counts[i]);
        match best {
            Some(i) if self.counts[i] < self.cap || self.shards.len() >= self.max_shards => i,
            _ => {
                let id = self.shards.len();
                let (handle, task) = (self.spawn)(id);
                self.shards.push(handle);
                self.tasks.push(task);
                self.counts.push(0);
                tracing::info!(shard = id, "spawned shard");
                id
            }
        }
    }

    fn add(&mut self, symbols: Vec<Symbol>) -> usize {
        let mut batches: HashMap<usize, Vec<Symbol>> = HashMap::new();
        let mut n = 0;
        for sym in symbols {
            if self.assigned.contains_key(&sym) {
                continue;
            }
            let shard = self.pick();
            self.counts[shard] += 1;
            self.assigned.insert(sym.clone(), shard);
            batches.entry(shard).or_default().push(sym);
            n += 1;
        }
        for (shard, syms) in batches {
            let _ = self.shards[shard].cmds.send(ShardCmd::Add(syms));
        }
        n
    }

    fn remove(&mut self, symbols: Vec<Symbol>) -> usize {
        let mut batches: HashMap<usize, Vec<Symbol>> = HashMap::new();
        let mut n = 0;
        for sym in symbols {
            if let Some(shard) = self.assigned.remove(&sym) {
                self.counts[shard] -= 1;
                batches.entry(shard).or_default().push(sym);
                n += 1;
            }
        }
        for (shard, syms) in batches {
            let _ = self.shards[shard].cmds.send(ShardCmd::Remove(syms));
        }
        n
    }
}

async fn discover_with_retry(ex: &Kalshi, cancel: &CancellationToken, attempts: usize) -> anyhow::Result<Vec<Symbol>> {
    let mut delay = Duration::from_secs(1);
    let mut last = None;
    for _ in 0..attempts {
        match ex.discover().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                tracing::warn!(error = %e, "discovery failed");
                last = Some(e);
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => anyhow::bail!("cancelled"),
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(Duration::from_secs(30));
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("discovery failed")))
}

/// Runs until cancelled. Returns the shard tasks so the caller can join them.
pub async fn run_universe(
    ex: Arc<Kalshi>,
    cfg: UniverseConfig,
    spawn_shard: impl FnMut(usize) -> (ShardHandle, tokio::task::JoinHandle<()>),
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    let mut alloc = Allocator {
        cap: cfg.markets_per_socket.max(1),
        max_shards: cfg.max_shards.max(1),
        spawn: spawn_shard,
        shards: Vec::new(),
        tasks: Vec::new(),
        counts: Vec::new(),
        assigned: HashMap::new(),
    };

    if let Some(symbols) = cfg.fixed_symbols.clone() {
        let n = alloc.add(symbols);
        tracing::info!(symbols = n, shards = alloc.shards.len(), "fixed universe");
        cancel.cancelled().await;
        return Ok(alloc.tasks);
    }

    // 1. Lifecycle socket first; buffer until discovery is done.
    let (lc_cmd_tx, lc_cmd_rx) = mpsc::unbounded_channel();
    let (lc_tx, mut lc_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_socket(
        ex.clone(),
        0,
        "lifecycle".to_owned(),
        lc_cmd_rx,
        lc_tx,
        cancel.child_token(),
        cfg.socket.clone(),
        metrics.clone(),
    ));
    let mut buffered: Vec<(Symbol, LifecycleKind)> = Vec::new();
    let mut lc_id = 1u64;

    // 2. Discovery, while lifecycle frames accumulate.
    let discovery = discover_with_retry(ex.as_ref(), &cancel, 6);
    tokio::pin!(discovery);
    let symbols = loop {
        tokio::select! {
            r = &mut discovery => break r?,
            Some(msg) = lc_rx.recv() => {
                handle_lifecycle(ex.as_ref(), &lc_cmd_tx, &mut lc_id, msg, &mut |sym, kind| buffered.push((sym, kind)));
            }
        }
    };
    let n = alloc.add(symbols.clone());
    let mut known: HashSet<Symbol> = symbols.into_iter().collect();
    tracing::info!(symbols = n, shards = alloc.shards.len(), cap = alloc.cap, "initial universe");

    // 3. Apply what the lifecycle channel said meanwhile.
    for (sym, kind) in buffered.drain(..) {
        apply_lifecycle(&mut alloc, &mut known, sym, kind);
    }

    let mut reconcile = tokio::time::interval(cfg.reconcile_every);
    reconcile.tick().await; // first tick is immediate; discovery just ran
    // Reconcile discovery runs as its own task so lifecycle events keep
    // flowing during the (slow) REST pass.
    let (disc_tx, mut disc_rx) = mpsc::channel::<anyhow::Result<Vec<Symbol>>>(1);
    let mut discovering = false;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            msg = lc_rx.recv() => match msg {
                Some(msg) => {
                    handle_lifecycle(ex.as_ref(), &lc_cmd_tx, &mut lc_id, msg, &mut |sym, kind| {
                        apply_lifecycle(&mut alloc, &mut known, sym, kind);
                    });
                }
                None => break,
            },
            _ = reconcile.tick() => {
                if !discovering {
                    discovering = true;
                    let ex = ex.clone();
                    let tx = disc_tx.clone();
                    tokio::spawn(async move {
                        let _ = tx.send(ex.discover().await).await;
                    });
                }
            }
            Some(result) = disc_rx.recv() => {
                discovering = false;
                match result {
                    Ok(fresh) => {
                        let fresh: HashSet<Symbol> = fresh.into_iter().collect();
                        let added: Vec<Symbol> = fresh.difference(&known).cloned().collect();
                        let removed: Vec<Symbol> = known.difference(&fresh).cloned().collect();
                        let na = alloc.add(added.clone());
                        let nr = alloc.remove(removed.clone());
                        known.extend(added);
                        for r in &removed { known.remove(r); }
                        tracing::info!(added = na, removed = nr, total = known.len(), shards = alloc.shards.len(), "reconciled with REST");
                    }
                    Err(e) => tracing::warn!(error = %e, "reconcile discovery failed"),
                }
            }
        }
    }
    Ok(alloc.tasks)
}

fn handle_lifecycle(
    ex: &Kalshi,
    cmds: &mpsc::UnboundedSender<SocketCmd>,
    next_id: &mut u64,
    msg: SocketMsg,
    on_event: &mut dyn FnMut(Symbol, LifecycleKind),
) {
    match msg {
        SocketMsg::Connected { .. } => {
            let id = *next_id;
            *next_id += 1;
            let _ = cmds.send(SocketCmd::Send(ex.subscribe_lifecycle(id)));
        }
        SocketMsg::Disconnected { reason, .. } => {
            tracing::warn!(%reason, "lifecycle socket dropped; REST reconcile covers the gap");
        }
        SocketMsg::Frame { text, .. } => match ex.decode(&text) {
            Ok(d) => match d.event {
                Event::Lifecycle { symbol, kind } if !symbol.is_empty() => on_event(symbol, kind),
                Event::Error { code, msg, .. } => tracing::error!(code, %msg, "lifecycle error"),
                _ => {}
            },
            Err(e) => tracing::warn!(error = %e, "undecodable lifecycle frame"),
        },
    }
}

fn apply_lifecycle<F: FnMut(usize) -> (ShardHandle, tokio::task::JoinHandle<()>)>(
    alloc: &mut Allocator<F>,
    known: &mut HashSet<Symbol>,
    sym: Symbol,
    kind: LifecycleKind,
) {
    match kind {
        LifecycleKind::Activated => {
            if known.insert(sym.clone()) {
                tracing::info!(%sym, "lifecycle: activated, subscribing");
                alloc.add(vec![sym]);
            }
        }
        LifecycleKind::Deactivated => {
            if known.remove(&sym) {
                tracing::info!(%sym, "lifecycle: stopped trading, unsubscribing");
                alloc.remove(vec![sym]);
            }
        }
        LifecycleKind::Other => {}
    }
}
