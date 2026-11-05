//! A shard: one socket subscribed to a slice of the universe, one engine that
//! owns the books for those markets and turns the stream into writer messages.
//!
//! Books live here, not in a central store: a single owner, no locks, in-order
//! processing of one socket's stream, and the minute snapshot is a local
//! iteration.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::book::{ApplyError, Book, level_update};
use crate::exchange::kalshi::Kalshi;
use crate::exchange::{Decoded, Event, Symbol};
use crate::metrics::Metrics;
use crate::socket::{SocketCmd, SocketConfig, SocketMsg, run_socket};
use crate::writer::{Out, OutSender};

#[derive(Debug)]
pub enum ShardCmd {
    Add(Vec<Symbol>),
    Remove(Vec<Symbol>),
}

#[derive(Clone, Debug)]
pub struct ShardConfig {
    pub socket: SocketConfig,
    pub snapshot_every: Duration,
}

pub struct ShardHandle {
    pub id: usize,
    pub cmds: mpsc::UnboundedSender<ShardCmd>,
}

#[derive(Debug, Clone, Copy)]
enum Pending {
    Subscribe,
    Add,
    Delete,
    GetSnapshot,
}

pub struct Engine {
    id: usize,
    ex: Arc<Kalshi>,
    cfg: ShardConfig,
    socket: mpsc::UnboundedSender<SocketCmd>,
    connected: bool,
    sid: Option<u64>,
    next_seq: Option<u64>,
    books: HashMap<Symbol, Book>,
    /// Symbols the server has been told about on the current connection.
    subscribed: HashSet<Symbol>,
    symbols: HashSet<Symbol>,
    syncing: usize,
    pending: HashMap<u64, Pending>,
    out: OutSender,
    metrics: Arc<Metrics>,
    next_id: u64,
    reported_live: i64,
    reported_syncing: i64,
}

pub fn spawn_shard(
    id: usize,
    ex: Arc<Kalshi>,
    cfg: ShardConfig,
    out: OutSender,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) -> (ShardHandle, tokio::task::JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (sock_tx, sock_rx) = mpsc::unbounded_channel();
    let (socket_cmd_tx, socket_cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(run_socket(
        ex.clone(),
        0,
        format!("shard{id}"),
        socket_cmd_rx,
        sock_tx,
        cancel.child_token(),
        cfg.socket.clone(),
        metrics.clone(),
    ));
    let engine = Engine {
        id,
        ex,
        cfg,
        socket: socket_cmd_tx,
        connected: false,
        sid: None,
        next_seq: None,
        books: HashMap::new(),
        subscribed: HashSet::new(),
        symbols: HashSet::new(),
        syncing: 0,
        pending: HashMap::new(),
        out,
        metrics,
        next_id: 1,
        reported_live: 0,
        reported_syncing: 0,
    };
    let task = tokio::spawn(engine.run(cmd_rx, sock_rx, cancel));
    (ShardHandle { id, cmds: cmd_tx }, task)
}

/// Align the periodic snapshot to the wall clock so every shard emits its
/// books at the same instant.
fn next_boundary(every: Duration) -> tokio::time::Instant {
    let period_ms = every.as_millis().max(1000) as i64;
    let into = Utc::now().timestamp_millis().rem_euclid(period_ms);
    tokio::time::Instant::now() + Duration::from_millis((period_ms - into) as u64)
}

impl Engine {
    async fn run(
        mut self,
        mut cmds: mpsc::UnboundedReceiver<ShardCmd>,
        mut sock: mpsc::UnboundedReceiver<SocketMsg>,
        cancel: CancellationToken,
    ) {
        let mut snap_tick =
            tokio::time::interval_at(next_boundary(self.cfg.snapshot_every), self.cfg.snapshot_every);
        snap_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut gauge_tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                cmd = cmds.recv() => match cmd {
                    Some(cmd) => self.on_cmd(cmd),
                    None => break,
                },
                msg = sock.recv() => match msg {
                    Some(msg) => self.on_socket(msg),
                    None => break,
                },
                _ = snap_tick.tick() => self.periodic_snapshots(),
                _ = gauge_tick.tick() => self.update_gauges(),
            }
        }
        tracing::info!(shard = self.id, "engine stopped");
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn send(&mut self, kind: Pending, text: String) {
        let id = self.next_id();
        self.pending.insert(id, kind);
        let _ = self.socket.send(SocketCmd::Send(text));
    }

    fn command_id(&self) -> u64 {
        self.next_id
    }

    fn emit(&self, msg: Out) {
        self.metrics.writer_queue.inc();
        let _ = self.out.send(msg);
    }

    fn mark_all_syncing(&mut self) {
        for b in self.books.values_mut() {
            b.mark_syncing();
        }
        self.syncing = self.books.len();
    }

    fn subscribe_all(&mut self) {
        if self.symbols.is_empty() {
            return;
        }
        let all: Vec<Symbol> = self.symbols.iter().cloned().collect();
        let text = self.ex.subscribe_books(self.command_id(), &all);
        self.send(Pending::Subscribe, text);
        self.subscribed = self.symbols.clone();
    }

    fn on_cmd(&mut self, cmd: ShardCmd) {
        match cmd {
            ShardCmd::Add(symbols) => {
                let mut added = Vec::new();
                for sym in symbols {
                    if self.symbols.insert(sym.clone()) {
                        self.books.entry(sym.clone()).or_default();
                        self.syncing += 1;
                        added.push(sym);
                    }
                }
                if added.is_empty() {
                    return;
                }
                self.metrics.symbols.add(added.len() as i64);
                match self.sid {
                    Some(sid) => {
                        let new: Vec<Symbol> =
                            added.into_iter().filter(|s| !self.subscribed.contains(s)).collect();
                        if !new.is_empty() {
                            let text = self.ex.add_markets(self.command_id(), sid, &new);
                            self.send(Pending::Add, text);
                            self.subscribed.extend(new);
                        }
                    }
                    None => {
                        // Connected before we had any symbols: subscribe now.
                        let in_flight = self.pending.values().any(|p| matches!(p, Pending::Subscribe));
                        if self.connected && self.subscribed.is_empty() && !in_flight {
                            self.subscribe_all();
                        }
                    }
                }
            }
            ShardCmd::Remove(symbols) => {
                let mut removed = Vec::new();
                for sym in symbols {
                    if self.symbols.remove(&sym) {
                        if let Some(b) = self.books.remove(&sym)
                            && !b.is_live()
                        {
                            self.syncing = self.syncing.saturating_sub(1);
                        }
                        removed.push(sym);
                    }
                }
                if removed.is_empty() {
                    return;
                }
                self.metrics.symbols.add(-(removed.len() as i64));
                let del: Vec<Symbol> = removed.into_iter().filter(|s| self.subscribed.remove(s)).collect();
                if let (Some(sid), false) = (self.sid, del.is_empty()) {
                    let text = self.ex.delete_markets(self.command_id(), sid, &del);
                    self.send(Pending::Delete, text);
                }
            }
        }
    }

    fn on_socket(&mut self, msg: SocketMsg) {
        match msg {
            SocketMsg::Connected { .. } => {
                self.connected = true;
                self.sid = None;
                self.next_seq = None;
                self.subscribed.clear();
                self.pending.clear();
                self.mark_all_syncing();
                self.subscribe_all();
            }
            SocketMsg::Disconnected { reason, .. } => {
                self.connected = false;
                self.sid = None;
                self.mark_all_syncing();
                tracing::info!(shard = self.id, %reason, "disconnected; books syncing until resubscribed");
            }
            SocketMsg::Frame { received_at, text, .. } => match self.ex.decode(&text) {
                Ok(d) => self.on_event(received_at, d),
                Err(e) => {
                    self.metrics.decode_errors.inc();
                    tracing::warn!(shard = self.id, error = %e, frame = %text.chars().take(300).collect::<String>(), "undecodable frame");
                }
            },
        }
    }

    fn on_event(&mut self, received_at: DateTime<Utc>, d: Decoded) {
        // Per-subscription ordering check.
        if let (Some(sid), Some(seq)) = (d.sid, d.seq)
            && self.sid == Some(sid)
        {
            if let Some(expected) = self.next_seq
                && seq != expected
            {
                self.metrics.gaps.inc();
                tracing::warn!(shard = self.id, expected, got = seq, "seq gap");
                self.next_seq = Some(seq + 1);
                self.resync_all("seq gap");
                return;
            }
            self.next_seq = Some(seq + 1);
        }
        match d.event {
            Event::Subscribed { sid, .. } => {
                self.sid = Some(sid);
                if let Some(seq) = d.seq {
                    self.next_seq = Some(seq + 1);
                }
                self.pending.retain(|_, p| !matches!(p, Pending::Subscribe));
                // Anything that changed while the subscribe was in flight.
                let missing: Vec<Symbol> = self.symbols.difference(&self.subscribed).cloned().collect();
                let extra: Vec<Symbol> = self.subscribed.difference(&self.symbols).cloned().collect();
                if !missing.is_empty() {
                    let text = self.ex.add_markets(self.command_id(), sid, &missing);
                    self.send(Pending::Add, text);
                    self.subscribed.extend(missing);
                }
                if !extra.is_empty() {
                    let text = self.ex.delete_markets(self.command_id(), sid, &extra);
                    self.send(Pending::Delete, text);
                    for x in &extra {
                        self.subscribed.remove(x);
                    }
                }
            }
            Event::Snapshot { symbol, bids, asks, clock, exchange_ts } => {
                self.metrics.snapshots_rx.inc();
                if !self.symbols.contains(&symbol) {
                    return; // snapshot for a market removed meanwhile
                }
                let book = self.books.entry(symbol.clone()).or_default();
                let was_live = book.is_live();
                book.apply_snapshot(&bids, &asks, clock, exchange_ts.or(d.sending_ts));
                let snap = book.to_snapshot(&symbol, received_at);
                if !was_live {
                    self.syncing = self.syncing.saturating_sub(1);
                }
                self.emit(Out::Snapshot(snap));
            }
            Event::Delta { symbol, side, price, delta, clock, exchange_ts } => {
                self.metrics.deltas_rx.inc();
                if let Some(st) = d.sending_ts {
                    self.metrics.record_send_latency((received_at - st).num_microseconds().unwrap_or(0));
                }
                if let Some(et) = exchange_ts {
                    self.metrics.record_exchange_latency((received_at - et).num_microseconds().unwrap_or(0));
                }
                let Some(book) = self.books.get_mut(&symbol) else {
                    self.metrics.dropped_syncing.inc();
                    return;
                };
                match book.apply_delta(side, price, delta, clock, exchange_ts) {
                    Ok(qty) => {
                        self.metrics.deltas_applied.inc();
                        self.emit(Out::Update(level_update(&symbol, side, price, qty, received_at, exchange_ts)));
                    }
                    Err(ApplyError::NotLive) => self.metrics.dropped_syncing.inc(),
                    Err(e @ ApplyError::NegativeQuantity { .. }) => {
                        // The local book is wrong for this market; a fresh
                        // snapshot is the only honest fix.
                        self.metrics.resyncs.inc();
                        tracing::warn!(shard = self.id, %symbol, error = %e, "resyncing one book");
                        book.mark_syncing();
                        self.syncing += 1;
                        if let Some(sid) = self.sid {
                            let text = self.ex.get_snapshot(self.command_id(), sid, std::slice::from_ref(&symbol));
                            self.send(Pending::GetSnapshot, text);
                        }
                    }
                }
            }
            Event::Error { id, code, msg } => {
                self.metrics.exchange_errors.inc();
                let pending = id.and_then(|i| self.pending.remove(&i));
                tracing::error!(shard = self.id, code, %msg, ?pending, "exchange error");
                // 25 is terminal for the subscription; a fresh connection
                // resubscribes. A failed get_snapshot gets the same treatment.
                if code == 25 || matches!(pending, Some(Pending::GetSnapshot)) {
                    let _ = self.socket.send(SocketCmd::Reconnect);
                }
                if code == 26 {
                    tracing::error!(shard = self.id, symbols = self.symbols.len(), "per-subscription market limit hit; lower --markets-per-socket");
                }
            }
            Event::Ok { id } => {
                if let Some(id) = id {
                    self.pending.remove(&id);
                }
            }
            Event::Unsubscribed { .. } | Event::Lifecycle { .. } | Event::Other => {}
        }
    }

    /// Everything on this socket is suspect: ask for fresh snapshots of every
    /// subscribed market (Kalshi's documented recovery), or reconnect when
    /// there is no subscription to ask on.
    fn resync_all(&mut self, reason: &str) {
        self.metrics.resyncs.inc();
        self.mark_all_syncing();
        match self.sid {
            Some(sid) if !self.subscribed.is_empty() => {
                let all: Vec<Symbol> = self.subscribed.iter().cloned().collect();
                let text = self.ex.get_snapshot(self.command_id(), sid, &all);
                self.send(Pending::GetSnapshot, text);
                tracing::info!(shard = self.id, reason, n = all.len(), "requested snapshots");
            }
            _ => {
                let _ = self.socket.send(SocketCmd::Reconnect);
            }
        }
    }

    fn periodic_snapshots(&mut self) {
        let now = Utc::now();
        let snaps: Vec<Out> = self
            .books
            .iter()
            .filter(|(_, book)| book.is_live())
            .map(|(sym, book)| Out::Snapshot(book.to_snapshot(sym, now)))
            .collect();
        let n = snaps.len();
        for snap in snaps {
            self.emit(snap);
        }
        tracing::debug!(shard = self.id, n, "periodic snapshots");
    }

    fn update_gauges(&mut self) {
        let live = (self.books.len() - self.syncing) as i64;
        let syncing = self.syncing as i64;
        self.metrics.books_live.add(live - self.reported_live);
        self.metrics.books_syncing.add(syncing - self.reported_syncing);
        self.reported_live = live;
        self.reported_syncing = syncing;
    }
}
