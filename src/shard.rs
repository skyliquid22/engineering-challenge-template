//! A shard: one engine, `redundancy` socket workers subscribed to the same
//! symbols. Each source keeps its own set of books driven by its own
//! `seq`-ordered stream; only the active source's changes are written. When
//! the active source fails (disconnect, seq gap, terminal error) the engine
//! switches to a healthy standby and emits a snapshot of every live book
//! first, as extension 1 requires.
//!
//! Books live here, not in a central store: a single owner, no locks, in-order
//! processing of one socket's stream, and the minute snapshot is a local
//! iteration.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::book::{ApplyError, Book, level_update};
use crate::exchange::kalshi::Kalshi;
use crate::exchange::{Decoded, Event, Symbol};
use crate::journal::{JournalLine, JournalSender};
use crate::metrics::Metrics;
use crate::socket::{SocketCmd, SocketConfig, SocketMsg, run_socket};
use crate::writer::{Out, OutSender};

#[derive(Debug)]
pub enum ShardCmd {
    Add(Vec<Symbol>),
    Remove(Vec<Symbol>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalMode {
    Off,
    /// Frames from the active source only (what produced the output).
    Active,
    All,
}

#[derive(Clone, Debug)]
pub struct ShardConfig {
    /// Sockets per shard; 1 disables redundancy.
    pub redundancy: usize,
    pub socket: SocketConfig,
    pub snapshot_every: Duration,
    /// A source still missing snapshots this long after (re)connecting is
    /// declared healthy anyway if it has received some; the rest may be
    /// markets the exchange will never snapshot (closed since discovery).
    pub sync_grace: Duration,
    pub journal: JournalMode,
}

pub struct ShardHandle {
    pub id: usize,
    pub cmds: mpsc::UnboundedSender<ShardCmd>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Health {
    /// First connection, snapshots still arriving. Not a failure.
    Syncing,
    Healthy,
    /// Disconnected or gapped; not trusted until resynced.
    Failed,
}

#[derive(Debug, Clone, Copy)]
enum Pending {
    Subscribe,
    Add,
    Delete,
    GetSnapshot,
}

struct Source {
    label: String,
    cmds: mpsc::UnboundedSender<SocketCmd>,
    connected: bool,
    sid: Option<u64>,
    next_seq: Option<u64>,
    books: HashMap<Symbol, Book>,
    /// Symbols the server has been told about on the current connection.
    subscribed: HashSet<Symbol>,
    syncing: usize,
    health: Health,
    unhealthy_since: Instant,
    snapshots_since_connect: u64,
    last_frame: Option<Instant>,
    pending: HashMap<u64, Pending>,
}

impl Source {
    fn live_count(&self) -> usize {
        self.books.len() - self.syncing
    }

    fn mark_all_syncing(&mut self) {
        for b in self.books.values_mut() {
            b.mark_syncing();
        }
        self.syncing = self.books.len();
    }

    fn set_health(&mut self, h: Health, reason: &str) {
        if self.health != h {
            tracing::info!(source = %self.label, from = ?self.health, to = ?h, reason, "source health");
            if h != Health::Healthy {
                self.unhealthy_since = Instant::now();
            }
            self.health = h;
        }
    }

    fn send(&self, text: String) {
        let _ = self.cmds.send(SocketCmd::Send(text));
    }
}

pub struct Engine {
    id: usize,
    ex: Arc<Kalshi>,
    cfg: ShardConfig,
    sources: Vec<Source>,
    active: usize,
    symbols: HashSet<Symbol>,
    out: OutSender,
    journal: Option<JournalSender>,
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
    journal: Option<JournalSender>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) -> (ShardHandle, tokio::task::JoinHandle<()>) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (sock_tx, sock_rx) = mpsc::unbounded_channel();
    let mut sources = Vec::with_capacity(cfg.redundancy.max(1));
    for s in 0..cfg.redundancy.max(1) {
        let (tx, rx) = mpsc::unbounded_channel();
        let label = format!("shard{id}/{s}");
        tokio::spawn(run_socket(
            ex.clone(),
            s,
            label.clone(),
            rx,
            sock_tx.clone(),
            cancel.child_token(),
            cfg.socket.clone(),
            metrics.clone(),
        ));
        sources.push(Source {
            label,
            cmds: tx,
            connected: false,
            sid: None,
            next_seq: None,
            books: HashMap::new(),
            subscribed: HashSet::new(),
            syncing: 0,
            health: Health::Syncing,
            unhealthy_since: Instant::now(),
            snapshots_since_connect: 0,
            last_frame: None,
            pending: HashMap::new(),
        });
    }
    drop(sock_tx);
    let engine = Engine {
        id,
        ex,
        cfg,
        sources,
        active: 0,
        symbols: HashSet::new(),
        out,
        journal,
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
        let mut health_tick = tokio::time::interval(Duration::from_secs(1));
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
                _ = health_tick.tick() => self.health_check(),
            }
        }
        tracing::info!(shard = self.id, "engine stopped");
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn emit(&self, msg: Out) {
        self.metrics.writer_queue.inc();
        let _ = self.out.send(msg);
    }

    /// Queue `text` on source `i` and remember what the reply means.
    fn send(&mut self, i: usize, kind: Pending, text: String) {
        let id = self.next_id();
        let s = &mut self.sources[i];
        s.pending.insert(id, kind);
        s.send(text);
    }

    fn subscribe_all(&mut self, i: usize) {
        if self.symbols.is_empty() {
            return;
        }
        let all: Vec<Symbol> = self.symbols.iter().cloned().collect();
        let text = self.ex.subscribe_books(self.next_id, &all);
        self.send(i, Pending::Subscribe, text);
        self.sources[i].subscribed = self.symbols.clone();
    }

    fn on_cmd(&mut self, cmd: ShardCmd) {
        match cmd {
            ShardCmd::Add(symbols) => {
                let mut added = Vec::new();
                for sym in symbols {
                    if self.symbols.insert(sym.clone()) {
                        for s in &mut self.sources {
                            s.books.entry(sym.clone()).or_default();
                            s.syncing += 1;
                        }
                        added.push(sym);
                    }
                }
                if added.is_empty() {
                    return;
                }
                self.metrics.symbols.add(added.len() as i64);
                for i in 0..self.sources.len() {
                    let s = &self.sources[i];
                    match s.sid {
                        Some(sid) => {
                            let new: Vec<Symbol> =
                                added.iter().filter(|x| !s.subscribed.contains(*x)).cloned().collect();
                            if !new.is_empty() {
                                let text = self.ex.add_markets(self.next_id, sid, &new);
                                self.send(i, Pending::Add, text);
                                self.sources[i].subscribed.extend(new);
                            }
                        }
                        None => {
                            // Connected before we had any symbols: subscribe now.
                            let in_flight = s.pending.values().any(|p| matches!(p, Pending::Subscribe));
                            if s.connected && s.subscribed.is_empty() && !in_flight {
                                self.subscribe_all(i);
                            }
                        }
                    }
                }
            }
            ShardCmd::Remove(symbols) => {
                let mut removed = Vec::new();
                for sym in symbols {
                    if self.symbols.remove(&sym) {
                        for s in &mut self.sources {
                            if let Some(b) = s.books.remove(&sym)
                                && !b.is_live()
                            {
                                s.syncing = s.syncing.saturating_sub(1);
                            }
                        }
                        removed.push(sym);
                    }
                }
                if removed.is_empty() {
                    return;
                }
                self.metrics.symbols.add(-(removed.len() as i64));
                for i in 0..self.sources.len() {
                    let s = &mut self.sources[i];
                    let del: Vec<Symbol> = removed.iter().filter(|x| s.subscribed.remove(*x)).cloned().collect();
                    if let (Some(sid), false) = (s.sid, del.is_empty()) {
                        let text = self.ex.delete_markets(self.next_id, sid, &del);
                        self.send(i, Pending::Delete, text);
                    }
                }
            }
        }
        self.maybe_failover();
    }

    fn on_socket(&mut self, msg: SocketMsg) {
        match msg {
            SocketMsg::Connected { source, conn } => {
                let s = &mut self.sources[source];
                s.connected = true;
                s.sid = None;
                s.next_seq = None;
                s.subscribed.clear();
                s.pending.clear();
                s.snapshots_since_connect = 0;
                s.last_frame = Some(Instant::now());
                s.mark_all_syncing();
                if conn == 1 {
                    s.set_health(Health::Syncing, "connected");
                } else {
                    s.set_health(Health::Failed, "reconnected, resyncing");
                }
                self.subscribe_all(source);
            }
            SocketMsg::Disconnected { source, reason } => {
                let s = &mut self.sources[source];
                s.connected = false;
                s.sid = None;
                s.mark_all_syncing();
                s.set_health(Health::Failed, &format!("disconnected: {reason}"));
            }
            SocketMsg::Frame { source, received_at, text } => {
                self.sources[source].last_frame = Some(Instant::now());
                let journal_it = match self.cfg.journal {
                    JournalMode::Off => false,
                    JournalMode::Active => source == self.active,
                    JournalMode::All => true,
                };
                if journal_it && let Some(j) = &self.journal {
                    self.metrics.journal_queue.inc();
                    let _ = j.send(JournalLine {
                        received_at,
                        source: self.sources[source].label.clone(),
                        text: text.clone(),
                    });
                }
                match self.ex.decode(&text) {
                    Ok(d) => self.on_event(source, received_at, d),
                    Err(e) => {
                        self.metrics.decode_errors.inc();
                        tracing::warn!(shard = self.id, source, error = %e, frame = %text.chars().take(300).collect::<String>(), "undecodable frame");
                    }
                }
            }
        }
        self.maybe_failover();
    }

    fn on_event(&mut self, source: usize, received_at: DateTime<Utc>, d: Decoded) {
        let is_active = source == self.active;
        // Per-subscription ordering check.
        if let (Some(sid), Some(seq)) = (d.sid, d.seq)
            && self.sources[source].sid == Some(sid)
        {
            if let Some(expected) = self.sources[source].next_seq
                && seq != expected
            {
                self.metrics.gaps.inc();
                tracing::warn!(shard = self.id, source, expected, got = seq, "seq gap");
                self.sources[source].next_seq = Some(seq + 1);
                self.resync_source(source, "seq gap");
                return;
            }
            self.sources[source].next_seq = Some(seq + 1);
        }
        match d.event {
            Event::Subscribed { sid, .. } => {
                let want = self.symbols.clone();
                let s = &mut self.sources[source];
                s.sid = Some(sid);
                if let Some(seq) = d.seq {
                    s.next_seq = Some(seq + 1);
                }
                s.pending.retain(|_, p| !matches!(p, Pending::Subscribe));
                // Anything that changed while the subscribe was in flight.
                let missing: Vec<Symbol> = want.difference(&s.subscribed).cloned().collect();
                let extra: Vec<Symbol> = s.subscribed.difference(&want).cloned().collect();
                if !missing.is_empty() {
                    let text = self.ex.add_markets(self.next_id, sid, &missing);
                    self.send(source, Pending::Add, text);
                    self.sources[source].subscribed.extend(missing);
                }
                if !extra.is_empty() {
                    let text = self.ex.delete_markets(self.next_id, sid, &extra);
                    self.send(source, Pending::Delete, text);
                    for x in &extra {
                        self.sources[source].subscribed.remove(x);
                    }
                }
            }
            Event::Snapshot { symbol, bids, asks, clock, exchange_ts } => {
                self.metrics.snapshots_rx.inc();
                if !self.symbols.contains(&symbol) {
                    return; // snapshot for a market removed meanwhile
                }
                let s = &mut self.sources[source];
                let book = s.books.entry(symbol.clone()).or_default();
                let was_live = book.is_live();
                book.apply_snapshot(&bids, &asks, clock, exchange_ts.or(d.sending_ts));
                let snap = is_active.then(|| book.to_snapshot(&symbol, received_at));
                if !was_live {
                    s.syncing = s.syncing.saturating_sub(1);
                }
                s.snapshots_since_connect += 1;
                if s.syncing == 0 && s.health != Health::Healthy {
                    s.set_health(Health::Healthy, "all books synced");
                }
                if let Some(snap) = snap {
                    self.emit(Out::Snapshot(snap));
                }
            }
            Event::Delta { symbol, side, price, delta, clock, exchange_ts } => {
                self.metrics.deltas_rx.inc();
                if is_active {
                    if let Some(st) = d.sending_ts {
                        self.metrics.record_send_latency((received_at - st).num_microseconds().unwrap_or(0));
                    }
                    if let Some(et) = exchange_ts {
                        self.metrics.record_exchange_latency((received_at - et).num_microseconds().unwrap_or(0));
                    }
                }
                let s = &mut self.sources[source];
                let Some(book) = s.books.get_mut(&symbol) else {
                    self.metrics.dropped_syncing.inc();
                    return;
                };
                match book.apply_delta(side, price, delta, clock, exchange_ts) {
                    Ok(qty) => {
                        if is_active {
                            self.metrics.deltas_applied.inc();
                            self.emit(Out::Update(level_update(&symbol, side, price, qty, received_at, exchange_ts)));
                        }
                    }
                    Err(ApplyError::NotLive) => self.metrics.dropped_syncing.inc(),
                    Err(e @ ApplyError::NegativeQuantity { .. }) => {
                        // The local book is wrong for this market; a fresh
                        // snapshot is the only honest fix.
                        self.metrics.resyncs.inc();
                        tracing::warn!(shard = self.id, source, %symbol, error = %e, "resyncing one book");
                        book.mark_syncing();
                        s.syncing += 1;
                        if let Some(sid) = s.sid {
                            let text = self.ex.get_snapshot(self.next_id, sid, std::slice::from_ref(&symbol));
                            self.send(source, Pending::GetSnapshot, text);
                        }
                    }
                }
            }
            Event::Error { id, code, msg } => {
                self.metrics.exchange_errors.inc();
                let s = &mut self.sources[source];
                let pending = id.and_then(|i| s.pending.remove(&i));
                tracing::error!(shard = self.id, source, code, %msg, ?pending, "exchange error");
                // 25 is terminal for the subscription; a fresh connection
                // resubscribes. A failed get_snapshot gets the same treatment.
                if code == 25 || matches!(pending, Some(Pending::GetSnapshot)) {
                    let _ = s.cmds.send(SocketCmd::Reconnect);
                }
                if code == 26 {
                    tracing::error!(shard = self.id, symbols = self.symbols.len(), "per-subscription market limit hit; lower --markets-per-socket");
                }
            }
            Event::Ok { id } => {
                if let Some(id) = id {
                    self.sources[source].pending.remove(&id);
                }
            }
            Event::Unsubscribed { .. } | Event::Lifecycle { .. } | Event::Other => {}
        }
    }

    /// Everything on `source` is suspect: request fresh snapshots for all of
    /// its markets (Kalshi's documented recovery), or reconnect if there is
    /// no subscription to ask on.
    fn resync_source(&mut self, source: usize, reason: &str) {
        self.metrics.resyncs.inc();
        let s = &mut self.sources[source];
        s.mark_all_syncing();
        s.set_health(Health::Failed, reason);
        match s.sid {
            Some(sid) if !s.subscribed.is_empty() => {
                let all: Vec<Symbol> = s.subscribed.iter().cloned().collect();
                let text = self.ex.get_snapshot(self.next_id, sid, &all);
                self.send(source, Pending::GetSnapshot, text);
            }
            _ => {
                let _ = s.cmds.send(SocketCmd::Reconnect);
            }
        }
    }

    fn maybe_failover(&mut self) {
        let a = self.active;
        if self.sources[a].connected && self.sources[a].health != Health::Failed {
            return;
        }
        let Some(b) = (0..self.sources.len())
            .filter(|&i| i != a)
            .find(|&i| self.sources[i].connected && self.sources[i].health == Health::Healthy)
        else {
            return;
        };
        self.active = b;
        self.metrics.failovers.inc();
        tracing::warn!(shard = self.id, from = a, to = b, "failover: switching active source");
        let now = Utc::now();
        let snaps: Vec<Out> = self.sources[b]
            .books
            .iter()
            .filter(|(_, book)| book.is_live())
            .map(|(sym, book)| Out::Snapshot(book.to_snapshot(sym, now)))
            .collect();
        for snap in snaps {
            self.emit(snap);
        }
    }

    fn periodic_snapshots(&mut self) {
        let now = Utc::now();
        let snaps: Vec<Out> = self.sources[self.active]
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

    fn health_check(&mut self) {
        let now = Instant::now();
        let grace = self.cfg.sync_grace;
        for s in &mut self.sources {
            if !s.connected || s.health == Health::Healthy {
                continue;
            }
            if s.syncing == 0 {
                s.set_health(Health::Healthy, "all books synced");
            } else if s.snapshots_since_connect > 0
                && now.duration_since(s.unhealthy_since) > grace
                && s.last_frame.is_some_and(|t| now.duration_since(t) < Duration::from_secs(5))
            {
                let still = s.syncing;
                s.set_health(Health::Healthy, &format!("grace elapsed with {still} books still syncing"));
            }
        }
        self.maybe_failover();
        // Gauges describe the active source.
        let live = self.sources[self.active].live_count() as i64;
        let syncing = self.sources[self.active].syncing as i64;
        self.metrics.books_live.add(live - self.reported_live);
        self.metrics.books_syncing.add(syncing - self.reported_syncing);
        self.reported_live = live;
        self.reported_syncing = syncing;
    }
}
