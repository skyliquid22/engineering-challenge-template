//! A shard: one engine, `redundancy` socket workers subscribed to the same
//! symbols. Each source keeps its own set of books driven by its own
//! `seq`-ordered stream; only the active source's changes are written. When
//! the active source fails (disconnect, seq gap, terminal error) the engine
//! switches to a healthy standby and emits a snapshot of every live book
//! first, as extension 1 requires.
//!
//! Subscriptions are flow-controlled: the probe showed Kalshi drains one
//! connection at roughly 2000 frames/s and overflows the subscription's
//! outbound buffer (error 25) when asked for far more snapshots at once, and
//! the first full-universe run showed a fixed timer is not enough on a slow
//! link. Each source keeps about one request in flight and sends the next
//! only when those snapshots have arrived, so whatever the pipe delivers sets
//! the pace and deltas for live books are not stuck behind snapshots.
//!
//! Books live here, not in a central store: a single owner, no locks, in-order
//! processing of one socket's stream, and the minute snapshot is a local
//! iteration.

use std::collections::{HashMap, HashSet, VecDeque};
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
    /// Active source silent this long while a standby is flowing = failover.
    pub silence_window: Duration,
    pub journal: JournalMode,
    /// Largest subscribe / add_markets / get_snapshot request; each source
    /// adapts its own size to how fast the previous request arrived.
    pub subscribe_chunk: usize,
    /// Interval between chunks, per source.
    pub pace: Duration,
    /// Shard `i` opens its sockets `i * stagger` after spawn, so dozens of
    /// shards do not all take their initial snapshot burst at once.
    pub stagger: Duration,
    /// A standby waits for the active source to finish its initial sync (at
    /// most this long) before subscribing, so startup bandwidth goes to the
    /// books being written.
    pub standby_defer: Duration,
}

pub struct ShardHandle {
    pub id: usize,
    pub cmds: mpsc::UnboundedSender<ShardCmd>,
}

/// Smallest snapshot request; the chunk never adapts below this.
const MIN_CHUNK: usize = 10;
/// A chunk should arrive within this; slower halves the next one, twice as
/// fast doubles it. Keeps roughly a second of snapshots in flight per socket
/// whatever the link delivers.
const CHUNK_TARGET: Duration = Duration::from_secs(1);

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
    /// Not yet told; drained by the pacer in chunks.
    to_subscribe: VecDeque<Symbol>,
    /// Books to re-request snapshots for; drained by the pacer in chunks.
    to_snapshot: VecDeque<Symbol>,
    /// Snapshots requested (subscribe/add/get_snapshot) but not yet received.
    outstanding: usize,
    /// Current request size; adapts to how fast the previous one arrived.
    chunk: usize,
    /// Size of the previous request, 0 once its delivery time was used.
    requested_prev: usize,
    last_request_at: Instant,
    last_snapshot_at: Instant,
    connected_at: Instant,
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

    /// Nothing queued and nothing in flight: the books are whatever the
    /// exchange was willing to send.
    fn settled(&self) -> bool {
        self.connected
            && self.to_subscribe.is_empty()
            && self.to_snapshot.is_empty()
            && self.outstanding == 0
    }

    fn update_health(&mut self) {
        if self.settled() && self.health != Health::Healthy {
            let still = self.syncing;
            self.set_health(Health::Healthy, &format!("synced ({still} books never snapshotted)"));
        }
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
        let mut socket_cfg = cfg.socket.clone();
        if let Some(every) = socket_cfg.reconnect_every {
            socket_cfg.initial_offset = every * s as u32 / cfg.redundancy.max(1) as u32;
        }
        socket_cfg.initial_connect_delay =
            cfg.stagger * id as u32 + cfg.stagger * s as u32 / cfg.redundancy.max(1) as u32;
        tokio::spawn(run_socket(
            ex.clone(),
            s,
            label.clone(),
            rx,
            sock_tx.clone(),
            cancel.child_token(),
            socket_cfg,
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
            to_subscribe: VecDeque::new(),
            to_snapshot: VecDeque::new(),
            outstanding: 0,
            chunk: (cfg.subscribe_chunk / 4).clamp(MIN_CHUNK, cfg.subscribe_chunk.max(MIN_CHUNK)),
            requested_prev: 0,
            last_request_at: Instant::now(),
            last_snapshot_at: Instant::now(),
            connected_at: Instant::now(),
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
        let mut pace_tick = tokio::time::interval(self.cfg.pace.max(Duration::from_millis(20)));
        pace_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                _ = pace_tick.tick() => self.pace(),
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
                for s in &mut self.sources {
                    if s.connected {
                        s.to_subscribe.extend(added.iter().cloned());
                    }
                }
                self.pace();
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
                    if !s.to_subscribe.is_empty() || !s.to_snapshot.is_empty() {
                        let gone: HashSet<&Symbol> = removed.iter().collect();
                        s.to_subscribe.retain(|x| !gone.contains(x));
                        s.to_snapshot.retain(|x| !gone.contains(x));
                    }
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
                s.to_snapshot.clear();
                s.to_subscribe = self.symbols.iter().cloned().collect();
                s.outstanding = 0;
                s.requested_prev = 0;
                s.connected_at = Instant::now();
                s.last_request_at = s.connected_at;
                s.last_snapshot_at = s.connected_at;
                s.snapshots_since_connect = 0;
                s.last_frame = Some(Instant::now());
                s.mark_all_syncing();
                if conn == 1 {
                    s.set_health(Health::Syncing, "connected");
                } else {
                    s.set_health(Health::Failed, "reconnected, resyncing");
                }
                self.pace();
            }
            SocketMsg::Disconnected { source, reason } => {
                let s = &mut self.sources[source];
                s.connected = false;
                s.sid = None;
                s.to_subscribe.clear();
                s.to_snapshot.clear();
                s.outstanding = 0;
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
                // Symbols removed while the subscribe was in flight; added
                // ones are already queued for the pacer.
                let extra: Vec<Symbol> = s.subscribed.difference(&want).cloned().collect();
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
                s.outstanding = s.outstanding.saturating_sub(1);
                s.last_snapshot_at = Instant::now();
                s.update_health();
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
                        s.to_snapshot.push_back(symbol.clone());
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
        s.outstanding = 0;
        if s.sid.is_some() {
            // Only books the server knows about can be re-snapshotted.
            s.to_snapshot = s.subscribed.iter().cloned().collect();
        } else {
            let _ = s.cmds.send(SocketCmd::Reconnect);
        }
    }

    /// Send at most one chunk per source per tick: a `subscribe` when the
    /// source has no subscription yet, `add_markets` for queued symbols, or
    /// `get_snapshot` for books waiting to be resynced.
    fn pace(&mut self) {
        const STALL: Duration = Duration::from_secs(10);
        let max_chunk = self.cfg.subscribe_chunk.max(MIN_CHUNK);
        let now = Instant::now();
        let active = self.active;
        let active_ready = self.sources[active].health == Health::Healthy;
        for i in 0..self.sources.len() {
            let s = &self.sources[i];
            if !s.connected {
                continue;
            }
            // Standbys let the active source sync first.
            if i != active
                && !active_ready
                && s.subscribed.is_empty()
                && now.duration_since(s.connected_at) < self.cfg.standby_defer
            {
                continue;
            }
            // Flow control: about one chunk in flight per source. Whatever the
            // pipe delivers sets the pace, so exchange-side queues stay short
            // and deltas for live books are not stuck behind snapshots.
            if s.outstanding > s.chunk / 4 {
                let stalled = now.duration_since(s.last_request_at) > STALL
                    && now.duration_since(s.last_snapshot_at) > STALL;
                if !stalled {
                    continue;
                }
                // The rest of that chunk is not coming (markets closed since
                // discovery, or a request the exchange ignored); move on.
                tracing::debug!(shard = self.id, source = i, outstanding = s.outstanding, "snapshot stall; writing off outstanding");
                self.sources[i].outstanding = 0;
                self.sources[i].update_health();
            }
            // Adapt the request size to how fast the previous one arrived.
            let s = &mut self.sources[i];
            if s.requested_prev > 0 {
                let took = now.duration_since(s.last_request_at);
                if took > CHUNK_TARGET {
                    s.chunk = (s.chunk / 2).max(MIN_CHUNK);
                } else if took < CHUNK_TARGET / 2 {
                    s.chunk = (s.chunk * 2).min(max_chunk);
                }
                s.requested_prev = 0;
            }
            let chunk = s.chunk;
            let s = &self.sources[i];
            match s.sid {
                None => {
                    if s.to_subscribe.is_empty()
                        || s.pending.values().any(|p| matches!(p, Pending::Subscribe))
                    {
                        continue;
                    }
                    let s = &mut self.sources[i];
                    let batch: Vec<Symbol> = s.to_subscribe.drain(..chunk.min(s.to_subscribe.len())).collect();
                    s.subscribed.extend(batch.iter().cloned());
                    s.outstanding += batch.len();
                    s.requested_prev = batch.len();
                    s.last_request_at = now;
                    let text = self.ex.subscribe_books(self.next_id, &batch);
                    self.send(i, Pending::Subscribe, text);
                }
                Some(sid) => {
                    if !s.to_subscribe.is_empty() {
                        let s = &mut self.sources[i];
                        let batch: Vec<Symbol> = s.to_subscribe.drain(..chunk.min(s.to_subscribe.len())).collect();
                        s.subscribed.extend(batch.iter().cloned());
                        s.outstanding += batch.len();
                        s.requested_prev = batch.len();
                        s.last_request_at = now;
                        let text = self.ex.add_markets(self.next_id, sid, &batch);
                        self.send(i, Pending::Add, text);
                    } else if !s.to_snapshot.is_empty() {
                        let s = &mut self.sources[i];
                        let batch: Vec<Symbol> = s.to_snapshot.drain(..chunk.min(s.to_snapshot.len())).collect();
                        s.outstanding += batch.len();
                        s.requested_prev = batch.len();
                        s.last_request_at = now;
                        let text = self.ex.get_snapshot(self.next_id, sid, &batch);
                        self.send(i, Pending::GetSnapshot, text);
                    }
                }
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
        for s in &mut self.sources {
            s.update_health();
        }
        // Extension 2: the active source went quiet while a standby is flowing.
        // Only meaningful once neither side is waiting on snapshots.
        if self.sources.len() > 1 {
            let a = self.active;
            let act = &self.sources[a];
            let standby_flowing = self.sources.iter().enumerate().any(|(i, s)| {
                i != a
                    && s.settled()
                    && s.health == Health::Healthy
                    && s.last_frame.is_some_and(|t| now.duration_since(t) < Duration::from_secs(2))
            });
            let active_silent = act.settled()
                && act.health == Health::Healthy
                && act.last_frame.is_some_and(|t| now.duration_since(t) > self.cfg.silence_window);
            if standby_flowing && active_silent {
                tracing::warn!(shard = self.id, source = a, "active source silent while standby flows");
                self.resync_source(a, "silent while standby flows");
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
