//! A long-lived client sync [`Session`]: the persistent sibling of this module's
//! one-shot [`reconcile_sync`](super::reconcile_sync)/[`pull_reconcile`](super::pull_reconcile).
//!
//! Where the one-shot path connects, does a single negentropy pass, and returns
//! (right for a CLI), a [`Session`] stays up: it **owns a [`RelayPool`]**, keeps a
//! **live `REQ`** open per subscription *and* **NIP-77 negentropy-backfills** that
//! subscription's history, tracks a **settle** point, and reconnects/keepalives —
//! all over **any** set of filters. It is entirely kind-agnostic: the caller
//! decides what to sync by handing it filters. Every embedding constructs one with
//! its own db and relays (notedeck-host over the account's private relays,
//! agentium/iOS over its own pool, the CLIs over the embedded relay) and drives it
//! through the same direct methods.
//!
//! # Shape
//!
//! [`Session::new`] `tokio::spawn`s a background [`session_loop`] that owns the
//! pool and ingests inbound events into the db; the returned handle is a thin
//! front end whose methods enqueue commands the loop drains over an internal mpsc
//! channel. Dropping the [`Session`] closes that channel and the loop returns
//! (tearing down the pool). Because the loop is spawned, `new` must be called from
//! within a Tokio runtime.
//!
//! # `!Send` discipline
//!
//! `nostrdb::Filter`/`Note`/`Transaction` wrap raw pointers and are `!Send`, so
//! the loop and its backfill tasks must never hold one across an `.await` — even a
//! value merely *in scope* across an await counts. Two rules keep every future
//! `Send` (so they spawn on the shared multi-thread runtime):
//!
//! - Filters cross the command channel and live in the loop's state as
//!   [`SendFilter`] (nostrdb's sendable, non-custom filter wrapper), converted from
//!   the caller's `Filter`s **on the caller's thread** in [`Session::set_subscription`].
//!   They are turned back into a transient `Filter` — only synchronously, never
//!   across an await — to build a `REQ` or a negentropy fold. (A filter with a
//!   custom predicate can't be a `SendFilter`, but such predicates are local-only
//!   and meaningless on the wire, so dropping them for a remote subscription is
//!   correct.)
//! - The backfill leg reduces each filter to its wire JSON synchronously, then
//!   hands it to the `Send` [`pull_reconcile_windowed`](super::pull_reconcile_windowed),
//!   whose future stays `Send` precisely because it takes no `Filter`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use nostrdb::{Filter, Ndb, SendFilter};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::interval;

use super::{Relay, pull_reconcile_windowed};
use crate::{ClientMessage, RelayPool, RelayStatus, WsEvent, WsMessage};

/// How often the loop pings/reconnects relays to keep the pool alive.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

/// Upper bound for a history backfill's initial `created_at` window: `u32::MAX`
/// (unix second `4294967295` ≈ year 2106).
///
/// Far past any real event time — so windowing covers events even when their
/// `created_at` runs ahead of this device's clock (the observed case for
/// freshly-authored envelopes) — yet still within the 32-bit range nostrdb's
/// filter `since`/`until` accept (a larger value fails `Filter::from_json` with
/// `BufferOverflow`). Windows over the relay's per-sync cap hone in by bisection,
/// so an over-wide upper bound costs only a few cheap empty-range reconciles.
const BACKFILL_UNTIL: u64 = u32::MAX as u64;

/// A handle onto a long-lived client sync loop.
///
/// Holds the sending half of the command channel into the background
/// [`session_loop`]; every method enqueues a [`Cmd`] the loop applies against its
/// owned [`RelayPool`]. Cheap to hold and `Send`; dropping it stops the loop.
///
/// See the [module docs](self) for the loop shape and the `!Send` discipline.
pub struct Session {
    cmd_tx: mpsc::UnboundedSender<Cmd>,
}

impl Session {
    /// Spawn a sync loop over `ndb` and return a handle onto it.
    ///
    /// `ndb` is held by value (it is `Clone` over an `Arc`-backed handle, so this
    /// is a cheap reference to the same database); inbound events ingest into it,
    /// and it is the settle target for backfills. Must be called from within a
    /// Tokio runtime, as it `tokio::spawn`s the loop.
    pub fn new(ndb: Ndb) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        tokio::spawn(session_loop(ndb, cmd_rx));
        Self { cmd_tx }
    }

    /// Declare (upsert) a subscription `id` targeting one relay `url`.
    ///
    /// `live_filters` drive the ongoing live `REQ` (re-sent whenever the relay
    /// reconnects); `history_filters` (possibly empty) request a bounded NIP-77
    /// negentropy backfill of past events. Both are plain nostrdb `Filter`s,
    /// converted to the sendable [`SendFilter`] here on the caller's thread before
    /// they cross to the loop — a filter carrying a custom (local-only) predicate
    /// is dropped, since it has no wire meaning. Re-declaring an existing `id`
    /// replaces its live filters and spawns a fresh backfill.
    pub fn set_subscription(
        &self,
        id: impl Into<String>,
        url: impl Into<String>,
        live_filters: Vec<Filter>,
        history_filters: Vec<Filter>,
    ) {
        let _ = self.cmd_tx.send(Cmd::SetSubscription {
            id: id.into(),
            url: url.into(),
            live: to_send_filters(live_filters),
            history: to_send_filters(history_filters),
        });
    }

    /// Drop the subscription previously declared under `id`, sending a `CLOSE` to
    /// every relay and forgetting its live filters so a reconnect won't re-send it.
    pub fn drop_subscription(&self, id: impl Into<String>) {
        let _ = self.cmd_tx.send(Cmd::DropSubscription(id.into()));
    }

    /// Publish a pre-serialized event JSON (the bare `{...}` object) to each of
    /// `relays`, connecting to any not already in the pool. A relay that isn't up
    /// yet has the frame queued until it opens.
    pub fn publish(&self, note_json: String, relays: Vec<String>) {
        let _ = self.cmd_tx.send(Cmd::Publish { note_json, relays });
    }

    /// Resolve once the history backfill(s) in flight *at this call* have settled.
    ///
    /// Returns immediately if none are in flight (no history filters, or nothing
    /// subscribed yet). Deterministic: because the barrier rides the same FIFO
    /// command channel behind every prior [`set_subscription`](Self::set_subscription),
    /// by the time the loop handles it every earlier subscription has already
    /// spawned its backfill and bumped the started count — and each backfill's
    /// `sync_into` returns only once its received events are queryable, so
    /// "settled" means the reconciled history is actually readable, not that a
    /// timer elapsed. A dropped loop (session gone) resolves immediately.
    pub async fn wait_for_sync(&self) {
        let (reply, rx) = oneshot::channel();
        if self.cmd_tx.send(Cmd::WaitForSync(reply)).is_err() {
            return;
        }
        if let Ok(settle) = rx.await {
            settle.settled().await;
        }
    }
}

/// Wrap caller filters as [`SendFilter`] so they can cross to the loop thread,
/// dropping any with a custom predicate (not sendable, and not meaningful on the
/// wire for a remote subscription).
fn to_send_filters(filters: Vec<Filter>) -> Vec<SendFilter> {
    filters
        .into_iter()
        .filter_map(|f| SendFilter::try_from_filter(f).ok())
        .collect()
}

/// A command from a [`Session`] method to the background [`session_loop`].
///
/// Every field is `Send`: filters are wrapped as [`SendFilter`] and relay urls
/// are strings, converted on the caller's thread.
enum Cmd {
    SetSubscription {
        id: String,
        url: String,
        live: Vec<SendFilter>,
        history: Vec<SendFilter>,
    },
    DropSubscription(String),
    Publish {
        note_json: String,
        relays: Vec<String>,
    },
    /// A settle barrier (see [`Session::wait_for_sync`]). Because this rides the
    /// same FIFO command channel, by the time the loop handles it every
    /// previously-enqueued [`SetSubscription`](Cmd::SetSubscription) has already
    /// spawned its backfill and bumped the started count — so the loop can hand
    /// back a [`SyncSettle`] that resolves once exactly those backfills complete.
    WaitForSync(oneshot::Sender<SyncSettle>),
}

/// Shared backfill-completion tracker owned by the [`session_loop`] and observed
/// by [`SyncSettle`] waiters.
///
/// `done` counts every backfill task that has *finished* — success, error, or
/// panic all count, via a drop guard — so a settle wait can never hang on a task
/// that died. `notify` wakes waiters on each completion. The started count is held
/// loop-locally (only the loop spawns backfills), so it needs no atomic.
#[derive(Default)]
struct BackfillProgress {
    done: AtomicU64,
    notify: Notify,
}

/// A one-shot settle handle: resolves once the tracked history backfill(s) have
/// completed. Captures a `target` snapshot of how many backfills had been started
/// when the barrier reached the loop, and resolves once that many are done.
struct SyncSettle {
    progress: Arc<BackfillProgress>,
    target: u64,
}

impl SyncSettle {
    /// Resolve once the tracked backfills have completed. Returns immediately if
    /// none were in flight at the barrier.
    async fn settled(self) {
        loop {
            // Register for the wakeup *before* re-checking, so a completion that
            // lands between the check and the await is not lost.
            let notified = self.progress.notify.notified();
            if self.progress.done.load(Ordering::Acquire) >= self.target {
                return;
            }
            notified.await;
        }
    }
}

/// The mutable state the [`session_loop`] threads through each [`apply_cmd`]: the
/// relay pool, the desired-subscription and pending-publish maps, and the backfill
/// settle tracker. Bundled into one struct so commands are applied by
/// `&mut LoopState` rather than a long argument list.
#[derive(Default)]
struct LoopState {
    pool: RelayPool,
    /// Desired live subscriptions per relay url (subid -> filters), re-sent
    /// whenever a relay (re)connects.
    desired: HashMap<String, HashMap<String, Vec<SendFilter>>>,
    /// Publish frames queued until their target relay is connected.
    pending: HashMap<String, Vec<String>>,
    /// Backfill settle tracking (see [`Session::wait_for_sync`]): the shared
    /// done-counter the detached backfill tasks bump, paired with `started`.
    progress: Arc<BackfillProgress>,
    /// Count of backfills spawned so far, snapshotted by a `WaitForSync` barrier
    /// as its settle target (against `progress.done`).
    started: u64,
}

/// The background relay loop: owns the [`RelayPool`], ingests inbound events into
/// `ndb`, and applies [`Cmd`]s. Returns when the command channel closes (the
/// [`Session`] handle dropped).
async fn session_loop(ndb: Ndb, mut cmd_rx: mpsc::UnboundedReceiver<Cmd>) {
    let notify = Arc::new(Notify::new());
    let wakeup = {
        let notify = notify.clone();
        move || notify.notify_one()
    };

    let mut state = LoopState::default();
    let mut keepalive = interval(KEEPALIVE_INTERVAL);

    loop {
        // Drain everything the pool has ready: ingest events, and flush desired
        // subs / queued publishes to relays as they come up.
        while let Some(ev) = state.pool.try_recv().map(|e| e.into_owned()) {
            match ev.event {
                WsEvent::Opened => {
                    if let Some(subs) = state.desired.get(&ev.relay) {
                        for (sid, filters) in subs {
                            state
                                .pool
                                .send_to(&req_message(sid.clone(), filters), &ev.relay);
                        }
                    }
                    if let Some(frames) = state.pending.remove(&ev.relay) {
                        for frame in frames {
                            state.pool.send_to(&ClientMessage::raw(frame), &ev.relay);
                        }
                    }
                }
                WsEvent::Message(WsMessage::Text(text)) => {
                    // Only EVENT frames ingest; EOSE/NOTICE/OK aren't events, so a
                    // parse/ingest failure here is expected, not an error.
                    if let Err(e) = ndb.process_event(&text) {
                        tracing::trace!("session: skipped non-event relay message: {e}");
                    }
                }
                _ => {}
            }
        }

        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break }; // session dropped
                apply_cmd(&ndb, &mut state, &wakeup, cmd);
            }
            // The pool signalled new data; loop back around to drain it.
            _ = notify.notified() => {}
            _ = keepalive.tick() => state.pool.keepalive_ping(wakeup.clone()),
        }
    }
}

/// Apply one [`Cmd`] against the loop's [`LoopState`].
fn apply_cmd(
    ndb: &Ndb,
    state: &mut LoopState,
    wakeup: &(impl Fn() + Send + Sync + Clone + 'static),
    cmd: Cmd,
) {
    match cmd {
        Cmd::SetSubscription {
            id,
            url,
            live,
            history,
        } => {
            // Key `desired` by the pool's canonical url so the `Opened` handler
            // (which sees the pool's own url) finds it. `add_url` canonicalizes
            // internally, so an un-normalized key here would silently never flush.
            let url = RelayPool::canonicalize_url(url);
            let _ = state.pool.add_url(url.clone(), wakeup.clone());
            // Send the live REQ now if the relay is already up; otherwise it is
            // flushed from `desired` on the relay's `Opened` event.
            if relay_connected(&state.pool, &url) {
                state.pool.send_to(&req_message(id.clone(), &live), &url);
            }
            state
                .desired
                .entry(url.clone())
                .or_default()
                .insert(id, live);
            // NIP-77 negentropy history backfill, off the loop on its own task.
            // Track its completion (via a drop guard so a panic still counts) so a
            // `WaitForSync` barrier can observe when history has settled.
            if !history.is_empty() {
                state.started += 1;
                let ndb = ndb.clone();
                let progress = state.progress.clone();
                tokio::spawn(async move {
                    let _guard = BackfillDoneGuard(progress);
                    backfill(ndb, url, history).await;
                });
            }
        }
        Cmd::DropSubscription(id) => {
            for subs in state.desired.values_mut() {
                subs.remove(&id);
            }
            state.pool.unsubscribe(id);
        }
        Cmd::Publish { note_json, relays } => {
            let frame = format!(r#"["EVENT",{note_json}]"#);
            for url in relays {
                // Canonicalize so a queued frame is keyed the same way the pool
                // reports the relay on `Opened` — otherwise it never flushes.
                let url = RelayPool::canonicalize_url(url);
                let _ = state.pool.add_url(url.clone(), wakeup.clone());
                if relay_connected(&state.pool, &url) {
                    state.pool.send_to(&ClientMessage::raw(frame.clone()), &url);
                } else {
                    state.pending.entry(url).or_default().push(frame.clone());
                }
            }
        }
        Cmd::WaitForSync(reply) => {
            // Snapshot how many backfills have been started by now; the returned
            // handle resolves once that many have completed. Because this command
            // rode the FIFO channel behind every prior `SetSubscription`, that
            // snapshot already includes their backfills. A dropped `reply` (the
            // waiter went away) is harmless.
            let _ = reply.send(SyncSettle {
                progress: state.progress.clone(),
                target: state.started,
            });
        }
    }
}

/// Drop guard that records a backfill task's completion on the shared
/// [`BackfillProgress`]. Using `Drop` (rather than a bump at the end of the task
/// body) means an error return or a panic unwinding through the task still counts
/// as done, so a [`SyncSettle`] wait can never hang on a dead backfill.
struct BackfillDoneGuard(Arc<BackfillProgress>);

impl Drop for BackfillDoneGuard {
    fn drop(&mut self) {
        self.0.done.fetch_add(1, Ordering::Release);
        self.0.notify.notify_waiters();
    }
}

/// Build a `REQ` [`ClientMessage`] for `filters`. Cloning each [`SendFilter`] back
/// to a transient `Filter` is done synchronously (never across an await), so the
/// loop future stays `Send`.
fn req_message(sid: String, filters: &[SendFilter]) -> ClientMessage {
    ClientMessage::req(sid, filters.iter().map(|f| f.as_filter().clone()).collect())
}

/// Whether `url` is currently connected in the pool (so a `REQ`/`EVENT` can be
/// sent now rather than deferred until its `Opened` event).
fn relay_connected(pool: &RelayPool, url: &str) -> bool {
    pool.relays
        .iter()
        .any(|r| r.relay.url == url && matches!(r.relay.status, RelayStatus::Connected))
}

/// Pull history for a subscription from `url` using NIP-77 negentropy.
///
/// Opens a dedicated reconcile connection (separate from the live pool) and, for
/// each history filter, reconciles the relay against the local db and fetches the
/// ids the relay holds that we lack — via [`pull_reconcile_windowed`], so a filter
/// matching more events than the relay's per-sync cap still syncs (it bisects the
/// `created_at` range under the cap). See the [module docs](self) for why each
/// filter is reduced to its wire JSON synchronously before awaiting.
async fn backfill(ndb: Ndb, url: String, filters: Vec<SendFilter>) {
    let mut relay = match Relay::connect(&url).await {
        Ok(relay) => relay,
        Err(e) => {
            tracing::warn!("session backfill: connect {url} failed: {e}");
            return;
        }
    };

    // Reduce each filter to its wire JSON *synchronously* — the transient
    // `nostrdb::Filter` from `as_filter()` drops at the `;`, so it never crosses an
    // await — then hand the JSON to `pull_reconcile_windowed`, whose future stays
    // `Send` precisely because it holds no `Filter` (an owned `SendFilter` would be
    // `Send`, but a `&SendFilter` would need `SendFilter: Sync`, which it isn't).
    for filter in filters {
        let Ok(filter_json) = filter.as_filter().json() else {
            continue;
        };
        if let Err(e) =
            pull_reconcile_windowed(&mut relay, &ndb, &filter_json, BACKFILL_UNTIL).await
        {
            tracing::warn!("session backfill: {url}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::server::{self, RelayHandle};
    use futures_util::StreamExt;
    use nostrdb::{Config, Ndb, NoteBuilder, SubscriptionStream, Transaction};
    use tempfile::TempDir;

    /// A fixed secret key for signing test events, so ids are deterministic.
    const TEST_SECKEY: [u8; 32] = [7u8; 32];

    /// The kind we sync in these tests.
    const KIND: u32 = 30000;
    /// A kind the seeded event is *not*, so a live filter over it can't match —
    /// isolating the backfill leg.
    const UNRELATED_KIND: u32 = 30001;

    /// Open a fresh nostrdb under a throwaway directory. The returned [`TempDir`]
    /// must outlive the db.
    fn temp_ndb() -> (TempDir, Ndb) {
        let dir = TempDir::new().expect("tmp dir");
        let ndb = Ndb::new(dir.path().to_str().expect("path"), &Config::new()).expect("ndb");
        (dir, ndb)
    }

    /// Spawn an in-process hermetic relay backed by `ndb` on an ephemeral port.
    fn spawn_relay(ndb: Ndb) -> RelayHandle {
        server::spawn(ndb, "127.0.0.1:0".parse().expect("addr")).expect("spawn relay")
    }

    /// Count events of `kind` currently queryable in `ndb`.
    fn count_kind(ndb: &Ndb, kind: u64) -> usize {
        let txn = Transaction::new(ndb).expect("txn");
        let filter = Filter::new().kinds([kind]).build();
        ndb.query(&txn, &[filter], 1_000_000).expect("query").len()
    }

    /// Build a signed note, returning its bare event JSON and 32-byte id.
    fn signed_note(kind: u32, content: &str) -> (String, [u8; 32]) {
        let note = NoteBuilder::new()
            .kind(kind)
            .content(content)
            .sign(&TEST_SECKEY)
            .build()
            .expect("build note");
        (note.json().expect("note json"), *note.id())
    }

    /// The `["EVENT", {...}]` client frame for a bare event JSON.
    fn event_frame(note_json: &str) -> String {
        format!(r#"["EVENT",{note_json}]"#)
    }

    /// Ingest `n` distinct signed events of `kind` directly into `ndb` (used to
    /// stock a relay's store before a client reconciles it). Distinct content
    /// gives distinct ids. Returns the ids in creation order.
    fn seed_events(ndb: &Ndb, kind: u32, n: usize) -> Vec<[u8; 32]> {
        (0..n)
            .map(|i| {
                let (json, id) = signed_note(kind, &format!("seed-{i}"));
                ndb.process_client_event(&event_frame(&json))
                    .expect("ingest seed");
                id
            })
            .collect()
    }

    /// Wait until note `id` (of `kind`) is queryable in `ndb`, driven by a
    /// [`SubscriptionStream`] with a backstop `timeout`. Subscribes first, then
    /// checks presence and awaits ingests, so it catches a note whether it landed
    /// just before or after the call.
    async fn await_note(ndb: &Ndb, id: [u8; 32], kind: u64, timeout: Duration) -> bool {
        let filter = Filter::new().kinds([kind]).build();
        let sub = ndb
            .subscribe(std::slice::from_ref(&filter))
            .expect("subscribe");
        let mut stream = SubscriptionStream::new(ndb.clone(), sub);

        tokio::time::timeout(timeout, async {
            loop {
                {
                    let txn = Transaction::new(ndb).expect("txn");
                    if ndb.get_note_by_id(&txn, &id).is_ok() {
                        return true;
                    }
                }
                if stream.next().await.is_none() {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
    }

    /// A [`Session`] backfills a relay's existing history into a second db via the
    /// negentropy leg — proven in isolation by a live filter that cannot match the
    /// seeded event, so only the history backfill can deliver it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfills_history_into_a_second_db() {
        let (_relay_dir, relay_ndb) = temp_ndb();
        let relay = spawn_relay(relay_ndb.clone());
        let url = relay.url();

        // Author on A: ingest locally, then publish the event to the relay.
        let (_a_dir, ndb_a) = temp_ndb();
        let (note_json, note_id) = signed_note(KIND, "backfill me");
        ndb_a
            .process_client_event(&event_frame(&note_json))
            .expect("ingest on A");
        let session_a = Session::new(ndb_a.clone());
        session_a.publish(note_json.clone(), vec![url.clone()]);

        // The relay must actually hold + index it before B reconciles history.
        assert!(
            await_note(&relay_ndb, note_id, KIND as u64, Duration::from_secs(5)).await,
            "relay should store A's published event"
        );

        // B subscribes with a live filter that can't match the seed (different
        // kind) and a history filter that can — so an arrival proves the backfill
        // leg, not live replay.
        let (_b_dir, ndb_b) = temp_ndb();
        let session_b = Session::new(ndb_b.clone());
        let live_none = Filter::new().kinds([UNRELATED_KIND as u64]).build();
        let history = Filter::new().kinds([KIND as u64]).build();
        session_b.set_subscription("backfill", url.clone(), vec![live_none], vec![history]);

        session_b.wait_for_sync().await;

        assert!(
            await_note(&ndb_b, note_id, KIND as u64, Duration::from_secs(5)).await,
            "B should backfill the event from the relay"
        );
        relay.shutdown();
    }

    /// `wait_for_sync` resolves only once the backfilled history is actually
    /// queryable: a plain *synchronous* count the instant it returns already sees
    /// the whole seeded set (settle = readable, not a timer), and the count
    /// matches what the relay held. The live filter is over an unrelated kind so
    /// it cannot match — only the negentropy backfill leg can deliver the events.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_sync_settles_with_whole_history_queryable() {
        const SEEDED: usize = 12;
        let (_relay_dir, relay_ndb) = temp_ndb();
        let relay = spawn_relay(relay_ndb.clone());
        let url = relay.url();

        let ids = seed_events(&relay_ndb, KIND, SEEDED);
        let last = *ids.last().expect("seeded ids");
        assert!(
            await_note(&relay_ndb, last, KIND as u64, Duration::from_secs(5)).await,
            "relay should index all seeded events before B reconciles"
        );

        // B subscribes with a live filter that can't match (different kind) and a
        // history filter that can, so an arrival proves the backfill, not replay.
        let (_b_dir, ndb_b) = temp_ndb();
        let session_b = Session::new(ndb_b.clone());
        let live_none = Filter::new().kinds([UNRELATED_KIND as u64]).build();
        let history = Filter::new().kinds([KIND as u64]).build();
        session_b.set_subscription("hist", url.clone(), vec![live_none], vec![history]);

        tokio::time::timeout(Duration::from_secs(10), session_b.wait_for_sync())
            .await
            .expect("backfill should settle within the bound");

        // The instant settle resolves, every event is queryable — no extra wait.
        assert_eq!(
            count_kind(&ndb_b, KIND as u64),
            SEEDED,
            "settle must mean the whole reconciled history is readable"
        );
        relay.shutdown();
    }

    /// With no history filter there is nothing to backfill, so the barrier
    /// snapshots a target of zero and `wait_for_sync` returns immediately rather
    /// than blocking on a sync. Covers both a live-only subscription and a
    /// session that never subscribed at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_sync_returns_immediately_without_history() {
        let (_relay_dir, relay_ndb) = temp_ndb();
        let relay = spawn_relay(relay_ndb.clone());
        let url = relay.url();

        // Live-only subscription: no backfill task is ever started.
        let (_b_dir, ndb_b) = temp_ndb();
        let session = Session::new(ndb_b);
        let live = Filter::new().kinds([KIND as u64]).build();
        session.set_subscription("live", url, vec![live], vec![]);
        tokio::time::timeout(Duration::from_secs(2), session.wait_for_sync())
            .await
            .expect("live-only settle must not block on a backfill");

        // A session that never subscribed also settles at once.
        let (_c_dir, ndb_c) = temp_ndb();
        let fresh = Session::new(ndb_c);
        tokio::time::timeout(Duration::from_secs(2), fresh.wait_for_sync())
            .await
            .expect("un-subscribed settle must not block");
        relay.shutdown();
    }

    /// The settle barrier resolves when the backfill *task finishes*, and the
    /// [`BackfillDoneGuard`] drop-guard counts a task that errored (or panicked)
    /// as done too — so a backfill against an unreachable relay still settles,
    /// having synced nothing. This pins the current settle-on-failure semantics:
    /// a settle is *not* proof of a complete history, only that the attempt
    /// finished. A future success/failure distinction (retry-on-failure) should
    /// update this test deliberately.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_sync_settles_even_when_backfill_fails() {
        let (_b_dir, ndb_b) = temp_ndb();
        let session = Session::new(ndb_b.clone());
        // Nothing listens on port 1, so the backfill's connect is refused fast.
        let dead = "ws://127.0.0.1:1".to_string();
        let live = Filter::new().kinds([UNRELATED_KIND as u64]).build();
        let history = Filter::new().kinds([KIND as u64]).build();
        session.set_subscription("dead", dead, vec![live], vec![history]);

        tokio::time::timeout(Duration::from_secs(10), session.wait_for_sync())
            .await
            .expect("settle must resolve even when the backfill fails");
        assert_eq!(
            count_kind(&ndb_b, KIND as u64),
            0,
            "a failed backfill syncs nothing"
        );
    }

    /// A [`Session`]'s live `REQ` delivers an event published *after* it
    /// subscribes: B holds an open live subscription, A publishes, B receives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_req_delivers_new_events() {
        let (_relay_dir, relay_ndb) = temp_ndb();
        let relay = spawn_relay(relay_ndb.clone());
        let url = relay.url();

        // B opens a live subscription (no history) and settles the connect.
        let (_b_dir, ndb_b) = temp_ndb();
        let session_b = Session::new(ndb_b.clone());
        let live = Filter::new().kinds([KIND as u64]).build();
        session_b.set_subscription("live", url.clone(), vec![live], vec![]);
        session_b.wait_for_sync().await;

        // A publishes a fresh event to the relay.
        let (_a_dir, ndb_a) = temp_ndb();
        let (note_json, note_id) = signed_note(KIND, "live delivery");
        ndb_a
            .process_client_event(&event_frame(&note_json))
            .expect("ingest on A");
        let session_a = Session::new(ndb_a.clone());
        session_a.publish(note_json, vec![url.clone()]);

        assert!(
            await_note(&ndb_b, note_id, KIND as u64, Duration::from_secs(10)).await,
            "B's live subscription should receive A's new event"
        );
        relay.shutdown();
    }

    /// PROBE (ignored, hits the network): run a raw NIP-77 session against the
    /// real relay to observe how strfry's `maxSyncEvents` cap manifests — whether
    /// it streams partial `need` ids across rounds and then `NEG-ERR`s, or refuses
    /// up front. Prints a frame-by-frame trace. Run with:
    ///   PROBE_RELAY=ws://relay.jb55.com cargo test -p nostrdb_net --lib \
    ///     probe_real_relay_negentropy -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "hits the real network relay; set PROBE_RELAY"]
    async fn probe_real_relay_negentropy() {
        use futures_util::SinkExt;
        use negentropy::{Negentropy, NegentropyStorageVector};
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::Message;

        let url = std::env::var("PROBE_RELAY").unwrap_or_else(|_| "ws://relay.jb55.com".into());
        let filter_json =
            std::env::var("PROBE_FILTER").unwrap_or_else(|_| r#"{"kinds":[1080]}"#.into());
        eprintln!("probe: {url} filter={filter_json}");

        let (mut ws, _) = connect_async(&url).await.expect("connect");

        // Empty local set: we hold nothing, so `need` is the relay's whole match.
        let mut storage = NegentropyStorageVector::new();
        storage.seal().expect("seal");
        let mut neg = Negentropy::owned(storage, 0).expect("neg");
        let initial = neg.initiate().expect("initiate");
        ws.send(Message::Text(format!(
            r#"["NEG-OPEN","probe",{filter_json},"{}"]"#,
            hex::encode(&initial)
        )))
        .await
        .expect("send NEG-OPEN");

        let mut round = 0u32;
        let mut total_need = 0usize;
        let started = std::time::Instant::now();
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(30), ws.next())
                .await
                .expect("frame timeout")
                .expect("stream end")
                .expect("ws error");
            let Message::Text(text) = msg else { continue };
            let frame: Vec<serde_json::Value> = serde_json::from_str(&text).expect("json");
            match frame.first().and_then(|v| v.as_str()) {
                Some("NEG-MSG") => {
                    round += 1;
                    let payload = frame.get(2).and_then(|v| v.as_str()).expect("payload");
                    let bytes = hex::decode(payload).expect("hex");
                    let mut have = Vec::new();
                    let mut need = Vec::new();
                    let reply = neg
                        .reconcile_with_ids(&bytes, &mut have, &mut need)
                        .expect("reconcile");
                    total_need += need.len();
                    eprintln!(
                        "round {round}: +{} need (total {total_need}), +{} have, more={}, {:?} elapsed",
                        need.len(),
                        have.len(),
                        reply.is_some(),
                        started.elapsed()
                    );
                    match reply {
                        Some(reply) => ws
                            .send(Message::Text(format!(
                                r#"["NEG-MSG","probe","{}"]"#,
                                hex::encode(&reply)
                            )))
                            .await
                            .expect("send NEG-MSG"),
                        None => {
                            eprintln!("CONVERGED: {total_need} need in {round} rounds");
                            break;
                        }
                    }
                }
                Some("NEG-ERR") => {
                    eprintln!("NEG-ERR after {round} rounds / {total_need} need: {}", text);
                    break;
                }
                other => {
                    eprintln!("other frame {other:?}: {}", &text[..text.len().min(200)]);
                    break;
                }
            }
        }
    }

    /// E2E (ignored, hits the network): a [`Session`] backfills a filter whose
    /// match exceeds the relay's per-sync cap into a fresh db, driven entirely
    /// through the public API (`set_subscription` + `wait_for_sync`). Proves the
    /// windowed backfill bisects under the cap and settles with the whole set
    /// queryable. Run with (the default relay has tens of thousands of kind-1080
    /// PNS envelopes behind `maxSyncEvents = 5000`):
    ///   PROBE_RELAY=ws://relay.jb55.com cargo test -p nostrdb_net --lib \
    ///     session_backfills_past_relay_cap -- --ignored --nocapture
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "hits the real network relay; set PROBE_RELAY"]
    async fn session_backfills_past_relay_cap() {
        let url = std::env::var("PROBE_RELAY").unwrap_or_else(|_| "ws://relay.jb55.com".into());
        let kind: u64 = std::env::var("PROBE_KIND")
            .ok()
            .and_then(|k| k.parse().ok())
            .unwrap_or(1080);
        eprintln!("e2e: {url} kind={kind}");

        let (_dir, ndb) = temp_ndb();
        let session = Session::new(ndb.clone());
        // Same filter for live + history; the history leg is what backfills the
        // capped set, the live leg just keeps the sub open (harmless replay).
        let hist = Filter::new().kinds([kind]).build();
        let live = Filter::new().kinds([kind]).build();
        session.set_subscription("probe", url, vec![live], vec![hist]);

        let started = std::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(180), session.wait_for_sync())
            .await
            .expect("backfill should settle within 180s");
        let n = count_kind(&ndb, kind);
        eprintln!("settled: {n} kind-{kind} events in {:?}", started.elapsed());

        assert!(
            n > 5000,
            "windowed backfill should sync past the relay's 5000-event cap; got {n}"
        );
    }
}
