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
//! - The backfill leg reduces each filter to its wire JSON + sealed local set
//!   synchronously, then hands both to the `Send` [`pull_reconcile`](super::pull_reconcile),
//!   whose future stays `Send` precisely because it takes no `Filter`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use nostrdb::{Filter, Ndb, SendFilter};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::interval;

use super::{Relay, local_set, pull_reconcile};
use crate::{ClientMessage, RelayPool, RelayStatus, WsEvent, WsMessage};

/// How often the loop pings/reconnects relays to keep the pool alive.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);

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

/// Pull bounded history for a subscription from `url` using NIP-77 negentropy.
///
/// Opens a dedicated reconcile connection (separate from the live pool), and for
/// each history filter reconciles the local set against the relay, then fetches
/// the ids the relay holds that the local db lacks. If the relay can't reconcile,
/// falls back to a plain `REQ` pull of the filter. See the [module docs](self) for
/// why this reduces each filter synchronously before awaiting.
async fn backfill(ndb: Ndb, url: String, filters: Vec<SendFilter>) {
    let mut relay = match Relay::connect(&url).await {
        Ok(relay) => relay,
        Err(e) => {
            tracing::warn!("session backfill: connect {url} failed: {e}");
            return;
        }
    };

    // Consume by value: an owned `SendFilter` is `Send` and may cross the awaits
    // below, whereas a `&SendFilter` would require `SendFilter: Sync` (it isn't).
    // Reduce each filter to its wire JSON and sealed local set *synchronously* —
    // the transient `nostrdb::Filter` from `as_filter()` drops at each `;`, so it
    // never crosses an await — then hand both to `pull_reconcile`, whose future
    // stays `Send` precisely because it takes no `Filter`.
    for filter in filters {
        let Ok(filter_json) = filter.as_filter().json() else {
            continue;
        };
        let local = match local_set(&ndb, filter.as_filter()) {
            Ok(local) => local,
            Err(e) => {
                tracing::warn!("session backfill: local set failed: {e}");
                continue;
            }
        };
        if let Err(e) = pull_reconcile(&mut relay, &ndb, &filter_json, local).await {
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
}
