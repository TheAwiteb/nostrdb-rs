//! Minimal async NIP-01 + NIP-77 client for a running notedeck's embedded relay.
//!
//! Shared by the headway and notebook CLIs (and any other tool that keeps its own
//! nostrdb cache and reconciles it against the app's relay):
//! - [`Relay::reconcile`] runs a NIP-77 negentropy session as the initiator to
//!   learn the set difference both ways — which events the relay has that we
//!   don't, and which we have that it doesn't — without transferring the data.
//! - [`Relay::sync_into`] `REQ`s events (by id, after reconcile) and processes
//!   each stored one into a local nostrdb, so the caller can fold locally.
//! - [`Relay::publish`] forwards the `["EVENT", {...}]` frames produced by an
//!   edit (or surfaced by reconcile) back to the relay, so the app sees them.

use std::collections::HashSet;
use std::time::Duration;

use crate::NoteId;
use futures_util::{SinkExt, StreamExt};
use negentropy::{Negentropy, NegentropyStorageVector};
use nostrdb::{Filter, Ndb, Subscription, SubscriptionStream, Transaction};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use super::Result;

/// The subscription id used for the one-shot sync and reconciliation. Arbitrary —
/// the relay only needs it to be consistent across the session's frames.
const SUB: &str = "sync";

/// How long to wait for nostrdb's background ingester to make freshly-synced
/// events queryable before giving up. Localhost ingest is sub-millisecond; this
/// is only a backstop so a dropped/stuck ingest can't hang the CLI.
const INGEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The set difference a [`Relay::reconcile`] uncovers, as raw event ids.
pub struct Diff {
    /// Ids the relay holds that the local cache is missing — pull these down.
    pub need: Vec<[u8; 32]>,
    /// Ids the local cache holds that the relay is missing — push these up.
    pub have: Vec<[u8; 32]>,
}

/// [`Relay::reconcile`] error: the relay refused the whole reconciliation because
/// the filter matches more events than its per-sync cap (strfry `maxSyncEvents`,
/// surfaced as `NEG-ERR ... "blocked: too many query results"`).
///
/// The relay counts the filter's *total* match, not the set difference, and
/// streams no partial result — so a single reconcile can't sync such a filter.
/// This is distinct from a fatal error precisely because it is *recoverable* by
/// narrowing the filter so each sub-query stays under the cap; see
/// [`pull_reconcile_windowed`](super::pull_reconcile_windowed), which bisects the
/// `created_at` range on exactly this error.
#[derive(Debug)]
pub struct TooManyResults {
    /// The cap the relay advertised in the `NEG-ERR` frame, if present (the 4th
    /// element — e.g. `5000`).
    pub limit: Option<u64>,
}

impl std::fmt::Display for TooManyResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.limit {
            Some(limit) => write!(
                f,
                "reconcile filter matches more than the relay's cap of {limit}"
            ),
            None => write!(
                f,
                "reconcile filter matches more than the relay's per-sync cap"
            ),
        }
    }
}

impl std::error::Error for TooManyResults {}

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct Relay {
    ws: Ws,
    url: String,
}

impl Relay {
    pub async fn connect(url: &str) -> Result<Self> {
        let (ws, _resp) = connect_async(url)
            .await
            .map_err(|e| format!("connecting to {url}: {e}"))?;
        Ok(Relay {
            ws,
            url: url.to_string(),
        })
    }

    /// Run a NIP-77 negentropy reconciliation as the initiator over the events
    /// matching `filter_json`. `storage` is the sealed local set `(created_at,
    /// id)`. Returns the [`Diff`]: the ids each side is missing, computed in
    /// O(difference) without transferring the matching events themselves.
    pub async fn reconcile(
        &mut self,
        filter_json: &str,
        storage: NegentropyStorageVector,
    ) -> Result<Diff> {
        // frame_size_limit 0 = unlimited; the data is small and this is
        // localhost, so we don't bound per-message size.
        let mut neg = Negentropy::owned(storage, 0)?;
        let initial = neg.initiate()?;
        self.ws
            .send(Message::Text(format!(
                r#"["NEG-OPEN","{SUB}",{filter_json},"{}"]"#,
                hex::encode(&initial)
            )))
            .await?;

        let mut diff = Diff {
            need: Vec::new(),
            have: Vec::new(),
        };
        loop {
            let text = self.next_text().await?;
            let frame: Vec<Value> = serde_json::from_str(&text)?;
            match frame.first().and_then(Value::as_str) {
                Some("NEG-MSG") => {
                    let msg = frame
                        .get(2)
                        .and_then(Value::as_str)
                        .ok_or("NEG-MSG missing payload")?;
                    let msg = hex::decode(msg).map_err(|e| format!("bad NEG-MSG hex: {e}"))?;

                    // `have`/`need` are from our (the initiator's) perspective:
                    // have = we hold, relay lacks; need = relay holds, we lack.
                    let mut have = Vec::new();
                    let mut need = Vec::new();
                    let reply = neg.reconcile_with_ids(&msg, &mut have, &mut need)?;
                    diff.have.extend(have.iter().map(|id| *id.as_bytes()));
                    diff.need.extend(need.iter().map(|id| *id.as_bytes()));

                    match reply {
                        // More ranges to probe — answer and keep going.
                        Some(reply) => {
                            self.ws
                                .send(Message::Text(format!(
                                    r#"["NEG-MSG","{SUB}","{}"]"#,
                                    hex::encode(&reply)
                                )))
                                .await?;
                        }
                        // Nothing left to ask: reconciliation is complete.
                        None => break,
                    }
                }
                Some("NEG-ERR") => {
                    let reason = frame.get(2).and_then(Value::as_str).unwrap_or("");
                    // "too many query results" (strfry) / "RESULTS_TOO_BIG" (the
                    // NIP-77 spec code) is the recoverable cap refusal — surface it
                    // as a typed error so a windowed caller can narrow and retry,
                    // rather than a fatal opaque string.
                    if reason.contains("too many query results")
                        || reason.contains("RESULTS_TOO_BIG")
                    {
                        let limit = frame.get(3).and_then(Value::as_u64);
                        return Err(Box::new(TooManyResults { limit }));
                    }
                    return Err(format!("relay refused reconciliation: {reason}").into());
                }
                // A relay that doesn't speak NIP-77 answers NEG-OPEN with a
                // NOTICE (or nothing). Anything that isn't a reconciliation frame
                // means the handshake won't progress — bail rather than block
                // forever waiting for a NEG-MSG that will never come.
                Some("NOTICE") => {
                    let msg = frame.get(1).and_then(Value::as_str).unwrap_or("");
                    return Err(format!("relay does not support NIP-77 negentropy: {msg}").into());
                }
                other => {
                    return Err(format!(
                        "unexpected '{}' frame during negentropy reconcile",
                        other.unwrap_or("?")
                    )
                    .into());
                }
            }
        }
        self.ws
            .send(Message::Text(format!(r#"["NEG-CLOSE","{SUB}"]"#)))
            .await?;
        Ok(diff)
    }

    /// `REQ` `filter_json`, processing every stored EVENT into `ndb`. Returns the
    /// ids of the events received once they're queryable in `ndb`.
    ///
    /// nostrdb ingests on a background thread, so an event handed to
    /// `process_event` isn't immediately readable. We subscribe to `filter_json`
    /// *before* the `REQ` and, after `EOSE`, await that [`SubscriptionStream`]
    /// until every received id has landed — driving the ingest off a wakeup
    /// rather than polling for it.
    pub async fn sync_into(&mut self, ndb: &Ndb, filter_json: &str) -> Result<Vec<[u8; 32]>> {
        // Subscribe first so notes ingested during this REQ wake the stream we
        // await below. (A subscription only fires for future ingests, so it must
        // exist before we feed events in.) The parsed `Filter` is dropped
        // immediately: a `nostrdb::Filter` is `!Send`, and holding one across the
        // await loop below would make this future `!Send`, blocking callers that
        // drive sync on a multi-thread runtime (e.g. a `tokio::spawn`ed engine).
        let sub = {
            let filter = Filter::from_json(filter_json)?;
            ndb.subscribe(std::slice::from_ref(&filter))?
        };

        self.ws
            .send(Message::Text(format!(r#"["REQ","{SUB}",{filter_json}]"#)))
            .await?;

        let mut received = Vec::new();
        loop {
            let text = self.next_text().await?;
            let frame: Vec<Value> = serde_json::from_str(&text)?;
            match frame.first().and_then(Value::as_str) {
                // Relay form is ["EVENT", <sub>, <note>]; hand nostrdb the whole
                // message verbatim so it ingests the note as received.
                Some("EVENT") => {
                    ndb.process_event(&text)
                        .map_err(|e| format!("ingesting event: {e}"))?;
                    if let Some(id) = frame
                        .get(2)
                        .and_then(|note| note.get("id"))
                        .and_then(Value::as_str)
                        .and_then(|hex| NoteId::from_hex(hex).ok())
                    {
                        received.push(*id.bytes());
                    }
                }
                Some("EOSE") => break,
                Some("CLOSED") => return Err(format!("relay closed subscription: {text}").into()),
                _ => {}
            }
        }
        self.ws
            .send(Message::Text(format!(r#"["CLOSE","{SUB}"]"#)))
            .await?;

        await_ingest(ndb, sub, &received).await;
        Ok(received)
    }

    /// Send each `["EVENT", {...}]` frame and wait for its `OK`, erroring if the
    /// relay genuinely rejects one.
    ///
    /// A `duplicate`/`replaced` rejection is **not** an error: it means the relay
    /// already holds the event (or, for an addressable event, a newer version
    /// with the same `d`-tag). Either way our copy — or something better — is
    /// already there, so the push has nothing to do. This is the common case when
    /// reconcile flushes a cached event whose newer replacement made the relay
    /// drop the old id we still hold.
    pub async fn publish(&mut self, frames: &[String]) -> Result<()> {
        for frame in frames {
            self.ws.send(Message::Text(frame.clone())).await?;
        }
        let mut acked = 0;
        while acked < frames.len() {
            let text = self.next_text().await?;
            let frame: Vec<Value> = serde_json::from_str(&text)?;
            if frame.first().and_then(Value::as_str) == Some("OK") {
                let accepted = frame.get(2).and_then(Value::as_bool).unwrap_or(false);
                if !accepted {
                    let reason = frame.get(3).and_then(Value::as_str).unwrap_or("");
                    if !is_benign_reject(reason) {
                        return Err(format!("relay rejected event: {reason}").into());
                    }
                }
                acked += 1;
            }
        }
        Ok(())
    }

    /// Await the next text frame, skipping pings/binary.
    async fn next_text(&mut self) -> Result<String> {
        loop {
            let msg = self
                .ws
                .next()
                .await
                .ok_or_else(|| format!("{} closed the connection", self.url))??;
            if let Message::Text(text) = msg {
                return Ok(text);
            }
        }
    }
}

/// Wait until every id in `received` is queryable in `ndb`, driven by `sub`'s
/// [`SubscriptionStream`] rather than a sleep-poll. `sub` must have been created
/// before the events were fed in, so its stream fires as each one is ingested.
///
/// Ids already present when we start (a re-synced duplicate the relay returned
/// but ndb already held) never re-fire the subscription, so we discount them up
/// front; only genuinely-new ids are awaited. Dropping the stream on return
/// unsubscribes.
async fn await_ingest(ndb: &Ndb, sub: Subscription, received: &[[u8; 32]]) {
    let mut pending: HashSet<[u8; 32]> = {
        let Ok(txn) = Transaction::new(ndb) else {
            return;
        };
        received
            .iter()
            .filter(|id| ndb.get_note_by_id(&txn, id).is_err())
            .copied()
            .collect()
    };
    if pending.is_empty() {
        return;
    }

    let mut stream = SubscriptionStream::new(ndb.clone(), sub);
    // A backstop deadline: localhost ingest is near-instant, but a stuck ingest
    // mustn't hang the CLI forever.
    let _ = tokio::time::timeout(INGEST_TIMEOUT, async {
        while let Some(keys) = stream.next().await {
            let Ok(txn) = Transaction::new(ndb) else {
                break;
            };
            for key in keys {
                if let Ok(note) = ndb.get_note_by_key(&txn, key) {
                    pending.remove(note.id());
                }
            }
            if pending.is_empty() {
                break;
            }
        }
    })
    .await;
}

/// Whether an `OK: false` reason means the relay already has the event (or a
/// newer replaceable version) rather than truly refusing it. NIP-01 machine
/// reasons are `<prefix>: <message>`; strfry replies `duplicate: ...` for an
/// event it already stored and `replaced: have newer event` for an addressable
/// event superseded by a newer one. Both leave the relay's state at-or-ahead of
/// what we pushed, so a best-effort sync should treat them as delivered.
fn is_benign_reject(reason: &str) -> bool {
    let prefix = reason.split(':').next().unwrap_or("").trim();
    matches!(prefix, "duplicate" | "replaced")
}

#[cfg(test)]
mod tests {
    use super::is_benign_reject;

    #[test]
    fn classifies_ok_reasons() {
        // The relay already has this (or a newer replaceable version): nothing to do.
        assert!(is_benign_reject("replaced: have newer event"));
        assert!(is_benign_reject("duplicate: already have this event"));
        // A real refusal must still surface as an error.
        assert!(!is_benign_reject("blocked: pubkey not allowed"));
        assert!(!is_benign_reject("invalid: bad signature"));
        assert!(!is_benign_reject("rate-limited: slow down"));
        assert!(!is_benign_reject(""));
    }
}
