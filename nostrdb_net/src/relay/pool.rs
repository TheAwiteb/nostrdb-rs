use crate::relay::{Relay, RelayStatus};
use crate::{ClientMessage, Result};
use nostrdb::Filter;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use url::Url;

use crate::relay::ws::WsEvent;

use tracing::{debug, error};

pub type WebsockEvent = WsEvent;

#[derive(Debug)]
pub struct PoolEvent<'a> {
    pub relay: &'a str,

    pub event: WebsockEvent,
}

impl PoolEvent<'_> {
    pub fn into_owned(self) -> PoolEventBuf {
        PoolEventBuf {
            relay: self.relay.to_owned(),
            event: self.event,
        }
    }
}

pub struct PoolEventBuf {
    pub relay: String,
    pub event: WsEvent,
}

pub struct PoolRelay {
    pub relay: Relay,
    pub last_ping: Instant,
    pub last_connect_attempt: Instant,
    pub retry_connect_after: Duration,
}

impl PoolRelay {
    pub fn new(relay: Relay) -> PoolRelay {
        PoolRelay {
            relay,
            last_ping: Instant::now(),
            last_connect_attempt: Instant::now(),
            retry_connect_after: Self::initial_reconnect_duration(),
        }
    }

    pub fn initial_reconnect_duration() -> Duration {
        Duration::from_secs(5)
    }
}

pub struct RelayPool {
    pub relays: Vec<PoolRelay>,
    pub ping_rate: Duration,
}

impl Default for RelayPool {
    fn default() -> Self {
        RelayPool::new()
    }
}

impl RelayPool {
    // Constructs a new, empty RelayPool.
    pub fn new() -> RelayPool {
        RelayPool {
            relays: vec![],
            ping_rate: Duration::from_secs(25),
        }
    }

    pub fn ping_rate(&mut self, duration: Duration) -> &mut Self {
        self.ping_rate = duration;
        self
    }

    pub fn has(&self, url: &str) -> bool {
        for relay in &self.relays {
            if relay.relay.url == url {
                return true;
            }
        }

        false
    }

    pub fn urls(&self) -> BTreeSet<String> {
        self.relays
            .iter()
            .map(|pool_relay| pool_relay.relay.url.clone())
            .collect()
    }

    pub fn send(&mut self, cmd: &ClientMessage) {
        for relay in &mut self.relays {
            relay.relay.send(cmd);
        }
    }

    pub fn unsubscribe(&mut self, subid: String) {
        for relay in &mut self.relays {
            relay.relay.send(&ClientMessage::close(subid.clone()));
        }
    }

    pub fn subscribe(&mut self, subid: String, filter: Vec<Filter>) {
        for relay in &mut self.relays {
            relay.relay.subscribe(subid.clone(), filter.clone());
        }
    }

    /// Keep relay connectiongs alive by pinging relays that haven't been
    /// pinged in awhile. Adjust ping rate with [`ping_rate`].
    pub fn keepalive_ping(&mut self, wakeup: impl Fn() + Send + Sync + Clone + 'static) {
        for relay in &mut self.relays {
            let now = std::time::Instant::now();

            match relay.relay.status {
                RelayStatus::Disconnected => {
                    let reconnect_at = relay.last_connect_attempt + relay.retry_connect_after;
                    if now > reconnect_at {
                        relay.last_connect_attempt = now;
                        let next_duration = Duration::from_millis(
                            ((relay.retry_connect_after.as_millis() as f64) * 1.5) as u64,
                        );
                        debug!(
                            "bumping reconnect duration from {:?} to {:?} and retrying connect",
                            relay.retry_connect_after, next_duration
                        );
                        relay.retry_connect_after = next_duration;
                        if let Err(err) = relay.relay.connect(wakeup.clone()) {
                            error!("error connecting to relay: {}", err);
                        }
                    } else {
                        // let's wait a bit before we try again
                    }
                }

                RelayStatus::Connected => {
                    relay.retry_connect_after = PoolRelay::initial_reconnect_duration();

                    let should_ping = now - relay.last_ping > self.ping_rate;
                    if should_ping {
                        debug!("pinging {}", relay.relay.url);
                        relay.relay.ping();
                        relay.last_ping = Instant::now();
                    }
                }

                RelayStatus::Connecting => {
                    // cool story bro
                }
            }
        }
    }

    pub fn send_to(&mut self, cmd: &ClientMessage, relay_url: &str) {
        for relay in &mut self.relays {
            let relay = &mut relay.relay;
            if relay.url == relay_url {
                relay.send(cmd);
                return;
            }
        }
    }

    // Adds a websocket url to the RelayPool.
    pub fn add_url(
        &mut self,
        url: String,
        wakeup: impl Fn() + Send + Sync + Clone + 'static,
    ) -> Result<()> {
        let url = Self::canonicalize_url(url);
        // Check if the URL already exists in the pool.
        if self.has(&url) {
            return Ok(());
        }
        let relay = Relay::new(url, wakeup)?;
        let pool_relay = PoolRelay::new(relay);

        self.relays.push(pool_relay);

        Ok(())
    }

    pub fn add_urls(
        &mut self,
        urls: BTreeSet<String>,
        wakeup: impl Fn() + Send + Sync + Clone + 'static,
    ) -> Result<()> {
        for url in urls {
            self.add_url(url, wakeup.clone())?;
        }
        Ok(())
    }

    pub fn remove_urls(&mut self, urls: &BTreeSet<String>) {
        self.relays
            .retain(|pool_relay| !urls.contains(&pool_relay.relay.url));
    }

    /// Standardize a relay url's format (e.g. trailing slashes) the same way
    /// [`add_url`](Self::add_url) does, so callers that key their own state by
    /// relay url match what the pool stores and reports in [`PoolEvent`]s.
    pub fn canonicalize_url(url: String) -> String {
        match Url::parse(&url) {
            Ok(parsed_url) => parsed_url.to_string(),
            Err(_) => url, // If parsing fails, return the original URL.
        }
    }

    /// Attempts to receive a pool event from the list of relays. The function
    /// searches each relay in order, polling it non-blocking. If an event is
    /// available it is returned; if no relay has one, `None` is returned.
    ///
    /// The native transport answers protocol pings itself and never surfaces
    /// them here, so this only tracks connection status and forwards data
    /// frames on to the caller.
    pub fn try_recv(&mut self) -> Option<PoolEvent<'_>> {
        for relay in &mut self.relays {
            let relay = &mut relay.relay;
            if let Some(event) = relay.receiver.try_recv() {
                match &event {
                    WsEvent::Opened => {
                        relay.status = RelayStatus::Connected;
                    }
                    WsEvent::Closed => {
                        relay.status = RelayStatus::Disconnected;
                    }
                    WsEvent::Error(err) => {
                        error!("{:?}", err);
                        relay.status = RelayStatus::Disconnected;
                    }
                    WsEvent::Message(_ev) => {}
                }
                return Some(PoolEvent {
                    event,
                    relay: &relay.url,
                });
            }
        }

        None
    }
}
