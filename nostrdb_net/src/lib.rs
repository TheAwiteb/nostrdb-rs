mod client;
mod error;
mod filter;
mod keypair;
mod note;
pub mod pns;
mod profile;
mod pubkey;
pub mod relay;

pub use client::ClientMessage;
pub use error::Error;
pub use filter::Filter;
pub use keypair::{FilledKeypair, FullKeypair, Keypair, SerializableKeypair};
pub use nostr::SecretKey;
pub use note::{Note, NoteId};
pub use profile::Profile;
pub use pubkey::Pubkey;
pub use relay::message::{RelayEvent, RelayMessage};
pub use relay::pool::{PoolEvent, PoolEventBuf, RelayPool};
pub use relay::ws::{self, WsEvent, WsMessage, WsReceiver, WsSender};
pub use relay::{Relay, RelayStatus};

pub type Result<T> = std::result::Result<T, error::Error>;
