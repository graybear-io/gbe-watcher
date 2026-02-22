use gbe_nexus::TransportError;
use gbe_state_store::StateStoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WatcherError {
    #[error("transport: {0}")]
    Transport(#[from] TransportError),

    #[error("state store: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("lock: {0}")]
    Lock(String),

    #[error("redis: {0}")]
    Redis(String),

    #[error("writer: {0}")]
    Writer(String),

    #[error("{0}")]
    Other(String),
}
