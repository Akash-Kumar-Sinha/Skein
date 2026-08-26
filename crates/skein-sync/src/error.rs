use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Transport error: {0}")]
    Peer(#[from] skein_peer::PeerError),

    #[error("Storage error: {0}")]
    Store(#[from] skein_store::StoreError),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Channel error: {0}")]
    Channel(String),

    #[error("Custom error: {0}")]
    Custom(String),
}
