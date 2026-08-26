use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Transport error: {0}")]
    Peer(#[from] skein_peer::PeerError),

    #[error("Database error: {0}")]
    Db(#[from] kvdb::DbError),

    #[error("Downloaded blob hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("RemoteChunkStore is read-only from this peer's perspective")]
    ReadOnlyStore,

    #[error("Blob storage error: {0}")]
    BlobStore(String),

    #[error("{0}")]
    Custom(String),
}
