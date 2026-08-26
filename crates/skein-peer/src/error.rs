use thiserror::Error;

#[derive(Error, Debug)]
pub enum PeerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Transport is closed")]
    TransportClosed,

    #[error("Connection closed prematurely")]
    ConnectionClosed,

    #[error("Payload size {size} exceeds maximum allowed size {max}")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("Incoming channel closed")]
    ChannelClosed,

    #[error("Network endpoint error: {0}")]
    Endpoint(String),

    #[error("Blob store error: {0}")]
    BlobStore(String),

    #[error("Transport type mismatch: {0}")]
    TransportMismatch(String),

    #[error("{0}")]
    Custom(String),
}
