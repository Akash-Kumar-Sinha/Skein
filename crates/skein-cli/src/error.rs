use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Transport error: {0}")]
    Peer(#[from] skein_peer::PeerError),

    #[error("Store error: {0}")]
    Store(#[from] skein_store::StoreError),

    #[error("Invalid peer ID: {0}")]
    InvalidPeerId(String),
}
