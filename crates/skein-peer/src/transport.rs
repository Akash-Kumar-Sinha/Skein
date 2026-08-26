use std::fmt::{Debug, Display};
use std::hash::Hash;

use crate::error::PeerError;

#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    type PeerId: Clone + Copy + Send + Sync + Debug + Display + Eq + Hash + 'static;

    type Connection: Clone + Send + Sync + 'static;

    fn id(&self) -> Self::PeerId;

    fn remote_id(&self, conn: &Self::Connection) -> Self::PeerId;

    async fn connect(&self, peer: Self::PeerId) -> Result<Self::Connection, PeerError>;

    async fn accept(&self) -> Result<Self::Connection, PeerError>;

    async fn send(&self, conn: &Self::Connection, data: &[u8]) -> Result<(), PeerError>;

    async fn receive(&self, conn: &Self::Connection) -> Result<Vec<u8>, PeerError>;

    async fn close(&self);
}
