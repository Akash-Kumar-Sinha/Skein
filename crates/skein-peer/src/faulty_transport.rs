use std::time::Duration;

use crate::error::PeerError;
use crate::transport::Transport;

#[derive(Clone)]
pub struct FaultyTransport<T: Transport> {
    inner: T,
    drop_rate: f64,
    max_delay_ms: u64,
}

impl<T: Transport> FaultyTransport<T> {
    pub fn new(inner: T, drop_rate: f64, max_delay_ms: u64) -> Self {
        Self {
            inner,
            drop_rate: drop_rate.clamp(0.0, 1.0),
            max_delay_ms,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub async fn maybe_drop(&self) -> bool {
        if self.drop_rate <= 0.0 {
            return false;
        }
        rand::random::<f64>() < self.drop_rate
    }

    pub async fn maybe_delay(&self) {
        if self.max_delay_ms > 0 {
            let ms = rand::random::<u64>() % self.max_delay_ms.max(1);
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
    }
}

#[async_trait::async_trait]
impl<T: Transport> Transport for FaultyTransport<T> {
    type PeerId = T::PeerId;
    type Connection = T::Connection;

    fn id(&self) -> Self::PeerId {
        self.inner.id()
    }

    fn remote_id(&self, conn: &Self::Connection) -> Self::PeerId {
        self.inner.remote_id(conn)
    }

    async fn connect(&self, peer: Self::PeerId) -> Result<Self::Connection, PeerError> {
        self.maybe_delay().await;
        if self.maybe_drop().await {
            return Err(PeerError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "Fault injection: connection dropped",
            )));
        }
        self.inner.connect(peer).await
    }

    async fn accept(&self) -> Result<Self::Connection, PeerError> {
        self.maybe_delay().await;
        self.inner.accept().await
    }

    async fn send(&self, conn: &Self::Connection, data: &[u8]) -> Result<(), PeerError> {
        self.maybe_delay().await;
        if self.maybe_drop().await {
            return Err(PeerError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "Fault injection: packet dropped on send",
            )));
        }
        self.inner.send(conn, data).await
    }

    async fn receive(&self, conn: &Self::Connection) -> Result<Vec<u8>, PeerError> {
        self.maybe_delay().await;
        if self.maybe_drop().await {
            return Err(PeerError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Fault injection: packet dropped on receive",
            )));
        }
        self.inner.receive(conn).await
    }

    async fn close(&self) {
        self.inner.close().await;
    }
}
