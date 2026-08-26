use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{Connection, presets},
};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, store::fs::FsStore};

use crate::Transport;
use crate::error::PeerError;

pub const PEER_ALPN: &[u8] = b"skein/peer/0";
pub const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

pub type ConnectionReceiver = mpsc::UnboundedReceiver<Connection>;

#[derive(Debug, Clone)]
struct ChatHandler {
    tx: mpsc::UnboundedSender<Connection>,
}

impl ProtocolHandler for ChatHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let _ = self.tx.send(connection);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct IrohTransport {
    endpoint: Endpoint,
    store: FsStore,
    router: Arc<Mutex<Option<Router>>>,
    incoming: Arc<Mutex<mpsc::UnboundedReceiver<Connection>>>,
}

impl IrohTransport {
    pub async fn new(store_path: impl AsRef<Path>) -> Result<Self, PeerError> {
        let endpoint = Endpoint::builder(presets::N0)
            .bind()
            .await
            .map_err(|e| PeerError::Endpoint(e.to_string()))?;

        let store = FsStore::load(store_path.as_ref())
            .await
            .map_err(|e| PeerError::BlobStore(e.to_string()))?;
        let blobs = BlobsProtocol::new(&store, None);

        let (tx, rx) = mpsc::unbounded_channel();
        let chat = ChatHandler { tx };

        let router = Router::builder(endpoint.clone())
            .accept(BLOBS_ALPN, blobs)
            .accept(PEER_ALPN, chat)
            .spawn();

        Ok(Self {
            endpoint,
            store,
            router: Arc::new(Mutex::new(Some(router))),
            incoming: Arc::new(Mutex::new(rx)),
        })
    }

    pub fn from_parts(
        endpoint: Endpoint,
        store: FsStore,
        router: Router,
        incoming: mpsc::UnboundedReceiver<Connection>,
    ) -> Self {
        Self {
            endpoint,
            store,
            router: Arc::new(Mutex::new(Some(router))),
            incoming: Arc::new(Mutex::new(incoming)),
        }
    }

    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    pub fn remote_id(&self, conn: &Connection) -> EndpointId {
        conn.remote_id()
    }

    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    pub fn store(&self) -> FsStore {
        self.store.clone()
    }

    pub async fn connect(&self, remote_id: EndpointId) -> Result<Connection, PeerError> {
        let remote_addr = EndpointAddr::from(remote_id);
        self.endpoint
            .connect(remote_addr, PEER_ALPN)
            .await
            .map_err(|e| PeerError::Endpoint(e.to_string()))
    }

    pub async fn accept(&self) -> Result<Connection, PeerError> {
        let mut rx = self.incoming.lock().await;
        rx.recv().await.ok_or(PeerError::ChannelClosed)
    }

    pub async fn send(&self, connection: &Connection, data: &[u8]) -> Result<(), PeerError> {
        send_bytes(connection, data).await
    }

    pub async fn receive(&self, connection: &Connection) -> Result<Vec<u8>, PeerError> {
        receive_bytes(connection).await
    }

    pub async fn send_message(
        &self,
        connection: &Connection,
        data: &[u8],
    ) -> Result<(), PeerError> {
        self.send(connection, data).await
    }

    pub async fn receive_message(&self, connection: &Connection) -> Result<Vec<u8>, PeerError> {
        self.receive(connection).await
    }

    pub async fn close(&self) {
        let mut router_guard = self.router.lock().await;
        if let Some(router) = router_guard.take() {
            let _ = router.shutdown().await;
        }
        self.endpoint.close().await;
    }
}

#[async_trait::async_trait]
impl Transport for IrohTransport {
    type PeerId = EndpointId;
    type Connection = Connection;

    fn id(&self) -> Self::PeerId {
        self.id()
    }

    fn remote_id(&self, conn: &Self::Connection) -> Self::PeerId {
        self.remote_id(conn)
    }

    async fn connect(&self, peer: Self::PeerId) -> Result<Self::Connection, PeerError> {
        self.connect(peer).await
    }

    async fn accept(&self) -> Result<Self::Connection, PeerError> {
        self.accept().await
    }

    async fn send(&self, conn: &Self::Connection, data: &[u8]) -> Result<(), PeerError> {
        self.send(conn, data).await
    }

    async fn receive(&self, conn: &Self::Connection) -> Result<Vec<u8>, PeerError> {
        self.receive(conn).await
    }

    async fn close(&self) {
        self.close().await;
    }
}

pub async fn send_bytes(connection: &Connection, data: &[u8]) -> Result<(), PeerError> {
    let mut send_stream = connection
        .open_uni()
        .await
        .map_err(|e| PeerError::Endpoint(e.to_string()))?;
    send_stream
        .write_all(data)
        .await
        .map_err(|e| PeerError::Endpoint(e.to_string()))?;
    send_stream
        .finish()
        .map_err(|e| PeerError::Endpoint(e.to_string()))?;
    Ok(())
}

pub async fn receive_bytes(connection: &Connection) -> Result<Vec<u8>, PeerError> {
    let mut recv_stream = connection
        .accept_uni()
        .await
        .map_err(|e| PeerError::Endpoint(e.to_string()))?;
    let data = recv_stream
        .read_to_end(MAX_MESSAGE_SIZE)
        .await
        .map_err(|e| PeerError::Endpoint(e.to_string()))?;
    Ok(data)
}
