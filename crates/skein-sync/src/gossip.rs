use futures_util::Stream;
use skein_edl::EditOp;
use skein_peer::Transport;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use tokio_stream::StreamExt;

use crate::error::SyncError;

pub const OP_GOSSIP_TOPIC: &str = "skein/ops/0";
const GOSSIP_CHANNEL_CAPACITY: usize = 1024;
const OP_MAGIC: &[u8] = b"SKEIN_OP:";

#[derive(Clone)]
pub struct OpGossip<T: Transport> {
    transport: T,
    peers: Arc<Mutex<HashMap<T::PeerId, T::Connection>>>,
    tx: broadcast::Sender<EditOp>,
}

impl<T: Transport + Clone> OpGossip<T> {
    pub fn new(transport: T) -> Self {
        let (tx, _) = broadcast::channel(GOSSIP_CHANNEL_CAPACITY);
        Self {
            transport,
            peers: Arc::new(Mutex::new(HashMap::new())),
            tx,
        }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub async fn add_peer(&self, peer_id: T::PeerId, connection: T::Connection) {
        let mut guard = self.peers.lock().await;
        guard.insert(peer_id, connection);
    }

    pub async fn remove_peer(&self, peer_id: &T::PeerId) {
        let mut guard = self.peers.lock().await;
        guard.remove(peer_id);
    }

    pub async fn connect_peer(&self, peer_id: T::PeerId) -> Result<T::Connection, SyncError> {
        let conn = self.transport.connect(peer_id).await?;
        self.add_peer(peer_id, conn.clone()).await;
        self.spawn_receiver(conn.clone());
        Ok(conn)
    }

    pub async fn broadcast(&self, op: &EditOp) -> Result<(), SyncError> {
        let serialized =
            postcard::to_allocvec(op).map_err(|e| SyncError::Serialization(e.to_string()))?;

        let mut payload = Vec::with_capacity(OP_MAGIC.len() + serialized.len());
        payload.extend_from_slice(OP_MAGIC);
        payload.extend_from_slice(&serialized);

        let peers: Vec<T::Connection> = {
            let guard = self.peers.lock().await;
            guard.values().cloned().collect()
        };

        for conn in peers {
            if let Err(e) = self.transport.send(&conn, &payload).await {
                eprintln!("Failed to send op to peer: {e}");
            }
        }

        let _ = self.tx.send(op.clone());

        Ok(())
    }

    pub fn subscribe(&self) -> Pin<Box<dyn Stream<Item = EditOp> + Send>> {
        let rx = self.tx.subscribe();
        Box::pin(tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|res| res.ok()))
    }

    pub fn spawn_receiver(&self, connection: T::Connection) -> tokio::task::JoinHandle<()> {
        let transport = self.transport.clone();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            while let Ok(bytes) = transport.receive(&connection).await {
                if bytes.starts_with(OP_MAGIC) {
                    let op_bytes = &bytes[OP_MAGIC.len()..];
                    if let Ok(op) = postcard::from_bytes::<EditOp>(op_bytes) {
                        let _ = tx.send(op);
                    }
                }
            }
        })
    }
}
