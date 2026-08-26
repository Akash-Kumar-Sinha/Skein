use skein_edl::{EditOp, PersonalTree};
use skein_peer::Transport;
use skein_store::ChunkStore;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::error::SyncError;
use crate::gossip::OpGossip;

pub struct OpSyncer<T: Transport + Clone> {
    gossip: OpGossip<T>,
    shared_ops: Arc<Mutex<Vec<EditOp>>>,
}

impl<T: Transport + Clone> OpSyncer<T> {
    pub fn new(gossip: OpGossip<T>) -> Self {
        Self {
            gossip,
            shared_ops: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn gossip(&self) -> &OpGossip<T> {
        &self.gossip
    }

    pub fn shared_ops(&self) -> Arc<Mutex<Vec<EditOp>>> {
        Arc::clone(&self.shared_ops)
    }

    pub async fn push_unmerged(&self, personal: &mut PersonalTree) -> Result<usize, SyncError> {
        let unmerged_ops: Vec<EditOp> = personal.unmerged().cloned().collect();
        let count = unmerged_ops.len();

        for op in &unmerged_ops {
            self.gossip.broadcast(op).await?;
            personal.mark_merged(op.op_id());

            let mut shared = self.shared_ops.lock().await;
            if !shared.iter().any(|o| o.op_id() == op.op_id()) {
                shared.push(op.clone());
            }
        }

        Ok(count)
    }

    pub async fn process_incoming_op(
        &self,
        op: EditOp,
        store: Option<&impl ChunkStore>,
    ) -> Result<(), SyncError> {
        if let EditOp::Insert { chunk_hash, .. } = &op
            && let Some(store) = store
        {
            let _ = store.get(*chunk_hash).await;
        }

        let mut shared = self.shared_ops.lock().await;
        if !shared.iter().any(|o| o.op_id() == op.op_id()) {
            shared.push(op);
        }

        Ok(())
    }
}
