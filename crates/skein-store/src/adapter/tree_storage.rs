use crate::chunk_index::{ChunkEntry, Position, RootId};
use crate::error::StoreError;

pub trait TreeStorage: Send + Sync + 'static {
    fn current_root(&self) -> RootId;
    fn branch_from(&self, from_root: RootId) -> Result<RootId, StoreError>;
    fn root_snapshot_id(&self, root: RootId) -> Option<RootId>;
    fn root_hash(&self, root: RootId) -> Result<[u8; 32], StoreError>;
    fn insert_entry(
        &self,
        root: RootId,
        position: Position,
        entry: ChunkEntry,
    ) -> Result<RootId, StoreError>;
    fn delete_entry(&self, root: RootId, position: Position) -> Result<RootId, StoreError>;
    fn get_entry(&self, root: RootId, position: Position) -> Result<ChunkEntry, StoreError>;
    fn all_entries(&self, root: RootId) -> Result<Vec<(Position, ChunkEntry)>, StoreError>;
}
