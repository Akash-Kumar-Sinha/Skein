use std::path::Path;

use kvdb::{KvDb, Unlocked};

use crate::adapter::TreeStorage;
use crate::chunk_index::{ChunkEntry, Position, RootId};
use crate::error::StoreError;

pub struct KvDbAdapter {
    db: KvDb<Position, Unlocked>,
}

impl KvDbAdapter {
    pub fn new(db: KvDb<Position, Unlocked>) -> Self {
        Self { db }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db = KvDb::<Position, Unlocked>::open(path.as_ref().to_str().unwrap_or_default())?;
        Ok(Self { db })
    }

    pub fn inner(&self) -> &KvDb<Position, Unlocked> {
        &self.db
    }
}

impl TreeStorage for KvDbAdapter {
    fn current_root(&self) -> RootId {
        self.db.current_root()
    }

    fn branch_from(&self, from_root: RootId) -> Result<RootId, StoreError> {
        self.db.branch_from(from_root).map_err(StoreError::Db)
    }

    fn root_snapshot_id(&self, root: RootId) -> Option<RootId> {
        self.db.root_snapshot_id(root)
    }

    fn root_hash(&self, root: RootId) -> Result<[u8; 32], StoreError> {
        let mut handle = self.db.open_root(root).map_err(StoreError::Db)?;
        handle.root_hash().map_err(StoreError::Db)
    }

    fn insert_entry(
        &self,
        root: RootId,
        position: Position,
        entry: ChunkEntry,
    ) -> Result<RootId, StoreError> {
        self.db
            .update_cow(root, position, entry)
            .map_err(StoreError::Db)
    }

    fn delete_entry(&self, root: RootId, position: Position) -> Result<RootId, StoreError> {
        self.db.delete_cow(root, position).map_err(StoreError::Db)
    }

    fn get_entry(&self, root: RootId, position: Position) -> Result<ChunkEntry, StoreError> {
        self.db
            .get_at::<ChunkEntry>(root, &position)
            .map_err(StoreError::Db)
    }

    fn all_entries(&self, root: RootId) -> Result<Vec<(Position, ChunkEntry)>, StoreError> {
        let raw_entries = self.db.range_at(root).map_err(StoreError::Db)?;
        let mut results = Vec::with_capacity(raw_entries.len());
        for (pos, val) in raw_entries {
            let entry =
                ChunkEntry::try_from(val).map_err(|e| StoreError::Db(kvdb::DbError::Value(e)))?;
            results.push((pos, entry));
        }
        Ok(results)
    }
}
