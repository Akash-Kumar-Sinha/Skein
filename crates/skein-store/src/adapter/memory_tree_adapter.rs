use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use crate::adapter::TreeStorage;
use crate::chunk_index::{ChunkEntry, Position, RootId};
use crate::error::StoreError;

#[derive(Default)]
pub struct MemoryTreeAdapter {
    roots: RwLock<HashMap<RootId, BTreeMap<Position, ChunkEntry>>>,
    ancestry: RwLock<HashMap<RootId, RootId>>,
    next_root: RwLock<RootId>,
}

impl MemoryTreeAdapter {
    pub fn new() -> Self {
        let mut roots = HashMap::new();
        roots.insert(0, BTreeMap::new());
        Self {
            roots: RwLock::new(roots),
            ancestry: RwLock::new(HashMap::new()),
            next_root: RwLock::new(1),
        }
    }
}

impl TreeStorage for MemoryTreeAdapter {
    fn current_root(&self) -> RootId {
        0
    }

    fn branch_from(&self, from_root: RootId) -> Result<RootId, StoreError> {
        let mut roots = self
            .roots
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let mut next = self
            .next_root
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let from_data = roots.get(&from_root).cloned().unwrap_or_default();
        let new_id = *next;
        *next += 1;
        roots.insert(new_id, from_data);

        let mut ancestry = self
            .ancestry
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        ancestry.insert(new_id, from_root);

        Ok(new_id)
    }

    fn root_snapshot_id(&self, root: RootId) -> Option<RootId> {
        self.ancestry.read().ok()?.get(&root).copied()
    }

    fn root_hash(&self, root: RootId) -> Result<[u8; 32], StoreError> {
        let roots = self
            .roots
            .read()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let tree = roots
            .get(&root)
            .ok_or_else(|| StoreError::Custom("Invalid root".into()))?;
        let bytes = postcard::to_allocvec(tree).map_err(|e| StoreError::Custom(e.to_string()))?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }

    fn insert_entry(
        &self,
        root: RootId,
        position: Position,
        entry: ChunkEntry,
    ) -> Result<RootId, StoreError> {
        let mut roots = self
            .roots
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let mut next = self
            .next_root
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let mut tree = roots.get(&root).cloned().unwrap_or_default();
        tree.insert(position, entry);
        let new_id = *next;
        *next += 1;
        roots.insert(new_id, tree);

        let mut ancestry = self
            .ancestry
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        ancestry.insert(new_id, root);

        Ok(new_id)
    }

    fn delete_entry(&self, root: RootId, position: Position) -> Result<RootId, StoreError> {
        let mut roots = self
            .roots
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let mut next = self
            .next_root
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let mut tree = roots.get(&root).cloned().unwrap_or_default();
        tree.remove(&position);
        let new_id = *next;
        *next += 1;
        roots.insert(new_id, tree);

        let mut ancestry = self
            .ancestry
            .write()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        ancestry.insert(new_id, root);

        Ok(new_id)
    }

    fn get_entry(&self, root: RootId, position: Position) -> Result<ChunkEntry, StoreError> {
        let roots = self
            .roots
            .read()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let tree = roots
            .get(&root)
            .ok_or_else(|| StoreError::Custom("Invalid root".into()))?;
        tree.get(&position)
            .cloned()
            .ok_or_else(|| StoreError::Custom("Key not found".into()))
    }

    fn all_entries(&self, root: RootId) -> Result<Vec<(Position, ChunkEntry)>, StoreError> {
        let roots = self
            .roots
            .read()
            .map_err(|_| StoreError::Custom("Lock poisoned".into()))?;
        let tree = roots
            .get(&root)
            .ok_or_else(|| StoreError::Custom("Invalid root".into()))?;
        Ok(tree.iter().map(|(p, e)| (*p, e.clone())).collect())
    }
}
