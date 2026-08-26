use kvdb::{PageId, Value, ValueError};
use std::ops::Range;

use crate::adapter::{KvDbAdapter, MemoryTreeAdapter, TreeStorage};
use crate::error::StoreError;

pub type RootId = PageId;
pub type Position = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Branch {
    pub root: RootId,
    pub branched_from: RootId,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChunkEntry {
    pub hash: iroh_blobs::Hash,
    pub duration_ms: u64,
    pub codec: String,
    pub source_offset: u64,
    pub is_keyframe: bool,
    pub keyframe_offset_ms: u64,
    pub byte_size: u64,
}

impl ChunkEntry {
    pub fn new(
        hash: iroh_blobs::Hash,
        duration_ms: u64,
        codec: impl Into<String>,
        source_offset: u64,
        is_keyframe: bool,
        keyframe_offset_ms: u64,
        byte_size: u64,
    ) -> Self {
        Self {
            hash,
            duration_ms,
            codec: codec.into(),
            source_offset,
            is_keyframe,
            keyframe_offset_ms,
            byte_size,
        }
    }

    pub fn is_seekable(&self) -> bool {
        self.is_keyframe || self.keyframe_offset_ms == 0
    }
}

impl From<ChunkEntry> for Value {
    fn from(entry: ChunkEntry) -> Value {
        Value::Bytes(postcard::to_allocvec(&entry).expect("ChunkEntry always serializes"))
    }
}

impl TryFrom<Value> for ChunkEntry {
    type Error = ValueError;
    fn try_from(value: Value) -> Result<Self, ValueError> {
        match value {
            Value::Bytes(bytes) => {
                postcard::from_bytes(&bytes).map_err(|_| ValueError::TypeMismatch)
            }
            _ => Err(ValueError::TypeMismatch),
        }
    }
}

pub struct GenericChunkIndex<S: TreeStorage> {
    storage: S,
}

pub type ChunkIndex = GenericChunkIndex<KvDbAdapter>;
pub type MemoryChunkIndex = GenericChunkIndex<MemoryTreeAdapter>;

impl ChunkIndex {
    pub fn open(tree: kvdb::KvDb<Position, kvdb::Unlocked>) -> Self {
        Self::new(KvDbAdapter::new(tree))
    }

    pub fn open_path(path: impl AsRef<std::path::Path>) -> Result<Self, StoreError> {
        let adapter = KvDbAdapter::open(path)?;
        Ok(Self::new(adapter))
    }

    pub fn db(&self) -> &kvdb::KvDb<Position, kvdb::Unlocked> {
        self.storage.inner()
    }
}

impl MemoryChunkIndex {
    pub fn memory() -> Self {
        Self::new(MemoryTreeAdapter::new())
    }
}

impl<S: TreeStorage> GenericChunkIndex<S> {
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn current_root(&self) -> RootId {
        self.storage.current_root()
    }

    pub fn branch(&self, from: RootId) -> Result<Branch, StoreError> {
        let root = self.storage.branch_from(from)?;
        Ok(Branch {
            root,
            branched_from: from,
        })
    }

    pub fn branched_from(&self, root: RootId) -> Option<RootId> {
        self.storage.root_snapshot_id(root)
    }

    pub fn root_hash(&self, root: RootId) -> Result<[u8; 32], StoreError> {
        self.storage.root_hash(root)
    }

    pub fn insert(
        &self,
        root: RootId,
        position: Position,
        entry: ChunkEntry,
    ) -> Result<RootId, StoreError> {
        self.storage.insert_entry(root, position, entry)
    }

    pub fn delete(&self, root: RootId, position: Position) -> Result<RootId, StoreError> {
        self.storage.delete_entry(root, position)
    }

    pub fn get(&self, root: RootId, position: Position) -> Result<ChunkEntry, StoreError> {
        self.storage.get_entry(root, position)
    }

    pub fn all(&self, root: RootId) -> Result<Vec<(Position, ChunkEntry)>, StoreError> {
        self.storage.all_entries(root)
    }

    pub fn range(
        &self,
        root: RootId,
        span: Range<Position>,
    ) -> Result<Vec<(Position, ChunkEntry)>, StoreError> {
        let all = self.all(root)?;
        Ok(all
            .into_iter()
            .filter(|(pos, _)| span.contains(pos))
            .collect())
    }

    pub fn find_nearest_keyframe(
        &self,
        root: RootId,
        position: Position,
    ) -> Result<Option<(Position, ChunkEntry)>, StoreError> {
        let all = self.all(root)?;
        let mut best: Option<(Position, ChunkEntry)> = None;

        for (pos, entry) in all {
            if pos <= position && entry.is_seekable() {
                best = Some((pos, entry));
            }
        }

        Ok(best)
    }
}
