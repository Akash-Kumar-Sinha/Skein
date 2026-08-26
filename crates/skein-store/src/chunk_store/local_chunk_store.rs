use iroh_blobs::Hash;
use iroh_blobs::store::fs::FsStore;
use std::path::{Path, PathBuf};

use crate::chunk_store::ChunkStore;
use crate::error::StoreError;

pub struct LocalChunkStore {
    store: FsStore,
}

impl LocalChunkStore {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let store = FsStore::load(path.as_ref())
            .await
            .map_err(|e| StoreError::BlobStore(e.to_string()))?;
        Ok(Self { store })
    }

    pub fn from_store(store: FsStore) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl ChunkStore for LocalChunkStore {
    async fn add(&self, path: PathBuf) -> Result<Hash, StoreError> {
        // CRITICAL: use add_bytes to create raw blobs rather than collections
        let bytes = tokio::fs::read(&path).await?;
        let outcome = self
            .store
            .blobs()
            .add_bytes(bytes)
            .await
            .map_err(|e| StoreError::BlobStore(e.to_string()))?;
        Ok(outcome.hash)
    }

    async fn get(&self, hash: Hash) -> Result<Vec<u8>, StoreError> {
        let bytes = self
            .store
            .blobs()
            .get_bytes(hash)
            .await
            .map_err(|e| StoreError::BlobStore(e.to_string()))?;
        Ok(bytes.to_vec())
    }

    async fn export(&self, hash: Hash, dest: PathBuf) -> Result<(), StoreError> {
        self.store
            .blobs()
            .export(hash, dest)
            .await
            .map_err(|e| StoreError::BlobStore(e.to_string()))?;
        Ok(())
    }
}
