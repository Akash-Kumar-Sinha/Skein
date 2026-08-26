use iroh_blobs::Hash;
use std::path::PathBuf;

use crate::error::StoreError;

#[async_trait::async_trait]
pub trait ChunkStore: Send + Sync {
    async fn add(&self, path: PathBuf) -> Result<Hash, StoreError>;

    async fn get(&self, hash: Hash) -> Result<Vec<u8>, StoreError>;

    async fn export(&self, hash: Hash, dest: PathBuf) -> Result<(), StoreError>;
}
