use futures_util::StreamExt;
use iroh_blobs::Hash;
use iroh_blobs::store::fs::FsStore;
use skein_peer::{
    Conduit, ConduitPeerId, IrohTransport, TcpTransport, Transport, format_size,
    render_progress_bar,
};
use std::path::PathBuf;
use std::time::Instant;
use tokio::io::AsyncWriteExt;

use crate::chunk_store::ChunkStore;
use crate::error::StoreError;

#[async_trait::async_trait]
pub trait BlobDownloader: Transport {
    async fn fetch_blob(
        &self,
        remote_id: Self::PeerId,
        hash: Hash,
        total_size: Option<u64>,
        local_store: &FsStore,
    ) -> Result<(), StoreError>;
}

#[async_trait::async_trait]
impl BlobDownloader for IrohTransport {
    async fn fetch_blob(
        &self,
        remote_id: Self::PeerId,
        hash: Hash,
        total_size: Option<u64>,
        local_store: &FsStore,
    ) -> Result<(), StoreError> {
        let downloader = local_store.downloader(&self.endpoint());
        let progress = downloader.download(hash, Some(remote_id));
        let mut stream = progress
            .stream()
            .await
            .map_err(|e| StoreError::BlobStore(e.to_string()))?;
        let start_time = Instant::now();

        while let Some(item) = stream.next().await {
            use iroh_blobs::api::downloader::DownloadProgressItem;
            match item {
                DownloadProgressItem::Progress(bytes) => {
                    let total = total_size.unwrap_or(0) as usize;
                    if total > 0 {
                        print!("{}", render_progress_bar(bytes as usize, total, start_time));
                    } else {
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let speed_str = if elapsed > 0.05 {
                            format!("{}/s", format_size((bytes as f64 / elapsed) as usize))
                        } else {
                            "-- B/s".to_string()
                        };
                        print!(
                            "\r< [downloading...] {} ({speed_str})",
                            format_size(bytes as usize)
                        );
                    }
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                }
                DownloadProgressItem::TryProvider { .. } => {}
                DownloadProgressItem::ProviderFailed { id, .. } => {
                    println!("\n< Provider failed: {id}");
                }
                DownloadProgressItem::PartComplete { .. } => {}
                DownloadProgressItem::Error(e) => {
                    return Err(StoreError::BlobStore(format!("download error: {e}")));
                }
                DownloadProgressItem::DownloadError => {
                    return Err(StoreError::BlobStore(
                        "download failed (provider unreachable)".to_string(),
                    ));
                }
            }
        }

        if let Some(total) = total_size {
            print!(
                "{}",
                render_progress_bar(total as usize, total as usize, start_time)
            );
        }
        println!("\n< Download complete");
        Ok(())
    }
}

#[async_trait::async_trait]
impl BlobDownloader for TcpTransport {
    async fn fetch_blob(
        &self,
        remote_id: Self::PeerId,
        hash: Hash,
        _total_size: Option<u64>,
        local_store: &FsStore,
    ) -> Result<(), StoreError> {
        let conn = self.connect(remote_id).await?;
        let req = format!("SKEIN_CHUNK_REQ:{}", hash);
        self.send(&conn, req.as_bytes()).await?;
        let bytes = self.receive(&conn).await?;
        let outcome = local_store
            .blobs()
            .add_bytes(bytes)
            .await
            .map_err(|e| StoreError::BlobStore(e.to_string()))?;
        if outcome.hash != hash {
            return Err(StoreError::HashMismatch {
                expected: hash.to_string(),
                actual: outcome.hash.to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BlobDownloader for Conduit {
    async fn fetch_blob(
        &self,
        remote_id: Self::PeerId,
        hash: Hash,
        total_size: Option<u64>,
        local_store: &FsStore,
    ) -> Result<(), StoreError> {
        match (self, remote_id) {
            (Conduit::Iroh(t), ConduitPeerId::Iroh(id)) => {
                t.fetch_blob(id, hash, total_size, local_store).await
            }
            (Conduit::Tcp(t), ConduitPeerId::Tcp(addr)) => {
                t.fetch_blob(addr, hash, total_size, local_store).await
            }
            _ => Err(StoreError::Custom(
                "Transport and PeerId type mismatch in blob download".to_string(),
            )),
        }
    }
}

pub struct RemoteChunkStore<T: Transport> {
    transport: T,
    remote_id: T::PeerId,
    local_store: FsStore,
    total_size: Option<u64>,
}

impl<T: Transport> RemoteChunkStore<T> {
    pub fn new(transport: T, remote_id: T::PeerId, local_store: FsStore) -> Self {
        Self {
            transport,
            remote_id,
            local_store,
            total_size: None,
        }
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.total_size = Some(size);
        self
    }

    pub fn set_total_size(&mut self, size: u64) {
        self.total_size = Some(size);
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn remote_id(&self) -> &T::PeerId {
        &self.remote_id
    }

    pub fn local_store(&self) -> &FsStore {
        &self.local_store
    }
}

impl<T: BlobDownloader> RemoteChunkStore<T> {
    pub async fn ensure_downloaded(&self, hash: Hash) -> Result<(), StoreError> {
        if self.local_store.blobs().get_bytes(hash).await.is_ok() {
            return Ok(());
        }

        self.transport
            .fetch_blob(self.remote_id, hash, self.total_size, &self.local_store)
            .await
    }
}

#[async_trait::async_trait]
impl<T: BlobDownloader> ChunkStore for RemoteChunkStore<T> {
    async fn add(&self, _path: PathBuf) -> Result<Hash, StoreError> {
        Err(StoreError::ReadOnlyStore)
    }

    async fn get(&self, hash: Hash) -> Result<Vec<u8>, StoreError> {
        self.ensure_downloaded(hash).await?;
        let bytes = self
            .local_store
            .blobs()
            .get_bytes(hash)
            .await
            .map_err(|e| StoreError::BlobStore(e.to_string()))?;
        Ok(bytes.to_vec())
    }

    async fn export(&self, hash: Hash, dest: PathBuf) -> Result<(), StoreError> {
        self.ensure_downloaded(hash).await?;

        let mut reader = self.local_store.blobs().reader(hash);
        let mut file = tokio::fs::File::create(&dest).await?;
        tokio::io::copy(&mut reader, &mut file).await?;
        file.flush().await?;

        Ok(())
    }
}
