use kvdb::{KvDb, Unlocked};
use skein_store::{ChunkEntry, ChunkIndex, LocalChunkStore, Position, RootId, segment_video_file};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone)]
pub struct BenchmarkReport {
    pub video_name: String,
    pub video_size_bytes: u64,
    pub naive_duration: Duration,
    pub naive_bytes_transferred: u64,
    pub delta_duration: Duration,
    pub delta_bytes_transferred: u64,
    pub total_chunks: usize,
    pub touched_chunks: usize,
    pub bandwidth_saved_pct: f64,
    pub speedup_factor: f64,
}

impl BenchmarkReport {
    pub fn display_summary(&self) -> String {
        format!(
            "\n=================================================================\n\
             BENCHMARK REPORT: {}\n\
             File Size: {} bytes ({:.2} MB)\n\
             Total Chunks: {}, Touched Chunks: {}\n\
             -----------------------------------------------------------------\n\
             Metric                 | Naive Full Upload | Delta Sync (Skein)  \n\
             -----------------------+-------------------+--------------------\n\
             Bytes Transferred      | {:>15} B | {:>16} B \n\
             Wall-Clock Latency     | {:>15.4?} | {:>16.4?} \n\
             -----------------------------------------------------------------\n\
             Bandwidth Reduction    | {:>36.2} %\n\
             Throughput Speedup     | {:>36.2} x\n\
             =================================================================",
            self.video_name,
            self.video_size_bytes,
            self.video_size_bytes as f64 / (1024.0 * 1024.0),
            self.total_chunks,
            self.touched_chunks,
            self.naive_bytes_transferred,
            self.delta_bytes_transferred,
            self.naive_duration,
            self.delta_duration,
            self.bandwidth_saved_pct,
            self.speedup_factor,
        )
    }
}

pub async fn naive_full_reupload(
    path: &Path,
) -> Result<(Duration, u64), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let mut file = tokio::fs::File::open(path).await?;
    let mut total_bytes = 0u64;
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        total_bytes += n as u64;
    }

    let duration = start.elapsed();
    Ok((duration, total_bytes))
}

pub fn delta_sync_after_trim(
    tree: &mut KvDb<Position, Unlocked>,
    root_before: RootId,
    root_after: RootId,
) -> Result<(Duration, u64, usize), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let diffs = skein_merge::diff(tree, root_before, root_after)?;
    let duration = start.elapsed();

    let mut transferred_bytes = 0u64;
    let touched_chunks = diffs.len();

    for d in &diffs {
        if d.is_insert() || d.is_modify() {
            if let Ok(entry) = tree.open_root(root_after)?.get::<ChunkEntry>(&d.position) {
                transferred_bytes += entry.byte_size;
            } else {
                transferred_bytes += 32;
            }
        }
        transferred_bytes += 64;
    }

    if transferred_bytes == 0 && touched_chunks > 0 {
        transferred_bytes = touched_chunks as u64 * 64;
    }

    Ok((duration, transferred_bytes, touched_chunks))
}

pub async fn run_comparison_benchmark(
    video_path: &Path,
    chunk_size_bytes: usize,
) -> Result<BenchmarkReport, Box<dyn std::error::Error>> {
    let video_name = video_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("video.mp4")
        .to_string();

    let video_size_bytes = tokio::fs::metadata(video_path).await?.len();

    let (naive_duration, naive_bytes) = naive_full_reupload(video_path).await?;

    let temp = tempfile::tempdir()?;
    let fs_store = iroh_blobs::store::fs::FsStore::load(temp.path())
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let local_store = LocalChunkStore::from_store(fs_store);

    let db_path = temp.path().join("bench_index.db");
    let mut kv = KvDb::<Position, Unlocked>::open(db_path.to_str().unwrap_or_default())?;
    let index = ChunkIndex::open(kv.clone());

    let initial_root = index.current_root();
    let (master_root, chunks) = segment_video_file(
        video_path,
        &local_store,
        &index,
        initial_root,
        chunk_size_bytes,
    )
    .await?;
    let total_chunks = chunks.len();

    let branch = index.branch(master_root)?;

    let first_chunk = &chunks[0].1;
    let trimmed_entry = ChunkEntry::new(
        first_chunk.hash,
        first_chunk.duration_ms / 2,
        first_chunk.codec.clone(),
        first_chunk.source_offset + 100,
        first_chunk.is_keyframe,
        first_chunk.keyframe_offset_ms,
        first_chunk.byte_size,
    );

    let modified_root = index.insert(branch.root, chunks[0].0, trimmed_entry)?;

    let (delta_duration, delta_bytes, touched_chunks) =
        delta_sync_after_trim(&mut kv, master_root, modified_root)?;

    let bandwidth_saved_pct = if naive_bytes > 0 {
        ((naive_bytes.saturating_sub(delta_bytes)) as f64 / naive_bytes as f64) * 100.0
    } else {
        0.0
    };

    let speedup_factor = if delta_duration.as_nanos() > 0 {
        naive_duration.as_nanos() as f64 / delta_duration.as_nanos() as f64
    } else {
        1.0
    };

    Ok(BenchmarkReport {
        video_name,
        video_size_bytes,
        naive_duration,
        naive_bytes_transferred: naive_bytes,
        delta_duration,
        delta_bytes_transferred: delta_bytes,
        total_chunks,
        touched_chunks,
        bandwidth_saved_pct,
        speedup_factor,
    })
}
