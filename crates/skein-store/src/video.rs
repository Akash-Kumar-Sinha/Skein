use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::chunk_index::{ChunkEntry, ChunkIndex, Position, RootId};
use crate::chunk_store::ChunkStore;
use crate::error::StoreError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoMetadata {
    pub duration_ms: u64,
    pub codec: String,
    pub file_size: u64,
    pub timescale: u32,
}

pub async fn probe_mp4_metadata(path: &Path) -> Result<VideoMetadata, StoreError> {
    let mut file = tokio::fs::File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let mut duration_ms = 0;
    let mut timescale = 1000;
    let mut codec = "h264".to_string();

    let mut buf = [0u8; 8];
    while file.read_exact(&mut buf).await.is_ok() {
        let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64;
        let fourcc = &buf[4..8];

        if fourcc == b"moov" {
            let box_payload_size = size.saturating_sub(8);
            let mut moov_data = vec![0u8; box_payload_size.min(2 * 1024 * 1024) as usize];
            if file.read_exact(&mut moov_data).await.is_ok() {
                if let Some((d, ts)) = parse_mvhd(&moov_data) {
                    duration_ms = d;
                    timescale = ts;
                }
                if let Some(c) = find_codec_in_moov(&moov_data) {
                    codec = c;
                }
            }
            break;
        } else if size > 8 {
            if file
                .seek(std::io::SeekFrom::Current((size - 8) as i64))
                .await
                .is_err()
            {
                break;
            }
        } else {
            break;
        }
    }

    if duration_ms == 0 {
        duration_ms = (file_size / (200 * 1024)).max(1) * 1000;
    }

    Ok(VideoMetadata {
        duration_ms,
        codec,
        file_size,
        timescale,
    })
}

fn parse_mvhd(data: &[u8]) -> Option<(u64, u32)> {
    let mvhd_tag = b"mvhd";
    let pos = data.windows(4).position(|w| w == mvhd_tag)?;
    let start = pos + 4;
    if start + 20 > data.len() {
        return None;
    }
    let version = data[start];
    if version == 0 {
        if start + 20 <= data.len() {
            let ts = u32::from_be_bytes([
                data[start + 12],
                data[start + 13],
                data[start + 14],
                data[start + 15],
            ]);
            let dur = u32::from_be_bytes([
                data[start + 16],
                data[start + 17],
                data[start + 18],
                data[start + 19],
            ]) as u64;
            if ts > 0 {
                return Some(((dur * 1000) / ts as u64, ts));
            }
        }
    } else if version == 1 && start + 32 <= data.len() {
        let ts = u32::from_be_bytes([
            data[start + 20],
            data[start + 21],
            data[start + 22],
            data[start + 23],
        ]);
        let dur = u64::from_be_bytes([
            data[start + 24],
            data[start + 25],
            data[start + 26],
            data[start + 27],
            data[start + 28],
            data[start + 29],
            data[start + 30],
            data[start + 31],
        ]);
        if ts > 0 {
            return Some(((dur * 1000) / ts as u64, ts));
        }
    }
    None
}

fn find_codec_in_moov(data: &[u8]) -> Option<String> {
    if data.windows(4).any(|w| w == b"avc1" || w == b"avc3") {
        Some("h264".to_string())
    } else if data.windows(4).any(|w| w == b"hvc1" || w == b"hev1") {
        Some("hevc".to_string())
    } else if data.windows(4).any(|w| w == b"av01") {
        Some("av1".to_string())
    } else if data.windows(4).any(|w| w == b"vp09") {
        Some("vp9".to_string())
    } else {
        None
    }
}

pub async fn chunk_video_file(
    path: &Path,
    store: &dyn ChunkStore,
    index: &ChunkIndex,
    root: RootId,
) -> Result<RootId, StoreError> {
    let meta = probe_mp4_metadata(path)
        .await
        .unwrap_or_else(|_| VideoMetadata {
            duration_ms: 10_000,
            codec: "h264".to_string(),
            file_size: 0,
            timescale: 1000,
        });

    let hash = store.add(path.to_path_buf()).await?;
    let entry = ChunkEntry::new(
        hash,
        meta.duration_ms,
        meta.codec,
        0,
        true,
        0,
        meta.file_size,
    );

    index.insert(root, 0, entry)
}

pub async fn segment_video_file(
    path: &Path,
    store: &dyn ChunkStore,
    index: &ChunkIndex,
    root: RootId,
    chunk_size_bytes: usize,
) -> Result<(RootId, Vec<(Position, ChunkEntry)>), StoreError> {
    let meta = probe_mp4_metadata(path)
        .await
        .unwrap_or_else(|_| VideoMetadata {
            duration_ms: 10_000,
            codec: "h264".to_string(),
            file_size: 0,
            timescale: 1000,
        });

    let mut file = tokio::fs::File::open(path).await?;
    let total_len = file.metadata().await?.len();

    let num_chunks = (total_len as usize).div_ceil(chunk_size_bytes).max(1);
    let duration_per_chunk_ms = (meta.duration_ms / num_chunks as u64).max(1);

    let temp_dir = tempfile::tempdir()?;
    let mut current_root = root;
    let mut chunks = Vec::with_capacity(num_chunks);

    let mut offset = 0u64;
    let mut chunk_idx = 0;

    let mut buffer = vec![0u8; chunk_size_bytes];
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }

        let chunk_file_path = temp_dir.path().join(format!("chunk_{chunk_idx}.bin"));
        tokio::fs::write(&chunk_file_path, &buffer[..n]).await?;

        let hash = store.add(chunk_file_path).await?;
        let pos = chunk_idx as u64 * duration_per_chunk_ms;
        let is_keyframe = chunk_idx == 0;

        let entry = ChunkEntry::new(
            hash,
            duration_per_chunk_ms,
            meta.codec.clone(),
            offset,
            is_keyframe,
            0,
            n as u64,
        );

        current_root = index.insert(current_root, pos, entry.clone())?;
        chunks.push((pos, entry));

        offset += n as u64;
        chunk_idx += 1;
    }

    Ok((current_root, chunks))
}
