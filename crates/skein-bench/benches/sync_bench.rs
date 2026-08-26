use criterion::{Criterion, criterion_group, criterion_main};
use kvdb::{KvDb, Unlocked};
use skein_bench::{delta_sync_after_trim, naive_full_reupload};
use skein_store::{ChunkEntry, ChunkIndex, LocalChunkStore, Position, segment_video_file};
use std::path::Path;

fn bench_sync_after_trim(c: &mut Criterion) {
    let video_path =
        Path::new("/home/aks/vs_stuff/Development/rust_devs/skein/video/spiderman.mp4");
    if !video_path.exists() {
        return;
    }

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let (mut kv, master_root, modified_root) = rt.block_on(async {
        let temp = tempfile::tempdir().expect("tempdir");
        let fs_store = iroh_blobs::store::fs::FsStore::load(temp.path())
            .await
            .expect("fs_store");
        let local_store = LocalChunkStore::from_store(fs_store);

        let db_path = temp.path().join("bench_criterion.db");
        let kv =
            KvDb::<Position, Unlocked>::open(db_path.to_str().unwrap_or_default()).expect("kvdb");
        let index = ChunkIndex::open(kv.clone());

        let (master_root, chunks) = segment_video_file(
            video_path,
            &local_store,
            &index,
            index.current_root(),
            1024 * 1024,
        )
        .await
        .expect("segment");

        let branch = index.branch(master_root).expect("branch");
        let first_chunk = &chunks[0].1;
        let trimmed_entry = ChunkEntry::new(
            first_chunk.hash,
            first_chunk.duration_ms / 2,
            first_chunk.codec.clone(),
            first_chunk.source_offset + 50,
            first_chunk.is_keyframe,
            first_chunk.keyframe_offset_ms,
            first_chunk.byte_size,
        );
        let modified_root = index
            .insert(branch.root, chunks[0].0, trimmed_entry)
            .expect("insert");

        (kv, master_root, modified_root)
    });

    let mut group = c.benchmark_group("video_sync_comparison");

    group.bench_function("naive_full_reupload", |b| {
        b.to_async(&rt).iter(|| async {
            naive_full_reupload(video_path).await.expect("naive upload");
        });
    });

    group.bench_function("delta_sync_after_trim", |b| {
        b.iter(|| {
            delta_sync_after_trim(&mut kv, master_root, modified_root).expect("delta sync");
        });
    });

    group.finish();
}

criterion_group!(benches, bench_sync_after_trim);
criterion_main!(benches);
