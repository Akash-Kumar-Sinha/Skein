use iroh_blobs::Hash;
use kvdb::{KvDb, Unlocked};
use serde::{Deserialize, Serialize};
use skein_edl::{EditOp, replay};
use skein_merge::{apply_merge_resolution, resolve_lww, three_way_merge};
use skein_store::{ChunkEntry, ChunkIndex, Position, RootId};
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

use crate::error::CliError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliState {
    pub branches: HashMap<String, RootId>,
    pub parent_branches: HashMap<String, String>,
    pub branch_ops: HashMap<String, Vec<EditOp>>,
    pub base_ops: Vec<EditOp>,
    pub clock: u64,
}

impl Default for CliState {
    fn default() -> Self {
        let clip1 = Uuid::parse_str("11111111-1111-1111-1111-111111111111")
            .unwrap_or_else(|_| Uuid::new_v4());
        let clip2 = Uuid::parse_str("22222222-2222-2222-2222-222222222222")
            .unwrap_or_else(|_| Uuid::new_v4());
        let hash1 = Hash::from([10u8; 32]);
        let hash2 = Hash::from([20u8; 32]);

        let base_ops = vec![
            EditOp::Insert {
                op_id: clip1,
                chunk_hash: hash1,
                position: 0,
                clock: 1,
            },
            EditOp::Insert {
                op_id: clip2,
                chunk_hash: hash2,
                position: 1000,
                clock: 2,
            },
        ];

        let mut branches = HashMap::new();
        branches.insert("master".to_string(), 0);

        Self {
            branches,
            parent_branches: HashMap::new(),
            branch_ops: HashMap::new(),
            base_ops,
            clock: 3,
        }
    }
}

impl CliState {
    pub fn load(path: &Path) -> Self {
        std::fs::read(path)
            .ok()
            .and_then(|data| serde_json::from_slice(&data).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, path: &Path) -> Result<(), CliError> {
        let data = serde_json::to_vec_pretty(self)
            .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
        std::fs::write(path, data)?;
        Ok(())
    }
}

pub fn execute_branch(
    state_file: &Path,
    db_path: &Path,
    from: &str,
    name: &str,
) -> Result<(), CliError> {
    let mut state = CliState::load(state_file);
    let kv = KvDb::<Position, Unlocked>::open(db_path.to_str().unwrap_or_default())
        .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
    let index = ChunkIndex::open(kv);

    let from_root = state
        .branches
        .get(from)
        .copied()
        .unwrap_or_else(|| index.current_root());

    let branch = index
        .branch(from_root)
        .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;

    state.branches.insert(name.to_string(), branch.root);
    state
        .parent_branches
        .insert(name.to_string(), from.to_string());
    state.branch_ops.entry(name.to_string()).or_default();
    state.save(state_file)?;

    println!(
        "Created branch '{}' from '{}' (Root: {})",
        name, from, branch.root
    );
    Ok(())
}

pub fn execute_edit_trim(
    state_file: &Path,
    branch_name: &str,
    target: Uuid,
    new_in: u64,
    new_out: u64,
) -> Result<(), CliError> {
    let mut state = CliState::load(state_file);
    state.clock += 1;

    let op = EditOp::Trim {
        op_id: Uuid::new_v4(),
        target,
        new_in,
        new_out,
        clock: state.clock,
    };

    state
        .branch_ops
        .entry(branch_name.to_string())
        .or_default()
        .push(op);

    state.save(state_file)?;
    println!(
        "Recorded trim edit on clip {} for branch '{}' (in: {}, out: {}, clock: {})",
        target, branch_name, new_in, new_out, state.clock
    );
    Ok(())
}

pub fn execute_sync(state_file: &Path, _with_peer: Option<&str>) -> Result<(), CliError> {
    let mut state = CliState::load(state_file);

    let mut all_outcomes = Vec::new();
    let branch_names: Vec<String> = state.branch_ops.keys().cloned().collect();

    for i in 0..branch_names.len() {
        for j in (i + 1)..branch_names.len() {
            let ops_a = state
                .branch_ops
                .get(&branch_names[i])
                .cloned()
                .unwrap_or_default();
            let ops_b = state
                .branch_ops
                .get(&branch_names[j])
                .cloned()
                .unwrap_or_default();

            let outcomes = three_way_merge(0, &ops_a, &ops_b);
            all_outcomes.extend(outcomes);
        }
    }

    let resolved = apply_merge_resolution(&all_outcomes, resolve_lww);
    for op in resolved {
        if !state.base_ops.contains(&op) {
            state.base_ops.push(op);
        }
    }

    for name in &branch_names {
        state.branch_ops.insert(name.clone(), Vec::new());
    }

    state.save(state_file)?;
    println!("Sync complete. All concurrent branch edits merged deterministically.");
    Ok(())
}

pub fn execute_root_hash(
    state_file: &Path,
    db_path: &Path,
    branch_name: &str,
) -> Result<(), CliError> {
    let state = CliState::load(state_file);
    let kv = KvDb::<Position, Unlocked>::open(db_path.to_str().unwrap_or_default())
        .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
    let index = ChunkIndex::open(kv);

    let mut ops = state.base_ops.clone();
    if let Some(branch_ops) = state.branch_ops.get(branch_name) {
        ops.extend(branch_ops.clone());
    }

    let timeline = replay(&ops);

    let mut current_root = index.current_root();
    for clip in timeline.sorted_by_position() {
        let entry = ChunkEntry::new(
            clip.chunk_hash,
            1000,
            "h264",
            clip.position,
            true,
            0,
            64 * 1024,
        );
        current_root = index
            .insert(current_root, clip.position, entry)
            .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
    }

    let hash = index
        .root_hash(current_root)
        .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;

    println!("{}", hex::encode(hash));
    Ok(())
}
