mod diff;
mod merge;

pub use diff::{DiffEntry, diff};
pub use merge::{
    MergeOutcome, apply_merge_resolution, find_conflicting, resolve_lww, three_way_merge,
};
