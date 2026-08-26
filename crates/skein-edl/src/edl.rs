use iroh_blobs::Hash;
use uuid::Uuid;

pub type OpId = Uuid;
pub type Position = u64;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EditOp {
    Insert {
        op_id: OpId,
        chunk_hash: Hash,
        position: Position,
        clock: u64,
    },
    Trim {
        op_id: OpId,
        target: OpId,
        new_in: Position,
        new_out: Position,
        clock: u64,
    },
    Delete {
        op_id: OpId,
        target: OpId,
        clock: u64,
    },
    Reorder {
        op_id: OpId,
        target: OpId,
        new_position: Position,
        clock: u64,
    },
}

impl EditOp {
    pub fn op_id(&self) -> OpId {
        match self {
            EditOp::Insert { op_id, .. } => *op_id,
            EditOp::Trim { op_id, .. } => *op_id,
            EditOp::Delete { op_id, .. } => *op_id,
            EditOp::Reorder { op_id, .. } => *op_id,
        }
    }

    pub fn target_id(&self) -> OpId {
        match self {
            EditOp::Insert { op_id, .. } => *op_id,
            EditOp::Trim { target, .. } => *target,
            EditOp::Delete { target, .. } => *target,
            EditOp::Reorder { target, .. } => *target,
        }
    }

    pub fn clock(&self) -> u64 {
        match self {
            EditOp::Insert { clock, .. } => *clock,
            EditOp::Trim { clock, .. } => *clock,
            EditOp::Delete { clock, .. } => *clock,
            EditOp::Reorder { clock, .. } => *clock,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Clip {
    pub op_id: OpId,
    pub chunk_hash: Hash,
    pub position: Position,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Timeline {
    pub clips: Vec<Clip>,
}

impl Timeline {
    pub fn len(&self) -> usize {
        self.clips.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clips.is_empty()
    }

    pub fn find(&self, op_id: OpId) -> Option<&Clip> {
        self.clips.iter().find(|c| c.op_id == op_id)
    }

    pub fn sorted_by_position(&self) -> Vec<Clip> {
        let mut sorted = self.clips.clone();
        sorted.sort_by_key(|c| (c.position, c.op_id));
        sorted
    }
}

pub fn replay(ops: &[EditOp]) -> Timeline {
    let mut sorted_ops: Vec<EditOp> = ops.to_vec();
    sorted_ops.sort_by_key(|op| (op.clock(), op.op_id()));

    let mut timeline = Timeline::default();
    for op in &sorted_ops {
        apply_one(&mut timeline, op);
    }
    timeline
}

fn apply_one(timeline: &mut Timeline, op: &EditOp) {
    match op {
        EditOp::Insert {
            op_id,
            chunk_hash,
            position,
            ..
        } => {
            timeline.clips.push(Clip {
                op_id: *op_id,
                chunk_hash: *chunk_hash,
                position: *position,
            });
        }
        EditOp::Trim { target, new_in, .. } => {
            if let Some(clip) = timeline.clips.iter_mut().find(|c| c.op_id == *target) {
                clip.position = *new_in;
            }
        }
        EditOp::Delete { target, .. } => {
            timeline.clips.retain(|c| c.op_id != *target);
        }
        EditOp::Reorder {
            target,
            new_position,
            ..
        } => {
            if let Some(clip) = timeline.clips.iter_mut().find(|c| c.op_id == *target) {
                clip.position = *new_position;
            }
        }
    }
}
