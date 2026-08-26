use crate::edl::{EditOp, OpId, Timeline, replay};

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PersonalTree {
    entries: Vec<(EditOp, bool)>,
}

impl PersonalTree {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn record(&mut self, op: EditOp) {
        self.entries.push((op, false));
    }

    pub fn mark_merged(&mut self, op_id: OpId) {
        if let Some(entry) = self.entries.iter_mut().find(|(op, _)| op.op_id() == op_id) {
            entry.1 = true;
        }
    }

    pub fn unmerged(&self) -> impl Iterator<Item = &EditOp> {
        self.entries
            .iter()
            .filter(|(_, merged)| !merged)
            .map(|(op, _)| op)
    }

    pub fn merged(&self) -> impl Iterator<Item = &EditOp> {
        self.entries
            .iter()
            .filter(|(_, merged)| *merged)
            .map(|(op, _)| op)
    }

    pub fn all(&self) -> impl Iterator<Item = &(EditOp, bool)> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub fn derive_local_tree(merged_ops: &[EditOp], personal: &PersonalTree) -> Timeline {
    let mut all: Vec<EditOp> = merged_ops.to_vec();
    all.extend(personal.unmerged().cloned());
    replay(&all)
}
