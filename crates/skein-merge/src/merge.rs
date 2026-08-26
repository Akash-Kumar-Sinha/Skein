use skein_edl::{EditOp, OpId};
use skein_store::RootId;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MergeOutcome {
    Clean(EditOp),
    Conflict {
        op_id: OpId,
        ours: EditOp,
        theirs: EditOp,
    },
}

impl MergeOutcome {
    pub fn is_clean(&self) -> bool {
        matches!(self, MergeOutcome::Clean(_))
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, MergeOutcome::Conflict { .. })
    }
}

pub fn three_way_merge(_base: RootId, ours: &[EditOp], theirs: &[EditOp]) -> Vec<MergeOutcome> {
    let mut outcomes = Vec::new();
    let mut handled_their_targets = HashSet::new();

    for our_op in ours {
        match find_conflicting(our_op, theirs) {
            Some(their_op) => {
                handled_their_targets.insert(their_op.target_id());
                if our_op == their_op {
                    outcomes.push(MergeOutcome::Clean(our_op.clone()));
                } else {
                    outcomes.push(MergeOutcome::Conflict {
                        op_id: our_op.target_id(),
                        ours: our_op.clone(),
                        theirs: their_op.clone(),
                    });
                }
            }
            None => {
                outcomes.push(MergeOutcome::Clean(our_op.clone()));
            }
        }
    }

    for their_op in theirs {
        if !handled_their_targets.contains(&their_op.target_id())
            && !ours.iter().any(|o| o.target_id() == their_op.target_id())
        {
            outcomes.push(MergeOutcome::Clean(their_op.clone()));
        }
    }

    outcomes
}

pub fn find_conflicting<'a>(op: &EditOp, others: &'a [EditOp]) -> Option<&'a EditOp> {
    others
        .iter()
        .find(|other| other.target_id() == op.target_id())
}

pub fn resolve_lww(ours: EditOp, theirs: EditOp) -> EditOp {
    if ours.clock() > theirs.clock() {
        ours
    } else if theirs.clock() > ours.clock() {
        theirs
    } else if ours.op_id() >= theirs.op_id() {
        ours
    } else {
        theirs
    }
}

pub fn apply_merge_resolution(
    outcomes: &[MergeOutcome],
    conflict_resolver: impl Fn(EditOp, EditOp) -> EditOp,
) -> Vec<EditOp> {
    let mut merged = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        match outcome {
            MergeOutcome::Clean(op) => merged.push(op.clone()),
            MergeOutcome::Conflict { ours, theirs, .. } => {
                let resolved = conflict_resolver(ours.clone(), theirs.clone());
                merged.push(resolved);
            }
        }
    }
    merged
}
