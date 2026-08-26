use kvdb::{DbError, KvDb, PageId, Unlocked, Value};
use skein_store::{Position, RootId};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiffEntry {
    pub position: Position,
    pub old_hash: Option<[u8; 32]>,
    pub new_hash: Option<[u8; 32]>,
}

impl DiffEntry {
    pub fn is_insert(&self) -> bool {
        self.old_hash.is_none() && self.new_hash.is_some()
    }

    pub fn is_delete(&self) -> bool {
        self.old_hash.is_some() && self.new_hash.is_none()
    }

    pub fn is_modify(&self) -> bool {
        self.old_hash.is_some() && self.new_hash.is_some()
    }
}

pub fn diff(
    tree: &mut KvDb<Position, Unlocked>,
    root_a: RootId,
    root_b: RootId,
) -> Result<Vec<DiffEntry>, DbError> {
    let mut handle_a = tree.open_root(root_a)?;
    let mut handle_b = tree.open_root(root_b)?;

    let hash_a = handle_a.root_hash()?;
    let hash_b = handle_b.root_hash()?;

    // CRITICAL: O(1) Short-circuit when subtree Merkle hashes match
    if hash_a == hash_b {
        return Ok(Vec::new());
    }

    let page_a = handle_a.current_root_page_id()?;
    let page_b = handle_b.current_root_page_id()?;

    let mut out = Vec::new();
    diff_subtrees(tree, page_a, page_b, &mut handle_a, &mut handle_b, &mut out)?;
    Ok(out)
}

fn diff_subtrees(
    tree: &mut KvDb<Position, Unlocked>,
    node_a: PageId,
    node_b: PageId,
    handle_a: &mut KvDb<Position, Unlocked>,
    handle_b: &mut KvDb<Position, Unlocked>,
    out: &mut Vec<DiffEntry>,
) -> Result<(), DbError> {
    let hash_a = tree.hash_at(node_a)?;
    let hash_b = tree.hash_at(node_b)?;

    if hash_a == hash_b {
        return Ok(());
    }

    let children_a = tree.children_of(node_a)?;
    let children_b = tree.children_of(node_b)?;

    if !children_a.is_empty() && !children_b.is_empty() {
        let same_keys = children_a.len() == children_b.len()
            && children_a.iter().zip(&children_b).all(|(a, b)| a.0 == b.0);

        if same_keys {
            for (a, b) in children_a.iter().zip(&children_b) {
                if a.2 != b.2 {
                    diff_subtrees(tree, a.1, b.1, handle_a, handle_b, out)?;
                }
            }
            return Ok(());
        }
    }

    let entries_a = handle_a.range()?;
    let entries_b = handle_b.range()?;
    diff_entries(&entries_a, &entries_b, out);
    Ok(())
}

fn diff_entries(
    entries_a: &[(Position, Value)],
    entries_b: &[(Position, Value)],
    out: &mut Vec<DiffEntry>,
) {
    let mut i = 0;
    let mut j = 0;

    while i < entries_a.len() && j < entries_b.len() {
        let (pos_a, val_a) = &entries_a[i];
        let (pos_b, val_b) = &entries_b[j];

        if pos_a < pos_b {
            out.push(DiffEntry {
                position: *pos_a,
                old_hash: Some(hash_value(val_a)),
                new_hash: None,
            });
            i += 1;
        } else if pos_a > pos_b {
            out.push(DiffEntry {
                position: *pos_b,
                old_hash: None,
                new_hash: Some(hash_value(val_b)),
            });
            j += 1;
        } else {
            let h_a = hash_value(val_a);
            let h_b = hash_value(val_b);
            if h_a != h_b {
                out.push(DiffEntry {
                    position: *pos_a,
                    old_hash: Some(h_a),
                    new_hash: Some(h_b),
                });
            }
            i += 1;
            j += 1;
        }
    }

    while i < entries_a.len() {
        let (pos_a, val_a) = &entries_a[i];
        out.push(DiffEntry {
            position: *pos_a,
            old_hash: Some(hash_value(val_a)),
            new_hash: None,
        });
        i += 1;
    }

    while j < entries_b.len() {
        let (pos_b, val_b) = &entries_b[j];
        out.push(DiffEntry {
            position: *pos_b,
            old_hash: None,
            new_hash: Some(hash_value(val_b)),
        });
        j += 1;
    }
}

fn hash_value(val: &Value) -> [u8; 32] {
    match val {
        Value::Bytes(bytes) => *blake3::hash(bytes).as_bytes(),
        _ => {
            let serialized = postcard::to_allocvec(val).unwrap_or_default();
            *blake3::hash(&serialized).as_bytes()
        }
    }
}
