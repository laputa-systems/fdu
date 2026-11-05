use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModifiedTime(i64);

impl ModifiedTime {
    pub const fn from_unix_seconds(seconds: i64) -> Option<Self> {
        if seconds == i64::MIN { None } else { Some(Self(seconds)) }
    }

    pub const fn unix_seconds(self) -> Option<i64> {
        if self.0 == i64::MIN { None } else { Some(self.0) }
    }
}

impl Default for ModifiedTime {
    fn default() -> Self {
        Self(i64::MIN)
    }
}

/// An index into the tree, stored plus one so that `Option<NodeId>` is four bytes: every record
/// holds four of them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NodeId(std::num::NonZeroU32);

impl NodeId {
    pub fn index(self) -> usize {
        self.0.get() as usize - 1
    }

    pub fn from_index(index: usize) -> Option<Self> {
        let stored = u32::try_from(index).ok()?.checked_add(1)?;
        std::num::NonZeroU32::new(stored).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DirectoryToken(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryType {
    Root,
    Directory,
    RegularFile,
    Symlink,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExclusionReason {
    MountBoundary,
    UnsupportedAlias,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
    Scanning,
    Complete,
    Incomplete,
    Excluded(ExclusionReason),
    Stale,
    Tombstone,
}

/// A node state that can be assigned without changing tree totals. Tombstones
/// are created only by `Tree::tombstone_subtree`, which also updates totals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeStatus {
    Scanning,
    Complete,
    Incomplete,
    Excluded(ExclusionReason),
    Stale,
}

impl From<NodeStatus> for NodeState {
    fn from(status: NodeStatus) -> Self {
        match status {
            NodeStatus::Scanning => Self::Scanning,
            NodeStatus::Complete => Self::Complete,
            NodeStatus::Incomplete => Self::Incomplete,
            NodeStatus::Excluded(reason) => Self::Excluded(reason),
            NodeStatus::Stale => Self::Stale,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScanEntry {
    /// The directory this entry was found in.
    pub parent: DirectoryToken,
    pub name: Range<u32>,
    pub directory_token: Option<DirectoryToken>,
    pub entry_type: EntryType,
    pub identity: FileIdentity,
    pub modified_time: ModifiedTime,
    pub link_count: u64,
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
    pub state: NodeStatus,
}

/// Entries found by the scanner, in discovery order. Entries of different directories may share
/// a batch; each carries its parent, and a directory's own entry always precedes the entries
/// found inside it.
#[derive(Debug)]
pub struct EntryBatch {
    pub entries: Vec<ScanEntry>,
    pub names: Vec<u8>,
}

impl EntryBatch {
    pub fn with_capacity(entry_capacity: usize, name_capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(entry_capacity),
            names: Vec::with_capacity(name_capacity),
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.names.clear();
    }
}

#[derive(Debug)]
pub enum ScanEvent {
    Started {
        root_name: Vec<u8>,
        identity: FileIdentity,
        link_count: u64,
    },
    Entries(EntryBatch),
    /// Every entry directly in the root has been sent. Nothing the scan finds later is directly
    /// in the root, so entries created there afterwards are never indexed.
    RootListed,
    DirectoryFinished {
        directory: DirectoryToken,
        complete: bool,
    },
    /// Completion updates grouped to reduce event traffic on scans with many directories.
    DirectoriesFinished(Vec<(DirectoryToken, bool)>),
    DirectoryExcluded {
        directory: DirectoryToken,
        reason: ExclusionReason,
    },
    DirectoryFailed {
        directory: DirectoryToken,
        message: String,
    },
    Failed {
        message: String,
    },
    Cancelled,
    Finished,
}

#[derive(Clone, Debug)]
pub struct NodeRecord {
    parent: Option<NodeId>,
    first_child: Option<NodeId>,
    last_child: Option<NodeId>,
    next_sibling: Option<NodeId>,
    name: Range<u32>,
    pub entry_type: EntryType,
    pub identity: FileIdentity,
    modified_time: ModifiedTime,
    pub link_count: u64,
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
    pub state: NodeState,
}

impl NodeRecord {
    pub fn parent(&self) -> Option<NodeId> {
        self.parent
    }

    pub fn first_child(&self) -> Option<NodeId> {
        self.first_child
    }

    pub fn next_sibling(&self) -> Option<NodeId> {
        self.next_sibling
    }

    pub fn name_range(&self) -> Range<u32> {
        self.name.clone()
    }

    pub fn modified_time(&self) -> ModifiedTime {
        self.modified_time
    }
}

pub struct Tree {
    nodes: Vec<NodeRecord>,
    names: Vec<u8>,
    root: NodeId,
}

impl Tree {
    pub fn new(
        root_name: &[u8],
        identity: FileIdentity,
        link_count: u64,
    ) -> Option<Self> {
        let root = NodeId::from_index(0).expect("index zero fits");
        let name_end = u32::try_from(root_name.len()).ok()?;
        let mut names = Vec::with_capacity(root_name.len());
        names.extend_from_slice(root_name);
        Some(Self {
            nodes: vec![NodeRecord {
                parent: None,
                first_child: None,
                last_child: None,
                next_sibling: None,
                name: 0..name_end,
                entry_type: EntryType::Root,
                identity,
                modified_time: ModifiedTime::default(),
                link_count,
                apparent_bytes: 0,
                allocated_bytes: 0,
                state: NodeState::Scanning,
            }],
            names,
            root,
        })
    }

    pub fn root(&self) -> NodeId {
        self.root
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Gives back the capacity that growth left unused, once the tree stops growing.
    pub fn shrink_to_fit(&mut self) {
        self.nodes.shrink_to_fit();
        self.names.shrink_to_fit();
    }

    pub fn retained_arena_bytes(&self) -> usize {
        self.nodes
            .capacity()
            .saturating_mul(std::mem::size_of::<NodeRecord>())
            .saturating_add(self.names.capacity())
    }

    pub fn record(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(id.index())
    }

    pub fn name(&self, id: NodeId) -> Option<&[u8]> {
        let range = self.record(id)?.name.clone();
        self.names.get(range.start as usize..range.end as usize)
    }

    pub fn children(&self, id: NodeId) -> Children<'_> {
        Children {
            tree: self,
            next: self.record(id).and_then(NodeRecord::first_child),
        }
    }

    // One record holds every field of an entry, so the constructor takes them all.
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        parent: NodeId,
        name: &[u8],
        entry_type: EntryType,
        identity: FileIdentity,
        link_count: u64,
        apparent_bytes: u64,
        allocated_bytes: u64,
        state: NodeStatus,
    ) -> Option<NodeId> {
        let index = self.nodes.len();
        let id = NodeId::from_index(index)?;
        let name_start = u32::try_from(self.names.len()).ok()?;
        let name_end = name_start.checked_add(u32::try_from(name.len()).ok()?)?;
        self.record(parent)?;
        self.names.extend_from_slice(name);
        self.nodes.push(NodeRecord {
            parent: Some(parent),
            first_child: None,
            last_child: None,
            next_sibling: None,
            name: name_start..name_end,
            entry_type,
            identity,
            modified_time: ModifiedTime::default(),
            link_count,
            apparent_bytes,
            allocated_bytes,
            state: state.into(),
        });
        if let Some(last) = self.nodes[parent.index()].last_child {
            self.nodes[last.index()].next_sibling = Some(id);
        } else {
            self.nodes[parent.index()].first_child = Some(id);
        }
        self.nodes[parent.index()].last_child = Some(id);
        Some(id)
    }

    pub fn set_state(&mut self, id: NodeId, state: NodeStatus) -> bool {
        let Some(record) = self.nodes.get_mut(id.index()) else {
            return false;
        };
        record.state = state.into();
        true
    }

    pub fn set_modified_time(&mut self, id: NodeId, modified_time: ModifiedTime) -> bool {
        let Some(record) = self.nodes.get_mut(id.index()) else {
            return false;
        };
        record.modified_time = modified_time;
        true
    }

    /// Adds bytes to `id` and every ancestor, or to none of them: when any total would overflow,
    /// the lineage is marked incomplete instead and false is returned.
    pub fn add_to_ancestors(&mut self, id: NodeId, apparent: u64, allocated: u64) -> bool {
        let mut fits = true;
        let mut current = Some(id);
        while let Some(node) = current {
            let Some(record) = self.nodes.get(node.index()) else {
                return false;
            };
            if record.apparent_bytes.checked_add(apparent).is_none()
                || record.allocated_bytes.checked_add(allocated).is_none()
            {
                fits = false;
                break;
            }
            current = record.parent;
        }
        let mut current = Some(id);
        while let Some(node) = current {
            let record = &mut self.nodes[node.index()];
            if fits {
                record.apparent_bytes += apparent;
                record.allocated_bytes += allocated;
            } else {
                record.state = NodeState::Incomplete;
            }
            current = record.parent;
        }
        fits
    }

    pub fn mark_incomplete_to_root(&mut self, id: NodeId) -> usize {
        let mut marked = 0;
        let mut current = Some(id);
        while let Some(node) = current {
            let Some(record) = self.nodes.get_mut(node.index()) else {
                break;
            };
            if record.state == NodeState::Scanning || record.state == NodeState::Complete {
                record.state = NodeState::Incomplete;
                marked += 1;
            }
            current = record.parent;
        }
        marked
    }

    pub fn materialize_path(&self, id: NodeId) -> Option<Vec<&[u8]>> {
        let mut names = Vec::new();
        let mut current = Some(id);
        while let Some(node) = current {
            let record = self.record(node)?;
            if node != self.root {
                names.push(self.name(node)?);
            }
            current = record.parent;
        }
        names.reverse();
        Some(names)
    }

    pub fn tombstone_subtree(&mut self, id: NodeId) -> Option<(u64, u64, usize)> {
        if id == self.root {
            return None;
        }
        let (apparent, allocated, parent) = {
            let record = self.record(id)?;
            if record.state == NodeState::Tombstone {
                return Some((0, 0, 0));
            }
            (record.apparent_bytes, record.allocated_bytes, record.parent?)
        };
        let mut current = Some(parent);
        let mut lineage = Vec::new();
        while let Some(node) = current {
            let record = self.nodes.get(node.index())?;
            if record.apparent_bytes < apparent || record.allocated_bytes < allocated {
                return None;
            }
            lineage.push(node);
            current = record.parent;
        }
        let mut child = self.record(id)?.first_child;
        let mut all_children_tombstoned = true;
        while let Some(child_id) = child {
            let child_record = self.record(child_id)?;
            if child_record.state != NodeState::Tombstone {
                all_children_tombstoned = false;
                break;
            }
            child = child_record.next_sibling;
        }

        let mut tombstoned = 0usize;
        let mut current = Some(id);
        while let Some(node) = current {
            let record = self.record(node)?;
            if record.state == NodeState::Tombstone {
                current = next_sibling_after_subtree(self, node, id);
                continue;
            }
            let next = if node == id && all_children_tombstoned {
                None
            } else if let Some(child) = record.first_child {
                Some(child)
            } else {
                next_sibling_after_subtree(self, node, id)
            };
            self.nodes.get_mut(node.index())?.state = NodeState::Tombstone;
            tombstoned = tombstoned.saturating_add(1);
            current = next;
        }
        for node in lineage {
            let record = &mut self.nodes[node.index()];
            record.apparent_bytes -= apparent;
            record.allocated_bytes -= allocated;
        }
        Some((apparent, allocated, tombstoned))
    }

    pub fn mark_stale_to_root(&mut self, id: NodeId) {
        let mut current = Some(id);
        while let Some(node) = current {
            let Some(record) = self.nodes.get_mut(node.index()) else {
                break;
            };
            if record.state != NodeState::Tombstone {
                record.state = NodeState::Stale;
            }
            current = record.parent;
        }
    }

    pub fn mark_scanning_incomplete(&mut self) -> usize {
        let mut changed = 0;
        for record in &mut self.nodes {
            if record.state == NodeState::Scanning {
                record.state = NodeState::Incomplete;
                changed += 1;
            }
        }
        changed
    }
}

fn next_sibling_after_subtree(tree: &Tree, node: NodeId, root: NodeId) -> Option<NodeId> {
    let mut current = node;
    loop {
        if current == root {
            return None;
        }
        let record = tree.record(current)?;
        if let Some(sibling) = record.next_sibling {
            return Some(sibling);
        }
        current = record.parent?;
    }
}

pub struct Children<'a> {
    tree: &'a Tree,
    next: Option<NodeId>,
}

impl<'a> Iterator for Children<'a> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(id) = self.next {
            let record = self.tree.record(id)?;
            self.next = record.next_sibling;
            if record.state != NodeState::Tombstone {
                return Some(id);
            }
        }
        None
    }
}

/// What folding one scanner batch changed, so the browser and `--rmrf` can
/// map the same fold onto their own bookkeeping without reimplementing it.
#[derive(Debug, Default)]
pub struct BatchFoldOutcome {
    pub appended: Vec<NodeId>,
    pub incomplete_entries: usize,
    pub excluded_entries: usize,
    pub marked_incomplete: usize,
    pub unknown_directory: bool,
    pub invalid_range: bool,
    pub exhausted: bool,
    pub overflow: bool,
}

/// Appends one scanner batch and rolls byte totals to ancestors. Entries of
/// different directories may share a batch; each run of one directory's
/// entries is contiguous, and totals are added once per run.
///
/// Both scan consumers fold through this function so their trees match: every
/// appended entry keeps its scanner `modified_time`, and only `Complete`
/// non-directory entries contribute to ancestor totals. `Incomplete` and
/// `Excluded` entries mark the parent incomplete, as do `Scanning` and
/// `Stale` entries, whose totals are not final. A directory the scan will
/// enter is stored as `Scanning` until `finish_scan_directory` settles it, so
/// the tree tells a finished subtree from one still being indexed.
pub fn fold_entry_batch(
    tree: &mut Tree,
    token_nodes: &mut Vec<Option<NodeId>>,
    batch: &EntryBatch,
) -> BatchFoldOutcome {
    struct Run {
        parent: NodeId,
        apparent: u64,
        allocated: u64,
        fits: bool,
        problem: bool,
    }
    let mut outcome = BatchFoldOutcome::default();
    let mut runs: Vec<Run> = Vec::new();
    let mut run_token = None;
    for entry in &batch.entries {
        if run_token != Some(entry.parent) {
            let parent = token_nodes.get(entry.parent.0 as usize).copied().flatten();
            let Some(parent) = parent else {
                outcome.unknown_directory = true;
                run_token = None;
                continue;
            };
            run_token = Some(entry.parent);
            runs.push(Run { parent, apparent: 0, allocated: 0, fits: true, problem: false });
        }
        let run = runs.last_mut().expect("a run exists for the current directory");
        let Some(name) = batch.names.get(entry.name.start as usize..entry.name.end as usize) else {
            outcome.invalid_range = true;
            run.problem = true;
            continue;
        };
        let Some(id) = tree.append(
            run.parent,
            name,
            entry.entry_type,
            entry.identity,
            entry.link_count,
            entry.apparent_bytes,
            entry.allocated_bytes,
            match (entry.state, entry.directory_token) {
                (NodeStatus::Complete, Some(_)) => NodeStatus::Scanning,
                (state, _) => state,
            },
        ) else {
            outcome.exhausted = true;
            run.problem = true;
            break;
        };
        assert!(tree.set_modified_time(id, entry.modified_time));
        outcome.appended.push(id);
        if let Some(token) = entry.directory_token {
            let index = token.0 as usize;
            if token_nodes.len() <= index {
                token_nodes.resize(index + 1, None);
            }
            token_nodes[index] = Some(id);
        }
        match entry.state {
            NodeStatus::Incomplete => {
                outcome.incomplete_entries += 1;
                run.problem = true;
            }
            NodeStatus::Excluded(_) => {
                outcome.excluded_entries += 1;
                run.problem = true;
            }
            NodeStatus::Scanning | NodeStatus::Stale => {
                run.problem = true;
            }
            NodeStatus::Complete => {
                if entry.entry_type != EntryType::Directory {
                    match (
                        run.apparent.checked_add(entry.apparent_bytes),
                        run.allocated.checked_add(entry.allocated_bytes),
                    ) {
                        (Some(apparent), Some(allocated)) => {
                            run.apparent = apparent;
                            run.allocated = allocated;
                        }
                        _ => run.fits = false,
                    }
                }
            }
        }
    }
    if outcome.unknown_directory {
        outcome.marked_incomplete += tree.mark_incomplete_to_root(tree.root());
    }
    for run in &runs {
        if run.problem {
            outcome.marked_incomplete += tree.mark_incomplete_to_root(run.parent);
        }
        // Directories add nothing to their ancestors' totals, so a run of them skips the walk.
        let added = !run.fits
            || (run.apparent == 0 && run.allocated == 0)
            || tree.add_to_ancestors(run.parent, run.apparent, run.allocated);
        if !run.fits || !added {
            outcome.overflow = true;
            outcome.marked_incomplete += tree.mark_incomplete_to_root(run.parent);
        }
    }
    outcome
}

/// Settles one directory's scan state after its entries folded. Returns how
/// many ancestors flipped to `Incomplete`. Directories the scanner excluded
/// stay excluded: neither completion nor failure rewrites them.
pub fn finish_scan_directory(
    tree: &mut Tree,
    token_nodes: &[Option<NodeId>],
    directory: DirectoryToken,
    complete: bool,
) -> usize {
    let Some(node) = token_nodes.get(directory.0 as usize).copied().flatten() else {
        return 0;
    };
    if matches!(
        tree.record(node).map(|record| record.state),
        Some(NodeState::Excluded(_)) | Some(NodeState::Tombstone)
    ) {
        return 0;
    }
    if complete {
        if tree.record(node).map(|record| record.state) == Some(NodeState::Scanning) {
            tree.set_state(node, NodeStatus::Complete);
        }
        0
    } else {
        tree.mark_incomplete_to_root(node)
    }
}

#[cfg(test)]
mod tests {
    use super::{EntryType, FileIdentity, NodeState, NodeStatus, Tree};

    #[test]
    fn names_and_sibling_links_share_the_arena_and_keep_stable_ids() {
        let mut tree = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 2).unwrap();
        let first = tree
            .append(
                tree.root(),
                b"first-\xff",
                EntryType::RegularFile,
                FileIdentity { device: 1, inode: 2 },
                1,
                7,
                512,
                NodeStatus::Complete,
            )
            .unwrap();
        let second = tree
            .append(
                tree.root(),
                b"second",
                EntryType::RegularFile,
                FileIdentity { device: 1, inode: 2 },
                2,
                7,
                512,
                NodeStatus::Complete,
            )
            .unwrap();
        assert_eq!(tree.name(first), Some(b"first-\xff".as_slice()));
        assert_eq!(tree.children(tree.root()).collect::<Vec<_>>(), vec![first, second]);
        assert_eq!(tree.record(first).unwrap().next_sibling(), Some(second));
    }

    #[test]
    fn checked_batch_totals_propagate_atomically_and_tombstones_subtract_once() {
        let mut tree = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 1).unwrap();
        let directory = tree
            .append(
                tree.root(),
                b"dir",
                EntryType::Directory,
                FileIdentity { device: 1, inode: 2 },
                2,
                0,
                0,
                NodeStatus::Complete,
            )
            .unwrap();
        let file = tree
            .append(
                directory,
                b"file",
                EntryType::RegularFile,
                FileIdentity { device: 1, inode: 3 },
                1,
                7,
                512,
                NodeStatus::Complete,
            )
            .unwrap();
        assert!(tree.add_to_ancestors(directory, 7, 512));
        assert_eq!(tree.record(tree.root()).unwrap().apparent_bytes, 7);
        assert_eq!(tree.tombstone_subtree(file), Some((7, 512, 1)));
        assert_eq!(tree.record(directory).unwrap().apparent_bytes, 0);
        assert_eq!(tree.tombstone_subtree(directory), Some((0, 0, 1)));
        assert_eq!(tree.tombstone_subtree(directory), Some((0, 0, 0)));
        assert_eq!(tree.record(tree.root()).unwrap().allocated_bytes, 0);
        assert!(tree.children(directory).next().is_none());
    }

    #[test]
    fn batch_fold_keeps_states_names_times_and_totals_for_both_callers() {
        use super::{
            finish_scan_directory, fold_entry_batch, DirectoryToken, EntryBatch, ModifiedTime, NodeId, ScanEntry,
        };

        type Snapshot = Vec<(Vec<u8>, NodeState, u64, u64, Option<i64>)>;

        fn snapshot(tree: &Tree) -> Snapshot {
            let mut out = Vec::new();
            let mut stack = vec![(tree.root(), Vec::new())];
            while let Some((id, path)) = stack.pop() {
                let record = tree.record(id).expect("snapshot visits live nodes");
                out.push((
                    path.clone(),
                    record.state,
                    record.apparent_bytes,
                    record.allocated_bytes,
                    record.modified_time().unix_seconds(),
                ));
                let mut children: Vec<NodeId> = tree.children(id).collect();
                children.reverse();
                for child in children {
                    let mut child_path = path.clone();
                    child_path.extend_from_slice(tree.name(child).expect("child has a name"));
                    stack.push((child, child_path));
                }
            }
            out
        }

        fn batch() -> EntryBatch {
            let mut batch = EntryBatch::with_capacity(4, 64);
            let push = |batch: &mut EntryBatch,
                            name: &[u8],
                            entry_type: EntryType,
                            token: Option<u32>,
                            state: NodeStatus,
                            apparent: u64,
                            allocated: u64,
                            mtime: i64| {
                let start = batch.names.len();
                batch.names.extend_from_slice(name);
                let end = batch.names.len();
                batch.entries.push(ScanEntry {
                    parent: DirectoryToken(0),
                    name: start as u32..end as u32,
                    directory_token: token.map(DirectoryToken),
                    entry_type,
                    identity: FileIdentity { device: 1, inode: batch.entries.len() as u64 + 2 },
                    modified_time: ModifiedTime::from_unix_seconds(mtime).unwrap(),
                    link_count: 1,
                    apparent_bytes: apparent,
                    allocated_bytes: allocated,
                    state,
                });
            };
            push(&mut batch, b"sub", EntryType::Directory, Some(1), NodeStatus::Complete, 0, 0, 1_700_000_001);
            push(&mut batch, b"kept.bin", EntryType::RegularFile, None, NodeStatus::Complete, 100, 512, 1_700_000_002);
            push(
                &mut batch,
                b"broken.bin",
                EntryType::RegularFile,
                None,
                NodeStatus::Incomplete,
                50,
                64,
                1_700_000_003,
            );
            batch
        }

        // Both scan consumers fold through the same function, so folding the
        // same batch twice — once per caller — must produce identical trees.
        let mut browser_tree = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 1).unwrap();
        let mut browser_tokens = vec![Some(browser_tree.root())];
        let browser_outcome = fold_entry_batch(&mut browser_tree, &mut browser_tokens, &batch());
        let mut rmrf_tree = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 1).unwrap();
        let mut rmrf_tokens = vec![Some(rmrf_tree.root())];
        let rmrf_outcome = fold_entry_batch(&mut rmrf_tree, &mut rmrf_tokens, &batch());
        assert_eq!(browser_outcome.appended.len(), 3);
        assert_eq!(rmrf_outcome.appended.len(), 3);
        assert_eq!(snapshot(&browser_tree), snapshot(&rmrf_tree));

        let tree = &mut browser_tree;
        let token_nodes = &mut browser_tokens;
        let outcome = browser_outcome;
        assert_eq!(outcome.appended.len(), 3);
        assert_eq!(outcome.incomplete_entries, 1);
        assert!(!outcome.unknown_directory && !outcome.invalid_range && !outcome.exhausted && !outcome.overflow);

        let sub = token_nodes[1].expect("directory token is registered");
        assert_eq!(tree.name(sub), Some(b"sub".as_slice()));
        assert_eq!(tree.record(sub).unwrap().modified_time().unix_seconds(), Some(1_700_000_001));
        assert_eq!(tree.record(sub).unwrap().state, NodeState::Scanning, "a directory stays scanning until its finish arrives");
        let kept = outcome.appended[1];
        assert_eq!(tree.name(kept), Some(b"kept.bin".as_slice()));
        assert_eq!(tree.record(kept).unwrap().modified_time().unix_seconds(), Some(1_700_000_002));
        assert_eq!(tree.record(kept).unwrap().state, NodeState::Complete);
        let broken = outcome.appended[2];
        assert_eq!(tree.record(broken).unwrap().modified_time().unix_seconds(), Some(1_700_000_003));
        assert_eq!(tree.record(broken).unwrap().state, NodeState::Incomplete);
        assert_eq!(tree.record(tree.root()).unwrap().state, NodeState::Incomplete);
        assert_eq!(tree.record(tree.root()).unwrap().apparent_bytes, 100);
        assert_eq!(tree.record(tree.root()).unwrap().allocated_bytes, 512);

        assert_eq!(finish_scan_directory(tree, token_nodes, DirectoryToken(1), true), 0);
        assert_eq!(tree.record(sub).unwrap().state, NodeState::Complete);
    }

    #[test]
    fn unfinished_and_excluded_directories_settle_without_changing_exclusions() {
        use super::{finish_scan_directory, ExclusionReason};
        let mut tree = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 1).unwrap();
        let scanning = tree
            .append(
                tree.root(),
                b"scanning",
                EntryType::Directory,
                FileIdentity { device: 1, inode: 2 },
                1,
                0,
                0,
                NodeStatus::Scanning,
            )
            .unwrap();
        let excluded = tree
            .append(
                tree.root(),
                b"excluded",
                EntryType::Directory,
                FileIdentity { device: 1, inode: 3 },
                1,
                0,
                0,
                NodeStatus::Excluded(ExclusionReason::MountBoundary),
            )
            .unwrap();
        let token_nodes = vec![Some(tree.root()), Some(scanning), Some(excluded)];
        assert_eq!(finish_scan_directory(&mut tree, &token_nodes, super::DirectoryToken(1), true), 0);
        assert_eq!(tree.record(scanning).unwrap().state, NodeState::Complete);
        assert_eq!(finish_scan_directory(&mut tree, &token_nodes, super::DirectoryToken(2), false), 0);
        assert_eq!(tree.record(excluded).unwrap().state, NodeState::Excluded(ExclusionReason::MountBoundary));
        assert_eq!(tree.record(tree.root()).unwrap().state, NodeState::Scanning);
        assert_eq!(finish_scan_directory(&mut tree, &token_nodes, super::DirectoryToken(1), false), 2);
        assert_eq!(tree.record(scanning).unwrap().state, NodeState::Incomplete);
        assert_eq!(tree.record(tree.root()).unwrap().state, NodeState::Incomplete);
    }

    #[test]
    fn overflow_marks_every_ancestor_without_partially_adding_the_batch() {
        let mut tree = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 1).unwrap();
        let directory = tree
            .append(
                tree.root(),
                b"dir",
                EntryType::Directory,
                FileIdentity { device: 1, inode: 2 },
                2,
                0,
                0,
                NodeStatus::Complete,
            )
            .unwrap();
        assert!(tree.add_to_ancestors(tree.root(), u64::MAX, u64::MAX));
        assert!(!tree.add_to_ancestors(directory, 1, 1));
        assert_eq!(tree.record(directory).unwrap().apparent_bytes, 0);
        assert_eq!(tree.record(tree.root()).unwrap().apparent_bytes, u64::MAX);
        assert_eq!(tree.record(directory).unwrap().state, NodeState::Incomplete);
        assert_eq!(tree.record(tree.root()).unwrap().state, NodeState::Incomplete);
    }
}
