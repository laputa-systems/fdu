use fdu_core::{EntryType, ExclusionReason, FileIdentity, NodeId, NodeRecord, NodeState, Tree};
use hashbrown::HashSet;
use std::fmt;
use std::io;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

const OUTCOME_BATCH_SIZE: usize = 2048;
/// Progress is advisory, so it is sent at most this often instead of once per
/// operation. A send per operation made the worker wait on the interface.
const PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Disposition {
    Retain,
    Enter,
    Remove,
}

pub struct DeletionPlan {
    tree: Arc<Tree>,
    pub roots: Vec<NodeId>,
    targets: Vec<NodeId>,
    disposition: Vec<Disposition>,
    pub skipped: Vec<Skipped>,
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
}

impl DeletionPlan {
    pub fn operation_count(&self) -> usize {
        self.targets.len()
    }

    pub fn tree(&self) -> &Arc<Tree> {
        &self.tree
    }
}

/// An indexed entry under a selected root that the plan leaves in place because
/// its scan is not complete or it is excluded. A directory that is only kept
/// because something inside it is skipped is not listed.
#[derive(Debug)]
pub struct Skipped {
    pub node: NodeId,
    pub reason: &'static str,
}

/// A selection the planner refuses as a whole. `node` is `None` for problems
/// with the selection itself, such as an empty selection or a byte total that
/// overflows.
#[derive(Debug)]
pub struct Rejection {
    pub node: Option<NodeId>,
    pub reason: String,
}

#[derive(Debug)]
pub struct PlanError {
    pub rejected: Vec<Rejection>,
}

impl fmt::Display for PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} selected entr{} cannot be deleted", self.rejected.len(), if self.rejected.len() == 1 { "y" } else { "ies" })
    }
}

impl std::error::Error for PlanError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeKind {
    Deleted,
    AlreadyAbsent,
    Changed,
    Failed,
    Cancelled,
}

#[derive(Debug)]
pub struct DeleteOutcome {
    pub node: NodeId,
    pub kind: OutcomeKind,
    pub message: Option<String>,
}

#[derive(Debug)]
pub enum DeleteEvent {
    Progress { completed: usize, total: usize, current: NodeId },
    Outcomes(Vec<DeleteOutcome>),
    Failed { message: String },
    Finished,
}

/// Plans the permanent removal of `selected` roots, which may live in different
/// directories. Roots must not overlap, because a directory root already covers
/// every descendant. The scan may still be running elsewhere in the tree; the
/// caller must not add entries to the tree while the plan exists.
///
/// Scan problems are advisory. Entries still being scanned, whose scan is
/// incomplete, or that are excluded are left in place and listed in `skipped`,
/// together with the directories that hold them; everything else under the
/// roots is planned. The plan is refused only when nothing at all can be
/// deleted.
pub fn create_plan(tree: Arc<Tree>, selected: &[NodeId]) -> Result<DeletionPlan, PlanError> {
    let mut rejected = Vec::new();
    if selected.is_empty() {
        rejected.push(Rejection { node: None, reason: "no entries are selected".to_owned() });
    }
    let mut unique = Vec::with_capacity(selected.len());
    let mut seen = HashSet::with_capacity(selected.len());
    for id in selected {
        if seen.insert(*id) {
            unique.push(*id);
        }
    }
    for id in &unique {
        let reject = |reason: &str| Rejection { node: Some(*id), reason: reason.to_owned() };
        match tree.record(*id) {
            None => rejected.push(reject("absent from the index")),
            Some(record) if *id == tree.root() || record.entry_type == EntryType::Root => {
                rejected.push(reject("the scan root is protected"));
            }
            Some(_) if has_selected_ancestor(&tree, &seen, *id) => {
                rejected.push(reject("already covered by a selected parent directory"));
            }
            Some(_) => {}
        }
    }
    if !unique.is_empty() {
        let root_state = tree.record(tree.root()).expect("root record exists").state;
        let block = match root_state {
            NodeState::Complete | NodeState::Incomplete | NodeState::Scanning => None,
            _ => Some("root totals are stale; rescan first"),
        };
        if let Some(reason) = block {
            rejected.push(Rejection { node: None, reason: reason.to_owned() });
        }
    }
    if !rejected.is_empty() {
        return Err(PlanError { rejected });
    }

    let mut disposition = vec![Disposition::Retain; tree.len()];
    let mut targets = Vec::new();
    let mut skipped = Vec::new();
    let mut roots = Vec::with_capacity(unique.len());
    for root in &unique {
        classify_subtree(&tree, *root, &mut disposition, &mut targets, &mut skipped).map_err(|reason| PlanError {
            rejected: vec![Rejection { node: Some(*root), reason: reason.to_owned() }],
        })?;
        if disposition[root.index()] != Disposition::Retain {
            roots.push(*root);
        }
    }
    if targets.is_empty() {
        return Err(PlanError {
            rejected: skipped
                .into_iter()
                .map(|entry| Rejection { node: Some(entry.node), reason: entry.reason.to_owned() })
                .collect(),
        });
    }

    let overflow = |what: &str| PlanError {
        rejected: vec![Rejection { node: None, reason: format!("selected {what} byte total exceeds u64") }],
    };
    let mut apparent_bytes = 0u64;
    let mut allocated_bytes = 0u64;
    for target in &targets {
        let record = tree.record(*target).expect("validated deletion target");
        let covered = record.parent().is_some_and(|parent| disposition[parent.index()] == Disposition::Remove);
        if !covered {
            apparent_bytes = apparent_bytes.checked_add(record.apparent_bytes).ok_or_else(|| overflow("apparent"))?;
            allocated_bytes = allocated_bytes.checked_add(record.allocated_bytes).ok_or_else(|| overflow("allocated"))?;
        }
    }
    Ok(DeletionPlan {
        tree,
        roots,
        targets,
        disposition,
        skipped,
        apparent_bytes,
        allocated_bytes,
    })
}

fn has_selected_ancestor(tree: &Tree, selected: &HashSet<NodeId>, id: NodeId) -> bool {
    let mut current = tree.record(id).and_then(|record| record.parent());
    while let Some(ancestor) = current {
        if selected.contains(&ancestor) {
            return true;
        }
        current = tree.record(ancestor).and_then(|record| record.parent());
    }
    false
}

fn skip_reason(state: NodeState) -> &'static str {
    match state {
        NodeState::Excluded(ExclusionReason::MountBoundary) => "mount boundary is protected",
        NodeState::Excluded(ExclusionReason::UnsupportedAlias) => "unsupported directory alias is protected",
        NodeState::Complete => "nothing inside it can be deleted",
        NodeState::Scanning => "scan is still in progress",
        NodeState::Incomplete => "scan is incomplete",
        NodeState::Stale => "entry totals are stale; rescan first",
        NodeState::Tombstone => "entry has already been removed from the index",
    }
}

struct ClassifyFrame {
    node: NodeId,
    state: NodeState,
    next_child: Option<NodeId>,
    skipped_start: usize,
    all_removed: bool,
    any_planned: bool,
}

impl ClassifyFrame {
    fn settle(&mut self, child: Disposition) {
        self.all_removed &= child == Disposition::Remove;
        self.any_planned |= child != Disposition::Retain;
    }
}

enum Visit {
    Leaf(Disposition),
    Descend(ClassifyFrame),
}

const INVALID_INDEX: &str = "directory index is invalid";

fn visit(tree: &Tree, id: NodeId, targets: &mut Vec<NodeId>, disposition: &mut [Disposition], skipped: &mut Vec<Skipped>) -> Result<Visit, &'static str> {
    let record = tree.record(id).ok_or(INVALID_INDEX)?;
    if record.entry_type == EntryType::Directory && matches!(record.state, NodeState::Complete | NodeState::Incomplete) {
        return Ok(Visit::Descend(ClassifyFrame {
            node: id,
            state: record.state,
            next_child: record.first_child(),
            skipped_start: skipped.len(),
            all_removed: true,
            any_planned: false,
        }));
    }
    if record.state == NodeState::Complete {
        disposition[id.index()] = Disposition::Remove;
        targets.push(id);
        return Ok(Visit::Leaf(Disposition::Remove));
    }
    skipped.push(Skipped { node: id, reason: skip_reason(record.state) });
    Ok(Visit::Leaf(Disposition::Retain))
}

/// Walks the subtree below `root` without recursion and appends its removable
/// entries to `targets`, descendants before their directory. A directory is
/// removed only when everything inside it is; otherwise it is entered when
/// something inside it is removable and left alone when nothing is.
fn classify_subtree(
    tree: &Tree,
    root: NodeId,
    disposition: &mut [Disposition],
    targets: &mut Vec<NodeId>,
    skipped: &mut Vec<Skipped>,
) -> Result<(), &'static str> {
    let mut stack = Vec::new();
    if let Visit::Descend(frame) = visit(tree, root, targets, disposition, skipped)? {
        stack.push(frame);
    }
    while let Some(top) = stack.last_mut() {
        if let Some(child) = top.next_child {
            top.next_child = tree.record(child).ok_or(INVALID_INDEX)?.next_sibling();
            match visit(tree, child, targets, disposition, skipped)? {
                Visit::Leaf(outcome) => top.settle(outcome),
                Visit::Descend(frame) => stack.push(frame),
            }
            continue;
        }
        let frame = stack.pop().expect("frame exists");
        let outcome = if frame.state == NodeState::Complete && frame.all_removed {
            targets.push(frame.node);
            Disposition::Remove
        } else if frame.any_planned {
            Disposition::Enter
        } else {
            skipped.truncate(frame.skipped_start);
            skipped.push(Skipped { node: frame.node, reason: skip_reason(frame.state) });
            Disposition::Retain
        };
        disposition[frame.node.index()] = outcome;
        if let Some(parent) = stack.last_mut() {
            parent.settle(outcome);
        }
    }
    Ok(())
}

/// Executes a confirmed deletion plan. The root must be writable. Other
/// processes must leave private quarantine directories, their contents, and
/// recovery records untouched, including through existing directory descriptors.
/// Interrupted captures are restored without overwrite before any removal, then
/// execution fails with a request to rescan. Recovery never deletes captures.
pub fn execute(
    root: OwnedFd,
    plan: DeletionPlan,
    cancelled: Arc<AtomicBool>,
    sender: SyncSender<DeleteEvent>,
) {
    posix::run(root, plan, cancelled, sender);
}

mod posix {
    mod quarantine;
    use super::*;
    use rustc_hash::FxHashMap;
    use rustix::fs::{self, AtFlags, FileType, Mode, OFlags, Stat};
    use rustix::io::Errno;
    use std::os::fd::{AsFd, BorrowedFd};
    use std::sync::atomic::{AtomicU64, AtomicUsize};
    use std::sync::Mutex;
    use std::time::Instant;

    const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);
    const DELETE_THREADS: usize = 16;

    /// Identifies the mount a directory lives on, so a directory that became a mount point since
    /// scanning is never entered.
    #[cfg(target_os = "macos")]
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct MountIdentity {
        device: u64,
        fsid: [i32; 2],
        mount_point: Vec<u8>,
    }

    /// The kernel mount ID tells bind mounts of one filesystem apart; the device separates
    /// filesystem boundaries that share a mount, such as btrfs subvolumes.
    #[cfg(target_os = "linux")]
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct MountIdentity {
        device: u64,
        mount_id: u64,
    }

    struct DirContext {
        fd: OwnedFd,
        node: NodeId,
    }

    struct DirectoryWalkFrame {
        node: NodeId,
        parent: NodeId,
        children: Vec<NodeId>,
        next_child: usize,
    }

    struct Deletion<'a> {
        tree: &'a Tree,
        disposition: &'a [Disposition],
        quarantine: &'a quarantine::Session,
        root_mount: MountIdentity,
        cancelled: &'a AtomicBool,
        sender: &'a SyncSender<DeleteEvent>,
        after_operation: &'a (dyn Fn(usize) + Sync),
        #[cfg(test)]
        before_capture: &'a (dyn Fn(NodeId) + Sync),
        recorded: Vec<AtomicBool>,
        outcomes: Mutex<Vec<DeleteOutcome>>,
        completed: AtomicUsize,
        total: usize,
        started: Instant,
        last_progress_nanos: AtomicU64,
        receiver_gone: AtomicBool,
    }

    pub(super) fn run(
        root: OwnedFd,
        plan: DeletionPlan,
        cancelled: Arc<AtomicBool>,
        sender: SyncSender<DeleteEvent>,
    ) {
        run_with(root, plan, cancelled, sender, DELETE_THREADS, |_| {});
    }

    pub(super) fn run_with(
        root: OwnedFd,
        plan: DeletionPlan,
        cancelled: Arc<AtomicBool>,
        sender: SyncSender<DeleteEvent>,
        threads: usize,
        after_operation: impl Fn(usize) + Sync,
    ) {
        run_inner(root, plan, cancelled, sender, threads, after_operation, #[cfg(test)] |_| {});
    }

    #[cfg(test)]
    pub(super) fn run_with_capture_hook(
        root: OwnedFd,
        plan: DeletionPlan,
        cancelled: Arc<AtomicBool>,
        sender: SyncSender<DeleteEvent>,
        threads: usize,
        after_operation: impl Fn(usize) + Sync,
        before_capture: impl Fn(NodeId) + Sync,
    ) {
        run_inner(root, plan, cancelled, sender, threads, after_operation, before_capture);
    }

    fn run_inner(
        root: OwnedFd,
        plan: DeletionPlan,
        cancelled: Arc<AtomicBool>,
        sender: SyncSender<DeleteEvent>,
        threads: usize,
        after_operation: impl Fn(usize) + Sync,
        #[cfg(test)] before_capture: impl Fn(NodeId) + Sync,
    ) {
        let tree: &Tree = &plan.tree;
        let root_record = tree.record(tree.root()).expect("tree has root");
        let expected_root = root_record.identity;
        let root_mount = match directory_state(root.as_fd()) {
            Ok((identity, mount)) if identity == expected_root => mount,
            Ok(_) => {
                send_failure(&sender, "scan root identity changed before deletion");
                return;
            }
            Err(error) => {
                send_failure(&sender, &error.to_string());
                return;
            }
        };
        let quarantine = match quarantine::Session::start(root.as_fd()) {
            Ok(session) => session,
            Err(error) => { send_failure(&sender, &error.to_string()); return; }
        };
        let deletion = Deletion {
            tree,
            disposition: &plan.disposition,
            quarantine: &quarantine,
            root_mount,
            cancelled: &cancelled,
            sender: &sender,
            after_operation: &after_operation,
            #[cfg(test)]
            before_capture: &before_capture,
            recorded: (0..tree.len()).map(|_| AtomicBool::new(false)).collect(),
            outcomes: Mutex::new(Vec::with_capacity(OUTCOME_BATCH_SIZE)),
            completed: AtomicUsize::new(0),
            total: plan.targets.len(),
            started: Instant::now(),
            last_progress_nanos: AtomicU64::new(0),
            receiver_gone: AtomicBool::new(false),
        };

        // Selected entries are grouped by the directory they live in, so each such directory is
        // opened once.
        let mut groups: FxHashMap<usize, Vec<NodeId>> = FxHashMap::default();
        for selected in &plan.roots {
            let parent = tree
                .record(*selected)
                .and_then(NodeRecord::parent)
                .expect("deletion targets have parents");
            groups.entry(parent.index()).or_default().push(*selected);
        }
        let root_context = match root.try_clone() {
            Ok(fd) => Arc::new(DirContext { fd, node: tree.root() }),
            Err(error) => {
                send_failure(&sender, &error.to_string());
                return;
            }
        };
        let pool = match rayon::ThreadPoolBuilder::new()
            .num_threads(threads.max(1))
            .thread_name(|index| format!("fdu-delete-{index}"))
            .build()
        {
            Ok(pool) => pool,
            Err(error) => {
                send_failure(&sender, &error.to_string());
                return;
            }
        };
        let deletion = &deletion;
        pool.scope(|scope| {
            for (parent, selected) in groups {
                let root_context = Arc::clone(&root_context);
                scope.spawn(move |scope| {
                    let parent = NodeId::from_index(parent).expect("indexed parent");
                    delete_group(deletion, &root_context, parent, selected, scope);
                });
            }
        });
        drop(pool);

        if deletion.receiver_gone.load(Ordering::Relaxed) {
            return;
        }
        // Whatever was not attempted, in plan order, because the deletion was stopped.
        let mut outcomes = std::mem::take(&mut *deletion.outcomes.lock().expect("outcomes are not poisoned"));
        for node in plan.targets.iter().copied() {
            if !deletion.recorded[node.index()].swap(true, Ordering::Relaxed) {
                outcomes.push(DeleteOutcome { node, kind: OutcomeKind::Cancelled, message: None });
                if outcomes.len() >= OUTCOME_BATCH_SIZE && !flush(&sender, &mut outcomes) {
                    return;
                }
            }
        }
        if !flush(&sender, &mut outcomes) {
            return;
        }
        if let Err(error) = quarantine.finish() {
            send_failure(&sender, &error.to_string());
            return;
        }
        let _ = sender.send(DeleteEvent::Finished);
    }

    impl Deletion<'_> {
        fn stopped(&self) -> bool {
            self.cancelled.load(Ordering::Relaxed) || self.receiver_gone.load(Ordering::Relaxed) || self.quarantine.recovery_required()
        }

        fn record(&self, node: NodeId, kind: OutcomeKind, message: Option<String>) {
            if self.disposition[node.index()] != Disposition::Remove || self.recorded[node.index()].swap(true, Ordering::Relaxed) {
                return;
            }
            let completed = self.completed.fetch_add(1, Ordering::Relaxed) + 1;
            (self.after_operation)(completed);
            let batch = {
                let mut outcomes = self.outcomes.lock().expect("outcomes are not poisoned");
                outcomes.push(DeleteOutcome { node, kind, message });
                (outcomes.len() >= OUTCOME_BATCH_SIZE)
                    .then(|| std::mem::replace(&mut *outcomes, Vec::with_capacity(OUTCOME_BATCH_SIZE)))
            };
            if let Some(batch) = batch {
                if self.sender.send(DeleteEvent::Outcomes(batch)).is_err() {
                    self.receiver_gone.store(true, Ordering::Relaxed);
                }
            }
            self.report_progress(completed, node);
        }

        /// Progress is advisory, so it goes out at most every interval, from whichever thread
        /// notices the interval has passed, and never waits on the interface.
        fn report_progress(&self, completed: usize, current: NodeId) {
            let now = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let interval = u64::try_from(PROGRESS_INTERVAL.as_nanos()).unwrap_or(u64::MAX);
            let last = self.last_progress_nanos.load(Ordering::Relaxed);
            if now.saturating_sub(last) < interval
                || self
                    .last_progress_nanos
                    .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
            {
                return;
            }
            match self.sender.try_send(DeleteEvent::Progress { completed, total: self.total, current }) {
                // A full queue means the interface is behind; the next report supersedes this one.
                Ok(()) | Err(std::sync::mpsc::TrySendError::Full(_)) => {}
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    self.receiver_gone.store(true, Ordering::Relaxed);
                }
            }
        }

        /// Gives every entry below `node` the same outcome, for a directory that could not be
        /// entered.
        fn record_below(&self, node: NodeId, kind: OutcomeKind, message: &str) {
            let mut stack = vec![node];
            while let Some(current) = stack.pop() {
                for child in self.tree.children(current) {
                    if self.tree.record(child).is_some_and(|record| record.entry_type == EntryType::Directory) {
                        stack.push(child);
                    }
                    self.record(child, kind, Some(message.to_owned()));
                }
            }
        }
    }

    fn delete_group<'scope>(
        deletion: &'scope Deletion<'_>,
        root_context: &Arc<DirContext>,
        parent: NodeId,
        selected: Vec<NodeId>,
        scope: &rayon::Scope<'scope>,
    ) {
        let context = match open_context(deletion, root_context, parent) {
            Ok(context) => context,
            Err(message) => {
                for node in selected {
                    deletion.record(node, OutcomeKind::Failed, Some(message.clone()));
                    deletion.record_below(node, OutcomeKind::Failed, &message);
                }
                return;
            }
        };
        for node in selected {
            if deletion.stopped() {
                return;
            }
            let is_directory = deletion
                .tree
                .record(node)
                .is_some_and(|record| record.entry_type == EntryType::Directory);
            if is_directory {
                let context = Arc::clone(&context);
                scope.spawn(move |_| delete_directory(deletion, context, node));
            } else {
                delete_file(deletion, context.fd.as_fd(), node);
            }
        }
    }

    fn open_context(
        deletion: &Deletion<'_>,
        root_context: &Arc<DirContext>,
        node: NodeId,
    ) -> Result<Arc<DirContext>, String> {
        let tree = deletion.tree;
        let mut path = Vec::new();
        let mut current = Some(node);
        while let Some(next) = current {
            if next == tree.root() {
                break;
            }
            path.push(next);
            current = tree.record(next).and_then(NodeRecord::parent);
            if current.is_none() {
                return Err("parent missing from index".to_owned());
            }
        }
        let mut fd = root_context.fd.try_clone().map_err(|error| error.to_string())?;
        let mut current = root_context.node;
        for next in path.into_iter().rev() {
            fd = open_checked(deletion, fd.as_fd(), next).map_err(|(_, message)| message)?;
            current = next;
        }
        Ok(Arc::new(DirContext { fd, node: current }))
    }

    fn open_checked(
        deletion: &Deletion<'_>,
        parent: BorrowedFd<'_>,
        node: NodeId,
    ) -> Result<OwnedFd, (OutcomeKind, String)> {
        let record = deletion.tree.record(node).expect("path node exists");
        let name = deletion.tree.name(node).ok_or((OutcomeKind::Failed, "directory name missing".to_owned()))?;
        let fd = match retry_errno(|| fs::openat(parent, name, DIRECTORY_FLAGS, Mode::empty())) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => return Err((OutcomeKind::AlreadyAbsent, "directory is already gone".to_owned())),
            Err(error) => {
                if let Ok(latest) = stat_at(parent, name) {
                    if identity(&latest).ok() != Some(record.identity) || entry_type(&latest) != record.entry_type {
                        return Err((OutcomeKind::Changed, "directory was replaced during validation".to_owned()));
                    }
                }
                return Err((OutcomeKind::Failed, io::Error::from(error).to_string()));
            }
        };
        let (found, mount) = directory_state(fd.as_fd()).map_err(|error| (OutcomeKind::Failed, error.to_string()))?;
        if found != record.identity {
            return Err((OutcomeKind::Changed, "directory was replaced".to_owned()));
        }
        if mount != deletion.root_mount {
            return Err((OutcomeKind::Changed, "directory became a mount boundary".to_owned()));
        }
        Ok(fd)
    }

    fn delete_directory(
        deletion: &Deletion<'_>,
        parent: Arc<DirContext>,
        node: NodeId,
    ) {
        if deletion.stopped() {
            return;
        }
        let mut current = match open_checked(deletion, parent.fd.as_fd(), node) {
            Ok(fd) => fd,
            Err((kind, message)) => {
                deletion.record(node, kind, Some(message.clone()));
                let below = if kind == OutcomeKind::AlreadyAbsent { OutcomeKind::AlreadyAbsent } else { OutcomeKind::Failed };
                deletion.record_below(node, below, &message);
                return;
            }
        };
        let mut stack = vec![directory_walk_frame(deletion.tree, deletion.disposition, node, parent.node)];

        loop {
            if deletion.stopped() {
                return;
            }

            let child = {
                let frame = stack.last_mut().expect("directory frame exists");
                let child = frame.children.get(frame.next_child).copied();
                frame.next_child += usize::from(child.is_some());
                child
            };
            if let Some(child) = child {
                if deletion.tree.record(child).expect("indexed child").entry_type == EntryType::Directory {
                    match open_checked(deletion, current.as_fd(), child) {
                        Ok(directory) => {
                            let parent_node = stack.last().expect("parent directory frame exists").node;
                            current = directory;
                            stack.push(directory_walk_frame(deletion.tree, deletion.disposition, child, parent_node));
                        }
                        Err((kind, message)) => {
                            deletion.record(child, kind, Some(message.clone()));
                            let below = if kind == OutcomeKind::AlreadyAbsent { OutcomeKind::AlreadyAbsent } else { OutcomeKind::Failed };
                            deletion.record_below(child, below, &message);
                        }
                    }
                } else {
                    delete_file(deletion, current.as_fd(), child);
                }
                continue;
            }

            let frame = stack.last().expect("directory frame exists");
            let finished = frame.node;
            let removable = deletion.disposition[finished.index()] == Disposition::Remove;
            if stack.len() == 1 {
                if removable {
                    remove_directory(deletion, parent.fd.as_fd(), finished);
                }
                return;
            }

            let expected_parent = frame.parent;
            match open_parent_checked(deletion, current.as_fd(), expected_parent) {
                Ok(parent_fd) => {
                    if removable {
                        remove_directory(deletion, parent_fd.as_fd(), finished);
                    }
                    stack.pop();
                    current = parent_fd;
                }
                Err((kind, message)) => {
                    let selected = stack[0].node;
                    deletion.record(finished, kind, Some(message.clone()));
                    deletion.record(selected, OutcomeKind::Failed, Some(message.clone()));
                    deletion.record_below(selected, OutcomeKind::Failed, &message);
                    return;
                }
            }
        }
    }

    fn directory_walk_frame(tree: &Tree, disposition: &[Disposition], node: NodeId, parent: NodeId) -> DirectoryWalkFrame {
        let mut children: Vec<_> = tree.children(node).filter(|child| disposition[child.index()] != Disposition::Retain).collect();
        children.sort_unstable_by_key(|child| {
            let record = tree.record(*child).expect("indexed child");
            (record.entry_type == EntryType::Directory, record.identity.inode)
        });
        DirectoryWalkFrame { node, parent, children, next_child: 0 }
    }

    fn open_parent_checked(
        deletion: &Deletion<'_>,
        child: BorrowedFd<'_>,
        parent: NodeId,
    ) -> Result<OwnedFd, (OutcomeKind, String)> {
        let fd = retry_errno(|| fs::openat(child, c"..", DIRECTORY_FLAGS, Mode::empty()))
            .map_err(|error| (OutcomeKind::Failed, io::Error::from(error).to_string()))?;
        let (found, mount) = directory_state(fd.as_fd()).map_err(|error| (OutcomeKind::Failed, error.to_string()))?;
        let expected = deletion.tree.record(parent).expect("parent directory is indexed");
        if found != expected.identity {
            return Err((OutcomeKind::Changed, "parent directory changed during deletion".to_owned()));
        }
        if mount != deletion.root_mount {
            return Err((OutcomeKind::Changed, "parent became a mount boundary".to_owned()));
        }
        Ok(fd)
    }

    fn remove_directory(deletion: &Deletion<'_>, parent_fd: BorrowedFd<'_>, node: NodeId) {
        let record = deletion.tree.record(node).expect("deletion target exists");
        let name = deletion.tree.name(node).expect("indexed entry has name");
        let latest = match stat_at(parent_fd, name) {
            Ok(stat) => stat,
            Err(Errno::NOENT) => return deletion.record(node, OutcomeKind::AlreadyAbsent, None),
            Err(error) => return deletion.record(node, OutcomeKind::Failed, Some(io::Error::from(error).to_string())),
        };
        match identity(&latest) {
            Ok(found) if found == record.identity && entry_type(&latest) == record.entry_type => {}
            Ok(_) => {
                return deletion.record(
                    node,
                    OutcomeKind::Changed,
                    Some("entry changed immediately before removal".to_owned()),
                );
            }
            Err(error) => return deletion.record(node, OutcomeKind::Failed, Some(error.to_string())),
        }
        capture_and_remove(deletion, parent_fd, name, node, AtFlags::REMOVEDIR);
    }

    fn delete_file(deletion: &Deletion<'_>, parent_fd: BorrowedFd<'_>, node: NodeId) {
        let record = deletion.tree.record(node).expect("deletion target exists");
        let name = deletion.tree.name(node).expect("indexed entry has name");
        let current = match stat_at(parent_fd, name) {
            Ok(stat) => stat,
            Err(Errno::NOENT) => return deletion.record(node, OutcomeKind::AlreadyAbsent, None),
            Err(error) => return deletion.record(node, OutcomeKind::Failed, Some(io::Error::from(error).to_string())),
        };
        match identity(&current) {
            Ok(found) if found == record.identity && entry_type(&current) == record.entry_type => {}
            Ok(_) => {
                return deletion.record(
                    node,
                    OutcomeKind::Changed,
                    Some("entry was replaced since scanning".to_owned()),
                );
            }
            Err(error) => return deletion.record(node, OutcomeKind::Failed, Some(error.to_string())),
        }
        capture_and_remove(deletion, parent_fd, name, node, AtFlags::empty());
    }

    fn capture_and_remove(deletion: &Deletion<'_>, parent_fd: BorrowedFd<'_>, name: &[u8], node: NodeId, flags: AtFlags) {
        let (kind, message) = deletion.quarantine.remove(deletion, parent_fd, name, node, flags);
        deletion.record(node, kind, message);
    }

    fn stat_at(parent: BorrowedFd<'_>, name: &[u8]) -> Result<Stat, Errno> {
        retry_errno(|| fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW))
    }

    // `st_dev` is 32 bits on macOS but 64 bits on Linux: the checked conversion is
    // required on one platform and a no-op where the field is already `u64`.
    #[allow(clippy::useless_conversion)]
    fn identity(metadata: &Stat) -> io::Result<FileIdentity> {
        Ok(FileIdentity {
            device: u64::try_from(metadata.st_dev).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid device number"))?,
            inode: metadata.st_ino,
        })
    }

    fn entry_type(metadata: &Stat) -> EntryType {
        match FileType::from_raw_mode(metadata.st_mode) {
            FileType::Directory => EntryType::Directory,
            FileType::RegularFile => EntryType::RegularFile,
            FileType::Symlink => EntryType::Symlink,
            _ => EntryType::Other,
        }
    }

    /// The identity and mount of an open directory.
    #[cfg(target_os = "macos")]
    fn directory_state(fd: BorrowedFd<'_>) -> io::Result<(FileIdentity, MountIdentity)> {
        let metadata = retry(|| fs::fstat(fd))?;
        let filesystem = retry(|| fs::fstatfs(fd))?;
        // Apple's fsid_t is exactly two i32 words.
        const _: () = assert!(std::mem::size_of::<[i32; 2]>() == 8);
        let fsid = unsafe { std::mem::transmute_copy::<_, [i32; 2]>(&filesystem.f_fsid) };
        let mount_point = filesystem
            .f_mntonname
            .iter()
            .take_while(|byte| **byte != 0)
            .map(|byte| *byte as u8)
            .collect();
        let identity = identity(&metadata)?;
        Ok((identity, MountIdentity { device: identity.device, fsid, mount_point }))
    }

    #[cfg(target_os = "linux")]
    fn directory_state(fd: BorrowedFd<'_>) -> io::Result<(FileIdentity, MountIdentity)> {
        let wanted = fs::StatxFlags::INO | fs::StatxFlags::MNT_ID;
        let metadata = retry(|| fs::statx(fd, c"", AtFlags::EMPTY_PATH, wanted))?;
        if !fs::StatxFlags::from_bits_retain(metadata.stx_mask).contains(wanted) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kernel did not report the directory identity and mount ID",
            ));
        }
        let device = fs::makedev(metadata.stx_dev_major, metadata.stx_dev_minor);
        Ok((
            FileIdentity { device, inode: metadata.stx_ino },
            MountIdentity { device, mount_id: metadata.stx_mnt_id },
        ))
    }

    fn flush(sender: &SyncSender<DeleteEvent>, outcomes: &mut Vec<DeleteOutcome>) -> bool {
        if outcomes.is_empty() {
            return true;
        }
        let values = std::mem::replace(outcomes, Vec::with_capacity(OUTCOME_BATCH_SIZE));
        sender.send(DeleteEvent::Outcomes(values)).is_ok()
    }

    fn retry_errno<T>(mut operation: impl FnMut() -> rustix::io::Result<T>) -> rustix::io::Result<T> {
        loop {
            match operation() {
                Err(Errno::INTR) => continue,
                result => return result,
            }
        }
    }

    fn retry<T>(operation: impl FnMut() -> rustix::io::Result<T>) -> io::Result<T> {
        retry_errno(operation).map_err(io::Error::from)
    }

    fn send_failure(sender: &SyncSender<DeleteEvent>, message: &str) {
        let _ = sender.send(DeleteEvent::Failed {
            message: message.to_owned(),
        });
        let _ = sender.send(DeleteEvent::Finished);
    }
}

#[cfg(test)]
mod tests {
    use super::{create_plan, Disposition};
    use fdu_core::{EntryType, FileIdentity, NodeId, NodeStatus, Tree};
    use std::sync::Arc;

    fn test_tree() -> (Arc<Tree>, NodeId, NodeId, NodeId) {
        let identity = FileIdentity { device: 1, inode: 1 };
        let mut tree = Tree::new(b"root", identity, 1).unwrap();
        let directory = tree
            .append(tree.root(), b"dir", EntryType::Directory, FileIdentity { device: 1, inode: 2 }, 2, 10, 512, NodeStatus::Complete)
            .unwrap();
        let file = tree
            .append(directory, b"file", EntryType::RegularFile, FileIdentity { device: 1, inode: 3 }, 1, 10, 512, NodeStatus::Complete)
            .unwrap();
        let second = tree
            .append(directory, b"second", EntryType::RegularFile, FileIdentity { device: 1, inode: 4 }, 1, 0, 0, NodeStatus::Complete)
            .unwrap();
        tree.set_state(tree.root(), NodeStatus::Complete);
        (Arc::new(tree), directory, file, second)
    }

    #[test]
    fn plan_emits_directory_descendants_before_their_parent() {
        let (tree, directory, file, second) = test_tree();
        let plan = create_plan(tree, &[directory]).unwrap();
        assert_eq!(plan.targets, vec![file, second, directory]);
        assert!(plan.skipped.is_empty());
        assert_eq!(plan.apparent_bytes, 10);
    }

    #[test]
    fn scan_errors_inside_a_directory_skip_only_that_entry() {
        let (tree, directory, file, second) = test_tree();
        let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
        mutable.set_state(file, NodeStatus::Incomplete);
        mutable.set_state(directory, NodeStatus::Incomplete);
        mutable.set_state(mutable.root(), NodeStatus::Incomplete);
        let plan = create_plan(Arc::new(mutable), &[directory]).unwrap();
        assert_eq!(plan.targets, vec![second]);
        assert_eq!(plan.roots, vec![directory]);
        assert_eq!(plan.disposition[directory.index()], Disposition::Enter);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].node, file);
        assert_eq!(plan.skipped[0].reason, "scan is incomplete");
    }

    #[test]
    fn a_directory_with_nothing_deletable_is_one_skipped_entry() {
        let (tree, directory, file, second) = test_tree();
        let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
        mutable.set_state(file, NodeStatus::Incomplete);
        mutable.set_state(second, NodeStatus::Incomplete);
        mutable.set_state(directory, NodeStatus::Incomplete);
        let top_level = mutable
            .append(mutable.root(), b"top", EntryType::RegularFile, FileIdentity { device: 1, inode: 9 }, 1, 5, 512, NodeStatus::Complete)
            .unwrap();
        let plan = create_plan(Arc::new(mutable), &[directory, top_level]).unwrap();
        assert_eq!(plan.targets, vec![top_level]);
        assert_eq!(plan.roots, vec![top_level]);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].node, directory);
    }

    #[test]
    fn a_selection_with_nothing_deletable_is_rejected_with_each_reason() {
        let (tree, directory, file, _) = test_tree();
        let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
        mutable.set_state(file, NodeStatus::Incomplete);
        mutable.set_state(directory, NodeStatus::Excluded(fdu_core::ExclusionReason::MountBoundary));
        let rejection = create_plan(Arc::new(mutable), &[directory]).err().expect("nothing to delete");
        assert_eq!(rejection.rejected.len(), 1);
        assert_eq!(rejection.rejected[0].node, Some(directory));
        assert_eq!(rejection.rejected[0].reason, "mount boundary is protected");
    }

    #[test]
    fn incomplete_root_scan_still_plans_complete_entries() {
        let (tree, _, file, _) = test_tree();
        let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
        mutable.set_state(mutable.root(), NodeStatus::Incomplete);
        let plan = create_plan(Arc::new(mutable), &[file]).unwrap();
        assert_eq!(plan.targets, vec![file]);
    }

    #[test]
    fn running_scan_plans_finished_subtrees_and_skips_directories_still_scanning() {
        let (tree, directory, file, second) = test_tree();
        let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
        let scanning = mutable
            .append(mutable.root(), b"scanning", EntryType::Directory, FileIdentity { device: 1, inode: 9 }, 2, 0, 0, NodeStatus::Scanning)
            .unwrap();
        mutable.set_state(mutable.root(), NodeStatus::Scanning);
        let plan = create_plan(Arc::new(mutable), &[directory, scanning]).unwrap();
        assert_eq!(plan.targets, vec![file, second, directory]);
        assert_eq!(plan.disposition[scanning.index()], Disposition::Retain);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].node, scanning);
        assert_eq!(plan.skipped[0].reason, "scan is still in progress");
    }

    #[test]
    fn excluded_entries_are_skipped_and_never_entered() {
        for reason in [fdu_core::ExclusionReason::MountBoundary, fdu_core::ExclusionReason::UnsupportedAlias] {
            let (tree, directory, file, second) = test_tree();
            let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
            let nested = mutable
                .append(directory, b"nested", EntryType::Directory, FileIdentity { device: 1, inode: 8 }, 2, 0, 0, NodeStatus::Excluded(reason))
                .unwrap();
            let plan = create_plan(Arc::new(mutable), &[directory]).unwrap();
            assert_eq!(plan.targets, vec![file, second]);
            assert_eq!(plan.disposition[nested.index()], Disposition::Retain);
            assert_eq!(plan.disposition[directory.index()], Disposition::Enter);
            assert_eq!(plan.skipped.len(), 1);
            assert_eq!(plan.skipped[0].node, nested);
        }
    }

    #[test]
    fn plan_accepts_roots_from_different_directories() {
        let (tree, _, file, _) = test_tree();
        let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
        let top_level = mutable
            .append(mutable.root(), b"top", EntryType::RegularFile, FileIdentity { device: 1, inode: 9 }, 1, 5, 512, NodeStatus::Complete)
            .unwrap();
        let tree = Arc::new(mutable);
        let plan = create_plan(tree, &[file, top_level]).unwrap();
        assert_eq!(plan.roots, vec![file, top_level]);
        assert_eq!(plan.operation_count(), 2);
        assert_eq!(plan.apparent_bytes, 15);
    }

    #[test]
    fn plan_rejects_a_root_covered_by_another_selected_directory() {
        let (tree, directory, file, _) = test_tree();
        let rejection = create_plan(tree, &[directory, file]).err().expect("selection is rejected");
        assert_eq!(rejection.rejected.len(), 1);
        assert_eq!(rejection.rejected[0].node, Some(file));
        assert!(rejection.rejected[0].reason.contains("covered"));
    }

    #[test]
    fn plan_skips_ineligible_roots_and_keeps_the_rest() {
        let (tree, _, file, _) = test_tree();
        let mut mutable = Arc::try_unwrap(tree).ok().unwrap();
        let blocked = mutable
            .append(mutable.root(), b"blocked", EntryType::RegularFile, FileIdentity { device: 1, inode: 9 }, 1, 5, 512, NodeStatus::Incomplete)
            .unwrap();
        let tree = Arc::new(mutable);
        let plan = create_plan(tree, &[file, blocked]).unwrap();
        assert_eq!(plan.roots, vec![file]);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].node, blocked);
    }

    mod filesystem {
        use crate::posix::{run_with, run_with_capture_hook};
        use crate::{create_plan, DeleteEvent, DeleteOutcome, DeletionPlan, OutcomeKind};
        use fdu_core::{EntryType, FileIdentity, NodeId, NodeStatus, Tree};
        use rustix::fs::{Mode, OFlags};
        use std::fs;
        use std::io;
        use std::os::fd::OwnedFd;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::MetadataExt;
        use std::path::{Path, PathBuf};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::mpsc;
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        static TEMP_ID: AtomicUsize = AtomicUsize::new(0);

        struct TempRoot {
            parent: PathBuf,
            root: PathBuf,
        }

        impl TempRoot {
            fn new() -> io::Result<Self> {
                loop {
                    let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
                    let parent = std::env::temp_dir().join(format!("fdu-delete-{}-{id}", std::process::id()));
                    match fs::create_dir(&parent) {
                        Ok(()) => {
                            let root = parent.join("root");
                            fs::create_dir(&root)?;
                            return Ok(Self { parent, root });
                        }
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                        Err(error) => return Err(error),
                    }
                }
            }
        }

        impl Drop for TempRoot {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.parent);
            }
        }

        fn append_path(tree: &mut Tree, parent: NodeId, path: &Path, name: &[u8]) -> io::Result<NodeId> {
            let metadata = fs::symlink_metadata(path)?;
            let file_type = metadata.file_type();
            let entry_type = if file_type.is_dir() {
                EntryType::Directory
            } else if file_type.is_symlink() {
                EntryType::Symlink
            } else if file_type.is_file() {
                EntryType::RegularFile
            } else {
                EntryType::Other
            };
            let identity = FileIdentity { device: metadata.dev(), inode: metadata.ino() };
            let apparent = if entry_type == EntryType::Directory { 0 } else { metadata.len() };
            let allocated = if entry_type == EntryType::Directory {
                0
            } else {
                metadata.blocks().checked_mul(512).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "allocated size overflow"))?
            };
            let id = tree
                .append(parent, name, entry_type, identity, metadata.nlink(), apparent, allocated, NodeStatus::Complete)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "index exhausted"))?;
            if entry_type == EntryType::Directory {
                let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
                entries.sort_by(|left, right| left.file_name().as_bytes().cmp(right.file_name().as_bytes()));
                for entry in entries {
                    append_path(tree, id, &entry.path(), entry.file_name().as_bytes())?;
                }
            }
            assert!(tree.add_to_ancestors(parent, apparent, allocated, 1));
            Ok(id)
        }

        fn index_root(root: &Path) -> io::Result<Arc<Tree>> {
            let metadata = fs::symlink_metadata(root)?;
            let mut tree = Tree::new(
                root.as_os_str().as_bytes(),
                FileIdentity { device: metadata.dev(), inode: metadata.ino() },
                metadata.nlink(),
            )
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "root name exceeds index limits"))?;
            let mut entries = fs::read_dir(root)?.collect::<Result<Vec<_>, _>>()?;
            entries.sort_by(|left, right| left.file_name().as_bytes().cmp(right.file_name().as_bytes()));
            let root_id = tree.root();
            for entry in entries {
                append_path(&mut tree, root_id, &entry.path(), entry.file_name().as_bytes())?;
            }
            tree.set_state(tree.root(), NodeStatus::Complete);
            Ok(Arc::new(tree))
        }

        fn child(tree: &Tree, parent: NodeId, name: &[u8]) -> NodeId {
            tree.children(parent).find(|id| tree.name(*id) == Some(name)).expect("fixture entry is indexed")
        }

        fn root_fd(root: &Path) -> io::Result<OwnedFd> {
            Ok(rustix::fs::open(
                root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?)
        }

        fn append_directory_at(
            tree: &mut Tree,
            parent_node: NodeId,
            parent_fd: &OwnedFd,
            name: &[u8],
        ) -> io::Result<(NodeId, OwnedFd)> {
            rustix::fs::mkdirat(parent_fd, name, Mode::RWXU)?;
            let directory = rustix::fs::openat(
                parent_fd,
                name,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
            let metadata = fs::File::from(directory.try_clone()?).metadata()?;
            let id = tree
                .append(
                    parent_node,
                    name,
                    EntryType::Directory,
                    FileIdentity { device: metadata.dev(), inode: metadata.ino() },
                    metadata.nlink(),
                    0,
                    0,
                    NodeStatus::Complete,
                )
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "tree index exhausted"))?;
            Ok((id, directory))
        }

        struct DeepFixtureCleanup(PathBuf);

        impl Drop for DeepFixtureCleanup {
            fn drop(&mut self) {
                if let Err(error) = clear_deep_fixture(&self.0) {
                    eprintln!("could not clean deep deletion fixture: {error}");
                }
            }
        }

        fn clear_deep_fixture(root: &Path) -> io::Result<()> {
            let root_fd = root_fd(root)?;
            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let mut current = match rustix::fs::openat(&root_fd, "deep", flags, Mode::empty()) {
                Ok(fd) => fd,
                Err(rustix::io::Errno::NOENT) => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            let mut depth = 0usize;
            loop {
                match rustix::fs::openat(&current, "a", flags, Mode::empty()) {
                    Ok(child) => {
                        current = child;
                        depth += 1;
                    }
                    Err(rustix::io::Errno::NOENT) => break,
                    Err(error) => return Err(error.into()),
                }
            }
            match rustix::fs::unlinkat(&current, "leaf", rustix::fs::AtFlags::empty()) {
                Ok(()) | Err(rustix::io::Errno::NOENT) => {}
                Err(error) => return Err(error.into()),
            }
            while depth != 0 {
                let parent = rustix::fs::openat(&current, c"..", flags, Mode::empty())?;
                rustix::fs::unlinkat(&parent, "a", rustix::fs::AtFlags::REMOVEDIR)?;
                current = parent;
                depth -= 1;
            }
            rustix::fs::unlinkat(&root_fd, "deep", rustix::fs::AtFlags::REMOVEDIR)?;
            Ok(())
        }

        fn execute_plan(
            root: &Path,
            plan: DeletionPlan,
            cancel_after: Option<usize>,
        ) -> Vec<DeleteOutcome> {
            let cancelled = Arc::new(AtomicBool::new(false));
            let hook_cancelled = Arc::clone(&cancelled);
            let (sender, receiver) = mpsc::sync_channel(2);
            let root = root_fd(root).unwrap();
            let worker = thread::spawn(move || {
                // One thread makes "stop after exactly this many operations" deterministic.
                let threads = if cancel_after.is_some() { 1 } else { 4 };
                run_with(root, plan, cancelled, sender, threads, move |completed| {
                    if cancel_after == Some(completed) {
                        hook_cancelled.store(true, Ordering::Relaxed);
                    }
                });
            });
            let mut outcomes = Vec::new();
            loop {
                match receiver.recv_timeout(Duration::from_secs(5)).expect("deletion worker reports completion") {
                    DeleteEvent::Outcomes(batch) => outcomes.extend(batch),
                    DeleteEvent::Failed { message } => panic!("unexpected worker failure: {message}"),
                    DeleteEvent::Progress { .. } => {}
                    DeleteEvent::Finished => break,
                }
            }
            worker.join().expect("deletion worker exits");
            outcomes
        }

        fn execute_with_replacement(
            root: &Path,
            plan: DeletionPlan,
            target: NodeId,
            directory: bool,
        ) -> io::Result<(Vec<DeleteOutcome>, FileIdentity, fs::File)> {
            let cancelled = Arc::new(AtomicBool::new(false));
            let (sender, receiver) = mpsc::sync_channel(8);
            let (reached_sender, reached_receiver) = mpsc::sync_channel(1);
            let after_metadata = Arc::new(std::sync::Barrier::new(2));
            let replacement_ready = Arc::new(std::sync::Barrier::new(2));
            let worker_after_metadata = Arc::clone(&after_metadata);
            let worker_replacement_ready = Arc::clone(&replacement_ready);
            let root = root.to_path_buf();
            let worker_root = root.clone();
            let worker = thread::spawn(move || {
                run_with_capture_hook(
                    root_fd(&worker_root).unwrap(),
                    plan,
                    cancelled,
                    sender,
                    1,
                    |_| {},
                    move |node| {
                        if node == target {
                            reached_sender.send(()).expect("test runner receives the unlink boundary");
                            worker_after_metadata.wait();
                            worker_replacement_ready.wait();
                        }
                    },
                );
            });

            if reached_receiver.recv_timeout(Duration::from_secs(5)).is_err() {
                worker.join().expect("deletion worker exits when the boundary is not reached");
                return Err(io::Error::new(io::ErrorKind::TimedOut, "deletion worker did not reach unlink boundary"));
            }

            let target_path = root.join("target");
            let indexed_path = root.join("indexed-original");
            let replacement_result: io::Result<(FileIdentity, fs::File)> = (|| {
                fs::rename(&target_path, &indexed_path)?;
                if directory {
                    fs::create_dir(&target_path)?;
                } else {
                    fs::write(&target_path, b"unindexed replacement")?;
                }
                let held = fs::File::open(&target_path)?;
                let metadata = held.metadata()?;
                Ok((FileIdentity { device: metadata.dev(), inode: metadata.ino() }, held))
            })();
            after_metadata.wait();
            replacement_ready.wait();

            let mut outcomes = Vec::new();
            let mut execution_error = None;
            loop {
                match receiver.recv_timeout(Duration::from_secs(5)) {
                    Ok(DeleteEvent::Outcomes(batch)) => outcomes.extend(batch),
                    Ok(DeleteEvent::Failed { message }) => execution_error = Some(message),
                    Ok(DeleteEvent::Progress { .. }) => {}
                    Ok(DeleteEvent::Finished) => break,
                    Err(error) => {
                        execution_error = Some(format!("deletion worker did not finish: {error}"));
                        break;
                    }
                }
            }
            worker.join().expect("deletion worker exits after unlink boundary release");
            if let Some(message) = execution_error {
                return Err(io::Error::other(message));
            }
            let (replacement_identity, replacement_handle) = replacement_result?;
            Ok((outcomes, replacement_identity, replacement_handle))
        }

        fn outcome(outcomes: &[DeleteOutcome], node: NodeId) -> OutcomeKind {
            outcomes.iter().find(|item| item.node == node).expect("target has an outcome").kind
        }

        #[test]
        fn cleanup_failure_preserves_completed_removal_outcomes() -> io::Result<()> {
            let temp = TempRoot::new()?;
            fs::write(temp.root.join("target"), b"indexed file")?;
            let tree = index_root(&temp.root)?;
            let target = child(&tree, tree.root(), b"target");
            let plan = create_plan(tree, &[target]).unwrap();
            let (sender, receiver) = mpsc::sync_channel(8);
            run_with(root_fd(&temp.root)?, plan, Arc::new(AtomicBool::new(false)), sender, 1, |_| {
                let workspace = fs::read_dir(&temp.root).unwrap().map(Result::unwrap)
                    .find(|entry| entry.file_name().as_bytes().starts_with(b".fdu-quarantine-v1-"))
                    .expect("active workspace");
                fs::remove_file(workspace.path().join("header")).unwrap();
            });
            let mut outcomes = Vec::new();
            let mut failed = false;
            for event in receiver {
                match event {
                    DeleteEvent::Outcomes(batch) => {
                        assert!(!failed, "outcomes precede terminal cleanup failure");
                        outcomes.extend(batch);
                    }
                    DeleteEvent::Failed { .. } => failed = true,
                    DeleteEvent::Progress { .. } | DeleteEvent::Finished => {}
                }
            }
            assert!(failed);
            assert_eq!(outcomes.len(), 1);
            assert_eq!(outcome(&outcomes, target), OutcomeKind::Deleted);
            assert!(!temp.root.join("target").exists());
            Ok(())
        }

        #[test]
        fn file_replacement_after_metadata_check_is_not_deleted() -> io::Result<()> {
            let temp = TempRoot::new()?;
            fs::write(temp.root.join("target"), b"indexed file")?;
            let tree = index_root(&temp.root)?;
            let target = child(&tree, tree.root(), b"target");
            let indexed_identity = tree.record(target).expect("target is indexed").identity;
            let plan = create_plan(Arc::clone(&tree), &[target]).unwrap();
            let (outcomes, replacement_identity, replacement_handle) =
                execute_with_replacement(&temp.root, plan, target, false)?;

            assert_ne!(indexed_identity, replacement_identity);
            let held_metadata = replacement_handle.metadata()?;
            assert_eq!(held_metadata.dev(), replacement_identity.device);
            assert_eq!(held_metadata.ino(), replacement_identity.inode);
            assert_ne!(
                outcome(&outcomes, target),
                OutcomeKind::Deleted,
                "unindexed replacement was deleted; indexed identity {indexed_identity:?}, replacement identity {replacement_identity:?}, held FD identity {}:{}",
                held_metadata.dev(),
                held_metadata.ino()
            );
            assert_eq!(fs::read(temp.root.join("target"))?, b"unindexed replacement");
            assert_eq!(fs::read(temp.root.join("indexed-original"))?, b"indexed file");
            drop(replacement_handle);
            Ok(())
        }

        #[test]
        fn empty_directory_replacement_after_metadata_check_is_not_deleted() -> io::Result<()> {
            let temp = TempRoot::new()?;
            fs::create_dir(temp.root.join("target"))?;
            let tree = index_root(&temp.root)?;
            let target = child(&tree, tree.root(), b"target");
            let indexed_identity = tree.record(target).expect("target is indexed").identity;
            let plan = create_plan(Arc::clone(&tree), &[target]).unwrap();
            let (outcomes, replacement_identity, replacement_handle) =
                execute_with_replacement(&temp.root, plan, target, true)?;

            assert_ne!(indexed_identity, replacement_identity);
            let held_metadata = replacement_handle.metadata()?;
            assert_eq!(held_metadata.dev(), replacement_identity.device);
            assert_eq!(held_metadata.ino(), replacement_identity.inode);
            assert_ne!(
                outcome(&outcomes, target),
                OutcomeKind::Deleted,
                "unindexed replacement was deleted; indexed identity {indexed_identity:?}, replacement identity {replacement_identity:?}, held FD identity {}:{}",
                held_metadata.dev(),
                held_metadata.ino()
            );
            assert_eq!(fs::metadata(temp.root.join("target"))?.ino(), replacement_identity.inode);
            assert!(temp.root.join("indexed-original").is_dir());
            assert_eq!(fs::read_dir(temp.root.join("target"))?.count(), 0);
            drop(replacement_handle);
            Ok(())
        }

        #[test]
        fn indexed_group_deletion_removes_symlinks_themselves_and_keeps_external_hardlinks() -> io::Result<()> {
            let temp = TempRoot::new()?;
            let group = temp.root.join("group");
            fs::create_dir(&group)?;
            fs::create_dir(group.join("sub"))?;
            fs::write(group.join("sub/payload"), b"group data")?;
            let external_hardlink = temp.parent.join("outside-hardlink");
            fs::hard_link(group.join("sub/payload"), &external_hardlink)?;
            let sentinel = temp.parent.join("sentinel");
            fs::write(&sentinel, b"independent target")?;
            std::os::unix::fs::symlink("../../sentinel", group.join("sentinel-link"))?;

            let tree = index_root(&temp.root)?;
            let selected = child(&tree, tree.root(), b"group");
            let expected_link = child(&tree, selected, b"sentinel-link");
            let plan = create_plan(Arc::clone(&tree), &[selected]).unwrap();
            assert_eq!(plan.operation_count(), 4);
            let outcomes = execute_plan(&temp.root, plan, None);
            assert!(outcomes.iter().all(|item| item.kind == OutcomeKind::Deleted));
            assert!(!group.exists());
            assert_eq!(fs::read(&sentinel)?, b"independent target");
            assert!(external_hardlink.exists());
            assert_eq!(outcome(&outcomes, expected_link), OutcomeKind::Deleted);
            Ok(())
        }

        #[test]
        fn individual_hardlink_selection_removes_only_the_selected_name() -> io::Result<()> {
            let temp = TempRoot::new()?;
            let first = temp.root.join("first");
            let second = temp.root.join("second");
            fs::write(&first, b"shared inode")?;
            fs::hard_link(&first, &second)?;
            let tree = index_root(&temp.root)?;
            let first_id = child(&tree, tree.root(), b"first");
            let second_id = child(&tree, tree.root(), b"second");
            assert_ne!(first_id, second_id);
            assert_eq!(tree.record(first_id).unwrap().identity, tree.record(second_id).unwrap().identity);
            let plan = create_plan(Arc::clone(&tree), &[first_id]).unwrap();
            let outcomes = execute_plan(&temp.root, plan, None);
            assert_eq!(outcome(&outcomes, first_id), OutcomeKind::Deleted);
            assert!(!first.exists());
            assert_eq!(fs::read(second)?, b"shared inode");
            Ok(())
        }

        #[test]
        fn absent_and_replaced_entries_are_distinguished_without_deleting_replacements() -> io::Result<()> {
            let temp = TempRoot::new()?;
            let absent = temp.root.join("absent");
            let replacement_path = temp.parent.join("replacement-source");
            fs::write(&absent, b"old")?;
            fs::write(&replacement_path, b"new")?;
            let tree = index_root(&temp.root)?;
            let absent_id = child(&tree, tree.root(), b"absent");
            let plan = create_plan(Arc::clone(&tree), &[absent_id]).unwrap();
            fs::remove_file(&absent)?;
            let outcomes = execute_plan(&temp.root, plan, None);
            assert_eq!(outcome(&outcomes, absent_id), OutcomeKind::AlreadyAbsent);

            let target = temp.root.join("target");
            fs::write(&target, b"indexed original")?;
            let tree = index_root(&temp.root)?;
            let target_id = child(&tree, tree.root(), b"target");
            let plan = create_plan(Arc::clone(&tree), &[target_id]).unwrap();
            let replacement = temp.parent.join("replacement-new");
            fs::write(&replacement, b"replacement stays")?;
            fs::rename(replacement, &target)?;
            let outcomes = execute_plan(&temp.root, plan, None);
            assert_eq!(outcome(&outcomes, target_id), OutcomeKind::Changed);
            assert_eq!(fs::read(&target)?, b"replacement stays");
            Ok(())
        }

        #[test]
        fn selections_from_several_directories_are_removed_together() -> io::Result<()> {
            let temp = TempRoot::new()?;
            fs::create_dir(temp.root.join("left"))?;
            fs::create_dir(temp.root.join("right"))?;
            fs::write(temp.root.join("left/a"), b"a")?;
            fs::write(temp.root.join("left/keep"), b"keep")?;
            fs::write(temp.root.join("right/b"), b"b")?;
            fs::write(temp.root.join("top"), b"top")?;
            let tree = index_root(&temp.root)?;
            let left = child(&tree, tree.root(), b"left");
            let right = child(&tree, tree.root(), b"right");
            let a = child(&tree, left, b"a");
            let b = child(&tree, right, b"b");
            let top = child(&tree, tree.root(), b"top");
            let plan = create_plan(Arc::clone(&tree), &[a, b, top]).unwrap();
            let outcomes = execute_plan(&temp.root, plan, None);
            assert!(outcomes.iter().all(|item| item.kind == OutcomeKind::Deleted));
            assert!(!temp.root.join("left/a").exists());
            assert!(!temp.root.join("right/b").exists());
            assert!(!temp.root.join("top").exists());
            assert!(temp.root.join("left/keep").exists());
            Ok(())
        }

        #[test]
        fn nested_directories_and_siblings_are_removed() -> io::Result<()> {
            let temp = TempRoot::new()?;
            let deep = temp.root.join("a/b/c/d");
            fs::create_dir_all(&deep)?;
            fs::create_dir_all(temp.root.join("a/b/side"))?;
            for directory in ["a", "a/b", "a/b/c", "a/b/c/d", "a/b/side"] {
                fs::write(temp.root.join(directory).join("one"), b"1")?;
                fs::write(temp.root.join(directory).join("two"), b"22")?;
            }
            let tree = index_root(&temp.root)?;
            let top = child(&tree, tree.root(), b"a");
            let plan = create_plan(Arc::clone(&tree), &[top]).unwrap();
            assert_eq!(plan.operation_count(), 15);
            let outcomes = execute_plan(&temp.root, plan, None);
            assert_eq!(outcomes.len(), 15);
            assert!(outcomes.iter().all(|item| item.kind == OutcomeKind::Deleted), "{outcomes:?}");
            assert!(!temp.root.join("a").exists());
            Ok(())
        }

        #[test]
        fn deep_paths_can_be_selected_and_their_directories_removed() -> io::Result<()> {
            const DEPTH: usize = 1000;

            let temp = TempRoot::new()?;
            let _cleanup = DeepFixtureCleanup(temp.root.clone());
            let root_metadata = fs::symlink_metadata(&temp.root)?;
            let mut tree = Tree::new(
                temp.root.as_os_str().as_bytes(),
                FileIdentity { device: root_metadata.dev(), inode: root_metadata.ino() },
                root_metadata.nlink(),
            )
            .unwrap();
            let mut parent_node = tree.root();
            let mut parent_fd = root_fd(&temp.root)?;
            let (selected, directory) = append_directory_at(&mut tree, parent_node, &parent_fd, b"deep")?;
            parent_node = selected;
            parent_fd = directory;

            for _ in 0..DEPTH {
                let (child, directory) = append_directory_at(&mut tree, parent_node, &parent_fd, b"a")?;
                parent_node = child;
                parent_fd = directory;
            }
            let leaf = rustix::fs::openat(
                &parent_fd,
                "leaf",
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?;
            let metadata = fs::File::from(leaf.try_clone()?).metadata()?;
            let leaf = tree
                .append(
                    parent_node,
                    b"leaf",
                    EntryType::RegularFile,
                    FileIdentity { device: metadata.dev(), inode: metadata.ino() },
                    metadata.nlink(),
                    metadata.len(),
                    metadata.blocks() * 512,
                    NodeStatus::Complete,
                )
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "tree index exhausted"))?;
            tree.set_state(tree.root(), NodeStatus::Complete);

            let tree = Arc::new(tree);
            let leaf_plan = create_plan(Arc::clone(&tree), &[leaf]).unwrap();
            let leaf_outcomes = execute_plan(&temp.root, leaf_plan, None);
            assert_eq!(outcome(&leaf_outcomes, leaf), OutcomeKind::Deleted);

            let plan = create_plan(Arc::clone(&tree), &[selected]).unwrap();
            assert_eq!(plan.operation_count(), DEPTH + 2);
            let outcomes = execute_plan(&temp.root, plan, None);

            assert_eq!(outcomes.len(), DEPTH + 2);
            assert_eq!(outcome(&outcomes, leaf), OutcomeKind::AlreadyAbsent);
            let failure = outcomes.iter().find(|item| item.node != leaf && item.kind != OutcomeKind::Deleted);
            assert!(failure.is_none(), "first non-deleted directory outcome: {failure:?}");
            assert!(fs::read_dir(&temp.root)?.next().is_none());
            Ok(())
        }

        #[test]
        fn new_children_after_confirmation_leave_the_directory_visible() -> io::Result<()> {
            let temp = TempRoot::new()?;
            let directory = temp.root.join("selected");
            fs::create_dir(&directory)?;
            fs::write(directory.join("indexed"), b"known")?;
            let tree = index_root(&temp.root)?;
            let selected = child(&tree, tree.root(), b"selected");
            let plan = create_plan(Arc::clone(&tree), &[selected]).unwrap();
            fs::write(directory.join("new-child"), b"not in the frozen plan")?;

            let outcomes = execute_plan(&temp.root, plan, None);
            assert_eq!(outcome(&outcomes, selected), OutcomeKind::Failed);
            assert!(directory.join("new-child").exists());
            assert!(directory.exists());
            assert!(!directory.join("indexed").exists());
            Ok(())
        }

        #[test]
        fn replacement_ancestors_are_not_followed_or_removed() -> io::Result<()> {
            let temp = TempRoot::new()?;
            let directory = temp.root.join("selected");
            fs::create_dir(&directory)?;
            fs::write(directory.join("indexed"), b"inside")?;
            let outside = temp.parent.join("moved-outside");
            let tree = index_root(&temp.root)?;
            let selected = child(&tree, tree.root(), b"selected");
            let child_id = child(&tree, selected, b"indexed");
            let plan = create_plan(Arc::clone(&tree), &[selected]).unwrap();
            fs::rename(&directory, &outside)?;
            fs::create_dir(&directory)?;
            fs::write(directory.join("replacement"), b"leave me")?;

            let outcomes = execute_plan(&temp.root, plan, None);
            assert_eq!(outcome(&outcomes, child_id), OutcomeKind::Failed);
            assert_eq!(outcome(&outcomes, selected), OutcomeKind::Changed);
            assert_eq!(fs::read(outside.join("indexed"))?, b"inside");
            assert_eq!(fs::read(directory.join("replacement"))?, b"leave me");
            Ok(())
        }

        #[test]
        fn cancellation_stops_after_one_successful_removal() -> io::Result<()> {
            let temp = TempRoot::new()?;
            fs::write(temp.root.join("first"), b"first")?;
            fs::write(temp.root.join("second"), b"second")?;
            let tree = index_root(&temp.root)?;
            let first = child(&tree, tree.root(), b"first");
            let second = child(&tree, tree.root(), b"second");
            let plan = create_plan(Arc::clone(&tree), &[first, second]).unwrap();
            let outcomes = execute_plan(&temp.root, plan, Some(1));
            assert_eq!(outcome(&outcomes, first), OutcomeKind::Deleted);
            assert_eq!(outcome(&outcomes, second), OutcomeKind::Cancelled);
            assert!(!temp.root.join("first").exists());
            assert_eq!(fs::read(temp.root.join("second"))?, b"second");
            Ok(())
        }

        /// A tree with deep chains, fan-outs of small directories, and wide directories of files,
        /// so deletion splits into many tasks of very different sizes.
        fn build_mixed_tree(root: &Path) -> io::Result<usize> {
            let mut entries = 0;
            let mut make = |path: &Path, directory: bool| -> io::Result<()> {
                if directory {
                    fs::create_dir(path)?;
                } else {
                    fs::write(path, b"x")?;
                }
                entries += 1;
                Ok(())
            };
            for top in 0..10 {
                let top_dir = root.join(format!("top-{top:02}"));
                make(&top_dir, true)?;
                let mut chain = top_dir.join("chain");
                make(&chain, true)?;
                for level in 0..7 {
                    chain = chain.join(format!("level-{level}"));
                    make(&chain, true)?;
                    make(&chain.join("file"), false)?;
                }
                let fan = top_dir.join("fan");
                make(&fan, true)?;
                for branch in 0..25 {
                    let branch_dir = fan.join(format!("branch-{branch:02}"));
                    make(&branch_dir, true)?;
                    for leaf in 0..2 {
                        make(&branch_dir.join(format!("leaf-{leaf}")), false)?;
                    }
                }
                let wide = top_dir.join("wide");
                make(&wide, true)?;
                for file in 0..120 {
                    make(&wide.join(format!("file-{file:03}")), false)?;
                }
            }
            Ok(entries)
        }

        /// Many deletions at once, each with its own pool, over trees that split into tasks of very
        /// different sizes: every entry is removed once, nothing hangs, and nothing is left. Raise
        /// FDU_STRESS_DELETES for a soak.
        #[test]
        fn concurrent_deletions_remove_every_entry_exactly_once() -> io::Result<()> {
            let rounds = std::env::var("FDU_STRESS_DELETES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(6);
            thread::scope(|scope| {
                let threads = (0..4)
                    .map(|_| {
                        scope.spawn(|| -> io::Result<()> {
                            for _ in 0..rounds {
                                let temp = TempRoot::new()?;
                                let expected = build_mixed_tree(&temp.root)?;
                                let tree = index_root(&temp.root)?;
                                let selected = tree.children(tree.root()).collect::<Vec<_>>();
                                let plan = create_plan(Arc::clone(&tree), &selected).unwrap();
                                assert_eq!(plan.operation_count(), expected);
                                let outcomes = execute_plan(&temp.root, plan, None);
                                assert_eq!(outcomes.len(), expected, "one outcome per entry");
                                assert!(
                                    outcomes.iter().all(|item| item.kind == OutcomeKind::Deleted),
                                    "{:?}",
                                    outcomes.iter().find(|item| item.kind != OutcomeKind::Deleted)
                                );
                                assert_eq!(fs::read_dir(&temp.root)?.count(), 0, "nothing is left");
                            }
                            Ok(())
                        })
                    })
                    .collect::<Vec<_>>();
                for thread in threads {
                    thread.join().expect("stress thread does not panic")?;
                }
                Ok(())
            })
        }

        #[test]
        fn entries_with_scan_errors_are_left_in_place_and_everything_else_is_removed() -> io::Result<()> {
            let temp = TempRoot::new()?;
            let selected = temp.root.join("selected");
            fs::create_dir_all(selected.join("unreadable"))?;
            fs::create_dir_all(selected.join("clean/inner"))?;
            fs::write(selected.join("top"), b"1")?;
            fs::write(selected.join("clean/inner/leaf"), b"2")?;
            fs::write(selected.join("unreadable/known"), b"3")?;
            fs::write(selected.join("unreadable/unscanned"), b"4")?;
            let mut tree = Arc::try_unwrap(index_root(&temp.root)?).ok().unwrap();
            let selected_node = child(&tree, tree.root(), b"selected");
            let unreadable = child(&tree, selected_node, b"unreadable");
            let unscanned = child(&tree, unreadable, b"unscanned");
            tree.set_state(unscanned, NodeStatus::Incomplete);
            tree.set_state(unreadable, NodeStatus::Incomplete);
            tree.set_state(selected_node, NodeStatus::Incomplete);
            tree.set_state(tree.root(), NodeStatus::Incomplete);
            let tree = Arc::new(tree);

            let plan = create_plan(Arc::clone(&tree), &[selected_node]).unwrap();
            assert_eq!(plan.skipped.len(), 1);
            let outcomes = execute_plan(&temp.root, plan, None);

            assert!(outcomes.iter().all(|item| item.kind == OutcomeKind::Deleted), "{outcomes:?}");
            assert_eq!(outcomes.len(), 5, "top, leaf, inner, clean, known");
            assert!(!selected.join("top").exists());
            assert!(!selected.join("clean").exists());
            assert!(!selected.join("unreadable/known").exists());
            assert_eq!(fs::read(selected.join("unreadable/unscanned"))?, b"4");
            Ok(())
        }
    }
}
