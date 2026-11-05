use fdu_core::{NodeState, NodeStatus, ScanEvent, Tree};
use fdu_delete::{DeleteEvent, OutcomeKind, create_plan};
use fdu_scan::{RootAnchor, open_root_no_follow, start_indexed_scan};
use std::error::Error;
use std::ffi::OsStr;
use std::io::{self, BufRead, IsTerminal, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const SCAN_EVENT_CHANNEL_CAPACITY: usize = 512;
const SCAN_BATCH_CAPACITY: usize = 1024;
const SCAN_BATCH_POOL_CAPACITY: usize = 8;
const DELETE_CHANNEL_CAPACITY: usize = 8;
const PROGRESS_REDRAW_INTERVAL: Duration = Duration::from_millis(50);
const PROGRESS_BAR_WIDTH: usize = 30;

/// A live status line on stderr, shown only when stderr is a terminal so that
/// redirected output and logs stay clean. The line is erased when the value is
/// dropped, so no exit path leaves it behind to be overwritten by an error.
struct Progress {
    enabled: bool,
    drawn: bool,
    last_draw: Option<Instant>,
}

impl Progress {
    fn new() -> Self {
        Self { enabled: io::stderr().is_terminal(), drawn: false, last_draw: None }
    }

    /// The scan has no known total, so it reports a running count.
    fn scanning(&mut self, entries: usize) {
        self.draw(|| format!("Scanning... {entries} entries"));
    }

    fn deleting(&mut self, completed: usize, total: usize) {
        self.draw(|| {
            let fraction = if total == 0 { 1.0 } else { completed as f64 / total as f64 };
            let filled = ((fraction * PROGRESS_BAR_WIDTH as f64) as usize).min(PROGRESS_BAR_WIDTH);
            format!(
                "Deleting [{}{}] {:>3}% {completed}/{total}",
                "#".repeat(filled),
                "-".repeat(PROGRESS_BAR_WIDTH - filled),
                (fraction * 100.0) as usize,
            )
        });
    }

    fn draw(&mut self, line: impl FnOnce() -> String) {
        if !self.enabled || self.last_draw.is_some_and(|last| last.elapsed() < PROGRESS_REDRAW_INTERVAL) {
            return;
        }
        self.last_draw = Some(Instant::now());
        self.drawn = true;
        // A failed write to a status line must not abort a deletion.
        let _ = write!(io::stderr(), "\r\x1b[2K{}", line()).and_then(|()| io::stderr().flush());
    }

    fn clear(&mut self) {
        if self.drawn {
            let _ = write!(io::stderr(), "\r\x1b[2K").and_then(|()| io::stderr().flush());
            self.drawn = false;
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Deletes every entry directly below `path`, leaving `path` itself in place.
/// The scan builds the same index the browser builds, and removal goes through
/// the descriptor-relative deletion engine with its identity checks.
///
/// `path` must name a directory, not a symlink to one, and must not be `/`, the
/// home directory, or a parent of the current directory. Unless `assume_yes` is
/// set, the resolved target and entry count are shown and the user must type
/// `yes`; without a terminal to ask on, that is an error.
pub fn run(path: PathBuf, assume_yes: bool) -> Result<(), Box<dyn Error>> {
    let root = open_root_no_follow(&path).map_err(|error| describe_open_failure(&path, error))?;
    let confirmation_path = (!assume_yes)
        .then(|| resolve_confirmation_path(&root, &path))
        .transpose()?;
    let cwd = std::env::current_dir()?;
    let home = std::env::var_os("HOME");
    if let Some(reason) = protected_root_reason(&root, &cwd, home.as_deref())? {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("refusing to empty {}: it is {reason}", path.display()),
        )
        .into());
    }
    let mut progress = Progress::new();
    let tree = build_index(&root, &mut progress)?;
    progress.clear();
    require_complete_scan_root(&tree).map_err(|error| {
        io::Error::other(format!("refusing to empty {}: {error}", path.display()))
    })?;
    let selected: Vec<fdu_core::NodeId> = tree.children(tree.root()).collect();
    if selected.is_empty() {
        return Ok(());
    }
    let tree = Arc::new(tree);
    let plan = create_plan(Arc::clone(&tree), &selected).map_err(|error| {
        let mut details = error
            .rejected
            .iter()
            .map(|rejection| match rejection.node {
                Some(node) => format!("{}: {}", display_name(&tree, node), rejection.reason),
                None => rejection.reason.clone(),
            })
            .collect::<Vec<_>>();
        details.sort();
        let details = details.join("; ");
        io::Error::other(format!("{error}: {details}"))
    })?;
    if !plan.skipped.is_empty() {
        let mut details = plan
            .skipped
            .iter()
            .map(|entry| format!("{}: {}", display_name(&tree, entry.node), entry.reason))
            .collect::<Vec<_>>();
        details.sort();
        return Err(io::Error::other(format!(
            "refusing to empty {}: {} entr{} cannot be deleted: {}",
            path.display(),
            details.len(),
            if details.len() == 1 { "y" } else { "ies" },
            details.join("; ")
        ))
        .into());
    }
    let total = plan.operation_count();
    if !assume_yes {
        confirm_interactively(
            &root,
            &path,
            confirmation_path.as_deref().expect("confirmation path is resolved"),
            selected.len(),
            total,
        )?;
    }
    let root_fd = root.try_clone_fd()?;
    let (sender, receiver) = mpsc::sync_channel(DELETE_CHANNEL_CAPACITY);
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = thread::spawn(move || {
        fdu_delete::execute(root_fd, plan, cancelled, sender);
    });
    let mut deleted = 0usize;
    let mut already_absent = 0usize;
    let mut changed = 0usize;
    let mut failed = 0usize;
    let mut not_attempted = 0usize;
    let mut first_problem: Option<String> = None;
    loop {
        match receiver.recv() {
            Ok(DeleteEvent::Outcomes(outcomes)) => {
                for outcome in outcomes {
                    match outcome.kind {
                        OutcomeKind::Deleted => deleted += 1,
                        OutcomeKind::AlreadyAbsent => already_absent += 1,
                        OutcomeKind::Changed => {
                            changed += 1;
                            first_problem.get_or_insert_with(|| {
                                let detail = outcome.message.unwrap_or_else(|| "entry changed".to_owned());
                                format!("{}: {detail}", display_name(&tree, outcome.node))
                            });
                        }
                        OutcomeKind::Failed => {
                            failed += 1;
                            first_problem.get_or_insert_with(|| {
                                let detail = outcome.message.unwrap_or_else(|| "entry could not be removed".to_owned());
                                format!("{}: {detail}", display_name(&tree, outcome.node))
                            });
                        }
                        OutcomeKind::Cancelled => not_attempted += 1,
                    }
                }
            }
            Ok(DeleteEvent::Progress { completed, total, .. }) => progress.deleting(completed, total),
            Ok(DeleteEvent::Failed { message }) => {
                let _ = worker.join();
                return Err(io::Error::other(format!("deletion stopped: {message}")).into());
            }
            Ok(DeleteEvent::Finished) => break,
            Err(_) => {
                let _ = worker.join();
                return Err(io::Error::other("deletion worker stopped without finishing").into());
            }
        }
    }
    let _ = worker.join();
    progress.clear();
    if changed == 0 && failed == 0 && not_attempted == 0 {
        if deleted + already_absent != total {
            return Err(io::Error::other(format!(
                "deletion reported {} outcomes for {total} planned entries",
                deleted + already_absent
            ))
            .into());
        }
        return Ok(());
    }
    let mut summary = format!(
        "Deleted {deleted} · {already_absent} already absent · {changed} changed · {failed} failed · {not_attempted} not attempted"
    );
    if let Some(problem) = first_problem {
        summary.push_str(&format!("; first problem: {problem}"));
    }
    Err(io::Error::other(summary).into())
}

/// A symlink root is refused rather than followed, since emptying the link's
/// target is almost never what a caller who names a link means. The kernel
/// reports that refusal as a bare ELOOP or ENOTDIR, so say what happened.
fn describe_open_failure(path: &Path, error: io::Error) -> io::Error {
    if last_path_component_is_symlink(path) {
        return io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is a symlink; name the directory it points to instead", path.display()),
        );
    }
    io::Error::new(error.kind(), format!("{}: {error}", path.display()))
}

/// `symlink_metadata` follows a final symlink if the path still ends in `/`.
fn last_path_component_is_symlink(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;

    let bytes = path.as_os_str().as_bytes();
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1] == b'/' {
        end -= 1;
    }
    end > 0
        && std::fs::symlink_metadata(Path::new(OsStr::from_bytes(&bytes[..end]))).is_ok_and(|metadata| {
            metadata.file_type().is_symlink()
        })
}

/// Why `root` must never be emptied, if it is one of the directories whose
/// loss would take the user's whole working environment with it. Directories
/// are compared by device and inode, so aliases, bind mounts of the same
/// directory, and `..` spellings cannot slip past. The current directory itself
/// is allowed: naming it explicitly is how a caller empties it.
fn protected_root_reason(
    root: &RootAnchor,
    cwd: &Path,
    home: Option<&OsStr>,
) -> io::Result<Option<&'static str>> {
    let is_root = |candidate: &Path| -> io::Result<bool> {
        let metadata = std::fs::metadata(candidate)
            .map_err(|error| io::Error::new(error.kind(), format!("{}: {error}", candidate.display())))?;
        Ok(metadata.dev() == root.identity.device && metadata.ino() == root.identity.inode)
    };
    if is_root(Path::new("/"))? {
        return Ok(Some("the filesystem root"));
    }
    if let Some(home) = home.filter(|home| !home.is_empty()) {
        // A HOME that does not exist cannot be the directory being emptied.
        match is_root(Path::new(home)) {
            Ok(true) => return Ok(Some("your home directory")),
            Ok(false) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    for parent in cwd.ancestors().skip(1) {
        if is_root(parent)? {
            return Ok(Some("a parent of the current directory"));
        }
    }
    Ok(None)
}

fn resolve_confirmation_path(root: &RootAnchor, path: &Path) -> io::Result<PathBuf> {
    let resolved = std::fs::canonicalize(path)?;
    verify_confirmation_target(root, path)?;
    verify_confirmation_target(root, &resolved)?;
    Ok(resolved)
}

fn verify_confirmation_target(root: &RootAnchor, path: &Path) -> io::Result<()> {
    if last_path_component_is_symlink(path) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "selected path became a symlink after it was opened",
        ));
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.dev() != root.identity.device || metadata.ino() != root.identity.inode {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "selected path no longer resolves to the opened directory",
        ));
    }
    Ok(())
}

fn confirm_interactively(
    root: &RootAnchor,
    selected_path: &Path,
    resolved_path: &Path,
    top_level: usize,
    total: usize,
) -> io::Result<()> {
    if !io::stdin().is_terminal() {
        return Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "cannot ask for confirmation without a terminal; pass --yes to delete without asking",
        ));
    }
    verify_confirmation_target(root, selected_path)?;
    verify_confirmation_target(root, resolved_path)?;
    let mut stderr = io::stderr().lock();
    write!(
        stderr,
        "Permanently delete {top_level} entries ({total} including nested) below {}? \
         The directory itself is kept. Type 'yes' to continue: ",
        resolved_path.display()
    )?;
    stderr.flush()?;
    if read_confirmation(&mut io::stdin().lock())? {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::Interrupted, "not confirmed; nothing was deleted"))
    }
}

/// Only the exact answer `yes` approves; anything else, including end of
/// input, declines.
fn read_confirmation(input: &mut impl BufRead) -> io::Result<bool> {
    let mut answer = String::new();
    input.read_line(&mut answer)?;
    Ok(answer.trim() == "yes")
}

fn display_name(tree: &Tree, id: fdu_core::NodeId) -> String {
    match tree.name(id) {
        Some(name) => String::from_utf8_lossy(name).into_owned(),
        None => "<unknown>".to_owned(),
    }
}

/// The root is outside the deletion plan, so its state must be checked separately.
fn require_complete_scan_root(tree: &Tree) -> io::Result<()> {
    if tree.record(tree.root()).expect("root exists").state != NodeState::Complete {
        return Err(io::Error::other("root scan is incomplete"));
    }
    Ok(())
}

/// Runs the indexed scanner to completion and folds its events into the tree
/// the same way the browser does, so the deletion plan sees identical states
/// and totals.
fn build_index(root: &RootAnchor, progress: &mut Progress) -> io::Result<Tree> {
    let mut tree = Tree::new(&root.name, root.identity, root.link_count)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "root path exceeds index limits"))?;
    let mut token_nodes = vec![Some(tree.root())];
    let (sender, receiver) = mpsc::sync_channel(SCAN_EVENT_CHANNEL_CAPACITY);
    let (batch_sender, batch_receiver) = mpsc::sync_channel(SCAN_BATCH_POOL_CAPACITY);
    for _ in 0..SCAN_BATCH_POOL_CAPACITY {
        let batch = fdu_core::EntryBatch::with_capacity(SCAN_BATCH_CAPACITY, SCAN_BATCH_CAPACITY * 24);
        if batch_sender.send(batch).is_err() {
            break;
        }
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = start_indexed_scan(root.clone(), sender, batch_receiver, cancelled);
    let mut fatal: Option<String> = None;
    let mut scanned = 0usize;
    let mut done = false;
    while !done {
        let event = receiver
            .recv()
            .map_err(|_| io::Error::other("scanner stopped without finishing"))?;
        match event {
            ScanEvent::Started { identity, .. } => {
                if identity != tree.record(tree.root()).expect("root exists").identity {
                    tree.mark_incomplete_to_root(tree.root());
                    fatal.get_or_insert("scan root identity changed after it was opened".to_owned());
                }
            }
            ScanEvent::Entries(mut batch) => {
                scanned += batch.entries.len();
                progress.scanning(scanned);
                if let Err(error) = apply_batch(&mut tree, &mut token_nodes, &batch) {
                    fatal.get_or_insert_with(|| error.to_string());
                }
                batch.clear();
                let _ = batch_sender.try_send(batch);
            }
            ScanEvent::DirectoryFinished { directory, complete } => {
                finish_directory(&mut tree, &token_nodes, directory, complete);
            }
            ScanEvent::DirectoriesFinished(directories) => {
                for (directory, complete) in directories {
                    finish_directory(&mut tree, &token_nodes, directory, complete);
                }
            }
            ScanEvent::DirectoryExcluded { directory, reason } => {
                if let Some(node) = token_nodes.get(directory.0 as usize).copied().flatten() {
                    tree.set_state(node, NodeStatus::Excluded(reason));
                    if let Some(parent) = tree.record(node).and_then(|record| record.parent()) {
                        tree.mark_incomplete_to_root(parent);
                    }
                }
            }
            ScanEvent::DirectoryFailed { directory, .. } => {
                if let Some(node) = token_nodes.get(directory.0 as usize).copied().flatten() {
                    tree.mark_incomplete_to_root(node);
                } else {
                    tree.mark_incomplete_to_root(tree.root());
                    fatal.get_or_insert("scan error in an unknown directory".to_owned());
                }
            }
            ScanEvent::Failed { message } => {
                fatal.get_or_insert(format!("scan could not continue: {message}"));
                tree.mark_incomplete_to_root(tree.root());
            }
            ScanEvent::Cancelled => {
                fatal.get_or_insert("scan cancelled".to_owned());
                tree.mark_scanning_incomplete();
            }
            ScanEvent::Finished => done = true,
            ScanEvent::RootListed => {}
        }
    }
    worker
        .join()
        .map_err(|_| io::Error::other("scanner worker terminated unexpectedly"))?;
    if let Some(message) = fatal {
        return Err(io::Error::other(message));
    }
    if tree.record(tree.root()).is_some_and(|record| record.state == NodeState::Scanning) {
        tree.set_state(tree.root(), NodeStatus::Complete);
    }
    tree.shrink_to_fit();
    Ok(tree)
}

fn finish_directory(
    tree: &mut Tree,
    token_nodes: &[Option<fdu_core::NodeId>],
    directory: fdu_core::DirectoryToken,
    complete: bool,
) {
    fdu_core::finish_scan_directory(tree, token_nodes, directory, complete);
}

/// Folds one scanner batch through the shared fold, so the deletion plan sees
/// the same states and totals as the browser. The fold already marked whatever
/// it could not complete; only unrecoverable batch defects fail the index.
fn apply_batch(
    tree: &mut Tree,
    token_nodes: &mut Vec<Option<fdu_core::NodeId>>,
    batch: &fdu_core::EntryBatch,
) -> io::Result<()> {
    let outcome = fdu_core::fold_entry_batch(tree, token_nodes, batch);
    if outcome.unknown_directory {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scanner sent entries for an unknown directory",
        ));
    }
    if outcome.invalid_range {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "scanner sent an invalid filename range",
        ));
    }
    if outcome.exhausted {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "index exhausted the supported number of entries",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        protected_root_reason, read_confirmation, require_complete_scan_root, resolve_confirmation_path, run,
        verify_confirmation_target,
    };
    use fdu_core::{EntryType, FileIdentity, NodeStatus, Tree};
    use fdu_scan::open_root_no_follow;
    use std::ffi::OsStr;
    use std::fs;
    use std::io::{self, Cursor};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::fs::symlink;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    /// Keep test-owned filesystem trees inside the repository's ignored scratch directory.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> io::Result<Self> {
            loop {
                let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
                let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join(".work/tmp");
                fs::create_dir_all(&scratch)?;
                let path = scratch.join(format!("fdu-rmrf-{}-{id}", std::process::id()));
                match fs::create_dir(&path) {
                    Ok(()) => return Ok(Self(path)),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn entries(path: &Path) -> Vec<PathBuf> {
        fs::read_dir(path).unwrap().map(|entry| entry.unwrap().path()).collect()
    }

    #[test]
    fn removes_everything_below_the_root_and_keeps_the_root() {
        let temp = TempDir::new().unwrap();
        let root = temp.0.join("root");
        fs::create_dir(&root).unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::create_dir(root.join("sub/nested")).unwrap();
        fs::create_dir(root.join("empty")).unwrap();
        fs::write(root.join("top"), b"top").unwrap();
        fs::write(root.join("sub/file"), b"file").unwrap();
        fs::write(root.join("sub/nested/deep"), b"deep").unwrap();
        let external = temp.0.join("external-target");
        fs::write(&external, b"external").unwrap();
        std::os::unix::fs::symlink(&external, root.join("sub/link")).unwrap();

        run(root.clone(), true).unwrap();
        assert!(entries(&root).is_empty(), "every entry below the root is gone");
        assert!(root.is_dir(), "the root itself remains");
        assert_eq!(fs::read(&external).unwrap(), b"external", "only the link is removed, not its target");
    }

    #[test]
    fn empty_directory_is_a_successful_no_op() {
        let temp = TempDir::new().unwrap();
        run(temp.0.clone(), true).unwrap();
        assert!(entries(&temp.0).is_empty());
    }

    #[test]
    fn non_directory_root_is_an_error() {
        let temp = TempDir::new().unwrap();
        let file = temp.0.join("file");
        fs::write(&file, b"data").unwrap();
        assert!(run(file, true).is_err());
    }

    #[test]
    fn symlink_root_is_refused_and_its_target_left_alone() {
        let temp = TempDir::new().unwrap();
        let target = temp.0.join("target");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("keep"), b"keep").unwrap();
        let link = temp.0.join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let error = run(link, true).unwrap_err().to_string();
        assert!(error.contains("symlink"), "{error}");
        assert_eq!(fs::read(target.join("keep")).unwrap(), b"keep");
    }

    #[test]
    fn confirmation_path_stays_bound_to_the_open_root_when_an_ancestor_link_moves() {
        let temp = TempDir::new().unwrap();
        let first = temp.0.join("first");
        let second = temp.0.join("second");
        fs::create_dir_all(first.join("root")).unwrap();
        fs::create_dir_all(second.join("root")).unwrap();
        let alias = temp.0.join("alias");
        symlink(&first, &alias).unwrap();
        let selected_path = alias.join("root");
        let root = open_root_no_follow(&selected_path).unwrap();

        let confirmation_path = resolve_confirmation_path(&root, &selected_path).unwrap();
        assert_eq!(confirmation_path, first.join("root"));

        fs::remove_file(&alias).unwrap();
        symlink(&second, &alias).unwrap();

        let error = verify_confirmation_target(&root, &selected_path).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        verify_confirmation_target(&root, &confirmation_path).unwrap();
        assert_eq!(confirmation_path, first.join("root"));
    }

    #[test]
    fn trailing_slash_symlink_root_is_refused_and_its_target_left_alone() {
        let temp = TempDir::new().unwrap();
        let target = temp.0.join("target");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("keep"), b"keep").unwrap();
        let link = temp.0.join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let mut trailing_slash = link.as_os_str().to_os_string();
        trailing_slash.push("/");

        let error = run(PathBuf::from(trailing_slash), true).unwrap_err().to_string();
        assert!(error.contains("symlink"), "{error}");
        assert_eq!(fs::read(target.join("keep")).unwrap(), b"keep");
    }

    #[test]
    fn incomplete_root_scan_is_refused_before_children_are_selected() {
        let mut tree = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 1).unwrap();
        let root = tree.root();
        tree.set_state(root, NodeStatus::Incomplete);
        tree.append(
            root,
            b"visible",
            EntryType::RegularFile,
            FileIdentity { device: 1, inode: 2 },
            1,
            1,
            1,
            NodeStatus::Complete,
        )
        .unwrap();
        assert_eq!(tree.children(root).count(), 1, "one indexed child would otherwise be selected");

        let error = require_complete_scan_root(&tree).unwrap_err();
        assert!(error.to_string().contains("incomplete"), "{error}");
    }

    #[test]
    fn unreadable_subdirectory_leaves_everything_in_place() {
        let temp = TempDir::new().unwrap();
        let root = temp.0.join("root");
        fs::create_dir_all(root.join("locked")).unwrap();
        fs::write(root.join("locked/inside"), b"inside").unwrap();
        fs::write(root.join("sibling"), b"sibling").unwrap();
        fs::set_permissions(root.join("locked"), fs::Permissions::from_mode(0o000)).unwrap();
        // Decide before running the delete: a privileged user reads the directory anyway, so the
        // scan is complete and run() would remove the tree, which leaves nothing to restore.
        let readable_anyway = fs::read_dir(root.join("locked")).is_ok();
        if readable_anyway {
            fs::set_permissions(root.join("locked"), fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }

        let result = run(root.clone(), true);
        fs::set_permissions(root.join("locked"), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(result.is_err(), "an incomplete scan must not delete anything");
        assert!(root.join("sibling").exists(), "no entry is removed when the plan is rejected");
        assert!(root.join("locked/inside").exists());
    }

    #[test]
    fn protected_roots_are_recognised_by_identity() {
        let temp = TempDir::new().unwrap();
        let parent = temp.0.join("parent");
        let cwd = parent.join("work");
        fs::create_dir_all(&cwd).unwrap();
        let unrelated = temp.0.join("unrelated");
        fs::create_dir(&unrelated).unwrap();
        let reason = |root: &Path, cwd: &Path, home: Option<&OsStr>| {
            let anchor = open_root_no_follow(root).unwrap();
            protected_root_reason(&anchor, cwd, home).unwrap()
        };

        assert!(reason(Path::new("/"), &cwd, None).is_some(), "the filesystem root");
        assert!(reason(&parent, &cwd, None).is_some(), "a parent of the current directory");
        assert!(
            reason(&cwd.join("../../parent"), &cwd, None).is_some(),
            "a spelling that differs from the real path"
        );
        assert!(reason(&unrelated, &cwd, Some(unrelated.as_os_str())).is_some(), "the home directory");
        assert!(reason(&cwd, &cwd, None).is_none(), "the current directory itself may be emptied");
        assert!(reason(&unrelated, &cwd, Some(parent.as_os_str())).is_none());
        assert!(reason(&unrelated, &cwd, Some(OsStr::new("/nonexistent-home"))).is_none());
    }

    #[test]
    fn only_an_exact_yes_confirms() {
        let answer = |text: &str| read_confirmation(&mut Cursor::new(text.to_owned())).unwrap();
        assert!(answer("yes\n"));
        assert!(!answer("y\n"));
        assert!(!answer("YES\n"));
        assert!(!answer("no\n"));
        assert!(!answer(""), "end of input declines");
    }
}
