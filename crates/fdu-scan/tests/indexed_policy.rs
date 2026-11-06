use fdu_core::{
    DirectoryToken, EntryType, NodeId, NodeState, NodeStatus, ScanEvent, Tree,
};
use fdu_scan::{
    open_root, scan, scan_with_exclusions, start_indexed_scan, start_indexed_scan_with_exclusions, EntryBatch,
    RootAnchor, ScanExclusions, ScanOptions,
};
use hashbrown::HashSet;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{symlink, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

static TEMP_ID: AtomicUsize = AtomicUsize::new(0);
const BATCH_SIZE: usize = 1024;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> io::Result<Self> {
        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.work/tmp");
        fs::create_dir_all(&scratch)?;
        loop {
            let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = scratch.join(format!("fdu-indexed-{}-{id}", std::process::id()));
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

struct IndexedResult {
    tree: Tree,
    errors: Vec<String>,
    batches: Vec<usize>,
    directory_finish_batch_sizes: Vec<usize>,
}

/// Applies a completion the way the browser does, and checks the order the scanner promises:
/// everything inside a directory has been reported, and finished, before the directory finishes.
fn finish_directory(
    tree: &mut Tree,
    tokens: &[Option<NodeId>],
    finished: &mut HashSet<DirectoryToken>,
    directory: DirectoryToken,
    complete: bool,
) -> io::Result<()> {
    let out_of_order = |message: &str| io::Error::new(io::ErrorKind::InvalidData, message.to_owned());
    let node = tokens
        .get(directory.0 as usize)
        .copied()
        .flatten()
        .ok_or_else(|| out_of_order("a directory finished before its entry was reported"))?;
    if !finished.insert(directory) {
        return Err(out_of_order("a directory finished twice"));
    }
    for child in tree.children(node) {
        if tree.record(child).unwrap().entry_type == EntryType::Directory
            && tree.record(child).unwrap().state == NodeState::Scanning
        {
            return Err(out_of_order("a directory finished before a directory inside it"));
        }
    }
    if complete && tree.record(node).unwrap().state == NodeState::Scanning {
        tree.set_state(node, NodeStatus::Complete);
    } else if !complete {
        tree.mark_incomplete_to_root(node);
    }
    Ok(())
}

fn scan_to_tree(root: RootAnchor, capacity: usize) -> io::Result<IndexedResult> {
    scan_to_tree_with_exclusions(root, capacity, ScanExclusions::default())
}

fn scan_to_tree_with_exclusions(
    root: RootAnchor,
    capacity: usize,
    exclusions: ScanExclusions,
) -> io::Result<IndexedResult> {
    let mut tree = Tree::new(&root.name, root.identity, root.link_count)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "root index name too long"))?;
    assert!(tree.set_modified_time(tree.root(), root.modified_time()));
    let mut tokens = vec![Some(tree.root())];
    let mut finished = HashSet::new();
    let (sender, receiver) = mpsc::sync_channel(capacity);
    let (batch_sender, batch_receiver) = mpsc::sync_channel(capacity);
    for _ in 0..capacity {
        batch_sender
            .send(EntryBatch::with_capacity(BATCH_SIZE, BATCH_SIZE * 24))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "batch pool closed"))?;
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = start_indexed_scan_with_exclusions(
        root.clone(),
        exclusions,
        sender,
        batch_receiver,
        Arc::clone(&cancelled),
    );
    let mut errors = Vec::new();
    let mut batches = Vec::new();
    let mut directory_finish_batch_sizes = Vec::new();
    let mut root_listed = false;
    let mut stopped = false;
    let mut done = false;
    while !done {
        let event = receiver
            .recv_timeout(Duration::from_secs(10))
            .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?;
        match event {
            ScanEvent::Started { identity, .. } => {
                assert_eq!(identity, tree.record(tree.root()).unwrap().identity);
            }
            ScanEvent::Entries(mut batch) => {
                batches.push(batch.entries.len());
                // Entries of several directories share a batch; totals are added per run of
                // entries with the same parent.
                let mut run: Option<(NodeId, u64, u64, u32)> = None;
                for entry in &batch.entries {
                    assert!(
                        !(root_listed && entry.parent == DirectoryToken(0)),
                        "a root entry arrived after the root was reported listed"
                    );
                    let parent = tokens
                        .get(entry.parent.0 as usize)
                        .copied()
                        .flatten()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown directory token"))?;
                    if run.is_some_and(|(node, _, _, _)| node != parent) {
                        let (node, apparent, allocated, entries) = run.take().unwrap();
                        assert!(tree.add_to_ancestors(node, apparent, allocated, entries));
                    }
                    let (_, apparent, allocated, entries) = run.get_or_insert((parent, 0, 0, 0));
                    let name = &batch.names[entry.name.start as usize..entry.name.end as usize];
                    let id = tree
                        .append(
                            parent,
                            name,
                            entry.entry_type,
                            entry.identity,
                            entry.link_count,
                            entry.apparent_bytes,
                            entry.allocated_bytes,
                            entry.state,
                        )
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "index exhausted"))?;
                    assert!(tree.set_modified_time(id, entry.modified_time));
                    *entries += 1;
                    if let Some(token) = entry.directory_token {
                        if tokens.len() <= token.0 as usize {
                            tokens.resize(token.0 as usize + 1, None);
                        }
                        tokens[token.0 as usize] = Some(id);
                    }
                    if entry.entry_type != EntryType::Directory && entry.state == NodeStatus::Complete {
                        *apparent = apparent.checked_add(entry.apparent_bytes).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "test apparent overflow")
                        })?;
                        *allocated = allocated.checked_add(entry.allocated_bytes).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "test allocated overflow")
                        })?;
                    }
                    match entry.state {
                        NodeStatus::Incomplete => {
                            tree.mark_incomplete_to_root(parent);
                        }
                        NodeStatus::Excluded(reason) => {
                            tree.set_state(id, NodeStatus::Excluded(reason));
                            tree.mark_incomplete_to_root(parent);
                        }
                        _ => {}
                    }
                }
                if let Some((node, apparent, allocated, entries)) = run {
                    assert!(tree.add_to_ancestors(node, apparent, allocated, entries));
                }
                batch.clear();
                let _ = batch_sender.try_send(batch);
            }
            ScanEvent::DirectoryFinished { directory, complete } => {
                directory_finish_batch_sizes.push(1);
                finish_directory(&mut tree, &tokens, &mut finished, directory, complete)?;
            }
            ScanEvent::DirectoriesFinished(directories) => {
                directory_finish_batch_sizes.push(directories.len());
                for (directory, complete) in directories {
                    finish_directory(&mut tree, &tokens, &mut finished, directory, complete)?;
                }
            }
            ScanEvent::DirectoryExcluded { directory, reason } => {
                if let Some(node) = tokens.get(directory.0 as usize).copied().flatten() {
                    tree.set_state(node, NodeStatus::Excluded(reason));
                }
            }
            ScanEvent::DirectoryFailed { directory, message } => {
                errors.push(message);
                if let Some(node) = tokens.get(directory.0 as usize).copied().flatten() {
                    tree.mark_incomplete_to_root(node);
                }
            }
            ScanEvent::DirectoryWaiting { .. } => {}
            ScanEvent::RootListed => {
                assert!(!root_listed, "the root is reported listed once");
                root_listed = true;
            }
            ScanEvent::Failed { message } => {
                stopped = true;
                errors.push(message);
            }
            ScanEvent::Cancelled => {
                stopped = true;
                errors.push("cancelled".to_owned());
            }
            ScanEvent::Finished => done = true,
        }
    }
    assert!(root_listed || stopped, "a scan that runs to the end reports the root listed");
    worker
        .join()
        .map_err(|_| io::Error::other("scanner worker panicked"))?;
    Ok(IndexedResult {
        tree,
        errors,
        batches,
        directory_finish_batch_sizes,
    })
}

#[test]
#[cfg(target_os = "linux")]
fn indexed_wide_tree_completes_with_a_low_hard_descriptor_limit() -> io::Result<()> {
    const CHILD: &str = "FDU_LOW_DESCRIPTOR_CHILD";
    const ROOT: &str = "FDU_LOW_DESCRIPTOR_ROOT";
    let Some(limit) = std::env::var_os(CHILD) else {
        let temp = TempDir::new()?;
        for branch in 0..48 {
            for leaf in 0..80 {
                fs::create_dir_all(temp.0.join(format!("{branch}/{leaf}")))?;
            }
        }
        let mut chain = temp.0.join("deep");
        fs::create_dir(&chain)?;
        for _ in 0..96 {
            for sibling in 0..4 {
                fs::create_dir(chain.join(format!("sibling-{sibling}")))?;
            }
            chain = chain.join("next");
            fs::create_dir(&chain)?;
        }
        for limit in [64, 256] {
            let output = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "indexed_wide_tree_completes_with_a_low_hard_descriptor_limit", "--nocapture"])
                .env(CHILD, limit.to_string())
                .env(ROOT, &temp.0)
                .output()?;
            assert!(output.status.success(), "descriptor limit {limit}: {}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        }
        return Ok(());
    };
    let limit = limit.to_str().unwrap().parse::<u64>().unwrap();
    rustix::process::setrlimit(
        rustix::process::Resource::Nofile,
        rustix::process::Rlimit { current: Some(limit), maximum: Some(limit) },
    )?;
    let root = PathBuf::from(std::env::var_os(ROOT).expect("parent fixture path"));
    let result = scan_to_tree(open_root(&root)?, 64)?;
    assert!(result.errors.is_empty(), "{} scan errors; first: {:?}", result.errors.len(), result.errors.first());
    assert_eq!(result.tree.len(), 1 + 48 + 48 * 80 + 1 + 96 * 5);
    assert_eq!(result.tree.record(result.tree.root()).unwrap().state, NodeState::Complete);
    Ok(())
}

#[test]
fn repeated_indexed_scans_on_one_anchor_start_at_the_root_directory_beginning() -> io::Result<()> {
    let temp = TempDir::new()?;
    fs::create_dir_all(temp.0.join("subdirectory"))?;
    fs::write(temp.0.join("first"), b"first")?;
    fs::write(temp.0.join("subdirectory/second"), b"second")?;
    let root = open_root(&temp.0)?;

    let first = scan_to_tree(root.clone(), 4)?;
    let repeated = scan_to_tree(root, 4)?;

    for result in [&first, &repeated] {
        assert!(result.errors.is_empty(), "scanner errors: {:?}", result.errors);
        assert_eq!(result.tree.len(), 4);
        assert_eq!(result.tree.record(result.tree.root()).unwrap().state, NodeState::Complete);
    }
    Ok(())
}

#[test]
fn indexed_scan_preserves_modification_times_for_root_files_and_directories() -> io::Result<()> {
    let temp = TempDir::new()?;
    fs::create_dir(temp.0.join("directory"))?;
    fs::write(temp.0.join("file"), b"contents")?;
    let root = open_root(&temp.0)?;
    let expected_root_time = fs::metadata(&temp.0)?.mtime();
    let expected_directory_time = fs::metadata(temp.0.join("directory"))?.mtime();
    let expected_file_time = fs::metadata(temp.0.join("file"))?.mtime();

    let result = scan_to_tree(root, 4)?;

    let root_record = result.tree.record(result.tree.root()).unwrap();
    assert_eq!(root_record.modified_time().unix_seconds(), Some(expected_root_time));
    let directory = child_named(&result.tree, result.tree.root(), b"directory").unwrap();
    let directory_record = result.tree.record(directory).unwrap();
    assert_eq!(directory_record.modified_time().unix_seconds(), Some(expected_directory_time));
    let file = child_named(&result.tree, result.tree.root(), b"file").unwrap();
    let file_record = result.tree.record(file).unwrap();
    assert_eq!(file_record.modified_time().unix_seconds(), Some(expected_file_time));
    Ok(())
}

fn child_named(tree: &Tree, parent: NodeId, name: &[u8]) -> Option<NodeId> {
    tree.children(parent).find(|id| tree.name(*id) == Some(name))
}

#[test]
fn indexed_exclusions_omit_exact_paths_and_mark_the_index_incomplete() -> io::Result<()> {
    let temp = TempDir::new()?;
    let root = &temp.0;
    fs::create_dir_all(root.join("parent/target"))?;
    fs::create_dir_all(root.join("parent/targeted"))?;
    fs::create_dir_all(root.join("parent/keep/target"))?;
    fs::create_dir_all(root.join("parent/keep/targeted"))?;
    fs::write(root.join("parent/target/hidden.txt"), b"hidden directory")?;
    fs::write(root.join("parent/targeted/kept.txt"), b"substring sibling")?;
    fs::write(root.join("parent/keep/target/hidden.txt"), b"hidden file")?;
    fs::write(root.join("parent/keep/targeted/kept.txt"), b"same basename path")?;

    let exclusions = ScanExclusions::from_paths([
        PathBuf::from("parent/target"),
        PathBuf::from("parent/keep/target/hidden.txt"),
    ])?;
    let result = scan_to_tree_with_exclusions(open_root(root)?, 8, exclusions.clone())?;
    assert!(result.errors.is_empty(), "scan errors: {:?}", result.errors);

    let tree = &result.tree;
    let root_id = tree.root();
    let parent = child_named(tree, root_id, b"parent").expect("parent is indexed");
    assert!(child_named(tree, parent, b"target").is_none());
    let targeted = child_named(tree, parent, b"targeted").expect("substring sibling remains indexed");
    assert!(child_named(tree, targeted, b"kept.txt").is_some());
    let keep = child_named(tree, parent, b"keep").expect("parent path remains indexed");
    let target = child_named(tree, keep, b"target").expect("same basename at a different path remains indexed");
    assert!(child_named(tree, target, b"hidden.txt").is_none());
    let other_target = child_named(tree, keep, b"targeted").expect("second substring sibling remains indexed");
    assert!(child_named(tree, other_target, b"kept.txt").is_some());
    assert_eq!(tree.record(root_id).unwrap().state, NodeState::Incomplete);
    assert_eq!(tree.record(parent).unwrap().state, NodeState::Incomplete);

    let summary = scan_with_exclusions(
        &ScanOptions {
            path: root.to_path_buf(),
            apparent: true,
        },
        &exclusions,
    )?;
    let parent_summary = summary
        .directories
        .iter()
        .find(|directory| directory.name == "parent")
        .expect("summary keeps the non-excluded parent directory");
    assert_eq!(
        parent_summary.disk_size,
        b"substring sibling".len() as u64 + b"same basename path".len() as u64
    );
    Ok(())
}

fn allocated(path: &Path) -> io::Result<u64> {
    fs::symlink_metadata(path)?
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "allocated test bytes overflow"))
}

#[test]
fn indexed_policy_keeps_both_size_modes_and_entry_identity() -> io::Result<()> {
    let temp = TempDir::new()?;
    let actual = temp.0.join("actual root");
    let supplied = temp.0.join("root-link");
    let outer = actual.join("outer");
    let nested = outer.join("nested");
    fs::create_dir(&actual)?;
    fs::create_dir(&outer)?;
    fs::create_dir(&nested)?;
    symlink(&actual, &supplied)?;

    let payload = outer.join("payload");
    let hardlink = outer.join("payload-hardlink");
    let sparse = outer.join("sparse");
    let unusual = outer.join(OsString::from_vec(b"unusual-\xc3\xa9-\n-name".to_vec()));
    let file_link = outer.join("file-link");
    let dangling_link = outer.join("dangling-link");
    let directory_link = outer.join("directory-link");
    let nested_payload = nested.join("nested-file");
    let root_file = actual.join("root-file");
    let parallel = actual.join("parallel");
    let parallel_payload = parallel.join("parallel-file");
    fs::write(&payload, b"payload data")?;
    fs::hard_link(&payload, &hardlink)?;
    File::create(&sparse)?.set_len(1 << 20)?;
    fs::write(&unusual, b"raw name")?;
    symlink("payload", &file_link)?;
    symlink("missing-target", &dangling_link)?;
    symlink("nested", &directory_link)?;
    fs::write(&nested_payload, b"nested")?;
    fs::write(&root_file, b"direct root data")?;
    fs::create_dir(&parallel)?;
    fs::write(&parallel_payload, b"parallel branch data")?;

    let status = std::process::Command::new("mkfifo").arg(outer.join("fifo")).status()?;
    assert!(status.success());

    let root = open_root(&supplied)?;
    let result = scan_to_tree(root, 2)?;
    assert!(result.errors.is_empty(), "scanner errors: {:?}", result.errors);
    assert!(!result.batches.is_empty());
    let tree = result.tree;
    let root_id = tree.root();
    let outer_id = child_named(&tree, root_id, b"outer").unwrap();
    let root_file_id = child_named(&tree, root_id, b"root-file").unwrap();
    let parallel_id = child_named(&tree, root_id, b"parallel").unwrap();
    let parallel_payload_id = child_named(&tree, parallel_id, b"parallel-file").unwrap();
    let payload_id = child_named(&tree, outer_id, b"payload").unwrap();
    let hardlink_id = child_named(&tree, outer_id, b"payload-hardlink").unwrap();
    let sparse_id = child_named(&tree, outer_id, b"sparse").unwrap();
    let file_link_id = child_named(&tree, outer_id, b"file-link").unwrap();
    let dangling_id = child_named(&tree, outer_id, b"dangling-link").unwrap();
    let directory_link_id = child_named(&tree, outer_id, b"directory-link").unwrap();
    let fifo_id = child_named(&tree, outer_id, b"fifo").unwrap();

    assert_eq!(tree.name(root_id), Some(supplied.as_os_str().as_bytes()));
    assert_eq!(tree.record(outer_id).unwrap().state, NodeState::Complete);
    assert_eq!(tree.record(parallel_id).unwrap().state, NodeState::Complete);
    assert_eq!(tree.record(parallel_payload_id).unwrap().apparent_bytes, fs::metadata(&parallel_payload)?.len());
    assert_eq!(
        tree.record(outer_id).unwrap().link_count,
        fs::symlink_metadata(&outer)?.nlink()
    );
    assert_ne!(payload_id, hardlink_id);
    assert_eq!(tree.record(payload_id).unwrap().identity, tree.record(hardlink_id).unwrap().identity);
    assert_eq!(tree.record(payload_id).unwrap().link_count, 2);
    assert_eq!(tree.record(hardlink_id).unwrap().link_count, 2);
    assert_eq!(tree.record(file_link_id).unwrap().entry_type, EntryType::Symlink);
    assert_eq!(tree.record(dangling_id).unwrap().entry_type, EntryType::Symlink);
    assert_eq!(tree.record(directory_link_id).unwrap().entry_type, EntryType::Symlink);
    assert_eq!(tree.children(directory_link_id).count(), 0);
    assert_eq!(tree.record(fifo_id).unwrap().entry_type, EntryType::Other);
    assert_eq!(tree.record(sparse_id).unwrap().apparent_bytes, fs::metadata(&sparse)?.len());
    assert_eq!(tree.record(sparse_id).unwrap().allocated_bytes, allocated(&sparse)?);

    let outer_apparent = [
        fs::symlink_metadata(&payload)?.len(),
        fs::symlink_metadata(&hardlink)?.len(),
        fs::symlink_metadata(&sparse)?.len(),
        fs::symlink_metadata(&unusual)?.len(),
        fs::symlink_metadata(&file_link)?.len(),
        fs::symlink_metadata(&dangling_link)?.len(),
        fs::symlink_metadata(&directory_link)?.len(),
        fs::symlink_metadata(&nested_payload)?.len(),
    ]
    .into_iter()
    .sum::<u64>();
    let outer_allocated = [
        allocated(&payload)?,
        allocated(&hardlink)?,
        allocated(&sparse)?,
        allocated(&unusual)?,
        allocated(&file_link)?,
        allocated(&dangling_link)?,
        allocated(&directory_link)?,
        allocated(&nested_payload)?,
    ]
    .into_iter()
    .sum::<u64>();
    assert_eq!(tree.record(outer_id).unwrap().apparent_bytes, outer_apparent);
    assert_eq!(tree.record(outer_id).unwrap().allocated_bytes, outer_allocated);
    assert_eq!(tree.record(root_file_id).unwrap().apparent_bytes, fs::metadata(&root_file)?.len());
    assert_eq!(tree.record(root_file_id).unwrap().allocated_bytes, allocated(&root_file)?);
    assert_eq!(
        tree.record(root_id).unwrap().apparent_bytes,
        outer_apparent + fs::metadata(root_file)?.len() + fs::metadata(&parallel_payload)?.len()
    );
    assert_eq!(
        tree.record(root_id).unwrap().allocated_bytes,
        outer_allocated + allocated(&actual.join("root-file"))? + allocated(&parallel_payload)?
    );

    for apparent in [false, true] {
        let summary = scan(&ScanOptions { path: supplied.clone(), apparent })?;
        assert_eq!(summary.mount_boundaries, 0);
        let outer_summary = summary
            .directories
            .iter()
            .find(|directory| directory.name.as_bytes() == b"outer")
            .expect("summary includes the immediate child directory");
        let indexed_size = if apparent {
            tree.record(outer_id).unwrap().apparent_bytes
        } else {
            tree.record(outer_id).unwrap().allocated_bytes
        };
        assert_eq!(outer_summary.disk_size, indexed_size);
        assert_eq!(outer_summary.exclusion, None);
    }
    Ok(())
}

#[test]
fn indexed_scan_batches_directory_finish_events_without_losing_states() -> io::Result<()> {
    let temp = TempDir::new()?;
    for index in 0..300 {
        fs::create_dir(temp.0.join(format!("directory-{index:03}")))?;
    }

    let result = scan_to_tree(open_root(&temp.0)?, 8)?;

    assert!(result.errors.is_empty(), "scan errors: {:?}", result.errors);
    assert!(result
        .directory_finish_batch_sizes
        .iter()
        .any(|size| *size > 1));
    assert_eq!(result.tree.record(result.tree.root()).unwrap().state, NodeState::Complete);
    assert!(result.tree.children(result.tree.root()).all(|node| {
        result.tree.record(node).unwrap().state == NodeState::Complete
    }));
    Ok(())
}

#[test]
fn indexed_worker_reopens_sibling_directories_after_the_handle_cap() -> io::Result<()> {
    let temp = TempDir::new()?;
    let wide = temp.0.join("wide");
    fs::create_dir(&wide)?;
    for index in 0..140 {
        let directory = wide.join(format!("branch-{index:03}"));
        fs::create_dir(&directory)?;
        fs::write(directory.join("payload"), b"x")?;
    }

    let result = scan_to_tree(open_root(&temp.0)?, 2)?;
    assert!(result.errors.is_empty(), "scanner errors: {:?}", result.errors);
    let wide_id = child_named(&result.tree, result.tree.root(), b"wide").unwrap();
    assert_eq!(result.tree.children(wide_id).count(), 140);
    for index in 0..140 {
        let name = format!("branch-{index:03}");
        let branch = child_named(&result.tree, wide_id, name.as_bytes()).unwrap();
        assert_eq!(result.tree.record(branch).unwrap().state, NodeState::Complete);
        assert_eq!(result.tree.children(branch).count(), 1);
    }
    Ok(())
}

#[test]
fn indexed_worker_publishes_wide_directory_chunks_before_completion() -> io::Result<()> {
    let temp = TempDir::new()?;
    for index in 0..2600 {
        fs::write(temp.0.join(format!("entry-{index:04}")), b"x")?;
    }
    let root = open_root(&temp.0)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let (batch_sender, batch_receiver) = mpsc::sync_channel(1);
    batch_sender
        .send(EntryBatch::with_capacity(BATCH_SIZE, BATCH_SIZE * 24))
        .unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = start_indexed_scan(root, sender, batch_receiver, Arc::clone(&cancelled));
    assert!(matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok(ScanEvent::Started { .. })));
    let first = receiver.recv_timeout(Duration::from_secs(5)).map_err(io::Error::other)?;
    match first {
        ScanEvent::Entries(batch) => assert_eq!(batch.entries.len(), BATCH_SIZE),
        other => panic!("expected entry batch, got {other:?}"),
    }
    let second = receiver.recv_timeout(Duration::from_secs(5)).map_err(io::Error::other)?;
    assert!(matches!(second, ScanEvent::Entries(_)));
    cancelled.store(true, Ordering::Relaxed);
    drop(receiver);
    worker.join().map_err(|_| io::Error::other("scanner worker panicked"))?;
    Ok(())
}

#[test]
fn dropping_a_full_scan_queue_releases_a_blocked_worker() -> io::Result<()> {
    let temp = TempDir::new()?;
    for index in 0..1000 {
        fs::write(temp.0.join(format!("entry-{index:04}")), b"x")?;
    }
    let root = open_root(&temp.0)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    let (batch_sender, batch_receiver) = mpsc::sync_channel(1);
    batch_sender
        .send(EntryBatch::with_capacity(BATCH_SIZE, BATCH_SIZE * 24))
        .unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = start_indexed_scan(root, sender, batch_receiver, Arc::clone(&cancelled));
    assert!(matches!(receiver.recv_timeout(Duration::from_secs(5)), Ok(ScanEvent::Started { .. })));
    cancelled.store(true, Ordering::Relaxed);
    drop(receiver);
    worker.join().map_err(|_| io::Error::other("scanner worker panicked"))?;
    Ok(())
}

/// Mounts need privileges; release validation runs this test with CAP_SYS_ADMIN.
#[cfg(target_os = "linux")]
struct Mount(PathBuf);

#[cfg(target_os = "linux")]
impl Mount {
    fn tmpfs(target: &Path) -> io::Result<Self> {
        rustix::mount::mount("tmpfs", target, "tmpfs", rustix::mount::MountFlags::empty(), None)?;
        Ok(Self(target.to_owned()))
    }

    fn bind(source: &Path, target: &Path) -> io::Result<Self> {
        rustix::mount::mount_bind(source, target)?;
        Ok(Self(target.to_owned()))
    }
}

#[cfg(target_os = "linux")]
impl Drop for Mount {
    fn drop(&mut self) {
        let _ = rustix::mount::unmount(&self.0, rustix::mount::UnmountFlags::DETACH);
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires CAP_SYS_ADMIN; run through `make test-mounts`"]
fn mounts_and_bind_mounts_below_the_root_are_boundaries_without_children() -> io::Result<()> {
    let temp = TempDir::new()?;
    let plain = temp.0.join("plain");
    let tmpfs = temp.0.join("tmpfs");
    let bound = temp.0.join("bound");
    for directory in [&plain, &tmpfs, &bound] {
        fs::create_dir(directory)?;
    }
    fs::write(plain.join("payload"), b"counted once")?;
    let _tmpfs = Mount::tmpfs(&tmpfs)?;
    let _bound = Mount::bind(&plain, &bound)?;
    fs::write(tmpfs.join("hidden"), b"on another filesystem")?;

    let result = scan_to_tree(open_root(&temp.0)?, 4)?;
    assert!(result.errors.is_empty(), "scanner errors: {:?}", result.errors);
    let tree = result.tree;
    let root = tree.root();
    for name in [&b"tmpfs"[..], b"bound"] {
        let id = child_named(&tree, root, name).unwrap();
        assert_eq!(
            tree.record(id).unwrap().state,
            NodeState::Excluded(fdu_core::ExclusionReason::MountBoundary),
            "{}",
            String::from_utf8_lossy(name)
        );
        assert_eq!(tree.children(id).count(), 0);
    }
    let plain_id = child_named(&tree, root, b"plain").unwrap();
    assert_eq!(tree.record(plain_id).unwrap().state, NodeState::Complete);
    assert_eq!(tree.record(plain_id).unwrap().apparent_bytes, 12);
    assert_eq!(tree.record(root).unwrap().apparent_bytes, 12);
    assert_ne!(tree.record(root).unwrap().state, NodeState::Complete);

    let summary = scan(&ScanOptions { path: temp.0.clone(), apparent: true })?;
    assert_eq!(summary.mount_boundaries, 2);
    for name in ["tmpfs", "bound"] {
        let directory = summary.directories.iter().find(|directory| directory.name == name).unwrap();
        assert_eq!(directory.disk_size, 0);
        assert_eq!(directory.exclusion, Some(fdu_scan::SummaryExclusion::MountBoundary));
    }
    Ok(())
}

/// A tree with deep chains, wide fan-outs of tiny directories, and wide directories of files,
/// so scans split into many tasks of very different sizes. Returns the entry count.
fn build_mixed_tree(root: &Path) -> io::Result<usize> {
    let mut entries = 0;
    let mut create_dir = |path: &Path| -> io::Result<()> {
        fs::create_dir(path)?;
        entries += 1;
        Ok(())
    };
    let mut files = 0;
    let mut write = |path: &Path| -> io::Result<()> {
        fs::write(path, b"x")?;
        files += 1;
        Ok(())
    };
    for top in 0..12 {
        let top_dir = root.join(format!("top-{top:02}"));
        create_dir(&top_dir)?;
        // A deep chain.
        let mut chain = top_dir.join("chain");
        create_dir(&chain)?;
        for level in 0..7 {
            chain = chain.join(format!("level-{level}"));
            create_dir(&chain)?;
            write(&chain.join("file"))?;
        }
        // A fan-out of small directories.
        let fan = top_dir.join("fan");
        create_dir(&fan)?;
        for branch in 0..30 {
            let branch_dir = fan.join(format!("branch-{branch:02}"));
            create_dir(&branch_dir)?;
            for leaf in 0..2 {
                write(&branch_dir.join(format!("leaf-{leaf}")))?;
            }
        }
        // A wide directory.
        let wide = top_dir.join("wide");
        create_dir(&wide)?;
        for file in 0..150 {
            write(&wide.join(format!("file-{file:03}")))?;
        }
    }
    Ok(entries + files)
}

#[test]
fn concurrent_scans_all_complete_with_every_entry() -> io::Result<()> {
    let temp = TempDir::new()?;
    let expected = build_mixed_tree(&temp.0)?;
    // Raise FDU_STRESS_SCANS for a soak; every scan runs its own worker pool, so even the default
    // oversubscribes the machine.
    let scans_per_thread = std::env::var("FDU_STRESS_SCANS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(12);
    std::thread::scope(|scope| {
        let threads = (0..6)
            .map(|_| {
                scope.spawn(|| -> io::Result<()> {
                    for _ in 0..scans_per_thread {
                        let result = scan_to_tree(open_root(&temp.0)?, 4)?;
                        assert!(result.errors.is_empty(), "scanner errors: {:?}", result.errors);
                        assert_eq!(result.tree.len(), expected + 1, "entries plus the root");
                        assert_eq!(result.tree.record(result.tree.root()).unwrap().state, NodeState::Complete);
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
