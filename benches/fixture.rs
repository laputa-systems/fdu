//! Benchmarks over the generated layout fixture (see `scripts/generate_layout_fixture.py`).
//!
//! Run with `cargo bench --bench fixture`; `FDU_BENCH_FIXTURE` selects another
//! copy of the fixture (default: `.work/tmp/fdu-layout-fixture`). The whole-fixture
//! delete benchmark clones the fixture for every sample, outside the timed
//! region, under `FDU_BENCH_SCRATCH` (default: `.work/tmp`).
//!
//! Compare runs with the rustybench `baseline` and `diff` commands.

use fdu_core::{EntryType, NodeStatus, ScanEvent, Tree};
use fdu_delete::{create_plan, DeleteEvent};
use fdu_scan::{open_root, scan, start_indexed_scan, EntryBatch, ScanOptions};
use rustybench::{black_box, counter::ItemsCount, Bencher};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};

/// Entries under the root of the generated fixture; asserted so a different fixture is noticed.
const FIXTURE_ENTRIES: usize = 192_238;
const BATCH_CAPACITY: usize = 1024;
const EVENT_CHANNEL_CAPACITY: usize = 512;
const BATCH_POOL: usize = 8;

fn main() {
    rustybench::main();
}

fn fixture() -> &'static Path {
    static FIXTURE: OnceLock<PathBuf> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let path = std::env::var_os("FDU_BENCH_FIXTURE").map_or_else(
            || Path::new(env!("CARGO_MANIFEST_DIR")).join(".work/tmp/fdu-layout-fixture"),
            PathBuf::from,
        );
        if !path.is_dir() {
            std::fs::create_dir_all(path.parent().expect("fixture has a parent directory"))
                .expect("fixture scratch directory is created");
            let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/generate_layout_fixture.py");
            let status = Command::new("python3").arg(script).arg(&path).status().expect("python3 runs");
            assert!(status.success(), "fixture generation failed");
        }
        path
    })
}

/// Runs the indexed scanner and hands every event to `on_event`, returning batches to the
/// scanner the way the browser does.
fn run_indexed_scan(root: &Path, mut on_event: impl FnMut(&mut ScanEvent)) {
    let root = open_root(root).expect("fixture root opens");
    let (sender, receiver) = mpsc::sync_channel(EVENT_CHANNEL_CAPACITY);
    let (batch_sender, batch_receiver) = mpsc::sync_channel(BATCH_POOL);
    for _ in 0..BATCH_POOL {
        batch_sender
            .send(EntryBatch::with_capacity(BATCH_CAPACITY, BATCH_CAPACITY * 24))
            .unwrap();
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = start_indexed_scan(root, sender, batch_receiver, cancelled);
    while let Ok(mut event) = receiver.recv() {
        let finished = matches!(event, ScanEvent::Finished);
        on_event(&mut event);
        if let ScanEvent::Entries(mut batch) = event {
            batch.clear();
            let _ = batch_sender.try_send(batch);
        }
        if finished {
            break;
        }
    }
    worker.join().expect("scan worker exits");
}

/// Builds the same index the browser builds from scan events.
fn index_fixture(root: &Path) -> Tree {
    let anchor = open_root(root).expect("fixture root opens");
    let mut tree = Tree::new(&anchor.name, anchor.identity, anchor.link_count).expect("root name fits");
    drop(anchor);
    let mut nodes = vec![Some(tree.root())];
    run_indexed_scan(root, |event| match event {
        ScanEvent::Entries(batch) => {
            let mut run: Option<(fdu_core::NodeId, u64, u64, u32)> = None;
            for entry in &batch.entries {
                let parent = nodes[entry.parent.0 as usize].expect("entries follow their directory's entry");
                if run.is_some_and(|(node, ..)| node != parent) {
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
                    .expect("index has room");
                *entries += 1;
                if let Some(token) = entry.directory_token {
                    let index = token.0 as usize;
                    if nodes.len() <= index {
                        nodes.resize(index + 1, None);
                    }
                    nodes[index] = Some(id);
                }
                if entry.entry_type != EntryType::Directory && entry.state == NodeStatus::Complete {
                    *apparent += entry.apparent_bytes;
                    *allocated += entry.allocated_bytes;
                }
            }
            if let Some((node, apparent, allocated, entries)) = run {
                assert!(tree.add_to_ancestors(node, apparent, allocated, entries));
            }
        }
        ScanEvent::DirectoriesFinished(directories) => {
            for (directory, complete) in directories.iter() {
                if let Some(node) = nodes.get(directory.0 as usize).copied().flatten() {
                    if *complete {
                        tree.set_state(node, NodeStatus::Complete);
                    } else {
                        tree.mark_incomplete_to_root(node);
                    }
                }
            }
        }
        ScanEvent::Failed { message } | ScanEvent::DirectoryFailed { message, .. } => panic!("scan failed: {message}"),
        _ => {}
    });
    tree.set_state(tree.root(), NodeStatus::Complete);
    tree
}

#[rustybench::bench_group(sample_count = 20, sample_size = 1)]
mod scan {
    use super::*;

    /// The summary scanner: immediate child directory totals only, no index.
    #[rustybench::bench(args = [false, true])]
    fn summary(bencher: Bencher, apparent: bool) {
        bencher.counter(ItemsCount::new(FIXTURE_ENTRIES)).bench_local(|| {
            black_box(scan(&ScanOptions { path: fixture().to_owned(), apparent }).expect("fixture scans"))
        });
    }

    /// Indexed scan with the events dropped on arrival: the scanner's own cost.
    #[rustybench::bench]
    fn indexed_drain(bencher: Bencher) {
        bencher.counter(ItemsCount::new(FIXTURE_ENTRIES)).bench_local(|| {
            let mut entries = 0usize;
            run_indexed_scan(fixture(), |event| {
                if let ScanEvent::Entries(batch) = event {
                    entries += batch.entries.len();
                }
            });
            assert_eq!(black_box(entries), FIXTURE_ENTRIES);
        });
    }

    /// Indexed scan building the in-memory tree: what the browser does before it is ready.
    #[rustybench::bench]
    fn indexed_tree(bencher: Bencher) {
        bencher.counter(ItemsCount::new(FIXTURE_ENTRIES)).bench_local(|| {
            let tree = index_fixture(fixture());
            assert_eq!(tree.len(), FIXTURE_ENTRIES + 1, "entries plus the root");
            black_box(tree)
        });
    }
}

/// A private copy of the fixture that is removed with the value, whatever the benchmark did.
struct Clone {
    parent: PathBuf,
    root: PathBuf,
}

impl Clone {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let scratch = std::env::var_os("FDU_BENCH_SCRATCH").map_or_else(
            || Path::new(env!("CARGO_MANIFEST_DIR")).join(".work/tmp"),
            PathBuf::from,
        );
        std::fs::create_dir_all(&scratch).expect("benchmark scratch directory is created");
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let parent = scratch.join(format!("fdu-bench-{}-{id}", std::process::id()));
        std::fs::create_dir(&parent).expect("scratch directory is created");
        let root = parent.join("layout");
        let status = Command::new("cp")
            .args(["-R", "--reflink=auto"])
            .arg(fixture())
            .arg(&root)
            .status()
            .expect("cp runs");
        assert!(status.success(), "cloning the fixture failed");
        // Start from written-back metadata so the timed region does not pay for the clone.
        let _ = Command::new("sync").status();
        Self { parent, root }
    }
}

impl Drop for Clone {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.parent);
    }
}

#[rustybench::bench_group(sample_count = 7, sample_size = 1)]
mod delete {
    use super::*;

    /// Deletes every entry of a fresh clone of the fixture through the deletion engine, as the
    /// interface does after `*`, Tab, Ctrl-R, Enter. The clone, its scan, and the plan are inputs.
    #[rustybench::bench]
    fn whole_fixture(bencher: Bencher) {
        bencher
            .counter(ItemsCount::new(FIXTURE_ENTRIES))
            .with_inputs(|| {
                let clone = Clone::new();
                let tree = Arc::new(index_fixture(&clone.root));
                let selected = tree.children(tree.root()).collect::<Vec<_>>();
                let plan = create_plan(Arc::clone(&tree), &selected).expect("fixture entries are deletable");
                let root = open_root(&clone.root).expect("clone root opens").try_clone_fd().unwrap();
                (clone, Some((plan, root)))
            })
            .bench_local_refs(|(clone, work)| {
                let (plan, root) = work.take().expect("each input is used once");
                let (sender, receiver) = mpsc::sync_channel(8);
                let worker = std::thread::spawn(move || {
                    fdu_delete::execute(root, plan, Arc::new(AtomicBool::new(false)), sender)
                });
                let mut deleted = 0usize;
                while let Ok(event) = receiver.recv() {
                    match event {
                        DeleteEvent::Outcomes(outcomes) => {
                            deleted += outcomes.iter().filter(|o| o.kind == fdu_delete::OutcomeKind::Deleted).count()
                        }
                        DeleteEvent::Failed { message } => panic!("deletion failed: {message}"),
                        DeleteEvent::Progress { .. } => {}
                        DeleteEvent::Finished => break,
                    }
                }
                worker.join().expect("delete worker exits");
                assert_eq!(deleted, FIXTURE_ENTRIES);
                assert_eq!(std::fs::read_dir(&clone.root).unwrap().count(), 0);
            });
    }
}
