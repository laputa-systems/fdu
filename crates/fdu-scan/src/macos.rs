use crate::fsutil::{self, retry};
use crate::indexed::{BulkMetadata, DirectoryFacts, EntryHint, IndexedScanLimits};
use crate::{ScanExclusions, ScanReport, SummaryExclusion, TopLevelDirectory};
use fdu_core::{EntryType, FileIdentity};
use rustix::fs::{self, Dir, DirEntry, FileType};
use std::cell::RefCell;
use hashbrown::{HashMap, HashSet};
use std::ffi::{CStr, CString, OsStr, OsString};
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

mod attributes;

const BULK_RECORD_BUFFER_BYTES: usize = 64 * 1024;
const INDEXED_SCAN_WORKER_LIMIT: usize = 128;
const STORAGE_WAIT_WORKER_HEADROOM: usize = 118;
const BASE_WORKER_CORE_RESERVE: usize = 2;
const INDEXED_TASKS_PER_WORKER: usize = 8;

pub(super) type DirectoryBuffer = attributes::AlignedBuffer<BULK_RECORD_BUFFER_BYTES>;

pub(super) fn indexed_scan_limits() -> IndexedScanLimits {
    let cores = thread::available_parallelism().map(usize::from).unwrap_or(1);
    let workers = cores.saturating_add(STORAGE_WAIT_WORKER_HEADROOM).min(INDEXED_SCAN_WORKER_LIMIT);
    let base_workers = cores.saturating_sub(BASE_WORKER_CORE_RESERVE).max(1);
    IndexedScanLimits {
        workers,
        base_workers: base_workers.min(workers),
        queued_directories: workers.saturating_mul(INDEXED_TASKS_PER_WORKER),
        retained_directory_budget: usize::MAX,
    }
}

// Each scanning thread handles one directory at a time, so it can reuse its aligned bulk buffer.
thread_local! {
    static INDEXED_DIRECTORY_BUFFER: RefCell<Box<DirectoryBuffer>> =
        RefCell::new(Box::new(attributes::AlignedBuffer::new()));
}

pub(super) fn with_indexed_directory_buffer<T>(
    use_buffer: impl FnOnce(&mut DirectoryBuffer) -> T,
) -> T {
    INDEXED_DIRECTORY_BUFFER.with(|buffer| use_buffer(buffer.borrow_mut().as_mut()))
}

/// Facts about the scan root's filesystem that apply to every directory below it. macOS has none
/// that the scanner uses.
#[derive(Clone, Copy)]
pub(super) struct ScanHints;

pub(super) fn scan_hints(_root: BorrowedFd<'_>) -> ScanHints {
    ScanHints
}

struct DirectoryEntry {
    name: CString,
    kind: EntryKind,
    bulk: Option<BulkMetadata>,
}

#[derive(Clone, Copy)]
enum EntryKind {
    Directory,
    Other,
    Unknown,
}

fn entry_kind(file_type: FileType) -> EntryKind {
    match file_type {
        FileType::Directory => EntryKind::Directory,
        FileType::Unknown => EntryKind::Unknown,
        _ => EntryKind::Other,
    }
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct MountIdentity {
    device: u64,
    filesystem_id: [i32; 2],
    mount_point: Vec<u8>,
}

enum OpenedDirectory {
    InsideRoot(OwnedFd),
    MountBoundary,
}

/// Lists a directory, without `.` and `..`.
fn read_entries(directory: &OwnedFd) -> io::Result<Vec<DirectoryEntry>> {
    let mut entries = Vec::new();
    for entry in Dir::read_from(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            entries.push(DirectoryEntry {
                name: name.to_owned(),
                kind: entry_kind(entry.file_type()),
                bulk: None,
            });
        }
    }
    Ok(entries)
}

fn read_entries_with_metadata(directory: &OwnedFd) -> io::Result<Vec<DirectoryEntry>> {
    with_indexed_directory_buffer(|buffer| {
        let mut entries = Vec::new();
        let mut stream = IndexedDirectoryStream::new(directory.as_fd(), buffer, ScanHints);
        while let Some(entry) = stream.next_entry()? {
            let name_bytes = entry.name().to_bytes();
            if name_bytes == b"." || name_bytes == b".." {
                continue;
            }
            let owned = CString::new(name_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "directory entry has interior NUL")
            })?;
            match entry {
                StreamEntry::Bulk { metadata, .. } => {
                    let kind = match metadata {
                        Some(meta) if meta.entry_type == EntryType::Directory => EntryKind::Directory,
                        Some(_) => EntryKind::Other,
                        None => EntryKind::Unknown,
                    };
                    entries.push(DirectoryEntry { name: owned, kind, bulk: metadata });
                }
                StreamEntry::Listed(listed) => {
                    entries.push(DirectoryEntry {
                        name: owned,
                        kind: entry_kind(listed.file_type()),
                        bulk: None,
                    });
                }
            }
        }
        Ok(entries)
    })
}

/// One directory entry. Bulk enumeration carries its metadata; the fallback carries a name and a
/// type.
pub(super) enum StreamEntry<'a> {
    Bulk { name: &'a CStr, metadata: Option<BulkMetadata> },
    Listed(DirEntry),
}

impl StreamEntry<'_> {
    pub(super) fn name(&self) -> &CStr {
        match self {
            Self::Bulk { name, .. } => name,
            Self::Listed(entry) => entry.file_name(),
        }
    }

    pub(super) fn hint(&self) -> EntryHint {
        match self {
            Self::Bulk { metadata: Some(metadata), .. } if metadata.entry_type == EntryType::Directory => {
                EntryHint::Directory
            }
            Self::Bulk { metadata: Some(_), .. } => EntryHint::NotDirectory,
            Self::Bulk { metadata: None, .. } => EntryHint::Unknown,
            Self::Listed(entry) => match entry.file_type() {
                FileType::Directory => EntryHint::Directory,
                FileType::Unknown => EntryHint::Unknown,
                _ => EntryHint::NotDirectory,
            },
        }
    }

    pub(super) fn bulk(&self) -> Option<BulkMetadata> {
        match self {
            Self::Bulk { metadata, .. } => *metadata,
            Self::Listed(_) => None,
        }
    }
}

pub(super) struct IndexedDirectoryStream<'buffer> {
    directory: BorrowedFd<'buffer>,
    buffer: &'buffer mut DirectoryBuffer,
    offset: usize,
    remaining: usize,
    exhausted: bool,
    returned_any: bool,
    fallback: Option<Dir>,
}

impl<'buffer> IndexedDirectoryStream<'buffer> {
    pub(super) fn new(
        directory: BorrowedFd<'buffer>,
        buffer: &'buffer mut DirectoryBuffer,
        _hints: ScanHints,
    ) -> Self {
        Self {
            directory,
            buffer,
            offset: 0,
            remaining: 0,
            exhausted: false,
            returned_any: false,
            fallback: None,
        }
    }

    pub(super) fn next_entry(&mut self) -> io::Result<Option<StreamEntry<'_>>> {
        loop {
            if let Some(fallback) = &mut self.fallback {
                return match fallback.read() {
                    Some(Ok(entry)) => Ok(Some(StreamEntry::Listed(entry))),
                    Some(Err(error)) => Err(error.into()),
                    None => Ok(None),
                };
            }
            if self.exhausted {
                return Ok(None);
            }
            if self.remaining == 0 && !self.refill()? {
                return Ok(None);
            }
            if self.fallback.is_some() {
                continue;
            }

            let start = self.offset;
            let length = attributes::read_record_length(&self.buffer.as_bytes()[start..])?;
            let end = start
                .checked_add(length)
                .filter(|end| length >= 24 && *end <= self.buffer.as_bytes().len())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "macOS directory record exceeds its buffer",
                    )
                })?;
            self.offset = end;
            self.remaining -= 1;
            self.returned_any = true;
            let entry = attributes::parse_record(&self.buffer.as_bytes()[start..end])?;
            return Ok(Some(StreamEntry::Bulk { name: entry.name, metadata: entry.metadata }));
        }
    }

    fn refill(&mut self) -> io::Result<bool> {
        loop {
            let mut requested = attributes::requested_attributes();
            // SAFETY: the descriptor stays open for the lifetime of the stream, the attrlist is
            // initialized, and the aligned buffer is writable for its full size.
            let count = unsafe {
                libc::getattrlistbulk(
                    self.directory.as_raw_fd(),
                    (&mut requested as *mut libc::attrlist).cast(),
                    self.buffer.as_mut_bytes().as_mut_ptr().cast(),
                    self.buffer.as_bytes().len(),
                    u64::from(libc::FSOPT_PACK_INVAL_ATTRS),
                )
            };
            if count > 0 {
                self.offset = 0;
                self.remaining = usize::try_from(count).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid macOS directory record count")
                })?;
                return Ok(true);
            }
            if count == 0 {
                self.exhausted = true;
                return Ok(false);
            }

            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            let fallback_error = matches!(
                error.raw_os_error(),
                Some(libc::EACCES | libc::EPERM | libc::ENOTSUP | libc::EOPNOTSUPP | libc::ENOSYS)
            ) || error.kind() == io::ErrorKind::Unsupported;
            if !self.returned_any && fallback_error {
                // Nothing was returned, so the plain listing starts from the beginning.
                self.fallback = Some(Dir::read_from(self.directory)?);
                return Ok(true);
            }
            return Err(error);
        }
    }
}

/// How long one directory open may take before the summary skips that directory. Opening a
/// folder under macOS privacy protection blocks until the user answers its consent prompt.
const SUMMARY_OPEN_PATIENCE: Duration = Duration::from_secs(2);
/// An open pending this long hands the rest of the work to a fresh worker, so waits overlap.
const SUMMARY_OPEN_HANDOFF: Duration = Duration::from_millis(50);
/// Workers alive at once, most of them waiting in an open.
const SUMMARY_WORKER_LIMIT: usize = 32;
const SUMMARY_WORKER_STACK_BYTES: usize = 256 * 1024;

pub(super) fn scan(path: &Path, apparent: bool, exclusions: &ScanExclusions) -> io::Result<ScanReport> {
    let root = fsutil::open_directory(path)?;
    let entries = read_entries(&root)?;
    let root_mount = mount_identity(root.as_fd())?;
    let mut state = SummaryState {
        rows: Vec::new(),
        paths: Vec::new(),
        pending: Vec::new(),
        seen_directories: HashSet::new(),
        skipped_entries: 0,
        mount_boundaries: 0,
        unsupported_aliases: 0,
        timed_out: Vec::new(),
        workers: HashMap::new(),
    };
    state.seen_directories.insert(fsutil::identity_of(root.as_fd())?);

    for entry in entries {
        let exclusion_node = exclusions.child(exclusions.root_node(), entry.name.as_bytes());
        if exclusion_node.is_some_and(|node| exclusions.excludes(node)) {
            continue;
        }
        let is_directory = match entry.kind {
            EntryKind::Directory => true,
            EntryKind::Other => false,
            EntryKind::Unknown => match fsutil::stat_at(root.as_fd(), &entry.name) {
                Ok(stat) => fsutil::entry_type(&stat) == EntryType::Directory,
                Err(_) => {
                    mark_skipped(&mut state.skipped_entries);
                    continue;
                }
            },
        };
        if !is_directory {
            continue;
        }
        state.paths.push(SummaryPath {
            parent: None,
            name: entry.name.clone(),
            row: state.rows.len(),
            exclusion_node: exclusion_node.filter(|node| exclusions.has_children(*node)),
        });
        state.rows.push(SummaryRow {
            name: entry.name,
            directory: None,
            disk_size: 0,
            state: RowState::Listed,
        });
    }
    state.pending.extend((0..state.paths.len()).rev());

    let scan = Arc::new(SummaryScan {
        state: Mutex::new(state),
        changed: Condvar::new(),
        root,
        root_mount,
        apparent,
        exclusions: exclusions.clone(),
    });
    fn poisoned<T>(_: T) -> io::Error {
        io::Error::other("summary scan worker panicked")
    }
    let mut handles = HashMap::new();
    let mut next_worker = 0u64;
    let mut state = scan.state.lock().map_err(poisoned)?;
    loop {
        let now = Instant::now();
        let timed_out = state
            .workers
            .iter()
            .filter_map(|(worker, phase)| match phase {
                WorkerPhase::Opening { path, started } if now - *started >= SUMMARY_OPEN_PATIENCE => {
                    Some((*worker, *path))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for (worker, path) in timed_out {
            state.workers.remove(&worker);
            handles.remove(&worker);
            state.timed_out.push(path);
            state.exclude_top_level(path, SummaryExclusion::OpenTimedOut);
        }
        handles.retain(|worker, handle: &mut thread::JoinHandle<()>| {
            !handle.is_finished() || state.workers.contains_key(worker)
        });
        if handles.values().any(thread::JoinHandle::is_finished) {
            return Err(io::Error::other("summary scan worker stopped before finishing"));
        }
        if state.pending.is_empty() && state.workers.is_empty() {
            break;
        }
        let progressing = state.workers.values().any(|phase| match phase {
            WorkerPhase::Listing => true,
            WorkerPhase::Opening { started, .. } => now - *started < SUMMARY_OPEN_HANDOFF,
        });
        if !progressing && !state.pending.is_empty() && state.workers.len() < SUMMARY_WORKER_LIMIT {
            state.workers.insert(next_worker, WorkerPhase::Listing);
            handles.insert(next_worker, spawn_summary_worker(&scan, next_worker)?);
            next_worker += 1;
        }
        state = scan.changed.wait_timeout(state, SUMMARY_OPEN_HANDOFF).map_err(poisoned)?.0;
    }
    Ok(state.report())
}

struct SummaryScan {
    state: Mutex<SummaryState>,
    /// Signalled when a worker stops, so the scan can finish or hand work on without waiting.
    changed: Condvar,
    root: OwnedFd,
    root_mount: MountIdentity,
    apparent: bool,
    exclusions: ScanExclusions,
}

/// The summary's remaining work and results, shared so that a fresh worker can carry on when
/// the current one is stuck in an open.
struct SummaryState {
    rows: Vec<SummaryRow>,
    paths: Vec<SummaryPath>,
    pending: Vec<usize>,
    seen_directories: HashSet<FileIdentity>,
    skipped_entries: usize,
    mount_boundaries: usize,
    unsupported_aliases: usize,
    timed_out: Vec<usize>,
    /// Live workers. One abandoned in an open is removed and discards its result if the open
    /// ever returns.
    workers: HashMap<u64, WorkerPhase>,
}

/// A directory below the root. Top-level directories have no parent and are opened from the
/// root; the rest are opened from their top-level directory.
struct SummaryPath {
    parent: Option<usize>,
    name: CString,
    row: usize,
    exclusion_node: Option<usize>,
}

struct SummaryRow {
    name: CString,
    directory: Option<Arc<OwnedFd>>,
    disk_size: u64,
    state: RowState,
}

enum RowState {
    Listed,
    Dropped,
    Excluded(SummaryExclusion),
}

enum WorkerPhase {
    Opening { path: usize, started: Instant },
    Listing,
}

/// What one directory contributed, gathered without holding the summary lock.
struct DirectoryListing {
    disk_size: u64,
    skipped_entries: usize,
    child_directories: Vec<(CString, Option<usize>)>,
}

impl SummaryState {
    /// The directory to open `path` from, and the names leading from it to `path`.
    fn open_plan(&self, path: usize) -> (Option<Arc<OwnedFd>>, Vec<CString>) {
        let mut names = Vec::new();
        let mut current = path;
        loop {
            let entry = &self.paths[current];
            match entry.parent {
                Some(parent) => {
                    names.push(entry.name.clone());
                    current = parent;
                }
                None if current == path => return (None, vec![entry.name.clone()]),
                None => break,
            }
        }
        names.reverse();
        let top_level = self.rows[self.paths[path].row]
            .directory
            .clone()
            .expect("a top-level directory is open before its children are listed");
        (Some(top_level), names)
    }

    fn drop_top_level(&mut self, path: usize) {
        if self.paths[path].parent.is_none() {
            let row = self.paths[path].row;
            self.rows[row].state = RowState::Dropped;
        }
    }

    fn exclude_top_level(&mut self, path: usize, exclusion: SummaryExclusion) {
        if self.paths[path].parent.is_none() {
            let row = self.paths[path].row;
            self.rows[row].state = RowState::Excluded(exclusion);
        }
    }

    fn accept_opened(&mut self, path: usize, opened: io::Result<OpenedDirectory>) -> Option<Arc<OwnedFd>> {
        let directory = match opened {
            Ok(OpenedDirectory::InsideRoot(directory)) => directory,
            Ok(OpenedDirectory::MountBoundary) => {
                mark_mount_boundary(&mut self.mount_boundaries);
                self.exclude_top_level(path, SummaryExclusion::MountBoundary);
                return None;
            }
            Err(_) => {
                mark_skipped(&mut self.skipped_entries);
                self.drop_top_level(path);
                return None;
            }
        };
        let identity = match fsutil::identity_of(directory.as_fd()) {
            Ok(identity) => identity,
            Err(_) => {
                mark_skipped(&mut self.skipped_entries);
                self.drop_top_level(path);
                return None;
            }
        };
        if !self.seen_directories.insert(identity) {
            mark_unsupported_alias(&mut self.unsupported_aliases);
            self.exclude_top_level(path, SummaryExclusion::UnsupportedAlias);
            return None;
        }
        let directory = Arc::new(directory);
        if self.paths[path].parent.is_none() {
            let row = self.paths[path].row;
            self.rows[row].directory = Some(Arc::clone(&directory));
        }
        Some(directory)
    }

    fn merge(&mut self, path: usize, listing: io::Result<DirectoryListing>) {
        let listing = match listing {
            Ok(listing) => listing,
            Err(_) => {
                mark_skipped(&mut self.skipped_entries);
                self.drop_top_level(path);
                return;
            }
        };
        let row = self.paths[path].row;
        add_size(&mut self.rows[row].disk_size, listing.disk_size, &mut self.skipped_entries);
        self.skipped_entries = self.skipped_entries.saturating_add(listing.skipped_entries);
        for (name, exclusion_node) in listing.child_directories {
            self.pending.push(self.paths.len());
            self.paths.push(SummaryPath { parent: Some(path), name, row, exclusion_node });
        }
    }

    fn relative_path(&self, path: usize) -> PathBuf {
        let mut names = Vec::new();
        let mut current = Some(path);
        while let Some(index) = current {
            names.push(OsStr::from_bytes(self.paths[index].name.as_bytes()));
            current = self.paths[index].parent;
        }
        names.iter().rev().collect()
    }

    fn report(&self) -> ScanReport {
        let mut directories = self
            .rows
            .iter()
            .filter_map(|row| {
                let exclusion = match row.state {
                    RowState::Listed => None,
                    RowState::Dropped => return None,
                    RowState::Excluded(exclusion) => Some(exclusion),
                };
                Some(TopLevelDirectory {
                    name: OsString::from_vec(row.name.as_bytes().to_vec()),
                    disk_size: if exclusion.is_some() { 0 } else { row.disk_size },
                    exclusion,
                })
            })
            .collect::<Vec<_>>();
        directories.sort_unstable_by(|left, right| {
            left.exclusion
                .is_some()
                .cmp(&right.exclusion.is_some())
                .then_with(|| right.disk_size.cmp(&left.disk_size))
        });
        ScanReport {
            directories,
            skipped_entries: self.skipped_entries,
            mount_boundaries: self.mount_boundaries,
            unsupported_aliases: self.unsupported_aliases,
            timed_out_directories: self.timed_out.iter().map(|path| self.relative_path(*path)).collect(),
        }
    }
}

fn spawn_summary_worker(scan: &Arc<SummaryScan>, worker: u64) -> io::Result<thread::JoinHandle<()>> {
    let scan = Arc::clone(scan);
    thread::Builder::new()
        .name("fdu-summary".to_owned())
        .stack_size(SUMMARY_WORKER_STACK_BYTES)
        .spawn(move || summary_worker(&scan, worker))
}

fn summary_worker(scan: &SummaryScan, worker: u64) {
    let lock = || scan.state.lock().expect("summary scan state is not poisoned");
    loop {
        let (path, base, names) = {
            let mut state = lock();
            if !state.workers.contains_key(&worker) {
                return;
            }
            let Some(path) = state.pending.pop() else {
                state.workers.remove(&worker);
                scan.changed.notify_all();
                return;
            };
            let (base, names) = state.open_plan(path);
            state.workers.insert(worker, WorkerPhase::Opening { path, started: Instant::now() });
            (path, base, names)
        };
        let opened = open_relative(base.as_deref().map_or(scan.root.as_fd(), AsFd::as_fd), &names, &scan.root_mount);
        drop(base);
        let (directory, exclusion_node) = {
            let mut state = lock();
            match state.workers.get_mut(&worker) {
                Some(phase) => *phase = WorkerPhase::Listing,
                None => return,
            }
            match state.accept_opened(path, opened) {
                Some(directory) => (directory, state.paths[path].exclusion_node),
                None => continue,
            }
        };
        let listing = list_directory(&directory, exclusion_node, scan);
        drop(directory);
        lock().merge(path, listing);
    }
}

fn list_directory(
    directory: &OwnedFd,
    exclusion_node: Option<usize>,
    scan: &SummaryScan,
) -> io::Result<DirectoryListing> {
    let exclusions = &scan.exclusions;
    let mut listing = DirectoryListing { disk_size: 0, skipped_entries: 0, child_directories: Vec::new() };
    for entry in read_entries_with_metadata(directory)? {
        let child_exclusion_node = exclusions.child(exclusion_node, entry.name.as_bytes());
        if child_exclusion_node.is_some_and(|node| exclusions.excludes(node)) {
            continue;
        }
        let size = match entry.kind {
            EntryKind::Directory => None,
            EntryKind::Other => match entry.bulk {
                Some(bulk) => Some(Ok(if scan.apparent { bulk.apparent_bytes } else { bulk.allocated_bytes })),
                None => Some(
                    fsutil::stat_at(directory.as_fd(), &entry.name)
                        .and_then(|stat| fsutil::file_size(&stat, scan.apparent)),
                ),
            },
            EntryKind::Unknown => match fsutil::stat_at(directory.as_fd(), &entry.name) {
                Ok(stat) if fsutil::entry_type(&stat) == EntryType::Directory => None,
                Ok(stat) => Some(fsutil::file_size(&stat, scan.apparent)),
                Err(error) => Some(Err(error)),
            },
        };
        match size {
            None => listing.child_directories.push((
                entry.name,
                child_exclusion_node.filter(|node| exclusions.has_children(*node)),
            )),
            Some(Ok(size)) => add_size(&mut listing.disk_size, size, &mut listing.skipped_entries),
            Some(Err(_)) => mark_skipped(&mut listing.skipped_entries),
        }
    }
    Ok(listing)
}

/// Opens `names` in turn below `base`, stopping at the first one on another mount.
fn open_relative(
    base: BorrowedFd<'_>,
    names: &[CString],
    root_mount: &MountIdentity,
) -> io::Result<OpenedDirectory> {
    let mut current: Option<OwnedFd> = None;
    for name in names {
        let parent = current.as_ref().map_or(base, AsFd::as_fd);
        let directory = fsutil::open_directory_at(parent, name.as_c_str())?;
        match keep_directory_on_root_mount(directory, root_mount)? {
            OpenedDirectory::InsideRoot(directory) => current = Some(directory),
            OpenedDirectory::MountBoundary => return Ok(OpenedDirectory::MountBoundary),
        }
    }
    current
        .map(OpenedDirectory::InsideRoot)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no directory names to open"))
}

fn keep_directory_on_root_mount(
    directory: OwnedFd,
    root_mount: &MountIdentity,
) -> io::Result<OpenedDirectory> {
    let device = fsutil::identity_of(directory.as_fd())?.device;
    if same_mount_on_fd(directory.as_fd(), root_mount, device)? {
        Ok(OpenedDirectory::InsideRoot(directory))
    } else {
        Ok(OpenedDirectory::MountBoundary)
    }
}

#[cfg(test)]
fn same_mount(left: &MountIdentity, right: &MountIdentity) -> bool {
    left == right
}

pub(super) fn mount_identity(fd: BorrowedFd<'_>) -> io::Result<MountIdentity> {
    let device = fsutil::identity_of(fd)?.device;
    let filesystem = retry(|| fs::fstatfs(fd))?;
    let mount_point = filesystem
        .f_mntonname
        .iter()
        .take_while(|byte| **byte != 0)
        .map(|byte| *byte as u8)
        .collect();

    Ok(MountIdentity {
        device,
        filesystem_id: fsid_words(&filesystem),
        mount_point,
    })
}

fn same_mount_on_fd(
    fd: BorrowedFd<'_>,
    root_mount: &MountIdentity,
    device: u64,
) -> io::Result<bool> {
    let filesystem = retry(|| fs::fstatfs(fd))?;
    let mount_point = filesystem
        .f_mntonname
        .iter()
        .take_while(|byte| **byte != 0)
        .map(|byte| *byte as u8);
    Ok(device == root_mount.device
        && fsid_words(&filesystem) == root_mount.filesystem_id
        && mount_point.eq(root_mount.mount_point.iter().copied()))
}

/// Identity, link count, and mount membership of an opened directory.
pub(super) fn describe_directory(
    fd: BorrowedFd<'_>,
    root_mount: &MountIdentity,
) -> io::Result<DirectoryFacts> {
    let (identity, link_count, modified_time) = fsutil::identity_link_count_and_modified_time(fd)?;
    Ok(DirectoryFacts {
        identity,
        modified_time,
        link_count,
        same_mount: same_mount_on_fd(fd, root_mount, identity.device)?,
    })
}

fn fsid_words(filesystem: &rustix::fs::StatFs) -> [i32; 2] {
    // Apple's fsid_t is exactly two i32 words; libc keeps those fields private.
    const _: () = assert!(std::mem::size_of::<[i32; 2]>() == 8);
    // SAFETY: fsid_t is two i32 words, so reading eight bytes from it stays in bounds.
    unsafe { std::mem::transmute_copy::<_, [i32; 2]>(&filesystem.f_fsid) }
}

fn add_size(total: &mut u64, size: u64, skipped_entries: &mut usize) {
    match total.checked_add(size) {
        Some(value) => *total = value,
        None => mark_skipped(skipped_entries),
    }
}

fn mark_skipped(skipped_entries: &mut usize) {
    *skipped_entries = skipped_entries.saturating_add(1);
}

fn mark_mount_boundary(mount_boundaries: &mut usize) {
    *mount_boundaries = mount_boundaries.saturating_add(1);
}

fn mark_unsupported_alias(unsupported_aliases: &mut usize) {
    *unsupported_aliases = unsupported_aliases.saturating_add(1);
}

#[cfg(test)]
mod tests {
    use super::{same_mount, MountIdentity};

    #[test]
    fn mount_identity_separates_filesystems_and_mount_points() {
        let root = MountIdentity {
            device: 1,
            filesystem_id: [2, 3],
            mount_point: b"/volume".to_vec(),
        };
        assert!(same_mount(&root, &root));
        let nested_mount = MountIdentity {
            device: 1,
            filesystem_id: [2, 3],
            mount_point: b"/volume/nested".to_vec(),
        };
        assert!(!same_mount(&root, &nested_mount));
        assert!(!same_mount(
            &root,
            &MountIdentity {
                device: 4,
                filesystem_id: [2, 3],
                mount_point: b"/volume".to_vec(),
            }
        ));
    }
}
