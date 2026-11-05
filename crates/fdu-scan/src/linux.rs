use crate::fsutil::{self, retry};
use crate::{ScanExclusions, ScanReport, TopLevelDirectory};
use rayon::prelude::*;
use rustix::fs::{self, AtFlags, FileType, Mode, OFlags, RawDir, ResolveFlags, Statx, StatxFlags};
use rustix::io::Errno;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::ffi::{CStr, OsString};
use std::io;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ops::Deref;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) mod platform;

// A small reusable buffer bounds retained memory while batching wide directories.
const DIRECTORY_BUFFER_BYTES: usize = 64 * 1024;
// These limits retain at most 512 KiB of parsed records in each scanning thread's pool.
const MAX_CACHED_DIRECTORY_ENTRY_CAPACITY: usize = 4096;
const MAX_CACHED_DIRECTORY_ENTRY_VECTORS: usize = 8;
// Only wide directories need metadata jobs beyond the parallel directory traversal: the first
// files are sized in place, and later ones are gathered into batches that run in parallel.
const SERIAL_FILE_LIMIT: usize = 128;
const PARALLEL_FILE_BATCH: usize = 2048;
// Growth hints for the names and entries of a directory that outgrows its inline storage.
const NAME_RESERVE_HINT: usize = 512;
const ENTRY_RESERVE_HINT: usize = 64;
const EXT4_SUPER_MAGIC: u64 = 0xef53;
pub(crate) const EXT4_HTREE_EOF_32BIT: u64 = 0x7fff_ffff;
pub(crate) const EXT4_HTREE_EOF_64BIT: u64 = i64::MAX as u64;
// Switch to heap-backed descent before a deep tree can exhaust a Rayon worker stack.
const MAX_RECURSIVE_DIRECTORY_DEPTH: usize = 64;

thread_local! {
    static DIRECTORY_BUFFER: RefCell<Vec<MaybeUninit<u8>>> = const { RefCell::new(Vec::new()) };
}

// A metadata batch can yield to another directory job on the same Rayon worker.
// Move the buffer out of TLS so that nested enumeration leases independent storage.
struct DirectoryBuffer {
    bytes: Vec<MaybeUninit<u8>>,
    _worker: PhantomData<Rc<()>>,
}

impl DirectoryBuffer {
    fn with<T>(inspect: impl FnOnce(&mut [MaybeUninit<u8>]) -> T) -> T {
        let mut bytes = DIRECTORY_BUFFER.with(|buffer| mem::take(&mut *buffer.borrow_mut()));
        if bytes.is_empty() {
            bytes.resize(DIRECTORY_BUFFER_BYTES, MaybeUninit::uninit());
        }
        let mut buffer = Self {
            bytes,
            _worker: PhantomData,
        };
        inspect(&mut buffer.bytes)
    }
}

impl Drop for DirectoryBuffer {
    fn drop(&mut self) {
        let bytes = mem::take(&mut self.bytes);
        let _ = DIRECTORY_BUFFER.try_with(|buffer| {
            let mut cached = buffer.borrow_mut();
            if cached.is_empty() {
                *cached = bytes;
            }
        });
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DirectoryEntry {
    name_offset: usize,
    name_length: u16,
    kind: EntryKind,
}

thread_local! {
    static DIRECTORY_ENTRY_POOL: RefCell<Vec<Vec<DirectoryEntry>>> = const { RefCell::new(Vec::new()) };
}

// The Rc marker keeps each buffer lease on the thread whose TLS pool owns its capacity.
struct PooledDirectoryEntries {
    entries: DirectoryEntryStorage,
    _worker: PhantomData<Rc<()>>,
}

// Single-child chains retain their records without allocating at every level of descent.
enum DirectoryEntryStorage {
    Inline {
        entries: [DirectoryEntry; 2],
        len: usize,
    },
    Heap(Vec<DirectoryEntry>),
}

impl PooledDirectoryEntries {
    fn take() -> Self {
        Self {
            entries: DirectoryEntryStorage::Inline {
                entries: [DirectoryEntry {
                    name_offset: 0,
                    name_length: 0,
                    kind: EntryKind::Unknown,
                }; 2],
                len: 0,
            },
            _worker: PhantomData,
        }
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        match &self.entries {
            DirectoryEntryStorage::Inline { entries, .. } => entries.len(),
            DirectoryEntryStorage::Heap(entries) => entries.capacity(),
        }
    }

    fn reserve(&mut self, additional: usize) {
        if let DirectoryEntryStorage::Inline { entries, len } = &self.entries {
            if *len + additional <= entries.len() {
                return;
            }
            let mut heap = DIRECTORY_ENTRY_POOL.with(|pool| {
                pool.borrow_mut().pop().unwrap_or_default()
            });
            heap.reserve(*len + additional);
            heap.extend_from_slice(&entries[..*len]);
            self.entries = DirectoryEntryStorage::Heap(heap);
        } else if let DirectoryEntryStorage::Heap(entries) = &mut self.entries {
            entries.reserve(additional);
        }
    }

    fn push(&mut self, entry: DirectoryEntry, capacity_hint: usize) {
        if let DirectoryEntryStorage::Inline { entries, len } = &mut self.entries {
            if *len < entries.len() {
                entries[*len] = entry;
                *len += 1;
                return;
            }
            let additional = capacity_hint.saturating_sub(*len).max(1);
            self.reserve(additional);
        }
        let DirectoryEntryStorage::Heap(entries) = &mut self.entries else {
            unreachable!("inline entries promote before exceeding their capacity");
        };
        entries.push(entry);
    }
}

impl Deref for PooledDirectoryEntries {
    type Target = [DirectoryEntry];

    fn deref(&self) -> &Self::Target {
        match &self.entries {
            DirectoryEntryStorage::Inline { entries, len } => &entries[..*len],
            DirectoryEntryStorage::Heap(entries) => entries,
        }
    }
}

impl Drop for PooledDirectoryEntries {
    fn drop(&mut self) {
        let DirectoryEntryStorage::Heap(entries) = &mut self.entries else {
            return;
        };
        if entries.capacity() > MAX_CACHED_DIRECTORY_ENTRY_CAPACITY {
            return;
        }
        entries.clear();
        let entries = mem::take(entries);
        let _ = DIRECTORY_ENTRY_POOL.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_CACHED_DIRECTORY_ENTRY_VECTORS {
                pool.push(entries);
            }
        });
    }
}

struct DirectoryEntries {
    names: DirectoryNames,
    entries: PooledDirectoryEntries,
    // Inline subtotal for known non-directory entries sized during enumeration.
    disk_size: u64,
}

struct DirectoryFrame<'a> {
    directory: OwnedFd,
    names: DirectoryNames,
    entries: PooledDirectoryEntries,
    next_entry: usize,
    disk_size: u64,
    overflowed: bool,
    position: SummaryScanPosition<'a>,
}

/// Carries the current path filter and depth through recursive and iterative traversal alike.
#[derive(Clone, Copy)]
struct SummaryScanPosition<'a> {
    exclusions: &'a ScanExclusions,
    exclusion_node: Option<usize>,
    ext4_eof_cookie: bool,
    depth: usize,
}

impl<'a> SummaryScanPosition<'a> {
    fn root(exclusions: &'a ScanExclusions, ext4_eof_cookie: bool) -> Self {
        Self {
            exclusions,
            exclusion_node: exclusions.root_node(),
            ext4_eof_cookie,
            depth: 0,
        }
    }

    fn child(self, name: &[u8]) -> Self {
        let exclusion_node = self.exclusions.child(self.exclusion_node, name);
        Self {
            exclusions: self.exclusions,
            exclusion_node: exclusion_node.filter(|node| self.exclusions.has_children(*node)),
            ext4_eof_cookie: self.ext4_eof_cookie,
            depth: self.depth + 1,
        }
    }

    fn excludes_child(self, name: &[u8]) -> bool {
        self.exclusions
            .child(self.exclusion_node, name)
            .is_some_and(|node| self.exclusions.excludes(node))
    }
}

enum ScannedEntry {
    Item(u64),
    MountBoundary,
    Directory {
        directory: OwnedFd,
        ext4_eof_cookie: bool,
    },
}

enum OpenedChildDirectory {
    MountBoundary,
    Directory(OwnedFd, bool),
}

enum UnknownEntry {
    Directory,
    Item(u64),
}

// Keep short directory name lists inline so small directories avoid a separate name allocation.
enum DirectoryNames {
    Inline([u8; 16]),
    Heap(Vec<u8>),
}

impl DirectoryNames {
    fn new() -> Self {
        Self::Inline([0; 16])
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline(bytes) => &bytes[..inline_name_bytes_len(bytes)],
            Self::Heap(names) => names,
        }
    }

    fn append_name(&mut self, name: &[u8], reserve_hint: usize) -> usize {
        let additional = name
            .len()
            .checked_add(1)
            .expect("directory name length is representable");
        if let Self::Inline(bytes) = self {
            let current_len = inline_name_bytes_len(bytes);
            if current_len + additional <= bytes.len() {
                bytes[current_len..current_len + name.len()].copy_from_slice(name);
                bytes[current_len + name.len()] = 0;
                return current_len;
            }

            let inline = *bytes;
            let capacity = current_len
                .saturating_add(reserve_hint)
                .max(current_len.saturating_add(additional));
            let mut names = Vec::with_capacity(capacity);
            names.extend_from_slice(&inline[..current_len]);
            names.extend_from_slice(name);
            names.push(0);
            *self = Self::Heap(names);
            return current_len;
        }

        let Self::Heap(names) = self else {
            unreachable!("inline names transition to heap storage before overflow");
        };
        let start = names.len();
        names.extend_from_slice(name);
        names.push(0);
        start
    }
}

#[cfg(target_pointer_width = "64")]
const _: [(); 24] = [(); mem::size_of::<DirectoryNames>()];

// Stored names have no embedded NUL and each has exactly one trailing terminator.
fn inline_name_bytes_len(bytes: &[u8; 16]) -> usize {
    bytes
        .iter()
        .rposition(|byte| *byte != 0)
        .map_or(0, |last_name_byte| (last_name_byte + 2).min(bytes.len()))
}

impl DirectoryEntry {
    fn as_c_str<'a>(&self, names: &'a [u8]) -> &'a CStr {
        let end = self.name_offset + usize::from(self.name_length) + 1;
        // read_directory copies only bytes before the first NUL, then adds one terminator.
        unsafe { CStr::from_bytes_with_nul_unchecked(&names[self.name_offset..end]) }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum EntryKind {
    Directory,
    Other,
    Unknown,
}

const _: [(); 16] = [(); mem::size_of::<DirectoryEntry>()];


/// Whether the kernel filled in the statx fields in `flags`.
fn reports(stat: &Statx, flags: StatxFlags) -> bool {
    StatxFlags::from_bits_retain(stat.stx_mask).contains(flags)
}

fn is_directory(stat: &Statx) -> bool {
    reports(stat, StatxFlags::TYPE) && FileType::from_raw_mode(u32::from(stat.stx_mode)) == FileType::Directory
}

// Choose size accounting before descent so every metadata call requests a fixed field.
pub(super) fn scan(path: &Path, apparent: bool, exclusions: &ScanExclusions) -> io::Result<ScanReport> {
    if apparent {
        scan_mode::<true>(path, exclusions)
    } else {
        scan_mode::<false>(path, exclusions)
    }
}

fn scan_mode<const APPARENT: bool>(path: &Path, exclusions: &ScanExclusions) -> io::Result<ScanReport> {
    let root = fsutil::open_directory(path)?;
    let root_fd = root.as_fd();
    let ext4_eof_cookie = is_ext4(root_fd);
    let root_position = SummaryScanPosition::root(exclusions, ext4_eof_cookie);
    let skipped_entries = AtomicUsize::new(0);
    let mount_boundaries = AtomicUsize::new(0);
    let DirectoryEntries { names, entries, .. } = read_directory::<APPARENT, true>(
        root_fd,
        root_position,
        &skipped_entries,
    )?;
    let name_bytes = names.as_slice();

    let mut directories = entries
        .par_iter()
        .copied()
        .filter_map(|entry| {
            match scan_top_level_entry::<APPARENT>(
                root_fd,
                name_bytes,
                entry,
                root_position,
                &skipped_entries,
                &mount_boundaries,
            ) {
                Ok(directory) => directory,
                Err(_) => {
                    skipped_entries.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
        })
        .collect::<Vec<_>>();
    directories.sort_unstable_by_key(|directory| Reverse(directory.disk_size));

    Ok(ScanReport {
        directories,
        skipped_entries: skipped_entries.load(Ordering::Relaxed),
        mount_boundaries: mount_boundaries.load(Ordering::Relaxed),
        unsupported_aliases: 0,
        timed_out_directories: Vec::new(),
    })
}

fn scan_top_level_entry<const APPARENT: bool>(
    parent: BorrowedFd<'_>,
    names: &[u8],
    entry: DirectoryEntry,
    position: SummaryScanPosition<'_>,
    skipped_entries: &AtomicUsize,
    mount_boundaries: &AtomicUsize,
) -> io::Result<Option<TopLevelDirectory>> {
    let child_position = position.child(entry.as_c_str(names).to_bytes());
    if matches!(entry.kind, EntryKind::Other) {
        return Ok(None);
    }

    if matches!(entry.kind, EntryKind::Unknown) {
        let is_directory = with_stat_at(
            parent,
            entry.as_c_str(names),
            StatxFlags::TYPE,
            |stat| {
                if !reports(stat, StatxFlags::TYPE) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "filesystem did not report an unknown entry type",
                    ));
                }
                Ok(is_directory(stat))
            },
        )?;
        if !is_directory {
            return Ok(None);
        }
    }

    let (disk_size, exclusion) = match open_child_directory(
        parent,
        entry.as_c_str(names),
        child_position.ext4_eof_cookie,
    )? {
        OpenedChildDirectory::MountBoundary => {
            mount_boundaries.fetch_add(1, Ordering::Relaxed);
            (0, Some(crate::SummaryExclusion::MountBoundary))
        }
        OpenedChildDirectory::Directory(child, child_ext4_eof_cookie) => {
            let disk_size = scan_directory_contents::<APPARENT>(
                child,
                SummaryScanPosition {
                    ext4_eof_cookie: child_ext4_eof_cookie,
                    ..child_position
                },
                skipped_entries,
                mount_boundaries,
            )?;
            (disk_size, None)
        }
    };
    Ok(Some(TopLevelDirectory {
        name: OsString::from_vec(entry.as_c_str(names).to_bytes().to_vec()),
        disk_size,
        exclusion,
    }))
}

fn scan_directory_contents<const APPARENT: bool>(
    directory: OwnedFd,
    position: SummaryScanPosition<'_>,
    skipped_entries: &AtomicUsize,
    mount_boundaries: &AtomicUsize,
) -> io::Result<u64> {
    if position.depth >= MAX_RECURSIVE_DIRECTORY_DEPTH {
        return scan_directory_iterative::<APPARENT>(
            directory,
            position,
            skipped_entries,
            mount_boundaries,
        );
    }

    let DirectoryEntries {
        names,
        entries,
        disk_size,
    } = read_directory::<APPARENT, false>(directory.as_fd(), position, skipped_entries)?;
    if entries.is_empty() {
        return Ok(disk_size);
    }
    let name_bytes = names.as_slice();
    let directory_fd = directory.as_fd();
    let child_size = entries
        .par_iter()
        .copied()
        .map(|entry| {
            match scan_entry_size::<APPARENT>(
                directory_fd,
                name_bytes,
                entry,
                position,
                skipped_entries,
                mount_boundaries,
            ) {
                Ok(size) => Ok(size),
                Err(_) => {
                    skipped_entries.fetch_add(1, Ordering::Relaxed);
                    Ok(0)
                }
            }
        })
        .try_reduce(
            || 0u64,
            |left, right| {
                left.checked_add(right).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "directory total exceeds the supported byte-count range",
                    )
                })
            },
        )?;
    disk_size.checked_add(child_size).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "directory total exceeds the supported byte-count range",
        )
    })
}

impl<'a> DirectoryFrame<'a> {
    fn new<const APPARENT: bool>(
        directory: OwnedFd,
        position: SummaryScanPosition<'a>,
        skipped_entries: &AtomicUsize,
    ) -> io::Result<Self> {
        let DirectoryEntries {
            names,
            entries,
            disk_size,
        } = read_directory::<APPARENT, false>(directory.as_fd(), position, skipped_entries)?;
        Ok(Self {
            directory,
            names,
            entries,
            next_entry: 0,
            disk_size,
            overflowed: false,
            position,
        })
    }

    fn add_size(&mut self, size: u64) {
        if let Some(total) = self.disk_size.checked_add(size) {
            self.disk_size = total;
        } else {
            self.overflowed = true;
        }
    }

    fn finish(self) -> io::Result<u64> {
        if self.overflowed {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "directory total exceeds the supported byte-count range",
            ))
        } else {
            Ok(self.disk_size)
        }
    }
}

fn scan_directory_iterative<const APPARENT: bool>(
    directory: OwnedFd,
    position: SummaryScanPosition<'_>,
    skipped_entries: &AtomicUsize,
    mount_boundaries: &AtomicUsize,
) -> io::Result<u64> {
    let root = DirectoryFrame::new::<APPARENT>(directory, position, skipped_entries)?;
    let mut stack = vec![root];

    loop {
        let entry = {
            let frame = stack.last_mut().expect("root directory frame remains present");
            match frame.entries.get(frame.next_entry) {
                Some(entry) => {
                    frame.next_entry += 1;
                    Some(*entry)
                }
                None => None,
            }
        };

        if let Some(entry) = entry {
            let scanned = {
                let frame = stack.last().expect("root directory frame remains present");
                let child_position = frame.position.child(entry.as_c_str(frame.names.as_slice()).to_bytes());
                (
                    scan_entry_iterative::<APPARENT>(
                        frame.directory.as_fd(),
                        frame.names.as_slice(),
                        entry,
                        frame.position.ext4_eof_cookie,
                    ),
                    child_position,
                )
            };

            let (scanned, child_position) = scanned;
            match scanned {
                Ok(ScannedEntry::Item(size)) => stack
                    .last_mut()
                    .expect("root directory frame remains present")
                    .add_size(size),
                Ok(ScannedEntry::MountBoundary) => {
                    mount_boundaries.fetch_add(1, Ordering::Relaxed);
                }
                Ok(ScannedEntry::Directory {
                    directory,
                    ext4_eof_cookie: child_ext4_eof_cookie,
                }) => {
                    let child_position = SummaryScanPosition {
                        ext4_eof_cookie: child_ext4_eof_cookie,
                        ..child_position
                    };
                    match DirectoryFrame::new::<APPARENT>(directory, child_position, skipped_entries) {
                    Ok(frame) => stack.push(frame),
                    Err(_) => {
                        skipped_entries.fetch_add(1, Ordering::Relaxed);
                    }
                    }
                }
                Err(_) => {
                    skipped_entries.fetch_add(1, Ordering::Relaxed);
                }
            }
            continue;
        }

        let finished = stack
            .pop()
            .expect("root directory frame remains present")
            .finish();
        if let Some(parent) = stack.last_mut() {
            match finished {
                Ok(size) => parent.add_size(size),
                Err(_) => {
                    skipped_entries.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            return finished;
        }
    }
}

fn scan_entry_size<const APPARENT: bool>(
    parent: BorrowedFd<'_>,
    names: &[u8],
    entry: DirectoryEntry,
    position: SummaryScanPosition<'_>,
    skipped_entries: &AtomicUsize,
    mount_boundaries: &AtomicUsize,
) -> io::Result<u64> {
    let child_position = position.child(entry.as_c_str(names).to_bytes());
    match entry.kind {
        EntryKind::Directory => scan_child_directory::<APPARENT>(
            parent,
            entry.as_c_str(names),
            child_position,
            skipped_entries,
            mount_boundaries,
        ),
        EntryKind::Other => with_stat_at(
            parent,
            entry.as_c_str(names),
            entry_size_mask::<APPARENT>(),
            file_size::<APPARENT>,
        ),
        EntryKind::Unknown => {
            let scanned = with_stat_at(
                parent,
                entry.as_c_str(names),
                StatxFlags::TYPE | entry_size_mask::<APPARENT>(),
                |stat| {
                    if !reports(stat, StatxFlags::TYPE) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "filesystem did not report an unknown entry type",
                        ));
                    }
                    if is_directory(stat) {
                        Ok(UnknownEntry::Directory)
                    } else {
                        file_size::<APPARENT>(stat).map(UnknownEntry::Item)
                    }
                },
            )?;
            match scanned {
                UnknownEntry::Directory => scan_child_directory::<APPARENT>(
                    parent,
                    entry.as_c_str(names),
                    child_position,
                    skipped_entries,
                    mount_boundaries,
                ),
                UnknownEntry::Item(size) => Ok(size),
            }
        }
    }
}

fn scan_entry_iterative<const APPARENT: bool>(
    parent: BorrowedFd<'_>,
    names: &[u8],
    entry: DirectoryEntry,
    ext4_eof_cookie: bool,
) -> io::Result<ScannedEntry> {
    match entry.kind {
        EntryKind::Directory => open_child_entry(
            parent,
            entry.as_c_str(names),
            ext4_eof_cookie,
        ),
        EntryKind::Other => with_stat_at(
            parent,
            entry.as_c_str(names),
            entry_size_mask::<APPARENT>(),
            |stat| file_size::<APPARENT>(stat).map(ScannedEntry::Item),
        ),
        EntryKind::Unknown => {
            let scanned = with_stat_at(
                parent,
                entry.as_c_str(names),
                StatxFlags::TYPE | entry_size_mask::<APPARENT>(),
                |stat| {
                    if !reports(stat, StatxFlags::TYPE) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "filesystem did not report an unknown entry type",
                        ));
                    }
                    if is_directory(stat) {
                        Ok(UnknownEntry::Directory)
                    } else {
                        file_size::<APPARENT>(stat).map(UnknownEntry::Item)
                    }
                },
            )?;
            match scanned {
                UnknownEntry::Directory => open_child_entry(
                    parent,
                    entry.as_c_str(names),
                    ext4_eof_cookie,
                ),
                UnknownEntry::Item(size) => Ok(ScannedEntry::Item(size)),
            }
        }
    }
}

fn entry_size_mask<const APPARENT: bool>() -> StatxFlags {
    if APPARENT {
        StatxFlags::SIZE
    } else {
        StatxFlags::BLOCKS
    }
}

fn scan_child_directory<const APPARENT: bool>(
    parent: BorrowedFd<'_>,
    name: &CStr,
    position: SummaryScanPosition<'_>,
    skipped_entries: &AtomicUsize,
    mount_boundaries: &AtomicUsize,
) -> io::Result<u64> {
    let (child, child_ext4_eof_cookie) = match open_child_directory(
        parent,
        name,
        position.ext4_eof_cookie,
    )? {
        OpenedChildDirectory::MountBoundary => {
            mount_boundaries.fetch_add(1, Ordering::Relaxed);
            return Ok(0);
        }
        OpenedChildDirectory::Directory(child, child_ext4_eof_cookie) => {
            (child, child_ext4_eof_cookie)
        }
    };
    scan_directory_contents::<APPARENT>(
        child,
        SummaryScanPosition {
            ext4_eof_cookie: child_ext4_eof_cookie,
            ..position
        },
        skipped_entries,
        mount_boundaries,
    )
}

fn open_child_entry(
    parent: BorrowedFd<'_>,
    name: &CStr,
    ext4_eof_cookie: bool,
) -> io::Result<ScannedEntry> {
    match open_child_directory(parent, name, ext4_eof_cookie)? {
        OpenedChildDirectory::MountBoundary => Ok(ScannedEntry::MountBoundary),
        OpenedChildDirectory::Directory(directory, ext4_eof_cookie) => {
            Ok(ScannedEntry::Directory {
                directory,
                ext4_eof_cookie,
            })
        }
    }
}

/// Opens a child directory without leaving the parent's mount: `RESOLVE_NO_XDEV` where the
/// kernel supports it, otherwise a plain open followed by a mount ID comparison.
fn open_child_directory(
    parent_fd: BorrowedFd<'_>,
    name: &CStr,
    ext4_eof_cookie: bool,
) -> io::Result<OpenedChildDirectory> {
    match fsutil::with_descriptor_retry(|| retry(|| openat2_directory(parent_fd, name))) {
        Ok(child) => Ok(OpenedChildDirectory::Directory(child, ext4_eof_cookie)),
        Err(error) if error.raw_os_error() == Some(Errno::XDEV.raw_os_error()) => {
            Ok(OpenedChildDirectory::MountBoundary)
        }
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(code) if code == Errno::NOSYS.raw_os_error() || code == Errno::PERM.raw_os_error()
            ) =>
        {
            let child = fsutil::open_directory_at(parent_fd, name)?;
            if mount_id_for_fd(parent_fd)? == mount_id_for_fd(child.as_fd())? {
                Ok(OpenedChildDirectory::Directory(child, ext4_eof_cookie))
            } else {
                Ok(OpenedChildDirectory::MountBoundary)
            }
        }
        Err(error) => Err(error),
    }
}

fn openat2_directory(parent_fd: BorrowedFd<'_>, name: &CStr) -> rustix::io::Result<OwnedFd> {
    fs::openat2(
        parent_fd,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        ResolveFlags::NO_XDEV,
    )
}

#[inline(always)]
fn file_size<const APPARENT: bool>(stat: &Statx) -> io::Result<u64> {
    let size_mask = entry_size_mask::<APPARENT>();
    if !reports(stat, size_mask) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "filesystem did not report the requested file size",
        ));
    }
    if APPARENT {
        Ok(stat.stx_size)
    } else {
        stat.stx_blocks.checked_mul(512).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "file total exceeds the supported byte-count range",
            )
        })
    }
}

fn with_stat_at<T>(
    parent_fd: BorrowedFd<'_>,
    name: &CStr,
    mask: StatxFlags,
    inspect: impl FnOnce(&Statx) -> io::Result<T>,
) -> io::Result<T> {
    // Avoid forcing remote metadata refreshes; remote results may reflect cached state.
    let flags = AtFlags::SYMLINK_NOFOLLOW | AtFlags::STATX_DONT_SYNC;
    let stat = retry(|| fs::statx(parent_fd, name, flags, mask))?;
    inspect(&stat)
}

fn mount_id_for_fd(fd: BorrowedFd<'_>) -> io::Result<u64> {
    let stat = retry(|| fs::statx(fd, c"", AtFlags::EMPTY_PATH, StatxFlags::MNT_ID))?;
    if reports(&stat, StatxFlags::MNT_ID) {
        Ok(stat.stx_mnt_id)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kernel did not report the directory mount ID",
        ))
    }
}

pub(super) fn is_ext4(fd: BorrowedFd<'_>) -> bool {
    retry(|| fs::fstatfs(fd)).is_ok_and(|filesystem| filesystem.f_type as u64 == EXT4_SUPER_MAGIC)
}

/// Lists a directory, applying exclusions before sizing files in place. With `DIRECTORIES_ONLY`
/// it keeps only entries that may be directories.
fn read_directory<const APPARENT: bool, const DIRECTORIES_ONLY: bool>(
    fd: BorrowedFd<'_>,
    position: SummaryScanPosition<'_>,
    skipped_entries: &AtomicUsize,
) -> io::Result<DirectoryEntries> {
    DirectoryBuffer::with(|buffer| {
        let mut collected = DirectoryEntries {
            names: DirectoryNames::new(),
            entries: PooledDirectoryEntries::take(),
            disk_size: 0,
        };
        let DirectoryEntries { names, entries, disk_size } = &mut collected;
        let mut files = FileBatch::default();
        let mut sized_in_place = 0usize;
        let mut directory = RawDir::new(fd, buffer);

        while let Some(record) = directory.next() {
            let record = match record {
                Ok(record) => record,
                Err(Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
            };
            // Ext4 HTree reserves these cookies for the last entry of an exhausted directory.
            let last_entry = position.ext4_eof_cookie
                && matches!(record.next_entry_cookie(), EXT4_HTREE_EOF_32BIT | EXT4_HTREE_EOF_64BIT);
            let name = record.file_name();
            let name_bytes = name.to_bytes();
            let kind = match record.file_type() {
                FileType::Directory => EntryKind::Directory,
                FileType::Unknown => EntryKind::Unknown,
                _ => EntryKind::Other,
            };

            let excluded = position.excludes_child(name_bytes);
            if name_bytes != b"." && name_bytes != b".." && !excluded {
                if !DIRECTORIES_ONLY && matches!(kind, EntryKind::Other) {
                    if sized_in_place < SERIAL_FILE_LIMIT {
                        // Narrow directories size files without retaining their names.
                        sized_in_place += 1;
                        match with_stat_at(fd, name, entry_size_mask::<APPARENT>(), |stat| {
                            file_size::<APPARENT>(stat)
                        }) {
                            Ok(size) => add_to_total(disk_size, size)?,
                            Err(_) => {
                                skipped_entries.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        files.push(name_bytes);
                        if files.entries.len() >= PARALLEL_FILE_BATCH {
                            add_to_total(disk_size, files.size::<APPARENT>(fd, skipped_entries)?)?;
                        }
                    }
                } else if !DIRECTORIES_ONLY || !matches!(kind, EntryKind::Other) {
                    let length = u16::try_from(name_bytes.len()).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "directory entry name exceeds the supported length",
                        )
                    })?;
                    let start = names.append_name(name_bytes, NAME_RESERVE_HINT);
                    entries.push(
                        DirectoryEntry {
                            name_offset: start,
                            name_length: length,
                            kind,
                        },
                        ENTRY_RESERVE_HINT,
                    );
                }
            }
            if last_entry {
                break;
            }
        }
        if !files.entries.is_empty() {
            add_to_total(disk_size, files.size::<APPARENT>(fd, skipped_entries)?)?;
        }
        Ok(collected)
    })
}

fn add_to_total(total: &mut u64, size: u64) -> io::Result<()> {
    *total = total.checked_add(size).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "directory total exceeds the supported byte-count range",
        )
    })?;
    Ok(())
}

/// Names of files in a wide directory, gathered so their sizes can be read in parallel.
#[derive(Default)]
struct FileBatch {
    names: Vec<u8>,
    entries: Vec<DirectoryEntry>,
}

impl FileBatch {
    fn push(&mut self, name: &[u8]) {
        self.entries.push(DirectoryEntry {
            name_offset: self.names.len(),
            name_length: name.len() as u16,
            kind: EntryKind::Other,
        });
        self.names.extend_from_slice(name);
        self.names.push(0);
    }

    /// Sizes and forgets the gathered files.
    fn size<const APPARENT: bool>(
        &mut self,
        fd: BorrowedFd<'_>,
        skipped_entries: &AtomicUsize,
    ) -> io::Result<u64> {
        let total = self
            .entries
            .par_iter()
            .map(|entry| {
                match with_stat_at(fd, entry.as_c_str(&self.names), entry_size_mask::<APPARENT>(), |stat| {
                    file_size::<APPARENT>(stat)
                }) {
                    Ok(size) => Ok(size),
                    Err(_) => {
                        skipped_entries.fetch_add(1, Ordering::Relaxed);
                        Ok(0)
                    }
                }
            })
            .try_reduce(
                || 0u64,
                |left, right| {
                    left.checked_add(right).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "directory total exceeds the supported byte-count range",
                        )
                    })
                },
            );
        self.entries.clear();
        self.names.clear();
        total
    }
}

#[cfg(test)]
mod tests {
    use super::{
        scan_entry_size, DirectoryEntry, DirectoryNames, EntryKind, DIRECTORY_ENTRY_POOL,
        MAX_CACHED_DIRECTORY_ENTRY_CAPACITY,
    };
    use crate::fsutil;
    use std::fs::{self, File};
    use std::io::{self, Write};
    use std::mem::MaybeUninit;
    use std::os::fd::AsFd;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEMP_DIRECTORY_ID: AtomicUsize = AtomicUsize::new(0);

    struct TemporaryDirectory(PathBuf);

    impl TemporaryDirectory {
        fn new() -> io::Result<Self> {
            loop {
                let id = TEMP_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
                let path = std::env::temp_dir().join(format!(
                    "fdu-test-{}-{id}",
                    std::process::id()
                ));
                match fs::create_dir(&path) {
                    Ok(()) => return Ok(Self(path)),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            }
        }
    }

    impl Drop for TemporaryDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn allocated_size(path: &std::path::Path) -> io::Result<u64> {
        Ok(fs::symlink_metadata(path)?.blocks() * 512)
    }

    #[test]
    fn directory_names_stay_inline_until_the_fixed_capacity_is_exceeded() {
        let mut names = DirectoryNames::new();
        names.append_name(b"first", 32);
        names.append_name(b"123456789", 32);
        assert_eq!(names.as_slice(), b"first\x00123456789\0");
        assert!(matches!(&names, DirectoryNames::Inline(_)));

        names.append_name(b"second", 32);
        assert_eq!(names.as_slice(), b"first\x00123456789\0second\0");
        assert!(matches!(&names, DirectoryNames::Heap(_)));
    }

    #[test]
    fn reuses_small_entry_vectors_and_drops_oversized_ones() {
        let mut entries = super::PooledDirectoryEntries::take();
        entries.reserve(64);
        let cached_capacity = entries.capacity();
        assert!(cached_capacity <= MAX_CACHED_DIRECTORY_ENTRY_CAPACITY);
        drop(entries);

        let mut entries = super::PooledDirectoryEntries::take();
        entries.reserve(64);
        assert_eq!(entries.capacity(), cached_capacity);
        drop(entries);

        let mut oversized = super::PooledDirectoryEntries::take();
        oversized.reserve(MAX_CACHED_DIRECTORY_ENTRY_CAPACITY + 1);
        assert!(oversized.capacity() > MAX_CACHED_DIRECTORY_ENTRY_CAPACITY);
        drop(oversized);

        let replacement = super::PooledDirectoryEntries::take();
        assert!(replacement.capacity() <= MAX_CACHED_DIRECTORY_ENTRY_CAPACITY);
        drop(replacement);

        let mut buffers = (0..=super::MAX_CACHED_DIRECTORY_ENTRY_VECTORS)
            .map(|_| super::PooledDirectoryEntries::take())
            .collect::<Vec<_>>();
        for buffer in &mut buffers {
            buffer.reserve(64);
        }
        drop(buffers);
        let cached_count = DIRECTORY_ENTRY_POOL.with(|pool| pool.borrow().len());
        assert_eq!(cached_count, super::MAX_CACHED_DIRECTORY_ENTRY_VECTORS);
    }

    #[test]
    fn top_level_mount_boundaries_preserve_their_exclusion() -> io::Result<()> {
        let root = fsutil::open_directory(std::path::Path::new("/"))?;
        let proc = fsutil::open_directory(std::path::Path::new("/proc"))?;
        assert_ne!(
            super::mount_id_for_fd(root.as_fd())?,
            super::mount_id_for_fd(proc.as_fd())?,
        );
        let exclusions = crate::ScanExclusions::default();
        let position = super::SummaryScanPosition::root(&exclusions, super::is_ext4(root.as_fd()));
        for apparent in [false, true] {
            for kind in [EntryKind::Directory, EntryKind::Unknown] {
                let skipped_entries = AtomicUsize::new(0);
                let mount_boundaries = AtomicUsize::new(0);
                let entry = DirectoryEntry {
                    name_offset: 0,
                    name_length: 4,
                    kind,
                };
                let scan = if apparent {
                    super::scan_top_level_entry::<true>
                } else {
                    super::scan_top_level_entry::<false>
                };
                let directory = scan(
                    root.as_fd(),
                    b"proc\0",
                    entry,
                    position,
                    &skipped_entries,
                    &mount_boundaries,
                )?.unwrap();
                assert_eq!(directory.name, "proc");
                assert_eq!(directory.disk_size, 0);
                assert_eq!(
                    directory.exclusion,
                    Some(crate::SummaryExclusion::MountBoundary),
                );
                assert_eq!(skipped_entries.load(Ordering::Relaxed), 0);
                assert_eq!(mount_boundaries.load(Ordering::Relaxed), 1);
            }
        }
        Ok(())
    }

    #[test]
    fn scans_unknown_file_and_directory_types() -> io::Result<()> {
        let temporary = TemporaryDirectory::new()?;
        fs::create_dir(temporary.0.join("child"))?;
        let mut nested = File::create(temporary.0.join("child").join("nested"))?;
        nested.write_all(b"abc")?;
        let mut file = File::create(temporary.0.join("file"))?;
        file.write_all(b"xy")?;
        let root = fsutil::open_directory(&temporary.0)?;
        let names = b"file\0child\0";
        let skipped_entries = AtomicUsize::new(0);
        let mount_boundaries = AtomicUsize::new(0);
        let exclusions = crate::ScanExclusions::default();
        let position = super::SummaryScanPosition::root(&exclusions, false);

        let file_size = scan_entry_size::<true>(
            root.as_fd(),
            names,
            DirectoryEntry {
                name_offset: 0,
                name_length: 4,
                kind: EntryKind::Unknown,
            },
            position,
            &skipped_entries,
            &mount_boundaries,
        )?;
        assert_eq!(file_size, 2);

        let child_size = scan_entry_size::<true>(
            root.as_fd(),
            names,
            DirectoryEntry {
                name_offset: 5,
                name_length: 5,
                kind: EntryKind::Unknown,
            },
            position,
            &skipped_entries,
            &mount_boundaries,
        )?;
        assert_eq!(child_size, 3);
        assert_eq!(skipped_entries.load(Ordering::Relaxed), 0);
        assert_eq!(mount_boundaries.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[test]
    fn directory_buffer_leases_preserve_outer_bytes_during_nested_enumeration() {
        super::DirectoryBuffer::with(|outer| {
            outer[0] = MaybeUninit::new(41);
            let outer_pointer = outer.as_ptr();
            super::DirectoryBuffer::with(|nested| {
                assert_ne!(nested.as_ptr(), outer_pointer);
                nested[0] = MaybeUninit::new(73);
                assert_eq!(unsafe { outer[0].assume_init() }, 41);
            });
            assert_eq!(unsafe { outer[0].assume_init() }, 41);
        });
        super::DirectoryBuffer::with(|reused| {
            assert_eq!(unsafe { reused[0].assume_init() }, 73);
            assert_eq!(reused.len(), super::DIRECTORY_BUFFER_BYTES);
        });
    }

    #[test]
    fn keeps_two_child_directories_inline_during_enumeration() -> io::Result<()> {
        let temporary = TemporaryDirectory::new()?;
        fs::create_dir(temporary.0.join("first"))?;
        fs::create_dir(temporary.0.join("second"))?;
        let directory = File::open(&temporary.0)?;
        let skipped_entries = AtomicUsize::new(0);
        let exclusions = crate::ScanExclusions::default();
        let result = super::read_directory::<true, false>(
            directory.as_fd(),
            super::SummaryScanPosition::root(&exclusions, false),
            &skipped_entries,
        )?;
        assert_eq!(result.entries.len(), 2);
        assert!(matches!(result.entries.entries, super::DirectoryEntryStorage::Inline { .. }));
        assert_eq!(result.disk_size, 0);
        assert_eq!(skipped_entries.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[test]
    fn sizes_wide_file_batches_without_retaining_file_records() -> io::Result<()> {
        let temporary = TemporaryDirectory::new()?;
        let mut expected_allocated = 0;
        for index in 0..(super::SERIAL_FILE_LIMIT + 160) {
            let path = temporary.0.join(format!("file-{index:04}"));
            fs::write(&path, b"batch")?;
            expected_allocated += allocated_size(&path)?;
        }
        let directory = File::open(&temporary.0)?;
        let skipped_entries = AtomicUsize::new(0);
        let exclusions = crate::ScanExclusions::default();
        let result = super::read_directory::<false, false>(
            directory.as_fd(),
            super::SummaryScanPosition::root(&exclusions, false),
            &skipped_entries,
        )?;
        assert_eq!(result.disk_size, expected_allocated);
        assert!(result.entries.is_empty());
        assert!(result.names.as_slice().is_empty());
        assert_eq!(skipped_entries.load(Ordering::Relaxed), 0);
        Ok(())
    }

}
