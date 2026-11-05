use std::borrow::Cow;
use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

pub use fdu_core::{DirectoryToken, EntryBatch, FileIdentity, ModifiedTime, ScanEvent};
pub use exclusions::ScanExclusions;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("fdu supports Linux and macOS");

mod fsutil;
mod exclusions;
mod indexed;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

/// Selects the path and size representation used by the summary scanner.
pub struct ScanOptions {
    pub path: PathBuf,
    pub apparent: bool,
}

/// One immediate child directory and the non-directory bytes below it.
pub struct TopLevelDirectory {
    pub name: std::ffi::OsString,
    pub disk_size: u64,
    pub exclusion: Option<SummaryExclusion>,
}

/// Why a summary row has no total.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SummaryExclusion {
    MountBoundary,
    UnsupportedAlias,
    /// The directory did not open in time, typically because macOS is waiting for the user to
    /// answer a privacy prompt for it.
    OpenTimedOut,
}

/// Aggregate summary results and counts of omitted work.
pub struct ScanReport {
    pub directories: Vec<TopLevelDirectory>,
    pub skipped_entries: usize,
    pub mount_boundaries: usize,
    pub unsupported_aliases: usize,
    /// Root-relative directories skipped because they did not open in time, top-level ones
    /// included. Only the macOS scanner skips directories this way.
    pub timed_out_directories: Vec<PathBuf>,
}

#[derive(Clone)]
pub struct RootAnchor {
    fd: Arc<OwnedFd>,
    pub name: Vec<u8>,
    pub identity: FileIdentity,
    modified_time: ModifiedTime,
    pub link_count: u64,
}

impl RootAnchor {
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    pub fn try_clone_fd(&self) -> io::Result<OwnedFd> {
        self.fd.try_clone()
    }

    pub fn modified_time(&self) -> ModifiedTime {
        self.modified_time
    }
}

#[derive(Clone)]
pub struct ScanQueueMetrics {
    capacity: usize,
    enabled: bool,
    outstanding: Arc<std::sync::atomic::AtomicUsize>,
    high_water: Arc<std::sync::atomic::AtomicUsize>,
}

impl Default for ScanQueueMetrics {
    fn default() -> Self {
        Self::new(0, false)
    }
}

impl ScanQueueMetrics {
    pub fn new(capacity: usize, enabled: bool) -> Self {
        Self {
            capacity,
            enabled,
            outstanding: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            high_water: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub fn event_queue_high_water(&self) -> usize {
        self.high_water.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn send(&self, sender: &SyncSender<ScanEvent>, event: ScanEvent) -> Result<(), ScanEvent> {
        if self.enabled {
            let outstanding = self.outstanding.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            self.high_water
                .fetch_max(outstanding.min(self.capacity), std::sync::atomic::Ordering::Relaxed);
        }
        match sender.send(event) {
            Ok(()) => Ok(()),
            Err(error) => {
                if self.enabled {
                    self.decrement_outstanding();
                }
                Err(error.0)
            }
        }
    }

    /// Records that the indexed-scan consumer has removed an event from the bounded queue.
    pub fn event_received(&self) {
        if self.enabled {
            self.decrement_outstanding();
        }
    }

    fn decrement_outstanding(&self) {
        let mut current = self.outstanding.load(std::sync::atomic::Ordering::Relaxed);
        while current != 0 {
            match self.outstanding.compare_exchange_weak(
                current,
                current - 1,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(updated) => current = updated,
            }
        }
    }
}

pub fn open_root(path: &std::path::Path) -> io::Result<RootAnchor> {
    anchor_root(path, fsutil::open_directory(path)?)
}

/// Like [`open_root`], but fails when the final path component is a symlink
/// instead of following it. Trailing slashes are removed before opening so
/// they cannot make the kernel resolve a final symlink as a directory. Earlier
/// components still resolve through symlinks.
pub fn open_root_no_follow(path: &std::path::Path) -> io::Result<RootAnchor> {
    let open_path = without_trailing_slashes(path);
    anchor_root(path, fsutil::open_directory_no_follow(open_path.as_ref())?)
}

/// The kernel resolves a trailing slash before applying `O_NOFOLLOW`, so remove
/// only trailing separators before opening and preserve the caller's root name.
fn without_trailing_slashes(path: &Path) -> Cow<'_, Path> {
    let bytes = path.as_os_str().as_bytes();
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1] == b'/' {
        end -= 1;
    }
    if end == bytes.len() || end == 0 {
        Cow::Borrowed(path)
    } else {
        Cow::Owned(Path::new(std::ffi::OsStr::from_bytes(&bytes[..end])).to_path_buf())
    }
}

fn anchor_root(path: &std::path::Path, fd: OwnedFd) -> io::Result<RootAnchor> {
    use std::os::unix::ffi::OsStrExt;
    let (identity, link_count, modified_time) = fsutil::identity_link_count_and_modified_time(fd.as_fd())?;
    Ok(RootAnchor {
        fd: Arc::new(fd),
        name: path.as_os_str().as_bytes().to_vec(),
        identity,
        modified_time,
        link_count,
    })
}

pub fn scan(options: &ScanOptions) -> io::Result<ScanReport> {
    scan_with_exclusions(options, &ScanExclusions::default())
}

/// Scans the selected root while omitting exact relative paths and their subtrees.
///
/// On macOS a directory whose open does not return in time is skipped and reported, and the
/// thread blocked in that open is left behind; it exits once the open returns.
pub fn scan_with_exclusions(options: &ScanOptions, exclusions: &ScanExclusions) -> io::Result<ScanReport> {
    #[cfg(target_os = "linux")]
    {
        linux::scan(&options.path, options.apparent, exclusions)
    }
    #[cfg(target_os = "macos")]
    {
        macos::scan(&options.path, options.apparent, exclusions)
    }
}

/// Starts an indexed scan on a worker thread. The supplied sender should be bounded.
pub fn start_indexed_scan(
    root: RootAnchor,
    sender: SyncSender<ScanEvent>,
    returned_batches: Receiver<EntryBatch>,
    cancelled: Arc<AtomicBool>,
) -> JoinHandle<()> {
    start_indexed_scan_with_exclusions_and_metrics(
        root,
        ScanExclusions::default(),
        sender,
        returned_batches,
        cancelled,
        ScanQueueMetrics::default(),
    )
}

/// Starts an indexed scan that omits exact relative paths and their subtrees.
pub fn start_indexed_scan_with_exclusions(
    root: RootAnchor,
    exclusions: ScanExclusions,
    sender: SyncSender<ScanEvent>,
    returned_batches: Receiver<EntryBatch>,
    cancelled: Arc<AtomicBool>,
) -> JoinHandle<()> {
    start_indexed_scan_with_exclusions_and_metrics(
        root,
        exclusions,
        sender,
        returned_batches,
        cancelled,
        ScanQueueMetrics::default(),
    )
}

/// Starts an indexed scan with optional event-queue instrumentation.
pub fn start_indexed_scan_with_metrics(
    root: RootAnchor,
    sender: SyncSender<ScanEvent>,
    returned_batches: Receiver<EntryBatch>,
    cancelled: Arc<AtomicBool>,
    metrics: ScanQueueMetrics,
) -> JoinHandle<()> {
    start_indexed_scan_with_exclusions_and_metrics(
        root,
        ScanExclusions::default(),
        sender,
        returned_batches,
        cancelled,
        metrics,
    )
}

/// Starts an indexed scan with exclusions and optional event-queue instrumentation.
pub fn start_indexed_scan_with_exclusions_and_metrics(
    root: RootAnchor,
    exclusions: ScanExclusions,
    sender: SyncSender<ScanEvent>,
    returned_batches: Receiver<EntryBatch>,
    cancelled: Arc<AtomicBool>,
    metrics: ScanQueueMetrics,
) -> JoinHandle<()> {
    thread::spawn(move || {
        indexed::scan_indexed(&root, &exclusions, sender, returned_batches, cancelled, metrics)
    })
}

#[cfg(test)]
mod tests {
    use super::ScanQueueMetrics;
    use fdu_core::ScanEvent;
    use std::sync::mpsc;

    #[test]
    fn event_queue_instrumentation_reports_the_bounded_high_water_mark() {
        let metrics = ScanQueueMetrics::new(2, true);
        let (sender, receiver) = mpsc::sync_channel(2);
        metrics.send(&sender, ScanEvent::Cancelled).unwrap();
        metrics.send(&sender, ScanEvent::Finished).unwrap();
        assert_eq!(metrics.event_queue_high_water(), 2);
        receiver.recv().unwrap();
        metrics.event_received();
        receiver.recv().unwrap();
        metrics.event_received();
        assert_eq!(metrics.event_queue_high_water(), 2);
    }
}
