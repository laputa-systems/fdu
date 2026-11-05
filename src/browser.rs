use fdu_core::{
    DirectoryToken, EntryType, ExclusionReason, NodeId, NodeState, NodeStatus, ScanEvent, Tree,
};
use fdu_delete::{create_plan, DeleteEvent, DeleteOutcome, DeletionPlan, OutcomeKind};
use fdu_scan::{
    open_root, start_indexed_scan_with_exclusions_and_metrics, RootAnchor, ScanExclusions, ScanQueueMetrics,
};
use fdu_tui::{
    self, Cursor, Intent, LsColors, Modal, Operation, Pane, Phase, SizeMode, Split, SortMode,
    TerminalSession, TerminalSize, View,
};
use hashbrown::{HashMap, HashSet};
use signal_hook::consts::signal::{SIGHUP, SIGTERM};
use signal_hook::iterator::Signals;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const SCAN_EVENT_CHANNEL_CAPACITY: usize = 512;
const SCAN_BATCH_CAPACITY: usize = 1024;
const SCAN_BATCH_POOL_CAPACITY: usize = 8;
const DELETE_CHANNEL_CAPACITY: usize = 8;
/// A tick applies scan events until the channel is empty or this much time has passed, so a
/// scan that produces many small events is not paced by the tick rate while input and drawing
/// still get their turn.
const SCAN_DRAIN_BUDGET: Duration = Duration::from_millis(4);
const MAX_DELETE_EVENTS_PER_TICK: usize = 32;
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(20);
/// While scanning, an idle tick waits this long for the next scan event rather than for input,
/// so the scanner is never left blocked on a full channel while the interface sleeps.
const SCAN_EVENT_WAIT: Duration = Duration::from_millis(1);
const SCAN_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const SCAN_RESORT_COST_FACTOR: u32 = 10;

enum AppPhase {
    Scanning(ScanRun),
    Ready,
    Deleting(DeleteRun),
}

impl AppPhase {
    fn view_phase(&self) -> Phase {
        match self {
            Self::Scanning(_) => Phase::Scanning,
            Self::Ready => Phase::Ready,
            Self::Deleting(_) => Phase::Deleting,
        }
    }
}

struct ScanRun {
    receiver: Option<Receiver<ScanEvent>>,
    returned_batches: Option<SyncSender<fdu_core::EntryBatch>>,
    cancelled: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    queue_metrics: ScanQueueMetrics,
}

impl ScanRun {
    fn start(root: RootAnchor, exclusions: ScanExclusions, queue_metrics: ScanQueueMetrics) -> Self {
        let (sender, receiver) = mpsc::sync_channel(SCAN_EVENT_CHANNEL_CAPACITY);
        let (batch_sender, batch_receiver) = mpsc::sync_channel(SCAN_BATCH_POOL_CAPACITY);
        for _ in 0..SCAN_BATCH_POOL_CAPACITY {
            let batch = fdu_core::EntryBatch::with_capacity(SCAN_BATCH_CAPACITY, SCAN_BATCH_CAPACITY * 24);
            if batch_sender.send(batch).is_err() {
                break;
            }
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker = start_indexed_scan_with_exclusions_and_metrics(
            root,
            exclusions,
            sender,
            batch_receiver,
            Arc::clone(&cancelled),
            queue_metrics.clone(),
        );
        Self {
            receiver: Some(receiver),
            returned_batches: Some(batch_sender),
            cancelled,
            worker: Some(worker),
            queue_metrics,
        }
    }

    fn return_batch(&self, mut batch: fdu_core::EntryBatch) {
        batch.clear();
        if let Some(sender) = &self.returned_batches {
            let _ = sender.try_send(batch);
        }
    }

    fn finish(&mut self) -> Result<(), String> {
        let worker_result = self.worker.take().map(JoinHandle::join);
        self.receiver.take();
        self.returned_batches.take();
        match worker_result {
            Some(Ok(())) => Ok(()),
            Some(Err(_)) => Err("scanner worker terminated unexpectedly".to_owned()),
            None => Ok(()),
        }
    }
}

/// An abandoned scan is cancelled but not joined: a worker can be stuck in an `open` that the OS
/// holds until the user answers a privacy prompt, and quitting must not wait for that.
impl Drop for ScanRun {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

struct DeleteRun {
    receiver: Option<Receiver<DeleteEvent>>,
    cancelled: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    outcomes: Vec<DeleteOutcome>,
    failure: Option<String>,
    completed: usize,
    total: usize,
    skipped: usize,
    started: Instant,
    /// The scan that was running when deletion started. Its events wait until deletion ends,
    /// because the plan shares the index.
    paused_scan: Option<ScanRun>,
}

impl DeleteRun {
    fn start(root: std::os::fd::OwnedFd, plan: DeletionPlan, paused_scan: Option<ScanRun>) -> Self {
        let total = plan.operation_count();
        let skipped = plan.skipped.len();
        let (sender, receiver) = mpsc::sync_channel(DELETE_CHANNEL_CAPACITY);
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        let worker = thread::spawn(move || {
            fdu_delete::execute(root, plan, worker_cancelled, sender);
        });
        Self {
            receiver: Some(receiver),
            cancelled,
            worker: Some(worker),
            outcomes: Vec::with_capacity(total.min(256)),
            failure: None,
            completed: 0,
            total,
            skipped,
            started: Instant::now(),
            paused_scan,
        }
    }

    fn finish(&mut self) -> Result<(Vec<DeleteOutcome>, Option<String>), String> {
        let worker_result = self.worker.take().map(JoinHandle::join);
        self.receiver.take();
        match worker_result {
            Some(Ok(())) => Ok((std::mem::take(&mut self.outcomes), self.failure.take())),
            Some(Err(_)) => Err("deletion worker terminated unexpectedly".to_owned()),
            None => Ok((std::mem::take(&mut self.outcomes), self.failure.take())),
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

impl Drop for DeleteRun {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.receiver.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Debug)]
struct BrowserShutdownError {
    operation: Box<dyn std::error::Error>,
    restoration: io::Error,
}

impl std::fmt::Display for BrowserShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}; terminal restoration also failed: {}", self.operation, self.restoration)
    }
}

impl std::error::Error for BrowserShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.operation.as_ref())
    }
}

fn combine_shutdown_result<T>(
    operation: Result<T, Box<dyn std::error::Error>>,
    restoration: io::Result<()>,
) -> Result<T, Box<dyn std::error::Error>> {
    match (operation, restoration) {
        (Err(operation), Ok(())) => Err(operation),
        (Err(operation), Err(restoration)) => Err(BrowserShutdownError { operation, restoration }.into()),
        (Ok(_), Err(restoration)) => Err(restoration.into()),
        (Ok(value), Ok(())) => Ok(value),
    }
}

enum DialogState {
    None,
    Help,
    Filter(String),
}

/// The deletion basket: marked roots in the order they were marked, with
/// constant-time membership checks for row rendering.
#[derive(Default)]
struct MarkSet {
    members: HashSet<NodeId>,
    order: Vec<NodeId>,
}

impl MarkSet {
    fn contains(&self, id: &NodeId) -> bool {
        self.members.contains(id)
    }

    fn insert(&mut self, id: NodeId) -> bool {
        let added = self.members.insert(id);
        if added {
            self.order.push(id);
        }
        added
    }

    fn remove(&mut self, id: &NodeId) -> bool {
        let removed = self.members.remove(id);
        if removed {
            self.order.retain(|candidate| candidate != id);
        }
        removed
    }

    fn remove_at(&mut self, index: usize) -> Option<NodeId> {
        if index >= self.order.len() {
            return None;
        }
        let id = self.order.remove(index);
        self.members.remove(&id);
        Some(id)
    }

    fn retain(&mut self, keep: impl Fn(&NodeId) -> bool) {
        self.members.retain(|id| keep(id));
        self.order.retain(|id| keep(id));
    }

    fn clear(&mut self) {
        self.members.clear();
        self.order.clear();
    }

    fn len(&self) -> usize {
        self.order.len()
    }

    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    fn get(&self, index: usize) -> Option<NodeId> {
        self.order.get(index).copied()
    }
}

enum Action {
    None,
    Quit,
    Refresh,
    Delete(DeletionPlan),
    CancelDeletion,
}

struct BrowserModel {
    tree: Arc<Tree>,
    token_nodes: Vec<Option<NodeId>>,
    current_directory: NodeId,
    listing: Vec<NodeId>,
    visible: Vec<NodeId>,
    cursor: Cursor,
    cursor_index: Option<usize>,
    /// Remembers selections by entry identity so directory visits survive sort and filter changes.
    directory_cursors: HashMap<NodeId, Cursor>,
    cursor_moved_during_scan: bool,
    marks: MarkSet,
    marked_cursor: usize,
    focus: Pane,
    terminal: TerminalSize,
    split: Split,
    filter: String,
    size_mode: SizeMode,
    sort_mode: SortMode,
    range_mode: bool,
    range_anchor: Option<NodeId>,
    dialog: DialogState,
    /// A frozen plan awaiting Enter. While set, only Enter and Esc act.
    pending_delete: Option<DeletionPlan>,
    message: Option<String>,
    status_by_node: HashMap<NodeId, String>,
    indexed_entries: usize,
    incomplete_entries: usize,
    exclusions: usize,
    scan_errors: usize,
    actual_deletions: usize,
    scanning: bool,
    /// The running scan has sent every entry directly in the root. Deletion creates its
    /// quarantine there, so it waits for this to keep the scan from indexing the quarantine.
    root_listed: bool,
    read_only: bool,
    ls_colors: LsColors,
}

struct BrowserProfile {
    enabled: bool,
    started: Instant,
    first_render: Option<Duration>,
    first_usable_listing: Option<Duration>,
    first_entries: Option<Duration>,
    initial_scan_settled: Option<Duration>,
    /// Time the deletion worker ran, and time spent reconciling its outcomes.
    delete_worker: Option<Duration>,
    delete_apply: Option<Duration>,
    pending_input: Option<Instant>,
    input_render_micros: Vec<u64>,
    queue_metrics: ScanQueueMetrics,
    /// Main-thread time by activity, to show what paces the interface during a scan.
    busy: BusyTime,
}

#[derive(Default)]
struct BusyTime {
    entry_batches: Duration,
    entry_batch_count: u64,
    max_entry_batch_micros: u64,
    max_scan_drain_micros: u64,
    other_events: Duration,
    draws: Duration,
    draw_count: u64,
    waiting_for_input: Duration,
}

impl BrowserProfile {
    fn new(enabled: bool, queue_metrics: ScanQueueMetrics) -> Self {
        Self {
            enabled,
            started: Instant::now(),
            first_render: None,
            first_usable_listing: None,
            first_entries: None,
            initial_scan_settled: None,
            delete_worker: None,
            delete_apply: None,
            pending_input: None,
            input_render_micros: Vec::new(),
            queue_metrics,
            busy: BusyTime::default(),
        }
    }

    /// Starts timing an activity when profiling is on.
    fn timer(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    fn reset_scan_queue(&mut self, queue_metrics: ScanQueueMetrics) {
        self.queue_metrics = queue_metrics;
    }

    fn note_input(&mut self) {
        if self.enabled {
            self.pending_input = Some(Instant::now());
        }
    }

    fn note_render(&mut self, model: &BrowserModel, phase: Phase) {
        if !self.enabled {
            return;
        }
        let elapsed = self.started.elapsed();
        self.first_render.get_or_insert(elapsed);
        let root_is_visible = model.current_directory == model.tree.root() && !model.visible.is_empty();
        if self.first_usable_listing.is_none() && (root_is_visible || phase == Phase::Ready) {
            self.first_usable_listing = Some(elapsed);
        }
        if let Some(input) = self.pending_input.take() {
            self.input_render_micros
                .push(input.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
        }
    }

    fn note_scan_settled(&mut self) {
        self.initial_scan_settled.get_or_insert_with(|| self.started.elapsed());
    }

    fn report(&mut self, model: &BrowserModel) {
        if !self.enabled {
            return;
        }
        self.input_render_micros.sort_unstable();
        let median = self
            .input_render_micros
            .get(self.input_render_micros.len() / 2)
            .copied();
        let maximum = self.input_render_micros.last().copied();
        let entries = model.indexed_entries;
        let arena_bytes = model.tree.retained_arena_bytes();
        eprintln!(
            "fdu-profile elapsed_ms={} initial_scan_settled_ms={:.3} first_render_ms={:.3} first_usable_listing_ms={:.3} first_entries_ms={:.3} entries={} tree_arena_bytes={} tree_arena_bytes_per_entry={:.2} scan_event_queue_high_water={} input_render_samples={} input_render_p50_us={} input_render_max_us={} delete_worker_ms={:.3} delete_apply_ms={:.3} busy_entry_batches_ms={} entry_batches={} max_entry_batch_us={} max_scan_drain_us={} busy_other_events_ms={} busy_draw_ms={} draws={} waiting_for_input_ms={}",
            self.started.elapsed().as_millis(),
            duration_ms(self.initial_scan_settled),
            duration_ms(self.first_render),
            duration_ms(self.first_usable_listing),
            duration_ms(self.first_entries),
            entries,
            arena_bytes,
            if entries == 0 { 0.0 } else { arena_bytes as f64 / entries as f64 },
            self.queue_metrics.event_queue_high_water(),
            self.input_render_micros.len(),
            median.unwrap_or(0),
            maximum.unwrap_or(0),
            duration_ms(self.delete_worker),
            duration_ms(self.delete_apply),
            self.busy.entry_batches.as_millis(),
            self.busy.entry_batch_count,
            self.busy.max_entry_batch_micros,
            self.busy.max_scan_drain_micros,
            self.busy.other_events.as_millis(),
            self.busy.draws.as_millis(),
            self.busy.draw_count,
            self.busy.waiting_for_input.as_millis(),
        );
    }
}

/// Fractional milliseconds: whole-millisecond truncation would make a listing drawn just
/// before the scan settles indistinguishable from one drawn at the same time.
fn duration_ms(duration: Option<Duration>) -> f64 {
    duration.map_or(0.0, |duration| duration.as_secs_f64() * 1000.0)
}

impl BrowserModel {
    fn new(root: &RootAnchor, read_only: bool, apparent: bool, terminal: TerminalSize) -> io::Result<Self> {
        let mut tree = Tree::new(&root.name, root.identity, root.link_count).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "root path exceeds index limits")
        })?;
        let current_directory = tree.root();
        assert!(tree.set_modified_time(current_directory, root.modified_time()));
        let mut model = Self {
            tree: Arc::new(tree),
            token_nodes: vec![Some(current_directory)],
            current_directory,
            listing: Vec::new(),
            visible: Vec::new(),
            cursor: Cursor::None,
            cursor_index: None,
            directory_cursors: HashMap::new(),
            cursor_moved_during_scan: false,
            marks: MarkSet::default(),
            marked_cursor: 0,
            focus: Pane::Browser,
            terminal,
            split: Split::Vertical,
            filter: String::new(),
            size_mode: SizeMode::Allocated,
            sort_mode: SortMode::Size,
            range_mode: false,
            range_anchor: None,
            dialog: DialogState::None,
            pending_delete: None,
            message: None,
            status_by_node: HashMap::new(),
            indexed_entries: 0,
            incomplete_entries: 0,
            exclusions: 0,
            scan_errors: 0,
            actual_deletions: 0,
            scanning: true,
            root_listed: false,
            read_only,
            ls_colors: LsColors::from_env(),
        };
        model.size_mode = if apparent { SizeMode::Apparent } else { SizeMode::Allocated };
        model.rebuild_listing(false);
        Ok(model)
    }

    fn reset_for_rescan(&mut self, root: &RootAnchor) -> io::Result<()> {
        let mut tree = Tree::new(&root.name, root.identity, root.link_count).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "root path exceeds index limits")
        })?;
        assert!(tree.set_modified_time(tree.root(), root.modified_time()));
        self.current_directory = tree.root();
        self.tree = Arc::new(tree);
        self.token_nodes.clear();
        self.token_nodes.push(Some(self.current_directory));
        self.listing.clear();
        self.visible.clear();
        self.cursor = Cursor::None;
        self.cursor_index = None;
        self.directory_cursors.clear();
        self.cursor_moved_during_scan = false;
        self.marks.clear();
        self.marked_cursor = 0;
        self.normalize_focus();
        self.filter.clear();
        self.range_mode = false;
        self.range_anchor = None;
        self.dialog = DialogState::None;
        self.pending_delete = None;
        self.root_listed = false;
        self.message = None;
        self.status_by_node.clear();
        self.indexed_entries = 0;
        self.incomplete_entries = 0;
        self.exclusions = 0;
        self.scan_errors = 0;
        self.scanning = true;
        self.rebuild_listing(false);
        Ok(())
    }

    fn apply_scan_event(&mut self, event: ScanEvent) -> bool {
        match event {
            ScanEvent::Started {
                identity,
                link_count: _,
                root_name: _,
            } => {
                let root = self.tree.record(self.tree.root()).expect("root record exists");
                if root.identity != identity {
                    self.message = Some("Opened root identity changed before scanning started.".to_owned());
                    self.set_state(self.tree.root(), NodeStatus::Incomplete);
                    self.mark_incomplete_chain(self.tree.root());
                }
                true
            }
            ScanEvent::Entries(_) => false,
            ScanEvent::RootListed => {
                self.root_listed = true;
                false
            }
            ScanEvent::DirectoryFinished { directory, complete } => {
                self.finish_directory(directory, complete);
                true
            }
            ScanEvent::DirectoriesFinished(directories) => {
                for (directory, complete) in directories {
                    self.finish_directory(directory, complete);
                }
                true
            }
            ScanEvent::DirectoryExcluded { directory, reason } => {
                if let Some(node) = self.node_for_token(directory) {
                    self.set_state(node, NodeStatus::Excluded(reason));
                    if let Some(parent) = self.tree.record(node).and_then(|record| record.parent()) {
                        self.mark_incomplete_chain(parent);
                    }
                }
                true
            }
            ScanEvent::DirectoryFailed { directory, message } => {
                self.scan_errors = self.scan_errors.saturating_add(1);
                if let Some(node) = self.node_for_token(directory) {
                    self.mark_incomplete_chain(node);
                    self.explain_scan_error(node, message);
                }
                true
            }
            ScanEvent::Failed { message } => {
                self.scan_errors = self.scan_errors.saturating_add(1);
                self.message = Some(format!("Scan could not continue: {message}"));
                self.mark_incomplete_chain(self.tree.root());
                true
            }
            ScanEvent::Cancelled => {
                self.cancel_incomplete_nodes();
                self.message = Some("Scan cancelled; visible totals may be incomplete.".to_owned());
                true
            }
            ScanEvent::Finished => true,
        }
    }

    fn finish_directory(&mut self, directory: DirectoryToken, complete: bool) {
        let marked = match Arc::get_mut(&mut self.tree) {
            Some(tree) => fdu_core::finish_scan_directory(tree, &self.token_nodes, directory, complete),
            None => 0,
        };
        self.bump_incomplete(marked);
    }

    fn apply_entry_batch(&mut self, mut batch: fdu_core::EntryBatch) -> fdu_core::EntryBatch {
        let current_directory = self.current_directory;
        match Arc::get_mut(&mut self.tree) {
            None => {
                self.message = Some("Index became shared while scanning was active.".to_owned());
                // Nothing was appended, so nothing is added to any total.
            }
            Some(tree) => {
                let outcome = fdu_core::fold_entry_batch(tree, &mut self.token_nodes, &batch);
                if outcome.unknown_directory {
                    self.message = Some("Scanner sent entries for an unknown directory.".to_owned());
                }
                if outcome.invalid_range {
                    self.message = Some("Scanner sent an invalid filename range.".to_owned());
                }
                if outcome.exhausted {
                    self.message = Some("Index exhausted the supported number of entries.".to_owned());
                }
                if outcome.overflow {
                    self.message = Some("A directory total exceeded the supported byte range.".to_owned());
                }
                self.bump_incomplete(outcome.incomplete_entries + outcome.marked_incomplete);
                self.exclusions = self.exclusions.saturating_add(outcome.excluded_entries);
                self.indexed_entries = self.tree.len().saturating_sub(1);
                let mut new_ids = Vec::new();
                for id in outcome.appended {
                    if self.tree.record(id).and_then(|record| record.parent()) == Some(current_directory) {
                        new_ids.push(id);
                    }
                }
                if !new_ids.is_empty() {
                    self.listing.extend(new_ids.iter().copied());
                    for id in new_ids {
                        if self.filter_matches(id) {
                            self.visible.push(id);
                        }
                    }
                    if matches!(self.cursor, Cursor::None) {
                        self.cursor = self.visible.first().copied().map(Cursor::Entry).unwrap_or(Cursor::None);
                        self.refresh_cursor_index();
                    }
                }
            }
        }
        batch.clear();
        batch
    }

    fn handle_intent(&mut self, intent: Intent, phase: Phase) -> (Action, bool) {
        if matches!(intent, Intent::Resize(_)) {
            return self.dispatch_intent(intent, phase);
        }
        let had_message = self.message.take().is_some();
        let (action, changed) = self.dispatch_intent(intent, phase);
        (action, changed || had_message)
    }

    fn dispatch_intent(&mut self, intent: Intent, phase: Phase) -> (Action, bool) {
        if let Intent::Resize(size) = intent {
            self.terminal = size;
            self.normalize_focus();
            return (Action::None, true);
        }
        if matches!(&self.dialog, DialogState::Help) {
            if matches!(intent, Intent::Cancel | Intent::Help) {
                self.dialog = DialogState::None;
                return (Action::None, true);
            }
            return (Action::None, false);
        }
        if let DialogState::Filter(value) = &mut self.dialog {
            match intent {
                Intent::FilterCharacter(character) => {
                    value.push(character);
                    return (Action::None, true);
                }
                Intent::FilterBackspace => {
                    value.pop();
                    return (Action::None, true);
                }
                Intent::SubmitFilter => {
                    let DialogState::Filter(value) = std::mem::replace(&mut self.dialog, DialogState::None) else {
                        unreachable!()
                    };
                    self.filter = value;
                    self.range_mode = false;
                    self.range_anchor = None;
                    self.rebuild_visible(true);
                    self.message = None;
                    return (Action::None, true);
                }
                Intent::Cancel => {
                    self.dialog = DialogState::None;
                    return (Action::None, true);
                }
                _ => return (Action::None, false),
            }
        }
        if self.pending_delete.is_some() {
            return match intent {
                Intent::Enter => {
                    let plan = self.pending_delete.take().expect("a plan is pending");
                    (Action::Delete(plan), true)
                }
                Intent::Cancel => {
                    self.pending_delete = None;
                    (Action::None, true)
                }
                _ => (Action::None, false),
            };
        }
        // Deletion owns the interface: Esc is the only command that does anything.
        if phase == Phase::Deleting {
            return if matches!(intent, Intent::Cancel) {
                (Action::CancelDeletion, true)
            } else {
                (Action::None, false)
            };
        }
        if matches!(intent, Intent::Help) {
            self.dialog = DialogState::Help;
            return (Action::None, true);
        }
        if matches!(intent, Intent::Quit) {
            if phase == Phase::Scanning {
                self.message = Some("Scan cancelled on exit.".to_owned());
            }
            return (Action::Quit, true);
        }
        match intent {
            Intent::SwitchPane => return (Action::None, self.switch_pane()),
            Intent::ToggleSplit => {
                self.split = self.split.toggled();
                return (Action::None, true);
            }
            Intent::StartFilter if self.focus == Pane::Browser => {
                self.dialog = DialogState::Filter(self.filter.clone());
                self.message = None;
                return (Action::None, true);
            }
            Intent::StartFilter | Intent::ToggleRange | Intent::MarkAll if self.focus == Pane::Marked => {
                self.message = Some("That applies to the list; press Tab.".to_owned());
                return (Action::None, true);
            }
            _ => {}
        }
        if phase == Phase::Scanning {
            match intent {
                Intent::Delete if !self.root_listed => {
                    self.message = Some("Deletion is unavailable until the root directory is listed.".to_owned());
                    return (Action::None, true);
                }
                Intent::Refresh => {
                    self.message = Some("A full rescan is unavailable while scanning.".to_owned());
                    return (Action::None, true);
                }
                _ => {}
            }
        }
        match intent {
            Intent::ToggleSizeMode => {
                self.size_mode = match self.size_mode {
                    SizeMode::Allocated => SizeMode::Apparent,
                    SizeMode::Apparent => SizeMode::Allocated,
                };
                let changed = if phase == Phase::Ready { self.sort_visible_if_needed() } else { true };
                return (Action::None, changed);
            }
            Intent::ToggleSort => {
                let next = match self.sort_mode {
                    SortMode::Size => SortMode::Name,
                    SortMode::Name => SortMode::Date,
                    SortMode::Date => SortMode::Size,
                };
                return (Action::None, self.set_sort(next, phase));
            }
            Intent::ClearMarks => {
                let changed = !self.marks.is_empty();
                self.marks.clear();
                self.marked_cursor = 0;
                self.normalize_focus();
                return (Action::None, changed);
            }
            Intent::Delete => return self.begin_delete(),
            Intent::Refresh if phase == Phase::Ready => return (Action::Refresh, true),
            Intent::Refresh => {
                self.message = Some("A full rescan is only available when the index is ready.".to_owned());
                return (Action::None, true);
            }
            _ => {}
        }
        match self.focus {
            Pane::Browser => self.handle_browser_intent(intent),
            Pane::Marked => self.handle_marked_intent(intent),
        }
    }

    fn handle_browser_intent(&mut self, intent: Intent) -> (Action, bool) {
        let changed = match intent {
            Intent::MoveUp => self.move_cursor(-1),
            Intent::MoveDown => self.move_cursor(1),
            Intent::PageUp => self.move_cursor(-15),
            Intent::PageDown => self.move_cursor(15),
            Intent::Home => self.move_to_edge(false),
            Intent::End => self.move_to_edge(true),
            Intent::Enter => self.enter_cursor(),
            Intent::Parent => self.go_parent(),
            Intent::ToggleMark => self.toggle_mark(),
            Intent::ToggleRange => self.toggle_range(),
            Intent::MarkAll => self.mark_all_visible(),
            _ => false,
        };
        (Action::None, changed)
    }

    fn handle_marked_intent(&mut self, intent: Intent) -> (Action, bool) {
        let changed = match intent {
            Intent::MoveUp => self.move_marked_cursor(-1),
            Intent::MoveDown => self.move_marked_cursor(1),
            Intent::PageUp => self.move_marked_cursor(-15),
            Intent::PageDown => self.move_marked_cursor(15),
            Intent::Home => self.move_marked_cursor(isize::MIN),
            Intent::End => self.move_marked_cursor(isize::MAX),
            Intent::Enter => self.reveal_marked(),
            Intent::ToggleMark => self.unmark_at_cursor(),
            _ => false,
        };
        (Action::None, changed)
    }

    /// The marked-items pane is shown whenever something is marked. A terminal
    /// too narrow for both panes shows only the one that has focus.
    fn side_visible(&self) -> bool {
        !self.marks.is_empty() && (fdu_tui::fits_two_panes(self.split, self.terminal) || self.focus == Pane::Marked)
    }

    /// Focus belongs on the browser whenever nothing is marked.
    fn normalize_focus(&mut self) {
        if self.marks.is_empty() {
            self.focus = Pane::Browser;
        }
    }

    fn switch_pane(&mut self) -> bool {
        if self.marks.is_empty() {
            self.message = Some("Nothing is marked.".to_owned());
        } else {
            self.focus = match self.focus {
                Pane::Browser => Pane::Marked,
                Pane::Marked => Pane::Browser,
            };
        }
        true
    }

    fn move_marked_cursor(&mut self, amount: isize) -> bool {
        let count = self.marks.len();
        if count == 0 {
            return false;
        }
        let next = (self.marked_cursor as isize).saturating_add(amount).clamp(0, count as isize - 1) as usize;
        let changed = next != self.marked_cursor;
        self.marked_cursor = next;
        changed
    }

    fn unmark_at_cursor(&mut self) -> bool {
        if self.marks.remove_at(self.marked_cursor).is_none() {
            return false;
        }
        self.marked_cursor = self.marked_cursor.min(self.marks.len().saturating_sub(1));
        self.normalize_focus();
        true
    }

    /// Shows the highlighted marked entry in the browser, in its own directory.
    fn reveal_marked(&mut self) -> bool {
        let Some(id) = self.marks.get(self.marked_cursor) else {
            return false;
        };
        let Some(parent) = self.tree.record(id).and_then(|record| record.parent()) else {
            return false;
        };
        let filter_hid_it = !self.filter.is_empty();
        self.filter.clear();
        self.remember_current_cursor();
        self.current_directory = parent;
        self.range_mode = false;
        self.range_anchor = None;
        self.message = filter_hid_it.then(|| "Filter cleared to show the marked entry.".to_owned());
        self.rebuild_listing(false);
        if self.visible.contains(&id) {
            self.cursor = Cursor::Entry(id);
            self.refresh_cursor_index();
        }
        self.cursor_moved_during_scan = self.scanning;
        self.focus = Pane::Browser;
        self.normalize_focus();
        true
    }

    /// Freezes a deletion plan for every mark and asks for confirmation. The
    /// marked-items pane is the review, so deletion starts only from there.
    fn begin_delete(&mut self) -> (Action, bool) {
        if self.read_only {
            self.message = Some("This session is read only; deletion is disabled.".to_owned());
            return (Action::None, true);
        }
        if self.focus != Pane::Marked {
            self.message = Some("Press Tab to review marks, then Ctrl-R.".to_owned());
            return (Action::None, true);
        }
        self.range_mode = false;
        self.range_anchor = None;
        if self.marks.is_empty() {
            self.message = Some("Nothing is marked.".to_owned());
            return (Action::None, true);
        }
        match create_plan(Arc::clone(&self.tree), &self.marks.order) {
            Ok(plan) => {
                self.pending_delete = Some(plan);
                self.message = None;
            }
            Err(error) => {
                let more = error.rejected.len().saturating_sub(1);
                let first = error.rejected.first().map(|rejection| match rejection.node {
                    Some(node) => format!("{}: {}", self.display_path(node), rejection.reason),
                    None => rejection.reason.clone(),
                });
                self.message = Some(format!(
                    "Nothing can be deleted: {}{}",
                    first.unwrap_or_default(),
                    if more > 0 { format!(" (+{more} more)") } else { String::new() },
                ));
            }
        }
        (Action::None, true)
    }

    fn view<'a>(&'a self, phase: &'a AppPhase) -> View<'a> {
        let modal = match &self.dialog {
            DialogState::None => Modal::None,
            DialogState::Help => Modal::Help { focus: self.focus },
            DialogState::Filter(value) => Modal::Filter { value },
        };
        let operation = match (&self.pending_delete, phase) {
            (_, AppPhase::Deleting(worker)) => Operation::Deleting {
                completed: worker.completed,
                total: worker.total,
                stopping: worker.cancelled.load(Ordering::Relaxed),
            },
            (Some(plan), _) => Operation::ConfirmDelete {
                roots: plan.roots.len(),
                entries: plan.operation_count(),
                allocated_bytes: plan.allocated_bytes,
                apparent_bytes: plan.apparent_bytes,
                skipped: plan.skipped.len(),
            },
            (None, _) => Operation::Idle,
        };
        let detail_message = match self.cursor {
            Cursor::Entry(id) => self.status_by_node.get(&id).map(String::as_str),
            _ => None,
        };
        View {
            tree: &self.tree,
            current_directory: self.current_directory,
            rows: &self.visible,
            cursor_index: self.cursor_index,
            marks: &self.marks.members,
            marked: &self.marks.order,
            marked_cursor: (!self.marks.is_empty()).then(|| self.marked_cursor.min(self.marks.len() - 1)),
            covered_by: self.covering_mark(),
            focus: self.focus,
            side_visible: self.side_visible(),
            split: self.split,
            filter: &self.filter,
            size_mode: self.size_mode,
            sort_mode: self.sort_mode,
            phase: phase.view_phase(),
            indexed_entries: self.indexed_entries,
            directory_items: self.listing.len(),
            visible_items: self.visible.len(),
            incomplete_entries: self.incomplete_entries,
            exclusions: self.exclusions,
            scan_errors: self.scan_errors,
            read_only: self.read_only,
            range_mode: self.range_mode,
            message: self.message.as_deref(),
            detail_message,
            modal,
            operation,
            ls_colors: &self.ls_colors,
        }
    }

    fn finish_scan(&mut self, error: Option<String>) {
        let failed = error.is_some();
        if let Some(error) = error {
            self.message = Some(error);
            self.scan_errors = self.scan_errors.saturating_add(1);
            self.mark_incomplete_chain(self.tree.root());
        }
        if failed {
            let changed = Arc::get_mut(&mut self.tree)
                .expect("scan worker is joined before model updates")
                .mark_scanning_incomplete();
            self.bump_incomplete(changed);
        } else if self.tree.record(self.tree.root()).is_some_and(|record| record.state == NodeState::Scanning) {
            self.set_state(self.tree.root(), NodeStatus::Complete);
        }
        if let Some(tree) = Arc::get_mut(&mut self.tree) {
            tree.shrink_to_fit();
        }
        self.range_mode = false;
        self.range_anchor = None;
        self.scanning = false;
        let preserve_cursor = self.cursor_moved_during_scan;
        self.cursor_moved_during_scan = false;
        self.rebuild_listing(preserve_cursor);
    }

    fn apply_delete_outcomes(&mut self, outcomes: Vec<DeleteOutcome>, worker_error: Option<String>, skipped: usize) {
        let mut summary = DeleteSummary::default();
        let mut unfinished = Vec::new();
        for outcome in outcomes {
            if matches!(outcome.kind, OutcomeKind::Changed | OutcomeKind::Failed | OutcomeKind::Cancelled) {
                unfinished.push(outcome.node);
            }
            match outcome.kind {
                OutcomeKind::Deleted => {
                    summary.deleted = summary.deleted.saturating_add(1);
                    if self.tombstone(outcome.node).is_none() {
                        summary.failed = summary.failed.saturating_add(1);
                        self.mark_stale_chain(outcome.node);
                        self.status_by_node.insert(outcome.node, "Deleted on disk; index accounting could not be reconciled. Refresh required.".to_owned());
                    }
                }
                OutcomeKind::AlreadyAbsent => {
                    summary.already_absent = summary.already_absent.saturating_add(1);
                    if self.tombstone(outcome.node).is_none() {
                        summary.failed = summary.failed.saturating_add(1);
                        self.mark_stale_chain(outcome.node);
                    }
                }
                OutcomeKind::Changed | OutcomeKind::Failed => {
                    if outcome.kind == OutcomeKind::Changed {
                        summary.changed = summary.changed.saturating_add(1);
                    } else {
                        summary.failed = summary.failed.saturating_add(1);
                    }
                    let message = outcome.message.unwrap_or_else(|| "entry could not be removed".to_owned());
                    self.mark_stale_chain(outcome.node);
                    self.status_by_node.insert(outcome.node, message);
                }
                OutcomeKind::Cancelled => summary.not_attempted = summary.not_attempted.saturating_add(1),
            }
        }
        if let Some(error) = &worker_error {
            self.mark_stale_chain(self.tree.root());
            self.message = Some(format!(
                "Deletion stopped: {error} · {} deleted, {} not attempted",
                summary.deleted, summary.not_attempted
            ));
        } else {
            self.message = Some(summary.describe(skipped));
        }
        self.actual_deletions = self.actual_deletions.saturating_add(summary.deleted);
        self.keep_unfinished_marks(&unfinished, worker_error.is_some());
        self.normalize_focus();
        self.range_mode = false;
        self.range_anchor = None;
        self.rebuild_listing(true);
    }

    /// After a deletion only the marks that still have work left stay marked,
    /// so a retry needs no re-marking. A stopped deletion keeps every surviving
    /// mark because its unattempted entries are not all reported.
    fn keep_unfinished_marks(&mut self, unfinished: &[NodeId], stopped: bool) {
        let mut unfinished_marks = HashSet::new();
        for node in unfinished {
            let mut current = Some(*node);
            while let Some(id) = current {
                if self.marks.contains(&id) {
                    unfinished_marks.insert(id);
                    break;
                }
                current = self.tree.record(id).and_then(|record| record.parent());
            }
        }
        let tree = &self.tree;
        self.marks.retain(|id| {
            tree.record(*id).is_some_and(|record| record.state != NodeState::Tombstone)
                && (stopped || unfinished_marks.contains(id))
        });
        self.marked_cursor = self.marked_cursor.min(self.marks.len().saturating_sub(1));
    }

    fn node_for_token(&self, token: DirectoryToken) -> Option<NodeId> {
        self.token_nodes.get(token.0 as usize).copied().flatten()
    }

    fn set_state(&mut self, id: NodeId, state: NodeStatus) {
        let old = self.tree.record(id).map(|record| record.state);
        let next_state = NodeState::from(state);
        if old == Some(next_state) {
            return;
        }
        if let Some(old) = old {
            self.adjust_state_count(old, -1);
        }
        if Arc::get_mut(&mut self.tree).is_some_and(|tree| tree.set_state(id, state)) {
            self.adjust_state_count(next_state, 1);
        }
    }

    fn adjust_state_count(&mut self, state: NodeState, amount: isize) {
        match state {
            NodeState::Incomplete => {
                if amount > 0 {
                    self.incomplete_entries = self.incomplete_entries.saturating_add(amount as usize);
                } else {
                    self.incomplete_entries = self.incomplete_entries.saturating_sub(amount.unsigned_abs());
                }
            }
            NodeState::Excluded(reason) => self.bump_exclusion(reason, amount),
            _ => {}
        }
    }

    fn bump_incomplete(&mut self, count: usize) {
        self.incomplete_entries = self.incomplete_entries.saturating_add(count);
    }

    fn bump_exclusion(&mut self, reason: ExclusionReason, amount: isize) {
        let counter = &mut self.exclusions;
        match reason {
            ExclusionReason::MountBoundary | ExclusionReason::UnsupportedAlias => {
                if amount > 0 {
                    *counter = counter.saturating_add(amount as usize);
                } else {
                    *counter = counter.saturating_sub(amount.unsigned_abs());
                }
            }
        }
    }

    fn explain_scan_error(&mut self, node: NodeId, message: String) {
        self.status_by_node.insert(node, message);
        let mut current = self.tree.record(node).and_then(|record| record.parent());
        while let Some(ancestor) = current {
            if self.status_by_node.contains_key(&ancestor) {
                break;
            }
            self.status_by_node.insert(ancestor, "Contains entries with scan errors.".to_owned());
            current = self.tree.record(ancestor).and_then(|record| record.parent());
        }
    }

    fn mark_incomplete_chain(&mut self, start: NodeId) {
        let mut current = Some(start);
        while let Some(id) = current {
            let Some(record) = self.tree.record(id) else {
                break;
            };
            let parent = record.parent();
            if matches!(record.state, NodeState::Scanning | NodeState::Complete) {
                self.set_state(id, NodeStatus::Incomplete);
            }
            current = parent;
        }
    }

    fn mark_stale_chain(&mut self, start: NodeId) {
        let mut current = Some(start);
        while let Some(id) = current {
            let Some(record) = self.tree.record(id) else {
                break;
            };
            let parent = record.parent();
            if record.state != NodeState::Tombstone {
                self.set_state(id, NodeStatus::Stale);
            }
            current = parent;
        }
    }

    fn cancel_incomplete_nodes(&mut self) {
        let changed = Arc::get_mut(&mut self.tree)
            .expect("scanner is joined before cancellation is applied")
            .mark_scanning_incomplete();
        self.bump_incomplete(changed);
    }

    fn tombstone(&mut self, id: NodeId) -> Option<(u64, u64)> {
        let (apparent, allocated, removed_entries) =
            Arc::get_mut(&mut self.tree)?.tombstone_subtree(id)?;
        self.indexed_entries = self.indexed_entries.saturating_sub(removed_entries);
        Some((apparent, allocated))
    }

    fn rebuild_listing(&mut self, preserve_cursor: bool) {
        let previous = if preserve_cursor { self.cursor } else { Cursor::None };
        self.listing = self.tree.children(self.current_directory).collect();
        if !self.is_scanning() {
            self.sort_listing();
        }
        self.visible = self
            .listing
            .iter()
            .copied()
            .filter(|id| self.filter_matches(*id))
            .collect();
        self.cursor = self.valid_cursor(previous).unwrap_or_else(|| self.first_cursor());
        self.refresh_cursor_index();
    }

    fn rebuild_visible(&mut self, preserve_cursor: bool) {
        let previous = if preserve_cursor { self.cursor } else { Cursor::None };
        self.visible = self
            .listing
            .iter()
            .copied()
            .filter(|id| self.filter_matches(*id))
            .collect();
        self.cursor = self.valid_cursor(previous).unwrap_or_else(|| self.first_cursor());
        self.refresh_cursor_index();
    }

    fn sort_listing(&mut self) {
        let mode = self.sort_mode;
        let size_mode = self.size_mode;
        let tree = &self.tree;
        self.listing.sort_unstable_by(|left, right| {
            let left_record = tree.record(*left).expect("listing node exists");
            let right_record = tree.record(*right).expect("listing node exists");
            match mode {
                SortMode::Name => tree.name(*left).unwrap_or_default().cmp(tree.name(*right).unwrap_or_default()),
                SortMode::Size => {
                    size_for(right_record, size_mode)
                        .cmp(&size_for(left_record, size_mode))
                        .then_with(|| tree.name(*left).unwrap_or_default().cmp(tree.name(*right).unwrap_or_default()))
                }
                SortMode::Date => right_record
                    .modified_time()
                    .unix_seconds()
                    .cmp(&left_record.modified_time().unix_seconds())
                    .then_with(|| tree.name(*left).unwrap_or_default().cmp(tree.name(*right).unwrap_or_default())),
            }
        });
    }

    fn resort_during_scan(&mut self) {
        if !self.scanning || self.range_mode {
            return;
        }
        self.sort_listing();
        self.rebuild_visible(self.cursor_moved_during_scan);
    }

    fn sort_visible_if_needed(&mut self) -> bool {
        if self.range_mode {
            self.message = Some("Finish range marking before changing row order.".to_owned());
            return true;
        }
        if self.sort_mode == SortMode::Size {
            let previous = self.cursor;
            self.sort_listing();
            self.rebuild_visible(true);
            self.cursor = self.valid_cursor(previous).unwrap_or_else(|| self.first_cursor());
            self.refresh_cursor_index();
        }
        true
    }

    fn set_sort(&mut self, sort: SortMode, phase: Phase) -> bool {
        if self.range_mode {
            self.message = Some("Finish range marking before changing row order.".to_owned());
            return true;
        }
        self.sort_mode = sort;
        if phase == Phase::Ready {
            let cursor = self.cursor;
            self.sort_listing();
            self.rebuild_visible(true);
            self.cursor = self.valid_cursor(cursor).unwrap_or_else(|| self.first_cursor());
            self.refresh_cursor_index();
            self.message = Some(format!("Sorted by {}", sort_name(sort)));
        } else {
            self.message = Some(format!("Will sort by {} after scanning.", sort_name(sort)));
        }
        true
    }

    fn filter_matches(&self, id: NodeId) -> bool {
        if self.filter.is_empty() {
            return true;
        }
        let name = self.tree.name(id).unwrap_or_default();
        name.windows(self.filter.len())
            .any(|candidate| candidate.eq_ignore_ascii_case(self.filter.as_bytes()))
    }

    fn valid_cursor(&self, cursor: Cursor) -> Option<Cursor> {
        match cursor {
            Cursor::Entry(id) if self.visible.contains(&id) => Some(Cursor::Entry(id)),
            _ => None,
        }
    }

    fn first_cursor(&self) -> Cursor {
        if let Some(first) = self.visible.first() {
            Cursor::Entry(*first)
        } else {
            Cursor::None
        }
    }

    fn cursor_index(&self) -> Option<usize> {
        self.cursor_index
    }

    fn refresh_cursor_index(&mut self) {
        self.cursor_index = match self.cursor {
            Cursor::Entry(id) => self.visible.iter().position(|candidate| *candidate == id),
            Cursor::None => None,
        };
    }

    fn move_cursor(&mut self, amount: isize) -> bool {
        let count = self.visible.len();
        if count == 0 {
            return false;
        }
        let current = self.cursor_index().unwrap_or(0) as isize;
        let next = current.saturating_add(amount).clamp(0, count.saturating_sub(1) as isize) as usize;
        let changed = self.set_cursor_index(next);
        if self.range_mode {
            self.extend_range();
        }
        changed
    }

    fn move_to_edge(&mut self, end: bool) -> bool {
        let count = self.visible.len();
        if count == 0 {
            return false;
        }
        let changed = self.set_cursor_index(if end { count - 1 } else { 0 });
        if self.range_mode {
            self.extend_range();
        }
        changed
    }

    fn set_cursor_index(&mut self, index: usize) -> bool {
        let next = self.visible.get(index).copied().map(Cursor::Entry).unwrap_or(Cursor::None);
        let changed = !same_cursor(self.cursor, next);
        self.cursor = next;
        self.cursor_index = Some(index);
        if changed && self.scanning {
            self.cursor_moved_during_scan = true;
        }
        changed
    }

    /// Marked directories that contain at least one other mark.
    fn ancestors_of_marks(&self) -> HashSet<NodeId> {
        let mut ancestors = HashSet::new();
        for mark in &self.marks.order {
            let mut current = self.tree.record(*mark).and_then(|record| record.parent());
            while let Some(node) = current {
                if !ancestors.insert(node) {
                    break;
                }
                current = self.tree.record(node).and_then(|record| record.parent());
            }
        }
        ancestors
    }

    /// The nearest marked directory above `id`, if any.
    fn marked_ancestor(&self, id: NodeId) -> Option<NodeId> {
        let mut current = self.tree.record(id).and_then(|record| record.parent());
        while let Some(node) = current {
            if self.marks.contains(&node) {
                return Some(node);
            }
            current = self.tree.record(node).and_then(|record| record.parent());
        }
        None
    }

    /// The marked directory that already covers everything in the current
    /// directory, which may be the current directory itself.
    fn covering_mark(&self) -> Option<NodeId> {
        if self.marks.contains(&self.current_directory) {
            Some(self.current_directory)
        } else {
            self.marked_ancestor(self.current_directory)
        }
    }

    fn is_inside(&self, node: NodeId, ancestor: NodeId) -> bool {
        let mut current = self.tree.record(node).and_then(|record| record.parent());
        while let Some(candidate) = current {
            if candidate == ancestor {
                return true;
            }
            current = self.tree.record(candidate).and_then(|record| record.parent());
        }
        false
    }

    fn display_path(&self, id: NodeId) -> String {
        self.tree
            .materialize_path(id)
            .unwrap_or_default()
            .into_iter()
            .map(fdu_tui::escape_name)
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Marks `id`, replacing any marks inside it, unless a marked directory
    /// already covers it. Returns the reason it was refused.
    fn try_mark(&mut self, id: NodeId, ancestors_of_marks: &HashSet<NodeId>) -> Result<bool, String> {
        if self.marks.contains(&id) {
            return Ok(false);
        }
        if self.tree.record(id).is_none_or(|record| record.state == NodeState::Tombstone) {
            return Err("That entry has already been removed.".to_owned());
        }
        if let Some(ancestor) = self.marked_ancestor(id) {
            return Err(format!(
                "Already covered by marked directory {}.",
                self.display_path(ancestor)
            ));
        }
        if ancestors_of_marks.contains(&id) {
            self.unmark_inside(id);
        }
        Ok(self.marks.insert(id))
    }

    fn unmark_inside(&mut self, directory: NodeId) {
        let inside: Vec<NodeId> = self.marks.order.iter().copied().filter(|mark| self.is_inside(*mark, directory)).collect();
        for mark in inside {
            self.marks.remove(&mark);
        }
        self.marked_cursor = self.marked_cursor.min(self.marks.len().saturating_sub(1));
    }

    fn toggle_mark(&mut self) -> bool {
        let Cursor::Entry(id) = self.cursor else {
            return false;
        };
        let changed = if self.marks.remove(&id) {
            self.marked_cursor = self.marked_cursor.min(self.marks.len().saturating_sub(1));
            true
        } else {
            let ancestors = self.ancestors_of_marks();
            match self.try_mark(id, &ancestors) {
                Ok(added) => added,
                Err(reason) => {
                    // Stay on the refused row so the reason reads against it.
                    self.message = Some(reason);
                    return true;
                }
            }
        };
        self.normalize_focus();
        self.move_cursor(1);
        changed
    }

    fn toggle_range(&mut self) -> bool {
        if self.range_mode {
            self.range_mode = false;
            self.range_anchor = None;
            self.message = None;
            return true;
        }
        let Cursor::Entry(id) = self.cursor else {
            return false;
        };
        let ancestors = self.ancestors_of_marks();
        if let Err(reason) = self.try_mark(id, &ancestors) {
            self.message = Some(reason);
            return true;
        }
        self.range_anchor = Some(id);
        self.range_mode = true;
        self.normalize_focus();
        true
    }

    fn extend_range(&mut self) {
        let (Some(anchor), Cursor::Entry(target)) = (self.range_anchor, self.cursor) else {
            return;
        };
        let Some(start) = self.visible.iter().position(|id| *id == anchor) else {
            return;
        };
        let Some(end) = self.visible.iter().position(|id| *id == target) else {
            return;
        };
        let range: Vec<NodeId> = self.visible[start.min(end)..=start.max(end)].to_vec();
        self.mark_many(range);
    }

    fn mark_all_visible(&mut self) -> bool {
        let changed = self.mark_many(self.visible.clone());
        changed || self.message.is_some()
    }

    /// Marks each entry that is not covered by a marked directory and reports
    /// how many were skipped. Returns whether any mark was added.
    fn mark_many(&mut self, entries: Vec<NodeId>) -> bool {
        let ancestors = self.ancestors_of_marks();
        let mut skipped = 0usize;
        let mut changed = false;
        for id in entries {
            match self.try_mark(id, &ancestors) {
                Ok(added) => changed |= added,
                Err(_) => skipped += 1,
            }
        }
        self.normalize_focus();
        if skipped > 0 {
            self.message = Some(format!(
                "{skipped} entr{} skipped: already covered by a marked directory.",
                if skipped == 1 { "y" } else { "ies" }
            ));
        }
        changed
    }

    fn enter_cursor(&mut self) -> bool {
        match self.cursor {
            Cursor::Entry(id) => {
                let Some(record) = self.tree.record(id) else {
                    return false;
                };
                if record.entry_type != EntryType::Directory {
                    self.message = Some("This entry is not a directory.".to_owned());
                    return true;
                }
                match record.state {
                    NodeState::Excluded(ExclusionReason::MountBoundary) => {
                        self.message = Some("This directory is an excluded mount boundary.".to_owned());
                        return true;
                    }
                    NodeState::Excluded(ExclusionReason::UnsupportedAlias) => {
                        self.message = Some("This directory is an unsupported filesystem alias.".to_owned());
                        return true;
                    }
                    _ => {}
                }
                self.change_directory(id);
                true
            }
            Cursor::None => false,
        }
    }

    fn go_parent(&mut self) -> bool {
        let Some(parent) = self.tree.record(self.current_directory).and_then(|record| record.parent()) else {
            return false;
        };
        self.change_directory(parent);
        true
    }

    /// Marks belong to the whole root, so they stay when the directory changes.
    fn change_directory(&mut self, directory: NodeId) {
        self.remember_current_cursor();
        self.current_directory = directory;
        self.cursor_moved_during_scan = false;
        self.range_mode = false;
        self.range_anchor = None;
        self.message = self.covering_mark().map(|marked| {
            format!(
                "Inside marked directory {}.",
                self.display_path(marked)
            )
        });
        self.rebuild_listing(false);
        let previous = self.directory_cursors.get(&directory).copied().unwrap_or(Cursor::None);
        let restored = self.valid_cursor(previous);
        self.cursor = restored.unwrap_or_else(|| self.first_cursor());
        self.refresh_cursor_index();
        self.cursor_moved_during_scan = self.scanning && restored.is_some();
    }

    fn remember_current_cursor(&mut self) {
        self.directory_cursors.insert(self.current_directory, self.cursor);
    }

    fn is_scanning(&self) -> bool {
        self.scanning
    }
}

fn same_cursor(left: Cursor, right: Cursor) -> bool {
    match (left, right) {
        (Cursor::None, Cursor::None) => true,
        (Cursor::Entry(left), Cursor::Entry(right)) => left == right,
        _ => false,
    }
}

fn sort_name(sort: SortMode) -> &'static str {
    match sort {
        SortMode::Size => "size",
        SortMode::Name => "name",
        SortMode::Date => "date",
    }
}

/// Outcome counts for one deletion, reported in the status strip.
#[derive(Default)]
struct DeleteSummary {
    deleted: usize,
    already_absent: usize,
    changed: usize,
    failed: usize,
    not_attempted: usize,
}

impl DeleteSummary {
    fn describe(&self, skipped: usize) -> String {
        let mut parts = vec![format!("Deleted {}", self.deleted)];
        for (count, label) in [
            (self.already_absent, "already absent"),
            (self.changed, "changed"),
            (self.failed, "failed"),
            (self.not_attempted, "not attempted"),
            (skipped, "skipped"),
        ] {
            if count > 0 {
                parts.push(format!("{count} {label}"));
            }
        }
        if self.changed + self.failed > 0 {
            parts.push("select a ~ row for why".to_owned());
        }
        parts.join(" · ")
    }
}

fn size_for(record: &fdu_core::NodeRecord, mode: SizeMode) -> u64 {
    match mode {
        SizeMode::Allocated => record.allocated_bytes,
        SizeMode::Apparent => record.apparent_bytes,
    }
}

pub fn run(
    path: std::path::PathBuf,
    read_only: bool,
    apparent: bool,
    exclusions: ScanExclusions,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut signals = Signals::new([SIGHUP, SIGTERM])?;
    let mut terminal = TerminalSession::enter()?;
    match run_session(path, read_only, apparent, exclusions, &mut terminal, &mut signals) {
        Err(operation) => {
            let result = combine_shutdown_result(Err(operation), terminal.restore());
            if let Some(signal) = signals.pending().next() {
                if let Err(error) = &result {
                    eprintln!("fdu: {error}");
                }
                drop(signals);
                if let Err(error) = signal_hook::low_level::emulate_default_handler(signal) {
                    eprintln!("fdu: could not restore default handling for signal {signal}: {error}");
                }
                return Err(crate::SignalExit(signal).into());
            }
            result
        }
        Ok((mut termination_signal, mut profile, model)) => {
            let restoration = terminal.restore();
            if termination_signal.is_none() {
                termination_signal = signals.pending().next();
            }
            if let Some(signal) = termination_signal {
                if let Err(error) = restoration {
                    eprintln!("fdu: could not restore terminal: {error}");
                }
                drop(signals);
                if let Err(error) = signal_hook::low_level::emulate_default_handler(signal) {
                    eprintln!("fdu: could not restore default handling for signal {signal}: {error}");
                }
                return Err(crate::SignalExit(signal).into());
            }
            combine_shutdown_result(Ok(()), restoration)?;
            profile.report(&model);
            Ok(())
        }
    }
}

fn run_session(
    path: std::path::PathBuf,
    read_only: bool,
    apparent: bool,
    exclusions: ScanExclusions,
    terminal: &mut TerminalSession,
    signals: &mut Signals,
) -> Result<(Option<i32>, BrowserProfile, BrowserModel), Box<dyn std::error::Error>> {
    let root = open_root(&path)?;
    let mut model = BrowserModel::new(&root, read_only, apparent, fdu_tui::terminal_size()?)?;
    let profiling_enabled = std::env::var_os("FDU_PROFILE").is_some();
    let scan_metrics = ScanQueueMetrics::new(SCAN_EVENT_CHANNEL_CAPACITY, profiling_enabled);
    let mut profile = BrowserProfile::new(profiling_enabled, scan_metrics.clone());
    let mut phase = AppPhase::Scanning(ScanRun::start(root.clone(), exclusions.clone(), scan_metrics));
    let mut last_draw = Instant::now() - SCAN_REDRAW_INTERVAL;
    let mut draw_pending = true;
    let mut next_resort_at = Instant::now();
    let mut first_live_listing_directory = None;
    let mut termination_signal = None;
    loop {
        if let Some(signal) = signals.pending().next() {
            termination_signal = Some(signal);
            break;
        }
        let text_input = matches!(&model.dialog, DialogState::Filter(_));
        // The scanner feeds events through a small bounded channel, so a long
        // idle wait here would cap how fast scanning can run. Deletion reports
        // progress without blocking, so it can wait the usual interval.
        let input_poll_interval = if matches!(&phase, AppPhase::Scanning(_)) {
            // Input is only checked here; the wait for scan events is below.
            Duration::ZERO
        } else {
            INPUT_POLL_INTERVAL
        };
        let waiting = profile.timer();
        let intent = match fdu_tui::poll_intent(input_poll_interval, text_input) {
            Ok(intent) => intent,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                if let Some(signal) = signals.pending().next() {
                    termination_signal = Some(signal);
                    break;
                }
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(started) = waiting {
            profile.busy.waiting_for_input += started.elapsed();
        }
        let mut input_changed = false;
        let mut action = Action::None;
        if let Some(intent) = intent {
            profile.note_input();
            let (next_action, changed) = model.handle_intent(intent, phase.view_phase());
            action = next_action;
            input_changed = changed;
        }
        match action {
            Action::None => {}
            Action::Quit => break,
            Action::Refresh => {
                model.reset_for_rescan(&root)?;
                terminal.begin_scan();
                first_live_listing_directory = None;
                let metrics = ScanQueueMetrics::new(SCAN_EVENT_CHANNEL_CAPACITY, profiling_enabled);
                profile.reset_scan_queue(metrics.clone());
                phase = AppPhase::Scanning(ScanRun::start(root.clone(), exclusions.clone(), metrics));
                draw_pending = true;
            }
            Action::Delete(plan) => match root.try_clone_fd() {
                Ok(root_fd) => {
                    let paused_scan = match std::mem::replace(&mut phase, AppPhase::Ready) {
                        AppPhase::Scanning(scan) => Some(scan),
                        _ => None,
                    };
                    phase = AppPhase::Deleting(DeleteRun::start(root_fd, plan, paused_scan));
                    draw_pending = true;
                }
                Err(error) => {
                    model.message = Some(format!("Could not anchor deletion to the scan root: {error}"));
                    draw_pending = true;
                }
            },
            Action::CancelDeletion => {
                if let AppPhase::Deleting(worker) = &phase {
                    worker.cancel();
                    draw_pending = true;
                }
            }
        }

        let mut model_changed = false;
        let mut scan_finished = false;
        // A pending plan shares the index, so scan events wait in the bounded channel, which
        // pauses the scanner until the plan is confirmed or dropped.
        if let AppPhase::Scanning(worker) = &mut phase
            && model.pending_delete.is_none()
            && let Some(receiver) = worker.receiver.as_ref() {
            let drain_started = Instant::now();
            let mut first_event = true;
            loop {
                if drain_started.elapsed() >= SCAN_DRAIN_BUDGET {
                    break;
                }
                // Only an idle tick waits; one that already has work goes straight on to draw.
                let received = if first_event {
                    receiver.recv_timeout(SCAN_EVENT_WAIT).map_err(|error| match error {
                        RecvTimeoutError::Timeout => TryRecvError::Empty,
                        RecvTimeoutError::Disconnected => TryRecvError::Disconnected,
                    })
                } else {
                    receiver.try_recv()
                };
                first_event = false;
                match received {
                    Ok(event) => {
                        worker.queue_metrics.event_received();
                        if matches!(&event, ScanEvent::Finished) {
                            scan_finished = true;
                            break;
                        }
                        match event {
                            ScanEvent::Entries(batch) => {
                                let applying = profile.timer();
                                if profile.enabled { profile.first_entries.get_or_insert_with(|| profile.started.elapsed()); }
                                let batch = model.apply_entry_batch(batch);
                                worker.return_batch(batch);
                                model_changed = true;
                                if let Some(started) = applying {
                                    let elapsed = started.elapsed();
                                    profile.busy.entry_batches += elapsed;
                                    profile.busy.entry_batch_count += 1;
                                    profile.busy.max_entry_batch_micros = profile
                                        .busy
                                        .max_entry_batch_micros
                                        .max(elapsed.as_micros().min(u128::from(u64::MAX)) as u64);
                                }
                                // Draw the first listing before applying anything further, so a scan that
                                // finishes within this tick still settles after the listing is on screen.
                                if !model.visible.is_empty() && first_live_listing_directory != Some(model.current_directory) {
                                    break;
                                }
                            }
                            other => {
                                let applying = profile.timer();
                                model_changed |= model.apply_scan_event(other);
                                if let Some(started) = applying {
                                    profile.busy.other_events += started.elapsed();
                                }
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        scan_finished = true;
                        break;
                    }
                }
            }
            if profile.enabled {
                profile.busy.max_scan_drain_micros = profile
                    .busy
                    .max_scan_drain_micros
                    .max(drain_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
            }
        }
        if scan_finished {
            let scan_error = match &mut phase {
                AppPhase::Scanning(worker) => worker.finish().err(),
                _ => None,
            };
            model.finish_scan(scan_error);
            phase = AppPhase::Ready;
            profile.note_scan_settled();
            model_changed = true;
        }

        let mut delete_finished = false;
        if let AppPhase::Deleting(worker) = &mut phase
            && let Some(receiver) = worker.receiver.as_ref() {
            for _ in 0..MAX_DELETE_EVENTS_PER_TICK {
                match receiver.try_recv() {
                    Ok(DeleteEvent::Progress { completed, total, .. }) => {
                        worker.completed = completed;
                        worker.total = total;
                        model_changed = true;
                    }
                    Ok(DeleteEvent::Outcomes(outcomes)) => {
                        worker.outcomes.extend(outcomes);
                    }
                    Ok(DeleteEvent::Failed { message }) => {
                        worker.failure = Some(message.clone());
                        model.message = Some(format!("Deletion stopped: {message}"));
                        model_changed = true;
                    }
                    Ok(DeleteEvent::Finished) => {
                        delete_finished = true;
                        break;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        delete_finished = true;
                        break;
                    }
                }
            }
        }
        if delete_finished {
            let (outcomes, delete_error, skipped, paused_scan) = match &mut phase {
                AppPhase::Deleting(worker) => {
                    profile.delete_worker = Some(worker.started.elapsed());
                    let skipped = worker.skipped;
                    let paused_scan = worker.paused_scan.take();
                    match worker.finish() {
                        Ok((outcomes, failure)) => (outcomes, failure, skipped, paused_scan),
                        Err(error) => (Vec::new(), Some(error), skipped, paused_scan),
                    }
                }
                _ => (Vec::new(), None, 0, None),
            };
            let applying = Instant::now();
            model.apply_delete_outcomes(outcomes, delete_error, skipped);
            profile.delete_apply = Some(applying.elapsed());
            phase = paused_scan.map_or(AppPhase::Ready, AppPhase::Scanning);
            model_changed = true;
        }

        if input_changed || model_changed {
            draw_pending = true;
        }
        let first_live_listing_pending = !model.visible.is_empty()
            && first_live_listing_directory != Some(model.current_directory);
        let draw_now = match &phase {
            AppPhase::Scanning(_) => {
                draw_pending && (last_draw.elapsed() >= SCAN_REDRAW_INTERVAL || first_live_listing_pending)
                    || input_changed
            }
            AppPhase::Deleting(_) => draw_pending && last_draw.elapsed() >= SCAN_REDRAW_INTERVAL || input_changed,
            AppPhase::Ready => draw_pending,
        };
        if draw_now {
            if matches!(&phase, AppPhase::Scanning(_)) && Instant::now() >= next_resort_at {
                let sorting = Instant::now();
                model.resort_during_scan();
                next_resort_at = Instant::now() + sorting.elapsed() * SCAN_RESORT_COST_FACTOR;
            }
            let drawing = profile.timer();
            let view = model.view(&phase);
            terminal.draw(&view)?;
            if let Some(started) = drawing {
                profile.busy.draws += started.elapsed();
                profile.busy.draw_count += 1;
            }
            profile.note_render(&model, phase.view_phase());
            if !model.visible.is_empty() {
                first_live_listing_directory = Some(model.current_directory);
            }
            draw_pending = false;
            last_draw = Instant::now();
        }
    }
    drop(phase);
    Ok((termination_signal, profile, model))
}

#[cfg(test)]
mod tests {
    use super::{combine_shutdown_result, Action, BrowserModel, DeleteSummary, DialogState};
    use fdu_core::{DirectoryToken, EntryBatch, EntryType, FileIdentity, ModifiedTime, NodeId, NodeState, NodeStatus, ScanEntry, ScanEvent, Tree};
    use fdu_delete::{DeleteOutcome, OutcomeKind};
    use fdu_scan::{open_root, RootAnchor};
    use fdu_tui::{Cursor, Intent, Pane, Phase, SizeMode, SortMode, Split, TerminalSize};
    use std::fs;
    use std::io;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    static TEMP_ID: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn shutdown_keeps_operation_error_as_source_and_reports_restore_error() {
        let operation = io::Error::new(io::ErrorKind::InvalidData, "operation failed");
        let restoration = io::Error::new(io::ErrorKind::PermissionDenied, "restore failed");
        let error = combine_shutdown_result::<()>(Err(Box::new(operation)), Err(restoration)).unwrap_err();

        assert!(error.to_string().contains("operation failed"));
        assert!(error.to_string().contains("restore failed"));
        let source = std::error::Error::source(error.as_ref()).unwrap();
        assert_eq!(source.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::InvalidData);
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> io::Result<Self> {
            loop {
                let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
                let path = std::env::temp_dir().join(format!("fdu-browser-{}-{id}", std::process::id()));
                match fs::create_dir(&path) {
                    Ok(()) => return Ok(Self(path)),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            }
        }

        fn root(&self) -> io::Result<RootAnchor> {
            open_root(&self.0)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn append(
        model: &mut BrowserModel,
        parent: NodeId,
        name: &[u8],
        entry_type: EntryType,
        bytes: u64,
        inode: u64,
    ) -> NodeId {
        let state = NodeStatus::Complete;
        let tree = Arc::get_mut(&mut model.tree).expect("test model has a single tree owner");
        let id = tree
            .append(
                parent,
                name,
                entry_type,
                FileIdentity { device: 1, inode },
                1,
                if entry_type == EntryType::Directory { 0 } else { bytes },
                if entry_type == EntryType::Directory { 0 } else { bytes },
                state,
            )
            .unwrap();
        if entry_type != EntryType::Directory {
            assert!(tree.add_to_ancestors(parent, bytes, bytes));
        }
        model.indexed_entries = model.tree.len().saturating_sub(1);
        if parent == model.current_directory {
            model.listing.push(id);
            if model.filter_matches(id) {
                model.visible.push(id);
            }
            if matches!(model.cursor, Cursor::None) {
                model.cursor = Cursor::Entry(id);
                model.refresh_cursor_index();
            }
        }
        id
    }

    fn ready(model: &mut BrowserModel) {
        model.finish_scan(None);
    }

    fn make_model(temp: &TempDir, read_only: bool) -> io::Result<(RootAnchor, BrowserModel)> {
        let root = temp.root()?;
        let model = BrowserModel::new(&root, read_only, false, TerminalSize { columns: 120, rows: 40 })?;
        Ok((root, model))
    }

    #[test]
    fn finish_scan_selects_sorted_top_entry_when_cursor_was_untouched() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        append(&mut model, root, b"first-discovered", EntryType::RegularFile, 1, 2);
        let largest = append(&mut model, root, b"largest", EntryType::RegularFile, 100, 3);

        ready(&mut model);

        assert!(matches!(model.cursor, Cursor::Entry(id) if id == largest));
        assert_eq!(model.cursor_index(), Some(0));
        Ok(())
    }

    #[test]
    fn size_sort_orders_incomplete_directory_by_its_known_total() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let incomplete_directory = append(&mut model, root, b"incomplete", EntryType::Directory, 0, 2);
        append(&mut model, incomplete_directory, b"known.bin", EntryType::RegularFile, 1000, 3);
        model.set_state(incomplete_directory, NodeStatus::Incomplete);
        let complete_file = append(&mut model, root, b"complete.bin", EntryType::RegularFile, 100, 4);

        ready(&mut model);

        assert_eq!(model.listing, [incomplete_directory, complete_file]);
        Ok(())
    }

    #[test]
    fn finish_scan_preserves_deliberate_cursor_movement_by_node_id() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        append(&mut model, root, b"first-discovered", EntryType::RegularFile, 1, 2);
        let selected = append(&mut model, root, b"selected", EntryType::RegularFile, 10, 3);
        append(&mut model, root, b"largest", EntryType::RegularFile, 100, 4);

        let (_, changed) = model.handle_intent(Intent::MoveDown, Phase::Scanning);
        assert!(changed);
        ready(&mut model);

        assert!(matches!(model.cursor, Cursor::Entry(id) if id == selected));
        assert_eq!(model.cursor_index(), Some(1));
        Ok(())
    }

    #[test]
    fn live_resort_orders_rows_by_size_and_keeps_the_cursor_entry() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        append(&mut model, root, b"small", EntryType::RegularFile, 1, 2);
        let selected = append(&mut model, root, b"selected", EntryType::RegularFile, 10, 3);
        let largest = append(&mut model, root, b"largest", EntryType::RegularFile, 100, 4);

        model.resort_during_scan();
        assert_eq!(model.visible[0], largest);
        assert!(matches!(model.cursor, Cursor::Entry(id) if id == largest), "untouched cursor stays on the first row");

        let (_, changed) = model.handle_intent(Intent::MoveDown, Phase::Scanning);
        assert!(changed);
        assert!(matches!(model.cursor, Cursor::Entry(id) if id == selected));
        let bigger = append(&mut model, root, b"bigger", EntryType::RegularFile, 1000, 5);
        model.resort_during_scan();

        assert_eq!(model.visible[0], bigger);
        assert!(matches!(model.cursor, Cursor::Entry(id) if id == selected));
        assert_eq!(model.cursor_index(), Some(2));
        Ok(())
    }

    #[test]
    fn marks_survive_filter_sort_and_navigation_and_clear_on_rescan() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (root, mut model) = make_model(&temp, false)?;
        let root_node = model.tree.root();
        let alpha = append(&mut model, root_node, b"alpha", EntryType::Directory, 0, 2);
        let nested = append(&mut model, alpha, b"nested", EntryType::Directory, 0, 3);
        append(&mut model, nested, b"payload", EntryType::RegularFile, 50, 4);
        let bravo = append(&mut model, root_node, b"bravo", EntryType::RegularFile, 100, 5);
        let charlie = append(&mut model, root_node, b"charlie", EntryType::RegularFile, 25, 6);
        ready(&mut model);

        model.marks.insert(bravo);
        model.marks.insert(charlie);
        model.filter = "charlie".to_owned();
        model.rebuild_visible(true);
        assert_eq!(model.visible, vec![charlie]);
        assert_eq!(model.marks.len(), 2);
        assert!(model.set_sort(SortMode::Name, Phase::Ready));
        assert_eq!(model.marks.len(), 2);

        model.filter.clear();
        model.rebuild_visible(true);
        model.cursor = Cursor::Entry(alpha);
        model.refresh_cursor_index();
        assert!(model.enter_cursor());
        assert_eq!(model.current_directory, alpha);
        assert_eq!(model.visible, vec![nested]);
        assert_eq!(model.marks.len(), 2, "marks outlive navigation");
        assert!(model.go_parent());
        assert!(!model.go_parent(), "navigation stops at the opened root");
        assert_eq!(model.current_directory, root_node);

        model.reset_for_rescan(&root)?;
        assert!(model.marks.is_empty());
        assert_eq!(model.current_directory, model.tree.root());
        assert!(model.is_scanning());
        assert!(matches!(model.size_mode, SizeMode::Allocated));
        Ok(())
    }

    #[test]
    fn directory_navigation_restores_cursor_positions() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        append(&mut model, root, b"alpha", EntryType::Directory, 0, 2);
        let bravo = append(&mut model, root, b"bravo", EntryType::Directory, 0, 5);
        append(&mut model, bravo, b"first", EntryType::RegularFile, 2, 3);
        let selected = append(&mut model, bravo, b"selected", EntryType::RegularFile, 1, 4);

        model.cursor = Cursor::Entry(bravo);
        model.refresh_cursor_index();
        assert!(model.enter_cursor());
        model.cursor = Cursor::Entry(selected);
        model.refresh_cursor_index();

        assert!(model.go_parent());
        assert!(matches!(model.cursor, Cursor::Entry(id) if id == bravo));
        ready(&mut model);
        assert!(matches!(model.cursor, Cursor::Entry(id) if id == bravo));
        assert!(model.enter_cursor());
        assert!(matches!(model.cursor, Cursor::Entry(id) if id == selected));
        assert_eq!(model.cursor_index(), Some(1));
        Ok(())
    }

    #[test]
    fn marks_accumulate_across_directories_in_marking_order() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let alpha = append(&mut model, root, b"alpha", EntryType::Directory, 0, 2);
        let inner = append(&mut model, alpha, b"inner.bin", EntryType::RegularFile, 9, 3);
        let top = append(&mut model, root, b"top.bin", EntryType::RegularFile, 5, 4);
        ready(&mut model);

        model.cursor = Cursor::Entry(top);
        model.refresh_cursor_index();
        assert!(model.toggle_mark());
        model.cursor = Cursor::Entry(alpha);
        model.refresh_cursor_index();
        assert!(model.enter_cursor());
        model.cursor = Cursor::Entry(inner);
        model.refresh_cursor_index();
        assert!(model.toggle_mark());

        assert_eq!(model.marks.order, vec![top, inner]);
        Ok(())
    }

    #[test]
    fn overlapping_marks_are_refused_with_a_reason_in_both_directions() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let alpha = append(&mut model, root, b"alpha", EntryType::Directory, 0, 2);
        let inner = append(&mut model, alpha, b"inner.bin", EntryType::RegularFile, 9, 3);
        ready(&mut model);

        // A marked directory covers its descendants.
        model.marks.insert(alpha);
        let ancestors = model.ancestors_of_marks();
        let reason = model.try_mark(inner, &ancestors).unwrap_err();
        assert!(reason.contains("covered by marked directory alpha"), "{reason}");
        model.cursor = Cursor::Entry(alpha);
        model.refresh_cursor_index();
        assert!(model.enter_cursor());
        assert!(model.message.as_deref().unwrap().contains("Inside marked directory alpha"));
        assert_eq!(model.covering_mark(), Some(alpha));

        // Marking the ancestor of a marked descendant replaces the descendant.
        model.marks.clear();
        model.marks.insert(inner);
        let ancestors = model.ancestors_of_marks();
        assert_eq!(model.try_mark(alpha, &ancestors), Ok(true));
        assert_eq!(model.marks.order, vec![alpha]);
        Ok(())
    }

    #[test]
    fn bulk_marking_a_directory_replaces_marks_inside_it() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let alpha = append(&mut model, root, b"alpha", EntryType::Directory, 0, 2);
        let inner = append(&mut model, alpha, b"inner.bin", EntryType::RegularFile, 9, 3);
        let beta = append(&mut model, root, b"beta.bin", EntryType::RegularFile, 5, 4);
        ready(&mut model);
        model.marks.insert(inner);

        assert!(model.mark_all_visible());
        assert!(model.marks.contains(&alpha));
        assert!(model.marks.contains(&beta));
        assert!(!model.marks.contains(&inner));
        assert_eq!(model.message, None);
        Ok(())
    }

    #[test]
    fn bulk_marking_inside_a_marked_directory_skips_entries_and_says_so() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let alpha = append(&mut model, root, b"alpha", EntryType::Directory, 0, 2);
        let inner = append(&mut model, alpha, b"inner.bin", EntryType::RegularFile, 9, 3);
        ready(&mut model);
        model.marks.insert(alpha);
        model.change_directory(alpha);

        model.mark_all_visible();
        assert!(!model.marks.contains(&inner));
        assert!(model.message.as_deref().unwrap().contains("1 entry skipped"));
        Ok(())
    }

    #[test]
    fn marked_pane_opens_with_the_first_mark_and_closes_with_the_last() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let file = append(&mut model, root, b"file", EntryType::RegularFile, 5, 2);
        ready(&mut model);

        assert!(!model.side_visible(), "no marks, no pane");
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        assert!(model.message.as_deref().unwrap().contains("Nothing is marked"));
        assert_eq!(model.focus, Pane::Browser);

        model.handle_intent(Intent::ToggleMark, Phase::Ready);
        assert!(model.marks.contains(&file));
        assert!(model.side_visible(), "marking opens the pane");
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        assert_eq!(model.focus, Pane::Marked);
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        assert_eq!(model.focus, Pane::Browser);
        assert!(model.side_visible(), "the pane stays open while marks remain");

        // Unmarking the last entry from the pane closes it and returns focus.
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        model.handle_intent(Intent::ToggleMark, Phase::Ready);
        assert!(model.marks.is_empty());
        assert!(!model.side_visible());
        assert_eq!(model.focus, Pane::Browser);
        Ok(())
    }

    #[test]
    fn narrow_terminal_shows_only_the_focused_pane_and_resize_preserves_focus() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let file = append(&mut model, root, b"file", EntryType::RegularFile, 5, 2);
        ready(&mut model);
        model.marks.insert(file);
        assert!(model.side_visible());

        // Narrowing keeps the list; the pane is one Tab away.
        model.handle_intent(Intent::Resize(TerminalSize { columns: 50, rows: 40 }), Phase::Ready);
        assert!(!model.side_visible());
        assert_eq!(model.focus, Pane::Browser);
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        assert!(model.side_visible());
        assert_eq!(model.focus, Pane::Marked);

        // Widening shows both and keeps focus; narrowing again keeps the pane.
        model.handle_intent(Intent::Resize(TerminalSize { columns: 140, rows: 40 }), Phase::Ready);
        assert_eq!(model.focus, Pane::Marked);
        model.handle_intent(Intent::Resize(TerminalSize { columns: 40, rows: 40 }), Phase::Ready);
        assert!(model.side_visible());
        assert_eq!(model.focus, Pane::Marked);

        // Tab returns to the list, which hides the pane at this width.
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        assert!(!model.side_visible());
        assert_eq!(model.focus, Pane::Browser);
        Ok(())
    }

    #[test]
    fn minus_stacks_the_panes_and_the_fit_rule_follows_the_orientation() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let file = append(&mut model, root, b"file", EntryType::RegularFile, 5, 2);
        ready(&mut model);
        model.marks.insert(file);
        assert_eq!(model.split, Split::Vertical);

        // Wide but short: side by side fits, stacked does not.
        model.handle_intent(Intent::Resize(TerminalSize { columns: 120, rows: 8 }), Phase::Ready);
        assert!(model.side_visible());
        model.handle_intent(Intent::ToggleSplit, Phase::Ready);
        assert_eq!(model.split, Split::Horizontal);
        assert!(!model.side_visible(), "no height for two stacked panes");

        // Narrow but tall: the reverse.
        model.handle_intent(Intent::Resize(TerminalSize { columns: 40, rows: 40 }), Phase::Ready);
        assert!(model.side_visible());
        model.handle_intent(Intent::ToggleSplit, Phase::Ready);
        assert_eq!(model.split, Split::Vertical);
        assert!(!model.side_visible());
        Ok(())
    }

    #[test]
    fn marked_pane_removes_marks_and_shows_entries_in_their_directory() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let alpha = append(&mut model, root, b"alpha", EntryType::Directory, 0, 2);
        let inner = append(&mut model, alpha, b"inner.bin", EntryType::RegularFile, 9, 3);
        let top = append(&mut model, root, b"top.bin", EntryType::RegularFile, 5, 4);
        ready(&mut model);
        model.marks.insert(inner);
        model.marks.insert(top);
        model.filter = "top".to_owned();
        model.rebuild_visible(true);
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        assert_eq!(model.focus, Pane::Marked);

        // The pane lists marks the filter hides; Enter shows one where it lives.
        let (_, changed) = model.handle_intent(Intent::Enter, Phase::Ready);
        assert!(changed);
        assert_eq!(model.current_directory, alpha);
        assert!(model.filter.is_empty());
        assert!(matches!(model.cursor, Cursor::Entry(id) if id == inner));
        assert_eq!(model.focus, Pane::Browser);

        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        model.handle_intent(Intent::MoveDown, Phase::Ready);
        assert_eq!(model.marked_cursor, 1);
        model.handle_intent(Intent::ToggleMark, Phase::Ready);
        assert_eq!(model.marks.order, vec![inner]);
        assert_eq!(model.marked_cursor, 0);
        Ok(())
    }

    #[test]
    fn deletion_locks_every_command_except_the_stop_request() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let file = append(&mut model, root, b"file", EntryType::RegularFile, 5, 2);
        let other = append(&mut model, root, b"other", EntryType::RegularFile, 5, 3);
        ready(&mut model);
        model.marks.insert(file);
        let cursor_before = model.cursor_index();

        for intent in [
            Intent::SwitchPane,
            Intent::ToggleSplit,
            Intent::MoveDown,
            Intent::ToggleMark,
            Intent::ToggleRange,
            Intent::MarkAll,
            Intent::ClearMarks,
            Intent::ToggleSort,
            Intent::ToggleSizeMode,
            Intent::StartFilter,
            Intent::Refresh,
            Intent::Delete,
            Intent::Enter,
            Intent::Parent,
            Intent::Help,
            Intent::Quit,
            Intent::FilterCharacter('x'),
        ] {
            let (action, changed) = model.handle_intent(intent, Phase::Deleting);
            assert!(matches!(action, Action::None));
            assert!(!changed);
        }
        assert_eq!(model.marks.order, vec![file]);
        assert!(!model.marks.contains(&other));
        assert_eq!(model.cursor_index(), cursor_before);
        assert!(matches!(model.dialog, DialogState::None));
        assert_eq!(model.focus, Pane::Browser);

        let (action, _) = model.handle_intent(Intent::Cancel, Phase::Deleting);
        assert!(matches!(action, Action::CancelDeletion));
        Ok(())
    }

    /// Marks `ids`, moves focus to the marked pane, and presses Ctrl-R.
    fn press_delete_on_marks(model: &mut BrowserModel, ids: &[NodeId]) -> Action {
        for id in ids {
            model.marks.insert(*id);
        }
        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        model.handle_intent(Intent::Delete, Phase::Ready).0
    }

    #[test]
    fn delete_from_the_marked_pane_asks_once_and_commits_every_root_together() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let alpha = append(&mut model, root, b"alpha", EntryType::Directory, 0, 2);
        let inner = append(&mut model, alpha, b"inner.bin", EntryType::RegularFile, 9, 3);
        let top = append(&mut model, root, b"top.bin", EntryType::RegularFile, 5, 4);
        ready(&mut model);

        assert!(matches!(press_delete_on_marks(&mut model, &[inner, top]), Action::None));
        assert!(model.pending_delete.is_some(), "Ctrl-R only asks");

        // Everything but Enter and Esc is ignored while the question is open.
        for intent in [Intent::ToggleMark, Intent::MoveDown, Intent::ClearMarks, Intent::SwitchPane, Intent::Quit] {
            let (action, changed) = model.handle_intent(intent, Phase::Ready);
            assert!(matches!(action, Action::None));
            assert!(!changed);
        }
        assert_eq!(model.marks.len(), 2);

        let (action, _) = model.handle_intent(Intent::Enter, Phase::Ready);
        match action {
            Action::Delete(plan) => assert_eq!(plan.operation_count(), 2),
            _ => panic!("Enter commits the frozen plan"),
        }
        assert!(model.pending_delete.is_none());
        Ok(())
    }

    #[test]
    fn escape_withdraws_the_question_and_keeps_the_marks() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let file = append(&mut model, root, b"file", EntryType::RegularFile, 9, 2);
        ready(&mut model);
        press_delete_on_marks(&mut model, &[file]);
        let (action, _) = model.handle_intent(Intent::Cancel, Phase::Ready);
        assert!(matches!(action, Action::None));
        assert!(model.pending_delete.is_none());
        assert_eq!(model.marks.len(), 1);
        Ok(())
    }

    #[test]
    fn ineligible_marks_are_skipped_and_counted_in_the_confirmation() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let good = append(&mut model, root, b"good.bin", EntryType::RegularFile, 9, 2);
        let bad = append(&mut model, root, b"bad.bin", EntryType::RegularFile, 5, 3);
        ready(&mut model);
        model.set_state(bad, NodeStatus::Incomplete);

        assert!(matches!(press_delete_on_marks(&mut model, &[good, bad]), Action::None));
        let plan = model.pending_delete.as_ref().expect("the complete entry is still deletable");
        assert_eq!(plan.operation_count(), 1);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(model.marks.len(), 2, "no mark is dropped");

        model.pending_delete = None;
        model.marks.clear();
        model.focus = Pane::Browser;
        assert!(matches!(press_delete_on_marks(&mut model, &[bad]), Action::None));
        let message = model.message.as_deref().unwrap();
        assert!(message.contains("Nothing can be deleted"), "{message}");
        assert!(message.contains("bad.bin: scan is incomplete"), "{message}");
        Ok(())
    }

    #[test]
    fn delete_is_only_offered_from_the_marked_pane_with_marks() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let file = append(&mut model, root, b"file", EntryType::RegularFile, 9, 2);
        ready(&mut model);

        // From the list nothing is armed, even with a mark.
        model.marks.insert(file);
        model.handle_intent(Intent::Delete, Phase::Ready);
        assert!(model.pending_delete.is_none());
        assert!(model.message.as_deref().unwrap().contains("Tab"));

        // With nothing marked there is nothing to delete.
        model.marks.clear();
        model.focus = Pane::Marked;
        model.handle_intent(Intent::Delete, Phase::Ready);
        assert!(model.pending_delete.is_none());
        assert!(model.message.as_deref().unwrap().contains("Nothing is marked"));
        Ok(())
    }

    #[test]
    fn mark_all_and_range_marking_work_from_the_list_and_are_refused_in_the_marked_pane() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let first = append(&mut model, root, b"first", EntryType::RegularFile, 1, 2);
        let second = append(&mut model, root, b"second", EntryType::RegularFile, 1, 3);
        let third = append(&mut model, root, b"third", EntryType::RegularFile, 1, 4);
        ready(&mut model);

        model.cursor = Cursor::Entry(first);
        model.refresh_cursor_index();
        model.handle_intent(Intent::ToggleRange, Phase::Ready);
        assert!(model.range_mode);
        model.handle_intent(Intent::MoveDown, Phase::Ready);
        model.handle_intent(Intent::ToggleRange, Phase::Ready);
        assert_eq!(model.marks.len(), 2, "a range marks both ends and what lies between");
        model.handle_intent(Intent::ClearMarks, Phase::Ready);

        model.handle_intent(Intent::MarkAll, Phase::Ready);
        assert_eq!(model.marks.len(), 3);
        assert!(model.marks.contains(&first) && model.marks.contains(&second) && model.marks.contains(&third));

        model.handle_intent(Intent::SwitchPane, Phase::Ready);
        model.handle_intent(Intent::ClearMarks, Phase::Ready);
        model.marks.insert(first);
        model.focus = Pane::Marked;
        model.handle_intent(Intent::MarkAll, Phase::Ready);
        assert_eq!(model.marks.len(), 1);
        assert!(model.message.as_deref().unwrap().contains("applies to the list"));
        Ok(())
    }

    #[test]
    fn mark_all_captures_only_known_matching_entries_during_streaming() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let alpha = append(&mut model, root, b"alpha", EntryType::RegularFile, 1, 2);
        let beta = append(&mut model, root, b"beta", EntryType::RegularFile, 1, 3);
        model.filter = "alpha".to_owned();
        model.rebuild_visible(true);
        assert!(model.mark_all_visible());
        assert!(model.marks.contains(&alpha));
        assert!(!model.marks.contains(&beta));

        let gamma = append(&mut model, root, b"gamma", EntryType::RegularFile, 1, 4);
        assert!(!model.marks.contains(&gamma));
        model.marks.insert(beta);
        assert_eq!(model.marks.len(), 2);
        assert_eq!(model.visible, vec![alpha]);
        Ok(())
    }

    #[test]
    fn scan_phase_deletes_finished_marks_once_the_root_is_listed_and_rejects_refresh() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root_node = model.tree.root();
        let entry = append(&mut model, root_node, b"file", EntryType::RegularFile, 8, 2);
        let still_scanning = append(&mut model, root_node, b"still-scanning", EntryType::Directory, 0, 3);
        Arc::get_mut(&mut model.tree).unwrap().set_state(still_scanning, NodeStatus::Scanning);
        model.marks.insert(entry);
        model.marks.insert(still_scanning);
        model.handle_intent(Intent::SwitchPane, Phase::Scanning);
        let (action, changed) = model.handle_intent(Intent::Delete, Phase::Scanning);
        assert!(matches!(action, Action::None));
        assert!(changed);
        assert!(model.message.as_deref().unwrap().contains("until the root directory is listed"));
        assert!(model.pending_delete.is_none());

        model.apply_scan_event(ScanEvent::RootListed);
        model.handle_intent(Intent::Delete, Phase::Scanning);
        let plan = model.pending_delete.as_ref().expect("finished marks are planned while scanning");
        assert_eq!(plan.operation_count(), 1);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].node, still_scanning);
        model.handle_intent(Intent::Cancel, Phase::Scanning);

        let (action, _) = model.handle_intent(Intent::Refresh, Phase::Scanning);
        assert!(matches!(action, Action::None));

        let (_, mut read_only) = make_model(&temp, true)?;
        let read_only_root = read_only.tree.root();
        let file = append(&mut read_only, read_only_root, b"file", EntryType::RegularFile, 8, 9);
        ready(&mut read_only);
        assert!(matches!(press_delete_on_marks(&mut read_only, &[file]), Action::None));
        assert!(read_only.pending_delete.is_none());
        assert!(read_only.message.as_deref().unwrap().contains("read only"));
        Ok(())
    }

    #[test]
    fn incomplete_root_still_allows_deleting_complete_entries() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let directory = append(&mut model, root, b"directory", EntryType::Directory, 0, 2);
        let file = append(&mut model, directory, b"complete-file", EntryType::RegularFile, 8, 3);
        model.token_nodes.push(Some(directory));

        assert!(model.apply_scan_event(ScanEvent::DirectoryFailed {
            directory: DirectoryToken(1),
            message: "permission denied".to_owned(),
        }));
        model.finish_scan(None);

        assert_eq!(model.scan_errors, 1);
        assert_eq!(model.incomplete_entries, 2);
        assert_eq!(model.tree.record(root).unwrap().state, NodeState::Incomplete);
        assert_eq!(model.tree.record(directory).unwrap().state, NodeState::Incomplete);
        assert_eq!(model.tree.record(file).unwrap().state, NodeState::Complete);
        assert!(matches!(press_delete_on_marks(&mut model, &[file]), Action::None));
        assert_eq!(model.pending_delete.as_ref().map(fdu_delete::DeletionPlan::operation_count), Some(1));
        Ok(())
    }

    #[test]
    fn a_message_lasts_until_the_next_command() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        append(&mut model, root, b"file", EntryType::RegularFile, 5, 2);
        ready(&mut model);
        model.message = Some("stale".to_owned());

        let (_, changed) = model.handle_intent(Intent::MoveDown, Phase::Ready);
        assert!(changed, "clearing the message needs a redraw");
        assert_eq!(model.message, None);
        Ok(())
    }

    #[test]
    fn delete_summary_lists_only_nonzero_counts() {
        let clean = DeleteSummary { deleted: 3, ..DeleteSummary::default() };
        assert_eq!(clean.describe(0), "Deleted 3");
        let mixed = DeleteSummary { deleted: 3, failed: 2, ..DeleteSummary::default() };
        assert_eq!(mixed.describe(1), "Deleted 3 · 2 failed · 1 skipped · select a ~ row for why");
    }

    #[test]
    fn only_marks_with_unfinished_work_stay_marked_after_a_deletion() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let done = append(&mut model, root, b"done", EntryType::RegularFile, 1, 2);
        let directory = append(&mut model, root, b"dir", EntryType::Directory, 0, 3);
        let stuck = append(&mut model, directory, b"stuck", EntryType::RegularFile, 1, 4);
        let ok = append(&mut model, root, b"ok", EntryType::RegularFile, 1, 5);
        ready(&mut model);
        model.marks.insert(done);
        model.marks.insert(directory);
        model.marks.insert(ok);

        model.apply_delete_outcomes(
            vec![
                DeleteOutcome { node: done, kind: OutcomeKind::Deleted, message: None },
                DeleteOutcome { node: stuck, kind: OutcomeKind::Failed, message: Some("busy".to_owned()) },
                DeleteOutcome { node: directory, kind: OutcomeKind::Failed, message: Some("busy".to_owned()) },
                DeleteOutcome { node: ok, kind: OutcomeKind::Deleted, message: None },
            ],
            None,
            0,
        );
        assert_eq!(model.marks.order, vec![directory]);
        Ok(())
    }

    #[test]
    fn a_directory_is_deletable_during_the_scan_only_after_its_finish_arrives() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let mut batch = EntryBatch::with_capacity(1, 8);
        batch.names.extend_from_slice(b"build");
        batch.entries.push(ScanEntry {
            parent: DirectoryToken(0),
            name: 0..5,
            directory_token: Some(DirectoryToken(1)),
            entry_type: EntryType::Directory,
            identity: FileIdentity { device: 1, inode: 2 },
            modified_time: ModifiedTime::from_unix_seconds(1_700_000_000).unwrap(),
            link_count: 2,
            apparent_bytes: 0,
            allocated_bytes: 0,
            state: NodeStatus::Complete,
        });
        model.apply_entry_batch(batch);
        model.apply_scan_event(ScanEvent::RootListed);
        let build = model.token_nodes[1].expect("directory token is registered");
        model.marks.insert(build);
        model.focus = Pane::Marked;

        model.handle_intent(Intent::Delete, Phase::Scanning);
        assert!(model.pending_delete.is_none(), "an unfinished directory is not planned");
        assert!(model.message.as_deref().unwrap().contains("scan is still in progress"), "{:?}", model.message);

        model.apply_scan_event(ScanEvent::DirectoryFinished { directory: DirectoryToken(1), complete: true });
        model.handle_intent(Intent::Delete, Phase::Scanning);
        let plan = model.pending_delete.as_ref().expect("a finished directory is planned while scanning");
        assert_eq!(plan.operation_count(), 1);
        Ok(())
    }

    #[test]
    fn a_failed_directory_explains_itself_and_its_ancestors() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let outer = append(&mut model, root, b"outer", EntryType::Directory, 0, 2);
        let inner = append(&mut model, outer, b"inner", EntryType::Directory, 0, 3);
        model.token_nodes.push(Some(outer));
        model.token_nodes.push(Some(inner));

        model.apply_scan_event(ScanEvent::DirectoryFailed {
            directory: DirectoryToken(2),
            message: "permission denied".to_owned(),
        });
        assert_eq!(model.scan_errors, 1);
        assert_eq!(model.message, None);
        assert_eq!(model.status_by_node.get(&inner).map(String::as_str), Some("permission denied"));
        assert_eq!(model.status_by_node.get(&outer).map(String::as_str), Some("Contains entries with scan errors."));
        Ok(())
    }

    #[test]
    fn delete_outcomes_tombstone_postorder_once_and_adjust_entry_count() -> io::Result<()> {
        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        let root = model.tree.root();
        let directory = append(&mut model, root, b"dir", EntryType::Directory, 0, 2);
        let file = append(&mut model, directory, b"file", EntryType::RegularFile, 12, 3);
        ready(&mut model);
        assert_eq!(model.indexed_entries, 2);

        model.apply_delete_outcomes(
            vec![
                DeleteOutcome { node: file, kind: OutcomeKind::Deleted, message: None },
                DeleteOutcome { node: directory, kind: OutcomeKind::Deleted, message: None },
            ],
            None,
            0,
        );
        assert_eq!(model.indexed_entries, 0);
        assert_eq!(model.actual_deletions, 2);
        assert_eq!(model.tree.record(root).unwrap().apparent_bytes, 0);
        assert_eq!(model.tree.record(directory).unwrap().state, NodeState::Tombstone);
        assert!(model.visible.is_empty());
        Ok(())
    }

    #[test]
    fn entry_batch_fold_matches_rmrf_states_names_times_and_totals() -> io::Result<()> {
        type Snapshot = Vec<(Vec<u8>, NodeState, u64, u64, Option<i64>)>;

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

        let temp = TempDir::new()?;
        let (_, mut model) = make_model(&temp, false)?;
        model.apply_entry_batch(batch());
        assert_eq!(model.message, None, "the batch folds without scanner defects");

        // `rmrf::apply_batch` is this same fold plus error mapping, so folding
        // the same batch into a bare tree is the `--rmrf` side of the parity check.
        let mut plain = Tree::new(b"root", FileIdentity { device: 1, inode: 1 }, 1).unwrap();
        assert!(plain.set_modified_time(
            plain.root(),
            model.tree.record(model.tree.root()).expect("model has a root").modified_time()
        ));
        let mut plain_tokens = vec![Some(plain.root())];
        let outcome = fdu_core::fold_entry_batch(&mut plain, &mut plain_tokens, &batch());
        assert!(!outcome.unknown_directory && !outcome.invalid_range && !outcome.exhausted && !outcome.overflow);
        assert_eq!(snapshot(&model.tree), snapshot(&plain));

        let root = model.tree.root();
        assert_eq!(model.tree.record(root).unwrap().state, NodeState::Incomplete);
        assert_eq!(model.tree.record(root).unwrap().apparent_bytes, 100);
        assert_eq!(model.tree.record(root).unwrap().allocated_bytes, 512);
        let kept = model.tree.children(root).nth(1).expect("file follows the directory");
        assert_eq!(model.tree.name(kept), Some(b"kept.bin".as_slice()));
        assert_eq!(model.tree.record(kept).unwrap().modified_time().unix_seconds(), Some(1_700_000_002));
        // Browser-only listing updates stay outside the shared fold.
        assert_eq!(model.listing.len(), 3);
        assert_eq!(model.visible.len(), 3);

        model.finish_directory(DirectoryToken(1), true);
        fdu_core::finish_scan_directory(&mut plain, &plain_tokens, DirectoryToken(1), true);
        assert_eq!(snapshot(&model.tree), snapshot(&plain));
        Ok(())
    }
}
