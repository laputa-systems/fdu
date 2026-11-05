# Scanning

fdu has a summary scan and an indexed scan. The summary reports one row per
immediate child directory. The indexed scan builds the tree used by the browser
and deletion planner.

## Accounting and traversal

- Allocated bytes are the default; apparent mode uses logical file lengths.
- Directory inode storage is excluded. Hardlinked paths count separately.
- Hidden and ignored names are included. fdu does not apply ignore-file rules.
- `-x PATH` / `--exclude PATH` omits an exact path relative to the selected root
  and its subtree. Repeat the option for multiple paths; matching is by path
  component, not by substring. These exclusions apply to summary and browser
  scans, and are not accepted with `--rmrf`. Browser directories that contain
  omitted paths are marked incomplete, so browser deletion keeps them and
  removes the entries around the omitted paths.
- Discovered symlinks are leaves. Files and directories beyond a filesystem or
  mount boundary are not traversed.
- Errors and unsupported directory aliases make affected totals incomplete.
  Root open or enumeration errors stop the scan.
- The tree is live rather than an atomic snapshot, so concurrent filesystem or
  mount changes can be observed during a scan.

Summary output orders immediate child directories by descending size. On Linux
the summary scanner uses four Rayon workers by default; `RAYON_NUM_THREADS`
sets another count. The macOS summary scanner is sequential.

An immediate child that is itself a mount boundary is labeled `MOUNT BOUNDARY`
on both platforms. The warning reports excluded boundaries. Contents below
mount boundaries are omitted from totals.

## Indexed scan

The shared indexed scanner uses worker-local directory queues and work stealing.
It batches entries and completion events over the browser's bounded channel.
Platform code supplies enumeration, metadata, mount identity, and pool limits.
Linux sizes a shared descriptor retention budget from its file descriptor limit,
reserving capacity for temporary opens and other process descriptors. The budget
covers active workers, queues, and unpublished batches. A worker that cannot
retain a descriptor records the
directory's names and identities from the root and reopens it later without
waiting for a permit. Reopening uses at most two temporary descriptors per
worker and checks every ancestor's identity and mount without following
symlinks. A renamed or replaced ancestor can therefore make a deferred directory
incomplete, even when its original directory remains accessible elsewhere.

When enumeration or `lstat` already identifies a child directory, as macOS bulk
attributes do, the child is opened by its own task rather than while its parent
is read. That task checks the child's identity and mount. Opening a
privacy-protected macOS folder such as `~/Documents` can block until the user
answers the consent prompt; only that subtree waits, and the parent's listing
is published without it. If work stays queued while no directory starts for
half a second, the extra workers start. Quitting the browser does not wait for
a blocked scan.

See [`indexed.rs`](../crates/fdu-scan/src/indexed.rs),
[`linux.rs`](../crates/fdu-scan/src/linux.rs), and
[`macos.rs`](../crates/fdu-scan/src/macos.rs).

Linux requires kernel 6.0 or later. Its indexed scan compares device and kernel
mount IDs; the summary scanner uses `openat2` with `RESOLVE_NO_XDEV` where
available and a descriptor mount-ID fallback. macOS compares device,
filesystem ID, and mount point from opened directory descriptors. These checks
cannot make traversal atomic with concurrent mount or path changes.
