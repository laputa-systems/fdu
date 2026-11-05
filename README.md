# fdu

`fdu` scans disk usage and lets you browse a directory in a terminal. It
supports Linux 6.0 or later and macOS 26 or later; published macOS builds are
for Apple silicon.

## Install

Download the archive for your platform from [GitHub Releases](https://github.com/laputa-systems/fdu/releases):

- macOS arm64: `fdu-aarch64-apple-darwin.zip`
- Linux x86_64: `fdu-x86_64-unknown-linux-musl-static.zip`
- Linux arm64: `fdu-aarch64-unknown-linux-musl-static.zip`

The Linux archives are static and do not need a musl dynamic loader. Extract
the archive, then put its executable on your `PATH`; for example:

```sh
mkdir -p ~/.local/bin
unzip fdu-aarch64-apple-darwin.zip
install -m 755 fdu-aarch64-apple-darwin ~/.local/bin/fdu
export PATH="$HOME/.local/bin:$PATH"
```

Use the matching executable name from the archive you downloaded.
The export applies to the current shell; add it to your shell startup file to
keep `~/.local/bin` on `PATH` in new sessions.

## Run

```text
fdu [--apparent] [-x PATH | --exclude PATH]... [--summary | --interactive] [--read-only] [PATH]
fdu --rmrf [--yes] PATH
```

With no path, fdu scans the current directory. In a terminal it opens the
browser; when stdin or stdout is redirected it prints a summary. Use
`--summary` to select summary output explicitly. `--interactive` requires
terminal input and output.

Use `-x PATH` or `--exclude PATH` to omit an exact path beneath the selected
root, including its subtree. Exclusion paths are relative to the selected root;
repeat the option to omit more paths. Path components must match exactly.
In the browser, directories whose contents include excluded paths are marked
incomplete. Browser deletion skips incomplete and excluded entries and deletes
the rest. Deletion does not wait for the whole scan: once the root directory is
listed, a marked entry whose scan has finished can be deleted while the rest of
the tree is still scanning. The scan pauses during deletion and then resumes.
On macOS, a directory that has not opened after two seconds, usually because
macOS is waiting for you to answer a privacy prompt for it, shows `? no access`
and is counted as awaiting access in the status line. The scan keeps waiting
and continues with that directory once access is granted.

By default, sizes are filesystem-allocated bytes; `--apparent` uses logical
file lengths. Use `--read-only` to disable deletion in the browser. The summary
prints each included immediate child directory in descending size order as a
compact size (such as `5G` or `2.4M`), a tab, and a quoted name. Sizes use
1024-based units. Root-level files are not listed. An immediate child that is a
mount boundary is labeled `MOUNT BOUNDARY` on both platforms, and a warning
reports excluded boundaries. On macOS, a directory that does not open within
two seconds, usually because macOS is waiting for you to answer a privacy
prompt for it, is skipped: an immediate child is labeled
`SKIPPED: OPEN TIMED OUT`, and a warning names the shallowest skipped
directories and counts the rest. Grant your terminal Full Disk Access to scan
them. Hidden names are included, and fdu does not read `.gitignore`.

The browser shows each file or directory's modification age in one rounded
unit, such as `5m`, `1h`, `4w`, or `20mo`.

The supplied root path may resolve through symlinks; the browser shows
discovered symlinks beneath it as entries and never follows them.
Nested filesystem or mount boundaries are skipped and reported. Results
describe a live scan, not an atomic snapshot.

### Browser keys

- Move with arrows or `j`/`k`; open with Enter, right, or `l`; go up with
  left, Backspace, or `h`. Page Up/Down and Home/End move through a listing.
- Press `d` to mark an entry, `v` to start or end a range, `*` to mark
  matching entries, and `c` to clear marks. Marking a directory replaces any
  marks inside it.
- Press `s` to sort by name or size, `a` to switch size modes, `r` to
  rescan, and `/` to filter names in the current directory.
- Marked entries appear in a second pane. Tab changes panes; `-` changes
  between side-by-side and stacked layout. In the marked pane, `d` removes a
  mark and Enter shows it in the listing.
- To delete marks, focus the marked pane and press Ctrl-R. Review the prompt
  and press Enter to delete permanently or Esc to cancel. There is no Trash.
  Entries with scan errors or exclusions are skipped and counted in the prompt;
  their directories stay in place.
- Press `?` for in-app help, or `q` or Ctrl-C to quit.

### Empty a directory

`--rmrf` permanently deletes every entry below the explicit path and leaves
the directory itself in place. Exclusions cannot be combined with `--rmrf`. It
is unavailable in summary-only builds.

- A symlink as the final path component is refused, including when the path
  has trailing slashes. Earlier path components may resolve through symlinks.
  The filesystem root, home directory, and any parent of the current directory
  are refused.
- Symlinks below the path are unlinked, never followed.
- On a terminal, fdu shows the resolved path and entry count and requires the
  exact answer `yes`. `--yes` skips the prompt and is required when stdin is
  not a terminal.
- An incomplete root scan, incomplete selected directory, or nested mount
  rejects the deletion plan before any entry is removed.
- Only names found by the scan are planned. A child created under a different
  name after scanning is left in place. A replacement of a scanned entry is
  left intact or restored without overwriting concurrent changes.

Both deletion modes require write access to the scan root for a private
`.fdu-quarantine-v1-*` directory. Other processes must leave the quarantine
directory, its contents, and recovery records untouched, including through open
directory descriptors.
After an interruption, a confirmed deletion request restores retained captures
and stops for a rescan. Conflicts retain both entries and report the capture
location. See [deletion recovery](docs/deletion.md#interruption-and-recovery).

### Environment

- `LS_COLORS` controls browser colors.
- On Linux, `RAYON_NUM_THREADS` sets the summary scanner's worker count
  (default: 4).

Run `fdu --help` for command-line help. Report problems in
[GitHub Issues](https://github.com/laputa-systems/fdu/issues). fdu is
distributed under the [MIT License](LICENSE).
