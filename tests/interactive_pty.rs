//! Terminal-level tests of the interactive browser.
//!
//! Each test runs the real binary on a kernel PTY and asserts on the semantic
//! screen kept by `ptytest`, so layout and lifecycle behavior is checked as a
//! user would see it. They run on both supported platforms; a pass on one is
//! not evidence for the other.
#![cfg(feature = "interactive")]

mod support;

use ptytest::{ExitStatus, Key, ScreenSnapshot};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::{list_is_dimmed, row_containing, ScreenText, Session, TempDir, STEP};

#[test]
fn startup_shows_a_minimal_title_and_the_entry_count_and_restores_the_terminal() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("startup-ready", &root, &[], 100, 24);
    let screen = session.wait_ready();
    let title = screen.lines().remove(0);
    assert_eq!(title.split_whitespace().collect::<Vec<_>>(), ["fdu"]);
    let status = screen.lines().pop().unwrap();
    for noise in ["Ready", "allocated", "sort", "Scanning"] {
        assert!(!title.contains(noise), "{noise} is not in the title");
    }
    for noise in ["Ready", "allocated", "Scanning", "incomplete", "excluded"] {
        assert!(!status.contains(noise), "{noise} is not in the status bar");
    }
    for name in ["alpha/", "beta/", "top.txt"] {
        assert!(screen.shows(name), "{name} is listed");
    }
    // The pane title names the root and shows the visible count and recursive total.
    assert!(screen.shows("root (3 shown, 6 total,"));
    // The bottom bar leads with the total entry count (3 at the top plus 3
    // below) and follows it with the key hints.
    let status = screen.lines().pop().unwrap();
    assert!(status.trim().starts_with("6 entries · d mark"), "{status}");
    assert!(status.contains("? help"), "{status}");
    // No marks yet: only the list is drawn.
    assert!(!screen.shows("Marked"));
    session.quit();
    Ok(())
}

#[test]
fn repeated_exclusions_hide_exact_root_relative_paths_in_the_browser() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("exclude-paths", &root, &["-x", "alpha", "--exclude", "beta"], 100, 24);
    let screen = session.wait_ready();
    assert!(screen.shows("top.txt"));
    assert!(!screen.shows("alpha/"));
    assert!(!screen.shows("beta/"));
    assert!(screen.shows("root (1 shown, 1 total,"));
    session.quit();
    Ok(())
}

#[test]
fn directory_with_excluded_contents_is_deleted_around_the_excluded_entry() -> io::Result<()> {
    let temp = TempDir::new()?;
    let root = temp.0.join("root");
    fs::create_dir_all(root.join("alpha/nested"))?;
    fs::write(root.join("alpha/nested/hidden.txt"), b"excluded")?;
    fs::write(root.join("alpha/visible.txt"), b"included")?;

    let mut session = Session::start("exclude-delete-safety", &root, &["--exclude", "alpha/nested"], 100, 24);
    session.ready_by_name();
    session.text("d");
    session.wait_for("Marked 1 item");
    session.key(Key::Tab);
    session.key(Key::Ctrl('r'));
    session.wait_for("Permanently delete");
    session.key(Key::Enter);
    session.wait_for("Deleted 1");
    session.quit();

    assert!(root.join("alpha/nested/hidden.txt").exists());
    assert!(!root.join("alpha/visible.txt").exists());
    Ok(())
}

#[test]
fn marking_reveals_a_second_pane_on_a_typical_terminal() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    // 80 columns is the classic terminal size; both panes must fit.
    let mut session = Session::start("two-panes-80", &root, &[], 80, 24);
    session.ready_by_name();
    session.text("d");
    let screen = session.wait_for("Marked 1 item");
    assert!(screen.shows("alpha/"), "the list stays visible beside the marked pane");
    assert!(screen.shows("[x]"));
    session.quit();
    Ok(())
}

#[test]
fn tab_moves_focus_between_the_list_and_the_marked_pane() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("pane-focus", &root, &[], 100, 24);
    session.ready_by_name();
    let screen = session.wait_ready();
    assert!(!screen.shows("Marked"), "the pane stays closed until something is marked");
    session.text("d");
    let screen = session.wait_for("Marked 1 item");
    assert!(screen.shows("[x]"));
    assert!(!list_is_dimmed(&screen), "the list has focus after marking");

    session.key(Key::Tab);
    session.wait("marked focus", list_is_dimmed);

    // d in the marked pane removes the mark and closes the pane.
    session.text("d");
    let screen = session.wait_until_absent("Marked 1 item");
    assert!(!screen.shows("[x]"));
    assert!(!list_is_dimmed(&screen));
    session.quit();
    Ok(())
}

#[test]
fn minus_toggles_between_side_by_side_and_stacked_panes() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("split-toggle", &root, &[], 100, 24);
    session.ready_by_name();
    session.text("d");
    let screen = session.wait_for("Marked 1 item");
    let list_row = screen.lines().iter().position(|line| line.contains("root (")).unwrap();
    let marked_row = screen.lines().iter().position(|line| line.contains("Marked 1 item")).unwrap();
    assert_eq!(list_row, marked_row, "side by side by default");

    session.text("-");
    let screen = session.wait("stacked", |screen| {
        let lines = screen.lines();
        match (lines.iter().position(|line| line.contains("root (")), lines.iter().position(|line| line.contains("Marked 1 item"))) {
            (Some(list), Some(marked)) => list < marked,
            _ => false,
        }
    });
    assert!(screen.shows("alpha/"), "the list stays readable above the marks");

    session.text("-");
    session.wait("side by side again", |screen| {
        let lines = screen.lines();
        lines.iter().position(|line| line.contains("root (")) == lines.iter().position(|line| line.contains("Marked 1 item"))
    });
    session.quit();
    Ok(())
}

#[test]
fn space_no_longer_marks() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("space-inert", &root, &[], 100, 24);
    session.ready_by_name();
    session.text(" ");
    session.text("d");
    let screen = session.wait_for("Marked 1 item");
    assert!(!screen.shows("Marked 2 items"));
    session.quit();
    Ok(())
}

#[test]
fn s_cycles_between_size_name_and_date_order() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("sort-toggle", &root, &[], 100, 24);
    session.wait_ready();
    // beta/ holds the most data, so size order lists it first.
    session.wait("size order", |screen| screen.lines()[2].contains("beta/"));
    session.text("s");
    session.wait("name order", |screen| screen.lines()[2].contains("alpha/"));
    session.text("s");
    session.wait_for("Sorted by date");
    session.text("s");
    session.wait("size order again", |screen| screen.lines()[2].contains("beta/"));
    session.quit();
    Ok(())
}

#[test]
fn marks_collect_across_directories_and_show_root_relative_paths() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("marks-across-directories", &root, &[], 100, 24);
    session.ready_by_name();
    session.key(Key::Enter); // into alpha
    session.wait_for("inner.txt");
    session.key(Key::Home);
    session.text("d"); // inner.txt
    session.key(Key::Left); // back to the root
    session.wait_for("top.txt");
    session.key(Key::End);
    session.text("d"); // top.txt

    let screen = session.wait_for("Marked 2 items");
    assert!(screen.shows("alpha/inner.txt"));
    assert!(screen.shows("top.txt"));
    // Navigation stops at the opened root: going up again changes nothing.
    session.key(Key::Left);
    assert!(session.wait_for("Marked 2 items").shows("alpha/"));
    session.quit();
    Ok(())
}

#[test]
fn marked_pane_shows_marks_hidden_by_a_filter_and_reveals_them() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("filter-hides-marks", &root, &[], 100, 24);
    session.ready_by_name();
    session.key(Key::End);
    session.text("d"); // top.txt
    session.wait_for("Marked 1 item");
    session.text("/");
    session.text("alpha");
    session.key(Key::Enter);
    let screen = session.wait("filtered list", |screen| screen.shows("(1 shown, 6 total"));
    assert!(!screen.shows("beta/"), "the filter hides non-matching rows");
    assert!(screen.shows("Marked 1 item"), "the mark is still counted");
    assert!(screen.shows("filter \"alpha\""));

    session.key(Key::Tab);
    session.key(Key::Enter); // show top.txt where it lives
    session.wait("filter cleared", |screen| screen.shows("(3 shown, 6 total") && screen.shows("Filter cleared"));
    session.quit();
    Ok(())
}

#[test]
fn marking_a_directory_covers_its_contents_and_overlaps_are_refused() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("overlap", &root, &[], 100, 24);
    session.ready_by_name();
    session.text("d"); // alpha/
    session.wait_for("Marked 1 item");
    session.key(Key::Home);
    session.key(Key::Enter);
    let screen = session.wait_for("Inside marked directory alpha");
    assert!(screen.shows("[=]"), "covered rows are cued without color");

    session.text("d");
    let screen = session.wait_for("Already covered by marked directory alpha");
    assert!(screen.shows("Marked 1 item"), "the refused mark is not added");

    // The reverse: marking an ancestor replaces its marked descendants.
    session.key(Key::Tab);
    session.text("d");
    session.wait_until_absent("Marked 1 item");
    session.key(Key::Home);
    session.text("d"); // inner.txt, marked directly now that alpha is unmarked
    session.wait_for("Marked 1 item");
    session.key(Key::Left);
    session.key(Key::Home);
    session.text("d");
    session.wait_until_absent("alpha/inner.txt");
    session.quit();
    Ok(())
}

#[test]
fn resize_keeps_focus_marks_and_a_readable_listing() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("resize", &root, &[], 100, 24);
    session.ready_by_name();
    session.text("d");
    session.key(Key::Tab);
    session.wait("marked focus", list_is_dimmed);

    // Narrow: the list gives way instead of being crushed beside the pane.
    session.resize(50, 24);
    session.wait("single pane", |screen| screen.shows("Marked 1 item") && !screen.shows("root ("));

    session.resize(140, 30);
    let screen = session.wait("two panes", |screen| screen.shows("root (") && screen.shows("Marked 1 item"));
    assert!(list_is_dimmed(&screen), "focus survives resizing");

    // The browser pane is 55% of the width, so at 60 columns its right edge
    // sits at column 32. Waiting on that edge proves the app repainted.
    session.resize(60, 24);
    session.wait("boundary width", |screen| {
        screen.lines()[1].chars().nth(32) == Some('┐') && screen.shows("Marked 1 item")
    });
    session.resize(59, 24);
    session.wait("below the split", |screen| screen.lines()[1].chars().nth(58) == Some('┐') && !screen.shows("root ("));
    session.quit();
    Ok(())
}

#[test]
fn narrow_terminal_shows_one_pane_at_a_time_and_tab_swaps_them() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("narrow-swap", &root, &[], 50, 24);
    session.ready_by_name();
    session.text("d");
    // Marking does not take the list away on a narrow terminal.
    let screen = session.wait("mark in list", |screen| screen.shows("[x]"));
    assert!(!screen.shows("Marked 1 item"));
    session.key(Key::Tab);
    let screen = session.wait_for("Marked 1 item");
    assert!(!screen.shows("beta/"), "only one pane fits at 50 columns");
    session.key(Key::Tab);
    let screen = session.wait_for("beta/");
    assert!(!screen.shows("Marked 1 item"));
    session.quit();
    Ok(())
}

#[test]
fn long_wide_combining_and_control_names_stay_inside_their_panes() -> io::Result<()> {
    let temp = TempDir::new()?;
    let root = temp.0.join("root");
    fs::create_dir(&root)?;
    fs::write(root.join("日本語のとても長いファイル名-with-a-long-ascii-tail-0123456789.txt"), b"x")?;
    fs::write(root.join("cafe\u{301}-combining.txt"), b"x")?;
    fs::write(root.join("x\ny\u{1b}[31m.txt"), b"x")?;
    let mut session = Session::start("odd-names", &root, &[], 100, 16);
    session.ready_by_name();
    session.key(Key::Down);
    session.text("d");
    let screen = session.wait_for("Marked 1 item");
    assert!(screen.shows("x\\u{a}y\\u{1b}[31m.txt"), "controls are escaped, not interpreted");
    // Every pane row keeps its right border in the final column.
    for row in 2..=12 {
        let text = screen.lines().swap_remove(row);
        assert!(text.trim_end().ends_with('┃') || text.trim_end().ends_with('│') || text.trim_end().ends_with('┓') || text.trim_end().ends_with('┐'), "row {row} keeps its border: {text:?}");
    }
    session.quit();
    Ok(())
}

#[test]
fn unicode_filter_input_and_backspace_preserve_complete_characters() -> io::Result<()> {
    let temp = TempDir::new()?;
    let root = temp.0.join("root");
    fs::create_dir(&root)?;
    fs::write(root.join("日本-é.txt"), b"x")?;
    fs::write(root.join("other.txt"), b"x")?;
    let mut session = Session::start("unicode-filter", &root, &[], 100, 16);
    session.wait_ready();
    session.text("/");
    session.text("日本é");
    session.wait_for("/ 日本é");
    session.key(Key::Backspace);
    session.wait("unicode backspace", |screen| screen.shows("/ 日本") && !screen.shows("/ 日本é"));
    session.key(Key::Enter);
    let screen = session.wait_for("(1 shown, 2 total");
    assert!(screen.shows("日本-é.txt"));
    assert!(!screen.shows("other.txt"));
    session.quit();
    Ok(())
}

#[test]
fn tiny_terminals_and_empty_directories_stay_usable() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("tiny", &root, &[], 100, 24);
    session.wait_ready();
    // Keys sent while a resize is still being processed can be lost, so each
    // step waits for the app to repaint before the next input.
    session.resize(10, 3);
    session.wait("tiny layout", |screen| screen.lines()[0].starts_with('┌'));
    session.text("?");
    session.wait_for("Help");
    session.key(Key::Escape);
    session.wait("help closed", |screen| !screen.shows("Help"));
    session.text("d");
    session.key(Key::Tab);
    session.wait_for("Marke");
    session.key(Key::Tab);
    session.wait("pane closed", |screen| !screen.shows("Marke"));
    session.resize(60, 12);
    session.wait_ready();
    session.quit();
    Ok(())
}

#[test]
fn help_describes_the_panes_and_closes() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("help", &root, &[], 100, 24);
    session.wait_ready();
    session.text("?");
    session.wait("help", |screen| screen.shows("Tab switches") && screen.shows("there is no Trash"));
    session.key(Key::Escape);
    session.wait_until_absent("Tab switches");
    session.quit();
    Ok(())
}

#[test]
fn read_only_session_rejects_delete_and_restores_terminal_state() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("read-only", &root, &["--read-only"], 100, 24);
    let screen = session.wait_ready();
    assert!(screen.shows("read only"));
    session.text("d");
    session.wait_for("Marked 1 item");
    session.key(Key::Tab);
    session.wait("marked focus", list_is_dimmed);
    session.key(Key::Ctrl('r'));
    let screen = session.wait_for("This session is read only; deletion is disabled.");
    assert!(!screen.shows("Permanently delete"));
    session.quit();
    assert!(root.join("alpha/inner.txt").exists(), "read-only input must not remove fixture entries");
    assert!(root.join("top.txt").exists());
    Ok(())
}

#[test]
fn ineligible_marks_are_skipped_while_the_rest_is_deleted() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let (_temp, root) = TempDir::standard_tree()?;
    let locked = root.join("beta");
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000))?;
    if fs::read_dir(&locked).is_ok() {
        eprintln!("skipping: directory permissions are not enforced for this user");
        return Ok(());
    }
    let mut session = Session::start("ineligible", &root, &[], 100, 24);
    session.ready_by_name();
    session.key(Key::Down); // beta/
    session.text("d");
    session.key(Key::End);
    session.text("d"); // top.txt
    session.wait_for("Marked 2 items");
    session.key(Key::Tab);
    session.key(Key::Ctrl('r'));
    let screen = session.wait_for("Permanently delete");
    assert!(screen.shows("1 skipped"));
    session.key(Key::Enter);
    session.wait_for("Deleted 1");
    session.quit();
    assert!(!root.join("top.txt").exists());
    assert!(root.join("beta").exists());
    Ok(())
}

#[test]
fn cancelling_confirmation_leaves_everything_in_place_and_ctrl_c_restores_the_terminal() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("cancel-confirmation", &root, &[], 100, 24);
    session.ready_by_name();
    session.key(Key::End);
    session.text("d"); // top.txt
    session.key(Key::Home);
    session.text("d"); // alpha/
    session.wait_for("Marked 2 items");
    session.key(Key::Tab);
    session.key(Key::Ctrl('r'));
    let screen = session.wait_for("Permanently delete 2 items");
    assert!(screen.shows("Enter confirms · Esc cancels"));
    assert!(screen.shows("alpha/"), "the marked pane is the review");
    assert!(screen.shows("top.txt"));
    // While the question is open other keys do nothing.
    session.text("d");
    session.text("q");
    session.key(Key::Escape);
    let screen = session.wait_until_absent("Permanently delete");
    assert!(screen.shows("Marked 2 items"));

    session.resize(90, 24);
    session.wait("resized", |screen| screen.shows("Marked 2 items") && screen.lines()[1].chars().nth(49) == Some('┐'));
    session.key(Key::Ctrl('c'));
    let deadline = session.terminal.deadline(STEP);
    let status = session.terminal.wait_for_exit(deadline).unwrap();
    assert_eq!(status, ExitStatus::Code(0));
    session.terminal.assert_terminal_restored(&session.baseline).unwrap();
    assert!(root.join("alpha/inner.txt").exists(), "cancelling confirmation must leave the fixture unchanged");
    assert!(root.join("top.txt").exists());
    Ok(())
}

#[test]
fn sigterm_restores_terminal_state() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("sigterm", &root, &[], 100, 24);
    session.wait_ready();
    session.terminal.signal(15).unwrap();
    let deadline = session.terminal.deadline(STEP);
    let status = session.terminal.wait_for_exit(deadline).unwrap();
    session.terminal.assert_terminal_restored(&session.baseline).unwrap();
    assert_eq!(status, ExitStatus::Signal(15));
    let deadline = session.terminal.deadline(STEP);
    session.terminal.finish(deadline).unwrap();
    Ok(())
}

#[test]
fn sighup_restores_terminal_state() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("sighup", &root, &[], 100, 24);
    session.wait_ready();
    session.terminal.signal(1).unwrap();
    let deadline = session.terminal.deadline(STEP);
    let status = session.terminal.wait_for_exit(deadline).unwrap();
    session.terminal.assert_terminal_restored(&session.baseline).unwrap();
    assert_eq!(status, ExitStatus::Signal(1));
    let deadline = session.terminal.deadline(STEP);
    session.terminal.finish(deadline).unwrap();
    Ok(())
}

/// Opens the marked pane, asks to delete, and confirms.
fn confirm_delete(session: &mut Session, summary_prefix: &str) {
    session.key(Key::Tab);
    session.key(Key::Ctrl('r'));
    session.wait_for("Permanently delete");
    session.key(Key::Enter);
    let _ = summary_prefix;
}

#[test]
fn confirmed_deletion_removes_marks_from_several_directories_and_reports_the_result() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let mut session = Session::start("delete-several", &root, &[], 100, 24);
    session.ready_by_name();
    session.key(Key::Enter); // into alpha
    session.wait_for("inner.txt");
    session.key(Key::Home);
    session.text("d"); // alpha/inner.txt
    session.key(Key::Left);
    session.wait_for("top.txt");
    session.key(Key::End);
    session.text("d"); // top.txt
    session.wait_for("Marked 2 items");
    confirm_delete(&mut session, "Deleted");
    let screen = session.wait_for("Deleted 2");
    assert!(!screen.shows("failed") && !screen.shows("not attempted"), "zero counters are omitted");
    assert!(!screen.shows("top.txt"));
    assert!(!screen.shows("Marked"), "the basket is empty after deletion");
    assert!(!root.join("alpha/inner.txt").exists());
    assert!(!root.join("top.txt").exists());
    assert!(root.join("alpha/other.txt").exists(), "unmarked siblings stay");
    assert!(root.join("beta/deep.txt").exists());
    session.quit();
    Ok(())
}

fn populate_bulk(root: &Path, directories: usize, files_per_directory: usize) -> io::Result<usize> {
    let bulk = root.join("bulk");
    let mut operations = 1; // the bulk directory itself
    for directory in 0..directories {
        let path = bulk.join(format!("d{directory:02}"));
        fs::create_dir_all(&path)?;
        operations += 1;
        for file in 0..files_per_directory {
            fs::write(path.join(format!("f{file:05}")), b"")?;
            operations += 1;
        }
    }
    Ok(operations)
}

fn count_entries(path: &Path) -> usize {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => {
            1 + fs::read_dir(path)
                .map(|entries| entries.flatten().map(|entry| count_entries(&entry.path())).sum())
                .unwrap_or(0)
        }
        Ok(_) => 1,
        Err(_) => 0,
    }
}

/// The number after `label` on the screen, e.g. `Deleted 12` or `3 not attempted`.
fn count_after(screen: &ScreenSnapshot, label: &str) -> usize {
    let row = row_containing(screen, label);
    row.split(label)
        .nth(1)
        .unwrap()
        .trim_start()
        .split(|character: char| !character.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap_or_else(|_| panic!("no count after {label:?} in {row:?}"))
}

#[test]
fn deletion_locks_the_interface_until_it_finishes() -> io::Result<()> {
    const CRASH_SAFE_DELETION_STEP: Duration = Duration::from_secs(300);
    let (_temp, root) = TempDir::standard_tree()?;
    let operations = populate_bulk(&root, 8, 2500)?;
    let mut session = Session::start("deletion-lock", &root, &[], 100, 24);
    session.step = CRASH_SAFE_DELETION_STEP;
    session.ready_by_name();
    session.text("d"); // alpha/, then the cursor moves down
    session.text("d"); // beta/
    session.text("d"); // bulk/
    session.wait_for("Marked 3 items");
    confirm_delete(&mut session, "Deleted");
    let screen = session.wait("progress", |screen| screen.shows("Deleting ") && screen.shows(" of ") && screen.shows("removed entries cannot be restored"));
    assert!(!screen.shows("┌Deleting"), "progress is not a modal");
    assert!(screen.shows("Marked 3 items"), "the marked pane stays on screen");

    // Commands that would change panes, marks, order, or the process are ignored.
    for text in ["-", "c", "s", "/", "?", "r", "d", "q", " ", "*", "v", "a"] {
        session.text(text);
    }
    session.key(Key::Ctrl('r'));
    session.key(Key::Tab);
    session.key(Key::Down);
    session.key(Key::Enter);

    let screen = session.wait("summary", |screen| screen.shows("Deleted "));
    assert!(!screen.shows("not attempted"), "only Esc may stop deletion");
    assert!(!screen.shows("failed"));
    // alpha is a directory and two files, beta a directory and one file.
    assert_eq!(count_after(&screen, "Deleted "), operations + 3 + 2);
    assert!(!root.join("bulk").exists());
    assert!(!root.join("alpha").exists());
    assert!(!root.join("beta").exists());
    session.quit();
    Ok(())
}

#[test]
fn escape_stops_later_removals_without_undoing_completed_ones() -> io::Result<()> {
    let (_temp, root) = TempDir::standard_tree()?;
    let operations = populate_bulk(&root, 20, 3000)?;
    let before = count_entries(&root.join("bulk"));
    assert_eq!(before, operations);
    let mut session = Session::start("deletion-stop", &root, &[], 100, 24);
    session.ready_by_name();
    session.key(Key::Down);
    session.key(Key::Down);
    session.text("d"); // bulk/
    session.wait_for("Marked 1 item");
    confirm_delete(&mut session, "Deleted");
    session.wait_for("Deleting ");
    session.key(Key::Escape);

    let screen = session.wait("summary", |screen| screen.shows("Deleted "));
    let deleted = count_after(&screen, "Deleted ");
    let not_attempted = if screen.shows(" not attempted") {
        let row = row_containing(&screen, " not attempted");
        row.split(" not attempted").next().unwrap().rsplit(' ').next().unwrap().parse().unwrap()
    } else {
        0
    };
    assert_eq!(deleted + not_attempted, operations, "every planned operation is accounted for");
    assert_eq!(
        count_entries(&root.join("bulk")),
        not_attempted,
        "exactly the entries that were not attempted remain on disk"
    );
    session.quit();
    Ok(())
}

#[test]
fn first_listing_is_drawn_before_the_nested_scan_settles() -> io::Result<()> {
    const DIRECTORIES: usize = 512;

    let temp = TempDir::new()?;
    let root = temp.0.join("root");
    fs::create_dir(&root)?;
    for index in 0..DIRECTORIES {
        let child = root.join(format!("child-{index:03}"));
        fs::create_dir(&child)?;
        fs::write(child.join("payload"), b"x")?;
    }

    let mut session = Session::start_with_env("first-listing", &root, &["--read-only"], 400, 24, &[("FDU_PROFILE", "1")]);
    session.wait_ready();
    session.text("q");
    let deadline = session.terminal.deadline(STEP);
    assert_eq!(session.terminal.wait_for_exit(deadline).unwrap(), ExitStatus::Code(0));
    session.terminal.assert_terminal_restored(&session.baseline).unwrap();

    let output = String::from_utf8_lossy(session.terminal.raw_output()).into_owned();
    let profile_field = |name: &str| output.split_whitespace().find_map(|field| field.strip_prefix(name));
    let first_listing_ms: f64 = profile_field("first_usable_listing_ms=")
        .expect("profile output must report the first usable listing time")
        .parse()
        .expect("listing time must be a number of milliseconds");
    let settled_ms: f64 = profile_field("initial_scan_settled_ms=")
        .expect("profile output must report the initial scan settle time")
        .parse()
        .expect("settle time must be a number of milliseconds");
    assert!(
        first_listing_ms < settled_ms,
        "the root listing should be drawn before its nested scan completes; first listing {first_listing_ms} ms, settled {settled_ms} ms"
    );
    Ok(())
}

#[test]
#[ignore = "release PTY navigation latency benchmark; run by name with --ignored --exact"]
fn navigation_latency_during_indexed_scan() -> io::Result<()> {
    const ACTION_PAIRS: usize = 16;
    const INPUT_INTERVAL: Duration = Duration::from_millis(10);
    const SCAN_STEP: Duration = Duration::from_secs(120);
    let profile_enabled = std::env::var_os("FDU_PROFILE").is_some();
    let mut child_env = Vec::new();
    if profile_enabled {
        child_env.push(("FDU_PROFILE", "1"));
    }

    let scan_started = Instant::now();
    let mut session = Session::start_with_env(
        "navigation-scan-latency",
        support::navigation_fixture(),
        &["--read-only"],
        100,
        24,
        &child_env,
    );
    session.step = SCAN_STEP;
    let initial = session.wait("first live root listing", |screen| {
        screen.lines().get(1).is_some_and(|line| line.contains("fdu-directory-navigation-root ("))
            && screen.lines().last().is_some_and(|line| line.contains("entries…"))
            && selected_row_text(screen).is_some()
    });
    let indexed_entries_at_start = indexed_entry_count(&initial);
    let mut max_indexed_entries = indexed_entries_at_start;
    assert!(
        selected_row_text(&initial).is_some_and(|row| row.contains("bulk/")),
        "selected row: {:?}; screen: {:?}",
        selected_row_text(&initial),
        initial.lines()
    );
    session.key(Key::Enter);
    let deadline = session.terminal.deadline(session.step);
    let bulk = session
        .terminal
        .wait_for_screen(deadline, "enter the large directory while scanning", |screen| {
            screen.lines().get(1).is_some_and(|line| {
                line.contains("fdu-directory-navigation-root/bulk (")
                    && !line.contains("fdu-directory-navigation-root/bulk/")
            }) && screen.lines().last().is_some_and(|line| line.contains("entries…"))
                && selected_row_text(screen).is_some()
        })
        .unwrap();
    max_indexed_entries = max_indexed_entries.max(indexed_entry_count(&bulk));
    assert!(selected_row_text(&bulk).is_some_and(|row| row.contains("directory-")));
    session.key(Key::Enter);
    let deadline = session.terminal.deadline(session.step);
    let directory_screen = session
        .terminal
        .wait_for_screen(deadline, "enter a nested directory while scanning", |screen| {
            screen.lines().get(1).is_some_and(|line| line.contains("fdu-directory-navigation-root/bulk/directory-"))
                && screen.lines().last().is_some_and(|line| line.contains("entries…"))
                && selected_row_text(screen).is_some()
        })
        .unwrap();
    max_indexed_entries = max_indexed_entries.max(indexed_entry_count(&directory_screen));
    assert!(max_indexed_entries > 0);
    session.key(Key::Left);
    let deadline = session.terminal.deadline(session.step);
    let parent_screen = session
        .terminal
        .wait_for_screen(deadline, "return to the parent directory while scanning", |screen| {
            screen.lines().get(1).is_some_and(|line| {
                line.contains("fdu-directory-navigation-root/bulk (")
                    && !line.contains("fdu-directory-navigation-root/bulk/directory-")
            }) && screen.lines().last().is_some_and(|line| line.contains("entries…"))
                && selected_row_text(screen).is_some()
        })
        .unwrap();
    max_indexed_entries = max_indexed_entries.max(indexed_entry_count(&parent_screen));

    let mut samples = Vec::with_capacity(ACTION_PAIRS * 2);
    for _ in 0..ACTION_PAIRS {
        std::thread::sleep(INPUT_INTERVAL);
        let started = Instant::now();
        session.key(Key::Enter);
        let deadline = session.terminal.deadline(session.step);
        let screen = session
            .terminal
            .wait_for_screen(deadline, "enter a nested directory while scanning", |screen| {
                screen.lines().get(1).is_some_and(|line| line.contains("fdu-directory-navigation-root/bulk/directory-"))
            })
            .unwrap();
        max_indexed_entries = max_indexed_entries.max(indexed_entry_count(&screen));
        samples.push(started.elapsed().as_micros() as u64);
        assert!(screen.lines().last().is_some_and(|line| line.contains("entries…")));

        std::thread::sleep(INPUT_INTERVAL);
        let started = Instant::now();
        session.key(Key::Left);
        let deadline = session.terminal.deadline(session.step);
        let screen = session
            .terminal
            .wait_for_screen(deadline, "return to root while scanning", |screen| {
                screen.lines().get(1).is_some_and(|line| {
                    line.contains("fdu-directory-navigation-root/bulk (")
                        && !line.contains("fdu-directory-navigation-root/bulk/directory-")
                })
            })
            .unwrap();
        max_indexed_entries = max_indexed_entries.max(indexed_entry_count(&screen));
        samples.push(started.elapsed().as_micros() as u64);
        assert!(screen.lines().last().is_some_and(|line| line.contains("entries…")));
    }
    assert!(
        max_indexed_entries > indexed_entries_at_start,
        "indexed entry count did not progress during navigation: {indexed_entries_at_start} -> {max_indexed_entries}"
    );

    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p95 = samples[samples.len() * 95 / 100];
    let min = samples[0];
    let max = samples[samples.len() - 1];
    let ready_screen = session.wait_ready();
    let scan_ready_ms = scan_started.elapsed().as_secs_f64() * 1000.0;
    let indexed_entries = indexed_entry_count(&ready_screen);
    assert_eq!(indexed_entries, 180_001);
    let output = session.quit_with_output();
    if profile_enabled {
        let profile_start = output.find("fdu-profile ").expect("profile output is enabled");
        let profile = output[profile_start..].lines().next().expect("profile output has a line");
        let entries = profile
            .split_whitespace()
            .find_map(|field| field.strip_prefix("entries="))
            .expect("profile reports indexed entries")
            .parse::<usize>()
            .expect("indexed entry count is an integer");
        assert_eq!(entries, 180_001);
        eprintln!("{profile}");
    }
    eprintln!(
        "navigation latency during scan: fixture_entries={indexed_entries} samples={} p50_us={p50} p95_us={p95} range_us={min}..{max} scan_ready_ms={scan_ready_ms:.3}",
        samples.len(),
    );
    Ok(())
}

fn selected_row_text(screen: &ScreenSnapshot) -> Option<String> {
    (2..screen.row_count().saturating_sub(2)).find_map(|row| {
        let selected = (0..usize::from(screen.size().columns())).any(|column| {
            screen.cell(row, column).is_some_and(|cell| cell.attributes().inverse)
        });
        selected.then(|| screen.row(row).unwrap_or_default())
    })
}

fn indexed_entry_count(screen: &ScreenSnapshot) -> usize {
    screen
        .lines()
        .last()
        .and_then(|line| line.split_once(" entries").map(|(count, _)| count.trim()))
        .and_then(|count| count.parse().ok())
        .unwrap_or_default()
}

#[test]
#[ignore = "host-dependent release-tree timing; run cargo test --release --test interactive_pty release_artifact_tree_reaches_ready_under_100ms -- --ignored --exact"]
fn release_artifact_tree_reaches_ready_under_100ms() -> io::Result<()> {
    const RUNS: usize = 5;
    const MIN_INDEXED_ENTRIES: usize = 1_000;

    let release_tree = PathBuf::from(env!("CARGO_BIN_EXE_fdu"))
        .parent()
        .expect("the fdu binary must be inside target/release")
        .to_path_buf();
    let mut samples = Vec::with_capacity(RUNS);

    for _ in 0..RUNS {
        let mut session = Session::start_with_env("release-ready", &release_tree, &["--read-only"], 400, 24, &[("FDU_PROFILE", "1")]);
        session.wait_ready();
        session.text("q");
        let deadline = session.terminal.deadline(STEP);
        assert_eq!(session.terminal.wait_for_exit(deadline).unwrap(), ExitStatus::Code(0));
        session.terminal.assert_terminal_restored(&session.baseline).unwrap();

        let output = String::from_utf8_lossy(session.terminal.raw_output()).into_owned();
        let profile_field = |name: &str| output.split_whitespace().find_map(|field| field.strip_prefix(name));
        let settled_ms: f64 = profile_field("initial_scan_settled_ms=")
            .expect("profile output must report the initial scan settle time")
            .parse()
            .expect("settle time must be a number of milliseconds");
        let entries: usize = profile_field("entries=")
            .expect("profile output must report the indexed entry count")
            .parse()
            .expect("indexed entry count must be an integer");
        assert!(
            entries >= MIN_INDEXED_ENTRIES,
            "expected a populated release tree with at least {MIN_INDEXED_ENTRIES} entries, got {entries}"
        );
        samples.push(settled_ms);
    }

    samples.sort_unstable_by(f64::total_cmp);
    let median_ms = samples[samples.len() / 2];
    eprintln!("initial scan settle samples: {samples:?} ms; median: {median_ms} ms");
    assert!(median_ms < 100.0, "initial scan settle median must be below 100 ms; samples: {samples:?}");
    Ok(())
}
