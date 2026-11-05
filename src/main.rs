#[cfg(feature = "allocation-profile")]
mod allocation_profile;
#[cfg(feature = "interactive")]
mod browser;
#[cfg(feature = "interactive")]
mod rmrf;

use fdu_scan::{scan_with_exclusions, ScanExclusions, ScanOptions, SummaryExclusion, TopLevelDirectory};
use std::env;
use std::error::Error;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process;

#[cfg(target_os = "linux")]
const DEFAULT_RAYON_THREADS: usize = 4;

struct Options {
    path: PathBuf,
    exclusions: Vec<PathBuf>,
    apparent: bool,
    help: bool,
    mode: RunMode,
    read_only: bool,
    rmrf: bool,
    assume_yes: bool,
}

#[derive(Debug)]
struct SignalExit(i32);

impl std::fmt::Display for SignalExit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "terminated by signal {}", self.0)
    }
}

impl Error for SignalExit {}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RunMode {
    Automatic,
    Summary,
    Interactive,
}

fn main() {
    let result = run();
    #[cfg(feature = "allocation-profile")]
    allocation_profile::report();
    if let Err(error) = result {
        if let Some(signal) = error.downcast_ref::<SignalExit>() {
            process::exit(128 + signal.0);
        }
        eprintln!("fdu: {error}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let options = parse_args()?;
    if options.help {
        print_help();
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        require_linux_6()?;
        configure_rayon_threads();
    }

    if options.rmrf {
        return run_rmrf(options.path, options.assume_yes);
    }
    let exclusions = ScanExclusions::from_paths(options.exclusions)?;

    let terminal_available = io::stdin().is_terminal() && io::stdout().is_terminal();
    let use_interactive = match options.mode {
        RunMode::Summary => false,
        RunMode::Interactive if !terminal_available => {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "--interactive requires terminal input and output",
            )
            .into());
        }
        RunMode::Interactive => true,
        RunMode::Automatic => automatic_mode_uses_tui(terminal_available),
    };

    if use_interactive {
        return run_interactive(options.path, options.read_only, options.apparent, exclusions);
    }

    let report = scan_with_exclusions(
        &ScanOptions {
            path: options.path,
            apparent: options.apparent,
        },
        &exclusions,
    )?;
    if report.skipped_entries != 0 {
        eprintln!(
            "fdu: warning: skipped {} entries after errors; size totals may be incomplete",
            report.skipped_entries
        );
    }
    if report.mount_boundaries != 0 {
        eprintln!(
            "fdu: warning: excluded {} filesystem or mount boundaries; totals omit their contents",
            report.mount_boundaries
        );
    }
    if report.unsupported_aliases != 0 {
        eprintln!(
            "fdu: warning: skipped {} unsupported directory aliases; affected totals omit their contents",
            report.unsupported_aliases
        );
    }
    write_timed_out_warning(&mut io::stderr().lock(), &report.timed_out_directories)?;

    let stdout = io::stdout();
    let mut output = stdout.lock();
    for directory in report.directories {
        write_summary_entry(&mut output, &directory)?;
    }
    output.flush()?;
    Ok(())
}

fn write_summary_entry(output: &mut impl Write, directory: &TopLevelDirectory) -> io::Result<()> {
    match directory.exclusion {
        Some(SummaryExclusion::MountBoundary) => {
            writeln!(output, "MOUNT BOUNDARY\t{:?}", directory.name)
        }
        Some(SummaryExclusion::UnsupportedAlias) => {
            writeln!(output, "UNSUPPORTED DIRECTORY ALIAS\t{:?}", directory.name)
        }
        Some(SummaryExclusion::OpenTimedOut) => {
            writeln!(output, "SKIPPED: OPEN TIMED OUT\t{:?}", directory.name)
        }
        None => writeln!(output, "{}\t{:?}", format_summary_size(directory.disk_size), directory.name),
    }
}

/// Directories the timed-out warning names one by one; the rest are counted by area.
const NAMED_TIMED_OUT_DIRECTORIES: usize = 10;

fn write_timed_out_warning(output: &mut impl Write, directories: &[PathBuf]) -> io::Result<()> {
    if directories.is_empty() {
        return Ok(());
    }
    writeln!(
        output,
        "fdu: warning: skipped {} directories that did not open in time, usually because macOS is waiting for a privacy prompt to be answered; totals omit them:",
        directories.len()
    )?;
    let mut shallowest_first = directories.iter().collect::<Vec<_>>();
    shallowest_first.sort_by(|left, right| {
        left.components().count().cmp(&right.components().count()).then_with(|| left.cmp(right))
    });
    let (named, rest) = shallowest_first.split_at(shallowest_first.len().min(NAMED_TIMED_OUT_DIRECTORIES));
    for directory in named {
        writeln!(output, "fdu:   {directory:?}")?;
    }
    if rest.is_empty() {
        return Ok(());
    }
    let mut areas = std::collections::BTreeMap::<PathBuf, usize>::new();
    for directory in rest {
        let area = directory.components().take(2).collect::<PathBuf>();
        *areas.entry(area).or_default() += 1;
    }
    let areas = areas
        .iter()
        .map(|(area, count)| format!("{count} in {area:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(output, "fdu:   and {} more: {areas}", rest.len())
}

fn format_summary_size(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "K", "M", "G", "T", "P", "E"];
    if bytes < 1024 {
        return format!("{bytes}B");
    }

    let bytes = u128::from(bytes);
    let mut divisor = 1024u128;
    let mut unit = 1usize;
    let tenths = loop {
        let rounded = (bytes * 10 + divisor / 2) / divisor;
        if rounded < 10_240 || unit == UNITS.len() - 1 {
            break rounded;
        }
        divisor *= 1024;
        unit += 1;
    };
    let value = if tenths % 10 == 0 {
        (tenths / 10).to_string()
    } else {
        format!("{}.{:01}", tenths / 10, tenths % 10)
    };
    format!("{value}{}", UNITS[unit])
}

fn parse_args() -> io::Result<Options> {
    parse_args_from(env::args_os().skip(1))
}

fn parse_args_from(arguments: impl IntoIterator<Item = std::ffi::OsString>) -> io::Result<Options> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut exclusions = Vec::new();
    let mut apparent = false;
    let mut help = false;
    let mut summary = false;
    let mut interactive = false;
    let mut read_only = false;
    let mut rmrf = false;
    let mut assume_yes = false;
    let mut positional_only = false;

    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        if !positional_only {
            match argument.to_str() {
                Some("--") => {
                    positional_only = true;
                    continue;
                }
                Some("-h") | Some("--help") => {
                    help = true;
                    continue;
                }
                Some("-a") | Some("--apparent") => {
                    apparent = true;
                    continue;
                }
                Some("-x") | Some("--exclude") => {
                    exclusions.push(PathBuf::from(arguments.next().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "-x/--exclude requires a PATH")
                    })?));
                    continue;
                }
                Some("--summary") => {
                    summary = true;
                    continue;
                }
                Some("--interactive") => {
                    interactive = true;
                    continue;
                }
                Some("--read-only") => {
                    read_only = true;
                    continue;
                }
                Some("--rmrf") => {
                    rmrf = true;
                    continue;
                }
                Some("--yes") => {
                    assume_yes = true;
                    continue;
                }
                Some(option) if option.starts_with('-') => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown option: {option}"),
                    ));
                }
                _ => {}
            }
        }

        paths.push(PathBuf::from(argument));
        if paths.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "only one path may be given",
            ));
        }
    }

    if summary && interactive {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--summary and --interactive cannot be used together",
        ));
    }
    if rmrf {
        for (conflicting, flag) in [
            (summary, "--summary"),
            (interactive, "--interactive"),
            (read_only, "--read-only"),
            (apparent, "--apparent"),
        ] {
            if conflicting {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("--rmrf cannot be used with {flag}"),
                ));
            }
        }
        // Emptying the current directory must be spelled out, never implied.
        if paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--rmrf requires an explicit PATH",
            ));
        }
        if !exclusions.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--rmrf cannot be used with --exclude",
            ));
        }
    } else if assume_yes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--yes only applies to --rmrf",
        ));
    }

    Ok(Options {
        path: paths.pop().unwrap_or_else(|| PathBuf::from(".")),
        exclusions,
        apparent,
        help,
        mode: if summary {
            RunMode::Summary
        } else if interactive {
            RunMode::Interactive
        } else {
            RunMode::Automatic
        },
        read_only,
        rmrf,
        assume_yes,
    })
}

fn automatic_mode_uses_tui(terminal_available: bool) -> bool {
    terminal_available && cfg!(feature = "interactive")
}

#[cfg(target_os = "linux")]
fn require_linux_6() -> io::Result<()> {
    let system = rustix::system::uname();
    let release = system.release();
    let major = release
        .to_bytes()
        .split(|byte| *byte == b'.')
        .next()
        .and_then(|major| std::str::from_utf8(major).ok())
        .and_then(|major| major.parse::<u32>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "could not parse the running Linux kernel version",
            )
        })?;

    if major < 6 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("Linux kernel 6.0 or newer is required; running {}", release.to_string_lossy()),
        ));
    }
    Ok(())
}

// Set the default before Rayon creates its global pool on the first parallel operation.
#[cfg(target_os = "linux")]
fn configure_rayon_threads() {
    if env::var_os("RAYON_NUM_THREADS").is_none() {
        // This runs before Rayon or the terminal UI starts worker threads.
        unsafe {
            env::set_var("RAYON_NUM_THREADS", DEFAULT_RAYON_THREADS.to_string());
        }
    }
}

fn print_help() {
    #[cfg(feature = "interactive")]
    println!("Usage: fdu [--apparent] [-x PATH | --exclude PATH]... [--summary | --interactive] [--read-only] [PATH]");
    #[cfg(not(feature = "interactive"))]
    println!("Usage: fdu [--apparent] [-x PATH | --exclude PATH]... [--summary] [PATH]");

    #[cfg(feature = "interactive")]
    println!("       fdu --rmrf [--yes] PATH");
    println!();

    #[cfg(feature = "interactive")]
    println!("Browse a directory in the terminal, or print immediate child-directory totals.");
    #[cfg(not(feature = "interactive"))]
    println!("Print immediate child-directory totals.");
    println!("By default, sizes are allocated bytes; --apparent uses logical lengths.");

    #[cfg(feature = "interactive")]
    println!("--summary forces output mode; --interactive requires a usable terminal.");
    #[cfg(not(feature = "interactive"))]
    println!("--summary selects summary output explicitly.");

    #[cfg(feature = "interactive")]
    println!("--read-only disables deletion in interactive mode.");
    println!("-x, --exclude PATH omits an exact path relative to the selected root; repeat as needed.");

    #[cfg(feature = "interactive")]
    {
        println!("--rmrf deletes indexed entries below PATH, keeping PATH itself, without a");
        println!("terminal interface. PATH is required. A symlink root is refused, even");
        println!("with trailing slashes. /, your home directory, and any parent of");
        println!("the current directory are refused. Incomplete selected directories and");
        println!("mount boundaries reject the plan. An incomplete root scan stops before");
        println!("any entries are removed.");
        println!("It asks for confirmation on a terminal; --yes skips that and is required");
        println!("when stdin is not a terminal.");
        println!("Exclusions are not accepted with --rmrf.");
    }
}

fn run_rmrf(path: PathBuf, assume_yes: bool) -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "interactive")]
    {
        rmrf::run(path, assume_yes)
    }
    #[cfg(not(feature = "interactive"))]
    {
        let _ = (path, assume_yes);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "--rmrf support is disabled in this summary-only build",
        )
        .into())
    }
}

fn run_interactive(
    path: PathBuf,
    read_only: bool,
    apparent: bool,
    exclusions: ScanExclusions,
) -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "interactive")]
    {
        browser::run(path, read_only, apparent, exclusions)
    }
    #[cfg(not(feature = "interactive"))]
    {
        let _ = (path, read_only, apparent, exclusions);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "interactive support is disabled in this summary-only build",
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::{automatic_mode_uses_tui, parse_args_from, write_summary_entry, write_timed_out_warning};
    use fdu_scan::{SummaryExclusion, TopLevelDirectory};
    use std::ffi::OsString;
    use std::io;
    use std::path::PathBuf;

    #[test]
    fn summary_keeps_regular_row_format_and_names_excluded_boundaries() {
        let mut output = Vec::new();
        write_summary_entry(
            &mut output,
            &TopLevelDirectory { name: OsString::from("included"), disk_size: 42, exclusion: None },
        ).unwrap();
        write_summary_entry(
            &mut output,
            &TopLevelDirectory {
                name: OsString::from("mounted"),
                disk_size: 0,
                exclusion: Some(SummaryExclusion::MountBoundary),
            },
        ).unwrap();
        write_summary_entry(
            &mut output,
            &TopLevelDirectory {
                name: OsString::from("Desktop"),
                disk_size: 0,
                exclusion: Some(SummaryExclusion::OpenTimedOut),
            },
        ).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "42B\t\"included\"\nMOUNT BOUNDARY\t\"mounted\"\nSKIPPED: OPEN TIMED OUT\t\"Desktop\"\n"
        );
    }

    #[test]
    fn timed_out_warning_names_the_shallowest_directories_and_counts_the_rest_by_area() {
        let mut directories = vec![PathBuf::from("Library/Mobile Documents"), PathBuf::from("Desktop")];
        directories.extend((0..9).map(|index| PathBuf::from(format!("Library/Containers/app{index}/Data"))));
        directories.push(PathBuf::from("Library/Group Containers/group.mail"));
        let mut output = Vec::new();
        write_timed_out_warning(&mut output, &directories).unwrap();
        let output = String::from_utf8(output).unwrap();
        let lines = output.lines().collect::<Vec<_>>();
        assert!(lines[0].starts_with("fdu: warning: skipped 12 directories that did not open in time"), "{output}");
        assert_eq!(lines[1], "fdu:   \"Desktop\"");
        assert_eq!(lines[2], "fdu:   \"Library/Mobile Documents\"");
        assert_eq!(lines[3], "fdu:   \"Library/Group Containers/group.mail\"");
        assert_eq!(lines.len(), 12);
        assert_eq!(lines[11], "fdu:   and 2 more: 2 in \"Library/Containers\"");

        let mut quiet = Vec::new();
        write_timed_out_warning(&mut quiet, &[]).unwrap();
        assert!(quiet.is_empty());
    }

    #[test]
    fn automatic_mode_uses_tui_only_when_the_build_supports_it() {
        assert!(!automatic_mode_uses_tui(false));
        assert_eq!(
            automatic_mode_uses_tui(true),
            cfg!(feature = "interactive"),
        );
    }

    #[test]
    fn parses_repeatable_short_and_long_exclude_options() -> io::Result<()> {
        let options = parse_args_from(
            ["-x", "build/cache", "--exclude", "vendor", "--summary", "root"]
                .into_iter()
                .map(OsString::from),
        )?;

        assert_eq!(options.path, PathBuf::from("root"));
        assert_eq!(options.exclusions, [PathBuf::from("build/cache"), PathBuf::from("vendor")]);
        Ok(())
    }

    #[test]
    fn rejects_exclusions_for_rmrf() {
        let result = parse_args_from(
            ["--rmrf", "-x", "cache", "root"]
                .into_iter()
                .map(OsString::from),
        );
        let error = match result {
            Ok(_) => panic!("rmrf exclusions must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("--rmrf cannot be used with --exclude"));
    }
}
