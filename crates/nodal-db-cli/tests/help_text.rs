//! The CLI's help text is user documentation: it names the public format
//! page, describes `--no-retain-content` correctly, and carries no references
//! into private design documents (section numbers, spec paths, decision ids).

use std::process::Command;

const FORMAT_PAGE: &str = "https://nodal-tools.github.io/nodal-db/dev/arrow_import_format/";

fn help(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_nodal-db"))
        .args(args)
        .output()
        .expect("run nodal-db");
    assert!(out.status.success(), "{args:?} --help failed");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn cli_help_has_no_private_spec_refs() {
    for args in [
        &["--help"][..],
        &["import", "--help"],
        &["init", "--help"],
        &["erase-alternative", "--help"],
        &["extract", "--help"],
        &["purge", "--help"],
        &["maintain", "--help"],
        &["maintain", "compact", "--help"],
        &["maintain", "refresh-views", "--help"],
        &["template", "--help"],
        &["template", "plan", "--help"],
        &["template", "apply", "--help"],
        &["template", "load", "--help"],
        &["template", "export", "--help"],
    ] {
        let text = help(args);
        for bad in ["§", "specs/", "OPEN-Q", "D-E6", "_plan"] {
            assert!(
                !text.contains(bad),
                "{args:?} help contains {bad:?}:\n{text}"
            );
        }
    }
}

#[test]
fn import_help_names_the_format_page_and_describes_retain_content() {
    let text = help(&["import", "--help"]);
    assert!(text.contains(FORMAT_PAGE), "{text}");
    assert!(text.contains("content-less stub"), "{text}");
    assert!(!text.contains("no-op"), "{text}");
}

#[test]
fn top_level_help_is_release_ready() {
    let text = help(&["--help"]);
    assert!(
        text.contains("nodal-db: create, import into and maintain nodal-db database files"),
        "{text}"
    );
    assert!(!text.contains("migrate"), "{text}");
    assert!(text.contains("75"), "exit codes documented: {text}");
    for bad in ["v1 core", "belt", "init-with-template", "Template IR"] {
        assert!(!text.contains(bad), "{bad:?} in:\n{text}");
    }
    // Every subcommand's summary is one line.
    let commands = text
        .split("Commands:")
        .nth(1)
        .unwrap()
        .split("Options:")
        .next()
        .unwrap();
    for line in commands.lines().filter(|l| !l.trim().is_empty()) {
        assert!(
            line.starts_with("  ") && !line.starts_with("    "),
            "wrapped summary: {line:?}"
        );
    }
}

/// Exit codes of the argument parser: a usage error (an unknown flag, a
/// missing argument) is 2, distinct from a runtime error (1); `--help` and
/// `--version` succeed.
#[test]
fn usage_errors_exit_2_and_help_version_exit_0() {
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_nodal-db"))
            .args(args)
            .output()
            .expect("run nodal-db")
            .status
            .code()
    };
    assert_eq!(run(&["--no-such-flag"]), Some(2));
    assert_eq!(run(&["init"]), Some(2), "missing argument");
    assert_eq!(
        run(&[
            "import",
            "f.arrows",
            "--db",
            "x",
            "--retain-content",
            "--no-retain-content"
        ]),
        Some(2)
    );
    assert_eq!(run(&["--help"]), Some(0));
    assert_eq!(run(&["--version"]), Some(0));
    assert!(help(&["--help"]).contains("2 usage error"));
}
