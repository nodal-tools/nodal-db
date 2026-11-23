//! The CLI's help text is user documentation: it names the public format
//! page, describes `--force-replace` correctly, and carries no references
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
fn import_help_names_the_format_page_and_describes_force_replace() {
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
