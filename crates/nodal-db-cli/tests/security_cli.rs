//! The CLI side of the pre-release security review:
//!
//! - a file whose read-surface marker reads a local file (`read_text`)
//!   fails `maintain refresh-views --check` with DuckDB's Permission Error,
//!   and the secret never reaches the output (engine rule 12);
//! - names from a file reach the terminal with control characters escaped;
//! - `--memory-limit` / `--threads` are validated;
//! - `init` refuses an existing path.

use std::path::Path;
use std::process::{Command, Output};

use nodal_db_core::duckdb::Connection;

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_nodal-db"));
    c.env("USER", "cli_tester");
    c
}

fn run(args: &[&std::ffi::OsStr]) -> Output {
    bin().args(args).output().unwrap()
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn init(db: &Path, extra: &[&str]) {
    let out = bin().arg("init").arg(db).args(extra).output().unwrap();
    assert!(out.status.success(), "{}", text(&out));
}

#[test]
fn refresh_views_check_on_a_marker_that_reads_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("evil.duckdb");
    init(&db, &[]);
    let secret = dir.path().join("secret.txt");
    std::fs::write(&secret, "SECRET-123").unwrap();
    let c = Connection::open(&db).unwrap();
    c.execute_batch(&format!(
        "CREATE OR REPLACE MACRO nodal_read_surface() AS TABLE \
         SELECT 1::INTEGER AS contract_major, 7::INTEGER AS revision, \
                (SELECT content FROM read_text('{}')) AS ddl_hash",
        secret.display()
    ))
    .unwrap();
    c.close().unwrap();
    let out = bin()
        .args(["maintain", "refresh-views", "--check"])
        .arg(&db)
        .output()
        .unwrap();
    let all = text(&out);
    assert!(!out.status.success(), "{all}");
    assert!(all.contains("Permission Error"), "{all}");
    assert!(!all.contains("SECRET-123"), "the secret leaked: {all}");
}

#[test]
fn names_reach_the_terminal_with_control_characters_escaped() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("names.duckdb");
    let evil = "Evil\u{1b}]0;pwned\u{7}\u{202e}";
    init(&db, &["--alternative", evil]);
    let out = bin()
        .args(["erase-alternative"])
        .arg(&db)
        .arg(evil)
        .arg("--preview")
        .output()
        .unwrap();
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(!all.contains('\u{1b}') && !all.contains('\u{7}') && !all.contains('\u{202e}'));
    assert!(all.contains(r"Evil\u{1b}]0;pwned\u{7}\u{202e}"), "{all}");
    // An error message quoting a name is escaped too.
    let out = bin()
        .args(["erase-alternative"])
        .arg(&db)
        .arg("No\u{1b}[2Jpe")
        .arg("--preview")
        .output()
        .unwrap();
    let all = text(&out);
    assert!(!out.status.success());
    assert!(!all.contains('\u{1b}'), "{all}");

    // A newline in a name cannot forge a line of the CLI's own output.
    let forged = "x\nerased alternative 'Base'";
    let db2 = dir.path().join("lines.duckdb");
    init(&db2, &["--alternative", forged]);
    let out = bin()
        .args(["erase-alternative"])
        .arg(&db2)
        .arg(forged)
        .arg("--preview")
        .output()
        .unwrap();
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(
        !all.lines().any(|l| l.starts_with("erased alternative")),
        "{all}"
    );
    assert!(all.contains(r"x\u{a}erased"), "{all}");
}

#[test]
fn resource_flags_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("caps.duckdb");
    init(&db, &["--memory-limit", "1GiB", "--threads", "2"]);
    let check = |flags: &[&str]| {
        let mut args: Vec<&std::ffi::OsStr> = flags.iter().map(|f| f.as_ref()).collect();
        args.extend(["maintain", "refresh-views", "--check"].map(std::ffi::OsStr::new));
        args.push(db.as_os_str());
        run(&args)
    };
    let ok = check(&["--memory-limit", "25%", "--threads", "1"]);
    assert!(ok.status.success(), "{}", text(&ok));
    for bad in [&["--threads", "0"][..], &["--memory-limit", "lots"][..]] {
        let out = check(bad);
        assert!(!out.status.success(), "{bad:?}");
        assert!(
            text(&out).contains("--memory-limit / --threads"),
            "{}",
            text(&out)
        );
    }
}

#[test]
fn init_refuses_an_existing_path() {
    let dir = tempfile::tempdir().unwrap();
    let foreign = dir.path().join("mine.duckdb");
    let c = Connection::open(&foreign).unwrap();
    c.execute_batch("CREATE TABLE mine (x INTEGER)").unwrap();
    c.close().unwrap();
    let out = bin().arg("init").arg(&foreign).output().unwrap();
    assert!(!out.status.success());
    assert!(text(&out).contains("already exists"), "{}", text(&out));
    let touched = dir.path().join("touched.duckdb");
    std::fs::write(&touched, b"").unwrap();
    let out = bin().arg("init").arg(&touched).output().unwrap();
    assert!(!out.status.success());
    assert!(text(&out).contains("already exists"), "{}", text(&out));
}
