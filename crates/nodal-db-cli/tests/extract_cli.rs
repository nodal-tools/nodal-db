//! End-to-end CLI tests for `nodal-db extract` and `nodal-db maintain
//! compact` (the core behaviour is covered by the core's `tests/extract.rs`
//! and `tests/compaction.rs`; these pin the CLI surface: arguments, output,
//! refusals, exit codes).

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use nodal_db_core::api::{db_identity, write_parameter};
use nodal_db_core::duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use nodal_db_core::duckdb::arrow::datatypes::{DataType, Field, Schema};
use nodal_db_core::duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::duckdb::Connection;
use nodal_db_core::types::WriteOptions;

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_nodal-db"));
    c.env("USER", "cli_tester");
    c
}

fn run_ok(c: &mut Command) -> String {
    let out = c.output().expect("spawn nodal-db");
    assert!(
        out.status.success(),
        "command failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn run_err(c: &mut Command) -> String {
    let out = c.output().expect("spawn nodal-db");
    assert!(!out.status.success(), "expected failure");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn scalar(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// A source with a few superseded scalar versions.
fn source(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("src.duckdb");
    run_ok(bin().arg("init").arg(&db).args(["--alternative", "Base"]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    for i in 0..4 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["n1", "n2"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![i as f64, 10.0 + i as f64])) as ArrayRef,
            ],
        )
        .unwrap();
        write_parameter(
            &db,
            "Node",
            "demand",
            "Base",
            batch,
            &[],
            WriteOptions::default(),
            "cli_tester",
            "w",
        )
        .unwrap();
    }
    db
}

#[test]
fn extract_head_and_as_of_through_the_cli() {
    let dir = tempfile::tempdir().unwrap();
    let src = source(dir.path());
    let head = scalar(&src, "SELECT max(id) FROM \"commit\"");

    let dest = dir.path().join("slim.duckdb");
    let out = run_ok(
        bin()
            .arg("extract")
            .arg(&src)
            .arg(&dest)
            .args(["--note", "for sharing"]),
    );
    assert!(
        out.contains(&format!("the current state (commit {head})")),
        "{out}"
    );
    assert!(out.contains(&db_identity(&src).unwrap()), "{out}");
    assert!(out.contains(&db_identity(&dest).unwrap()), "{out}");
    assert!(out.contains("NEW database identity"), "{out}");
    assert!(out.contains("rows left out"), "{out}");
    assert_eq!(
        scalar(
            &dest,
            "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL"
        ),
        0
    );
    let user: String = {
        let conn = Connection::open(&dest).unwrap();
        let u = conn
            .query_row(
                "SELECT c.user FROM erasure_marker m JOIN \"commit\" c ON c.id = m.at_commit \
                 WHERE m.kind = 'horizon'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.close().unwrap();
        u
    };
    assert_eq!(user, "cli_tester", "--user defaults to the OS username");

    // As of an earlier commit.
    let past = dir.path().join("past.duckdb");
    let out = run_ok(bin().arg("extract").arg(&src).arg(&past).args([
        "--as-of",
        &(head - 1).to_string(),
        "--user",
        "auditor",
    ]));
    assert!(out.contains(&format!("as of commit {}", head - 1)), "{out}");
    assert_eq!(
        scalar(&past, "SELECT max(version) FROM value_scalar"),
        scalar(&src, "SELECT max(version) FROM value_scalar") - 1
    );

    // Refusals: existing dest, missing source, as-of a non-commit.
    let err = run_err(bin().arg("extract").arg(&src).arg(&dest));
    assert!(err.contains("already exists"), "{err}");
    let err = run_err(
        bin()
            .arg("extract")
            .arg(dir.path().join("missing.duckdb"))
            .arg(dir.path().join("x.duckdb")),
    );
    assert!(err.contains("no such database file"), "{err}");
    let err = run_err(
        bin()
            .arg("extract")
            .arg(&src)
            .arg(dir.path().join("y.duckdb"))
            .args(["--as-of", "99999"]),
    );
    assert!(err.contains("not a commit id"), "{err}");
    assert!(!dir.path().join("y.duckdb").exists());
}

#[test]
fn maintain_compact_through_the_cli() {
    let dir = tempfile::tempdir().unwrap();
    let db = source(dir.path());
    let id = db_identity(&db).unwrap();
    let rows = scalar(&db, "SELECT count(*) FROM value_scalar");

    let out = run_ok(bin().args(["maintain", "compact"]).arg(&db));
    assert!(out.contains("compacted"), "{out}");
    assert_eq!(db_identity(&db).unwrap(), id);
    assert_eq!(scalar(&db, "SELECT count(*) FROM value_scalar"), rows);

    // --if-needed right after a compaction: below the threshold.
    let out = run_ok(bin().args(["maintain", "compact", "--if-needed"]).arg(&db));
    assert!(out.contains("no compaction needed"), "{out}");
    // A tiny growth factor fires.
    let out = run_ok(
        bin()
            .args(["maintain", "compact", "--if-needed", "--growth", "0.5"])
            .arg(&db),
    );
    assert!(out.contains("compacted"), "{out}");
    // --growth needs --if-needed.
    let err = run_err(
        bin()
            .args(["maintain", "compact", "--growth", "2"])
            .arg(&db),
    );
    assert!(err.contains("--if-needed"), "{err}");
    // A missing file is refused, not created.
    let missing = dir.path().join("missing.duckdb");
    let err = run_err(bin().args(["maintain", "compact"]).arg(&missing));
    assert!(err.contains("no such database file"), "{err}");
    assert!(!missing.exists());
}
