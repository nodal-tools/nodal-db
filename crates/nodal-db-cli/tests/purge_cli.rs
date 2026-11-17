//! End-to-end CLI test for `nodal-db purge` (the core behaviour is covered
//! by the core's `tests/purge.rs`; this pins the CLI surface: arguments,
//! the preview, the refusal and the report).

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use nodal_db_core::api::write_parameter;
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

fn run(c: &mut Command) -> (bool, String, String) {
    let out = c.output().expect("spawn nodal-db");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn live(db: &Path, table: &str) -> i64 {
    Connection::open(db)
        .unwrap()
        .query_row(
            &format!("SELECT count(*) FROM {table} WHERE valid_to_commit IS NULL"),
            [],
            |r| r.get(0),
        )
        .unwrap()
}

fn write(db: &Path, class: &str, entities: &[&str]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0; entities.len()])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parameter(
        db,
        class,
        "p",
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "t",
        "",
    )
    .unwrap();
}

#[test]
fn purge_previews_refuses_and_closes() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    let (ok, _, err) = run(bin().arg("init").arg(&db));
    assert!(ok, "{err}");
    write(&db, "node", &["n1", "n2"]);
    write(&db, "unit", &["u1"]);

    let (ok, out, err) = run(bin().arg("purge").arg(&db).args([
        "--scope",
        "values",
        "--class",
        "node",
        "--preview",
    ]));
    assert!(ok, "{err}");
    assert!(out.contains("would close 0 object(s), 2 row(s)"), "{out}");
    assert!(out.contains("value_scalar"), "{out}");
    assert_eq!(live(&db, "value_scalar"), 3, "a preview writes nothing");

    let (ok, _, err) =
        run(bin()
            .arg("purge")
            .arg(&db)
            .args(["--scope", "memberships", "--class", "node"]));
    assert!(!ok);
    assert!(err.contains("does not take --class"), "{err}");
    assert_eq!(live(&db, "value_scalar"), 3);

    let (ok, _, err) = run(bin().arg("purge").arg(&db).args(["--scope", "everything"]));
    assert!(!ok);
    assert!(err.contains("unknown scope"), "{err}");
    let (ok, _, err) = run(bin()
        .arg("purge")
        .arg(&db)
        .args(["--scope", "entities", "--class", "nope"]));
    assert!(!ok);
    assert!(err.contains("unknown class"), "{err}");

    let (ok, out, err) = run(bin()
        .arg("purge")
        .arg(&db)
        .args(["--scope", "values", "--class", "node"]));
    assert!(ok, "{err}");
    assert!(out.contains("purged 0 object(s), 2 row(s)"), "{out}");
    assert_eq!(live(&db, "value_scalar"), 1);

    let (ok, out, err) = run(bin()
        .arg("purge")
        .arg(&db)
        .args(["--scope", "classes", "--except", "unit"]));
    assert!(ok, "{err}");
    assert!(out.contains("purged 4 object(s)"), "{out}");
    assert_eq!(live(&db, "entity_class"), 1);
}
