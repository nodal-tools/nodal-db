//! Phase-0 de-risk spike (rust_implementation_plan.md Part 7, risks R1/R2).
//!
//! Confirms the duckdb-rs *bundled* DuckDB matches the 1.4.4-verified
//! semantics the schema rests on. Every check here is a STOP-gate: a
//! divergence means the whole history mechanism / one-current guard may not
//! hold and must be escalated, not papered over.
//!
//! Checks:
//!   4a  bundled DuckDB version >= 1.4
//!   4b  schema/*.sql applies cleanly in numbered order
//!   4c  generated-column one-current unique index rejects a 2nd current row
//!   4d  DOUBLE[] LIST value round-trips (through duckdb::arrow — risk R2)
//!   4e  TransactionException conflict on concurrent update

use std::fs;
use std::path::{Path, PathBuf};

use duckdb::arrow::array::{Array, Float64Array, ListArray};
use duckdb::Connection;
use spinal_db_core::MINIMUM_DUCKDB;

fn schema_dir() -> PathBuf {
    // crates/spinal-db-core -> repo root -> schema
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("schema")
}

/// Numbered .sql files in lexical order (mirrors build.py `sql_files`).
fn numbered_sql_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .expect("read schema dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().map(|x| x == "sql").unwrap_or(false)
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false))
                    .unwrap_or(false)
        })
        .collect();
    files.sort();
    files
}

/// Apply the full schema into `conn` (build.py algorithm, in Rust).
fn apply_schema(conn: &Connection) {
    for f in numbered_sql_files(&schema_dir()) {
        let sql = fs::read_to_string(&f).expect("read sql file");
        conn.execute_batch(&sql)
            .unwrap_or_else(|e| panic!("FAILED applying {}: {e}", f.display()));
    }
}

/// Parse the bundled version string (e.g. "v1.4.4") into (major, minor).
fn bundled_version(conn: &Connection) -> (u32, u32, String) {
    let raw: String = conn
        .query_row("SELECT version()", [], |r| r.get(0))
        .expect("SELECT version()");
    let trimmed = raw.trim_start_matches('v');
    let mut it = trimmed.split('.');
    let major: u32 = it.next().unwrap().parse().unwrap();
    let minor: u32 = it.next().unwrap_or("0").parse().unwrap_or(0);
    (major, minor, raw)
}

#[test]
fn spike_bundled_duckdb_matches_144_semantics() {
    let tmp = tempfile::Builder::new()
        .suffix(".duckdb")
        .tempfile()
        .expect("tempfile");
    let db_path = tmp.path().to_path_buf();
    // The file must not exist as an empty non-DuckDB file when opened; drop the
    // handle but keep the unique path.
    drop(tmp);
    let conn = Connection::open(&db_path).expect("open duckdb file");

    // ---- 4a: bundled version >= 1.4 --------------------------------------
    let (major, minor, raw) = bundled_version(&conn);
    println!("bundled DuckDB version: {raw} (parsed {major}.{minor})");
    assert!(
        (major, minor) >= MINIMUM_DUCKDB,
        "STOP R1: bundled DuckDB {raw} < required {}.{}",
        MINIMUM_DUCKDB.0,
        MINIMUM_DUCKDB.1
    );

    // ---- 4b: schema applies cleanly --------------------------------------
    apply_schema(&conn);
    println!("4b OK: all schema/*.sql applied");

    // Reuse the commit the bootstrap (900_bootstrap.sql) already inserted as
    // the validity edge, and add one axis row (FK target) for the value
    // inserts below. seq_axis is unused by the schema, so id 1 is free.
    let commit_id: i64 = conn
        .query_row("SELECT max(id) FROM commit", [], |r| r.get(0))
        .expect("bootstrap commit id");
    conn.execute(
        "INSERT INTO axis (id, kind, length, content_hash) VALUES (1, 'integer', 3, 0)",
        [],
    )
    .expect("seed axis");

    // ---- 4c: one-current generated-column unique index -------------------
    conn.execute(
        "INSERT INTO value_scalar
           (entity_id, parameter_definition_id, alternative_id,
            valid_from_commit, version, value_kind, v_float)
         VALUES (1, 1, 1, ?, 1, 'float', 1.0)",
        [commit_id],
    )
    .expect("first current scalar row");
    // Second *current* row (valid_to_commit NULL) for the same key triple must
    // violate one_current_scalar (ce, cp, ca).
    let dup = conn.execute(
        "INSERT INTO value_scalar
           (entity_id, parameter_definition_id, alternative_id,
            valid_from_commit, version, value_kind, v_float)
         VALUES (1, 1, 1, ?, 2, 'float', 2.0)",
        [commit_id],
    );
    let err = dup.expect_err("STOP: duplicate current scalar row was NOT rejected");
    let es = err.to_string();
    println!("4c OK: duplicate current row rejected: {es}");
    assert!(
        es.to_lowercase().contains("constraint")
            || es.to_lowercase().contains("unique")
            || es.to_lowercase().contains("duplicate"),
        "unexpected error text for one-current violation: {es}"
    );

    // ---- 4d: DOUBLE[] LIST round-trip (through duckdb::arrow) -------------
    conn.execute(
        "INSERT INTO value_array
           (entity_id, parameter_definition_id, alternative_id,
            valid_from_commit, version, axis1_id, values)
         VALUES (2, 2, 2, ?, 1, 1, [1.5, 2.5, 3.5]::DOUBLE[])",
        [commit_id],
    )
    .expect("insert DOUBLE[] array");

    let mut stmt = conn
        .prepare("SELECT values FROM value_array WHERE entity_id = 2")
        .unwrap();
    let batches: Vec<_> = stmt
        .query_arrow([])
        .expect("query_arrow")
        .collect();
    let batch = batches.first().expect("one record batch");
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("column is a ListArray");
    let inner = list.value(0);
    let floats = inner
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("list values are Float64");
    let got: Vec<f64> = (0..floats.len()).map(|i| floats.value(i)).collect();
    println!("4d OK: DOUBLE[] round-trip -> {got:?}");
    assert_eq!(got, vec![1.5, 2.5, 3.5], "DOUBLE[] round-trip mismatch");

    // ---- 4e: TransactionException conflict on concurrent update ----------
    // Two connections against the SAME database instance (try_clone shares the
    // catalog/MVCC state, unlike two independent file opens). Both update the
    // same current scalar row; one wins, the other must conflict.
    let con_a = &conn;
    let con_b = conn.try_clone().expect("second connection on same db");

    con_a.execute_batch("BEGIN").unwrap();
    con_b.execute_batch("BEGIN").unwrap();

    con_a
        .execute(
            "UPDATE value_scalar SET v_float = 10.0 WHERE entity_id = 1",
            [],
        )
        .expect("con_a update");

    // The write-write conflict surfaces either at con_b's UPDATE (DuckDB
    // detects the concurrent uncommitted write) or at con_b's COMMIT.
    let b_update = con_b.execute(
        "UPDATE value_scalar SET v_float = 20.0 WHERE entity_id = 1",
        [],
    );

    con_a.execute_batch("COMMIT").expect("con_a commit wins");

    let conflict_text = match b_update {
        Err(e) => {
            let _ = con_b.execute_batch("ROLLBACK");
            e.to_string()
        }
        Ok(_) => {
            // update didn't conflict yet; the commit must.
            let commit = con_b.execute_batch("COMMIT");
            let e = commit.expect_err("STOP: con_b neither update nor commit conflicted");
            e.to_string()
        }
    };
    println!("4e OK: concurrent-update conflict raised: {conflict_text}");
    assert!(
        conflict_text.contains("Conflict") || conflict_text.contains("TransactionException"),
        "unexpected error text for concurrent-update conflict: {conflict_text}"
    );

    println!("SPIKE PASSED: all Phase-0 semantic checks hold on {raw}");
}
