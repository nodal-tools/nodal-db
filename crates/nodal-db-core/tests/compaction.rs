//! End-to-end tests for the physical-repack primitive (`compaction`), a new
//! member of the round-trip/non-destructiveness conformance family
//! (`roundtrip_duckdb.rs`). Each builds a real DB through the public write path,
//! compacts it, and asserts the file shrank while ALL logical content — every
//! history row (live + dead), the whole commit log, sequence state, current AND
//! as-of reads — survived byte-for-byte at the query level.
//!
//! The generated-column drift guard (R3) and the self-check discrepancy guard
//! (B3) need private access and live as in-module unit tests in `compaction.rs`.

use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::write_parameter;
use nodal_db_core::compaction::{baseline_path, compact, compact_if_needed, sweep_orphan_temps};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

// --- helpers ---------------------------------------------------------------

fn scalar_batch(entity: &str, v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ent = Arc::new(StringArray::from(vec![entity])) as ArrayRef;
    let val = Arc::new(Float64Array::from(vec![v])) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, val]).unwrap()
}

fn write_scalar(db: &Path, class: &str, param: &str, alt: &str, entity: &str, v: f64) {
    write_parameter(
        db,
        class,
        param,
        alt,
        scalar_batch(entity, v),
        &[],
        WriteOptions::default(),
        "tester",
        "compaction test write",
    )
    .expect("write_parameter");
}

/// Force a checkpoint so blocks accrue (the exact source of the bloat this
/// feature targets: many small commit-checkpoints stranding dead blocks).
fn checkpoint(db: &Path) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch("CHECKPOINT").unwrap();
    conn.close().unwrap();
}

/// Order-independent fingerprint of ANY query: `(row_count, bit_xor(hash(row)))`
/// over the whole result. Two DBs returning the same fingerprint for the same
/// query returned identical result sets (modulo row order).
fn fingerprint(db: &Path, query: &str) -> (i64, String) {
    let conn = Connection::open(db).unwrap();
    let sql =
        format!("SELECT count(*), CAST(COALESCE(bit_xor(hash(t)), 0) AS VARCHAR) FROM ({query}) t");
    let out = conn
        .query_row(&sql, [], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap();
    conn.close().unwrap();
    out
}

fn scalar(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// Every base table (same set as `compaction::TABLE_ORDER`) — used to assert
/// per-table row counts are preserved.
const ALL_TABLES: &[&str] = &[
    "commit", "axis", "metadata", "entity_metadata", "parameter_value_metadata", "run",
    "alternative", "scenario", "scenario_alternative", "entity_alternative", "membership",
    "axis_datetime", "axis_period", "axis_label", "axis_integer", "axis_float", "axis_time_pattern",
    "display_mode", "entity_class_display_mode", "entity_class", "class_set", "class_set_member",
    "entity_class_dimension", "entity", "entity_element", "entity_location", "units_of_measure",
    "parameter_value_list", "list_value", "parameter_definition", "entity_parameter_unit",
    "parameter_type", "parameter_type_dimension", "value_scalar", "value_long", "value_array",
    "value_array_hist", "definition_value_scalar", "definition_value_long", "definition_value_array",
    "semantic_annotation",
];

/// Build a deliberately bloated DB: repeated overwrites of a handful of keys
/// (accruing dead history rows) with a checkpoint after each commit (accruing
/// dead blocks). Returns the DB path (owned by `dir`).
fn build_bloated(dir: &Path, iters: usize) -> std::path::PathBuf {
    let db = dir.join("bloated.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    for i in 0..iters {
        // Overwrite the same key repeatedly ⇒ one live row + many superseded
        // (dead) history rows, exactly what non-destructiveness must preserve.
        write_scalar(&db, "Widget", "size", "Base", "w1", i as f64);
        write_scalar(&db, "Widget", "mass", "Base", "w2", (i * 2) as f64);
        checkpoint(&db);
    }
    db
}

// --- (a) non-destructiveness round-trip ------------------------------------

#[test]
fn compact_shrinks_file_and_preserves_all_content() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 80);

    // Snapshot the pre-compact file to compare against, byte-frozen.
    let before = dir.path().join("before.duckdb");
    std::fs::copy(&db, &before).unwrap();

    // A representative as-of commit in the middle of history.
    let max_commit = scalar(&db, "SELECT max(id) FROM \"commit\"");
    let mid = max_commit / 2;

    // Fingerprints that MUST match before vs after: raw tables (live + dead
    // rows), the commit log, current reads, and an as-of / time-travel read.
    let probes: Vec<String> = vec![
        // raw base tables (includes superseded rows)
        "SELECT * FROM value_scalar".into(),
        "SELECT * FROM \"commit\"".into(),
        "SELECT * FROM alternative".into(),
        "SELECT * FROM entity".into(),
        "SELECT * FROM parameter_definition".into(),
        // current reads via the public read macro
        "SELECT * FROM read_parameter('Widget','size')".into(),
        "SELECT * FROM read_parameter('Widget','mass')".into(),
        // as-of / time-travel reads
        format!("SELECT * FROM value_scalar_asof({mid})"),
        format!("SELECT * FROM entity_asof({mid})"),
    ];
    let before_fps: Vec<(i64, String)> = probes.iter().map(|q| fingerprint(&before, q)).collect();

    let report = compact(&db).expect("compact");
    assert!(report.swapped, "compact must have swapped");

    // File actually shrank (a perf property, asserted separately from content).
    assert!(
        report.bytes_after < report.bytes_before,
        "file did not shrink: {} -> {} bytes",
        report.bytes_before,
        report.bytes_after
    );
    assert!(
        report.blocks_after < report.blocks_before,
        "blocks did not shrink: {} -> {}",
        report.blocks_before,
        report.blocks_after
    );

    // Every probed query is byte-identical before vs after.
    for (q, before_fp) in probes.iter().zip(&before_fps) {
        let after_fp = fingerprint(&db, q);
        assert_eq!(after_fp, *before_fp, "query fingerprint drifted: {q}");
    }

    // Per-table counts (live + dead) preserved, and the report agrees.
    for t in ALL_TABLES {
        let n_before = scalar(&before, &format!("SELECT count(*) FROM \"{t}\""));
        let n_after = scalar(&db, &format!("SELECT count(*) FROM \"{t}\""));
        assert_eq!(n_before, n_after, "count drift on {t}");
        let reported = report
            .rows_per_table
            .iter()
            .find(|(name, _)| name == t)
            .map(|(_, c)| *c as i64)
            .unwrap_or(-1);
        assert_eq!(reported, n_after, "report count mismatch on {t}");
    }
}

// --- (b) sequence continuity ------------------------------------------------

#[test]
fn compact_preserves_sequence_state_for_next_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 20);

    let entities_before = scalar(&db, "SELECT count(*) FROM entity WHERE valid_to_commit IS NULL");
    let max_entity_before = scalar(&db, "SELECT COALESCE(max(id),0) FROM entity");

    compact(&db).expect("compact");

    // A NEW entity write after compact must get a fresh, non-colliding id — if
    // sequences were left at their DDL start it would try id=1 and violate
    // `one_current_entity` / reuse a live id.
    write_scalar(&db, "Widget", "size", "Base", "w_new", 42.0);

    let entities_after = scalar(&db, "SELECT count(*) FROM entity WHERE valid_to_commit IS NULL");
    let max_entity_after = scalar(&db, "SELECT COALESCE(max(id),0) FROM entity");
    assert_eq!(entities_after, entities_before + 1, "new entity not created");
    assert!(
        max_entity_after > max_entity_before,
        "new entity id {max_entity_after} did not advance past {max_entity_before}"
    );

    // And it reads back.
    let v: f64 = {
        let conn = Connection::open(&db).unwrap();
        let v = conn
            .query_row(
                "SELECT v_float FROM read_parameter('Widget','size') WHERE entity = 'w_new'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.close().unwrap();
        v
    };
    assert_eq!(v, 42.0);
}

// --- (d) schema_version mismatch rejected ----------------------------------

#[test]
fn compact_rejects_schema_version_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 5);
    let before_bytes = std::fs::metadata(&db).unwrap().len();

    // Poison the source's schema_version.
    {
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "UPDATE metadata SET value = '9.9-bogus' WHERE lower(name) = 'schema_version'",
        )
        .unwrap();
        conn.close().unwrap();
    }

    let err = compact(&db).expect_err("mismatch must be rejected");
    assert!(
        format!("{err}").contains("schema_version"),
        "unexpected error: {err}"
    );
    assert!(matches!(err, DbError::Api(_)));

    // Original left intact (no temp swapped in): still opens and matches the
    // (poisoned) source, unchanged in size aside from the in-place UPDATE.
    let after_bytes = std::fs::metadata(&db).unwrap().len();
    assert_eq!(before_bytes, after_bytes, "original file was modified");
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM value_scalar"),
        scalar(&db, "SELECT count(*) FROM value_scalar"),
        "db no longer readable"
    );
    // No orphan temp left behind.
    let leftover = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .any(|e| e.file_name().to_str().map(|n| n.contains(".compact.")).unwrap_or(false));
    assert!(!leftover, "a temp file was left behind after abort");
}

// --- (f) crash-safety: orphan sweep + intact original ----------------------

#[test]
fn compact_sweeps_orphan_temps_and_keeps_original() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 15);

    let before_fp = fingerprint(&db, "SELECT * FROM read_parameter('Widget','size')");

    // Simulate an abandoned temp from a crashed prior run.
    let orphan = dir.path().join("bloated.duckdb.compact.999999.tmp");
    std::fs::write(&orphan, b"garbage from a crashed compaction").unwrap();
    let orphan_wal = dir.path().join("bloated.duckdb.compact.888888.tmp.wal");
    std::fs::write(&orphan_wal, b"garbage wal").unwrap();

    // A bare sweep removes them, leaving the original untouched.
    sweep_orphan_temps(&db);
    assert!(!orphan.exists(), "orphan temp not swept");
    assert!(!orphan_wal.exists(), "orphan temp wal not swept");
    assert!(db.exists(), "original removed by sweep");

    // Re-create an orphan and confirm compact() itself sweeps on entry, then
    // repacks correctly with content preserved.
    std::fs::write(&orphan, b"garbage again").unwrap();
    let report = compact(&db).expect("compact");
    assert!(report.swapped);
    assert!(!orphan.exists(), "compact() did not sweep the orphan on entry");
    let after_fp = fingerprint(&db, "SELECT * FROM read_parameter('Widget','size')");
    assert_eq!(after_fp, before_fp, "content changed across compact");
}

// --- compact_if_needed byte-growth watermark (R1 CALIBRATION FIX) -----------

#[test]
fn compact_if_needed_fires_on_byte_growth_and_manages_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 12);
    let sidecar = baseline_path(&db);
    assert!(!sidecar.exists(), "no baseline sidecar before the first run");

    // (1) No baseline ⇒ establish it: compact once and shrink the file. The
    // sidecar then equals the post-compaction byte size.
    let before = std::fs::metadata(&db).unwrap().len();
    let r1 = compact_if_needed(&db, 3.0).expect("establish-baseline run");
    assert!(r1.swapped, "first run (no baseline) must compact to establish it");
    assert!(r1.bytes_after < before, "establish run should shrink the file");
    let compacted = std::fs::metadata(&db).unwrap().len();
    assert!(sidecar.exists(), "baseline sidecar must be written after compaction");
    let baseline: u64 = std::fs::read_to_string(&sidecar).unwrap().trim().parse().unwrap();
    assert_eq!(baseline, compacted, "sidecar must equal the post-compaction size");

    // (2) Immediately again ⇒ NO-OP: the file has not tripled since the
    // baseline, and the cheap gate must not touch it.
    let r2 = compact_if_needed(&db, 3.0).expect("no-op run");
    assert!(!r2.swapped, "must no-op below the 3x growth watermark");
    assert_eq!(
        std::fs::metadata(&db).unwrap().len(),
        compacted,
        "no-op must leave the file byte-identical"
    );

    // (3) Trip the watermark by shrinking the recorded baseline (equivalent to
    // the file having grown past 3x its last-compacted size). It must fire.
    std::fs::write(&sidecar, b"1").unwrap();
    let r3 = compact_if_needed(&db, 3.0).expect("watermark-tripped run");
    assert!(r3.swapped, "must fire once current bytes exceed 3x the baseline");
    // Firing refreshes the baseline to the fresh size (no longer the tiny "1").
    let refreshed: u64 = std::fs::read_to_string(&sidecar).unwrap().trim().parse().unwrap();
    assert_eq!(refreshed, std::fs::metadata(&db).unwrap().len());

    // (4) The sidecar is NOT an orphan temp: neither an unconditional compact()
    // nor the orphan sweep may remove it.
    compact(&db).expect("unconditional compact leaves the sidecar");
    assert!(baseline_path(&db).exists(), "compact() must not sweep the baseline sidecar");
    sweep_orphan_temps(&db);
    assert!(baseline_path(&db).exists(), "orphan sweep must not delete the baseline sidecar");
}
