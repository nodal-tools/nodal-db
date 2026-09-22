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

use nodal_db_core::api::{db_identity, write_parameter};
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
    "commit", "axis", "erasure_marker", "metadata", "entity_metadata", "parameter_value_metadata", "run",
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

// --- (c) DB-identity survival -----------------------------------------------

/// CRITICAL invariant (schema_interop.md §9): the DB-identity UUID is DATA,
/// never a re-mintable DDL side effect. The repack's fresh temp mints its OWN
/// identity via 900_bootstrap.sql; the B1 bootstrap wipe must delete it and the
/// copy must carry the source's row verbatim — after compact the identity is
/// byte-identical and still the single current `db_identity` row.
#[test]
fn compact_preserves_db_identity() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 10);

    let before = db_identity(&db).expect("identity before compact");
    let report = compact(&db).expect("compact");
    assert!(report.swapped);

    let after = db_identity(&db).expect("identity after compact");
    assert_eq!(before, after, "compaction must not re-mint the DB identity");
    let n = scalar(
        &db,
        "SELECT count(*) FROM metadata \
         WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL",
    );
    assert_eq!(n, 1, "exactly one current db_identity row after compact");
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

// --- (g) clustering (erasure_compaction_plan §4.1, D-E4 always-on) ----------

/// Raw-build a value_scalar population with live and dead rows physically
/// INTERLEAVED (each key inserts its dead v1 then its live v2, so the layout
/// alternates dead/live), plus the two commit rows the spans reference.
fn build_interleaved(dir: &Path, n_keys: i64) -> std::path::PathBuf {
    let db = dir.join("interleaved.duckdb");
    nodal_db_core::schema_init::init_schema(&db).expect("init");
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO \"commit\" (user, message) VALUES ('t', 'c2');
         INSERT INTO value_scalar
           (entity_id, parameter_definition_id, alternative_id, valid_from_commit,
            valid_to_commit, version, value_kind, v_float)
         SELECT i.range, 1, 1, 1, x.vt, x.v, 'float', i.range * 10 + x.v
         FROM range({n_keys}) i,
              (VALUES (1::SMALLINT, 2::BIGINT), (2::SMALLINT, NULL::BIGINT)) x(v, vt)
         ORDER BY i.range, x.v;
         CHECKPOINT;"
    ))
    .expect("raw interleaved build");
    conn.close().unwrap();
    db
}

/// The physical row order of `(valid_to_commit IS NULL)` over value_scalar
/// (rowid order = insertion/physical order), as a vec of bools.
fn liveness_sequence(db: &Path) -> Vec<bool> {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch("SET threads = 1").unwrap();
    let mut stmt = conn
        .prepare("SELECT valid_to_commit IS NULL FROM value_scalar ORDER BY rowid")
        .unwrap();
    let seq: Vec<bool> = stmt
        .query_map([], |r| r.get::<_, bool>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    seq
}

/// D-E4: the rebuild copy is clustered current-first — after compaction the
/// physical layout of a from/to tier is one contiguous live prefix followed by
/// the dead rows (the ordering IS the acceptance assertion at CI tier; the
/// scan-time ratio is the `#[ignore]`d bench below, kept out of CI for
/// flakiness). Content is untouched (self-check + count assertions).
#[test]
fn compact_clusters_from_to_tiers_current_first() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_interleaved(dir.path(), 5_000);

    // Pre-compact: interleaved by construction (sanity — the fixture must not
    // be accidentally clustered, or the post-assert is vacuous).
    let pre = liveness_sequence(&db);
    assert_eq!(pre.len(), 10_000);
    assert!(!pre[..2].iter().all(|&l| l), "fixture must start interleaved");

    let report = compact(&db).expect("compact");
    assert!(report.swapped);

    let post = liveness_sequence(&db);
    assert_eq!(post.len(), 10_000, "row count must survive");
    assert_eq!(post.iter().filter(|&&l| l).count(), 5_000, "live count must survive");
    let first_dead = post.iter().position(|&l| !l).expect("dead rows exist");
    assert_eq!(first_dead, 5_000, "live rows must form one contiguous prefix");
    assert!(
        post[first_dead..].iter().all(|&l| !l),
        "no live row may follow the first dead row (current-first clustering)"
    );
}

/// Acceptance 6's scan-time metric (target ≥1.5x per the critique's measured
/// ~2x): clustered (compacted) vs unclustered (interleaved raw layout of the
/// SAME data) current-state scan. `#[ignore]`d: wall-time ratios are too
/// machine-sensitive for the CI tier — run with `--include-ignored` (the
/// `just full` release gate). The CI-tier guarantee is the ordering assert
/// above; the MECHANISM is contiguous validity-vector filtering, NOT
/// row-group skipping (critique M2 — do not re-metric this on row-group
/// counts; 1.5.5 does not skip row groups on liveness).
#[test]
#[ignore = "bench-style wall-time ratio; run in the release gate (just full)"]
fn clustered_current_scan_is_measurably_faster() {
    let dir = tempfile::tempdir().unwrap();
    // 1M keys => 2M rows, 50% dead — the critique's fixture shape.
    let unclustered = build_interleaved(dir.path(), 1_000_000);
    let clustered = dir.path().join("clustered.duckdb");
    std::fs::copy(&unclustered, &clustered).unwrap();
    compact(&clustered).expect("compact clustered copy");

    let time_scan = |db: &Path| -> f64 {
        let conn = Connection::open(db).unwrap();
        conn.execute_batch("SET threads = 1").unwrap();
        let mut best = f64::INFINITY;
        for _ in 0..7 {
            let t0 = std::time::Instant::now();
            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NULL",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1_000_000);
            best = best.min(t0.elapsed().as_secs_f64());
        }
        conn.close().unwrap();
        best
    };
    let t_unclustered = time_scan(&unclustered);
    let t_clustered = time_scan(&clustered);
    let ratio = t_unclustered / t_clustered;
    println!("current-state scan: unclustered {t_unclustered:.4}s, clustered {t_clustered:.4}s, ratio {ratio:.2}x");
    assert!(
        ratio >= 1.3,
        "clustered current-state scan not measurably faster: {ratio:.2}x (target >=1.5x, floor 1.3x)"
    );

    // Zone-map pruning on the sortable key part of the ORDER BY (the part
    // that DOES skip row groups — critique M2's verified sanity leg).
    let conn = Connection::open(&clustered).unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM value_scalar WHERE entity_id < 1000",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 2000);
    conn.close().unwrap();
}

// --- (h) stale-WAL sidecar hygiene (erasure plan §7 item 7 / critique m7b) --

/// A `<db>.wal` left by a previously-crashed writer must not survive a
/// compaction swap: its content is already in the copied file (the RO
/// open/ATTACH replays it), and replaying it against the freshly-swapped file
/// on the next RW open is the hazard. DuckDB 1.5.5 silently tolerates an
/// unparseable WAL (verified), so the unlink is hygiene + hazard removal, not
/// crash-recovery interference.
#[test]
fn compact_unlinks_stale_wal_sidecar_after_swap() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 5);
    let wal = {
        let mut w = db.as_os_str().to_os_string();
        w.push(".wal");
        std::path::PathBuf::from(w)
    };
    // Simulate the crashed-writer leftover (header-invalid: DuckDB ignores it
    // on open, so the compaction's source probe still succeeds — verified
    // engine semantics on 1.5.5).
    std::fs::write(&wal, b"stale wal from a crashed writer").unwrap();

    let report = compact(&db).expect("compact with stale wal present");
    assert!(report.swapped);
    assert!(!wal.exists(), "stale <db>.wal must be unlinked after the swap");

    // Next RW open + write works on the swapped file.
    write_scalar(&db, "Widget", "size", "Base", "w_after_wal", 7.0);
    let n = scalar(&db, "SELECT count(*) FROM entity WHERE cur_name = 'w_after_wal'");
    assert_eq!(n, 1);
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
