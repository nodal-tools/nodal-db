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

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::head_commit;
use nodal_db_core::api::{db_identity, write_parameter};
use nodal_db_core::compaction::{
    baseline_path, compact, compact_if_due, compact_if_needed, keeps_original, read_baseline,
    sweep_orphan_temps, CommitBackstop, CompactionBackoff, CompactionPolicy, CompactionReport,
    CompactionTrigger,
};
use nodal_db_core::error::DbError;
use nodal_db_core::extract::{extract, ExtractOpts};
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
        .query_row(&sql, [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
        })
        .unwrap();
    conn.close().unwrap();
    out
}

/// A gated call that fired: it either swapped or (the size guard) rebuilt and
/// kept the original. Tiny fixtures often do not shrink, so the gate tests
/// assert "ran", not "swapped".
fn ran(r: &CompactionReport) -> bool {
    r.trigger.is_some() && (r.swapped || r.kept_original)
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
    "commit",
    "axis",
    "erasure_marker",
    "metadata",
    "entity_metadata",
    "parameter_value_metadata",
    "run",
    "alternative",
    "scenario",
    "scenario_alternative",
    "entity_alternative",
    "membership",
    "axis_datetime",
    "axis_period",
    "axis_label",
    "axis_integer",
    "axis_float",
    "axis_time_pattern",
    "display_mode",
    "entity_class_display_mode",
    "entity_class",
    "class_set",
    "class_set_member",
    "entity_class_dimension",
    "entity",
    "entity_element",
    "entity_location",
    "units_of_measure",
    "parameter_value_list",
    "list_value",
    "parameter_definition",
    "entity_parameter_unit",
    "parameter_type",
    "parameter_type_dimension",
    "value_scalar",
    "value_long",
    "value_array",
    "value_array_hist",
    "definition_value_scalar",
    "definition_value_long",
    "definition_value_array",
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

    let entities_before = scalar(
        &db,
        "SELECT count(*) FROM entity WHERE valid_to_commit IS NULL",
    );
    let max_entity_before = scalar(&db, "SELECT COALESCE(max(id),0) FROM entity");

    compact(&db).expect("compact");

    // A NEW entity write after compact must get a fresh, non-colliding id — if
    // sequences were left at their DDL start it would try id=1 and violate
    // `one_current_entity` / reuse a live id.
    write_scalar(&db, "Widget", "size", "Base", "w_new", 42.0);

    let entities_after = scalar(
        &db,
        "SELECT count(*) FROM entity WHERE valid_to_commit IS NULL",
    );
    let max_entity_after = scalar(&db, "SELECT COALESCE(max(id),0) FROM entity");
    assert_eq!(
        entities_after,
        entities_before + 1,
        "new entity not created"
    );
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
    assert!(
        matches!(&err, DbError::SchemaMismatch { found, expected, .. }
            if found == "9.9-bogus" && expected == nodal_db_core::compaction::SCHEMA_VERSION),
        "{err:?}"
    );

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
    let leftover = std::fs::read_dir(dir.path()).unwrap().flatten().any(|e| {
        e.file_name()
            .to_str()
            .map(|n| n.contains(".compact."))
            .unwrap_or(false)
    });
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
    assert!(
        !orphan.exists(),
        "compact() did not sweep the orphan on entry"
    );
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
    assert!(
        !pre[..2].iter().all(|&l| l),
        "fixture must start interleaved"
    );

    let report = compact(&db).expect("compact");
    assert!(report.swapped);

    let post = liveness_sequence(&db);
    assert_eq!(post.len(), 10_000, "row count must survive");
    assert_eq!(
        post.iter().filter(|&&l| l).count(),
        5_000,
        "live count must survive"
    );
    let first_dead = post.iter().position(|&l| !l).expect("dead rows exist");
    assert_eq!(
        first_dead, 5_000,
        "live rows must form one contiguous prefix"
    );
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
/// counts; 1.5.5 does not skip row groups on liveness — not re-measured on
/// 2.0.0).
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
        ratio >= 1.5,
        "clustered current-state scan not measurably faster: {ratio:.2}x \
         (acceptance target >=1.5x; the critique measured ~2x and this harness 2.07x)"
    );

    // Zone-map pruning on the sortable key part of the ORDER BY (the part
    // that DOES skip row groups — critique M2's verified sanity leg),
    // asserted on ROWS SCANNED via query profiling, not on the result: the
    // keyed range read over the clustered layout must touch a small fraction
    // of the 2 M rows (the critique's method measured 245 K of 2 M).
    let prof = dir.path().join("prof.json");
    let conn = Connection::open(&clustered).unwrap();
    conn.execute_batch(&format!(
        "SET threads = 1; \
         PRAGMA enable_profiling='json'; \
         PRAGMA profiling_output='{}'; \
         SET profiling_mode='detailed';",
        prof.display().to_string().replace('\'', "''")
    ))
    .unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM value_scalar WHERE entity_id < 1000",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 2000);
    conn.close().unwrap();
    let json = std::fs::read_to_string(&prof).expect("profiling output");
    // Crude but dependency-free: collect every per-operator rows-scanned
    // count. DuckDB 1.5.5 names the key "operator_rows_scanned"; 2.0 renamed
    // it "rows_scanned" (the quoted match excludes 2.0's query-level
    // "total_rows_scanned").
    let scanned_max = ["\"operator_rows_scanned\"", "\"rows_scanned\""]
        .iter()
        .flat_map(|key| json.match_indices(key))
        .filter_map(|(i, _)| {
            let tail = &json[i..];
            let colon = tail.find(':')?;
            tail[colon + 1..]
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<i64>()
                .ok()
        })
        .max()
        .expect("a rows-scanned count present in the profile");
    println!("keyed range read: {scanned_max} of 2000000 rows scanned");
    assert!(
        scanned_max < 500_000,
        "zone maps did not prune the keyed range read: {scanned_max} of 2 M rows scanned"
    );
}

// --- (h2) valid-WAL replay pin (LOAD-BEARING engine behavior) ----------------

/// PIN of the engine behavior the pre-rename WAL unlink rests on: DuckDB
/// (1.5.5; re-verified on 2.0.0) REPLAYS a valid `<db>.wal` on read-only
/// open/ATTACH, so a
/// crashed-writer's committed-but-uncheckpointed content is carried into the
/// compaction copy BEFORE the stale sidecar is removed — nothing durable is
/// ever discarded. Like the guard's consumer-shape sweep, this is a permanent
/// engine-upgrade tripwire (the claim is behavioral, not documented contract):
/// if a future engine stops replaying WALs on RO open, this test fails and
/// the unlink strategy must be redesigned — never weaken it to keep CI green.
///
/// The crashed writer is simulated exactly: a connection with
/// checkpoint-on-shutdown disabled commits a row (durable in the WAL only)
/// and closes — the same on-disk state a SIGKILLed post-commit writer leaves.
#[test]
fn compact_carries_stale_wal_content_and_unlinks_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 5);
    let wal = {
        let mut w = db.as_os_str().to_os_string();
        w.push(".wal");
        std::path::PathBuf::from(w)
    };

    // Leave a VALID wal holding one committed row the main file lacks.
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "PRAGMA disable_checkpoint_on_shutdown; \
         INSERT INTO \"commit\" (user, message) \
           VALUES ('walpin', 'committed only to the wal');",
    )
    .unwrap();
    conn.close().unwrap();
    assert!(wal.exists(), "fixture: a valid WAL must remain after close");

    let report = compact(&db).expect("compact with a valid stale wal");
    assert!(report.swapped);
    assert!(!wal.exists(), "the stale wal must be gone after the swap");
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM \"commit\" WHERE message = 'committed only to the wal'"
        ),
        1,
        "the WAL-only commit must survive INTO the compacted file (RO replay)"
    );
}

// --- (h) stale-WAL sidecar hygiene (erasure plan §7 item 7 / critique m7b) --

/// A `<db>.wal` left by a previously-crashed writer must not survive a
/// compaction swap: its content is already in the copied file (the RO
/// open/ATTACH replays it), and replaying it against the freshly-swapped file
/// on the next RW open is the hazard. DuckDB silently tolerates an
/// unparseable WAL (verified on 1.5.5; re-verified on 2.0.0), so the unlink is hygiene + hazard removal, not
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
    // engine semantics on 1.5.5; re-verified on 2.0.0).
    std::fs::write(&wal, b"stale wal from a crashed writer").unwrap();

    let report = compact(&db).expect("compact with stale wal present");
    assert!(report.swapped);
    assert!(
        !wal.exists(),
        "stale <db>.wal must be unlinked after the swap"
    );

    // Next RW open + write works on the swapped file.
    write_scalar(&db, "Widget", "size", "Base", "w_after_wal", 7.0);
    let n = scalar(
        &db,
        "SELECT count(*) FROM entity WHERE cur_name = 'w_after_wal'",
    );
    assert_eq!(n, 1);
}

// --- compact_if_needed byte-growth watermark (R1 CALIBRATION FIX) -----------

#[test]
fn compact_if_needed_fires_on_byte_growth_and_manages_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_bloated(dir.path(), 12);
    let sidecar = baseline_path(&db);
    assert!(
        !sidecar.exists(),
        "no baseline sidecar before the first run"
    );

    // (1) No baseline ⇒ establish it: compact once and shrink the file. The
    // sidecar then equals the post-compaction byte size.
    let before = std::fs::metadata(&db).unwrap().len();
    let r1 = compact_if_needed(&db, 3.0).expect("establish-baseline run");
    assert!(
        r1.swapped,
        "first run (no baseline) must compact to establish it"
    );
    assert!(
        r1.bytes_after < before,
        "establish run should shrink the file"
    );
    let compacted = std::fs::metadata(&db).unwrap().len();
    assert!(
        sidecar.exists(),
        "baseline sidecar must be written after compaction"
    );
    let baseline = read_baseline(&db).expect("v2 sidecar parses");
    assert_eq!(
        baseline.bytes, compacted,
        "sidecar must equal the post-compaction size"
    );
    assert_eq!(baseline.commit, Some(head_commit(&db).unwrap()));
    assert_eq!(r1.trigger, Some(CompactionTrigger::NoBaseline));

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
    assert!(
        ran(&r3),
        "must fire once current bytes exceed 3x the baseline"
    );
    // Firing refreshes the baseline to the fresh size (no longer the tiny "1").
    assert_eq!(r3.trigger, Some(CompactionTrigger::Growth));
    let refreshed = read_baseline(&db).unwrap();
    assert_eq!(refreshed.bytes, std::fs::metadata(&db).unwrap().len());

    // (4) The sidecar is NOT an orphan temp: neither an unconditional compact()
    // nor the orphan sweep may remove it.
    let r4 = compact(&db).expect("unconditional compact leaves the sidecar");
    assert_eq!(r4.trigger, Some(CompactionTrigger::Requested));
    assert!(
        baseline_path(&db).exists(),
        "compact() must not sweep the baseline sidecar"
    );
    sweep_orphan_temps(&db);
    assert!(
        baseline_path(&db).exists(),
        "orphan sweep must not delete the baseline sidecar"
    );
}

// --- array_id: copied verbatim, stride and parity survive --------------------

fn write_array(db: &Path, entity: &str, vals: &[f64]) {
    let n = vals.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("step", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; n])) as ArrayRef,
            Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap();
    write_parameter(
        db,
        "node",
        "profile",
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "tester",
        "array write",
    )
    .expect("write array");
}

/// Compaction copies every `array_id` verbatim (live and `_hist`), and the
/// sequence restore keeps `seq_value_array`'s INCREMENT BY 2 (F10: a bare
/// `START WITH n` used to reset it to 1) and the ids' parity.
#[test]
fn compact_preserves_array_ids_and_sequence_stride() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("arr.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    for i in 0..4 {
        for e in ["a", "b", "c"] {
            write_array(&db, e, &[i as f64, 1.0, 2.0]);
        }
    }
    let ids_query = "SELECT 'live', array_id, entity_id, version FROM value_array \
                     UNION ALL SELECT 'hist', array_id, entity_id, version FROM value_array_hist";
    let before = fingerprint(&db, ids_query);
    let last_before = scalar(
        &db,
        "SELECT last_value FROM duckdb_sequences() WHERE sequence_name = 'seq_value_array'",
    );

    compact(&db).expect("compact");

    assert_eq!(
        fingerprint(&db, ids_query),
        before,
        "array_ids copied verbatim"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT increment_by FROM duckdb_sequences() WHERE sequence_name = 'seq_value_array'"
        ),
        2,
        "stride survives the sequence restore"
    );
    // A fresh array value after compaction mints last + 2 (odd), never a
    // re-used or adjacent id.
    write_array(&db, "d", &[9.0, 9.0, 9.0]);
    let new_id = scalar(
        &db,
        "SELECT va.array_id FROM value_array va JOIN entity e ON e.id = va.entity_id \
         WHERE e.name = 'd'",
    );
    assert_eq!(new_id, last_before + 2);
    assert_eq!(new_id % 2, 1);
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM audit_array_id_coherence"),
        0
    );
}

/// Two compactions with no write in between: after the first, every sequence
/// was re-created with `START WITH n` and reports a NULL `last_value` until
/// its next use. The second compaction must carry `n` (the source's
/// `start_value`), not reset the sequence to its DDL start, or the next
/// auto-id re-issues an id already in the copied rows.
#[test]
fn compact_twice_keeps_unused_sequences_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("twice.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    write_scalar(&db, "Widget", "size", "Base", "w1", 1.0);
    write_scalar(&db, "Widget", "size", "Base", "w2", 2.0);
    compact(&db).expect("first compaction");
    compact(&db).expect("second compaction, no write in between");

    // New alternative, class, parameter and entities: each mints from a
    // sequence the two compactions left untouched since the first rebuild.
    write_scalar(&db, "Gadget", "mass", "Alt2", "g1", 3.0);
    write_scalar(&db, "Widget", "size", "Alt2", "w3", 4.0);
    for (table, n) in [
        ("alternative", 2),
        ("entity_class", 2),
        ("parameter_definition", 2),
        ("entity", 4),
    ] {
        assert_eq!(
            scalar(
                &db,
                &format!("SELECT count(DISTINCT id) FROM {table} WHERE valid_to_commit IS NULL")
            ),
            n,
            "{table}: a re-issued id"
        );
    }
    assert_eq!(
        scalar(&db, "SELECT count(*) - count(DISTINCT id) FROM \"commit\""),
        0
    );
}

/// A database file whose name starts with `src.`: the rebuild's temp
/// (`src.duckdb.compact.<pid>.tmp`) gets DuckDB's default catalog name `src`,
/// which used to collide with the alias the source was attached under
/// ("database with name src already exists").
#[test]
fn compact_a_file_named_src() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("src.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    write_scalar(&db, "Widget", "size", "Base", "w1", 1.0);
    let id = db_identity(&db).unwrap();
    compact(&db).expect("compaction of src.duckdb");
    assert_eq!(db_identity(&db).unwrap(), id);
    write_scalar(&db, "Widget", "size", "Base", "w1", 2.0);
}

// --- compact_if_due: the commit-count backstop (save-path plan §5) ----------

/// Byte gate effectively off, so only the commit count can fire.
fn commits_only(n: u64) -> CompactionPolicy {
    CompactionPolicy {
        growth_factor: 1.0e9,
        max_commits: CommitBackstop::Fixed(n),
    }
}

/// A fresh small DB, compacted once so a v2 baseline records its head.
fn baselined(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("due.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    write_scalar(&db, "Widget", "size", "Base", "w1", 0.0);
    compact(&db).expect("establish baseline");
    db
}

fn write_sidecar(db: &Path, text: &str) {
    std::fs::write(baseline_path(db), text).unwrap();
}

/// Plan test 15: The commit gate fires at exactly N commits after the baseline, not
/// before; `trigger == Commits`; the sidecar records the new head.
#[test]
fn commit_gate_fires_at_exactly_n() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    let base = read_baseline(&db).unwrap().commit.unwrap();
    for i in 1..3 {
        write_scalar(&db, "Widget", "size", "Base", "w1", i as f64);
        let head = head_commit(&db).unwrap();
        assert_eq!(head, base + i);
        let r = compact_if_due(&db, commits_only(3), Some(head)).unwrap();
        assert!(!r.swapped, "{i} commits < 3");
        assert_eq!(r.trigger, None);
    }
    write_scalar(&db, "Widget", "size", "Base", "w1", 3.0);
    let head = head_commit(&db).unwrap();
    let r = compact_if_due(&db, commits_only(3), Some(head)).unwrap();
    assert!(ran(&r), "3 commits = N");
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
    let b = read_baseline(&db).unwrap();
    assert_eq!(b.commit, Some(head));
    assert_eq!(b.bytes, std::fs::metadata(&db).unwrap().len());
    // Reset: the next call is a no-op again.
    let r = compact_if_due(&db, commits_only(3), Some(head)).unwrap();
    assert!(!r.swapped);
}

/// A no-op save reports `commit_id == 0` (skip_unchanged_writes_PLAN D1). A
/// caller passing that report's id as the head must not read as "head below
/// the baseline" (another file), which would force a repack: 0 means "read
/// the head".
#[test]
fn a_zero_head_is_read_not_taken_as_another_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    write_scalar(&db, "Widget", "size", "Base", "w1", 1.0);
    let r = compact_if_due(&db, commits_only(3), Some(0)).unwrap();
    assert!(!r.swapped, "a zero head must not force a repack");
    assert_eq!(r.trigger, None);
}

/// Plan test 16: The byte gate still fires through `compact_if_due` (`Growth`), and a
/// missing sidecar compacts (`NoBaseline`).
#[test]
fn byte_gate_and_missing_baseline_through_compact_if_due() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    let head = head_commit(&db).unwrap();
    write_sidecar(&db, &format!("bytes=1\ncommit={head}\n"));
    let r = compact_if_due(&db, CompactionPolicy::default(), Some(head)).unwrap();
    assert!(ran(&r));
    assert_eq!(r.trigger, Some(CompactionTrigger::Growth));

    std::fs::remove_file(baseline_path(&db)).unwrap();
    let r = compact_if_due(&db, CompactionPolicy::default(), Some(head)).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::NoBaseline));
    assert_eq!(read_baseline(&db).unwrap().commit, Some(head));

    // A torn sidecar is treated as missing.
    write_sidecar(&db, "bytes=12\n");
    let r = compact_if_due(&db, CompactionPolicy::default(), Some(head)).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::NoBaseline));
}

/// Plan test 17: An old-format (bytes-only) sidecar is adopted: no compaction, and the
/// sidecar gains `commit = head` with the same bytes.
#[test]
fn old_format_sidecar_is_adopted() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    for i in 0..5 {
        write_scalar(&db, "Widget", "size", "Base", "w1", 10.0 + i as f64);
    }
    let head = head_commit(&db).unwrap();
    let bytes = std::fs::metadata(&db).unwrap().len();
    write_sidecar(&db, &format!("{bytes}\n"));
    let r = compact_if_due(&db, commits_only(3), Some(head)).unwrap();
    assert!(!r.swapped, "adopt, never compact on an upgrade");
    let b = read_baseline(&db).unwrap();
    assert_eq!(b.bytes, bytes);
    assert_eq!(b.commit, Some(head));
    // Counting starts at the adopted head.
    for i in 0..3 {
        write_scalar(&db, "Widget", "size", "Base", "w1", 20.0 + i as f64);
    }
    let r = compact_if_due(&db, commits_only(3), None).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
}

/// Plan test 18: `head = None` reads the head itself (one RO open) and gates the same.
#[test]
fn head_none_reads_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    write_scalar(&db, "Widget", "size", "Base", "w1", 1.0);
    let r = compact_if_due(&db, commits_only(2), None).unwrap();
    assert!(!r.swapped);
    write_scalar(&db, "Widget", "size", "Base", "w1", 2.0);
    let r = compact_if_due(&db, commits_only(2), None).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
}

/// Plan test 19: Backstop off ⇒ byte gate only, however many commits landed.
#[test]
fn backstop_off_is_byte_gate_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    for i in 0..6 {
        write_scalar(&db, "Widget", "size", "Base", "w1", i as f64);
    }
    for backstop in [CommitBackstop::Off, CommitBackstop::Fixed(0)] {
        let policy = CompactionPolicy {
            growth_factor: 1.0e9,
            max_commits: backstop,
        };
        let r = compact_if_due(&db, policy, None).unwrap();
        assert!(!r.swapped, "{backstop:?}");
    }
    // The default (size-scaled, N = 50 here) is not due after 6 commits.
    let r = compact_if_due(&db, CompactionPolicy::default(), None).unwrap();
    assert!(!r.swapped);
}

/// Plan test 20: A head below the baseline's commit (a file restored or replaced under
/// its old sidecar) is a broken baseline ⇒ compact (plan C8 i).
#[test]
fn head_below_baseline_commit_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    let head = head_commit(&db).unwrap();
    let bytes = std::fs::metadata(&db).unwrap().len();
    write_sidecar(&db, &format!("bytes={bytes}\ncommit={}\n", head + 100));
    let r = compact_if_due(&db, CompactionPolicy::default(), Some(head)).unwrap();
    assert!(ran(&r));
    assert_eq!(r.trigger, Some(CompactionTrigger::NoBaseline));
    assert_eq!(read_baseline(&db).unwrap().commit, Some(head));
}

/// Plan test 21: `extract` writes a v2 sidecar with `commit = E`; `compact_if_due` on
/// the extract does not compact until N commits later (plan C8 ii).
#[test]
fn extract_writes_a_v2_baseline_with_its_own_commit() {
    let dir = tempfile::tempdir().unwrap();
    let src = build_bloated(dir.path(), 3);
    let dest = dir.path().join("x.duckdb");
    let r = extract(
        &src,
        &dest,
        &ExtractOpts {
            as_of: None,
            user: "tester",
            note: None,
        },
    )
    .expect("extract");
    let b = read_baseline(&dest).unwrap();
    assert_eq!(b.commit, Some(r.extract_commit));
    assert_eq!(b.bytes, std::fs::metadata(&dest).unwrap().len());
    assert_eq!(head_commit(&dest).unwrap(), r.extract_commit);
    for i in 0..2 {
        write_scalar(&dest, "Widget", "size", "Base", "w1", 100.0 + i as f64);
        let r = compact_if_due(&dest, commits_only(3), None).unwrap();
        assert!(!r.swapped);
    }
    write_scalar(&dest, "Widget", "size", "Base", "w1", 200.0);
    let r = compact_if_due(&dest, commits_only(3), None).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
}

/// Plan test 23: The repack records the head it read on the fresh file, not the
/// caller's `head` argument (plan C10).
#[test]
fn repack_records_the_true_head_not_the_callers() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    for i in 0..3 {
        write_scalar(&db, "Widget", "size", "Base", "w1", i as f64);
    }
    let head = head_commit(&db).unwrap();
    let bytes = std::fs::metadata(&db).unwrap().len();
    write_sidecar(&db, &format!("bytes={bytes}\ncommit={}\n", head - 2));
    // A lagging caller value (head - 1) still trips N = 1 ...
    let r = compact_if_due(&db, commits_only(1), Some(head - 1)).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
    // ... and the sidecar records the true head.
    assert_eq!(read_baseline(&db).unwrap().commit, Some(head));
}

/// C12: a failed repack arms the in-memory back-off; calls inside the window
/// do not attempt the repack (no error, `backed_off`), the first call past it
/// does, and a success clears it.
#[test]
fn failed_repack_backs_off() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    let head = head_commit(&db).unwrap();
    let bytes = std::fs::metadata(&db).unwrap().len();
    // Due under N = 3, and the repack will refuse (schema-version mismatch).
    write_sidecar(&db, &format!("bytes={bytes}\ncommit={}\n", head - 3));
    let set_version = |v: &str| {
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "UPDATE metadata SET value = ? WHERE lower(name) = 'schema_version' \
             AND valid_to_commit IS NULL",
            [v],
        )
        .unwrap();
        conn.close().unwrap();
    };
    let real = {
        let conn = Connection::open(&db).unwrap();
        let v: String = conn
            .query_row(
                "SELECT value FROM metadata WHERE lower(name) = 'schema_version' \
                 AND valid_to_commit IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.close().unwrap();
        v
    };
    set_version("9.9-bogus");

    let mut b = CompactionBackoff::new();
    let policy = commits_only(3);
    let err = b.compact_if_due(&db, policy, head).unwrap_err();
    assert!(err.to_string().contains("schema_version"), "{err}");
    assert!(matches!(err, DbError::SchemaMismatch { .. }), "{err:?}");
    assert_eq!(b.failed_at(), Some(head));
    // Inside the window (max(5, 3/5) = 5 commits): skipped, no attempt.
    for h in [head, head + 1, head + 4] {
        let r = b
            .compact_if_due(&db, policy, h)
            .expect("backed off, no attempt");
        assert!(r.backed_off && !r.swapped, "head {h}");
    }
    // Past the window: attempted again (and fails again).
    assert!(b.compact_if_due(&db, policy, head + 5).is_err());
    assert_eq!(b.failed_at(), Some(head + 5));
    // The cause clears; past the next window the repack runs and clears it.
    set_version(&real);
    let r = b.compact_if_due(&db, policy, head + 10).unwrap();
    assert!(ran(&r) && !r.backed_off);
    assert_eq!(b.failed_at(), None);
}

// --- closed rows by close commit (compaction_growth_FINDINGS, D-E4 amended) --

/// `PRAGMA database_size().used_blocks` (the findings' "used" measure).
fn used_blocks(db: &Path) -> i64 {
    scalar(db, "SELECT used_blocks FROM pragma_database_size()")
}

/// Raw-build the history-heavy long shape of the growth findings, scaled to
/// test size: `cells` rank-1 long cells (`series` entities × `cells/series`
/// positions on one integer axis), fully rewritten `rounds` times, one commit
/// per round, appended round by round (the write path's append order) with a
/// checkpoint after each round. Round `r` closes at the next round's commit;
/// the last round is current. Raw SQL like [`build_interleaved`]: compaction
/// copies rows verbatim, so the write path adds nothing to what is measured.
fn build_history_heavy(dir: &Path, series: i64, cells: i64, rounds: i64) -> std::path::PathBuf {
    let db = dir.join("history_heavy.duckdb");
    nodal_db_core::schema_init::init_schema(&db).expect("init");
    let per = cells / series;
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO axis (kind, length, content_hash, p_start_int, p_step_int) \
         VALUES ('integer', {per}, 0, 0, 1);"
    ))
    .unwrap();
    let axis: i64 = conn
        .query_row("SELECT max(id) FROM axis", [], |r| r.get(0))
        .unwrap();
    let mut commits = Vec::with_capacity(rounds as usize);
    for _ in 0..rounds {
        commits.push(
            conn.query_row(
                "INSERT INTO \"commit\" (user, message) VALUES ('t', 'round') RETURNING id",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
        );
    }
    for r in 0..rounds as usize {
        let from = commits[r];
        let to = commits
            .get(r + 1)
            .map_or_else(|| "NULL".to_string(), |c| c.to_string());
        conn.execute_batch(&format!(
            "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, \
               valid_from_commit, valid_to_commit, version, axis1_id, i1_pos, value_kind, v_float) \
             SELECT c // {per}, 1, 1, {from}, {to}, {ver}, {axis}, (c % {per})::INTEGER, \
                    'float', (hash(c, {r}) % 100000) / 7.0 \
             FROM range({cells}) t(c) ORDER BY c; \
             CHECKPOINT;",
            ver = r + 1,
        ))
        .unwrap();
    }
    conn.close().unwrap();
    db
}

/// The growth regression (compaction_growth_FINDINGS §1, §3): closed rows
/// sorted by key interleaved each cell's versions, which bloated the hidden
/// commit-FK indexes and broke the commit columns' RLE, so compaction grew
/// history-heavy files (used space 20.8 → 24.0 MiB at the findings'
/// test-fixture size). Sorted by close commit, it shrinks them. Asserts the
/// size (file and used blocks) AND the order property itself.
#[test]
fn compact_shrinks_history_heavy_long_table() {
    let dir = tempfile::tempdir().unwrap();
    // 50 k cells × 21 full rewrites = 1.05 M rows (the findings' fixture).
    let db = build_history_heavy(dir.path(), 50, 50_000, 21);
    let rows = scalar(&db, "SELECT count(*) FROM value_long");
    assert_eq!(rows, 1_050_000);
    let used_before = used_blocks(&db);
    let fp = fingerprint(&db, "SELECT * FROM value_long");

    let r = compact(&db).expect("compact");
    let used_after = used_blocks(&db);
    println!(
        "history-heavy: file {} -> {} bytes; used blocks {used_before} -> {used_after}",
        r.bytes_before, r.bytes_after
    );
    assert!(r.swapped && !r.kept_original);
    assert!(
        r.bytes_after < r.bytes_before,
        "compaction must shrink a history-heavy file: {} -> {}",
        r.bytes_before,
        r.bytes_after
    );
    assert!(r.warning.is_none(), "{:?}", r.warning);
    assert!(
        used_after <= used_before,
        "compaction grew the used space: {used_before} -> {used_after} blocks"
    );
    assert_eq!(fingerprint(&db, "SELECT * FROM value_long"), fp);

    // The order itself (rowid = physical order): the current rows form one
    // prefix in key order; the closed rows follow, non-decreasing by close
    // commit, key order within one close commit.
    let violations = scalar(
        &db,
        "SELECT count(*) FROM ( \
           SELECT valid_to_commit AS t, entity_id AS e, i1_pos AS p, \
                  lag(valid_to_commit) OVER w AS pt, lag(entity_id) OVER w AS pe, \
                  lag(i1_pos) OVER w AS pp, \
                  lag(valid_to_commit IS NULL) OVER w AS plive \
           FROM value_long WINDOW w AS (ORDER BY rowid)) \
         WHERE plive IS NOT NULL AND ( \
           (t IS NULL AND NOT plive) \
           OR (t IS NULL AND (e, p) < (pe, pp)) \
           OR (t IS NOT NULL AND pt IS NOT NULL AND t < pt) \
           OR (t IS NOT NULL AND t = pt AND (e, p) < (pe, pp)))",
    );
    assert_eq!(
        violations, 0,
        "physical order is not current-first / by close commit"
    );
}

/// The size guard on the automatic triggers: a rebuild that is not smaller
/// (here: re-compacting a freshly compacted file) keeps the original, deletes
/// the temp, warns, and still resets the baseline (bytes and commit), so the
/// commit backstop waits another N commits instead of retrying every save.
#[test]
fn automatic_trigger_keeps_original_when_rebuild_is_not_smaller() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    let head = head_commit(&db).unwrap();
    let bytes = std::fs::metadata(&db).unwrap().len();
    let content = std::fs::read(&db).unwrap();

    // The growth gate fires (a tiny recorded baseline) on a file that is
    // already densely packed.
    write_sidecar(&db, &format!("bytes=1\ncommit={head}\n"));
    let r = compact_if_due(&db, CompactionPolicy::default(), Some(head)).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Growth));
    assert!(r.kept_original && !r.swapped, "{r:?}");
    assert!(r.bytes_rebuilt >= bytes, "{r:?}");
    assert_eq!((r.bytes_before, r.bytes_after), (bytes, bytes));
    assert!(
        !r.rows_per_table.is_empty(),
        "the rebuild ran and self-checked"
    );
    assert!(
        r.warning
            .as_deref()
            .is_some_and(|w| w.contains("original kept")),
        "{:?}",
        r.warning
    );
    // The original is untouched, and no temp is left behind.
    assert_eq!(
        std::fs::read(&db).unwrap(),
        content,
        "original must be kept"
    );
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".compact."))
        .collect();
    assert!(leftovers.is_empty(), "temp left behind: {leftovers:?}");
    // The baseline is reset to the kept file's size and the head.
    let b = read_baseline(&db).unwrap();
    assert_eq!((b.bytes, b.commit), (bytes, Some(head)));

    // The same through compact_if_needed (the byte gate alone).
    write_sidecar(&db, &format!("bytes=1\ncommit={head}\n"));
    let r = compact_if_needed(&db, 3.0).unwrap();
    assert!(r.kept_original && r.trigger == Some(CompactionTrigger::Growth));
    assert_eq!(read_baseline(&db).unwrap().bytes, bytes);

    // The commit backstop keeps the original only on a LARGER rebuild
    // (`commit_backstop_keeps_original_only_when_rebuild_is_larger`); this
    // equal-size rebuild is swapped in, with a warning, and the baseline
    // restarts from the head either way.
    write_sidecar(&db, &format!("bytes={bytes}\ncommit={}\n", head - 3));
    let r = compact_if_due(&db, commits_only(3), Some(head)).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
    assert_eq!(r.bytes_rebuilt, bytes, "re-compacting a compacted file");
    assert!(r.swapped && !r.kept_original, "{r:?}");
    assert!(
        r.warning
            .as_deref()
            .is_some_and(|w| w.contains("swapped anyway (commit backstop")),
        "{:?}",
        r.warning
    );
    assert_eq!(
        read_baseline(&db).unwrap().commit,
        Some(head),
        "the backstop restarts from the head"
    );
    let r = compact_if_due(&db, commits_only(3), Some(head)).unwrap();
    assert!(r.trigger.is_none(), "no retry on the next save: {r:?}");
    for i in 1..3 {
        write_scalar(&db, "Widget", "size", "Base", "w1", 50.0 + i as f64);
        let h = head_commit(&db).unwrap();
        let r = compact_if_due(&db, commits_only(3), Some(h)).unwrap();
        assert!(r.trigger.is_none(), "{i} commits < N");
    }
    write_scalar(&db, "Widget", "size", "Base", "w1", 60.0);
    let r = compact_if_due(&db, commits_only(3), None).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
}

/// The size guard's rule per trigger (`keeps_original`): growth and
/// no-baseline keep the original when the rebuild is not smaller, the commit
/// backstop only when it is strictly larger, an explicit compact never.
#[test]
fn size_guard_rule_per_trigger() {
    use CompactionTrigger::*;
    // (trigger, keeps on smaller, keeps on equal, keeps on larger)
    for (t, smaller, equal, larger) in [
        (Growth, false, true, true),
        (NoBaseline, false, true, true),
        (Commits, false, false, true),
        (Requested, false, false, false),
    ] {
        assert_eq!(keeps_original(t, 99, 100), smaller, "{t:?} smaller");
        assert_eq!(keeps_original(t, 100, 100), equal, "{t:?} equal");
        assert_eq!(keeps_original(t, 101, 100), larger, "{t:?} larger");
    }
}

/// The commit backstop on a rebuild that is strictly larger than the source
/// keeps the original (and resets the baseline); the same source under the
/// growth trigger keeps it too. The fixture: a few checkpointed commits after
/// a compaction leave a source that DuckDB truncated below its rebuild
/// (measured: 9 187 328 vs 10 498 048 bytes at DuckDB 1.5.5).
#[test]
fn commit_backstop_keeps_original_only_when_rebuild_is_larger() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    for i in 0..3 {
        write_scalar(&db, "Widget", "size", "Base", "w1", 10.0 + i as f64);
        checkpoint(&db);
    }
    let head = head_commit(&db).unwrap();
    let bytes = std::fs::metadata(&db).unwrap().len();
    let content = std::fs::read(&db).unwrap();

    let r = compact_if_due(&db, commits_only(3), Some(head)).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Commits));
    assert!(
        r.bytes_rebuilt > bytes,
        "fixture drift: the rebuild is no longer larger than the source: {r:?}"
    );
    assert!(r.kept_original && !r.swapped, "{r:?}");
    assert_eq!((r.bytes_before, r.bytes_after), (bytes, bytes));
    assert!(
        r.warning
            .as_deref()
            .is_some_and(|w| w.contains("original kept")),
        "{:?}",
        r.warning
    );
    assert_eq!(std::fs::read(&db).unwrap(), content, "original kept");
    let b = read_baseline(&db).unwrap();
    assert_eq!((b.bytes, b.commit), (bytes, Some(head)));

    // A source smaller than its rebuild by any margin is kept under growth.
    write_sidecar(&db, &format!("bytes=1\ncommit={head}\n"));
    let r = compact_if_due(&db, CompactionPolicy::default(), Some(head)).unwrap();
    assert_eq!(r.trigger, Some(CompactionTrigger::Growth));
    assert!(r.kept_original, "{r:?}");
}

/// An explicit compact swaps even when the rebuild is not smaller, and says
/// so in the report's warning.
#[test]
fn explicit_compact_swaps_anyway_with_a_warning() {
    let dir = tempfile::tempdir().unwrap();
    let db = baselined(dir.path());
    let head = head_commit(&db).unwrap();
    // Mark the current file so the swap is observable without inode APIs.
    let marker = dir.path().join("pre-swap.duckdb");
    std::fs::hard_link(&db, &marker).unwrap();
    let r = compact(&db).expect("re-compact a compacted file");
    assert_eq!(r.trigger, Some(CompactionTrigger::Requested));
    assert!(r.swapped && !r.kept_original, "{r:?}");
    assert!(r.bytes_rebuilt >= r.bytes_before, "{r:?}");
    assert_eq!(r.bytes_after, r.bytes_rebuilt);
    assert!(
        r.warning
            .as_deref()
            .is_some_and(|w| w.contains("swapped anyway")),
        "{:?}",
        r.warning
    );
    // The old inode now has only the marker link: the file was replaced.
    let replaced = !same_file(&db, &marker);
    assert!(replaced, "the file must have been replaced");
    let b = read_baseline(&db).unwrap();
    assert_eq!((b.bytes, b.commit), (r.bytes_after, Some(head)));
}

/// Same inode (the rename-over replacement is observable on unix; elsewhere
/// this check is skipped).
fn same_file(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let (ma, mb) = (std::fs::metadata(a).unwrap(), std::fs::metadata(b).unwrap());
        ma.ino() == mb.ino() && ma.dev() == mb.dev()
    }
    #[cfg(not(unix))]
    {
        let _ = (a, b);
        false
    }
}
