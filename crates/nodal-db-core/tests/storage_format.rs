//! Native-storage-format guard (DuckDB 2.0 upgrade, risk R8).
//!
//! Every file the core creates must be in DuckDB's native 2.0 storage format
//! (`duckdb_databases().tags['storage_version'] = 'v2.0.0+'`). That rests on a
//! single engine default (`storage_compatibility_version = latest`) which
//! nothing in the repo sets. It matters for performance, not correctness: on a
//! 1.5-format file every write to `value_array` (a table with a primary key
//! and a list column) logs the whole list column to the WAL, and the commit
//! then stalls on an automatic checkpoint. A config line, a duckdb-rs default
//! change or a stale engine would bring that back without any functional test
//! failing, so these tests pin it down:
//!
//! - every core create path yields the exact `v2.0.0+` tag (a future 2.x
//!   format bump fails here loudly and is re-baselined on purpose);
//! - an array replace through the core's write path (`history::write_value`)
//!   on a native file logs a small WAL, with a negative
//!   control on a `v1.5.0`-compat file in the same test so the guard can
//!   never pass vacuously.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{Config, Connection};

use nodal_db_core::api::write_parameter;
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::compaction::compact;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::{
    apply_schema_from, init_schema, init_schema_from_sources_with_default, schema_dir,
};
use nodal_db_core::types::{ShapedData, Tier, ValueKey, ValueKind, WriteOptions};

/// The storage tag DuckDB 2.0 gives a file created in its native format.
const NATIVE_TAG: &str = "v2.0.0+";

/// Array size for the WAL test: 200 × 8 760 floats ≈ 13 MiB of list payload,
/// enough that the 1.5 format logs ≥ 13 MiB for a single-row replace
/// (measured scaling: 1.3 MiB at 20 rows, 13.4 at 200, 133.7 at 2 000).
const ARRAYS: usize = 200;
const CELLS: usize = 8_760;
const T0_US: i64 = 1_704_067_200_000_000; // 2024-01-01T00:00:00
const HOUR_US: i64 = 3_600_000_000;

/// Native-file bound for the single-row replace WAL (measured ~0.14 MiB
/// through `history::write_value`; ~0.07 MiB for a bare list UPDATE).
const NATIVE_WAL_MAX: u64 = 1 << 20;
/// Negative-control floor on a `v1.5.0`-compat file (measured ~13 MiB).
const COMPAT_WAL_MIN: u64 = 10 << 20;

fn storage_tag(db: &Path) -> String {
    let conn = Connection::open_with_flags(
        db,
        Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .unwrap(),
    )
    .expect("open read-only");
    let tag: String = conn
        .query_row(
            "SELECT tags['storage_version'] FROM duckdb_databases() \
             WHERE database_name = current_database()",
            [],
            |r| r.get(0),
        )
        .expect("storage_version tag");
    conn.close().unwrap();
    tag
}

/// The negative controls really are old-format files (a `v1.5.0`-compat
/// file is tagged `v1.5.0+`; a file 1.5.5 created by default is `v1.0.0+`).
fn assert_v15(db: &Path) {
    let tag = storage_tag(db);
    assert!(
        tag.starts_with("v1."),
        "control: expected a 1.x-format file, got storage tag {tag}"
    );
}

/// Create an empty file with the schema in the 1.5 storage format (the
/// pre-2.0 default), the way an old engine or a stray config line would.
fn init_compat_v15(db: &Path) {
    let conn = Connection::open_with_flags(
        db,
        Config::default()
            .with("storage_compatibility_version", "v1.5.0")
            .unwrap(),
    )
    .expect("open v1.5.0-compat");
    apply_schema_from(&conn, &schema_dir()).expect("apply schema");
    conn.close().unwrap();
}

fn schema_sources() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = fs::read_dir(schema_dir())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().is_some_and(|x| x == "sql")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(|c: char| c.is_ascii_digit()))
        })
        .map(|p| {
            (
                p.file_name().unwrap().to_str().unwrap().to_string(),
                fs::read_to_string(&p).unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

/// One logical-long hourly float batch for `n` entities (Tier A: full grid).
fn arrays_batch(n: usize) -> RecordBatch {
    let mut ent = Vec::with_capacity(n * CELLS);
    let mut ts = Vec::with_capacity(n * CELLS);
    let mut val = Vec::with_capacity(n * CELLS);
    for e in 0..n {
        let name = format!("a{e:04}");
        for h in 0..CELLS {
            ent.push(name.clone());
            ts.push(T0_US + h as i64 * HOUR_US);
            val.push(((e * CELLS + h) % 977) as f64 * 1.7 - 800.0);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts)) as ArrayRef,
            Arc::new(Float64Array::from(val)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write_arrays(db: &Path, n: usize) {
    write_parameter(
        db,
        "Series",
        "profile",
        "Base",
        arrays_batch(n),
        &[],
        WriteOptions::default(),
        "tester",
        "storage-format test arrays",
    )
    .expect("array write");
}

/// Write ONE array through the core API (a canonical row with real axes),
/// then bulk-copy it raw to `ARRAYS` rows with distinct entity ids and
/// payloads. The core API costs ~1.5 s per 8 760-cell array in a debug build
/// (~5 min for 200), too slow for the quick tier; the bulk rows only supply
/// list payload volume for the WAL measurement. Test-only raw SQL on a
/// scratch DB — not a writer path (CLAUDE.md invariant 2 does not apply).
/// `array_id` is left to the column DEFAULT (`BY NAME`, excluded from the
/// copy): the sequence is the only minter.
fn fill_arrays(db: &Path) {
    write_arrays(db, 1);
    let conn = Connection::open(db).expect("open rw");
    conn.execute_batch(&format!(
        "INSERT INTO value_array BY NAME
           SELECT va.* EXCLUDE (array_id) REPLACE (va.entity_id + r.k AS entity_id,
                                list_transform(va.values, lambda v: v + r.k) AS values)
           FROM value_array va, range(1, {ARRAYS}) r(k);
         CHECKPOINT;"
    ))
    .expect("bulk copy arrays");
    conn.close().unwrap();
}

fn wal_len(db: &Path) -> u64 {
    let mut wal = db.as_os_str().to_owned();
    wal.push(".wal");
    fs::metadata(PathBuf::from(wal))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Replace ONE array through the core's own write path —
/// `history::write_value`, the function every API write funnels into (it
/// issues the real pre-image copy into `value_array_hist` and the real
/// `UPDATE value_array` statement) — on a raw connection with checkpointing
/// deferred, and return the WAL size BEFORE close (close checkpoints the WAL
/// away, which is why the `api::` entry points, which close their own
/// connection, cannot be measured directly). The raw connection only
/// replaces `conn::with_write_txn`'s open/BEGIN/commit-row/COMMIT wrapper;
/// the bytes measured are the core's value_array writes.
fn replace_one_row_wal(db: &Path) -> u64 {
    let conn = Connection::open(db).expect("open rw");
    conn.execute_batch("SET checkpoint_threshold = '1TB'")
        .unwrap();
    let before = wal_len(db);
    assert!(
        before < NATIVE_WAL_MAX,
        "WAL not empty before the measured replace: {before} bytes"
    );
    // The core-written row (the bulk copies have synthetic entity ids).
    let (key, pt_id): (ValueKey, Option<i64>) = conn
        .query_row(
            "SELECT entity_id, parameter_definition_id, alternative_id, parameter_type_id \
             FROM value_array WHERE rowid = (SELECT min(rowid) FROM value_array)",
            [],
            |r| {
                Ok((
                    ValueKey {
                        entity_id: r.get(0)?,
                        parameter_definition_id: r.get(1)?,
                        alternative_id: r.get(2)?,
                    },
                    r.get(3)?,
                ))
            },
        )
        .unwrap();
    let value = CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![CanonicalAxis {
            length: CELLS as i32,
            properties: AxisProperties::default(),
            body: AxisBody::IntegerParametric { start: 0, step: 1 },
        }],
        shape: ShapedData::Array((0..CELLS).map(|h| h as f64 * 0.5 + 1.0).collect()),
    };
    conn.execute_batch("BEGIN").unwrap();
    let c: i64 = conn
        .query_row(
            "INSERT INTO commit (user, message) VALUES ('tester', 'wal probe') RETURNING id",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let outcome = write_value(
        &conn,
        c,
        key,
        &value,
        &mut AxisCache::new(),
        &WriteOptions::default(),
        pt_id,
    )
    .expect("core array replace");
    conn.execute_batch("COMMIT").unwrap();
    assert_eq!(
        (outcome.tier, outcome.rows_written, outcome.rows_closed),
        (Some(Tier::Array), 1, 1),
        "the measured write was not a same-tier array replace"
    );
    let n_arrays: i64 = conn
        .query_row("SELECT count(*) FROM value_array", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n_arrays, ARRAYS as i64, "array rows were not all written");
    let wal = wal_len(db);
    conn.close().unwrap();
    wal
}

#[test]
fn init_schema_creates_native_format() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("init.duckdb");
    init_schema(&db).expect("init");
    assert_eq!(storage_tag(&db), NATIVE_TAG);
}

#[test]
fn init_schema_from_sources_creates_native_format() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("sources.duckdb");
    let sources = schema_sources();
    let refs: Vec<(&str, &str)> = sources
        .iter()
        .map(|(n, s)| (n.as_str(), s.as_str()))
        .collect();
    init_schema_from_sources_with_default(&db, &refs, Some("Base")).expect("init");
    assert_eq!(storage_tag(&db), NATIVE_TAG);
}

/// Compaction rebuilds into a temp file created through `init_schema`, so its
/// output is native even when the source is a 1.5-format file — the one
/// supported way to move an old file onto the fast commit path.
#[test]
fn compaction_output_is_native_even_from_v15_source() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("compact.duckdb");
    init_compat_v15(&db);
    write_arrays(&db, 1);
    assert_v15(&db);
    let report = compact(&db).expect("compact");
    assert!(report.swapped, "forced compaction did not swap");
    assert_eq!(storage_tag(&db), NATIVE_TAG);
}

#[test]
fn wal_stays_small_on_array_replace() {
    let dir = tempfile::tempdir().unwrap();

    // Native file, created by the core.
    let native = dir.path().join("native.duckdb");
    init_schema(&native).expect("init");
    fill_arrays(&native);
    assert_eq!(storage_tag(&native), NATIVE_TAG);
    let native_wal = replace_one_row_wal(&native);

    // Negative control: the same DB in the 1.5 storage format. If a later
    // 2.0.x stops bloating old-format files, delete this control and say so
    // in the commit; do not loosen the native assertion.
    let compat = dir.path().join("compat.duckdb");
    init_compat_v15(&compat);
    fill_arrays(&compat);
    assert_v15(&compat);
    let compat_wal = replace_one_row_wal(&compat);

    eprintln!("single-row array replace WAL: native {native_wal} B, v1.5.0-compat {compat_wal} B");
    assert!(
        compat_wal >= COMPAT_WAL_MIN,
        "negative control: v1.5.0-compat file logged only {compat_wal} B (expected >= {COMPAT_WAL_MIN}); \
         the measurement no longer distinguishes the formats"
    );
    assert!(
        native_wal > 0,
        "native file logged no WAL at all: the measurement did not observe the commit"
    );
    assert!(
        native_wal < NATIVE_WAL_MAX,
        "native file logged {native_wal} B of WAL for a single-row array replace \
         (expected < {NATIVE_WAL_MAX}); has the 1.5-format WAL behaviour come back?"
    );
}
