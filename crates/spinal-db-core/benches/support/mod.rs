//! Bench support: the synthetic-DB builder for the pre-freeze benchmark gate
//! (rust_implementation_plan.md Part 5 deliverable 4), mirroring the
//! ddl_sketch.md §10 methodology:
//!
//!   - ~1.23 M live long-tier rows at ~17% dead   (140 entities × 8760 int
//!     cells = 1 226 400 live; 30 entity-rewrites → 262 800 dead → 17.6%)
//!   - 1 000 × 8 760-cell float arrays (~70 MB payload, Tier A)
//!   - 100 000 single-parameter scalars
//!
//! Everything is built THROUGH the core write path (`api::write_parameter` →
//! `ingest_segments` → canon → tier → axis dedup → history) — dogfooding the
//! writer is the point, and the build wall time IS the bulk-write benchmark
//! number recorded in RESULTS.md. No SQL bypasses the canonicalization
//! pipeline (CLAUDE.md invariant 2).
//!
//! The DB lives in a `spinal-bench-<pid>` dir under the system temp dir
//! (disk is tight: ~150-200 MB, removed by `cleanup()` from the bench
//! binary's custom `main`; stale dirs from crashed runs are swept on start).
//!
//! Chunking rationale (measured behavior, not style): long-tier writes issue a
//! per-cell `MAX(version)` probe. Inside one transaction those probes scan the
//! transaction-local (un-zonemapped) rows, so one-entity-per-commit keeps each
//! probe on ≤ 8 760 local rows; committed row groups are zonemap-pruned by the
//! monotone `entity_id`. Batching many entities per commit measured
//! quadratically worse. That per-cell probe cost is itself a gate finding.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, AccessMode, Config, Connection};

use spinal_db_core::api::write_parameter;
use spinal_db_core::schema_init::init_schema;
use spinal_db_core::types::WriteOptions;

// --- ddl_sketch §10 scale -------------------------------------------------

pub const LONG_ENTITIES: usize = 140; // × 8760 = 1 226 400 live long rows
pub const CHURN_REWRITES: usize = 30; // × 8760 = 262 800 dead → 17.6% dead
pub const ARRAY_ENTITIES: usize = 1000; // × 8760 × 8 B ≈ 70 MB
pub const SCALAR_ENTITIES: usize = 100_000;
pub const CELLS: usize = 8760; // hourly year

pub const ALT: &str = "Base";
pub const USER: &str = "bench";

/// 2025-01-01T00:00:00Z in µs — the hourly axis origin.
const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;

// --- shared state ----------------------------------------------------------

/// Key-triple ids for the point-read benches (mid-table picks).
pub struct PointKeys {
    pub scalar: (i64, i64, i64),
    /// (entity, pd, alt, i1_pos) — one long-tier cell.
    pub long: (i64, i64, i64, i32),
    pub array: (i64, i64, i64),
    /// pd id of the long parameter, for the raw current-scan benches.
    pub long_pd: i64,
}

// Fields are reported via the builder's eprintln (transcribed into
// RESULTS.md), so most are never read back in code.
#[allow(dead_code)]
pub struct BuildTimings {
    pub scalars: Duration,
    pub arrays: Duration,
    pub long_live: Duration,
    pub churn: Duration,
    pub total: Duration,
    /// Raw long-tier current scan at 0% dead (median of 5), measured in the
    /// builder before churn — the denominator of the degradation ratio.
    pub long_scan_zero_dead_ms: f64,
    pub live_long: i64,
    pub dead_long: i64,
    pub db_bytes: u64,
    pub duckdb_version: String,
    /// Fresh read-only open + `SELECT 1` (median of 5) — context for the
    /// per-operation connection discipline; §10 numbers are warm-connection.
    pub ro_open_ms: f64,
}

pub struct Synthetic {
    pub dir: PathBuf,
    pub db: PathBuf,
    pub keys: PointKeys,
    pub timings: BuildTimings,
}

static SYNTH: OnceLock<Synthetic> = OnceLock::new();

/// Fresh-name counter for benches that create new entities per iteration.
pub static WRITE_SEQ: AtomicU64 = AtomicU64::new(0);

pub fn next_seq() -> u64 {
    WRITE_SEQ.fetch_add(1, Ordering::Relaxed)
}

pub fn synthetic() -> &'static Synthetic {
    SYNTH.get_or_init(build)
}

/// Remove the synthetic DB dir. Called from the bench binary's custom `main`
/// after the criterion summary (a static `OnceLock` never drops, so a Drop
/// guard cannot do this).
pub fn cleanup() {
    if let Some(s) = SYNTH.get() {
        let _ = fs::remove_dir_all(&s.dir);
    }
}

// --- Arrow batch builders ---------------------------------------------------

fn scalar_batch(names: &[String], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let ent = Arc::new(StringArray::from_iter_values(names.iter().map(String::as_str))) as ArrayRef;
    let val = Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, val]).unwrap()
}

/// Logical-long rank-1 hourly batch: every entity carries the full 8760-hour
/// grid. Float measure → Tier A (full Cartesian grid); int measure → Tier L.
fn series_batch(names: &[String], float_vals: Option<&[f64]>, int_vals: Option<&[i64]>) -> RecordBatch {
    let n = names.len() * CELLS;
    let mut ent = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    for name in names {
        for h in 0..CELLS {
            ent.push(name.as_str());
            ts.push(T0_US + h as i64 * HOUR_US);
        }
    }
    let (vfield, val): (Field, ArrayRef) = match (float_vals, int_vals) {
        (Some(v), None) => (
            Field::new("value", DataType::Float64, false),
            Arc::new(Float64Array::from(v.to_vec())),
        ),
        (None, Some(v)) => (
            Field::new("value", DataType::Int64, false),
            Arc::new(Int64Array::from(v.to_vec())),
        ),
        _ => unreachable!("exactly one measure column"),
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("time", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        vfield,
    ]));
    let ent = Arc::new(StringArray::from_iter_values(ent)) as ArrayRef;
    let ts = Arc::new(TimestampMicrosecondArray::from(ts)) as ArrayRef;
    RecordBatch::try_new(schema, vec![ent, ts, val]).unwrap()
}

pub fn float_series(seed: usize) -> Vec<f64> {
    (0..CELLS).map(|h| ((seed * 8760 + h) % 977) as f64 * 1.7 - 800.0).collect()
}

pub fn int_series(seed: usize) -> Vec<i64> {
    (0..CELLS).map(|h| ((seed as i64) * 100_000 + h as i64) % 65_536).collect()
}

/// One-entity float-array write (Tier A replace path in the write benches).
pub fn write_array_entity(db: &Path, name: &str, seed: usize) {
    let batch = series_batch(&[name.to_string()], Some(&float_series(seed)), None);
    write_parameter(db, "bench_array", "p_series", ALT, batch, &[], WriteOptions::default(), USER, "bench array write")
        .expect("array write");
}

/// One-entity int-series write (Tier L create/replace path; a replace closes
/// out all 8760 live cells and re-inserts them — the churn primitive).
pub fn write_long_entity(db: &Path, name: &str, seed: usize) {
    let batch = series_batch(&[name.to_string()], None, Some(&int_series(seed)));
    write_parameter(db, "bench_long", "p_long", ALT, batch, &[], WriteOptions::default(), USER, "bench long write")
        .expect("long write");
}

/// N-entity scalar write in one call (the per-entity-cost bench primitive).
pub fn write_scalars(db: &Path, names: &[String], vals: &[f64], message: &str) {
    let batch = scalar_batch(names, vals);
    write_parameter(db, "bench_scalar", "p_scalar", ALT, batch, &[], WriteOptions::default(), USER, message)
        .expect("scalar write");
}

// --- connections / measurement helpers --------------------------------------

pub fn open_ro(db: &Path) -> Connection {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    Connection::open_with_flags(db, config).expect("RO open")
}

fn open_rw(db: &Path) -> Connection {
    Connection::open(db).expect("RW open")
}

/// `CHECKPOINT` through a short-lived RW connection (the write benches' cycle
/// tail; `conn::with_write_txn` deliberately does not checkpoint yet).
pub fn checkpoint(db: &Path) {
    let conn = open_rw(db);
    conn.execute_batch("CHECKPOINT").expect("checkpoint");
    conn.close().map_err(|(_, e)| e).expect("close");
}

/// Run a parameterized query end-to-end to Arrow record batches; returns the
/// row count (the client-visible-Arrow endpoint the VMP budgets are defined at).
pub fn arrow_rows(conn: &Connection, sql: &str, args: &[&dyn duckdb::ToSql]) -> usize {
    let mut stmt = conn.prepare(sql).expect("prepare");
    let arrow = stmt.query_arrow(args).expect("query_arrow");
    arrow.map(|b| b.num_rows()).sum()
}

/// Raw long-tier current scan (from/to filtered), the degradation probe: the
/// §10 raw-scan shape — ids + accelerator + measure, Arrow end-to-end.
pub const LONG_SCAN_SQL: &str = "SELECT vl.entity_id, vl.i1_pos, vl.i1_dt, vl.v_int \
     FROM value_long vl \
     WHERE vl.parameter_definition_id = ? AND vl.valid_to_commit IS NULL";

pub fn long_scan_rows(conn: &Connection, pd: i64) -> usize {
    arrow_rows(conn, LONG_SCAN_SQL, &[&pd])
}

fn median_ms(n: usize, mut f: impl FnMut()) -> f64 {
    let mut ms: Vec<f64> = (0..n)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    ms[n / 2]
}

/// Live/dead long-tier row counts for the long parameter (dead fraction is
/// re-measured at bench time — the write benches themselves add dead rows).
pub fn long_counts(conn: &Connection, pd: i64) -> (i64, i64) {
    let live: i64 = conn
        .query_row(
            "SELECT count(*) FROM value_long WHERE parameter_definition_id=? AND valid_to_commit IS NULL",
            params![pd],
            |r| r.get(0),
        )
        .unwrap();
    let dead: i64 = conn
        .query_row(
            "SELECT count(*) FROM value_long WHERE parameter_definition_id=? AND valid_to_commit IS NOT NULL",
            params![pd],
            |r| r.get(0),
        )
        .unwrap();
    (live, dead)
}

/// Rewrite already-live long entities (each rewrite closes 8 760 rows → +8 760
/// dead) until the dead fraction reaches `target` (e.g. 0.33). Returns the
/// number of rewrites done. Used by the scan-degradation bench's setup.
pub fn churn_long_to_dead_fraction(db: &Path, target: f64) -> usize {
    let s = synthetic();
    let conn = open_ro(db);
    let (live, dead) = long_counts(&conn, s.keys.long_pd);
    drop(conn);
    let need_dead = (live as f64 * target / (1.0 - target)) as i64;
    let mut done = 0usize;
    let mut dead_now = dead;
    let mut round = 100; // seed offset so churn always writes new content
    while dead_now < need_dead {
        let e = done % LONG_ENTITIES;
        write_long_entity(db, &format!("l{e:04}"), round * 1000 + e);
        dead_now += CELLS as i64;
        done += 1;
        round += 1;
    }
    checkpoint(db);
    done
}

// --- the builder -------------------------------------------------------------

fn sweep_stale() {
    let tmp = std::env::temp_dir();
    if let Ok(rd) = fs::read_dir(&tmp) {
        for e in rd.flatten() {
            let name = e.file_name();
            let name = name.to_string_lossy().to_string();
            if name.starts_with("spinal-bench-") {
                let _ = fs::remove_dir_all(e.path());
            }
        }
    }
}

fn build() -> Synthetic {
    sweep_stale();
    let dir = std::env::temp_dir().join(format!("spinal-bench-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("bench tempdir");
    let db = dir.join("synthetic.db");

    eprintln!("[synthetic] building at {} ...", db.display());
    let t_total = Instant::now();
    init_schema(&db).expect("schema init");

    // 100 k scalars, 10 chunks of 10 k entities per commit.
    let t = Instant::now();
    for chunk in 0..10 {
        let names: Vec<String> = (0..10_000).map(|i| format!("s{:06}", chunk * 10_000 + i)).collect();
        let vals: Vec<f64> = (0..10_000).map(|i| (chunk * 10_000 + i) as f64 * 0.5).collect();
        write_scalars(&db, &names, &vals, "synthetic scalars");
        eprintln!("[synthetic] scalars chunk {}/10 done ({:.1}s)", chunk + 1, t.elapsed().as_secs_f64());
    }
    let scalars = t.elapsed();

    // 1 000 × 8 760-cell float arrays (~70 MB), 10 chunks of 100 entities.
    let t = Instant::now();
    for chunk in 0..10 {
        let names: Vec<String> = (0..100).map(|i| format!("a{:04}", chunk * 100 + i)).collect();
        let mut vals: Vec<f64> = Vec::with_capacity(100 * CELLS);
        for i in 0..100 {
            vals.extend(float_series(chunk * 100 + i));
        }
        let batch = series_batch(&names, Some(&vals), None);
        write_parameter(&db, "bench_array", "p_series", ALT, batch, &[], WriteOptions::default(), USER, "synthetic arrays")
            .expect("array chunk");
        eprintln!("[synthetic] arrays chunk {}/10 done ({:.1}s)", chunk + 1, t.elapsed().as_secs_f64());
    }
    let arrays = t.elapsed();

    // 140 × 8 760 int long rows, one entity per commit (see module docs).
    let t = Instant::now();
    for e in 0..LONG_ENTITIES {
        write_long_entity(&db, &format!("l{e:04}"), e);
        if (e + 1) % 20 == 0 {
            eprintln!("[synthetic] long entities {}/{} ({:.1}s)", e + 1, LONG_ENTITIES, t.elapsed().as_secs_f64());
        }
    }
    let long_live = t.elapsed();
    checkpoint(&db);

    // Point/scan ids, then the 0%-dead scan baseline BEFORE churn.
    let conn = open_ro(&db);
    let keys = resolve_keys(&conn);
    let long_scan_zero_dead_ms = median_ms(5, || {
        let rows = long_scan_rows(&conn, keys.long_pd);
        assert_eq!(rows, LONG_ENTITIES * CELLS);
    });
    conn.close().map_err(|(_, e)| e).expect("close");
    eprintln!("[synthetic] long current-scan @ 0% dead: {long_scan_zero_dead_ms:.1} ms");

    // Churn to ~17% dead: rewrite 30 entities once each.
    let t = Instant::now();
    for e in 0..CHURN_REWRITES {
        write_long_entity(&db, &format!("l{e:04}"), 10_000 + e);
        if (e + 1) % 10 == 0 {
            eprintln!("[synthetic] churn {}/{} ({:.1}s)", e + 1, CHURN_REWRITES, t.elapsed().as_secs_f64());
        }
    }
    let churn = t.elapsed();
    checkpoint(&db);
    let total = t_total.elapsed();

    // Environment + state facts for RESULTS.md.
    let ro_open_ms = median_ms(5, || {
        let c = open_ro(&db);
        let one: i64 = c.query_row("SELECT 1", [], |r| r.get(0)).unwrap();
        assert_eq!(one, 1);
    });
    let conn = open_ro(&db);
    let duckdb_version: String = conn.query_row("SELECT version()", [], |r| r.get(0)).unwrap();
    let (live_long, dead_long) = long_counts(&conn, keys.long_pd);
    conn.close().map_err(|(_, e)| e).expect("close");
    let db_bytes = fs::metadata(&db).map(|m| m.len()).unwrap_or(0);

    eprintln!(
        "[synthetic] built in {:.1}s (scalars {:.1}s, arrays {:.1}s, long {:.1}s, churn {:.1}s); \
         long live {live_long} dead {dead_long} ({:.1}% dead); db {} MB; duckdb {duckdb_version}; \
         RO open {ro_open_ms:.1} ms",
        total.as_secs_f64(),
        scalars.as_secs_f64(),
        arrays.as_secs_f64(),
        long_live.as_secs_f64(),
        churn.as_secs_f64(),
        100.0 * dead_long as f64 / (live_long + dead_long) as f64,
        db_bytes / (1024 * 1024),
    );

    Synthetic {
        dir,
        db,
        keys,
        timings: BuildTimings {
            scalars,
            arrays,
            long_live,
            churn,
            total,
            long_scan_zero_dead_ms,
            live_long,
            dead_long,
            db_bytes,
            duckdb_version,
            ro_open_ms,
        },
    }
}

fn id(conn: &Connection, sql: &str, name: &str) -> i64 {
    conn.query_row(sql, params![name], |r| r.get(0)).expect("id lookup")
}

fn resolve_keys(conn: &Connection) -> PointKeys {
    let pd = |class: &str, param: &str| -> i64 {
        conn.query_row(
            "SELECT pd.id FROM parameter_definition pd \
             JOIN entity_class ec ON ec.id = pd.entity_class_id \
             WHERE lower(ec.name)=lower(?) AND lower(pd.name)=lower(?) \
               AND pd.valid_to_commit IS NULL",
            params![class, param],
            |r| r.get(0),
        )
        .expect("pd lookup")
    };
    let ent = "SELECT id FROM entity WHERE lower(name)=lower(?) AND valid_to_commit IS NULL";
    let alt_id: i64 = conn
        .query_row("SELECT id FROM alternative WHERE lower(name)=lower(?)", params![ALT], |r| r.get(0))
        .expect("alt lookup");
    let scalar = (id(conn, ent, "s050000"), pd("bench_scalar", "p_scalar"), alt_id);
    let long_pd = pd("bench_long", "p_long");
    let long = (id(conn, ent, "l0070"), long_pd, alt_id, (CELLS / 2) as i32);
    let array = (id(conn, ent, "a0500"), pd("bench_array", "p_series"), alt_id);
    PointKeys { scalar, long, array, long_pd }
}
