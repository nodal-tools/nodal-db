//! Results-path benchmark: bulk result ingest + windowed reads at
//! solver-output scale, measured through the nodal-db core.
//!
//! Restored from the original harness (results-path findings, 2026-09-18;
//! `specs/results_path_benchmarks_FINDINGS.md`) and extended with per-series
//! read paths (whole / slice / cell / min-max), long-tier reads, a row-group
//! steel-man, net bytes per value, and a `--quick` scale. The raw-DuckDB
//! format comparison (no nodal-db schema, any DuckDB version) lives in
//! `scripts/bench/raw_duckdb_layout.py`; recorded numbers for both are in
//! `scripts/bench/RESULTS_results_path.md`.
//!
//!   1. **Bulk result ingest**, two lanes compared:
//!      (a) the core API — a `write_parameter` loop (one commit per
//!      100-entity chunk, the gate-bench convention), and
//!      (b) the Arrow-IPC import lane — one bulk container driven through the
//!      built `nodal-db import` binary (one commit for the whole container;
//!      includes process spawn + IPC parse).
//!      Shapes: dense hourly year (8760 floats) per (entity, parameter,
//!      alternative) at 1k and 10k series (8.76 M / 87.6 M values), and a
//!      long-tier shape (sparse int series, every 10th hour).
//!   2. **Bind probe** — the 8760-placeholder `list_value(?,…)::DOUBLE[]`
//!      INSERT that `history.rs` generates per array, in isolation (the
//!      statement-building share of the array write).
//!   3. **Reads** on the 10k-series DBs: one week (168 points) x 20 series
//!      via four SQL shapes, plus one series' whole value / week slice /
//!      single cell / min-max only. Cold (fresh per-operation RO connection,
//!      invariant 1; open timed separately) and warm.
//!   4. **Long-tier reads** on the long-shape DB (cell / window / series /
//!      min-max / canonical `read_parameter`).
//!   5. **`value_array_cells` gate** (`cellsgate`): the bulk canonical
//!      `read_parameter` with the single-branch array_id self-join cells view
//!      vs the old two-branch view, as paired interleaved A/B in one process
//!      on one file (median per-pair ratio + bootstrap CI vs 1.10).
//!   6. **Row-group steel-man** — the 10k DB's `value_array` copied into
//!      scratch files at the default and a small ROW_GROUP_SIZE; same reads.
//!
//! Every DB write goes through the one canonicalization pipeline (CLAUDE.md
//! invariant 2): lane (a) calls `api::write_parameter`, lane (b) drives the
//! `import` verb (`api::ingest_segments`). The row-group phase copies an
//! already-written table into a *scratch* raw-DuckDB file for reading only;
//! it never writes a nodal-db database. Data is deterministic (splitmix64,
//! fixed seed); `scripts/bench/raw_duckdb_layout.py` generates bit-identical
//! float values.
//!
//! Run (release; ~30 min for `all` at full scale, dominated by the two
//! 87.6 M-value ingests and the long-tier ingests; `--quick` is ~1-2 min):
//!
//!     cargo bench -p nodal-db-cli --bench results_path -- all
//!     cargo bench -p nodal-db-cli --bench results_path -- all --quick
//!     cargo bench -p nodal-db-cli --bench results_path -- dense1k bindprobe
//!     cargo bench -p nodal-db-cli --bench results_path -- dense10k reads rowgroup
//!     cargo bench -p nodal-db-cli --bench results_path -- long longreads
//!
//! With NO phase argument the bench prints usage and exits, so a bare
//! workspace `cargo bench` (the gate) does not start a half-hour run.
//! Phases: dense1k, dense10k, dense10k-api, dense10k-ipc, long, longreads,
//! bindprobe, reads, cellsgate, rowgroup. `reads`/`cellsgate`/`rowgroup` need
//! the 10k DBs: run them with `dense10k` (as `all` does), or across invocations via
//! NODAL_RESULTS_DIR=<fixed workdir> + NODAL_RESULTS_KEEP=1 on the ingest
//! run, then NODAL_RESULTS_DB=<kept .db> (and NODAL_RESULTS_LONG_DB for
//! `longreads`) on the read run.
//! `[[bench]] test = false` keeps this out of `cargo test --workspace`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, StringArray, TimestampMicrosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use nodal_db_core::api::write_parameter;
use nodal_db_core::duckdb::types::Value;
use nodal_db_core::duckdb::{params_from_iter, AccessMode, Config, Connection};
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::WriteOptions;

// --- shapes (FlexTool-result scale) ----------------------------------------

const CELLS: usize = 8760; // dense hourly year
const CHUNK: usize = 100; // entities per write_parameter commit / IPC batch

/// Long-tier shape: sparse int series, every 10th hour (876 points/series).
const SPARSE_STRIDE: usize = 10;
const SPARSE_CELLS: usize = CELLS / SPARSE_STRIDE;
const LONG_PARAMS: usize = 1;

const ALT: &str = "run1"; // results-DB convention: one alternative per run
const USER: &str = "bench";
const CLASS_DENSE: &str = "res_dense";
const CLASS_LONG: &str = "res_long";

/// 2025-01-01T00:00:00Z in us — the hourly axis origin.
const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;

/// One-week window for the read benches: hours [4000, 4168) 0-based
/// (mid-June); DuckDB list slicing is 1-based inclusive -> [4001:4168].
const WIN_LO_POS: usize = 4000;
const WIN_PTS: usize = 168;
const READ_SERIES: usize = 20;

const SEED: u64 = 0x006e_6f64_616c_2025; // deterministic data

/// Run scale. Full = the recorded shapes; quick = a smoke-sized run that
/// exercises every phase in a minute or two (numbers not comparable).
struct Scale {
    quick: bool,
    /// Entities per parameter (and long-shape series count).
    entities: usize,
    /// Parameters in the "1k" and "10k" dense phases.
    small_params: usize,
    big_params: usize,
    /// Fresh-DB repetitions for the 1k and long ingests (10k runs once).
    reps_small: usize,
    reps_long: usize,
    cold_reps: usize,
    warm_reps: usize,
}

impl Scale {
    fn full() -> Self {
        Scale {
            quick: false,
            entities: 1000,
            small_params: 1,
            big_params: 10,
            reps_small: 2,
            reps_long: 2,
            cold_reps: 5,
            warm_reps: 15,
        }
    }
    fn quick() -> Self {
        Scale {
            quick: true,
            entities: 100,
            small_params: 1,
            big_params: 2,
            reps_small: 1,
            reps_long: 1,
            cold_reps: 3,
            warm_reps: 5,
        }
    }
}

// --- deterministic data -----------------------------------------------------

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// Dense float cell value for (param p, entity e, hour h) — full f64 noise in
/// [-100, 100) (worst case for compression; solver outputs are not smooth).
/// `scripts/bench/raw_duckdb_layout.py` reproduces this bit-for-bit in SQL.
fn fval(p: usize, e: usize, h: usize) -> f64 {
    let r = splitmix64(SEED ^ ((p as u64) << 40) ^ ((e as u64) << 20) ^ h as u64);
    (r as f64 / u64::MAX as f64) * 200.0 - 100.0
}

/// Sparse int cell value.
fn ival(p: usize, e: usize, k: usize) -> i64 {
    (splitmix64(!SEED ^ ((p as u64) << 40) ^ ((e as u64) << 20) ^ k as u64) % 100_000) as i64
}

fn ename(e: usize) -> String {
    format!("e{e:04}")
}

fn pname_dense(p: usize) -> String {
    format!("flow{p:02}")
}

// --- timing helpers ---------------------------------------------------------

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

struct Stats {
    min: f64,
    median: f64,
    n: usize,
}

fn stats(mut v: Vec<f64>) -> Stats {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Stats {
        min: v[0],
        median: v[v.len() / 2],
        n: v.len(),
    }
}

fn report(label: &str, s: &Stats) {
    println!(
        "  {label:60} min {:>9.2} ms   median {:>9.2} ms   (n={})",
        s.min, s.median, s.n
    );
}

// --- DB / connection helpers (per-operation, invariant 1) -------------------

fn open_ro(db: &Path) -> Connection {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    Connection::open_with_flags(db, config).expect("RO open")
}

fn close(conn: Connection) {
    conn.close().map_err(|(_, e)| e).expect("close");
}

fn checkpoint(db: &Path) -> f64 {
    let t = Instant::now();
    let conn = Connection::open(db).expect("RW open");
    conn.execute_batch("CHECKPOINT").expect("checkpoint");
    close(conn);
    ms(t)
}

fn file_bytes(p: &Path) -> u64 {
    fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// (count(*), sum(len(values))) over value_array — ingest sanity check.
fn array_counts(db: &Path) -> (i64, i64) {
    let conn = open_ro(db);
    let r = conn
        .query_row(
            "SELECT count(*), coalesce(sum(len(\"values\")), 0)::BIGINT FROM value_array",
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )
        .expect("array counts");
    close(conn);
    r
}

fn long_live_count(db: &Path) -> i64 {
    let conn = open_ro(db);
    let n = conn
        .query_row(
            "SELECT count(*) FROM value_long WHERE valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )
        .expect("long count");
    close(conn);
    n
}

/// Size of a freshly initialized, checkpointed, empty-schema DB: subtracted
/// from ingest DB sizes to get net bytes per value.
fn baseline_bytes(dir: &Path) -> u64 {
    let db = dir.join("baseline_empty.db");
    init_schema(&db).expect("schema init");
    checkpoint(&db);
    let b = file_bytes(&db);
    let _ = fs::remove_file(&db);
    b
}

// --- Arrow batch builders ---------------------------------------------------

/// Logical-long float batch for the core API lane: (entity Utf8, time ts[us],
/// value f64) — every entity carries the full 8760-hour grid (-> Tier A).
fn api_dense_batch(p: usize, e_lo: usize, e_hi: usize) -> RecordBatch {
    let n = (e_hi - e_lo) * CELLS;
    let mut ent: Vec<String> = Vec::with_capacity(n);
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut val: Vec<f64> = Vec::with_capacity(n);
    for e in e_lo..e_hi {
        let name = ename(e);
        for h in 0..CELLS {
            ent.push(name.clone());
            ts.push(T0_US + h as i64 * HOUR_US);
            val.push(fval(p, e, h));
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

/// Logical-long sparse int batch (every SPARSE_STRIDE-th hour -> Tier L).
fn api_sparse_batch(p: usize, e_lo: usize, e_hi: usize) -> RecordBatch {
    let n = (e_hi - e_lo) * SPARSE_CELLS;
    let mut ent: Vec<String> = Vec::with_capacity(n);
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut val: Vec<i64> = Vec::with_capacity(n);
    for e in e_lo..e_hi {
        let name = ename(e);
        for k in 0..SPARSE_CELLS {
            ent.push(name.clone());
            ts.push(T0_US + (k * SPARSE_STRIDE) as i64 * HOUR_US);
            val.push(ival(p, e, k));
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts)) as ArrayRef,
            Arc::new(Int64Array::from(val)) as ArrayRef,
        ],
    )
    .unwrap()
}

// --- Arrow-IPC bulk container builders (specs/arrow_ipc_bulk_format.md) -----

fn seg_metadata(
    n_segments: usize,
    class: &str,
    parameter: &str,
    form: &str,
) -> HashMap<String, String> {
    HashMap::from([
        ("spinal:format".into(), "spinal-db-bulk".into()),
        ("spinal:format_version".into(), "1.0".into()),
        ("spinal:segment_count".into(), n_segments.to_string()),
        ("spinal:class".into(), class.to_string()),
        ("spinal:parameter".into(), parameter.to_string()),
        ("spinal:alternative".into(), ALT.into()),
        ("spinal:form".into(), form.to_string()),
    ])
}

/// Dense-form wire batch: one row per entity — entity Utf8, time List<ts[us]>
/// (the shared hourly axis, repeated per row by spec), value List<f64>.
fn ipc_dense_batch(schema: Arc<Schema>, p: usize, e_lo: usize, e_hi: usize) -> RecordBatch {
    let rows = e_hi - e_lo;
    let ent: Vec<String> = (e_lo..e_hi).map(ename).collect();

    let mut ts_flat: Vec<i64> = Vec::with_capacity(rows * CELLS);
    let mut val_flat: Vec<f64> = Vec::with_capacity(rows * CELLS);
    for e in e_lo..e_hi {
        for h in 0..CELLS {
            ts_flat.push(T0_US + h as i64 * HOUR_US);
            val_flat.push(fval(p, e, h));
        }
    }
    let ts_item = Arc::new(Field::new(
        "item",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    ));
    let val_item = Arc::new(Field::new("item", DataType::Float64, false));
    let offsets = OffsetBuffer::<i32>::from_lengths(std::iter::repeat_n(CELLS, rows));
    let time_col = ListArray::new(
        ts_item,
        offsets.clone(),
        Arc::new(TimestampMicrosecondArray::from(ts_flat)) as ArrayRef,
        None,
    );
    let val_col = ListArray::new(
        val_item,
        offsets,
        Arc::new(Float64Array::from(val_flat)) as ArrayRef,
        None,
    );

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(time_col) as ArrayRef,
            Arc::new(val_col) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Write the dense-shape container: one segment per parameter, batches of
/// CHUNK entities. Returns build wall ms.
fn build_dense_container(path: &Path, n_params: usize, entities: usize) -> f64 {
    let t = Instant::now();
    let mut file = fs::File::create(path).expect("container create");
    for p in 0..n_params {
        let ts_item = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ));
        let val_item = Arc::new(Field::new("item", DataType::Float64, false));
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("entity", DataType::Utf8, false),
                Field::new("time", DataType::List(ts_item), false),
                Field::new("value", DataType::List(val_item), false),
            ])
            .with_metadata(seg_metadata(
                n_params,
                CLASS_DENSE,
                &pname_dense(p),
                "dense",
            )),
        );
        let mut w = StreamWriter::try_new(&mut file, &schema).expect("stream writer");
        let mut e = 0;
        while e < entities {
            let hi = (e + CHUNK).min(entities);
            w.write(&ipc_dense_batch(schema.clone(), p, e, hi))
                .expect("ipc write");
            e = hi;
        }
        w.finish().expect("ipc finish");
    }
    ms(t)
}

/// Write the long-shape container (form=long, sparse int series).
fn build_long_container(path: &Path, entities: usize) -> f64 {
    let t = Instant::now();
    let mut file = fs::File::create(path).expect("container create");
    for p in 0..LONG_PARAMS {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("entity", DataType::Utf8, false),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("value", DataType::Int64, false),
            ])
            .with_metadata(seg_metadata(LONG_PARAMS, CLASS_LONG, "state", "long")),
        );
        let mut w = StreamWriter::try_new(&mut file, &schema).expect("stream writer");
        let mut e = 0;
        while e < entities {
            let hi = (e + CHUNK).min(entities);
            // Same logical rows as the API lane, re-stamped with the
            // metadata-carrying schema.
            let b = api_sparse_batch(p, e, hi);
            let b = RecordBatch::try_new(schema.clone(), b.columns().to_vec()).unwrap();
            w.write(&b).expect("ipc write");
            e = hi;
        }
        w.finish().expect("ipc finish");
    }
    ms(t)
}

// --- ingest lanes -----------------------------------------------------------

struct IngestResult {
    wall_ms: f64,
    chunk_ms: Vec<f64>,
    checkpoint_ms: f64,
    db_bytes: u64,
}

/// Lane (a): core API `write_parameter` loop, CHUNK entities per commit.
fn ingest_api_dense(db: &Path, n_params: usize, entities: usize) -> IngestResult {
    let t = Instant::now();
    let mut chunk_ms = Vec::new();
    for p in 0..n_params {
        let param = pname_dense(p);
        let mut e = 0;
        while e < entities {
            let hi = (e + CHUNK).min(entities);
            let batch = api_dense_batch(p, e, hi);
            let tc = Instant::now();
            write_parameter(
                db,
                CLASS_DENSE,
                &param,
                ALT,
                batch,
                &[],
                WriteOptions::default(),
                USER,
                "bench dense",
            )
            .expect("dense write");
            chunk_ms.push(ms(tc));
            e = hi;
        }
    }
    let wall_ms = ms(t);
    let checkpoint_ms = checkpoint(db);
    IngestResult {
        wall_ms,
        chunk_ms,
        checkpoint_ms,
        db_bytes: file_bytes(db),
    }
}

fn ingest_api_long(db: &Path, entities: usize) -> IngestResult {
    let t = Instant::now();
    let mut chunk_ms = Vec::new();
    for p in 0..LONG_PARAMS {
        let mut e = 0;
        while e < entities {
            let hi = (e + CHUNK).min(entities);
            let batch = api_sparse_batch(p, e, hi);
            let tc = Instant::now();
            write_parameter(
                db,
                CLASS_LONG,
                "state",
                ALT,
                batch,
                &[],
                WriteOptions::default(),
                USER,
                "bench long",
            )
            .expect("long write");
            chunk_ms.push(ms(tc));
            e = hi;
        }
    }
    let wall_ms = ms(t);
    let checkpoint_ms = checkpoint(db);
    IngestResult {
        wall_ms,
        chunk_ms,
        checkpoint_ms,
        db_bytes: file_bytes(db),
    }
}

/// Lane (b): the built `nodal-db import` binary on a container file — one
/// process, one commit for the whole container.
fn ingest_import(db: &Path, container: &Path) -> IngestResult {
    let t = Instant::now();
    let out = Command::new(env!("CARGO_BIN_EXE_nodal-db"))
        .args([
            "import",
            container.to_str().unwrap(),
            "--db",
            db.to_str().unwrap(),
            "--user",
            USER,
            "--message",
            "bench import",
        ])
        .output()
        .expect("run nodal-db import");
    let wall_ms = ms(t);
    if !out.status.success() {
        panic!(
            "import failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let checkpoint_ms = checkpoint(db);
    IngestResult {
        wall_ms,
        chunk_ms: Vec::new(),
        checkpoint_ms,
        db_bytes: file_bytes(db),
    }
}

fn report_ingest(label: &str, r: &IngestResult, series: usize, values: usize, baseline: u64) {
    let s = r.wall_ms / 1e3;
    let net = r.db_bytes.saturating_sub(baseline);
    println!(
        "  {label:30} wall {:>8.1} s  {:>6.1} series/s  {:>8.0} values/s  ckpt {:>5.0} ms  db {:>12} B ({:>7.1} MiB)  {:>6.2} B/value gross  {:>6.2} B/value net",
        s,
        series as f64 / s,
        values as f64 / s,
        r.checkpoint_ms,
        r.db_bytes,
        mib(r.db_bytes),
        r.db_bytes as f64 / values as f64,
        net as f64 / values as f64,
    );
    if !r.chunk_ms.is_empty() {
        let st = stats(r.chunk_ms.clone());
        println!(
            "  {:30} per-{CHUNK}-entity commit: min {:.0} ms  median {:.0} ms  first {:.0} ms  last {:.0} ms",
            "",
            st.min,
            st.median,
            r.chunk_ms[0],
            r.chunk_ms[r.chunk_ms.len() - 1]
        );
    }
}

// --- run context ------------------------------------------------------------

struct Ctx {
    dir: PathBuf,
    scale: Scale,
    baseline: u64,
    /// Keep the rep-0 DBs of the 10k / long phases for the read phases.
    keep_for_reads: bool,
    keep_env: bool,
    reads_dbs: Vec<(String, PathBuf)>,
    long_db: Option<PathBuf>,
    /// Median ms per series of the 1k API lane (for the statement share).
    api_ms_per_series: Option<f64>,
    ipc_ms_per_series: Option<f64>,
}

// --- phase: dense ingest at a given scale -----------------------------------

fn phase_dense_api(ctx: &mut Ctx, n_params: usize, reps: usize, keep: bool) {
    let ents = ctx.scale.entities;
    let series = n_params * ents;
    let values = series * CELLS;
    println!(
        "\n== dense ingest (core API lane): {n_params} param(s) x {ents} entities x {CELLS} h = {series} series, {values} values =="
    );
    for rep in 0..reps {
        let db = ctx.dir.join(format!("api_dense_{n_params}p_r{rep}.db"));
        init_schema(&db).expect("schema init");
        let r = ingest_api_dense(&db, n_params, ents);
        let (rows, cells) = array_counts(&db);
        assert_eq!(
            (rows, cells),
            (series as i64, values as i64),
            "API-lane row/cell count mismatch"
        );
        report_ingest(
            &format!("core API loop (rep {rep})"),
            &r,
            series,
            values,
            ctx.baseline,
        );
        if n_params == ctx.scale.small_params {
            // Best rep: the share is a mechanics ratio, not a noise sample.
            let per = r.wall_ms / series as f64;
            ctx.api_ms_per_series = Some(ctx.api_ms_per_series.map_or(per, |m| m.min(per)));
        }
        if keep && rep == 0 {
            println!("  kept API-lane DB: {}", db.display());
            ctx.reads_dbs
                .push((format!("API-built, {CHUNK}-entity commits"), db));
        } else {
            let _ = fs::remove_file(&db);
        }
    }
}

fn phase_dense_ipc(ctx: &mut Ctx, n_params: usize, reps: usize, keep: bool) {
    let ents = ctx.scale.entities;
    let series = n_params * ents;
    let values = series * CELLS;
    println!(
        "\n== dense ingest (Arrow-IPC lane): {n_params} param(s) x {ents} entities x {CELLS} h = {series} series, {values} values =="
    );
    let container = ctx.dir.join(format!("dense_{n_params}p.arrows"));
    let build_ms = build_dense_container(&container, n_params, ents);
    println!(
        "  container: {:.1} MiB, built in {:.1} s (producer-side cost, not in the import wall)",
        mib(file_bytes(&container)),
        build_ms / 1e3
    );
    for rep in 0..reps {
        let db = ctx.dir.join(format!("ipc_dense_{n_params}p_r{rep}.db"));
        init_schema(&db).expect("schema init");
        let r = ingest_import(&db, &container);
        let (rows, cells) = array_counts(&db);
        assert_eq!(
            (rows, cells),
            (series as i64, values as i64),
            "IPC-lane row/cell count mismatch"
        );
        report_ingest(
            &format!("Arrow-IPC import (rep {rep})"),
            &r,
            series,
            values,
            ctx.baseline,
        );
        if n_params == ctx.scale.small_params {
            let per = r.wall_ms / series as f64;
            ctx.ipc_ms_per_series = Some(ctx.ipc_ms_per_series.map_or(per, |m| m.min(per)));
        }
        if keep && rep == 0 {
            println!("  kept IPC-lane DB: {}", db.display());
            // The one-commit file is the primary read target: put it first.
            ctx.reads_dbs
                .insert(0, ("IPC-built, one commit".into(), db));
        } else {
            let _ = fs::remove_file(&db);
        }
    }
    let _ = fs::remove_file(&container);
}

// --- phase: long-tier ingest ------------------------------------------------

fn phase_long(ctx: &mut Ctx, keep: bool) {
    let ents = ctx.scale.entities;
    let series = LONG_PARAMS * ents;
    let values = series * SPARSE_CELLS;
    println!(
        "\n== long-tier ingest: sparse int, {series} series x {SPARSE_CELLS} points (every {SPARSE_STRIDE}th hour) = {values} cells =="
    );
    for rep in 0..ctx.scale.reps_long {
        let db = ctx.dir.join(format!("api_long_r{rep}.db"));
        init_schema(&db).expect("schema init");
        let r = ingest_api_long(&db, ents);
        assert_eq!(
            long_live_count(&db),
            values as i64,
            "API-lane long row count mismatch"
        );
        report_ingest(
            &format!("core API loop (rep {rep})"),
            &r,
            series,
            values,
            ctx.baseline,
        );
        let _ = fs::remove_file(&db);
    }

    let container = ctx.dir.join("long_sparse.arrows");
    let build_ms = build_long_container(&container, ents);
    println!(
        "  container: {:.1} MiB, built in {:.1} s (producer-side cost, not in the import wall)",
        mib(file_bytes(&container)),
        build_ms / 1e3
    );
    for rep in 0..ctx.scale.reps_long {
        let db = ctx.dir.join(format!("ipc_long_r{rep}.db"));
        init_schema(&db).expect("schema init");
        let r = ingest_import(&db, &container);
        assert_eq!(
            long_live_count(&db),
            values as i64,
            "IPC-lane long row count mismatch"
        );
        report_ingest(
            &format!("Arrow-IPC import (rep {rep})"),
            &r,
            series,
            values,
            ctx.baseline,
        );
        if keep && rep == 0 {
            println!("  kept IPC-lane long DB: {}", db.display());
            ctx.long_db = Some(db);
        } else {
            let _ = fs::remove_file(&db);
        }
    }
    let _ = fs::remove_file(&container);
}

// --- phase: bind probe ------------------------------------------------------

/// Isolates the statement mechanics of an array write (history.rs
/// `list_value_sql`): an 8760-placeholder `list_value(?,...)::DOUBLE[]`
/// INSERT's prepare (parse) and execute (bind + insert) cost. In-memory DB,
/// scratch table — no pipeline. Returns (prepare median, execute median).
fn phase_bindprobe() -> (f64, f64) {
    println!("\n== bind probe: 8760-placeholder list_value INSERT (in-memory scratch table) ==");
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch("CREATE TABLE t(id BIGINT, v DOUBLE[])")
        .expect("create");

    let mut sql = String::from("INSERT INTO t VALUES (?, list_value(");
    for i in 0..CELLS {
        if i > 0 {
            sql.push(',');
        }
        sql.push('?');
    }
    sql.push_str(")::DOUBLE[])");

    // Parse/prepare cost (fresh statement each time).
    let prep = stats(
        (0..10)
            .map(|_| {
                let t = Instant::now();
                let stmt = conn.prepare(&sql).expect("prepare");
                let m = ms(t);
                drop(stmt);
                m
            })
            .collect(),
    );
    report("prepare (parse) of the 8761-placeholder INSERT", &prep);

    // Execute cost on one prepared statement (bind 8761 params + insert).
    let mut stmt = conn.prepare(&sql).expect("prepare");
    let exec = stats(
        (0..30)
            .map(|i| {
                let mut params: Vec<Value> = Vec::with_capacity(CELLS + 1);
                params.push(Value::BigInt(i as i64));
                for h in 0..CELLS {
                    params.push(Value::Double(fval(0, i, h)));
                }
                let t = Instant::now();
                stmt.execute(params_from_iter(params)).expect("execute");
                ms(t)
            })
            .collect(),
    );
    report("execute (bind 8761 params + insert), prepared stmt", &exec);
    (prep.median, exec.median)
}

// --- read helpers -----------------------------------------------------------

/// Run a query end-to-end to Arrow and count materialized points: for a list
/// column 0, the total list length; otherwise the row count. Sums the f64s it
/// can see so materialization cannot be skipped.
fn run_count(conn: &Connection, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).expect("prepare");
    let batches: Vec<RecordBatch> = stmt.query_arrow([]).expect("query_arrow").collect();
    let mut pts = 0usize;
    let mut sum = 0.0f64;
    for b in &batches {
        let col = b.column(0);
        if let Some(lists) = col.as_any().downcast_ref::<ListArray>() {
            for row in 0..lists.len() {
                let vals = lists.value(row);
                if let Some(f) = vals.as_any().downcast_ref::<Float64Array>() {
                    for k in 0..f.len() {
                        sum += f.value(k);
                    }
                }
                pts += vals.len();
            }
        } else {
            if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
                for k in 0..f.len() {
                    sum += f.value(k);
                }
            }
            pts += b.num_rows();
        }
    }
    assert!(sum.is_finite());
    pts
}

/// Time `sql` cold (fresh RO connection per rep, open excluded) and warm
/// (one connection, 3 warmups), asserting the point count each time.
fn time_cold_warm(
    db: &Path,
    label: &str,
    sql: &str,
    expect: usize,
    scale: &Scale,
) -> (Stats, Stats) {
    let cold = stats(
        (0..scale.cold_reps)
            .map(|_| {
                let conn = open_ro(db);
                let t = Instant::now();
                let n = run_count(&conn, sql);
                let m = ms(t);
                assert_eq!(n, expect, "{label}");
                close(conn);
                m
            })
            .collect(),
    );
    let conn = open_ro(db);
    for _ in 0..3 {
        assert_eq!(run_count(&conn, sql), expect, "{label}");
    }
    let warm = stats(
        (0..scale.warm_reps)
            .map(|_| {
                let t = Instant::now();
                let n = run_count(&conn, sql);
                let m = ms(t);
                assert_eq!(n, expect, "{label}");
                m
            })
            .collect(),
    );
    close(conn);
    (cold, warm)
}

fn report_cw(label: &str, cw: &(Stats, Stats)) {
    println!(
        "  {label:52} warm min {:>8.2} / med {:>8.2} ms   cold min {:>8.2} / med {:>8.2} ms   (n={}/{})",
        cw.1.min, cw.1.median, cw.0.min, cw.0.median, cw.1.n, cw.0.n
    );
}

fn time_open(db: &Path, reps: usize) -> Stats {
    stats(
        (0..reps.max(5))
            .map(|_| {
                let t = Instant::now();
                let c = open_ro(db);
                let m = ms(t);
                close(c);
                m
            })
            .collect(),
    )
}

/// Row groups and payload-column blocks of a table (pragma_storage_info).
fn storage_shape(conn: &Connection, table: &str, list_col: &str) -> (i64, i64) {
    conn.query_row(
        &format!(
            "SELECT count(DISTINCT row_group_id), \
                    count(DISTINCT block_id) FILTER (WHERE column_name = '{list_col}' AND block_id >= 0) \
             FROM pragma_storage_info('{table}')"
        ),
        [],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
    )
    .expect("storage info")
}

// --- phase: windowed + per-series reads (dense) ------------------------------

/// The 20 deterministic series picked for the window reads (37 is coprime to
/// both 1000 and 100, so the picks are distinct at either scale).
fn read_entities(entities: usize) -> Vec<String> {
    (0..READ_SERIES)
        .map(|k| ename((37 * k + 11) % entities))
        .collect()
}

struct ReadIds {
    pd: i64,
    ids: Vec<i64>,
    names: Vec<String>,
}

fn read_ids(db: &Path, class: &str, param: &str, names: Vec<String>) -> ReadIds {
    let conn = open_ro(db);
    let pd: i64 = conn
        .query_row(
            "SELECT pd.id FROM parameter_definition pd \
             JOIN entity_class ec ON ec.id = pd.entity_class_id \
             WHERE lower(ec.name)=lower(?) AND lower(pd.name)=lower(?) AND pd.valid_to_commit IS NULL",
            [class, param],
            |r| r.get(0),
        )
        .expect("pd lookup");
    let ids: Vec<i64> = names
        .iter()
        .map(|n| {
            conn.query_row(
                "SELECT id FROM entity WHERE lower(name)=lower(?) AND valid_to_commit IS NULL",
                [n.as_str()],
                |r| r.get(0),
            )
            .expect("entity id")
        })
        .collect();
    close(conn);
    ReadIds { pd, ids, names }
}

fn in_list_i64(ids: &[i64]) -> String {
    ids.iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn in_list_str(names: &[String]) -> String {
    names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Variant (a) of the original harness: full payloads to Arrow, sliced
/// client-side. Returns window points.
fn run_client_slice(conn: &Connection, sql: &str) -> usize {
    let mut stmt = conn.prepare(sql).expect("prepare");
    let batches: Vec<RecordBatch> = stmt.query_arrow([]).expect("query_arrow").collect();
    let mut pts = 0usize;
    let mut sum = 0.0f64;
    for b in &batches {
        let lists = b
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list col");
        for row in 0..lists.len() {
            let vals = lists.value(row);
            let vals = vals.as_any().downcast_ref::<Float64Array>().expect("f64");
            for k in WIN_LO_POS..WIN_LO_POS + WIN_PTS {
                sum += vals.value(k);
                pts += 1;
            }
        }
    }
    assert!(sum.is_finite());
    pts
}

fn phase_reads(db: &Path, label: &str, scale: &Scale) {
    println!(
        "\n== reads on the {}-series DB [{label}] ({:.1} MiB) ==",
        scale.big_params * scale.entities,
        mib(file_bytes(db))
    );
    let r = read_ids(db, CLASS_DENSE, "flow00", read_entities(scale.entities));
    {
        let conn = open_ro(db);
        let (rgs, blocks) = storage_shape(&conn, "value_array", "values");
        println!(
            "  value_array storage: {rgs} row group(s); `values` column spans {blocks} blocks"
        );
        close(conn);
    }
    let open = time_open(db, scale.cold_reps);
    report("RO connection open (per-operation tax)", &open);

    let ids = in_list_i64(&r.ids);
    let one = r.ids[0];
    let lo1 = WIN_LO_POS + 1; // 1-based inclusive slice bounds
    let hi1 = WIN_LO_POS + WIN_PTS;
    let dense = format!("read_parameter_dense('{CLASS_DENSE}','flow00')");

    println!("one series ({CELLS} values), read_parameter_dense, entity_id filter:");
    let single: [(&str, String, usize); 4] = [
        (
            "whole value (8760 pts)",
            format!("SELECT \"values\" FROM {dense} WHERE entity_id = {one}"),
            CELLS,
        ),
        (
            "week slice values[4001:4168]",
            format!("SELECT \"values\"[{lo1}:{hi1}] FROM {dense} WHERE entity_id = {one}"),
            WIN_PTS,
        ),
        (
            "single cell values[4001]",
            format!("SELECT \"values\"[{lo1}] FROM {dense} WHERE entity_id = {one}"),
            1,
        ),
        (
            "min/max only (v_min, v_max)",
            format!("SELECT v_min, v_max FROM {dense} WHERE entity_id = {one}"),
            1,
        ),
    ];
    for (l, sql, expect) in &single {
        report_cw(l, &time_cold_warm(db, l, sql, *expect, scale));
    }

    println!("one week ({WIN_PTS} pts) x {READ_SERIES} series:");
    let expect = READ_SERIES * WIN_PTS;
    // (a) needs client-side slicing — timed with its own loop.
    {
        let sql = format!("SELECT entity_id, \"values\" FROM {dense} WHERE entity_id IN ({ids})");
        let cold = stats(
            (0..scale.cold_reps)
                .map(|_| {
                    let conn = open_ro(db);
                    let t = Instant::now();
                    assert_eq!(run_client_slice(&conn, &sql), expect);
                    let m = ms(t);
                    close(conn);
                    m
                })
                .collect(),
        );
        let conn = open_ro(db);
        for _ in 0..3 {
            assert_eq!(run_client_slice(&conn, &sql), expect);
        }
        let warm = stats(
            (0..scale.warm_reps)
                .map(|_| {
                    let t = Instant::now();
                    assert_eq!(run_client_slice(&conn, &sql), expect);
                    ms(t)
                })
                .collect(),
        );
        close(conn);
        report_cw(
            "(a) read_parameter_dense + client-side slice",
            &(cold, warm),
        );
    }
    let t_lo = T0_US + WIN_LO_POS as i64 * HOUR_US;
    let t_hi = T0_US + (WIN_LO_POS + WIN_PTS - 1) as i64 * HOUR_US;
    let window: [(&str, String); 3] = [
        (
            "(b) SQL list slice values[4001:4168]",
            format!("SELECT \"values\"[{lo1}:{hi1}] AS w FROM {dense} WHERE entity_id IN ({ids})"),
        ),
        (
            "(c) unnest + filter on position",
            format!(
                "SELECT v FROM ( \
                   SELECT va.entity_id, generate_subscripts(va.\"values\", 1) AS pos, unnest(va.\"values\") AS v \
                   FROM value_array va \
                   WHERE va.parameter_definition_id = {pd} AND va.entity_id IN ({ids}) \
                 ) WHERE pos BETWEEN {lo1} AND {hi1}",
                pd = r.pd
            ),
        ),
        (
            "(d) read_parameter + idx1_ts window",
            format!(
                "SELECT v_float FROM read_parameter('{CLASS_DENSE}','flow00') \
                 WHERE entity IN ({names}) \
                   AND idx1_ts >= make_timestamp({t_lo}) AND idx1_ts <= make_timestamp({t_hi})",
                names = in_list_str(&r.names)
            ),
        ),
    ];
    for (l, sql) in &window {
        report_cw(l, &time_cold_warm(db, l, sql, expect, scale));
    }

    // array_id key-set shapes. `read_parameter_dense` fetches the payload by
    // an IN filter on array_id (index scan) only while the key set stays an IN
    // list of at most 50 keys: two ADJACENT entities exercise the dense-id
    // rewrite the sequence stride defeats; 51 series cross the engine's
    // dynamic-IN threshold and fall back to a scan (expected, bulk cost).
    println!("array_id key-set shapes, read_parameter_dense whole values:");
    let adj = read_ids(db, CLASS_DENSE, "flow00", vec![ename(11), ename(12)]);
    let wide: Vec<String> = (100..151).map(|k| ename(k % scale.entities)).collect();
    let wide = read_ids(db, CLASS_DENSE, "flow00", wide);
    let shapes: [(&str, String, usize); 2] = [
        (
            "2 adjacent series (dense-id tripwire)",
            format!(
                "SELECT \"values\" FROM {dense} WHERE entity_id IN ({})",
                in_list_i64(&adj.ids)
            ),
            2 * CELLS,
        ),
        (
            "51 series (past the 50-key index limit)",
            format!(
                "SELECT \"values\" FROM {dense} WHERE entity_id IN ({})",
                in_list_i64(&wide.ids)
            ),
            51 * CELLS,
        ),
    ];
    for (l, sql, n) in &shapes {
        report_cw(l, &time_cold_warm(db, l, sql, *n, scale));
    }

    // Bulk canonical read of one whole parameter (the api::read_parameter
    // shape): exercises value_array_cells over every series.
    println!("bulk canonical read:");
    let bulk = scale.entities * CELLS;
    let l = "read_parameter, whole parameter";
    let sql = format!("SELECT v_float FROM read_parameter('{CLASS_DENSE}','flow00')");
    report_cw(l, &time_cold_warm(db, l, &sql, bulk, scale));
}

// --- phase: value_array_cells A/B gate (paired, interleaved) ----------------

/// The pre-array_id two-branch `value_array_cells` (the parity oracle of
/// `tests/array_id_semantics.rs`), installed as a TEMP view for the A/B.
const OLD_CELLS_SQL: &str =
    include_str!("../../nodal-db-core/tests/fixtures/value_array_cells_two_branch.sql");

/// Install TEMP copies of the canonical read chain (`value_cells` ->
/// `value_rows` -> `read_parameter`) bound to the old two-branch cells view,
/// built from the database's own catalog text with the one dependency
/// renamed. Both variants then run identical SQL except for the cells view.
fn install_old_read_chain(conn: &Connection) {
    conn.execute_batch(
        &OLD_CELLS_SQL.replace("value_array_cells_two_branch", "value_array_cells_old"),
    )
    .expect("old cells view");
    // duckdb_views().sql / macro_definition hold the engine's normalized text
    // ("FROM value_cells AS c"), so rename by identifier, then prove the old
    // name is gone (a silent miss would compare the new chain with itself).
    let rename = |text: String, from: &str, to: &str| -> String {
        let out = replace_ident(&text, from, to);
        assert!(
            out.contains(to) && !has_ident(&out, from),
            "rename {from} -> {to} failed in: {out}"
        );
        out
    };
    let view_sql = |name: &str| -> String {
        conn.query_row(
            "SELECT sql FROM duckdb_views() WHERE view_name = ? AND NOT internal",
            [name],
            |r| r.get(0),
        )
        .expect("view sql")
    };
    let cells = rename(
        rename(view_sql("value_cells"), "value_cells", "value_cells_old"),
        "value_array_cells",
        "value_array_cells_old",
    )
    .replacen("CREATE VIEW", "CREATE TEMP VIEW", 1);
    conn.execute_batch(&cells).expect("value_cells_old");
    let rows = rename(
        rename(view_sql("value_rows"), "value_rows", "value_rows_old"),
        "value_cells",
        "value_cells_old",
    )
    .replacen("CREATE VIEW", "CREATE TEMP VIEW", 1);
    conn.execute_batch(&rows).expect("value_rows_old");
    let body: String = conn
        .query_row(
            "SELECT macro_definition FROM duckdb_functions() \
             WHERE function_name = 'read_parameter' AND function_type = 'table_macro' \
               AND NOT internal",
            [],
            |r| r.get(0),
        )
        .expect("read_parameter body");
    let body = rename(body, "value_rows", "value_rows_old");
    conn.execute_batch(&format!(
        "CREATE TEMP MACRO read_parameter_old(class_name, parameter_name) AS TABLE {body}"
    ))
    .expect("read_parameter_old");
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Byte offsets of `name` occurring as a whole SQL identifier in `s`.
fn ident_hits(s: &str, name: &str) -> Vec<usize> {
    s.match_indices(name)
        .filter(|(i, _)| {
            let before = s[..*i].chars().next_back();
            let after = s[*i + name.len()..].chars().next();
            !before.is_some_and(is_ident_char) && !after.is_some_and(is_ident_char)
        })
        .map(|(i, _)| i)
        .collect()
}

fn has_ident(s: &str, name: &str) -> bool {
    !ident_hits(s, name).is_empty()
}

fn replace_ident(s: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last = 0;
    for i in ident_hits(s, from) {
        out.push_str(&s[last..i]);
        out.push_str(to);
        last = i + from.len();
    }
    out.push_str(&s[last..]);
    out
}

fn loadavg() -> String {
    fs::read_to_string("/proc/loadavg")
        .map(|s| s.split_whitespace().take(3).collect::<Vec<_>>().join("/"))
        .unwrap_or_else(|_| "?".into())
}

fn load1(s: &str) -> f64 {
    s.split('/')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

fn cpu_model() -> String {
    fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .and_then(|l| l.split_once(':'))
                .map(|(_, v)| v.trim().to_string())
        })
        .unwrap_or_else(|| "?".into())
}

fn median(v: &[f64]) -> f64 {
    let mut v = v.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Bootstrap 95 % CI of the median (deterministic splitmix64 resampling).
fn bootstrap_ci(v: &[f64], n_boot: usize) -> (f64, f64) {
    let mut state = SEED ^ 0xb007;
    let mut meds: Vec<f64> = (0..n_boot)
        .map(|_| {
            let sample: Vec<f64> = (0..v.len())
                .map(|_| {
                    state = splitmix64(state);
                    v[(state % v.len() as u64) as usize]
                })
                .collect();
            median(&sample)
        })
        .collect();
    meds.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (
        meds[(0.025 * n_boot as f64) as usize],
        meds[(0.975 * n_boot as f64) as usize - 1],
    )
}

/// The `value_array_cells` acceptance gate (array_id design, D9 / §6.1):
/// `read_parameter` over one whole parameter (the `api::read_parameter`
/// shape: `SELECT *`, Arrow end-to-end), new single-branch self-join cells
/// view vs the old two-branch view, in ONE process on ONE file, threads fixed
/// at 8, N interleaved pairs with a randomized order inside each pair. Load
/// drift hits both sides of a pair alike and cancels in the per-pair ratio
/// new/old. Pass: bootstrap 95 % CI upper bound of the median ratio <= 1.10;
/// fail: CI lower bound > 1.10; else ambiguous (rerun with
/// NODAL_GATE_PAIRS=61, then decide on the median). A run whose 1-minute load
/// average changes by more than 2x is flagged as discarded. The bench itself
/// keeps ~8 threads busy, so the start sample is taken after a warm-up of at
/// least 60 s (the 1-minute average has absorbed the bench's own load by
/// then); otherwise every run from an idle machine would "drift".
fn phase_cellsgate(db: &Path) {
    let pairs: usize = std::env::var("NODAL_GATE_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(21);
    println!(
        "\n== value_array_cells gate: read_parameter bulk, new vs old cells view ({:.1} MiB) ==",
        mib(file_bytes(db))
    );
    let conn = open_ro(db);
    conn.execute_batch("SET threads = 8").expect("threads");
    install_old_read_chain(&conn);
    let new_sql = format!("SELECT * FROM read_parameter('{CLASS_DENSE}','flow00')");
    let old_sql = format!("SELECT * FROM read_parameter_old('{CLASS_DENSE}','flow00')");
    // Same rows from both variants (count + order-free checksum).
    let chk = |m: &str| -> (i64, String) {
        conn.query_row(
            &format!(
                "SELECT count(*), CAST(sum(hash(entity, alternative, idx1_ts, idx1_num, \
                   idx1_text, v_float, v_int)) AS VARCHAR) FROM {m}('{CLASS_DENSE}','flow00')"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("checksum")
    };
    let (n_new, n_old) = (chk("read_parameter"), chk("read_parameter_old"));
    assert_eq!(n_new, n_old, "new and old read chains disagree");
    let expect = n_new.0 as usize;

    let timed = |sql: &str| -> f64 {
        let t = Instant::now();
        let mut stmt = conn.prepare(sql).expect("prepare");
        let n: usize = stmt
            .query_arrow([])
            .expect("query_arrow")
            .map(|b| b.num_rows())
            .sum();
        let m = ms(t);
        assert_eq!(n, expect);
        m
    };
    let warm = Instant::now();
    while warm.elapsed().as_secs() < 60 {
        timed(&old_sql);
        timed(&new_sql);
    }
    let load_start = loadavg();
    let mut state = SEED ^ 0x9a7e;
    let (mut t_new, mut t_old, mut ratios) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..pairs {
        state = splitmix64(state);
        let (a, b) = if state & 1 == 0 {
            let o = timed(&old_sql);
            (timed(&new_sql), o)
        } else {
            let n = timed(&new_sql);
            (n, timed(&old_sql))
        };
        t_new.push(a);
        t_old.push(b);
        ratios.push(a / b);
    }
    close(conn);
    let load_end = loadavg();
    let med = median(&ratios);
    let (lo, hi) = bootstrap_ci(&ratios, 20_000);
    let (l0, l1) = (load1(&load_start), load1(&load_end));
    let drift = l0.max(l1) / l0.min(l1).max(0.01);
    let verdict = if hi <= 1.10 {
        "PASS"
    } else if lo > 1.10 {
        "FAIL (revert value_array_cells to the two-branch form)"
    } else {
        "AMBIGUOUS (rerun with NODAL_GATE_PAIRS=61; then decide on the median)"
    };
    let rmin = ratios.iter().cloned().fold(f64::INFINITY, f64::min);
    let rmax = ratios.iter().cloned().fold(0.0, f64::max);
    println!(
        "  cpu {} ({} logical), threads=8",
        cpu_model(),
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );
    println!("  rows per read: {expect}; pairs: {pairs} (randomized order)");
    println!("  load avg: start {load_start}, end {load_end} (1-min drift {drift:.2}x)");
    println!(
        "  old: min {:.0} / median {:.0} ms    new: min {:.0} / median {:.0} ms",
        t_old.iter().cloned().fold(f64::INFINITY, f64::min),
        median(&t_old),
        t_new.iter().cloned().fold(f64::INFINITY, f64::min),
        median(&t_new)
    );
    println!(
        "  ratio new/old: median {med:.3}, 95% bootstrap CI [{lo:.3}, {hi:.3}], min {rmin:.3}, max {rmax:.3}"
    );
    let ratios_txt: Vec<String> = ratios.iter().map(|r| format!("{r:.3}")).collect();
    println!("  per-pair ratios: {}", ratios_txt.join(" "));
    if drift > 2.0 {
        println!("  RUN DISCARDED: the 1-min load average changed by more than 2x");
    }
    println!("  verdict: {verdict} (threshold 1.10)");
}

// --- phase: long-tier reads -------------------------------------------------

fn phase_longreads(db: &Path, scale: &Scale) {
    let cells = scale.entities * SPARSE_CELLS;
    println!(
        "\n== long-tier reads on the long-shape DB ({:.1} MiB, {cells} live cells) ==",
        mib(file_bytes(db))
    );
    let r = read_ids(db, CLASS_LONG, "state", read_entities(scale.entities));
    {
        let conn = open_ro(db);
        let (rgs, _) = storage_shape(&conn, "value_long", "v_int");
        println!("  value_long storage: {rgs} row group(s)");
        close(conn);
    }
    report(
        "RO connection open (per-operation tax)",
        &time_open(db, scale.cold_reps),
    );
    // The sparse axis is every 10th hour: positions [400, 416] cover the
    // week [4000, 4168) -> 17 points.
    let lo = WIN_LO_POS / SPARSE_STRIDE;
    let hi = (WIN_LO_POS + WIN_PTS - 1) / SPARSE_STRIDE;
    let wpts = hi - lo + 1;
    let base = format!(
        "FROM value_long WHERE parameter_definition_id = {pd} AND valid_to_commit IS NULL",
        pd = r.pd
    );
    let one = r.ids[0];
    let ids = in_list_i64(&r.ids);
    let t_lo = T0_US + WIN_LO_POS as i64 * HOUR_US;
    let t_hi = T0_US + (WIN_LO_POS + WIN_PTS - 1) as i64 * HOUR_US;
    let qs: Vec<(String, String, usize)> = vec![
        ("one cell (value_long, i1_pos)".into(), format!("SELECT v_int {base} AND entity_id = {one} AND i1_pos = {lo}"), 1),
        (
            format!("one series window ({wpts} pts)"),
            format!("SELECT v_int {base} AND entity_id = {one} AND i1_pos BETWEEN {lo} AND {hi}"),
            wpts,
        ),
        (
            format!("one series whole ({SPARSE_CELLS} pts)"),
            format!("SELECT v_int {base} AND entity_id = {one}"),
            SPARSE_CELLS,
        ),
        ("one series min/max (aggregate)".into(), format!("SELECT min(v_int), max(v_int) {base} AND entity_id = {one}"), 1),
        (
            format!("{READ_SERIES} series window ({} pts)", READ_SERIES * wpts),
            format!("SELECT v_int {base} AND entity_id IN ({ids}) AND i1_pos BETWEEN {lo} AND {hi}"),
            READ_SERIES * wpts,
        ),
        (
            format!("read_parameter, {READ_SERIES} series, ts window"),
            format!(
                "SELECT v_int FROM read_parameter('{CLASS_LONG}','state') WHERE entity IN ({names}) \
                 AND idx1_ts >= make_timestamp({t_lo}) AND idx1_ts <= make_timestamp({t_hi})",
                names = in_list_str(&r.names)
            ),
            READ_SERIES * wpts,
        ),
    ];
    for (l, sql, expect) in &qs {
        report_cw(l, &time_cold_warm(db, l, sql, *expect, scale));
    }
}

// --- phase: row-group steel-man ---------------------------------------------

/// Copy the 10k DB's `value_array` into scratch raw-DuckDB files at the
/// default and a small ROW_GROUP_SIZE, then time the same one-series reads
/// directly on the copy. Read-only against the nodal-db file.
fn phase_rowgroup(src: &Path, ctx: &Ctx) {
    println!(
        "\n== row-group steel-man: value_array copied to scratch files, direct table reads =="
    );
    let r = read_ids(
        src,
        CLASS_DENSE,
        "flow00",
        read_entities(ctx.scale.entities),
    );
    let one = r.ids[0];
    let ids = in_list_i64(&r.ids);
    let lo1 = WIN_LO_POS + 1;
    let hi1 = WIN_LO_POS + WIN_PTS;
    for rg in [None, Some(2048usize)] {
        let dst = ctx.dir.join(format!(
            "rg_{}.db",
            rg.map(|n| n.to_string()).unwrap_or("default".into())
        ));
        let _ = fs::remove_file(&dst);
        let t = Instant::now();
        {
            let conn = Connection::open_in_memory().expect("open");
            let opt = rg
                .map(|n| format!(" (ROW_GROUP_SIZE {n})"))
                .unwrap_or_default();
            conn.execute_batch(&format!(
                "ATTACH '{}' AS d{opt}; ATTACH '{}' AS s (READ_ONLY); \
                 CREATE TABLE d.va AS SELECT entity_id, parameter_definition_id, \"values\", v_min, v_max \
                   FROM s.value_array ORDER BY parameter_definition_id, entity_id; \
                 CHECKPOINT d; DETACH d; DETACH s;",
                dst.display(),
                src.display()
            ))
            .expect("row-group copy");
            close(conn);
        }
        let conn = open_ro(&dst);
        let (rgs, blocks) = storage_shape(&conn, "va", "values");
        close(conn);
        println!(
            "ROW_GROUP_SIZE {}: {rgs} row group(s), `values` spans {blocks} blocks, {:.1} MiB (copy {:.1} s)",
            rg.map(|n| n.to_string()).unwrap_or("default".into()),
            mib(file_bytes(&dst)),
            ms(t) / 1e3
        );
        let w = format!("FROM va WHERE parameter_definition_id = {pd}", pd = r.pd);
        let qs: [(&str, String, usize); 5] = [
            (
                "one series whole value",
                format!("SELECT \"values\" {w} AND entity_id = {one}"),
                CELLS,
            ),
            (
                "one series week slice",
                format!("SELECT \"values\"[{lo1}:{hi1}] {w} AND entity_id = {one}"),
                WIN_PTS,
            ),
            (
                "one series single cell",
                format!("SELECT \"values\"[{lo1}] {w} AND entity_id = {one}"),
                1,
            ),
            (
                "one series min/max only",
                format!("SELECT v_min, v_max {w} AND entity_id = {one}"),
                1,
            ),
            (
                "20 series week slice",
                format!("SELECT \"values\"[{lo1}:{hi1}] {w} AND entity_id IN ({ids})"),
                READ_SERIES * WIN_PTS,
            ),
        ];
        for (l, sql, expect) in &qs {
            report_cw(l, &time_cold_warm(&dst, l, sql, *expect, &ctx.scale));
        }
        let _ = fs::remove_file(&dst);
    }
}

// --- main -------------------------------------------------------------------

const PHASES: [&str; 11] = [
    "all",
    "dense1k",
    "dense10k",
    "dense10k-api",
    "dense10k-ipc",
    "long",
    "longreads",
    "bindprobe",
    "reads",
    "cellsgate",
    "rowgroup",
];

fn sweep_stale() {
    let tmp = std::env::temp_dir();
    if let Ok(rd) = fs::read_dir(&tmp) {
        for e in rd.flatten() {
            if e.file_name()
                .to_string_lossy()
                .starts_with("nodal-results-")
            {
                let _ = fs::remove_dir_all(e.path());
            }
        }
    }
}

fn main() {
    // cargo passes `--bench`; keep only the phase names and flags we know.
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let quick = raw.iter().any(|a| a == "--quick");
    let args: Vec<String> = raw
        .into_iter()
        .filter(|a| PHASES.contains(&a.as_str()))
        .collect();
    if args.is_empty() {
        println!(
            "results_path: no phase given, nothing to do (a bare `cargo bench` must not start this \
             ~30 min run).\n  usage: cargo bench -p nodal-db-cli --bench results_path -- <all|phase...> [--quick]\n  \
             phases: {}\n  or: just bench-results [--quick]",
            PHASES[1..].join(", ")
        );
        return;
    }
    let phases: Vec<&str> = if args.iter().any(|a| a == "all") {
        vec![
            "dense1k",
            "dense10k",
            "long",
            "bindprobe",
            "reads",
            "cellsgate",
            "rowgroup",
            "longreads",
        ]
    } else {
        args.iter().map(String::as_str).collect()
    };

    // NODAL_RESULTS_DIR pins the workdir (so split invocations can share a
    // kept DB); the stale sweep only runs on the default per-pid dir AND only
    // when no cross-invocation state is in play.
    let dir = match std::env::var("NODAL_RESULTS_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => {
            if std::env::var("NODAL_RESULTS_DB").is_err() {
                sweep_stale();
            }
            std::env::temp_dir().join(format!("nodal-results-{}", std::process::id()))
        }
    };
    fs::create_dir_all(&dir).expect("bench tempdir");
    let keep_env = std::env::var("NODAL_RESULTS_KEEP").is_ok();
    let scale = if quick { Scale::quick() } else { Scale::full() };

    println!(
        "results_path bench — phases: {phases:?}; scale: {}; workdir {}",
        if scale.quick {
            "QUICK (smoke-sized; numbers not comparable)"
        } else {
            "full"
        },
        dir.display()
    );
    {
        let conn = Connection::open_in_memory().expect("open");
        let v: String = conn
            .query_row("SELECT version()", [], |r| r.get(0))
            .unwrap();
        println!("duckdb {v}");
    }

    let wants = |p: &str| phases.contains(&p);
    let mut ctx = Ctx {
        baseline: baseline_bytes(&dir),
        dir: dir.clone(),
        keep_for_reads: wants("reads") || wants("rowgroup") || wants("cellsgate") || keep_env,
        keep_env,
        scale,
        reads_dbs: Vec::new(),
        long_db: None,
        api_ms_per_series: None,
        ipc_ms_per_series: None,
    };
    println!(
        "empty-schema baseline DB (init + CHECKPOINT): {} B ({:.1} MiB) — subtracted for net B/value",
        ctx.baseline,
        mib(ctx.baseline)
    );
    if let Ok(db) = std::env::var("NODAL_RESULTS_DB") {
        ctx.reads_dbs
            .push(("NODAL_RESULTS_DB".into(), PathBuf::from(db)));
    }
    if let Ok(db) = std::env::var("NODAL_RESULTS_LONG_DB") {
        ctx.long_db = Some(PathBuf::from(db));
    }
    let mut bind: Option<(f64, f64)> = None;

    let t_all = Instant::now();
    for phase in &phases {
        let (small, big, reps_small) = (
            ctx.scale.small_params,
            ctx.scale.big_params,
            ctx.scale.reps_small,
        );
        let keep = ctx.keep_for_reads;
        match *phase {
            "dense1k" => {
                phase_dense_api(&mut ctx, small, reps_small, false);
                phase_dense_ipc(&mut ctx, small, reps_small, false);
            }
            "dense10k" => {
                phase_dense_api(&mut ctx, big, 1, keep);
                phase_dense_ipc(&mut ctx, big, 1, keep);
            }
            "dense10k-api" => phase_dense_api(&mut ctx, big, 1, keep),
            "dense10k-ipc" => phase_dense_ipc(&mut ctx, big, 1, keep),
            "long" => {
                let keep_long = wants("longreads") || ctx.keep_env;
                phase_long(&mut ctx, keep_long);
            }
            "bindprobe" => bind = Some(phase_bindprobe()),
            "reads" => {
                if ctx.reads_dbs.is_empty() {
                    eprintln!(
                        "reads: no 10k DB — run together with dense10k or set NODAL_RESULTS_DB"
                    );
                }
                let dbs = ctx.reads_dbs.clone();
                for (label, db) in &dbs {
                    phase_reads(db, label, &ctx.scale);
                }
            }
            "cellsgate" => match ctx.reads_dbs.first() {
                Some((_, db)) => phase_cellsgate(&db.clone()),
                None => eprintln!(
                    "cellsgate: no 10k DB — run together with dense10k or set NODAL_RESULTS_DB"
                ),
            },
            "rowgroup" => match ctx.reads_dbs.first() {
                Some((_, db)) => {
                    let db = db.clone();
                    phase_rowgroup(&db, &ctx);
                }
                None => eprintln!(
                    "rowgroup: no 10k DB — run together with dense10k or set NODAL_RESULTS_DB"
                ),
            },
            "longreads" => match ctx.long_db.clone() {
                Some(db) => phase_longreads(&db, &ctx.scale),
                None => eprintln!(
                    "longreads: no long DB — run with `long` or set NODAL_RESULTS_LONG_DB"
                ),
            },
            _ => unreachable!(),
        }
    }

    // Statement-building share: bind-probe mechanics vs the measured
    // end-to-end per-series write cost of the 1k lanes.
    if let Some((prep, exec)) = bind {
        let mech = prep + exec;
        println!("\n== statement-building share ==");
        println!("  prepare + execute of one 8760-value list_value INSERT: {mech:.1} ms/series");
        for (lane, v) in [
            ("core API loop", ctx.api_ms_per_series),
            ("Arrow-IPC import", ctx.ipc_ms_per_series),
        ] {
            if let Some(per) = v {
                println!(
                    "  {lane:18} end-to-end {per:.1} ms/series (best rep) -> statement share {:.0} %",
                    100.0 * mech / per
                );
            }
        }
    }
    println!("\ntotal bench wall: {:.1} s", t_all.elapsed().as_secs_f64());

    if keep_env {
        println!("NODAL_RESULTS_KEEP set — keeping {}", dir.display());
    } else {
        let _ = fs::remove_dir_all(&dir);
    }
}
