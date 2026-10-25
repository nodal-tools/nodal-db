//! The synthetic fixture of the criterion gate (`crates/nodal-db-core/
//! benches/support/mod.rs`, ddl_sketch §10 shape), copied here so both
//! arms build it with the same harness code through their own core's
//! public write API, and the read / replace-cycle gate modes on it.
//!
//! Scale `full` is the criterion gate's: 10^5 scalars, 1 000 x 8 760-cell
//! float arrays (~70 MB), 140 x 8 760 int long entities, 30 of them
//! rewritten (~17 % dead). Scale `quick` is a tenth of each (10^4, 100,
//! 14, 3 rewrites: the same dead fraction). `NODAL_AB_SCALE` names the
//! scale of the fixture in `NODAL_AB_SYNTH`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::write_parameter;
use nodal_db_core::conn::with_read;
use nodal_db_core::duckdb;
use nodal_db_core::duckdb::{params, Connection};
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::WriteOptions;

use crate::check::{
    check, check_commits, check_rows, elapsed_ms, head, in_list, mean, median, read_hash, rows,
    RowHash, Sample,
};

pub const CELLS: usize = 8760;
const ALT: &str = "Base";
const USER: &str = "bench";
const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;

#[derive(Clone, Copy)]
pub struct Scale {
    pub scalars: usize,
    pub arrays: usize,
    pub longs: usize,
    pub churn: usize,
}

pub fn scale(name: &str) -> Scale {
    match name {
        "full" => Scale {
            scalars: 100_000,
            arrays: 1_000,
            longs: 140,
            churn: 30,
        },
        "quick" => Scale {
            scalars: 10_000,
            arrays: 100,
            longs: 14,
            churn: 3,
        },
        other => panic!("unknown scale {other:?} (quick | full)"),
    }
}

pub fn env_scale() -> Scale {
    scale(&std::env::var("NODAL_AB_SCALE").unwrap_or_else(|_| "quick".into()))
}

fn synth_path() -> PathBuf {
    PathBuf::from(
        std::env::var("NODAL_AB_SYNTH").expect("set NODAL_AB_SYNTH to a build_synth file"),
    )
}

fn scalar_batch(names: &[String], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn series_batch(
    names: &[String],
    float_vals: Option<&[f64]>,
    int_vals: Option<&[i64]>,
) -> RecordBatch {
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
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        vfield,
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts)) as ArrayRef,
            val,
        ],
    )
    .unwrap()
}

fn float_series(seed: usize) -> Vec<f64> {
    (0..CELLS)
        .map(|h| ((seed * 8760 + h) % 977) as f64 * 1.7 - 800.0)
        .collect()
}

fn int_series(seed: usize) -> Vec<i64> {
    (0..CELLS)
        .map(|h| ((seed as i64) * 100_000 + h as i64) % 65_536)
        .collect()
}

fn write(db: &Path, class: &str, param: &str, batch: RecordBatch, msg: &str) {
    write_parameter(
        db,
        class,
        param,
        ALT,
        batch,
        &[],
        WriteOptions::default(),
        USER,
        msg,
    )
    .expect("synthetic write");
}

fn write_scalars(db: &Path, names: &[String], vals: &[f64]) {
    write(
        db,
        "bench_scalar",
        "p_scalar",
        scalar_batch(names, vals),
        "scalars",
    );
}

fn write_array_entity(db: &Path, name: &str, seed: usize) {
    let b = series_batch(&[name.to_string()], Some(&float_series(seed)), None);
    write(db, "bench_array", "p_series", b, "array");
}

fn write_long_entity(db: &Path, name: &str, seed: usize) {
    let b = series_batch(&[name.to_string()], None, Some(&int_series(seed)));
    write(db, "bench_long", "p_long", b, "long");
}

fn checkpoint(db: &Path) {
    let conn = Connection::open(db).expect("RW open");
    conn.execute_batch("CHECKPOINT").expect("checkpoint");
    conn.close().map_err(|(_, e)| e).expect("close");
}

/// `build_synth <path> <quick|full>`.
pub fn build(db: &Path, scale_name: &str) {
    let s = scale(scale_name);
    let t = Instant::now();
    init_schema(db).expect("schema init");
    let chunk = s.scalars / 10;
    for c in 0..10 {
        let names: Vec<String> = (0..chunk)
            .map(|i| format!("s{:06}", c * chunk + i))
            .collect();
        let vals: Vec<f64> = (0..chunk).map(|i| (c * chunk + i) as f64 * 0.5).collect();
        write_scalars(db, &names, &vals);
    }
    let per = s.arrays / 10;
    for c in 0..10 {
        let names: Vec<String> = (0..per).map(|i| format!("a{:04}", c * per + i)).collect();
        let mut vals = Vec::with_capacity(per * CELLS);
        for i in 0..per {
            vals.extend(float_series(c * per + i));
        }
        write(
            db,
            "bench_array",
            "p_series",
            series_batch(&names, Some(&vals), None),
            "arrays",
        );
    }
    for e in 0..s.longs {
        write_long_entity(db, &format!("l{e:04}"), e);
    }
    checkpoint(db);
    for e in 0..s.churn {
        write_long_entity(db, &format!("l{e:04}"), 10_000 + e);
    }
    checkpoint(db);
    eprintln!(
        "built {} ({scale_name}) in {:.1} s",
        db.display(),
        t.elapsed().as_secs_f64()
    );
}

// ------------------------------------------------------------ point keys

struct Keys {
    scalar: (i64, i64, i64),
    long: (i64, i64, i64, i32),
    array: (i64, i64, i64),
    array_id: i64,
    long_pd: i64,
}

fn names(s: Scale) -> (String, String, String) {
    (
        format!("s{:06}", s.scalars / 2),
        format!("l{:04}", s.longs / 2),
        format!("a{:04}", s.arrays / 2),
    )
}

fn keys(conn: &Connection, s: Scale) -> Result<Keys, duckdb::Error> {
    let pd = |class: &str, param: &str| -> Result<i64, duckdb::Error> {
        conn.query_row(
            "SELECT pd.id FROM parameter_definition pd \
             JOIN entity_class ec ON ec.id = pd.entity_class_id \
             WHERE lower(ec.name)=lower(?) AND lower(pd.name)=lower(?) \
               AND pd.valid_to_commit IS NULL",
            params![class, param],
            |r| r.get(0),
        )
    };
    let ent = |name: &str| -> Result<i64, duckdb::Error> {
        conn.query_row(
            "SELECT id FROM entity WHERE lower(name)=lower(?) AND valid_to_commit IS NULL",
            params![name],
            |r| r.get(0),
        )
    };
    let alt: i64 = conn.query_row(
        "SELECT id FROM alternative WHERE lower(name)=lower(?)",
        params![ALT],
        |r| r.get(0),
    )?;
    let (sn, ln, an) = names(s);
    let array = (ent(&an)?, pd("bench_array", "p_series")?, alt);
    let array_id: i64 = conn.query_row(
        "SELECT array_id FROM value_array \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?",
        params![array.0, array.1, array.2],
        |r| r.get(0),
    )?;
    let long_pd = pd("bench_long", "p_long")?;
    Ok(Keys {
        scalar: (ent(&sn)?, pd("bench_scalar", "p_scalar")?, alt),
        long: (ent(&ln)?, long_pd, alt, (CELLS / 2) as i32),
        array,
        array_id,
        long_pd,
    })
}

fn query(
    conn: &Connection,
    sql: &str,
    args: &[&dyn duckdb::ToSql],
) -> Result<Vec<RecordBatch>, duckdb::Error> {
    let mut stmt = conn.prepare(sql)?;
    let arrow = stmt.query_arrow(args)?;
    Ok(arrow.collect())
}

/// Point read: 20 warm-up executions, then 200 timed ones on one warm RO
/// connection; the sample is the median per execution. Every execution
/// must return exactly one row; the digest covers the last one's logical
/// columns.
fn point(what: &str, sql: &str, args: impl Fn(&Keys) -> Vec<Box<dyn duckdb::ToSql>>) -> Sample {
    let s = env_scale();
    let db = synth_path();
    with_read(&db, |conn| {
        let k = keys(conn, s)?;
        let owned = args(&k);
        let a: Vec<&dyn duckdb::ToSql> = owned.iter().map(|b| b.as_ref()).collect();
        for _ in 0..20 {
            check_rows(what, rows(&query(conn, sql, &a)?), 1);
        }
        let mut ms = Vec::with_capacity(200);
        let mut last = vec![];
        for _ in 0..200 {
            let t = Instant::now();
            let b = query(conn, sql, &a)?;
            ms.push(elapsed_ms(t));
            check_rows(what, rows(&b), 1);
            last = b;
        }
        Ok(Sample::new(median(ms)).hash("read", &RowHash::of(&last)))
    })
    .unwrap_or_else(|e| crate::check::query_failed(&format!("{what}: {e}")))
}

/// Bulk read: 1 warm-up, then the median of `reps` timed executions on one
/// warm RO connection (Arrow end to end). Row counts are checked on every
/// execution; the digest covers the last result.
fn bulk(what: &str, sql: &str, want_rows: usize, pd_arg: bool, reps: usize) -> Sample {
    let s = env_scale();
    let db = synth_path();
    with_read(&db, |conn| {
        let k = keys(conn, s)?;
        let pd = k.long_pd;
        let args: Vec<&dyn duckdb::ToSql> = if pd_arg { vec![&pd] } else { vec![] };
        check_rows(what, rows(&query(conn, sql, &args)?), want_rows);
        let mut ms = Vec::with_capacity(reps);
        let mut last = vec![];
        for _ in 0..reps {
            let t = Instant::now();
            let b = query(conn, sql, &args)?;
            ms.push(elapsed_ms(t));
            check_rows(what, rows(&b), want_rows);
            last = b;
        }
        Ok(Sample::new(median(ms)).hash("read", &RowHash::of(&last)))
    })
    .unwrap_or_else(|e| crate::check::query_failed(&format!("{what}: {e}")))
}

fn b<T: duckdb::ToSql + Copy + 'static>(v: T) -> Box<dyn duckdb::ToSql> {
    Box::new(v)
}

/// A rendered point / few-key read through the point-read macros
/// (`read_value`, `read_values`; fast_point_reads_PLAN §6): 5 warm-up and
/// `reps` timed executions on `conn`, the sample is the median per
/// execution. Every execution must return `want` rows. With `reference`
/// (the same rows through `read_parameter`, read once, untimed), the last
/// result's digest must equal the reference's: the macros are a second
/// copy of `value_rows`' rendering.
fn rendered(
    conn: &Connection,
    what: &str,
    sql: &str,
    want: usize,
    reference: Option<&str>,
    reps: usize,
) -> Result<Sample, duckdb::Error> {
    for _ in 0..5 {
        check_rows(what, rows(&query(conn, sql, &[])?), want);
    }
    let mut ms = Vec::with_capacity(reps);
    let mut last = vec![];
    for _ in 0..reps {
        let t = Instant::now();
        let b = query(conn, sql, &[])?;
        ms.push(elapsed_ms(t));
        check_rows(what, rows(&b), want);
        last = b;
    }
    let got = RowHash::of(&last);
    if let Some(r) = reference {
        let want_hash = RowHash::of(&query(conn, r, &[])?);
        check(
            got == want_hash,
            &format!(
                "{what}: the macro's rows differ from read_parameter's ({got:?} vs {want_hash:?})"
            ),
        );
    }
    Ok(Sample::new(median(ms)).hash("read", &got))
}

/// `rendered` on one warm RO connection through the core's open.
fn rendered_core(
    what: &str,
    sql: &str,
    want: usize,
    reference: Option<&str>,
    reps: usize,
) -> Sample {
    with_read(&synth_path(), |conn| {
        Ok(rendered(conn, what, sql, want, reference, reps)?)
    })
    .unwrap_or_else(|e| crate::check::query_failed(&format!("{what}: {e}")))
}

/// `rendered` on a RAW read-only DuckDB connection with default settings
/// (caching operators on), not the core's open: the dict-cache bug shape is
/// a direct-SQL user's configuration, and a core-side mitigation must not
/// turn this metric into a no-op (fast_point_reads_PLAN D7).
fn rendered_raw(what: &str, sql: &str, want: usize, reps: usize) -> Sample {
    let open = || -> Result<Sample, duckdb::Error> {
        let config = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(synth_path(), config)?;
        let s = rendered(&conn, what, sql, want, None, reps)?;
        conn.close().map_err(|(_, e)| e)?;
        Ok(s)
    };
    open().unwrap_or_else(|e| crate::check::query_failed(&format!("{what}: {e}")))
}

fn quoted(names: &[String]) -> String {
    let v: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    format!("[{}]", v.join(", "))
}

pub const LONG_SCAN_SQL: &str = "SELECT vl.entity_id, vl.i1_pos, vl.i1_dt, vl.v_int \
     FROM value_long vl \
     WHERE vl.parameter_definition_id = ? AND vl.valid_to_commit IS NULL";

/// Dispatch a synthetic-fixture mode; `None` if `mode` is not one.
pub fn run(mode: &str, dir: &Path) -> Option<Sample> {
    let s = env_scale();
    // Executions per sample: enough timed work per sample (~100+ ms) that
    // a short multithreaded scan is not one scheduler hiccup (the A/A at
    // quick scale was too wide with 5 for the few-ms scans).
    let quick = s.longs < 140;
    let k = |quick_k: usize, full_k: usize| if quick { quick_k } else { full_k };
    Some(match mode {
        "rd_point_scalar" => point(
            mode,
            "SELECT vs.value_kind, vs.v_float FROM value_scalar vs \
             WHERE vs.entity_id=? AND vs.parameter_definition_id=? AND vs.alternative_id=? \
               AND vs.valid_to_commit IS NULL",
            |k| vec![b(k.scalar.0), b(k.scalar.1), b(k.scalar.2)],
        ),
        "rd_point_long" => point(
            mode,
            "SELECT vl.value_kind, vl.i1_dt, vl.v_int FROM value_long vl \
             WHERE vl.entity_id=? AND vl.parameter_definition_id=? AND vl.alternative_id=? \
               AND vl.i1_pos=? AND vl.valid_to_commit IS NULL",
            |k| vec![b(k.long.0), b(k.long.1), b(k.long.2), b(k.long.3)],
        ),
        "rd_point_array_dense" => point(
            mode,
            "SELECT d.values FROM read_parameter_dense('bench_array', 'p_series') d \
             WHERE d.entity_id=? AND d.alternative_id=?",
            |k| vec![b(k.array.0), b(k.array.2)],
        ),
        "rd_point_array_by_id" => point(
            mode,
            "SELECT va.values FROM value_array va WHERE va.array_id=?",
            |k| vec![b(k.array_id)],
        ),
        "rd_scalars_loader" => bulk(
            mode,
            "SELECT entity, alternative, value_kind, v_float, v_int, v_bool, v_str \
             FROM read_parameter_scalar('bench_scalar', 'p_scalar')",
            s.scalars,
            false,
            k(25, 9),
        ),
        "rd_dense" => bulk(
            mode,
            "SELECT * FROM read_parameter_dense('bench_array', 'p_series')",
            s.arrays,
            false,
            k(25, 5),
        ),
        "rd_model_load_long" => bulk(
            mode,
            "SELECT * FROM read_parameter('bench_long', 'p_long')",
            s.longs * CELLS,
            false,
            k(5, 5),
        ),
        "rd_long_scan_17pct" => bulk(mode, LONG_SCAN_SQL, s.longs * CELLS, true, k(25, 9)),
        // The point-read macros (fast_point_reads_PLAN §6): one rendered
        // array value, one scalar (the empty array-key path), the
        // dict-cache bug shape (a 6-hour window, raw connection), and 20
        // series in one call.
        "rd_point_value_array" => {
            let (_, _, an) = names(s);
            rendered_core(
                mode,
                &format!("SELECT * FROM read_value('bench_array', 'p_series', '{an}', 'Base')"),
                CELLS,
                Some(&format!(
                    "SELECT * FROM read_parameter('bench_array', 'p_series') \
                     WHERE entity = '{an}' AND alternative = 'Base'"
                )),
                k(60, 40),
            )
        }
        "rd_point_value_scalar" => {
            let (sn, _, _) = names(s);
            rendered_core(
                mode,
                &format!("SELECT * FROM read_value('bench_scalar', 'p_scalar', '{sn}', 'Base')"),
                1,
                Some(&format!(
                    "SELECT * FROM read_parameter('bench_scalar', 'p_scalar') \
                     WHERE entity = '{sn}' AND alternative = 'Base'"
                )),
                k(60, 40),
            )
        }
        "rd_point_value_window" => {
            let (_, _, an) = names(s);
            rendered_raw(
                mode,
                &format!(
                    "SELECT * FROM read_value('bench_array', 'p_series', '{an}', 'Base') \
                     WHERE idx1_ts BETWEEN TIMESTAMP '2025-03-01 00:00' \
                       AND TIMESTAMP '2025-03-01 05:00'"
                ),
                6,
                k(60, 40),
            )
        }
        "rd_values_20" => {
            let first = s.arrays / 2 - 10;
            let picked: Vec<String> = (first..first + 20).map(|i| format!("a{i:04}")).collect();
            rendered_core(
                mode,
                &format!(
                    "SELECT * FROM read_values('bench_array', 'p_series', {}, ['Base'])",
                    quoted(&picked)
                ),
                20 * CELLS,
                Some(&format!(
                    "SELECT * FROM read_parameter('bench_array', 'p_series') \
                     WHERE entity IN ({}) AND alternative = 'Base'",
                    in_list(&picked)
                )),
                k(25, 15),
            )
        }
        "wr_replace_scalar_cycle_ckpt" => cycle(dir, s, Tier::Scalar),
        "wr_replace_array_cycle_ckpt" => cycle(dir, s, Tier::Array),
        "wr_replace_long_cycle_ckpt" => cycle(dir, s, Tier::Long),
        _ => return None,
    })
}

enum Tier {
    Scalar,
    Array,
    Long,
}

/// A replace cycle (one whole `write_parameter` commit of one entity, then
/// CHECKPOINT) on a fresh copy of the synthetic file, 3 cycles per sample;
/// the sample is the mean cycle. The fresh copy removes criterion's
/// "later iterations are slower" accumulation. Checks: one commit per
/// cycle, and the entity reads back with the last written content.
fn cycle(dir: &Path, s: Scale, tier: Tier) -> Sample {
    let db = dir.join("synth.duckdb");
    std::fs::copy(synth_path(), &db).expect("copy synthetic");
    let (sn, ln, an) = names(s);
    let before = head(&db);
    let mut spans = vec![];
    for k in 0..3usize {
        let t = Instant::now();
        match tier {
            Tier::Scalar => write_scalars(&db, std::slice::from_ref(&sn), &[1e6 + k as f64]),
            Tier::Array => write_array_entity(&db, &an, 20_000 + k),
            Tier::Long => write_long_entity(&db, &ln, 30_000 + k),
        }
        checkpoint(&db);
        spans.push(elapsed_ms(t));
    }
    check_commits(&db, before, 3);
    let (sql, want) = match tier {
        Tier::Scalar => (
            format!(
                "SELECT entity, v_float FROM read_parameter_scalar('bench_scalar', 'p_scalar') \
                 WHERE entity IN ({})",
                in_list(std::slice::from_ref(&sn))
            ),
            1,
        ),
        Tier::Array => (
            format!(
                "SELECT entity, idx1_ts, v_float FROM read_parameter('bench_array', 'p_series') \
                 WHERE entity IN ({})",
                in_list(std::slice::from_ref(&an))
            ),
            CELLS,
        ),
        Tier::Long => (
            format!(
                "SELECT entity, idx1_ts, v_int FROM read_parameter('bench_long', 'p_long') \
                 WHERE entity IN ({})",
                in_list(std::slice::from_ref(&ln))
            ),
            CELLS,
        ),
    };
    let h = read_hash(&db, &sql);
    check_rows("read-back", h.rows as usize, want);
    // The last write's content, not an older version's: the first cell.
    // (Read the value and compare here: the equivalent `WHERE v_float = x`
    // filter trips a DuckDB 2.0.0 internal error on the full-scale file
    // after the array replace cycles, "dict-surviving cache: global
    // dictionary column 0 changed identity", which invalidates the
    // instance; reproduced in the Python client too.)
    let (sql1, want1) = match tier {
        Tier::Scalar => (
            format!(
                "SELECT v_float FROM read_parameter_scalar('bench_scalar', 'p_scalar') \
                 WHERE entity = '{sn}'"
            ),
            1e6 + 2.0,
        ),
        Tier::Array => (
            format!(
                "SELECT v_float FROM read_parameter('bench_array', 'p_series') \
                 WHERE entity = '{an}' ORDER BY idx1_ts LIMIT 1"
            ),
            float_series(20_002)[0],
        ),
        Tier::Long => (
            format!(
                "SELECT v_int::DOUBLE FROM read_parameter('bench_long', 'p_long') \
                 WHERE entity = '{ln}' ORDER BY idx1_ts LIMIT 1"
            ),
            int_series(30_002)[0] as f64,
        ),
    };
    let got: f64 = with_read(&db, |c| Ok(c.query_row(&sql1, [], |r| r.get(0))?))
        .unwrap_or_else(|e| crate::check::query_failed(&format!("{sql1}: {e}")));
    check(
        got == want1,
        &format!("read-back: the first cell is {got}, the last cycle wrote {want1}"),
    );
    Sample::new(mean(&spans)).hash("readback", &h)
}
