//! The `synth_axes` fixture and its read metrics (axis_position_cost_PLAN
//! §7): the synthetic fixture plus many axes, built through the public API
//! only. The plain `synth` fixture carries one axis, so it is blind to read
//! costs that grow with the axes in the file (fast_point_reads_PLAN M3/M4).
//!
//! On top of `synth::build` (same scale), K series of 8 760 hours each
//! (K = 500 full, 50 quick):
//!
//! 1. K parametric hourly series with K distinct starts, written to a
//!    scratch parameter and deleted (`api::delete_values`): K **unused**
//!    parametric axes, referenced only by history;
//! 2. K irregular series (each with its own gap pattern), written and
//!    deleted: K **unused stored** axes;
//! 3. K parametric series with distinct starts kept current under
//!    `bench_array.p_other`: K axes **in use** by another parameter;
//! 4. K irregular series kept current as `bench_array.p_irr`, one stored
//!    axis each: stored axes **in use** (the shape where `value_rows` pays
//!    for every axis in use, plan §11 C2).
//!
//! Each step is one `write_parameter` batch (plus one delete for 1 and 2).
//! `NODAL_AB_SYNTH_AXES` names the built file.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::{delete_values, DeleteKey};
use nodal_db_core::conn::with_read;
use nodal_db_core::duckdb;
use nodal_db_core::duckdb::Connection;
use nodal_db_core::types::WriteOptions;

use crate::check::{check_rows, elapsed_ms, median, rows, RowHash, Sample};
use crate::synth::{self, env_scale, Scale, CELLS, HOUR_US, T0_US};

const CLASS: &str = "bench_array";

/// K at the scale of the synthetic fixture.
fn k_of(s: Scale) -> usize {
    if s.longs < 140 {
        50
    } else {
        500
    }
}

fn axes_path() -> PathBuf {
    PathBuf::from(
        std::env::var("NODAL_AB_SYNTH_AXES")
            .expect("set NODAL_AB_SYNTH_AXES to a build_synth_axes file"),
    )
}

fn entity(prefix: &str, i: usize) -> String {
    format!("{prefix}{i:04}")
}

/// One series per entity: `ts(i, h)` for hour `h` of entity `i`.
fn series(prefix: &str, k: usize, ts: impl Fn(usize, usize) -> i64) -> RecordBatch {
    let n = k * CELLS;
    let (mut e, mut t, mut v) = (
        Vec::with_capacity(n),
        Vec::with_capacity(n),
        Vec::with_capacity(n),
    );
    for i in 0..k {
        let name = entity(prefix, i);
        for h in 0..CELLS {
            e.push(name.clone());
            t.push(ts(i, h));
            v.push(((i * 31 + h) % 997) as f64 * 0.75 - 300.0);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(e)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Parametric: entity `i` starts `i` hours (plus `offset_h`) after T0.
fn parametric(offset_h: i64) -> impl Fn(usize, usize) -> i64 {
    move |i, h| T0_US + (offset_h + i as i64 + h as i64) * HOUR_US
}

/// Irregular: hourly with an entity-specific sub-hour jitter (strictly
/// increasing; no two entities share the pattern).
fn irregular(salt: usize) -> impl Fn(usize, usize) -> i64 {
    move |i, h| {
        let jitter = ((h * (7 + 2 * i) + i * 13 + salt) % 1_801) as i64 * 1_000_000;
        T0_US + h as i64 * HOUR_US + jitter
    }
}

fn delete_all(db: &Path, param: &str, prefix: &str, k: usize) {
    let keys = (0..k)
        .map(|i| DeleteKey {
            class: CLASS.into(),
            parameter: param.into(),
            alternative: "Base".into(),
            entity: entity(prefix, i),
        })
        .collect();
    delete_values(db, keys, "bench", "drop", WriteOptions::default()).expect("delete");
}

/// `build_synth_axes <path> <quick|full>`.
pub fn build(db: &Path, scale_name: &str) {
    synth::build(db, scale_name);
    let k = k_of(synth::scale(scale_name));
    let t = Instant::now();
    // 1. unused parametric axes (history only)
    synth::write(
        db,
        CLASS,
        "p_scratch_par",
        series("u", k, parametric(20_000)),
        "unused parametric",
    );
    delete_all(db, "p_scratch_par", "u", k);
    // 2. unused stored axes
    synth::write(
        db,
        CLASS,
        "p_scratch_irr",
        series("v", k, irregular(1)),
        "unused stored",
    );
    delete_all(db, "p_scratch_irr", "v", k);
    // 3. parametric axes in use by another parameter
    synth::write(
        db,
        CLASS,
        "p_other",
        series("o", k, parametric(40_000)),
        "in use parametric",
    );
    // 4. stored axes in use
    synth::write(
        db,
        CLASS,
        "p_irr",
        series("i", k, irregular(2)),
        "in use stored",
    );
    synth::checkpoint(db);
    let axes: i64 = with_read(db, |c| {
        Ok(c.query_row("SELECT count(*) FROM axis", [], |r| r.get(0))?)
    })
    .expect("axis count");
    assert!(
        axes as usize > 4 * k,
        "synth_axes: {axes} axes for K = {k}: the writer deduplicated the series"
    );
    eprintln!(
        "built {} (synth_axes {scale_name}, K = {k}, {axes} axes) in +{:.1} s",
        db.display(),
        t.elapsed().as_secs_f64()
    );
}

// ------------------------------------------------------------------ reads

/// `entity_id = .. AND parameter_definition_id = .. AND alternative_id = ..`
/// for `name` of `class.param` (resolved untimed).
fn triple(
    conn: &Connection,
    class: &str,
    param: &str,
    name: &str,
) -> Result<String, duckdb::Error> {
    let (e, p, a): (i64, i64, i64) = conn.query_row(
        "SELECT e.id, pd.id, al.id FROM entity e \
         JOIN entity_class ec ON ec.id = e.entity_class_id AND ec.valid_to_commit IS NULL \
         JOIN parameter_definition pd ON pd.entity_class_id = ec.id AND pd.valid_to_commit IS NULL \
         JOIN alternative al ON lower(al.name) = 'base' AND al.valid_to_commit IS NULL \
         WHERE ec.name = ? AND pd.name = ? AND e.name = ? AND e.valid_to_commit IS NULL",
        duckdb::params![class, param, name],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    Ok(format!(
        "entity_id = {e} AND parameter_definition_id = {p} AND alternative_id = {a}"
    ))
}

/// 1 warm-up, then the median of `reps` timed executions on one warm RO
/// connection (Arrow end to end); every execution must return `want` rows,
/// the digest covers the last result's logical columns.
fn timed(
    conn: &Connection,
    what: &str,
    sql: &str,
    want: usize,
    reps: usize,
) -> Result<Sample, duckdb::Error> {
    check_rows(what, rows(&synth::query(conn, sql, &[])?), want);
    let mut ms = Vec::with_capacity(reps);
    let mut last = vec![];
    for _ in 0..reps {
        let t = Instant::now();
        let b = synth::query(conn, sql, &[])?;
        ms.push(elapsed_ms(t));
        check_rows(what, rows(&b), want);
        last = b;
    }
    Ok(Sample::new(median(ms)).hash("read", &RowHash::of(&last)))
}

fn on(
    db: PathBuf,
    what: &str,
    f: impl FnOnce(&Connection) -> Result<Sample, duckdb::Error>,
) -> Sample {
    with_read(&db, |conn| Ok(f(conn)?))
        .unwrap_or_else(|e| crate::check::query_failed(&format!("{what}: {e}")))
}

/// A `value_rows` point read (every column; the digest skips the ids).
const VR_COLS: &str = "SELECT * FROM value_rows WHERE ";

/// Dispatch an axes metric (or the plain-synth `value_rows` control);
/// `None` if `mode` is not one.
pub fn run(mode: &str) -> Option<Sample> {
    let s = env_scale();
    let quick = s.longs < 140;
    let reps = |q: usize, f: usize| if quick { q } else { f };
    let k = k_of(s);
    let (_, ln, _) = synth::names(s);
    Some(match mode {
        "rd_axes_rp_scalar" => on(axes_path(), mode, |c| {
            timed(
                c,
                mode,
                "SELECT * FROM read_parameter('bench_scalar', 'p_scalar')",
                s.scalars,
                reps(9, 7),
            )
        }),
        "rd_axes_rp_long" => on(axes_path(), mode, |c| {
            timed(
                c,
                mode,
                "SELECT * FROM read_parameter('bench_long', 'p_long')",
                s.longs * CELLS,
                reps(5, 5),
            )
        }),
        "rd_axes_rp_array" => on(axes_path(), mode, |c| {
            timed(
                c,
                mode,
                "SELECT * FROM read_parameter('bench_array', 'p_series')",
                s.arrays * CELLS,
                reps(5, 5),
            )
        }),
        "rd_axes_rp_irr" => on(axes_path(), mode, |c| {
            timed(
                c,
                mode,
                "SELECT * FROM read_parameter('bench_array', 'p_irr')",
                k * CELLS,
                reps(5, 5),
            )
        }),
        "rd_axes_vr_point_long" => on(axes_path(), mode, |c| {
            let sql = format!("{VR_COLS}{}", triple(c, "bench_long", "p_long", &ln)?);
            timed(c, mode, &sql, CELLS, reps(9, 7))
        }),
        "rd_axes_vr_point_stored" => on(axes_path(), mode, |c| {
            let sql = format!(
                "{VR_COLS}{}",
                triple(c, CLASS, "p_irr", &entity("i", k / 2))?
            );
            timed(c, mode, &sql, CELLS, reps(9, 7))
        }),
        "rd_axes_point_irr" => on(axes_path(), mode, |c| {
            let sql = format!(
                "SELECT * FROM read_value('{CLASS}', 'p_irr', '{}', 'Base')",
                entity("i", k / 2)
            );
            timed(c, mode, &sql, CELLS, reps(25, 15))
        }),
        // the plain-synth control of the value_rows cost (plan R1)
        "rd_vr_point_long" => on(synth::synth_path(), mode, |c| {
            let sql = format!("{VR_COLS}{}", triple(c, "bench_long", "p_long", &ln)?);
            timed(c, mode, &sql, CELLS, reps(9, 7))
        }),
        _ => return None,
    })
}
