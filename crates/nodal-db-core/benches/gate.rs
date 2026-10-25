//! The pre-freeze benchmark (rust_implementation_plan.md Part 5
//! deliverable 4), now a RECORDING bench (informational): measurements
//! land in `benches/RESULTS.md`; nothing here fails the build. Performance
//! is judged by the paired A/B perf gate (`just perf-gate`,
//! `scripts/bench/ab/`); the VMP Part 6 millisecond numbers in the comments
//! below are requirement context, not gates.
//!
//! Run with `cargo bench` (release opt; `cargo test` never runs this — the
//! `[[bench]] test = false` in Cargo.toml is load-bearing, the synthetic DB
//! takes minutes to build). Group order matters and is intentional:
//!
//!   1. point reads      — on the pristine ~17%-dead synthetic DB
//!   2. bulk reads       — ditto (incl. the ~17%-dead scan point)
//!   3. writes           — replace cycles mutate the DB (adds dead/hist rows)
//!   4. scan degradation — churns the long tier to ~33% dead, LAST
//!
//! Reads go through the SQL read surface (`read_parameter*` macros / raw tier
//! scans, ddl_sketch §7-10); point reads hold one warm RO connection for the
//! duration of the group, mirroring the §10 warm methodology (the
//! per-operation RO open cost is measured separately by the builder and
//! recorded as context in RESULTS.md).

mod support;

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, Criterion};

use support::*;

// --- 1. point reads (VMP Part 6 context, not a gate: < 5 ms, any tier) -------

fn bench_point_reads(c: &mut Criterion) {
    let s = synthetic();
    let conn = open_ro(&s.db);
    let mut g = c.benchmark_group("point_read");
    g.warm_up_time(Duration::from_millis(500));
    g.measurement_time(Duration::from_secs(3));

    let (e, p, a) = s.keys.scalar;
    g.bench_function("scalar_triple", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT vs.value_kind, vs.v_float FROM value_scalar vs \
                 WHERE vs.entity_id=? AND vs.parameter_definition_id=? AND vs.alternative_id=? \
                   AND vs.valid_to_commit IS NULL",
                &[&e, &p, &a],
            );
            assert_eq!(rows, 1);
            black_box(rows)
        })
    });

    let (e, p, a, pos) = s.keys.long;
    g.bench_function("long_cell_triple", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT vl.value_kind, vl.i1_dt, vl.v_int FROM value_long vl \
                 WHERE vl.entity_id=? AND vl.parameter_definition_id=? AND vl.alternative_id=? \
                   AND vl.i1_pos=? AND vl.valid_to_commit IS NULL",
                &[&e, &p, &a, &pos],
            );
            assert_eq!(rows, 1);
            black_box(rows)
        })
    });

    // The known §10 residual: full list-payload point read (~27-31 ms on
    // 1.4.4 vs the < 5 ms budget; re-measured on 1.5.5 and 2.0.0, see
    // RESULTS.md).
    let (e, p, a) = s.keys.array;
    g.bench_function("array_triple_payload", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT va.values FROM value_array va \
                 WHERE va.entity_id=? AND va.parameter_definition_id=? AND va.alternative_id=?",
                &[&e, &p, &a],
            );
            assert_eq!(rows, 1);
            black_box(rows)
        })
    });

    // array_id surrogate key: the same point read through the dense read
    // macro, which resolves array_id narrowly and fetches the payload by an
    // index scan. (`array_triple_payload` above stays as the documented slow
    // form: a direct table read by the triple scans the payload.)
    g.bench_function("array_point_dense_macro", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT d.values FROM read_parameter_dense('bench_array', 'p_series') d \
                 WHERE d.entity_id=? AND d.alternative_id=?",
                &[&e, &a],
            );
            assert_eq!(rows, 1);
            black_box(rows)
        })
    });

    // The floor: the payload fetched directly by its array_id (`=` filter).
    let array_id: i64 = conn
        .query_row(
            "SELECT array_id FROM value_array \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?",
            duckdb::params![e, p, a],
            |r| r.get(0),
        )
        .expect("array_id");
    g.bench_function("array_point_by_array_id", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT va.values FROM value_array va WHERE va.array_id=?",
                &[&array_id],
            );
            assert_eq!(rows, 1);
            black_box(rows)
        })
    });

    // §10 mitigation context: v_min/v_max-only point read avoids the payload
    // row-group decompression entirely.
    g.bench_function("array_triple_vmin_only", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT va.v_min, va.v_max FROM value_array va \
                 WHERE va.entity_id=? AND va.parameter_definition_id=? AND va.alternative_id=?",
                &[&e, &p, &a],
            );
            assert_eq!(rows, 1);
            black_box(rows)
        })
    });

    g.finish();
    conn.close().map_err(|(_, e)| e).expect("close");
}

// --- 2. bulk reads -----------------------------------------------------------

fn bench_bulk_reads(c: &mut Criterion) {
    let s = synthetic();
    let conn = open_ro(&s.db);
    let mut g = c.benchmark_group("bulk_read");
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(10));

    // 10^5 scalar rows, budget path (VMP context, not a gate: < 25 ms; 20 ms until 2026-10-25, owner
    // decision, see RESULTS.md): the columns a model loader
    // reads, here the nodal-editor's scalar-sheet load (`model.rs`: entity,
    // alternative, value_kind and the four value columns).
    g.bench_function("scalars_100k_read_parameter_scalar_loader_cols", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT entity, alternative, value_kind, v_float, v_int, v_bool, v_str \
                 FROM read_parameter_scalar('bench_scalar', 'p_scalar')",
                &[],
            );
            assert_eq!(rows, SCALAR_ENTITIES);
            black_box(rows)
        })
    });

    // The same read with every column (context; §10: 24.5 ms on 1.4.4).
    g.bench_function("scalars_100k_read_parameter_scalar", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT * FROM read_parameter_scalar('bench_scalar', 'p_scalar')",
                &[],
            );
            assert_eq!(rows, SCALAR_ENTITIES);
            black_box(rows)
        })
    });

    // 1 000 × 8 760 (~70 MB), dense budget path (VMP context, not a gate: < 300 ms
    // warm; §10: 219 ms).
    g.bench_function("arrays_70mb_read_parameter_dense", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT * FROM read_parameter_dense('bench_array', 'p_series')",
                &[],
            );
            assert_eq!(rows, ARRAY_ENTITIES);
            black_box(rows)
        })
    });

    // Same data through the canonical long shape (8.76 M rendered cells;
    // recorded, not the budget path — §10: ~4.2 s, VMP allows dense form).
    g.bench_function("arrays_70mb_read_parameter_canonical", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT * FROM read_parameter('bench_array', 'p_series')",
                &[],
            );
            assert_eq!(rows, ARRAY_ENTITIES * CELLS);
            black_box(rows)
        })
    });

    // Model-load context (< 3 s): the 1.23 M-row long parameter through the
    // canonical read (§10: 587 ms), and its raw current scan (§10: 453 ms) —
    // the raw number doubles as the ~17%-dead degradation point.
    g.bench_function("model_load_long_1p23m_canonical", |b| {
        b.iter(|| {
            let rows = arrow_rows(
                &conn,
                "SELECT * FROM read_parameter('bench_long', 'p_long')",
                &[],
            );
            assert_eq!(rows, LONG_ENTITIES * CELLS);
            black_box(rows)
        })
    });

    let pd = s.keys.long_pd;
    let dead_frac = {
        let (live, dead) = long_counts(&conn, pd);
        100.0 * dead as f64 / (live + dead) as f64
    };
    eprintln!("[gate] long current-scan bench runs at {dead_frac:.1}% dead");
    g.bench_function("long_current_scan_17pct_dead", |b| {
        b.iter(|| {
            let rows = long_scan_rows(&conn, pd);
            assert_eq!(rows, LONG_ENTITIES * CELLS);
            black_box(rows)
        })
    });

    g.finish();
    conn.close().map_err(|(_, e)| e).expect("close");
}

// --- 3. writes (replace cycles + per-entity cost) ----------------------------

fn bench_writes(c: &mut Criterion) {
    let s = synthetic();
    let db = s.db.clone();
    let mut g = c.benchmark_group("write");
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(10));

    // Replace cycles per tier, each cycle = one full write_parameter commit
    // (open-write-close, history close-out) + CHECKPOINT. Research baselines:
    // long-tier 10-cycle churn 0.9 s single-table; array replace under
    // pre-image copy 4.3 s/cycle (whole-1000-set; this is per-entity).
    g.bench_function("replace_scalar_cycle_ckpt", |b| {
        b.iter(|| {
            let seq = next_seq();
            write_scalars(
                &db,
                &["s000000".to_string()],
                &[seq as f64],
                "bench scalar replace",
            );
            checkpoint(&db);
        })
    });

    g.bench_function("replace_array_cycle_ckpt", |b| {
        b.iter(|| {
            write_array_entity(&db, "a0000", 20_000 + next_seq() as usize);
            checkpoint(&db);
        })
    });

    // NOTE: each cycle closes out + re-inserts 8 760 cells; version probes
    // scan this entity's accumulating row groups, so later iterations are
    // slower — criterion reports the mean over the cycle count it ran.
    g.bench_function("replace_long_8760_cycle_ckpt", |b| {
        b.iter(|| {
            write_long_entity(&db, "l0000", 30_000 + next_seq() as usize);
            checkpoint(&db);
        })
    });

    // Per-entity statement cost at release opt (the ~44 ms/entity DEBUG fact
    // from the F4 scale test, rust_impl_session_decisions.md): 100 fresh
    // scalar entities per iteration through one write_parameter call;
    // divide the reported time by 100.
    g.bench_function("write_100_new_scalar_entities", |b| {
        b.iter(|| {
            let run = next_seq();
            let names: Vec<String> = (0..100).map(|i| format!("w{run:06}_{i:03}")).collect();
            let vals: Vec<f64> = (0..100).map(|i| (run * 100 + i) as f64).collect();
            write_scalars(&db, &names, &vals, "bench per-entity write");
        })
    });

    g.finish();
}

// --- 4. from/to scan degradation vs dead fraction (baseline 1.6x @ 20%) ------

fn bench_scan_degradation(c: &mut Criterion) {
    let s = synthetic();
    // Churn the long tier up to ~33% dead (rewrites through the real write
    // path). Runs LAST: every earlier bench sees the pristine ~17% DB.
    let rewrites = churn_long_to_dead_fraction(&s.db, 1.0 / 3.0);
    let conn = open_ro(&s.db);
    let (live, dead) = long_counts(&conn, s.keys.long_pd);
    eprintln!(
        "[gate] degradation churn: {rewrites} rewrites -> {live} live / {dead} dead ({:.1}% dead); \
         0%-dead baseline {:.1} ms",
        100.0 * dead as f64 / (live + dead) as f64,
        s.timings.long_scan_zero_dead_ms,
    );

    let mut g = c.benchmark_group("scan_degradation");
    g.sample_size(10);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(10));
    let pd = s.keys.long_pd;
    g.bench_function("long_current_scan_33pct_dead", |b| {
        b.iter(|| {
            let rows = long_scan_rows(&conn, pd);
            assert_eq!(rows, LONG_ENTITIES * CELLS);
            black_box(rows)
        })
    });
    g.finish();
    conn.close().map_err(|(_, e)| e).expect("close");
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = bench_point_reads, bench_bulk_reads, bench_writes, bench_scan_degradation
);

/// Custom main (instead of `criterion_main!`) so the ~150-200 MB synthetic DB
/// tempdir is removed after the summary — a `OnceLock` static never drops.
fn main() {
    benches();
    Criterion::default().configure_from_args().final_summary();
    support::cleanup();
}
