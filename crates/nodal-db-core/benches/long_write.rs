//! Focused long-tier write micro-bench (RESULTS.md alarm #1: "long-tier writes
//! cost ~3.6 ms/cell"). Unlike the full `gate` bench, this builds NO ~150 MB
//! synthetic DB — it drives `history::write_value` directly (the same entry the
//! protocol conformance suite uses) on a fresh schema DB, so it isolates the
//! per-cell cost of the batched `write_long` path from CHECKPOINT / RW-open /
//! `api` overhead. Two shapes, each an 8 760-cell (hourly-year) long value:
//!
//!   - `long_insert_8760`  — first-write of a fresh key (no close-out): pure
//!     batched insert + the one-query-per-key version scan.
//!   - `long_replace_8760` — re-write of an existing key (close-out all 8 760
//!     live cells, then reinsert): the churn primitive behind the 38.1 s
//!     replace cycle recorded in RESULTS.md.
//!
//! Divide the reported per-iteration time by 8 760 for the per-cell number.
//! Run with `cargo bench -p nodal-db-core --bench long_write`.

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration as StdDuration;

use criterion::{criterion_group, criterion_main, Criterion};

use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, Duration};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

const CELLS: i32 = 8760; // hourly year
const T0_US: i64 = 1_735_689_600 * 1_000_000; // 2025-01-01T00:00:00Z
const HOUR_US: i64 = 3_600 * 1_000_000;

static ENTITY_SEQ: AtomicI64 = AtomicI64::new(1);

/// A fresh 8 760-cell long value over a parametric hourly datetime axis (so the
/// `make_timestamp` accelerator path is exercised, mirroring the synthetic).
fn long_value(seed: i64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes: vec![CanonicalAxis {
            length: CELLS,
            properties: AxisProperties::default(),
            body: AxisBody::DatetimeParametric {
                start_ts: T0_US,
                step: Duration {
                    months: 0,
                    micros: HOUR_US,
                },
            },
        }],
        shape: ShapedData::Long(
            (0..CELLS)
                .map(|h| LongRow {
                    positions: vec![h],
                    value: ScalarValue::Float((seed + h as i64) as f64 * 1.5),
                })
                .collect(),
        ),
    }
}

fn write(path: &Path, key: ValueKey, seed: i64) {
    let v = long_value(seed);
    with_write_txn(path, "bench", "long write", |conn, c| {
        let mut cache = AxisCache::new();
        write_value(conn, c, key, &v, &mut cache, &WriteOptions::default(), None)
    })
    .expect("long write");
}

fn bench_long_write(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("long_write.duckdb");
    init_schema(&path).expect("schema");

    let mut g = c.benchmark_group("long_write");
    g.sample_size(10);
    g.warm_up_time(StdDuration::from_secs(1));
    g.measurement_time(StdDuration::from_secs(20));

    // First-write of a fresh key each iteration (distinct entity_id): no
    // close-out, so this is the pure batched-insert + version-scan cost.
    g.bench_function("long_insert_8760", |b| {
        b.iter(|| {
            let e = ENTITY_SEQ.fetch_add(1, Ordering::Relaxed);
            write(
                &path,
                ValueKey {
                    entity_id: e,
                    parameter_definition_id: 1,
                    alternative_id: 1,
                },
                e,
            );
        })
    });

    // Replace cycle: one fixed key, re-written every iteration — each iter
    // closes out 8 760 live cells and reinserts them (the churn primitive).
    let replace_key = ValueKey {
        entity_id: -1,
        parameter_definition_id: 1,
        alternative_id: 1,
    };
    write(&path, replace_key, 0); // seed the live generation
    let mut seed = 1i64;
    g.bench_function("long_replace_8760", |b| {
        b.iter(|| {
            seed += 1;
            write(&path, replace_key, seed);
        })
    });

    g.finish();
}

criterion_group!(benches, bench_long_write);
criterion_main!(benches);
