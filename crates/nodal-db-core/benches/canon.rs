//! Pure canonicalization-pipeline bench (`canon::canonicalize`, no DB).
//!
//! Isolates the per-cell cost of pipeline step 1 (reading value cells), where
//! the value-float normalization (`-0.0 → +0.0`, every NaN → the one quiet
//! NaN) runs. Shapes:
//!   (a) rank-1 8 760-cell full float grid (array tier);
//!   (b) rank-2 1 000 × 1 000 full float grid, 1 M cells (bulk array path),
//!       all-finite and with ~1 % `-0.0`/NaN mixed in;
//!   (c) 8 760-cell sparse rank-2 value (long tier).
//!
//! `cargo bench -p nodal-db-core --bench canon`.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use nodal_db_core::canon::canonicalize;
use nodal_db_core::duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array};
use nodal_db_core::duckdb::arrow::datatypes::{DataType, Field, Schema};
use nodal_db_core::duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::encoding::canonical_value_f64;
use nodal_db_core::types::{DeclaredType, IndexKind, ValueKind};

fn batch(dims: Vec<Vec<i64>>, values: Vec<f64>) -> RecordBatch {
    let mut fields = Vec::new();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for (i, d) in dims.into_iter().enumerate() {
        fields.push(Field::new(format!("d{i}"), DataType::Int64, false));
        cols.push(Arc::new(Int64Array::from(d)));
    }
    fields.push(Field::new("v", DataType::Float64, true));
    cols.push(Arc::new(Float64Array::from(values)));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn declared(rank: usize) -> DeclaredType {
    DeclaredType {
        positions: (1..=rank as u8).map(|p| (p, IndexKind::Integer)).collect(),
    }
}

/// Deterministic values. With `specials`, every 100th cell is a special
/// value instead: alternately `-0.0` and a negative NaN.
fn values(n: usize, specials: bool) -> Vec<f64> {
    (0..n)
        .map(|i| {
            if specials && i % 100 == 7 {
                if (i / 100) % 2 == 0 {
                    -0.0
                } else {
                    f64::from_bits(0xfff8_0000_0000_0000)
                }
            } else {
                (i as f64) * 0.25 + 1.0
            }
        })
        .collect()
}

fn bench_canon(c: &mut Criterion) {
    // (a) rank-1 8 760-cell full grid.
    let a = batch(vec![(0..8760).collect()], values(8760, false));
    let d1 = declared(1);
    c.bench_function("canon_rank1_8760_full", |b| {
        b.iter(|| canonicalize(black_box(&a), &d1, ValueKind::Float, &[]).unwrap())
    });

    // (c) 8 760-cell sparse rank-2: (h, h % 7) pairs — never a full grid.
    let hs: Vec<i64> = (0..8760).collect();
    let ks: Vec<i64> = hs.iter().map(|h| h % 7).collect();
    let cc = batch(vec![hs, ks], values(8760, false));
    let d2 = declared(2);
    c.bench_function("canon_rank2_8760_sparse_long", |b| {
        b.iter(|| canonicalize(black_box(&cc), &d2, ValueKind::Float, &[]).unwrap())
    });

    // (b) rank-2 1 000 × 1 000 full grid (1 M cells).
    let n = 1000i64;
    let i: Vec<i64> = (0..n * n).map(|k| k / n).collect();
    let j: Vec<i64> = (0..n * n).map(|k| k % n).collect();
    let finite = batch(vec![i.clone(), j.clone()], values((n * n) as usize, false));
    let mixed = batch(vec![i, j], values((n * n) as usize, true));
    let mut g = c.benchmark_group("canon_rank2_1m_full");
    g.sample_size(10);
    g.bench_function("finite", |b| {
        b.iter(|| canonicalize(black_box(&finite), &d2, ValueKind::Float, &[]).unwrap())
    });
    g.bench_function("one_percent_negzero_nan", |b| {
        b.iter(|| canonicalize(black_box(&mixed), &d2, ValueKind::Float, &[]).unwrap())
    });
    g.finish();

    // The added work in isolation: `canonical_value_f64` over 1 M floats
    // (~1 % specials), the tiebreaker if the end-to-end delta is in the noise.
    let xs = values((n * n) as usize, true);
    c.bench_function("canonical_value_f64_1m", |b| {
        b.iter(|| {
            black_box(&xs)
                .iter()
                .map(|x| canonical_value_f64(*x).to_bits())
                .fold(0u64, |acc, x| acc ^ x)
        })
    });
}

criterion_group!(benches, bench_canon);
criterion_main!(benches);
