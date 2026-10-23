//! Paired A/B write-path sample (appender_write_path_PLAN §8): one invocation
//! = one measurement of one mode, printed as milliseconds per unit (per
//! value, or per key for `read_long`). Built twice (old core / new core) and
//! driven interleaved by `ab.py`. DB init and seeding are never timed; every
//! write goes through `history::write_value` or `api::write_parameter`.
//!
//! Modes: long_insert, long_replace (8 760-cell long values), array_insert,
//! array_replace (8 760-element arrays, 20 per transaction), axis_insert (a
//! long value on a new irregular 8 760-point datetime axis, whole commit),
//! scalar_insert, scalar_replace (400 per transaction), api_100_scalars (the
//! gate's write_100_new_scalar_entities shape), read_long (current-cell scan
//! of 100 x 876-cell long values written by the same build).
//!
//! Set-based scalar write modes (set_based_scalar_write_PLAN §6), all through
//! the API and timed per whole commit (open, write, commit, close):
//! api_10k_scalars (one 10 000-entity commit into a fresh file, per value),
//! and, on a COPY of a pre-built 10^5-scalar file named by the env var
//! `NODAL_AB_BASE`: api_100_scalars_100k (100 new entities, per value),
//! api_1_scalar_100k (a single-value replace, per commit; the editor save)
//! and api_20x1_scalars_100k (20 segments x 1 replaced value, per commit).
//! Build the base once with `ab_write_path build_100k <path>` (prints the
//! build time; build it with the faster arm, the file format is the same).

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::{commit_changes, write_parameter, Segment};
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::{with_read, with_write_txn};
use nodal_db_core::duckdb;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, Duration};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

const T0: i64 = 1_735_689_600 * 1_000_000;
const H: i64 = 3_600_000_000;

fn key(e: i64) -> ValueKey {
    ValueKey {
        entity_id: e,
        parameter_definition_id: 1,
        alternative_id: 1,
    }
}

fn hourly(n: i32) -> CanonicalAxis {
    CanonicalAxis {
        length: n,
        properties: AxisProperties::default(),
        body: AxisBody::DatetimeParametric {
            start_ts: T0,
            step: Duration {
                months: 0,
                micros: H,
            },
        },
    }
}

fn long_v(seed: i64, n: i32, axis: CanonicalAxis) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes: vec![axis],
        shape: ShapedData::Long(
            (0..n)
                .map(|h| LongRow {
                    positions: vec![h],
                    value: ScalarValue::Float((seed + h as i64) as f64 * 1.5),
                })
                .collect(),
        ),
    }
}

fn array_v(seed: i64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![hourly(8760)],
        shape: ShapedData::Array((0..8760).map(|h| (seed + h) as f64 * 0.25).collect()),
    }
}

fn scalar_v(x: f64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(x)),
    }
}

/// Write `vals` (key, value) in ONE transaction; returns elapsed ms.
fn txn(db: &Path, vals: &[(ValueKey, CanonicalValue)]) -> f64 {
    let t = Instant::now();
    with_write_txn(db, "ab", "ab", |conn, c| {
        let mut cache = AxisCache::new();
        for (k, v) in vals {
            write_value(conn, c, *k, v, &mut cache, &WriteOptions::default(), None)?;
        }
        Ok(())
    })
    .expect("write");
    t.elapsed().as_secs_f64() * 1e3
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

/// One `write_parameter` of a rank-0 float batch on class `node`; returns
/// the elapsed ms of the whole commit.
fn api_scalars(db: &Path, parameter: &str, names: &[String], vals: &[f64]) -> f64 {
    let t = Instant::now();
    write_parameter(
        db,
        "node",
        parameter,
        "Base",
        scalar_batch(names, vals),
        &[],
        WriteOptions::default(),
        "ab",
        "ab",
    )
    .unwrap();
    t.elapsed().as_secs_f64() * 1e3
}

/// The 20 editor-shaped parameters of the 10^5 base file.
fn q_param(i: usize) -> String {
    format!("q{i:02}")
}

/// Build the 10^5-scalar base file: parameter `p` over entities
/// `s000000..s099999` (10 commits of 10 000, the gate's shape), then one
/// value on `s000000` for each of the 20 parameters `q00..q19`.
fn build_100k(path: &Path) {
    let t = Instant::now();
    init_schema_with_default(path, Some("Base")).unwrap();
    for chunk in 0..10 {
        let names: Vec<String> = (0..10_000)
            .map(|i| format!("s{:06}", chunk * 10_000 + i))
            .collect();
        let vals: Vec<f64> = (0..10_000)
            .map(|i| (chunk * 10_000 + i) as f64 * 0.5)
            .collect();
        api_scalars(path, "p", &names, &vals);
    }
    let segs: Vec<Segment> = (0..20).map(|i| q_segment(i, "s000000", i as f64)).collect();
    commit_changes(
        path,
        segs,
        vec![],
        vec![],
        vec![],
        "ab",
        "q",
        WriteOptions::default(),
    )
    .unwrap();
    eprintln!(
        "built {} in {:.1} s",
        path.display(),
        t.elapsed().as_secs_f64()
    );
}

fn q_segment(i: usize, entity: &str, v: f64) -> Segment {
    Segment {
        class: "node".into(),
        parameter: q_param(i),
        alternative: "Base".into(),
        batches: vec![scalar_batch(&[entity.to_string()], &[v])],
        domains: vec![],
    }
}

/// A private copy of the `NODAL_AB_BASE` file in `dir` (never timed).
fn copy_base(dir: &Path) -> std::path::PathBuf {
    let base = std::env::var("NODAL_AB_BASE").expect("set NODAL_AB_BASE to a build_100k file");
    let db = dir.join("base.duckdb");
    std::fs::copy(base, &db).expect("copy base");
    db
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].as_str();
    if mode == "build_100k" {
        build_100k(Path::new(&args[2]));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("ab.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();

    let ms_per_unit = match mode {
        // 3 fresh 8 760-cell long values, one txn each.
        "long_insert" => {
            let mut tot = 0.0;
            for e in 1..=3 {
                tot += txn(&db, &[(key(e), long_v(e, 8760, hourly(8760)))]);
            }
            tot / 3.0
        }
        // seed one, then 3 full replaces (close-out 8 760 + reinsert).
        "long_replace" => {
            txn(&db, &[(key(1), long_v(0, 8760, hourly(8760)))]);
            let mut tot = 0.0;
            for s in 1..=3 {
                tot += txn(&db, &[(key(1), long_v(s, 8760, hourly(8760)))]);
            }
            tot / 3.0
        }
        // 20 fresh 8 760-element arrays in one txn.
        "array_insert" => {
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e))).collect();
            txn(&db, &v) / 20.0
        }
        // seed 20, then replace all 20 in one txn.
        "array_replace" => {
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e))).collect();
            txn(&db, &v);
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e + 7))).collect();
            txn(&db, &v) / 20.0
        }
        // one long value on a NEW irregular 8 760-coordinate datetime axis
        // (8 760 axis_datetime child rows), minus nothing: whole txn.
        "axis_insert" => {
            let coords: Vec<i64> = (0..8760).map(|h| T0 + h * H + (h % 7)).collect();
            let ax = CanonicalAxis {
                length: 8760,
                properties: AxisProperties::default(),
                body: AxisBody::DatetimeMaterialized { coords },
            };
            txn(&db, &[(key(1), long_v(1, 8760, ax))])
        }
        // 400 fresh scalars in one txn (µs/value reported as ms).
        "scalar_insert" => {
            let v: Vec<_> = (1..=400).map(|e| (key(e), scalar_v(e as f64))).collect();
            txn(&db, &v) / 400.0
        }
        "scalar_replace" => {
            let v: Vec<_> = (1..=400).map(|e| (key(e), scalar_v(e as f64))).collect();
            txn(&db, &v);
            let v: Vec<_> = (1..=400)
                .map(|e| (key(e), scalar_v(e as f64 + 0.5)))
                .collect();
            txn(&db, &v) / 400.0
        }
        // The gate's write_100_new_scalar_entities, through the API.
        "api_100_scalars" => {
            let mut tot = 0.0;
            for run in 0..3 {
                let names: Vec<String> = (0..100).map(|i| format!("w{run}_{i:03}")).collect();
                let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
                let t = Instant::now();
                write_parameter(
                    &db,
                    "node",
                    "p",
                    "Base",
                    scalar_batch(&names, &vals),
                    &[],
                    WriteOptions::default(),
                    "ab",
                    "ab",
                )
                .unwrap();
                tot += t.elapsed().as_secs_f64() * 1e3;
            }
            tot / 300.0
        }
        // The gate build's unit: one 10 000-new-entity scalar commit.
        "api_10k_scalars" => {
            let names: Vec<String> = (0..10_000).map(|i| format!("s{i:06}")).collect();
            let vals: Vec<f64> = (0..10_000).map(|i| i as f64).collect();
            api_scalars(&db, "p", &names, &vals) / 10_000.0
        }
        // 100 new scalar entities into the 10^5 file, 3 commits.
        "api_100_scalars_100k" => {
            let db = copy_base(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let names: Vec<String> = (0..100).map(|i| format!("w{run}_{i:03}")).collect();
                let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
                tot += api_scalars(&db, "p", &names, &vals);
            }
            tot / 300.0
        }
        // The editor save: one replaced value on the 10^5 file, per commit
        // (mean of 3 commits).
        "api_1_scalar_100k" => {
            let db = copy_base(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let names = vec![format!("s{:06}", 50_000 + run)];
                tot += api_scalars(&db, "p", &names, &[run as f64 + 0.25]);
            }
            tot / 3.0
        }
        // 20 segments x 1 replaced value in one commit on the 10^5 file, per
        // commit (mean of 3 commits).
        "api_20x1_scalars_100k" => {
            let db = copy_base(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let segs: Vec<Segment> = (0..20)
                    .map(|i| q_segment(i, "s000000", (run * 100 + i) as f64 + 0.5))
                    .collect();
                let t = Instant::now();
                commit_changes(
                    &db,
                    segs,
                    vec![],
                    vec![],
                    vec![],
                    "ab",
                    "ab",
                    WriteOptions::default(),
                )
                .unwrap();
                tot += t.elapsed().as_secs_f64() * 1e3;
            }
            tot / 3.0
        }
        // Read row: 100 keys x 876-cell long values written by THIS build,
        // then the current-cell scan of each key by triple (x5), per key.
        "read_long" => {
            let v: Vec<_> = (1..=100)
                .map(|e| (key(e), long_v(e, 876, hourly(876))))
                .collect();
            txn(&db, &v);
            let t = Instant::now();
            with_read(&db, |conn| {
                let mut stmt = conn.prepare(
                    "SELECT sum(v_float), count(*) FROM value_long \
                     WHERE entity_id = ? AND parameter_definition_id = 1 \
                       AND alternative_id = 1 AND valid_to_commit IS NULL",
                )?;
                for _ in 0..5 {
                    for e in 1..=100i64 {
                        let n: i64 = stmt.query_row([e], |r| r.get(1))?;
                        assert_eq!(n, 876);
                    }
                }
                Ok(())
            })
            .unwrap();
            t.elapsed().as_secs_f64() * 1e3 / 500.0
        }
        other => panic!("unknown mode {other}"),
    };
    println!("{ms_per_unit:.6}");
}
