//! The rendering drift guard of axis_position_cost_PLAN (§8 test 1;
//! invariant 2: one renderer, identical results).
//!
//! The read paths stopped expanding parametric axes: `value_rows` and
//! `read_parameter` compute each coordinate with the scalar macros
//! `axis_param_ts / _num / _coord` and join stored positions only for the
//! axes in use, and `axis_position` is rebuilt from the same blocks. The
//! stored-position legs exist twice (`axis_position_stored` and
//! `axis_position_stored_in_use`). This test compares every rewritten
//! surface, row for row, against the pre-change text frozen in
//! `tests/fixtures/axis_render_oracle.sql` (installed as `*_old` temp
//! objects): `axis_position`, `audit_index_accelerator`, `value_rows`
//! (whole, windows, label/coord filters, point reads), `read_parameter` for
//! every (class, parameter) plain, with mixed-case names and filters, and
//! through the core's prepared statement (`api::read_parameter`'s text),
//! an unknown name, and `resolved_value_rows` of a scenario.
//!
//! Rows are compared as multisets with every float by its BITS (`to_bits`):
//! DuckDB's `EXCEPT ALL` and `hash()` identify -0.0 with 0.0, so they cannot
//! prove bit identity. Column names and types are compared too.
//!
//! The fixture is written through the core where the writer produces the
//! kind (parametric hourly and monthly datetime, irregular datetime,
//! parametric and irregular integer, parametric and irregular float through
//! `history::write_value`, labels with a declared order, float and int
//! arrays, ranks 1 to 4, history, a delete), plus raw SQL for what it cannot
//! (periods, time patterns, mixed months+micros steps, adversarial
//! datetime steps, out-of-range and negative positions, unused axes of
//! every kind, a scenario). The conformance-vector-21 float axis (the FMA
//! pin) is written through the core and its coordinate at pos 5 is asserted
//! `0x40dda52635cbf3d8` through `axis_position`, `value_rows` and
//! `read_parameter` (as dimension 1 and as dimension 2).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::{TimeUnit as DTimeUnit, Value};
use duckdb::Connection;

use nodal_db_core::api::{delete_values, write_parameter, DeleteKey};
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::{CanonicalValue, LabelDomain};
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, LabelPos};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

const ORACLE: &str = include_str!("fixtures/axis_render_oracle.sql");
const T0_US: i64 = 1_735_689_600 * 1_000_000; // 2025-01-01
const HOUR_US: i64 = 3_600 * 1_000_000;
const FMA_START: f64 = -4675.83329566021;
const FMA_STEP: f64 = 7006.486065829911;
const FMA_POS5_BITS: u64 = 0x40dd_a526_35cb_f3d8;

// --- batches -----------------------------------------------------------------

fn ts_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

/// Rank-1 datetime series per entity at the given timestamps.
fn ts_series(names: &[&str], times: &[i64], f: impl Fn(usize, usize) -> f64) -> RecordBatch {
    let (mut e, mut t, mut v) = (vec![], vec![], vec![]);
    for (k, n) in names.iter().enumerate() {
        for (s, ts) in times.iter().enumerate() {
            e.push(n.to_string());
            t.push(*ts);
            v.push(f(k, s));
        }
    }
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            ts_field("time"),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            Arc::new(StringArray::from(e)),
            Arc::new(TimestampMicrosecondArray::from(t)),
            Arc::new(Float64Array::from(v)),
        ],
    )
}

/// Rank-1 datetime series with an int measure.
fn ts_series_int(names: &[&str], times: &[i64]) -> RecordBatch {
    let (mut e, mut t, mut v) = (vec![], vec![], vec![]);
    for (k, n) in names.iter().enumerate() {
        for (s, ts) in times.iter().enumerate() {
            e.push(n.to_string());
            t.push(*ts);
            v.push((k * 1000 + s) as i64 - 7);
        }
    }
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            ts_field("time"),
            Field::new("value", DataType::Int64, false),
        ],
        vec![
            Arc::new(StringArray::from(e)),
            Arc::new(TimestampMicrosecondArray::from(t)),
            Arc::new(Int64Array::from(v)),
        ],
    )
}

/// Rank-1 integer-indexed series.
fn int_series(names: &[&str], idx: &[i64], int_measure: bool) -> RecordBatch {
    let (mut e, mut t, mut vf, mut vi) = (vec![], vec![], vec![], vec![]);
    for (k, n) in names.iter().enumerate() {
        for (s, i) in idx.iter().enumerate() {
            e.push(n.to_string());
            t.push(*i);
            vf.push(k as f64 * 0.5 + s as f64);
            vi.push((k * 10 + s) as i64);
        }
    }
    let (vfield, varr): (Field, ArrayRef) = if int_measure {
        (
            Field::new("value", DataType::Int64, false),
            Arc::new(Int64Array::from(vi)),
        )
    } else {
        (
            Field::new("value", DataType::Float64, false),
            Arc::new(Float64Array::from(vf)),
        )
    };
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
            vfield,
        ],
        vec![
            Arc::new(StringArray::from(e)),
            Arc::new(Int64Array::from(t)),
            varr,
        ],
    )
}

/// Rank-2 label x datetime.
fn label_ts(names: &[&str], labels: &[&str], steps: usize) -> RecordBatch {
    let (mut e, mut l, mut t, mut v) = (vec![], vec![], vec![], vec![]);
    for (k, n) in names.iter().enumerate() {
        for (j, lab) in labels.iter().enumerate() {
            for s in 0..steps {
                e.push(n.to_string());
                l.push(lab.to_string());
                t.push(T0_US + s as i64 * HOUR_US);
                v.push((k * 1000 + j * 10 + s) as f64);
            }
        }
    }
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("lab", DataType::Utf8, false),
            ts_field("time"),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            Arc::new(StringArray::from(e)),
            Arc::new(StringArray::from(l)),
            Arc::new(TimestampMicrosecondArray::from(t)),
            Arc::new(Float64Array::from(v)),
        ],
    )
}

/// Rank-4 label x integer x label x datetime, ragged (some cells missing):
/// a long value.
fn rank4(name: &str) -> RecordBatch {
    let (mut e, mut a, mut b, mut c, mut d, mut v) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for (i, la) in ["p", "q"].iter().enumerate() {
        for n in [3i64, 7, 8] {
            for lc in ["u", "v", "w"] {
                for s in 0..3i64 {
                    if (i as i64 + n + s) % 4 == 0 {
                        continue;
                    }
                    e.push(name.to_string());
                    a.push(la.to_string());
                    b.push(n);
                    c.push(lc.to_string());
                    d.push(T0_US + s * 2 * HOUR_US + (n % 3) * 1_000);
                    v.push(i as f64 * 100.0 + n as f64 + s as f64 / 8.0);
                }
            }
        }
    }
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
            ts_field("d"),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            Arc::new(StringArray::from(e)),
            Arc::new(StringArray::from(a)),
            Arc::new(Int64Array::from(b)),
            Arc::new(StringArray::from(c)),
            Arc::new(TimestampMicrosecondArray::from(d)),
            Arc::new(Float64Array::from(v)),
        ],
    )
}

fn scalars(names: &[&str], base: f64) -> RecordBatch {
    let v: Vec<f64> = (0..names.len()).map(|i| base + i as f64).collect();
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            Arc::new(StringArray::from(
                names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(v)),
        ],
    )
}

fn write(db: &Path, class: &str, param: &str, alt: &str, b: RecordBatch, dom: &[LabelDomain]) {
    write_parameter(
        db,
        class,
        param,
        alt,
        b,
        dom,
        WriteOptions::default(),
        "tester",
        "axis render fixture",
    )
    .unwrap_or_else(|e| panic!("write {class}.{param}/{alt}: {e}"));
}

/// Months-from-2024-01-31 timestamps (the writer finds the monthly step and
/// checks it with the clamp: Feb 29, Mar 31, Apr 30, ...).
fn month_ends() -> Vec<i64> {
    // days in month for 2024-01 .. 2024-12 (leap year), month-end clamp
    let days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let jan31: i64 = 1_706_659_200 * 1_000_000 + 6 * HOUR_US; // 2024-01-31 06:00
    let mut out = vec![jan31];
    let mut start_of_month = jan31 - 30 * 24 * HOUR_US; // 2024-01-01 06:00
    for (m, d) in days.iter().enumerate().take(11) {
        start_of_month += *d as i64 * 24 * HOUR_US;
        let next_days = days[m + 1] as i64;
        out.push(start_of_month + (next_days.min(31) - 1) * 24 * HOUR_US);
    }
    out
}

// --- the fixture ---------------------------------------------------------------

const NAMES: [&str; 6] = ["n0", "n1", "n2", "n3", "n4", "n5"];

fn fixture(dir: &Path) -> PathBuf {
    let db = dir.join("render.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    let hourly: Vec<i64> = (0..24).map(|h| T0_US + h * HOUR_US).collect();
    // Public API: parametric hourly float arrays in two alternatives.
    write(
        &db,
        "node",
        "profile",
        "Base",
        ts_series(&NAMES, &hourly, |k, s| (k * 100 + s) as f64 * 0.1),
        &[],
    );
    write(
        &db,
        "node",
        "profile",
        "alt1",
        ts_series(&NAMES, &hourly, |k, s| -((k * 100 + s) as f64)),
        &[],
    );
    // int arrays (regular int measure)
    write(
        &db,
        "node",
        "counts",
        "Base",
        ts_series_int(&NAMES[..4], &hourly[..12]),
        &[],
    );
    // irregular datetime with sub-second and pre-1970 times
    let irr = [
        -86_400_000_001i64,
        0,
        1_500_000,
        T0_US,
        T0_US + 37 * HOUR_US + 123_456,
        T0_US + 400 * 24 * HOUR_US,
    ];
    write(
        &db,
        "node",
        "irr",
        "Base",
        ts_series(&NAMES[..3], &irr, |k, s| (k + s) as f64 / 3.0),
        &[],
    );
    // monthly from Jan 31 (parametric months, clamped)
    write(
        &db,
        "node",
        "monthly",
        "Base",
        ts_series(&NAMES[1..3], &month_ends(), |k, s| (k * 12 + s) as f64),
        &[],
    );
    // stored (irregular) integer index, int measure: long tier
    write(
        &db,
        "node",
        "ragged",
        "Base",
        int_series(&NAMES[..4], &[0, 2, 5, 11], true),
        &[],
    );
    // parametric integer index
    write(
        &db,
        "node",
        "steps",
        "alt1",
        int_series(&NAMES[2..5], &[-10, -5, 0, 5, 10], false),
        &[],
    );
    // rank 2 label x datetime, declared label order differs from sorted order
    let dom = vec![LabelDomain {
        position: 1,
        labels: vec!["z".into(), "x".into(), "y".into()],
    }];
    write(
        &db,
        "node",
        "grid",
        "Base",
        label_ts(&NAMES[..3], &["x", "y", "z"], 5),
        &dom,
    );
    // rank 4, long
    let dom4 = vec![
        LabelDomain {
            position: 1,
            labels: vec!["q".into(), "p".into()],
        },
        LabelDomain {
            position: 3,
            labels: vec!["u".into(), "v".into(), "w".into()],
        },
    ];
    write(&db, "node", "cube", "Base", rank4("n4"), &dom4);
    // another class with the same names, another start
    let later: Vec<i64> = (0..10).map(|h| T0_US + (100 + h) * HOUR_US).collect();
    write(
        &db,
        "unit",
        "profile",
        "Base",
        ts_series(&NAMES[..2], &later, |k, s| (k + s) as f64),
        &[],
    );
    write(&db, "node", "cap", "Base", scalars(&NAMES, 10.0), &[]);
    write(&db, "node", "cap", "alt1", scalars(&NAMES[..3], -0.0), &[]);
    // history: replace Base profile; delete one alt1 value
    write(
        &db,
        "node",
        "profile",
        "Base",
        ts_series(&NAMES, &hourly, |k, s| (k * 100 + s) as f64 * 7.0),
        &[],
    );
    delete_values(
        &db,
        vec![DeleteKey {
            class: "node".into(),
            parameter: "profile".into(),
            alternative: "alt1".into(),
            entity: "n1".into(),
        }],
        "tester",
        "delete one",
        WriteOptions::default(),
    )
    .expect("delete");

    core_float_axes(&db);
    raw_sql_additions(&db);
    db
}

fn one_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Float axes (the batch writer has no float index column) and a label axis
/// with an explicit ordinal order, written through the core's own value
/// writer (`history::write_value`: canonical axis encoding and dedup).
fn core_float_axes(db: &Path) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(
        "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit, version)
         SELECT pd.entity_class_id, x.name, pd.valid_from_commit, 1
         FROM parameter_definition pd, (VALUES ('fma'), ('fma2'), ('firr')) x(name)
         WHERE pd.name = 'cap';",
    )
    .unwrap();
    let pd = |n: &str| {
        one_i64(
            &conn,
            &format!("SELECT id FROM parameter_definition WHERE name = '{n}'"),
        )
    };
    let ent = |n: &str| {
        one_i64(
            &conn,
            &format!(
                "SELECT e.id FROM entity e JOIN entity_class ec ON ec.id = e.entity_class_id \
                 WHERE ec.name = 'node' AND e.name = '{n}'"
            ),
        )
    };
    let (pd_fma, pd_fma2, pd_firr) = (pd("fma"), pd("fma2"), pd("firr"));
    let ents: Vec<i64> = NAMES.iter().map(|n| ent(n)).collect();
    conn.close().unwrap();

    let fma = || CanonicalAxis {
        length: 6,
        properties: AxisProperties::default(),
        body: AxisBody::FloatParametric {
            start: FMA_START,
            step: FMA_STEP,
        },
    };
    let labels = CanonicalAxis {
        length: 3,
        properties: AxisProperties::default(),
        body: AxisBody::Label {
            positions: vec![
                LabelPos {
                    ordinal: 2,
                    label: "hi".into(),
                },
                LabelPos {
                    ordinal: 0,
                    label: "lo".into(),
                },
                LabelPos {
                    ordinal: 1,
                    label: "mid".into(),
                },
            ],
        },
    };
    let firr = CanonicalAxis {
        length: 5,
        properties: AxisProperties::default(),
        body: AxisBody::FloatMaterialized {
            coords: vec![-1.5, 0.0, 2.25, 1e10, 1.7976931348623157e308],
        },
    };
    let long = |axes: Vec<CanonicalAxis>, rows: Vec<LongRow>| CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes,
        shape: ShapedData::Long(rows),
    };
    let key = |e: i64, p: i64| ValueKey {
        entity_id: e,
        parameter_definition_id: p,
        alternative_id: 1,
    };
    let vals: Vec<(ValueKey, CanonicalValue)> = vec![
        // FMA axis, rank 1: long and array
        (
            key(ents[0], pd_fma),
            long(
                vec![fma()],
                (0..6)
                    .map(|p| LongRow {
                        positions: vec![p],
                        value: ScalarValue::Float(p as f64 + 0.5),
                    })
                    .collect(),
            ),
        ),
        (
            key(ents[1], pd_fma),
            CanonicalValue {
                tier: Tier::Array,
                value_kind: ValueKind::Float,
                axes: vec![fma()],
                shape: ShapedData::Array(vec![1.0, -2.0, 3.0, -4.0, 5.0, -6.0]),
            },
        ),
        // label (ordinal != pos) x FMA axis: the FMA coordinate as dimension 2
        (
            key(ents[2], pd_fma2),
            long(
                vec![labels.clone(), fma()],
                (0..3)
                    .flat_map(|a| {
                        (0..6).map(move |b| LongRow {
                            positions: vec![a, b],
                            value: ScalarValue::Float((a * 10 + b) as f64),
                        })
                    })
                    .collect(),
            ),
        ),
        (
            key(ents[3], pd_fma2),
            CanonicalValue {
                tier: Tier::Array,
                value_kind: ValueKind::Float,
                axes: vec![labels, fma()],
                shape: ShapedData::Array((0..18).map(|x| x as f64 * 0.25).collect()),
            },
        ),
        // irregular float axis
        (
            key(ents[4], pd_firr),
            CanonicalValue {
                tier: Tier::Array,
                value_kind: ValueKind::Float,
                axes: vec![firr],
                shape: ShapedData::Array(vec![0.5, 1.5, 2.5, 3.5, 4.5]),
            },
        ),
    ];
    with_write_txn(db, "tester", "float axes", |conn, c| {
        let mut cache = AxisCache::new();
        for (k, v) in &vals {
            write_value(conn, c, *k, v, &mut cache, &WriteOptions::default(), None)?;
        }
        Ok(())
    })
    .expect("core float axes");
}

/// What the writer cannot produce, by raw SQL in one commit (test-only):
/// periods, time patterns, mixed and adversarial parametric datetime steps,
/// integer and float extremes, out-of-range and negative positions, ranks 3
/// and 4 mixing kinds, unused axes of every kind, and a scenario.
fn raw_sql_additions(db: &Path) {
    let conn = Connection::open(db).unwrap();
    // adversarial parametric datetime axes: (start, months, micros, length)
    let mut dt: Vec<(String, i32, i64, i32)> = vec![
        ("1900-02-28 23:59:59.999999".into(), 0, 1, 5),
        ("2000-03-01".into(), 0, -86_400_000_000, 5),
        ("2023-01-31".into(), 1, 0, 30),
        ("2024-02-29".into(), 12, 0, 10),
        ("2024-03-31 23:30".into(), -1, 3_600_000_000, 20),
        ("0001-01-01".into(), 0, 500_000_000_000, 100),
        ("1970-01-01".into(), 0, 86_400_000_000 * 365 * 1000, 200),
        ("2024-01-31".into(), 1, -1, 15),
        ("2024-05-05".into(), 0, 0, 3),
        ("1969-12-31 23:59:59.5".into(), 0, 250_000, 50),
        ("2100-02-28".into(), 1, 86_400_000_000, 40),
        ("1600-02-29 12:00".into(), 48, 0, 30),
        ("2024-02-29".into(), 13, 86_400_000_000, 6),
        ("2024-01-31 06:00".into(), 1, 0, 12),
    ];
    // deterministic pseudo-random starts and steps (an LCG, no crate)
    let mut s: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = |m: u64| {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (s >> 33) % m
    };
    for _ in 0..15 {
        let start = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            1 + next(9000),
            1 + next(12),
            1 + next(28),
            next(24),
            next(60),
            next(60),
            next(1_000_000)
        );
        let months = [0, 0, 0, 1, 3, -2, 13][next(7) as usize];
        let micros = [
            1i64,
            999,
            3_600_000_000,
            -7,
            86_399_999_999,
            123_456_789_012,
        ][next(6) as usize];
        dt.push((start, months, micros, 5 + next(300) as i32));
    }
    let ints: [(i64, i64, i32); 4] = [
        (-9_000_000_000_000_000_000, 10_000_000_000_000_000, 10),
        (5, -3, 20),
        (0, 1, 1000),
        (1 << 62, 1, 5),
    ];
    let mut flts: Vec<(f64, f64, i32)> = vec![
        (0.1, 0.1, 1000),
        (1e300, 1e292, 50),
        (-0.0, 0.5, 5),
        (5e-324, 5e-324, 10),
        (0.1, 0.2, 7),
    ];
    for _ in 0..20 {
        let mag = 10f64.powi(next(21) as i32 - 10);
        let start = (next(2_000_001) as f64 - 1e6) * mag;
        let step = 1e-9 + next(1_000_000) as f64 * 0.7;
        flts.push((start, step, 5 + next(400) as i32));
    }

    let mut sql = String::from(
        "BEGIN;
         INSERT INTO commit (user, message) VALUES ('tester', 'raw axis kinds');
         CREATE TEMP TABLE adv (id BIGINT, len INTEGER);\n",
    );
    for (st, m, u, l) in &dt {
        sql += &format!(
            "INSERT INTO axis (id, kind, length, content_hash, p_start_ts, p_step_months, p_step_micros)
             VALUES (nextval('seq_axis'), 'datetime', {l}, hash(currval('seq_axis')), TIMESTAMP '{st}', {m}, {u});
             INSERT INTO adv VALUES (currval('seq_axis'), {l});\n"
        );
    }
    for (st, step, l) in ints {
        sql += &format!(
            "INSERT INTO axis (id, kind, length, content_hash, p_start_int, p_step_int)
             VALUES (nextval('seq_axis'), 'integer', {l}, hash(currval('seq_axis')), {st}, {step});
             INSERT INTO adv VALUES (currval('seq_axis'), {l});\n"
        );
    }
    for (st, step, l) in &flts {
        sql += &format!(
            "INSERT INTO axis (id, kind, length, content_hash, p_start_dbl, p_step_dbl)
             VALUES (nextval('seq_axis'), 'float', {l}, hash(currval('seq_axis')), {st:e}::DOUBLE, {step:e}::DOUBLE);
             INSERT INTO adv VALUES (currval('seq_axis'), {l});\n"
        );
    }
    sql += "
        -- stored kinds the batch writer does not produce
        CREATE TEMP TABLE sx AS SELECT nextval('seq_axis') AS id, k FROM range(7) t(k);
        INSERT INTO axis (id, kind, length, content_hash)
        SELECT id, ['period', 'time_pattern', 'integer', 'datetime', 'period', 'label', 'datetime'][k + 1],
               [3, 2, 4, 3, 1, 2, 2][k + 1], hash(id) FROM sx;
        INSERT INTO axis_period SELECT id, p, s, e, y FROM sx,
          (VALUES (0, TIMESTAMP '2025-01-01', TIMESTAMP '2026-01-01', 2025.0),
                  (1, TIMESTAMP '2026-01-01', TIMESTAMP '2027-01-01', NULL),
                  (2, TIMESTAMP '2027-01-01', TIMESTAMP '2030-01-01', 2028.5)) v(p, s, e, y) WHERE k = 0;
        INSERT INTO axis_time_pattern SELECT id, p, o, t, u, a, b FROM sx,
          (VALUES (0, 1, 1, 'M', 1, 3), (0, 1, 2, 'h', 1, 12), (1, 0, 1, 'WD', 6, 7)) v(p, o, t, u, a, b) WHERE k = 1;
        INSERT INTO axis_integer SELECT id, p, n FROM sx,
          (VALUES (0, -100), (1, 3), (2, 4), (3, 99999999999)) v(p, n) WHERE k = 2;
        INSERT INTO axis_datetime SELECT id, p, t FROM sx,
          (VALUES (0, TIMESTAMP '2020-01-01'), (1, TIMESTAMP '2020-03-01 12:34:56.789'),
                  (2, TIMESTAMP '2021-01-01')) v(p, t) WHERE k = 3;
        -- unused: a period, a label, an irregular datetime; and two unused parametric axes
        INSERT INTO axis_period SELECT id, 0, TIMESTAMP '1990-01-01', TIMESTAMP '1991-01-01', NULL FROM sx WHERE k = 4;
        INSERT INTO axis_label (axis_id, pos, label, ordinal) SELECT id, p, l, o FROM sx,
          (VALUES (0, 'q', 1), (1, 'r', 0)) v(p, l, o) WHERE k = 5;
        INSERT INTO axis_datetime SELECT id, p, t FROM sx,
          (VALUES (0, TIMESTAMP '1990-01-01'), (1, TIMESTAMP '1991-01-01')) v(p, t) WHERE k = 6;
        INSERT INTO axis (id, kind, length, content_hash, p_start_ts, p_step_months, p_step_micros)
        VALUES (nextval('seq_axis'), 'datetime', 100, hash(currval('seq_axis')), TIMESTAMP '1999-01-01', 0, 60000000);
        INSERT INTO axis (id, kind, length, content_hash, p_start_int, p_step_int)
        VALUES (nextval('seq_axis'), 'integer', 50, hash(currval('seq_axis')), 7, 2);
        INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit, version)
        SELECT pd.entity_class_id, x.name, currval('seq_commit'), 1
        FROM parameter_definition pd, (VALUES ('kinds'), ('kinds_arr'), ('adv'), ('adv2'), ('adv_arr')) x(name)
        WHERE pd.name = 'cap';
        CREATE TEMP TABLE ids AS SELECT
          (SELECT id FROM parameter_definition WHERE name = 'kinds') AS pd,
          (SELECT id FROM parameter_definition WHERE name = 'kinds_arr') AS pda,
          (SELECT id FROM parameter_definition WHERE name = 'adv') AS pdv,
          (SELECT id FROM parameter_definition WHERE name = 'adv2') AS pdv2,
          (SELECT id FROM parameter_definition WHERE name = 'adv_arr') AS pdva,
          (SELECT axis1_id FROM value_long WHERE parameter_definition_id =
              (SELECT id FROM parameter_definition WHERE name = 'fma') LIMIT 1) AS fma,
          (SELECT max(id) FROM entity_class WHERE name = 'node') AS ec,
          currval('seq_commit') AS cm;
        -- one entity per raw value (the long tier's unique key has no axis)
        INSERT INTO entity (entity_class_id, name, valid_from_commit)
        SELECT ec, printf('r%03d', i), cm FROM ids, range(400) t(i);
        CREATE TEMP TABLE ents AS SELECT row_number() OVER (ORDER BY e.id) - 1 AS k, e.id
          FROM entity e, ids WHERE e.entity_class_id = ids.ec AND e.name LIKE 'r%';
        CREATE TEMP TABLE advk AS SELECT row_number() OVER (ORDER BY id) + 99 AS k, id, len FROM adv;
        CREATE TEMP TABLE kinds AS
          SELECT row_number() OVER (ORDER BY id) - 1 AS k, id, length FROM axis
          WHERE id IN (SELECT id FROM sx WHERE k < 4)
             OR id IN (SELECT DISTINCT axis1_id FROM value_long UNION SELECT DISTINCT axis1_id FROM value_array
                       UNION SELECT DISTINCT axis2_id FROM value_long WHERE axis2_id IS NOT NULL);
        -- rank-1 long rows on every axis in use or new, every position plus
        -- out-of-range and negative ones (must render NULL, never drop)
        INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version,
                                axis1_id, i1_pos, value_kind, v_float)
        SELECT (SELECT id FROM ents WHERE ents.k = kinds.k), pd, 1 + kinds.k % 2, cm, 1, kinds.id, p, 'float', kinds.id * 1000 + p
        FROM ids, kinds, range(-2, 1002) r(p)
        WHERE p <= kinds.length + 1 AND kinds.k < 60;
        -- rank 4 mixing kinds: period x time pattern x FMA float x stored integer
        INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version,
                                axis1_id, axis2_id, axis3_id, axis4_id, i1_pos, i2_pos, i3_pos, i4_pos, value_kind, v_str)
        SELECT (SELECT id FROM ents WHERE k = 61), pd, 2, cm, 1,
               (SELECT id FROM sx WHERE k = 0), (SELECT id FROM sx WHERE k = 1), fma, (SELECT id FROM sx WHERE k = 2),
               a, b, c, d, 'string', a || '-' || b || '-' || c || '-' || d
        FROM ids, range(3) t1(a), range(3) t2(b), range(0, 8, 3) t3(c), range(-1, 5) t4(d);
        -- rank 3: irregular datetime x stored integer x period
        INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version,
                                axis1_id, axis2_id, axis3_id, i1_pos, i2_pos, i3_pos, value_kind, v_int)
        SELECT (SELECT id FROM ents WHERE k = 63), pd, 1, cm, 1,
               (SELECT id FROM sx WHERE k = 3), (SELECT id FROM sx WHERE k = 2), (SELECT id FROM sx WHERE k = 0),
               a, b, c, 'int', a * 100 + b * 10 + c
        FROM ids, range(3) t1(a), range(4) t2(b), range(3) t3(c);
        -- arrays over raw kinds: period x time pattern (float), stored integer (int)
        INSERT INTO value_array (array_id, entity_id, parameter_definition_id, alternative_id, valid_from_commit,
                                 version, axis1_id, axis2_id, values, values_int)
        SELECT nextval('seq_value_array'), (SELECT id FROM ents WHERE k = 70), pda, 1, cm, 1,
               (SELECT id FROM sx WHERE k = 0), (SELECT id FROM sx WHERE k = 1), [1.0, 2.0, -0.0, 4.0, 5.0, 6.0], NULL FROM ids
        UNION ALL
        SELECT nextval('seq_value_array'), (SELECT id FROM ents WHERE k = 71), pda, 1, cm, 1,
               (SELECT id FROM sx WHERE k = 2), NULL, NULL, [10, 20, 30, 40] FROM ids;
        -- adversarial parametric axes: long rows at -2 .. length+1, rank 2
        -- against the FMA axis, and rank-1/rank-2 arrays on each
        INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version,
                                axis1_id, i1_pos, value_kind, v_float)
        SELECT (SELECT id FROM ents WHERE ents.k = adv.k), pdv, 1 + adv.k % 2, cm, 1, adv.id, p, 'float', p
        FROM ids, advk adv, range(-2, 1003) r(p) WHERE p <= adv.len + 1;
        INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version,
                                axis1_id, axis2_id, i1_pos, i2_pos, value_kind, v_float)
        SELECT (SELECT id FROM ents WHERE ents.k = adv.k), pdv2, 1, cm, 1, fma, adv.id, a, b, 'float', a * 1000 + b
        FROM ids, advk adv, range(6) t(a), range(-1, 62) u(b) WHERE b <= least(adv.len, 60);
        INSERT INTO value_array (array_id, entity_id, parameter_definition_id, alternative_id, valid_from_commit,
                                 version, axis1_id, axis2_id, values, values_int)
        SELECT nextval('seq_value_array'), (SELECT id FROM ents WHERE ents.k = adv.k), pdva, 1 + adv.k % 2, cm, 1,
               adv.id, CASE WHEN adv.k % 3 = 0 THEN fma END,
               CASE WHEN adv.k % 3 <> 0 THEN [x * 1.0 FOR x IN range(adv.len)] END,
               CASE WHEN adv.k % 3 = 0 THEN [x FOR x IN range(adv.len * 6)] END
        FROM ids, advk adv;
        -- a scenario over two alternatives
        INSERT INTO scenario (id, name, valid_from_commit) SELECT nextval('seq_scenario'), 'Stress', cm FROM ids;
        INSERT INTO scenario_alternative (scenario_id, alternative_id, rank, valid_from_commit)
        SELECT currval('seq_scenario'), al.id, row_number() OVER (ORDER BY al.id), ids.cm
        FROM alternative al, ids WHERE al.valid_to_commit IS NULL;
        COMMIT;
        CHECKPOINT;";
    conn.execute_batch(&sql)
        .unwrap_or_else(|e| panic!("raw SQL additions: {e}"));
    conn.close().unwrap();
}

// --- comparison ----------------------------------------------------------------

fn render(v: &Value) -> String {
    match v {
        // bits, not values: -0.0 and 0.0 must not compare equal
        Value::Double(x) => format!("f:{:016x}", x.to_bits()),
        Value::Float(x) => format!("f32:{:08x}", x.to_bits()),
        Value::Timestamp(unit, x) => {
            let us = match unit {
                DTimeUnit::Second => x * 1_000_000,
                DTimeUnit::Millisecond => x * 1_000,
                DTimeUnit::Microsecond => *x,
                DTimeUnit::Nanosecond => x / 1_000,
            };
            format!("ts:{us}")
        }
        other => format!("{other:?}"),
    }
}

type Bag = HashMap<Vec<String>, usize>;

fn bag(conn: &Connection, sql: &str, args: &[&dyn duckdb::ToSql]) -> (Bag, usize) {
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("prepare {sql}: {e}"));
    let mut rows = stmt.query(args).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let n = rows.as_ref().unwrap().column_count();
    let mut out = Bag::new();
    let mut total = 0;
    while let Some(r) = rows.next().unwrap() {
        let key: Vec<String> = (0..n)
            .map(|i| render(&r.get::<_, Value>(i).unwrap()))
            .collect();
        *out.entry(key).or_default() += 1;
        total += 1;
    }
    (out, total)
}

fn described(conn: &Connection, sql: &str) -> Vec<(String, String)> {
    let mut stmt = conn.prepare(&format!("DESCRIBE {sql}")).unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .unwrap();
    rows.map(Result::unwrap).collect()
}

/// `sql` contains `{S}` where the object name takes its `_old` suffix.
fn assert_equal(conn: &Connection, sql: &str, args: &[&dyn duckdb::ToSql]) -> usize {
    let new_sql = sql.replace("{S}", "");
    let old_sql = sql.replace("{S}", "_old");
    let (a, na) = bag(conn, &new_sql, args);
    let (b, nb) = bag(conn, &old_sql, args);
    if a != b {
        let only_new: Vec<_> = a
            .iter()
            .filter(|(k, v)| b.get(*k) != Some(v))
            .take(3)
            .collect();
        let only_old: Vec<_> = b
            .iter()
            .filter(|(k, v)| a.get(*k) != Some(v))
            .take(3)
            .collect();
        panic!(
            "rendering drift (invariant 2): {new_sql}\n new {na} rows, frozen oracle {nb} rows\n \
             new-only (sample): {only_new:?}\n oracle-only (sample): {only_old:?}"
        );
    }
    if args.is_empty() {
        assert_eq!(
            described(conn, &new_sql),
            described(conn, &old_sql),
            "column names/types differ: {new_sql}"
        );
    }
    na
}

fn oracle_conn(db: &Path) -> Connection {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(ORACLE)
        .expect("install the frozen oracle");
    conn
}

#[test]
fn rewritten_reads_render_bit_identically_to_the_frozen_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = oracle_conn(&db);

    // non-vacuity: the fixture holds what the test claims
    let kinds: Vec<(String, bool)> = {
        let mut st = conn
            .prepare(
                "SELECT DISTINCT kind, (p_start_ts IS NOT NULL OR p_start_int IS NOT NULL \
                 OR p_start_dbl IS NOT NULL) FROM axis ORDER BY ALL",
            )
            .unwrap();
        let r = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        r.map(Result::unwrap).collect()
    };
    for want in [
        ("datetime", true),
        ("datetime", false),
        ("integer", true),
        ("integer", false),
        ("float", true),
        ("float", false),
        ("label", false),
        ("period", false),
        ("time_pattern", false),
    ] {
        assert!(
            kinds.contains(&(want.0.to_string(), want.1)),
            "fixture lacks a {want:?} axis: {kinds:?}"
        );
    }
    for (what, sql) in [
        ("closed-out value rows", "SELECT count(*) FROM value_array_hist"),
        ("a rank-4 value", "SELECT count(*) FROM value_long WHERE axis4_id IS NOT NULL"),
        ("int arrays", "SELECT count(*) FROM value_array WHERE values_int IS NOT NULL"),
        ("negative positions", "SELECT count(*) FROM value_long WHERE i1_pos < 0"),
        ("a parametric months+micros axis", "SELECT count(*) FROM axis WHERE p_step_months <> 0 AND p_step_micros <> 0"),
        ("unused axes", "SELECT count(*) FROM axis WHERE id NOT IN (SELECT axis1_id FROM value_long UNION SELECT axis2_id FROM value_long WHERE axis2_id IS NOT NULL UNION SELECT axis3_id FROM value_long WHERE axis3_id IS NOT NULL UNION SELECT axis4_id FROM value_long WHERE axis4_id IS NOT NULL UNION SELECT axis1_id FROM value_array UNION SELECT axis2_id FROM value_array WHERE axis2_id IS NOT NULL)"),
    ] {
        assert!(one_i64(&conn, sql) > 0, "fixture lacks {what}");
    }

    let mut checks = 0;
    let mut eq = |sql: &str| {
        assert_equal(&conn, sql, &[]);
        checks += 1;
    };
    eq("SELECT * FROM axis_position{S}");
    eq("SELECT * FROM audit_index_accelerator{S}");
    eq("SELECT * FROM value_rows{S}");
    eq("SELECT * FROM value_rows{S} WHERE idx1_ts BETWEEN TIMESTAMP '2025-01-01 03:00' AND TIMESTAMP '2025-01-01 09:00'");
    eq("SELECT * FROM value_rows{S} WHERE idx1_text IS NOT NULL OR idx2_coord > 0 OR idx3_num < 5");
    eq("SELECT * FROM value_rows{S} WHERE idx1_ts IS NULL AND tier <> 'scalar'");
    eq("SELECT * FROM value_rows{S} WHERE entity_id = (SELECT min(id) FROM entity) AND parameter_definition_id = 1 AND alternative_id = 1");
    eq("SELECT * FROM resolved_value_rows{S}('Stress')");
    eq("SELECT * FROM resolved_value_rows{S}('STRESS') WHERE idx1_ts > TIMESTAMP '2025-01-01 10:00'");
    eq("SELECT * FROM resolved_value_rows{S}('nope')");
    let pairs: Vec<(String, String)> = {
        let mut st = conn
            .prepare(
                "SELECT ec.name, pd.name FROM parameter_definition pd \
                 JOIN entity_class ec ON ec.id = pd.entity_class_id \
                 WHERE pd.valid_to_commit IS NULL AND ec.valid_to_commit IS NULL ORDER BY ALL",
            )
            .unwrap();
        let r = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        r.map(Result::unwrap).collect()
    };
    assert!(pairs.len() >= 15, "fixture parameters: {pairs:?}");
    for (cl, p) in &pairs {
        eq(&format!("SELECT * FROM read_parameter{{S}}('{cl}', '{p}')"));
        eq(&format!(
            "SELECT * FROM read_parameter{{S}}('{}', '{}') WHERE alternative = 'alt1' OR entity IN ('n0', 'n3')",
            cl.to_uppercase(),
            p.to_uppercase()
        ));
    }
    eq("SELECT * FROM read_parameter{S}('nope', 'nope')");
    eq("SELECT * FROM read_parameter{S}('node', 'nope')");

    // D15: the core's prepared statement (api::read_parameter), bound values
    let mut prepared = 0;
    for (cl, p) in &pairs {
        for alt in ["BASE", "alt1"] {
            assert_equal(
                &conn,
                "SELECT * FROM read_parameter{S}(?, ?) WHERE lower(alternative) = lower(?)",
                &[cl, p, &alt],
            );
            prepared += 1;
        }
    }
    eprintln!("{checks} literal + {prepared} prepared reads equal to the frozen oracle");
    conn.close().unwrap();
}

/// The FMA pin (conformance vector 21, written through the core): coord at
/// pos 5 is the two-rounding `start + step*pos`, 0x40dda52635cbf3d8, through
/// every rendering surface.
#[test]
fn fma_pin_holds_through_every_rendering_surface() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = Connection::open(&db).unwrap();
    let ax = one_i64(
        &conn,
        &format!("SELECT id FROM axis WHERE kind = 'float' AND p_start_dbl = {FMA_START:e}::DOUBLE AND p_step_dbl = {FMA_STEP:e}::DOUBLE"),
    );
    let bits = |sql: &str| -> Vec<u64> {
        let mut st = conn.prepare(sql).unwrap();
        let r = st.query_map([], |r| r.get::<_, f64>(0)).unwrap();
        r.map(|x| x.unwrap().to_bits()).collect()
    };
    for (what, sql) in [
        (
            "axis_position",
            format!("SELECT coord FROM axis_position WHERE axis_id = {ax} AND pos = 5"),
        ),
        (
            "value_rows rank 1",
            "SELECT idx1_coord FROM value_rows WHERE parameter_definition_id = \
             (SELECT id FROM parameter_definition WHERE name = 'fma') AND i1_pos = 5"
                .to_string(),
        ),
        (
            "value_rows dimension 2",
            "SELECT idx2_coord FROM value_rows WHERE parameter_definition_id = \
             (SELECT id FROM parameter_definition WHERE name = 'fma2') AND i2_pos = 5"
                .to_string(),
        ),
        (
            "read_parameter rank 1",
            "SELECT idx1_coord FROM read_parameter('node', 'fma') WHERE idx1_ordinal = 5"
                .to_string(),
        ),
        (
            "read_parameter dimension 2",
            "SELECT idx2_coord FROM read_parameter('node', 'fma2') WHERE idx2_ordinal = 5"
                .to_string(),
        ),
    ] {
        let got = bits(&sql);
        assert!(!got.is_empty(), "{what}: no row at pos 5\n{sql}");
        assert!(
            got.iter().all(|b| *b == FMA_POS5_BITS),
            "{what}: coord at pos 5 = {:?}, pinned {FMA_POS5_BITS:#018x} (non-FMA start + step*pos; \
             spec §10.2)\n{sql}",
            got.iter().map(|b| format!("{b:#018x}")).collect::<Vec<_>>()
        );
    }
    conn.close().unwrap();
}
