//! Randomized write-path equivalence workload (see `check.py` next to this
//! crate, and CONTRIBUTING "Write-path equivalence check"). Built against
//! two cores (an old and a new revision); each run drives the same seeded
//! random script of commits through the public API (segments of every
//! tier and value kind, several class and alternative spellings,
//! structural creates, value deletes with misses and duplicates, alternative
//! erasure, and direct `history::write_value` / `delete_value` commits),
//! then prints every commit's report or error, every row of every base
//! table, the sequence counters and the row count of every `audit_*` view.
//! Two cores whose write paths are meant to be equivalent print the same
//! text.
//!
//! Not deterministic across runs, so excluded from the dump: the commit
//! log's wall-clock `timestamp` and the per-file `db_identity` metadata
//! row. Engine error texts are reduced to "Duckdb error".
//!
//! The workload uses only API present since `73e70d6` (2026-10-24); to
//! compare older revisions, use a copy of this crate from their time.
//!
//! usage: write_equivalence <seed> <commits> <big:0|1> <dir>

use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use nodal_db_core::api::{commit_changes, DeleteKey, Segment};
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::{CanonicalValue, LabelDomain};
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::duckdb::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use nodal_db_core::duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit as ATU};
use nodal_db_core::duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::duckdb::types::{TimeUnit, Value};
use nodal_db_core::duckdb::Connection;
use nodal_db_core::encoding::{
    AxisBody, AxisProperties, CanonicalAxis, Duration, PeriodPos, TimePatternPos, TimePatternTerm,
    CANONICAL_NAN_BITS,
};
use nodal_db_core::erasure::{erase_alternative, EraseOpts};
use nodal_db_core::error::DbError;
use nodal_db_core::history::{delete_value, write_value};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::template::ir::{DimSpec, TypeDecl};
use nodal_db_core::types::{
    LongRow, ObjectDeleteKey, ObjectKind, ObjectRef, ScalarValue, ShapedData, StructuralChange,
    Tier, ValueKey, ValueKind, WriteOptions,
};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn subset(&mut self, n: usize, min: usize) -> Vec<usize> {
        loop {
            let v: Vec<usize> = (0..n).filter(|_| self.chance(50)).collect();
            if v.len() >= min {
                return v;
            }
        }
    }
}

fn nan() -> f64 {
    f64::from_bits(CANONICAL_NAN_BITS)
}

const T0: i64 = 1_577_836_800_000_000;
const H: i64 = 3_600_000_000;
const ENTITIES: usize = 24;

fn ename(i: usize) -> String {
    format!("e{i:02}")
}

#[derive(Clone, Copy, PartialEq)]
enum DimK {
    Int,
    Dt,
    Label,
}

#[derive(Clone, Copy, PartialEq)]
enum MeasK {
    F,
    I,
    B,
    S,
    T,
}

/// One parameter of the script: fixed dims and measure kind.
struct Param {
    name: &'static str,
    dims: &'static [(&'static str, DimK)],
    meas: MeasK,
    /// Grid extent per dim.
    ext: usize,
    /// Probability (%) of a dense (full-grid) value.
    dense: u64,
}

const PARAMS: &[Param] = &[
    Param {
        name: "sc_f",
        dims: &[],
        meas: MeasK::F,
        ext: 1,
        dense: 100,
    },
    Param {
        name: "sc_i",
        dims: &[],
        meas: MeasK::I,
        ext: 1,
        dense: 100,
    },
    Param {
        name: "sc_b",
        dims: &[],
        meas: MeasK::B,
        ext: 1,
        dense: 100,
    },
    Param {
        name: "sc_s",
        dims: &[],
        meas: MeasK::S,
        ext: 1,
        dense: 100,
    },
    Param {
        name: "sc_t",
        dims: &[],
        meas: MeasK::T,
        ext: 1,
        dense: 100,
    },
    Param {
        name: "l_i1",
        dims: &[("t", DimK::Int)],
        meas: MeasK::I,
        ext: 6,
        dense: 30,
    },
    Param {
        name: "l_f2",
        dims: &[("t", DimK::Int), ("s", DimK::Int)],
        meas: MeasK::F,
        ext: 3,
        dense: 30,
    },
    Param {
        name: "l_s2",
        dims: &[("u", DimK::Label), ("t", DimK::Int)],
        meas: MeasK::S,
        ext: 3,
        dense: 30,
    },
    Param {
        name: "l_b3",
        dims: &[("t", DimK::Dt), ("s", DimK::Int), ("u", DimK::Label)],
        meas: MeasK::B,
        ext: 2,
        dense: 30,
    },
    Param {
        name: "l_t4",
        dims: &[
            ("t", DimK::Dt),
            ("s", DimK::Int),
            ("u", DimK::Label),
            ("v", DimK::Int),
        ],
        meas: MeasK::T,
        ext: 2,
        dense: 30,
    },
    Param {
        name: "a_f1",
        dims: &[("t", DimK::Dt)],
        meas: MeasK::F,
        ext: 7,
        dense: 100,
    },
    Param {
        name: "a_f2",
        dims: &[("t", DimK::Int), ("u", DimK::Label)],
        meas: MeasK::F,
        ext: 3,
        dense: 70,
    },
    Param {
        name: "res",
        dims: &[("t", DimK::Int)],
        meas: MeasK::I,
        ext: 4,
        dense: 50,
    },
    Param {
        name: "res_a",
        dims: &[("t", DimK::Int)],
        meas: MeasK::F,
        ext: 4,
        dense: 100,
    },
];

const LABELS: &[&str] = &["la", "lb", "lc", "ld"];
const ALTS: &[&str] = &["Base", "alt1", "run1", "Alt1", "run2"];
/// Segment classes: `node` (where `mixed`, `res` and `res_a` are declared),
/// a second spelling of it, and a second class.
const CLASSES: &[&str] = &["node", "node", "node", "Node", "unit"];

fn float_val(r: &mut Rng) -> f64 {
    match r.below(10) {
        0 => nan(),
        1 => -0.0,
        2 => f64::INFINITY,
        _ => (r.below(2000) as f64 - 1000.0) * 0.125,
    }
}

/// Build one segment's batch for `entities` (names) of parameter `p`.
fn param_batch(r: &mut Rng, p: &Param, names: &[String]) -> RecordBatch {
    let rank = p.dims.len();
    let mut ent: Vec<String> = Vec::new();
    let mut coords: Vec<Vec<usize>> = vec![Vec::new(); rank];
    for n in names {
        // The grid cells of this entity: dense or a random non-empty subset.
        let total = p.ext.pow(rank as u32);
        let cells: Vec<usize> = if rank == 0 || r.chance(p.dense) {
            (0..total).collect()
        } else {
            r.subset(total, 1)
        };
        // A random offset per entity, so axes differ between values.
        let off = r.below(3);
        for c in cells {
            ent.push(n.clone());
            let mut x = c;
            for d in (0..rank).rev() {
                let shift = d == 0 && p.dims[0].1 != DimK::Label;
                coords[d].push(x % p.ext + if shift { off } else { 0 });
                x /= p.ext;
            }
        }
    }
    // Rows in a shuffled-ish order (canon sorts).
    let nrows = ent.len();
    let mut order: Vec<usize> = (0..nrows).collect();
    if r.chance(30) {
        order.reverse();
    }
    let mut fields = vec![Field::new("entity", DataType::Utf8, false)];
    let mut cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
        order.iter().map(|&i| ent[i].clone()).collect::<Vec<_>>(),
    ))];
    for (d, (dn, k)) in p.dims.iter().enumerate() {
        let c: Vec<usize> = order.iter().map(|&i| coords[d][i]).collect();
        match k {
            DimK::Int => {
                fields.push(Field::new(*dn, DataType::Int64, false));
                cols.push(Arc::new(Int64Array::from(
                    c.iter().map(|&x| x as i64 * 3 - 2).collect::<Vec<_>>(),
                )));
            }
            DimK::Dt => {
                fields.push(Field::new(
                    *dn,
                    DataType::Timestamp(ATU::Microsecond, None),
                    false,
                ));
                cols.push(Arc::new(TimestampMicrosecondArray::from(
                    c.iter().map(|&x| T0 + x as i64 * H).collect::<Vec<_>>(),
                )));
            }
            DimK::Label => {
                fields.push(Field::new(*dn, DataType::Utf8, false));
                cols.push(Arc::new(StringArray::from(
                    c.iter()
                        .map(|&x| LABELS[x % LABELS.len()])
                        .collect::<Vec<_>>(),
                )));
            }
        }
    }
    let vals: ArrayRef = match p.meas {
        MeasK::F => Arc::new(Float64Array::from(
            (0..nrows).map(|_| float_val(r)).collect::<Vec<_>>(),
        )),
        MeasK::I => Arc::new(Int64Array::from(
            (0..nrows)
                .map(|_| r.below(100) as i64 - 50)
                .collect::<Vec<_>>(),
        )),
        MeasK::B => Arc::new(BooleanArray::from(
            (0..nrows).map(|_| r.chance(50)).collect::<Vec<_>>(),
        )),
        MeasK::S => Arc::new(StringArray::from(
            (0..nrows)
                .map(|_| ["x", "", "it's", "ÄÖ", "''"][r.below(5)])
                .collect::<Vec<_>>(),
        )),
        MeasK::T => Arc::new(TimestampMicrosecondArray::from(
            (0..nrows)
                .map(|_| T0 + r.below(1000) as i64 * 60_000_000)
                .collect::<Vec<_>>(),
        )),
    };
    fields.push(Field::new("value", vals.data_type().clone(), false));
    cols.push(vals);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn label_domains(p: &Param) -> Vec<LabelDomain> {
    p.dims
        .iter()
        .enumerate()
        .filter(|(_, (_, k))| *k == DimK::Label)
        .map(|(i, _)| LabelDomain {
            position: (i + 1) as u8,
            labels: LABELS.iter().map(|s| s.to_string()).collect(),
        })
        .collect()
}

/// A `mixed` segment: rank 0 float (scalar), rank 1 int measure (long) or
/// rank 1 float (array).
fn mixed_batch(r: &mut Rng, names: &[String]) -> RecordBatch {
    let shape = r.below(3);
    let mut ent = Vec::new();
    let mut t = Vec::new();
    for n in names {
        let k = if shape == 0 { 1 } else { 1 + r.below(4) };
        for i in 0..k {
            ent.push(n.clone());
            t.push(i as i64 * 2);
        }
    }
    let nrows = ent.len();
    let mut fields = vec![Field::new("entity", DataType::Utf8, false)];
    let mut cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(ent))];
    if shape > 0 {
        fields.push(Field::new("t", DataType::Int64, false));
        cols.push(Arc::new(Int64Array::from(t)));
    }
    let vals: ArrayRef = if shape == 1 {
        Arc::new(Int64Array::from(
            (0..nrows).map(|i| i as i64).collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(Float64Array::from(
            (0..nrows).map(|_| float_val(r)).collect::<Vec<_>>(),
        ))
    };
    fields.push(Field::new("value", vals.data_type().clone(), false));
    cols.push(vals);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn int_axis(len: i32, start: i64) -> CanonicalAxis {
    CanonicalAxis {
        length: len,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start, step: 1 },
    }
}

/// A random direct value: every tier, every kind including duration,
/// period and time-pattern axes, empty arrays.
fn direct_value(r: &mut Rng) -> CanonicalValue {
    let scalar = |v: ScalarValue| CanonicalValue {
        tier: Tier::Scalar,
        value_kind: v.kind(),
        axes: vec![],
        shape: ShapedData::Scalar(v),
    };
    match r.below(7) {
        0 => scalar(ScalarValue::Float(float_val(r).max(f64::MIN) + 0.0)),
        1 => scalar(ScalarValue::Duration {
            months: r.below(5) as i32,
            micros: r.below(100) as i64 - 7,
        }),
        2 => {
            // Long over a period axis.
            let axis = CanonicalAxis {
                length: 2,
                properties: AxisProperties::default(),
                body: AxisBody::Period {
                    positions: vec![
                        PeriodPos {
                            start_ts: T0,
                            end_ts: T0 + 100 * H,
                            years_presented: None,
                        },
                        PeriodPos {
                            start_ts: T0 + 100 * H,
                            end_ts: T0 + 300 * H,
                            years_presented: Some(1.0),
                        },
                    ],
                },
            };
            let cells: Vec<i32> = r.subset(2, 1).into_iter().map(|x| x as i32).collect();
            CanonicalValue {
                tier: Tier::Long,
                value_kind: ValueKind::Duration,
                axes: vec![axis],
                shape: ShapedData::Long(
                    cells
                        .into_iter()
                        .map(|p| LongRow {
                            positions: vec![p],
                            value: ScalarValue::Duration {
                                months: p,
                                micros: -(p as i64),
                            },
                        })
                        .collect(),
                ),
            }
        }
        3 => {
            // Long over a time-pattern axis x int axis, sparse.
            let tp = CanonicalAxis {
                length: 2,
                properties: AxisProperties::default(),
                body: AxisBody::TimePattern {
                    positions: vec![
                        TimePatternPos {
                            ordinal: 0,
                            terms: vec![TimePatternTerm {
                                unit: "M".into(),
                                range_start: 1,
                                range_end: 6,
                            }],
                        },
                        TimePatternPos {
                            ordinal: 1,
                            terms: vec![TimePatternTerm {
                                unit: "h".into(),
                                range_start: 1,
                                range_end: 8,
                            }],
                        },
                    ],
                },
            };
            let cells = r.subset(6, 1);
            CanonicalValue {
                tier: Tier::Long,
                value_kind: ValueKind::Float,
                axes: vec![tp, int_axis(3, r.below(2) as i64)],
                shape: ShapedData::Long(
                    cells
                        .into_iter()
                        .map(|c| LongRow {
                            positions: vec![(c / 3) as i32, (c % 3) as i32],
                            value: ScalarValue::Float(c as f64 * 0.5),
                        })
                        .collect(),
                ),
            }
        }
        4 => CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(1, 0)],
            shape: ShapedData::Array(vec![]),
        },
        5 => {
            let n = 1 + r.below(5);
            CanonicalValue {
                tier: Tier::Array,
                value_kind: ValueKind::Float,
                axes: vec![CanonicalAxis {
                    length: n as i32,
                    properties: AxisProperties::default(),
                    body: AxisBody::DatetimeParametric {
                        start_ts: T0,
                        step: Duration {
                            months: 1,
                            micros: 0,
                        },
                    },
                }],
                shape: ShapedData::Array(
                    (0..n)
                        .map(|_| {
                            let x = float_val(r);
                            if x == 0.0 {
                                0.0
                            } else {
                                x
                            }
                        })
                        .collect(),
                ),
            }
        }
        _ => {
            let cells = r.subset(4, 1);
            CanonicalValue {
                tier: Tier::Long,
                value_kind: ValueKind::String,
                axes: vec![int_axis(4, 5)],
                shape: ShapedData::Long(
                    cells
                        .into_iter()
                        .map(|c| LongRow {
                            positions: vec![c as i32],
                            value: ScalarValue::Str(format!("s{c}")),
                        })
                        .collect(),
                ),
            }
        }
    }
}

fn dk(class: &str, parameter: &str, alternative: &str, entity: &str) -> DeleteKey {
    DeleteKey {
        class: class.into(),
        parameter: parameter.into(),
        alternative: alternative.into(),
        entity: entity.into(),
    }
}

fn err_text(e: &DbError) -> String {
    match e {
        DbError::Duckdb(_) => "Duckdb error".into(),
        other => format!("{other}"),
    }
}

fn run(seed: u64, commits: usize, big: bool, dir: &Path, log: &mut String) -> std::path::PathBuf {
    let db = dir.join("write_equivalence.duckdb");
    let _ = std::fs::remove_file(&db);
    init_schema_with_default(&db, Some("Base")).unwrap();
    let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mixed = StructuralChange::CreateParameter {
        class: "node".into(),
        name: "mixed".into(),
        types: vec![
            TypeDecl {
                kind: "float".into(),
                dims: None,
            },
            TypeDecl {
                kind: "indexed".into(),
                dims: Some(vec![DimSpec {
                    name: "t".into(),
                    kind: "integer".into(),
                }]),
            },
        ],
        value_list: None,
        description: None,
    };
    let rep = commit_changes(
        &db,
        vec![Segment {
            class: "node".into(),
            parameter: "sc_f".into(),
            alternative: "Base".into(),
            batches: vec![param_batch(&mut r, &PARAMS[0], &["ασ".to_string()])],
            domains: vec![],
        }],
        vec![],
        vec![],
        vec![],
        "r",
        "setup",
        WriteOptions::default(),
    );
    writeln!(log, "## setup {:?}", rep.map_err(|e| err_text(&e))).unwrap();
    let rep = commit_changes(
        &db,
        vec![],
        vec![mixed],
        vec![],
        vec![],
        "r",
        "mixed",
        WriteOptions::default(),
    );
    writeln!(log, "## setup mixed {:?}", rep.map_err(|e| err_text(&e))).unwrap();
    {
        let conn = Connection::open(&db).unwrap();
        for name in ["res", "res_a"] {
            conn.execute(
                "INSERT INTO parameter_definition (entity_class_id, name, retain_content, \
                   valid_from_commit) SELECT (SELECT id FROM entity_class WHERE cur_name = 'node'), \
                   ?, FALSE, (SELECT max(id) FROM \"commit\")",
                [name],
            )
            .unwrap();
        }
        conn.close().unwrap();
    }

    for ci in 0..commits {
        // Erasure of run1 now and then.
        if r.chance(3) {
            let rep = erase_alternative(
                &db,
                &ObjectDeleteKey {
                    kind: ObjectKind::Alternative,
                    class: None,
                    target: ObjectRef::Name("run1".into()),
                },
                &EraseOpts::default(),
                "r",
            );
            writeln!(
                log,
                "## {ci} erase {:?}",
                rep.map(|r| r.erased).map_err(|e| err_text(&e))
            )
            .unwrap();
        }
        // Direct commits now and then.
        if r.chance(12) {
            let steps: Vec<(i64, Option<CanonicalValue>, bool)> = (0..1 + r.below(6))
                .map(|_| {
                    let e = 100_000 + r.below(6) as i64;
                    let v = if r.chance(20) {
                        None
                    } else {
                        Some(direct_value(&mut r))
                    };
                    (e, v, r.chance(20))
                })
                .collect();
            // Distinct keys per commit (a key twice dies on the span CHECK).
            let mut seen = std::collections::HashSet::new();
            let steps: Vec<_> = steps.into_iter().filter(|s| seen.insert(s.0)).collect();
            let rep = with_write_txn(&db, "r", "direct", |conn, c| {
                let mut cache = AxisCache::new();
                let mut out = Vec::new();
                for (e, v, force) in &steps {
                    let key = ValueKey {
                        entity_id: *e,
                        parameter_definition_id: 900,
                        alternative_id: 900,
                    };
                    let opts = WriteOptions {
                        force_replace: *force,
                        ..WriteOptions::default()
                    };
                    out.push(match v {
                        Some(v) => write_value(conn, c, key, v, &mut cache, &opts, None)?,
                        None => delete_value(conn, c, key, &opts)?,
                    });
                }
                Ok(out)
            });
            writeln!(log, "## {ci} direct {:?}", rep.map_err(|e| err_text(&e))).unwrap();
            continue;
        }

        let opts = match r.below(20) {
            0..=2 => WriteOptions {
                force_replace: true,
                ..WriteOptions::default()
            },
            3..=4 => WriteOptions {
                retain_override: Some(false),
                ..WriteOptions::default()
            },
            5 => WriteOptions {
                retain_override: Some(true),
                ..WriteOptions::default()
            },
            _ => WriteOptions::default(),
        };
        // A structural create now and then (it runs before the segments).
        let mut structural: Vec<StructuralChange> = Vec::new();
        if r.chance(10) {
            structural.push(StructuralChange::CreateAlternative {
                name: ["run2", "Run2", "alt2"][r.below(3)].into(),
                description: None,
            });
        }
        let mut segments: Vec<Segment> = Vec::new();
        let mut written: std::collections::HashSet<(String, String, String, String)> =
            Default::default();
        let mut targets = std::collections::HashSet::new();
        let nseg = 1 + r.below(8);
        for _ in 0..nseg {
            let class = CLASSES[r.below(CLASSES.len())];
            let alt = ALTS[r.below(ALTS.len())];
            let (cl, al) = (class.to_lowercase(), alt.to_lowercase());
            let pi = r.below(PARAMS.len() + 2);
            let (pname, batch, domains, names) = if pi >= PARAMS.len() {
                if !targets.insert((cl.clone(), "mixed", al.clone())) {
                    continue;
                }
                let names: Vec<String> = r.subset(ENTITIES, 1).into_iter().map(ename).collect();
                ("mixed", mixed_batch(&mut r, &names), vec![], names)
            } else {
                let p = &PARAMS[pi];
                if !targets.insert((cl.clone(), p.name, al.clone())) {
                    continue;
                }
                let names: Vec<String> = r.subset(ENTITIES, 1).into_iter().map(ename).collect();
                (
                    p.name,
                    param_batch(&mut r, p, &names),
                    label_domains(p),
                    names,
                )
            };
            for n in &names {
                written.insert((cl.clone(), pname.to_string(), al.clone(), n.to_lowercase()));
            }
            segments.push(Segment {
                class: class.into(),
                parameter: pname.into(),
                alternative: alt.into(),
                batches: vec![batch],
                domains,
            });
        }
        // Big rounds now and then.
        let big_free =
            |p: &'static str| !targets.contains(&("node".to_string(), p, "alt1".to_string()));
        if big && r.chance(4) && big_free("l_i1") {
            let names: Vec<String> = (0..700).map(|i| format!("b{i:03}")).collect();
            let p = Param {
                name: "l_i1",
                dims: &[("t", DimK::Int)],
                meas: MeasK::I,
                ext: 24,
                dense: 100,
            };
            for n in &names {
                written.insert(("node".into(), "l_i1".into(), "alt1".into(), n.clone()));
            }
            segments.push(Segment {
                class: "node".into(),
                parameter: "l_i1".into(),
                alternative: "alt1".into(),
                batches: vec![param_batch(&mut r, &p, &names)],
                domains: vec![],
            });
        }
        if big && r.chance(3) && big_free("a_f1") {
            let names: Vec<String> = (0..31).map(|i| format!("b{i:03}")).collect();
            let p = Param {
                name: "a_f1",
                dims: &[("t", DimK::Dt)],
                meas: MeasK::F,
                ext: 8760,
                dense: 100,
            };
            for n in &names {
                written.insert(("node".into(), "a_f1".into(), "alt1".into(), n.clone()));
            }
            segments.push(Segment {
                class: "node".into(),
                parameter: "a_f1".into(),
                alternative: "alt1".into(),
                batches: vec![param_batch(&mut r, &p, &names)],
                domains: vec![],
            });
        }
        // Deletes, with misses, duplicates and final-sigma pairs.
        let mut deletes: Vec<DeleteKey> = Vec::new();
        if r.chance(50) {
            for _ in 0..1 + r.below(8) {
                let alt = ALTS[r.below(ALTS.len())];
                let pname = if r.chance(15) {
                    "mixed"
                } else {
                    PARAMS[r.below(PARAMS.len())].name
                };
                let ent = ename(r.below(ENTITIES));
                let k = match r.below(14) {
                    0 => dk("nope_class", pname, alt, &ent),
                    13 => dk("unit", pname, alt, &ent),
                    1 => dk("node", "nope_param", alt, &ent),
                    2 => dk("node", pname, "nope_alt", &ent),
                    3 => dk("node", pname, alt, "nope_entity"),
                    4 => dk("node", "sc_f", "Base", "ΑΣ"),
                    5 => dk("node", "sc_f", "Base", "ασ"),
                    _ => dk("node", pname, alt, &ent),
                };
                if written.contains(&(
                    k.class.to_lowercase(),
                    k.parameter.clone(),
                    k.alternative.to_lowercase(),
                    k.entity.to_lowercase(),
                )) {
                    continue;
                }
                if r.chance(10) {
                    deletes.push(k.clone());
                }
                deletes.push(k);
            }
        }
        if big && r.chance(2) && big_free("l_i1") {
            // 700 resolvable keys (if written) and 15 685 unknown names: one
            // set entity lookup of 16 385 names.
            for i in 0..16_385 {
                let ent = if i < 700 {
                    format!("b{i:03}")
                } else {
                    format!("x{i}")
                };
                deletes.push(dk("node", "l_i1", "alt1", &ent));
            }
        }
        if segments.is_empty() && deletes.is_empty() && structural.is_empty() {
            continue;
        }
        let rep = commit_changes(
            &db,
            segments,
            structural,
            deletes,
            vec![],
            "r",
            &format!("c{ci}"),
            opts,
        );
        writeln!(log, "## {ci} {:?}", rep.map_err(|e| err_text(&e))).unwrap();
    }
    db
}

fn render(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(x) => x.to_string(),
        Value::SmallInt(x) => x.to_string(),
        Value::Int(x) => x.to_string(),
        Value::BigInt(x) => x.to_string(),
        Value::HugeInt(x) => x.to_string(),
        Value::UBigInt(x) => x.to_string(),
        Value::Double(x) => format!("f:{:016x}", x.to_bits()),
        Value::Text(s) => format!("{s:?}"),
        Value::Timestamp(unit, x) => {
            let us = match unit {
                TimeUnit::Second => x * 1_000_000,
                TimeUnit::Millisecond => x * 1_000,
                TimeUnit::Microsecond => *x,
                TimeUnit::Nanosecond => x / 1_000,
            };
            format!("ts:{us}")
        }
        Value::List(items) => {
            let parts: Vec<String> = items.iter().map(render).collect();
            format!("[{}]", parts.join(","))
        }
        other => format!("{other:?}"),
    }
}

/// Every row of every base table (sorted), the sequence counters and the
/// row count of every `audit_*` view, rendered in Rust (never through
/// DuckDB's `::VARCHAR`, whose float formatting is engine dependent).
fn dump(db: &Path, out: &mut String) {
    let conn = Connection::open(db).unwrap();
    let names = |sql: &str| -> Vec<String> {
        let mut stmt = conn.prepare(sql).unwrap();
        let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
        rows.collect::<Result<_, _>>().unwrap()
    };
    let tables = names(
        "SELECT table_name FROM duckdb_tables() \
         WHERE database_name = current_database() AND schema_name = 'main' \
         ORDER BY table_name",
    );
    let audits = names(
        "SELECT view_name FROM duckdb_views() \
         WHERE database_name = current_database() AND schema_name = 'main' \
           AND starts_with(view_name, 'audit_') ORDER BY view_name",
    );
    let mut q = |title: &str, sql: &str| {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = stmt.query([]).unwrap();
        let n = rows.as_ref().unwrap().column_count();
        writeln!(out, "## {title}").unwrap();
        while let Some(row) = rows.next().unwrap() {
            let cells: Vec<String> = (0..n)
                .map(|i| render(&row.get::<_, Value>(i).unwrap()))
                .collect();
            writeln!(out, "{}", cells.join("|")).unwrap();
        }
    };
    for t in &tables {
        let sql = match t.as_str() {
            "commit" => "SELECT * EXCLUDE (timestamp) FROM \"commit\" ORDER BY id".to_string(),
            "metadata" => "SELECT * FROM metadata WHERE lower(name) <> 'db_identity' \
                           ORDER BY ALL"
                .to_string(),
            _ => format!("SELECT * FROM \"{t}\" ORDER BY ALL"),
        };
        q(t, &sql);
    }
    q(
        "sequences",
        "SELECT sequence_name, last_value FROM duckdb_sequences() ORDER BY sequence_name",
    );
    for a in &audits {
        q(a, &format!("SELECT count(*) FROM \"{a}\""));
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let seed: u64 = a[1].parse().unwrap();
    let commits: usize = a[2].parse().unwrap();
    let big = a[3] == "1";
    let dir = Path::new(&a[4]);
    std::fs::create_dir_all(dir).unwrap();
    let mut log = String::new();
    let db = run(seed, commits, big, dir, &mut log);
    dump(&db, &mut log);
    print!("{log}");
}
