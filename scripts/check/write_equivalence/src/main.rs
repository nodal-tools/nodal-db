//! Randomized write-path equivalence workload (see `check.py` next to this
//! crate, and CONTRIBUTING "Write-path equivalence check"). Built against
//! two cores (an old and a new revision); each run drives the same seeded
//! random script of commits through the public API (segments of every
//! tier and value kind, several class and alternative spellings,
//! structural creates, value deletes with misses and duplicates, alternative
//! erasure, and direct `history::write_value` / `delete_value` commits),
//! then prints every commit's report or error, every row of every base
//! table, the sequence counters and the row count of every `audit_*` view
//! that has findings.
//! Two cores whose write paths are meant to be equivalent print the same
//! text.
//!
//! Not deterministic across runs, so excluded from the dump: the commit
//! log's wall-clock `timestamp` and the per-file `db_identity` metadata
//! row. Engine error texts are reduced to "Duckdb error". Reports print a
//! fixed field subset (not `{:?}`), so a core that adds a report field
//! still prints the same text.
//!
//! Modes (the optional 5th argument):
//! - `plain` (default): the random workload above; values repeat now and
//!   then by chance.
//! - `fresh`: every written cell differs from every value any earlier
//!   commit wrote to its key (floats, ints, timestamps and strings carry a
//!   commit-indexed term; a bool cell flips the last value this workload
//!   committed to it), and a `commit_changes` call without segments is not
//!   made. No write is ever identical to the stored value and no call is a
//!   no-op, so a core that skips unchanged writes must print exactly what
//!   a core that versions them prints (skip_unchanged_writes_PLAN §6.1).
//! - `logical`: a repeat-heavy workload (about 30 % of the segments and
//!   direct values replay an earlier one) that prints only what the
//!   skip-unchanged change must leave alone: reports without commit ids,
//!   value and row counts or tiers; the current values (keys, axes,
//!   positions, payload; no commit, version or array_id columns) after
//!   every call; the as-of values at every call boundary at the end; then
//!   `history_rows` and the audit counts, which `check.py --logical`
//!   compares by rule instead of by text (plan §6.3).
//!
//! The workload uses only API present since `73e70d6` (2026-10-24); to
//! compare older revisions, use a copy of this crate from their time.
//!
//! usage: write_equivalence <seed> <commits> <big:0|1> <dir> [plain|fresh|logical]

use std::collections::{BTreeMap, HashMap};
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
    Tier, ValueKey, ValueKind, WriteOptions, WriteOutcome, WriteReport,
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

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Plain,
    Fresh,
    Logical,
}

/// The value generator's state: the mode, the commit-indexed term of the
/// current call (`fresh` mode) and the bool cells this workload committed
/// (`fresh` mode flips them; a cell is keyed by its class, parameter,
/// alternative and entity, all folded, plus its coordinates).
struct Gen {
    mode: Mode,
    /// 0 for the setup call, `ci + 1` for loop iteration `ci`.
    term: i64,
    bools: HashMap<String, bool>,
    /// The bool cells of the call being built, committed to `bools` only
    /// when the call succeeds.
    pending: Vec<(String, bool)>,
}

impl Gen {
    fn fresh(&self) -> bool {
        self.mode == Mode::Fresh
    }

    /// The call succeeded: its bool cells are now the stored ones.
    fn commit(&mut self) {
        for (k, v) in self.pending.drain(..) {
            self.bools.insert(k, v);
        }
    }

    /// The call failed: nothing it wrote is stored.
    fn rollback(&mut self) {
        self.pending.clear();
    }
}

fn float_val(r: &mut Rng, g: &Gen) -> f64 {
    if g.fresh() {
        // Disjoint per call: [term*2000 - 1000, term*2000 + 1000) * 0.125.
        return ((g.term * 2000 + r.below(2000) as i64) as f64 - 1000.0) * 0.125;
    }
    match r.below(10) {
        0 => nan(),
        1 => -0.0,
        2 => f64::INFINITY,
        _ => (r.below(2000) as f64 - 1000.0) * 0.125,
    }
}

/// Build one segment's batch for `entities` (names) of parameter `p`;
/// `cell_key` is the folded `class|parameter|alternative` of the segment
/// (the `fresh` mode's bool cells).
fn param_batch(
    r: &mut Rng,
    g: &mut Gen,
    cell_key: &str,
    p: &Param,
    names: &[String],
) -> RecordBatch {
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
    let term = if g.fresh() { g.term } else { 0 };
    let vals: ArrayRef = match p.meas {
        MeasK::F => Arc::new(Float64Array::from(
            (0..nrows).map(|_| float_val(r, g)).collect::<Vec<_>>(),
        )),
        MeasK::I => Arc::new(Int64Array::from(
            (0..nrows)
                .map(|_| r.below(100) as i64 - 50 + term * 1000)
                .collect::<Vec<_>>(),
        )),
        MeasK::B => {
            let mut v = Vec::with_capacity(nrows);
            for &i in &order {
                let x = r.chance(50);
                if g.fresh() {
                    let coord: Vec<usize> = (0..rank).map(|d| coords[d][i]).collect();
                    let k = format!("{cell_key}|{}|{coord:?}", ent[i].to_lowercase());
                    let x = g.bools.get(&k).map_or(x, |&last| !last);
                    g.pending.push((k, x));
                    v.push(x);
                } else {
                    v.push(x);
                }
            }
            Arc::new(BooleanArray::from(v))
        }
        MeasK::S => Arc::new(StringArray::from(
            (0..nrows)
                .map(|_| {
                    let base = ["x", "", "it's", "ÄÖ", "''"][r.below(5)];
                    if g.fresh() {
                        format!("{base}#{term}")
                    } else {
                        base.to_string()
                    }
                })
                .collect::<Vec<_>>(),
        )),
        MeasK::T => Arc::new(TimestampMicrosecondArray::from(
            (0..nrows)
                .map(|_| T0 + (term * 1000 + r.below(1000) as i64) * 60_000_000)
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
fn mixed_batch(r: &mut Rng, g: &Gen, names: &[String]) -> RecordBatch {
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
    let term = if g.fresh() { g.term } else { 0 };
    let vals: ArrayRef = if shape == 1 {
        Arc::new(Int64Array::from(
            (0..nrows)
                .map(|i| i as i64 + term * 1000)
                .collect::<Vec<_>>(),
        ))
    } else {
        Arc::new(Float64Array::from(
            (0..nrows).map(|_| float_val(r, g)).collect::<Vec<_>>(),
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
/// In `fresh` mode every cell carries the call's term, and the empty array
/// sits on a per-call axis (an empty payload cannot change).
fn direct_value(r: &mut Rng, g: &Gen) -> CanonicalValue {
    let term = if g.fresh() { g.term } else { 0 };
    let scalar = |v: ScalarValue| CanonicalValue {
        tier: Tier::Scalar,
        value_kind: v.kind(),
        axes: vec![],
        shape: ShapedData::Scalar(v),
    };
    match r.below(7) {
        0 => scalar(ScalarValue::Float(float_val(r, g).max(f64::MIN) + 0.0)),
        1 => scalar(ScalarValue::Duration {
            months: r.below(5) as i32 + term as i32 * 10,
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
                                months: p + term as i32 * 2,
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
                            value: ScalarValue::Float(c as f64 * 0.5 + term as f64 * 8.0),
                        })
                        .collect(),
                ),
            }
        }
        4 => CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(1, term)],
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
                            let x = float_val(r, g);
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
                            value: ScalarValue::Str(if g.fresh() {
                                format!("s{c}#{term}")
                            } else {
                                format!("s{c}")
                            }),
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

/// A report as text: a fixed field subset (every core this workload builds
/// against has these fields), so a core that adds a field prints the same
/// line. `logical` drops what a core that skips unchanged writes changes by
/// design: the commit id, the value and row counts and the tiers.
fn report_text(r: &WriteReport, mode: Mode) -> String {
    if mode == Mode::Logical {
        return format!(
            "axes_created={} axes_reused={} warnings={:?} object_deletes={:?} structural={:?}",
            r.axes_created, r.axes_reused, r.warnings, r.object_deletes, r.structural
        );
    }
    format!(
        "commit_id={} values_written={} rows_written={} rows_closed={} axes_created={} \
         axes_reused={} tiers={:?} warnings={:?} object_deletes={:?} structural={:?}",
        r.commit_id,
        r.values_written,
        r.rows_written,
        r.rows_closed,
        r.axes_created,
        r.axes_reused,
        r.tiers,
        r.warnings,
        r.object_deletes,
        r.structural
    )
}

/// A direct write's outcome as text (fixed field subset, as for reports).
fn outcome_text(o: &WriteOutcome, mode: Mode) -> String {
    if mode == Mode::Logical {
        return format!(
            "tier={:?} axes_created={} axes_reused={}",
            o.tier, o.axes_created, o.axes_reused
        );
    }
    format!(
        "tier={:?} version={} rows_written={} rows_closed={} axes_created={} axes_reused={}",
        o.tier, o.version, o.rows_written, o.rows_closed, o.axes_created, o.axes_reused
    )
}

fn result_text<T>(r: &Result<T, DbError>, f: impl Fn(&T) -> String) -> String {
    match r {
        Ok(v) => format!("Ok({})", f(v)),
        Err(e) => format!("Err({:?})", err_text(e)),
    }
}

/// The commit a call leaves the database at: its own commit, or (a failed
/// or no-op call, `commit_id == 0`) the boundary before it.
fn boundary(prev: i64, commit_id: Option<i64>) -> i64 {
    match commit_id {
        Some(c) if c > 0 => c,
        _ => prev,
    }
}

type Target = (String, String, String);

struct Run {
    db: std::path::PathBuf,
    /// The commit each call left the database at, in call order.
    boundaries: Vec<i64>,
}

fn run(seed: u64, commits: usize, big: bool, mode: Mode, dir: &Path, log: &mut String) -> Run {
    let db = dir.join("write_equivalence.duckdb");
    let _ = std::fs::remove_file(&db);
    init_schema_with_default(&db, Some("Base")).unwrap();
    let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut g = Gen {
        mode,
        term: 0,
        bools: HashMap::new(),
        pending: Vec::new(),
    };
    let mut boundaries: Vec<i64> = Vec::new();
    let mut head = 0i64;
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
    let setup = param_batch(
        &mut r,
        &mut g,
        "node|sc_f|base",
        &PARAMS[0],
        &["ασ".to_string()],
    );
    let rep = commit_changes(
        &db,
        vec![Segment {
            class: "node".into(),
            parameter: "sc_f".into(),
            alternative: "Base".into(),
            batches: vec![setup],
            domains: vec![],
        }],
        vec![],
        vec![],
        vec![],
        "r",
        "setup",
        WriteOptions::default(),
    );
    g.commit();
    writeln!(
        log,
        "## setup {}",
        result_text(&rep, |r| report_text(r, mode))
    )
    .unwrap();
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
    writeln!(
        log,
        "## setup mixed {}",
        result_text(&rep, |r| report_text(r, mode))
    )
    .unwrap();
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
        head = conn
            .query_row("SELECT max(id) FROM \"commit\"", [], |r| r.get(0))
            .unwrap_or(head);
        conn.close().unwrap();
    }

    // The `logical` mode's replay sources: the last segment generated per
    // target (with its entity names) and the last direct value per key.
    let mut last_seg: BTreeMap<Target, (Segment, Vec<String>)> = BTreeMap::new();
    let mut last_direct: HashMap<i64, CanonicalValue> = HashMap::new();
    let replay = |r: &mut Rng| mode == Mode::Logical && r.chance(30);

    for ci in 0..commits {
        g.term = ci as i64 + 1;
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
                rep.as_ref().map(|r| r.erased).map_err(err_text)
            )
            .unwrap();
            head = boundary(head, rep.as_ref().ok().map(|r| r.commit_id));
            boundaries.push(head);
            logical_state(&db, mode, &format!("{ci} erase"), log);
        }
        // Direct commits now and then.
        if r.chance(12) {
            let steps: Vec<(i64, Option<CanonicalValue>, bool)> = (0..1 + r.below(6))
                .map(|_| {
                    let e = 100_000 + r.below(6) as i64;
                    let v = if r.chance(20) {
                        None
                    } else if replay(&mut r) && last_direct.contains_key(&e) {
                        Some(last_direct[&e].clone())
                    } else {
                        Some(direct_value(&mut r, &g))
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
                Ok((c, out))
            });
            writeln!(
                log,
                "## {ci} direct {}",
                result_text(&rep, |(_, out)| {
                    let parts: Vec<String> = out.iter().map(|o| outcome_text(o, mode)).collect();
                    format!("[{}]", parts.join(", "))
                })
            )
            .unwrap();
            if rep.is_ok() {
                for (e, v, _) in &steps {
                    match v {
                        Some(v) => last_direct.insert(*e, v.clone()),
                        None => last_direct.remove(e),
                    };
                }
            }
            head = boundary(head, rep.as_ref().ok().map(|(c, _)| *c));
            boundaries.push(head);
            logical_state(&db, mode, &format!("{ci} direct"), log);
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
        let mut targets: std::collections::HashSet<Target> = Default::default();
        let nseg = 1 + r.below(8);
        for _ in 0..nseg {
            if replay(&mut r) && !last_seg.is_empty() {
                // Replay an earlier segment verbatim (identical values for
                // every entity whose value has not changed since).
                let (t, (seg, names)) = last_seg.iter().nth(r.below(last_seg.len())).unwrap();
                if !targets.insert(t.clone()) {
                    continue;
                }
                for n in names {
                    written.insert((t.0.clone(), t.1.clone(), t.2.clone(), n.to_lowercase()));
                }
                segments.push(seg.clone());
                continue;
            }
            let class = CLASSES[r.below(CLASSES.len())];
            let alt = ALTS[r.below(ALTS.len())];
            let (cl, al) = (class.to_lowercase(), alt.to_lowercase());
            let pi = r.below(PARAMS.len() + 2);
            let (pname, batch, domains, names) = if pi >= PARAMS.len() {
                if !targets.insert((cl.clone(), "mixed".into(), al.clone())) {
                    continue;
                }
                let names: Vec<String> = r.subset(ENTITIES, 1).into_iter().map(ename).collect();
                ("mixed", mixed_batch(&mut r, &g, &names), vec![], names)
            } else {
                let p = &PARAMS[pi];
                if !targets.insert((cl.clone(), p.name.into(), al.clone())) {
                    continue;
                }
                let names: Vec<String> = r.subset(ENTITIES, 1).into_iter().map(ename).collect();
                let cell_key = format!("{cl}|{}|{al}", p.name);
                (
                    p.name,
                    param_batch(&mut r, &mut g, &cell_key, p, &names),
                    label_domains(p),
                    names,
                )
            };
            for n in &names {
                written.insert((cl.clone(), pname.to_string(), al.clone(), n.to_lowercase()));
            }
            let seg = Segment {
                class: class.into(),
                parameter: pname.into(),
                alternative: alt.into(),
                batches: vec![batch],
                domains,
            };
            if mode == Mode::Logical {
                last_seg.insert((cl, pname.into(), al), (seg.clone(), names));
            }
            segments.push(seg);
        }
        // Big rounds now and then.
        let big_free = |p: &str| !targets.contains(&("node".into(), p.into(), "alt1".into()));
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
                batches: vec![param_batch(&mut r, &mut g, "node|l_i1|alt1", &p, &names)],
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
                batches: vec![param_batch(&mut r, &mut g, "node|a_f1|alt1", &p, &names)],
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
            g.rollback();
            continue;
        }
        // `fresh`: a call without a (changed) value write may change nothing
        // (deletes that miss, a create that exists); a core that commits no
        // such call would differ by a commit row, so it is not made.
        if mode == Mode::Fresh && segments.is_empty() {
            g.rollback();
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
        if rep.is_ok() {
            g.commit();
        } else {
            g.rollback();
        }
        writeln!(
            log,
            "## {ci} {}",
            result_text(&rep, |r| report_text(r, mode))
        )
        .unwrap();
        head = boundary(head, rep.as_ref().ok().map(|r| r.commit_id));
        boundaries.push(head);
        logical_state(&db, mode, &ci.to_string(), log);
    }
    Run { db, boundaries }
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

/// The logical value columns per tier: `(tier tag, identity columns,
/// payload columns, source)`. Identity: the key triple, the binding, the
/// axes, the positions and the value kind; payload: the value columns. No
/// commit, version or `array_id` column.
const LOGICAL: &[(&str, &str, &str, &str)] = &[
    (
        "S",
        "entity_id, parameter_definition_id, alternative_id, parameter_type_id, value_kind",
        "v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros",
        "value_scalar",
    ),
    (
        "L",
        "entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
         axis1_id, axis2_id, axis3_id, axis4_id, i1_pos, i2_pos, i3_pos, i4_pos, value_kind",
        "v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros",
        "value_long",
    ),
    (
        "A",
        "entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
         axis1_id, axis2_id, axis3_id, axis4_id",
        "values, values_int",
        "value_array",
    ),
];

/// The rows of one logical query as `identity\tpayload` lines (payload
/// `STUB` for a row whose content was not retained). With `span`, the
/// query's second-to-last column is the row's `valid_from_commit` and a
/// stub prints as `STUB#<valid_from_commit>`: the commit is arm-local, but
/// it tells one stubbed span from the next, which `check.py --logical`
/// needs to compare a stub with the other arm's content across its span.
/// `None` when the query fails (an as-of read behind an erasure marker).
fn logical_rows(conn: &Connection, sql: &str, n_id: usize, span: bool) -> Option<Vec<String>> {
    let mut stmt = conn.prepare(sql).ok()?;
    let mut rows = stmt.query([]).ok()?;
    let n = rows.as_ref().unwrap().column_count();
    let mut out = Vec::new();
    loop {
        let row = match rows.next() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(_) => return None,
        };
        let cells: Vec<String> = (0..n)
            .map(|i| render(&row.get::<_, Value>(i).unwrap()))
            .collect();
        // The last column is content_retained (after valid_from_commit
        // with `span`).
        let end = if span { n - 2 } else { n - 1 };
        let payload = match (cells[n - 1].as_str(), span) {
            ("false", true) => format!("STUB#{}", cells[n - 2]),
            ("false", false) => "STUB".to_string(),
            _ => cells[n_id..end].join("|"),
        };
        out.push(format!("{}\t{payload}", cells[..n_id].join("|")));
    }
    Some(out)
}

/// `logical` mode: the current values after a call, under `## {tag} state`.
fn logical_state(db: &Path, mode: Mode, tag: &str, log: &mut String) {
    if mode != Mode::Logical {
        return;
    }
    let conn = Connection::open(db).unwrap();
    writeln!(log, "## {tag} state").unwrap();
    for (t, id, payload, table) in LOGICAL {
        let current = if *table == "value_array" {
            "TRUE"
        } else {
            "valid_to_commit IS NULL"
        };
        let retained = if *table == "value_array" {
            "TRUE"
        } else {
            "content_retained"
        };
        let sql = format!(
            "SELECT '{t}', {id}, {payload}, {retained} FROM {table} WHERE {current} ORDER BY ALL"
        );
        let n_id = 1 + id.split(',').count();
        for line in logical_rows(&conn, &sql, n_id, false).expect("current state") {
            writeln!(log, "{line}").unwrap();
        }
    }
    conn.close().unwrap();
}

/// `logical` mode, at the end: the values as of every call boundary
/// (`ERR` where the as-of guard refuses), the history row count and the
/// audit counts.
fn logical_end(db: &Path, boundaries: &[i64], log: &mut String) {
    let conn = Connection::open(db).unwrap();
    for (k, c) in boundaries.iter().enumerate() {
        writeln!(log, "## asof {k}").unwrap();
        for (t, id, payload, table) in LOGICAL {
            let sql = format!(
                "SELECT '{t}', {id}, {payload}, valid_from_commit, content_retained \
                 FROM {table}_asof({c}) ORDER BY ALL"
            );
            let n_id = 1 + id.split(',').count();
            match logical_rows(&conn, &sql, n_id, true) {
                Some(lines) => {
                    for line in lines {
                        writeln!(log, "{line}").unwrap();
                    }
                }
                None => writeln!(log, "{t}\tERR").unwrap(),
            }
        }
    }
    let hist: i64 = conn
        .query_row(
            "SELECT (SELECT count(*) FROM value_scalar WHERE valid_to_commit IS NOT NULL) \
                  + (SELECT count(*) FROM value_long WHERE valid_to_commit IS NOT NULL) \
                  + (SELECT count(*) FROM value_array_hist)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    writeln!(log, "## history_rows {hist}").unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT view_name FROM duckdb_views() \
             WHERE database_name = current_database() AND schema_name = 'main' \
               AND starts_with(view_name, 'audit_') ORDER BY view_name",
        )
        .unwrap();
    let audits: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    drop(stmt);
    for a in audits {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM \"{a}\""), [], |r| r.get(0))
            .unwrap();
        writeln!(log, "## audit {a} {n}").unwrap();
    }
    conn.close().unwrap();
}

/// Every row of every base table (sorted), the sequence counters and the
/// row count of every `audit_*` view with findings, rendered in Rust (never through
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
    // Only the audits with findings: a core that adds an audit view (which
    // is empty on correct data) still prints the same text.
    drop(q);
    for a in &audits {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM \"{a}\""), [], |r| r.get(0))
            .unwrap();
        if n != 0 {
            writeln!(out, "## {a}\n{n}").unwrap();
        }
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let seed: u64 = a[1].parse().unwrap();
    let commits: usize = a[2].parse().unwrap();
    let big = a[3] == "1";
    let dir = Path::new(&a[4]);
    let mode = match a.get(5).map(String::as_str) {
        None | Some("plain") => Mode::Plain,
        Some("fresh") => Mode::Fresh,
        Some("logical") => Mode::Logical,
        Some(other) => panic!("unknown mode {other:?} (plain, fresh or logical)"),
    };
    std::fs::create_dir_all(dir).unwrap();
    let mut log = String::new();
    let out = run(seed, commits, big, mode, dir, &mut log);
    if mode == Mode::Logical {
        logical_end(&out.db, &out.boundaries, &mut log);
    } else {
        dump(&out.db, &mut log);
    }
    print!("{log}");
}
