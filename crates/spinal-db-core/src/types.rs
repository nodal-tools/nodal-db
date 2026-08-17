//! Plain, FFI-friendly data types for the canonicalization pipeline (VMP
//! Parts 2-3). No trait objects, no borrowed lifetimes crossing FFI — every
//! type here is owned `Clone`/`Copy` data a future PyO3 skin wraps 1:1
//! (`rust_implementation_plan.md` Part 0/Part 3).

use serde::{Deserialize, Serialize};

/// Scalar measure kinds (VMP Part 2). `tag()` is the `value_kind` string stored
/// in `value_scalar.value_kind` etc. and hashed by the value encoding (spec
/// §9.3). Note `DateTime` -> `"date_time"` (underscore), distinct from the
/// `datetime` axis-kind tag (spec §5.1.2 footgun).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueKind {
    Float,
    Int,
    Bool,
    String,
    DateTime,
    Duration,
}

impl ValueKind {
    /// The canonical `value_kind` tag (spec §9.3).
    pub fn tag(self) -> &'static str {
        match self {
            ValueKind::Float => "float",
            ValueKind::Int => "int",
            ValueKind::Bool => "bool",
            ValueKind::String => "string",
            ValueKind::DateTime => "date_time",
            ValueKind::Duration => "duration",
        }
    }
}

/// Index (axis) kinds (VMP Part 1). `Datetime`/`Integer`/`Label` are the kinds
/// this pure pipeline derives from long-form input; `Period`/`TimePattern` axis
/// *encoding* exists (spec §7.6/§7.7) but their long-input derivation needs a
/// richer multi-field wire form — deferred to a later chunk (canon returns
/// `CanonError::UnsupportedKind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    Datetime,
    Integer,
    Label,
    Period,
    TimePattern,
}

/// The physical tier (VMP Part 3 tier rule).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier {
    Scalar,
    Array,
    Long,
}

/// The declared shape of a parameter: ordered `(position, kind)` dimensions
/// (VMP Part 2 "where dimensionality is declared"). Rank 0 (empty) is a scalar.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredType {
    pub positions: Vec<(u8, IndexKind)>,
}

impl DeclaredType {
    pub fn rank(&self) -> usize {
        self.positions.len()
    }
}

/// Write options the pure pipeline is aware of; the full retention/force
/// semantics land with the history protocol (VMP Part 5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteOptions {
    pub force_replace: bool,
    pub retain_override: Option<bool>,
}

/// A value's identity key. OPEN-7 (jkiviluo, 2026-08-17) folds this into the
/// value encoding (spec §9.6): value equality is key + shape + measure, not
/// measure-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueKey {
    pub entity_id: i64,
    pub parameter_definition_id: i64,
    pub alternative_id: i64,
}

/// A single scalar measure — the slot-1 primary measure of the value encoding
/// (spec §9.5). Duration is one logical component carried as two fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
    Float(f64),
    Int(i64),
    Bool(bool),
    Str(String),
    DateTime(i64),
    Duration { months: i32, micros: i64 },
}

impl ScalarValue {
    pub fn kind(&self) -> ValueKind {
        match self {
            ScalarValue::Float(_) => ValueKind::Float,
            ScalarValue::Int(_) => ValueKind::Int,
            ScalarValue::Bool(_) => ValueKind::Bool,
            ScalarValue::Str(_) => ValueKind::String,
            ScalarValue::DateTime(_) => ValueKind::DateTime,
            ScalarValue::Duration { .. } => ValueKind::Duration,
        }
    }
}

/// One long-tier cell: the 0-based axis positions in declared-dimension order,
/// and the cell's measure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LongRow {
    pub positions: Vec<i32>,
    pub value: ScalarValue,
}

/// The shaped payload of a canonicalized value, per tier (VMP Part 3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ShapedData {
    /// Rank 0.
    Scalar(ScalarValue),
    /// Tier A: dense float payload, row-major flattened in declared order.
    Array(Vec<f64>),
    /// Tier L rank>=1: sparse/ragged cells, canonical row-major order.
    Long(Vec<LongRow>),
}

/// Outcome summary of a write. The pure pipeline fills `tier` and the counts it
/// can see; axis dedup / row-write totals are completed by the history protocol
/// chunk (VMP Part 5).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteReport {
    pub tier: Option<Tier>,
    pub rows: usize,
    pub axes: usize,
}
