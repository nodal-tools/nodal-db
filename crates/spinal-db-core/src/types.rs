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

/// A machine-readable warning surfaced to the caller of the public write API
/// (`api::write_parameter`). The founding case (decision B,
/// `rust_impl_session_decisions.md`) is name-resolution auto-create: a referent
/// named in the write did not exist and was created, which the caller MUST be
/// able to detect programmatically — never a mere log line.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WarningKind {
    /// A named referent (entity_class / parameter_definition / alternative /
    /// entity / parameter_type) was auto-created because name resolution found
    /// no current row.
    AutoCreated,
    /// A tz-bearing `Timestamp(µs, tz)` batch column was accepted (api critique
    /// F6): a tz-aware timestamp is a well-defined instant and is stored as the
    /// UTC-µs instant unchanged, but the tz display metadata is dropped —
    /// surfaced here so tz-aware feeds are never silently reinterpreted.
    TimezoneNormalized { column: String, tz: String },
    /// A [`DeleteKey`](crate::api::DeleteKey) named a referent that has no
    /// current row (case-insensitive lookup missed at `level` —
    /// `"entity_class"` / `"parameter_definition"` / `"alternative"` /
    /// `"entity"`); the key was skipped, never a hard error. `key_index` is the
    /// key's position in the caller's `deletes` vec, so the caller maps a skip
    /// back to its source key (delete critique MINOR-3).
    UnknownReferent { key_index: usize, level: String },
    /// A [`DeleteKey`](crate::api::DeleteKey) fully resolved but held no live
    /// value; the delete was a no-op. `key_index` is the key's position in the
    /// caller's `deletes` vec (delete critique MINOR-3).
    NothingToDelete { key_index: usize },
}

/// One structured warning. For [`WarningKind::AutoCreated`], `object_type` is
/// the referent table (`"entity_class"`, `"parameter_definition"`,
/// `"alternative"`, `"entity"`, `"parameter_type"`), `name` the resolved name,
/// `id` the id assigned to the created row. For
/// [`WarningKind::TimezoneNormalized`], `object_type` is `"column"`, `name`
/// the column name, and `id` is 0 (no row was created).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Warning {
    pub kind: WarningKind,
    pub object_type: String,
    pub name: String,
    pub id: i64,
}

/// The aggregate result of one `api::write_parameter` call (plan Part 3
/// `types`: "tiers touched, rows written, axes reused/created, commit id"),
/// plus the machine-readable auto-create warnings (decision B). Owned and
/// FFI-friendly. One report covers every per-entity value written by the call.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteReport {
    /// The commit id stamped on every row this call wrote (the validity edge).
    pub commit_id: i64,
    /// Distinct per-entity values written (one canonicalized value per entity).
    pub values_written: usize,
    /// Physical rows inserted/replaced as current across all values.
    pub rows_written: usize,
    /// Physical rows closed out / moved to a `_hist` twin across all values.
    pub rows_closed: usize,
    /// New `axis` rows inserted while resolving axes for this call.
    pub axes_created: usize,
    /// Existing `axis` rows reused (content-verified) for this call.
    pub axes_reused: usize,
    /// Distinct tiers touched (first-seen order).
    pub tiers: Vec<Tier>,
    /// Machine-readable warnings (auto-create, decision B).
    pub warnings: Vec<Warning>,
}

impl WriteReport {
    /// Fold one per-value [`WriteOutcome`] into the aggregate.
    pub fn absorb(&mut self, o: &WriteOutcome) {
        self.values_written += 1;
        self.rows_written += o.rows_written;
        self.rows_closed += o.rows_closed;
        self.axes_created += o.axes_created;
        self.axes_reused += o.axes_reused;
        if let Some(t) = o.tier {
            if !self.tiers.contains(&t) {
                self.tiers.push(t);
            }
        }
    }

    /// Fold one `history::delete_value` [`WriteOutcome`] into the aggregate.
    /// A delete writes no new current row and touches no tier, so this only
    /// folds `rows_closed` and — because a no-op delete (no live value) closes
    /// nothing — counts a "value deleted" in `values_written` **only** when it
    /// actually closed ≥1 row (delete design §1; keeps the write-path [`absorb`]
    /// untouched). Never pushes to `tiers` (a delete's tier is `None`).
    pub fn absorb_delete(&mut self, o: &WriteOutcome) {
        self.rows_closed += o.rows_closed;
        if o.rows_closed > 0 {
            self.values_written += 1;
        }
    }
}

/// The full result of one `history::write_value` / `history::delete_value` call
/// (VMP Part 5). Owned, FFI-friendly; folds into a [`WriteReport`] for callers
/// that only want the summary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteOutcome {
    /// The tier now current for the key; `None` after a delete.
    pub tier: Option<Tier>,
    /// Version stamped on the new current row(s); `0` after a delete.
    pub version: u16,
    /// Rows newly inserted (or, on the array tier, replaced) as current.
    pub rows_written: usize,
    /// Rows closed out (from/to tiers) or moved to the `_hist` twin (array).
    pub rows_closed: usize,
    /// New `axis` rows inserted while resolving this value's axes.
    pub axes_created: usize,
    /// Existing `axis` rows reused (content-verified dedup) for this value.
    pub axes_reused: usize,
}

