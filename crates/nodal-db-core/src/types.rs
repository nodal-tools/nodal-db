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
    /// Per-request outcomes of any object deletes in this commit (empty for
    /// pure value writes/deletes). Additive field (object_delete plan §1) — a
    /// structured result, not a warning, so the CLI's exhaustive `WarningKind`
    /// match is untouched.
    pub object_deletes: Vec<ObjectDeleteOutcome>,
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

// ---------------------------------------------------------------------------
// Object deletion (object_delete plan §1). Names, not ids — the core resolves
// (lookup-only, case-insensitive, never auto-created), exactly like DeleteKey.
// ---------------------------------------------------------------------------

/// Which object kind a delete targets (object_delete plan §1). P1 implements
/// `Alternative` only; the other variants are defined now so the enum (and the
/// leg registry) is complete for P2/P3/P4.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectKind {
    Entity,
    Parameter,
    Alternative,
    Class,
}

impl ObjectKind {
    /// A lowercase tag for messages/reports.
    pub fn tag(self) -> &'static str {
        match self {
            ObjectKind::Entity => "entity",
            ObjectKind::Parameter => "parameter",
            ObjectKind::Alternative => "alternative",
            ObjectKind::Class => "class",
        }
    }
}

/// How to address the target object (object_delete plan §1, §6). Two channels
/// for an N-D entity: its own unique `Name`, or its identifying `Elements`
/// tuple. Keeping it an enum (not two `Option`s) makes the "exactly one
/// channel" contract unrepresentable-when-wrong. P1 exercises only `Name`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectRef {
    Name(String),
    /// Element entity NAMES in declared-dimension order (only valid for an
    /// `Entity` in an N-D class; resolved in P3).
    Elements(Vec<String>),
}

/// A typed object-delete target (object_delete plan §1). `class` is REQUIRED
/// for `Entity`/`Parameter` (both are per-class) and None for
/// `Alternative`/`Class` (addressed by their own globally-unique name).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectDeleteKey {
    pub kind: ObjectKind,
    pub class: Option<String>,
    pub target: ObjectRef,
}

/// Cascade opt-in per request (object_delete plan §1, decision D1). This is the
/// REPORT CONTRACT, never a guard:
///   cascade:false ⇒ if the object has any dependent, close out NOTHING for
///                   this key and return `Restricted{blockers}`; if it has
///                   none, delete the bare object and return `Deleted{empty}`.
///   cascade:true  ⇒ close out the object + all dependents and return
///                   `Deleted{counts}`.
/// The core never refuses a cascade:true request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectDeleteOpts {
    pub cascade: bool,
}

/// Per-dependent-table close-out count (object_delete plan §1).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCount {
    pub table: String,
    pub rows: usize,
}

/// What a cascade closed out (object_delete plan §1). `object_rows` counts the
/// target object row(s) (and, for entity/parameter/class, its identity rows);
/// `per_table` is the machine-readable dependent breakdown; `total_rows` is the
/// precomputed headline (object_rows + Σ per_table.rows).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CascadeCounts {
    pub object_rows: usize,
    pub per_table: Vec<TableCount>,
    pub total_rows: usize,
}

/// One RESTRICT blocker (object_delete plan §1): a dependent table still
/// referencing the target, with a bounded sample of rendered identifiers for
/// the editor's red-block breakdown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocker {
    pub table: String,
    pub column: String,
    pub count: usize,
    pub sample: Vec<String>,
}

/// The outcome for one object-delete request (object_delete plan §1). No
/// `residual_danglers` (change-model ruling 2: a `run` leg to a closed-out —
/// not absent — object is coherent as-of, fixed by the audit-view change, not
/// by a report field).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectDeleteResult {
    Deleted { closed_out: CascadeCounts },
    Restricted { blockers: Vec<Blocker> },
    /// Target did not resolve, or already closed out, or was created in this
    /// same commit (the `valid_from < commit_id` guard matched nothing) —
    /// folded into one variant so an idempotent whole-save retry is safe (D4).
    NotFound,
}

/// One request's outcome, mapped back to the caller's staged item by
/// `key_index` (object_delete plan §1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectDeleteOutcome {
    pub key_index: usize,
    pub kind: ObjectKind,
    pub result: ObjectDeleteResult,
}

/// Read-only preview of ONE object delete (object_delete plan §3, decision D6).
/// Carries BOTH framings of the same dependency set so the editor renders the
/// cascade preview or the restrict block from one call. The commit-time
/// `ObjectDeleteOutcome` is authoritative; this is a best-effort snapshot
/// (critique C5 — a concurrent writer can still move the counts).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectDeletePreview {
    pub key_index: usize,
    pub kind: ObjectKind,
    pub found: bool,
    pub cascade: CascadeCounts,
    pub blockers: Vec<Blocker>,
}

