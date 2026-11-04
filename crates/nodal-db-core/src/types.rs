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
    /// Optimistic-concurrency precondition: commit only if the database's
    /// head commit is exactly this id (`conn::TxnOptions::expect_commit`),
    /// else [`crate::error::DbError::Stale`] with nothing written. Honored
    /// **only by the `api` entry points** (`commit_changes` and its
    /// wrappers `write_parameter`, `ingest_segments`, `delete_values`,
    /// `commit_structural`, `delete_objects`), which map it into the
    /// transaction. This struct is also passed per value to
    /// `history::write_value` / `delete_value`, where the field means
    /// nothing: a caller that drives `conn::with_write_txn` + `write_value`
    /// directly gets it silently ignored (use
    /// `conn::with_write_txn_retrying_opts` there). `None` (the default):
    /// no check.
    pub expect_commit: Option<i64>,
    /// What `api::commit_changes` does with a value write that a delete in
    /// the same call covers: a value delete of the same key
    /// (skip_unchanged_writes_PLAN D9), or an object delete whose cascade
    /// reaches the value (its entity or a container of it, its parameter
    /// definition, its alternative, or a class above them; D10). The default,
    /// [`CoveredWrites::Refuse`], refuses the whole call; a script that needs
    /// such a batch opts into [`CoveredWrites::DeleteWins`]. Meaningless
    /// outside the `api` entry points (like `expect_commit`).
    pub covered_writes: CoveredWrites,
}

/// The policy for a value write that a delete in the same `commit_changes`
/// call covers (see [`WriteOptions::covered_writes`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CoveredWrites {
    /// Refuse the call before anything is written, whatever the content,
    /// the tier or the object delete's `cascade` option (the default):
    /// [`crate::error::DbError::WriteAndDeleteSameKey`] for a value delete
    /// of a written key (D9), [`crate::error::DbError::CoveredWrite`] for an
    /// object delete covering a write (D10).
    #[default]
    Refuse,
    /// The delete wins: every covered write is dropped before anything runs
    /// (it is not resolved, canonicalized, auto-created or written) and
    /// reported as a [`WarningKind::DroppedByDelete`] warning; the deletes
    /// then run as usual and the uncovered writes proceed normally. A write
    /// is dropped only when a covering delete takes effect: a value delete,
    /// or an object delete that ends `Deleted` (or whose target did not
    /// exist). If every delete covering some write fails to take effect (a
    /// RESTRICT delete that ends `Restricted`), the whole call is refused as
    /// [`crate::error::DbError::Api`] and nothing is committed. Coverage is
    /// decided as the database resolves names (DuckDB's `lower()` fold) and,
    /// for an object delete, over its full cascade closure whatever its
    /// `cascade` option.
    DeleteWins,
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

/// The name fold: how nodal-db decides that two names are the same name
/// (engine rule 8). It is an exact copy of DuckDB's `lower()`, which the
/// schema's `cur_name` columns, their unique indexes and every SQL resolver
/// apply: Unicode 15.1.0 Simple_Lowercase_Mapping, one code point to one,
/// with no context rule (no final sigma), from the vendored table
/// [`crate::fold_table::LOWER`]. `tests/duckdb_lower_guard.rs` proves it
/// equal to the bundled engine's `lower()` on every Unicode scalar value,
/// and [`check_engine_fold`] re-checks the linked engine once per process.
/// Every name compare in Rust calls this and nothing else (a workspace
/// clippy ban keeps raw case mappings out). Apply it to NFC input: name
/// identity is `fold(nfc(s))` (engine rule 10).
pub fn fold(s: &str) -> String {
    if s.is_ascii() {
        // A-Z is the whole ASCII part of the table.
        #[allow(clippy::disallowed_methods)]
        return s.to_ascii_lowercase();
    }
    s.chars().map(fold_char).collect()
}

/// [`fold`] of one code point.
fn fold_char(c: char) -> char {
    if c.is_ascii() {
        // A-Z is the whole ASCII part of the table (inside the fold itself).
        #[allow(clippy::disallowed_methods)]
        return c.to_ascii_lowercase();
    }
    let table = &crate::fold_table::LOWER;
    match table.binary_search_by_key(&c, |&(from, _)| from) {
        Ok(i) => table[i].1,
        Err(_) => c,
    }
}

/// Outcome of the once-per-process engine check ([`check_engine_fold`]):
/// `None` once the engine agreed, the refusal message once it did not.
static ENGINE_FOLD: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

/// Refuse to write through an engine whose `lower()` is not [`fold`]
/// (name_fold_identity_PLAN C1 as reshaped by K5). Once per process, on the
/// first write connection, [`engine_fold_mismatch`] compares the engine's
/// `lower()` with the fold's table on every Unicode scalar value and on
/// strings where a context rule (final sigma, `İ`) would show. A mismatch
/// means the linked DuckDB changed its case table (a patched or newer
/// engine): its unique indexes and resolvers would then disagree with the
/// core's checks, so every write is refused with [`DbError::Internal`]
/// naming the first difference. This is the one check that runs in a
/// consumer's build (an editor never runs this repo's guard tests). A
/// failed query is not cached (the next write retries it) and says it
/// came from this check. Public only for the guard test, which runs it on
/// a connection whose `lower()` it has shadowed.
#[doc(hidden)]
pub fn check_engine_fold(conn: &duckdb::Connection) -> Result<(), crate::error::DbError> {
    if let Some(outcome) = ENGINE_FOLD.get() {
        return outcome
            .as_ref()
            .map_or(Ok(()), |m| Err(crate::error::DbError::Internal(m.clone())));
    }
    // A query failure is not cached (the next write retries), but it says
    // where it came from: a write failing here never reached its own SQL.
    let outcome = engine_fold_mismatch(conn, &crate::fold_table::LOWER).map_err(|e| {
        crate::error::DbError::Internal(format!(
            "the engine fold check (docs/dev/engine_rules.md §8: the linked DuckDB's \
             lower() against the core's name fold, once per process before the first \
             write) could not run its query: {e}"
        ))
    })?;
    let outcome = ENGINE_FOLD.get_or_init(|| outcome);
    outcome
        .as_ref()
        .map_or(Ok(()), |m| Err(crate::error::DbError::Internal(m.clone())))
}

/// Strings on which `lower()` of the whole string would differ from the
/// code-point-by-code-point mapping if the engine applied a context rule
/// (Unicode SpecialCasing: final sigma, which a sorted string of all code
/// points never triggers; `İ` with its dot; the Lithuanian and Turkish
/// dot rules; titlecase digraphs; the iota subscript), compared whole by
/// [`engine_fold_mismatch`].
#[doc(hidden)]
pub const FOLD_CONTEXT_PROBES: [&str; 16] = [
    "\u{3a3}",
    "\u{391}\u{3a3}",
    "\u{391}\u{3a3} ",
    "\u{391}\u{3a3}.",
    "\u{391}\u{3a3}\u{301}",
    "\u{391}\u{301}\u{3a3}",
    "\u{391}\u{3a3}\u{391}",
    "\u{391}\u{3a3}'\u{391}",
    "A\u{3a3}",
    "\u{3a3}\u{3a3}",
    "\u{130}",
    "\u{130}\u{307}",
    "I\u{307}",
    "I\u{300}",
    "\u{1c4}\u{1c5}\u{1c6}",
    "\u{399}\u{345}",
];

/// `s` mapped code point by code point through `table` (sorted
/// `(code point, lowercase)` pairs, like [`crate::fold_table::LOWER`]).
fn map_by_table(table: &[(char, char)], s: &str) -> String {
    s.chars()
        .map(
            |c| match table.binary_search_by_key(&c, |&(from, _)| from) {
                Ok(i) => table[i].1,
                Err(_) => c,
            },
        )
        .collect()
}

fn hex_cps(s: &str) -> String {
    s.chars()
        .map(|c| format!("U+{:04X}", c as u32))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The engine check's comparison, with the expected mapping injectable (the
/// guard test passes a wrong table to see the refusal; the core passes
/// [`crate::fold_table::LOWER`], which is what [`fold`] applies): `None` when
/// the engine's `lower()` is the code-point-by-code-point mapping `table`,
/// else the refusal message naming the first difference.
///
/// Two statements: `lower()` of one string holding every Unicode scalar
/// value in order (about 4.4 MB; every code point the engine changes must be
/// exactly a `table` entry, and every entry must be changed), then `lower()`
/// of each [`FOLD_CONTEXT_PROBES`] string, each compared whole with its
/// mapping. Together that is every code point plus the context rules
/// Unicode defines. It costs about 50 ms in a release build (about 120 ms
/// in a debug build; measured 2026-11, DuckDB 2.0.0, mostly the engine's
/// `lower()` of the long string), once per process;
/// `tests/duckdb_lower_guard.rs` prints it.
#[doc(hidden)]
pub fn engine_fold_mismatch(
    conn: &duckdb::Connection,
    table: &[(char, char)],
) -> Result<Option<String>, crate::error::DbError> {
    let refuse = |what: String| {
        Some(format!(
            "the linked DuckDB's lower() is not the core's name fold ({what}): this build's \
             engine changed its case table, so names would collide differently in the \
             database than in the core's checks; writes are refused (see \
             docs/dev/engine_rules.md §8 and the DuckDB upgrade procedure in CONTRIBUTING.md)"
        ))
    };
    // 1. Every scalar value, in one string.
    let all: String = (0..=0x10_FFFFu32).filter_map(char::from_u32).collect();
    let engine: String = conn.query_row("SELECT lower(?)", [&all], |r| r.get(0))?;
    if engine.chars().count() != all.chars().count() {
        // A one-to-many (or many-to-one) mapping: name the code point from a
        // per-code-point probe.
        return Ok(refuse(per_code_point_difference(conn, table)?));
    }
    let mut entries = table.iter().peekable();
    for (c, e) in all.chars().zip(engine.chars()) {
        let want = match entries.peek() {
            Some(&&(from, to)) if from == c => {
                entries.next();
                to
            }
            _ => c,
        };
        if e != want {
            return Ok(refuse(format!(
                "U+{:04X} lowers to U+{:04X} in the engine, U+{:04X} in the core",
                c as u32, e as u32, want as u32
            )));
        }
    }
    // 2. The context strings, whole.
    let sql = format!(
        "SELECT {}",
        vec!["lower(?)"; FOLD_CONTEXT_PROBES.len()].join(", ")
    );
    let lowered: Vec<String> = conn.query_row(
        &sql,
        duckdb::params_from_iter(FOLD_CONTEXT_PROBES.iter()),
        |r| (0..FOLD_CONTEXT_PROBES.len()).map(|i| r.get(i)).collect(),
    )?;
    for (probe, engine) in FOLD_CONTEXT_PROBES.iter().zip(lowered) {
        let want = map_by_table(table, probe);
        if engine != want {
            return Ok(refuse(format!(
                "the string {} lowers to {} in the engine, {} in the core",
                hex_cps(probe),
                hex_cps(&engine),
                hex_cps(&want)
            )));
        }
    }
    Ok(None)
}

/// The refusal detail when `lower()` of the all-code-points string did not
/// line up with it (a one-to-many or context mapping): the first code point
/// whose `lower()` on its own differs from `table`, from one row per code
/// point. Runs only on the way to a refusal.
fn per_code_point_difference(
    conn: &duckdb::Connection,
    table: &[(char, char)],
) -> Result<String, crate::error::DbError> {
    let mut stmt = conn.prepare(
        "SELECT cp::UINTEGER, lower(chr(cp::INTEGER)) \
         FROM (SELECT * FROM range(1, 55296) UNION ALL SELECT * FROM range(57344, 1114112)) t(cp) \
         ORDER BY cp",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let cp: u32 = row.get(0)?;
        let engine: String = row.get(1)?;
        let c = char::from_u32(cp).unwrap_or('\u{fffd}');
        let want = map_by_table(table, &c.to_string());
        if engine != want {
            return Ok(format!(
                "U+{cp:04X} lowers to {} in the engine, {} in the core",
                hex_cps(&engine),
                hex_cps(&want)
            ));
        }
    }
    Ok(
        "lower() of the string of all code points differs from lower() of each \
        code point on its own (a context rule)"
            .into(),
    )
}

/// The store's name **hygiene floor** for classes/entities/parameters (was the
/// `#2373` charset CHECK `[A-Za-z0-9_]+`, relaxed for Spine/FlexTool interop —
/// hyphens, spaces, dots, `%`, `__`, non-ASCII letters etc. are all allowed).
/// A name is valid iff it is non-empty, equals its trimmed form (no
/// leading/trailing whitespace), and contains no ASCII control characters
/// (bytes < 0x20 or 0x7f, including newlines, tabs, and NUL). The DB CHECK is
/// gone, so the writer is strictly *stricter* than the DB — the safe direction,
/// never "writer accepts, DB rejects" (api critique F3). This is the canonical
/// predicate: mirrored in Rust so a caller-contract problem fails as a clean
/// error, never a raw CHECK error mid-transaction. Shared by the G8 verbs and
/// the structural channel's pure prepare (invariant 2 — one name rule); the
/// template IR and reconcile predicates delegate here so they cannot drift.
pub(crate) fn charset_ok(s: &str) -> bool {
    !s.is_empty() && s == s.trim() && !s.chars().any(|c| c.is_ascii_control())
}

impl ScalarValue {
    /// A canonical identity string for scalar equality/dedup (kind-prefixed;
    /// float text is Rust's shortest round-trip form, so `1` written as float
    /// and `1.0` collapse). One encoding shared by the template IR's literal
    /// canonicalization, the reconcile diff, and list-member matching — a
    /// single definition so they cannot drift.
    pub(crate) fn identity_string(&self) -> String {
        match self {
            // Canonical first, so -0.0 and 0.0 (and every NaN) share one
            // identity, matching what is stored.
            ScalarValue::Float(f) => format!("f:{}", crate::encoding::canonical_value_f64(*f)),
            ScalarValue::Int(i) => format!("i:{i}"),
            ScalarValue::Bool(b) => format!("b:{b}"),
            ScalarValue::Str(s) => format!("s:{s}"),
            ScalarValue::DateTime(t) => format!("d:{t}"),
            ScalarValue::Duration { months, micros } => format!("u:{months}:{micros}"),
        }
    }

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
    /// A value write was dropped because a delete in the same call covers
    /// it ([`CoveredWrites::DeleteWins`]): nothing was written for it.
    /// `segment_index` is the segment's position in the caller's `segments`
    /// vec; `by` names one covering delete that took effect (a value delete
    /// if any, else the lowest-index object delete that took effect).
    /// `object_type` is `"value"`, `name` is
    /// `class:parameter/alternative/entity` in the segment's spelling and
    /// `id` is 0.
    DroppedByDelete {
        segment_index: usize,
        by: CoveringDelete,
    },
    /// Identifiers the writer normalized to NFC (Unicode 16.0.0) before
    /// storing or looking them up (the text rule, `text.rs`): `what` is
    /// `"label"` (index labels and label-domain entries) or `"name"`
    /// (entity, class, parameter, alternative, scenario names). One
    /// warning per segment and kind, never one per string: `count` is the
    /// number of distinct spellings that changed, `written` the first of
    /// them as the caller wrote it and `stored` its NFC form, which is
    /// what reads return. `object_type` is `"segment"` (`name` is
    /// `class:parameter/alternative`, normalized) or `"structural"` (the
    /// structural changes of the call; `name` is empty); `id` is 0.
    TextNormalized {
        what: String,
        count: usize,
        written: String,
        stored: String,
    },
    /// A name reference resolved, by the case-insensitive name identity
    /// (engine rule 8: DuckDB `lower()`, of which [`fold`] is the exact
    /// Rust copy), to an existing object whose stored spelling differs
    /// from the one written (after NFC): the write went to that object,
    /// under its stored name. Fires for every write-path reference to an
    /// existing object (value writes, value deletes, object deletes,
    /// structural references and idempotent creates), including objects
    /// created or renamed earlier in the same call; never for the
    /// reference that creates an object, a rename's new name, the verbs
    /// (they return ids, not reports) or reads. One warning per group and
    /// referent table, never one per string: `what` is the group
    /// (`"segment"`, `"delete"`, `"object_delete"` or `"structural"`),
    /// `spellings` every distinct (written spelling, resolved row) pair that
    /// resolved this way, in input order (one spelling resolving to two rows,
    /// e.g. entities of two classes, is two entries), `count` its length,
    /// and `written`/`stored` the first entry's pair.
    /// `object_type` is the referent table (`"entity_class"`,
    /// `"parameter_definition"`, `"alternative"`, `"entity"`,
    /// `"scenario"`, `"parameter_value_list"`), `name` the segment
    /// (`class:parameter/alternative`, as written) or empty, and `id` the
    /// first resolved row's id. Not a write by itself. A client that sends
    /// the spellings it loaded never sees one.
    NameFolded {
        what: String,
        count: usize,
        written: String,
        stored: String,
        spellings: Vec<FoldedSpelling>,
    },
}

/// One reference of a [`WarningKind::NameFolded`] warning: the spelling
/// written (NFC), the stored spelling it resolved to, and that row's id in
/// the warning's referent table (`object_type`). The id says which object
/// (two entities of different classes can share a spelling); a summary
/// over several warnings counts a name once per (`object_type`, `id`,
/// `written`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoldedSpelling {
    pub written: String,
    pub stored: String,
    pub id: i64,
}

/// The delete that covers a dropped write ([`WarningKind::DroppedByDelete`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CoveringDelete {
    /// A value delete of the same key; `key_index` is its position in the
    /// caller's `deletes` vec.
    Value { key_index: usize },
    /// An object delete whose cascade reaches the value; `key_index` is its
    /// position in the caller's `object_deletes` vec.
    Object { key_index: usize },
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
    /// `0` when the call changed nothing (every value unchanged, every delete
    /// a miss, every structural item a no-op, every object delete not found;
    /// skip_unchanged_writes_PLAN D1): no commit was made and the head did
    /// not move. Use `api::head_commit` for the commit the data is current
    /// at.
    pub commit_id: i64,
    /// Distinct per-entity values written (one canonicalized value per
    /// entity), plus values deleted. An unchanged value is not counted here
    /// but in [`Self::values_unchanged`].
    pub values_written: usize,
    /// Values whose write was skipped because the stored value already
    /// equals it (same tier, binding, axes and stored payload): no row was
    /// closed out or inserted for them.
    pub values_unchanged: usize,
    /// Physical rows inserted/replaced as current across all values.
    pub rows_written: usize,
    /// Physical rows closed out / moved to a `_hist` twin across all values.
    pub rows_closed: usize,
    /// New `axis` rows inserted while resolving axes for this call.
    pub axes_created: usize,
    /// Existing `axis` rows reused (content-verified) for this call.
    pub axes_reused: usize,
    /// Distinct tiers written (first-seen order); a tier whose values were
    /// all unchanged is not listed.
    pub tiers: Vec<Tier>,
    /// Machine-readable warnings (auto-create, decision B).
    pub warnings: Vec<Warning>,
    /// Per-request outcomes of any object deletes in this commit (empty for
    /// pure value writes/deletes). Additive field (object_delete plan §1) — a
    /// structured result, not a warning, so the CLI's exhaustive `WarningKind`
    /// match is untouched.
    pub object_deletes: Vec<ObjectDeleteOutcome>,
    /// Per-item outcomes of any structural (create/rename/set-ranks/activation/
    /// membership) changes in this commit (structural_write_api_plan §2.3).
    /// Empty for pure value work. Additive, like `object_deletes`.
    pub structural: Vec<StructuralOutcome>,
    /// Set when the database file's read surface was older than this
    /// build's and the refresh that runs before the write failed (a user
    /// table holding a reserved read-surface name): the write itself went
    /// through. `nodal-db maintain refresh-views` reports the cause loudly.
    /// Additive, like `object_deletes`.
    pub read_surface_warning: Option<String>,
}

impl WriteReport {
    /// Fold one per-value [`WriteOutcome`] into the aggregate. An unchanged
    /// outcome counts in `values_unchanged` and notes no tier.
    pub fn absorb(&mut self, o: &WriteOutcome) {
        if o.unchanged {
            self.values_unchanged += 1;
        } else {
            self.values_written += 1;
            if let Some(t) = o.tier {
                self.note_tier(t);
            }
        }
        self.rows_written += o.rows_written;
        self.rows_closed += o.rows_closed;
        self.axes_created += o.axes_created;
        self.axes_reused += o.axes_reused;
    }

    /// Record `t` in `tiers` if not yet seen. The write path calls this when
    /// a scalar is canonicalized, before its deferred set-based write is
    /// absorbed, so `tiers` keeps the order in which values were met, not
    /// the order in which they were written.
    pub(crate) fn note_tier(&mut self, t: Tier) {
        if !self.tiers.contains(&t) {
            self.tiers.push(t);
        }
    }

    /// Fold one delete's [`WriteOutcome`] (`history::delete_values`) into the
    /// aggregate.
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
    /// Version stamped on the new current row(s); `0` after a delete. For
    /// an unchanged write, the version of the current row. Long: the
    /// highest version among the value's current cells after the write
    /// (versions are per cell; a cell diff leaves equal cells at theirs).
    pub version: u16,
    /// The stored value already equals the written one (same tier, binding,
    /// axes and stored payload), so nothing was written: `rows_written` and
    /// `rows_closed` are 0 (skip_unchanged_writes_PLAN §2).
    pub unchanged: bool,
    /// Long tier: the current cells left as they were. Every cell of an
    /// unchanged long value; the equal cells of a cell diff; 0 for a long
    /// value rewritten whole and for the other tiers.
    pub cells_unchanged: usize,
    /// Rows newly inserted (or, on the array tier, replaced) as current.
    /// Long: the changed and added cells of a cell diff.
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
    Scenario,
}

impl ObjectKind {
    /// A lowercase tag for messages/reports.
    pub fn tag(self) -> &'static str {
        match self {
            ObjectKind::Entity => "entity",
            ObjectKind::Parameter => "parameter",
            ObjectKind::Alternative => "alternative",
            ObjectKind::Class => "class",
            ObjectKind::Scenario => "scenario",
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
    Deleted {
        closed_out: CascadeCounts,
    },
    Restricted {
        blockers: Vec<Blocker>,
    },
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

// ---------------------------------------------------------------------------
// Structural (non-value) writes — the fourth `commit_changes` channel
// (structural_write_api_plan §2.2). Names, not ids; the core resolves strictly
// (lookup-only, never auto-created — the opposite of the value path), except
// the `Create*` ops and the scenario auto-create inside SetScenarioAlternatives.
// ---------------------------------------------------------------------------

/// One `(class, name)` entity address — a group/member endpoint for membership,
/// resolved strictly. Mirrors the delete side's `(class, ObjectRef::Name)`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityAddr {
    pub class: String,
    pub name: String,
}

/// A sanctioned create/update of a structural (non-value) object, applied inside
/// `commit_changes`' atomic commit (structural_write_api_plan §2.2). Removal is
/// NOT here — it rides `object_deletes` ([`ObjectKind`]).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum StructuralChange {
    /// Idempotent create. Existing (folded) name ⇒ its id, no write.
    /// `description` applies on create only.
    CreateAlternative {
        name: String,
        description: Option<String>,
    },
    /// Versioned rename and/or description revision (close-out + same-id
    /// successor, version + 1). A `new_name` that differs from the stored
    /// name at all (case or Unicode form included, after NFC) is a rename,
    /// reported `Renamed`; the same bytes with a description change is a
    /// description-only revision, reported `Updated`. `description: None`
    /// keeps the stored description, `Some("")` clears it (a stored
    /// description is never the empty string), `Some(d)` sets it. The same
    /// bytes with no description change is a `NoOp`. The new name may not
    /// fold to a name an earlier rename of the same call freed (in-call
    /// swaps take two commits), and later items, segments and deletes of
    /// the call may not address the old name (it is refused unless a
    /// same-call create made the name exist again).
    RenameAlternative {
        current: String,
        new_name: String,
        description: Option<String>,
    },
    CreateScenario {
        name: String,
        description: Option<String>,
    },
    /// As [`StructuralChange::RenameAlternative`], for a scenario.
    RenameScenario {
        current: String,
        new_name: String,
        description: Option<String>,
    },
    /// Declarative: set the scenario's ordered alternative membership to EXACTLY
    /// this list. `alternatives[i]` takes rank i+1; the LAST element wins
    /// (higher-rank-wins). Creates the scenario when absent. An empty list clears
    /// the ranking. Alternatives resolve strictly; at most one per scenario per
    /// commit.
    SetScenarioAlternatives {
        scenario: String,
        alternatives: Vec<String>,
    },
    /// Per-edge activation of one entity in one alternative. `active: None`
    /// closes the current row (revert to class default). Idempotent.
    SetEntityActivation {
        class: String,
        entity: String,
        alternative: String,
        active: Option<bool>,
    },
    /// Idempotent 0-D/N-D entity create (existing ⇒ its id, verifies tuple).
    CreateEntity {
        class: String,
        name: Option<String>,
        elements: Vec<String>,
    },
    /// Idempotent entity-class create (0-D or N-D). `description` applies on
    /// create only (an existing class is a NoOp that checks the dims and
    /// ignores it); `Some("")` stores no description.
    CreateEntityClass {
        name: String,
        dims: Vec<String>,
        description: Option<String>,
    },
    /// Versioned rename and/or description revision of an entity class,
    /// with [`StructuralChange::RenameAlternative`]'s rules (effects,
    /// description, swaps, old-name references). The id is preserved, so
    /// dimensions, entities, parameters, values and N-D classes that use it
    /// follow the rename; class names are unique across the database.
    ///
    /// On a template-stamped database (metadata `template_name` present) a
    /// rename that changes the folded name of a class that existed before
    /// the call is refused with [`crate::error::DbError::TemplateStampedRename`]
    /// unless `allow_on_template` is true: the core cannot tell which
    /// classes the template declares, and a renamed declared class blocks
    /// the next reconcile of a changed template. Case-only and
    /// description-only revisions need no flag.
    RenameEntityClass {
        current: String,
        new_name: String,
        description: Option<String>,
        #[serde(default)]
        allow_on_template: bool,
    },
    /// Versioned rename and/or description revision of one parameter
    /// definition of `class` (addressed by its name at this point of the
    /// call), with [`StructuralChange::RenameEntityClass`]'s rules; names
    /// are unique per class. Values, defaults, type members and units follow
    /// the id; no value is rewritten.
    RenameParameter {
        class: String,
        current: String,
        new_name: String,
        description: Option<String>,
        #[serde(default)]
        allow_on_template: bool,
    },
    /// Idempotent, typed parameter-definition create.
    CreateParameter {
        class: String,
        name: String,
        types: Vec<crate::template::ir::TypeDecl>,
        value_list: Option<String>,
        description: Option<String>,
    },
    /// Per-edge membership of `member` in group `group`. `alternative: None`
    /// writes/updates the base row; Some(a) the alternative-scoped override.
    /// `included: None` closes the current row; Some(b) sets `included = b`.
    SetMembership {
        group: EntityAddr,
        member: EntityAddr,
        alternative: Option<String>,
        included: Option<bool>,
    },
}

/// Which structural object an outcome concerns (structural_write_api_plan §2.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StructuralKind {
    Alternative,
    Scenario,
    ScenarioRanking,
    Activation,
    Membership,
    Entity,
    EntityClass,
    Parameter,
}

/// What a structural change did (structural_write_api_plan §2.3): tell "created
/// a new object" from "it already existed", and "changed" from "already so".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructuralEffect {
    Created,
    Renamed,
    Updated,
    NoOp,
    Cleared,
}

/// One structural item's outcome, mapped back to the caller's `structural` vec
/// by `change_index` (structural_write_api_plan §2.3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralOutcome {
    /// Position in the caller's `structural` vec.
    pub change_index: usize,
    pub kind: StructuralKind,
    /// The affected/created object id (0 where n/a, e.g. a cleared membership).
    pub object_id: i64,
    pub effect: StructuralEffect,
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

#[cfg(test)]
mod tests {
    use super::ScalarValue;

    #[test]
    fn float_identity_string_is_canonical() {
        // -0.0 and 0.0 are one stored value, so one identity; every NaN too.
        let f = |x: f64| ScalarValue::Float(x).identity_string();
        assert_eq!(f(-0.0), f(0.0));
        let nans = [
            0x7ff8_0000_0000_0000u64,
            0xfff8_0000_0000_0000,
            0x7ff0_0000_0000_0001,
            0x7fff_ffff_ffff_ffff,
            0xfff0_0000_0000_0001,
        ];
        for b in nans {
            assert_eq!(f(f64::from_bits(b)), f(f64::NAN), "{b:#018x}");
        }
        assert_ne!(f(1.5), f(-1.5));
    }
}
