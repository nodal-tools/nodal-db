//! The public Arrow-in / plain-data-out write path (plan Part 3 `api`; VMP
//! Parts 3-6). `write_parameter` is the one function that ties the whole write
//! path together end-to-end: name resolution (auto-create, decision B), the
//! declared-type lookup from the DB (critique S2), `canon` → `tier` → axis
//! dedup → the `history` protocol, all inside ONE atomic, conflict-retried
//! commit. `read_parameter` reads a written parameter back through the SQL
//! views for round-trip verification.
//!
//! ## Batch column contract (logical-long)
//! `batch` has, in order:
//!   - column 0: `entity` — the entity **names** as `Utf8` (decision B is
//!     resolve-by-name; ids would defeat auto-create). One value is written per
//!     distinct entity name.
//!   - columns `1..rank+1`: the `rank` dimension columns, in declared position
//!     order (position = column index). Arrow type fixes the index kind:
//!     `Timestamp(µs)` → datetime, `Int64` → integer, `Utf8` → label. The field
//!     **name** becomes the dimension name when a definition is auto-created.
//!   - last column: `value` — the measure. Arrow type fixes the measure kind:
//!     `Float64`/`Int64`/`Boolean`/`Utf8`/`Timestamp(µs)`.
//! Rank 0 (scalar) is a two-column batch `(entity, value)`.
//!
//! The **library decides the tier** (VMP Part 6): callers never pick scalar /
//! array / long. Ordinal domains for label dimensions arrive by the in-process
//! `domains` arg and/or Arrow field metadata `spinal:label_domain` (a JSON array
//! of the ordered labels) — decision D, both channels.
//!
//! ## Concurrency contract
//! Safe to call from multiple threads and processes on one DB file: writes are
//! serialized, in-process by `conn`'s per-path lock (api critique F1) and
//! cross-process by DuckDB's file lock. Connections stay per-operation
//! (CLAUDE.md invariant 1) — never shared, never long-lived.

use std::collections::HashMap;
use std::path::Path;

use duckdb::arrow::array::{Array, StringArray, UInt32Array};
use duckdb::arrow::compute::{concat_batches, take_record_batch};
use duckdb::arrow::datatypes::{DataType, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, Connection};

use crate::axis::AxisCache;
use crate::canon::{canonicalize, LabelDomain};
use crate::conn::{with_read, with_write_txn_retrying};
use crate::error::DbError;
use crate::history::{delete_value, write_value};
use crate::object_delete::{apply_object_delete, prepare_object_deletes, preview_one};
use crate::types::{
    DeclaredType, IndexKind, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeletePreview, ValueKey,
    ValueKind, Warning, WarningKind, WriteOptions, WriteReport,
};

/// One ingest segment: a `(class, parameter, alternative)` target plus its
/// logical-long batches and label domains (`spec/arrow_ipc_bulk_format.md`
/// §2.2/§4). The batches are treated as ONE logical table (concatenated);
/// dense wire rows are expanded to logical-long *before* they reach here
/// (spec §6.3 — the importer expands, the core canonicalizes).
#[derive(Clone, Debug)]
pub struct Segment {
    pub class: String,
    pub parameter: String,
    pub alternative: String,
    pub batches: Vec<RecordBatch>,
    pub domains: Vec<LabelDomain>,
}

/// Write one parameter (all its per-entity values for one alternative) from a
/// logical-long Arrow batch. See the module docs for the batch column contract.
///
/// Missing referents (entity class, the parameter definition within that class,
/// the alternative, and every entity named in the batch) are **auto-created**
/// with a machine-readable [`Warning`] appended to the returned report
/// (decision B — never a hard error, never a mere log line). Name resolution,
/// auto-creation, and every value write happen in one transaction, retried as a
/// whole on a write-write conflict (VMP Part 5).
///
/// `user` / `message` stamp the `commit` row this call creates.
///
/// A thin single-segment wrapper over [`ingest_segments`] (spec OPEN-Q3-6):
/// the per-parameter body is shared, the transaction shape is identical.
#[allow(clippy::too_many_arguments)]
pub fn write_parameter(
    db: &Path,
    class: &str,
    parameter: &str,
    alternative: &str,
    batch: RecordBatch,
    domains: &[LabelDomain],
    opts: WriteOptions,
    user: &str,
    message: &str,
) -> Result<WriteReport, DbError> {
    ingest_segments(
        db,
        vec![Segment {
            class: class.to_string(),
            parameter: parameter.to_string(),
            alternative: alternative.to_string(),
            batches: vec![batch],
            domains: domains.to_vec(),
        }],
        user,
        message,
        opts,
    )
}

/// Ingest one or more segments in ONE atomic, conflict-retried transaction
/// with ONE `commit` row covering every value written
/// (`spec/arrow_ipc_bulk_format.md` §9.2, OPEN-Q3-6: whole-invocation
/// atomicity — all segments commit or nothing persists). This is the core
/// entry point behind `nodal-db import`; [`write_parameter`] is its
/// single-segment wrapper.
///
/// Preparation (batch validation, entity dedup, domain merge) is pure and runs
/// once, up front; on a write-write conflict `conn::with_write_txn_retrying`
/// re-executes the whole multi-segment closure against a fresh transaction and
/// a fresh `commit` row, so no DB state read before the conflict is reused.
/// `opts` applies to every segment (`--force-replace` is container-wide).
pub fn ingest_segments(
    db: &Path,
    segments: Vec<Segment>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    // A pure-write ingest is a `commit_changes` with no deletes (delete design
    // MAJOR-2): the write path and the retried-commit shape are shared, so
    // writes-only and writes+deletes cannot drift.
    commit_changes(db, segments, Vec::new(), Vec::new(), user, message, opts)
}

/// A typed delete target: one `(class, parameter, alternative, entity)` value,
/// all four given by NAME (resolved case-insensitively, never auto-created).
/// Carries no value, so there is nothing to canonicalize — deletes ride plain
/// owned strings, not the Arrow/`Segment` machinery (delete design §4c).
#[derive(Clone, Debug)]
pub struct DeleteKey {
    pub class: String,
    pub parameter: String,
    pub alternative: String,
    pub entity: String,
}

/// Commit writes AND deletes in ONE atomic, conflict-retried transaction under a
/// single `commit` row (delete design MAJOR-2). This is the primary write-path
/// entry: [`ingest_segments`] is the writes-only wrapper and [`delete_values`]
/// the deletes-only wrapper. The combined entry exists because
/// `history::write_value` is non-idempotent — splitting an editor save into two
/// core calls means a failed second call re-inflates versions on the whole-save
/// retry. Running both halves in one closure makes the save atomic.
///
/// Segments are prepared (validated, deduped) up front, purely; deletes are
/// validated + deduped up front too. Then, inside one retried closure and under
/// one `commit_id`, the prepared writes run FIRST, then each `DeleteKey` is
/// resolved (case-insensitive, lookup-only — never auto-created) and closed out
/// via `history::delete_value`. A missing referent or already-absent value is a
/// machine-readable [`Warning`] + skip, never a hard error (delete design §2).
/// Non-empty `user`; empty segments AND empty deletes ⇒ [`DbError::Api`].
///
/// `object_deletes` (object_delete plan §2) join the SAME atomic commit, applied
/// AFTER value writes and value deletes: a coord closed by a staged value delete
/// is no longer live when an object cascade runs (not double-closed, not
/// double-counted — D5), and a later phase's class delete can compose over
/// already-settled value state. Each is resolved lookup-only and closed under
/// the `valid_from < commit_id` guard; per-request outcomes land in
/// `report.object_deletes`. Empty segments AND deletes AND object_deletes ⇒
/// [`DbError::Api`].
#[allow(clippy::too_many_arguments)]
pub fn commit_changes(
    db: &Path,
    segments: Vec<Segment>,
    deletes: Vec<DeleteKey>,
    object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    // Every commit must carry a real author; message is the caller's choice
    // (may be empty) — jkiviluo 2026-08-18.
    if user.trim().is_empty() {
        return Err(DbError::Api("user must be non-empty".into()));
    }
    if segments.is_empty() && deletes.is_empty() && object_deletes.is_empty() {
        return Err(DbError::Api(
            "nothing to do: no segments, no deletes, no object deletes".into(),
        ));
    }
    // Two same-commit writes to one (class, parameter, alternative) key have no
    // defined history semantics (spec §4.2, E7 class); compare by the same
    // `lower()` fold every core name resolver applies. DuckDB `lower()` is
    // Unicode, and alternative names carry no ASCII charset CHECK, so the fold
    // must be Unicode too — an ASCII fold lets Ä/ä-aliased targets resolve to
    // one row and die as a raw CHECK error mid-transaction (critique F3).
    let mut seen: HashMap<(String, String, String), ()> = HashMap::new();
    for s in &segments {
        let key = (
            s.class.to_lowercase(),
            s.parameter.to_lowercase(),
            s.alternative.to_lowercase(),
        );
        if seen.insert(key, ()).is_some() {
            return Err(DbError::Api(format!(
                "duplicate target {}:{} (alternative {:?}) across segments \
                 (target comparison is case-insensitive)",
                s.class, s.parameter, s.alternative
            )));
        }
    }

    // --- Pure, DB-independent preparation (computed once; the retried closure
    // below re-does only the DB-touching work). ---
    let multi = segments.len() > 1;
    let prepared: Vec<PreparedSegment> = segments
        .iter()
        .enumerate()
        .map(|(i, s)| {
            prepare_segment(s).map_err(|e| {
                if multi {
                    // Name the failing segment (spec §9.1: errors name the
                    // segment by ordinal and class:parameter).
                    DbError::Api(format!("segment {} ({}:{}): {e}", i + 1, s.class, s.parameter))
                } else {
                    e
                }
            })
        })
        .collect::<Result<_, _>>()?;
    // Deletes: validate non-empty components + reject fold-colliding distinct
    // spellings, dedupe identical keys — the original index rides along so
    // warnings map back to the caller's `deletes` vec (delete design MINOR-3/4).
    let prepared_deletes = prepare_deletes(&deletes)?;
    // Object deletes: validate (class-presence rules, blank names) + dedupe
    // fold-colliding requests up front, carrying the caller index (object_delete
    // plan §2). Pure — no DB touched.
    let prepared_object_deletes = prepare_object_deletes(&object_deletes)?;

    // --- The atomic, retried commit. Everything below re-reads/re-creates DB
    // state inside a fresh transaction on each retry (VMP Part 5), so the axis
    // cache is built HERE, not outside (a rollback must not leave stale ids). ---
    with_write_txn_retrying(db, user, message, |conn, commit_id| {
        // One axis cache across all segments: axes are immutable and
        // content-verified, so cross-segment reuse within one txn is sound.
        let mut cache = AxisCache::new();
        let mut report = WriteReport {
            commit_id,
            ..Default::default()
        };
        for (i, p) in prepared.iter().enumerate() {
            write_prepared(conn, commit_id, p, opts, &mut cache, &mut report).map_err(|e| {
                // Name the failing segment (spec §9.1) — but never rewrap a raw
                // DuckDB error: the conflict-retry loop keys on its variant
                // (`conn::is_conflict`), and an Api wrapper would break retry.
                if multi && !matches!(e, DbError::Duckdb(_)) {
                    DbError::Api(format!("segment {} ({}:{}): {e}", i + 1, p.class, p.parameter))
                } else {
                    e
                }
            })?;
        }
        // Deletes resolve + close out AFTER the writes, under the same commit —
        // one atomic invocation (delete design MAJOR-2).
        for (key_index, dk) in &prepared_deletes {
            apply_delete(conn, commit_id, *key_index, dk, opts, &mut report)?;
        }
        // Object deletes run LAST, under the same commit_id (object_delete plan
        // §2): every close-out is guarded by `valid_from < commit_id`, so a row
        // this commit created is never closed (would trip the span CHECK).
        for p in &prepared_object_deletes {
            apply_object_delete(conn, commit_id, p, &mut report)?;
        }
        Ok(report)
    })
}

/// Delete (append-only close-out) each named value in ONE atomic, conflict-
/// retried commit — the deletes-only wrapper over [`commit_changes`]. Missing
/// referents and already-absent values are machine-readable warnings and
/// skipped, never a hard error (delete design §2).
pub fn delete_values(
    db: &Path,
    keys: Vec<DeleteKey>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    commit_changes(db, Vec::new(), keys, Vec::new(), user, message, opts)
}

/// Delete (append-only close-out) one or more OBJECTS in ONE atomic, conflict-
/// retried commit — the objects-only wrapper over [`commit_changes`]
/// (object_delete plan §2). Each request is a typed [`ObjectDeleteKey`] plus its
/// [`ObjectDeleteOpts`] (the `cascade` report contract). Missing / already-closed
/// / same-commit-created targets yield [`crate::types::ObjectDeleteResult::NotFound`]
/// in `report.object_deletes`, never a hard error.
pub fn delete_objects(
    db: &Path,
    object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    commit_changes(db, Vec::new(), Vec::new(), object_deletes, user, message, opts)
}

/// Read-only preview of each object delete (object_delete plan §3, decision D6):
/// what a `cascade:true` would close out (`cascade`) and what a `cascade:false`
/// would block on (`blockers`) — the SAME dependency rows in two framings,
/// driven by the SAME leg registry the commit path uses, so preview and commit
/// cannot drift by *implementation*. It is nonetheless a best-effort snapshot:
/// the commit-time [`crate::types::ObjectDeleteOutcome`] is authoritative, and a
/// concurrent writer between preview and commit can still move the counts
/// (critique C5). Runs under one fresh RO connection, opened and closed per call
/// (CLAUDE.md invariant 1) — cheap enough to call every frame the dialog is open.
pub fn preview_object_deletes(
    db: &Path,
    keys: Vec<ObjectDeleteKey>,
) -> Result<Vec<ObjectDeletePreview>, DbError> {
    with_read(db, |conn| {
        keys.iter()
            .enumerate()
            .map(|(i, k)| preview_one(conn, i, k))
            .collect()
    })
}

/// Validate delete keys purely (no DB): every component non-empty, and — over
/// the full 4-tuple folded to `lower()` — reject fold-colliding distinct
/// spellings (a caller-contract ambiguity, mirroring the write path's entity
/// dedup, critique MINOR-4) while silently deduping exact-identical keys. The
/// surviving keys carry their original `deletes`-vec index so a warning maps
/// back to the caller's key (MINOR-3).
fn prepare_deletes(keys: &[DeleteKey]) -> Result<Vec<(usize, DeleteKey)>, DbError> {
    let mut seen: HashMap<(String, String, String, String), usize> = HashMap::new();
    let mut out: Vec<(usize, DeleteKey)> = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        for (what, name) in [
            ("class", k.class.as_str()),
            ("parameter", k.parameter.as_str()),
            ("alternative", k.alternative.as_str()),
            ("entity", k.entity.as_str()),
        ] {
            if name.trim().is_empty() {
                return Err(DbError::Api(format!(
                    "delete key {i}: {what} name must be non-empty"
                )));
            }
        }
        let fold = (
            k.class.to_lowercase(),
            k.parameter.to_lowercase(),
            k.alternative.to_lowercase(),
            k.entity.to_lowercase(),
        );
        match seen.get(&fold) {
            Some(&j) => {
                // Same folded target seen before: identical spelling is a
                // harmless duplicate (dedupe); a distinct spelling is an
                // ambiguous case-alias — reject (critique MINOR-4).
                let prev = &out.iter().find(|(pi, _)| *pi == j).unwrap().1;
                let same_spelling = prev.class == k.class
                    && prev.parameter == k.parameter
                    && prev.alternative == k.alternative
                    && prev.entity == k.entity;
                if !same_spelling {
                    return Err(DbError::Api(format!(
                        "delete keys {j} and {i} resolve to the same value \
                         (name resolution is case-insensitive); use one spelling"
                    )));
                }
            }
            None => {
                seen.insert(fold, i);
                out.push((i, k.clone()));
            }
        }
    }
    Ok(out)
}

/// Resolve one `DeleteKey` (lookup-only, case-insensitive — never auto-create)
/// and close out its live value under `commit_id`. A missing referent short-
/// circuits to an [`WarningKind::UnknownReferent`] + skip; a fully-resolved key
/// with no live value yields [`WarningKind::NothingToDelete`]. Retention obeys
/// the definition's `retain_content` unless the caller overrode it — identical
/// to the write path (`api.rs` `write_prepared`).
fn apply_delete(
    conn: &Connection,
    commit_id: i64,
    key_index: usize,
    dk: &DeleteKey,
    opts: WriteOptions,
    report: &mut WriteReport,
) -> Result<(), DbError> {
    let class_id = match lookup_entity_class(conn, &dk.class)? {
        Some(id) => id,
        None => return Ok(unknown(report, key_index, "entity_class", &dk.class)),
    };
    let pd_id = match lookup_parameter(conn, class_id, &dk.parameter)? {
        Some(id) => id,
        None => return Ok(unknown(report, key_index, "parameter_definition", &dk.parameter)),
    };
    let alt_id = match lookup_alternative(conn, &dk.alternative)? {
        Some(id) => id,
        None => return Ok(unknown(report, key_index, "alternative", &dk.alternative)),
    };
    let entity_id = match lookup_entity(conn, class_id, &dk.entity)? {
        Some(id) => id,
        None => return Ok(unknown(report, key_index, "entity", &dk.entity)),
    };

    // Fold the definition's retain_content into retain_override unless the
    // caller already overrode it — else a delete on a retain_content=FALSE
    // definition would silently keep the closed-out payload (VMP Part 5).
    let effective_retain = opts.retain_override.unwrap_or(read_retain_content(conn, pd_id)?);
    let resolved_opts = WriteOptions {
        force_replace: opts.force_replace,
        retain_override: Some(effective_retain),
    };
    let key = ValueKey {
        entity_id,
        parameter_definition_id: pd_id,
        alternative_id: alt_id,
    };
    let outcome = delete_value(conn, commit_id, key, &resolved_opts)?;
    report.absorb_delete(&outcome);
    if outcome.rows_closed == 0 {
        report.warnings.push(Warning {
            kind: WarningKind::NothingToDelete { key_index },
            object_type: "value".into(),
            name: format!("{}:{}/{}/{}", dk.class, dk.parameter, dk.alternative, dk.entity),
            id: 0,
        });
    }
    Ok(())
}

/// Push an [`WarningKind::UnknownReferent`] for a lookup miss at `level`.
fn unknown(report: &mut WriteReport, key_index: usize, level: &str, name: &str) {
    report.warnings.push(Warning {
        kind: WarningKind::UnknownReferent {
            key_index,
            level: level.into(),
        },
        object_type: level.into(),
        name: name.into(),
        id: 0,
    });
}

// Lookup-only resolvers: the FIRST branch of each write-path resolver (§2), via
// `opt_id` — the case-insensitive current-row lookup, minus the auto-create tail.

pub(crate) fn lookup_entity_class(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(conn, "SELECT id FROM entity_class WHERE cur_name = lower(?)", -1, name)
}

pub(crate) fn lookup_alternative(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(conn, "SELECT id FROM alternative WHERE cur_name = lower(?)", -1, name)
}

pub(crate) fn lookup_parameter(
    conn: &Connection,
    class_id: i64,
    name: &str,
) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM parameter_definition WHERE cur_cls = ? AND cur_name = lower(?)",
        class_id,
        name,
    )
}

pub(crate) fn lookup_entity(conn: &Connection, class_id: i64, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM entity WHERE cur_cls = ? AND cur_name = lower(?)",
        class_id,
        name,
    )
}

/// The pure, DB-independent part of one segment's write: batch-contract
/// validation, kind inference, domain merge, tz warnings, and the per-entity
/// row-index lists. Everything here is safe to compute once outside the retried
/// transaction (it touches no DB state).
struct PreparedSegment {
    class: String,
    parameter: String,
    alternative: String,
    value_kind: ValueKind,
    inferred: Vec<(String, IndexKind)>,
    domains: Vec<LabelDomain>,
    tz_warnings: Vec<Warning>,
    /// Distinct entity names, first-seen order, case-insensitively deduped.
    names: Vec<String>,
    /// Per-entity row indices into the concatenated batch, ascending (i.e.
    /// original row order). O(nrows) total, vs O(entities × nrows) for the
    /// boolean masks these replace (critique F4).
    rows: Vec<Vec<u32>>,
    /// The dims+value sub-batch (entity column dropped).
    sub_all: RecordBatch,
}

fn prepare_segment(seg: &Segment) -> Result<PreparedSegment, DbError> {
    // Referent names must be non-empty (critique F5): a caller-contract
    // problem is caught here, never as a raw CHECK error mid-transaction.
    for (what, name) in [
        ("class", seg.class.as_str()),
        ("parameter", seg.parameter.as_str()),
        ("alternative", seg.alternative.as_str()),
    ] {
        if name.trim().is_empty() {
            return Err(DbError::Api(format!("{what} name must be non-empty")));
        }
    }
    // A segment's batches are one logical table (spec §2.3); an empty segment
    // writes nothing — reject before it can auto-create referents or burn a
    // commit (critique F8).
    if seg.batches.is_empty() {
        return Err(DbError::Api("empty batch: no rows to write".into()));
    }
    let batch = if seg.batches.len() == 1 {
        seg.batches[0].clone()
    } else {
        concat_batches(&seg.batches[0].schema(), &seg.batches)?
    };
    let ncols = batch.num_columns();
    if ncols < 2 {
        return Err(DbError::Api(format!(
            "batch needs at least an entity column and a value column, got {ncols}"
        )));
    }
    let rank = ncols - 2;
    if rank > 4 {
        return Err(DbError::Api(format!("declared rank {rank} exceeds the cap of 4")));
    }
    if batch.num_rows() == 0 {
        return Err(DbError::Api("empty batch: no rows to write".into()));
    }

    // Measure kind from the value column's Arrow type.
    let value_kind = measure_kind(batch.schema().field(ncols - 1).data_type())?;

    // Inferred (dimension_name, index kind) per dimension, from the dim columns'
    // Arrow types + field names. Used when a definition is auto-created; the
    // declared type of an EXISTING definition is read from the DB instead.
    let mut inferred: Vec<(String, IndexKind)> = Vec::with_capacity(rank);
    for i in 0..rank {
        let field = batch.schema().field(i + 1).clone();
        inferred.push((field.name().clone(), index_kind(field.data_type())?));
    }

    // Effective label domains: the in-process arg, plus any Arrow field-metadata
    // domain for a label dimension the arg did not cover (decision D).
    let domains = effective_domains(&batch, &inferred, &seg.domains)?;

    // Tz-bearing timestamp columns are accepted — a Timestamp(µs, tz) is a
    // well-defined instant, stored as the UTC-µs instant unchanged — but never
    // silently: each gets a TimezoneNormalized warning (critique F6).
    let mut tz_warnings: Vec<Warning> = Vec::new();
    for i in 1..ncols {
        let field = batch.schema().field(i).clone();
        if let DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) = field.data_type() {
            tz_warnings.push(Warning {
                kind: WarningKind::TimezoneNormalized {
                    column: field.name().clone(),
                    tz: tz.to_string(),
                },
                object_type: "column".into(),
                name: field.name().clone(),
                id: 0,
            });
        }
    }

    // The per-entity row-index lists and the dims+value sub-batch (entity col
    // dropped).
    let entity_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DbError::Api("entity column must be Utf8 (entity names)".into()))?;
    let nrows = batch.num_rows();
    let mut slot: HashMap<String, usize> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<u32>> = Vec::new();
    for row in 0..nrows {
        if entity_arr.is_null(row) {
            return Err(DbError::Api(format!("null entity name in row {row}")));
        }
        let name = entity_arr.value(row).to_string();
        if name.trim().is_empty() {
            return Err(DbError::Api(format!("empty entity name in row {row}")));
        }
        // Dedup by the same fold DB resolution uses (`lower(name)`; ASCII per
        // the #2373 charset) so case-aliases cannot double-write one entity.
        // Aliased spellings are ambiguous, not mergeable (critique F3).
        let idx = *slot.entry(name.to_ascii_lowercase()).or_insert_with(|| {
            names.push(name.clone());
            rows.push(Vec::new());
            names.len() - 1
        });
        if names[idx] != name {
            return Err(DbError::Api(format!(
                "entity names {:?} and {name:?} resolve to the same entity \
                 (name resolution is case-insensitive); use one spelling per batch",
                names[idx]
            )));
        }
        rows[idx].push(row as u32);
    }
    let project: Vec<usize> = (1..ncols).collect();
    let sub_all = batch.project(&project)?;

    Ok(PreparedSegment {
        class: seg.class.clone(),
        parameter: seg.parameter.clone(),
        alternative: seg.alternative.clone(),
        value_kind,
        inferred,
        domains,
        tz_warnings,
        names,
        rows,
        sub_all,
    })
}

/// One segment's DB-touching write sequence (resolve → validate → canon → tier
/// → axis → history), run inside the caller's open transaction. Callable N
/// times per transaction — this is what makes multi-segment ingest one commit.
fn write_prepared(
    conn: &Connection,
    commit_id: i64,
    p: &PreparedSegment,
    opts: WriteOptions,
    cache: &mut AxisCache,
    report: &mut WriteReport,
) -> Result<(), DbError> {
    report.warnings.extend(p.tz_warnings.iter().cloned());

    let class_id = resolve_entity_class(conn, commit_id, &p.class, &mut report.warnings)?;
    let (pd_id, pt_id, declared) = resolve_parameter(
        conn,
        commit_id,
        class_id,
        &p.parameter,
        &p.inferred,
        p.value_kind,
        &mut report.warnings,
    )?;
    let retain_content = read_retain_content(conn, pd_id)?;
    let alt_id = resolve_alternative(conn, commit_id, &p.alternative, &mut report.warnings)?;

    // Fold the definition's retain_content into retain_override unless the
    // caller already overrode it (VMP Part 5 primary retention lever): else a
    // write to a retain_content=FALSE definition would silently keep payloads.
    let effective_retain = opts.retain_override.unwrap_or(retain_content);
    let resolved_opts = WriteOptions {
        force_replace: opts.force_replace,
        retain_override: Some(effective_retain),
    };

    for (name, idx) in p.names.iter().zip(p.rows.iter()) {
        let entity_id =
            resolve_entity(conn, commit_id, class_id, &p.class, name, &mut report.warnings)?;
        let sub = take_record_batch(&p.sub_all, &UInt32Array::from(idx.clone()))?;
        let value = canonicalize(&sub, &declared, p.value_kind, &p.domains)?;
        let key = ValueKey {
            entity_id,
            parameter_definition_id: pd_id,
            alternative_id: alt_id,
        };
        let outcome = write_value(conn, commit_id, key, &value, cache, &resolved_opts, Some(pt_id))?;
        report.absorb(&outcome);
    }
    Ok(())
}

/// Read a written parameter back for one class/parameter/alternative into a
/// `RecordBatch`, through the `read_parameter` SQL view/macro (VMP Part 6; plan
/// Part 3 — reads are SQL, the library never gatekeeps them). One fresh RO
/// connection, opened and closed per call (CLAUDE.md invariant 1). Enough to
/// round-trip-verify a write; the columns are the macro's canonical long shape
/// (`entity, alternative, tier, value_kind, idx*_*, v_*`).
pub fn read_parameter(
    db: &Path,
    class: &str,
    parameter: &str,
    alternative: &str,
) -> Result<RecordBatch, DbError> {
    with_read(db, |conn| {
        let mut stmt = conn.prepare(
            "SELECT * FROM read_parameter(?, ?) WHERE lower(alternative) = lower(?)",
        )?;
        let arrow = stmt.query_arrow(params![class, parameter, alternative])?;
        let schema = arrow.get_schema();
        let batches: Vec<RecordBatch> = arrow.collect();
        Ok(concat_batches(&schema, &batches)?)
    })
}

/// The database's stable identity UUID — the `db_identity` reserved metadata
/// key, minted once by `900_bootstrap.sql` at init and never changed thereafter
/// (docs/dev/schema_interop.md §9: the anchor output DBs stamp their
/// "produced from input DB <identity> at commit C under scenario S" provenance
/// against). It survives reopen and compaction (the repack copies `metadata`
/// rows as data). One fresh RO connection per call (CLAUDE.md invariant 1).
/// Plain-SQL equivalent:
/// `SELECT value FROM metadata WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL`.
pub fn db_identity(db: &Path) -> Result<String, DbError> {
    with_read(db, |conn| {
        Ok(conn.query_row(
            "SELECT value FROM metadata \
             WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )?)
    })
}

// ---------------------------------------------------------------------------
// Arrow-type → kind mapping
// ---------------------------------------------------------------------------

fn measure_kind(dt: &DataType) -> Result<ValueKind, DbError> {
    Ok(match dt {
        DataType::Float64 => ValueKind::Float,
        DataType::Int64 => ValueKind::Int,
        DataType::Boolean => ValueKind::Bool,
        DataType::Utf8 => ValueKind::String,
        DataType::Timestamp(TimeUnit::Microsecond, _) => ValueKind::DateTime,
        other => {
            return Err(DbError::Api(format!(
                "unsupported value column Arrow type {other:?}"
            )))
        }
    })
}

fn index_kind(dt: &DataType) -> Result<IndexKind, DbError> {
    Ok(match dt {
        DataType::Timestamp(TimeUnit::Microsecond, _) => IndexKind::Datetime,
        DataType::Int64 => IndexKind::Integer,
        DataType::Utf8 => IndexKind::Label,
        other => {
            return Err(DbError::Api(format!(
                "unsupported dimension column Arrow type {other:?}"
            )))
        }
    })
}

fn index_kind_tag(k: IndexKind) -> &'static str {
    match k {
        IndexKind::Datetime => "datetime",
        IndexKind::Integer => "integer",
        IndexKind::Label => "label",
        IndexKind::Period => "period",
        IndexKind::TimePattern => "time_pattern",
    }
}

fn parse_index_kind(s: &str) -> Result<IndexKind, DbError> {
    Ok(match s {
        "datetime" => IndexKind::Datetime,
        "integer" => IndexKind::Integer,
        "label" => IndexKind::Label,
        "period" => IndexKind::Period,
        "time_pattern" => IndexKind::TimePattern,
        other => {
            return Err(DbError::Api(format!(
                "unknown dimension kind {other:?} in parameter_type_dimension"
            )))
        }
    })
}

/// Merge the caller's label domains with any carried in Arrow field metadata
/// (`spinal:label_domain` = a JSON array of the ordered labels) for a label
/// dimension the arg did not cover (decision D — both channels).
fn effective_domains(
    batch: &RecordBatch,
    inferred: &[(String, IndexKind)],
    provided: &[LabelDomain],
) -> Result<Vec<LabelDomain>, DbError> {
    let mut out: Vec<LabelDomain> = provided.to_vec();
    for (i, (_, kind)) in inferred.iter().enumerate() {
        if *kind != IndexKind::Label {
            continue;
        }
        let position = (i + 1) as u8;
        if out.iter().any(|d| d.position == position) {
            continue;
        }
        let field = batch.schema().field(i + 1).clone();
        if let Some(js) = field.metadata().get("spinal:label_domain") {
            let labels: Vec<String> = serde_json::from_str(js).map_err(|e| {
                DbError::Api(format!("bad spinal:label_domain metadata on {:?}: {e}", field.name()))
            })?;
            out.push(LabelDomain { position, labels });
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Name resolution + auto-create (decision B)
// ---------------------------------------------------------------------------

fn opt_id(conn: &Connection, sql: &str, id_param: i64, name: &str) -> Result<Option<i64>, DbError> {
    // Two shapes share this: class/alternative (name only) and entity (class +
    // name). `id_param` < 0 means "name only"; the SQL then ignores it.
    let r = if id_param < 0 {
        conn.query_row(sql, params![name], |r| r.get::<_, i64>(0))
    } else {
        conn.query_row(sql, params![id_param, name], |r| r.get::<_, i64>(0))
    };
    match r {
        Ok(id) => Ok(Some(id)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn resolve_entity_class(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<i64, DbError> {
    if let Some(id) = opt_id(conn, "SELECT id FROM entity_class WHERE cur_name = lower(?)", -1, name)? {
        return Ok(id);
    }
    let id: i64 = conn.query_row(
        "INSERT INTO entity_class (name, valid_from_commit) VALUES (?, ?) RETURNING id",
        params![name, commit_id],
        |r| r.get(0),
    )?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "entity_class".into(),
        name: name.into(),
        id,
    });
    Ok(id)
}

fn resolve_alternative(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<i64, DbError> {
    if let Some(id) = opt_id(conn, "SELECT id FROM alternative WHERE cur_name = lower(?)", -1, name)? {
        return Ok(id);
    }
    let id: i64 = conn.query_row(
        "INSERT INTO alternative (name, valid_from_commit) VALUES (?, ?) RETURNING id",
        params![name, commit_id],
        |r| r.get(0),
    )?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "alternative".into(),
        name: name.into(),
        id,
    });
    Ok(id)
}

fn resolve_entity(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    class: &str,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<i64, DbError> {
    if let Some(id) = opt_id(
        conn,
        "SELECT id FROM entity WHERE cur_cls = ? AND cur_name = lower(?)",
        class_id,
        name,
    )? {
        return Ok(id);
    }
    // Never auto-create into an N-D class (critique F10): an N-D entity's
    // identity is its member tuple, which the batch does not carry — a hollow
    // member-less entity would be a trap, not a convenience.
    let ndims: i64 = conn.query_row(
        "SELECT count(*) FROM entity_class_dimension WHERE cur_c = ?",
        params![class_id],
        |r| r.get(0),
    )?;
    if ndims > 0 {
        return Err(DbError::Api(format!(
            "cannot auto-create entity {name:?} in {ndims}-dimensional class {class:?}: \
             an N-D entity needs explicit membership (entity_element rows); create it first"
        )));
    }
    let id: i64 = conn.query_row(
        "INSERT INTO entity (entity_class_id, name, valid_from_commit) VALUES (?, ?, ?) RETURNING id",
        params![class_id, name, commit_id],
        |r| r.get(0),
    )?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "entity".into(),
        name: name.into(),
        id,
    });
    Ok(id)
}

/// Resolve the parameter definition within `class_id`, auto-creating it (and its
/// `parameter_type` + `parameter_type_dimension` declaration) when absent. The
/// declared type SET is READ FROM THE DB for an existing definition (critique
/// S2; template_ir_plan D2(a)); only a freshly created definition takes its
/// declared type from the batch's inferred dimensions. Returns
/// `(pd_id, parameter_type_id, shape)` — the member id is the exactly-one
/// binding persisted on every value row (D11, 2026-09-18); auto-created /
/// auto-materialized declarations bind their freshly created row.
fn resolve_parameter(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    name: &str,
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
    warnings: &mut Vec<Warning>,
) -> Result<(i64, i64, DeclaredType), DbError> {
    if let Some(pd_id) = opt_id(
        conn,
        "SELECT id FROM parameter_definition WHERE cur_cls = ? AND cur_name = lower(?)",
        class_id,
        name,
    )? {
        // Existing definition: read the declared allowed-type set from the DB
        // and hold the batch to it — it must instantiate exactly one member
        // (critique F2/F4/F11; VMP Part 7 as amended 2026-09-18, D2(a)). If the
        // definition somehow has no parameter_type row yet, materialize one
        // from the batch — never silently (F7). Auto-materialize fires ONLY at
        // zero rows: once a set is declared, a non-member batch is rejected,
        // never used to widen the set (widening is a template/reconcile
        // operation, template_ir_plan §3.3).
        let set = read_declared_types(conn, pd_id)?;
        if set.is_empty() {
            let pt_id = create_parameter_type(conn, commit_id, pd_id, inferred, value_kind)?;
            warnings.push(Warning {
                kind: WarningKind::AutoCreated,
                object_type: "parameter_type".into(),
                name: name.into(),
                id: pt_id,
            });
            Ok((pd_id, pt_id, inferred_declared(inferred)))
        } else {
            let matched = validate_declared(name, pd_id, set, inferred, value_kind)?;
            Ok((pd_id, matched.pt_id, matched.shape))
        }
    } else {
        let pd_id: i64 = conn.query_row(
            "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit) \
             VALUES (?, ?, ?) RETURNING id",
            params![class_id, name, commit_id],
            |r| r.get(0),
        )?;
        let pt_id = create_parameter_type(conn, commit_id, pd_id, inferred, value_kind)?;
        warnings.push(Warning {
            kind: WarningKind::AutoCreated,
            object_type: "parameter_definition".into(),
            name: name.into(),
            id: pd_id,
        });
        Ok((pd_id, pt_id, inferred_declared(inferred)))
    }
}

fn inferred_declared(inferred: &[(String, IndexKind)]) -> DeclaredType {
    DeclaredType {
        positions: inferred
            .iter()
            .enumerate()
            .map(|(i, (_, k))| ((i + 1) as u8, *k))
            .collect(),
    }
}

/// One member of a definition's declared allowed-type set as stored in the DB:
/// the `parameter_type` row id (the D11 binding persisted on value rows), the
/// `value_kind` tag, the dimension names in declared order, and the
/// `(position, kind)` shape canon consumes.
struct DbDeclared {
    pt_id: i64,
    value_kind: String,
    dim_names: Vec<String>,
    shape: DeclaredType,
}

/// Human-readable one-line description of a declared member, used by the
/// set-membership error messages (they must name the allowed set).
fn member_desc(m: &DbDeclared) -> String {
    let rank = m.shape.rank();
    if rank == 0 {
        format!("{} rank 0", m.value_kind)
    } else {
        let dims: Vec<String> = m
            .dim_names
            .iter()
            .zip(&m.shape.positions)
            .map(|(n, (_, k))| format!("{n}: {}", index_kind_tag(*k)))
            .collect();
        format!("{} rank {rank} ({})", m.value_kind, dims.join(", "))
    }
}

/// Human-readable description of the batch's shape, mirroring `member_desc`.
fn batch_desc(inferred: &[(String, IndexKind)], value_kind: ValueKind) -> String {
    if inferred.is_empty() {
        format!("rank 0, {}", value_kind.tag())
    } else {
        let dims: Vec<String> = inferred
            .iter()
            .map(|(n, k)| format!("{n}: {}", index_kind_tag(*k)))
            .collect();
        format!("rank {}, indexed ({}), measure {}", inferred.len(), dims.join(", "), value_kind.tag())
    }
}

/// The projection member matching AND set-distinguishability are both defined
/// on (VMP Part 7 as amended 2026-09-18, D2(a)): the `value_kind` tag plus the
/// ordered `(case-folded dimension name, index kind)` vector — rank is implied
/// by the vector's length. One key, two uses: a batch instantiates the member
/// whose key equals the batch's, and a set is valid iff member keys are
/// pairwise distinct. Kind matching is strict: the pipeline's Arrow-derivable
/// kinds (datetime/integer/label) map 1:1 onto column types, so a member whose
/// kind the batch column cannot carry is simply not a match (declared
/// `period`/`time_pattern` members stay unwritable until their long-input
/// derivation lands — canon's `UnsupportedKind`; types.rs note). Tag equality
/// is deliberately case-sensitive — the writer only ever stores lowercase
/// tags; a case-variant is import damage, surfaced by the no-match hint.
type MatchKey = (String, Vec<(String, IndexKind)>);

fn member_key(m: &DbDeclared) -> MatchKey {
    (
        m.value_kind.clone(),
        m.dim_names
            .iter()
            .zip(&m.shape.positions)
            .map(|(n, (_, k))| (n.to_ascii_lowercase(), *k))
            .collect(),
    )
}

fn batch_key(inferred: &[(String, IndexKind)], value_kind: ValueKind) -> MatchKey {
    let tag = if inferred.is_empty() { value_kind.tag() } else { "indexed" };
    (
        tag.to_string(),
        inferred
            .iter()
            .map(|(n, k)| (n.to_ascii_lowercase(), *k))
            .collect(),
    )
}

/// Hold the batch to an EXISTING declared allowed-type set (VMP Part 7 as
/// amended 2026-09-18, D2(a); template_ir_plan §3.3): the batch must
/// instantiate **exactly one** declared member — value-kind tag, rank,
/// per-position index kinds, and dimension names in declared order. A batch
/// instantiating no member is rejected loudly, naming the allowed set; the
/// writer never widens a declared set implicitly. A set whose members are not
/// pairwise distinguishable on that tuple is itself invalid and refused
/// EAGERLY, before any matching (deliberate: a damaged declaration is never
/// half-usable). Returns the matched member — the member the batch binds to
/// (its dimension kinds drive canon, its names/positions key label domains).
fn validate_declared(
    parameter: &str,
    pd_id: i64,
    mut set: Vec<DbDeclared>,
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
) -> Result<DbDeclared, DbError> {
    let keys: Vec<MatchKey> = set.iter().map(member_key).collect();
    for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            if keys[i] == keys[j] {
                return Err(DbError::Api(format!(
                    "parameter_definition {pd_id} ({parameter:?}) declares indistinguishable \
                     current parameter_type rows ({}): exactly-one-member matching is \
                     impossible; the declaration is invalid (import/hand-edit damage; \
                     repair by deleting the parameter through the object-delete API \
                     (commit_object_deletes) and re-declaring it)",
                    member_desc(&set[i])
                )));
            }
        }
    }
    let bkey = batch_key(inferred, value_kind);
    if let Some(ix) = keys.iter().position(|k| *k == bkey) {
        return Ok(set.swap_remove(ix));
    }
    // Probable-damage hint: a stored tag that differs from the batch's only by
    // case, or sits outside the known tag vocabulary, points at import/hand
    // edits rather than a caller mistake. Matching itself stays case-sensitive.
    const KNOWN_TAGS: [&str; 7] =
        ["float", "int", "bool", "string", "date_time", "duration", "indexed"];
    let damaged = set.iter().any(|m| {
        (m.value_kind != bkey.0 && m.value_kind.eq_ignore_ascii_case(&bkey.0))
            || !KNOWN_TAGS.contains(&m.value_kind.as_str())
    });
    let hint = if damaged {
        " (note: a declared value_kind tag differs from the batch's only by case, or is \
         outside the known tag vocabulary — probable import/hand-edit damage; tag \
         matching is case-sensitive)"
    } else {
        ""
    };
    let members: Vec<String> = set.iter().map(member_desc).collect();
    Err(DbError::Api(format!(
        "parameter {parameter:?}: batch ({}) instantiates none of the {} declared member \
         type(s): [{}]; a write never widens the declared type set — declare the new \
         shape first (template/reconcile operation){hint}",
        batch_desc(inferred, value_kind),
        members.len(),
        members.join("; ")
    )))
}

/// Read a definition's full current declared-type set from `parameter_type` /
/// `parameter_type_dimension` (VMP Part 2 "one or more declarations"; Part 7
/// entry check amended to set-membership, 2026-09-18 D2(a)). Empty = no
/// current `parameter_type` row. Multi-row is the legal declared-set state —
/// duplicate ranks are legal (members differ on names/kinds); exact-duplicate
/// members are refused at match time (`validate_declared`). Per member,
/// declared positions that are not exactly `1..=rank` (reachable only via
/// import/hand edits) are refused loudly. Contiguity is also what lets
/// `effective_domains` key metadata label domains by column order — column `i`
/// is exactly declared position `i + 1`.
fn read_declared_types(conn: &Connection, pd_id: i64) -> Result<Vec<DbDeclared>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, value_kind FROM parameter_type \
         WHERE parameter_definition_id = ? AND valid_to_commit IS NULL ORDER BY id",
    )?;
    let pts: Vec<(i64, String)> = stmt
        .query_map(params![pd_id], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?;

    // One statement, prepared once, executed per member (never re-prepared in
    // the loop).
    let mut dim_stmt = conn.prepare(
        "SELECT position, dimension_name, kind FROM parameter_type_dimension \
         WHERE parameter_type_id = ? AND valid_to_commit IS NULL ORDER BY position",
    )?;
    let mut out: Vec<DbDeclared> = Vec::with_capacity(pts.len());
    for (pt_id, value_kind) in pts {
        let mut positions: Vec<(u8, IndexKind)> = Vec::new();
        let mut dim_names: Vec<String> = Vec::new();
        let rows = dim_stmt.query_map(params![pt_id], |r| {
            Ok((r.get::<_, i8>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })?;
        for row in rows {
            let (pos, dname, kind) = row?;
            positions.push((pos as u8, parse_index_kind(&kind)?));
            dim_names.push(dname);
        }
        for (i, (pos, _)) in positions.iter().enumerate() {
            if *pos as usize != i + 1 {
                return Err(DbError::Api(format!(
                    "parameter_type {pt_id}: declared dimension positions are not contiguous \
                     1..=rank (position {pos} at index {i}; import/hand-edit damage)"
                )));
            }
        }
        out.push(DbDeclared { pt_id, value_kind, dim_names, shape: DeclaredType { positions } });
    }
    Ok(out)
}

/// Insert the `parameter_type` (+ its dimension rows) declaring the batch's
/// shape: `value_kind = 'indexed'` for rank ≥ 1 else the measure kind tag;
/// dimension names/kinds from the inferred columns (VMP Part 2/7). Returns the
/// created `parameter_type` id.
fn create_parameter_type(
    conn: &Connection,
    commit_id: i64,
    pd_id: i64,
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
) -> Result<i64, DbError> {
    let type_kind = if inferred.is_empty() {
        value_kind.tag()
    } else {
        "indexed"
    };
    let rank = inferred.len() as i8;
    let pt_id: i64 = conn.query_row(
        "INSERT INTO parameter_type (parameter_definition_id, value_kind, rank, valid_from_commit) \
         VALUES (?, ?, ?, ?) RETURNING id",
        params![pd_id, type_kind, rank, commit_id],
        |r| r.get(0),
    )?;
    for (i, (dname, kind)) in inferred.iter().enumerate() {
        conn.execute(
            "INSERT INTO parameter_type_dimension \
               (parameter_type_id, position, dimension_name, kind, valid_from_commit) \
             VALUES (?, ?, ?, ?, ?)",
            params![pt_id, (i + 1) as i8, dname, index_kind_tag(*kind), commit_id],
        )?;
    }
    Ok(pt_id)
}

/// The definition's `retain_content` — VMP Part 5's primary retention lever
/// (default TRUE; FALSE on result classes) that `history::write_value` must see
/// via `retain_override`, else a FALSE definition silently keeps full payloads.
fn read_retain_content(conn: &Connection, pd_id: i64) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT retain_content FROM parameter_definition WHERE cur_id = ?",
        params![pd_id],
        |r| r.get(0),
    )?)
}
