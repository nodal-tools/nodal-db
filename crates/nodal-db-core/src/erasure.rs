//! Hard erasure — physical removal **with a trace** (erasure_compaction_plan
//! §2–§3, decisions D-E1..E6 2026-09-19). The one sanctioned exception to
//! append-only history: rows are physically DELETEd, and every removal writes
//! an `erasure_marker` row in the SAME transaction (the removed rows carry no
//! close-out edge — the marker IS their trace). The `commit` log is never
//! touched; as-of reads that would silently span removed content fail loudly
//! on the marker (the WHERE-clause guard in `schema/110_views_history.sql`).
//! History may be *removed with a trace*; it may never be *rewritten*.
//!
//! ## v1 grain: erase-by-alternative
//! The results-architecture customer (`docs/dev/schema_interop.md` §9
//! overwrite-on-rerun): a discarded run's whole result-alternative. Scope is
//! closed by definition — every row keyed on the alternative, ANY validity,
//! across the seven alternative-keyed tables plus the `value_array_hist` twin
//! (plan §2.2, leg list independently re-verified by the critique). Retained:
//! the `alternative` identity row (closed, never removed — D-E2), the whole
//! `commit` log, `run` provenance rows, `semantic_annotation`, and `axis` rows
//! (orphan collection is a deferred maintain-pass job, D-E5 — an erase that
//! scanned total axis references would turn a fast targeted operation into a
//! whole-DB scan).
//!
//! ## Shared predicates, separate execution arms (critique M3)
//! Row selection comes from the object-delete leg registry
//! ([`crate::object_delete::erase_scope_legs`]) — ONE dependency definition
//! (invariant 2). The *executor* is erase's own: registry predicate +
//! any-validity + plain DELETE. The delete arm's live-guard and its array
//! hist-move do NOT run here — verbatim, the hist-move would copy the
//! discarded run's array payloads (the gigabytes this primitive exists to
//! reclaim) into `value_array_hist` only for the same transaction to delete
//! them, write-amplifying the erase by roughly the run's size.
//!
//! ## Marker kinds
//! The marker table is shaped for the whole grain family (alternative /
//! horizon / entity / detach-to-archive); the v1 core writes ONLY
//! `kind='alternative'` with `continuation_db` NULL, and there is no API that
//! writes any other kind — the unbuilt semantics cannot be half-used (plan
//! §6). [`erase_alternative`] refuses non-alternative [`ObjectDeleteKey`]s.
//!
//! ## Where erase may run (D-E6)
//! The mechanism is DB-agnostic; the input-vs-output-DB policy is a client
//! concern (editor/CLI warn on provenance-stamped input DBs). Nothing here
//! inspects provenance.
//!
//! Bytes return to the OS at the next [`crate::compaction::compact`]; the
//! primitive stays two calls — erase is fast and transactional, compaction is
//! heavyweight and exclusively locked (different lock scopes, never fused).
//! [`erase_alternative_and_compact`] is a thin convenience chain only.

use std::path::Path;

use duckdb::{params, Connection};

use crate::conn::{with_read, with_write_txn_retrying};
use crate::error::DbError;
use crate::object_delete::{erase_scope_legs, EraseLeg};
use crate::types::{ObjectDeleteKey, ObjectKind, ObjectRef, TableCount};

/// A resolved `(id, name)` reference for the preview's warning surface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedRef {
    pub id: i64,
    pub name: String,
}

/// Read-only preview of one [`erase_alternative`] (plan §3, mirroring the
/// object-delete preview convention): per-table row counts, an advisory bytes
/// estimate, and the reported-not-blocking reference surface. Best-effort
/// snapshot — the erase-time [`EraseReport`] is authoritative; a concurrent
/// writer between preview and erase can move the counts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ErasePreview {
    /// `false` ⇒ the name resolved to no alternative identity (any validity);
    /// every other field is zero/empty.
    pub found: bool,
    pub alternative_id: i64,
    /// The identity row's stored spelling (latest version).
    pub alternative_name: String,
    /// Rows that would be removed, per table (any validity; only tables with
    /// ≥ 1 row are listed).
    pub per_table: Vec<TableCount>,
    pub rows_total: u64,
    /// Advisory payload estimate (plan weakest-point (b): DuckDB gives no
    /// exact per-row physical size — array cells are counted exactly at
    /// 8 bytes each, other tiers use fixed per-row estimates). Acceptance
    /// measures the truth end-to-end via file sizes instead.
    pub bytes_estimate: u64,
    /// `true` ⇒ the identity row is still current, so erase will close it.
    pub still_current: bool,
    /// Live scenarios whose stacks lose a rank row (REPORTED, never blocking
    /// — plan §3: erase has no cascade-vs-refuse choice; the editor's job is
    /// to show this, the core's job is to be exact about it).
    pub referencing_scenarios: Vec<NamedRef>,
    /// `run` rows whose results this erase removes (`result_alternative_id`).
    /// The rows themselves are retained — a run pointing at an erased
    /// alternative IS the "results were erased" signal, via the marker.
    pub referencing_runs: Vec<i64>,
}

/// Options for [`erase_alternative`] (plan §3).
#[derive(Clone, Copy, Debug, Default)]
pub struct EraseOpts<'a> {
    /// Free-form operator context, stored on the marker row.
    pub note: Option<&'a str>,
    /// Optional machine-check of the rerun-overwrite ordering contract
    /// (plan m7a; `schema_interop.md` §9): names that MUST have a current
    /// alternative row (the replacement run) or the erase refuses, touching
    /// nothing. Belt for the client-ordered flow; empty = no check. The
    /// FlexTool runner should use it.
    pub expect_present: &'a [&'a str],
}

/// What one [`erase_alternative`] actually did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EraseReport {
    /// `false` ⇒ idempotent no-op: the identity was already closed and no
    /// in-scope row existed (e.g. a second erase of the same name). NO commit
    /// row and NO marker were written — a zero-row marker would extend the
    /// as-of guard's refusal range for no content change.
    pub erased: bool,
    /// The `erasure_marker` row id (0 when `!erased`).
    pub marker_id: i64,
    /// The erasure commit (0 when `!erased`).
    pub commit_id: i64,
    pub alternative_id: i64,
    /// `true` ⇒ step 2 closed the identity row (it was still current);
    /// `false` ⇒ it was already closed (a prior `delete_objects` — erase
    /// composes with, and does not require, a prior delete).
    pub identity_closed: bool,
    /// Rows physically removed, per table (DELETE rowcounts — authoritative).
    pub per_table: Vec<TableCount>,
    /// Total rows removed across tables — what the marker's `rows_removed`
    /// records. The closed identity row is NOT counted (it is retained).
    pub rows_total: u64,
    /// Advisory payload estimate, computed over the rows just before their
    /// DELETE (stored on the marker as `bytes_estimate`).
    pub bytes_estimate: u64,
}

/// Sentinel message for the internal nothing-to-do rollback (mapped to an
/// `EraseReport { erased: false, .. }` — never surfaced to callers).
const NOOP_SENTINEL: &str = "erase: nothing to erase (internal no-op sentinel)";

/// Preview one erase-by-alternative (read-only; one per-operation RO
/// connection, CLAUDE.md invariant 1). See [`ErasePreview`].
pub fn preview_erase_alternative(
    db: &Path,
    alt: &ObjectDeleteKey,
) -> Result<ErasePreview, DbError> {
    let name = validate_erase_key(alt)?.to_string();
    with_read(db, |conn| {
        let Some(resolved) = resolve_alternative_any(conn, &name)? else {
            return Ok(ErasePreview::default());
        };
        let legs = alternative_erase_legs();
        let mut per_table: Vec<TableCount> = Vec::new();
        let mut rows_total = 0u64;
        let mut bytes_estimate = 0u64;
        for leg in &legs {
            let n: i64 = conn.query_row(
                &format!("SELECT count(*) FROM {} WHERE {}", leg.table, leg.predicate),
                params![resolved.id],
                |r| r.get(0),
            )?;
            if n > 0 {
                bytes_estimate += estimate_leg_bytes(conn, leg, resolved.id)?;
                rows_total += n as u64;
                per_table.push(TableCount {
                    table: leg.table.to_string(),
                    rows: n as usize,
                });
            }
        }
        // Reported-not-blocking reference surface (plan §3).
        let mut stmt = conn.prepare(
            "SELECT DISTINCT s.id, s.name FROM scenario_alternative sa \
             JOIN scenario s ON s.id = sa.scenario_id AND s.valid_to_commit IS NULL \
             WHERE sa.alternative_id = ? AND sa.valid_to_commit IS NULL \
             ORDER BY s.id",
        )?;
        let referencing_scenarios = stmt
            .query_map(params![resolved.id], |r| {
                Ok(NamedRef {
                    id: r.get(0)?,
                    name: r.get(1)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut rstmt = conn.prepare(
            "SELECT id FROM run WHERE result_alternative_id = ? ORDER BY id",
        )?;
        let referencing_runs = rstmt
            .query_map(params![resolved.id], |r| r.get::<_, i64>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ErasePreview {
            found: true,
            alternative_id: resolved.id,
            alternative_name: resolved.name,
            per_table,
            rows_total,
            bytes_estimate,
            still_current: resolved.still_current,
            referencing_scenarios,
            referencing_runs,
        })
    })
}

/// Erase one alternative (plan §2.1): one ordinary write transaction
/// (per-operation connection, conflict-retried, [`DbError::Busy`] on
/// contention — nothing new), performing in order:
///
///  1. open the erasure commit C (`user` is the commit author; the message
///     records the operation);
///  2. close out the alternative identity row if still current (guarded
///     UPDATE — data rows get NO close-out: they are removed in step 3, and a
///     close-out edge the same transaction deletes is unobservable work);
///  3. physically DELETE every in-scope row (current and superseded alike)
///     via the erase execution arm over the shared registry predicates;
///  4. write the `erasure_marker` row (`kind='alternative'`, `at_commit = C`,
///     counts/bytes from step 3);
///  5. commit.
///
/// Transaction atomicity gives the plan's §1.2 invariant: a reader either
/// sees the pre-erase state or (identity closed + data gone + marker
/// present) — never anything between. Bytes return to the OS at the next
/// compaction.
///
/// An alternative name that resolves to no identity row (any validity) is a
/// loud error. A name whose identity is already closed AND has no in-scope
/// rows is an idempotent no-op (`erased: false` — no commit, no marker).
pub fn erase_alternative(
    db: &Path,
    alt: &ObjectDeleteKey,
    opts: &EraseOpts,
    user: &str,
) -> Result<EraseReport, DbError> {
    let name = validate_erase_key(alt)?.to_string();
    if user.trim().is_empty() {
        return Err(DbError::Api("user must be non-empty".into()));
    }
    for n in opts.expect_present {
        if n.trim().is_empty() {
            return Err(DbError::Api("expect_present names must be non-empty".into()));
        }
    }
    let note = opts.note.map(str::to_string);
    let expect: Vec<String> = opts.expect_present.iter().map(|s| s.to_string()).collect();
    let message = format!("erase alternative '{name}'");

    let outcome = with_write_txn_retrying(db, user, &message, |conn, commit_id| {
        // Resolve fresh inside every (re)try — never reuse pre-conflict state.
        let resolved = resolve_alternative_any(conn, &name)?.ok_or_else(|| {
            DbError::Api(format!("erase: alternative '{name}' not found (no identity row)"))
        })?;

        // The m7a ordering belt: every named replacement must have a CURRENT
        // identity row, checked inside the same transaction that erases.
        for n in &expect {
            match crate::api::lookup_alternative(conn, n)? {
                None => {
                    return Err(DbError::Api(format!(
                        "erase refused: expect_present alternative '{n}' has no current row \
                         (rerun-overwrite ordering: commit the replacement run fully, \
                         then erase the old one — schema_interop.md §9)"
                    )))
                }
                Some(id) if id == resolved.id => {
                    return Err(DbError::Api(format!(
                        "erase refused: expect_present names the alternative being erased ('{n}')"
                    )))
                }
                Some(_) => {}
            }
        }

        // Step 2 — close the identity row (retained, never removed: D-E2).
        // The standard guarded UPDATE; erase opens its own commit, so every
        // pre-existing row has valid_from_commit < C (same-commit guard
        // inherited without a special case).
        let identity_closed = conn.execute(
            "UPDATE alternative SET valid_to_commit = ? \
             WHERE id = ? AND valid_from_commit < ? AND valid_to_commit IS NULL",
            params![commit_id, resolved.id, commit_id],
        )? > 0;

        // Step 3 — the erase execution arm: plain any-validity DELETEs over
        // the shared registry predicates. Bytes are estimated per leg just
        // before its DELETE (the rows are gone after).
        let mut per_table: Vec<TableCount> = Vec::new();
        let mut rows_total = 0u64;
        let mut bytes_estimate = 0u64;
        for leg in &alternative_erase_legs() {
            let bytes = estimate_leg_bytes(conn, leg, resolved.id)?;
            let deleted = conn.execute(
                &format!("DELETE FROM {} WHERE {}", leg.table, leg.predicate),
                params![resolved.id],
            )?;
            if deleted > 0 {
                bytes_estimate += bytes;
                rows_total += deleted as u64;
                per_table.push(TableCount {
                    table: leg.table.to_string(),
                    rows: deleted,
                });
            }
        }

        if !identity_closed && rows_total == 0 {
            // Nothing to do — roll the (empty) transaction back rather than
            // burn a commit row and a zero-row marker (which would extend the
            // as-of guard's refusal range for no content change).
            return Err(DbError::Api(NOOP_SENTINEL.into()));
        }

        // Step 4 — the marker, in the SAME transaction as the removal
        // (append-only, immutable; the only sanctioned trace). v1 writes
        // kind='alternative' only; no other kind has a writer.
        let marker_id: i64 = conn.query_row(
            "INSERT INTO erasure_marker \
               (kind, alternative_id, at_commit, rows_removed, bytes_estimate, note) \
             VALUES ('alternative', ?, ?, ?, ?, ?) RETURNING id",
            params![
                resolved.id,
                commit_id,
                rows_total as i64,
                bytes_estimate as i64,
                note.as_deref()
            ],
            |r| r.get(0),
        )?;

        Ok(EraseReport {
            erased: true,
            marker_id,
            commit_id,
            alternative_id: resolved.id,
            identity_closed,
            per_table,
            rows_total,
            bytes_estimate,
        })
    });

    match outcome {
        Err(DbError::Api(m)) if m == NOOP_SENTINEL => {
            // Resolve again (read-only) purely to report the id.
            let id = with_read(db, |conn| {
                Ok(resolve_alternative_any(conn, &name)?.map(|r| r.id).unwrap_or(0))
            })?;
            Ok(EraseReport {
                erased: false,
                alternative_id: id,
                ..EraseReport::default()
            })
        }
        other => other,
    }
}

/// Convenience chain (plan §4.2): [`erase_alternative`] then
/// [`crate::compaction::compact`]. Deliberately TWO separate operations, not
/// one fused one — erase is a fast ordinary write transaction, compaction is
/// a heavyweight exclusively-locked whole-file rewrite; their lock scopes
/// must not be merged. Crash between the two leaves an erased-but-unpacked
/// file: fully correct (marker present, current reads exact), just not yet
/// shrunk — the next compaction reclaims it. In the rerun-overwrite flow a
/// crash between the replacement run's commit and the erase leaves BOTH
/// results present, never neither (each is its own commit).
pub fn erase_alternative_and_compact(
    db: &Path,
    alt: &ObjectDeleteKey,
    opts: &EraseOpts,
    user: &str,
) -> Result<(EraseReport, crate::compaction::CompactionReport), DbError> {
    let erase = erase_alternative(db, alt, opts, user)?;
    let compaction = crate::compaction::compact(db)?;
    Ok((erase, compaction))
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Validate the erase key: v1 erases ALTERNATIVES only (plan §6 — other
/// marker kinds have no writer), addressed by their own name (no class, no
/// element tuple). Returns the name.
fn validate_erase_key(alt: &ObjectDeleteKey) -> Result<&str, DbError> {
    if alt.kind != ObjectKind::Alternative {
        return Err(DbError::Api(format!(
            "erase for {} is not implemented in v1 (the erasure_marker kind is reserved; \
             only erase-by-alternative exists — erasure_compaction_plan §6)",
            alt.kind.tag()
        )));
    }
    if alt.class.is_some() {
        return Err(DbError::Api(
            "an alternative is addressed by its own name; class must be None".into(),
        ));
    }
    match &alt.target {
        ObjectRef::Name(n) if !n.trim().is_empty() => Ok(n),
        ObjectRef::Name(_) => Err(DbError::Api("alternative name must be non-empty".into())),
        ObjectRef::Elements(_) => Err(DbError::Api(
            "an alternative is addressed by name, not an element tuple".into(),
        )),
    }
}

/// The Alternative erase legs from the one registry. Non-empty by
/// construction (the Alternative set is fully erase-flagged).
fn alternative_erase_legs() -> Vec<EraseLeg> {
    let legs = erase_scope_legs(ObjectKind::Alternative)
        .expect("alternative leg set exists");
    debug_assert!(!legs.is_empty(), "alternative erase scope must be non-empty");
    legs
}

/// A resolved alternative identity.
struct ResolvedAlt {
    id: i64,
    name: String,
    still_current: bool,
}

/// Resolve an alternative name to its identity id, across ANY validity: the
/// current row wins (case-insensitive, like every core resolver); with no
/// current row, the closed identity rows are searched — erase composes with a
/// prior `delete_objects` (D-E2 keeps the closed row resolvable). Because
/// `uq_alternative_name` constrains only CURRENT rows, a name may have been
/// reused across closed identities; that ambiguity is a loud error, never a
/// silent pick.
fn resolve_alternative_any(
    conn: &Connection,
    name: &str,
) -> Result<Option<ResolvedAlt>, DbError> {
    if let Some(id) = crate::api::lookup_alternative(conn, name)? {
        let stored: String = conn.query_row(
            "SELECT name FROM alternative WHERE cur_id = ?",
            params![id],
            |r| r.get(0),
        )?;
        return Ok(Some(ResolvedAlt {
            id,
            name: stored,
            still_current: true,
        }));
    }
    let mut stmt = conn.prepare(
        "SELECT DISTINCT id FROM alternative WHERE lower(name) = lower(?) ORDER BY id",
    )?;
    let ids: Vec<i64> = stmt
        .query_map(params![name], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    match ids.as_slice() {
        [] => Ok(None),
        [id] => {
            let stored: String = conn.query_row(
                "SELECT name FROM alternative WHERE id = ? ORDER BY version DESC LIMIT 1",
                params![id],
                |r| r.get(0),
            )?;
            Ok(Some(ResolvedAlt {
                id: *id,
                name: stored,
                still_current: false,
            }))
        }
        many => Err(DbError::Api(format!(
            "erase: name '{name}' matches {} closed alternative identities (ids {many:?}) — \
             the name was reused; erase by a disambiguated path is not supported in v1",
            many.len()
        ))),
    }
}

/// Advisory payload-bytes estimate for one leg's in-scope rows (ANY
/// validity), computed BEFORE their DELETE. Array tiers count cells exactly
/// (8 bytes each); the scalar tier counts a fixed row overhead plus string
/// payload; other tables use fixed per-row estimates. Deliberately rough —
/// DuckDB exposes no exact per-row physical size; the marker stores this as
/// `bytes_estimate` (advisory), and the acceptance criteria measure real
/// reclaim end-to-end via file sizes.
fn estimate_leg_bytes(conn: &Connection, leg: &EraseLeg, id: i64) -> Result<u64, DbError> {
    let sql = match leg.table {
        "value_array" | "value_array_hist" => format!(
            "SELECT COALESCE(SUM((COALESCE(len(values), 0) + COALESCE(len(values_int), 0)) * 8), 0) \
             FROM {} WHERE {}",
            leg.table, leg.predicate
        ),
        "value_scalar" => format!(
            "SELECT COALESCE(SUM(48 + COALESCE(length(v_str), 0)), 0) FROM {} WHERE {}",
            leg.table, leg.predicate
        ),
        // Long rows are wide (30 real columns); narrow link tables are small.
        "value_long" => format!(
            "SELECT count(*) * 96 FROM {} WHERE {}",
            leg.table, leg.predicate
        ),
        _ => format!("SELECT count(*) * 32 FROM {} WHERE {}", leg.table, leg.predicate),
    };
    let n: i64 = conn.query_row(&sql, params![id], |r| r.get(0))?;
    Ok(n.max(0) as u64)
}
