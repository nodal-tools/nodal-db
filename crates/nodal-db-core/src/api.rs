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

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{Array, StringArray, UInt32Array};
use duckdb::arrow::compute::{concat_batches, take_record_batch};
use duckdb::arrow::datatypes::{DataType, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, Connection};

use crate::axis::AxisCache;
use crate::canon::{canonicalize_indexed, index_domains, CanonicalValue, DomainIndex, LabelDomain};
use crate::conn::{
    head_on, with_read_named, with_read_plain, with_write_txn_retrying_ends, TxnEnd, TxnOptions,
};
use crate::error::{CoveredWriteDetails, DbError, WriteAndDeleteSameKeyDetails};
use crate::history::{
    self as history, keeps_content, resolve_axes, write_arrays, write_longs, write_scalars,
    ArrayWrite, DeleteWrite, LongWrite, ScalarWrite, ARRAY_ROUND_ELEMENTS, LONG_INSERT_CHUNK,
};
use crate::object_delete::{
    apply_object_delete, cascade_scope, prepare_object_deletes, preview_one, reaches_created,
    written_value_covered, PreparedObjectDelete, WrittenIds,
};
use crate::structural::{apply_structural, prepare_structural};
use crate::text::{nfc, Normalized};
use crate::types::{
    CoveredWrites, CoveringDelete, DeclaredType, IndexKind, ObjectDeleteKey, ObjectDeleteOpts,
    ObjectDeletePreview, ObjectDeleteResult, ObjectKind, ObjectRef, ShapedData, StructuralChange,
    StructuralEffect, StructuralKind, Tier, ValueKey, ValueKind, Warning, WarningKind,
    WriteOptions, WriteReport,
};

/// One ingest segment: a `(class, parameter, alternative)` target plus its
/// logical-long batches and label domains (`specs/arrow_ipc_bulk_format.md`
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
/// (`specs/arrow_ipc_bulk_format.md` §9.2, OPEN-Q3-6: whole-invocation
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
    commit_changes(
        db,
        segments,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        user,
        message,
        opts,
    )
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
/// splitting an editor save into two core calls would let a failed second call
/// leave the first half committed on its own (identical re-writes are skipped,
/// but a changed first half would still land without its deletes). Running both
/// halves in one closure makes the save atomic.
///
/// Segments are prepared (validated, deduped) up front, purely; deletes are
/// validated + deduped up front too. Then, inside one retried closure and under
/// one `commit_id`, the prepared writes run FIRST, then the `DeleteKey`s are
/// resolved (case-insensitive, lookup-only — never auto-created) and closed out
/// in one set-based round (`history::delete_values`). A missing referent or
/// already-absent value is a machine-readable [`Warning`] + skip, never a hard
/// error (delete design §2).
/// Non-empty `user`; empty across ALL channels ⇒ [`DbError::Api`].
///
/// `object_deletes` (object_delete plan §2) join the SAME atomic commit, applied
/// AFTER value writes and value deletes: a coord closed by a staged value delete
/// is no longer live when an object cascade runs (not double-closed, not
/// double-counted — D5), and a later phase's class delete can compose over
/// already-settled value state. Each is resolved lookup-only and closed under
/// the `valid_from < commit_id` guard; per-request outcomes land in
/// `report.object_deletes`.
///
/// `structural` (structural_write_api_plan §2/§4) is the fourth channel, applied
/// FIRST inside the retried closure (structural → segments → deletes →
/// object_deletes): a same-save value write / rank / activation may target a
/// referent an earlier structural item just created, and items apply
/// sequentially against the one open txn, so create → use resolves. Pure,
/// name-level guards (§4.5) reject caller-contract problems (one set-ranks per
/// scenario, contradictory renames, the cross-channel object-delete orphan)
/// before any DB work; per-item outcomes land in `report.structural`.
/// Empty across every channel ⇒ [`DbError::Api`].
///
/// **Unchanged values and no-op calls** (skip_unchanged_writes_PLAN D1,
/// D2): a value equal to the stored one is not rewritten (it counts in
/// `report.values_unchanged`, not `values_written`). A call that changes
/// nothing — every value unchanged, every delete a miss, every structural
/// item `NoOp`, every object delete `NotFound`, nothing auto-created —
/// makes **no commit**: the transaction is rolled back, the report says
/// `commit_id == 0`, the head (the polling cookie) does not move and the
/// commit message is not recorded. Its `seq_commit` value stays consumed
/// (commit ids may skip, as after a conflict retry). A value delete of a
/// key the same call writes is refused up front as
/// [`DbError::WriteAndDeleteSameKey`], whatever the content (D9), and so is
/// a value write that an object delete in the same call covers — its
/// entity (or a container of it), parameter definition, alternative or
/// class — as [`DbError::CoveredWrite`], whatever the content, the tier or
/// the delete's `cascade` option (D10).
///
/// **Covered writes under `DeleteWins`.** With
/// `opts.covered_writes == CoveredWrites::DeleteWins` neither case is
/// refused: the delete wins. Inside the transaction, after the structural
/// changes and before the first segment, the deletes are resolved
/// lookup-only (`DropPlan`) and every write they cover is dropped before
/// it is resolved, canonicalized or buffered (so a dropped write
/// auto-creates nothing), each reported as a
/// [`WarningKind::DroppedByDelete`] warning. The deletes then run as usual
/// and the uncovered writes proceed normally, and no covered value can be
/// left partly closed by a cascade. The delete wins only when it takes
/// effect: after the deletes ran, every dropped write needs a covering
/// delete that did (a value delete; an object delete that ended `Deleted`
/// or whose target did not exist), else the whole call is refused as
/// [`DbError::Api`] and rolled back (a RESTRICT delete that ended
/// `Restricted` would otherwise lose the write while the object survives).
/// A dropped write is not a write: a call whose writes are all dropped and
/// whose deletes delete nothing makes no commit.
///
/// Under either policy, an object delete that reaches an object a
/// structural change of the same call creates (or a child of it) is
/// refused: by name before the transaction (`prepare_structural`), and on
/// the resolved ids after the structural changes ran.
#[allow(clippy::too_many_arguments)]
pub fn commit_changes(
    db: &Path,
    mut segments: Vec<Segment>,
    mut structural: Vec<StructuralChange>,
    mut deletes: Vec<DeleteKey>,
    mut object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    // Every commit must carry a real author; message is the caller's choice
    // (may be empty) — jkiviluo 2026-08-18.
    if user.trim().is_empty() {
        return Err(DbError::Api("user must be non-empty".into()));
    }
    if segments.is_empty()
        && structural.is_empty()
        && deletes.is_empty()
        && object_deletes.is_empty()
    {
        return Err(DbError::Api(
            "nothing to do: no segments, no structural changes, no deletes, no object deletes"
                .into(),
        ));
    }
    // The text rule for names (`text.rs`): every name of the call in NFC
    // before any check, lookup or store, so NFC-equal spellings are one
    // name everywhere below. Names that create or change something are
    // reported (one warning per segment, one for the structural changes);
    // delete keys only look names up and are normalized silently.
    let mut segment_names: Vec<Normalized> = Vec::with_capacity(segments.len());
    for s in &mut segments {
        let mut n = Normalized::default();
        n.nfc_in_place(&mut s.class);
        n.nfc_in_place(&mut s.parameter);
        n.nfc_in_place(&mut s.alternative);
        segment_names.push(n);
    }
    let mut structural_names = Normalized::default();
    for ch in &mut structural {
        nfc_structural_names(ch, &mut structural_names);
    }
    let structural_warning = structural_names.warning("name", "structural", "");
    let mut lookups = Normalized::default();
    for k in &mut deletes {
        for s in [
            &mut k.class,
            &mut k.parameter,
            &mut k.alternative,
            &mut k.entity,
        ] {
            lookups.nfc_in_place(s);
        }
    }
    for (k, _) in &mut object_deletes {
        nfc_object_delete_key(k, &mut lookups);
    }
    // Two same-commit writes to one (class, parameter, alternative) key have no
    // defined history semantics (spec §4.2, E7 class); compare by the same
    // `lower()` fold every core name resolver applies. DuckDB `lower()` is
    // Unicode, and alternative names carry no ASCII charset CHECK, so the fold
    // must be Unicode too — an ASCII fold lets Ä/ä-aliased targets resolve to
    // one row and die as a raw CHECK error mid-transaction (critique F3).
    // Rust's `to_lowercase` still differs from DuckDB's `lower()` on a few
    // letters (final sigma, dotted capital I), so `write_prepared` repeats
    // the check on the resolved ids; this one fails early with no DB work.
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
        .zip(segment_names)
        .enumerate()
        .map(|(i, (s, names))| {
            prepare_segment(s, names).map_err(|e| {
                if multi {
                    // Name the failing segment (spec §9.1: errors name the
                    // segment by ordinal and class:parameter).
                    DbError::Api(format!(
                        "segment {} ({}:{}): {e}",
                        i + 1,
                        s.class,
                        s.parameter
                    ))
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
    // Structural changes: pure, name-level guards (§4.5) — per-change name/
    // charset/type validation, one-set-per-scenario (M3), contradictory renames,
    // and the cross-channel object-delete orphan guard (M2). No DB touched.
    prepare_structural(&structural, &object_deletes)?;
    // A value written and deleted in one call: rejected whatever the
    // content (D9); likewise a value written while an object delete in
    // the same call covers it (D10).
    // Under `CoveredWrites::DeleteWins` the covered writes are dropped in
    // the transaction instead (`DropPlan`).
    let delete_wins = opts.covered_writes == CoveredWrites::DeleteWins;
    if !delete_wins {
        reject_write_and_delete(&prepared, &prepared_deletes)?;
        reject_write_and_object_delete(&prepared, &prepared_object_deletes)?;
    }

    // --- The atomic, retried commit. Everything below re-reads/re-creates DB
    // state inside a fresh transaction on each retry (VMP Part 5), so the axis
    // cache is built HERE, not outside (a rollback must not leave stale ids). ---
    // `opts.expect_commit` is the transaction's staleness precondition: the
    // head is checked after BEGIN and before the commit row, on every
    // conflict-retry attempt (save_path_open_reduction plan §2.1).
    let txn = TxnOptions {
        expect_commit: opts.expect_commit,
    };
    crate::read_surface::clear_write_warning();
    let mut report = with_write_txn_retrying_ends(db, user, message, txn, |conn, commit_id| {
        // One axis cache across all segments: axes are immutable and
        // content-verified, so cross-segment reuse within one txn is sound.
        let mut cache = AxisCache::new();
        let mut report = WriteReport {
            commit_id,
            ..Default::default()
        };
        report.warnings.extend(structural_warning.clone());
        // The commit's value writes, deferred to set-based writes per tier
        // (set_based_scalar_write_PLAN D1, set_based_long_and_delete_PLAN
        // D2): each buffer is flushed whenever it reaches one round and after
        // the last segment. A closure local, so a conflict retry rebuilds it
        // from scratch.
        let mut buffers = WriteBuffers::default();
        let mut entities = EntityCache::new();
        // The class / parameter / alternative resolutions of this attempt,
        // keyed by exact spelling (set_based_long_and_delete_PLAN §10 C11).
        // A closure local like the buffers: a conflict retry rebuilds it,
        // so no id read or minted in a rolled-back attempt is reused.
        let mut resolved_names = ResolveCache::new();
        // The resolved (parameter definition, alternative) of every segment
        // so far: the up-front duplicate-target check folds names in Rust,
        // which can disagree with DuckDB's `lower()` (final sigma, dotted
        // capital I), so two targets it keeps apart may resolve to one key
        // set. The deferred writes rely on distinct targets.
        let mut targets: HashSet<(i64, i64)> = HashSet::new();
        // Structural changes run FIRST (§4.1): a value write / rank / activation
        // later in this same commit may target a referent an earlier structural
        // item just created, and items apply sequentially against the one open
        // txn, so create → use resolves. Caller-given vec order is honored.
        for (i, ch) in structural.iter().enumerate() {
            apply_structural(conn, commit_id, i, ch, &mut report)?;
        }
        // An object delete may not reach an object a structural change of
        // this call created (under either policy): the same-commit guard
        // would leave it live. The resolved-id repeat of the name-level
        // guard in `prepare_structural`; it also sees cascade children.
        let created: HashSet<(ObjectKind, i64)> = report
            .structural
            .iter()
            .filter(|o| o.effect == StructuralEffect::Created)
            .filter_map(|o| {
                let kind = match o.kind {
                    StructuralKind::Alternative => ObjectKind::Alternative,
                    StructuralKind::Scenario => ObjectKind::Scenario,
                    StructuralKind::Entity => ObjectKind::Entity,
                    StructuralKind::EntityClass => ObjectKind::Class,
                    StructuralKind::Parameter => ObjectKind::Parameter,
                    _ => return None,
                };
                Some((kind, o.object_id))
            })
            .collect();
        for p in &prepared_object_deletes {
            if let Some((kind, id)) = reaches_created(conn, &p.key, &created)? {
                return Err(DbError::Api(format!(
                    "object-delete key {} ({} {}) reaches the {} id {id} that a \
                     structural change of this call creates: create and delete an \
                     object in separate calls",
                    p.key_index,
                    p.key.kind.tag(),
                    object_target_label(&p.key),
                    kind.tag()
                )));
            }
        }
        // `DeleteWins`: what the deletes cover, resolved after the
        // structural changes (a delete may name a referent one of them
        // created or renamed) and before any write.
        let plan = if delete_wins
            && !prepared.is_empty()
            && !(prepared_deletes.is_empty() && prepared_object_deletes.is_empty())
        {
            Some(DropPlan::build(
                conn,
                &prepared,
                &prepared_deletes,
                &prepared_object_deletes,
            )?)
        } else {
            None
        };
        // `DeleteWins`: the dropped writes, checked after the deletes ran.
        let mut dropped: Vec<Dropped> = Vec::new();
        // The index in `report.warnings` of each dropped write's warning.
        let mut dropped_at: Vec<usize> = Vec::new();
        for (i, p) in prepared.iter().enumerate() {
            let kept;
            let p = match &plan {
                None => p,
                Some(plan) => {
                    let covered = plan.covered(conn, p)?;
                    for (j, by) in &covered {
                        // Provisional `by`: re-pointed to a delete that took
                        // effect once the deletes ran (`check_effective`).
                        dropped_at.push(report.warnings.len());
                        report.warnings.push(dropped_warning(i, p, *j, by[0]));
                    }
                    let drop: Vec<usize> = covered.iter().map(|(j, _)| *j).collect();
                    dropped.extend(covered.into_iter().map(|(j, by)| (i, j, by)));
                    if drop.is_empty() {
                        p
                    } else if drop.len() == p.names.len() {
                        // Every write of the segment dropped: it resolves
                        // (and auto-creates) nothing.
                        continue;
                    } else {
                        kept = p.without(&drop);
                        &kept
                    }
                }
            };
            write_prepared(
                conn,
                commit_id,
                p,
                opts,
                &mut cache,
                &mut report,
                &mut buffers,
                &mut entities,
                &mut targets,
                &mut resolved_names,
            )
            .map_err(|e| {
                // Name the failing segment (spec §9.1) — but never rewrap a raw
                // DuckDB error: the conflict-retry loop keys on its variant
                // (`conn::is_conflict`), and an Api wrapper would break retry.
                // An Internal error (a core invariant, e.g. from a scalar flush
                // that ran mid-segment) keeps its variant too.
                if multi && !matches!(e, DbError::Duckdb(_) | DbError::Internal(_)) {
                    DbError::Api(format!(
                        "segment {} ({}:{}): {e}",
                        i + 1,
                        p.class,
                        p.parameter
                    ))
                } else {
                    e
                }
            })?;
        }
        // The remaining deferred writes, before any delete reads value rows
        // (deletes and object deletes run after every write, as before).
        buffers.flush(conn, commit_id, &mut report)?;
        if test_hooks::take_injected_conflict() {
            // A DuckDB error carrying the `Conflict` substring the retry
            // loop keys on (`conn::is_conflict`).
            conn.execute_batch("SELECT error('Conflict (test-injected)')")?;
        }
        // Deletes resolve + close out AFTER the writes, under the same commit —
        // one atomic invocation (delete design MAJOR-2).
        let deleted = apply_deletes(
            conn,
            commit_id,
            &prepared_deletes,
            opts,
            &buffers.keys,
            &mut report,
        )?;
        // D10 on the resolved ids, before any object closes: no object
        // delete may cover a value this call wrote (the up-front check
        // folds names in Rust and cannot see cascade containers).
        let written_ids = WrittenIds::new(&buffers.keys);
        for p in &prepared_object_deletes {
            if let Some(k) = written_value_covered(conn, &p.key, &written_ids)? {
                return Err(object_delete_covers_write_error(
                    p.key_index,
                    &p.key,
                    Some(k),
                ));
            }
        }
        // Object deletes run LAST, under the same commit_id (object_delete plan
        // §2): every close-out is guarded by `valid_from < commit_id`, so a row
        // this commit created is never closed (would trip the span CHECK).
        for p in &prepared_object_deletes {
            apply_object_delete(conn, commit_id, p, &mut report)?;
        }
        // `DeleteWins`: a write is dropped only for a delete that took
        // effect; otherwise the whole call is refused (rolled back). Each
        // warning's `by` then names a covering delete that took effect.
        if let Some(plan) = &plan {
            let effective = plan.check_effective(&dropped, &prepared, &report)?;
            for (&w, by) in dropped_at.iter().zip(effective) {
                if let WarningKind::DroppedByDelete { by: b, .. } = &mut report.warnings[w].kind {
                    *b = by;
                }
            }
        }
        // `tiers` lists the tiers written; the canonicalization noted every
        // value's tier, unchanged ones included.
        report.tiers.retain(|t| buffers.written_tiers.contains(t));
        let evidence = NoopEvidence {
            values: buffers.values,
            deleted,
            structural: structural.len(),
            object_deletes: prepared_object_deletes.len(),
        };
        if wrote_nothing(&report, &evidence) {
            // Nothing changed: no commit (D1). The report says so with
            // commit id 0.
            report.commit_id = 0;
            return Ok((report, TxnEnd::Rollback));
        }
        Ok((report, TxnEnd::Commit))
    })?;
    // A failed refresh of an older read surface before this write (the
    // write itself went through): `read_surface::refresh_before_write`.
    report.read_surface_warning = crate::read_surface::take_write_warning();
    Ok(report)
}

/// The names of one structural change in NFC (the text rule, `text.rs`).
/// Descriptions are free text and stay as written; so do the dimension
/// names inside a parameter's type declarations.
fn nfc_structural_names(ch: &mut StructuralChange, n: &mut Normalized) {
    match ch {
        StructuralChange::CreateAlternative { name, .. }
        | StructuralChange::CreateScenario { name, .. } => n.nfc_in_place(name),
        StructuralChange::RenameAlternative {
            current, new_name, ..
        }
        | StructuralChange::RenameScenario {
            current, new_name, ..
        } => {
            n.nfc_in_place(current);
            n.nfc_in_place(new_name);
        }
        StructuralChange::SetScenarioAlternatives {
            scenario,
            alternatives,
        } => {
            n.nfc_in_place(scenario);
            alternatives.iter_mut().for_each(|a| n.nfc_in_place(a));
        }
        StructuralChange::SetEntityActivation {
            class,
            entity,
            alternative,
            ..
        } => {
            n.nfc_in_place(class);
            n.nfc_in_place(entity);
            n.nfc_in_place(alternative);
        }
        StructuralChange::CreateEntity {
            class,
            name,
            elements,
        } => {
            n.nfc_in_place(class);
            if let Some(name) = name {
                n.nfc_in_place(name);
            }
            elements.iter_mut().for_each(|e| n.nfc_in_place(e));
        }
        StructuralChange::CreateEntityClass { name, dims } => {
            n.nfc_in_place(name);
            dims.iter_mut().for_each(|d| n.nfc_in_place(d));
        }
        StructuralChange::CreateParameter {
            class,
            name,
            value_list,
            ..
        } => {
            n.nfc_in_place(class);
            n.nfc_in_place(name);
            if let Some(list) = value_list {
                n.nfc_in_place(list);
            }
        }
        StructuralChange::SetMembership {
            group,
            member,
            alternative,
            ..
        } => {
            for addr in [group, member] {
                n.nfc_in_place(&mut addr.class);
                n.nfc_in_place(&mut addr.name);
            }
            if let Some(a) = alternative {
                n.nfc_in_place(a);
            }
        }
    }
}

/// The names of one object-delete key in NFC (the text rule, `text.rs`).
fn nfc_object_delete_key(k: &mut ObjectDeleteKey, n: &mut Normalized) {
    if let Some(class) = &mut k.class {
        n.nfc_in_place(class);
    }
    match &mut k.target {
        ObjectRef::Name(name) => n.nfc_in_place(name),
        ObjectRef::Elements(els) => els.iter_mut().for_each(|e| n.nfc_in_place(e)),
    }
}

/// What a `commit_changes` attempt submitted and what its deletes did, for
/// [`wrote_nothing`].
struct NoopEvidence {
    /// Values buffered (every one must have come back unchanged).
    values: usize,
    /// Value delete keys that closed at least one row.
    deleted: usize,
    /// Structural items submitted (each must report `NoOp`).
    structural: usize,
    /// Object deletes submitted (each must report `NotFound`).
    object_deletes: usize,
}

/// Did this call provably change nothing (skip_unchanged_writes_PLAN D1,
/// critique C2)? A false "nothing" would roll back real writes, so this is
/// an allow-list: `true` only when every item of every channel positively
/// reported a known no-op — every value unchanged, no delete closed a row,
/// every structural item `NoOp`, every object delete `NotFound`, no axis
/// created, no row written or closed, and no warning outside the known
/// no-op kinds (an `AutoCreated` referent is a write). Anything else,
/// including a report field or warning kind added later (the destructuring
/// and the `match` below are exhaustive, so adding one does not compile
/// until it is classified here), commits.
fn wrote_nothing(report: &WriteReport, ev: &NoopEvidence) -> bool {
    let WriteReport {
        commit_id: _,
        values_written,
        values_unchanged,
        rows_written,
        rows_closed,
        axes_created,
        axes_reused: _,
        tiers,
        warnings,
        object_deletes,
        structural,
        // Not a write: the read-surface refresh runs in its own transaction.
        read_surface_warning: _,
    } = report;
    let warnings_ok = warnings.iter().all(|w| match w.kind {
        WarningKind::AutoCreated => false,
        // Normalizing a name or label is not a write by itself.
        WarningKind::TimezoneNormalized { .. }
        | WarningKind::TextNormalized { .. }
        | WarningKind::UnknownReferent { .. }
        | WarningKind::NothingToDelete { .. }
        // A dropped write is not a write (CoveredWrites::DeleteWins).
        | WarningKind::DroppedByDelete { .. } => true,
    });
    *values_written == 0
        && *values_unchanged == ev.values
        && *rows_written == 0
        && *rows_closed == 0
        && *axes_created == 0
        && tiers.is_empty()
        && ev.deleted == 0
        && warnings_ok
        && structural.len() == ev.structural
        && structural
            .iter()
            .all(|s| s.effect == StructuralEffect::NoOp)
        && object_deletes.len() == ev.object_deletes
        && object_deletes
            .iter()
            .all(|o| matches!(o.result, ObjectDeleteResult::NotFound))
}

/// D9 (critique C1): a call that writes a value and also deletes it is
/// rejected up front, whatever the content. Deletes run after the writes,
/// so the delete would close the row the write just inserted (a span
/// CHECK error) when the write changed the value, but succeed when the
/// write was an unchanged no-op: the outcome would depend on the content.
/// This pure check compares Rust folds of the names; `apply_deletes`
/// repeats it on the resolved ids.
fn reject_write_and_delete(
    prepared: &[PreparedSegment],
    deletes: &[(usize, DeleteKey)],
) -> Result<(), DbError> {
    if deletes.is_empty() {
        return Ok(());
    }
    let mut written: HashSet<(String, String, String, String)> = HashSet::new();
    for p in prepared {
        let (c, pa, a) = (
            p.class.to_lowercase(),
            p.parameter.to_lowercase(),
            p.alternative.to_lowercase(),
        );
        for n in &p.names {
            written.insert((c.clone(), pa.clone(), a.clone(), n.to_lowercase()));
        }
    }
    for (i, k) in deletes {
        let fold = (
            k.class.to_lowercase(),
            k.parameter.to_lowercase(),
            k.alternative.to_lowercase(),
            k.entity.to_lowercase(),
        );
        if written.contains(&fold) {
            return Err(same_key_error(*i, k, None));
        }
    }
    Ok(())
}

/// D10 (P2 review): a call that writes a value and also deletes an object
/// whose cascade covers it (its entity, a container entity of it, its
/// parameter definition, its alternative, or a class above them) is
/// rejected up front, whatever the content, the tier or the request's
/// `cascade` option. The cascade closes only rows older than the commit
/// (`valid_from < commit_id`), so under the long cell diff it would close
/// the unchanged old cells and keep the changed ones the write inserted: a
/// partial value under a closed object. This pure check compares Rust
/// folds of the names the request spells out; the commit repeats it on the
/// resolved ids over the full cascade closure
/// (`object_delete::written_value_covered`).
fn reject_write_and_object_delete(
    prepared: &[PreparedSegment],
    object_deletes: &[PreparedObjectDelete],
) -> Result<(), DbError> {
    if prepared.is_empty() || object_deletes.is_empty() {
        return Ok(());
    }
    // Fold every written name once (sets, not a scan per object delete: a
    // call may carry many entity deletes beside many written entities).
    let mut alts: HashSet<String> = HashSet::new();
    let mut classes: HashSet<String> = HashSet::new();
    let mut params: HashSet<(String, String)> = HashSet::new();
    let mut entities: HashSet<(String, String)> = HashSet::new();
    for p in prepared {
        let c = p.class.to_lowercase();
        alts.insert(p.alternative.to_lowercase());
        params.insert((c.clone(), p.parameter.to_lowercase()));
        for n in &p.names {
            entities.insert((c.clone(), n.to_lowercase()));
        }
        classes.insert(c);
    }
    for od in object_deletes {
        let name = match &od.key.target {
            ObjectRef::Name(n) => n.to_lowercase(),
            // An element tuple resolves only in the DB (the commit-time check).
            ObjectRef::Elements(_) => continue,
        };
        let class = od.key.class.as_deref().map(str::to_lowercase);
        let covered = match (od.key.kind, class) {
            (ObjectKind::Alternative, _) => alts.contains(&name),
            (ObjectKind::Class, _) => classes.contains(&name),
            (ObjectKind::Parameter, Some(c)) => params.contains(&(c, name)),
            (ObjectKind::Entity, Some(c)) => entities.contains(&(c, name)),
            _ => false,
        };
        if covered {
            return Err(object_delete_covers_write_error(
                od.key_index,
                &od.key,
                None,
            ));
        }
    }
    Ok(())
}

/// An object-delete key's target as written: its name or element tuple.
fn object_target_label(k: &ObjectDeleteKey) -> String {
    match &k.target {
        ObjectRef::Name(n) => n.clone(),
        ObjectRef::Elements(es) => format!("({})", es.join(", ")),
    }
}

/// The [`DbError::CoveredWrite`] (D10) refusal; `ids` names the covered
/// (entity, parameter definition, alternative) when the commit-time check
/// found it.
fn object_delete_covers_write_error(
    i: usize,
    k: &ObjectDeleteKey,
    ids: Option<(i64, i64, i64)>,
) -> DbError {
    let target = object_target_label(k);
    let class = k
        .class
        .as_deref()
        .map(|c| format!(" in class {c}"))
        .unwrap_or_default();
    let which = match ids {
        Some((e, p, a)) => {
            format!(" (entity id {e}, parameter definition id {p}, alternative id {a})")
        }
        None => String::new(),
    };
    let message = format!(
        "object-delete key {i} ({} {target}{class}) covers a value this call also \
         writes{which}: a value cannot be written and its object deleted in one call",
        k.kind.tag()
    );
    DbError::CoveredWrite(Box::new(CoveredWriteDetails {
        key_index: i,
        kind: k.kind,
        target,
        class: k.class.clone(),
        resolved: ids,
        message,
    }))
}

/// The writes a call's deletes cover, for [`CoveredWrites::DeleteWins`]:
/// built inside the transaction after the structural changes and before
/// the first segment, from the deletes alone (lookup-only, nothing is
/// created). A write is covered when
///   - a value delete names the same key, compared by DuckDB's `lower()`
///     fold of all four names, which is how both sides resolve (D9); or
///   - an object delete names its alternative, class, (class, parameter) or
///     (class, entity), compared by the same fold: this catches a referent
///     that does not exist yet and that the write would auto-create; or
///   - the resolved entity, parameter definition or alternative of the
///     write lies in an object delete's full cascade closure
///     ([`cascade_scope`], whatever `cascade` says), which adds containers,
///     element-tuple targets and a class's children (D10).
///
/// Each covered key collects EVERY covering delete: the drop stands only if
/// at least one of them takes effect ([`DropPlan::check_effective`], after
/// the deletes ran), else the call is refused.
/// The commit-time D9/D10 checks still run after the writes as a backstop;
/// with every covered write dropped they find nothing.
struct DropPlan {
    /// DuckDB folds of every segment and delete name (exact spelling).
    folds: HashMap<String, String>,
    /// Value deletes by folded (class, parameter, alternative, entity).
    value_keys: HashMap<(String, String, String, String), Vec<usize>>,
    /// The folded (class, parameter, alternative) of every value delete.
    value_targets: HashSet<(String, String, String)>,
    /// Named object deletes by folded name: alternatives, classes,
    /// (class, parameter) and (class, entity).
    alts: HashMap<String, Vec<usize>>,
    classes: HashMap<String, Vec<usize>>,
    params: HashMap<(String, String), Vec<usize>>,
    entities: HashMap<(String, String), Vec<usize>>,
    /// The folded classes of the named entity deletes.
    entity_classes: HashSet<String>,
    /// The ids in the cascade closures, by kind.
    entity_ids: HashMap<i64, Vec<usize>>,
    param_ids: HashMap<i64, Vec<usize>>,
    alt_ids: HashMap<i64, Vec<usize>>,
    /// The object deletes (caller indices) whose target did not resolve when
    /// the plan was built: nothing of theirs exists, so a write they cover
    /// by name is dropped with nothing left behind.
    absent: HashSet<usize>,
}

/// One dropped write: (segment index, name index, every covering delete,
/// sorted, value deletes first).
type Dropped = (usize, usize, Vec<CoveringDelete>);

impl DropPlan {
    fn build(
        conn: &Connection,
        prepared: &[PreparedSegment],
        deletes: &[(usize, DeleteKey)],
        object_deletes: &[PreparedObjectDelete],
    ) -> Result<Self, DbError> {
        // Every name the plan compares, folded in one statement.
        let mut all: Vec<&str> = Vec::new();
        for p in prepared {
            all.extend([p.class.as_str(), &p.parameter, &p.alternative]);
        }
        for (_, k) in deletes {
            all.extend([k.class.as_str(), &k.parameter, &k.alternative, &k.entity]);
        }
        for od in object_deletes {
            all.extend(od.key.class.as_deref());
            if let ObjectRef::Name(n) = &od.key.target {
                all.push(n);
            }
        }
        let mut listed: HashSet<&str> = HashSet::new();
        let names: Vec<&str> = all.into_iter().filter(|n| listed.insert(n)).collect();
        let folds: HashMap<String, String> = names
            .iter()
            .map(|n| n.to_string())
            .zip(duck_folds(conn, &names)?)
            .collect();
        let f = |n: &str| folds[n].clone();
        let mut plan = DropPlan {
            folds: HashMap::new(),
            value_keys: HashMap::new(),
            value_targets: HashSet::new(),
            alts: HashMap::new(),
            classes: HashMap::new(),
            params: HashMap::new(),
            entities: HashMap::new(),
            entity_classes: HashSet::new(),
            entity_ids: HashMap::new(),
            param_ids: HashMap::new(),
            alt_ids: HashMap::new(),
            absent: HashSet::new(),
        };
        for (i, k) in deletes {
            let (c, p, a) = (f(&k.class), f(&k.parameter), f(&k.alternative));
            plan.value_targets.insert((c.clone(), p.clone(), a.clone()));
            plan.value_keys
                .entry((c, p, a, f(&k.entity)))
                .or_default()
                .push(*i);
        }
        for od in object_deletes {
            let i = od.key_index;
            if let ObjectRef::Name(n) = &od.key.target {
                let n = f(n);
                match (od.key.kind, od.key.class.as_deref().map(f)) {
                    (ObjectKind::Alternative, _) => plan.alts.entry(n).or_default().push(i),
                    (ObjectKind::Class, _) => plan.classes.entry(n).or_default().push(i),
                    (ObjectKind::Parameter, Some(c)) => {
                        plan.params.entry((c, n)).or_default().push(i)
                    }
                    (ObjectKind::Entity, Some(c)) => {
                        plan.entity_classes.insert(c.clone());
                        plan.entities.entry((c, n)).or_default().push(i);
                    }
                    _ => {}
                }
            }
            let Some(scope) = cascade_scope(conn, &od.key)? else {
                plan.absent.insert(i);
                continue;
            };
            for (k, id) in scope {
                let ids = match k {
                    ObjectKind::Entity => &mut plan.entity_ids,
                    ObjectKind::Parameter => &mut plan.param_ids,
                    ObjectKind::Alternative => &mut plan.alt_ids,
                    // A class reaches its values only through its child
                    // entities and parameters (both in the closure).
                    _ => continue,
                };
                ids.entry(id).or_default().push(i);
            }
        }
        plan.folds = folds;
        Ok(plan)
    }

    /// The names of segment `p` (indices into `p.names`, ascending) that the
    /// deletes cover, each with EVERY covering delete (sorted, deduped,
    /// value deletes first). Lookup-only.
    fn covered(
        &self,
        conn: &Connection,
        p: &PreparedSegment,
    ) -> Result<Vec<(usize, Vec<CoveringDelete>)>, DbError> {
        let obj = |v: &Vec<usize>| {
            v.iter()
                .map(|&i| CoveringDelete::Object { key_index: i })
                .collect::<Vec<_>>()
        };
        let (c, pa, a) = (
            &self.folds[&p.class],
            &self.folds[&p.parameter],
            &self.folds[&p.alternative],
        );
        // The whole segment: its alternative, class or parameter is deleted.
        let mut whole: Vec<CoveringDelete> = Vec::new();
        for v in [
            self.alts.get(a),
            self.classes.get(c),
            self.params.get(&(c.clone(), pa.clone())),
        ]
        .into_iter()
        .flatten()
        {
            whole.extend(obj(v));
        }
        let class_id = lookup_entity_class(conn, &p.class)?;
        if !(self.param_ids.is_empty() && self.alt_ids.is_empty()) {
            let pd = match class_id {
                Some(cid) => lookup_parameter(conn, cid, &p.parameter)?,
                None => None,
            };
            let alt = lookup_alternative(conn, &p.alternative)?;
            if let Some(v) = pd.and_then(|id| self.param_ids.get(&id)) {
                whole.extend(obj(v));
            }
            if let Some(v) = alt.and_then(|id| self.alt_ids.get(&id)) {
                whole.extend(obj(v));
            }
        }
        // Entity by entity: a value delete of the key, a named entity
        // delete, or an entity in a cascade closure.
        let by_value = self
            .value_targets
            .contains(&(c.clone(), pa.clone(), a.clone()));
        let by_name = self.entity_classes.contains(c);
        let by_id = class_id.is_some() && !self.entity_ids.is_empty();
        let mut out = Vec::new();
        if !(by_value || by_name || by_id) {
            if !whole.is_empty() {
                whole.sort();
                whole.dedup();
                out.extend((0..p.names.len()).map(|i| (i, whole.clone())));
            }
            return Ok(out);
        }
        let names: Vec<&str> = p.names.iter().map(String::as_str).collect();
        let looked: Vec<(String, Option<i64>)> = match class_id {
            Some(cid) => lookup_entities(conn, cid, &names)?,
            None => duck_folds(conn, &names)?
                .into_iter()
                .map(|f| (f, None))
                .collect(),
        };
        for (i, (fe, id)) in looked.into_iter().enumerate() {
            let mut by = whole.clone();
            if by_value {
                if let Some(v) =
                    self.value_keys
                        .get(&(c.clone(), pa.clone(), a.clone(), fe.clone()))
                {
                    by.extend(v.iter().map(|&k| CoveringDelete::Value { key_index: k }));
                }
            }
            if by_name {
                if let Some(v) = self.entities.get(&(c.clone(), fe.clone())) {
                    by.extend(obj(v));
                }
            }
            if let Some(v) = id.and_then(|id| self.entity_ids.get(&id)) {
                by.extend(obj(v));
            }
            if !by.is_empty() {
                by.sort();
                by.dedup();
                out.push((i, by));
            }
        }
        Ok(out)
    }

    /// "Delete wins" only when the delete takes effect: every dropped write
    /// needs at least one covering delete that did — a value delete (the
    /// key ends absent either way), or an object delete that ended
    /// `Deleted` or whose target did not exist when the plan was built. A
    /// RESTRICT delete that ended `Restricted`, or one that found nothing to
    /// close although its target existed, leaves the object live: dropping
    /// the write would lose it silently, so the whole call is refused
    /// (rolled back, nothing committed). On success, returns for each
    /// dropped write (in order) its first covering delete that took effect:
    /// a value delete if any, else the lowest-index effective object delete.
    fn check_effective(
        &self,
        dropped: &[Dropped],
        prepared: &[PreparedSegment],
        report: &WriteReport,
    ) -> Result<Vec<CoveringDelete>, DbError> {
        let deleted: HashSet<usize> = report
            .object_deletes
            .iter()
            .filter(|o| matches!(o.result, ObjectDeleteResult::Deleted { .. }))
            .map(|o| o.key_index)
            .collect();
        let effective = |d: &CoveringDelete| match *d {
            CoveringDelete::Value { .. } => true,
            CoveringDelete::Object { key_index } => {
                deleted.contains(&key_index) || self.absent.contains(&key_index)
            }
        };
        let lost: Vec<&Dropped> = dropped
            .iter()
            .filter(|(_, _, by)| !by.iter().any(effective))
            .collect();
        if lost.is_empty() {
            // `by` is sorted, value deletes first, then object deletes by
            // index; every list holds an effective delete.
            return Ok(dropped
                .iter()
                .filter_map(|(_, _, by)| by.iter().copied().find(|d| effective(d)))
                .collect());
        }
        let outcome = |k: usize| {
            report
                .object_deletes
                .iter()
                .find(|o| o.key_index == k)
                .map(|o| match o.result {
                    ObjectDeleteResult::Deleted { .. } => "deleted",
                    ObjectDeleteResult::Restricted { .. } => "restricted",
                    ObjectDeleteResult::NotFound => "not found",
                })
                .unwrap_or("not run")
        };
        let mut lines: Vec<String> = Vec::new();
        for (s, j, by) in lost.iter().take(5) {
            let p = &prepared[*s];
            let deletes: Vec<String> = by
                .iter()
                .map(|d| match *d {
                    CoveringDelete::Value { key_index } => format!("delete key {key_index}"),
                    CoveringDelete::Object { key_index } => {
                        format!("object-delete key {key_index} ({})", outcome(key_index))
                    }
                })
                .collect();
            lines.push(format!(
                "{}:{}/{}/{} (segment {s}; covered by {})",
                p.class,
                p.parameter,
                p.alternative,
                p.names[*j],
                deletes.join(", ")
            ));
        }
        let more = if lost.len() > 5 {
            format!(" and {} more", lost.len() - 5)
        } else {
            String::new()
        };
        Err(DbError::Api(format!(
            "covered_writes = DeleteWins: {} write(s) covered only by deletes that did not \
             take effect, so dropping them would lose them: {}{more}; nothing was committed",
            lost.len(),
            lines.join("; ")
        )))
    }
}

/// DuckDB's `lower()` fold of each name, in input order (one statement).
fn duck_folds(conn: &Connection, names: &[&str]) -> Result<Vec<String>, DbError> {
    use duckdb::types::Value;

    let n = names.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    let names_list = Value::List(names.iter().map(|s| Value::Text(s.to_string())).collect());
    let ord_list = Value::List((0..n).map(|i| Value::BigInt(i as i64)).collect());
    let mut stmt = conn.prepare_cached(
        "WITH k AS (SELECT unnest(?::VARCHAR[]) AS n, unnest(?::BIGINT[]) AS i) \
         SELECT i, lower(n) FROM k",
    )?;
    let mut out: Vec<Option<String>> = vec![None; n];
    let rows = stmt.query_map(params![names_list, ord_list], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (i, fold) = row?;
        let i = usize::try_from(i)
            .ok()
            .filter(|&i| i < n && out[i].is_none())
            .ok_or_else(|| DbError::Internal(format!("name fold: unexpected ordinal {i}")))?;
        out[i] = Some(fold);
    }
    out.into_iter()
        .map(|f| {
            f.ok_or_else(|| DbError::Internal("name fold: a name came back without its row".into()))
        })
        .collect()
}

/// The [`WarningKind::DroppedByDelete`] warning of name `j` of segment
/// `segment_index`.
fn dropped_warning(
    segment_index: usize,
    p: &PreparedSegment,
    j: usize,
    by: CoveringDelete,
) -> Warning {
    Warning {
        kind: WarningKind::DroppedByDelete { segment_index, by },
        object_type: "value".into(),
        name: format!(
            "{}:{}/{}/{}",
            p.class, p.parameter, p.alternative, p.names[j]
        ),
        id: 0,
    }
}

/// The [`DbError::WriteAndDeleteSameKey`] (D9) refusal; `ids` names the
/// same key's resolved (entity, parameter definition, alternative) when
/// the resolved-id check (inside `apply_deletes`) found it, `None` from
/// the up-front name check (`reject_write_and_delete`).
fn same_key_error(i: usize, k: &DeleteKey, ids: Option<(i64, i64, i64)>) -> DbError {
    let message = format!(
        "delete key {i} ({}:{}/{}/{}) names a value this call also writes: \
         a value cannot be written and deleted in one call",
        k.class, k.parameter, k.alternative, k.entity
    );
    DbError::WriteAndDeleteSameKey(Box::new(WriteAndDeleteSameKeyDetails {
        key_index: i,
        class: k.class.clone(),
        parameter: k.parameter.clone(),
        alternative: k.alternative.clone(),
        entity: k.entity.clone(),
        resolved: ids,
        message,
    }))
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
    commit_changes(
        db,
        Vec::new(),
        Vec::new(),
        keys,
        Vec::new(),
        user,
        message,
        opts,
    )
}

/// Apply one or more structural (create/rename/set-ranks/activation/membership)
/// changes in ONE atomic, conflict-retried commit — the structural-only wrapper
/// over [`commit_changes`] (structural_write_api_plan §2.1). Per-item outcomes
/// land in `report.structural`.
pub fn commit_structural(
    db: &Path,
    changes: Vec<StructuralChange>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    commit_changes(
        db,
        Vec::new(),
        changes,
        Vec::new(),
        Vec::new(),
        user,
        message,
        opts,
    )
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
    commit_changes(
        db,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        object_deletes,
        user,
        message,
        opts,
    )
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
    mut keys: Vec<ObjectDeleteKey>,
) -> Result<Vec<ObjectDeletePreview>, DbError> {
    let mut lookups = Normalized::default();
    for k in &mut keys {
        nfc_object_delete_key(k, &mut lookups);
    }
    with_read_plain(db, |conn| {
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

/// How one prepared delete key resolved (see [`apply_deletes`]).
enum DeleteResolution {
    /// The first missing referent level and its name.
    Unknown(&'static str, String),
    /// Resolved to the `i`-th key of the delete round.
    Key(usize),
    /// Resolved to the same key as an earlier delete key (two spellings that
    /// DuckDB's `lower()` folds together): nothing left to delete (D10).
    Repeat,
}

/// Resolve every `DeleteKey` (lookup-only, case-insensitive: never
/// auto-create) and close out their live values under `commit_id` in one
/// set-based round (set_based_long_and_delete_PLAN §3.4, D9). The class,
/// parameter and alternative lookups are today's point lookups, memoized per
/// exact spelling (nothing renames or creates them during the delete
/// phase); the entity names of each class resolve in one lookup
/// ([`lookup_entities`]). The values close through `history::delete_values`.
///
/// Per key, in key order, as one per-key delete each: the first missing
/// level (class, parameter, alternative, entity) yields
/// [`WarningKind::UnknownReferent`] and a skip; a resolved key with no live
/// value yields [`WarningKind::NothingToDelete`]. Two keys that resolve to
/// one value (spellings the up-front Rust fold keeps apart but DuckDB's
/// `lower()` merges, e.g. a final sigma): the first deletes, the later ones
/// get `NothingToDelete`, as a second per-key delete would (plan D10).
/// Retention obeys the definition's `retain_content` unless the caller
/// overrode it, as in the write path (`write_prepared`).
///
/// A key that resolves to a value this call also writes (`written`, the
/// resolved write keys) is rejected as [`DbError::WriteAndDeleteSameKey`]
/// before anything is closed (D9; the resolved-id repeat of
/// `reject_write_and_delete`).
/// Returns the number of keys that closed at least one row.
fn apply_deletes(
    conn: &Connection,
    commit_id: i64,
    deletes: &[(usize, DeleteKey)],
    opts: WriteOptions,
    written: &HashSet<(i64, i64, i64)>,
    report: &mut WriteReport,
) -> Result<usize, DbError> {
    if deletes.is_empty() {
        return Ok(0);
    }
    // --- Class, parameter and alternative: memoized point lookups, in key
    // order (a key stops at its first missing level, as before). ---
    let mut classes: HashMap<&str, Option<i64>> = HashMap::new();
    let mut params: HashMap<(i64, &str), Option<i64>> = HashMap::new();
    let mut alts: HashMap<&str, Option<i64>> = HashMap::new();
    // Per key: Err(unknown level + name) or Ok((class, pd, alt)).
    type Partial = Result<(i64, i64, i64), (&'static str, String)>;
    let mut partial: Vec<Partial> = Vec::with_capacity(deletes.len());
    for (_, dk) in deletes {
        let class_id = match classes.get(dk.class.as_str()) {
            Some(&id) => id,
            None => {
                let id = lookup_entity_class(conn, &dk.class)?;
                classes.insert(&dk.class, id);
                id
            }
        };
        let Some(class_id) = class_id else {
            partial.push(Err(("entity_class", dk.class.clone())));
            continue;
        };
        let pd_id = match params.get(&(class_id, dk.parameter.as_str())) {
            Some(&id) => id,
            None => {
                let id = lookup_parameter(conn, class_id, &dk.parameter)?;
                params.insert((class_id, &dk.parameter), id);
                id
            }
        };
        let Some(pd_id) = pd_id else {
            partial.push(Err(("parameter_definition", dk.parameter.clone())));
            continue;
        };
        let alt_id = match alts.get(dk.alternative.as_str()) {
            Some(&id) => id,
            None => {
                let id = lookup_alternative(conn, &dk.alternative)?;
                alts.insert(&dk.alternative, id);
                id
            }
        };
        let Some(alt_id) = alt_id else {
            partial.push(Err(("alternative", dk.alternative.clone())));
            continue;
        };
        partial.push(Ok((class_id, pd_id, alt_id)));
    }

    // --- Entities: one lookup per class over its distinct spellings. ---
    let mut by_class: HashMap<i64, Vec<&str>> = HashMap::new();
    let mut class_order: Vec<i64> = Vec::new();
    // Distinct (class, spelling) pairs: a set, not a scan of the class's
    // list, so a large delete batch stays linear in its keys.
    let mut listed: HashSet<(i64, &str)> = HashSet::new();
    for ((_, dk), p) in deletes.iter().zip(&partial) {
        if let Ok((class_id, _, _)) = p {
            let names = by_class.entry(*class_id).or_insert_with(|| {
                class_order.push(*class_id);
                Vec::new()
            });
            if listed.insert((*class_id, dk.entity.as_str())) {
                names.push(&dk.entity);
            }
        }
    }
    let mut entities: HashMap<(i64, &str), Option<i64>> = HashMap::new();
    for class_id in class_order {
        let names = &by_class[&class_id];
        for (name, (_, id)) in names.iter().zip(lookup_entities(conn, class_id, names)?) {
            entities.insert((class_id, name), id);
        }
    }

    // --- Resolve, in key order; retention per parameter definition. ---
    let mut retain: HashMap<i64, bool> = HashMap::new();
    let mut seen: HashSet<(i64, i64, i64)> = HashSet::new();
    let mut round: Vec<DeleteWrite> = Vec::new();
    let mut resolved: Vec<DeleteResolution> = Vec::with_capacity(deletes.len());
    for ((key_index, dk), p) in deletes.iter().zip(partial) {
        let (class_id, pd_id, alt_id) = match p {
            Ok(ids) => ids,
            Err((level, name)) => {
                resolved.push(DeleteResolution::Unknown(level, name));
                continue;
            }
        };
        let Some(entity_id) = entities[&(class_id, dk.entity.as_str())] else {
            resolved.push(DeleteResolution::Unknown("entity", dk.entity.clone()));
            continue;
        };
        if written.contains(&(entity_id, pd_id, alt_id)) {
            return Err(same_key_error(
                *key_index,
                dk,
                Some((entity_id, pd_id, alt_id)),
            ));
        }
        if !seen.insert((entity_id, pd_id, alt_id)) {
            resolved.push(DeleteResolution::Repeat);
            continue;
        }
        // Fold the definition's retain_content into retain_override unless
        // the caller already overrode it: else a delete on a
        // retain_content=FALSE definition would silently keep the
        // closed-out payload (VMP Part 5).
        let effective_retain = match opts.retain_override {
            Some(r) => r,
            None => match retain.get(&pd_id) {
                Some(&r) => r,
                None => {
                    let r = read_retain_content(conn, pd_id)?;
                    retain.insert(pd_id, r);
                    r
                }
            },
        };
        let resolved_opts = WriteOptions {
            retain_override: Some(effective_retain),
            ..opts
        };
        resolved.push(DeleteResolution::Key(round.len()));
        round.push(DeleteWrite {
            key: ValueKey {
                entity_id,
                parameter_definition_id: pd_id,
                alternative_id: alt_id,
            },
            retained: keeps_content(&resolved_opts),
        });
    }

    let outcomes = history::delete_values(conn, commit_id, &round)?;

    // --- Report and warnings, in key order. ---
    let mut deleted = 0usize;
    for ((key_index, dk), r) in deletes.iter().zip(resolved) {
        let rows_closed = match r {
            DeleteResolution::Unknown(level, name) => {
                unknown(report, *key_index, level, &name);
                continue;
            }
            DeleteResolution::Key(i) => {
                report.absorb_delete(&outcomes[i]);
                outcomes[i].rows_closed
            }
            DeleteResolution::Repeat => 0,
        };
        if rows_closed > 0 {
            deleted += 1;
        } else {
            report.warnings.push(Warning {
                kind: WarningKind::NothingToDelete {
                    key_index: *key_index,
                },
                object_type: "value".into(),
                name: format!(
                    "{}:{}/{}/{}",
                    dk.class, dk.parameter, dk.alternative, dk.entity
                ),
                id: 0,
            });
        }
    }
    Ok(deleted)
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
    opt_id(
        conn,
        "SELECT id FROM entity_class WHERE cur_name = lower(?)",
        -1,
        name,
    )
}

pub(crate) fn lookup_alternative(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM alternative WHERE cur_name = lower(?)",
        -1,
        name,
    )
}

pub(crate) fn lookup_scenario(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM scenario WHERE cur_name = lower(?)",
        -1,
        name,
    )
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

/// The single-name entity lookup of `lookup_entity` (`?1` class id, `?2`
/// name): the id of the current entity of the class whose DuckDB `lower()`
/// fold equals the name's. Same rows as `cur_cls = ?1 AND cur_name =
/// lower(?2)`, written so the `ix_entity_lname` expression index is used.
///
/// **The `OFFSET 0` fence.** DuckDB 2.0 scans through an index only when the
/// scan carries exactly one filter (`table_scan.cpp`: `FilterCount() != 1`
/// falls back to a sequential scan). The class and current-row filters can be
/// dropped from the scan only when zone-map statistics prove them always true
/// (one class, no closed-out rows: toy files), so on any real file the plain
/// three-conjunct form is a sequential scan. The inner query carries the
/// index filter alone; `OFFSET 0` stops the optimizer from pushing the outer
/// filters into it (filter pushdown stops at every LIMIT node except
/// `LIMIT 0`). It returns the 1–few rows of that folded name in any class,
/// current or closed out, and the outer WHERE picks the current one.
///
/// This is an optimizer behaviour, not a documented contract; the guard is
/// `tests/entity_name_index_scan.rs` (production SQL, realistic fixture,
/// unfenced negative control). Pre-agreed fallbacks if a DuckDB bump breaks
/// the fence (name_index_and_unused_indexes_PLAN.md D1, §10 C1), in order:
/// (d) the aggregate fence `SELECT any_value(id) FILTER (WHERE
/// entity_class_id = ?1 AND valid_to_commit IS NULL) FROM entity WHERE
/// lower(name) = lower(?2)`; then (c) fetch `(id, entity_class_id,
/// valid_to_commit)` by `lower(name)` alone and filter in Rust. Never a
/// `MATERIALIZED` CTE: DuckDB's `CTEFilterPusher` already pushes filters into
/// one of this shape.
#[doc(hidden)]
pub const ENTITY_NAME_LOOKUP_SQL: &str = "SELECT id FROM \
       (SELECT id, entity_class_id, valid_to_commit FROM entity \
        WHERE lower(name) = lower(?2) OFFSET 0) \
     WHERE entity_class_id = ?1 AND valid_to_commit IS NULL";

pub(crate) fn lookup_entity(
    conn: &Connection,
    class_id: i64,
    name: &str,
) -> Result<Option<i64>, DbError> {
    opt_id(conn, ENTITY_NAME_LOOKUP_SQL, class_id, name)
}

pub(crate) fn lookup_class_set(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM class_set WHERE cur_name = lower(?)",
        -1,
        name,
    )
}

pub(crate) fn lookup_value_list(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM parameter_value_list WHERE cur_name = lower(?)",
        -1,
        name,
    )
}

pub(crate) fn lookup_unit(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM units_of_measure WHERE cur_name = lower(?)",
        -1,
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
    /// The label lookup of `domains`, built once per segment (plan T12).
    domain_index: Arc<DomainIndex>,
    /// Warnings of the segment itself (timezone-normalized columns,
    /// normalized names and labels), added to the report on every attempt.
    prep_warnings: Vec<Warning>,
    /// Distinct entity names, first-seen order, case-insensitively deduped.
    names: Vec<String>,
    /// Per-entity row indices into the concatenated batch, ascending (i.e.
    /// original row order). O(nrows) total, vs O(entities × nrows) for the
    /// boolean masks these replace (critique F4).
    rows: Vec<Vec<u32>>,
    /// The dims+value sub-batch (entity column dropped).
    sub_all: RecordBatch,
}

impl PreparedSegment {
    /// Canonicalize the value of entity `j` (an index into `names`). An
    /// error that needs the value's location to be found in the source data
    /// (a duplicate index tuple) names the entity, class, parameter and
    /// alternative.
    fn canonicalize_entity(
        &self,
        j: usize,
        declared: &DeclaredType,
    ) -> Result<CanonicalValue, DbError> {
        let sub = take_record_batch(&self.sub_all, &UInt32Array::from(self.rows[j].clone()))?;
        canonicalize_indexed(&sub, declared, self.value_kind, &self.domain_index).map_err(|e| {
            DbError::Canon(e.at_value(
                &self.names[j],
                &self.class,
                &self.parameter,
                &self.alternative,
            ))
        })
    }

    /// This segment without the names `dropped` lists (indices into
    /// `names`; `CoveredWrites::DeleteWins`). The row indices of the kept
    /// names still point into the shared `sub_all`.
    fn without(&self, dropped: &[usize]) -> PreparedSegment {
        let drop: HashSet<usize> = dropped.iter().copied().collect();
        let (names, rows) = self
            .names
            .iter()
            .zip(&self.rows)
            .enumerate()
            .filter(|(i, _)| !drop.contains(i))
            .map(|(_, (n, r))| (n.clone(), r.clone()))
            .unzip();
        PreparedSegment {
            class: self.class.clone(),
            parameter: self.parameter.clone(),
            alternative: self.alternative.clone(),
            value_kind: self.value_kind,
            inferred: self.inferred.clone(),
            domain_index: Arc::clone(&self.domain_index),
            prep_warnings: self.prep_warnings.clone(),
            names,
            rows,
            sub_all: self.sub_all.clone(),
        }
    }
}

/// Validate and split one segment. `seg_names` holds what normalizing the
/// segment's class, parameter and alternative names changed; the entity
/// names are normalized here, and the segment's `TextNormalized` warnings
/// (names, labels) join its prepared warnings.
fn prepare_segment(seg: &Segment, mut seg_names: Normalized) -> Result<PreparedSegment, DbError> {
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
        return Err(DbError::Api(format!(
            "declared rank {rank} exceeds the cap of 4"
        )));
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
    // A domain that repeats a label has no well-defined order: refuse it once
    // per segment, before any value is canonicalized, while building the
    // segment's label lookup.
    let domain_index = Arc::new(index_domains(&domains, |position| {
        inferred
            .get(usize::from(position).wrapping_sub(1))
            .map(|(name, _)| name.clone())
    })?);

    // Tz-bearing timestamp columns are accepted — a Timestamp(µs, tz) is a
    // well-defined instant, stored as the UTC-µs instant unchanged — but never
    // silently: each gets a TimezoneNormalized warning (critique F6).
    let mut prep_warnings: Vec<Warning> = Vec::new();
    for i in 1..ncols {
        let field = batch.schema().field(i).clone();
        if let DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) = field.data_type() {
            prep_warnings.push(Warning {
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
        // Entity names are names: NFC before the dedup below (the text rule).
        let name = seg_names.nfc(entity_arr.value(row)).into_owned();
        if name.trim().is_empty() {
            return Err(DbError::Api(format!("empty entity name in row {row}")));
        }
        // Dedup by an ASCII case fold so case-aliases cannot double-write one
        // entity. Aliased spellings are ambiguous, not mergeable (critique
        // F3). Entity names are free text, and DB resolution folds by
        // DuckDB's Unicode `lower()`: a non-ASCII alias (`"Ä"`/`"ä"`) passes
        // here and is rejected with the same error by `resolve_entities`,
        // which groups by the fold DuckDB returns.
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

    // Labels (the text rule): canon compares and stores them in NFC; count
    // here, once per segment, the distinct spellings that it changes, in
    // the label columns and the domains alike, for the segment's warning.
    let mut labels = Normalized::default();
    for d in &domains {
        for l in &d.labels {
            labels.note(l);
        }
    }
    for (i, (_, kind)) in inferred.iter().enumerate() {
        if *kind != IndexKind::Label {
            continue;
        }
        if let Some(col) = batch.column(i + 1).as_any().downcast_ref::<StringArray>() {
            for v in col.iter().flatten() {
                labels.note(v);
            }
        }
    }
    let target = format!("{}:{}/{}", seg.class, seg.parameter, seg.alternative);
    prep_warnings.extend(seg_names.warning("name", "segment", &target));
    prep_warnings.extend(labels.warning("label", "segment", &target));

    Ok(PreparedSegment {
        class: seg.class.clone(),
        parameter: seg.parameter.clone(),
        alternative: seg.alternative.clone(),
        value_kind,
        inferred,
        domain_index,
        prep_warnings,
        names,
        rows,
        sub_all,
    })
}

/// One segment's DB-touching write sequence (resolve → validate → canon → tier
/// → axis → history), run inside the caller's open transaction. Callable N
/// times per transaction — this is what makes multi-segment ingest one commit.
#[allow(clippy::too_many_arguments)]
fn write_prepared(
    conn: &Connection,
    commit_id: i64,
    p: &PreparedSegment,
    opts: WriteOptions,
    cache: &mut AxisCache,
    report: &mut WriteReport,
    buffers: &mut WriteBuffers,
    entities: &mut EntityCache,
    targets: &mut HashSet<(i64, i64)>,
    names: &mut ResolveCache,
) -> Result<(), DbError> {
    report.warnings.extend(p.prep_warnings.iter().cloned());

    let class_id = names.entity_class(conn, commit_id, &p.class, &mut report.warnings)?;
    let ResolvedParameter {
        pd_id,
        pt_id,
        shape: declared,
        retain_content,
    } = names.parameter(
        conn,
        commit_id,
        class_id,
        &p.parameter,
        &p.inferred,
        p.value_kind,
        &mut report.warnings,
    )?;
    let alt_id = names.alternative(conn, commit_id, &p.alternative, &mut report.warnings)?;
    // Two segments that resolve to one (parameter, alternative) would write
    // the same value keys twice in one commit. The up-front check compares a
    // Rust fold; this one compares the ids DuckDB resolved, so a spelling
    // pair the two folds disagree on cannot reach the writers (with the
    // deferred writes, a minted entity skips its probes and would
    // otherwise end up live in two tiers).
    if !targets.insert((pd_id, alt_id)) {
        return Err(DbError::Api(format!(
            "duplicate target {}:{} (alternative {:?}): resolves to the same parameter \
             and alternative as an earlier segment (name resolution is case-insensitive)",
            p.class, p.parameter, p.alternative
        )));
    }

    // Fold the definition's retain_content into retain_override unless the
    // caller already overrode it (VMP Part 5 primary retention lever): else a
    // write to a retain_content=FALSE definition would silently keep payloads.
    let effective_retain = opts.retain_override.unwrap_or(retain_content);
    let resolved_opts = WriteOptions {
        retain_override: Some(effective_retain),
        ..opts
    };

    // Every entity of the segment is resolved up front in one set-based
    // round (set_based_scalar_write_PLAN §3.4, D7). The per-entity
    // canonicalization below is pure, so its order relative to resolution is
    // only observable through which error comes first; an N-D refusal keeps
    // the per-entity order (§3.5): the entities before the first missing name
    // are canonicalized first, and their error wins.
    let resolved = resolve_entities(
        conn,
        commit_id,
        class_id,
        &p.class,
        &p.names,
        entities,
        &mut report.warnings,
    )?;
    let (entity_ids, entity_minted) = match resolved {
        Resolved::Ids { ids, minted } => (ids, minted),
        Resolved::NdRefused { first_missing, err } => {
            for j in 0..first_missing {
                p.canonicalize_entity(j, &declared)?;
            }
            return Err(err);
        }
    };

    // The buffered writes rely on distinct keys (a key lives in one buffer
    // and its probes run once): `resolve_entities` rejects two spellings
    // DuckDB folds to one entity, so a repeated id here is a core invariant
    // breach (critique C5), checked where the buffers rely on it.
    let mut seen: HashSet<i64> = HashSet::with_capacity(entity_ids.len());
    if let Some(dup) = entity_ids.iter().find(|&&id| !seen.insert(id)) {
        return Err(DbError::Internal(format!(
            "segment {}:{}: entity id {dup} resolved twice",
            p.class, p.parameter
        )));
    }

    let retained = keeps_content(&resolved_opts);
    for (j, (&entity_id, &minted)) in entity_ids.iter().zip(&entity_minted).enumerate() {
        let value = p.canonicalize_entity(j, &declared)?;
        let key = ValueKey {
            entity_id,
            parameter_definition_id: pd_id,
            alternative_id: alt_id,
        };
        buffers.track(&key);
        match value.tier {
            Tier::Scalar => {
                let ShapedData::Scalar(m) = value.shape else {
                    unreachable!("shape/tier mismatch from canon")
                };
                // Deferred to the commit's set-based scalar write (plan D1).
                // The tier is recorded now, in the order values are met
                // (critique C5).
                report.note_tier(Tier::Scalar);
                buffers.scalars.push(ScalarWrite {
                    key,
                    value: m,
                    parameter_type_id: Some(pt_id),
                    retained,
                    entity_minted: minted,
                });
                if buffers.scalars.len() >= LONG_INSERT_CHUNK {
                    buffers.flush_scalars(conn, commit_id, report)?;
                }
            }
            Tier::Long => {
                // Deferred to the commit's set-based long write
                // (set_based_long_and_delete_PLAN §3.1). The axes resolve
                // now, in value order, so axis ids are minted as before.
                report.note_tier(Tier::Long);
                let (axis_ids, axes_created, axes_reused) = resolve_axes(conn, cache, &value.axes)?;
                let cells = match &value.shape {
                    ShapedData::Long(rows) => rows.len(),
                    _ => unreachable!("shape/tier mismatch from canon"),
                };
                buffers.longs.push(LongWrite {
                    key,
                    value: Cow::Owned(value),
                    axis_ids,
                    axes_created,
                    axes_reused,
                    parameter_type_id: Some(pt_id),
                    retained,
                    entity_minted: minted,
                });
                buffers.long_cells += cells;
                if buffers.long_cells >= LONG_INSERT_CHUNK
                    || buffers.longs.len() >= LONG_INSERT_CHUNK
                {
                    buffers.flush_longs(conn, commit_id, report)?;
                }
            }
            Tier::Array => {
                // Deferred to the commit's set-based array write (plan
                // §3.3), axes resolved now, as for a long value.
                report.note_tier(Tier::Array);
                let (axis_ids, axes_created, axes_reused) = resolve_axes(conn, cache, &value.axes)?;
                let elems = match &value.shape {
                    ShapedData::Array(vals) => vals.len(),
                    _ => unreachable!("shape/tier mismatch from canon"),
                };
                buffers.arrays.push(ArrayWrite {
                    key,
                    value: Cow::Owned(value),
                    axis_ids,
                    axes_created,
                    axes_reused,
                    parameter_type_id: Some(pt_id),
                    retained,
                    entity_minted: minted,
                });
                buffers.array_elems += elems;
                if buffers.array_elems >= ARRAY_ROUND_ELEMENTS
                    || buffers.arrays.len() >= LONG_INSERT_CHUNK
                {
                    buffers.flush_arrays(conn, commit_id, report)?;
                }
            }
        }
    }
    Ok(())
}

/// A segment's resolved parameter: the definition, the declared member the
/// batch binds to, its shape, and the definition's `retain_content`.
struct ResolvedParameter {
    pd_id: i64,
    pt_id: i64,
    shape: DeclaredType,
    retain_content: bool,
}

/// An existing definition as an earlier segment of the commit read it: its
/// non-empty declared-type set and its `retain_content`.
struct CachedParameter {
    pd_id: i64,
    declared: Vec<DbDeclared>,
    retain_content: bool,
}

/// The per-attempt class / parameter / alternative resolution cache of the
/// segment phase (set_based_long_and_delete_PLAN §10 C11). An editor save
/// carries many segments of one class and alternative; without the cache
/// every segment re-runs the same lookups.
///
/// Keys are the caller's exact spellings (a parameter's within its class
/// id), so a hit returns what the uncached resolver returns for the same
/// string; two spellings DuckDB folds together are two entries resolving to
/// one id, as before. Soundness rests on the segment phase not changing
/// what it reads: structural changes run before the first segment, deletes
/// and object deletes after the last, and the segments themselves only
/// (a) auto-create classes, definitions and alternatives, which the cache
/// records as it creates them (a later hit emits no second `AutoCreated`
/// warning, just as the uncached lookup finds the new row), and (b)
/// auto-create the `parameter_type` of a definition that has none, which is
/// never cached: only a set read non-empty from the DB is, and nothing in
/// the segment phase closes or adds members of a declared set. Lives in one
/// transaction attempt (a closure local of `commit_changes`), never across
/// attempts or commits.
struct ResolveCache {
    enabled: bool,
    classes: HashMap<String, i64>,
    alternatives: HashMap<String, i64>,
    parameters: HashMap<(i64, String), CachedParameter>,
}

impl ResolveCache {
    fn new() -> Self {
        ResolveCache {
            enabled: test_hooks::resolve_cache_enabled(),
            classes: HashMap::new(),
            alternatives: HashMap::new(),
            parameters: HashMap::new(),
        }
    }

    fn entity_class(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        name: &str,
        warnings: &mut Vec<Warning>,
    ) -> Result<i64, DbError> {
        if let Some(&id) = self.classes.get(name) {
            return Ok(id);
        }
        let id = resolve_entity_class(conn, commit_id, name, warnings)?;
        if self.enabled {
            self.classes.insert(name.to_string(), id);
        }
        Ok(id)
    }

    fn alternative(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        name: &str,
        warnings: &mut Vec<Warning>,
    ) -> Result<i64, DbError> {
        if let Some(&id) = self.alternatives.get(name) {
            return Ok(id);
        }
        let id = resolve_alternative(conn, commit_id, name, warnings)?;
        if self.enabled {
            self.alternatives.insert(name.to_string(), id);
        }
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    fn parameter(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        class_id: i64,
        name: &str,
        inferred: &[(String, IndexKind)],
        value_kind: ValueKind,
        warnings: &mut Vec<Warning>,
    ) -> Result<ResolvedParameter, DbError> {
        let key = (class_id, name.to_string());
        if let Some(c) = self.parameters.get(&key) {
            // The match the uncached path runs, on the same set.
            let matched = validate_declared(name, c.pd_id, &c.declared, inferred, value_kind)?;
            return Ok(ResolvedParameter {
                pd_id: c.pd_id,
                pt_id: matched.pt_id,
                shape: matched.shape,
                retain_content: c.retain_content,
            });
        }
        let (pd_id, pt_id, shape, set) = resolve_parameter(
            conn, commit_id, class_id, name, inferred, value_kind, warnings,
        )?;
        let retain_content = read_retain_content(conn, pd_id)?;
        if let (true, Some(declared)) = (self.enabled, set) {
            self.parameters.insert(
                key,
                CachedParameter {
                    pd_id,
                    declared,
                    retain_content,
                },
            );
        }
        Ok(ResolvedParameter {
            pd_id,
            pt_id,
            shape,
            retain_content,
        })
    }
}

/// Test-only switches. They exist in debug builds only; in a release build
/// the write path compiles to the production behaviour. Thread-local, so
/// parallel tests do not interfere (the write closure runs on the calling
/// thread).
#[doc(hidden)]
pub mod test_hooks {
    #[cfg(debug_assertions)]
    thread_local! {
        static RESOLVE_CACHE_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
        static INJECT_CONFLICTS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    /// Run `f` with the segment phase's name-resolution cache disabled (the
    /// reference arm of the cache equivalence test).
    #[cfg(debug_assertions)]
    pub fn without_resolve_cache<R>(f: impl FnOnce() -> R) -> R {
        RESOLVE_CACHE_ENABLED.with(|c| c.set(false));
        let out = f();
        RESOLVE_CACHE_ENABLED.with(|c| c.set(true));
        out
    }

    /// Make the next `n` `commit_changes` attempts on this thread fail with
    /// a synthetic write-write conflict after their segment phase, so the
    /// retry loop re-runs them.
    #[cfg(debug_assertions)]
    pub fn inject_conflicts(n: u32) {
        INJECT_CONFLICTS.with(|c| c.set(n));
    }

    pub(crate) fn resolve_cache_enabled() -> bool {
        #[cfg(debug_assertions)]
        {
            RESOLVE_CACHE_ENABLED.with(|c| c.get())
        }
        #[cfg(not(debug_assertions))]
        {
            true
        }
    }

    /// Consume one pending injected conflict, if any.
    pub(crate) fn take_injected_conflict() -> bool {
        #[cfg(debug_assertions)]
        {
            INJECT_CONFLICTS.with(|c| {
                let n = c.get();
                if n > 0 {
                    c.set(n - 1);
                }
                n > 0
            })
        }
        #[cfg(not(debug_assertions))]
        {
            false
        }
    }
}

/// The deferred writes of one commit (closure locals of the retried
/// commit). The keys of one commit are distinct across all buffers (the
/// up-front and resolved-id duplicate-target checks and the per-segment
/// entity dedup), so per-key writes commute and a key lives in exactly one
/// buffer; nothing between buffering and flushing reads value rows.
#[derive(Default)]
struct WriteBuffers {
    scalars: Vec<ScalarWrite>,
    longs: Vec<LongWrite<'static>>,
    /// The cells buffered in `longs` (plan D3 flush threshold).
    long_cells: usize,
    arrays: Vec<ArrayWrite<'static>>,
    /// The payload elements buffered in `arrays` (plan D3).
    array_elems: usize,
    /// Every value key buffered by the commit, flushed or not: the
    /// same-call write-and-delete check (skip_unchanged_writes_PLAN D9).
    keys: HashSet<(i64, i64, i64)>,
    /// Values buffered by the commit (the no-op evidence, D1: every one
    /// must come back unchanged).
    values: usize,
    /// The tiers of the values actually written (not unchanged), for
    /// `WriteReport::tiers`.
    written_tiers: Vec<Tier>,
}

impl WriteBuffers {
    /// Record one buffered value of key `k`.
    fn track(&mut self, k: &ValueKey) {
        self.values += 1;
        self.keys
            .insert((k.entity_id, k.parameter_definition_id, k.alternative_id));
    }

    /// Fold the outcomes of one flush into `report`, in buffer order.
    fn absorb(&mut self, outcomes: &[crate::types::WriteOutcome], report: &mut WriteReport) {
        for o in outcomes {
            if let (false, Some(t)) = (o.unchanged, o.tier) {
                if !self.written_tiers.contains(&t) {
                    self.written_tiers.push(t);
                }
            }
            report.absorb(o);
        }
    }

    /// Write the buffered scalars in one set-based call and fold their
    /// outcomes into `report` in buffer order (the tiers were already noted
    /// when each value was canonicalized). Empties the buffer.
    fn flush_scalars(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        report: &mut WriteReport,
    ) -> Result<(), DbError> {
        if self.scalars.is_empty() {
            return Ok(());
        }
        let outcomes = write_scalars(conn, commit_id, &self.scalars)?;
        self.absorb(&outcomes, report);
        self.scalars.clear();
        Ok(())
    }

    /// The long counterpart of [`Self::flush_scalars`].
    fn flush_longs(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        report: &mut WriteReport,
    ) -> Result<(), DbError> {
        if self.longs.is_empty() {
            return Ok(());
        }
        let outcomes = write_longs(conn, commit_id, &self.longs)?;
        self.absorb(&outcomes, report);
        self.longs.clear();
        self.long_cells = 0;
        Ok(())
    }

    /// The array counterpart of [`Self::flush_scalars`].
    fn flush_arrays(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        report: &mut WriteReport,
    ) -> Result<(), DbError> {
        if self.arrays.is_empty() {
            return Ok(());
        }
        let outcomes = write_arrays(conn, commit_id, &self.arrays)?;
        self.absorb(&outcomes, report);
        self.arrays.clear();
        self.array_elems = 0;
        Ok(())
    }

    /// Flush every buffer.
    fn flush(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        report: &mut WriteReport,
    ) -> Result<(), DbError> {
        self.flush_scalars(conn, commit_id, report)?;
        self.flush_longs(conn, commit_id, report)?;
        self.flush_arrays(conn, commit_id, report)
    }
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
    // Stored names are NFC (the text rule): look them up in NFC.
    let (class, parameter, alternative) = (nfc(class), nfc(parameter), nfc(alternative));
    with_read_named(db, &["read_parameter"], |conn| {
        let mut stmt =
            conn.prepare("SELECT * FROM read_parameter(?, ?) WHERE lower(alternative) = lower(?)")?;
        let arrow = stmt.query_arrow(params![
            class.as_ref(),
            parameter.as_ref(),
            alternative.as_ref()
        ])?;
        let schema = arrow.get_schema();
        let batches: Vec<RecordBatch> = arrow.collect();
        Ok(concat_batches(&schema, &batches)?)
    })
}

/// The read-surface status and refresh of a database file (the API face of
/// the `read_surface` module; `nodal-db maintain refresh-views`).
pub use crate::read_surface::{read_surface_status, refresh_read_surface, SurfaceReport};

/// The statement behind [`read_values`]: the `read_values` SQL macro with
/// the name lists bound as parameters (`?1` class, `?2` parameter, `?3`
/// entity names, `?4` alternative names or NULL). The explicit `::VARCHAR[]`
/// casts are load-bearing: an empty bound list carries no element type.
/// Exposed so the plan guard (`tests/point_read_index_scan.rs`) checks the
/// production text.
pub const READ_VALUES_SQL: &str = "SELECT * FROM read_values(?, ?, ?::VARCHAR[], ?::VARCHAR[])";

/// Read a few values of one parameter: the current rows of every
/// (entity, alternative) pair of `entities` x `alternatives` (all current
/// alternatives when `None`), through the `read_values` SQL macro, in
/// `read_parameter`'s columns. One fresh RO connection, opened and closed
/// per call (CLAUDE.md invariant 1).
///
/// This is the editor's per-entity read: it fetches only the arrays it
/// needs (by `array_id`, an index scan) instead of every array of the
/// parameter, so it costs milliseconds where [`read_parameter`] scans the
/// whole array table. Up to 50 value hits (arrays that exist for the
/// requested pairs) per call stay on the index; above that the macro scans
/// every array, as `read_parameter` does. Names match case-insensitively;
/// a name that does not resolve contributes no rows (no error). Row order
/// is unspecified.
///
/// No Rust-side rendering: the macro is the one renderer of axes
/// (CLAUDE.md invariant 2 applied to reads).
pub fn read_values(
    db: &Path,
    class: &str,
    parameter: &str,
    entities: &[&str],
    alternatives: Option<&[&str]>,
) -> Result<RecordBatch, DbError> {
    use duckdb::types::Value;
    // Stored names are NFC (the text rule): look them up in NFC.
    let list = |xs: &[&str]| {
        Value::List(
            xs.iter()
                .map(|x| Value::Text(nfc(x).into_owned()))
                .collect(),
        )
    };
    let ents = list(entities);
    let alts = alternatives.map_or(Value::Null, list);
    let (class, parameter) = (nfc(class), nfc(parameter));
    with_read_named(db, &["read_values"], |conn| {
        let mut stmt = conn.prepare(READ_VALUES_SQL)?;
        let arrow = stmt.query_arrow(params![class.as_ref(), parameter.as_ref(), ents, alts])?;
        let schema = arrow.get_schema();
        let batches: Vec<RecordBatch> = arrow.collect();
        Ok(concat_batches(&schema, &batches)?)
    })
}

/// The database's head commit id: `COALESCE(max(commit.id), 0)`, read on one
/// per-operation RO connection (CLAUDE.md invariant 1). Quote it to
/// [`WriteOptions::expect_commit`] for an atomic read → compute → write-only-
/// if-nobody-committed cycle. Compaction preserves commit ids, so a repack
/// never moves the head; every commit (any writer, any process) and an
/// erasure (its marker commit) do.
pub fn head_commit(db: &Path) -> Result<i64, DbError> {
    with_read_plain(db, head_on)
}

/// The database's stable identity UUID — the `db_identity` reserved metadata
/// key, minted once by `900_bootstrap.sql` at init and never changed thereafter
/// (specs/schema_interop.md §9: the anchor output DBs stamp their
/// "produced from input DB <identity> at commit C under scenario S" provenance
/// against). It survives reopen and compaction (the repack copies `metadata`
/// rows as data). One fresh RO connection per call (CLAUDE.md invariant 1).
/// Plain-SQL equivalent:
/// `SELECT value FROM metadata WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL`.
pub fn db_identity(db: &Path) -> Result<String, DbError> {
    with_read_plain(db, |conn| {
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

pub(crate) fn index_kind_tag(k: IndexKind) -> &'static str {
    match k {
        IndexKind::Datetime => "datetime",
        IndexKind::Integer => "integer",
        IndexKind::Label => "label",
        IndexKind::Period => "period",
        IndexKind::TimePattern => "time_pattern",
    }
}

pub(crate) fn parse_index_kind(s: &str) -> Result<IndexKind, DbError> {
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
                DbError::Api(format!(
                    "bad spinal:label_domain metadata on {:?}: {e}",
                    field.name()
                ))
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
    // `prepare_cached`: the small set of lookup statements is re-executed for
    // every referent of every write/apply — the per-connection statement
    // cache amortizes the prepares (review item 21).
    let mut stmt = conn.prepare_cached(sql)?;
    let r = if id_param < 0 {
        stmt.query_row(params![name], |r| r.get::<_, i64>(0))
    } else {
        stmt.query_row(params![id_param, name], |r| r.get::<_, i64>(0))
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
    if let Some(id) = opt_id(
        conn,
        "SELECT id FROM entity_class WHERE cur_name = lower(?)",
        -1,
        name,
    )? {
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

/// The one `alternative` create site (the write path's decision-B
/// auto-create and the G8 `ensure_alternative` verb share it; the verb is
/// the only caller that carries a description).
pub(crate) fn insert_alternative_row(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    description: Option<&str>,
) -> Result<i64, DbError> {
    Ok(conn.query_row(
        "INSERT INTO alternative (name, description, valid_from_commit) \
         VALUES (?, ?, ?) RETURNING id",
        params![name, description, commit_id],
        |r| r.get(0),
    )?)
}

fn resolve_alternative(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<i64, DbError> {
    if let Some(id) = opt_id(
        conn,
        "SELECT id FROM alternative WHERE cur_name = lower(?)",
        -1,
        name,
    )? {
        return Ok(id);
    }
    let id = insert_alternative_row(conn, commit_id, name, None)?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "alternative".into(),
        name: name.into(),
        id,
    });
    Ok(id)
}

/// The outcome of [`resolve_entities`].
enum Resolved {
    /// One entity id per input name, in input order, and whether this call
    /// minted it (auto-created in this commit).
    Ids { ids: Vec<i64>, minted: Vec<bool> },
    /// The class is N-D and `first_missing` is the first name (in input
    /// order) without a current entity: auto-creating it is refused with
    /// `err`. Nothing was inserted. The caller canonicalizes the entities
    /// before it first, so a canon error there still wins (plan §3.5).
    NdRefused { first_missing: usize, err: DbError },
}

/// The entity lookup of [`resolve_entities`]: every input name with its
/// ordinal, its DuckDB `lower()` fold and the id of the current entity of the
/// class holding that folded name (NULL when missing). The fold is DuckDB's,
/// returned to Rust, never recomputed there (CLAUDE.md invariant 2). The
/// predicate is the expansion of `cur_cls = ? AND cur_name = lower(n)` on the
/// base columns (the VIRTUAL `cur_*` columns are `CASE WHEN valid_to_commit
/// IS NULL THEN … END`): the same rows, but the plain column filters run
/// before `lower(name)` is evaluated.
const ENTITY_LOOKUP_SQL: &str = "WITH k AS (SELECT unnest(?::VARCHAR[]) AS n, \
                                         unnest(?::BIGINT[]) AS i) \
     SELECT k.i, lower(k.n), e.id FROM k \
     LEFT JOIN entity e ON e.entity_class_id = ? AND e.valid_to_commit IS NULL \
                       AND lower(e.name) = lower(k.n)";

/// The single-name form of `ENTITY_LOOKUP_SQL` (plan D3(b), a
/// statement-form switch only): the fold and the id (NULL when missing).
/// `?1` name, `?2` class id. The id comes through the same `OFFSET 0` fence
/// as [`ENTITY_NAME_LOOKUP_SQL`] (see there for why, the guard and the
/// fallbacks), so it uses the `ix_entity_lname` index.
#[doc(hidden)]
pub const ENTITY_LOOKUP_ONE_SQL: &str = "SELECT lower(?1), (SELECT id FROM \
       (SELECT id, entity_class_id, valid_to_commit FROM entity \
        WHERE lower(name) = lower(?1) OFFSET 0) \
     WHERE entity_class_id = ?2 AND valid_to_commit IS NULL)";

/// Look up `names` among the current entities of `class_id`: per name, in
/// input order, DuckDB's `lower()` fold of it and the id of the current
/// entity holding that folded name (`None` when missing). One statement:
/// [`ENTITY_LOOKUP_ONE_SQL`] for one name, [`ENTITY_LOOKUP_SQL`] with an
/// explicit ordinal list otherwise; every ordinal must come back exactly
/// once (hard check). The one entity lookup of the write path's
/// [`resolve_entities`] and of the delete channel.
fn lookup_entities(
    conn: &Connection,
    class_id: i64,
    names: &[&str],
) -> Result<Vec<(String, Option<i64>)>, DbError> {
    use duckdb::types::Value;

    if let [name] = names {
        let row: (String, Option<i64>) = conn
            .prepare_cached(ENTITY_LOOKUP_ONE_SQL)?
            .query_row(params![name, class_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
        return Ok(vec![row]);
    }
    let n = names.len();
    let mut out: Vec<Option<(String, Option<i64>)>> = vec![None; n];
    if n == 0 {
        return Ok(Vec::new());
    }
    let names_list = Value::List(names.iter().map(|s| Value::Text(s.to_string())).collect());
    let ord_list = Value::List((0..n).map(|i| Value::BigInt(i as i64)).collect());
    let mut stmt = conn.prepare_cached(ENTITY_LOOKUP_SQL)?;
    let rows = stmt.query_map(params![names_list, ord_list, class_id], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<i64>>(2)?,
        ))
    })?;
    for row in rows {
        let (i, fold, id) = row?;
        let i = usize::try_from(i)
            .ok()
            .filter(|&i| i < n && out[i].is_none())
            .ok_or_else(|| DbError::Internal(format!("entity lookup: unexpected ordinal {i}")))?;
        out[i] = Some((fold, id));
    }
    out.into_iter()
        .map(|r| {
            r.ok_or_else(|| {
                DbError::Internal("entity lookup: a name came back without its row".into())
            })
        })
        .collect()
}

/// One entity name already resolved by an earlier segment of this commit.
pub(crate) struct CachedEntity {
    id: i64,
    /// DuckDB's `lower()` of the name, as the lookup returned it.
    fold: String,
    /// Minted (auto-created) by this commit.
    minted: bool,
}

/// The entities the segments of one commit resolved so far, keyed by class
/// id and the EXACT input spelling (never a Rust fold). Sound because no
/// entity is renamed or closed between the structural phase and the
/// deletes: a spelling that resolved once resolves to the same id for the
/// rest of the segments phase. An editor save of many parameters over few
/// entities then pays one lookup per entity, not one per segment. A closure
/// local of the retried commit.
pub(crate) type EntityCache = HashMap<(i64, String), CachedEntity>;

/// The columnar auto-create insert of [`resolve_entities`]; RETURNING yields
/// the rows in input order (hard-checked).
const ENTITY_INSERT_SQL: &str = "INSERT INTO entity (entity_class_id, name, valid_from_commit) \
     SELECT ?, unnest(?::VARCHAR[]), ? RETURNING id, name";

/// Resolve every entity name of one segment in one set-based round
/// (set_based_scalar_write_PLAN §3.4, D7): one lookup statement (none for
/// names an earlier segment of the commit resolved, see [`EntityCache`]), at
/// most one N-D probe, and one columnar insert per ≤ `LONG_INSERT_CHUNK`
/// missing names. This is the write path's one decision-B entity
/// auto-create site.
///
/// Names are resolved by DuckDB's `lower()` (Unicode). Two distinct input
/// spellings that DuckDB folds to the same name are ambiguous and rejected
/// with the same Api error `prepare_segment` raises for ASCII case-aliases
/// (critique C2: the up-front dedup folds ASCII only, so `"Ä"`/`"ä"` reach
/// here). Missing names are inserted in input order; their `AutoCreated`
/// warnings follow in the same order, as the per-entity resolver pushed
/// them. Never auto-creates into an N-D class (critique F10): an N-D
/// entity's identity is its member tuple, which the batch does not carry.
fn resolve_entities(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    class: &str,
    names: &[String],
    cache: &mut EntityCache,
    warnings: &mut Vec<Warning>,
) -> Result<Resolved, DbError> {
    use duckdb::types::Value;

    let n = names.len();
    let mut ids: Vec<Option<i64>> = vec![None; n];
    let mut folds: Vec<Option<String>> = vec![None; n];
    let mut minted = vec![false; n];
    let mut lookup: Vec<usize> = Vec::new();
    for (i, name) in names.iter().enumerate() {
        match cache.get(&(class_id, name.clone())) {
            Some(c) => {
                ids[i] = Some(c.id);
                folds[i] = Some(c.fold.clone());
                minted[i] = c.minted;
            }
            None => lookup.push(i),
        }
    }
    let looked_up: Vec<&str> = lookup.iter().map(|&i| names[i].as_str()).collect();
    for (&i, (fold, id)) in lookup
        .iter()
        .zip(lookup_entities(conn, class_id, &looked_up)?)
    {
        folds[i] = Some(fold);
        ids[i] = id;
    }

    // Fold collisions (critique C2), in input order.
    let mut first_by_fold: HashMap<&str, usize> = HashMap::with_capacity(n);
    for (i, fold) in folds.iter().enumerate() {
        let fold = fold.as_deref().unwrap_or_default();
        if let Some(&j) = first_by_fold.get(fold) {
            return Err(DbError::Api(format!(
                "entity names {:?} and {:?} resolve to the same entity \
                 (name resolution is case-insensitive); use one spelling per batch",
                names[j], names[i]
            )));
        }
        first_by_fold.insert(fold, i);
    }

    let missing: Vec<usize> = (0..n).filter(|&i| ids[i].is_none()).collect();
    if let Some(&m) = missing.first() {
        let ndims: i64 = conn
            .prepare_cached("SELECT count(*) FROM entity_class_dimension WHERE cur_c = ?")?
            .query_row(params![class_id], |r| r.get(0))?;
        if ndims > 0 {
            let name = &names[m];
            return Ok(Resolved::NdRefused {
                first_missing: m,
                err: DbError::Api(format!(
                    "cannot auto-create entity {name:?} in {ndims}-dimensional class {class:?}: \
                     an N-D entity needs explicit membership (entity_element rows); create it first"
                )),
            });
        }
        let mut stmt = conn.prepare_cached(ENTITY_INSERT_SQL)?;
        let mut last_id = i64::MIN;
        for chunk in missing.chunks(LONG_INSERT_CHUNK) {
            let list = Value::List(
                chunk
                    .iter()
                    .map(|&i| Value::Text(names[i].clone()))
                    .collect(),
            );
            let rows: Vec<(i64, String)> = stmt
                .query_map(params![class_id, list, commit_id], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?
                .collect::<Result<_, _>>()?;
            // Hard check (plan D10): the rows come back in input order with
            // strictly increasing ids, i.e. exactly what one insert per name
            // in that order would have minted.
            if rows.len() != chunk.len() {
                return Err(DbError::Internal(format!(
                    "entity insert: {} rows returned for {} names",
                    rows.len(),
                    chunk.len()
                )));
            }
            for (&i, (id, name)) in chunk.iter().zip(rows) {
                if name != names[i] || id <= last_id {
                    return Err(DbError::Internal(format!(
                        "entity insert: RETURNING out of input order at {name:?} (id {id})"
                    )));
                }
                last_id = id;
                ids[i] = Some(id);
                minted[i] = true;
                warnings.push(Warning {
                    kind: WarningKind::AutoCreated,
                    object_type: "entity".into(),
                    name: name.clone(),
                    id,
                });
            }
        }
    }
    for &i in &lookup {
        if let (Some(id), Some(fold)) = (ids[i], folds[i].clone()) {
            cache.insert(
                (class_id, names[i].clone()),
                CachedEntity {
                    id,
                    fold,
                    minted: minted[i],
                },
            );
        }
    }
    Ok(Resolved::Ids {
        ids: ids.into_iter().flatten().collect(),
        minted,
    })
}

/// What [`resolve_parameter`] returns: `(pd_id, parameter_type_id, shape,
/// declared set as read)`.
type ParameterResolution = (i64, i64, DeclaredType, Option<Vec<DbDeclared>>);

/// Resolve the parameter definition within `class_id`, auto-creating it (and its
/// `parameter_type` + `parameter_type_dimension` declaration) when absent. The
/// declared type SET is READ FROM THE DB for an existing definition (critique
/// S2; template_ir_plan D2(a)); only a freshly created definition takes its
/// declared type from the batch's inferred dimensions. Returns
/// `(pd_id, parameter_type_id, shape)` — the member id is the exactly-one
/// binding persisted on every value row (D11, 2026-09-18); auto-created /
/// auto-materialized declarations bind their freshly created row. The fourth
/// element is the declared set as read from the DB, `None` when this call
/// created it (what [`ResolveCache`] may keep).
fn resolve_parameter(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    name: &str,
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
    warnings: &mut Vec<Warning>,
) -> Result<ParameterResolution, DbError> {
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
            Ok((pd_id, pt_id, inferred_declared(inferred), None))
        } else {
            let matched = validate_declared(name, pd_id, &set, inferred, value_kind)?;
            Ok((pd_id, matched.pt_id, matched.shape, Some(set)))
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
        Ok((pd_id, pt_id, inferred_declared(inferred), None))
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
/// `(position, kind)` shape canon consumes. `pub(crate)`: the template
/// module's DB snapshot reads declared sets through the SAME reader/validation
/// as the write path (review item: no raw-string shadow readers).
#[derive(Clone)]
pub(crate) struct DbDeclared {
    pub(crate) pt_id: i64,
    pub(crate) value_kind: String,
    pub(crate) dim_names: Vec<String>,
    pub(crate) shape: DeclaredType,
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
        format!(
            "rank {}, indexed ({}), measure {}",
            inferred.len(),
            dims.join(", "),
            value_kind.tag()
        )
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
    let tag = if inferred.is_empty() {
        value_kind.tag()
    } else {
        "indexed"
    };
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
    set: &[DbDeclared],
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
        return Ok(set[ix].clone());
    }
    // Probable-damage hint: a stored tag that differs from the batch's only by
    // case, or sits outside the known tag vocabulary, points at import/hand
    // edits rather than a caller mistake. Matching itself stays case-sensitive.
    const KNOWN_TAGS: [&str; 7] = [
        "float",
        "int",
        "bool",
        "string",
        "date_time",
        "duration",
        "indexed",
    ];
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
        let mut raw_dims: Vec<(i8, String, String)> = Vec::new();
        let rows = dim_stmt.query_map(params![pt_id], |r| {
            Ok((
                r.get::<_, i8>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            raw_dims.push(row?);
        }
        out.push(assemble_declared(pt_id, value_kind, raw_dims)?);
    }
    Ok(out)
}

/// Turn one `parameter_type` row + its ordered dimension rows into a
/// [`DbDeclared`], applying THE row validation (kind-tag parse via
/// [`parse_index_kind`], position contiguity 1..=rank). Shared by the
/// per-definition reader above and the whole-DB batch reader below, so the
/// write path and the template snapshot cannot diverge on what a stored
/// declaration means.
fn assemble_declared(
    pt_id: i64,
    value_kind: String,
    raw_dims: Vec<(i8, String, String)>,
) -> Result<DbDeclared, DbError> {
    let mut positions: Vec<(u8, IndexKind)> = Vec::with_capacity(raw_dims.len());
    let mut dim_names: Vec<String> = Vec::with_capacity(raw_dims.len());
    for (pos, dname, kind) in raw_dims {
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
    Ok(DbDeclared {
        pt_id,
        value_kind,
        dim_names,
        shape: DeclaredType { positions },
    })
}

/// Batch form of [`read_declared_types`]: every definition's current declared
/// set in two scans (one per table, JOIN-free grouping in Rust), through the
/// SAME [`assemble_declared`] validation. Used by the template module's
/// vocabulary snapshot (its whole-DB read idiom); the per-write path keeps the
/// targeted single-definition reader.
pub(crate) fn read_all_declared_types(
    conn: &Connection,
) -> Result<HashMap<i64, Vec<DbDeclared>>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, parameter_definition_id, value_kind FROM parameter_type \
         WHERE valid_to_commit IS NULL ORDER BY parameter_definition_id, id",
    )?;
    let pts: Vec<(i64, i64, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<_, _>>()?;
    let mut dims_by_pt: HashMap<i64, Vec<(i8, String, String)>> = HashMap::new();
    let mut dstmt = conn.prepare(
        "SELECT parameter_type_id, position, dimension_name, kind \
         FROM parameter_type_dimension WHERE valid_to_commit IS NULL \
         ORDER BY parameter_type_id, position",
    )?;
    let drows = dstmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i8>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
        ))
    })?;
    for row in drows {
        let (pt_id, pos, name, kind) = row?;
        dims_by_pt.entry(pt_id).or_default().push((pos, name, kind));
    }
    let mut out: HashMap<i64, Vec<DbDeclared>> = HashMap::new();
    for (pt_id, pd_id, value_kind) in pts {
        let raw = dims_by_pt.remove(&pt_id).unwrap_or_default();
        out.entry(pd_id)
            .or_default()
            .push(assemble_declared(pt_id, value_kind, raw)?);
    }
    Ok(out)
}

/// Insert the `parameter_type` (+ its dimension rows) declaring the batch's
/// shape: `value_kind = 'indexed'` for rank ≥ 1 else the measure kind tag;
/// dimension names/kinds from the inferred columns (VMP Part 2/7). Returns the
/// created `parameter_type` id.
///
/// `pub(crate)` for the template loader (template_ir_plan §1.3): declared
/// type-set members are created through this one insert path, so the loader
/// and the auto-create path cannot drift. When `inferred` is non-empty the
/// `value_kind` argument is ignored (the row stores `'indexed'`).
pub(crate) fn create_parameter_type(
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
            params![
                pt_id,
                (i + 1) as i8,
                dname,
                index_kind_tag(*kind),
                commit_id
            ],
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ObjectDeleteOutcome, ObjectKind, StructuralKind, StructuralOutcome};

    fn noop() -> (WriteReport, NoopEvidence) {
        let report = WriteReport {
            commit_id: 9,
            values_unchanged: 2,
            axes_reused: 1,
            warnings: vec![Warning {
                kind: WarningKind::NothingToDelete { key_index: 0 },
                object_type: "value".into(),
                name: "x".into(),
                id: 0,
            }],
            structural: vec![StructuralOutcome {
                change_index: 0,
                kind: StructuralKind::Alternative,
                object_id: 2,
                effect: StructuralEffect::NoOp,
            }],
            object_deletes: vec![ObjectDeleteOutcome {
                key_index: 0,
                kind: ObjectKind::Alternative,
                result: ObjectDeleteResult::NotFound,
            }],
            ..WriteReport::default()
        };
        let ev = NoopEvidence {
            values: 2,
            deleted: 0,
            structural: 1,
            object_deletes: 1,
        };
        (report, ev)
    }

    /// The D1 predicate is an allow-list (critique C2): the all-no-op report
    /// passes, and every single deviation from it commits.
    #[test]
    fn wrote_nothing_is_an_allow_list() {
        let (r, ev) = noop();
        assert!(wrote_nothing(&r, &ev));
        type Bend = Box<dyn Fn(&mut WriteReport, &mut NoopEvidence)>;
        let bends: Vec<(&str, Bend)> = vec![
            ("a value written", Box::new(|r, _| r.values_written = 1)),
            ("a value not reported", Box::new(|_, e| e.values = 3)),
            ("a row written", Box::new(|r, _| r.rows_written = 1)),
            ("a row closed", Box::new(|r, _| r.rows_closed = 1)),
            ("an axis created", Box::new(|r, _| r.axes_created = 1)),
            (
                "a tier written",
                Box::new(|r, _| r.tiers.push(Tier::Scalar)),
            ),
            ("a delete applied", Box::new(|_, e| e.deleted = 1)),
            (
                "an auto-created referent",
                Box::new(|r, _| r.warnings[0].kind = WarningKind::AutoCreated),
            ),
            (
                "a structural change",
                Box::new(|r, _| r.structural[0].effect = StructuralEffect::Created),
            ),
            (
                "a structural outcome missing",
                Box::new(|_, e| e.structural = 2),
            ),
            (
                "an object deleted (or restricted)",
                Box::new(|r, _| {
                    r.object_deletes[0].result = ObjectDeleteResult::Restricted { blockers: vec![] }
                }),
            ),
            (
                "an object delete outcome missing",
                Box::new(|_, e| e.object_deletes = 2),
            ),
        ];
        for (what, bend) in bends {
            let (mut r, mut ev) = noop();
            bend(&mut r, &mut ev);
            assert!(!wrote_nothing(&r, &ev), "{what} must commit");
        }
    }
}
