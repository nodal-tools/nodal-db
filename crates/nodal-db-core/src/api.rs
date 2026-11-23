//! The public Arrow-in / plain-data-out write path. [`commit`] is the one
//! write entry point: every channel of a save (value writes, structural
//! changes, value and object deletes, purges, metadata) in ONE atomic,
//! conflict-retried commit, through name resolution (missing referents are
//! auto-created and reported), the declared-type lookup, `canon` → `tier` →
//! axis dedup → the `history` protocol. [`write_parameter`],
//! [`ingest_segments`] and [`delete_values`] are wrappers over it.
//! [`read_parameter`] reads a written parameter back through the SQL views.
//!
//! ## Batch column contract (logical-long)
//! `batch` has, in order:
//!   - column 0: `entity` — the entity **names** as `Utf8` (names, not ids,
//!     so missing entities can be auto-created). One value is written per
//!     distinct entity name.
//!   - columns `1..rank+1`: the `rank` dimension columns, in declared position
//!     order (position = column index). Arrow type fixes the index kind:
//!     `Timestamp(µs)` → datetime, `Int64` → integer, `Utf8` → label. The field
//!     **name** becomes the dimension name when a definition is auto-created.
//!   - last column: `value` — the measure. Arrow type fixes the measure kind:
//!     `Float64`/`Int64`/`Boolean`/`Utf8`/`Timestamp(µs)`.
//!
//! Rank 0 (scalar) is a two-column batch `(entity, value)`.
//!
//! The **library decides the tier**: callers never pick scalar / array /
//! long. Ordinal domains for label dimensions arrive by the in-process
//! `domains` arg and/or Arrow field metadata `nodal:label_domain` (a JSON array
//! of the ordered labels); both channels are accepted.
//!
//! ## Concurrency contract
//! Safe to call from multiple threads and processes on one DB file: writes are
//! serialized, in-process by `conn`'s per-path lock and cross-process by
//! DuckDB's file lock. Connections stay per-operation — never shared, never
//! long-lived.

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
use crate::entity_tuple::TupleIndex;
use crate::error::{
    CoveredWriteDetails, DbError, UndeclaredValueTypeDetails, WriteAndDeleteSameKeyDetails,
};
use crate::history::{
    self as history, keeps_content, resolve_axes, write_arrays, write_longs, write_scalars,
    ArrayWrite, DeleteWrite, LongWrite, ScalarWrite, ARRAY_ROUND_ELEMENTS, LONG_INSERT_CHUNK,
};
use crate::object_delete::{
    apply_object_delete, cascade_scope, prepare_object_deletes, preview_one, reaches_created,
    written_value_covered, PreparedObjectDelete, WrittenIds,
};
use crate::structural::{
    apply_structural, prepare_structural, refuse_template_stamped_renames, rewrite_name_documents,
    CallRenames, CallTouches,
};
use crate::text::{nfc, NameFolds, Normalized};
use crate::types::fold;
use crate::types::{
    CoveredWrites, CoveringDelete, DeclaredType, IndexKind, MetadataChange, MetadataEffect,
    ObjectDeleteKey, ObjectDeleteOpts, ObjectDeletePreview, ObjectDeleteResult, ObjectKind,
    ObjectRef, PurgePreview, PurgeRequest, ShapedData, StructuralChange, StructuralEffect,
    StructuralKind, Tier, ValueKey, ValueKind, Warning, WarningKind, WriteOptions, WriteReport,
};

/// One ingest segment: a `(class, parameter, alternative)` target plus its
/// logical-long batches and label domains (the segment of the Arrow-IPC bulk
/// format, `docs/dev/arrow_import_format.md`). Batches whose columns have the same Arrow types are ONE
/// logical table (concatenated); batches of different column types are
/// separate groups of one target — values of several declared types for
/// one parameter and alternative in one call — each group bound to the
/// declared member it instantiates, and an entity may appear in one group
/// only. Dense wire rows are expanded to logical-long *before* they reach
/// here (the importer expands, the core canonicalizes).
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
/// with a machine-readable [`Warning`] appended to the returned report.
/// Name resolution,
/// auto-creation, and every value write happen in one transaction, retried as a
/// whole on a write-write conflict.
///
/// `user` / `message` stamp the `commit` row this call creates.
///
/// A thin single-segment wrapper over [`ingest_segments`]:
/// the per-parameter body is shared, the transaction shape is identical.
/// It carries no metadata statement: rewriting a value that has metadata
/// links fails with [`DbError::ValueMetadataUnstated`]; use
/// [`commit`] with a keep / set / clear statement.
#[allow(clippy::too_many_arguments)]
pub fn write_parameter(
    db: &Path,
    class: &str,
    parameter: &str,
    alternative: &str,
    batch: RecordBatch,
    domains: &[LabelDomain],
    user: &str,
    message: &str,
    opts: WriteOptions,
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
/// with ONE `commit` row covering every value written (whole-invocation
/// atomicity: all segments commit or nothing persists). This is the core
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
    // A pure-write ingest is a `commit` with only the segments channel: the
    // write path and the retried-commit shape are shared, so writes-only and
    // mixed saves cannot drift.
    commit(
        db,
        ChangeSet {
            segments,
            ..ChangeSet::default()
        },
        user,
        message,
        opts,
    )
}

/// A typed delete target: one `(class, parameter, alternative, entity)` value,
/// all four given by NAME (resolved case-insensitively, never auto-created).
/// Carries no value, so there is nothing to canonicalize — deletes ride plain
/// owned strings, not the Arrow/`Segment` machinery.
#[derive(Clone, Debug)]
pub struct DeleteKey {
    pub class: String,
    pub parameter: String,
    pub alternative: String,
    pub entity: String,
}

/// Everything one atomic commit can carry, by channel: the argument of
/// [`commit`], the one write entry point. `Default` is the empty set, so a
/// caller names only the channels it uses and a channel added later does
/// not break its literal.
#[derive(Clone, Debug, Default)]
pub struct ChangeSet {
    /// Value writes: one [`Segment`] per `(class, parameter, alternative)`.
    pub segments: Vec<Segment>,
    /// Structural changes: creates, renames, scenario rankings,
    /// activations, memberships (applied first).
    pub structural: Vec<StructuralChange>,
    /// Value deletes, by name.
    pub deletes: Vec<DeleteKey>,
    /// Object deletes (entities, parameters, alternatives, classes,
    /// scenarios) with their cascade options.
    pub object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
    /// Purges: close out everything in a scope except what this call writes.
    pub purges: Vec<PurgeRequest>,
    /// The metadata channel: pool and link ops and the keep / set / clear
    /// statements for new value versions (applied last).
    pub metadata: Vec<MetadataChange>,
}

/// Commit a [`ChangeSet`] in ONE atomic, conflict-retried transaction
/// under one `commit` row. This is the write entry point: every other write
/// function ([`write_parameter`], [`ingest_segments`], [`delete_values`],
/// the verbs, template apply) and the Python skin go through it, so the
/// canonicalization pipeline exists once. A save is never split into
/// several calls: a failed second call would leave the first half
/// committed on its own.
///
/// `user` must be non-empty; `message` may be empty. A set that is empty in
/// every channel is refused as [`DbError::Api`].
///
/// **Order of the channels.** Inside one retried transaction and under one
/// commit id: structural changes, then value writes (segments), then value
/// deletes, then object deletes, then purges, then metadata.
///
/// - *Structural* items apply in order against the one open transaction,
///   so a value write, ranking or activation of the same call may target a
///   referent a structural item just created. Name-level guards reject
///   caller-contract problems (one set-ranks per scenario, contradictory
///   renames, an object delete of something the call creates) before any
///   database work. Per-item outcomes land in `report.structural`.
/// - *Segments* are validated and deduplicated up front; missing referents
///   (class, parameter, alternative, entities) are auto-created, each with
///   a [`WarningKind::AutoCreated`] warning.
/// - *Value deletes* resolve case-insensitively and lookup-only (never
///   auto-created). A missing referent or an already-absent value is a
///   warning ([`WarningKind::UnknownReferent`],
///   [`WarningKind::NothingToDelete`]) and a skip, never an error.
/// - *Object deletes* resolve lookup-only and run after the value deletes:
///   a value a value delete of this call closed is not closed or counted
///   twice. Per-request outcomes land in `report.object_deletes`.
/// - *Purges* resolve lookup-only after the structural changes (an unknown
///   or blank name, `Values.parameter` without its class, or a name a
///   rename of this call freed is refused). Each request closes everything
///   in its scope except exactly what the call itself writes, at each
///   scope's own level (see [`crate::types::PurgeScope`]); a row this call
///   wrote is never closed, and closed values follow their definition's
///   retention (or the call's `retain_content`). A request whose cascade
///   reaches objects outside every request's scope refuses the whole call
///   unless it sets `allow_outside_scope`. Outcomes land in
///   `report.purges`.
/// - *Metadata*: pool creates and revisions in item order, the keep / set
///   / clear statements and the value-link items, the object links, then
///   the pool deletes (RESTRICT judged on the post-call state). A value
///   that gets a new version in this call (not an unchanged write, not a
///   new value, not a deleted one) and has metadata links needs a
///   statement (`Scope::Value` over `Scope::Target` over `Scope::Save`) or
///   the whole call fails with [`DbError::ValueMetadataUnstated`]. A save-
///   or target-scope `Set` also annotates new values; a value-scope `Set` /
///   `Clear` also edits an unchanged value. Outcomes land in
///   `report.metadata` and `report.value_links`.
///
/// **Unchanged values and no-op calls.** A value equal to the stored one is
/// not rewritten (it counts in `report.values_unchanged`, not
/// `values_written`). A call that changes nothing (every value unchanged,
/// every delete a miss, every structural item `NoOp`, every object delete
/// `NotFound`, nothing auto-created, no purge closing anything) makes **no
/// commit**: the transaction is rolled back, the report says
/// `commit_id == 0`, the head (the polling cookie) does not move and the
/// commit message is not recorded. Commit ids may therefore skip, as after
/// a conflict retry.
///
/// **Writes a delete of the same call covers.** A value delete of a key the
/// same call writes is refused up front as [`DbError::WriteAndDeleteSameKey`],
/// whatever the content, and so is a value write that an object delete in
/// the same call covers (its entity or a container of it, its parameter
/// definition, alternative or class) as [`DbError::CoveredWrite`], whatever
/// the content, the tier or the delete's `cascade` option.
///
/// With `opts.covered_writes == CoveredWrites::DeleteWins` neither case is
/// refused: the delete wins. Inside the transaction, after the structural
/// changes and before the first segment, the deletes are resolved
/// lookup-only and every write they cover is dropped before it is
/// resolved, canonicalized or buffered (so a dropped write auto-creates
/// nothing), each reported as a [`WarningKind::DroppedByDelete`] warning.
/// The delete wins only when it takes effect: after the deletes ran, every
/// dropped write needs a covering delete that did (a value delete; an
/// object delete that ended `Deleted` or whose target did not exist), else
/// the whole call is refused as [`DbError::Api`] and rolled back (a
/// RESTRICT delete that ended `Restricted` would otherwise lose the write
/// while the object survives). A call whose writes are all dropped and
/// whose deletes delete nothing makes no commit.
///
/// Under either policy, an object delete that reaches an object a
/// structural change of the same call creates (or a child of it) is
/// refused: by name before the transaction, and on the resolved ids after
/// the structural changes ran.
pub fn commit(
    db: &Path,
    set: ChangeSet,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    let ChangeSet {
        mut segments,
        mut structural,
        mut deletes,
        mut object_deletes,
        mut purges,
        mut metadata,
    } = set;
    // Every commit must carry a real author; message is the caller's choice
    // (may be empty) — jkiviluo 2026-08-18.
    if user.trim().is_empty() {
        return Err(DbError::Api("user must be non-empty".into()));
    }
    if segments.is_empty()
        && structural.is_empty()
        && deletes.is_empty()
        && object_deletes.is_empty()
        && purges.is_empty()
        && metadata.is_empty()
    {
        return Err(DbError::Api(
            "nothing to do: no segments, no structural changes, no deletes, no object deletes, \
             no purges, no metadata"
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
    for r in &mut purges {
        crate::purge::nfc_request(r, &mut lookups);
    }
    crate::metadata::nfc_items(&mut metadata);
    // Two same-commit writes to one (class, parameter, alternative) key have no
    // defined history semantics (spec §4.2, E7 class); compare by the same
    // `lower()` fold every core name resolver applies. DuckDB `lower()` is
    // Unicode, and alternative names carry no ASCII charset CHECK, so the fold
    // must be Unicode too — an ASCII fold lets Ä/ä-aliased targets resolve to
    // one row and die as a raw CHECK error mid-transaction (critique F3).
    // `types::fold` is the Rust copy of DuckDB's `lower()` (engine rule 8);
    // `write_prepared` still repeats the check on the resolved ids as the
    // second line; this one fails early with no DB work.
    let mut seen: HashMap<(String, String, String), ()> = HashMap::new();
    for s in &segments {
        let key = (fold(&s.class), fold(&s.parameter), fold(&s.alternative));
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
    // Purges: blank names and a parameter without its class (pure).
    crate::purge::validate_requests(&purges)?;
    // Metadata: identifiers, dates, policies, one statement per scope, one
    // item per edge and per object (pure).
    crate::metadata::prepare(&metadata)?;
    let bulk_set = crate::metadata::has_bulk_set(&metadata);
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
        // The values that get a new version are recorded for the metadata
        // statements: of the targets that carry value links (read once per
        // attempt), or of every target under a save- or target-scope set.
        let linked = if prepared.is_empty() {
            HashSet::new()
        } else {
            crate::metadata::linked_targets(conn)?
        };
        let had_links = !linked.is_empty() || crate::metadata::has_value_links(conn)?;
        // A save-scope set (or a target-scope set on a target this call
        // creates) annotates every new value; a target-scope set only its
        // target's.
        let set_targets = if bulk_set && !prepared.is_empty() {
            crate::metadata::set_targets(conn, &metadata)?
        } else {
            Some(HashSet::new())
        };
        buffers.track = match set_targets {
            None => Track::All,
            Some(t) => {
                let all: HashSet<(i64, i64)> = linked.into_iter().chain(t).collect();
                if all.is_empty() {
                    Track::Off
                } else {
                    Track::Targets(all)
                }
            }
        };
        let mut entities = EntityCache::new();
        // The class / parameter / alternative resolutions of this attempt,
        // keyed by exact spelling (set_based_long_and_delete_PLAN §10 C11).
        // A closure local like the buffers: a conflict retry rebuilds it,
        // so no id read or minted in a rolled-back attempt is reused.
        let mut resolved_names = ResolveCache::new();
        // The resolved (parameter definition, alternative) of every segment
        // so far: the second line behind the up-front duplicate-target
        // check (`types::fold` is proven equal to DuckDB's `lower()`, engine
        // rule 8; this holds even if that proof were bypassed). The deferred
        // writes rely on distinct targets.
        let mut targets: HashSet<(i64, i64)> = HashSet::new();
        // Structural changes run FIRST (§4.1): a value write / rank / activation
        // later in this same commit may target a referent an earlier structural
        // item just created, and items apply sequentially against the one open
        // txn, so create → use resolves. Caller-given vec order is honored.
        let mut structural_folds = NameFolds::default();
        // What this attempt's renames did (renamed-away names, original
        // names, template-stamp candidates): a closure local, so a conflict
        // retry rebuilds it.
        let mut calls = CallRenames::default();
        // What the structural items name, by id, for the purges' survivors
        // (a closure local, like `calls`; idle unless the call purges).
        let mut touches = CallTouches::new(!purges.is_empty());
        // The live element tuples of the N-D classes this attempt creates
        // in, loaded per class on first use (entity_rename_PLAN §5.3; a
        // closure local, like `calls`).
        let mut tuples = TupleIndex::for_changes(&structural);
        let mut i = 0;
        while i < structural.len() {
            // A run of RenameEntity items long enough for the set path
            // (entity_rename_PLAN §3.2) is applied as one batch.
            if crate::entity_rename_set::takes_set_path(&structural[i..]) {
                let n = crate::entity_rename_set::rename_run_len(&structural[i..]);
                crate::entity_rename_set::apply_rename_run(
                    conn,
                    commit_id,
                    i,
                    &structural[i..i + n],
                    &mut report,
                    &mut structural_folds,
                    &mut calls,
                    &mut touches,
                    &mut tuples,
                )?;
                i += n;
                continue;
            }
            apply_structural(
                conn,
                commit_id,
                i,
                &structural[i],
                &mut report,
                &mut structural_folds,
                &mut calls,
                &mut touches,
                &mut tuples,
            )?;
            i += 1;
        }
        report
            .warnings
            .extend(structural_folds.warnings("structural", ""));
        // Renames on a template-stamped database need `allow_on_template`
        // (OD7); every refused item is listed in one typed error.
        refuse_template_stamped_renames(conn, &calls, &report)?;
        // An object delete may not reach an object a structural change of
        // this call created or revised (under either policy): the
        // same-commit guard would leave the row this call wrote live. The
        // resolved-id repeat of the name-level guard in `prepare_structural`,
        // and the real guarantee: it also sees cascade children and names
        // the prepare check cannot match (class_param_rename_PLAN §4.5 d).
        let mut revised: HashSet<(ObjectKind, i64)> = report
            .structural
            .iter()
            .filter_map(|o| {
                let kind = match o.kind {
                    StructuralKind::Alternative => ObjectKind::Alternative,
                    StructuralKind::Scenario => ObjectKind::Scenario,
                    StructuralKind::Entity => ObjectKind::Entity,
                    StructuralKind::Class => ObjectKind::Class,
                    StructuralKind::Parameter => ObjectKind::Parameter,
                    _ => return None,
                };
                // A rename or description revision writes a successor row
                // (valid_from = this commit), entities included
                // (`RenameEntity`). Activation, membership and ranking
                // outcomes carry their own kinds: what their rows reference
                // is added below from `touches.row_refs`.
                let wrote_row = matches!(
                    o.effect,
                    StructuralEffect::Created
                        | StructuralEffect::Renamed
                        | StructuralEffect::Updated
                );
                wrote_row.then_some((kind, o.id))
            })
            .collect();
        // An activation, membership or ranking row this call wrote points
        // at its entity (group and member), scenario and alternative; a
        // delete whose cascade reaches one of them (a class delete, an
        // element entity cascading into its N-D container, a delete by
        // element tuple) would leave that row live over a closed object.
        revised.extend(touches.row_refs.iter().copied());
        for p in &prepared_object_deletes {
            if let Some((kind, id)) = reaches_created(conn, &p.key, &revised)? {
                return Err(DbError::Api(format!(
                    "object-delete key {} ({} {}) reaches the {} id {id} that a \
                     structural change of this call creates or revises (or sets \
                     an activation, membership or scenario ranking on): create, \
                     rename, revise or set an object and delete it in separate calls",
                    p.key_index,
                    p.key.kind.tag(),
                    object_target_label(&p.key),
                    kind.tag()
                )));
            }
        }
        // References to a name a rename of this call freed (OD5): a segment
        // would auto-create a new, empty object under the old name and a
        // delete would find nothing. Refused before any write.
        if !calls.frees_no_name() {
            refuse_renamed_away_references(
                conn,
                &calls,
                &prepared,
                &prepared_deletes,
                &prepared_object_deletes,
                &metadata,
            )?;
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
                // that ran mid-segment) keeps its variant too, and a typed
                // undeclared-type refusal keeps its variant, gaining the
                // segment's index (and, for several segments, the prefix).
                if let DbError::UndeclaredValueType(mut d) = e {
                    d.segment_index = i;
                    if multi {
                        d.message = format!(
                            "segment {} ({}:{}): {}",
                            i + 1,
                            p.class,
                            p.parameter,
                            d.message
                        );
                    }
                    return DbError::UndeclaredValueType(d);
                }
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
        // Purge names resolve after the writes (purge plan §3.5.2; review):
        // a class, parameter or alternative a segment of this call
        // auto-created is known (its pre-call scope is empty). An unknown or
        // renamed-away name refuses the whole call (it rolls back).
        let mut purge_folds = NameFolds::default();
        let purge_scopes = crate::purge::resolve_requests(conn, &purges, &calls, &mut purge_folds)?;
        report.warnings.extend(purge_folds.warnings("purge", ""));
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
        let mut object_folds = NameFolds::default();
        for p in &prepared_object_deletes {
            apply_object_delete(conn, commit_id, p, &mut report, &mut object_folds)?;
        }
        report
            .warnings
            .extend(object_folds.warnings("object_delete", ""));
        // An object delete that closed a class or parameter definition
        // leaves a template-stamped database unconverged: its certified
        // content hash goes, so the next apply replans and meets the
        // removed-locally blocker (purge plan §3.8, V-B2).
        // A class or parameter rename (`allow_on_template`) deliberately
        // keeps the hash: the renamed-away blocker (OD7) guards a CHANGED
        // template, and a re-apply of the same template short-circuits
        // harmlessly (re-creating the old name is what that blocker stops).
        // Read from the rows, not the outcomes: only a pre-call class or
        // definition whose id has no live row left counts.
        let deleted_vocabulary = !prepared_object_deletes.is_empty()
            && crate::purge::closed_vocabulary(conn, commit_id)?;
        if deleted_vocabulary {
            crate::property::clear_core(conn, commit_id, "template_content_hash")?;
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
        // Purges run last, under the same commit (purge plan §3.5.1).
        let purged_vocabulary = crate::purge::run(
            conn,
            commit_id,
            &crate::purge::PurgeCall {
                requests: &purges,
                scopes: &purge_scopes,
                touches: &touches,
                written: &buffers.keys,
                // A purge keeps content like a value delete: the call's
                // `retain_content` (else each definition's own setting).
                retain_content: opts.retain_content,
            },
            &mut report,
        )?;
        // A purge that closed (or unbound) vocabulary leaves a stamped
        // database unconverged, as an object delete does (V-B2).
        if purged_vocabulary {
            crate::property::clear_core(conn, commit_id, "template_content_hash")?;
        }
        // Metadata runs last (metadata_write plan §5): pool, statements and
        // value links, object links, pool deletes. Its resolutions are
        // fresh (closure locals), never the segment phase's caches.
        if !metadata.is_empty() || !buffers.changed.is_empty() {
            crate::metadata::apply_channel(
                conn,
                commit_id,
                &crate::metadata::ChannelCall {
                    items: &metadata,
                    changed: &buffers.changed,
                    covered_writes: opts.covered_writes,
                    had_links,
                },
                &mut report,
            )?;
        }
        // The name-keyed metadata documents follow the renames and drop
        // what this call closed or unbound, once per document per call
        // (OD8, B1; purge plan §3.7, V-S2), at the end of the call: every
        // channel has run, so one pass sees all it must follow.
        rewrite_name_documents(
            conn,
            commit_id,
            &calls,
            purged_vocabulary || deleted_vocabulary,
        )?;
        // A closed refinement class leaves the core's list in the same
        // commit (one writer: the core).
        if purged_vocabulary || deleted_vocabulary {
            crate::verbs::prune_refinement_classes(conn, commit_id)?;
        }
        // `tiers` lists the tiers written; the canonicalization noted every
        // value's tier, unchanged ones included.
        report.tiers.retain(|t| buffers.written_tiers.contains(t));
        let evidence = NoopEvidence {
            values: buffers.values,
            deleted,
            structural: structural.len(),
            object_deletes: prepared_object_deletes.len(),
            purges: purges.len(),
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
            ..
        } => {
            n.nfc_in_place(class);
            if let Some(name) = name {
                n.nfc_in_place(name);
            }
            elements.iter_mut().for_each(|e| n.nfc_in_place(e));
        }
        StructuralChange::CreateClass { name, dims, .. } => {
            n.nfc_in_place(name);
            dims.iter_mut().for_each(|d| n.nfc_in_place(d));
        }
        StructuralChange::RenameClass {
            current, new_name, ..
        } => {
            n.nfc_in_place(current);
            n.nfc_in_place(new_name);
        }
        StructuralChange::RenameParameter {
            class,
            current,
            new_name,
            ..
        }
        | StructuralChange::RenameEntity {
            class,
            current,
            new_name,
            ..
        } => {
            n.nfc_in_place(class);
            n.nfc_in_place(current);
            n.nfc_in_place(new_name);
        }
        StructuralChange::CreateParameter {
            class,
            name,
            types,
            value_list,
            ..
        } => {
            n.nfc_in_place(class);
            n.nfc_in_place(name);
            crate::template::ir::nfc_dim_names(types, |d| n.nfc_in_place(d));
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

/// Refuse every segment, value-delete key, object-delete key and metadata
/// item of the call that addresses a class, parameter, alternative,
/// scenario or entity by a name a rename earlier in the call freed and that
/// does not resolve now. A same-call create of the old name makes it
/// resolve again (to the new object), and a case-only rename frees no
/// name, so neither is refused. Runs after the structural changes and
/// before the first value write. Segment entity labels are checked only
/// for the classes with an entity renamed in the call.
fn refuse_renamed_away_references(
    conn: &Connection,
    calls: &CallRenames,
    segments: &[PreparedSegment],
    deletes: &[(usize, DeleteKey)],
    object_deletes: &[PreparedObjectDelete],
    metadata: &[MetadataChange],
) -> Result<(), DbError> {
    let refuse = |what: String, kind: &str, old: &str, to: &str| {
        DbError::Api(format!(
            "{what}: {kind} {old:?} was renamed to {to:?} earlier in this call; address it \
             by its new name (the old name would create a new, empty {kind} or find nothing)"
        ))
    };
    // (class, parameter, alternative) of a segment or a value-delete key.
    let check_value_key = |what: String, class: &str, parameter: &str, alternative: &str| {
        if let Some(to) = calls.renamed_class(conn, class)? {
            return Err(refuse(what, "class", class, &to));
        }
        if let Some(to) = calls.renamed_parameter(conn, class, parameter)? {
            return Err(refuse(what, "parameter", parameter, &to));
        }
        if let Some(to) = calls.renamed_alternative(conn, alternative)? {
            return Err(refuse(what, "alternative", alternative, &to));
        }
        Ok::<(), DbError>(())
    };
    // An entity of `class` (as it resolves now) by a renamed-away name.
    // Class names resolve once per fold (`types::fold` is DuckDB's
    // `lower()`, engine rule 8), only when an entity was renamed.
    let renamed_classes = calls.entity_rename_classes();
    let class_ids: std::cell::RefCell<HashMap<String, Option<i64>>> = Default::default();
    let class_id = |class: &str| -> Result<Option<i64>, DbError> {
        let k = fold(class);
        if let Some(id) = class_ids.borrow().get(&k) {
            return Ok(*id);
        }
        let id = lookup_entity_class(conn, class)?;
        class_ids.borrow_mut().insert(k, id);
        Ok(id)
    };
    let renamed_entity = |class: &str, name: &str| -> Result<Option<String>, DbError> {
        if renamed_classes.is_empty() {
            return Ok(None);
        }
        match class_id(class)? {
            Some(cid) if renamed_classes.contains(&cid) => calls.renamed_entity_of(conn, cid, name),
            _ => Ok(None),
        }
    };
    let check_entity = |what: &str, class: &str, name: &str| {
        if let Some(to) = renamed_entity(class, name)? {
            return Err(refuse(what.to_string(), "entity", name, &to));
        }
        Ok::<(), DbError>(())
    };
    for (i, s) in segments.iter().enumerate() {
        let what = format!("segment {} ({}:{})", i + 1, s.class, s.parameter);
        check_value_key(what.clone(), &s.class, &s.parameter, &s.alternative)?;
        if renamed_classes.is_empty() {
            continue;
        }
        let Some(cid) = class_id(&s.class)? else {
            continue;
        };
        if !renamed_classes.contains(&cid) {
            continue;
        }
        for name in &s.names {
            if let Some(to) = calls.renamed_entity_of(conn, cid, name)? {
                return Err(refuse(what, "entity", name, &to));
            }
        }
    }
    for (i, k) in deletes {
        let what = format!("delete key {i}");
        check_value_key(what.clone(), &k.class, &k.parameter, &k.alternative)?;
        check_entity(&what, &k.class, &k.entity)?;
    }
    for p in object_deletes {
        let what = format!("object-delete key {}", p.key_index);
        if let Some(class) = &p.key.class {
            if let Some(to) = calls.renamed_class(conn, class)? {
                return Err(refuse(what, "class", class, &to));
            }
        }
        let ObjectRef::Name(name) = &p.key.target else {
            continue;
        };
        let renamed = match (p.key.kind, &p.key.class) {
            (ObjectKind::Class, _) => calls.renamed_class(conn, name)?,
            (ObjectKind::Parameter, Some(class)) => calls.renamed_parameter(conn, class, name)?,
            (ObjectKind::Alternative, _) => calls.renamed_alternative(conn, name)?,
            (ObjectKind::Scenario, _) => calls.renamed_scenario(conn, name)?,
            _ => None,
        };
        if let Some(to) = renamed {
            return Err(refuse(what, p.key.kind.tag(), name, &to));
        }
    }
    // Entity object deletes: by name, and every element of a tuple address
    // (of the dimension's class, or of any member class of a class-set
    // dimension).
    if !calls.entities.is_empty() {
        for p in object_deletes {
            let (ObjectKind::Entity, Some(class)) = (p.key.kind, &p.key.class) else {
                continue;
            };
            let what = format!("object-delete key {}", p.key_index);
            match &p.key.target {
                ObjectRef::Name(name) => check_entity(&what, class, name)?,
                ObjectRef::Elements(els) => {
                    let Ok(shape) = crate::structural::entity_class_shape(conn, class) else {
                        continue;
                    };
                    for ((_, dc, ds), el) in shape.dims.iter().zip(els) {
                        let classes: Vec<i64> = match (dc, ds) {
                            (Some(c), _) => vec![*c],
                            (None, Some(set)) => crate::structural::class_set_members(conn, *set)?,
                            (None, None) => Vec::new(),
                        };
                        for c in classes {
                            if let Some(to) = calls.renamed_entity_of(conn, c, el)? {
                                return Err(refuse(what, "entity", el, &to));
                            }
                        }
                    }
                }
            }
        }
    }
    // Metadata items: subjects, values and target scopes (R-3).
    use crate::types::{Scope, Subject};
    for (i, it) in metadata.iter().enumerate() {
        let what = format!("metadata item {i}");
        let mut values: Vec<(&str, &str, &str, &str)> = Vec::new();
        let subject = match it {
            MetadataChange::Link { subject, .. } | MetadataChange::Unlink { subject, .. } => {
                Some(subject)
            }
            MetadataChange::LinkValue { value, .. } | MetadataChange::UnlinkValue { value, .. } => {
                values.push((
                    &value.class,
                    &value.parameter,
                    &value.alternative,
                    &value.entity,
                ));
                None
            }
            MetadataChange::ValueMetadata { scope, .. } => {
                match scope {
                    Scope::Save => {}
                    Scope::Target {
                        class,
                        parameter,
                        alternative,
                    } => values.push((class, parameter, alternative, "")),
                    Scope::Value(v) => {
                        values.push((&v.class, &v.parameter, &v.alternative, &v.entity))
                    }
                }
                None
            }
            _ => None,
        };
        for (c, p, a, e) in values {
            check_value_key(what.clone(), c, p, a)?;
            if !e.is_empty() {
                check_entity(&what, c, e)?;
            }
        }
        let Some(s) = subject else { continue };
        let hit = match s {
            Subject::Class(n) => calls.renamed_class(conn, n)?.map(|t| ("class", n, t)),
            Subject::Alternative(n) => calls
                .renamed_alternative(conn, n)?
                .map(|t| ("alternative", n, t)),
            Subject::Scenario(n) => calls.renamed_scenario(conn, n)?.map(|t| ("scenario", n, t)),
            Subject::Entity { class, name } => match calls.renamed_class(conn, class)? {
                Some(t) => Some(("class", class, t)),
                None => renamed_entity(class, name)?.map(|t| ("entity", name, t)),
            },
            Subject::Parameter { class, name } => match calls.renamed_class(conn, class)? {
                Some(t) => Some(("class", class, t)),
                None => calls
                    .renamed_parameter(conn, class, name)?
                    .map(|t| ("parameter", name, t)),
            },
            _ => None,
        };
        if let Some((kind, old, to)) = hit {
            return Err(refuse(what, kind, old, &to));
        }
    }
    Ok(())
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

/// What a `commit` attempt submitted and what its deletes did, for
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
    /// Purge requests submitted (each must have closed nothing).
    purges: usize,
}

/// Did this call provably change nothing? A false "nothing" would roll back real writes, so this is
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
        purges,
        metadata,
        value_links,
        // Not a write: the read-surface refresh runs in its own transaction.
        read_surface_warning: _,
    } = report;
    let warnings_ok = warnings.iter().all(|w| match w.kind {
        WarningKind::AutoCreated => false,
        // Normalizing a name or label is not a write by itself.
        WarningKind::TimezoneNormalized { .. }
        | WarningKind::TextNormalized { .. }
        // Resolving a reference by the name fold is not a write by itself.
        | WarningKind::NameFolded { .. }
        // A size notice is not a write.
        | WarningKind::PurgeLarge { .. }
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
        && purges.len() == ev.purges
        && purges
            .iter()
            .all(|p| p.closed_out.total_rows == 0 && p.unbound == 0)
        && metadata.iter().all(|m| {
            matches!(
                m.effect,
                MetadataEffect::NoOp | MetadataEffect::Restricted | MetadataEffect::DroppedByDelete
            )
        })
        && value_links.set == 0
        && value_links.cleared == 0
        && value_links.kept == 0
        && value_links.carried == 0
}

/// a call that writes a value and also deletes it is
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
        let (c, pa, a) = (fold(&p.class), fold(&p.parameter), fold(&p.alternative));
        for n in &p.names {
            written.insert((c.clone(), pa.clone(), a.clone(), fold(n)));
        }
    }
    for (i, k) in deletes {
        let fold = (
            fold(&k.class),
            fold(&k.parameter),
            fold(&k.alternative),
            fold(&k.entity),
        );
        if written.contains(&fold) {
            return Err(same_key_error(*i, k, None));
        }
    }
    Ok(())
}

/// a call that writes a value and also deletes an object
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
        let c = fold(&p.class);
        alts.insert(fold(&p.alternative));
        params.insert((c.clone(), fold(&p.parameter)));
        for n in &p.names {
            entities.insert((c.clone(), fold(n)));
        }
        classes.insert(c);
    }
    for od in object_deletes {
        let name = match &od.key.target {
            ObjectRef::Name(n) => fold(n),
            // An element tuple resolves only in the DB (the commit-time check).
            ObjectRef::Elements(_) => continue,
        };
        let class = od.key.class.as_deref().map(fold);
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

/// The [`DbError::CoveredWrite`] refusal; `ids` names the covered
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
///     fold of all four names, which is how both sides resolve; or
///   - an object delete names its alternative, class, (class, parameter) or
///     (class, entity), compared by the same fold: this catches a referent
///     that does not exist yet and that the write would auto-create; or
///   - the resolved entity, parameter definition or alternative of the
///     write lies in an object delete's full cascade closure
///     ([`cascade_scope`], whatever `cascade` says), which adds containers,
///     element-tuple targets and a class's children.
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
            Some(cid) => lookup_entities(conn, cid, &names)?
                .into_iter()
                .map(|(f, e)| (f, e.map(|e| e.id)))
                .collect(),
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

/// The [`DbError::WriteAndDeleteSameKey`] refusal; `ids` names the
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
/// retried commit — the deletes-only wrapper over [`commit`]. Missing
/// referents and already-absent values are machine-readable warnings and
/// skipped, never a hard error.
pub fn delete_values(
    db: &Path,
    keys: Vec<DeleteKey>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    commit(
        db,
        ChangeSet {
            deletes: keys,
            ..ChangeSet::default()
        },
        user,
        message,
        opts,
    )
}

/// Apply one or more structural (create/rename/set-ranks/activation/membership)
/// changes in ONE atomic, conflict-retried commit — the structural-only wrapper
/// over [`commit`]. Per-item outcomes
/// land in `report.structural`.
pub fn commit_structural(
    db: &Path,
    changes: Vec<StructuralChange>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    commit(
        db,
        ChangeSet {
            structural: changes,
            ..ChangeSet::default()
        },
        user,
        message,
        opts,
    )
}

/// Delete (append-only close-out) one or more OBJECTS in ONE atomic, conflict-
/// retried commit — the objects-only wrapper over [`commit`]. Each request is a typed [`ObjectDeleteKey`] plus its
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
    commit(
        db,
        ChangeSet {
            object_deletes,
            ..ChangeSet::default()
        },
        user,
        message,
        opts,
    )
}

/// Purge: close out every item of each request's scope in
/// ONE atomic, conflict-retried commit — the purges-only wrapper over
/// [`commit`]. A purge keeps history (as-of reads see
/// everything) and never shrinks the file. Outcomes land in
/// `report.purges`.
pub fn purge(
    db: &Path,
    requests: Vec<PurgeRequest>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    commit(
        db,
        ChangeSet {
            purges: requests,
            ..ChangeSet::default()
        },
        user,
        message,
        opts,
    )
}

/// Read-only preview of each purge request: what it would
/// close (`objects`, `cascade`: rows counted once per table), its reach
/// outside its scope and whether it closes vocabulary of a template-stamped
/// database, through the same sets and statements the commit runs, on one
/// fresh RO connection opened and closed per call.
/// Nothing is written, so no survivor spares anything: the counts are upper
/// bounds of a call that also writes. Names are refused as the commit
/// refuses them.
pub fn preview_purge(
    db: &Path,
    mut requests: Vec<PurgeRequest>,
) -> Result<Vec<PurgePreview>, DbError> {
    let mut lookups = Normalized::default();
    for r in &mut requests {
        crate::purge::nfc_request(r, &mut lookups);
    }
    crate::purge::validate_requests(&requests)?;
    with_read_plain(db, |conn| {
        crate::property::require_current(conn)?;
        crate::purge::preview(conn, &requests)
    })
}

/// Read-only preview of each object delete:
/// what a `cascade:true` would close out (`cascade`) and what a `cascade:false`
/// would block on (`blockers`) — the SAME dependency rows in two framings,
/// driven by the SAME leg registry the commit path uses, so preview and commit
/// cannot drift by *implementation*. It is nonetheless a best-effort snapshot:
/// the commit-time [`crate::types::ObjectDeleteOutcome`] is authoritative, and a
/// concurrent writer between preview and commit can still move the counts.
/// Runs under one fresh RO connection, opened and closed per call
/// — cheap enough to call every frame the dialog is open.
pub fn preview_object_deletes(
    db: &Path,
    mut keys: Vec<ObjectDeleteKey>,
) -> Result<Vec<ObjectDeletePreview>, DbError> {
    let mut lookups = Normalized::default();
    for k in &mut keys {
        nfc_object_delete_key(k, &mut lookups);
    }
    with_read_plain(db, |conn| {
        crate::property::require_current(conn)?;
        keys.iter()
            .enumerate()
            .map(|(i, k)| preview_one(conn, i, k))
            .collect()
    })
}

/// Validate delete keys purely (no DB): every component non-empty, and — over
/// the full 4-tuple folded to `lower()` — reject fold-colliding distinct
/// spellings while silently deduping exact-identical keys. The
/// surviving keys carry their original `deletes`-vec index so a warning maps
/// back to the caller's key.
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
            fold(&k.class),
            fold(&k.parameter),
            fold(&k.alternative),
            fold(&k.entity),
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
    /// DuckDB's `lower()` folds together): nothing left to delete.
    Repeat,
}

/// Resolve every `DeleteKey` (lookup-only, case-insensitive: never
/// auto-create) and close out their live values under `commit_id` in one
/// set-based round. The class,
/// parameter and alternative lookups are today's point lookups, memoized per
/// exact spelling (nothing renames or creates them during the delete
/// phase); the entity names of each class resolve in one lookup
/// ([`lookup_entities`]). The values close through `history::delete_values`.
///
/// Per key, in key order, as one per-key delete each: the first missing
/// level (class, parameter, alternative, entity) yields
/// [`WarningKind::UnknownReferent`] and a skip; a resolved key with no live
/// value yields [`WarningKind::NothingToDelete`]. Two keys that resolve to
/// one value (the up-front fold dedup makes that unreachable; this is the
/// second line): the first deletes, the later ones get `NothingToDelete`,
/// as a second per-key delete would.
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
    let mut classes: HashMap<&str, Option<Found>> = HashMap::new();
    let mut params: HashMap<(i64, &str), Option<Found>> = HashMap::new();
    let mut alts: HashMap<&str, Option<Found>> = HashMap::new();
    // References that resolved to a differently spelled existing object
    // (`NameFolded`, one warning per referent table of the deletes).
    let mut folds = NameFolds::default();
    // Per key: Err(unknown level + name) or Ok((class, pd, alt)).
    type Partial = Result<(i64, i64, i64), (&'static str, String)>;
    let mut partial: Vec<Partial> = Vec::with_capacity(deletes.len());
    for (_, dk) in deletes {
        let class = match classes.get(dk.class.as_str()) {
            Some(f) => f.clone(),
            None => {
                let f = find_entity_class(conn, &dk.class)?;
                classes.insert(&dk.class, f.clone());
                f
            }
        };
        let Some(class_id) = class.map(|f| f.noted(&mut folds, "entity_class", &dk.class)) else {
            partial.push(Err(("entity_class", dk.class.clone())));
            continue;
        };
        let pd = match params.get(&(class_id, dk.parameter.as_str())) {
            Some(f) => f.clone(),
            None => {
                let f = find_parameter(conn, class_id, &dk.parameter)?;
                params.insert((class_id, &dk.parameter), f.clone());
                f
            }
        };
        let Some(pd_id) = pd.map(|f| f.noted(&mut folds, "parameter_definition", &dk.parameter))
        else {
            partial.push(Err(("parameter_definition", dk.parameter.clone())));
            continue;
        };
        let alt = match alts.get(dk.alternative.as_str()) {
            Some(f) => f.clone(),
            None => {
                let f = find_alternative(conn, &dk.alternative)?;
                alts.insert(&dk.alternative, f.clone());
                f
            }
        };
        let Some(alt_id) = alt.map(|f| f.noted(&mut folds, "alternative", &dk.alternative)) else {
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
    let mut entities: HashMap<(i64, &str), Option<Found>> = HashMap::new();
    for class_id in class_order {
        let names = &by_class[&class_id];
        for (name, (_, found)) in names.iter().zip(lookup_entities(conn, class_id, names)?) {
            entities.insert((class_id, name), found);
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
        let Some(entity_id) = entities[&(class_id, dk.entity.as_str())]
            .clone()
            .map(|f| f.noted(&mut folds, "entity", &dk.entity))
        else {
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
        // Fold the definition's retain_content into the options unless
        // the caller already overrode it: else a delete on a
        // retain_content=FALSE definition would silently keep the
        // closed-out payload (VMP Part 5).
        let effective_retain = match opts.retain_content {
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
            retain_content: Some(effective_retain),
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

    report.warnings.extend(folds.warnings("delete", ""));
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
            level: crate::types::object_type_tag(level).into(),
        },
        object_type: crate::types::object_type_tag(level).into(),
        name: name.into(),
        id: 0,
    });
}

// Lookup-only resolvers: the FIRST branch of each write-path resolver (§2), via
// `opt_found` — the case-insensitive current-row lookup, minus the auto-create
// tail. Each statement returns the row's id and its stored spelling (the
// `find_*` form, for the `NameFolded` warning); the `lookup_*` form keeps the
// id only.

/// A current row a name lookup found: its id and its name as stored. The
/// lookup matched by the name fold (engine rule 8), so `name` may differ
/// from the spelling looked up in case (never in NFC: both sides are NFC).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Found {
    pub id: i64,
    pub name: String,
}

impl Found {
    /// The id, after recording a reference spelled `written` that resolved
    /// to this row (`NameFolded`, warned when the spellings differ).
    pub(crate) fn noted(self, folds: &mut NameFolds, referent: &'static str, written: &str) -> i64 {
        folds.note(referent, written, &self.name, self.id);
        self.id
    }
}

const CLASS_LOOKUP_SQL: &str = "SELECT id, name FROM entity_class WHERE cur_name = lower(?)";
const ALTERNATIVE_LOOKUP_SQL: &str = "SELECT id, name FROM alternative WHERE cur_name = lower(?)";
const SCENARIO_LOOKUP_SQL: &str = "SELECT id, name FROM scenario WHERE cur_name = lower(?)";
const PARAMETER_LOOKUP_SQL: &str =
    "SELECT id, name FROM parameter_definition WHERE cur_cls = ? AND cur_name = lower(?)";
const VALUE_LIST_LOOKUP_SQL: &str =
    "SELECT id, name FROM parameter_value_list WHERE cur_name = lower(?)";

pub(crate) fn find_entity_class(conn: &Connection, name: &str) -> Result<Option<Found>, DbError> {
    opt_found(conn, CLASS_LOOKUP_SQL, -1, name)
}

pub(crate) fn lookup_entity_class(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    Ok(find_entity_class(conn, name)?.map(|f| f.id))
}

pub(crate) fn find_alternative(conn: &Connection, name: &str) -> Result<Option<Found>, DbError> {
    opt_found(conn, ALTERNATIVE_LOOKUP_SQL, -1, name)
}

pub(crate) fn lookup_alternative(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    Ok(find_alternative(conn, name)?.map(|f| f.id))
}

pub(crate) fn find_scenario(conn: &Connection, name: &str) -> Result<Option<Found>, DbError> {
    opt_found(conn, SCENARIO_LOOKUP_SQL, -1, name)
}

pub(crate) fn lookup_scenario(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    Ok(find_scenario(conn, name)?.map(|f| f.id))
}

pub(crate) fn find_parameter(
    conn: &Connection,
    class_id: i64,
    name: &str,
) -> Result<Option<Found>, DbError> {
    opt_found(conn, PARAMETER_LOOKUP_SQL, class_id, name)
}

pub(crate) fn lookup_parameter(
    conn: &Connection,
    class_id: i64,
    name: &str,
) -> Result<Option<i64>, DbError> {
    Ok(find_parameter(conn, class_id, name)?.map(|f| f.id))
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
/// the fence, in order:
/// (d) the aggregate fence `SELECT any_value(id) FILTER (WHERE
/// entity_class_id = ?1 AND valid_to_commit IS NULL) FROM entity WHERE
/// lower(name) = lower(?2)`; then (c) fetch `(id, entity_class_id,
/// valid_to_commit)` by `lower(name)` alone and filter in Rust. Never a
/// `MATERIALIZED` CTE: DuckDB's `CTEFilterPusher` already pushes filters into
/// one of this shape.
#[doc(hidden)]
pub const ENTITY_NAME_LOOKUP_SQL: &str = "SELECT id, name FROM \
       (SELECT id, name, entity_class_id, valid_to_commit FROM entity \
        WHERE lower(name) = lower(?2) OFFSET 0) \
     WHERE entity_class_id = ?1 AND valid_to_commit IS NULL";

pub(crate) fn find_entity(
    conn: &Connection,
    class_id: i64,
    name: &str,
) -> Result<Option<Found>, DbError> {
    opt_found(conn, ENTITY_NAME_LOOKUP_SQL, class_id, name)
}

pub(crate) fn lookup_entity(
    conn: &Connection,
    class_id: i64,
    name: &str,
) -> Result<Option<i64>, DbError> {
    Ok(find_entity(conn, class_id, name)?.map(|f| f.id))
}

/// The current `entity` row a rename revises, with its `rowid` (the close
/// addresses it), read through the same `OFFSET 0` fence as
/// [`ENTITY_NAME_LOOKUP_SQL`] (`?1` class id, `?2` name; guard:
/// `tests/entity_name_index_scan.rs`).
#[doc(hidden)]
pub const ENTITY_ROW_LOOKUP_SQL: &str = "SELECT id, name, description, version, \
            valid_from_commit, rid FROM \
       (SELECT rowid AS rid, id, name, description, version, valid_from_commit, \
               entity_class_id, valid_to_commit FROM entity \
        WHERE lower(name) = lower(?2) OFFSET 0) \
     WHERE entity_class_id = ?1 AND valid_to_commit IS NULL";

/// The current row of the entity of class `class_id` named `name` (by the
/// name fold), for the one entity revision site
/// ([`crate::template::reconcile::revise_entity_row`]).
pub(crate) fn find_entity_row(
    conn: &Connection,
    class_id: i64,
    name: &str,
) -> Result<Option<EntityRow>, DbError> {
    let row = conn
        .prepare_cached(ENTITY_ROW_LOOKUP_SQL)?
        .query_row(params![class_id, name], |r| {
            Ok(EntityRow {
                id: r.get(0)?,
                entity_class_id: class_id,
                name: r.get(1)?,
                description: r.get(2)?,
                version: r.get(3)?,
                valid_from: r.get(4)?,
                rowid: r.get(5)?,
            })
        });
    match row {
        Ok(r) => Ok(Some(r)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// One current `entity` row, as the rename revises it.
#[derive(Clone, Debug)]
pub(crate) struct EntityRow {
    pub(crate) id: i64,
    pub(crate) entity_class_id: i64,
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) version: i64,
    /// The current row's `valid_from_commit`: equal to the writing commit
    /// when this commit created or already revised it.
    pub(crate) valid_from: i64,
    /// The current row's `rowid` as read (the close addresses it).
    pub(crate) rowid: i64,
}

pub(crate) fn lookup_class_set(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    opt_id(
        conn,
        "SELECT id FROM class_set WHERE cur_name = lower(?)",
        -1,
        name,
    )
}

pub(crate) fn find_value_list(conn: &Connection, name: &str) -> Result<Option<Found>, DbError> {
    opt_found(conn, VALUE_LIST_LOOKUP_SQL, -1, name)
}

pub(crate) fn lookup_value_list(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    Ok(find_value_list(conn, name)?.map(|f| f.id))
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
    /// The segment's value-type groups, in first-seen batch order: each the
    /// batches whose columns have identical Arrow types, concatenated
    /// (one group for a single-type segment). Shared by `without` copies.
    groups: Arc<Vec<PreparedGroup>>,
    /// Warnings of the segment itself (timezone-normalized columns,
    /// normalized names and labels), added to the report on every attempt.
    prep_warnings: Vec<Warning>,
    /// Distinct entity names, first-seen order, case-insensitively deduped
    /// across every group (one entity, one value).
    names: Vec<String>,
    /// Per entity (an index into `names`): its group (an index into
    /// `groups`).
    group_of: Vec<usize>,
    /// Per-entity row indices into its group's `sub_all`, ascending (i.e.
    /// original row order). O(nrows) total, vs O(entities × nrows) for the
    /// boolean masks these replace.
    rows: Vec<Vec<u32>>,
}

/// One value-type group of a segment: the segment's batches whose columns
/// all have the same Arrow data types, concatenated into one logical table.
/// A group instantiates exactly one declared member; groups of one segment may bind to different members
/// (a float constant for one entity, a string for another) or to the same
/// one (an indexed member does not fix the measure kind).
struct PreparedGroup {
    value_kind: ValueKind,
    inferred: Vec<(String, IndexKind)>,
    /// The label lookup of the group's effective domains, built once per
    /// group (plan T12).
    domain_index: Arc<DomainIndex>,
    /// The dims+value sub-batch (entity column dropped).
    sub_all: RecordBatch,
}

impl PreparedSegment {
    /// Canonicalize the value of entity `j` (an index into `names`) against
    /// `declared`, the member its group binds to. An error that needs the
    /// value's location to be found in the source data (a duplicate index
    /// tuple) names the entity, class, parameter and alternative.
    fn canonicalize_entity(
        &self,
        j: usize,
        declared: &DeclaredType,
    ) -> Result<CanonicalValue, DbError> {
        let g = &self.groups[self.group_of[j]];
        let sub = take_record_batch(&g.sub_all, &UInt32Array::from(self.rows[j].clone()))?;
        canonicalize_indexed(&sub, declared, g.value_kind, &g.domain_index).map_err(|e| {
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
    /// names still point into the shared groups.
    fn without(&self, dropped: &[usize]) -> PreparedSegment {
        let drop: HashSet<usize> = dropped.iter().copied().collect();
        let mut names = Vec::with_capacity(self.names.len());
        let mut group_of = Vec::with_capacity(self.names.len());
        let mut rows = Vec::with_capacity(self.names.len());
        for (i, ((n, g), r)) in self
            .names
            .iter()
            .zip(&self.group_of)
            .zip(&self.rows)
            .enumerate()
        {
            if !drop.contains(&i) {
                names.push(n.clone());
                group_of.push(*g);
                rows.push(r.clone());
            }
        }
        PreparedSegment {
            class: self.class.clone(),
            parameter: self.parameter.clone(),
            alternative: self.alternative.clone(),
            groups: Arc::clone(&self.groups),
            prep_warnings: self.prep_warnings.clone(),
            names,
            group_of,
            rows,
        }
    }
}

/// Validate and split one segment. `seg_names` holds what normalizing the
/// segment's class, parameter and alternative names changed; the entity
/// names are normalized here, and the segment's `TextNormalized` warnings
/// (names, labels) join its prepared warnings.
///
/// The batches are grouped by their columns' Arrow data types (the whole
/// vector: entity, dimensions, value), in first-seen order; each group is
/// concatenated into one logical table, as a single-type segment's batches
/// always were. An entity may appear in one group only.
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
    // A segment's batches are one logical table per value type (spec §2.3);
    // an empty segment writes nothing — reject before it can auto-create
    // referents or burn a commit (critique F8).
    if seg.batches.is_empty() {
        return Err(DbError::Api("empty batch: no rows to write".into()));
    }
    // Group the batches by column types, first-seen order. Batches of one
    // group concatenate exactly as before (same column count and types).
    // The key also holds the dimension names (NFC, as the declared names
    // they are matched against): two batches whose columns differ only in
    // a dimension's name may instantiate two declared members that only
    // their names tell apart (the reason the binding is persisted, D11),
    // so they are two groups, each matched by its own names.
    type GroupKey<'a> = (Vec<&'a DataType>, Vec<String>);
    let mut keys: Vec<GroupKey<'_>> = Vec::new();
    let mut members: Vec<Vec<&RecordBatch>> = Vec::new();
    for b in &seg.batches {
        let fields = b.schema_ref().fields();
        let n = fields.len();
        let key: GroupKey<'_> = (
            fields.iter().map(|f| f.data_type()).collect(),
            fields
                .iter()
                .enumerate()
                .filter(|(i, _)| *i > 0 && *i + 1 < n)
                .map(|(_, f)| nfc(f.name()).into_owned())
                .collect(),
        );
        match keys.iter().position(|k| *k == key) {
            Some(g) => members[g].push(b),
            None => {
                keys.push(key);
                members.push(vec![b]);
            }
        }
    }
    // A zero-row batch adds nothing to its group; a group of zero-row
    // batches is dropped unless every batch is empty (then the segment
    // writes nothing and is refused below, as before).
    if members.iter().any(|m| m.iter().any(|b| b.num_rows() > 0)) {
        members.retain(|m| m.iter().any(|b| b.num_rows() > 0));
    }
    let mut group_batches: Vec<RecordBatch> = Vec::with_capacity(members.len());
    for m in &members {
        group_batches.push(if m.len() == 1 {
            m[0].clone()
        } else {
            concat_batches(&m[0].schema(), m.iter().copied())?
        });
    }

    let mut prep_warnings: Vec<Warning> = Vec::new();
    let mut labels = Normalized::default();
    let mut groups: Vec<PreparedGroup> = Vec::with_capacity(group_batches.len());
    let mut slot: HashMap<String, usize> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    let mut group_of: Vec<usize> = Vec::new();
    let mut rows: Vec<Vec<u32>> = Vec::new();
    // The caller's positional label domains (`Segment::domains`) name a
    // dimension by position only. When groups put different dimensions at
    // one position, a domain belongs to the dimension the FIRST group with
    // a label dimension there has (matched by the name fold); a group whose
    // label dimension at that position has another name does not get it
    // and keeps its own `nodal:label_domain` field metadata, if any. The
    // alternative — the argument applying to every group — would give one
    // dimension's labels to another and shadow the metadata the other
    // batch carries for exactly that purpose. A single-group segment is
    // unchanged: the argument wins over field metadata at its positions.
    let anchors: Vec<Option<String>> = seg
        .domains
        .iter()
        .map(|d| {
            group_batches
                .iter()
                .find_map(|b| label_dim_fold(b, d.position))
        })
        .collect();
    for (g, batch) in group_batches.iter().enumerate() {
        let provided: Cow<'_, [LabelDomain]> = if group_batches.len() == 1 {
            Cow::Borrowed(&seg.domains)
        } else {
            Cow::Owned(
                seg.domains
                    .iter()
                    .zip(&anchors)
                    .filter(|(d, anchor)| match anchor {
                        None => true,
                        Some(a) => label_dim_fold(batch, d.position).as_ref() == Some(a),
                    })
                    .map(|(d, _)| d.clone())
                    .collect(),
            )
        };
        groups.push(prepare_group(
            batch,
            g,
            &provided,
            &mut seg_names,
            &mut labels,
            &mut prep_warnings,
        )?);
        // The per-entity row-index lists of this group.
        let entity_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DbError::Api("entity column must be Utf8 (entity names)".into()))?;
        for row in 0..batch.num_rows() {
            if entity_arr.is_null(row) {
                return Err(DbError::Api(format!("null entity name in row {row}")));
            }
            // Entity names are names: NFC before the dedup below (the text rule).
            let name = seg_names.nfc(entity_arr.value(row)).into_owned();
            if name.trim().is_empty() {
                return Err(DbError::Api(format!("empty entity name in row {row}")));
            }
            // Dedup by the name fold (`types::fold`, the Rust copy of DuckDB's
            // `lower()`) so case-aliases cannot double-write one entity, across
            // every group of the segment. Aliased spellings are ambiguous, not
            // mergeable (critique F3). `resolve_entities`, which groups by the
            // fold DuckDB returns, repeats the check as the second line.
            let idx = *slot.entry(fold(&name)).or_insert_with(|| {
                names.push(name.clone());
                group_of.push(g);
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
            if group_of[idx] != g {
                // One entity, one value: rows of two value types cannot be
                // one value, whatever they hold.
                return Err(DbError::Api(format!(
                    "entity {name:?} appears in batches of two value types in one \
                     segment ({}; {}); one entity has one value: send its rows in \
                     batches of one type (same column types and dimension names)",
                    groups[group_of[idx]].desc(),
                    groups[g].desc()
                )));
            }
            rows[idx].push(row as u32);
        }
    }

    let target = format!("{}:{}/{}", seg.class, seg.parameter, seg.alternative);
    prep_warnings.extend(seg_names.warning("name", "segment", &target));
    prep_warnings.extend(labels.warning("label", "segment", &target));

    Ok(PreparedSegment {
        class: seg.class.clone(),
        parameter: seg.parameter.clone(),
        alternative: seg.alternative.clone(),
        groups: Arc::new(groups),
        prep_warnings,
        names,
        group_of,
        rows,
    })
}

/// The folded NFC name of `batch`'s dimension at 1-based `position` when it
/// is a label dimension (a Utf8 column between the entity and the value
/// columns), else `None`.
fn label_dim_fold(batch: &RecordBatch, position: u8) -> Option<String> {
    let i = usize::from(position);
    let fields = batch.schema_ref().fields();
    if i == 0 || i + 1 >= fields.len() || fields[i].data_type() != &DataType::Utf8 {
        return None;
    }
    Some(fold(&nfc(fields[i].name())))
}

impl PreparedGroup {
    /// The group's shape in words (the error messages' batch description).
    fn desc(&self) -> String {
        batch_desc(&self.inferred, self.value_kind)
    }
}

/// Prepare group `g` of a segment from its concatenated batch: column-count
/// and rank checks, the measure and index kinds, the effective label domains
/// and their lookup, the tz warnings, and the label spellings for the
/// segment's `TextNormalized` warning.
fn prepare_group(
    batch: &RecordBatch,
    g: usize,
    provided: &[LabelDomain],
    seg_names: &mut Normalized,
    labels: &mut Normalized,
    prep_warnings: &mut Vec<Warning>,
) -> Result<PreparedGroup, DbError> {
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
        // Dimension names are names: NFC (the text rule), recorded for the
        // segment's `TextNormalized` warning like the other names.
        let dim = seg_names.nfc(field.name()).into_owned();
        inferred.push((dim, index_kind(field.data_type())?));
    }

    // Effective label domains: the in-process arg, plus any Arrow field-metadata
    // domain for a label dimension the arg did not cover (decision D).
    let domains = effective_domains(batch, &inferred, provided)?;
    // A domain that repeats a label has no well-defined order: refuse it once
    // per group, before any value is canonicalized, while building the
    // group's label lookup.
    let domain_index = Arc::new(index_domains(&domains, |position| {
        inferred
            .get(usize::from(position).wrapping_sub(1))
            .map(|(name, _)| name.clone())
    })?);

    // Tz-bearing timestamp columns are accepted — a Timestamp(µs, tz) is a
    // well-defined instant, stored as the UTC-µs instant unchanged — but never
    // silently: each gets a TimezoneNormalized warning (critique F6), once
    // per column and zone across the segment's groups.
    for i in 1..ncols {
        let field = batch.schema().field(i).clone();
        if let DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) = field.data_type() {
            let w = Warning {
                kind: WarningKind::TimezoneNormalized {
                    column: field.name().clone(),
                    tz: tz.to_string(),
                },
                object_type: "column".into(),
                name: field.name().clone(),
                id: 0,
            };
            if g == 0 || !prep_warnings.contains(&w) {
                prep_warnings.push(w);
            }
        }
    }

    // Labels (the text rule): canon compares and stores them in NFC; count
    // here the distinct spellings that it changes, in the label columns and
    // the domains alike, for the segment's warning (a set: a spelling met in
    // two groups counts once).
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

    let project: Vec<usize> = (1..ncols).collect();
    let sub_all = batch.project(&project)?;
    Ok(PreparedGroup {
        value_kind,
        inferred,
        domain_index,
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

    // References that resolved to a differently spelled existing object
    // (`NameFolded`, one warning per referent table of the segment).
    let mut folds = NameFolds::default();
    let class_id =
        names.entity_class(conn, commit_id, &p.class, &mut report.warnings, &mut folds)?;
    // Each value-type group binds to its declared member (D2(a): a batch
    // instantiates exactly one; the binding is persisted, D11), in group
    // order. A group with no entity left (`DeleteWins` dropped them all)
    // resolves nothing. Only the first group can auto-materialize the type
    // of a typeless definition; a later group of another type is then held
    // to that member, as a second commit would be.
    let mut first_of: Vec<Option<usize>> = vec![None; p.groups.len()];
    let mut count_of: Vec<usize> = vec![0; p.groups.len()];
    for (j, &g) in p.group_of.iter().enumerate() {
        first_of[g].get_or_insert(j);
        count_of[g] += 1;
    }
    let mut bound: Vec<Option<(i64, DeclaredType)>> = Vec::with_capacity(p.groups.len());
    let mut definition: Option<(i64, bool)> = None;
    let mut typed_here = false;
    for (g, group) in p.groups.iter().enumerate() {
        let Some(first) = first_of[g] else {
            bound.push(None);
            continue;
        };
        let subject = MatchSubject {
            class: &p.class,
            alternative: &p.alternative,
            entity: &p.names[first],
            entity_count: count_of[g],
        };
        let before = report.warnings.len();
        let r = names
            .parameter(
                conn,
                commit_id,
                class_id,
                &p.parameter,
                &group.inferred,
                group.value_kind,
                &subject,
                &mut report.warnings,
                &mut folds,
            )
            .map_err(|e| match e {
                DbError::UndeclaredValueType(mut d) if typed_here => {
                    d.message.push_str(
                        " (the parameter had no declared type: its type was set by an \
                         earlier group of batches in this call, the first one)",
                    );
                    DbError::UndeclaredValueType(d)
                }
                e => e,
            })?;
        if definition.is_none() {
            // The first group declared the type (an auto-created definition
            // or type row): a later group is held to it.
            typed_here = report.warnings[before..].iter().any(|w| {
                w.kind == WarningKind::AutoCreated
                    && matches!(w.object_type.as_str(), "parameter_type" | "parameter")
            });
        }
        match definition {
            None => definition = Some((r.pd_id, r.retain_content)),
            Some((pd, _)) if pd != r.pd_id => {
                return Err(DbError::Internal(format!(
                    "segment {}:{}: value-type groups resolved to parameter definitions \
                     {pd} and {}",
                    p.class, p.parameter, r.pd_id
                )))
            }
            Some(_) => {}
        }
        bound.push(Some((r.pt_id, r.shape)));
    }
    let Some((pd_id, retain_content)) = definition else {
        return Err(DbError::Internal(format!(
            "segment {}:{}: no entity to write",
            p.class, p.parameter
        )));
    };
    // The member entity `j`'s group binds to.
    let member = |j: usize| -> (i64, &DeclaredType) {
        let (pt_id, shape) = bound[p.group_of[j]]
            .as_ref()
            .expect("every group with an entity is bound");
        (*pt_id, shape)
    };
    let alt_id = names.alternative(
        conn,
        commit_id,
        &p.alternative,
        &mut report.warnings,
        &mut folds,
    )?;
    // Two segments that resolve to one (parameter, alternative) would write
    // the same value keys twice in one commit. The up-front check compares
    // `types::fold`; this one compares the ids DuckDB resolved, the second
    // line should the two ever disagree (with the deferred writes, a minted
    // entity skips its probes and would otherwise end up live in two
    // tiers).
    if !targets.insert((pd_id, alt_id)) {
        return Err(DbError::Api(format!(
            "duplicate target {}:{} (alternative {:?}): resolves to the same parameter \
             and alternative as an earlier segment (name resolution is case-insensitive)",
            p.class, p.parameter, p.alternative
        )));
    }

    // Fold the definition's retain_content into the options unless the
    // caller already overrode it (VMP Part 5 primary retention lever): else a
    // write to a retain_content=FALSE definition would silently keep payloads.
    let effective_retain = opts.retain_content.unwrap_or(retain_content);
    let resolved_opts = WriteOptions {
        retain_content: Some(effective_retain),
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
        &mut folds,
    )?;
    let target = format!("{}:{}/{}", p.class, p.parameter, p.alternative);
    report.warnings.extend(folds.warnings("segment", &target));
    let (entity_ids, entity_minted) = match resolved {
        Resolved::Ids { ids, minted } => (ids, minted),
        Resolved::NdRefused { first_missing, err } => {
            for j in 0..first_missing {
                p.canonicalize_entity(j, member(j).1)?;
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
        let (pt_id, declared) = member(j);
        let value = p.canonicalize_entity(j, declared)?;
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
    /// The definition's name as stored (for `NameFolded`).
    stored: String,
}

/// The per-attempt class / parameter / alternative resolution cache of the
/// segment phase. An editor save
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
/// transaction attempt (a closure local of `commit`), never across
/// attempts or commits.
struct ResolveCache {
    enabled: bool,
    classes: HashMap<String, Found>,
    alternatives: HashMap<String, Found>,
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

    /// The class id `name` resolves to, noted in `folds` (a hit notes the
    /// stored spelling the lookup returned, as the uncached path does).
    fn entity_class(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        name: &str,
        warnings: &mut Vec<Warning>,
        folds: &mut NameFolds,
    ) -> Result<i64, DbError> {
        if let Some(f) = self.classes.get(name) {
            return Ok(f.clone().noted(folds, "entity_class", name));
        }
        let found = resolve_entity_class(conn, commit_id, name, warnings)?;
        if self.enabled {
            self.classes.insert(name.to_string(), found.clone());
        }
        Ok(found.noted(folds, "entity_class", name))
    }

    /// The alternative id `name` resolves to, noted in `folds`.
    fn alternative(
        &mut self,
        conn: &Connection,
        commit_id: i64,
        name: &str,
        warnings: &mut Vec<Warning>,
        folds: &mut NameFolds,
    ) -> Result<i64, DbError> {
        if let Some(f) = self.alternatives.get(name) {
            return Ok(f.clone().noted(folds, "alternative", name));
        }
        let found = resolve_alternative(conn, commit_id, name, warnings)?;
        if self.enabled {
            self.alternatives.insert(name.to_string(), found.clone());
        }
        Ok(found.noted(folds, "alternative", name))
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
        subject: &MatchSubject<'_>,
        warnings: &mut Vec<Warning>,
        folds: &mut NameFolds,
    ) -> Result<ResolvedParameter, DbError> {
        let key = (class_id, name.to_string());
        if let Some(c) = self.parameters.get(&key) {
            // The match the uncached path runs, on the same set.
            let matched =
                validate_declared(name, c.pd_id, &c.declared, inferred, value_kind, subject)?;
            folds.note("parameter_definition", name, &c.stored, c.pd_id);
            return Ok(ResolvedParameter {
                pd_id: c.pd_id,
                pt_id: matched.pt_id,
                shape: matched.shape,
                retain_content: c.retain_content,
            });
        }
        let (pd_id, pt_id, shape, set, stored) = resolve_parameter(
            conn, commit_id, class_id, name, inferred, value_kind, subject, warnings,
        )?;
        folds.note("parameter_definition", name, &stored, pd_id);
        let retain_content = read_retain_content(conn, pd_id)?;
        if let (true, Some(declared)) = (self.enabled, set) {
            self.parameters.insert(
                key,
                CachedParameter {
                    pd_id,
                    declared,
                    retain_content,
                    stored,
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

    /// Make the next `n` `commit` attempts on this thread fail with
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
    /// The cells buffered in `longs`.
    long_cells: usize,
    arrays: Vec<ArrayWrite<'static>>,
    /// The payload elements buffered in `arrays`.
    array_elems: usize,
    /// Every value key buffered by the commit, flushed or not: the
    /// same-call write-and-delete check.
    keys: HashSet<(i64, i64, i64)>,
    /// Values buffered by the commit (the no-op evidence: every one must
    /// come back unchanged).
    values: usize,
    /// The tiers of the values actually written (not unchanged), for
    /// `WriteReport::tiers`.
    written_tiers: Vec<Tier>,
    /// Which written values to record in `changed`.
    track: Track,
    /// The values that got a new version (not unchanged), of the tracked
    /// targets: the metadata statements' subjects.
    changed: Vec<(i64, i64, i64)>,
}

/// Which changed values [`WriteBuffers`] records for the metadata
/// statements (a results write into an annotated file records nothing of
/// the targets that carry no links).
#[derive(Default)]
enum Track {
    #[default]
    Off,
    /// The `(parameter definition, alternative)` targets with value links.
    Targets(HashSet<(i64, i64)>),
    /// Every target (a save- or target-scope `Set` annotates new values).
    All,
}

impl Track {
    fn wants(&self, k: &ValueKey) -> bool {
        match self {
            Track::Off => false,
            Track::All => true,
            Track::Targets(t) => t.contains(&(k.parameter_definition_id, k.alternative_id)),
        }
    }
}

impl WriteBuffers {
    /// Record the keys of the changed outcomes (`keys` in buffer order).
    fn record_changed<'a>(
        &mut self,
        keys: impl Iterator<Item = &'a ValueKey>,
        outcomes: &[crate::types::WriteOutcome],
    ) {
        if matches!(self.track, Track::Off) {
            return;
        }
        for (k, o) in keys.zip(outcomes) {
            if !o.unchanged && self.track.wants(k) {
                self.changed
                    .push((k.entity_id, k.parameter_definition_id, k.alternative_id));
            }
        }
    }

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
        let keys: Vec<ValueKey> = self.scalars.iter().map(|w| w.key).collect();
        self.record_changed(keys.iter(), &outcomes);
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
        let keys: Vec<ValueKey> = self.longs.iter().map(|w| w.key).collect();
        self.record_changed(keys.iter(), &outcomes);
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
        let keys: Vec<ValueKey> = self.arrays.iter().map(|w| w.key).collect();
        self.record_changed(keys.iter(), &outcomes);
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
/// `RecordBatch`, through the `read_parameter` SQL view/macro (reads are
/// SQL; the library never gatekeeps them). One fresh RO
/// connection, opened and closed per call. Enough to
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
#[doc(hidden)]
pub const READ_VALUES_SQL: &str = "SELECT * FROM read_values(?, ?, ?::VARCHAR[], ?::VARCHAR[])";

/// Read a few values of one parameter: the current rows of every
/// (entity, alternative) pair of `entities` x `alternatives` (all current
/// alternatives when `None`), through the `read_values` SQL macro, in
/// `read_parameter`'s columns. One fresh RO connection, opened and closed
/// per call.
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
/// No Rust-side rendering: the macro is the one renderer of axes.
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
/// per-operation RO connection. Quote it to
/// [`WriteOptions::expect_commit`] for an atomic read → compute → write-only-
/// if-nobody-committed cycle. Compaction preserves commit ids, so a repack
/// never moves the head; every commit (any writer, any process) and an
/// erasure (its marker commit) do.
pub fn head_commit(db: &Path) -> Result<i64, DbError> {
    with_read_plain(db, head_on)
}

/// The database's stable identity UUID — the `db_identity` reserved metadata
/// key, minted once by `900_bootstrap.sql` at init and never changed thereafter
/// (output databases can record "produced from input DB `<identity>` at
/// commit C under scenario S" against it). It survives reopen and compaction (the repack copies
/// `database_property` rows as data). One fresh RO connection per call.
/// Plain-SQL equivalent:
/// `SELECT value FROM database_property WHERE lower(key) = 'db_identity' AND valid_to_commit IS NULL`.
pub fn db_identity(db: &Path) -> Result<String, DbError> {
    with_read_plain(db, |conn| {
        crate::property::read(conn, "db_identity")?
            .ok_or_else(|| DbError::Api("the database has no db_identity property".into()))
    })
}

/// One database property's current value (`None` when the key is unset).
/// Keys are case-insensitive. One fresh RO connection per call.
pub fn database_property(db: &Path, key: &str) -> Result<Option<String>, DbError> {
    let key = crate::text::nfc(key);
    with_read_plain(db, |conn| {
        crate::property::require_current(conn)?;
        crate::property::read(conn, &key)
    })
}

/// Every current database property, ordered by key, as
/// [`DatabaseProperty`] rows (core, user and tool keys). One fresh RO
/// connection per call.
pub fn database_properties(db: &Path) -> Result<Vec<DatabaseProperty>, DbError> {
    with_read_plain(db, |conn| {
        crate::property::require_current(conn)?;
        Ok(crate::property::read_all(conn)?
            .into_iter()
            .map(|(key, value, owner)| DatabaseProperty { key, value, owner })
            .collect())
    })
}

/// One current database property ([`database_properties`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseProperty {
    pub key: String,
    pub value: String,
    /// `"core"`, `"user"` or `"tool"`.
    pub owner: String,
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
/// (`nodal:label_domain` = a JSON array of the ordered labels) for a label
/// dimension the arg did not cover.
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
        if let Some(js) = field.metadata().get("nodal:label_domain") {
            let labels: Vec<String> = serde_json::from_str(js).map_err(|e| {
                DbError::Api(format!(
                    "bad nodal:label_domain metadata on {:?}: {e}",
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

/// [`opt_id`] for the name lookups whose statement returns `(id, name)`.
fn opt_found(
    conn: &Connection,
    sql: &str,
    id_param: i64,
    name: &str,
) -> Result<Option<Found>, DbError> {
    let mut stmt = conn.prepare_cached(sql)?;
    let row = |r: &duckdb::Row<'_>| {
        Ok(Found {
            id: r.get(0)?,
            name: r.get(1)?,
        })
    };
    let r = if id_param < 0 {
        stmt.query_row(params![name], row)
    } else {
        stmt.query_row(params![id_param, name], row)
    };
    match r {
        Ok(f) => Ok(Some(f)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

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

/// The class `name` resolves to (its stored spelling for `NameFolded`),
/// auto-created (`AutoCreated`, stored as written) when there is none.
fn resolve_entity_class(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<Found, DbError> {
    if let Some(found) = find_entity_class(conn, name)? {
        return Ok(found);
    }
    let id: i64 = conn.query_row(
        "INSERT INTO entity_class (name, valid_from_commit) VALUES (?, ?) RETURNING id",
        params![name, commit_id],
        |r| r.get(0),
    )?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "class".into(),
        name: name.into(),
        id,
    });
    Ok(Found {
        id,
        name: name.to_string(),
    })
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
        params![
            name,
            crate::text::stored_description(description),
            commit_id
        ],
        |r| r.get(0),
    )?)
}

/// The alternative `name` resolves to (its stored spelling for
/// `NameFolded`), auto-created (`AutoCreated`) when there is none.
fn resolve_alternative(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<Found, DbError> {
    if let Some(found) = find_alternative(conn, name)? {
        return Ok(found);
    }
    let id = insert_alternative_row(conn, commit_id, name, None)?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "alternative".into(),
        name: name.into(),
        id,
    });
    Ok(Found {
        id,
        name: name.to_string(),
    })
}

/// The outcome of [`resolve_entities`].
enum Resolved {
    /// One entity id per input name, in input order, and whether this call
    /// minted it (auto-created in this commit).
    Ids { ids: Vec<i64>, minted: Vec<bool> },
    /// The class is N-D and `first_missing` is the first name (in input
    /// order) without a current entity: auto-creating it is refused with
    /// `err`. Nothing was inserted. The caller canonicalizes the entities
    /// before it first, so a canon error there still wins.
    NdRefused { first_missing: usize, err: DbError },
}

/// The entity lookup of [`resolve_entities`]: every input name with its
/// ordinal, its DuckDB `lower()` fold and the id and stored name of the
/// current entity of the class holding that folded name (NULL when missing).
/// The fold is DuckDB's,
/// returned to Rust, never recomputed there. The
/// predicate is the expansion of `cur_cls = ? AND cur_name = lower(n)` on the
/// base columns (the VIRTUAL `cur_*` columns are `CASE WHEN valid_to_commit
/// IS NULL THEN … END`): the same rows, but the plain column filters run
/// before `lower(name)` is evaluated.
const ENTITY_LOOKUP_SQL: &str = "WITH k AS (SELECT unnest(?::VARCHAR[]) AS n, \
                                         unnest(?::BIGINT[]) AS i) \
     SELECT k.i, lower(k.n), e.id, e.name FROM k \
     LEFT JOIN entity e ON e.entity_class_id = ? AND e.valid_to_commit IS NULL \
                       AND lower(e.name) = lower(k.n)";

/// The single-name form of `ENTITY_LOOKUP_SQL` (a statement-form switch
/// only): the fold, the id and the stored name (NULL
/// when missing; one row always, through the outer join on a one-row
/// relation). `?1` name, `?2` class id. The entity comes through the same
/// `OFFSET 0` fence as [`ENTITY_NAME_LOOKUP_SQL`] (see there for why, the
/// guard and the fallbacks), so it uses the `ix_entity_lname` index.
#[doc(hidden)]
pub const ENTITY_LOOKUP_ONE_SQL: &str = "SELECT lower(?1), e.id, e.name \
     FROM (SELECT 1) AS one_row LEFT JOIN \
       (SELECT id, name FROM \
         (SELECT id, name, entity_class_id, valid_to_commit FROM entity \
          WHERE lower(name) = lower(?1) OFFSET 0) \
        WHERE entity_class_id = ?2 AND valid_to_commit IS NULL) AS e ON true";

/// Look up `names` among the current entities of `class_id`: per name, in
/// input order, DuckDB's `lower()` fold of it and the current entity holding
/// that folded name (`None` when missing). One statement:
/// [`ENTITY_LOOKUP_ONE_SQL`] for one name, [`ENTITY_LOOKUP_SQL`] with an
/// explicit ordinal list otherwise; every ordinal must come back exactly
/// once (hard check). The one entity lookup of the write path's
/// [`resolve_entities`] and of the delete channel.
pub(crate) fn lookup_entities(
    conn: &Connection,
    class_id: i64,
    names: &[&str],
) -> Result<Vec<(String, Option<Found>)>, DbError> {
    use duckdb::types::Value;

    fn found(id: Option<i64>, name: Option<String>) -> Option<Found> {
        Some(Found {
            id: id?,
            name: name?,
        })
    }
    if let [name] = names {
        let row = conn
            .prepare_cached(ENTITY_LOOKUP_ONE_SQL)?
            .query_row(params![name, class_id], |r| {
                Ok((r.get::<_, String>(0)?, found(r.get(1)?, r.get(2)?)))
            })?;
        return Ok(vec![row]);
    }
    let n = names.len();
    let mut out: Vec<Option<(String, Option<Found>)>> = vec![None; n];
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
            found(r.get(2)?, r.get(3)?),
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
    /// The entity's name as stored (for `NameFolded`).
    stored: String,
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

/// Resolve every entity name of one segment in one set-based round:
/// one lookup statement (none for
/// names an earlier segment of the commit resolved, see [`EntityCache`]), at
/// most one N-D probe, and one columnar insert per ≤ `LONG_INSERT_CHUNK`
/// missing names. This is the write path's one decision-B entity
/// auto-create site.
///
/// Names are resolved by DuckDB's `lower()` (Unicode). Two distinct input
/// spellings that DuckDB folds to the same name are ambiguous and rejected
/// with the same Api error `prepare_segment` raises for ASCII case-aliases.
/// Missing names are inserted in input order; their `AutoCreated`
/// warnings follow in the same order, as the per-entity resolver pushed
/// them. Never auto-creates into an N-D class: an N-D
/// entity's identity is its member tuple, which the batch does not carry.
#[allow(clippy::too_many_arguments)]
fn resolve_entities(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    class: &str,
    names: &[String],
    cache: &mut EntityCache,
    warnings: &mut Vec<Warning>,
    folds: &mut NameFolds,
) -> Result<Resolved, DbError> {
    use duckdb::types::Value;

    let n = names.len();
    let mut ids: Vec<Option<i64>> = vec![None; n];
    let mut name_folds: Vec<Option<String>> = vec![None; n];
    // The stored spelling of each name that resolved to an existing entity.
    let mut stored: Vec<Option<String>> = vec![None; n];
    let mut minted = vec![false; n];
    let mut lookup: Vec<usize> = Vec::new();
    for (i, name) in names.iter().enumerate() {
        match cache.get(&(class_id, name.clone())) {
            Some(c) => {
                ids[i] = Some(c.id);
                name_folds[i] = Some(c.fold.clone());
                stored[i] = Some(c.stored.clone());
                minted[i] = c.minted;
            }
            None => lookup.push(i),
        }
    }
    let looked_up: Vec<&str> = lookup.iter().map(|&i| names[i].as_str()).collect();
    for (&i, (fold, found)) in lookup
        .iter()
        .zip(lookup_entities(conn, class_id, &looked_up)?)
    {
        name_folds[i] = Some(fold);
        if let Some(f) = found {
            ids[i] = Some(f.id);
            stored[i] = Some(f.name);
        }
    }

    // Fold collisions (critique C2), in input order.
    let mut first_by_fold: HashMap<&str, usize> = HashMap::with_capacity(n);
    for (i, fold) in name_folds.iter().enumerate() {
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
                stored[i] = Some(name.clone());
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
    // `NameFolded`: every name that resolved to an existing entity spelled
    // differently (a minted entity is stored as written: silent).
    for (i, name) in names.iter().enumerate() {
        if let (Some(id), Some(st)) = (ids[i], &stored[i]) {
            folds.note("entity", name, st, id);
        }
    }
    for &i in &lookup {
        if let (Some(id), Some(fold), Some(st)) = (ids[i], name_folds[i].clone(), stored[i].clone())
        {
            cache.insert(
                (class_id, names[i].clone()),
                CachedEntity {
                    id,
                    fold,
                    stored: st,
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
/// declared set as read, the definition's stored name)`.
type ParameterResolution = (i64, i64, DeclaredType, Option<Vec<DbDeclared>>, String);

/// Resolve the parameter definition within `class_id`, auto-creating it (and its
/// `parameter_type` + `parameter_type_dimension` declaration) when absent. The
/// declared type SET is READ FROM THE DB for an existing definition; only a freshly created definition takes its
/// declared type from the batch's inferred dimensions. Returns
/// `(pd_id, parameter_type_id, shape)` — the member id is the exactly-one
/// binding persisted on every value row; auto-created /
/// auto-materialized declarations bind their freshly created row. The fourth
/// element is the declared set as read from the DB, `None` when this call
/// created it (what [`ResolveCache`] may keep).
#[allow(clippy::too_many_arguments)]
fn resolve_parameter(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    name: &str,
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
    subject: &MatchSubject<'_>,
    warnings: &mut Vec<Warning>,
) -> Result<ParameterResolution, DbError> {
    if let Some(Found {
        id: pd_id,
        name: stored,
    }) = find_parameter(conn, class_id, name)?
    {
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
            Ok((pd_id, pt_id, inferred_declared(inferred), None, stored))
        } else {
            let matched = validate_declared(name, pd_id, &set, inferred, value_kind, subject)?;
            Ok((pd_id, matched.pt_id, matched.shape, Some(set), stored))
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
            object_type: "parameter".into(),
            name: name.into(),
            id: pd_id,
        });
        Ok((
            pd_id,
            pt_id,
            inferred_declared(inferred),
            None,
            name.to_string(),
        ))
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
/// the `parameter_type` row id, the
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
/// on: the `value_kind` tag plus the
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
            .map(|(n, (_, k))| (fold(n), *k))
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
        inferred.iter().map(|(n, k)| (fold(n), *k)).collect(),
    )
}

/// Hold the batch to an EXISTING declared allowed-type set: the batch must
/// instantiate **exactly one** declared member — value-kind tag, rank,
/// per-position index kinds, and dimension names in declared order. A batch
/// instantiating no member is rejected loudly, naming the allowed set; the
/// writer never widens a declared set implicitly. A set whose members are not
/// pairwise distinguishable on that tuple is itself invalid and refused
/// EAGERLY, before any matching (deliberate: a damaged declaration is never
/// half-usable). Returns the matched member — the member the batch binds to
/// (its dimension kinds drive canon, its names/positions key label domains).
/// The no-match refusal is [`DbError::UndeclaredValueType`], naming
/// `subject` (the batch's target and its first entity).
fn validate_declared(
    parameter: &str,
    pd_id: i64,
    set: &[DbDeclared],
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
    subject: &MatchSubject<'_>,
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
        // `value_kind` tags are not names (an ASCII vocabulary), so the
        // name fold does not apply.
        #[allow(clippy::disallowed_methods)]
        let case_only = m.value_kind != bkey.0 && m.value_kind.eq_ignore_ascii_case(&bkey.0);
        case_only || !KNOWN_TAGS.contains(&m.value_kind.as_str())
    });
    let hint = if damaged {
        " (note: a declared value_kind tag differs from the batch's only by case, or is \
         outside the known tag vocabulary — probable import/hand-edit damage; tag \
         matching is case-sensitive)"
    } else {
        ""
    };
    let members: Vec<String> = set.iter().map(member_desc).collect();
    let more = match subject.entity_count {
        0 | 1 => String::new(),
        n => format!(" and {} more of this type", n - 1),
    };
    let message = format!(
        "parameter {parameter:?} (class {:?}, alternative {:?}), entity {:?}{more}: batch \
         ({}) instantiates none of the {} declared member type(s): [{}]; a write never \
         widens the declared type set — declare the new shape first (template/reconcile \
         operation){hint}",
        subject.class,
        subject.alternative,
        subject.entity,
        batch_desc(inferred, value_kind),
        members.len(),
        members.join("; ")
    );
    Err(DbError::UndeclaredValueType(Box::new(
        UndeclaredValueTypeDetails {
            segment_index: 0,
            class: subject.class.to_string(),
            parameter: parameter.to_string(),
            alternative: subject.alternative.to_string(),
            entity: subject.entity.to_string(),
            entity_count: subject.entity_count,
            value_kind: value_kind.tag().to_string(),
            dimensions: inferred
                .iter()
                .map(|(n, k)| (n.clone(), index_kind_tag(*k).to_string()))
                .collect(),
            declared: members,
            message,
        },
    )))
}

/// What a declared-type match is for, for its refusal: the batch's class
/// and alternative (the parameter is passed on its own), the first entity of
/// the batch and how many entities it carries.
struct MatchSubject<'a> {
    class: &'a str,
    alternative: &'a str,
    entity: &'a str,
    entity_count: usize,
}

/// Read a definition's full current declared-type set from `parameter_type` /
/// `parameter_type_dimension` (one or more declarations). Empty = no
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
/// dimension names/kinds from the inferred columns. Returns the
/// created `parameter_type` id.
///
/// `pub(crate)` for the template loader: declared
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

/// The definition's `retain_content` — the primary retention lever
/// (default TRUE; FALSE on result classes) that `history::write_value` must see
/// via `retain_content`, else a FALSE definition silently keeps full payloads.
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
                id: 2,
                effect: StructuralEffect::NoOp,
            }],
            object_deletes: vec![ObjectDeleteOutcome {
                key_index: 0,
                kind: ObjectKind::Alternative,
                result: ObjectDeleteResult::NotFound,
            }],
            purges: vec![crate::types::PurgeOutcome::default()],
            ..WriteReport::default()
        };
        let ev = NoopEvidence {
            values: 2,
            deleted: 0,
            structural: 1,
            object_deletes: 1,
            purges: 1,
        };
        (report, ev)
    }

    /// The D1 predicate is an allow-list: the all-no-op report
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
            (
                "a purge closed rows",
                Box::new(|r, _| r.purges[0].closed_out.total_rows = 1),
            ),
            (
                "a purge unbound a list",
                Box::new(|r, _| r.purges[0].unbound = 1),
            ),
            ("a purge outcome missing", Box::new(|_, e| e.purges = 2)),
        ];
        for (what, bend) in bends {
            let (mut r, mut ev) = noop();
            bend(&mut r, &mut ev);
            assert!(!wrote_nothing(&r, &ev), "{what} must commit");
        }
    }
}
