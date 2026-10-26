//! The G8 migration verbs — the core write-API surface `run:` migration
//! programs (and the Spine→nodal importer) use for the vocabulary and
//! scenario/activation writes the Arrow value path does not carry
//! (`specs/flextool_port_finale_plan.md` §1, decision D-F4, jkiviluo
//! 2026-09-22).
//!
//! **Eight verbs**: [`ensure_alternative`], [`set_scenario_alternatives`],
//! [`set_entity_activation`], [`set_class_active_by_default`],
//! [`ensure_entity_class`], [`ensure_parameter_definition`],
//! [`ensure_entity`], [`stamp_template_identity`].
//!
//! Governing rule (plan §1, CLAUDE.md invariant #2): every verb is a THIN
//! close-out-protocol citizen over the existing single write paths — where a
//! door exists it is reused (each verb's doc comment names the wrapped path);
//! where a table had no writer yet (`scenario`, `scenario_alternative`,
//! `entity_alternative`), the verb becomes its one door. No canonicalization,
//! typing, or history rule is duplicated here.
//!
//! Deliberately NOT surface (D-F4): a re-declare/widen mode on
//! [`ensure_parameter_definition`] — widening is a reconcile operation, and
//! the sanctioned mid-chain retype is the **close-out + recreate idiom**
//! through existing verbs (plan §1.6): read the old values →
//! `api::delete_objects` the definition (versioned close-out, cascade) →
//! `ensure_parameter_definition` with the new declaration → write the
//! converted values. Exercised by the `verbs` test suite.
//!
//! Connection discipline (CLAUDE.md invariant #1): every verb opens, works,
//! and closes per operation. Idempotent no-ops pre-check on a read-only
//! connection and burn **no commit**; the write transaction re-checks inside
//! (a racing writer costs one replan, never a wrong write).

use std::path::Path;

use duckdb::{params, Connection};

use crate::api::{
    insert_alternative_row, lookup_alternative, lookup_entity, lookup_entity_class,
    lookup_parameter, lookup_scenario, lookup_value_list,
};
use crate::conn::{with_read_plain, with_write_txn_retrying};
use crate::error::DbError;
use crate::structural::{
    apply_create_entity, apply_create_entity_class, apply_create_parameter,
    apply_set_entity_activation, apply_set_scenario_alternatives, check_class_dims,
    effective_entity_name, entity_class_shape, validate_type_decls, verify_entity_elements,
};
use crate::template::ir::TypeDecl;
use crate::template::reconcile::{
    clear_metadata, close_value_list, read_ec_row, revise_ec_row, set_metadata,
    value_list_binding_count,
};
use crate::text::nfc;
use crate::types::{charset_ok, fold};

/// Every name of `names` in NFC (the text rule, `text.rs`).
fn nfc_all(names: &[String]) -> Vec<String> {
    names.iter().map(|n| nfc(n).into_owned()).collect()
}

fn require_user(user: &str) -> Result<(), DbError> {
    if user.trim().is_empty() {
        return Err(DbError::Api("user must be non-empty".into()));
    }
    Ok(())
}

fn require_name(what: &str, name: &str) -> Result<(), DbError> {
    if name.trim().is_empty() {
        return Err(DbError::Api(format!("{what} name must be non-empty")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 1.1 (a) — alternatives and scenario ranks (v38)
// ---------------------------------------------------------------------------

/// Idempotent alternative creation (plan §1.1): create the alternative when
/// absent (with `description` — v38 sets a long description on its floor
/// alternative, critique MINOR 3), return the existing row's id otherwise.
///
/// Wraps the write path's one alternative create site
/// (`api::insert_alternative_row`, shared with decision-B auto-create),
/// carrying the description. An EXISTING alternative is returned as-is — the
/// description parameter applies on creation only (alternative descriptions
/// are fingerprint-excluded, MINOR 3; this verb never revises rows).
pub fn ensure_alternative(
    db: &Path,
    name: &str,
    description: Option<&str>,
    user: &str,
) -> Result<i64, DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let name = &*nfc(name);
    require_user(user)?;
    require_name("alternative", name)?;
    if let Some(id) = with_read_plain(db, |conn| lookup_alternative(conn, name))? {
        return Ok(id); // idempotent no-op: no commit
    }
    let msg = format!("ensure_alternative {name}");
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        if let Some(id) = lookup_alternative(conn, name)? {
            return Ok(id); // raced: created since the pre-check
        }
        insert_alternative_row(conn, c, name, description)
    })
}

/// Whole-list declarative replace of a scenario's alternative ranking (plan
/// §1.1): set the scenario's ordered ranking to EXACTLY the given list at
/// contiguous ranks `1..=n`, in ONE commit; a no-op (no commit) when the current
/// rank ORDER already equals the given order.
///
/// **A-min minimal diff** (structural_write_api_plan §4.3, D-S4): only the rows
/// whose rank actually changes are rewritten (two-phase close-then-insert) — an
/// unchanged prefix/suffix is left untouched, keeping its original
/// `valid_from_commit`/`version`. Rank values are still **order-only semantics**
/// (critique MINOR 4): only the order is contract; consumers (and the replay
/// fingerprint) compare rank order, never rank numerals. Deliberately NOT a
/// rank-insertion verb: v38's "insert strictly below the minimum" is read →
/// prepend → replace in the program, and the program must mirror FlexTool's edge
/// (only scenarios that already HAVE rank rows receive the floor alternative —
/// empty scenarios get nothing).
///
/// The scenario row is created when absent (this verb is the `scenario`
/// table's one writer — a new-but-single door). Alternative names resolve
/// strictly (never auto-created): an unknown name is an error naming it.
/// An empty `alternatives` list clears the scenario's ranking.
pub fn set_scenario_alternatives(
    db: &Path,
    scenario: &str,
    alternatives: &[String],
    user: &str,
) -> Result<i64, DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let scenario = &*nfc(scenario);
    let alternatives = &nfc_all(alternatives);
    require_user(user)?;
    require_name("scenario", scenario)?;
    // Caller-contract checks, before any DB work: non-empty names, no
    // fold-colliding duplicates (rank order over a duplicate is ambiguous).
    let mut seen: Vec<String> = Vec::new();
    for a in alternatives {
        require_name("alternative", a)?;
        let f = fold(a);
        if seen.contains(&f) {
            return Err(DbError::Api(format!(
                "set_scenario_alternatives: alternative {a:?} appears twice \
                 (name resolution is case-insensitive)"
            )));
        }
        seen.push(f);
    }

    let desired: Vec<String> = alternatives.iter().map(|a| fold(a)).collect();

    // Read the scenario's CURRENT ranking folded names in rank order.
    fn current_order_names(conn: &Connection, sid: i64) -> Result<Vec<String>, DbError> {
        let mut stmt = conn.prepare(
            "SELECT lower(a.name) FROM scenario_alternative sa \
             JOIN alternative a ON a.cur_id = sa.alternative_id \
             WHERE sa.cur_s = ? ORDER BY sa.cur_r",
        )?;
        Ok(stmt
            .query_map(params![sid], |r| r.get(0))?
            .collect::<Result<_, _>>()?)
    }

    // Idempotency pre-check (read-only, no commit).
    let pre = with_read_plain(db, |conn| {
        let Some(sid) = lookup_scenario(conn, scenario)? else {
            return Ok(None);
        };
        Ok(Some((sid, current_order_names(conn, sid)?)))
    })?;
    if let Some((sid, cur)) = pre {
        if cur == desired {
            return Ok(sid);
        }
    }

    let msg = format!(
        "set_scenario_alternatives {scenario}: [{}]",
        alternatives.join(", ")
    );
    // The shared A-min applier is the ONE scenario_alternative writer (invariant
    // 2, critique F2): the verb and the composable channel produce identical
    // history for the same logical op.
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        apply_set_scenario_alternatives(conn, c, scenario, alternatives).map(|(sid, _)| sid)
    })
}

// ---------------------------------------------------------------------------
// 1.2 (b) — activation (v56 Batch F)
// ---------------------------------------------------------------------------

/// Write, update, or (`active: None`) close the per-alternative activation
/// row of one entity via the close-out protocol (plan §1.2). This verb is the
/// `entity_alternative` table's one writer (a new-but-single door). All
/// referents resolve strictly — activation of a missing entity is a caller
/// bug, never an auto-create.
///
/// Idempotent: an equal current row (or `None` with no row) is a no-op with
/// no commit. Note (critique MINOR 11): the final reconcile is the authority
/// for end-state activation semantics — mid-chain verb effects converge into
/// it.
pub fn set_entity_activation(
    db: &Path,
    class: &str,
    entity: &str,
    alternative: &str,
    active: Option<bool>,
    user: &str,
) -> Result<(), DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let (class, entity, alternative) = (&*nfc(class), &*nfc(entity), &*nfc(alternative));
    require_user(user)?;
    require_name("class", class)?;
    require_name("entity", entity)?;
    require_name("alternative", alternative)?;

    // Idempotency pre-check (read-only, no commit) — resolves strictly, so a
    // missing referent errors here exactly as the applier would.
    let noop = with_read_plain(db, |conn| {
        let cid = lookup_entity_class(conn, class)?.ok_or_else(|| {
            DbError::Api(format!("set_entity_activation: class {class:?} not found"))
        })?;
        let entity_id = lookup_entity(conn, cid, entity)?.ok_or_else(|| {
            DbError::Api(format!(
                "set_entity_activation: entity {class}:{entity} not found"
            ))
        })?;
        let alt_id = lookup_alternative(conn, alternative)?.ok_or_else(|| {
            DbError::Api(format!(
                "set_entity_activation: alternative {alternative:?} not found"
            ))
        })?;
        let current: Option<bool> = match conn.query_row(
            "SELECT active FROM entity_alternative WHERE cur_e = ? AND cur_a = ?",
            params![entity_id, alt_id],
            |r| r.get(0),
        ) {
            Ok(v) => Some(v),
            Err(duckdb::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(e.into()),
        };
        Ok(match (active, current) {
            (None, None) => true,
            (Some(b), Some(cur)) => cur == b,
            _ => false,
        })
    })?;
    if noop {
        return Ok(());
    }

    let msg = format!(
        "set_entity_activation {class}:{entity} @ {alternative} = {:?}",
        active
    );
    // The shared applier is the ONE entity_alternative writer (invariant 2).
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        apply_set_entity_activation(conn, c, class, entity, alternative, active).map(|_| ())
    })
}

/// Versioned `entity_class.active_by_default` update (plan §1.2) — a thin
/// wrapper over the loader's one `entity_class` revision site
/// (`template::reconcile::{read_ec_row, revise_ec_row}`, the same
/// close-current-row + insert-successor path `rename_class` and
/// `UpdateClass` use). Idempotent: an equal flag is a no-op with no commit.
pub fn set_class_active_by_default(
    db: &Path,
    class: &str,
    active_by_default: bool,
    user: &str,
) -> Result<(), DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let class = &*nfc(class);
    require_user(user)?;
    require_name("class", class)?;
    let noop = with_read_plain(db, |conn| {
        let cid = lookup_entity_class(conn, class)?.ok_or_else(|| {
            DbError::Api(format!(
                "set_class_active_by_default: class {class:?} not found"
            ))
        })?;
        Ok(read_ec_row(conn, cid)?.active_by_default == active_by_default)
    })?;
    if noop {
        return Ok(());
    }
    let msg = format!("set_class_active_by_default {class} = {active_by_default}");
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        let cid = lookup_entity_class(conn, class)?.ok_or_else(|| {
            DbError::Api(format!(
                "set_class_active_by_default: class {class:?} not found"
            ))
        })?;
        let mut row = read_ec_row(conn, cid)?;
        if row.active_by_default == active_by_default {
            return Ok(()); // raced into the desired state
        }
        row.active_by_default = active_by_default;
        revise_ec_row(conn, c, &row)
    })
}

/// Drop an orphan value list at RUNTIME — the ninth verb, added under the
/// Track-B replay finding (2026-09-23; plan §1.6 addendum, route-back in §1.5).
///
/// Why a verb and not a ladder `drop_value_list` hint: v56 D.3 rebinds the six
/// boolean-as-enum definitions off their single-`yes` legacy lists onto
/// `yes_no` through the close-out+recreate idiom (a `run:` program), which
/// leaves the five lists orphaned. Those lists must go — path A's oracle drops
/// them and value-list membership is a strict-tier fingerprint component — but
/// the static `drop_value_list` hint cannot serve: apply's preflight simulates
/// migration steps against the *imported* vocabulary and treats a `run:`
/// program as opaque, so it still sees the imported binding and refuses the
/// drop before the program's rebind has run. This is the runtime door, reusing
/// MigrationStep::DropValueList's exact close logic and orphan-only guard:
/// a still-bound list refuses (rebind/drop its definitions first). Idempotent:
/// a missing list is a no-op that burns no commit.
pub fn drop_value_list(db: &Path, list: &str, user: &str) -> Result<(), DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let list = &*nfc(list);
    require_user(user)?;
    require_name("value list", list)?;
    let noop = with_read_plain(db, |conn| Ok(lookup_value_list(conn, list)?.is_none()))?;
    if noop {
        return Ok(());
    }
    let msg = format!("drop_value_list {list}");
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        let Some(lid) = lookup_value_list(conn, list)? else {
            return Ok(()); // raced away between the read pre-check and here
        };
        let bound = value_list_binding_count(conn, lid)?;
        if bound > 0 {
            return Err(DbError::Api(format!(
                "drop_value_list: {bound} current definition(s) still bind {list:?} \
                 — drop them first (drop_parameter) or rebind them"
            )));
        }
        close_value_list(conn, c, lid)?;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// 1.3 (c) — mid-chain vocabulary creation (v25 / v42 / v50 / v56 / v58)
// ---------------------------------------------------------------------------

/// Idempotent entity-class creation, 0-D or N-D (plan §1.3) — a wrapper over
/// the loader's class-create path (the same `entity_class` +
/// `entity_class_dimension` inserts as `reconcile::execute_action`'s
/// `CreateClass` / `CreateClassDims`, with the loader's defaults:
/// `active_by_default` TRUE, `required` FALSE). `dims` are class names in
/// position order (class-set dimensions are template-authoring surface, not
/// program surface); every dimension class must already exist.
///
/// An EXISTING class is returned as-is when its declared dimensions match
/// (by folded class name, in order); a mismatch is a hard error — in-place
/// re-dimensioning is the deferred reshape door, and v25/v58 are explicitly
/// create-new / copy / delete-old.
pub fn ensure_entity_class(
    db: &Path,
    name: &str,
    dims: &[String],
    user: &str,
) -> Result<i64, DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let name = &*nfc(name);
    let dims = &nfc_all(dims);
    require_user(user)?;
    if !charset_ok(name) {
        return Err(DbError::Api(format!(
            "ensure_entity_class: name {name:?} must be non-empty, trimmed, and free of control characters"
        )));
    }
    if dims.len() > 4 {
        return Err(DbError::Api(format!(
            "ensure_entity_class: {} dimensions exceeds the rank cap of 4",
            dims.len()
        )));
    }

    // Existing-class fast path (read-only, no commit): verify the dims match.
    if let Some(cid) = with_read_plain(db, |conn| lookup_entity_class(conn, name))? {
        return with_read_plain(db, |conn| check_class_dims(conn, name, cid, dims));
    }
    let msg = format!("ensure_entity_class {name} [{}]", dims.join(", "));
    // The shared applier is the one class-create body (invariant 2).
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        apply_create_entity_class(conn, c, name, dims).map(|(id, _)| id)
    })
}

/// Idempotent, properly-typed parameter-definition creation (plan §1.3) — a
/// wrapper over the loader's definition + type-member create path (the same
/// `parameter_definition` insert as `reconcile::execute_action`'s
/// `CreateParameter`, with declared members through
/// `template::reconcile::insert_type_member` →
/// `api::create_parameter_type`, the ONE type-member insert path).
///
/// `types` are IR member declarations ([`TypeDecl`]): a scalar kind, or
/// `indexed` with named-and-kinded dims. With an empty `types` and a
/// `value_list`, the single rank-0 member is inferred from the list's
/// uniform member kind (the loader's §3.5 inference); with neither, the
/// definition is created TYPELESS (auto-materializes on first write — the
/// results-template path).
///
/// An EXISTING definition is returned **as-is — never re-declared, never
/// widened** (decision D-F4: widening is a reconcile operation; the
/// sanctioned mid-chain retype is close-out + recreate, module docs).
pub fn ensure_parameter_definition(
    db: &Path,
    class: &str,
    name: &str,
    types: &[TypeDecl],
    value_list: Option<&str>,
    description: Option<&str>,
    user: &str,
) -> Result<i64, DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let (class, name) = (&*nfc(class), &*nfc(name));
    let value_list = value_list.map(nfc);
    let value_list = value_list.as_deref();
    require_user(user)?;
    require_name("class", class)?;
    if !charset_ok(name) {
        return Err(DbError::Api(format!(
            "ensure_parameter_definition: name {name:?} must be non-empty, trimmed, and free of control characters"
        )));
    }
    // Pure member validation (the load-time subset of `ir::validate`'s type
    // rules), shared with the structural channel's prepare (invariant 2).
    validate_type_decls("ensure_parameter_definition", class, name, types)?;

    if let Some(id) = with_read_plain(db, |conn| {
        let Some(cid) = lookup_entity_class(conn, class)? else {
            return Ok(None);
        };
        lookup_parameter(conn, cid, name)
    })? {
        return Ok(id); // exists: as-is, never re-declared (D-F4)
    }

    let msg = format!("ensure_parameter_definition {class}.{name}");
    // The shared applier is the one definition+type-member create body.
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        apply_create_parameter(conn, c, class, name, types, value_list, description)
            .map(|(id, _)| id)
    })
}

/// Idempotent 0-D / N-D entity creation (plan §1.3). 0-D: `name` required,
/// `elements` empty. N-D: `elements` (entity names in declared-dimension
/// order) required; `name` defaults to the elements joined with `__`.
///
/// Elements resolve through the ONE element-resolution path
/// (`object_delete::resolve_element` — shared with the seed loader and the
/// delete cascade), against each position's declared referent. The write
/// path's refusal to auto-create N-D entities stays: creation is explicit
/// here, through the same `entity` + `entity_element` inserts as the
/// loader's `CreateSeedEntity`.
///
/// An EXISTING entity (by folded name within the class) is returned as-is
/// after its element tuple is verified against `elements` when given — the
/// same name over a different tuple is a hard error, never a silent alias.
pub fn ensure_entity(
    db: &Path,
    class: &str,
    name: Option<&str>,
    elements: &[String],
    user: &str,
) -> Result<i64, DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let class = &*nfc(class);
    let name = name.map(nfc);
    let name = name.as_deref();
    let elements = &nfc_all(elements);
    require_user(user)?;
    require_name("class", class)?;
    let effective_name = effective_entity_name(name, elements)?;
    if !charset_ok(&effective_name) {
        return Err(DbError::Api(format!(
            "ensure_entity: entity name {effective_name:?} must be non-empty, trimmed, and free of control characters"
        )));
    }

    // Existing-entity fast path (read-only, no commit): verify the tuple.
    let pre = with_read_plain(db, |conn| {
        let s = entity_class_shape(conn, class)?;
        match lookup_entity(conn, s.cid, &effective_name)? {
            Some(eid) => {
                verify_entity_elements(conn, class, &effective_name, eid, elements)?;
                Ok(Some(eid))
            }
            None => Ok(None),
        }
    })?;
    if let Some(eid) = pre {
        return Ok(eid);
    }

    let msg = format!("ensure_entity {class}:{effective_name}");
    // The shared applier is the one entity+element create body (invariant 2).
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        apply_create_entity(conn, c, class, name, elements).map(|(id, _)| id)
    })
}

// ---------------------------------------------------------------------------
// 1.7 (g) — template-identity stamping (the eighth verb)
// ---------------------------------------------------------------------------

/// Stamp `template_name` + `template_version` — the public identity-stamp
/// surface (plan §1.7, critique M4). Without a stamp, `template_apply` runs
/// ZERO migration spans and ZERO programs and quietly reconciles to head
/// (`pending_spans` returns nothing for an unstamped DB) — exactly the
/// silent-skip the chain-gap hardening exists to prevent. The importer's
/// stamp is therefore non-optional in its commit sequence.
///
/// Wraps the loader's one reserved-metadata write
/// (`template::reconcile::set_metadata` — the same close-out-then-insert the
/// reconcile's `StampIdentity` action uses). Guard (mirrors
/// `allow_retarget`'s posture): a DIFFERENT existing stamp — name (folded) or
/// version — refuses unless `overwrite` is set. Restamping the identical
/// identity is an idempotent no-op with no commit. Any actual restamp closes
/// a stale `template_content_hash` (the certification belongs to the old
/// identity).
pub fn stamp_template_identity(
    db: &Path,
    name: &str,
    version: &str,
    overwrite: bool,
    user: &str,
) -> Result<(), DbError> {
    // Names in NFC (the text rule, `text.rs`).
    let name = &*nfc(name);
    require_user(user)?;
    require_name("template", name)?;
    if version.trim().is_empty() {
        return Err(DbError::Api("template version must be non-empty".into()));
    }

    fn read_stamp(conn: &Connection) -> Result<(Option<String>, Option<String>), DbError> {
        let get = |key: &str| -> Result<Option<String>, DbError> {
            match conn.query_row(
                "SELECT value FROM metadata WHERE cur_name = lower(?)",
                params![key],
                |r| r.get::<_, String>(0),
            ) {
                Ok(v) => Ok(Some(v)),
                Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        };
        Ok((get("template_name")?, get("template_version")?))
    }
    fn guard(
        existing: &(Option<String>, Option<String>),
        name: &str,
        version: &str,
        overwrite: bool,
    ) -> Result<bool, DbError> {
        let (db_name, db_version) = existing;
        let same = db_name.as_deref().map(fold) == Some(fold(name))
            && db_version.as_deref() == Some(version);
        if same {
            return Ok(true); // idempotent no-op
        }
        let differs = match (db_name, db_version) {
            (None, None) => false, // fresh: nothing to overwrite
            (n, v) => {
                n.as_deref()
                    .map(fold)
                    .map(|f| f != fold(name))
                    .unwrap_or(false)
                    || v.as_deref().map(|x| x != version).unwrap_or(false)
            }
        };
        if differs && !overwrite {
            return Err(DbError::Api(format!(
                "database is already stamped with template {:?} version {:?}; refusing \
                 to restamp as {name:?} version {version:?} — this is an explicit \
                 opt-in (overwrite), not a default (mirrors allow_retarget)",
                db_name.as_deref().unwrap_or("<none>"),
                db_version.as_deref().unwrap_or("<none>"),
            )));
        }
        Ok(false)
    }

    let pre = with_read_plain(db, read_stamp)?;
    if guard(&pre, name, version, overwrite)? {
        return Ok(());
    }
    let msg = format!("stamp_template_identity {name} version {version}");
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        let existing = read_stamp(conn)?;
        if guard(&existing, name, version, overwrite)? {
            return Ok(()); // raced into the desired stamp
        }
        set_metadata(conn, c, "template_name", name)?;
        set_metadata(conn, c, "template_version", version)?;
        // The stamped identity changed: any prior certified content hash
        // belongs to the OLD identity — close it so the apply short-circuit
        // can never believe stale certification.
        clear_metadata(conn, c, "template_content_hash")?;
        Ok(())
    })
}
