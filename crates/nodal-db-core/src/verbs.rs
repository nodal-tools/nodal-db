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
    lookup_parameter, lookup_value_list,
};
use crate::conn::{with_read, with_write_txn_retrying};
use crate::error::DbError;
use crate::object_delete::resolve_element as resolve_element_in_dim;
use crate::template::ir::{TypeDecl, DIM_KINDS, SCALAR_KINDS};
use crate::template::reconcile::{
    clear_metadata, close_row, insert_class_dim_row, insert_class_row, insert_definition_row,
    insert_entity_element_row, insert_entity_row, insert_type_member, read_ec_row,
    revise_ec_row, set_metadata,
};
use crate::types::fold;

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

/// The store's name charset CHECK (`#2373`) for classes/entities/parameters —
/// mirrored here so a caller-contract problem fails as a clean error, never a
/// raw CHECK error mid-transaction.
fn charset_ok(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn lookup_scenario(conn: &Connection, name: &str) -> Result<Option<i64>, DbError> {
    match conn.query_row(
        "SELECT id FROM scenario WHERE cur_name = lower(?)",
        params![name],
        |r| r.get::<_, i64>(0),
    ) {
        Ok(id) => Ok(Some(id)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
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
    require_user(user)?;
    require_name("alternative", name)?;
    if let Some(id) = with_read(db, |conn| lookup_alternative(conn, name))? {
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
/// §1.1): close out the scenario's current `scenario_alternative` rows and
/// insert the given list at ranks `1..=n`, in ONE commit; a no-op (no commit)
/// when the current rank ORDER already equals the given order.
///
/// Rank values are **order-only semantics** (critique MINOR 4): ranks are
/// renumbered `1..=n` on every replace, so only the order is contract —
/// consumers (and the replay fingerprint) must compare rank order, never rank
/// numerals. Deliberately NOT a rank-insertion verb: v38's "insert strictly
/// below the minimum" is read → prepend → replace in the program, and the
/// program must mirror FlexTool's edge (only scenarios that already HAVE
/// rank rows receive the floor alternative — empty scenarios get nothing).
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

    // Read the scenario's CURRENT ranking in rank order (order-only compare).
    fn current_order(
        conn: &Connection,
        sid: i64,
    ) -> Result<Vec<(i64, i64, String)>, DbError> {
        // (alternative_id, sa-row version, folded name), rank-ascending.
        let mut stmt = conn.prepare(
            "SELECT sa.alternative_id, sa.version, lower(a.name) \
             FROM scenario_alternative sa \
             JOIN alternative a ON a.cur_id = sa.alternative_id \
             WHERE sa.cur_s = ? ORDER BY sa.cur_r",
        )?;
        let rows = stmt
            .query_map(params![sid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }
    let desired: Vec<String> = alternatives.iter().map(|a| fold(a)).collect();

    // Idempotency pre-check (read-only, no commit).
    let pre = with_read(db, |conn| {
        let Some(sid) = lookup_scenario(conn, scenario)? else { return Ok(None) };
        let cur = current_order(conn, sid)?;
        Ok(Some((sid, cur.into_iter().map(|(_, _, n)| n).collect::<Vec<_>>())))
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
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        // Resolve strictly BEFORE touching anything.
        let mut alt_ids: Vec<i64> = Vec::with_capacity(alternatives.len());
        for a in alternatives {
            let id = lookup_alternative(conn, a)?.ok_or_else(|| {
                DbError::Api(format!(
                    "set_scenario_alternatives: alternative {a:?} does not exist \
                     (create it first — ensure_alternative)"
                ))
            })?;
            alt_ids.push(id);
        }
        let sid = match lookup_scenario(conn, scenario)? {
            Some(id) => id,
            None => conn.query_row(
                "INSERT INTO scenario (name, valid_from_commit) VALUES (?, ?) RETURNING id",
                params![scenario, c],
                |r| r.get(0),
            )?,
        };
        let cur = current_order(conn, sid)?;
        if cur.iter().map(|(_, _, n)| n.as_str()).eq(desired.iter().map(String::as_str)) {
            return Ok(sid); // raced into the desired state
        }
        // Versions carry across the replace per (scenario, alternative) pair
        // — close-out-protocol citizenship, so history shows a revision, not
        // an unrelated new row.
        let prev_version: std::collections::HashMap<i64, i64> =
            cur.iter().map(|(aid, v, _)| (*aid, *v)).collect();
        conn.execute(
            "UPDATE scenario_alternative SET valid_to_commit = ? \
             WHERE cur_s = ? AND valid_from_commit < ?",
            params![c, sid, c],
        )?;
        for (i, aid) in alt_ids.iter().enumerate() {
            let version = prev_version.get(aid).map(|v| v + 1).unwrap_or(1);
            conn.execute(
                "INSERT INTO scenario_alternative \
                   (scenario_id, alternative_id, rank, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?)",
                params![sid, aid, (i + 1) as i64, c, version],
            )?;
        }
        Ok(sid)
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
    require_user(user)?;
    require_name("class", class)?;
    require_name("entity", entity)?;
    require_name("alternative", alternative)?;

    struct Target {
        entity_id: i64,
        alt_id: i64,
        current: Option<(bool, i64)>, // (active, version)
    }
    fn resolve(
        conn: &Connection,
        class: &str,
        entity: &str,
        alternative: &str,
    ) -> Result<Target, DbError> {
        let cid = lookup_entity_class(conn, class)?
            .ok_or_else(|| DbError::Api(format!("set_entity_activation: class {class:?} not found")))?;
        let entity_id = lookup_entity(conn, cid, entity)?.ok_or_else(|| {
            DbError::Api(format!("set_entity_activation: entity {class}:{entity} not found"))
        })?;
        let alt_id = lookup_alternative(conn, alternative)?.ok_or_else(|| {
            DbError::Api(format!(
                "set_entity_activation: alternative {alternative:?} not found"
            ))
        })?;
        let current = match conn.query_row(
            "SELECT active, version FROM entity_alternative WHERE cur_e = ? AND cur_a = ?",
            params![entity_id, alt_id],
            |r| Ok((r.get::<_, bool>(0)?, r.get::<_, i64>(1)?)),
        ) {
            Ok(v) => Some(v),
            Err(duckdb::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(e.into()),
        };
        Ok(Target { entity_id, alt_id, current })
    }

    // Idempotency pre-check (read-only, no commit).
    let noop = with_read(db, |conn| {
        let t = resolve(conn, class, entity, alternative)?;
        Ok(match (active, t.current) {
            (None, None) => true,
            (Some(b), Some((cur, _))) => cur == b,
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
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        let t = resolve(conn, class, entity, alternative)?;
        match (active, t.current) {
            (None, None) => {}
            (Some(b), Some((cur, _))) if cur == b => {}
            (None, Some(_)) => {
                // Close the row (the None instruction): versioned close-out.
                conn.execute(
                    "UPDATE entity_alternative SET valid_to_commit = ? \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit < ?",
                    params![c, t.entity_id, t.alt_id, c],
                )?;
            }
            (Some(b), prev) => {
                let version = match prev {
                    Some((_, v)) => {
                        conn.execute(
                            "UPDATE entity_alternative SET valid_to_commit = ? \
                             WHERE cur_e = ? AND cur_a = ? AND valid_from_commit < ?",
                            params![c, t.entity_id, t.alt_id, c],
                        )?;
                        v + 1
                    }
                    None => 1,
                };
                conn.execute(
                    "INSERT INTO entity_alternative \
                       (entity_id, alternative_id, active, valid_from_commit, version) \
                     VALUES (?, ?, ?, ?, ?)",
                    params![t.entity_id, t.alt_id, b, c, version],
                )?;
            }
        }
        Ok(())
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
    require_user(user)?;
    require_name("class", class)?;
    let noop = with_read(db, |conn| {
        let cid = lookup_entity_class(conn, class)?.ok_or_else(|| {
            DbError::Api(format!("set_class_active_by_default: class {class:?} not found"))
        })?;
        Ok(read_ec_row(conn, cid)?.active_by_default == active_by_default)
    })?;
    if noop {
        return Ok(());
    }
    let msg = format!("set_class_active_by_default {class} = {active_by_default}");
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        let cid = lookup_entity_class(conn, class)?.ok_or_else(|| {
            DbError::Api(format!("set_class_active_by_default: class {class:?} not found"))
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
    require_user(user)?;
    require_name("value list", list)?;
    let noop = with_read(db, |conn| Ok(lookup_value_list(conn, list)?.is_none()))?;
    if noop {
        return Ok(());
    }
    let msg = format!("drop_value_list {list}");
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        let Some(lid) = lookup_value_list(conn, list)? else {
            return Ok(()); // raced away between the read pre-check and here
        };
        let bound: i64 = conn.query_row(
            "SELECT count(*) FROM parameter_definition \
             WHERE valid_to_commit IS NULL AND parameter_value_list_id = ?",
            params![lid],
            |r| r.get(0),
        )?;
        if bound > 0 {
            return Err(DbError::Api(format!(
                "drop_value_list: {bound} current definition(s) still bind {list:?} \
                 — drop them first (drop_parameter) or rebind them"
            )));
        }
        conn.execute(
            "UPDATE list_value SET valid_to_commit = ? \
             WHERE parameter_value_list_id = ? AND valid_to_commit IS NULL",
            params![c, lid],
        )?;
        close_row(conn, c, "parameter_value_list", lid)?;
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
    require_user(user)?;
    if !charset_ok(name) {
        return Err(DbError::Api(format!(
            "ensure_entity_class: name {name:?} must match [A-Za-z0-9_]+"
        )));
    }
    if dims.len() > 4 {
        return Err(DbError::Api(format!(
            "ensure_entity_class: {} dimensions exceeds the rank cap of 4",
            dims.len()
        )));
    }

    // Existing-class dimension check, shared by pre-check and transaction.
    fn dims_of(conn: &Connection, cid: i64) -> Result<Vec<String>, DbError> {
        let mut stmt = conn.prepare(
            "SELECT COALESCE((SELECT lower(ec.name) FROM entity_class ec \
                              WHERE ec.cur_id = d.dimension_class_id), '<class_set>') \
             FROM entity_class_dimension d WHERE d.cur_c = ? ORDER BY d.cur_p",
        )?;
        let rows = stmt.query_map(params![cid], |r| r.get(0))?.collect::<Result<_, _>>()?;
        Ok(rows)
    }
    fn check_match(
        conn: &Connection,
        name: &str,
        cid: i64,
        dims: &[String],
    ) -> Result<i64, DbError> {
        let have = dims_of(conn, cid)?;
        let want: Vec<String> = dims.iter().map(|d| fold(d)).collect();
        if have == want {
            Ok(cid)
        } else {
            Err(DbError::Api(format!(
                "ensure_entity_class: {name:?} already exists with dimensions \
                 [{}], not [{}] — in-place re-dimensioning is the deferred \
                 reshape door (create a new class and migrate, the v25/v58 shape)",
                have.join(", "),
                want.join(", ")
            )))
        }
    }

    if let Some(cid) = with_read(db, |conn| lookup_entity_class(conn, name))? {
        return with_read(db, |conn| check_match(conn, name, cid, dims));
    }
    let msg = format!("ensure_entity_class {name} [{}]", dims.join(", "));
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        if let Some(cid) = lookup_entity_class(conn, name)? {
            return check_match(conn, name, cid, dims);
        }
        // Resolve every dimension class BEFORE inserting anything.
        let mut dim_ids: Vec<i64> = Vec::with_capacity(dims.len());
        for d in dims {
            let id = lookup_entity_class(conn, d)?.ok_or_else(|| {
                DbError::Api(format!(
                    "ensure_entity_class: dimension class {d:?} does not exist \
                     (create it first)"
                ))
            })?;
            dim_ids.push(id);
        }
        let cid = insert_class_row(conn, c, name, None, true, false, None)?;
        for (i, did) in dim_ids.iter().enumerate() {
            insert_class_dim_row(conn, c, cid, (i + 1) as i64, Some(*did), None)?;
        }
        Ok(cid)
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
    require_user(user)?;
    require_name("class", class)?;
    if !charset_ok(name) {
        return Err(DbError::Api(format!(
            "ensure_parameter_definition: name {name:?} must match [A-Za-z0-9_]+"
        )));
    }
    // Pure member validation (the load-time subset of `ir::validate`'s type
    // rules): legal kinds, D3 dim rules, rank cap, pairwise
    // distinguishability (D2a).
    for (ti, ty) in types.iter().enumerate() {
        let loc = format!("ensure_parameter_definition {class}.{name} types[{ti}]");
        match ty.kind.as_str() {
            "indexed" => match ty.dims.as_deref() {
                None | Some([]) => {
                    return Err(DbError::Api(format!(
                        "{loc}: kind \"indexed\" requires a non-empty dims list (D3)"
                    )))
                }
                Some(dims) => {
                    if dims.len() > 4 {
                        return Err(DbError::Api(format!(
                            "{loc}: {} dims exceeds the rank cap of 4",
                            dims.len()
                        )));
                    }
                    for d in dims {
                        if d.name.trim().is_empty() {
                            return Err(DbError::Api(format!("{loc}: empty dimension name")));
                        }
                        if !DIM_KINDS.contains(&d.kind.as_str()) {
                            return Err(DbError::Api(format!(
                                "{loc}: unknown dimension kind {:?} (expected one of {DIM_KINDS:?})",
                                d.kind
                            )));
                        }
                    }
                }
            },
            k if SCALAR_KINDS.contains(&k) => {
                if ty.dims.as_deref().map(|d| !d.is_empty()).unwrap_or(false) {
                    return Err(DbError::Api(format!(
                        "{loc}: scalar kind {k:?} must not declare dims (use kind: indexed)"
                    )));
                }
            }
            other => {
                return Err(DbError::Api(format!(
                    "{loc}: unknown kind {other:?} (expected {SCALAR_KINDS:?} or \"indexed\")"
                )))
            }
        }
    }
    let keys: Vec<_> = types.iter().map(|t| t.match_key()).collect();
    for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            if keys[i] == keys[j] {
                return Err(DbError::Api(format!(
                    "ensure_parameter_definition {class}.{name}: declared members {i} and \
                     {j} are indistinguishable ({}) — exactly-one-member matching would \
                     be impossible (D2a)",
                    types[i].describe()
                )));
            }
        }
    }

    if let Some(id) = with_read(db, |conn| {
        let Some(cid) = lookup_entity_class(conn, class)? else { return Ok(None) };
        lookup_parameter(conn, cid, name)
    })? {
        return Ok(id); // exists: as-is, never re-declared (D-F4)
    }

    let msg = format!("ensure_parameter_definition {class}.{name}");
    let types = types.to_vec();
    let value_list = value_list.map(str::to_string);
    let description = description.map(str::to_string);
    with_write_txn_retrying(db, user, &msg, move |conn, c| {
        let cid = lookup_entity_class(conn, class)?.ok_or_else(|| {
            DbError::Api(format!(
                "ensure_parameter_definition: class {class:?} not found \
                 (create it first — ensure_entity_class)"
            ))
        })?;
        if let Some(id) = lookup_parameter(conn, cid, name)? {
            return Ok(id); // raced: created since the pre-check (as-is, D-F4)
        }
        let lid = match value_list.as_deref() {
            None => None,
            Some(l) => Some(lookup_value_list(conn, l)?.ok_or_else(|| {
                DbError::Api(format!(
                    "ensure_parameter_definition: value_list {l:?} not found"
                ))
            })?),
        };
        // §3.5 inference: no explicit types + a bound list ⇒ the single
        // rank-0 member of the list's uniform member kind.
        let mut effective = types.clone();
        if effective.is_empty() {
            if let Some(lid) = lid {
                let mut stmt = conn.prepare(
                    "SELECT DISTINCT value_kind FROM list_value \
                     WHERE cur_l = ? AND valid_to_commit IS NULL ORDER BY value_kind",
                )?;
                let kinds: Vec<String> =
                    stmt.query_map(params![lid], |r| r.get(0))?.collect::<Result<_, _>>()?;
                match kinds.as_slice() {
                    [k] => effective.push(TypeDecl { kind: k.clone(), dims: None }),
                    [] => {
                        return Err(DbError::Api(format!(
                            "ensure_parameter_definition: cannot infer a kind from empty \
                             value_list {:?} (declare types explicitly)",
                            value_list.as_deref().unwrap_or("")
                        )))
                    }
                    many => {
                        return Err(DbError::Api(format!(
                            "ensure_parameter_definition: value_list {:?} members mix \
                             kinds {many:?} — declare types explicitly",
                            value_list.as_deref().unwrap_or("")
                        )))
                    }
                }
            }
        }
        let pd_id = insert_definition_row(
            conn,
            c,
            cid,
            name,
            description.as_deref(),
            lid,
            None,
            false,
            None,
        )?;
        for m in &effective {
            insert_type_member(conn, c, pd_id, m)?;
        }
        Ok(pd_id)
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
    require_user(user)?;
    require_name("class", class)?;
    let effective_name: String = match name {
        Some(n) => n.to_string(),
        None if !elements.is_empty() => elements.join("__"),
        None => {
            return Err(DbError::Api(
                "ensure_entity: a 0-D entity needs a name (N-D may derive it from \
                 elements)"
                    .into(),
            ))
        }
    };
    if !charset_ok(&effective_name) {
        return Err(DbError::Api(format!(
            "ensure_entity: entity name {effective_name:?} must match [A-Za-z0-9_]+"
        )));
    }

    struct ClassShape {
        cid: i64,
        // (position, dimension_class_id, dimension_class_set_id)
        dims: Vec<(i64, Option<i64>, Option<i64>)>,
    }
    fn shape(conn: &Connection, class: &str) -> Result<ClassShape, DbError> {
        let cid = lookup_entity_class(conn, class)?
            .ok_or_else(|| DbError::Api(format!("ensure_entity: class {class:?} not found")))?;
        let mut stmt = conn.prepare(
            "SELECT cur_p, dimension_class_id, dimension_class_set_id \
             FROM entity_class_dimension WHERE cur_c = ? ORDER BY cur_p",
        )?;
        let dims = stmt
            .query_map(params![cid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<Result<_, _>>()?;
        Ok(ClassShape { cid, dims })
    }
    fn verify_elements(
        conn: &Connection,
        class: &str,
        name: &str,
        eid: i64,
        elements: &[String],
    ) -> Result<(), DbError> {
        if elements.is_empty() {
            return Ok(()); // caller gave no tuple to verify against
        }
        let mut stmt = conn.prepare(
            "SELECT lower(e.name) FROM entity_element el \
             JOIN entity e ON e.cur_id = el.element_entity_id \
             WHERE el.cur_e = ? ORDER BY el.cur_p",
        )?;
        let have: Vec<String> =
            stmt.query_map(params![eid], |r| r.get(0))?.collect::<Result<_, _>>()?;
        let want: Vec<String> = elements.iter().map(|e| fold(e)).collect();
        if have == want {
            Ok(())
        } else {
            Err(DbError::Api(format!(
                "ensure_entity: {class}:{name} already exists with elements \
                 [{}], not [{}]",
                have.join(", "),
                want.join(", ")
            )))
        }
    }

    let pre = with_read(db, |conn| {
        let s = shape(conn, class)?;
        match lookup_entity(conn, s.cid, &effective_name)? {
            Some(eid) => {
                verify_elements(conn, class, &effective_name, eid, elements)?;
                Ok(Some(eid))
            }
            None => Ok(None),
        }
    })?;
    if let Some(eid) = pre {
        return Ok(eid);
    }

    let msg = format!("ensure_entity {class}:{effective_name}");
    with_write_txn_retrying(db, user, &msg, |conn, c| {
        let s = shape(conn, class)?;
        if let Some(eid) = lookup_entity(conn, s.cid, &effective_name)? {
            verify_elements(conn, class, &effective_name, eid, elements)?;
            return Ok(eid);
        }
        if s.dims.is_empty() {
            if !elements.is_empty() {
                return Err(DbError::Api(format!(
                    "ensure_entity: class {class:?} is 0-D but elements were given"
                )));
            }
        } else if elements.len() != s.dims.len() {
            return Err(DbError::Api(format!(
                "ensure_entity: class {class:?} is {}-D; got {} element(s)",
                s.dims.len(),
                elements.len()
            )));
        }
        // Resolve every element BEFORE inserting anything.
        let mut el_ids: Vec<(i64, i64)> = Vec::with_capacity(elements.len());
        for ((pos, dc, ds), el) in s.dims.iter().zip(elements.iter()) {
            let id = resolve_element_in_dim(conn, *dc, *ds, el)?.ok_or_else(|| {
                DbError::Api(format!(
                    "ensure_entity: element {el:?} at position {pos} does not name an \
                     entity of the dimension's class"
                ))
            })?;
            el_ids.push((*pos, id));
        }
        let eid = insert_entity_row(conn, c, s.cid, &effective_name, None)?;
        for (pos, el_id) in &el_ids {
            insert_entity_element_row(conn, c, eid, *pos, *el_id)?;
        }
        Ok(eid)
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
                n.as_deref().map(fold).map(|f| f != fold(name)).unwrap_or(false)
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

    let pre = with_read(db, read_stamp)?;
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
