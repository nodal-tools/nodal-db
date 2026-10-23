//! The sanctioned structural (non-value) write path — the fourth
//! `commit_changes` channel (structural_write_api_plan §2/§4/§5).
//!
//! Every table these appliers touch (`alternative`, `scenario`,
//! `scenario_alternative`, `entity_alternative`, `membership`, and — via the
//! `Create*` ops — `entity_class`, `entity`, `parameter_definition`) has ONE
//! writer site (invariant 2): the appliers here are the conn-taking bodies both
//! the standalone G8 verbs (their own-commit wrappers with an idempotent read
//! pre-check) AND this composable channel call. No table gets a second writer;
//! the history/typing rules live in exactly one place.
//!
//! History discipline (VMP Part 5, invariant 3): append-only close-out — create
//! is a fresh row, rename is close + same-id successor (version+1), ranking is
//! A-min contiguous `1..=n` minimal-diff, activation/membership is the per-key
//! close-out protocol. The `valid_from == commit_id` **same-commit guard** edits
//! a row created THIS commit in place (rename/flag) or physically DELETEs it (a
//! close), never closing it (would trip the span CHECK). An uncommitted insert
//! was never visible, so editing/deleting it is not a history rewrite (D-S5).
//!
//! Referent resolution is **strict** (lookup-only, hard error naming a miss,
//! §4.4) — the only auto-creates are the `Create*` ops and the scenario
//! auto-create inside `SetScenarioAlternatives`.

use std::collections::{HashMap, HashSet};

use duckdb::{params, Connection};

use crate::api::{
    insert_alternative_row, lookup_alternative, lookup_entity, lookup_entity_class,
    lookup_parameter, lookup_scenario, lookup_value_list,
};
use crate::error::DbError;
use crate::object_delete::resolve_element as resolve_element_in_dim;
use crate::template::ir::{TypeDecl, DIM_KINDS, SCALAR_KINDS};
use crate::template::reconcile::{
    insert_class_dim_row, insert_class_row, insert_definition_row, insert_entity_element_row,
    insert_entity_row, insert_membership_row, insert_scenario_row, insert_type_member,
    revise_alternative_row, revise_scenario_row,
};
use crate::types::{
    charset_ok, fold, EntityAddr, ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef,
    StructuralChange, StructuralEffect, StructuralKind, StructuralOutcome, WriteReport,
};

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// Apply one [`StructuralChange`] under the open transaction's `commit_id`,
/// appending its [`StructuralOutcome`] to `report.structural`. Items apply
/// sequentially against the one open txn (§4.1), so item N's inserts are visible
/// to item N+1's strict lookups (create → activate resolves in one save).
pub(crate) fn apply_structural(
    conn: &Connection,
    c: i64,
    change_index: usize,
    change: &StructuralChange,
    report: &mut WriteReport,
) -> Result<(), DbError> {
    let (kind, object_id, effect) = match change {
        StructuralChange::CreateAlternative { name, description } => {
            let (id, e) = apply_create_alternative(conn, c, name, description.as_deref())?;
            (StructuralKind::Alternative, id, e)
        }
        StructuralChange::RenameAlternative {
            current,
            new_name,
            description,
        } => {
            let (id, e) =
                apply_rename_alternative(conn, c, current, new_name, description.as_deref())?;
            (StructuralKind::Alternative, id, e)
        }
        StructuralChange::CreateScenario { name, description } => {
            let (id, e) = apply_create_scenario(conn, c, name, description.as_deref())?;
            (StructuralKind::Scenario, id, e)
        }
        StructuralChange::RenameScenario {
            current,
            new_name,
            description,
        } => {
            let (id, e) =
                apply_rename_scenario(conn, c, current, new_name, description.as_deref())?;
            (StructuralKind::Scenario, id, e)
        }
        StructuralChange::SetScenarioAlternatives {
            scenario,
            alternatives,
        } => {
            let (id, e) = apply_set_scenario_alternatives(conn, c, scenario, alternatives)?;
            (StructuralKind::ScenarioRanking, id, e)
        }
        StructuralChange::SetEntityActivation {
            class,
            entity,
            alternative,
            active,
        } => {
            let (id, e) =
                apply_set_entity_activation(conn, c, class, entity, alternative, *active)?;
            (StructuralKind::Activation, id, e)
        }
        StructuralChange::SetMembership {
            group,
            member,
            alternative,
            included,
        } => {
            let (id, e) =
                apply_set_membership(conn, c, group, member, alternative.as_deref(), *included)?;
            (StructuralKind::Membership, id, e)
        }
        StructuralChange::CreateEntity {
            class,
            name,
            elements,
        } => {
            let (id, e) = apply_create_entity(conn, c, class, name.as_deref(), elements)?;
            (StructuralKind::Entity, id, e)
        }
        StructuralChange::CreateEntityClass { name, dims } => {
            let (id, e) = apply_create_entity_class(conn, c, name, dims)?;
            (StructuralKind::EntityClass, id, e)
        }
        StructuralChange::CreateParameter {
            class,
            name,
            types,
            value_list,
            description,
        } => {
            let (id, e) = apply_create_parameter(
                conn,
                c,
                class,
                name,
                types,
                value_list.as_deref(),
                description.as_deref(),
            )?;
            (StructuralKind::Parameter, id, e)
        }
    };
    report.structural.push(StructuralOutcome {
        change_index,
        kind,
        object_id,
        effect,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Alternatives + scenarios (identity rows)
// ---------------------------------------------------------------------------

pub(crate) fn apply_create_alternative(
    conn: &Connection,
    c: i64,
    name: &str,
    description: Option<&str>,
) -> Result<(i64, StructuralEffect), DbError> {
    if let Some(id) = lookup_alternative(conn, name)? {
        return Ok((id, StructuralEffect::NoOp)); // idempotent
    }
    let id = insert_alternative_row(conn, c, name, description)?;
    Ok((id, StructuralEffect::Created))
}

pub(crate) fn apply_create_scenario(
    conn: &Connection,
    c: i64,
    name: &str,
    description: Option<&str>,
) -> Result<(i64, StructuralEffect), DbError> {
    if let Some(id) = lookup_scenario(conn, name)? {
        return Ok((id, StructuralEffect::NoOp));
    }
    let id = insert_scenario_row(conn, c, name, description)?;
    Ok((id, StructuralEffect::Created))
}

struct IdentityRow {
    name: String,
    description: Option<String>,
    version: i64,
    valid_from: i64,
}

fn read_identity(conn: &Connection, table: &str, id: i64) -> Result<IdentityRow, DbError> {
    Ok(conn.query_row(
        &format!(
            "SELECT name, description, version, valid_from_commit FROM {table} WHERE cur_id = ?"
        ),
        params![id],
        |r| {
            Ok(IdentityRow {
                name: r.get(0)?,
                description: r.get(1)?,
                version: r.get(2)?,
                valid_from: r.get(3)?,
            })
        },
    )?)
}

/// Versioned identity rename (close + same-id successor, version+1), with the
/// same-commit in-place edit (D-S5) and the self-excluding uniqueness check (m5).
type Revise = fn(&Connection, i64, i64, &str, Option<&str>, i64) -> Result<(), DbError>;
type Lookup = fn(&Connection, &str) -> Result<Option<i64>, DbError>;

#[allow(clippy::too_many_arguments)]
fn rename_identity(
    conn: &Connection,
    c: i64,
    table: &str,
    id: i64,
    new_name: &str,
    description: Option<&str>,
    lookup: Lookup,
    revise: Revise,
) -> Result<StructuralEffect, DbError> {
    let row = read_identity(conn, table, id)?;
    // Uniqueness: `new_name` must not collide with a DIFFERENT live object; the
    // self row (by id) is excluded, so a pure case-restyle is not a false
    // self-collision (critique m5). A clean error, never a raw uq_* violation.
    if let Some(other) = lookup(conn, new_name)? {
        if other != id {
            return Err(DbError::Api(format!(
                "rename {table}: {new_name:?} already names a different {table}"
            )));
        }
    }
    let name_same = fold(&row.name) == fold(new_name);
    let desc_change = description.is_some() && description != row.description.as_deref();
    if name_same && !desc_change {
        return Ok(StructuralEffect::NoOp);
    }
    let new_desc = description.or(row.description.as_deref());
    if row.valid_from == c {
        // Created THIS commit: edit the not-yet-visible row in place (D-S5).
        conn.execute(
            &format!(
                "UPDATE {table} SET name = ?, description = ? \
                 WHERE id = ? AND valid_to_commit IS NULL"
            ),
            params![new_name, new_desc, id],
        )?;
    } else {
        revise(conn, c, id, new_name, new_desc, row.version)?;
    }
    Ok(StructuralEffect::Renamed)
}

pub(crate) fn apply_rename_alternative(
    conn: &Connection,
    c: i64,
    current: &str,
    new_name: &str,
    description: Option<&str>,
) -> Result<(i64, StructuralEffect), DbError> {
    let id = lookup_alternative(conn, current)?.ok_or_else(|| {
        DbError::Api(format!(
            "rename_alternative: alternative {current:?} does not exist"
        ))
    })?;
    let e = rename_identity(
        conn,
        c,
        "alternative",
        id,
        new_name,
        description,
        lookup_alternative,
        revise_alternative_row,
    )?;
    Ok((id, e))
}

pub(crate) fn apply_rename_scenario(
    conn: &Connection,
    c: i64,
    current: &str,
    new_name: &str,
    description: Option<&str>,
) -> Result<(i64, StructuralEffect), DbError> {
    let id = lookup_scenario(conn, current)?.ok_or_else(|| {
        DbError::Api(format!(
            "rename_scenario: scenario {current:?} does not exist"
        ))
    })?;
    let e = rename_identity(
        conn,
        c,
        "scenario",
        id,
        new_name,
        description,
        lookup_scenario,
        revise_scenario_row,
    )?;
    Ok((id, e))
}

// ---------------------------------------------------------------------------
// Scenario ranking — A-min (§4.3, critique F1: normatively two-phase)
// ---------------------------------------------------------------------------

pub(crate) fn apply_set_scenario_alternatives(
    conn: &Connection,
    c: i64,
    scenario: &str,
    alternatives: &[String],
) -> Result<(i64, StructuralEffect), DbError> {
    // Strict resolution of every alternative BEFORE touching anything.
    let mut alt_ids: Vec<i64> = Vec::with_capacity(alternatives.len());
    for a in alternatives {
        let id = lookup_alternative(conn, a)?.ok_or_else(|| {
            DbError::Api(format!(
                "set_scenario_alternatives: alternative {a:?} does not exist (create it first)"
            ))
        })?;
        alt_ids.push(id);
    }
    // Resolve or auto-create the scenario (the one sanctioned auto-create, §4.4).
    let (sid, scenario_created) = match lookup_scenario(conn, scenario)? {
        Some(id) => (id, false),
        None => (insert_scenario_row(conn, c, scenario, None)?, true),
    };

    // Current ranking: alternative_id -> (rank, version).
    let mut stmt = conn.prepare(
        "SELECT alternative_id, rank, version FROM scenario_alternative \
         WHERE cur_s = ? ORDER BY cur_r",
    )?;
    let cur: Vec<(i64, i64, i64)> = stmt
        .query_map(params![sid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<_, _>>()?;
    drop(stmt);
    let cur_map: HashMap<i64, (i64, i64)> = cur.iter().map(|(a, r, v)| (*a, (*r, *v))).collect();
    let desired: HashMap<i64, i64> = alt_ids
        .iter()
        .enumerate()
        .map(|(i, a)| (*a, (i + 1) as i64))
        .collect();

    // Classify: changed ∪ removed (close), changed ∪ added (insert).
    let mut to_close: Vec<i64> = Vec::new();
    for (aid, (rank, _)) in &cur_map {
        match desired.get(aid) {
            Some(d) if *d == *rank => {} // unchanged — never touched
            _ => to_close.push(*aid),    // changed or removed
        }
    }
    let mut to_insert: Vec<(i64, i64, i64)> = Vec::new(); // (aid, rank, version)
    for (i, aid) in alt_ids.iter().enumerate() {
        let rank = (i + 1) as i64;
        match cur_map.get(aid) {
            Some((cur_rank, ver)) => {
                if *cur_rank != rank {
                    to_insert.push((*aid, rank, ver + 1)); // changed: version carries +1
                }
            }
            None => to_insert.push((*aid, rank, 1)), // added: starts at 1
        }
    }

    if to_close.is_empty() && to_insert.is_empty() {
        // Nothing to write. If we just created the scenario (empty list on an
        // absent scenario), that create IS the effect; otherwise a true no-op.
        let effect = if scenario_created {
            StructuralEffect::Created
        } else {
            StructuralEffect::NoOp
        };
        return Ok((sid, effect));
    }

    // Phase 1 — close ALL of (changed ∪ removed) in ONE guarded UPDATE, BEFORE
    // any insert (F1). This vacates exactly the complementary rank-set of the
    // untouched rows, so every phase-2 insert hits a free rank — collision-free.
    if !to_close.is_empty() {
        let placeholders = vec!["?"; to_close.len()].join(", ");
        let sql = format!(
            "UPDATE scenario_alternative SET valid_to_commit = ? \
             WHERE cur_s = ? AND cur_a IN ({placeholders}) AND valid_from_commit < ?"
        );
        let mut binds: Vec<i64> = Vec::with_capacity(to_close.len() + 3);
        binds.push(c);
        binds.push(sid);
        binds.extend_from_slice(&to_close);
        binds.push(c);
        conn.execute(&sql, duckdb::params_from_iter(binds.iter()))?;
    }
    // Phase 2 — insert (changed ∪ added) at their desired ranks.
    for (aid, rank, version) in &to_insert {
        conn.execute(
            "INSERT INTO scenario_alternative \
               (scenario_id, alternative_id, rank, valid_from_commit, version) \
             VALUES (?, ?, ?, ?, ?)",
            params![sid, aid, rank, c, version],
        )?;
    }

    let effect = if to_insert.is_empty() {
        StructuralEffect::Cleared // desired empty / everything removed
    } else {
        StructuralEffect::Updated
    };
    Ok((sid, effect))
}

// ---------------------------------------------------------------------------
// Entity activation — per-key close-out protocol + same-commit guard (m4)
// ---------------------------------------------------------------------------

pub(crate) fn apply_set_entity_activation(
    conn: &Connection,
    c: i64,
    class: &str,
    entity: &str,
    alternative: &str,
    active: Option<bool>,
) -> Result<(i64, StructuralEffect), DbError> {
    let cid = lookup_entity_class(conn, class)?
        .ok_or_else(|| DbError::Api(format!("set_entity_activation: class {class:?} not found")))?;
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
    // (active, version, valid_from) of the current row, if any.
    let current = opt_row3(
        conn,
        "SELECT active, version, valid_from_commit FROM entity_alternative \
         WHERE cur_e = ? AND cur_a = ?",
        entity_id,
        alt_id,
    )?;
    let effect = match (active, current) {
        (None, None) => StructuralEffect::NoOp,
        (Some(b), Some((cur, _, _))) if cur == b => StructuralEffect::NoOp,
        (None, Some((_, _, vf))) => {
            if vf == c {
                // Created this commit: DELETE the uncommitted insert (D-S5).
                conn.execute(
                    "DELETE FROM entity_alternative \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit = ?",
                    params![entity_id, alt_id, c],
                )?;
            } else {
                conn.execute(
                    "UPDATE entity_alternative SET valid_to_commit = ? \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit < ?",
                    params![c, entity_id, alt_id, c],
                )?;
            }
            StructuralEffect::Cleared
        }
        (Some(b), Some((_, v, vf))) => {
            if vf == c {
                // Created this commit: edit `active` in place, no version bump.
                conn.execute(
                    "UPDATE entity_alternative SET active = ? \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit = ?",
                    params![b, entity_id, alt_id, c],
                )?;
            } else {
                conn.execute(
                    "UPDATE entity_alternative SET valid_to_commit = ? \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit < ?",
                    params![c, entity_id, alt_id, c],
                )?;
                insert_activation_row(conn, c, entity_id, alt_id, b, v + 1)?;
            }
            StructuralEffect::Updated
        }
        (Some(b), None) => {
            insert_activation_row(conn, c, entity_id, alt_id, b, 1)?;
            StructuralEffect::Updated
        }
    };
    Ok((entity_id, effect))
}

fn insert_activation_row(
    conn: &Connection,
    c: i64,
    entity_id: i64,
    alt_id: i64,
    active: bool,
    version: i64,
) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO entity_alternative \
           (entity_id, alternative_id, active, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?)",
        params![entity_id, alt_id, active, c, version],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Membership — activation mirror with the base-row sentinel spelled out (M4)
// ---------------------------------------------------------------------------

pub(crate) fn apply_set_membership(
    conn: &Connection,
    c: i64,
    group: &EntityAddr,
    member: &EntityAddr,
    alternative: Option<&str>,
    included: Option<bool>,
) -> Result<(i64, StructuralEffect), DbError> {
    let gid = resolve_entity_addr(conn, group, "group")?;
    let mid = resolve_entity_addr(conn, member, "member")?;
    let alt_id: Option<i64> =
        match alternative {
            Some(a) => Some(lookup_alternative(conn, a)?.ok_or_else(|| {
                DbError::Api(format!("set_membership: alternative {a:?} not found"))
            })?),
            None => None,
        };
    // Base rows join `one_current_membership` on cur_a = coalesce(alt, 0), so a
    // base lookup/close keys on cur_a = 0 (NEVER `alternative_id = ?` bound to
    // NULL, which matches nothing); an insert stores NULL, never 0 (M4).
    let cur_a: i64 = alt_id.unwrap_or(0);
    let current = membership_current(conn, gid, mid, cur_a)?;
    let effect = match (included, current) {
        (None, None) => StructuralEffect::NoOp,
        (Some(b), Some((cur, _, _))) if cur == b => StructuralEffect::NoOp,
        (None, Some((_, _, vf))) => {
            if vf == c {
                conn.execute(
                    "DELETE FROM membership \
                     WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit = ?",
                    params![gid, mid, cur_a, c],
                )?;
            } else {
                conn.execute(
                    "UPDATE membership SET valid_to_commit = ? \
                     WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit < ?",
                    params![c, gid, mid, cur_a, c],
                )?;
            }
            StructuralEffect::Cleared
        }
        (Some(b), prev) => {
            match prev {
                Some((_, _, vf)) if vf == c => {
                    // Created this commit: edit `included` in place (D-S5).
                    conn.execute(
                        "UPDATE membership SET included = ? \
                         WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit = ?",
                        params![b, gid, mid, cur_a, c],
                    )?;
                }
                Some((_, v, _)) => {
                    conn.execute(
                        "UPDATE membership SET valid_to_commit = ? \
                         WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit < ?",
                        params![c, gid, mid, cur_a, c],
                    )?;
                    insert_membership_row(conn, c, gid, mid, alt_id, b, v + 1)?;
                }
                None => insert_membership_row(conn, c, gid, mid, alt_id, b, 1)?,
            }
            StructuralEffect::Updated
        }
    };
    let object_id = if matches!(effect, StructuralEffect::Cleared) {
        0
    } else {
        gid
    };
    Ok((object_id, effect))
}

fn membership_current(
    conn: &Connection,
    gid: i64,
    mid: i64,
    cur_a: i64,
) -> Result<Option<(bool, i64, i64)>, DbError> {
    match conn.query_row(
        "SELECT included, version, valid_from_commit FROM membership \
         WHERE cur_g = ? AND cur_m = ? AND cur_a = ?",
        params![gid, mid, cur_a],
        |r| {
            Ok((
                r.get::<_, bool>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        },
    ) {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn resolve_entity_addr(conn: &Connection, addr: &EntityAddr, role: &str) -> Result<i64, DbError> {
    let cid = lookup_entity_class(conn, &addr.class)?.ok_or_else(|| {
        DbError::Api(format!(
            "set_membership: {role} class {:?} not found",
            addr.class
        ))
    })?;
    lookup_entity(conn, cid, &addr.name)?.ok_or_else(|| {
        DbError::Api(format!(
            "set_membership: {role} entity {}:{} not found",
            addr.class, addr.name
        ))
    })
}

/// A `(bool, i64, i64)` current-row read keyed on two ids (activation), or
/// `None` for no current row.
fn opt_row3(
    conn: &Connection,
    sql: &str,
    a: i64,
    b: i64,
) -> Result<Option<(bool, i64, i64)>, DbError> {
    match conn.query_row(sql, params![a, b], |r| {
        Ok((
            r.get::<_, bool>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    }) {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

// ---------------------------------------------------------------------------
// D-S9 creates — the ensure_* verb txn bodies, shared (critique F4)
// ---------------------------------------------------------------------------

/// Declared dimension class names of `cid`, in position order (`<class_set>` for
/// a class-set dimension). Shared by [`apply_create_entity_class`] and the verb's
/// read pre-check.
pub(crate) fn class_dims_of(conn: &Connection, cid: i64) -> Result<Vec<String>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT COALESCE((SELECT lower(ec.name) FROM entity_class ec \
                          WHERE ec.cur_id = d.dimension_class_id), '<class_set>') \
         FROM entity_class_dimension d WHERE d.cur_c = ? ORDER BY d.cur_p",
    )?;
    let rows = stmt
        .query_map(params![cid], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// An EXISTING class matches iff its declared dims equal `dims` (folded, in
/// order); otherwise a hard error — the deferred reshape door (the v25/v58 shape).
pub(crate) fn check_class_dims(
    conn: &Connection,
    name: &str,
    cid: i64,
    dims: &[String],
) -> Result<i64, DbError> {
    let have = class_dims_of(conn, cid)?;
    let want: Vec<String> = dims.iter().map(|d| fold(d)).collect();
    if have == want {
        Ok(cid)
    } else {
        Err(DbError::Api(format!(
            "ensure_entity_class: {name:?} already exists with dimensions [{}], not [{}] — \
             in-place re-dimensioning (reshape) is not supported; create a new class and \
             migrate the data",
            have.join(", "),
            want.join(", ")
        )))
    }
}

pub(crate) fn apply_create_entity_class(
    conn: &Connection,
    c: i64,
    name: &str,
    dims: &[String],
) -> Result<(i64, StructuralEffect), DbError> {
    if let Some(cid) = lookup_entity_class(conn, name)? {
        return Ok((
            check_class_dims(conn, name, cid, dims)?,
            StructuralEffect::NoOp,
        ));
    }
    // Resolve every dimension class BEFORE inserting anything.
    let mut dim_ids: Vec<i64> = Vec::with_capacity(dims.len());
    for d in dims {
        let id = lookup_entity_class(conn, d)?.ok_or_else(|| {
            DbError::Api(format!(
                "ensure_entity_class: dimension class {d:?} does not exist (create it first)"
            ))
        })?;
        dim_ids.push(id);
    }
    let cid = insert_class_row(conn, c, name, None, true, false, None)?;
    for (i, did) in dim_ids.iter().enumerate() {
        insert_class_dim_row(conn, c, cid, (i + 1) as i64, Some(*did), None)?;
    }
    Ok((cid, StructuralEffect::Created))
}

/// The class shape (`cid` + ordered `(position, dim_class, dim_set)`), shared by
/// [`apply_create_entity`] and the verb's read pre-check.
pub(crate) struct ClassShape {
    pub(crate) cid: i64,
    pub(crate) dims: Vec<(i64, Option<i64>, Option<i64>)>,
}

pub(crate) fn entity_class_shape(conn: &Connection, class: &str) -> Result<ClassShape, DbError> {
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

/// Verify an EXISTING entity's live element tuple equals `elements` (folded, in
/// order) when a tuple is given; a mismatch is a hard error (never a silent
/// alias). Shared by [`apply_create_entity`] and the verb's read pre-check.
pub(crate) fn verify_entity_elements(
    conn: &Connection,
    class: &str,
    name: &str,
    eid: i64,
    elements: &[String],
) -> Result<(), DbError> {
    if elements.is_empty() {
        return Ok(());
    }
    let mut stmt = conn.prepare(
        "SELECT lower(e.name) FROM entity_element el \
         JOIN entity e ON e.cur_id = el.element_entity_id \
         WHERE el.cur_e = ? ORDER BY el.cur_p",
    )?;
    let have: Vec<String> = stmt
        .query_map(params![eid], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    let want: Vec<String> = elements.iter().map(|e| fold(e)).collect();
    if have == want {
        Ok(())
    } else {
        Err(DbError::Api(format!(
            "ensure_entity: {class}:{name} already exists with elements [{}], not [{}]",
            have.join(", "),
            want.join(", ")
        )))
    }
}

/// The effective entity name: the given `name`, else the elements joined with
/// `__` (N-D), else an error (a 0-D entity needs a name).
pub(crate) fn effective_entity_name(
    name: Option<&str>,
    elements: &[String],
) -> Result<String, DbError> {
    match name {
        Some(n) => Ok(n.to_string()),
        None if !elements.is_empty() => Ok(elements.join("__")),
        None => Err(DbError::Api(
            "ensure_entity: a 0-D entity needs a name (N-D may derive it from elements)".into(),
        )),
    }
}

pub(crate) fn apply_create_entity(
    conn: &Connection,
    c: i64,
    class: &str,
    name: Option<&str>,
    elements: &[String],
) -> Result<(i64, StructuralEffect), DbError> {
    let effective_name = effective_entity_name(name, elements)?;
    let s = entity_class_shape(conn, class)?;
    if let Some(eid) = lookup_entity(conn, s.cid, &effective_name)? {
        verify_entity_elements(conn, class, &effective_name, eid, elements)?;
        return Ok((eid, StructuralEffect::NoOp));
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
    // Resolve every element BEFORE inserting anything (strict, §4.4).
    let mut el_ids: Vec<(i64, i64)> = Vec::with_capacity(elements.len());
    for ((pos, dc, ds), el) in s.dims.iter().zip(elements.iter()) {
        let id = resolve_element_in_dim(conn, *dc, *ds, el)?.ok_or_else(|| {
            DbError::Api(format!(
                "ensure_entity: element {el:?} at position {pos} does not name an entity of \
                 the dimension's class"
            ))
        })?;
        el_ids.push((*pos, id));
    }
    let eid = insert_entity_row(conn, c, s.cid, &effective_name, None)?;
    for (pos, el_id) in &el_ids {
        insert_entity_element_row(conn, c, eid, *pos, *el_id)?;
    }
    Ok((eid, StructuralEffect::Created))
}

/// Pure member-declaration validation (the load-time subset of `ir::validate`):
/// legal kinds, D3 dim rules, rank cap, pairwise distinguishability (D2a).
/// Shared by the G8 verb's pre-section and [`prepare_structural`] (`what` names
/// the calling op in messages).
pub(crate) fn validate_type_decls(
    what: &str,
    class: &str,
    name: &str,
    types: &[TypeDecl],
) -> Result<(), DbError> {
    for (ti, ty) in types.iter().enumerate() {
        let loc = format!("{what} {class}.{name} types[{ti}]");
        match ty.kind.as_str() {
            "indexed" => match ty.dims.as_deref() {
                None | Some([]) => {
                    return Err(DbError::Api(format!(
                        "{loc}: kind \"indexed\" requires a non-empty dims list"
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
                    "{what} {class}.{name}: declared members {i} and {j} are indistinguishable \
                     ({}) — exactly-one-member matching would be impossible",
                    types[i].describe()
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn apply_create_parameter(
    conn: &Connection,
    c: i64,
    class: &str,
    name: &str,
    types: &[TypeDecl],
    value_list: Option<&str>,
    description: Option<&str>,
) -> Result<(i64, StructuralEffect), DbError> {
    let cid = lookup_entity_class(conn, class)?.ok_or_else(|| {
        DbError::Api(format!(
            "ensure_parameter_definition: class {class:?} not found (create it first)"
        ))
    })?;
    if let Some(id) = lookup_parameter(conn, cid, name)? {
        return Ok((id, StructuralEffect::NoOp)); // exists: as-is, never re-declared (D-F4)
    }
    let lid = match value_list {
        None => None,
        Some(l) => Some(lookup_value_list(conn, l)?.ok_or_else(|| {
            DbError::Api(format!(
                "ensure_parameter_definition: value_list {l:?} not found"
            ))
        })?),
    };
    // §3.5 inference: no explicit types + a bound list ⇒ the single rank-0 member
    // of the list's uniform member kind.
    let mut effective = types.to_vec();
    if effective.is_empty() {
        if let Some(lid) = lid {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT value_kind FROM list_value \
                 WHERE cur_l = ? AND valid_to_commit IS NULL ORDER BY value_kind",
            )?;
            let kinds: Vec<String> = stmt
                .query_map(params![lid], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            match kinds.as_slice() {
                [k] => effective.push(TypeDecl {
                    kind: k.clone(),
                    dims: None,
                }),
                [] => {
                    return Err(DbError::Api(format!(
                        "ensure_parameter_definition: cannot infer a kind from empty value_list \
                         {:?} (declare types explicitly)",
                        value_list.unwrap_or("")
                    )))
                }
                many => {
                    return Err(DbError::Api(format!(
                        "ensure_parameter_definition: value_list {:?} members mix kinds {many:?} \
                         — declare types explicitly",
                        value_list.unwrap_or("")
                    )))
                }
            }
        }
    }
    let pd_id = insert_definition_row(conn, c, cid, name, description, lid, None, false, None)?;
    for m in &effective {
        insert_type_member(conn, c, pd_id, m)?;
    }
    Ok((pd_id, StructuralEffect::Created))
}

// ---------------------------------------------------------------------------
// Pure prepare-time guards (§4.5) — name-level, no DB. A caller-contract
// problem is a clean DbError::Api, never a raw CHECK/index error mid-txn.
// ---------------------------------------------------------------------------

pub(crate) fn prepare_structural(
    changes: &[StructuralChange],
    object_deletes: &[(ObjectDeleteKey, ObjectDeleteOpts)],
) -> Result<(), DbError> {
    // Per-change name/charset/type validation.
    for ch in changes {
        validate_change(ch)?;
    }

    // At most one SetScenarioAlternatives per folded scenario (critique M3).
    let mut set_scenarios: HashSet<String> = HashSet::new();
    for ch in changes {
        if let StructuralChange::SetScenarioAlternatives { scenario, .. } = ch {
            if !set_scenarios.insert(fold(scenario)) {
                return Err(DbError::Api(format!(
                    "structural: two SetScenarioAlternatives target scenario {scenario:?} in one \
                     commit — one declarative set per scenario per commit"
                )));
            }
        }
    }

    // Contradictory renames: two different new_names for one object (§4.5).
    check_rename_coherence(changes, |ch| match ch {
        StructuralChange::RenameAlternative {
            current, new_name, ..
        } => Some((current, new_name)),
        _ => None,
    })?;
    check_rename_coherence(changes, |ch| match ch {
        StructuralChange::RenameScenario {
            current, new_name, ..
        } => Some((current, new_name)),
        _ => None,
    })?;

    // Cross-channel object-delete guard (critique M2): a commit that both
    // structurally references an object AND object-deletes it by folded name
    // would leave a live orphan under the same-commit guard.
    let mut del_alts: HashSet<String> = HashSet::new();
    let mut del_scens: HashSet<String> = HashSet::new();
    let mut del_entities: HashSet<(String, String)> = HashSet::new();
    for (key, _) in object_deletes {
        if let ObjectRef::Name(n) = &key.target {
            match key.kind {
                ObjectKind::Alternative => {
                    del_alts.insert(fold(n));
                }
                ObjectKind::Scenario => {
                    del_scens.insert(fold(n));
                }
                ObjectKind::Entity => {
                    // Element-tuple-addressed (`ObjectRef::Elements`) entity
                    // deletes are intentionally NOT matched by this name-level
                    // guard — that residual self-heals in resolution (a D5-class
                    // edge, like the value-write/delete edges).
                    if let Some(cls) = &key.class {
                        del_entities.insert((fold(cls), fold(n)));
                    }
                }
                _ => {}
            }
        }
    }
    let clash_alt = |name: &str| del_alts.contains(&fold(name));
    let clash_scen = |name: &str| del_scens.contains(&fold(name));
    let clash_entity = |class: &str, name: &str| del_entities.contains(&(fold(class), fold(name)));
    let orphan = |kind: &str, name: &str| {
        DbError::Api(format!(
            "structural: {kind} {name:?} is both structurally referenced and object-deleted in \
             one commit — the same-commit guard would leave a live orphan"
        ))
    };
    for ch in changes {
        match ch {
            StructuralChange::SetScenarioAlternatives {
                scenario,
                alternatives,
            } => {
                for a in alternatives {
                    if clash_alt(a) {
                        return Err(orphan("alternative", a));
                    }
                }
                if !alternatives.is_empty() && clash_scen(scenario) {
                    return Err(orphan("scenario", scenario));
                }
            }
            StructuralChange::SetEntityActivation {
                class,
                entity,
                alternative,
                ..
            } => {
                if clash_entity(class, entity) {
                    return Err(orphan("entity", entity));
                }
                if clash_alt(alternative) {
                    return Err(orphan("alternative", alternative));
                }
            }
            StructuralChange::SetMembership {
                group,
                member,
                alternative,
                ..
            } => {
                if clash_entity(&group.class, &group.name) {
                    return Err(orphan("entity", &group.name));
                }
                if clash_entity(&member.class, &member.name) {
                    return Err(orphan("entity", &member.name));
                }
                if let Some(a) = alternative {
                    if clash_alt(a) {
                        return Err(orphan("alternative", a));
                    }
                }
            }
            StructuralChange::RenameAlternative {
                current, new_name, ..
            } => {
                if clash_alt(current) || clash_alt(new_name) {
                    return Err(orphan("alternative", current));
                }
            }
            StructuralChange::RenameScenario {
                current, new_name, ..
            } if (clash_scen(current) || clash_scen(new_name)) => {
                return Err(orphan("scenario", current));
            }
            _ => {}
        }
    }
    Ok(())
}

fn require_nonempty(what: &str, name: &str) -> Result<(), DbError> {
    if name.trim().is_empty() {
        return Err(DbError::Api(format!(
            "structural: {what} name must be non-empty"
        )));
    }
    Ok(())
}

fn require_charset(what: &str, name: &str) -> Result<(), DbError> {
    if !charset_ok(name) {
        return Err(DbError::Api(format!(
            "structural: {what} name {name:?} must be non-empty, trimmed, and free of control characters"
        )));
    }
    Ok(())
}

fn validate_change(ch: &StructuralChange) -> Result<(), DbError> {
    match ch {
        StructuralChange::CreateAlternative { name, .. } => require_nonempty("alternative", name),
        StructuralChange::RenameAlternative {
            current, new_name, ..
        } => {
            require_nonempty("alternative", current)?;
            require_nonempty("alternative", new_name)
        }
        StructuralChange::CreateScenario { name, .. } => require_nonempty("scenario", name),
        StructuralChange::RenameScenario {
            current, new_name, ..
        } => {
            require_nonempty("scenario", current)?;
            require_nonempty("scenario", new_name)
        }
        StructuralChange::SetScenarioAlternatives {
            scenario,
            alternatives,
        } => {
            require_nonempty("scenario", scenario)?;
            let mut seen: HashSet<String> = HashSet::new();
            for a in alternatives {
                require_nonempty("alternative", a)?;
                if !seen.insert(fold(a)) {
                    return Err(DbError::Api(format!(
                        "set_scenario_alternatives: alternative {a:?} appears twice \
                         (name resolution is case-insensitive)"
                    )));
                }
            }
            Ok(())
        }
        StructuralChange::SetEntityActivation {
            class,
            entity,
            alternative,
            ..
        } => {
            require_nonempty("class", class)?;
            require_nonempty("entity", entity)?;
            require_nonempty("alternative", alternative)
        }
        StructuralChange::SetMembership {
            group,
            member,
            alternative,
            ..
        } => {
            require_nonempty("group class", &group.class)?;
            require_nonempty("group", &group.name)?;
            require_nonempty("member class", &member.class)?;
            require_nonempty("member", &member.name)?;
            if let Some(a) = alternative {
                require_nonempty("alternative", a)?;
            }
            Ok(())
        }
        StructuralChange::CreateEntity {
            class,
            name,
            elements,
        } => {
            require_nonempty("class", class)?;
            let effective = effective_entity_name(name.as_deref(), elements)?;
            require_charset("entity", &effective)?;
            for el in elements {
                require_nonempty("element", el)?;
            }
            Ok(())
        }
        StructuralChange::CreateEntityClass { name, dims } => {
            require_charset("class", name)?;
            if dims.len() > 4 {
                return Err(DbError::Api(format!(
                    "create_entity_class: {} dimensions exceeds the rank cap of 4",
                    dims.len()
                )));
            }
            for d in dims {
                require_nonempty("dimension class", d)?;
            }
            Ok(())
        }
        StructuralChange::CreateParameter {
            class, name, types, ..
        } => {
            require_nonempty("class", class)?;
            require_charset("parameter", name)?;
            validate_type_decls("create_parameter", class, name, types)
        }
    }
}

fn check_rename_coherence(
    changes: &[StructuralChange],
    pick: impl Fn(&StructuralChange) -> Option<(&String, &String)>,
) -> Result<(), DbError> {
    let mut seen: HashMap<String, String> = HashMap::new();
    for ch in changes {
        if let Some((current, new_name)) = pick(ch) {
            let key = fold(current);
            let val = fold(new_name);
            match seen.get(&key) {
                Some(prev) if *prev != val => {
                    return Err(DbError::Api(format!(
                        "structural: contradictory renames of {current:?} (to {prev:?} and \
                         {new_name:?}) in one commit"
                    )));
                }
                _ => {
                    seen.insert(key, val);
                }
            }
        }
    }
    Ok(())
}
