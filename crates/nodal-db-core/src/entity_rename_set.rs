//! The set path of `RenameEntity`: a
//! run of at least [`RENAME_SET_MIN`] consecutive `RenameEntity` items of a
//! call is applied as one batch, with a constant number of statements per
//! run (per distinct class: its lookup and its class-set co-members).
//!
//! 1. One statement reads, for every `(class, name)` the run can look up
//!    (each item's `current` and `new_name`, and `new_name` in every
//!    co-member class of a class set), the current entity holding that
//!    name: a snapshot of the database before the run.
//! 2. The items are simulated in order in Rust against that snapshot plus
//!    an overlay of what the earlier items of the run changed, with exactly
//!    the per-item path's guards, decisions and messages (chains, the swap
//!    guard, uniqueness, the class-set check, `decide_revision`); the call's
//!    rename book-keeping, name folds, touches and the tuple index follow
//!    each item as on the per-item path.
//! 3. One set close and one `INSERT … SELECT` write the successors of the
//!    rows older than this commit; one `UPDATE … FROM` edits rows of this
//!    commit in place. The tables equal the per-item path's.
//!
//! Measured (10^6 entities): 10^4 renames per item 90–140 s, as a set
//! 2.7–3.6 s; the two cross near 20 items.

use std::collections::{HashMap, HashSet};

use duckdb::types::Value;
use duckdb::{params, Connection};

use crate::api::find_entity_class;
use crate::entity_tuple::TupleIndex;
use crate::error::DbError;
use crate::structural::{
    class_set_co_members, decide_revision, note_renamed_away, refuse_freed_in_class_sets,
    refuse_freed_name, CallRenames, CallTouches,
};
use crate::text::{stored_description, NameFolds};
use crate::types::{
    fold, StructuralChange, StructuralEffect, StructuralKind, StructuralOutcome, WriteReport,
};

/// From this many consecutive `RenameEntity` items on, a run takes the set
/// path (P0 spike crossover, ~20 items on a 10^6-entity file).
pub const RENAME_SET_MIN: usize = 20;

/// The number of consecutive `RenameEntity` items from the start of
/// `changes`.
pub(crate) fn rename_run_len(changes: &[StructuralChange]) -> usize {
    changes
        .iter()
        .take_while(|c| matches!(c, StructuralChange::RenameEntity { .. }))
        .count()
}

/// Should the run at the start of `changes` take the set path?
pub(crate) fn takes_set_path(changes: &[StructuralChange]) -> bool {
    test_hooks::set_path_enabled() && rename_run_len(changes) >= RENAME_SET_MIN
}

/// One current entity row, as the run sees it.
#[derive(Clone, Debug)]
struct Row {
    cid: i64,
    name: String,
    description: Option<String>,
    version: i64,
    valid_from: i64,
}

/// The snapshot plus what the run changed so far.
struct View {
    /// `(class id, fold(name))` -> holder id, in the database before the run.
    snapshot: HashMap<(i64, String), i64>,
    /// Rows by id: the snapshot's, then the run's edits.
    rows: HashMap<i64, Row>,
    /// Ids the run revised, in first-revision order, and as a set.
    changed: Vec<i64>,
    changed_set: HashSet<i64>,
    /// `(class id, fold(name))` whose holder the run changed: `Some(id)`
    /// taken, `None` freed.
    overlay: HashMap<(i64, String), Option<i64>>,
}

impl View {
    fn resolve(&self, cid: i64, name: &str) -> Option<i64> {
        let k = (cid, fold(name));
        match self.overlay.get(&k) {
            Some(v) => *v,
            None => self.snapshot.get(&k).copied(),
        }
    }
}

/// Apply the run of `RenameEntity` items `run` (the call's items
/// `first..first + run.len()`) as one batch.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_rename_run(
    conn: &Connection,
    c: i64,
    first: usize,
    run: &[StructuralChange],
    report: &mut WriteReport,
    folds: &mut NameFolds,
    calls: &mut CallRenames,
    touches: &mut CallTouches,
    tuples: &mut TupleIndex<'_>,
) -> Result<(), DbError> {
    let items: Vec<(&str, &str, &str, Option<&str>)> = run
        .iter()
        .map(|ch| match ch {
            StructuralChange::RenameEntity {
                class,
                current,
                new_name,
                description,
            } => (
                class.as_str(),
                current.as_str(),
                new_name.as_str(),
                description.as_deref(),
            ),
            _ => unreachable!("a rename run holds RenameEntity items only"),
        })
        .collect();

    // Classes, once per distinct fold; their class-set co-members.
    let mut classes: HashMap<String, Option<(i64, String)>> = HashMap::new();
    let mut co_members: HashMap<i64, Vec<(String, i64, String)>> = HashMap::new();
    for (class, ..) in &items {
        let k = fold(class);
        if classes.contains_key(&k) {
            continue;
        }
        test_hooks::count();
        let found = find_entity_class(conn, class)?.map(|f| (f.id, f.name));
        if let Some((cid, _)) = &found {
            test_hooks::count();
            co_members.insert(*cid, class_set_co_members(conn, *cid)?);
        }
        classes.insert(k, found);
    }

    // The snapshot: every (class, name) the run may look up.
    let mut keys: Vec<(i64, &str)> = Vec::new();
    for (class, current, new_name, _) in &items {
        let Some(Some((cid, _))) = classes.get(&fold(class)) else {
            continue;
        };
        keys.push((*cid, current));
        keys.push((*cid, new_name));
        for (_, member, _) in &co_members[cid] {
            keys.push((*member, new_name));
        }
    }
    let mut view = View {
        snapshot: HashMap::new(),
        rows: HashMap::new(),
        changed: Vec::new(),
        changed_set: HashSet::new(),
        overlay: HashMap::new(),
    };
    if !keys.is_empty() {
        test_hooks::count();
        let cids = Value::List(keys.iter().map(|(c, _)| Value::BigInt(*c)).collect());
        let names = Value::List(
            keys.iter()
                .map(|(_, n)| Value::Text(n.to_string()))
                .collect(),
        );
        let mut stmt = conn.prepare_cached(SNAPSHOT_SQL)?;
        let rows = stmt.query_map(params![cids, names], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                Row {
                    cid: r.get(0)?,
                    name: r.get(3)?,
                    description: r.get(4)?,
                    version: r.get(5)?,
                    valid_from: r.get(6)?,
                },
            ))
        })?;
        for row in rows {
            let (cid, key, id, row) = row?;
            view.snapshot.insert((cid, key), id);
            view.rows.entry(id).or_insert(row);
        }
    }

    // The items, in order, with the per-item path's rules and messages.
    for (k, (class, current, new_name, description)) in items.iter().enumerate() {
        let Some((cid, stored_class)) = classes[&fold(class)].clone() else {
            return Err(DbError::Api(format!(
                "rename_entity: class {class:?} not found"
            )));
        };
        folds.note("entity_class", class, &stored_class, cid);
        let Some(id) = view.resolve(cid, current) else {
            let mut m = format!("rename_entity: entity {class}:{current} not found");
            if let Some((_, to)) = calls.entities.get(&(cid, fold(current))) {
                m = format!(
                    "{m} (entity {current:?} was renamed to {to:?} earlier in this call; \
                     address it by its new name)"
                );
            }
            return Err(DbError::Api(m));
        };
        let row = view.rows[&id].clone();
        folds.note("entity", current, &row.name, id);
        // check_entity_rename, against the view.
        refuse_freed_name(
            "entity",
            current,
            new_name,
            &row.name,
            calls.entities.get(&(cid, fold(new_name))),
        )?;
        if fold(new_name) != fold(&row.name) {
            refuse_freed_in_class_sets(class, current, new_name, &co_members[&cid], calls)?;
            if let Some(other) = view.resolve(cid, new_name) {
                if other != id {
                    return Err(DbError::Api(format!(
                        "rename_entity: {new_name:?} already names a different entity of class \
                         {class:?}"
                    )));
                }
            }
            for (set, member, member_name) in &co_members[&cid] {
                if let Some(hit) = view.resolve(*member, new_name) {
                    return Err(DbError::Api(format!(
                        "rename_entity: {class}:{current} -> {new_name:?}: the name would be \
                         ambiguous across class set {set:?}, where {member_name}:{} exists (an \
                         element of a set-typed dimension is looked up by name in every member \
                         class)",
                        view.rows[&hit].name
                    )));
                }
            }
        }
        let effect = match decide_revision(
            &row.name,
            row.description.as_deref(),
            new_name,
            *description,
        ) {
            None => StructuralEffect::NoOp,
            Some(rev) => {
                let old = row.name.clone();
                let r = view.rows.get_mut(&id).expect("a resolved row");
                r.name = new_name.to_string();
                r.description = rev.description;
                if view.changed_set.insert(id) {
                    view.changed.push(id);
                }
                if fold(&old) != fold(new_name) {
                    view.overlay.insert((cid, fold(&old)), None);
                }
                view.overlay.insert((cid, fold(new_name)), Some(id));
                if rev.effect == StructuralEffect::Renamed {
                    note_renamed_away(&mut calls.entities, (cid, fold(&old)), id, &old, new_name);
                    tuples.renamed(id, new_name);
                }
                rev.effect
            }
        };
        touches.note(conn, &run[k], id)?;
        report.structural.push(StructuralOutcome {
            change_index: first + k,
            kind: StructuralKind::Entity,
            object_id: id,
            effect,
        });
    }

    write_run(conn, c, &view)
}

/// The run's writes: one set close and one insert for rows older than this
/// commit, one in-place update for rows of this commit.
fn write_run(conn: &Connection, c: i64, view: &View) -> Result<(), DbError> {
    let (older, ours): (Vec<i64>, Vec<i64>) = view
        .changed
        .iter()
        .partition(|id| view.rows[id].valid_from != c);
    let list = |ids: &[i64], f: &dyn Fn(&Row, i64) -> Value| {
        Value::List(ids.iter().map(|id| f(&view.rows[id], *id)).collect())
    };
    let text = |s: Option<&str>| match stored_description(s) {
        Some(d) => Value::Text(d.to_string()),
        None => Value::Null,
    };
    if !older.is_empty() {
        test_hooks::count();
        let n = conn.execute(
            "UPDATE entity SET valid_to_commit = ?1 \
             WHERE id IN (SELECT unnest(?2::BIGINT[])) \
               AND valid_from_commit < ?1 AND valid_to_commit IS NULL",
            params![c, list(&older, &|_, id| Value::BigInt(id))],
        )?;
        if n != older.len() {
            return Err(DbError::Internal(format!(
                "entity rename run: expected {} current rows to close, closed {n}",
                older.len()
            )));
        }
        test_hooks::count();
        conn.execute(
            "INSERT INTO entity \
               (id, entity_class_id, name, description, valid_from_commit, version) \
             SELECT unnest(?1::BIGINT[]), unnest(?2::BIGINT[]), unnest(?3::VARCHAR[]), \
                    unnest(?4::VARCHAR[]), ?5, unnest(?6::INTEGER[])",
            params![
                list(&older, &|_, id| Value::BigInt(id)),
                list(&older, &|r, _| Value::BigInt(r.cid)),
                list(&older, &|r, _| Value::Text(r.name.clone())),
                list(&older, &|r, _| text(r.description.as_deref())),
                c,
                list(&older, &|r, _| Value::BigInt(r.version + 1)),
            ],
        )?;
    }
    if !ours.is_empty() {
        test_hooks::count();
        let n = conn.execute(
            "UPDATE entity SET name = k.n, description = k.d \
             FROM (SELECT unnest(?1::BIGINT[]) AS id, unnest(?2::VARCHAR[]) AS n, \
                          unnest(?3::VARCHAR[]) AS d) k \
             WHERE entity.id = k.id AND entity.valid_from_commit = ?4 \
               AND entity.valid_to_commit IS NULL",
            params![
                list(&ours, &|_, id| Value::BigInt(id)),
                list(&ours, &|r, _| Value::Text(r.name.clone())),
                list(&ours, &|r, _| text(r.description.as_deref())),
                c,
            ],
        )?;
        if n != ours.len() {
            return Err(DbError::Internal(format!(
                "entity rename run: expected {} rows of this commit to edit, edited {n}",
                ours.len()
            )));
        }
    }
    Ok(())
}

/// The run's snapshot (`?1` class ids, `?2` names, zipped): per key that a
/// current entity holds, the class id, the name's DuckDB `lower()` fold
/// (equal to `types::fold`, engine rule 8), the holder's id and row. One
/// scan of `entity`.
const SNAPSHOT_SQL: &str = "WITH z AS (SELECT unnest(?1::BIGINT[]) AS cid, \
                                   unnest(?2::VARCHAR[]) AS n), \
          k AS (SELECT DISTINCT cid, lower(n) AS n FROM z) \
     SELECT k.cid, k.n, e.id, e.name, e.description, e.version, e.valid_from_commit \
     FROM k JOIN entity e ON e.entity_class_id = k.cid AND e.valid_to_commit IS NULL \
      AND lower(e.name) = k.n";

/// Test hooks: the run path's statement count, and a switch to the per-item
/// path (the reference arm of the equivalence test).
#[doc(hidden)]
pub mod test_hooks {
    #[cfg(debug_assertions)]
    thread_local! {
        static STATEMENTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
    }

    pub(crate) fn count() {
        #[cfg(debug_assertions)]
        STATEMENTS.with(|c| c.set(c.get() + 1));
    }

    /// Statements the set path ran on this thread so far.
    #[cfg(debug_assertions)]
    pub fn statements() -> u64 {
        STATEMENTS.with(|c| c.get())
    }

    /// Run `f` with the set path off (every rename per item).
    #[cfg(debug_assertions)]
    pub fn without_set_path<R>(f: impl FnOnce() -> R) -> R {
        ENABLED.with(|c| c.set(false));
        let out = f();
        ENABLED.with(|c| c.set(true));
        out
    }

    pub(crate) fn set_path_enabled() -> bool {
        #[cfg(debug_assertions)]
        {
            ENABLED.with(|c| c.get())
        }
        #[cfg(not(debug_assertions))]
        {
            true
        }
    }
}
