//! Element-tuple uniqueness of N-D entities (entity_rename_PLAN E2, §5.2,
//! §5.3): no two live entities of one class may hold the same element tuple.
//! The single writer enforces it (no index can: the tuple spans
//! `entity_element` rows); `audit_entity_tuple_dup` finds duplicates written
//! before the rule or by direct SQL.
//!
//! The per-call [`TupleIndex`] keeps the guard's cost constant per (call,
//! N-D class): the first N-D create of class K in a call resolves the
//! position-1 element names of every `CreateEntity` of K in the call (fenced
//! per name below [`TUPLE_SCOPE_MIN`] names, one set statement at or above
//! it) and loads, in one statement, the live tuples of K whose position-1
//! element is among those ids. Every create adds its own tuple. A create
//! whose position-1 element was not in the loaded scope (a name that
//! resolved to nothing at load time and to an existing entity later, after
//! a rename) loads that one element's tuples on demand. Elements created in
//! this call have new ids, so no pre-call entity can hold them.

use std::collections::{HashMap, HashSet};

use duckdb::types::Value;
use duckdb::{params, Connection};

use crate::api::{find_entity, lookup_entities, lookup_entity_class};
use crate::error::DbError;
use crate::types::{fold, StructuralChange};

/// Below this many position-1 element names, the scoped load resolves them
/// one fenced index lookup each; from it on, one set statement (a scan of
/// `entity`). P0 spike on 10^6 entities: a fenced lookup costs ~1 ms, the
/// set statement ~15–25 ms, so they cross near 20 names.
pub(crate) const TUPLE_SCOPE_MIN: usize = 20;

/// The live tuples of one class the call has seen.
#[derive(Default)]
struct ClassTuples {
    /// Position-1 element ids whose tuples are loaded.
    loaded: HashSet<i64>,
    /// Element ids (position order) -> holder entity ids.
    holders: HashMap<Vec<i64>, Vec<i64>>,
}

/// The per-call tuple index (a closure local of the write transaction,
/// rebuilt on a conflict retry). `scope` holds the `(class, position-1
/// element)` names of every N-D create of the call, for the scoped load.
#[derive(Default)]
pub(crate) struct TupleIndex<'a> {
    scope: Vec<(&'a str, &'a str)>,
    classes: HashMap<i64, ClassTuples>,
    /// Indexed entity id -> its current name (renames of this call update
    /// it, so messages and the derived-name check see the name now).
    names: HashMap<i64, String>,
}

/// One holder of a tuple: id and current name.
pub(crate) struct Holder {
    pub(crate) id: i64,
    pub(crate) name: String,
}

impl<'a> TupleIndex<'a> {
    /// An index scoped to `(class, position-1 element)` names.
    pub(crate) fn new(scope: Vec<(&'a str, &'a str)>) -> Self {
        TupleIndex {
            scope,
            classes: HashMap::new(),
            names: HashMap::new(),
        }
    }

    /// An index scoped to the `CreateEntity` items of `changes`.
    pub(crate) fn for_changes(changes: &'a [StructuralChange]) -> Self {
        Self::new(
            changes
                .iter()
                .filter_map(|ch| match ch {
                    StructuralChange::CreateEntity {
                        class, elements, ..
                    } => elements.first().map(|e| (class.as_str(), e.as_str())),
                    _ => None,
                })
                .collect(),
        )
    }

    /// The live entities of class `cid` holding exactly `elements` (ids in
    /// position order), loading the class's scope on first use.
    /// `dims` is the class's dimension typing (`(position, class, set)`).
    pub(crate) fn holders(
        &mut self,
        conn: &Connection,
        cid: i64,
        dims: &[(i64, Option<i64>, Option<i64>)],
        elements: &[i64],
    ) -> Result<Vec<Holder>, DbError> {
        let Some(&first) = elements.first() else {
            return Ok(Vec::new());
        };
        if !self.classes.contains_key(&cid) {
            let mut ids = self.scope_ids(conn, cid, dims)?;
            ids.insert(first);
            self.load(conn, cid, &ids)?;
        } else if !self.classes[&cid].loaded.contains(&first) {
            self.load(conn, cid, &HashSet::from([first]))?;
        }
        let ct = &self.classes[&cid];
        Ok(ct
            .holders
            .get(elements)
            .map(|ids| {
                ids.iter()
                    .map(|id| Holder {
                        id: *id,
                        name: self.names[id].clone(),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Record a tuple this call created (after [`TupleIndex::holders`] for
    /// the same class, so the class is loaded).
    pub(crate) fn add(&mut self, cid: i64, eid: i64, name: &str, elements: Vec<i64>) {
        let ct = self.classes.entry(cid).or_default();
        let ids = ct.holders.entry(elements).or_default();
        if !ids.contains(&eid) {
            ids.push(eid);
        }
        self.names.insert(eid, name.to_string());
    }

    /// Follow a rename of this call: an indexed entity's name changes.
    pub(crate) fn renamed(&mut self, eid: i64, new_name: &str) {
        if let Some(n) = self.names.get_mut(&eid) {
            *n = new_name.to_string();
        }
    }

    /// The ids of the position-1 element names of the scope's creates of
    /// class `cid` (resolved against dimension 1's class, or
    /// every member class of its class set). Names that resolve to nothing
    /// are dropped (an element created later in the call has a new id).
    fn scope_ids(
        &self,
        conn: &Connection,
        cid: i64,
        dims: &[(i64, Option<i64>, Option<i64>)],
    ) -> Result<HashSet<i64>, DbError> {
        let mut out = HashSet::new();
        let Some(&(_, dc, ds)) = dims.first() else {
            return Ok(out);
        };
        // Class names as written -> id, resolved once per distinct fold.
        let mut class_of: HashMap<String, Option<i64>> = HashMap::new();
        let mut names: Vec<&str> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for &(class, first) in &self.scope {
            let k = fold(class);
            let id = match class_of.get(&k) {
                Some(id) => *id,
                None => {
                    let id = lookup_entity_class(conn, class)?;
                    class_of.insert(k, id);
                    id
                }
            };
            if id == Some(cid) && seen.insert(fold(first)) {
                names.push(first);
            }
        }
        if names.is_empty() {
            return Ok(out);
        }
        let member_classes: Vec<i64> = match (dc, ds) {
            (Some(c), _) => vec![c],
            (None, Some(set)) => {
                test_hooks::count();
                let mut stmt = conn.prepare_cached(
                    "SELECT member_class_id FROM class_set_member \
                     WHERE class_set_id = ? AND valid_to_commit IS NULL",
                )?;
                let rows = stmt
                    .query_map(params![set], |r| r.get::<_, i64>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            }
            (None, None) => Vec::new(),
        };
        for mc in member_classes {
            if names.len() < TUPLE_SCOPE_MIN {
                for n in &names {
                    test_hooks::count();
                    if let Some(f) = find_entity(conn, mc, n)? {
                        out.insert(f.id);
                    }
                }
            } else {
                test_hooks::count();
                for (_, f) in lookup_entities(conn, mc, &names)? {
                    if let Some(f) = f {
                        out.insert(f.id);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Load the live tuples of class `cid` whose position-1 element is in
    /// `firsts`: one statement.
    fn load(&mut self, conn: &Connection, cid: i64, firsts: &HashSet<i64>) -> Result<(), DbError> {
        let ct = self.classes.entry(cid).or_default();
        let want: Vec<i64> = firsts
            .iter()
            .copied()
            .filter(|id| !ct.loaded.contains(id))
            .collect();
        ct.loaded.extend(want.iter().copied());
        if want.is_empty() {
            return Ok(());
        }
        test_hooks::count();
        let list = Value::List(want.into_iter().map(Value::BigInt).collect());
        let mut stmt = conn.prepare_cached(TUPLE_LOAD_SQL)?;
        let rows = stmt.query_map(params![list, cid], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Value>(2)?,
            ))
        })?;
        for row in rows {
            let (eid, name, ids) = row?;
            let Value::List(ids) = ids else {
                return Err(DbError::Internal(format!(
                    "tuple load: element ids of entity {eid} read as {ids:?}"
                )));
            };
            let ids: Vec<i64> = ids
                .into_iter()
                .map(|v| match v {
                    Value::BigInt(i) => Ok(i),
                    other => Err(DbError::Internal(format!(
                        "tuple load: element id read as {other:?}"
                    ))),
                })
                .collect::<Result<_, _>>()?;
            let h = ct.holders.entry(ids).or_default();
            if !h.contains(&eid) {
                h.push(eid);
            }
            self.names.insert(eid, name);
            test_hooks::loaded(1);
        }
        Ok(())
    }
}

/// The scoped tuple load (`?1` position-1 element ids, `?2` class id): every
/// live entity of the class whose position-1 element is listed, with its
/// name and its element ids in position order.
const TUPLE_LOAD_SQL: &str = "SELECT e.id, e.name, t.ids FROM \
       (SELECT el.entity_id, list(el.element_entity_id ORDER BY el.position) AS ids \
        FROM entity_element el \
        WHERE el.valid_to_commit IS NULL AND el.entity_id IN \
          (SELECT entity_id FROM entity_element \
           WHERE position = 1 AND valid_to_commit IS NULL \
             AND element_entity_id IN (SELECT unnest(?1::BIGINT[]))) \
        GROUP BY el.entity_id) t \
     JOIN entity e ON e.id = t.entity_id AND e.entity_class_id = ?2 \
      AND e.valid_to_commit IS NULL";

/// The current element names (position order) of entity `eid`, for messages.
pub(crate) fn element_names(conn: &Connection, eid: i64) -> Result<Vec<String>, DbError> {
    let mut stmt = conn.prepare_cached(
        "SELECT e.name FROM entity_element el \
         JOIN entity e ON e.cur_id = el.element_entity_id \
         WHERE el.cur_e = ? ORDER BY el.cur_p",
    )?;
    let rows = stmt
        .query_map(params![eid], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// The statement counter of the tuple guard (tests: the guard adds a
/// constant number of statements per call and N-D class).
#[doc(hidden)]
pub mod test_hooks {
    #[cfg(debug_assertions)]
    thread_local! {
        static STATEMENTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        static LOADED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    pub(crate) fn count() {
        #[cfg(debug_assertions)]
        STATEMENTS.with(|c| c.set(c.get() + 1));
    }

    pub(crate) fn loaded(_n: u64) {
        #[cfg(debug_assertions)]
        LOADED.with(|c| c.set(c.get() + _n));
    }

    /// Tuples the scoped loads read on this thread so far.
    #[cfg(debug_assertions)]
    pub fn tuples_loaded() -> u64 {
        LOADED.with(|c| c.get())
    }

    /// Statements the tuple guard ran on this thread so far.
    #[cfg(debug_assertions)]
    pub fn statements() -> u64 {
        STATEMENTS.with(|c| c.get())
    }
}
