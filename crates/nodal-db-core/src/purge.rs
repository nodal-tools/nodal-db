//! Purge: a bulk, versioned close-out of every item in a named scope, in
//! one commit (entity_descriptions_purge_PLAN §3). It never erases history
//! (as-of reads see everything, no erasure marker) and never shrinks the
//! file.
//!
//! ## Execution: set-based, one dependency definition (§3.6, invariant 2)
//! A purge runs the object delete's leg registry
//! ([`crate::object_delete::object_legs`] and its cascade edges) in **set
//! form**: every leg's [`crate::object_delete::LegKey`] is rendered against a
//! txn-local id table instead of one id, so the close-out of a whole scope
//! is O(legs + depth) statements, independent of the number of objects. The
//! per-id and set forms come from one template, and share one SET list and
//! one same-commit guard (`valid_from_commit < c AND valid_to_commit IS
//! NULL`): a row this call wrote is never closed.
//!
//! ## Two sets per kind (§3.6, V-B1)
//! For each object kind a request reaches, two txn-local sets:
//!   - the **dependent set**: every live object in the request's scope,
//!     spared ones included, expanded by the cascade edges (B1 containers of
//!     entities, a class's children and its B2 dependant classes) by an
//!     iterative frontier query;
//!   - the **close set**: the dependent set minus the call's survivors.
//!
//! The close set drives object rows and identity legs; the dependent set
//! drives dependent legs, each with its own survivor anti-join where its
//! table has a survivor grain ([`crate::object_delete::Grain`]). Survivors
//! are what the call itself re-states, spared upward to every object a
//! survivor's identity references (an entity's class and elements,
//! transitively; a parameter's class and bound list; a class's dimension
//! classes). The frontier of the close set must never reach a survivor:
//! that is a core bug, reported as [`DbError::Internal`].
//!
//! Every set is a `TEMP` table of the write connection, created inside the
//! write transaction (so a conflict retry rebuilds it) and dropped when the
//! purge phase ends; the connection is per-operation anyway (CLAUDE.md
//! invariant 1). The P0 spike (`scripts/bench/RESULTS_purge_spike.md`) chose
//! the temp-table form over bound `BIGINT[]` lists: the sets are computed in
//! SQL from the scope, so no id list ever passes through Rust.

#![allow(dead_code)] // P1: the set machinery; the purge phase wires it (P2).

use duckdb::Connection;

use crate::error::DbError;
use crate::object_delete::{cascade_edges, IdSet};
use crate::types::ObjectKind;

// ---------------------------------------------------------------------------
// Statement helpers (counted, for the statement-count scale guard)
// ---------------------------------------------------------------------------

/// `conn.execute`, counted by [`test_hooks::statements`].
pub(crate) fn exec<P: duckdb::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> Result<usize, duckdb::Error> {
    test_hooks::count();
    conn.execute(sql, params)
}

/// A batch of statements, counted as one each.
pub(crate) fn exec_batch(conn: &Connection, sql: &str) -> Result<(), duckdb::Error> {
    test_hooks::count();
    conn.execute_batch(sql)
}

/// A `SELECT count(*) …` (no binds), counted.
pub(crate) fn query_count(conn: &Connection, sql: &str) -> Result<i64, DbError> {
    test_hooks::count();
    Ok(conn.query_row(sql, [], |r| r.get(0))?)
}

// ---------------------------------------------------------------------------
// The object kinds a purge holds id sets of
// ---------------------------------------------------------------------------

/// An object kind with purge id sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SetKind {
    Entity,
    Class,
    Parameter,
    Alternative,
    Scenario,
    ValueList,
}

impl SetKind {
    pub(crate) const ALL: [SetKind; 6] = [
        SetKind::Entity,
        SetKind::Class,
        SetKind::Parameter,
        SetKind::Alternative,
        SetKind::Scenario,
        SetKind::ValueList,
    ];

    pub(crate) fn tag(self) -> &'static str {
        match self {
            SetKind::Entity => "entity",
            SetKind::Class => "class",
            SetKind::Parameter => "parameter",
            SetKind::Alternative => "alternative",
            SetKind::Scenario => "scenario",
            SetKind::ValueList => "value_list",
        }
    }

    /// The table whose `id` rows are the objects.
    pub(crate) fn object_table(self) -> &'static str {
        match self {
            SetKind::Entity => "entity",
            SetKind::Class => "entity_class",
            SetKind::Parameter => "parameter_definition",
            SetKind::Alternative => "alternative",
            SetKind::Scenario => "scenario",
            SetKind::ValueList => "parameter_value_list",
        }
    }

    /// The object delete's kind (its leg registry), if it has one.
    pub(crate) fn object_kind(self) -> Option<ObjectKind> {
        match self {
            SetKind::Entity => Some(ObjectKind::Entity),
            SetKind::Class => Some(ObjectKind::Class),
            SetKind::Parameter => Some(ObjectKind::Parameter),
            SetKind::Alternative => Some(ObjectKind::Alternative),
            SetKind::Scenario => Some(ObjectKind::Scenario),
            SetKind::ValueList => None,
        }
    }

    pub(crate) fn of(kind: ObjectKind) -> SetKind {
        match kind {
            ObjectKind::Entity => SetKind::Entity,
            ObjectKind::Class => SetKind::Class,
            ObjectKind::Parameter => SetKind::Parameter,
            ObjectKind::Alternative => SetKind::Alternative,
            ObjectKind::Scenario => SetKind::Scenario,
        }
    }
}

/// Request `r`'s dependent set of kind `k`.
pub(crate) fn dep_table(r: usize, k: SetKind) -> String {
    format!("pg{r}_dep_{}", k.tag())
}

/// Request `r`'s close set of kind `k`.
pub(crate) fn close_table(r: usize, k: SetKind) -> String {
    format!("pg{r}_close_{}", k.tag())
}

/// The call's survivors of kind `k` (spared upward).
pub(crate) fn surv_table(k: SetKind) -> String {
    format!("pg_surv_{}", k.tag())
}

/// The call's survivor value keys `(e, p, a)`.
pub(crate) const SURV_VALUE: &str = "pg_surv_value";
/// The call's survivor activations `(e, a)`.
pub(crate) const SURV_ACTIVATION: &str = "pg_surv_activation";
/// The call's survivor memberships `(g, m, a)`, `a = 0` for a base row.
pub(crate) const SURV_MEMBERSHIP: &str = "pg_surv_membership";
/// The scenarios whose ranking the call sets (`id`).
pub(crate) const SURV_RANKING: &str = "pg_surv_ranking";

/// Create (or empty) a one-column `id` temp table.
pub(crate) fn create_id_table(conn: &Connection, name: &str) -> Result<(), DbError> {
    exec_batch(
        conn,
        &format!("CREATE OR REPLACE TEMP TABLE {name} (id BIGINT)"),
    )?;
    Ok(())
}

/// Append `ids` to an `id` table (one statement, one bind).
pub(crate) fn insert_ids(conn: &Connection, name: &str, ids: &[i64]) -> Result<(), DbError> {
    if ids.is_empty() {
        return Ok(());
    }
    let list = duckdb::types::Value::List(ids.iter().map(|&i| i.into()).collect());
    exec(
        conn,
        &format!("INSERT INTO {name} SELECT unnest(?::BIGINT[])"),
        [list],
    )?;
    Ok(())
}

/// The live (ANY version current) rows of an `id` table's ids in `table`,
/// counted (an object set's size, closed or not by this call).
pub(crate) fn count_ids(conn: &Connection, set: &str) -> Result<usize, DbError> {
    Ok(query_count(conn, &format!("SELECT count(DISTINCT id) FROM {set}"))? as usize)
}

// ---------------------------------------------------------------------------
// The two sets (§3.6, V-B1)
// ---------------------------------------------------------------------------

/// Create the survivor tables, empty.
pub(crate) fn create_survivor_tables(conn: &Connection) -> Result<(), DbError> {
    for k in SetKind::ALL {
        create_id_table(conn, &surv_table(k))?;
    }
    create_id_table(conn, SURV_RANKING)?;
    exec_batch(
        conn,
        &format!(
            "CREATE OR REPLACE TEMP TABLE {SURV_VALUE} (e BIGINT, p BIGINT, a BIGINT); \
             CREATE OR REPLACE TEMP TABLE {SURV_ACTIVATION} (e BIGINT, a BIGINT); \
             CREATE OR REPLACE TEMP TABLE {SURV_MEMBERSHIP} (g BIGINT, m BIGINT, a BIGINT)"
        ),
    )?;
    Ok(())
}

/// Spare upward (§3.5.3): every object a survivor's identity references
/// survives too. Value keys, activations and memberships first lend their
/// entities, parameters and alternatives; then an entity's elements
/// (transitively, N-D chains), the classes of surviving entities and
/// parameters, a class's dimension classes (transitively) and the value list
/// a surviving definition actually binds (V-S6). Iterative SQL, no id
/// passes through Rust.
pub(crate) fn spare_upward(conn: &Connection) -> Result<(), DbError> {
    if test_hooks::upward_disabled() {
        return Ok(());
    }
    let ent = surv_table(SetKind::Entity);
    let class = surv_table(SetKind::Class);
    let param = surv_table(SetKind::Parameter);
    let alt = surv_table(SetKind::Alternative);
    let scen = surv_table(SetKind::Scenario);
    let list = surv_table(SetKind::ValueList);
    exec_batch(
        conn,
        &format!(
            "INSERT INTO {ent} SELECT e FROM {SURV_VALUE}; \
             INSERT INTO {param} SELECT p FROM {SURV_VALUE}; \
             INSERT INTO {alt} SELECT a FROM {SURV_VALUE}; \
             INSERT INTO {ent} SELECT e FROM {SURV_ACTIVATION}; \
             INSERT INTO {alt} SELECT a FROM {SURV_ACTIVATION}; \
             INSERT INTO {ent} SELECT g FROM {SURV_MEMBERSHIP}; \
             INSERT INTO {ent} SELECT m FROM {SURV_MEMBERSHIP}; \
             INSERT INTO {alt} SELECT a FROM {SURV_MEMBERSHIP} WHERE a <> 0; \
             INSERT INTO {scen} SELECT id FROM {SURV_RANKING}"
        ),
    )?;
    // An N-D entity's elements, transitively.
    fixpoint(
        conn,
        &format!(
            "INSERT INTO {ent} SELECT DISTINCT ee.element_entity_id FROM entity_element ee \
             WHERE ee.valid_to_commit IS NULL AND ee.entity_id IN (SELECT id FROM {ent}) \
               AND ee.element_entity_id NOT IN (SELECT id FROM {ent})"
        ),
    )?;
    exec_batch(
        conn,
        &format!(
            "INSERT INTO {class} SELECT DISTINCT entity_class_id FROM entity \
             WHERE valid_to_commit IS NULL AND id IN (SELECT id FROM {ent}); \
             INSERT INTO {class} SELECT DISTINCT entity_class_id FROM parameter_definition \
             WHERE valid_to_commit IS NULL AND id IN (SELECT id FROM {param}); \
             INSERT INTO {list} SELECT DISTINCT parameter_value_list_id FROM parameter_definition \
             WHERE valid_to_commit IS NULL AND parameter_value_list_id IS NOT NULL \
               AND id IN (SELECT id FROM {param})"
        ),
    )?;
    // A class's dimension classes, transitively.
    fixpoint(
        conn,
        &format!(
            "INSERT INTO {class} SELECT DISTINCT dimension_class_id FROM entity_class_dimension \
             WHERE valid_to_commit IS NULL AND dimension_class_id IS NOT NULL \
               AND entity_class_id IN (SELECT id FROM {class}) \
               AND dimension_class_id NOT IN (SELECT id FROM {class})"
        ),
    )?;
    Ok(())
}

/// Run an `INSERT … WHERE … NOT IN (target)` until it inserts nothing.
/// Returns the number of rounds that inserted rows.
fn fixpoint(conn: &Connection, insert: &str) -> Result<usize, DbError> {
    let mut rounds = 0;
    while exec(conn, insert, [])? > 0 {
        rounds += 1;
    }
    Ok(rounds)
}

/// Build request `r`'s dependent sets: `base` gives, per kind, a `SELECT`
/// of the scope's object ids (live rows only); then, when `expand`, the
/// cascade edges grow them by an iterative frontier until nothing is added
/// (a class adds its entities, parameters and B2 dependant classes; an
/// entity its B1 containers). Every kind's table exists afterwards (empty
/// when unreached).
pub(crate) fn build_dep_sets(
    conn: &Connection,
    r: usize,
    base: &[(SetKind, String)],
    expand: bool,
) -> Result<(), DbError> {
    for k in SetKind::ALL {
        create_id_table(conn, &dep_table(r, k))?;
    }
    for (k, select) in base {
        exec(
            conn,
            &format!("INSERT INTO {} {select}", dep_table(r, *k)),
            [],
        )?;
    }
    if !expand {
        return Ok(());
    }
    loop {
        let mut added = 0;
        for src in [SetKind::Class, SetKind::Entity] {
            let kind = src.object_kind().expect("class and entity have leg sets");
            for edge in cascade_edges(kind) {
                let dst = dep_table(r, SetKind::of(edge.neighbor_kind));
                let from = dep_table(r, src);
                added += exec(
                    conn,
                    &format!(
                        "INSERT INTO {dst} SELECT DISTINCT x.id FROM ({}) x(id) \
                         WHERE x.id NOT IN (SELECT id FROM {dst})",
                        edge.select_ids(IdSet::Table(&from))
                    ),
                    [],
                )?;
            }
        }
        if added == 0 {
            return Ok(());
        }
    }
}

/// Request `r`'s close sets: its dependent sets minus the call's survivors.
pub(crate) fn build_close_sets(conn: &Connection, r: usize) -> Result<(), DbError> {
    for k in SetKind::ALL {
        exec_batch(
            conn,
            &format!(
                "CREATE OR REPLACE TEMP TABLE {} AS SELECT DISTINCT id FROM {} \
                 WHERE id NOT IN (SELECT id FROM {})",
                close_table(r, k),
                dep_table(r, k),
                surv_table(k)
            ),
        )?;
    }
    Ok(())
}

/// The frontier hard check (§3.5.3): no cascade edge out of request `r`'s
/// close set may reach a survivor. Upward sparing makes that impossible (a
/// survivor's elements, class and dimension classes survive), so a hit is a
/// core bug: [`DbError::Internal`], and the call rolls back.
pub(crate) fn check_frontier(conn: &Connection, r: usize) -> Result<(), DbError> {
    for src in [SetKind::Class, SetKind::Entity] {
        let kind = src.object_kind().expect("class and entity have leg sets");
        for edge in cascade_edges(kind) {
            let dst = SetKind::of(edge.neighbor_kind);
            let n = query_count(
                conn,
                &format!(
                    "SELECT count(*) FROM ({}) x(id) WHERE x.id IN (SELECT id FROM {})",
                    edge.select_ids(IdSet::Table(&close_table(r, src))),
                    surv_table(dst)
                ),
            )?;
            if n > 0 {
                return Err(DbError::Internal(format!(
                    "purge request {r}: the {} -> {} frontier ({}.{}) of the close set \
                     reaches {n} surviving object(s)",
                    src.tag(),
                    dst.tag(),
                    edge.table,
                    edge.column
                )));
            }
        }
    }
    Ok(())
}

/// Drop every purge temp table of the connection (end of the purge phase).
pub(crate) fn drop_tables(conn: &Connection) -> Result<(), DbError> {
    let mut stmt = conn.prepare(
        "SELECT table_name FROM duckdb_tables() \
         WHERE temporary AND regexp_matches(table_name, '^pg[0-9]*_')",
    )?;
    let names: Vec<String> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    for n in names {
        exec_batch(conn, &format!("DROP TABLE IF EXISTS temp.main.{n}"))?;
    }
    Ok(())
}

/// Test-only switches (debug builds). Thread-local, like
/// [`crate::api::test_hooks`].
#[doc(hidden)]
pub mod test_hooks {
    #[cfg(debug_assertions)]
    thread_local! {
        static STATEMENTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        static NO_UPWARD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        static NO_SURVIVORS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    pub(crate) fn count() {
        #[cfg(debug_assertions)]
        STATEMENTS.with(|c| c.set(c.get() + 1));
    }

    /// The purge statements run on this thread so far.
    #[cfg(debug_assertions)]
    pub fn statements() -> u64 {
        STATEMENTS.with(|c| c.get())
    }

    /// Run `f` with upward sparing off: a survivor no longer spares the
    /// objects its identity references, so the frontier hard check fires.
    #[cfg(debug_assertions)]
    pub fn without_upward_sparing<R>(f: impl FnOnce() -> R) -> R {
        NO_UPWARD.with(|c| c.set(true));
        let out = f();
        NO_UPWARD.with(|c| c.set(false));
        out
    }

    /// Run `f` with the call's survivor sets forced empty (the same-commit
    /// guard alone must keep the rows the call wrote live).
    #[cfg(debug_assertions)]
    pub fn without_survivors<R>(f: impl FnOnce() -> R) -> R {
        NO_SURVIVORS.with(|c| c.set(true));
        let out = f();
        NO_SURVIVORS.with(|c| c.set(false));
        out
    }

    pub(crate) fn upward_disabled() -> bool {
        #[cfg(debug_assertions)]
        {
            NO_UPWARD.with(|c| c.get())
        }
        #[cfg(not(debug_assertions))]
        {
            false
        }
    }

    pub(crate) fn survivors_disabled() -> bool {
        #[cfg(debug_assertions)]
        {
            NO_SURVIVORS.with(|c| c.get())
        }
        #[cfg(not(debug_assertions))]
        {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::api::commit_structural;
    use crate::object_delete::{close_leg_set_sql, object_legs};
    use crate::types::{StructuralChange as SC, WriteOptions};

    fn class(name: &str, dims: &[&str]) -> SC {
        SC::CreateEntityClass {
            name: name.into(),
            dims: dims.iter().map(|d| d.to_string()).collect(),
            description: None,
        }
    }

    fn entity(class: &str, name: &str, elements: &[&str]) -> SC {
        SC::CreateEntity {
            class: class.into(),
            name: (!name.is_empty()).then(|| name.to_string()),
            elements: elements.iter().map(|e| e.to_string()).collect(),
            description: None,
        }
    }

    /// node, unit; link(node, unit); flow(link, node): a three-level N-D
    /// chain. Entities n1, n2, u1, l1 = (n1, u1), l2 = (n2, u1),
    /// f1 = (l1, n2).
    fn chain_db(dir: &tempfile::TempDir) -> std::path::PathBuf {
        let db = dir.path().join("t.duckdb");
        crate::schema_init::init_schema(&db).unwrap();
        commit_structural(
            &db,
            vec![
                class("node", &[]),
                class("unit", &[]),
                class("link", &["node", "unit"]),
                class("flow", &["link", "node"]),
                entity("node", "n1", &[]),
                entity("node", "n2", &[]),
                entity("unit", "u1", &[]),
                entity("link", "l1", &["n1", "u1"]),
                entity("link", "l2", &["n2", "u1"]),
                entity("flow", "f1", &["l1", "n2"]),
            ],
            "t",
            "m",
            WriteOptions::default(),
        )
        .unwrap();
        db
    }

    fn id(conn: &Connection, table: &str, name: &str) -> i64 {
        conn.query_row(
            &format!("SELECT id FROM {table} WHERE cur_name = lower(?)"),
            [name],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn names(conn: &Connection, set: &str, table: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT name FROM {table} WHERE valid_to_commit IS NULL \
                 AND id IN (SELECT id FROM {set}) ORDER BY name"
            ))
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn open(db: &Path) -> Connection {
        Connection::open(db).unwrap()
    }

    /// The dependent set grows by the B1 and class frontiers; the close
    /// set is it minus the survivors spared upward; the frontier of the
    /// close set never reaches a survivor (purge plan §3.6, V-B1).
    #[test]
    fn two_sets_spare_upward_through_an_nd_chain() {
        let dir = tempfile::tempdir().unwrap();
        let db = chain_db(&dir);
        let conn = open(&db);
        create_survivor_tables(&conn).unwrap();

        // Entities{class: node}: the B1 frontier reaches l1, l2, then f1.
        let node = id(&conn, "entity_class", "node");
        build_dep_sets(
            &conn,
            0,
            &[(
                SetKind::Entity,
                format!(
                    "SELECT id FROM entity WHERE valid_to_commit IS NULL \
                     AND entity_class_id = {node}"
                ),
            )],
            true,
        )
        .unwrap();
        assert_eq!(
            names(&conn, &dep_table(0, SetKind::Entity), "entity"),
            ["f1", "l1", "l2", "n1", "n2"]
        );

        // Classes{unit}: B2 reaches link, then flow; the classes' entities
        // and their B1 containers follow.
        let unit = id(&conn, "entity_class", "unit");
        build_dep_sets(
            &conn,
            1,
            &[(SetKind::Class, format!("SELECT {unit}"))],
            true,
        )
        .unwrap();
        assert_eq!(
            names(&conn, &dep_table(1, SetKind::Class), "entity_class"),
            ["flow", "link", "unit"]
        );
        assert_eq!(
            names(&conn, &dep_table(1, SetKind::Entity), "entity"),
            ["f1", "l1", "l2", "u1"]
        );

        // Entities{} with f1 re-stated: f1 spares l1 and n2, l1 spares n1
        // and u1, and their classes; only l2 closes.
        build_dep_sets(
            &conn,
            2,
            &[(
                SetKind::Entity,
                "SELECT id FROM entity WHERE valid_to_commit IS NULL".into(),
            )],
            true,
        )
        .unwrap();
        let f1 = id(&conn, "entity", "f1");
        insert_ids(&conn, &surv_table(SetKind::Entity), &[f1]).unwrap();
        spare_upward(&conn).unwrap();
        assert_eq!(
            names(&conn, &surv_table(SetKind::Entity), "entity"),
            ["f1", "l1", "n1", "n2", "u1"]
        );
        assert_eq!(
            names(&conn, &surv_table(SetKind::Class), "entity_class"),
            ["flow", "link", "node", "unit"]
        );
        build_close_sets(&conn, 2).unwrap();
        assert_eq!(
            names(&conn, &close_table(2, SetKind::Entity), "entity"),
            ["l2"]
        );
        check_frontier(&conn, 2).unwrap();
        drop_tables(&conn).unwrap();
        let left: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_tables() WHERE temporary",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
    }

    /// With upward sparing off, a re-stated f1 leaves its elements in the
    /// close set, and the B1 frontier of the closing l1 reaches f1: the hard
    /// check refuses (purge plan §3.5.3, test 7).
    #[test]
    fn frontier_reaching_a_survivor_is_internal() {
        let dir = tempfile::tempdir().unwrap();
        let db = chain_db(&dir);
        let conn = open(&db);
        create_survivor_tables(&conn).unwrap();
        build_dep_sets(
            &conn,
            0,
            &[(
                SetKind::Entity,
                "SELECT id FROM entity WHERE valid_to_commit IS NULL".into(),
            )],
            true,
        )
        .unwrap();
        let f1 = id(&conn, "entity", "f1");
        insert_ids(&conn, &surv_table(SetKind::Entity), &[f1]).unwrap();
        test_hooks::without_upward_sparing(|| spare_upward(&conn)).unwrap();
        build_close_sets(&conn, 0).unwrap();
        let err = check_frontier(&conn, 0).unwrap_err();
        assert!(matches!(err, DbError::Internal(_)), "{err}");
        assert!(err.to_string().contains("entity -> entity"), "{err}");
    }

    /// A set-form leg is a hash join of the table against the id set, never
    /// a nested loop (purge plan §3.6, test 11; P0 spike).
    #[test]
    fn set_form_leg_plans_as_a_hash_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("t.duckdb");
        crate::schema_init::init_schema(&db).unwrap();
        let conn = open(&db);
        conn.execute_batch(
            "INSERT INTO commit (user, message) VALUES ('t', 'fixture'); \
             INSERT INTO alternative (name, valid_from_commit) VALUES ('base', 2); \
             INSERT INTO entity_class (name, valid_from_commit) VALUES ('node', 2); \
             INSERT INTO entity (entity_class_id, name, valid_from_commit) \
               SELECT 1, 'e' || i, 2 FROM range(20000) t(i); \
             INSERT INTO entity_alternative (entity_id, alternative_id, active, valid_from_commit) \
               SELECT id, 1, TRUE, 2 FROM entity; \
             CREATE TEMP TABLE ids AS SELECT id FROM entity WHERE id % 2 = 0;",
        )
        .unwrap();
        let legs = object_legs(ObjectKind::Entity).unwrap();
        for leg in legs.dependents.iter().filter(|l| !l.array) {
            let sql = close_leg_set_sql(leg, "ids", None);
            let mut stmt = conn.prepare(&format!("EXPLAIN {sql}")).unwrap();
            let plan: Vec<String> = stmt
                .query_map([99i64, 99], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let plan = plan.join("\n");
            // DuckDB 2.0 labels operators "Hash Join"; older releases
            // "HASH_JOIN": accept either spelling.
            assert!(
                plan.contains("Hash Join") || plan.contains("HASH_JOIN"),
                "{}:\n{plan}",
                leg.table
            );
            for bad in [
                "Nested Loop",
                "NESTED_LOOP",
                "Blockwise NL",
                "BLOCKWISE_NL",
                "Piecewise Merge",
                "PIECEWISE_MERGE",
                "Cross Product",
                "CROSS_PRODUCT",
            ] {
                assert!(!plan.contains(bad), "{}: {bad}\n{plan}", leg.table);
            }
        }
    }
}
