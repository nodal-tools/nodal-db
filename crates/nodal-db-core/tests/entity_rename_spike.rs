//! P0 spike of entity_rename_PLAN (CRITIQUE2 D-1/D-2): how to close the
//! current `entity` row of a rename on a realistic file, and where a set
//! path pays off. Ignored: run by hand, alone, in release:
//!
//! ```text
//! cargo test --release -p nodal-db-core --test entity_rename_spike -- --ignored --nocapture
//! ```
//!
//! Fixture: 10^6 entities in three classes (`node` 3·10^5, `unit` 2·10^5,
//! 2-D `unit__node` 5·10^5 with 10^6 element rows), then 0, 10^4 or 10^5
//! prior revisions (renames of ids spread over the table, ten commits; each
//! successor row lands at the tail with an old id, widening the tail's
//! zone maps on `id`). On a copy of each fixture, one commit renames N
//! entities spread over the table (N = 1, 10, 100, 10^4), by:
//!
//! - `per_id`: fenced read, fenced uniqueness lookup, close
//!   `WHERE id = ? AND valid_from_commit < ? AND valid_to_commit IS NULL`
//!   (zone-map pruned), insert the successor;
//! - `rowid`: as `per_id`, the read also returns `rowid`, close
//!   `WHERE rowid = ?`;
//! - `rowid_guarded`: as `rowid`, the close also checks `id`, the span and
//!   the current-row filter (the form the core uses);
//! - `hybrid`: per-item fenced read and uniqueness lookup, then ONE
//!   set-form close (`id IN (temp table)`) and ONE `INSERT … SELECT`;
//! - `set`: everything set-based: one join resolves every name, one join
//!   checks uniqueness, one close, one insert.
//!
//! Also the element-name lookup crossover for the tuple index's scoped load
//! (k fenced lookups vs one set join) and the scoped load itself.

use std::path::Path;
use std::time::{Duration, Instant};

use duckdb::{params, AccessMode, Connection};
use nodal_db_core::api::ENTITY_NAME_LOOKUP_SQL;
use nodal_db_core::conn::core_config;
use nodal_db_core::schema_init::init_schema;

const N_NODE: i64 = 300_000;
const N_UNIT: i64 = 200_000;
const N_PAIR: i64 = 500_000;

/// The fenced read of the row a rename revises (`?1` class, `?2` name).
const READ_SQL: &str = "SELECT rid, id, name, description, version, valid_from_commit FROM \
       (SELECT rowid AS rid, id, name, description, version, valid_from_commit, \
               entity_class_id, valid_to_commit FROM entity \
        WHERE lower(name) = lower(?2) OFFSET 0) \
     WHERE entity_class_id = ?1 AND valid_to_commit IS NULL";

fn open(db: &Path) -> Connection {
    Connection::open_with_flags(db, core_config(AccessMode::ReadWrite).unwrap()).unwrap()
}

fn begin(conn: &Connection, msg: &str) -> i64 {
    conn.execute_batch("BEGIN").unwrap();
    conn.query_row(
        "INSERT INTO commit (user, message) VALUES ('spike', ?) RETURNING id",
        params![msg],
        |r| r.get(0),
    )
    .unwrap()
}

fn class(conn: &Connection, c: i64, name: &str) -> i64 {
    conn.query_row(
        "INSERT INTO entity_class (name, valid_from_commit) VALUES (?, ?) RETURNING id",
        params![name, c],
        |r| r.get(0),
    )
    .unwrap()
}

fn build_fixture(db: &Path, prior: i64) {
    init_schema(db).unwrap();
    let conn = open(db);
    let c = begin(&conn, "fixture");
    let node = class(&conn, c, "node");
    let unit = class(&conn, c, "unit");
    let pair = class(&conn, c, "unit__node");
    conn.execute(
        "INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, \
         valid_from_commit) VALUES (?1, 1, ?2, ?4), (?1, 2, ?3, ?4)",
        params![pair, unit, node, c],
    )
    .unwrap();
    let ins = |cid: i64, n: i64, name: &str| -> i64 {
        conn.execute(
            &format!(
                "INSERT INTO entity (entity_class_id, name, valid_from_commit) \
                 SELECT ?, {name}, ? FROM range(?) t(i)"
            ),
            params![cid, c, n],
        )
        .unwrap();
        conn.query_row(
            "SELECT min(id) FROM entity WHERE entity_class_id = ?",
            params![cid],
            |r| r.get(0),
        )
        .unwrap()
    };
    let n0 = ins(node, N_NODE, "'n' || i");
    let u0 = ins(unit, N_UNIT, "'u' || i");
    let p0 = ins(
        pair,
        N_PAIR,
        &format!("'u' || (i % {N_UNIT}) || '__n' || (i % {N_NODE})"),
    );
    conn.execute(
        &format!(
            "INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit) \
             SELECT ?1 + i, 1, ?2 + i % {N_UNIT}, ?4 FROM range({N_PAIR}) t(i) \
             UNION ALL SELECT ?1 + i, 2, ?3 + i % {N_NODE}, ?4 FROM range({N_PAIR}) t(i)"
        ),
        params![p0, u0, n0, c],
    )
    .unwrap();
    conn.execute_batch("COMMIT").unwrap();
    // Prior revisions, in ten commits: ids spread over the whole table.
    let total = N_NODE + N_UNIT + N_PAIR;
    let per = prior / 10;
    for k in 0..10 {
        if per == 0 {
            break;
        }
        let c = begin(&conn, "prior");
        conn.execute_batch(&format!(
            "CREATE TEMP TABLE pick AS SELECT DISTINCT {n0} + (j * 7919 + {k} * 104729) % {total} \
             AS id FROM range({per}) t(j)"
        ))
        .unwrap();
        conn.execute(
            "UPDATE entity SET valid_to_commit = ?1 WHERE id IN (SELECT id FROM pick) \
             AND valid_from_commit < ?1 AND valid_to_commit IS NULL",
            params![c],
        )
        .unwrap();
        conn.execute(
            &format!(
                "INSERT INTO entity (id, entity_class_id, name, description, valid_from_commit, \
                 version) SELECT id, entity_class_id, name || '_r{k}', description, ?1, \
                 version + 1 FROM entity WHERE valid_to_commit = ?1"
            ),
            params![c],
        )
        .unwrap();
        conn.execute_batch("DROP TABLE pick; COMMIT").unwrap();
    }
    conn.execute_batch("CHECKPOINT").unwrap();
}

#[derive(Default, Debug)]
struct Times {
    read: Duration,
    close: Duration,
    insert: Duration,
    commit: Duration,
    total: Duration,
}

fn targets(db: &Path, n: usize) -> Vec<(i64, String)> {
    let conn = open(db);
    let mut stmt = conn
        .prepare(
            "SELECT entity_class_id, name FROM entity WHERE valid_to_commit IS NULL \
             ORDER BY hash(id) LIMIT ?",
        )
        .unwrap();
    let rows = stmt
        .query_map(params![n as i64], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<(i64, String)>, _>>()
        .unwrap();
    rows
}

type Row = (i64, i64, String, Option<String>, i64, i64);

fn read_row(conn: &Connection, cid: i64, name: &str) -> Row {
    conn.prepare_cached(READ_SQL)
        .unwrap()
        .query_row(params![cid, name], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })
        .unwrap()
}

fn unique_ok(conn: &Connection, cid: i64, name: &str) {
    let hit = conn
        .prepare_cached(ENTITY_NAME_LOOKUP_SQL)
        .unwrap()
        .query_row(params![cid, name], |r| r.get::<_, i64>(0));
    assert!(hit.is_err(), "new name {name} taken");
}

fn insert_successor(conn: &Connection, c: i64, cid: i64, row: &Row, new: &str) {
    conn.prepare_cached(
        "INSERT INTO entity (id, entity_class_id, name, description, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .unwrap()
    .execute(params![row.1, cid, new, row.3, c, row.4 + 1])
    .unwrap();
}

fn run(db: &Path, method: &str, t: &[(i64, String)]) -> Times {
    let conn = open(db);
    let c = begin(&conn, "renames");
    let mut tm = Times::default();
    let start = Instant::now();
    match method {
        "per_id" | "rowid" | "rowid_guarded" => {
            for (cid, name) in t {
                let new = format!("{name}_x");
                let s = Instant::now();
                let row = read_row(&conn, *cid, name);
                unique_ok(&conn, *cid, &new);
                tm.read += s.elapsed();
                let s = Instant::now();
                let n = if method == "per_id" {
                    conn.prepare_cached(
                        "UPDATE entity SET valid_to_commit = ?1 WHERE id = ?2 \
                         AND valid_from_commit < ?1 AND valid_to_commit IS NULL",
                    )
                    .unwrap()
                    .execute(params![c, row.1])
                    .unwrap()
                } else if method == "rowid" {
                    conn.prepare_cached("UPDATE entity SET valid_to_commit = ?1 WHERE rowid = ?2")
                        .unwrap()
                        .execute(params![c, row.0])
                        .unwrap()
                } else {
                    conn.prepare_cached(
                        "UPDATE entity SET valid_to_commit = ?1 WHERE rowid = ?2 AND id = ?3 \
                         AND valid_from_commit < ?1 AND valid_to_commit IS NULL",
                    )
                    .unwrap()
                    .execute(params![c, row.0, row.1])
                    .unwrap()
                };
                assert_eq!(n, 1);
                tm.close += s.elapsed();
                let s = Instant::now();
                insert_successor(&conn, c, *cid, &row, &new);
                tm.insert += s.elapsed();
            }
        }
        "hybrid" => {
            let s = Instant::now();
            let mut rows = Vec::with_capacity(t.len());
            for (cid, name) in t {
                let new = format!("{name}_x");
                let row = read_row(&conn, *cid, name);
                unique_ok(&conn, *cid, &new);
                rows.push((*cid, row, new));
            }
            tm.read += s.elapsed();
            let s = Instant::now();
            let ids = list_i64(rows.iter().map(|r| r.1 .1));
            let cids = list_i64(rows.iter().map(|r| r.0));
            let names = list_str(rows.iter().map(|r| r.2.clone()));
            let vers = list_i64(rows.iter().map(|r| r.1 .4 + 1));
            conn.execute(
                "CREATE TEMP TABLE ren AS SELECT unnest(?::BIGINT[]) AS id, \
                 unnest(?::BIGINT[]) AS cid, unnest(?::VARCHAR[]) AS new_name, \
                 unnest(?::BIGINT[]) AS version",
                params![ids, cids, names, vers],
            )
            .unwrap();
            let n = conn
                .execute(
                    "UPDATE entity SET valid_to_commit = ?1 WHERE id IN (SELECT id FROM ren) \
                     AND valid_from_commit < ?1 AND valid_to_commit IS NULL",
                    params![c],
                )
                .unwrap();
            assert_eq!(n, t.len());
            tm.close += s.elapsed();
            let s = Instant::now();
            conn.execute(
                "INSERT INTO entity (id, entity_class_id, name, description, valid_from_commit, \
                 version) SELECT id, cid, new_name, NULL, ?, version FROM ren",
                params![c],
            )
            .unwrap();
            conn.execute_batch("DROP TABLE ren").unwrap();
            tm.insert += s.elapsed();
        }
        "set" => {
            let s = Instant::now();
            let cids = list_i64(t.iter().map(|r| r.0));
            let cur = list_str(t.iter().map(|r| r.1.clone()));
            let new = list_str(t.iter().map(|r| format!("{}_x", r.1)));
            let ords = list_i64(0..t.len() as i64);
            conn.execute(
                "CREATE TEMP TABLE ren AS SELECT unnest(?::BIGINT[]) AS ord, \
                 unnest(?::BIGINT[]) AS cid, unnest(?::VARCHAR[]) AS cur, \
                 unnest(?::VARCHAR[]) AS new_name",
                params![ords, cids, cur, new],
            )
            .unwrap();
            conn.execute_batch(
                "CREATE TEMP TABLE rid AS SELECT r.ord, e.id, e.description, e.version \
                 FROM ren r JOIN entity e ON e.entity_class_id = r.cid \
                 AND e.valid_to_commit IS NULL AND lower(e.name) = lower(r.cur)",
            )
            .unwrap();
            let hits: i64 = conn
                .query_row(
                    "SELECT count(*) FROM ren r JOIN entity e ON e.entity_class_id = r.cid \
                     AND e.valid_to_commit IS NULL AND lower(e.name) = lower(r.new_name)",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(hits, 0);
            tm.read += s.elapsed();
            let s = Instant::now();
            let n = conn
                .execute(
                    "UPDATE entity SET valid_to_commit = ?1 WHERE id IN (SELECT id FROM rid) \
                     AND valid_from_commit < ?1 AND valid_to_commit IS NULL",
                    params![c],
                )
                .unwrap();
            assert_eq!(n, t.len());
            tm.close += s.elapsed();
            let s = Instant::now();
            conn.execute(
                "INSERT INTO entity (id, entity_class_id, name, description, valid_from_commit, \
                 version) SELECT rid.id, ren.cid, ren.new_name, rid.description, ?, \
                 rid.version + 1 FROM rid JOIN ren USING (ord)",
                params![c],
            )
            .unwrap();
            conn.execute_batch("DROP TABLE ren; DROP TABLE rid")
                .unwrap();
            tm.insert += s.elapsed();
        }
        _ => unreachable!(),
    }
    let s = Instant::now();
    conn.execute_batch("COMMIT").unwrap();
    tm.commit = s.elapsed();
    tm.total = start.elapsed();
    let (live, closed): (i64, i64) = conn
        .query_row(
            "SELECT count(*) FILTER (WHERE valid_to_commit IS NULL), \
                    count(*) FILTER (WHERE valid_to_commit = ?) FROM entity",
            params![c],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(live, N_NODE + N_UNIT + N_PAIR);
    assert_eq!(closed, t.len() as i64);
    tm
}

fn list_i64(it: impl Iterator<Item = i64>) -> duckdb::types::Value {
    duckdb::types::Value::List(it.map(duckdb::types::Value::BigInt).collect())
}

fn list_str(it: impl Iterator<Item = String>) -> duckdb::types::Value {
    duckdb::types::Value::List(it.map(duckdb::types::Value::Text).collect())
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

/// k element names of `node`: k fenced lookups vs one set join; then the
/// scoped tuple load of the 2-D class over those position-1 elements.
fn lookups(db: &Path) {
    let conn = open(db);
    let node: i64 = conn
        .query_row("SELECT id FROM entity_class WHERE name = 'unit'", [], |r| {
            r.get(0)
        })
        .unwrap();
    let pair: i64 = conn
        .query_row(
            "SELECT id FROM entity_class WHERE name = 'unit__node'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    for k in [1usize, 10, 30, 100, 300, 1000] {
        let names: Vec<String> = (0..k)
            .map(|i| format!("u{}", i * 97 % N_UNIT as usize))
            .collect();
        let s = Instant::now();
        let mut ids = Vec::new();
        for n in &names {
            if let Ok(id) = conn
                .prepare_cached(ENTITY_NAME_LOOKUP_SQL)
                .unwrap()
                .query_row(params![node, n], |r| r.get::<_, i64>(0))
            {
                ids.push(id);
            }
        }
        let fenced = s.elapsed();
        let s = Instant::now();
        let mut stmt = conn
            .prepare_cached(
                "WITH k AS (SELECT unnest(?::VARCHAR[]) AS n) SELECT e.id FROM k JOIN entity e \
                 ON e.entity_class_id = ? AND e.valid_to_commit IS NULL \
                 AND lower(e.name) = lower(k.n)",
            )
            .unwrap();
        let set_ids: Vec<i64> = stmt
            .query_map(params![list_str(names.iter().cloned()), node], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let set = s.elapsed();
        assert_eq!(set_ids.len(), ids.len());
        let s = Instant::now();
        let mut stmt = conn
            .prepare_cached(
                "SELECT el.entity_id, list(el.element_entity_id ORDER BY el.position) \
                 FROM entity_element el JOIN entity e ON e.id = el.entity_id \
                   AND e.entity_class_id = ?2 AND e.valid_to_commit IS NULL \
                 WHERE el.valid_to_commit IS NULL AND el.entity_id IN \
                   (SELECT entity_id FROM entity_element WHERE position = 1 \
                      AND valid_to_commit IS NULL \
                      AND element_entity_id IN (SELECT unnest(?1::BIGINT[]))) \
                 GROUP BY el.entity_id",
            )
            .unwrap();
        let loaded = stmt
            .query_map(params![list_i64(ids.iter().copied()), pair], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
            .count();
        let load = s.elapsed();
        println!(
            "lookup k={k:5}: fenced {:8.2} ms, set {:8.2} ms; scoped tuple load {:8.2} ms \
             ({loaded} tuples)",
            ms(fenced),
            ms(set),
            ms(load)
        );
    }
    let s = Instant::now();
    let all: i64 = conn
        .query_row(
            "SELECT count(*) FROM (SELECT el.entity_id, list(el.element_entity_id ORDER BY \
             el.position) FROM entity_element el JOIN entity e ON e.id = el.entity_id \
             AND e.entity_class_id = ? AND e.valid_to_commit IS NULL \
             WHERE el.valid_to_commit IS NULL GROUP BY el.entity_id)",
            params![pair],
            |r| r.get(0),
        )
        .unwrap();
    println!(
        "whole-class tuple load: {:8.2} ms ({all} tuples)",
        ms(s.elapsed())
    );
}

#[test]
#[ignore = "P0 spike: run by hand in release (see the module docs)"]
fn entity_rename_close_forms() {
    let dir = tempfile::tempdir().unwrap();
    let only: Option<String> = std::env::var("SPIKE_METHODS").ok();
    let list = |var: &str, all: &[i64]| -> Vec<i64> {
        std::env::var(var)
            .ok()
            .map(|v| v.split(',').map(|x| x.parse().unwrap()).collect())
            .unwrap_or_else(|| all.to_vec())
    };
    for prior in list("SPIKE_PRIOR", &[0, 10_000, 100_000]) {
        let fx = dir.path().join(format!("fx{prior}.duckdb"));
        let s = Instant::now();
        build_fixture(&fx, prior);
        println!(
            "\n== fixture: 10^6 entities, {prior} prior revisions (built in {:.1} s, {} MB)",
            s.elapsed().as_secs_f64(),
            std::fs::metadata(&fx).unwrap().len() / 1_000_000
        );
        if prior == 0 && only.is_none() {
            lookups(&fx);
        }
        for n in list("SPIKE_N", &[1, 10, 100, 10_000]) {
            let n = n as usize;
            let t = targets(&fx, n);
            for method in ["per_id", "rowid", "rowid_guarded", "hybrid", "set"] {
                if method == "rowid_guarded" && only.is_none() {
                    continue;
                }
                if only
                    .as_deref()
                    .is_some_and(|o| !o.split(',').any(|m| m == method))
                {
                    continue;
                }
                let copy = dir.path().join("run.duckdb");
                let _ = std::fs::remove_file(&copy);
                std::fs::copy(&fx, &copy).unwrap();
                let tm = run(&copy, method, &t);
                println!(
                    "N={n:6} {method:7}: total {:9.2} ms ({:7.3} ms/item) | read {:9.2} \
                     close {:9.2} insert {:9.2} commit {:7.2}",
                    ms(tm.total),
                    ms(tm.total) / n as f64,
                    ms(tm.read),
                    ms(tm.close),
                    ms(tm.insert),
                    ms(tm.commit)
                );
            }
        }
        std::fs::remove_file(&fx).unwrap();
    }
}
