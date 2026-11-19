//! Metadata: the pair pool, sources, licenses, roles and the two link tables
//! (`schema/001_meta.sql`). This module is the one writer of
//! `metadata_pair`, `source`, `license`, `source_license`, `metadata_role`,
//! `object_metadata_link` and `value_metadata_link` (invariant 2: one writer
//! per table); `crate::property` owns `database_property`.
//!
//! Value links belong to a **value version**: a link row is valid while the
//! value version current at its `valid_from_commit` is. Every new version of
//! a value with links closes them, and a *keep* reopens them at the same
//! commit (`version + 1`), so link spans tile with value versions. The
//! write path's keep / set / clear statements are the metadata channel of
//! `api::commit_change_set`; the lifecycle helpers here serve the other
//! value writers and closers:
//!   - a value delete closes the value's links ([`close_value_links`]);
//!   - a template `map_values` rewrite keeps them ([`keep_value_links_where`],
//!     owner ruling OD3), its drop arm closes them
//!     ([`close_orphaned_value_links`]);
//!   - a template `merge_parameter` carries them to the filled target slots
//!     and carries the source definition's own links ([`carry_merged_links`]);
//!   - object deletes close links through the registry legs
//!     (`object_delete::object_legs`; links never block, owner ruling OD2).

use duckdb::{params, Connection};

use crate::error::DbError;

/// One key triple `(entity, parameter definition, alternative)`.
pub(crate) type Triple = (i64, i64, i64);

/// The rows of a value-link triple set: a `FROM` source with columns
/// `e, p, a`. `Lists` binds three `BIGINT[]` lists; `Query` is a SELECT of
/// three columns (no binds).
pub(crate) enum TripleSet<'a> {
    Lists(&'a [Triple]),
    Query(&'a str),
}

impl TripleSet<'_> {
    fn source_sql(&self) -> String {
        match self {
            TripleSet::Lists(_) => "(SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                 unnest(?::BIGINT[]) AS a) k"
                .to_string(),
            TripleSet::Query(q) => format!("(SELECT * FROM ({q}) q(e, p, a)) k"),
        }
    }

    fn binds(&self) -> Vec<duckdb::types::Value> {
        match self {
            TripleSet::Lists(ts) => {
                let list = |f: fn(&Triple) -> i64| {
                    duckdb::types::Value::List(
                        ts.iter().map(|t| duckdb::types::Value::BigInt(f(t))).collect(),
                    )
                };
                vec![list(|t| t.0), list(|t| t.1), list(|t| t.2)]
            }
            TripleSet::Query(_) => Vec::new(),
        }
    }
}

/// The match of a `value_metadata_link` row against the triple set `k`.
const K_MATCH: &str = "value_metadata_link.entity_id = k.e \
     AND value_metadata_link.parameter_definition_id = k.p \
     AND value_metadata_link.alternative_id = k.a";

/// Does the file hold any live value link? (One index-free probe; a
/// link-free file skips every link statement.)
pub(crate) fn has_value_links(conn: &Connection) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM value_metadata_link WHERE valid_to_commit IS NULL)",
        [],
        |r| r.get(0),
    )?)
}

/// Close the live links of `set`'s triples at commit `c` (same-commit
/// guard: a link this commit created is never closed). Returns rows closed.
pub(crate) fn close_links(conn: &Connection, c: i64, set: &TripleSet<'_>) -> Result<usize, DbError> {
    let mut binds: Vec<duckdb::types::Value> = vec![c.into()];
    binds.extend(set.binds());
    binds.push(c.into());
    Ok(conn.execute(
        &format!(
            "UPDATE value_metadata_link SET valid_to_commit = ? FROM {} \
             WHERE {K_MATCH} AND value_metadata_link.valid_to_commit IS NULL \
               AND value_metadata_link.valid_from_commit < ?",
            set.source_sql()
        ),
        duckdb::params_from_iter(binds.iter()),
    )?)
}

/// Reopen at `c` (`version + 1`) the links of `set`'s triples that were
/// closed at `c`: the second half of a *keep*. Returns rows inserted.
pub(crate) fn reopen_links(conn: &Connection, c: i64, set: &TripleSet<'_>) -> Result<usize, DbError> {
    let mut binds: Vec<duckdb::types::Value> = vec![c.into()];
    binds.extend(set.binds());
    binds.push(c.into());
    Ok(conn.execute(
        &format!(
            "INSERT INTO value_metadata_link (entity_id, parameter_definition_id, \
               alternative_id, target_kind, target_id, role_id, valid_from_commit, version) \
             SELECT value_metadata_link.entity_id, value_metadata_link.parameter_definition_id, \
                    value_metadata_link.alternative_id, target_kind, target_id, role_id, ?, \
                    version + 1 \
             FROM value_metadata_link, {} \
             WHERE {K_MATCH} AND value_metadata_link.valid_to_commit = ? \
               AND NOT EXISTS (SELECT 1 FROM value_metadata_link x \
                 WHERE x.valid_to_commit IS NULL \
                   AND x.entity_id = value_metadata_link.entity_id \
                   AND x.parameter_definition_id = value_metadata_link.parameter_definition_id \
                   AND x.alternative_id = value_metadata_link.alternative_id \
                   AND x.target_kind = value_metadata_link.target_kind \
                   AND x.target_id = value_metadata_link.target_id \
                   AND coalesce(x.role_id, 0) = coalesce(value_metadata_link.role_id, 0))",
            set.source_sql()
        ),
        duckdb::params_from_iter(binds.iter()),
    )?)
}

/// A value delete (or any closer of whole values): close the links of the
/// closed `triples`, chunked. Skips every statement on a link-free file.
/// Returns rows closed (reported apart from the value rows closed).
pub fn close_value_links(conn: &Connection, c: i64, triples: &[Triple]) -> Result<usize, DbError> {
    if triples.is_empty() || !has_value_links(conn)? {
        return Ok(0);
    }
    let mut n = 0;
    for chunk in triples.chunks(crate::history::LONG_INSERT_CHUNK) {
        n += close_links(conn, c, &TripleSet::Lists(chunk))?;
    }
    Ok(n)
}

/// *Keep* across a new value version written outside the write path (a
/// template rewrite, OD3): close and reopen at `c` the links of the triples
/// `triple_sql` selects (three columns). Returns the links kept.
pub(crate) fn keep_value_links_where(
    conn: &Connection,
    c: i64,
    triple_sql: &str,
) -> Result<usize, DbError> {
    if !has_value_links(conn)? {
        return Ok(0);
    }
    let set = TripleSet::Query(triple_sql);
    close_links(conn, c, &set)?;
    reopen_links(conn, c, &set)
}

/// Close the live links of definition `pd_id` whose value is no longer live
/// in any tier (a template drop arm closed it at `c`). Returns rows closed.
pub(crate) fn close_orphaned_value_links(
    conn: &Connection,
    c: i64,
    pd_id: i64,
) -> Result<usize, DbError> {
    if !has_value_links(conn)? {
        return Ok(0);
    }
    let q = format!(
        "SELECT l.entity_id, l.parameter_definition_id, l.alternative_id \
         FROM value_metadata_link l \
         WHERE l.valid_to_commit IS NULL AND l.parameter_definition_id = {pd_id} \
           AND NOT EXISTS (SELECT 1 FROM value_keys v WHERE v.entity_id = l.entity_id \
                 AND v.parameter_definition_id = l.parameter_definition_id \
                 AND v.alternative_id = l.alternative_id)"
    );
    close_links(conn, c, &TripleSet::Query(&q))
}

/// What a `merge_parameter` carried ([`carry_merged_links`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Carried {
    /// Value links copied to the target's filled slots.
    pub(crate) value_links: usize,
    /// The source definition's own object links copied to the target.
    pub(crate) object_links: usize,
}

/// `merge_parameter` (template reconcile): before the source definition
/// closes, copy (a) the value links of every source value carried into an
/// empty target slot at `c` (a target value with `valid_from_commit = c`)
/// onto that target value, and (b) the source definition's own object links
/// onto the target definition, anti-joined on the target's live edges. The
/// source's links then close with its cascade. "Copy carries" (owner ruling
/// 2026-11-17).
pub(crate) fn carry_merged_links(
    conn: &Connection,
    c: i64,
    src: i64,
    tgt: i64,
) -> Result<Carried, DbError> {
    let value_links = conn.execute(
        "INSERT INTO value_metadata_link (entity_id, parameter_definition_id, alternative_id, \
           target_kind, target_id, role_id, valid_from_commit, version) \
         SELECT l.entity_id, ?1, l.alternative_id, l.target_kind, l.target_id, l.role_id, ?2, \
                coalesce((SELECT max(x.version) FROM value_metadata_link x \
                   WHERE x.entity_id = l.entity_id AND x.parameter_definition_id = ?1 \
                     AND x.alternative_id = l.alternative_id AND x.target_kind = l.target_kind \
                     AND x.target_id = l.target_id \
                     AND coalesce(x.role_id, 0) = coalesce(l.role_id, 0)), 0) + 1 \
         FROM value_metadata_link l \
         WHERE l.valid_to_commit IS NULL AND l.parameter_definition_id = ?3 \
           AND EXISTS (SELECT 1 FROM value_scalar s WHERE s.entity_id = l.entity_id \
                 AND s.parameter_definition_id = ?1 AND s.alternative_id = l.alternative_id \
                 AND s.valid_from_commit = ?2 AND s.valid_to_commit IS NULL) \
           AND NOT EXISTS (SELECT 1 FROM value_metadata_link t \
                 WHERE t.valid_to_commit IS NULL AND t.entity_id = l.entity_id \
                   AND t.parameter_definition_id = ?1 AND t.alternative_id = l.alternative_id \
                   AND t.target_kind = l.target_kind AND t.target_id = l.target_id \
                   AND coalesce(t.role_id, 0) = coalesce(l.role_id, 0))",
        params![tgt, c, src],
    )?;
    let object_links = conn.execute(
        "INSERT INTO object_metadata_link (object_type, object_id, target_kind, target_id, \
           role_id, valid_from_commit, version) \
         SELECT 'parameter_definition', ?1, l.target_kind, l.target_id, l.role_id, ?2, \
                coalesce((SELECT max(x.version) FROM object_metadata_link x \
                   WHERE x.object_type = 'parameter_definition' AND x.object_id = ?1 \
                     AND x.target_kind = l.target_kind AND x.target_id = l.target_id \
                     AND coalesce(x.role_id, 0) = coalesce(l.role_id, 0)), 0) + 1 \
         FROM object_metadata_link l \
         WHERE l.valid_to_commit IS NULL AND l.object_type = 'parameter_definition' \
           AND l.object_id = ?3 \
           AND NOT EXISTS (SELECT 1 FROM object_metadata_link t \
                 WHERE t.valid_to_commit IS NULL AND t.object_type = 'parameter_definition' \
                   AND t.object_id = ?1 AND t.target_kind = l.target_kind \
                   AND t.target_id = l.target_id \
                   AND coalesce(t.role_id, 0) = coalesce(l.role_id, 0))",
        params![tgt, c, src],
    )?;
    Ok(Carried {
        value_links,
        object_links,
    })
}

/// Close the object links whose subject is one of the value lists `ids`
/// (a SELECT of ids) at `c` (the value-list close door, which runs outside
/// the object-delete registry). Returns rows closed.
pub(crate) fn close_object_links_of(
    conn: &Connection,
    c: i64,
    object_type: &str,
    ids_sql: &str,
) -> Result<usize, DbError> {
    Ok(conn.execute(
        &format!(
            "UPDATE object_metadata_link SET valid_to_commit = ? \
             WHERE object_type = ? AND object_id IN ({ids_sql}) \
               AND valid_to_commit IS NULL AND valid_from_commit < ?"
        ),
        params![c, object_type, c],
    )?)
}

/// Fail when any of `keys` has a live value link: the raw per-key writers
/// (`history::write_value`, `history::write_long_values`) cannot state keep
/// / set / clear, so they refuse an annotated value (use
/// `api::commit_change_set`).
pub(crate) fn refuse_linked(conn: &Connection, keys: &[Triple]) -> Result<(), DbError> {
    if keys.is_empty() || !has_value_links(conn)? {
        return Ok(());
    }
    let set = TripleSet::Lists(keys);
    let n: i64 = conn.query_row(
        &format!(
            "SELECT count(*) FROM value_metadata_link, {} \
             WHERE {K_MATCH} AND value_metadata_link.valid_to_commit IS NULL",
            set.source_sql()
        ),
        duckdb::params_from_iter(set.binds().iter()),
        |r| r.get(0),
    )?;
    if n > 0 {
        return Err(DbError::Api(format!(
            "this value has {n} metadata link(s): a new version must state keep, set or clear \
             — write it through commit_change_set with a value_metadata statement"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh schema with commit 2 holding three scalar values of pd 10
    /// (entities 1..=3, alternative 1), each with one pair link, and one
    /// object link on pd 10. Returns the connection.
    fn fixture(dir: &std::path::Path) -> Connection {
        let db = dir.join("u.duckdb");
        crate::schema_init::init_schema(&db).unwrap();
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO commit (user, message) VALUES ('t', 'v');
             INSERT INTO metadata_pair (name, value, valid_from_commit) VALUES ('n', 'x', 2);
             INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,
                 valid_from_commit, version, content_retained, value_kind, v_str)
               SELECT e, 10, 1, 2, 1, TRUE, 'string', 'old' FROM range(1, 4) t(e);
             INSERT INTO value_metadata_link (entity_id, parameter_definition_id,
                 alternative_id, target_kind, target_id, valid_from_commit)
               SELECT e, 10, 1, 'pair', 1, 2 FROM range(1, 4) t(e);
             INSERT INTO object_metadata_link (object_type, object_id, target_kind, target_id,
                 valid_from_commit) VALUES ('parameter_definition', 10, 'pair', 1, 2);
             INSERT INTO commit (user, message) VALUES ('t', 'c3');",
        )
        .unwrap();
        conn
    }

    fn n(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn a_template_rewrite_keeps_and_a_drop_closes_the_links() {
        let dir = tempfile::tempdir().unwrap();
        let conn = fixture(dir.path());
        // Rewrite entity 1 ('old' -> 'new') at commit 3: its link is kept.
        conn.execute_batch(
            "UPDATE value_scalar SET valid_to_commit = 3 WHERE entity_id = 1;
             INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,
                 valid_from_commit, version, content_retained, value_kind, v_str)
               VALUES (1, 10, 1, 3, 2, TRUE, 'string', 'new');",
        )
        .unwrap();
        let kept = keep_value_links_where(
            &conn,
            3,
            "SELECT entity_id, parameter_definition_id, alternative_id FROM value_scalar \
             WHERE valid_from_commit = 3 AND valid_to_commit IS NULL",
        )
        .unwrap();
        assert_eq!(kept, 1);
        // A second keep at the same commit reopens nothing twice.
        assert_eq!(
            keep_value_links_where(
                &conn,
                3,
                "SELECT entity_id, parameter_definition_id, alternative_id FROM value_scalar \
                 WHERE valid_from_commit = 3 AND valid_to_commit IS NULL",
            )
            .unwrap(),
            0
        );
        assert_eq!(
            n(&conn, "SELECT version FROM value_metadata_link WHERE entity_id = 1 \
                      AND valid_to_commit IS NULL"),
            2
        );
        // Drop entity 2's value: its link closes, entity 3's stays.
        conn.execute_batch("UPDATE value_scalar SET valid_to_commit = 3 WHERE entity_id = 2")
            .unwrap();
        assert_eq!(close_orphaned_value_links(&conn, 3, 10).unwrap(), 1);
        assert_eq!(
            n(&conn, "SELECT count(*) FROM value_metadata_link WHERE valid_to_commit IS NULL"),
            2
        );
    }

    #[test]
    fn a_merge_carries_value_and_parameter_links() {
        let dir = tempfile::tempdir().unwrap();
        let conn = fixture(dir.path());
        // pd 20 is the target; entity 1's value is carried into it at 3.
        conn.execute_batch(
            "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,
                 valid_from_commit, version, content_retained, value_kind, v_str)
               VALUES (1, 20, 1, 3, 1, TRUE, 'string', 'old');",
        )
        .unwrap();
        let c = carry_merged_links(&conn, 3, 10, 20).unwrap();
        assert_eq!(
            c,
            Carried {
                value_links: 1,
                object_links: 1
            }
        );
        // Carrying again adds no second parameter link.
        assert_eq!(carry_merged_links(&conn, 3, 10, 20).unwrap().object_links, 0);
        assert_eq!(
            n(&conn, "SELECT count(*) FROM value_metadata_link WHERE parameter_definition_id = 20"),
            1
        );
    }

    #[test]
    fn a_value_delete_closes_links_and_skips_a_link_free_file() {
        let dir = tempfile::tempdir().unwrap();
        let conn = fixture(dir.path());
        assert_eq!(close_value_links(&conn, 3, &[(1, 10, 1), (9, 9, 9)]).unwrap(), 1);
        conn.execute_batch("DELETE FROM value_metadata_link").unwrap();
        assert!(!has_value_links(&conn).unwrap());
        assert_eq!(close_value_links(&conn, 3, &[(2, 10, 1)]).unwrap(), 0);
    }
}
