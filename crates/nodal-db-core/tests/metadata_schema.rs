//! Schema guards of the metadata tables (`schema/001_meta.sql`):
//!
//! - `property::CORE_KEYS` equals the `database_property` CHECK list (the
//!   DDL is the one classifier of core keys; a new core key edits both, with
//!   a `SCHEMA_VERSION` bump);
//! - the value-link index key order is a size decision (engine rule 4):
//!   `uq_value_metadata_link (cur_tk, cur_t, cur_r, cur_a, cur_p, cur_e)`
//!   (few targets and roles first, the entity last) measured at 10^5 links
//!   against the reverse order. The failure is silent (a bigger file), so
//!   this test is the tripwire.

use std::collections::BTreeSet;
use std::path::Path;

use duckdb::Connection;
use nodal_db_core::property::{owner_of, Owner, CORE_KEYS};
use nodal_db_core::schema_init::init_schema;

fn query_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

#[test]
fn core_keys_equal_the_ddl_check_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("k.duckdb");
    init_schema(&db).unwrap();
    let conn = Connection::open(&db).unwrap();
    let exprs: Vec<String> = conn
        .prepare(
            "SELECT constraint_text FROM duckdb_constraints() \
             WHERE table_name = 'database_property' AND constraint_type = 'CHECK'",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let list = exprs
        .iter()
        .find(|e| e.contains("schema_version"))
        .unwrap_or_else(|| panic!("no core-key CHECK in {exprs:?}"));
    let inside = &list[list.find("IN (").expect("an IN list") + 4..];
    let inside = &inside[..inside.find(')').unwrap()];
    let ddl: BTreeSet<String> = inside
        .split(',')
        .map(|s| s.trim().trim_matches('\'').to_string())
        .collect();
    let core: BTreeSet<String> = CORE_KEYS.iter().map(|s| s.to_string()).collect();
    assert_eq!(core, ddl, "property::CORE_KEYS must equal the DDL CHECK list");
    for k in CORE_KEYS {
        assert_eq!(owner_of(k), Owner::Core, "{k}");
    }
    assert_eq!(owner_of("Schema_Version"), Owner::Core, "any case");
    assert_eq!(owner_of("flextool:frozen"), Owner::Tool);
    assert_eq!(owner_of("title"), Owner::User);
    assert_eq!(owner_of(":x"), Owner::User, "the DDL classifies ':x' as user");
    conn.close().unwrap();
}

const BUNDLED: &str = "[cur_tk, cur_t, cur_r, cur_a, cur_p, cur_e]";
const REVERSE: &str = "cur_e, cur_p, cur_a, cur_r, cur_t, cur_tk";

/// Bytes freed by dropping `uq_value_metadata_link` on a copy of `db`.
fn index_bytes(db: &Path) -> i64 {
    let copy = db.with_extension("measure.duckdb");
    std::fs::copy(db, &copy).unwrap();
    let conn = Connection::open(&copy).unwrap();
    conn.execute_batch("CHECKPOINT").unwrap();
    let used = "SELECT used_blocks FROM pragma_database_size()";
    let before = query_i64(&conn, used);
    let block = query_i64(&conn, "SELECT block_size FROM pragma_database_size()");
    conn.execute_batch("DROP INDEX uq_value_metadata_link; CHECKPOINT")
        .unwrap();
    let freed = (before - query_i64(&conn, used)) * block;
    conn.close().unwrap();
    std::fs::remove_file(&copy).unwrap();
    freed
}

/// 10^5 live value links: 5 sources, 2 roles (and none), 2 alternatives, 10
/// parameters, 2 500 entities: the realistic shape (few targets, many values).
fn build(db: &Path, order: Option<&str>) {
    init_schema(db).unwrap();
    let conn = Connection::open(db).unwrap();
    if let Some(cols) = order {
        conn.execute_batch(&format!(
            "DROP INDEX uq_value_metadata_link; \
             CREATE UNIQUE INDEX uq_value_metadata_link ON value_metadata_link ({cols});"
        ))
        .unwrap();
    }
    let c = query_i64(
        &conn,
        "INSERT INTO \"commit\" (user, message) VALUES ('t', 'links') RETURNING id",
    );
    conn.execute_batch(&format!(
        "INSERT INTO value_metadata_link (entity_id, parameter_definition_id, alternative_id, \
           target_kind, target_id, role_id, valid_from_commit) \
         SELECT 1001 + (i // 20), 101 + (i % 10), 11 + ((i // 10) % 2), 'source', \
                1 + (i % 5), CASE i % 3 WHEN 0 THEN NULL ELSE i % 3 END, {c} \
         FROM range(100000) t(i) ORDER BY i; CHECKPOINT;"
    ))
    .unwrap();
    assert_eq!(
        query_i64(&conn, "SELECT count(*) FROM value_metadata_link"),
        100_000
    );
    conn.close().unwrap();
}

#[test]
fn value_link_index_key_order_is_smaller_than_the_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let bundled = dir.path().join("bundled.duckdb");
    build(&bundled, None);
    {
        let conn = Connection::open(&bundled).unwrap();
        let e: String = conn
            .query_row(
                "SELECT expressions FROM duckdb_indexes() \
                 WHERE index_name = 'uq_value_metadata_link'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(e, BUNDLED);
        conn.close().unwrap();
    }
    let reverse = dir.path().join("reverse.duckdb");
    build(&reverse, Some(REVERSE));
    let (b, r) = (index_bytes(&bundled), index_bytes(&reverse));
    eprintln!(
        "uq_value_metadata_link at 1e5 links: bundled {b} B ({:.1} B/link), reverse {r} B \
         ({:.1} B/link)",
        b as f64 / 1e5,
        r as f64 / 1e5
    );
    assert!(
        b < r,
        "the bundled key order must be smaller than the reverse (bundled {b} B, reverse {r} B)"
    );
}
