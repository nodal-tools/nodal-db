//! The shadowing guard (engine rule 8, "A file must not shadow a
//! built-in"; `conn::guard_builtins`): DuckDB resolves an unqualified
//! function name in a file's `main` schema before the built-ins, on every
//! later connection to the file, so a persisted `CREATE MACRO lower(x) AS …`
//! would silently replace the name fold (and `max`, `nextval`, `range`, …
//! the same way). Every core open of such a file is refused with
//! `DbError::ShadowedBuiltin` before any core SQL runs.
//!
//! - the refusal on every path (write, read, compaction, extract, init) and
//!   after a clean check was cached (the file changed since);
//! - which definitions count: scalar and table macros, any spelling, `OR
//!   REPLACE` with another arity; not other schemas, not a table macro named
//!   like a scalar built-in, not a non-built-in name;
//! - that the shadow is real: the same file read with plain DuckDB loses a
//!   name (the corruption the guard prevents);
//! - a normal file is unaffected, and dropping the macro lifts the refusal.
//!
//! The engine fold check is covered in its own binary
//! (`tests/shadowed_builtin_engine_check.rs`): it runs once per process.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{head_commit, read_parameter, write_parameter};
use nodal_db_core::compaction::compact;
use nodal_db_core::error::DbError;
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("shadow.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("schema");
    (dir, db)
}

fn scalar(entities: &[&str], v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vec![v; entities.len()])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, entities: &[&str], v: f64) -> Result<(), DbError> {
    write_parameter(
        db,
        "unit",
        "capacity",
        "Base",
        scalar(entities, v),
        &[],
        WriteOptions::default(),
        "t",
        "m",
    )
    .map(|_| ())
}

/// Direct SQL on the file, as a user outside the core would run it.
fn direct_sql(db: &Path, sql: &str) {
    let conn = Connection::open(db).expect("direct open");
    conn.execute_batch(sql).expect("direct sql");
    conn.close().expect("close");
}

#[track_caller]
fn assert_shadowed(r: Result<impl std::fmt::Debug, DbError>, name: &str, kind: &str) {
    match r {
        Err(DbError::ShadowedBuiltin {
            name: n,
            schema,
            kind: k,
        }) => {
            assert_eq!((n.as_str(), k.as_str()), (name, kind));
            assert_eq!(schema, "shadow.main");
        }
        other => panic!("expected ShadowedBuiltin({name}), got {other:?}"),
    }
}

#[test]
fn a_persisted_lower_macro_is_refused_on_every_path() {
    let (dir, db) = fresh_db();
    write(&db, &["Foo"], 1.0).expect("clean write");
    // A clean check is now cached for this file; the macro changes the file.
    assert!(head_commit(&db).expect("clean read") > 0);
    direct_sql(&db, "CREATE MACRO lower(x) AS concat('h:', x);");

    let err = write(&db, &["foo"], 2.0).expect_err("write refused");
    let text = err.to_string();
    assert!(
        text.contains("shadow.main defines a macro `lower`")
            && text.contains("DROP MACRO shadow.main.lower"),
        "{text}"
    );
    assert_shadowed(write(&db, &["foo"], 2.0), "lower", "macro");
    assert_shadowed(head_commit(&db), "lower", "macro");
    assert_shadowed(
        read_parameter(&db, "unit", "capacity", "Base"),
        "lower",
        "macro",
    );
    assert_shadowed(compact(&db), "lower", "macro");
    let opts = ExtractOpts {
        user: "t",
        ..Default::default()
    };
    assert_shadowed(
        extract(&db, &dir.path().join("x.duckdb"), &opts),
        "lower",
        "macro",
    );
    assert!(!dir.path().join("x.duckdb").exists());

    // The shadow is real: plain DuckDB on the file now misses the stored
    // name through `cur_name`, and its unique index would let a duplicate in.
    let conn = Connection::open(&db).unwrap();
    let hits: i64 = conn
        .query_row(
            "SELECT count(*) FROM entity WHERE cur_name = lower('FOO')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hits, 0, "the macro replaced lower() in the file's queries");
    conn.execute_batch("DROP MACRO lower;").unwrap();
    conn.close().unwrap();

    // Dropped: the file is accepted again, and the name resolves as before.
    write(&db, &["FOO"], 3.0).expect("write after the drop");
    let batch = read_parameter(&db, "unit", "capacity", "Base").expect("read");
    assert_eq!(batch.num_rows(), 1, "FOO is Foo");
}

#[test]
fn every_spelling_and_family_of_a_shadow_counts() {
    for (sql, name, kind) in [
        ("CREATE MACRO \"LoWeR\"(x) AS x;", "LoWeR", "macro"),
        // Another arity still captures every call of the name.
        (
            "CREATE OR REPLACE MACRO main.lower(x, y) AS x;",
            "lower",
            "macro",
        ),
        ("CREATE MACRO max(x) AS 0;", "max", "macro"),
        ("CREATE MACRO nextval(x) AS 7;", "nextval", "macro"),
        (
            "CREATE MACRO range(x) AS TABLE SELECT 42 AS r;",
            "range",
            "table macro",
        ),
        (
            "CREATE MACRO duckdb_functions() AS TABLE SELECT 1 AS x;",
            "duckdb_functions",
            "table macro",
        ),
    ] {
        let (_dir, db) = fresh_db();
        direct_sql(&db, sql);
        assert_shadowed(head_commit(&db), name, kind);
        assert_shadowed(write(&db, &["a"], 1.0), name, kind);
        if kind == "table macro" {
            let r = format!("DROP MACRO TABLE {name};");
            direct_sql(&db, &r);
        } else {
            direct_sql(&db, &format!("DROP MACRO \"{name}\";"));
        }
        write(&db, &["a"], 1.0).expect("accepted after the drop");
    }
}

#[test]
fn definitions_that_do_not_shadow_are_allowed() {
    let (_dir, db) = fresh_db();
    direct_sql(
        &db,
        "CREATE SCHEMA s; CREATE MACRO s.lower(x) AS 'x'; \
         CREATE MACRO lower(x) AS TABLE SELECT 1 AS v; \
         CREATE MACRO my_fold(x) AS x;",
    );
    write(&db, &["Foo"], 1.0).expect("write");
    write(&db, &["FOO"], 2.0).expect("write");
    assert_eq!(
        read_parameter(&db, "unit", "capacity", "Base")
            .expect("read")
            .num_rows(),
        1
    );
    compact(&db).expect("compact");
}

#[test]
fn init_refuses_a_shadowed_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("shadow.duckdb");
    direct_sql(&db, "CREATE MACRO lower(x) AS x;");
    assert_shadowed(
        init_schema_with_default(&db, Some("Base")),
        "lower",
        "macro",
    );
}

#[test]
fn a_normal_file_is_unaffected_and_the_cached_check_is_cheap() {
    let (_dir, db) = fresh_db();
    write(&db, &["a"], 1.0).expect("write");
    head_commit(&db).expect("read");
    // Cached: the file did not change, so these opens only stat it.
    let t = std::time::Instant::now();
    for _ in 0..20 {
        head_commit(&db).expect("read");
    }
    let cached = t.elapsed().as_secs_f64() * 50.0;
    // The listing an uncached check runs (`conn::check_no_shadowed_builtins`).
    let conn = Connection::open(&db).unwrap();
    let t = std::time::Instant::now();
    for _ in 0..20 {
        let mut stmt = conn
            .prepare(
                "SELECT database_name, schema_name, function_name, function_type, internal \
                 FROM system.main.duckdb_functions()",
            )
            .unwrap();
        let n = stmt
            .query_map([], |r| r.get::<_, String>(2))
            .unwrap()
            .count();
        assert!(n > 1000);
    }
    let listing = t.elapsed().as_secs_f64() * 50.0;
    eprintln!(
        "head_commit, guard cached: {cached:.2} ms per call; the guard's listing: {listing:.2} ms"
    );
}
