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
//! - a normal file is unaffected, and dropping the macro lifts the refusal
//!   (with the DROP statement the message gives);
//! - the cache: a file is listed once until it changes (a save, or another
//!   client's change, a WAL-only change included);
//! - what dropping cannot repair: a duplicate written under the macro, which
//!   compaction refuses.
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
use nodal_db_core::conn::builtin_guard_listings;
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
            assert_eq!(schema, "main");
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
        text.contains("defines a macro `lower` in its `main` schema")
            && text.contains("DROP MACRO main.\"lower\"")
            && text.contains("compact"),
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

/// The DROP statement the refusal message suggests, run as given.
fn run_hint(db: &Path, err: &DbError) {
    let text = err.to_string();
    let start = text.find("(DROP MACRO").expect("a DROP hint") + 1;
    let end = start + text[start..].find(')').expect("hint end");
    direct_sql(db, &format!("{};", &text[start..end]));
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
        // Operators other than the comparisons are shadowed too (`1 + 1`).
        ("CREATE MACRO \"+\"(a, b) AS 0;", "+", "macro"),
        ("CREATE MACRO \"~~\"(a, b) AS true;", "~~", "macro"),
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
        let err = write(&db, &["a"], 1.0).expect_err("refused");
        assert_shadowed(Err::<(), _>(err), name, kind);
        // The message's DROP statement works as given.
        run_hint(&db, &head_commit(&db).expect_err("refused"));
        write(&db, &["a"], 1.0).expect("accepted after the drop");
    }
}

/// The hint names the schema only (`main.` of whatever catalog the user's
/// connection opens the file as), so file names DuckDB would rename
/// (`temp.duckdb` is catalog `temp_db`) or that need quoting do not matter.
#[test]
fn the_drop_hint_works_for_awkward_file_names() {
    for file in ["temp.duckdb", "my-db.duckdb"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join(file);
        init_schema_with_default(&db, Some("Base")).expect("schema");
        direct_sql(&db, "CREATE MACRO lower(x) AS x;");
        let err = head_commit(&db).expect_err("refused");
        assert!(matches!(&err, DbError::ShadowedBuiltin { schema, .. } if schema == "main"));
        run_hint(&db, &err);
        head_commit(&db).expect("accepted after the drop");
    }
}

#[test]
fn definitions_that_do_not_shadow_are_allowed() {
    let (_dir, db) = fresh_db();
    direct_sql(
        &db,
        "CREATE SCHEMA s; CREATE MACRO s.lower(x) AS 'x'; \
         CREATE MACRO lower(x) AS TABLE SELECT 1 AS v; \
         CREATE MACRO my_fold(x) AS x; \
         CREATE MACRO \"=\"(a, b) AS 0; CREATE MACRO \"<\"(a, b) AS 0; \
         CREATE MACRO platform() AS 'p';",
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
fn a_normal_file_is_listed_once_until_it_changes() {
    let (_dir, db) = fresh_db();
    let listings = || builtin_guard_listings(&db);
    head_commit(&db).expect("read");
    let base = listings();
    for _ in 0..5 {
        head_commit(&db).expect("read");
    }
    assert_eq!(listings(), base, "an unchanged file is not listed again");

    // A save changes the file: the open after it lists once, then none.
    write(&db, &["a"], 1.0).expect("write");
    let after_save = listings();
    head_commit(&db).expect("read");
    assert_eq!(listings(), after_save + 1, "one listing after a save");
    read_parameter(&db, "unit", "capacity", "Base").expect("read");
    head_commit(&db).expect("read");
    assert_eq!(listings(), after_save + 1, "and none after that");

    // Another client's change is listed (and a macro caught) on the next open.
    direct_sql(&db, "CREATE MACRO lower(x) AS x;");
    assert_shadowed(head_commit(&db), "lower", "macro");
    assert_eq!(listings(), after_save + 2);
}

/// A change that only reaches the WAL (the other client skipped its
/// checkpoint) leaves the database file's stat as it was: the WAL's part of
/// the stamp is what triggers the listing.
#[test]
fn a_change_only_in_the_wal_is_caught() {
    let (_dir, db) = fresh_db();
    head_commit(&db).expect("read");
    let meta = |p: &Path| {
        let m = std::fs::metadata(p).unwrap();
        (m.len(), m.modified().unwrap())
    };
    let before = meta(&db);
    direct_sql(
        &db,
        "PRAGMA disable_checkpoint_on_shutdown; CREATE MACRO lower(x) AS x;",
    );
    let mut wal = db.as_os_str().to_owned();
    wal.push(".wal");
    assert!(Path::new(&wal).exists(), "the macro is in the WAL only");
    assert_eq!(meta(&db), before, "the database file is untouched");
    assert_shadowed(head_commit(&db), "lower", "macro");
}

/// What the guard cannot undo: a duplicate written by another client while
/// the macro existed stays after the drop (DuckDB tracks no dependency, so
/// the drop succeeds), with index keys that no longer match the column.
/// Compaction rebuilds every index from the rows and fails loudly on it.
#[test]
fn a_duplicate_written_under_the_macro_fails_compaction_loudly() {
    let (_dir, db) = fresh_db();
    write(&db, &["a"], 1.0).expect("write");
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE MACRO lower(x) AS x; \
         INSERT INTO entity_class (name, valid_from_commit) VALUES ('UNIT', 1); \
         DROP MACRO lower;",
    )
    .unwrap();
    let live: i64 = conn
        .query_row(
            "SELECT count(*) FROM entity_class WHERE cur_name = 'unit'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    assert_eq!(live, 2, "two live rows of one name after the drop");

    // The guard accepts the file again: the macro is gone.
    head_commit(&db).expect("accepted after the drop");
    let err = compact(&db).expect_err("compaction must not pass a duplicate");
    let text = err.to_string();
    assert!(
        text.contains("UNIQUE constraint violation") && text.contains("duplicate key \"unit\""),
        "{text}"
    );
}
