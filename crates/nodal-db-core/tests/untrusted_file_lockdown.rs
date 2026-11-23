//! Engine rule 12, "A file's SQL runs without file, network or extension
//! access" (`conn::core_config`): a database file from someone else carries
//! its own views and macros, and the core calls some of them by name
//! (`nodal_read_surface()` on every read and write, the read macros, a base
//! table the file replaced with a view during compaction and extract). Every
//! core connection is therefore opened with DuckDB's external access,
//! extension autoload/autoinstall, persistent secrets and community
//! extensions off, and its configuration locked.
//!
//! The PoCs of the pre-release security review, one per core path: a file
//! whose `nodal_read_surface()` reads a local file through `read_text`, a
//! base table replaced by a view that does, and a macro that would make
//! DuckDB download an extension. Each must fail (or, where the core
//! deliberately swallows the version check, run without the file read) and
//! never return the secret. `Access::Trusted` is the explicit opt-in.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{read_parameter, read_surface_status, write_parameter};
use nodal_db_core::compaction::compact;
use nodal_db_core::conn::{with_read, with_read_retrying_as, with_write_txn, Access};
use nodal_db_core::error::DbError;
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

const SECRET: &str = "SECRET-123";

struct Fixture {
    dir: tempfile::TempDir,
    db: PathBuf,
    secret: PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("evil.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("schema");
    write(&db, 1.0).expect("a clean write");
    let secret = dir.path().join("secret.txt");
    std::fs::write(&secret, SECRET).unwrap();
    Fixture { dir, db, secret }
}

fn write(db: &Path, v: f64) -> Result<(), DbError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["u1"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parameter(
        db,
        "unit",
        "capacity",
        "Base",
        batch,
        &[],
        "t",
        "m",
        WriteOptions::default(),
    )
    .map(|_| ())
}

/// SQL on the file as its author would run it: plain DuckDB, full access.
fn author_sql(db: &Path, sql: &str) {
    let conn = Connection::open(db).expect("direct open");
    conn.execute_batch(sql).expect("author sql");
    conn.close().expect("close");
}

fn lit(p: &Path) -> String {
    format!("'{}'", p.display().to_string().replace('\'', "''"))
}

/// The review's PoC: the version marker reads a local file.
fn plant_marker_reading(f: &Fixture) {
    author_sql(
        &f.db,
        &format!(
            "CREATE OR REPLACE MACRO nodal_read_surface() AS TABLE \
             SELECT 1::INTEGER AS contract_major, 7::INTEGER AS revision, \
                    (SELECT content FROM read_text({})) AS ddl_hash",
            lit(&f.secret)
        ),
    );
}

/// A base table replaced by a view that reads a local file (compaction and
/// extract copy `nodal_rebuild_source.<table>`).
fn plant_table_view_reading(f: &Fixture) {
    author_sql(
        &f.db,
        &format!(
            "CREATE TABLE em_rows AS SELECT * FROM erasure_marker; \
             DROP TABLE erasure_marker; \
             CREATE VIEW erasure_marker AS \
             SELECT t.* FROM em_rows t, read_text({}) s WHERE length(s.content) > 0",
            lit(&f.secret)
        ),
    );
}

#[track_caller]
fn assert_refused<T: std::fmt::Debug>(r: Result<T, DbError>, what: &str) {
    match r {
        Err(e) => {
            let text = e.to_string();
            assert!(
                text.contains("Permission Error") && text.contains("disabled by configuration"),
                "{what}: expected DuckDB's Permission Error, got: {text}"
            );
            assert!(!text.contains(SECRET), "{what}: the secret leaked: {text}");
        }
        Ok(v) => panic!("{what}: expected a refusal, got {v:?}"),
    }
}

#[test]
fn a_marker_that_reads_a_file_fails_on_reads_and_status() {
    let f = fixture();
    plant_marker_reading(&f);
    assert_refused(read_parameter(&f.db, "unit", "capacity", "Base"), "read");
    assert_refused(read_surface_status(&f.db), "status");
    // A caller's own query on the core's read connection (Python db.query).
    assert_refused(
        with_read(&f.db, |c| {
            Ok(
                c.query_row("SELECT ddl_hash FROM nodal_read_surface()", [], |r| {
                    r.get::<_, String>(0)
                })?,
            )
        }),
        "with_read",
    );
}

#[test]
fn the_write_connection_is_locked_down_too() {
    let f = fixture();
    plant_marker_reading(&f);
    // The write path's own version check swallows the error by design
    // (`refresh_before_write` never fails a write); the write goes through
    // and the file was not read.
    write(&f.db, 2.0).expect("the write itself goes through");
    // On the write connection itself the macro fails.
    assert_refused(
        with_write_txn(&f.db, "t", "m", |c, _| {
            Ok(
                c.query_row("SELECT ddl_hash FROM nodal_read_surface()", [], |r| {
                    r.get::<_, String>(0)
                })?,
            )
        }),
        "write connection",
    );
}

#[test]
fn compaction_and_extract_refuse_a_table_view_that_reads_a_file() {
    let f = fixture();
    plant_table_view_reading(&f);
    assert_refused(compact(&f.db), "compaction");
    let dest = f.dir.path().join("x.duckdb");
    let opts = ExtractOpts {
        user: "t",
        ..Default::default()
    };
    assert_refused(extract(&f.db, &dest, &opts), "extract");
    assert!(!dest.exists(), "no extract was published");
    let temps: Vec<_> = std::fs::read_dir(f.dir.path())
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.ends_with(".tmp") || n.ends_with(".tmp.wal"))
        .collect();
    assert!(temps.is_empty(), "temps left behind: {temps:?}");
}

#[test]
fn compaction_and_extract_of_a_marker_that_reads_a_file_never_read_it() {
    // The rebuild reads the source's marker only to warn about a downgrade
    // (an error there is ignored), then re-creates every view and macro from
    // this build's DDL: the result carries no trace of the planted macro.
    let f = fixture();
    plant_marker_reading(&f);
    let dest = f.dir.path().join("x.duckdb");
    let opts = ExtractOpts {
        user: "t",
        ..Default::default()
    };
    match extract(&f.db, &dest, &opts) {
        Ok(_) => {
            let status = read_surface_status(&dest).expect("the extract is clean");
            assert!(!format!("{status:?}").contains(SECRET));
        }
        Err(e) => assert_refused::<()>(Err(e), "extract"),
    }
    match compact(&f.db) {
        Ok(_) => {
            let status = read_surface_status(&f.db).expect("the compacted file is clean");
            assert!(!format!("{status:?}").contains(SECRET));
        }
        Err(e) => assert_refused::<()>(Err(e), "compaction"),
    }
}

#[test]
fn a_core_connection_never_installs_or_loads_an_extension() {
    // The review's PoC was a macro whose body calls `read_parquet`, which
    // made the core try to download the parquet extension. Planting that
    // macro needs a DuckDB that has parquet (the author's Python one); the
    // bundled engine here has none, so the same SQL runs directly on the
    // core's connections, where a file's macro body would run.
    let f = fixture();
    let parquet = f.dir.path().join("p.parquet");
    std::fs::write(&parquet, b"not really parquet").unwrap();
    let q = format!("SELECT count(*) FROM read_parquet({})", lit(&parquet));
    for (what, r) in [
        ("read", with_read(&f.db, |c| Ok(c.execute_batch(&q)?))),
        (
            "write",
            with_write_txn(&f.db, "t", "m", |c, _| Ok(c.execute_batch(&q)?)),
        ),
        (
            "start_ui_server",
            with_read(&f.db, |c| Ok(c.execute_batch("SELECT start_ui_server()")?)),
        ),
        (
            "INSTALL",
            with_read(&f.db, |c| Ok(c.execute_batch("INSTALL httpfs")?)),
        ),
    ] {
        let e = r.expect_err(what).to_string();
        assert!(
            !e.contains("Failed to download")
                && !e.contains("Autoloading Error")
                && !e.contains("automatically install")
                && !e.contains("extensions.duckdb.org"),
            "{what}: an extension download was attempted: {e}"
        );
    }
}

#[test]
fn a_core_connection_cannot_unlock_itself() {
    let f = fixture();
    for sql in [
        "SET enable_external_access = true",
        "SET lock_configuration = false",
        "SET autoinstall_known_extensions = true",
        "SET allowed_paths = ['/']",
    ] {
        let e = with_read(&f.db, |c| Ok(c.execute_batch(sql)?))
            .expect_err(sql)
            .to_string();
        assert!(e.contains("configuration has been locked"), "{sql}: {e}");
    }
    let e = with_write_txn(&f.db, "t", "m", |c, _| {
        Ok(c.execute_batch("SET enable_external_access = true")?)
    })
    .expect_err("write connection")
    .to_string();
    assert!(e.contains("configuration has been locked"), "{e}");
}

#[test]
fn a_trusted_query_has_full_access_and_a_locked_one_none() {
    let f = fixture();
    let q = format!("SELECT content FROM read_text({})", lit(&f.secret));
    let read = |access| {
        with_read_retrying_as(&f.db, access, |c| {
            Ok(c.query_row(&q, [], |r| r.get::<_, String>(0))?)
        })
    };
    assert_refused(read(Access::Locked), "locked query");
    assert_eq!(read(Access::Trusted).expect("trusted query"), SECRET);
    // Trusted is per call: the next locked call is locked again.
    assert_refused(read(Access::Locked), "locked query after a trusted one");
}

#[test]
fn a_clean_file_still_reads_writes_compacts_and_extracts() {
    let f = fixture();
    write(&f.db, 3.0).expect("write");
    read_parameter(&f.db, "unit", "capacity", "Base").expect("read");
    compact(&f.db).expect("compaction");
    let dest = f.dir.path().join("x.duckdb");
    let opts = ExtractOpts {
        user: "t",
        ..Default::default()
    };
    extract(&f.db, &dest, &opts).expect("extract");
    read_parameter(&dest, "unit", "capacity", "Base").expect("read the extract");
}
