//! The read-surface contract and stale read surfaces in old files
//! (`docs/dev/read_surface.md`; `specs/read_surface_contract_PLAN.md`).
//!
//! - the gates: the DDL hash in `130_read_surface.sql`, the `@surface` tag
//!   census, the committed listing's header, the guarded "missing object"
//!   error text;
//! - the refresh before a write: an old file (its views and macros from the
//!   frozen DDL in `tests/fixtures/read_surface_v0/`, the `070`-`120` files
//!   at `f2bb4a5^`, whose tables equal this build's) gets this build's
//!   surface with its data untouched; user views survive; a user table with
//!   a reserved name blocks the refresh with a warning while the write goes
//!   through; a newer file is never touched; dev drift refreshes;
//! - reads of a stale file: the core's named reads overlay their closure and
//!   return what they return after a refresh, generic reads use the file's
//!   surface and map a missing object to `ReadSurfaceStale` (or retry with
//!   the full overlay), nothing is written, one notice per file;
//! - extract of a stale (even tampered) source keeps its full self-check;
//!   compaction of a newer file warns.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMicrosecondArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::display::array_value_to_string;
use duckdb::Connection;

use nodal_db_core::api::{self, read_parameter, read_values, write_parameter};
use nodal_db_core::compaction;
use nodal_db_core::conn::{with_read, with_read_retrying};
use nodal_db_core::error::DbError;
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::read_surface::{
    self, build_version, is_missing_object_error, surface, SurfaceStatus,
};
use nodal_db_core::schema_init::{init_schema_with_default, EMBEDDED_SCHEMA};
use nodal_db_core::types::{WriteOptions, WriteReport};

const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;

// --- fixtures ----------------------------------------------------------------

fn old_ddl_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/read_surface_v0")
}

fn series(ents: &[&str], n: i64, k: f64) -> RecordBatch {
    let (mut e, mut t, mut v) = (vec![], vec![], vec![]);
    for (i, name) in ents.iter().enumerate() {
        for h in 0..n {
            e.push(name.to_string());
            t.push(T0_US + h * HOUR_US);
            v.push(k * (i as f64 * 100.0 + h as f64));
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(e)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(t)),
            Arc::new(Float64Array::from(v)),
        ],
    )
    .unwrap()
}

fn scalars(ents: &[&str], k: f64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(
                ents.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                (0..ents.len()).map(|i| k + i as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn write(db: &Path, class: &str, param: &str, alt: &str, b: RecordBatch) -> WriteReport {
    write_parameter(
        db,
        class,
        param,
        alt,
        b,
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .expect("write")
}

/// A core-built file with data in every tier the readers below touch.
fn data_file(dir: &Path, name: &str) -> PathBuf {
    let db = dir.join(name);
    init_schema_with_default(&db, Some("Base")).expect("init");
    write(
        &db,
        "Node",
        "demand",
        "Base",
        series(&["n1", "n2", "n3"], 48, 1.5),
    );
    write(
        &db,
        "Node",
        "demand",
        "High",
        series(&["n1", "n2"], 48, 2.5),
    );
    write(
        &db,
        "Node",
        "capacity",
        "Base",
        scalars(&["n1", "n2", "n3"], 10.0),
    );
    db
}

/// Raw read-write connection (outside the core: no refresh hook).
fn raw(db: &Path) -> Connection {
    Connection::open(db).expect("raw open")
}

/// Replace the file's whole read surface with the frozen old DDL (no
/// version marker): the state of a file written by a build before
/// versioning. Every current surface object is dropped first.
fn age(db: &Path) {
    let c = raw(db);
    let mut drops = String::new();
    for o in &surface().objects {
        let what = match o.kind {
            read_surface::ObjectKind::View => "VIEW",
            read_surface::ObjectKind::TableMacro => "MACRO TABLE",
            read_surface::ObjectKind::ScalarMacro => "MACRO",
        };
        drops.push_str(&format!("DROP {what} IF EXISTS \"{}\";\n", o.name));
    }
    c.execute_batch("BEGIN").unwrap();
    c.execute_batch(&drops).unwrap();
    let mut files: Vec<PathBuf> = fs::read_dir(old_ddl_dir())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    for f in files {
        c.execute_batch(&fs::read_to_string(f).unwrap()).unwrap();
    }
    c.execute_batch("COMMIT").unwrap();
    c.close().unwrap();
}

/// Replace the file's version marker (a newer or drifted surface).
fn set_marker(db: &Path, major: i32, revision: i32, hash: &str) {
    let c = raw(db);
    c.execute_batch(&format!(
        "DROP MACRO TABLE IF EXISTS nodal_read_surface;
         CREATE MACRO nodal_read_surface() AS TABLE
         SELECT CAST({major} AS INTEGER) AS contract_major,
                CAST({revision} AS INTEGER) AS revision,
                CAST('{hash}' AS VARCHAR) AS ddl_hash;"
    ))
    .unwrap();
    c.close().unwrap();
}

fn status(db: &Path) -> SurfaceStatus {
    api::read_surface_status(db).expect("status").check.status
}

/// Every view of the file with its SQL, and every macro name, sorted.
/// (Macro bodies are left out; view SQL is compared as a bag of words.)
fn catalog_sql(db: &Path) -> Vec<(String, String)> {
    let c = raw(db);
    let mut out: Vec<(String, String)> = c
        .prepare(
            "SELECT view_name, sql FROM duckdb_views() WHERE NOT internal AND NOT temporary \
             UNION ALL \
             SELECT DISTINCT function_name, function_type FROM duckdb_functions() \
             WHERE NOT internal AND function_type IN ('macro', 'table_macro') \
               AND database_name = current_database()",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .map(|r| r.map(|(n, sql)| (n, token_bag(&sql))))
        .collect::<Result<_, _>>()
        .unwrap();
    c.close().unwrap();
    out.sort();
    out
}

/// The words of a statement, sorted: DuckDB renders some lists (an
/// `EXCLUDE (…)`) in no fixed order.
fn token_bag(sql: &str) -> String {
    let mut w: Vec<&str> = sql
        .split(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
        .filter(|t| !t.is_empty())
        .collect();
    w.sort_unstable();
    w.join(" ")
}

/// Row count and order-independent content hash of every base table.
fn table_digests(db: &Path) -> Vec<(String, i64, String)> {
    let c = raw(db);
    let tables: Vec<String> = c
        .prepare(
            "SELECT table_name FROM duckdb_tables() WHERE NOT internal \
             AND database_name = current_database() ORDER BY 1",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let out = tables
        .into_iter()
        .map(|t| {
            let (n, h): (i64, String) = c
                .query_row(
                    &format!(
                        "SELECT count(*), CAST(COALESCE(bit_xor(hash(t)), 0) AS VARCHAR) \
                         FROM \"{t}\" t"
                    ),
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            (t, n, h)
        })
        .collect();
    c.close().unwrap();
    out
}

/// A record batch as sorted rendered rows with its column names.
fn rows(b: &RecordBatch) -> (Vec<String>, Vec<String>) {
    let names = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut out: Vec<String> = (0..b.num_rows())
        .map(|r| {
            b.columns()
                .iter()
                .map(|c| array_value_to_string(c, r).unwrap())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    out.sort();
    (names, out)
}

fn temp_object_count(c: &Connection) -> i64 {
    c.query_row(
        "SELECT (SELECT count(*) FROM duckdb_views() WHERE temporary AND NOT internal) \
              + (SELECT count(*) FROM (SELECT DISTINCT function_name FROM duckdb_functions() \
                 WHERE database_name = 'temp' AND NOT internal \
                   AND function_type IN ('macro', 'table_macro')))",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

/// The notices for one file this thread produced (drained).
fn notices_for(db: &Path) -> usize {
    read_surface::take_notices()
        .iter()
        .filter(|n| n.path == db)
        .count()
}

// --- the gates ---------------------------------------------------------------

/// The "missing object" error text the core matches (one helper) is pinned
/// here: a DuckDB upgrade that rewords it fails this test instead of making
/// every file "current" (or every read error "stale").
#[test]
fn missing_object_error_text_is_pinned() {
    let c = Connection::open_in_memory().unwrap();
    for sql in [
        "SELECT * FROM nope_table_macro(1)",
        "SELECT * FROM nope_view",
        "SELECT nope_scalar(1)",
    ] {
        let e = c
            .prepare(sql)
            .and_then(|mut s| s.query([]).map(|_| ()))
            .unwrap_err();
        assert!(is_missing_object_error(&e), "{sql}: {e}");
    }
    c.execute_batch("CREATE MACRO m(a) AS TABLE SELECT a AS x")
        .unwrap();
    for sql in ["SELEC 1", "SELECT * FROM m(1, 2)", "SELECT 'a' + 1"] {
        let e = c
            .prepare(sql)
            .and_then(|mut s| s.query([]).map(|_| ()))
            .unwrap_err();
        assert!(!is_missing_object_error(&e), "{sql}: {e}");
    }
}

/// The DDL hash gate: the normalized hash of `070`-`120` equals the literal
/// in `130_read_surface.sql`. A view or macro change without a revision bump
/// fails here (comment-only and whitespace-only edits do not).
#[test]
fn ddl_hash_matches_the_version_marker() {
    let computed = read_surface::build_ddl_hash();
    let v = build_version();
    assert_eq!(
        v.ddl_hash, computed,
        "the read surface changed: bump `revision` in schema/130_read_surface.sql \
         (now {}) and paste this hash: {computed}",
        v.revision
    );
}

/// Every view and macro of `070`-`130` carries exactly one `@surface` class
/// tag directly above its CREATE, the files hold nothing else, and the
/// catalog of a fresh file is exactly this set.
#[test]
fn surface_tag_census() {
    let s = surface();
    assert!(
        s.other_statements.is_empty(),
        "surface files hold a statement that is not CREATE VIEW|MACRO: {:?}",
        s.other_statements
    );
    let untagged: Vec<&str> = s
        .objects
        .iter()
        .filter(|o| {
            !matches!(
                o.class.as_deref(),
                Some("stable" | "mirror" | "advisory" | "internal")
            )
        })
        .map(|o| o.name.as_str())
        .collect();
    assert!(
        untagged.is_empty(),
        "no valid `-- @surface:` tag: {untagged:?}"
    );
    // the classification of the plan (A-D2), plus the version marker (S)
    let count = |c: &str| {
        s.objects
            .iter()
            .filter(|o| o.class.as_deref() == Some(c))
            .count()
    };
    assert_eq!(
        (
            count("stable"),
            count("mirror"),
            count("advisory"),
            count("internal")
        ),
        (28, 62, 27, 9)
    );
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("census.duckdb");
    init_schema_with_default(&db, None).unwrap();
    let mut names: Vec<String> = catalog_sql(&db).into_iter().map(|(n, _)| n).collect();
    names.dedup();
    let mut want: Vec<String> = s.objects.iter().map(|o| o.name.clone()).collect();
    want.sort();
    assert_eq!(
        names, want,
        "the catalog of a fresh file != the surface files' objects"
    );
}

/// The committed listing (`schema/read_surface_contract.tsv`, generated by
/// `scripts/docs/gen_schema.py`) states this build's contract major and
/// lists exactly the stable, mirror and advisory objects. (The column-level
/// staleness is the generator's `--check`, run by `just docs-check`.)
#[test]
fn committed_listing_matches_the_build() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schema/read_surface_contract.tsv");
    let text = fs::read_to_string(path).expect("schema/read_surface_contract.tsv");
    let header = text
        .lines()
        .find(|l| l.starts_with("# nodal-db read-surface contract:"))
        .expect("header line");
    assert!(
        header.contains(&format!(
            "contract_major={} ",
            build_version().contract_major
        )),
        "{header}"
    );
    assert!(
        header.ends_with(&format!("schema_version={}", compaction::SCHEMA_VERSION)),
        "{header}"
    );
    let mut listed: Vec<&str> = text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.starts_with("class\t"))
        .filter_map(|l| l.split('\t').nth(1))
        .collect();
    listed.dedup();
    let mut want: Vec<&str> = surface()
        .objects
        .iter()
        .filter(|o| o.class.as_deref() != Some("internal"))
        .map(|o| o.name.as_str())
        .collect();
    want.sort();
    assert_eq!(listed, want, "regenerate with `just docs-gen`");
}

/// The core's named reads overlay a dozen objects, not the whole surface.
#[test]
fn named_read_closures_are_small() {
    let s = surface();
    for (read, max) in [("read_parameter", 8), ("read_values", 12)] {
        let c = s.closure(&[read]);
        assert!(
            c.len() <= max && c.iter().any(|o| o.name == read),
            "{read}: {:?}",
            c.iter().map(|o| &o.name).collect::<Vec<_>>()
        );
    }
    assert!(s.objects.len() > 120);
}

// --- the refresh before a write ----------------------------------------------

#[test]
fn a_write_refreshes_an_old_file_and_keeps_user_views() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "old.duckdb");
    age(&db);
    assert_eq!(status(&db), SurfaceStatus::Missing);
    // user views over surface objects, a chain of them, created in the old file
    let c = raw(&db);
    c.execute_batch(
        "CREATE VIEW my_demand AS SELECT * FROM read_parameter('Node', 'demand');
         CREATE VIEW my_count AS SELECT count(*) AS n FROM my_demand;",
    )
    .unwrap();
    let before: i64 = c
        .query_row("SELECT n FROM my_count", [], |r| r.get(0))
        .unwrap();
    assert!(c
        .prepare("SELECT * FROM read_value('Node', 'demand', 'n1')")
        .is_err());
    c.close().unwrap();

    // the write that refreshes is pinned to the head it read: the refresh
    // writes no commit row, so the precondition still holds
    let head = api::head_commit(&db).unwrap();
    let r = write_parameter(
        &db,
        "Node",
        "capacity",
        "High",
        scalars(&["n1"], 99.0),
        &[],
        WriteOptions {
            expect_commit: Some(head),
            ..WriteOptions::default()
        },
        "tester",
        "",
    )
    .expect("expect_commit holds across the refresh");
    assert!(
        r.read_surface_warning.is_none(),
        "{:?}",
        r.read_surface_warning
    );
    assert_eq!(status(&db), SurfaceStatus::Current);
    assert_eq!(
        api::head_commit(&db).unwrap(),
        head + 1,
        "one commit: the write's"
    );

    let c = raw(&db);
    let after: i64 = c
        .query_row("SELECT n FROM my_count", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        before, after,
        "the user view chain survives and reads the same rows"
    );
    let n: i64 = c
        .query_row(
            "SELECT count(*) FROM read_value('Node', 'demand', 'n1', 'Base')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 48, "the refresh brought read_value");
    // a user view's SELECT * re-expands at query time: it gains the
    // appended columns
    let cols: i64 = c
        .query_row(
            "SELECT count(*) FROM (DESCRIBE SELECT * FROM my_demand)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cols, 35);
    c.close().unwrap();
    // the second write does not refresh again (no DDL: identical catalog)
    let cat = catalog_sql(&db);
    write(&db, "Node", "capacity", "High", scalars(&["n1"], 98.0));
    assert_eq!(catalog_sql(&db), cat);
}

#[test]
fn the_explicit_refresh_changes_no_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "old.duckdb");
    age(&db);
    let head = api::head_commit(&db).unwrap();
    let tables = table_digests(&db);
    let r = api::refresh_read_surface(&db, false).expect("refresh");
    assert!(r.refreshed);
    assert_eq!(r.check.status, SurfaceStatus::Missing);
    assert_eq!(
        r.schema_version.as_deref(),
        Some(compaction::SCHEMA_VERSION)
    );
    assert_eq!(status(&db), SurfaceStatus::Current);
    assert_eq!(
        table_digests(&db),
        tables,
        "a refresh touches no base table row"
    );
    assert_eq!(api::head_commit(&db).unwrap(), head, "no commit row");
    // current: a no-op
    let r = api::refresh_read_surface(&db, false).unwrap();
    assert!(!r.refreshed);
    assert_eq!(r.check.status, SurfaceStatus::Current);
}

#[test]
fn a_user_table_with_a_reserved_name_blocks_the_refresh_not_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "blocked.duckdb");
    age(&db);
    // `axis_parametric` is a view this build adds and the old file lacks
    let c = raw(&db);
    c.execute_batch("CREATE TABLE axis_parametric (x INTEGER)")
        .unwrap();
    c.close().unwrap();
    read_surface::take_notices();

    let r = write(&db, "Node", "capacity", "High", scalars(&["n2"], 7.0));
    let w = r.read_surface_warning.expect("a warning on the report");
    assert!(
        w.contains("could not be refreshed") && w.contains("refresh-views"),
        "{w}"
    );
    assert!(r.commit_id > 0, "the write went through");
    assert_eq!(notices_for(&db), 1);
    // the old catalog is intact (the refresh rolled back as one transaction)
    assert_eq!(status(&db), SurfaceStatus::Missing);
    let c = raw(&db);
    assert!(c
        .prepare("SELECT * FROM read_value('Node', 'capacity', 'n2')")
        .is_err());
    let n: i64 = c
        .query_row(
            "SELECT count(*) FROM read_parameter('Node', 'capacity') WHERE alternative = 'High'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1);
    c.close().unwrap();
    // later writes report it again without retrying; no new notice
    let r = write(&db, "Node", "capacity", "High", scalars(&["n2"], 8.0));
    assert!(r.read_surface_warning.is_some());
    assert_eq!(notices_for(&db), 0);
    // the explicit verb fails loudly and changes nothing
    let e = api::refresh_read_surface(&db, false).unwrap_err();
    assert!(e.to_string().contains("refresh-views failed"), "{e}");
    assert_eq!(status(&db), SurfaceStatus::Missing);
    // reads still work, through the overlay
    let b = read_values(&db, "Node", "capacity", &["n2"], Some(&["High"])).unwrap();
    assert_eq!(b.num_rows(), 1);
}

#[test]
fn a_newer_file_is_never_downgraded_and_reads_use_its_surface() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "newer.duckdb");
    let v = build_version();
    set_marker(&db, v.contract_major, v.revision + 1, "00000000000000aa");
    // a "newer" read_values body, recognisable by its one row
    let c = raw(&db);
    c.execute_batch(
        "DROP MACRO TABLE read_values;
         CREATE MACRO read_values(class_name, parameter_name, entity_names, \
                                  alternative_names := NULL) AS TABLE
         SELECT 'from-the-file' AS entity;",
    )
    .unwrap();
    c.close().unwrap();
    let cat = catalog_sql(&db);
    let r = write(&db, "Node", "capacity", "High", scalars(&["n3"], 1.0));
    assert!(r.read_surface_warning.is_none());
    assert_eq!(catalog_sql(&db), cat, "no DDL ran on a newer file");
    assert_eq!(status(&db), SurfaceStatus::Newer);
    let r = api::refresh_read_surface(&db, false).unwrap();
    assert!(
        !r.refreshed,
        "the verb leaves a newer file alone without --force"
    );
    // same major: the core's reads use the file's (newer) surface
    let b = read_values(&db, "Node", "demand", &["n1"], None).unwrap();
    assert_eq!(rows(&b).1, vec!["from-the-file".to_string()]);
    // a newer MAJOR: the core's reads use this build's own objects
    set_marker(&db, v.contract_major + 1, 1, "00000000000000bb");
    let b = read_values(&db, "Node", "demand", &["n1"], Some(&["Base"])).unwrap();
    assert_eq!(b.num_rows(), 48);
    // ... and a generic read gets the full overlay up front
    let temps = with_read(&db, |c| Ok(temp_object_count(c))).unwrap();
    assert_eq!(temps as usize, surface().objects.len());
    // the closure overlay of the named read == the full overlay
    for alt in ["Base", "High"] {
        let full = with_read(&db, |c| {
            let mut st =
                c.prepare("SELECT * FROM read_parameter('Node', 'demand') WHERE alternative = ?")?;
            let arrow = st.query_arrow([alt])?;
            let schema = arrow.get_schema();
            let batches: Vec<RecordBatch> = arrow.collect();
            Ok(duckdb::arrow::compute::concat_batches(&schema, &batches)?)
        })
        .unwrap();
        let closure = read_parameter(&db, "Node", "demand", alt).unwrap();
        assert_eq!(rows(&full), rows(&closure), "{alt}");
        assert_eq!(full.num_rows(), if alt == "Base" { 144 } else { 96 });
        let full = with_read(&db, |c| {
            let mut st = c.prepare(
                "SELECT * FROM read_values('Node', 'demand', ['n1', 'N2', 'nx']::VARCHAR[], [?]::VARCHAR[])",
            )?;
            let arrow = st.query_arrow([alt])?;
            let schema = arrow.get_schema();
            let batches: Vec<RecordBatch> = arrow.collect();
            Ok(duckdb::arrow::compute::concat_batches(&schema, &batches)?)
        })
        .unwrap();
        let closure =
            read_values(&db, "Node", "demand", &["n1", "N2", "nx"], Some(&[alt])).unwrap();
        assert_eq!(rows(&full), rows(&closure), "read_values {alt}");
        assert_eq!(full.num_rows(), 96);
    }
    // --force downgrades (development only)
    let r = api::refresh_read_surface(&db, true).unwrap();
    assert!(r.refreshed);
    assert_eq!(status(&db), SurfaceStatus::Current);
}

#[test]
fn same_revision_other_hash_refreshes() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "drift.duckdb");
    let v = build_version();
    set_marker(&db, v.contract_major, v.revision, "0123456789abcdef");
    assert_eq!(status(&db), SurfaceStatus::Drift);
    write(&db, "Node", "capacity", "High", scalars(&["n1"], 3.0));
    assert_eq!(status(&db), SurfaceStatus::Current);
}

/// A file of another schema version is never refreshed (its tables differ):
/// the write path's own SchemaMismatch handling decides, unchanged.
#[test]
fn another_schema_version_is_not_refreshed() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "other.duckdb");
    age(&db);
    let c = raw(&db);
    c.execute_batch("UPDATE metadata SET value = '0.0-other' WHERE name = 'schema_version'")
        .unwrap();
    c.close().unwrap();
    let cat = catalog_sql(&db);
    let _ = write_parameter(
        &db,
        "Node",
        "capacity",
        "High",
        scalars(&["n1"], 3.0),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    );
    assert_eq!(catalog_sql(&db), cat);
    let e = api::refresh_read_surface(&db, false).unwrap_err();
    assert!(matches!(e, DbError::SchemaMismatch { .. }), "{e}");
}

// --- reads of a stale file ---------------------------------------------------

#[test]
fn stale_reads_overlay_and_match_the_refreshed_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "stale.duckdb");
    age(&db);
    let refreshed = dir.path().join("refreshed.duckdb");
    fs::copy(&db, &refreshed).unwrap();
    api::refresh_read_surface(&refreshed, false).unwrap();
    read_surface::take_notices();
    let bytes = fs::read(&db).unwrap();

    // the named reads: read_values does not exist in the old file at all
    for (ents, alts) in [
        (vec!["n1"], None),
        (vec!["n1", "N2", "nx"], Some(vec!["Base"])),
    ] {
        let a = read_values(&db, "Node", "demand", &ents, alts.as_deref()).unwrap();
        let b = read_values(&refreshed, "Node", "demand", &ents, alts.as_deref()).unwrap();
        assert_eq!(rows(&a), rows(&b));
        assert!(a.num_rows() > 0);
    }
    for (p, alt) in [("demand", "Base"), ("demand", "High"), ("capacity", "Base")] {
        let a = read_parameter(&db, "Node", p, alt).unwrap();
        let b = read_parameter(&refreshed, "Node", p, alt).unwrap();
        assert_eq!(rows(&a), rows(&b), "{p}/{alt}");
        assert_eq!(
            a.num_columns(),
            35,
            "this build's columns, end bounds appended"
        );
    }
    assert_eq!(notices_for(&db), 1, "one notice per file and process");

    // a generic read within one major uses the file's own surface ...
    let temps = with_read(&db, |c| Ok(temp_object_count(c))).unwrap();
    assert_eq!(temps, 0);
    // ... and a missing object is the defined error
    let sql = "SELECT count(*) FROM read_values('Node', 'demand', ['n1'])";
    let e = with_read(&db, |c| Ok(c.query_row(sql, [], |r| r.get::<_, i64>(0))?)).unwrap_err();
    match &e {
        DbError::ReadSurfaceStale {
            found, expected, ..
        } => {
            assert_eq!(found, &None);
            assert_eq!(expected, &build_version().to_string());
        }
        other => panic!("{other:?}"),
    }
    // the retrying read installs the full overlay and runs again
    let n = with_read_retrying(&db, |c| Ok(c.query_row(sql, [], |r| r.get::<_, i64>(0))?)).unwrap();
    assert_eq!(n, 96);
    // a current file's missing object stays the plain engine error
    let e = with_read(&refreshed, |c| {
        Ok(c.query_row("SELECT * FROM no_such_view", [], |r| r.get::<_, i64>(0))?)
    })
    .unwrap_err();
    assert!(matches!(e, DbError::Duckdb(_)), "{e:?}");

    assert_eq!(fs::read(&db).unwrap(), bytes, "reads never write");
    assert_eq!(status(&db), SurfaceStatus::Missing);
}

// --- extract and compaction --------------------------------------------------

/// Extract compares the derived views of source and extract through THIS
/// build's definitions. The source here is old AND its `value_keys` view is
/// tampered with (an extra row): comparing the source's own view with the
/// extract's would fail; the overlay makes the comparison fair.
#[test]
fn extract_of_a_stale_source_keeps_its_full_check() {
    let dir = tempfile::tempdir().unwrap();
    let src = data_file(dir.path(), "src.duckdb");
    age(&src);
    let c = raw(&src);
    c.execute_batch(
        "CREATE OR REPLACE VIEW value_keys AS
         SELECT entity_id, parameter_definition_id, alternative_id, 'scalar' AS tier
         FROM value_scalar WHERE valid_to_commit IS NULL
         UNION ALL SELECT -1, -1, -1, 'bogus'",
    )
    .unwrap();
    c.close().unwrap();
    let r = extract(
        &src,
        &dir.path().join("x.duckdb"),
        &ExtractOpts {
            as_of: None,
            user: "tester",
            note: None,
        },
    )
    .expect("extract of a stale source");
    assert_eq!(r.warning, None, "the derived-view check ran");
    assert_eq!(status(&dir.path().join("x.duckdb")), SurfaceStatus::Current);
    // the source was only read
    assert_eq!(status(&src), SurfaceStatus::Missing);
}

#[test]
fn compaction_of_a_newer_file_warns_and_rebuilds_this_builds_surface() {
    let dir = tempfile::tempdir().unwrap();
    let db = data_file(dir.path(), "cnewer.duckdb");
    let v = build_version();
    set_marker(&db, v.contract_major, v.revision + 5, "00000000000000cc");
    let r = compaction::compact(&db).expect("compact");
    assert!(r.swapped);
    let w = r.warning.expect("a downgrade warning");
    assert!(w.contains("newer than this nodal-db"), "{w}");
    assert_eq!(status(&db), SurfaceStatus::Current);
    // a current file compacts without that warning
    let r = compaction::compact(&db).unwrap();
    assert!(!r
        .warning
        .unwrap_or_default()
        .contains("newer than this nodal-db"));
}

#[test]
fn every_embedded_surface_file_is_applied_by_the_refresh() {
    // the refresh applies exactly 070-130 of the embedded schema
    let names: Vec<&str> = read_surface::surface_files()
        .iter()
        .map(|(n, _)| *n)
        .collect();
    let want: Vec<&str> = EMBEDDED_SCHEMA
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| ("070".."131").contains(&&n[..3]))
        .collect();
    assert_eq!(names, want);
    assert_eq!(names.last(), Some(&"130_read_surface.sql"));
}
