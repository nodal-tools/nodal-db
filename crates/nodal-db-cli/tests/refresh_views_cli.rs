//! `nodal-db maintain refresh-views` end to end: the `--check` exit codes,
//! the refresh and its output, and kill-safety: a refresh process killed at
//! arbitrary points leaves the old or the new read surface, never a mix
//! (DDL is transactional and the WAL replays all of a commit or none).

use std::path::Path;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use nodal_db_core::duckdb::Connection;
use nodal_db_core::read_surface::{build_version, surface};

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_nodal-db"));
    c.env("USER", "cli_tester");
    c
}

fn init(db: &Path) {
    let out = bin().arg("init").arg(db).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Replace the file's version marker (a stale or newer surface).
fn set_marker(db: &Path, major: i32, revision: i32, hash: &str) {
    let c = Connection::open(db).unwrap();
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

fn check_code(db: &Path) -> (i32, String) {
    let out = bin()
        .args(["maintain", "refresh-views", "--check"])
        .arg(db)
        .output()
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
    )
}

#[test]
fn check_exit_codes_and_refresh() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.duckdb");
    init(&db);
    let v = build_version();
    let (code, out) = check_code(&db);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("current") && out.contains(&v.to_string()),
        "{out}"
    );

    set_marker(&db, v.contract_major, v.revision + 1, "00000000000000aa");
    assert_eq!(check_code(&db).0, 2, "newer");
    let out = bin()
        .args(["maintain", "refresh-views"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("left alone"));
    assert_eq!(check_code(&db).0, 2, "a newer file is left alone");

    let c = Connection::open(&db).unwrap();
    c.execute_batch("DROP MACRO TABLE nodal_read_surface")
        .unwrap();
    c.close().unwrap();
    let (code, out) = check_code(&db);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("missing") && out.contains("predates versioning"),
        "{out}"
    );

    let out = bin()
        .args(["maintain", "refresh-views"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("refreshed the read surface"));
    assert_eq!(check_code(&db).0, 0);
}

#[test]
fn a_blocked_refresh_fails_loudly() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("b.duckdb");
    init(&db);
    let c = Connection::open(&db).unwrap();
    c.execute_batch(
        "DROP MACRO TABLE nodal_read_surface; DROP VIEW axis_parametric; \
         CREATE TABLE axis_parametric (x INTEGER);",
    )
    .unwrap();
    c.close().unwrap();
    let out = bin()
        .args(["maintain", "refresh-views"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("refresh-views failed"), "{err}");
    assert_eq!(check_code(&db).0, 1, "nothing changed");
}

/// The state of the file's read surface after a (possibly killed) refresh:
/// every object present, every view binds, and the marker is either the
/// stale one or this build's.
fn assert_consistent(db: &Path, stale_hash: &str) -> bool {
    let c = Connection::open(db).unwrap();
    let hash: String = c
        .query_row("SELECT ddl_hash FROM nodal_read_surface()", [], |r| {
            r.get(0)
        })
        .unwrap();
    let refreshed = hash == build_version().ddl_hash;
    assert!(refreshed || hash == stale_hash, "marker {hash}");
    let names: Vec<(String, bool)> = c
        .prepare(
            "SELECT view_name, true FROM duckdb_views() WHERE NOT internal \
             UNION ALL SELECT DISTINCT function_name, false FROM duckdb_functions() \
             WHERE NOT internal AND function_type IN ('macro', 'table_macro') \
               AND database_name = current_database()",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names.len(), surface().objects.len(), "{names:?}");
    for (name, is_view) in &names {
        if *is_view {
            c.prepare(&format!("SELECT * FROM \"{name}\" LIMIT 0"))
                .unwrap_or_else(|e| panic!("view {name} does not bind: {e}"));
        }
    }
    c.close().unwrap();
    refreshed
}

#[test]
fn a_killed_refresh_leaves_the_old_or_the_new_surface() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("k.duckdb");
    init(&db);
    let v = build_version();
    let stale = "00000000000000ee";
    let (mut old, mut new) = (0, 0);
    let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
    for _ in 0..12 {
        // a stale marker (same revision, other hash): the refresh runs
        set_marker(&db, v.contract_major, v.revision, stale);
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let delay = 10 + (seed >> 33) % 400;
        let mut child = bin()
            .args(["maintain", "refresh-views"])
            .arg(&db)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        sleep(Duration::from_millis(delay));
        let _ = child.kill();
        let _ = child.wait();
        if assert_consistent(&db, stale) {
            new += 1;
        } else {
            old += 1;
        }
    }
    // informative: both outcomes are valid; the run is useful when some
    // kills landed mid-refresh
    eprintln!("killed refreshes: {old} left the old surface, {new} the new one");
}
