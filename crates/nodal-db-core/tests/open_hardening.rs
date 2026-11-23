//! Hardening of the core's file handling against files and directories
//! someone else controls (pre-release security review, low findings):
//!
//! - `init` refuses a path that exists and is not an empty file, or that
//!   has a `.wal` (it used to apply the schema into a foreign DuckDB file);
//! - a non-UTF-8 database path is refused up front (a lossy conversion
//!   named another file in the rebuild's `ATTACH`);
//! - compaction's and extract's temps and the baseline sidecar's temp are
//!   new files under unpredictable names: a symlink planted under the old,
//!   predictable names is never written through;
//! - template YAML: size and flow-nesting caps before the parser (a 200 KB
//!   file nested 100 000 deep took 17 s to reject).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use duckdb::Connection;

use nodal_db_core::compaction::{baseline_path, compact};
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init::{init_schema, init_schema_with_default};
use nodal_db_core::template::parse_str;

fn table_count(db: &Path) -> i64 {
    let c = Connection::open(db).unwrap();
    let n = c
        .query_row(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = current_database()",
            [],
            |r| r.get(0),
        )
        .unwrap();
    c.close().unwrap();
    n
}

#[test]
fn init_refuses_an_existing_file_or_wal() {
    let dir = tempfile::tempdir().unwrap();
    // Someone's own DuckDB file.
    let foreign = dir.path().join("mine.duckdb");
    {
        let c = Connection::open(&foreign).unwrap();
        c.execute_batch("CREATE TABLE mine (x INTEGER); INSERT INTO mine VALUES (1)")
            .unwrap();
        c.close().unwrap();
    }
    let e = init_schema_with_default(&foreign, Some("Base")).expect_err("foreign file");
    assert!(
        matches!(&e, DbError::Api(m) if m.contains("already exists")),
        "{e}"
    );
    assert_eq!(table_count(&foreign), 1, "the foreign file was written");

    // An initialized nodal-db file: a second init would half-apply.
    let ours = dir.path().join("ours.duckdb");
    init_schema(&ours).unwrap();
    let before = std::fs::read(&ours).unwrap();
    init_schema(&ours).expect_err("a second init");
    assert_eq!(std::fs::read(&ours).unwrap(), before);

    // A stale WAL next to a fresh path.
    let walled = dir.path().join("walled.duckdb");
    std::fs::write(dir.path().join("walled.duckdb.wal"), b"junk").unwrap();
    let e = init_schema(&walled).expect_err("a stale wal");
    assert!(e.to_string().contains("write-ahead log"), "{e}");
    assert!(!walled.exists());

    // A symlink is not a fresh path, even a dangling one.
    #[cfg(unix)]
    {
        let link = dir.path().join("link.duckdb");
        std::os::unix::fs::symlink(dir.path().join("nowhere"), &link).unwrap();
        init_schema(&link).expect_err("a dangling symlink");
        assert!(!dir.path().join("nowhere").exists());
    }

    // A zero-byte file is an existing entry too (DuckDB 2.0 cannot open
    // it); an absent path is fresh.
    let touched = dir.path().join("touched.duckdb");
    std::fs::write(&touched, b"").unwrap();
    init_schema(&touched).expect_err("a zero-byte file");
    init_schema(&dir.path().join("new.duckdb")).expect("an absent path");
}

#[cfg(unix)]
#[test]
fn a_non_utf8_path_is_refused() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir().unwrap();
    let good = dir.path().join("good.duckdb");
    init_schema(&good).unwrap();
    let bad: PathBuf = dir.path().join(OsStr::from_bytes(b"bad-\xff.duckdb"));
    // The file exists under the non-UTF-8 name; a lossy spelling of it does
    // too (the review's hazard: ATTACH used to open that other file).
    std::fs::copy(&good, &bad).unwrap();
    std::fs::copy(&good, dir.path().join("bad-\u{fffd}.duckdb")).unwrap();
    let utf8 = |e: DbError| assert!(e.to_string().contains("not valid UTF-8"), "{e}");
    utf8(compact(&bad).expect_err("compaction"));
    utf8(nodal_db_core::api::head_commit(&bad).expect_err("read"));
    let opts = nodal_db_core::extract::ExtractOpts {
        user: "t",
        ..Default::default()
    };
    utf8(
        nodal_db_core::extract::extract(&bad, &dir.path().join("x.duckdb"), &opts)
            .expect_err("extract src"),
    );
    utf8(
        nodal_db_core::extract::extract(
            &good,
            &dir.path().join(OsStr::from_bytes(b"x-\xff")),
            &opts,
        )
        .expect_err("extract dest"),
    );
    utf8(init_schema(&dir.path().join(OsStr::from_bytes(b"new-\xff"))).expect_err("init"));
    // Nothing was named after a fallback: no `db.compact.*` / `db.extract.*`.
    let stray: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.ends_with(".tmp") || n.ends_with(".tmp.wal"))
        .collect();
    assert!(stray.is_empty(), "{stray:?}");
}

#[cfg(unix)]
#[test]
fn planted_symlinks_at_the_old_temp_names_are_never_written_through() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("shared.duckdb");
    init_schema(&db).unwrap();
    let victim = dir.path().join("victim.txt");
    std::fs::write(&victim, b"precious").unwrap();
    let pid = std::process::id();
    let mut baseline_tmp = baseline_path(&db).into_os_string();
    baseline_tmp.push(format!(".{pid}.tmp"));
    for planted in [
        PathBuf::from(baseline_tmp),
        dir.path()
            .join(format!("shared.duckdb.compact.{pid}.tmp.wal")),
    ] {
        std::os::unix::fs::symlink(&victim, &planted).unwrap();
    }
    compact(&db).expect("compaction");
    assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
    let text = std::fs::read_to_string(baseline_path(&db)).unwrap();
    assert!(text.starts_with("bytes="), "{text}");
}

/// A crafted sidecar `commit=i64::MIN` used to overflow `head - commit`
/// (a debug panic; a wrap in release): it now just counts as due.
#[test]
fn a_crafted_sidecar_commit_does_not_overflow() {
    use nodal_db_core::compaction::{compact_if_due, CommitBackstop, CompactionPolicy};
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("side.duckdb");
    init_schema(&db).unwrap();
    let bytes = std::fs::metadata(&db).unwrap().len();
    std::fs::write(
        baseline_path(&db),
        format!("bytes={bytes}\ncommit=-9223372036854775808\n"),
    )
    .unwrap();
    let policy = CompactionPolicy {
        growth_factor: 1000.0,
        max_commits: CommitBackstop::Fixed(60),
    };
    let r = compact_if_due(&db, policy, None).expect("no overflow");
    assert!(r.trigger.is_some(), "{r:?}");
}

#[test]
fn deep_flow_nesting_is_refused_before_the_parser() {
    let deep = format!("x: {}", "[".repeat(100_000));
    let t = Instant::now();
    let e = parse_str(&deep).expect_err("deep nesting");
    assert!(e.to_string().contains("nested more than"), "{e}");
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());

    // Quoted closers inside the nesting cannot hide it.
    let mut hidden = String::from("x: ");
    for _ in 0..100 {
        hidden.push_str("['a]]]]', \"b]]}}\", ");
    }
    parse_str(&hidden).expect_err("quoted closers do not lower the depth");
    // Nor can a plain scalar's quote, a comment or a block scalar.
    let tricks = format!(
        "a: rock 'n roll\nb: x'y: {deep}\n# ]]]]\nd: |\n  ]]]]\ne: {}",
        "[".repeat(100),
        deep = "[".repeat(10)
    );
    parse_str(&tricks).expect_err("the nesting after the tricks is counted");

    // Ordinary flow lists, quoted brackets, comments and block scalars with
    // brackets do not count against the cap.
    let ok = "a: [1, [2, [3]]]\nb: {c: [d, e]}\nf: 'x [[[[ y'\ng: \"z {{{{\"\n# [[[[[[\nh: |\n  [[[[[[[[\n  ((\ni: it's [ok]\n";
    check_ok(ok);
}

/// `parse_str` got past the shape check (a later schema error is fine).
fn check_ok(text: &str) {
    if let Err(e) = parse_str(text) {
        let m = e.to_string();
        assert!(
            !m.contains("nested more than") && !m.contains("MiB a"),
            "refused by the shape check: {m}"
        );
    }
}

#[test]
fn an_oversized_template_is_refused() {
    let big = "a".repeat(nodal_db_core::template::ir::MAX_TEMPLATE_BYTES + 1);
    let e = parse_str(&big).expect_err("too big");
    assert!(e.to_string().contains("MiB a template may be"), "{e}");
}
