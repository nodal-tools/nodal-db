//! Engine rule 12's lock-down keeps DuckDB's own files working: with
//! external access off and the configuration locked, a sort larger than the
//! memory limit still spills to DuckDB's temp directory, and the rebuild's
//! `ATTACH` (compaction, extract) still reaches its source. Its own test
//! binary: `set_resource_limits` is process-wide.

use std::path::Path;

use nodal_db_core::compaction::compact;
use nodal_db_core::conn::{resource_limits, set_resource_limits, with_write_txn, ResourceLimits};
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::schema_init::init_schema_with_default;

fn setting(db: &Path, name: &str) -> String {
    nodal_db_core::conn::with_read(db, |c| {
        Ok(c.query_row(
            &format!("SELECT current_setting('{name}')::VARCHAR"),
            [],
            |r| r.get(0),
        )?)
    })
    .unwrap()
}

#[test]
fn a_locked_connection_spills_and_the_rebuild_attaches() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("spill.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();

    // Bad limits are refused up front and change nothing.
    assert!(set_resource_limits(ResourceLimits {
        memory_limit: Some("lots".into()),
        threads: None,
    })
    .is_err());
    assert!(set_resource_limits(ResourceLimits {
        memory_limit: None,
        threads: Some(0),
    })
    .is_err());
    assert_eq!(resource_limits(), ResourceLimits::default());

    set_resource_limits(ResourceLimits {
        memory_limit: Some("64MiB".into()),
        threads: Some(2),
    })
    .unwrap();
    assert_eq!(setting(&db, "threads"), "2");
    assert_eq!(setting(&db, "memory_limit"), "64.0 MiB");

    // ~4 M doubles (32 MB of keys, more with the sort's payload) sorted
    // under a 64 MiB limit: only possible by spilling.
    let n: i64 = with_write_txn(&db, "t", "m", |c, _| {
        c.execute_batch(
            "CREATE TEMP TABLE big AS SELECT random() AS r, i FROM range(4000000) t(i); \
             CREATE TEMP TABLE sorted AS SELECT r, i FROM big ORDER BY r",
        )?;
        Ok(c.query_row("SELECT count(*) FROM sorted", [], |r| r.get(0))?)
    })
    .expect("a spilling sort on a locked connection");
    assert_eq!(n, 4_000_000);

    compact(&db).expect("compaction attaches its source");
    let dest = dir.path().join("x.duckdb");
    extract(
        &db,
        &dest,
        &ExtractOpts {
            user: "t",
            ..Default::default()
        },
    )
    .expect("extract attaches its source");
    set_resource_limits(ResourceLimits::default()).unwrap();
}
