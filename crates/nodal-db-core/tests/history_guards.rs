//! Two focused history-layer guards (Phase 3a):
//!   - **F1 writer-boundary guard**: a malformed materialized axis (length !=
//!     child count, or empty) is rejected in RELEASE at the axis write boundary.
//!   - **Conflict wording**: DuckDB's write-write conflict really contains the
//!     `Conflict` substring the retry matcher keys on — pinned so a version bump
//!     that changes the phrase is caught. The full phrase depends on the
//!     operation (`Conflict on update!` vs `Conflict on tuple deletion!`), on
//!     1.5.5 and 2.0.0 alike.

use std::path::PathBuf;

use duckdb::Connection;

use nodal_db_core::axis::{dedup_or_insert, AxisCache};
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::{with_write_txn, with_write_txn_retrying};
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis};
use nodal_db_core::error::DbError;
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.duckdb");
    init_schema(&path).expect("init schema");
    (dir, path)
}

#[test]
fn f1_guard_rejects_malformed_materialized_axis() {
    let (_dir, path) = fresh_db();
    // length says 5 but only 2 coords are present — must be rejected, not stored.
    let bad = CanonicalAxis {
        length: 5,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerMaterialized { coords: vec![1, 2] },
    };
    let err = with_write_txn(&path, "t", "f1", |conn, _c| {
        let mut cache = AxisCache::new();
        dedup_or_insert(conn, &mut cache, &bad)
    })
    .expect_err("malformed axis must be rejected");
    assert!(
        matches!(
            err,
            DbError::MalformedAxis {
                length: 5,
                children: 2,
                ..
            }
        ),
        "unexpected error: {err}"
    );

    // An empty child list is likewise rejected.
    let empty = CanonicalAxis {
        length: 0,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerMaterialized { coords: vec![] },
    };
    let err2 = with_write_txn(&path, "t", "f1-empty", |conn, _c| {
        let mut cache = AxisCache::new();
        dedup_or_insert(conn, &mut cache, &empty)
    })
    .expect_err("empty child list must be rejected");
    assert!(matches!(err2, DbError::MalformedAxis { children: 0, .. }));
}

#[test]
fn retrying_wrapper_happy_path_writes_value() {
    // The `Fn` (re-runnable) closure drives the history protocol; with no
    // contention it succeeds on the first attempt and commits.
    let (_dir, path) = fresh_db();
    let key = ValueKey {
        entity_id: 7,
        parameter_definition_id: 7,
        alternative_id: 7,
    };
    let value = CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(42.0)),
    };
    let outcome = with_write_txn_retrying(&path, "t", "retry-happy", |conn, c| {
        let mut cache = AxisCache::new();
        write_value(
            conn,
            c,
            key,
            &value,
            &mut cache,
            &WriteOptions::default(),
            None,
        )
    })
    .expect("retrying write succeeds");
    assert_eq!(outcome.tier, Some(Tier::Scalar));
    assert_eq!(outcome.version, 1);

    let conn = Connection::open(&path).unwrap();
    let v: f64 = conn
        .query_row(
            "SELECT v_float FROM value_scalar WHERE entity_id=7 AND valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(v, 42.0);
}

#[test]
fn batched_long_cells_bump_versions_independently() {
    // Batching edge (RESULTS.md alarm #1 fix): the one-query-per-key version
    // computation must still advance each cell's version independently when the
    // cells of a single write sit at DIFFERENT current versions. A three-write
    // sequence drives pos0 to v3 and pos1 to v2 within one batched insert:
    //   1. write {0,1}            -> pos0 v1, pos1 v1  (both live)
    //   2. write {0}              -> close both; reinsert pos0 v2; pos1 stays dead v1
    //   3. write {0,1}            -> close pos0 v2; reinsert pos0 v3, pos1 v2
    // The last write inserts two cells whose next versions differ (3 vs 2).
    let (_dir, path) = fresh_db();
    let key = ValueKey {
        entity_id: 55,
        parameter_definition_id: 65,
        alternative_id: 75,
    };

    let long = |cells: &[(i32, f64)]| CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes: vec![CanonicalAxis {
            length: 2,
            properties: AxisProperties::default(),
            body: AxisBody::IntegerParametric { start: 0, step: 1 },
        }],
        shape: ShapedData::Long(
            cells
                .iter()
                .map(|&(pos, value)| LongRow {
                    positions: vec![pos],
                    value: ScalarValue::Float(value),
                })
                .collect(),
        ),
    };

    for cells in [
        vec![(0i32, 10.0f64), (1, 20.0)],
        vec![(0, 11.0)],
        vec![(0, 12.0), (1, 21.0)],
    ] {
        let v = long(&cells);
        with_write_txn(&path, "t", "batch-versions", |conn, c| {
            let mut cache = AxisCache::new();
            write_value(conn, c, key, &v, &mut cache, &WriteOptions::default(), None)
        })
        .expect("long write");
    }

    let conn = Connection::open(&path).unwrap();
    // Live rows: pos0 at v3 = 12.0, pos1 at v2 = 21.0 — independent bumps.
    let live: Vec<(i32, i16, f64)> = conn
        .prepare(
            "SELECT i1_pos, version, v_float FROM value_long \
             WHERE entity_id=55 AND valid_to_commit IS NULL ORDER BY i1_pos",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        live,
        vec![(0, 3, 12.0), (1, 2, 21.0)],
        "per-cell version continuity"
    );

    // The append-only tiling audit must stay clean across the batched writes.
    let tiling: i64 = conn
        .query_row("SELECT count(*) FROM audit_history_tiling", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(tiling, 0, "audit_history_tiling must be empty");
}

#[test]
fn duckdb_write_conflict_wording_contains_conflict() {
    // Two connections into ONE DuckDB instance (try_clone) race the same row.
    // The loser's COMMIT raises the write-write conflict; assert its message
    // carries the `Conflict` substring the retry loop matches on.
    let (_dir, path) = fresh_db();
    let base = Connection::open(&path).expect("open");
    base.execute_batch(
        "INSERT INTO commit (user, message) VALUES ('t', 'seed');
         INSERT INTO value_scalar
           (entity_id, parameter_definition_id, alternative_id, valid_from_commit,
            version, value_kind, v_float)
         VALUES (1, 1, 1, currval('seq_commit'), 1, 'float', 1.0);",
    )
    .expect("seed");

    // try_clone opens a second independent connection on the SAME database
    // instance (duckdb_connect), so the two run concurrent transactions.
    let c1 = base.try_clone().expect("clone c1");
    let c2 = base.try_clone().expect("clone c2");
    let upd = "UPDATE value_scalar SET v_float = ? \
               WHERE entity_id=1 AND parameter_definition_id=1 AND alternative_id=1";
    c1.execute_batch("BEGIN").unwrap();
    c2.execute_batch("BEGIN").unwrap();
    // Both transactions write the same row while both are uncommitted: DuckDB's
    // optimistic concurrency raises the write-write conflict on the second
    // writer (at the statement, or at its COMMIT — capture whichever errors).
    c1.execute(upd, duckdb::params![2.0]).unwrap();
    let loser = c2
        .execute(upd, duckdb::params![3.0])
        .err()
        .or_else(|| {
            let _ = c1.execute_batch("COMMIT");
            c2.execute_batch("COMMIT").err()
        })
        .expect("c2 must lose the race");
    let _ = c1.execute_batch("ROLLBACK");
    let _ = c2.execute_batch("ROLLBACK");
    assert!(
        loser.to_string().contains("Conflict"),
        "conflict wording changed: {loser}"
    );
}
