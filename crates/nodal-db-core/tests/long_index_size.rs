//! Size guard for the long tier's one-current unique index
//! (`one_current_long`, `schema/050_values.sql`).
//!
//! The index's column ORDER is a size decision, not semantics: uniqueness of
//! a tuple set does not depend on column order, but DuckDB's ART stores each
//! row's key bytes after its last branch point, so columns that are constant
//! within a value (`c4, c3, c2` below the declared rank, the key triple, the
//! zero high bytes of every position) are paid on every row when they trail.
//! The order `(c4, c3, c2, ca, cp, ce, c1)` puts them first and the
//! highest-fan-out column last; the previous order `(ce, cp, ca, c1..c4)` cost
//! ~5x more. The failure is silent (no error, just a bigger file), so this
//! test fails `cargo test --workspace` when either the DDL order or a
//! duckdb-rs bump changes the ART layout.
//!
//! Measurement: on a copy of the file, CHECKPOINT, drop `one_current_long`,
//! CHECKPOINT again; the index's bytes are the drop in `used_blocks` times
//! the block size (256 KiB), divided by the CURRENT rows (closed rows have
//! all seven key columns NULL and are not in the ART). The resolution is one
//! block, ~1.5 B/row at these fixture sizes. The hidden FK ARTs are not
//! touched by the drop. The figures are deterministic (identical blocks at
//! `threads = 1` and the default, and for 2 048- and 100 000-row insert
//! chunks), so no `SET threads`.
//!
//! Thresholds against the figures measured when this test was written
//! (DuckDB 2.0.0 through duckdb-rs `=1.20000.0`); the old order is the
//! control:
//!
//! | fixture | current rows | blocks (B/row) | threshold | old order |
//! |---|---|---|---|---|
//! | (a) sparse rank-2, 1 000 × 300 at 62 % | 186 001 | 9 (12.7) | ≤ 20 B/row (14.2 blocks) | 47 (66.2) |
//! | (b) dense rank-1, 200 × 876 | 175 200 | 6 (9.0) | ≤ 15 B/row (10.0 blocks) | 38 (56.9) |
//! | (c) (b) + 1, 2, 3 replace rounds | 175 200 | 6, 7, 6 | ≤ 15 B/row and ≤ 1.5× (b) | |
//! | (e) (a) in the old order, compacted | 186 001 | 9 (12.7) | ≤ 20 B/row | |
//!
//! A benign DuckDB patch would have to move (b) by 3-4 blocks (50 % or more)
//! to trip it: that is an ART layout change worth a look anyway. Why the order
//! and how it was chosen: `specs/Done/long_index_reorder_PLAN.md` §1.

use std::path::{Path, PathBuf};

use duckdb::Connection;
use nodal_db_core::compaction::compact;
use nodal_db_core::schema_init::init_schema;

/// The bundled order (`schema/050_values.sql`), as `duckdb_indexes()`
/// renders it.
const BUNDLED_ORDER: &str = "[c4, c3, c2, ca, cp, ce, c1]";
/// The order before the reorder: the control and the migration source.
const OLD_ORDER: &str = "ce, cp, ca, c1, c2, c3, c4";

/// `one_current_long`'s measured size.
struct IndexSize {
    bytes: i64,
    blocks: i64,
    current: i64,
    total: i64,
}

impl IndexSize {
    fn per_current_row(&self) -> f64 {
        self.bytes as f64 / self.current as f64
    }

    fn show(&self, label: &str) -> String {
        format!(
            "{label}: one_current_long {} bytes = {} blocks, {:.2} B per current row \
             ({} current, {} total rows)",
            self.bytes,
            self.blocks,
            self.per_current_row(),
            self.current,
            self.total
        )
    }
}

fn query_i64(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn used_blocks(conn: &Connection) -> i64 {
    query_i64(conn, "SELECT used_blocks FROM pragma_database_size()")
}

/// Drop `one_current_long` on a copy of `db` and return the space it freed.
fn index_size(db: &Path) -> IndexSize {
    let copy = db.with_extension("measure.duckdb");
    std::fs::copy(db, &copy).unwrap();
    let conn = Connection::open(&copy).unwrap();
    conn.execute_batch("CHECKPOINT").unwrap();
    let before = used_blocks(&conn);
    let block_size = query_i64(&conn, "SELECT block_size FROM pragma_database_size()");
    conn.execute_batch("DROP INDEX one_current_long; CHECKPOINT")
        .unwrap();
    let blocks = before - used_blocks(&conn);
    let current = query_i64(
        &conn,
        "SELECT count(*) FROM value_long WHERE valid_to_commit IS NULL",
    );
    let total = query_i64(&conn, "SELECT count(*) FROM value_long");
    conn.close().unwrap();
    std::fs::remove_file(&copy).unwrap();
    IndexSize {
        bytes: blocks * block_size,
        blocks,
        current,
        total,
    }
}

fn index_expressions(db: &Path) -> String {
    let conn = Connection::open(db).unwrap();
    let e: String = conn
        .query_row(
            "SELECT expressions FROM duckdb_indexes() WHERE index_name = 'one_current_long'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.close().unwrap();
    e
}

/// Re-create `one_current_long` in `cols` order (a bulk build over the
/// existing rows; it measures the same as an incremental one).
fn recreate_index(db: &Path, cols: &str) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(&format!(
        "DROP INDEX one_current_long; \
         CREATE UNIQUE INDEX one_current_long ON value_long ({cols}); CHECKPOINT;"
    ))
    .unwrap();
    conn.close().unwrap();
}

/// A fresh schema with one commit and an integer axis per entry of
/// `lengths`; returns the connection, the commit and the axis ids.
fn fresh(db: &Path, lengths: &[i64]) -> (Connection, i64, Vec<i64>) {
    init_schema(db).expect("init");
    let conn = Connection::open(db).unwrap();
    let commit = query_i64(
        &conn,
        "INSERT INTO \"commit\" (user, message) VALUES ('t', 'v1') RETURNING id",
    );
    let axes = lengths
        .iter()
        .map(|len| {
            query_i64(
                &conn,
                &format!(
                    "INSERT INTO axis (kind, length, content_hash, p_start_int, p_step_int) \
                     VALUES ('integer', {len}, {len}, 0, 1) RETURNING id"
                ),
            )
        })
        .collect();
    (conn, commit, axes)
}

/// Fixture (a): ONE rank-2 value of 1 000 × 300 positions at ~62 % fill,
/// row-major (i1 outer). The fill is a fixed arithmetic predicate (a
/// golden-ratio multiplicative hash of the cell index, top 32 bits as a
/// fraction), never `random()`, so the row set is the same on every run.
/// Inserted in one statement through the index (the write path's
/// incremental build).
fn build_sparse(dir: &Path) -> PathBuf {
    let db = dir.join("sparse.duckdb");
    let (conn, commit, axes) = fresh(&db, &[1000, 300]);
    conn.execute_batch(&format!(
        "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, \
           valid_from_commit, version, axis1_id, axis2_id, i1_pos, i2_pos, value_kind, v_float) \
         SELECT 1, 1, 1, {commit}, 1, {a1}, {a2}, (c // 300)::INTEGER, (c % 300)::INTEGER, \
                'float', (c % 1000) / 8.0 \
         FROM range(300000) t(c) \
         WHERE (c * 2654435761) % 4294967296 < 2662879723 \
         ORDER BY c; \
         CHECKPOINT;",
        a1 = axes[0],
        a2 = axes[1],
    ))
    .unwrap();
    conn.close().unwrap();
    db
}

/// Fixture (b): 200 dense rank-1 values of 876 positions each.
fn build_dense(dir: &Path) -> PathBuf {
    let db = dir.join("dense.duckdb");
    let (conn, commit, axes) = fresh(&db, &[876]);
    conn.execute_batch(&format!(
        "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, \
           valid_from_commit, version, axis1_id, i1_pos, value_kind, v_float) \
         SELECT c // 876, 1, 1, {commit}, 1, {a1}, (c % 876)::INTEGER, 'float', (c % 997) / 8.0 \
         FROM range(175200) t(c) ORDER BY c; \
         CHECKPOINT;",
        a1 = axes[0],
    ))
    .unwrap();
    conn.close().unwrap();
    db
}

/// One full replace round, as the write path does it: in one transaction,
/// the core's close-out UPDATE (`history::LONG_CLOSE_SQL`, keyed by triple)
/// and a re-insert of every cell as the next version.
fn replace_round(db: &Path, version: i64) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    let c = query_i64(
        &conn,
        "INSERT INTO \"commit\" (user, message) VALUES ('t', 'round') RETURNING id",
    );
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE replaced AS \
           SELECT * EXCLUDE (valid_from_commit, version) FROM value_long \
           WHERE valid_to_commit IS NULL; \
         UPDATE value_long SET valid_to_commit = {c} \
           FROM (SELECT DISTINCT entity_id AS e, parameter_definition_id AS p, \
                        alternative_id AS a \
                 FROM value_long WHERE valid_to_commit IS NULL) k \
           WHERE value_long.entity_id = k.e AND value_long.parameter_definition_id = k.p \
             AND value_long.alternative_id = k.a AND value_long.valid_to_commit IS NULL; \
         INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, \
             valid_from_commit, version, axis1_id, axis2_id, i1_pos, i2_pos, value_kind, v_float) \
           SELECT entity_id, parameter_definition_id, alternative_id, {c}, {version}, \
                  axis1_id, axis2_id, i1_pos, i2_pos, value_kind, v_float + 1 \
           FROM replaced ORDER BY entity_id, i1_pos, i2_pos; \
         DROP TABLE replaced; \
         COMMIT; \
         CHECKPOINT;"
    ))
    .unwrap();
    conn.close().unwrap();
}

/// (a) and (d): the sparse rank-2 value is small in the bundled order, and
/// the old order measures ≥ 3× it (the control: a broken measurement that
/// always returns ~0 fails here).
#[test]
fn sparse_rank2_value_index_is_small() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_sparse(dir.path());
    assert_eq!(index_expressions(&db), BUNDLED_ORDER);
    let ar = index_size(&db);
    println!("{}", ar.show("(a) sparse rank-2, bundled order"));
    assert!(
        (180_000..=192_000).contains(&ar.current) && ar.current == ar.total,
        "fixture drifted: {} current / {} total rows",
        ar.current,
        ar.total
    );
    assert!(
        ar.per_current_row() <= 20.0,
        "one_current_long grew past 20 B per current row on a sparse rank-2 value \
         (expected ~13): {}",
        ar.show("(a)")
    );

    recreate_index(&db, OLD_ORDER);
    let old = index_size(&db);
    println!("{}", old.show("(d) sparse rank-2, old order (control)"));
    assert!(
        old.bytes >= 3 * ar.bytes,
        "control: the old order must measure >= 3x the bundled one \
         (expected ~5x); old {} vs bundled {}",
        old.show("old"),
        ar.show("bundled")
    );
}

/// (b) and (c): dense rank-1 values are small, and history does not grow the
/// index (closed rows are not in the ART). Do not assert a per-closed-row
/// bound: one round adds 1-2 blocks of allocator slack, not indexed rows (an
/// indexed closed row would cost ~9 B each, 3× over after three rounds).
#[test]
fn dense_rank1_index_is_small_and_history_is_free() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_dense(dir.path());
    let start = index_size(&db);
    println!("{}", start.show("(b) dense rank-1, x1"));
    assert_eq!(start.current, 175_200);
    assert!(
        start.per_current_row() <= 15.0,
        "one_current_long grew past 15 B per current row on dense rank-1 values \
         (expected ~9): {}",
        start.show("(b)")
    );

    for round in 1..=3 {
        replace_round(&db, round + 1);
        let s = index_size(&db);
        println!(
            "{}",
            s.show(&format!("(c) dense rank-1, +{round} round(s)"))
        );
        assert_eq!(s.current, 175_200);
        assert_eq!(s.total, 175_200 * (round + 1));
        assert!(
            s.per_current_row() <= 15.0,
            "after {round} replace round(s), one_current_long exceeds 15 B per \
             current row: {}",
            s.show("(c)")
        );
        assert!(
            s.bytes * 2 <= start.bytes * 3,
            "after {round} replace round(s), one_current_long grew past 1.5x its \
             x1 size ({} -> {} blocks): closed rows must cost ~nothing in the ART",
            start.blocks,
            s.blocks
        );
    }
}

/// (e): a file whose index has the old order gets the bundled order (and
/// size) from compaction, which rebuilds the catalog from the bundled DDL.
#[test]
fn compaction_migrates_old_index_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = build_sparse(dir.path());
    recreate_index(&db, OLD_ORDER);
    assert_eq!(
        index_expressions(&db),
        "[ce, cp, ca, c1, c2, c3, c4]",
        "the migration source must carry the old order"
    );

    let r = compact(&db).expect("compact");
    assert!(r.swapped, "{r:?}");
    assert_eq!(index_expressions(&db), BUNDLED_ORDER);
    let after = index_size(&db);
    println!("{}", after.show("(e) sparse rank-2, old order compacted"));
    assert!(
        after.per_current_row() <= 20.0,
        "compaction must rebuild the small index: {}",
        after.show("(e)")
    );
}
