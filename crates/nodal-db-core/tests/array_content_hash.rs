//! The stored array payload hash (skip-unchanged P3,
//! `value_array.content_hash`, spec canonical_encoding §13).
//!
//! The hash narrows, the bytes decide: in the unchanged check a stored hash
//! that differs from the new payload's proves "changed" without fetching
//! the payload; a match or a NULL hash falls through to the byte compare.
//! So a wrong stored hash can only cost one extra version, never a lost
//! write. Each property is checked in the set form (a round of several
//! keys, `write_array_round`) and the point form (one key,
//! `write_array_one`).

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, delete_values, DeleteKey, Segment};
use nodal_db_core::encoding::array_payload_hash;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{WriteOptions, WriteReport};

use common::assert_array_hashes;

const T: [i64; 4] = [0, 1, 2, 3];
/// The base payload of entity `k`: min `k`, max `k + 3`.
fn base(k: usize) -> Vec<f64> {
    (0..4).map(|i| k as f64 + i as f64).collect()
}
/// `base(k)` with the interior moved: the same min and max (so the
/// v_min / v_max prefilter passes), other bytes.
fn pinned(k: usize) -> Vec<f64> {
    let mut v = base(k);
    v[1] += 0.25;
    v[2] += 0.5;
    v
}

fn fresh(dir: &Path) -> PathBuf {
    let db = dir.join("hash.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    db
}

fn names(n: usize) -> Vec<String> {
    (0..n).map(|k| format!("e{k}")).collect()
}

/// One array per entity of `ents`, entity `k`'s payload `pay(k)`.
fn arrays(ents: &[String], pay: impl Fn(usize) -> Vec<f64>) -> Segment {
    let (mut e, mut t, mut v) = (vec![], vec![], vec![]);
    for (k, name) in ents.iter().enumerate() {
        for (i, x) in pay(k).into_iter().enumerate() {
            e.push(name.as_str());
            t.push(T[i]);
            v.push(x);
        }
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(e)) as ArrayRef,
            Arc::new(Int64Array::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap();
    Segment {
        class: "node".into(),
        parameter: "a".into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains: vec![],
    }
}

fn commit_opts(db: &Path, seg: Segment, opts: WriteOptions) -> WriteReport {
    commit_changes(db, vec![seg], vec![], vec![], vec![], "t", "m", opts).expect("commit")
}

fn commit(db: &Path, seg: Segment) -> WriteReport {
    commit_opts(db, seg, WriteOptions::default())
}

fn exec(db: &Path, sql: &str) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(sql).unwrap();
    conn.close().unwrap();
}

fn q(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// The live rows' stored hashes equal their payloads' (the hist may hold
/// a planted one).
fn assert_live_hashes(db: &Path, ents: &[String], pay: impl Fn(usize) -> Vec<f64>) {
    let conn = Connection::open(db).unwrap();
    for (k, name) in ents.iter().enumerate() {
        let h: Option<u64> = conn
            .query_row(
                "SELECT v.content_hash FROM value_array v JOIN entity e ON e.id = v.entity_id \
                 WHERE e.name = ?",
                [name],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(h, Some(array_payload_hash(&pay(k))), "{name}");
    }
    conn.close().unwrap();
}

/// The set form (three keys in one round) and the point form (one key).
const FORMS: [(&str, usize); 2] = [("set", 3), ("point", 1)];

#[test]
fn every_core_row_carries_the_true_hash() {
    for (form, n) in FORMS {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path());
        let ents = names(n);
        commit(&db, arrays(&ents, base));
        assert_live_hashes(&db, &ents, base);
        // A changed rewrite (retained pre-image), then one whose superseded
        // version is a stub, then force_replace (also a stub), then a delete.
        commit(&db, arrays(&ents, pinned));
        let stub = WriteOptions {
            retain_override: Some(false),
            ..Default::default()
        };
        commit_opts(&db, arrays(&ents, |k| base(k + 10)), stub);
        let force = WriteOptions {
            force_replace: true,
            ..Default::default()
        };
        commit_opts(&db, arrays(&ents, |k| base(k + 20)), force);
        assert_live_hashes(&db, &ents, |k| base(k + 20));
        let stubs = q(
            &db,
            "SELECT count(*) FROM value_array_hist WHERE NOT content_retained",
        );
        assert!(stubs >= n as i64, "{form}: {stubs} stubs");
        assert_eq!(
            q(
                &db,
                "SELECT count(*) FROM value_array_hist \
                 WHERE NOT content_retained AND content_hash IS NOT NULL"
            ),
            0,
            "{form}: a stub carries no hash"
        );
        delete_values(
            &db,
            vec![DeleteKey {
                class: "node".into(),
                parameter: "a".into(),
                alternative: "Base".into(),
                entity: ents[0].clone(),
            }],
            "t",
            "del",
            WriteOptions::default(),
        )
        .expect("delete");
        let rows = assert_array_hashes(&db);
        // Live: n - 1; hist: n retained, n + n stubs, 1 deleted (retained).
        assert_eq!(rows, 4 * n, "{form}: rows checked");
    }
}

/// [N2] A stored hash that differs from the payload's, over equal bytes:
/// the write is a change (the mismatch alone proved it: the bytes are
/// equal), its new row carries the right hash, and the next identical
/// write is unchanged (self-healing).
#[test]
fn a_stale_hash_over_equal_bytes_is_a_change_that_heals() {
    for (form, n) in FORMS {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path());
        let ents = names(n);
        commit(&db, arrays(&ents, base));
        exec(&db, "UPDATE value_array SET content_hash = 12345");
        let hist = q(&db, "SELECT count(*) FROM value_array_hist");
        let r = commit(&db, arrays(&ents, base));
        assert!(r.commit_id > 0, "{form}: {r:?}");
        assert_eq!((r.values_written, r.values_unchanged), (n, 0), "{form}");
        assert_eq!(
            q(&db, "SELECT count(*) FROM value_array_hist"),
            hist + n as i64,
            "{form}: one version per key"
        );
        assert_live_hashes(&db, &ents, base);
        let r = commit(&db, arrays(&ents, base));
        assert_eq!(r.commit_id, 0, "{form}: healed, unchanged: {r:?}");
        assert_eq!(r.values_unchanged, n, "{form}");
    }
}

/// [N2] A stored hash equal to the new payload's over different bytes (a
/// forced collision: the payload replaced by SQL, the hash kept): the
/// bytes decide, the write is a change.
#[test]
fn an_equal_hash_over_other_bytes_is_a_change() {
    for (form, n) in FORMS {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path());
        let ents = names(n);
        commit(&db, arrays(&ents, base));
        // Same length, same min and max: only the bytes differ.
        exec(
            &db,
            "UPDATE value_array SET values = [values[1], values[2] + 0.125, values[3], values[4]]",
        );
        let r = commit(&db, arrays(&ents, base));
        assert!(r.commit_id > 0, "{form}: {r:?}");
        assert_eq!((r.values_written, r.values_unchanged), (n, 0), "{form}");
        assert_live_hashes(&db, &ents, base);
    }
}

/// A NULL hash is "unknown": the bytes decide, so an identical write is
/// unchanged; a changed one gets a hash.
#[test]
fn a_null_hash_falls_back_to_the_bytes() {
    for (form, n) in FORMS {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path());
        let ents = names(n);
        commit(&db, arrays(&ents, base));
        exec(&db, "UPDATE value_array SET content_hash = NULL");
        let r = commit(&db, arrays(&ents, base));
        assert_eq!(r.commit_id, 0, "{form}: {r:?}");
        assert_eq!(r.values_unchanged, n, "{form}");
        let r = commit(&db, arrays(&ents, pinned));
        assert_eq!((r.values_written, r.values_unchanged), (n, 0), "{form}");
        assert_live_hashes(&db, &ents, pinned);
    }
}

/// A raw-SQL row (no hash) with a payload equal to the next write: the
/// write is unchanged.
#[test]
fn a_raw_sql_row_without_a_hash_compares_by_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh(dir.path());
    let ents = names(1);
    commit(&db, arrays(&ents, base));
    // Re-create the live row by hand, without a hash (as a fixture or a
    // user's SQL would).
    exec(
        &db,
        "CREATE TEMP TABLE r AS SELECT * EXCLUDE (array_id, content_hash) FROM value_array; \
         DELETE FROM value_array; \
         INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id, \
           parameter_type_id, valid_from_commit, version, axis1_id, axis2_id, axis3_id, \
           axis4_id, values, values_int, v_min, v_max) SELECT * FROM r;",
    );
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM value_array WHERE content_hash IS NULL"
        ),
        1
    );
    let r = commit(&db, arrays(&ents, base));
    assert_eq!(r.commit_id, 0, "{r:?}");
}

/// A change that keeps every array's min and max (the prefilter passes on
/// v_min / v_max; the stored hash proves the change): every key is
/// written, with the right hash.
#[test]
fn a_pinned_min_max_change_is_written() {
    for (form, n) in FORMS {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path());
        let ents = names(n);
        commit(&db, arrays(&ents, base));
        let r = commit(&db, arrays(&ents, pinned));
        assert!(r.commit_id > 0, "{form}");
        assert_eq!((r.values_written, r.values_unchanged), (n, 0), "{form}");
        assert_live_hashes(&db, &ents, pinned);
        assert_eq!(assert_array_hashes(&db), 2 * n, "{form}");
    }
}
