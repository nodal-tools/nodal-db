//! Helpers shared by several integration tests (`mod common;`).

use std::path::Path;

use duckdb::types::Value;
use duckdb::Connection;
use nodal_db_core::encoding::{array_payload_hash, array_payload_hash_i64};

fn doubles(v: &Value) -> Option<Vec<f64>> {
    match v {
        Value::Null => None,
        Value::List(items) => Some(
            items
                .iter()
                .map(|x| match x {
                    Value::Double(d) => *d,
                    other => panic!("a DOUBLE[] element read as {other:?}"),
                })
                .collect(),
        ),
        other => panic!("a DOUBLE[] payload read as {other:?}"),
    }
}

fn bigints(v: &Value) -> Option<Vec<i64>> {
    match v {
        Value::Null => None,
        Value::List(items) => Some(
            items
                .iter()
                .map(|x| match x {
                    Value::BigInt(i) => *i,
                    other => panic!("a BIGINT[] element read as {other:?}"),
                })
                .collect(),
        ),
        other => panic!("a BIGINT[] payload read as {other:?}"),
    }
}

/// Every core-written array row carries the true payload hash
/// (skip-unchanged P3): each `value_array` row and each retained
/// `value_array_hist` row has `content_hash = array_payload_hash(payload)`,
/// each stub (`content_retained = FALSE`) a NULL hash. Only for files the
/// core alone wrote (a raw-SQL row has a NULL hash). Returns the rows
/// checked.
#[allow(dead_code)]
pub fn assert_array_hashes(db: &Path) -> usize {
    let conn = Connection::open(db).unwrap();
    let mut n = 0;
    for (table, sql) in [
        (
            "value_array",
            "SELECT array_id, version, values, values_int, content_hash, TRUE \
             FROM value_array",
        ),
        (
            "value_array_hist",
            "SELECT array_id, version, values, values_int, content_hash, content_retained \
             FROM value_array_hist",
        ),
    ] {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Some(r) = rows.next().unwrap() {
            let id: i64 = r.get(0).unwrap();
            let version: i16 = r.get(1).unwrap();
            let retained: bool = r.get(5).unwrap();
            let stored: Option<u64> = r.get(4).unwrap();
            let what = format!("{table} array_id {id} version {version}");
            if !retained {
                assert_eq!(stored, None, "{what}: a stub carries no hash");
            } else {
                let want = match (
                    doubles(&r.get::<_, Value>(2).unwrap()),
                    bigints(&r.get::<_, Value>(3).unwrap()),
                ) {
                    (Some(v), None) => array_payload_hash(&v),
                    (None, Some(v)) => array_payload_hash_i64(&v),
                    other => panic!("{what}: payload columns {other:?}"),
                };
                assert_eq!(stored, Some(want), "{what}: content_hash");
            }
            n += 1;
        }
    }
    conn.close().unwrap();
    n
}
