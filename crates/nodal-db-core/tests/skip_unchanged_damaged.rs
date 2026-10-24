//! Unchanged-write detection against damaged or out-of-contract stored
//! rows (skip_unchanged_writes_PLAN critique C3): whatever the stored side
//! looks like, a write that is not provably equal to it goes ahead. Each
//! case writes a value, tampers with its stored rows through raw SQL, then
//! writes the original value again through `commit_changes` and requires a
//! real write (a commit, `values_written == 1`, nothing unchanged), for the
//! point statements (one entity) and the set-based round (three).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, Segment};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{WriteOptions, WriteReport};

fn fresh(dir: &Path) -> PathBuf {
    let db = dir.join("damaged.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    db
}

fn scalar_batch(entities: &[&str], vals: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(entities.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn series(entities: &[&str], t: &[i64], vals: ArrayRef) -> RecordBatch {
    let mut ent = Vec::new();
    let mut tt = Vec::new();
    for e in entities {
        for &x in t {
            ent.push(*e);
            tt.push(x);
        }
    }
    let vtype = vals.data_type().clone();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Int64, false),
            Field::new("value", vtype, false),
        ])),
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(tt)) as ArrayRef,
            vals,
        ],
    )
    .unwrap()
}

fn seg(parameter: &str, batch: RecordBatch) -> Segment {
    Segment {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains: vec![],
    }
}

fn commit(db: &Path, segments: Vec<Segment>) -> WriteReport {
    commit_changes(
        db,
        segments,
        vec![],
        vec![],
        vec![],
        "t",
        "m",
        WriteOptions::default(),
    )
    .expect("commit")
}

fn sql(db: &Path, s: &str) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(s).unwrap();
    conn.close().unwrap();
}

fn q(db: &Path, s: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n: i64 = conn.query_row(s, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

/// Write `segs()`, check the identical rewrite is unchanged, run `tamper`
/// on the stored rows of entity `a`, then rewrite: `a` must be written
/// again, the other entities stay unchanged.
fn case(what: &str, segs: impl Fn(&[&str]) -> Vec<Segment>, tamper: &str) {
    for ents in [&["a"][..], &["a", "b", "c"][..]] {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(dir.path());
        assert!(commit(&db, segs(ents)).commit_id > 0);
        let same = commit(&db, segs(ents));
        assert_eq!(
            (same.commit_id, same.values_unchanged),
            (0, ents.len()),
            "{what}: the untampered rewrite must be unchanged"
        );
        let a = q(&db, "SELECT id FROM entity WHERE name = 'a'");
        sql(&db, &tamper.replace("$A", &a.to_string()));
        let r = commit(&db, segs(ents));
        assert!(
            r.commit_id > 0,
            "{what} ({} ents): no commit {r:?}",
            ents.len()
        );
        assert_eq!(
            (r.values_written, r.values_unchanged),
            (1, ents.len() - 1),
            "{what} ({} ents): {r:?}",
            ents.len()
        );
    }
}

fn scalars(ents: &[&str]) -> Vec<Segment> {
    vec![seg("s", scalar_batch(ents, &vec![0.0; ents.len()]))]
}

fn longs(ents: &[&str]) -> Vec<Segment> {
    let t = [0, 2, 5];
    let vals: Vec<i64> = (0..t.len() * ents.len()).map(|i| i as i64).collect();
    vec![seg("l", series(ents, &t, Arc::new(Int64Array::from(vals))))]
}

fn arrays(ents: &[&str]) -> Vec<Segment> {
    let t = [0, 1, 2, 3];
    let vals: Vec<f64> = (0..t.len() * ents.len())
        .map(|i| (i % 4) as f64 * 0.5)
        .collect();
    vec![seg(
        "a",
        series(ents, &t, Arc::new(Float64Array::from(vals))),
    )]
}

const CUR: &str = "entity_id = $A AND valid_to_commit IS NULL";

/// C3.1: the stored side is not re-canonicalized, so a stored `-0.0` (out
/// of contract: canon writes `0.0`) is replaced by a new `0.0`. (No NaN
/// twin: DuckDB returns every stored NaN, whatever payload was bound, as
/// the canonical quiet NaN `0x7ff8_0000_0000_0000`, checked by hand on the
/// bundled engine, so a non-canonical NaN cannot be stored.)
#[test]
fn stored_negative_zero_is_a_change() {
    case(
        "-0.0",
        scalars,
        &format!("UPDATE value_scalar SET v_float = -0.0::DOUBLE WHERE {CUR}"),
    );
}

#[test]
fn stored_second_payload_column_is_a_change() {
    case(
        "two payload columns",
        scalars,
        &format!("UPDATE value_scalar SET v_int = 0 WHERE {CUR}"),
    );
}

#[test]
fn stored_scalar_stub_or_null_binding_or_kind_is_a_change() {
    case(
        "scalar stub",
        scalars,
        &format!("UPDATE value_scalar SET content_retained = FALSE WHERE {CUR}"),
    );
    case(
        "scalar NULL binding",
        scalars,
        &format!("UPDATE value_scalar SET parameter_type_id = NULL WHERE {CUR}"),
    );
    case(
        "scalar kind",
        scalars,
        &format!("UPDATE value_scalar SET value_kind = 'int' WHERE {CUR}"),
    );
}

#[test]
fn damaged_long_cells_are_a_change() {
    case(
        "long: a missing cell",
        longs,
        &format!("DELETE FROM value_long WHERE {CUR} AND i1_pos = (SELECT max(i1_pos) FROM value_long WHERE {CUR})"),
    );
    case(
        "long: one cell's payload",
        longs,
        &format!("UPDATE value_long SET v_int = v_int + 1 WHERE {CUR} AND i1_pos = (SELECT max(i1_pos) FROM value_long WHERE {CUR})"),
    );
    case(
        "long: one cell's kind",
        longs,
        &format!("UPDATE value_long SET value_kind = 'float' WHERE {CUR} AND i1_pos = (SELECT max(i1_pos) FROM value_long WHERE {CUR})"),
    );
    case(
        "long: one cell stubbed",
        longs,
        &format!("UPDATE value_long SET content_retained = FALSE, v_int = NULL WHERE {CUR} AND i1_pos = (SELECT max(i1_pos) FROM value_long WHERE {CUR})"),
    );
    case(
        "long: one cell's binding NULL",
        longs,
        &format!("UPDATE value_long SET parameter_type_id = NULL WHERE {CUR} AND i1_pos = (SELECT max(i1_pos) FROM value_long WHERE {CUR})"),
    );
    case(
        "long: one cell on another axis",
        longs,
        &format!(
            "INSERT INTO axis SELECT * REPLACE (id + 1000 AS id) FROM axis \
               WHERE id = (SELECT max(axis1_id) FROM value_long WHERE {CUR}); \
             UPDATE value_long SET axis1_id = axis1_id + 1000 WHERE {CUR} \
               AND i1_pos = (SELECT max(i1_pos) FROM value_long WHERE {CUR})"
        ),
    );
}

#[test]
fn damaged_arrays_are_a_change() {
    case(
        "array: a longer payload, same bounds",
        arrays,
        "UPDATE value_array SET values = list_append(values, values[2]) WHERE entity_id = $A",
    );
    case(
        "array: an interior element",
        arrays,
        "UPDATE value_array SET values = [values[1], values[2] + 1e-12, values[3], values[4]] WHERE entity_id = $A",
    );
    case(
        "array: NULL binding",
        arrays,
        "UPDATE value_array SET parameter_type_id = NULL WHERE entity_id = $A",
    );
    case(
        "array: a NULL element",
        arrays,
        "UPDATE value_array SET values = [values[1], NULL, values[3], values[4]] WHERE entity_id = $A",
    );
}
