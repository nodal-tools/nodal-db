//! Template float literals are canonical at the template's own
//! canonicalization point (`Lit::resolve`): a `-0.0` default or value-list
//! member is stored as `+0.0`, the template converges (the next plan is
//! empty), and a list naming both `0.0` and `-0.0` is a duplicate member
//! (they are one stored value).

use std::path::Path;

use duckdb::Connection;

use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::{apply, parse_str, plan, ApplyOptions, MigrationRegistry};

const NEG_ZERO: &str = r#"
template: {name: fz, version: "1"}
value_lists:
  - name: zl
    members:
      - {value: -0.0, kind: float}
      - {value: 1.5, kind: float}
classes:
  - name: widget
    parameters:
      - name: size
        types: [{kind: float}]
        default: {kind: float, value: -0.0}
"#;

fn floats(db: &Path, sql: &str) -> Vec<u64> {
    let conn = Connection::open(db).unwrap();
    let v: Vec<u64> = conn
        .prepare(sql)
        .unwrap()
        .query_map([], |r| r.get::<_, f64>(0))
        .unwrap()
        .map(|x| x.unwrap().to_bits())
        .collect();
    conn.close().unwrap();
    v
}

#[test]
fn negative_zero_literals_store_as_positive_zero_and_converge() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    let t = parse_str(NEG_ZERO).unwrap();
    apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap();

    assert_eq!(
        floats(
            &db,
            "SELECT v_float FROM definition_value_scalar WHERE valid_to_commit IS NULL"
        ),
        vec![0],
        "default -0.0 stored as +0.0"
    );
    assert_eq!(
        floats(
            &db,
            "SELECT v_float FROM list_value WHERE valid_to_commit IS NULL ORDER BY v_float"
        ),
        vec![0, 1.5f64.to_bits()],
        "member -0.0 stored as +0.0"
    );

    let p = plan(&db, &t).unwrap();
    assert!(
        p.actions.is_empty(),
        "the template converges: {:?}",
        p.actions
    );
}

#[test]
fn zero_and_negative_zero_members_are_duplicates() {
    let doc = r#"
template: {name: fz, version: "1"}
value_lists:
  - name: zl
    members:
      - {value: 0.0, kind: float}
      - {value: -0.0, kind: float}
classes: []
"#;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("t.duckdb");
    init_schema(&db).unwrap();
    let t = parse_str(doc).unwrap();
    let err = apply(
        &db,
        &t,
        &MigrationRegistry::new(),
        "tester",
        "",
        &ApplyOptions::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("duplicate member value"), "{err}");
}
