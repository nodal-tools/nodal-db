//! Equality conformance for the point-read macros (fast_point_reads_PLAN §7.2):
//! the drift guard between their second copy of the rendering and
//! `value_rows` / `read_parameter`.
//!
//! - `read_values(c, p, E, A)` = `read_parameter(c, p) WHERE lower(entity) IN E
//!   AND lower(alternative) IN A`, and `read_value` likewise;
//! - `value_rows_for(pd, ids, alt_ids)` = `value_rows WHERE <keys>`;
//!
//! compared as multisets of rows (both directions of `EXCEPT ALL`) AND by
//! `DESCRIBE` column names and types. The fixture is core-built: every tier,
//! the scalar kinds the write API takes (float, int, bool, string, datetime),
//! ranks 1-3, parametric and irregular datetime and integer axes, label axes,
//! a parameter straddling the array and long tiers, two classes sharing entity
//! and parameter names, three alternatives, replaced and deleted values, a
//! deleted entity, an erased alternative, and an int array (`values_int`),
//! which the core's writer never produces, inserted directly. The smoke suite
//! (`schema/smoke_test.py`) runs the same equality over every value of its
//! hand-built database (period, time-pattern and float axes, durations,
//! rank 4).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{delete_objects, delete_values, write_parameter, DeleteKey};
use nodal_db_core::canon::LabelDomain;
use nodal_db_core::erasure::{erase_alternative, EraseOpts};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef, WriteOptions,
};

const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;

// --- fixture -----------------------------------------------------------------

fn ts_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn strs(v: &[String]) -> ArrayRef {
    Arc::new(StringArray::from(v.to_vec()))
}

/// Rank-1 series over `hours` (datetime index) for each entity.
fn dt_series(ents: &[&str], hours: &[i64], k: f64) -> RecordBatch {
    let (mut e, mut t, mut v) = (vec![], vec![], vec![]);
    for (i, name) in ents.iter().enumerate() {
        for (j, h) in hours.iter().enumerate() {
            e.push(name.to_string());
            t.push(T0_US + h * HOUR_US);
            v.push(k * (i * 100 + j) as f64);
        }
    }
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            ts_field("time"),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            strs(&e),
            Arc::new(TimestampMicrosecondArray::from(t)),
            Arc::new(Float64Array::from(v)),
        ],
    )
}

/// Rank-1 series over integer coordinates; `int_measure` puts it in the long tier.
fn int_indexed(ents: &[&str], coords: &[i64], int_measure: bool) -> RecordBatch {
    let (mut e, mut c, mut vf, mut vi) = (vec![], vec![], vec![], vec![]);
    for (i, name) in ents.iter().enumerate() {
        for (j, x) in coords.iter().enumerate() {
            e.push(name.to_string());
            c.push(*x);
            vf.push((i * 10 + j) as f64 + 0.25);
            vi.push((i * 10 + j) as i64);
        }
    }
    let (vfield, val): (Field, ArrayRef) = if int_measure {
        (
            Field::new("value", DataType::Int64, false),
            Arc::new(Int64Array::from(vi)),
        )
    } else {
        (
            Field::new("value", DataType::Float64, false),
            Arc::new(Float64Array::from(vf)),
        )
    };
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("i", DataType::Int64, false),
            vfield,
        ],
        vec![strs(&e), Arc::new(Int64Array::from(c)), val],
    )
}

/// Rank-2 (label x datetime) float series; `sparse` drops cells (long tier).
fn label_dt(ents: &[&str], sparse: bool) -> RecordBatch {
    let (mut e, mut l, mut t, mut v) = (vec![], vec![], vec![], vec![]);
    for (i, name) in ents.iter().enumerate() {
        for (j, lab) in ["x", "y", "z"].iter().enumerate() {
            for h in 0..4i64 {
                if sparse && (j + h as usize) % 2 == 1 {
                    continue;
                }
                e.push(name.to_string());
                l.push(lab.to_string());
                t.push(T0_US + h * HOUR_US);
                v.push((i * 100 + j * 10) as f64 + h as f64);
            }
        }
    }
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("lab", DataType::Utf8, false),
            ts_field("time"),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            strs(&e),
            strs(&l),
            Arc::new(TimestampMicrosecondArray::from(t)),
            Arc::new(Float64Array::from(v)),
        ],
    )
}

/// Rank-3 (label x label x integer) float array.
fn rank3(ents: &[&str]) -> RecordBatch {
    let (mut e, mut a, mut b, mut c, mut v) = (vec![], vec![], vec![], vec![], vec![]);
    for name in ents {
        for x in ["p", "q"] {
            for y in ["m", "n"] {
                for i in 0..3i64 {
                    e.push(name.to_string());
                    a.push(x.to_string());
                    b.push(y.to_string());
                    c.push(i);
                    v.push(i as f64 + if x == "p" { 0.5 } else { 1.5 });
                }
            }
        }
    }
    batch(
        vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ],
        vec![
            strs(&e),
            strs(&a),
            strs(&b),
            Arc::new(Int64Array::from(c)),
            Arc::new(Float64Array::from(v)),
        ],
    )
}

fn scalar(ents: &[&str], col: ArrayRef) -> RecordBatch {
    let names: Vec<String> = ents.iter().map(|s| s.to_string()).collect();
    let field = Field::new("value", col.data_type().clone(), false);
    batch(
        vec![Field::new("entity", DataType::Utf8, false), field],
        vec![strs(&names), col],
    )
}

fn w(db: &Path, class: &str, param: &str, alt: &str, b: RecordBatch, dom: &[LabelDomain]) {
    write_parameter(
        db,
        class,
        param,
        alt,
        b,
        dom,
        WriteOptions::default(),
        "tester",
        "fixture",
    )
    .unwrap_or_else(|e| panic!("write {class}.{param}/{alt}: {e}"));
}

fn dom(position: u8, labels: &[&str]) -> LabelDomain {
    LabelDomain {
        position,
        labels: labels.iter().map(|s| s.to_string()).collect(),
    }
}

const ENTS: [&str; 6] = ["u0", "u1", "u2", "u3", "U_Mixed", "gone"];
const ALTS: [&str; 3] = ["Base", "Alt1", "alt2"];

fn fixture(dir: &Path) -> PathBuf {
    let db = dir.join("eq.duckdb");
    init_schema_with_default(&db, Some("Base")).expect("init");
    for (k, alt) in ALTS.iter().enumerate() {
        let kf = 1.0 + k as f64;
        w(
            &db,
            "unit",
            "series",
            alt,
            dt_series(&ENTS, &[0, 1, 2, 3, 4, 5], kf),
            &[],
        );
        w(
            &db,
            "unit",
            "irregular",
            alt,
            dt_series(&ENTS[..3], &[0, 1, 5, 6, 20], kf),
            &[],
        );
        w(
            &db,
            "unit",
            "by_int",
            alt,
            int_indexed(&ENTS[..4], &[0, 3, 6, 9], false),
            &[],
        );
        w(
            &db,
            "unit",
            "by_irr_int",
            alt,
            int_indexed(&ENTS[..4], &[1, 2, 7], false),
            &[],
        );
        w(
            &db,
            "unit",
            "counts",
            alt,
            int_indexed(&ENTS[..4], &[0, 2, 5], true),
            &[],
        );
        w(
            &db,
            "unit",
            "grid",
            alt,
            label_dt(&ENTS[..3], false),
            &[dom(1, &["x", "y", "z"])],
        );
        w(
            &db,
            "unit",
            "sparse",
            alt,
            label_dt(&ENTS[..3], true),
            &[dom(1, &["x", "y", "z"])],
        );
        w(
            &db,
            "unit",
            "cube",
            alt,
            rank3(&ENTS[..2]),
            &[dom(1, &["p", "q"]), dom(2, &["m", "n"])],
        );
        let e5 = &ENTS[..5];
        w(
            &db,
            "unit",
            "s_float",
            alt,
            scalar(
                e5,
                Arc::new(Float64Array::from(vec![kf, 2.5, -1.0, 0.0, 9.0])),
            ),
            &[],
        );
        w(
            &db,
            "unit",
            "s_int",
            alt,
            scalar(
                &ENTS[..3],
                Arc::new(Int64Array::from(vec![1, k as i64, -3])),
            ),
            &[],
        );
        w(
            &db,
            "unit",
            "s_bool",
            alt,
            scalar(&ENTS[..2], Arc::new(BooleanArray::from(vec![true, k == 1]))),
            &[],
        );
        w(
            &db,
            "unit",
            "s_str",
            alt,
            scalar(&ENTS[..2], Arc::new(StringArray::from(vec!["a", ALTS[k]]))),
            &[],
        );
        w(
            &db,
            "unit",
            "s_dt",
            alt,
            scalar(
                &ENTS[..2],
                Arc::new(TimestampMicrosecondArray::from(vec![
                    T0_US,
                    T0_US + k as i64 * HOUR_US,
                ])),
            ),
            &[],
        );
    }
    // A parameter straddling the array and long tiers (same rank and kinds):
    // u0 full Cartesian (array), u1 sparse (long).
    w(
        &db,
        "unit",
        "mix",
        "Base",
        label_dt(&["u0"], false),
        &[dom(1, &["x", "y", "z"])],
    );
    w(
        &db,
        "unit",
        "mix",
        "Base",
        label_dt(&["u1"], true),
        &[dom(1, &["x", "y", "z"])],
    );
    // Another class with the same entity and parameter names.
    w(
        &db,
        "node",
        "series",
        "Base",
        dt_series(&ENTS[..3], &[0, 1, 2, 3, 4, 5], -1.0),
        &[],
    );
    w(
        &db,
        "node",
        "s_float",
        "Base",
        scalar(&ENTS[..2], Arc::new(Float64Array::from(vec![-5.0, -6.0]))),
        &[],
    );
    // Replaced values (closed-out rows and array history).
    w(
        &db,
        "unit",
        "series",
        "Base",
        dt_series(&ENTS[..2], &[0, 1, 2, 3, 4, 5], 7.0),
        &[],
    );
    w(
        &db,
        "unit",
        "s_float",
        "Base",
        scalar(&ENTS[..1], Arc::new(Float64Array::from(vec![99.0]))),
        &[],
    );
    w(
        &db,
        "unit",
        "counts",
        "Base",
        int_indexed(&ENTS[..1], &[0, 2, 5, 8], true),
        &[],
    );
    // Deleted values.
    let del = |p: &str, e: &str| DeleteKey {
        class: "unit".into(),
        parameter: p.into(),
        alternative: "Base".into(),
        entity: e.into(),
    };
    delete_values(
        &db,
        vec![
            del("series", "u2"),
            del("s_float", "u1"),
            del("counts", "u3"),
        ],
        "tester",
        "deletes",
        WriteOptions::default(),
    )
    .expect("delete values");
    // A deleted entity (closed-out entity row and its values).
    delete_objects(
        &db,
        vec![(
            ObjectDeleteKey {
                kind: ObjectKind::Entity,
                class: Some("unit".into()),
                target: ObjectRef::Name("gone".into()),
            },
            ObjectDeleteOpts { cascade: true },
        )],
        "tester",
        "delete entity",
        WriteOptions::default(),
    )
    .expect("delete entity");
    // An erased alternative (its values physically removed with a marker).
    w(
        &db,
        "unit",
        "series",
        "scratch",
        dt_series(&ENTS[..2], &[0, 1, 2], 3.0),
        &[],
    );
    w(
        &db,
        "unit",
        "s_float",
        "scratch",
        scalar(&ENTS[..1], Arc::new(Float64Array::from(vec![1.0]))),
        &[],
    );
    let rep = erase_alternative(
        &db,
        &ObjectDeleteKey {
            kind: ObjectKind::Alternative,
            class: None,
            target: ObjectRef::Name("scratch".into()),
        },
        &EraseOpts::default(),
        "tester",
    )
    .expect("erase");
    assert!(rep.erased);
    // An int array (values_int): the writer sends int series to the long tier,
    // so insert one directly, on u3's by_int axis.
    // (A test fixture writing what the public API cannot, as the smoke suite
    // and array_id_semantics.rs do; the replaced row gets no history.)
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        "INSERT INTO commit (user, message) VALUES ('tester', 'int array');
         CREATE TEMP TABLE k AS
           SELECT va.array_id, va.entity_id, va.parameter_definition_id, va.alternative_id,
                  va.axis1_id
           FROM value_array va
           JOIN parameter_definition pd ON pd.id = va.parameter_definition_id
           JOIN entity e ON e.id = va.entity_id
           JOIN entity_class ec ON ec.id = pd.entity_class_id
           WHERE ec.name = 'unit' AND pd.name = 'by_int' AND e.name = 'u3'
             AND va.alternative_id = (SELECT id FROM alternative WHERE name = 'alt2');
         DELETE FROM value_array WHERE array_id = (SELECT array_id FROM k);
         INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id,
             valid_from_commit, version, axis1_id, values_int, v_min, v_max)
           SELECT entity_id, parameter_definition_id, alternative_id, currval('seq_commit'),
                  1, axis1_id, [5, -1, 9007199254740993, 0], -1, 9007199254740993
           FROM k;",
    )
    .expect("int array");
    conn.close().unwrap();
    db
}

// --- comparison ----------------------------------------------------------------

fn rows(conn: &Connection, sql: &str) -> Vec<(String, String)> {
    let mut st = conn.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn n(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// Same column names and types, and the same multiset of rows. Returns the
/// row count.
fn same(conn: &Connection, label: &str, got: &str, want: &str) -> i64 {
    let tg = rows(
        conn,
        &format!("SELECT column_name, column_type FROM (DESCRIBE {got})"),
    );
    let tw = rows(
        conn,
        &format!("SELECT column_name, column_type FROM (DESCRIBE {want})"),
    );
    assert_eq!(
        tg, tw,
        "{label}: column names / types differ\n{got}\n{want}"
    );
    let sql = format!(
        "WITH g AS MATERIALIZED ({got}), w AS MATERIALIZED ({want}) \
         SELECT (SELECT count(*) FROM (FROM g EXCEPT ALL FROM w)), \
                (SELECT count(*) FROM (FROM w EXCEPT ALL FROM g)), \
                (SELECT count(*) FROM g)"
    );
    let (only_got, only_want, total): (i64, i64, i64) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap_or_else(|e| panic!("{label}: {e}\n{sql}"));
    assert_eq!(
        (only_got, only_want),
        (0, 0),
        "{label}: rows differ (only in the macro, only in the reference)\n{got}\n{want}"
    );
    total
}

fn list(xs: &[&str]) -> String {
    let v: Vec<String> = xs.iter().map(|x| format!("'{x}'")).collect();
    format!("[{}]::VARCHAR[]", v.join(", "))
}

fn read_values_ref(class: &str, param: &str, ents: &str, alts: Option<&str>) -> String {
    let alt_filter = alts
        .map(|a| format!(" AND lower(alternative) IN (SELECT lower(unnest({a})))"))
        .unwrap_or_default();
    format!(
        "SELECT * FROM read_parameter('{class}', '{param}') \
         WHERE lower(entity) IN (SELECT lower(unnest({ents}))){alt_filter}"
    )
}

// --- tests -----------------------------------------------------------------------

#[test]
fn read_values_equals_filtered_read_parameter() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = Connection::open(&db).unwrap();
    let params: Vec<(String, String)> = rows(
        &conn,
        "SELECT ec.name, pd.name FROM parameter_definition pd \
         JOIN entity_class ec ON ec.id = pd.entity_class_id \
         WHERE pd.valid_to_commit IS NULL ORDER BY 1, 2",
    );
    assert!(params.len() >= 15, "fixture parameters: {params:?}");
    // Entity and alternative sets, paired (not crossed: each read plans two
    // macros, and the cross product made the test slow without adding cases
    // of a different kind).
    let sets: Vec<(Vec<&str>, Option<Vec<&str>>)> = vec![
        (vec!["u0"], Some(vec!["Base"])),
        (vec!["U1", "u2", "u3"], Some(vec!["alt1", "ALT2"])),
        (vec!["u1", "u1", "U1"], Some(vec!["base", "Base", "nope"])),
        (vec!["u_mixed", "nope"], None),
        (vec!["gone"], None),
        (ENTS.to_vec(), None),
        (ENTS.to_vec(), Some(vec!["scratch"])),
        (ENTS.to_vec(), Some(vec!["Base", "alt2"])),
        (vec![], Some(vec![])),
        (vec!["u0", "u2"], Some(vec![])),
    ];
    let mut nonempty = 0;
    let mut cases = 0;
    for (class, param) in &params {
        for (es, al) in &sets {
            let ents = list(es);
            let (got, want) = match al {
                Some(a) => {
                    let alts = list(a);
                    (
                        format!(
                            "SELECT * FROM read_values('{}', '{param}', {ents}, {alts})",
                            class.to_uppercase()
                        ),
                        read_values_ref(class, param, &ents, Some(&alts)),
                    )
                }
                None => (
                    format!("SELECT * FROM read_values('{class}', '{param}', {ents})"),
                    read_values_ref(class, param, &ents, None),
                ),
            };
            if same(
                &conn,
                &format!("{class}.{param} {es:?} {al:?}"),
                &got,
                &want,
            ) > 0
            {
                nonempty += 1;
            }
            cases += 1;
        }
        // read_value, with and without the alternative (critique M2).
        for e in ["u0", "U1"] {
            same(
                &conn,
                &format!("read_value {class}.{param} {e} Base"),
                &format!("SELECT * FROM read_value('{class}', '{param}', '{e}', 'BASE')"),
                &read_values_ref(class, param, &list(&[e]), Some(&list(&["base"]))),
            );
            same(
                &conn,
                &format!("read_value {class}.{param} {e}, alternative omitted"),
                &format!("SELECT * FROM read_value('{class}', '{param}', '{e}')"),
                &read_values_ref(class, param, &list(&[e]), None),
            );
        }
    }
    assert!(
        nonempty > cases / 4,
        "too few non-empty cases: {nonempty} of {cases}"
    );

    // Targeted expectations on top of the equality.
    let c = |sql: &str| n(&conn, &format!("SELECT count(*) FROM ({sql})"));
    assert_eq!(
        c("SELECT * FROM read_value('unit', 'series', 'u0', 'Base')"),
        6
    );
    assert_eq!(
        c("SELECT * FROM read_value('unit', 'series', 'u0')"),
        18,
        "D3: 3 alternatives"
    );
    assert_eq!(
        c("SELECT * FROM read_values('unit', 'series', ['u0'], NULL)"),
        18,
        "D3: NULL = all current alternatives"
    );
    assert_eq!(
        c("SELECT * FROM read_values('unit', 'series', ['u0'], [])"),
        0,
        "[] = none"
    );
    assert_eq!(
        c("SELECT * FROM read_value('unit', 'series', 'u2', 'Base')"),
        0,
        "deleted value"
    );
    assert_eq!(
        c("SELECT * FROM read_value('unit', 'series', 'gone')"),
        0,
        "deleted entity"
    );
    assert_eq!(
        c("SELECT * FROM read_value('unit', 'series', 'u0', 'scratch')"),
        0,
        "erased"
    );
    assert_eq!(
        c("SELECT * FROM read_value('nope', 'series', 'u0')"),
        0,
        "missing class"
    );
    assert_eq!(
        c("SELECT * FROM read_value('unit', 'nope', 'u0')"),
        0,
        "missing parameter"
    );
    assert_eq!(
        c("SELECT * FROM read_values('unit', 'series', ['u0', NULL], [NULL, 'Base'])"),
        6,
        "a NULL element matches nothing"
    );
    assert_eq!(
        c("SELECT * FROM read_value('node', 'series', 'u0', 'Base') WHERE v_float <= 0"),
        6,
        "the class restricts the entity: node.u0, not unit.u0"
    );
    let tiers = rows(
        &conn,
        "SELECT DISTINCT entity, tier FROM read_values('unit', 'mix', ['u0', 'u1']) ORDER BY 1",
    );
    assert_eq!(
        tiers,
        vec![("u0".into(), "array".into()), ("u1".into(), "long".into())],
        "the straddling parameter reads as one relation"
    );
    let ints: Vec<(String, String)> = rows(
        &conn,
        "SELECT value_kind, v_int::VARCHAR FROM read_value('unit', 'by_int', 'u3', 'alt2') \
         ORDER BY idx1_num",
    );
    assert_eq!(
        ints,
        vec![
            ("int".into(), "5".into()),
            ("int".into(), "-1".into()),
            ("int".into(), "9007199254740993".into()),
            ("int".into(), "0".into()),
        ],
        "the int array decodes exactly"
    );
    conn.close().unwrap();
}

#[test]
fn value_rows_for_equals_value_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let conn = Connection::open(&db).unwrap();
    let pds: Vec<i64> = {
        let mut st = conn
            .prepare("SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL ORDER BY 1")
            .unwrap();
        st.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let all_e = "(SELECT list(id ORDER BY id) FROM entity)";
    let all_a = "(SELECT list(id ORDER BY id) FROM alternative)";
    for pd in pds {
        for (ents, alts) in [
            (all_e.to_string(), all_a.to_string()),
            (
                "(SELECT list(id ORDER BY id) FROM entity WHERE name IN ('u0', 'u2'))".into(),
                "(SELECT list(id) FROM alternative WHERE name = 'Base')".into(),
            ),
            // duplicate ids (critique M1: no duplicated scalar/long rows)
            (
                format!("list_concat({all_e}, {all_e})"),
                format!("list_concat({all_a}, {all_a})"),
            ),
            // a NULL element and an unknown id
            (
                format!("list_concat({all_e}, [NULL, -7])"),
                all_a.to_string(),
            ),
            ("[]::BIGINT[]".into(), all_a.to_string()),
        ] {
            let got = format!("SELECT * FROM value_rows_for({pd}, {ents}, {alts})");
            let want = format!(
                "SELECT * FROM value_rows WHERE parameter_definition_id = {pd} \
                 AND entity_id IN (SELECT unnest({ents})) \
                 AND alternative_id IN (SELECT unnest({alts}))"
            );
            same(&conn, &format!("pd {pd}"), &got, &want);
        }
    }
    // Unknown parameter: no rows, same columns.
    same(
        &conn,
        "unknown pd",
        &format!("SELECT * FROM value_rows_for(-1, {all_e}, {all_a})"),
        "SELECT * FROM value_rows WHERE false",
    );
    conn.close().unwrap();
}
