//! The value-link and object-link lifecycle outside the metadata channel
//! (`crate::metadata`): a value delete closes the value's links; object
//! deletes close links through the registry legs and links never block a
//! delete without cascade (owner ruling OD2); erasure removes them; the raw
//! per-key writers refuse an annotated value; and a census keeps every
//! value-table closer in the reviewed files.

use std::path::Path;
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;

use nodal_db_core::api::{commit_changes, delete_values, DeleteKey, Segment};
use nodal_db_core::erasure::{erase_alternative, EraseOpts};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteResult, ObjectKind, ObjectRef, WriteOptions,
};

fn batch(entities: &[&str], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(entities.to_vec())),
        Arc::new(Float64Array::from(vals.to_vec())),
    ];
    RecordBatch::try_new(schema, cols).unwrap()
}

fn write(db: &Path, param: &str, alt: &str, entities: &[&str], vals: &[f64]) {
    commit_changes(
        db,
        vec![Segment {
            class: "node".into(),
            parameter: param.into(),
            alternative: alt.into(),
            batches: vec![batch(entities, vals)],
            domains: vec![],
        }],
        vec![],
        vec![],
        vec![],
        "t",
        "w",
        WriteOptions::default(),
    )
    .expect("write");
}

fn q(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    let n = conn.query_row(sql, [], |r| r.get(0)).unwrap();
    conn.close().unwrap();
    n
}

fn exec(db: &Path, sql: &str) {
    let conn = Connection::open(db).unwrap();
    conn.execute_batch(sql).unwrap();
    conn.close().unwrap();
}

/// A file with node.a / node.b values of `p` under Base and alt2, one pair,
/// value links of both under Base, and object links on the entity a, the
/// parameter, the alternative alt2, the class and a scenario.
fn fixture(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("m.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    write(&db, "p", "Base", &["a", "b"], &[1.0, 2.0]);
    write(&db, "p", "alt2", &["a"], &[3.0]);
    exec(
        &db,
        "INSERT INTO commit (user, message) VALUES ('t', 'links');
         INSERT INTO scenario (name, valid_from_commit) VALUES ('s', currval('seq_commit'));
         INSERT INTO metadata_pair (name, value, valid_from_commit)
           VALUES ('note', 'x', currval('seq_commit'));
         INSERT INTO value_metadata_link (entity_id, parameter_definition_id, alternative_id,
             target_kind, target_id, valid_from_commit)
           SELECT v.entity_id, v.parameter_definition_id, v.alternative_id, 'pair', m.id,
                  currval('seq_commit')
           FROM value_scalar v, metadata_pair m WHERE v.valid_to_commit IS NULL;
         INSERT INTO object_metadata_link (object_type, object_id, target_kind, target_id,
             valid_from_commit)
           SELECT 'entity', e.id, 'pair', m.id, currval('seq_commit')
           FROM entity e, metadata_pair m WHERE e.cur_name = 'a'
           UNION ALL SELECT 'parameter_definition', pd.id, 'pair', m.id, currval('seq_commit')
           FROM parameter_definition pd, metadata_pair m WHERE pd.cur_name = 'p'
           UNION ALL SELECT 'alternative', a.id, 'pair', m.id, currval('seq_commit')
           FROM alternative a, metadata_pair m WHERE a.cur_name = 'alt2'
           UNION ALL SELECT 'entity_class', c.id, 'pair', m.id, currval('seq_commit')
           FROM entity_class c, metadata_pair m WHERE c.cur_name = 'node'
           UNION ALL SELECT 'scenario', s.id, 'pair', m.id, currval('seq_commit')
           FROM scenario s, metadata_pair m WHERE s.cur_name = 's';",
    );
    db
}

fn clean(db: &Path) {
    assert_eq!(
        q(
            db,
            "SELECT count(*) FROM audit_dangling_ref \
             WHERE src_table IN ('value_metadata_link', 'object_metadata_link', 'source_license')"
        ),
        0,
        "no dangling metadata link"
    );
    assert_eq!(q(db, "SELECT count(*) FROM audit_value_link_version"), 0);
}

fn live_value_links(db: &Path) -> i64 {
    q(
        db,
        "SELECT count(*) FROM value_metadata_link WHERE valid_to_commit IS NULL",
    )
}

fn live_object_links(db: &Path, ot: &str) -> i64 {
    q(
        db,
        &format!(
            "SELECT count(*) FROM object_metadata_link \
             WHERE valid_to_commit IS NULL AND object_type = '{ot}'"
        ),
    )
}

#[test]
fn a_value_delete_closes_the_values_links() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    assert_eq!(live_value_links(&db), 3);
    clean(&db);
    delete_values(
        &db,
        vec![DeleteKey {
            class: "node".into(),
            parameter: "p".into(),
            alternative: "Base".into(),
            entity: "a".into(),
        }],
        "t",
        "del",
        WriteOptions::default(),
    )
    .unwrap();
    assert_eq!(live_value_links(&db), 2);
    clean(&db);
}

fn delete(db: &Path, kind: ObjectKind, class: Option<&str>, name: &str, cascade: bool) -> ObjectDeleteResult {
    let r = commit_changes(
        db,
        vec![],
        vec![],
        vec![],
        vec![(
            ObjectDeleteKey {
                kind,
                class: class.map(str::to_string),
                target: ObjectRef::Name(name.into()),
            },
            ObjectDeleteOpts { cascade },
        )],
        "t",
        "obj",
        WriteOptions::default(),
    )
    .unwrap();
    r.object_deletes[0].result.clone()
}

#[test]
fn links_never_block_a_delete_and_close_with_their_object() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    // A scenario whose only dependent is a metadata link: deleted without
    // cascade (OD2), the link closed and counted.
    match delete(&db, ObjectKind::Scenario, None, "s", false) {
        ObjectDeleteResult::Deleted { closed_out } => {
            let t = closed_out
                .per_table
                .iter()
                .find(|t| t.table == "object_metadata_link")
                .expect("link counted");
            assert_eq!(t.rows, 1);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(live_object_links(&db, "scenario"), 0);
    // An entity with values: the values block, the links are not listed.
    match delete(&db, ObjectKind::Entity, Some("node"), "a", false) {
        ObjectDeleteResult::Restricted { blockers } => {
            assert!(blockers.iter().all(|b| !b.table.contains("metadata")), "{blockers:?}");
        }
        other => panic!("{other:?}"),
    }
    // With cascade: the entity's values and links close.
    assert!(matches!(
        delete(&db, ObjectKind::Entity, Some("node"), "a", true),
        ObjectDeleteResult::Deleted { .. }
    ));
    assert_eq!(live_object_links(&db, "entity"), 0);
    assert_eq!(live_value_links(&db), 1, "b's link stays");
    clean(&db);
    // The parameter, then the class: their links close too.
    assert!(matches!(
        delete(&db, ObjectKind::Parameter, Some("node"), "p", true),
        ObjectDeleteResult::Deleted { .. }
    ));
    assert_eq!(live_object_links(&db, "parameter_definition"), 0);
    assert_eq!(live_value_links(&db), 0);
    assert!(matches!(
        delete(&db, ObjectKind::Class, None, "node", true),
        ObjectDeleteResult::Deleted { .. }
    ));
    assert_eq!(live_object_links(&db, "entity_class"), 0);
    // The alternative alt2 (no values left): its link closes.
    assert!(matches!(
        delete(&db, ObjectKind::Alternative, None, "alt2", false),
        ObjectDeleteResult::Deleted { .. }
    ));
    assert_eq!(live_object_links(&db, "alternative"), 0);
    clean(&db);
}

#[test]
fn erasing_an_alternative_removes_its_links() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let alt = q(&db, "SELECT id FROM alternative WHERE cur_name = 'alt2'");
    erase_alternative(
        &db,
        &ObjectDeleteKey {
            kind: ObjectKind::Alternative,
            class: None,
            target: ObjectRef::Name("alt2".into()),
        },
        &EraseOpts::default(),
        "t",
    )
    .unwrap();
    assert_eq!(
        q(
            &db,
            &format!("SELECT count(*) FROM value_metadata_link WHERE alternative_id = {alt}")
        ),
        0
    );
    assert_eq!(
        q(
            &db,
            &format!(
                "SELECT count(*) FROM object_metadata_link \
                 WHERE object_type = 'alternative' AND object_id = {alt}"
            )
        ),
        0
    );
    assert_eq!(live_value_links(&db), 2, "Base's links stay");
    // The pool row stays, with its history.
    assert_eq!(q(&db, "SELECT count(*) FROM metadata_pair"), 1);
}

#[test]
fn the_raw_value_writer_refuses_an_annotated_value() {
    use nodal_db_core::axis::AxisCache;
    use nodal_db_core::conn::with_write_txn;
    use nodal_db_core::history::write_value;
    use nodal_db_core::canon::CanonicalValue;
    use nodal_db_core::types::{ScalarValue, ShapedData, Tier, ValueKey, ValueKind};
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let (e, p, a) = {
        let conn = Connection::open(&db).unwrap();
        let t = conn
            .query_row(
                "SELECT entity_id, parameter_definition_id, alternative_id \
                 FROM value_metadata_link LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        conn.close().unwrap();
        t
    };
    let value = CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(9.0)),
    };
    let r = with_write_txn(&db, "t", "raw", |conn, c| {
        write_value(
            conn,
            c,
            ValueKey {
                entity_id: e,
                parameter_definition_id: p,
                alternative_id: a,
            },
            &value,
            &mut AxisCache::new(),
            &WriteOptions::default(),
            None,
        )
    });
    let err = r.expect_err("refused");
    assert!(
        matches!(err, nodal_db_core::error::DbError::ValueMetadataUnstated { count: 1, .. }),
        "{err}"
    );
}

/// Every statement that closes or inserts value-tier rows lives in the
/// reviewed files (the per-file counts are pinned): a new closer must also
/// close or keep the values' metadata links, so adding one is a deliberate
/// edit here.
#[test]
fn value_table_closers_are_in_the_reviewed_files() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let pats = [
        "UPDATE value_scalar SET",
        "UPDATE value_long SET",
        "UPDATE value_scalar ",
        "INSERT INTO value_scalar",
        "INSERT INTO value_long",
        "INSERT INTO value_array",
        "DELETE FROM value_array",
    ];
    let mut found: Vec<(String, usize)> = Vec::new();
    let mut stack = vec![src.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let text = std::fs::read_to_string(&p).unwrap();
            // Unit-test modules (fixtures) are not closers.
            let text = text.split("#[cfg(test)]").next().unwrap_or_default();
            let n: usize = text
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .filter(|l| pats.iter().any(|pat| l.contains(pat)))
                .count();
            if n > 0 {
                let rel = p.strip_prefix(&src).unwrap().to_string_lossy().to_string();
                found.push((rel, n));
            }
        }
    }
    found.sort();
    let want: Vec<(String, usize)> = [
        ("history.rs", 25),
        ("object_delete.rs", 1),
        ("purge.rs", 1),
        ("template/reconcile.rs", 1),
    ]
    .iter()
    .map(|(f, n)| (f.to_string(), *n))
    .collect();
    assert_eq!(
        found, want,
        "value-table closers changed: review that each new one closes or keeps the values' \
         metadata links, then update the pinned counts"
    );
}
