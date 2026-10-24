//! History write-protocol conformance (VMP Part 5; rust_implementation_plan.md
//! Part 3 `history`). Each `conformance/protocol_cases/*.json` file is a sequence
//! of writes/edits/deletes on one key with the expected table state after every
//! step; this driver builds a fresh schema DB via `schema_init`, drives each step
//! through `history::write_value` / `delete_value` inside a real
//! `conn::with_write_txn`, and asserts the row states with SQL.
//!
//! `valid_to_step` in a case is a 1-based step index resolved to that step's
//! commit id (the DB bootstraps commit id 1, so step N's write is commit N+1);
//! `null` means the row is current (`valid_to_commit IS NULL`).

use std::path::PathBuf;

use duckdb::Connection;
use serde::Deserialize;

use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis};
use nodal_db_core::history::{delete_value, write_value};
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

// --- JSON case model -------------------------------------------------------

#[derive(Deserialize)]
struct Case {
    #[allow(dead_code)]
    name: String,
    /// A case for semantics that are planned but not built yet: it must
    /// currently FAIL (so the marker cannot go stale) and is reported as
    /// pending. Remove the marker in the change that makes it pass.
    #[serde(default)]
    pending: Option<String>,
    key: [i64; 3],
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct Step {
    op: Op,
    #[serde(default)]
    force_replace: bool,
    #[serde(default)]
    expect: Expect,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Op {
    ScalarFloat {
        v: f64,
    },
    ArrayFloat {
        start: i64,
        step: i64,
        length: i32,
        values: Vec<f64>,
    },
    Long {
        start: i64,
        step: i64,
        length: i32,
        cells: Vec<Cell>,
    },
    Delete,
}

#[derive(Deserialize)]
struct Cell {
    pos: i32,
    value: f64,
}

#[derive(Deserialize, Default)]
struct Expect {
    #[serde(default)]
    scalar: Vec<ScalarRow>,
    #[serde(default)]
    array: Vec<ArrayRow>,
    #[serde(default)]
    array_hist: Vec<ArrayHistRow>,
    #[serde(default)]
    long: Vec<LongRowE>,
}

#[derive(Deserialize)]
struct ScalarRow {
    version: i16,
    valid_to_step: Option<usize>,
    retained: bool,
    v_float: Option<f64>,
}

#[derive(Deserialize)]
struct ArrayRow {
    version: i16,
    v_min: f64,
    v_max: f64,
    first: f64,
}

#[derive(Deserialize)]
struct ArrayHistRow {
    version: i16,
    valid_to_step: Option<usize>,
    retained: bool,
    values_present: bool,
    first: Option<f64>,
}

#[derive(Deserialize)]
struct LongRowE {
    i1_pos: i32,
    version: i16,
    valid_to_step: Option<usize>,
    retained: bool,
    v_float: Option<f64>,
}

// --- helpers ---------------------------------------------------------------

fn integer_axis(start: i64, step: i64, length: i32) -> CanonicalAxis {
    CanonicalAxis {
        length,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start, step },
    }
}

/// Build the `CanonicalValue` an op writes (delete returns `None`).
fn build_value(op: &Op) -> Option<CanonicalValue> {
    match op {
        Op::ScalarFloat { v } => Some(CanonicalValue {
            tier: Tier::Scalar,
            value_kind: ValueKind::Float,
            axes: vec![],
            shape: ShapedData::Scalar(ScalarValue::Float(*v)),
        }),
        Op::ArrayFloat {
            start,
            step,
            length,
            values,
        } => Some(CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![integer_axis(*start, *step, *length)],
            shape: ShapedData::Array(values.clone()),
        }),
        Op::Long {
            start,
            step,
            length,
            cells,
        } => Some(CanonicalValue {
            tier: Tier::Long,
            value_kind: ValueKind::Float,
            axes: vec![integer_axis(*start, *step, *length)],
            shape: ShapedData::Long(
                cells
                    .iter()
                    .map(|c| LongRow {
                        positions: vec![c.pos],
                        value: ScalarValue::Float(c.value),
                    })
                    .collect(),
            ),
        }),
        Op::Delete => None,
    }
}

fn run_case(path: &std::path::Path) {
    let text = std::fs::read_to_string(path).expect("read case");
    let case: Case = serde_json::from_str(&text).expect("parse case");
    let key = ValueKey {
        entity_id: case.key[0],
        parameter_definition_id: case.key[1],
        alternative_id: case.key[2],
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("case.duckdb");
    init_schema(&db).expect("schema");

    let mut commit_ids: Vec<i64> = Vec::new(); // step index (1-based) -> commit id

    for (i, step) in case.steps.iter().enumerate() {
        let opts = WriteOptions {
            force_replace: step.force_replace,
            retain_override: None,
            expect_commit: None,
        };
        let value = build_value(&step.op);
        let commit_id = with_write_txn(&db, "tester", "protocol step", |conn, c| {
            let mut cache = AxisCache::new();
            match &value {
                Some(v) => {
                    write_value(conn, c, key, v, &mut cache, &opts, None)?;
                }
                None => {
                    delete_value(conn, c, key, &opts)?;
                }
            }
            Ok(c)
        })
        .unwrap_or_else(|e| panic!("case {:?} step {}: {e}", path.file_name().unwrap(), i + 1));
        commit_ids.push(commit_id);

        assert_step(&db, &key, &step.expect, &commit_ids, path, i + 1);
    }
}

fn resolve(step: Option<usize>, commit_ids: &[i64]) -> Option<i64> {
    step.map(|s| commit_ids[s - 1])
}

#[allow(clippy::type_complexity)]
fn assert_step(
    db: &PathBuf,
    key: &ValueKey,
    e: &Expect,
    commit_ids: &[i64],
    path: &std::path::Path,
    step_no: usize,
) {
    let conn = Connection::open(db).expect("open ro");
    let ctx = format!("{:?} step {step_no}", path.file_name().unwrap());
    let triple = duckdb::params![
        key.entity_id,
        key.parameter_definition_id,
        key.alternative_id
    ];

    // value_scalar
    let got: Vec<(i16, Option<i64>, bool, Option<f64>)> = conn
        .prepare(
            "SELECT version, valid_to_commit, content_retained, v_float FROM value_scalar \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? ORDER BY version",
        )
        .unwrap()
        .query_map(triple, |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(got.len(), e.scalar.len(), "{ctx}: value_scalar row count");
    for (row, exp) in got.iter().zip(&e.scalar) {
        assert_eq!(row.0, exp.version, "{ctx}: scalar version");
        assert_eq!(
            row.1,
            resolve(exp.valid_to_step, commit_ids),
            "{ctx}: scalar valid_to"
        );
        assert_eq!(row.2, exp.retained, "{ctx}: scalar content_retained");
        assert_eq!(row.3, exp.v_float, "{ctx}: scalar v_float");
    }

    // value_array (live only)
    let got_arr: Vec<(i16, f64, f64, f64)> = conn
        .prepare(
            "SELECT version, v_min, v_max, values[1] FROM value_array \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? ORDER BY version",
        )
        .unwrap()
        .query_map(triple, |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(got_arr.len(), e.array.len(), "{ctx}: value_array row count");
    for (row, exp) in got_arr.iter().zip(&e.array) {
        assert_eq!(row.0, exp.version, "{ctx}: array version");
        assert_eq!(row.1, exp.v_min, "{ctx}: array v_min");
        assert_eq!(row.2, exp.v_max, "{ctx}: array v_max");
        assert_eq!(row.3, exp.first, "{ctx}: array values[1]");
    }

    // value_array_hist
    let got_h: Vec<(i16, Option<i64>, bool, bool, Option<f64>)> = conn
        .prepare(
            "SELECT version, valid_to_commit, content_retained, (values IS NOT NULL), values[1] \
             FROM value_array_hist \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? ORDER BY version",
        )
        .unwrap()
        .query_map(triple, |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        got_h.len(),
        e.array_hist.len(),
        "{ctx}: value_array_hist row count"
    );
    for (row, exp) in got_h.iter().zip(&e.array_hist) {
        assert_eq!(row.0, exp.version, "{ctx}: hist version");
        assert_eq!(
            row.1,
            resolve(exp.valid_to_step, commit_ids),
            "{ctx}: hist valid_to"
        );
        assert_eq!(row.2, exp.retained, "{ctx}: hist content_retained");
        assert_eq!(row.3, exp.values_present, "{ctx}: hist values present");
        assert_eq!(row.4, exp.first, "{ctx}: hist values[1]");
    }

    // value_long
    let got_l: Vec<(i32, i16, Option<i64>, bool, Option<f64>)> = conn
        .prepare(
            "SELECT i1_pos, version, valid_to_commit, content_retained, v_float FROM value_long \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
             ORDER BY version, i1_pos",
        )
        .unwrap()
        .query_map(triple, |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(got_l.len(), e.long.len(), "{ctx}: value_long row count");
    for (row, exp) in got_l.iter().zip(&e.long) {
        assert_eq!(row.0, exp.i1_pos, "{ctx}: long i1_pos");
        assert_eq!(row.1, exp.version, "{ctx}: long version");
        assert_eq!(
            row.2,
            resolve(exp.valid_to_step, commit_ids),
            "{ctx}: long valid_to"
        );
        assert_eq!(row.3, exp.retained, "{ctx}: long content_retained");
        assert_eq!(row.4, exp.v_float, "{ctx}: long v_float");
    }

    // Every step must leave the history-tiling audit clean (VMP Part 5).
    let tiling: i64 = conn
        .query_row("SELECT count(*) FROM audit_history_tiling", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(tiling, 0, "{ctx}: audit_history_tiling must be empty");
}

#[test]
fn all_protocol_cases() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../conformance/protocol_cases");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read protocol_cases dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no protocol cases found in {dir:?}");
    let mut pending = 0;
    for f in &files {
        let text = std::fs::read_to_string(f).expect("read case");
        let case: Case = serde_json::from_str(&text).expect("parse case");
        match case.pending {
            None => run_case(f),
            Some(why) => {
                pending += 1;
                let passed = std::panic::catch_unwind(|| run_case(f)).is_ok();
                assert!(
                    !passed,
                    "{f:?} is marked pending ({why}) but passes: remove the marker"
                );
                eprintln!("pending (fails as expected): {f:?} ({why})");
            }
        }
    }
    eprintln!("ran {} protocol cases ({pending} pending)", files.len());
}
