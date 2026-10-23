//! Canonicalization-pipeline conformance cases (rust_implementation_plan.md
//! Part 5, deliverable 3; VMP Part 3). Each `conformance/pipeline_cases/*.json`
//! describes a value in long form (index tuples -> measure, NULLs included),
//! its declared dimensions, and any label domains; the runner builds the Arrow
//! `RecordBatch`, runs `canon::canonicalize`, and asserts the derived tier,
//! canonical axes, and shaped payload match the golden. The goldens are
//! human-authored against VMP Part 3 — the derived axes are asserted
//! *structurally* (kind + form + params), so a wrong derivation cannot bless its
//! own hash.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use duckdb::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use serde::Deserialize;
use serde_json::Value as Json;

use nodal_db_core::canon::{canonicalize, LabelDomain};
use nodal_db_core::encoding::CanonicalAxis;
use nodal_db_core::types::{
    DeclaredType, IndexKind, ScalarValue, ShapedData, Tier, ValueKey, ValueKind,
};

#[derive(Deserialize)]
struct Case {
    description: String,
    value_kind: ValueKind,
    #[serde(default)]
    declared: Vec<DimSpec>,
    #[serde(default)]
    domains: Vec<DomainSpec>,
    rows: Vec<RowSpec>,
    expected: Expected,
}

#[derive(Deserialize)]
struct DimSpec {
    position: u8,
    kind: IndexKind,
}

#[derive(Deserialize)]
struct DomainSpec {
    position: u8,
    labels: Vec<String>,
}

#[derive(Deserialize)]
struct RowSpec {
    #[serde(default)]
    dims: Vec<Json>,
    value: Json,
}

#[derive(Deserialize)]
struct Expected {
    tier: Tier,
    axes: Vec<CanonicalAxis>,
    shape: ShapedData,
}

fn cases_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("conformance/pipeline_cases")
}

fn case_files() -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(cases_dir())
        .expect("read pipeline_cases dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    v.sort();
    v
}

/// Build the Arrow `RecordBatch`: one column per declared dimension (declared
/// order) then the value column, matching the layout `canon` expects.
fn build_batch(case: &Case) -> RecordBatch {
    let n = case.rows.len();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (i, dim) in case.declared.iter().enumerate() {
        let name = format!("d{i}");
        match dim.kind {
            IndexKind::Datetime => {
                let vals: Vec<i64> = case
                    .rows
                    .iter()
                    .map(|r| r.dims[i].as_i64().unwrap())
                    .collect();
                fields.push(Field::new(
                    name,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ));
                columns.push(Arc::new(TimestampMicrosecondArray::from(vals)));
            }
            IndexKind::Integer => {
                let vals: Vec<i64> = case
                    .rows
                    .iter()
                    .map(|r| r.dims[i].as_i64().unwrap())
                    .collect();
                fields.push(Field::new(name, DataType::Int64, false));
                columns.push(Arc::new(Int64Array::from(vals)));
            }
            IndexKind::Label => {
                let vals: Vec<String> = case
                    .rows
                    .iter()
                    .map(|r| r.dims[i].as_str().unwrap().to_string())
                    .collect();
                fields.push(Field::new(name, DataType::Utf8, false));
                columns.push(Arc::new(StringArray::from(vals)));
            }
            other => panic!("unsupported test dim kind {other:?}"),
        }
    }

    // Value column, nulls preserved (a JSON null is the absent value).
    let opt_i64 = |f: fn(&Json) -> Option<i64>| -> Vec<Option<i64>> {
        case.rows.iter().map(|r| f(&r.value)).collect()
    };
    let value_col: (Field, ArrayRef) = match case.value_kind {
        ValueKind::Float => {
            let v: Vec<Option<f64>> = case.rows.iter().map(|r| r.value.as_f64()).collect();
            (
                Field::new("value", DataType::Float64, true),
                Arc::new(Float64Array::from(v)),
            )
        }
        ValueKind::Int => (
            Field::new("value", DataType::Int64, true),
            Arc::new(Int64Array::from(opt_i64(|j| j.as_i64()))),
        ),
        ValueKind::Bool => {
            let v: Vec<Option<bool>> = case.rows.iter().map(|r| r.value.as_bool()).collect();
            (
                Field::new("value", DataType::Boolean, true),
                Arc::new(BooleanArray::from(v)),
            )
        }
        ValueKind::String => {
            let v: Vec<Option<String>> = case
                .rows
                .iter()
                .map(|r| r.value.as_str().map(|s| s.to_string()))
                .collect();
            (
                Field::new("value", DataType::Utf8, true),
                Arc::new(StringArray::from(v)),
            )
        }
        ValueKind::DateTime => (
            Field::new(
                "value",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Arc::new(TimestampMicrosecondArray::from(opt_i64(|j| j.as_i64()))),
        ),
        ValueKind::Duration => panic!("duration value column not supported in pipeline cases"),
    };
    fields.push(value_col.0);
    columns.push(value_col.1);

    assert_eq!(
        columns.iter().map(|c| c.len()).max().unwrap_or(0),
        n,
        "row count"
    );
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("build record batch")
}

/// Every float measure of a shaped payload, as raw bits, in payload order.
fn float_bits(shape: &ShapedData) -> Vec<u64> {
    let of = |m: &ScalarValue| match m {
        ScalarValue::Float(x) => Some(x.to_bits()),
        _ => None,
    };
    match shape {
        ShapedData::Scalar(m) => of(m).into_iter().collect(),
        ShapedData::Array(vals) => vals.iter().map(|x| x.to_bits()).collect(),
        ShapedData::Long(rows) => rows.iter().filter_map(|r| of(&r.value)).collect(),
    }
}

#[test]
fn pipeline_cases_match() {
    let files = case_files();
    assert!(
        files.len() >= 7,
        "expected >=7 pipeline cases, got {}",
        files.len()
    );

    for path in &files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let raw = fs::read_to_string(path).expect("read case");
        let case: Case = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"));

        let declared = DeclaredType {
            positions: case.declared.iter().map(|d| (d.position, d.kind)).collect(),
        };
        let domains: Vec<LabelDomain> = case
            .domains
            .iter()
            .map(|d| LabelDomain {
                position: d.position,
                labels: d.labels.clone(),
            })
            .collect();

        let batch = build_batch(&case);
        let cv = canonicalize(&batch, &declared, case.value_kind, &domains)
            .unwrap_or_else(|e| panic!("{name}: canonicalize failed: {e} ({})", case.description));

        assert_eq!(
            cv.tier, case.expected.tier,
            "{name}: tier ({})",
            case.description
        );
        assert_eq!(
            cv.axes, case.expected.axes,
            "{name}: axes ({})",
            case.description
        );
        assert_eq!(
            cv.shape, case.expected.shape,
            "{name}: shape ({})",
            case.description
        );
        // Bitwise too: the derived `PartialEq` treats -0.0 == 0.0 and never
        // matches NaN, so it cannot see float normalization.
        assert_eq!(
            float_bits(&cv.shape),
            float_bits(&case.expected.shape),
            "{name}: float bits ({})",
            case.description
        );

        // The value encoding (spec §9, OPEN-7) must produce bytes without error
        // for the derived value — a smoke check of the whole pipeline output.
        let key = ValueKey {
            entity_id: 1,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        cv.encode(&key)
            .unwrap_or_else(|e| panic!("{name}: value encode failed: {e:?}"));
    }

    println!("pipeline cases: {} checked", files.len());
}
