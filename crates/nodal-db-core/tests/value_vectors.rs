//! Value-encoding conformance vectors (spec §9, OPEN-7). Each
//! `conformance/value_vectors/*.json` pins a value's canonical byte string and
//! XXH3-64/seed-0 hash. The Rust `encode_value` must reproduce them; the
//! independent Python re-encoder (`conformance/oracle/encode_value.py`, run by
//! `python_oracle.rs`) re-derives the same bytes from the spec sharing no code —
//! the "second canonicalizer" cross-check for the value encoding (CLAUDE.md
//! invariant 2). Goldens are authoritative literals, never machine-rewritten.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use nodal_db_core::encoding::{content_hash, encode_value, ValueContent};
use nodal_db_core::types::{ScalarValue, ValueKey, ValueKind};

#[derive(Deserialize)]
struct VVector {
    description: String,
    key: ValueKey,
    content: ContentSpec,
    /// Flat measure indices to set to NaN before encoding (JSON has no NaN
    /// literal). A Scalar float's measure is index 0; an Array's payload is
    /// indexed by element. The VALUE encoder canonicalizes each NaN to
    /// 0x7ff8000000000000 (spec §3.3.1) — it accepts NaN, never rejects it.
    #[serde(default)]
    nan_at: Vec<usize>,
    expected_canonical_hex: Option<String>,
    expected_hash_u64: Option<u64>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ContentSpec {
    Scalar {
        value: ScalarValue,
    },
    Array {
        axis_hashes: Vec<u64>,
        values: Vec<f64>,
    },
    Long {
        value_kind: ValueKind,
        axis_hashes: Vec<u64>,
        cells: Vec<CellSpec>,
    },
}

#[derive(Deserialize)]
struct CellSpec {
    positions: Vec<i32>,
    value: ScalarValue,
}

fn vectors_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("conformance/value_vectors")
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn value_vectors_match() {
    let mut files: Vec<PathBuf> = fs::read_dir(vectors_dir())
        .expect("read value_vectors dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    files.sort();
    assert!(files.len() >= 4, "expected >=4 value vectors, got {}", files.len());

    for path in &files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let raw = fs::read_to_string(path).expect("read vector");
        let v: VVector = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"));

        // Build the borrowing ValueContent from the owned spec, injecting NaN at
        // any `nan_at` indices first (the value encoder canonicalizes it — it
        // does NOT reject, unlike the axis encoder in encoding_vectors.rs).
        let scalar_owned: ScalarValue;
        let values_owned: Vec<f64>;
        let cells_owned: Vec<(Vec<i32>, ScalarValue)>;
        let content = match &v.content {
            ContentSpec::Scalar { value } => {
                scalar_owned = if v.nan_at.is_empty() {
                    value.clone()
                } else {
                    match value {
                        ScalarValue::Float(_) => ScalarValue::Float(f64::NAN),
                        _ => panic!("{name}: nan_at on a Scalar needs a Float measure"),
                    }
                };
                ValueContent::Scalar(&scalar_owned)
            }
            ContentSpec::Array {
                axis_hashes,
                values,
            } => {
                values_owned = {
                    let mut vv = values.clone();
                    for &i in &v.nan_at {
                        vv[i] = f64::NAN;
                    }
                    vv
                };
                ValueContent::Array {
                    axis_hashes,
                    values: &values_owned,
                }
            }
            ContentSpec::Long {
                value_kind,
                axis_hashes,
                cells,
            } => {
                cells_owned = cells
                    .iter()
                    .map(|c| (c.positions.clone(), c.value.clone()))
                    .collect();
                ValueContent::Long {
                    value_kind: *value_kind,
                    axis_hashes,
                    cells: &cells_owned,
                }
            }
        };

        let bytes = encode_value(&v.key, &content).unwrap_or_else(|e| panic!("{name}: {e:?}"));
        let want_hex = v
            .expected_canonical_hex
            .as_deref()
            .unwrap_or_else(|| panic!("{name}: committed vector is missing expected_canonical_hex"));
        assert_eq!(
            to_hex(&bytes),
            want_hex,
            "{name}: canonical bytes ({})",
            v.description
        );
        let want_hash = v
            .expected_hash_u64
            .unwrap_or_else(|| panic!("{name}: committed vector is missing expected_hash_u64"));
        assert_eq!(
            content_hash(&bytes),
            want_hash,
            "{name}: content hash ({})",
            v.description
        );
    }

    println!("value vectors: {} checked", files.len());
}
