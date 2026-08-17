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
use spinal_db_core::encoding::{content_hash, encode_value, ValueContent};
use spinal_db_core::types::{ScalarValue, ValueKey, ValueKind};

#[derive(Deserialize)]
struct VVector {
    description: String,
    key: ValueKey,
    content: ContentSpec,
    expected_canonical_hex: String,
    expected_hash_u64: u64,
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

        // Build the borrowing ValueContent from the owned spec.
        let cells_owned: Vec<(Vec<i32>, ScalarValue)>;
        let content = match &v.content {
            ContentSpec::Scalar { value } => ValueContent::Scalar(value),
            ContentSpec::Array {
                axis_hashes,
                values,
            } => ValueContent::Array {
                axis_hashes,
                values,
            },
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
        assert_eq!(
            to_hex(&bytes),
            v.expected_canonical_hex,
            "{name}: canonical bytes ({})",
            v.description
        );
        assert_eq!(
            content_hash(&bytes),
            v.expected_hash_u64,
            "{name}: content hash ({})",
            v.description
        );
    }

    println!("value vectors: {} checked", files.len());
}
