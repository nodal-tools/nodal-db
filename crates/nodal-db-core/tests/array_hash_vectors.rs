//! Array payload hash conformance vectors (spec §13). Each
//! `conformance/array_hash_vectors/*.json` pins the XXH3-64/seed-0 hash of a
//! payload's elements as 8-byte little-endian words, the value stored in
//! `value_array.content_hash`. The Rust `encoding::array_payload_hash` (and
//! its `_i64` twin) must reproduce them; the independent Python oracle
//! (`conformance/oracle/array_hash.py`, run by `python_oracle.rs`) re-derives
//! them sharing no code. Goldens are authoritative literals, never rewritten.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use nodal_db_core::encoding::{array_payload_hash, array_payload_hash_i64};
use serde::Deserialize;

#[derive(Deserialize)]
struct Ramp {
    len: usize,
    start: f64,
    step: f64,
}

#[derive(Deserialize)]
struct HVector {
    description: String,
    element_type: String,
    #[serde(default)]
    values: Vec<serde_json::Number>,
    #[serde(default)]
    bits_at: Vec<(usize, String)>,
    ramp: Option<Ramp>,
    same_hash_as: Option<String>,
    expected_hash_u64: u64,
}

fn vectors_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("conformance/array_hash_vectors")
}

fn hash_of(name: &str, v: &HVector) -> u64 {
    match v.element_type.as_str() {
        "bigint" => {
            assert!(
                v.bits_at.is_empty() && v.ramp.is_none(),
                "{name}: bigint spells values only"
            );
            let ints: Vec<i64> = v
                .values
                .iter()
                .map(|n| {
                    n.as_i64()
                        .unwrap_or_else(|| panic!("{name}: {n} is not an i64"))
                })
                .collect();
            array_payload_hash_i64(&ints)
        }
        "double" => {
            let mut vals: Vec<f64> = match &v.ramp {
                Some(r) => (0..r.len).map(|i| r.start + i as f64 * r.step).collect(),
                None => v.values.iter().map(|n| n.as_f64().unwrap()).collect(),
            };
            for (i, b) in &v.bits_at {
                let h = b.strip_prefix("0x").expect("bits_at bits are 0x-prefixed");
                vals[*i] = f64::from_bits(u64::from_str_radix(h, 16).expect("hex bits"));
            }
            array_payload_hash(&vals)
        }
        other => panic!("{name}: unknown element_type {other}"),
    }
}

#[test]
fn array_hash_vectors_match() {
    let mut files: Vec<PathBuf> = fs::read_dir(vectors_dir())
        .expect("read array_hash_vectors dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    assert!(
        files.len() >= 11,
        "expected >= 11 vectors, got {}",
        files.len()
    );

    let mut got = BTreeMap::new();
    let mut same = vec![];
    for path in &files {
        let name = path.file_stem().unwrap().to_string_lossy().to_string();
        let raw = fs::read_to_string(path).expect("read vector");
        let v: HVector = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"));
        let h = hash_of(&name, &v);
        assert_eq!(h, v.expected_hash_u64, "{name}: {}", v.description);
        if let Some(other) = &v.same_hash_as {
            same.push((name.clone(), other.clone()));
        }
        got.insert(name, h);
    }
    for (a, b) in same {
        assert_eq!(got[&a], got[&b], "{a} must hash equal to {b}");
    }
    // The pinned differences the spec calls out.
    assert_ne!(got["a02_zero"], got["a03_negative_zero"]);
    assert_ne!(got["a04_nan_canonical"], got["a05_nan_noncanonical"]);
    assert_ne!(got["a08_small"], got["a11_order_matters"]);
    println!("array hash vectors: {} checked", files.len());
}
