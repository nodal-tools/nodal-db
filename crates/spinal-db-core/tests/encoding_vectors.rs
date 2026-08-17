//! Encoding conformance vectors — the oracle for the axis canonical encoding.
//!
//! Loads every `conformance/encoding_vectors/*.json`, re-encodes the axis, and
//! asserts the canonical bytes + XXH3-64/seed-0 hash match. It also checks the
//! parametric-first store decisions (`regularity`), the NaN-reject vector, and
//! that the NON-DEDUP property family hashes are pairwise distinct (spec §6).
//!
//! The committed `expected_canonical_hex` / `expected_hash_u64` in each vector
//! are **authoritative literals**: this runner asserts the encoder reproduces
//! them and NEVER rewrites them. There is deliberately no in-test regeneration
//! path — a spec-divergent edit that also "regenerated" the goldens would stay
//! green, defeating the oracle. Goldens change only by a human editing the JSON
//! (a rehash-class event, spec §2.4), cross-checked by the independent Python
//! oracle (`conformance/oracle/encode_axis.py`, run by `python_oracle.rs`).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use spinal_db_core::encoding::{
    content_hash, datetime_reproduces, encode_axis, float_regular_step, integer_regular_step,
    AxisBody, CanonicalAxis, Duration,
};

#[derive(Deserialize)]
struct Vector {
    description: String,
    axis: CanonicalAxis,
    #[serde(default)]
    encode_error: bool,
    #[serde(default)]
    expected_canonical_hex: Option<String>,
    #[serde(default)]
    expected_hash_u64: Option<u64>,
    #[serde(default)]
    regularity: Option<Regularity>,
}

/// A parametric-first store-decision assertion (spec §7.1, §10).
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum Regularity {
    Integer {
        coords: Vec<i64>,
        expect_parametric: bool,
    },
    Float {
        coords: Vec<f64>,
        expect_parametric: bool,
    },
    Datetime {
        coords: Vec<i64>,
        step: Duration,
        expect_parametric: bool,
    },
}

fn vectors_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("conformance/encoding_vectors")
}

fn vector_files() -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(vectors_dir())
        .expect("read vectors dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    v.sort();
    v
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn encoding_vectors_match() {
    let mut dedup_group: BTreeMap<String, u64> = BTreeMap::new();
    let mut checked = 0usize;

    for path in vector_files() {
        let raw = fs::read_to_string(&path).expect("read vector");
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let vector: Vector =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"));

        // Parametric-first store decision (reproduce-or-materialize).
        if let Some(reg) = &vector.regularity {
            let (got, want) = match reg {
                Regularity::Integer {
                    coords,
                    expect_parametric,
                } => (integer_regular_step(coords).is_some(), *expect_parametric),
                Regularity::Float {
                    coords,
                    expect_parametric,
                } => (float_regular_step(coords).is_some(), *expect_parametric),
                Regularity::Datetime {
                    coords,
                    step,
                    expect_parametric,
                } => (
                    datetime_reproduces(coords[0], step, coords),
                    *expect_parametric,
                ),
            };
            assert_eq!(
                got, want,
                "{name}: store decision mismatch ({})",
                vector.description
            );
        }

        // NaN-reject vector: the encoder MUST error. JSON has no NaN literal, so
        // inject one into the float body declared by the vector.
        if vector.encode_error {
            let mut axis = vector.axis.clone();
            match &mut axis.body {
                AxisBody::FloatMaterialized { coords } => coords[0] = f64::NAN,
                AxisBody::FloatParametric { start, .. } => *start = f64::NAN,
                _ => panic!("{name}: encode_error vector needs a float body to hold the NaN"),
            }
            assert!(
                encode_axis(&axis).is_err(),
                "{name}: NaN axis must be rejected, not hashed"
            );
            checked += 1;
            continue;
        }

        let bytes = encode_axis(&vector.axis).unwrap_or_else(|e| panic!("{name}: encode: {e:?}"));
        let hash = content_hash(&bytes);
        let got_hex = to_hex(&bytes);

        if vector.description.contains("NON-DEDUP GROUP") {
            dedup_group.insert(name.clone(), hash);
        }

        // Goldens are authoritative literals; assert, never rewrite.
        let want_hex = vector.expected_canonical_hex.as_deref().unwrap_or_else(|| {
            panic!("{name}: committed vector is missing expected_canonical_hex")
        });
        assert_eq!(
            got_hex, want_hex,
            "{name}: canonical bytes mismatch ({})",
            vector.description
        );
        let want_hash = vector
            .expected_hash_u64
            .unwrap_or_else(|| panic!("{name}: committed vector is missing expected_hash_u64"));
        assert_eq!(
            hash, want_hash,
            "{name}: content hash mismatch ({})",
            vector.description
        );
        checked += 1;
    }

    // Property variants share one body and differ only in a calendar/timezone/
    // crs tag (incl. a case-only difference), so none may dedup: all distinct.
    let hashes: Vec<u64> = dedup_group.values().copied().collect();
    let mut uniq = hashes.clone();
    uniq.sort_unstable();
    uniq.dedup();
    assert_eq!(
        uniq.len(),
        hashes.len(),
        "NON-DEDUP GROUP collided: {dedup_group:?}"
    );
    assert!(
        hashes.len() >= 5,
        "expected >=5 non-dedup property vectors, got {}",
        hashes.len()
    );

    assert!(checked >= 18, "expected >=18 vectors, got {checked}");
    println!(
        "encoding vectors: {checked} checked, {} in NON-DEDUP GROUP",
        hashes.len()
    );
}
