//! The name fold's table (`fold_table::LOWER`, generated) against its
//! normative copy `conformance/unicode/lower_simple_15.1.0.txt` (generated
//! from the pinned Unicode 15.1.0 `UnicodeData.txt` by
//! `conformance/unicode/gen_lower_table.py`), entry for entry; and the
//! table's own shape. `duckdb_lower_guard.rs` then proves the table equal to
//! the engine's `lower()`.

use nodal_db_core::fold_table::{LOWER, LOWER_UNICODE_VERSION};
use nodal_db_core::types::fold;

const VENDORED: &str = include_str!("../../../conformance/unicode/lower_simple_15.1.0.txt");

#[test]
fn rust_table_equals_the_vendored_file() {
    assert_eq!(LOWER_UNICODE_VERSION, (15, 1, 0));
    assert!(
        VENDORED.contains(
            "# Source: UnicodeData.txt 15.1.0 sha256 \
             2fc713e6a31a87c4850a37fe2caffa4218180fadb5de86b43a143ddb4581fb86"
        ),
        "the vendored file names its pinned source"
    );
    let parsed: Vec<(char, char)> = VENDORED
        .lines()
        .filter(|l| !l.starts_with('#'))
        .map(|l| {
            let (a, b) = l.split_once(';').expect("cp;lower");
            let cp = |h: &str| char::from_u32(u32::from_str_radix(h, 16).unwrap()).unwrap();
            (cp(a), cp(b))
        })
        .collect();
    assert_eq!(parsed.len(), 1433);
    assert_eq!(parsed.as_slice(), LOWER.as_slice());
}

#[test]
fn table_is_sorted_one_to_one_and_idempotent() {
    assert!(LOWER.windows(2).all(|w| w[0].0 < w[1].0), "sorted, unique");
    for &(from, to) in &LOWER {
        assert_ne!(from, to);
        // A lowercase is its own lowercase: fold is idempotent.
        assert_eq!(fold(&to.to_string()), to.to_string(), "U+{:04X}", to as u32);
        assert_eq!(fold(&from.to_string()), to.to_string());
    }
    // ASCII fast path and table agree.
    for b in 0u8..128 {
        let c = b as char;
        let want = LOWER.iter().find(|&&(f, _)| f == c).map_or(c, |&(_, t)| t);
        assert_eq!(fold(&c.to_string()), want.to_string());
    }
}
