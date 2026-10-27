//! Name-identity equality guard (engine rule 8,
//! `docs/dev/engine_rules.md` §8): the Rust name fold (`types::fold`) IS
//! DuckDB's `lower()` (the `cur_name` columns, their unique indexes, every
//! SQL resolver and the read-surface macros).
//!
//! `types::fold` applies the vendored Unicode 15.1.0 simple lowercase table
//! code point by code point. That equals `lower()` exactly when (a) the two
//! agree on every Unicode scalar value and (b) `lower()` is context-free,
//! i.e. the lower of a string is the concatenation of the lowers of its code
//! points (no final-sigma or other context rule). (a) is checked
//! exhaustively; (b) on 20,000 random strings over the cased code points and
//! the context-rule characters, plus the classic cases as strings. Together
//! they are a proof for every string, not a sample: two per-code-point maps
//! that agree everywhere agree on every concatenation.
//!
//! When it fails after a DuckDB bump: the engine changed `lower()`, which
//! changes name identity (two stored names may now be one, or the reverse).
//! Follow CONTRIBUTING.md "DuckDB upgrades": regenerate the table at the
//! engine's Unicode version (`conformance/unicode/gen_lower_table.py`), run a
//! collision report on the dev files and compact them (the `lower(name)`
//! indexes persist keys computed by the engine that built them).

// Tests may use raw case mappings (as data or to build fixtures); the
// workspace clippy.toml ban on them is for name compares in the crates.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;

use nodal_db_core::duckdb::Connection;
use nodal_db_core::fold_table::{CANARY, LOWER};
use nodal_db_core::types::{engine_fold_mismatch, fold};

fn hex(s: &str) -> String {
    s.chars()
        .map(|c| format!("U+{:04X}", c as u32))
        .collect::<Vec<_>>()
        .join(" ")
}

/// DuckDB's `lower(chr(cp))` for every scalar value it changes.
fn duckdb_changed(conn: &Connection) -> BTreeMap<u32, String> {
    let mut stmt = conn
        .prepare(
            "SELECT cp::UINTEGER, lower(chr(cp::INTEGER)) FROM range(1, 1114112) t(cp) \
             WHERE cp NOT BETWEEN 55296 AND 57343 AND lower(chr(cp::INTEGER)) <> chr(cp::INTEGER)",
        )
        .expect("prepare");
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, u32>(0)?, r.get::<_, String>(1)?)))
        .expect("query");
    rows.map(|r| r.expect("row")).collect()
}

fn duckdb_lower(conn: &Connection, s: &str) -> String {
    conn.query_row("SELECT lower(?)", [s], |r| r.get(0))
        .expect("lower")
}

#[test]
fn rust_fold_equals_duckdb_lower_on_every_scalar_value() {
    let conn = Connection::open_in_memory().expect("open");
    let duck = duckdb_changed(&conn);
    assert_eq!(
        duck.len(),
        LOWER.len(),
        "DuckDB lower() changes {} code points, the vendored table {}",
        duck.len(),
        LOWER.len()
    );
    let mut diffs = Vec::new();
    for cp in 1..=0x10_FFFFu32 {
        let Some(c) = char::from_u32(cp) else {
            continue; // surrogates
        };
        let s = c.to_string();
        let rust = fold(&s);
        let duckdb = duck.get(&cp).cloned().unwrap_or_else(|| s.clone());
        if rust != duckdb {
            diffs.push(format!(
                "  U+{cp:04X}: duckdb {} rust {}",
                hex(&duckdb),
                hex(&rust)
            ));
        }
    }
    assert!(
        diffs.is_empty(),
        "DuckDB lower() and the name fold differ on {} code point(s): the engine changed \
         its case table, which changes name identity (see this file's header):\n{}",
        diffs.len(),
        diffs.join("\n")
    );
}

/// A small deterministic PRNG (xorshift64*): reproducible strings without a
/// dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

#[test]
fn duckdb_lower_is_context_free_and_equals_the_fold_on_strings() {
    let conn = Connection::open_in_memory().expect("open");
    // The pool: every cased code point (and its lowercase), the
    // context-rule characters, combining marks, punctuation and spaces.
    let mut pool: Vec<char> = CANARY.to_vec();
    pool.extend(LOWER.iter().map(|&(_, l)| l));
    pool.extend(
        [
            'Σ', 'σ', 'ς', '\u{130}', 'I', 'i', '\u{307}', '\u{301}', '\u{345}', 'ß', 'ẞ', ' ',
            '.', '\'', '-', '_', '\u{200d}', '0',
        ]
        .iter(),
    );
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let strings: Vec<String> = (0..20_000)
        .map(|_| {
            let len = 1 + rng.below(12);
            (0..len).map(|_| pool[rng.below(pool.len())]).collect()
        })
        .collect();
    // One statement for all of them, and one for the pool's code points
    // one by one (NUL separates: no string contains it, lower() keeps it).
    let lower_each = |items: Vec<String>| -> Vec<String> {
        let lowered = duckdb_lower(&conn, &items.join("\u{0}"));
        let out: Vec<String> = lowered.split('\u{0}').map(str::to_string).collect();
        assert_eq!(out.len(), items.len());
        out
    };
    let single: BTreeMap<char, String> = pool
        .iter()
        .copied()
        .zip(lower_each(pool.iter().map(|c| c.to_string()).collect()))
        .collect();
    let lowered = lower_each(strings.clone());
    for (s, l) in strings.iter().zip(lowered) {
        let per_cp: String = s.chars().map(|c| single[&c].as_str()).collect();
        assert_eq!(l, per_cp, "lower() is not context-free on {}", hex(s));
        assert_eq!(fold(s), l, "fold != lower() on {}", hex(s));
    }
}

/// The cases where Rust's `to_lowercase` (the former fold) and `lower()`
/// differed, as strings on both sides: they are now one function.
#[test]
fn the_former_drift_cases_are_equal() {
    let conn = Connection::open_in_memory().expect("open");
    for s in [
        "ΟΔΟΣ",
        "οδοσ",
        "οδος",
        "ΑΣ ΑΣ",
        "\u{130}",
        "İSTANBUL",
        "Straße",
        "STRASSE",
        "ẞ",
        "ǅ",
        "\u{1c89}",
        "\u{10d50}",
        "\u{a7cb}",
    ] {
        assert_eq!(fold(s), duckdb_lower(&conn, s), "{}", hex(s));
    }
    // What the shared function means for identity (engine rule 8): final
    // sigma is not special, İ folds to plain i, ß is not ss, and the
    // Unicode 16 case pairs are case-sensitive.
    assert_eq!(fold("ΟΔΟΣ"), fold("οδοσ"));
    assert_ne!(fold("ΟΔΟΣ"), fold("οδος"));
    assert_eq!(fold("\u{130}x"), fold("Ix"));
    assert_ne!(fold("Straße"), fold("STRASSE"));
    assert_ne!(fold("\u{1c89}"), fold("\u{1c8a}"));
}

/// The runtime engine check (`types::check_engine_fold`) passes on the
/// bundled engine and refuses one whose `lower()` disagrees: here simulated
/// by a wrong expected fold (Unicode 16's U+1C89 → U+1C8A pair).
#[test]
fn engine_check_passes_here_and_refuses_a_mismatch() {
    let conn = Connection::open_in_memory().expect("open");
    assert_eq!(engine_fold_mismatch(&conn, fold).expect("check"), None);
    let unicode16 = |s: &str| -> String {
        fold(s)
            .chars()
            .map(|c| if c == '\u{1c89}' { '\u{1c8a}' } else { c })
            .collect()
    };
    let refusal = engine_fold_mismatch(&conn, unicode16)
        .expect("check")
        .expect("a mismatch is refused");
    assert!(
        refusal.contains("U+1C89 lowers to U+1C89 in the engine, U+1C8A in the core"),
        "{refusal}"
    );
    // The canary covers the code points either Unicode version cases.
    assert!(CANARY.contains(&'\u{1c89}') && CANARY.contains(&'\u{130}'));
    // Speed: one statement, well under the plan's millisecond budget even
    // in a debug build's timing noise.
    let start = std::time::Instant::now();
    engine_fold_mismatch(&conn, fold).expect("check");
    assert!(
        start.elapsed() < std::time::Duration::from_millis(250),
        "{:?}",
        start.elapsed()
    );
}
