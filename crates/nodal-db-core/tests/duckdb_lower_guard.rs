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
//! exhaustively, and given (b) it covers every string: two per-code-point
//! maps that agree everywhere agree on every concatenation. (b) cannot be
//! checked exhaustively; it is tested, not proven: on strings built for
//! every context rule Unicode's SpecialCasing defines (final sigma, the
//! dot-above rules; `types::FOLD_CONTEXT_PROBES` and the drift cases below)
//! and on a sample of 20,000 random strings over the cased code points and
//! the context-rule characters.
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
use nodal_db_core::fold_table::LOWER;
use nodal_db_core::types::{engine_fold_mismatch, fold, FOLD_CONTEXT_PROBES};

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

/// Every code point cased in the toolchain's Unicode (a case mapping
/// changes it, either way): the random strings' alphabet. The toolchain's
/// tables are newer than the fold's 15.1, so the pool also holds the
/// Unicode 16 and 17 case pairs the fold leaves alone.
fn cased_pool() -> Vec<char> {
    (0..=0x10_FFFFu32)
        .filter_map(char::from_u32)
        .filter(|&c| {
            !c.to_lowercase().eq(std::iter::once(c)) || !c.to_uppercase().eq(std::iter::once(c))
        })
        .collect()
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
    let mut pool: Vec<char> = cased_pool();
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

/// `LOWER` with `from -> to` added (or replaced), still sorted.
fn table_with(from: char, to: char) -> Vec<(char, char)> {
    let mut t: Vec<(char, char)> = LOWER.iter().copied().filter(|&(f, _)| f != from).collect();
    t.push((from, to));
    t.sort();
    t
}

/// The runtime engine check (`types::check_engine_fold`) passes on the
/// bundled engine and refuses one whose `lower()` disagrees with the table,
/// simulated by a wrong table: a Unicode 16 pair (U+1C89 -> U+1C8A) and a
/// Unicode 17 one (U+A7CE -> U+A7CF), which a probe of only the code points
/// cased in 15.1 or 16.0 missed; a code point the table maps that the
/// engine leaves alone; and a table that drops a mapping. It also reports
/// what the check costs, and pins it under a generous bound.
#[test]
fn engine_check_passes_here_and_refuses_a_mismatch() {
    let conn = Connection::open_in_memory().expect("open");
    let start = std::time::Instant::now();
    assert_eq!(engine_fold_mismatch(&conn, &LOWER).expect("check"), None);
    let cost = start.elapsed();
    eprintln!("engine fold check: {cost:?}");
    for (table, want) in [
        (
            table_with('\u{1c89}', '\u{1c8a}'),
            "U+1C89 lowers to U+1C89 in the engine, U+1C8A in the core",
        ),
        (
            table_with('\u{a7ce}', '\u{a7cf}'),
            "U+A7CE lowers to U+A7CE in the engine, U+A7CF in the core",
        ),
        (
            table_with('\u{16ea0}', '\u{16ebb}'),
            "U+16EA0 lowers to U+16EA0 in the engine, U+16EBB in the core",
        ),
        (
            LOWER.iter().copied().filter(|&(f, _)| f != 'Σ').collect(),
            "U+03A3 lowers to U+03C3 in the engine, U+03A3 in the core",
        ),
    ] {
        let refusal = engine_fold_mismatch(&conn, &table)
            .expect("check")
            .expect("a mismatch is refused");
        assert!(refusal.contains(want), "{refusal}");
    }
    // Speed: once per process, so a generous bound that still catches an
    // accidental per-code-point round trip (debug builds included).
    assert!(cost < std::time::Duration::from_millis(1500), "{cost:?}");
}

/// The engine check's context strings: `lower()` of each, whole, is the
/// fold (no final sigma, no SpecialCasing), and each probe is something
/// the all-code-points string cannot show.
#[test]
fn the_context_probes_lower_as_the_fold() {
    let conn = Connection::open_in_memory().expect("open");
    for p in FOLD_CONTEXT_PROBES {
        assert_eq!(duckdb_lower(&conn, p), fold(p), "{}", hex(p));
    }
    assert!(FOLD_CONTEXT_PROBES.contains(&"\u{391}\u{3a3}"));
    assert!(FOLD_CONTEXT_PROBES.contains(&"\u{391}\u{3a3} "));
    assert!(FOLD_CONTEXT_PROBES.contains(&"\u{130}\u{307}"));
}
