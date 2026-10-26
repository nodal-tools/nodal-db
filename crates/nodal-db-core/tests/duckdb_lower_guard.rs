//! Name-fold drift guard (plan T11, `specs/unicode_text_rule_PLAN.md` §1.3
//! and §4): the Rust name fold (`types::fold`, Rust's `str::to_lowercase`)
//! and DuckDB's `lower()` (the `cur_name` columns, their unique indexes and
//! every SQL resolver) are two independent case tables, and they already
//! differ. Neither is pinned by anything else: the Rust tables move with the
//! toolchain (there is no `rust-toolchain` file), DuckDB's with the bundled
//! engine.
//!
//! Where they differ, a Rust-side collision check and the SQL unique index
//! disagree: `ΟΔΟΣ` and `οδοσ` pass the Rust check and then collide on the
//! DuckDB index mid-transaction. Fixing that is its own plan (T11); this
//! guard only makes any CHANGE in the difference loud. It compares the two
//! folds on every Unicode scalar value and asserts the set of differences
//! is exactly the committed `KNOWN_DRIFT` list, plus the context rules a
//! per-code-point scan cannot see (final sigma) and `İ`.
//!
//! When it fails after a toolchain or DuckDB bump: the message lists the
//! code points that started or stopped differing. Re-evaluate the name-fold
//! plan (T11) with them, then update `KNOWN_DRIFT`; if DuckDB's case mapping
//! changed, compact the dev files too (CONTRIBUTING "DuckDB upgrades": the
//! `lower(name)` indexes persist keys computed by the engine that built
//! them).

use std::collections::BTreeMap;

use nodal_db_core::duckdb::Connection;
use nodal_db_core::types::fold;

/// The code points whose one-character fold differs, as
/// `(code point, DuckDB lower(), Rust fold)` (DuckDB 2.0.0, Rust 1.98):
/// `İ`, then case pairs added in recent Unicode versions that DuckDB's
/// tables lack and Rust's have.
#[rustfmt::skip]
const KNOWN_DRIFT: &[(u32, &str, &str)] = &[
    (0x0130, "i", "i\u{307}"),
    (0x1C89, "\u{1c89}", "\u{1c8a}"),
    (0xA7CB, "\u{a7cb}", "\u{264}"),
    (0xA7CC, "\u{a7cc}", "\u{a7cd}"),
    (0xA7CE, "\u{a7ce}", "\u{a7cf}"),
    (0xA7D2, "\u{a7d2}", "\u{a7d3}"),
    (0xA7D4, "\u{a7d4}", "\u{a7d5}"),
    (0xA7DA, "\u{a7da}", "\u{a7db}"),
    (0xA7DC, "\u{a7dc}", "\u{19b}"),
    (0x10D50, "\u{10d50}", "\u{10d70}"),
    (0x10D51, "\u{10d51}", "\u{10d71}"),
    (0x10D52, "\u{10d52}", "\u{10d72}"),
    (0x10D53, "\u{10d53}", "\u{10d73}"),
    (0x10D54, "\u{10d54}", "\u{10d74}"),
    (0x10D55, "\u{10d55}", "\u{10d75}"),
    (0x10D56, "\u{10d56}", "\u{10d76}"),
    (0x10D57, "\u{10d57}", "\u{10d77}"),
    (0x10D58, "\u{10d58}", "\u{10d78}"),
    (0x10D59, "\u{10d59}", "\u{10d79}"),
    (0x10D5A, "\u{10d5a}", "\u{10d7a}"),
    (0x10D5B, "\u{10d5b}", "\u{10d7b}"),
    (0x10D5C, "\u{10d5c}", "\u{10d7c}"),
    (0x10D5D, "\u{10d5d}", "\u{10d7d}"),
    (0x10D5E, "\u{10d5e}", "\u{10d7e}"),
    (0x10D5F, "\u{10d5f}", "\u{10d7f}"),
    (0x10D60, "\u{10d60}", "\u{10d80}"),
    (0x10D61, "\u{10d61}", "\u{10d81}"),
    (0x10D62, "\u{10d62}", "\u{10d82}"),
    (0x10D63, "\u{10d63}", "\u{10d83}"),
    (0x10D64, "\u{10d64}", "\u{10d84}"),
    (0x10D65, "\u{10d65}", "\u{10d85}"),
    (0x16EA0, "\u{16ea0}", "\u{16ebb}"),
    (0x16EA1, "\u{16ea1}", "\u{16ebc}"),
    (0x16EA2, "\u{16ea2}", "\u{16ebd}"),
    (0x16EA3, "\u{16ea3}", "\u{16ebe}"),
    (0x16EA4, "\u{16ea4}", "\u{16ebf}"),
    (0x16EA5, "\u{16ea5}", "\u{16ec0}"),
    (0x16EA6, "\u{16ea6}", "\u{16ec1}"),
    (0x16EA7, "\u{16ea7}", "\u{16ec2}"),
    (0x16EA8, "\u{16ea8}", "\u{16ec3}"),
    (0x16EA9, "\u{16ea9}", "\u{16ec4}"),
    (0x16EAA, "\u{16eaa}", "\u{16ec5}"),
    (0x16EAB, "\u{16eab}", "\u{16ec6}"),
    (0x16EAC, "\u{16eac}", "\u{16ec7}"),
    (0x16EAD, "\u{16ead}", "\u{16ec8}"),
    (0x16EAE, "\u{16eae}", "\u{16ec9}"),
    (0x16EAF, "\u{16eaf}", "\u{16eca}"),
    (0x16EB0, "\u{16eb0}", "\u{16ecb}"),
    (0x16EB1, "\u{16eb1}", "\u{16ecc}"),
    (0x16EB2, "\u{16eb2}", "\u{16ecd}"),
    (0x16EB3, "\u{16eb3}", "\u{16ece}"),
    (0x16EB4, "\u{16eb4}", "\u{16ecf}"),
    (0x16EB5, "\u{16eb5}", "\u{16ed0}"),
    (0x16EB6, "\u{16eb6}", "\u{16ed1}"),
    (0x16EB7, "\u{16eb7}", "\u{16ed2}"),
    (0x16EB8, "\u{16eb8}", "\u{16ed3}"),
];

fn hex(s: &str) -> String {
    s.chars()
        .map(|c| format!("U+{:04X}", c as u32))
        .collect::<Vec<_>>()
        .join(" ")
}

/// `s` as a Rust string literal body, non-ASCII escaped.
fn esc(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii() {
                c.to_string()
            } else {
                format!("\\u{{{:x}}}", c as u32)
            }
        })
        .collect()
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
fn rust_fold_and_duckdb_lower_differ_only_where_known() {
    let conn = Connection::open_in_memory().expect("open");
    let duck = duckdb_changed(&conn);
    assert!(
        duck.len() > 1_000,
        "DuckDB lower() changed only {} code points",
        duck.len()
    );

    // Every scalar value: where neither side changes it, both are the
    // identity; elsewhere compare (DuckDB's identity when it is not listed).
    let mut drift: BTreeMap<u32, (String, String)> = BTreeMap::new();
    for cp in 1..=0x10_FFFFu32 {
        let Some(c) = char::from_u32(cp) else {
            continue; // surrogates
        };
        let s = c.to_string();
        let rust = fold(&s);
        let duckdb = duck.get(&cp).cloned().unwrap_or_else(|| s.clone());
        if rust != duckdb {
            drift.insert(cp, (duckdb, rust));
        }
    }

    let known: BTreeMap<u32, (String, String)> = KNOWN_DRIFT
        .iter()
        .map(|(cp, d, r)| (*cp, (d.to_string(), r.to_string())))
        .collect();
    if drift != known {
        let mut msg = String::from(
            "the Rust name fold and DuckDB lower() drifted from the committed list \
             (plan T11; see this file's header)\n",
        );
        for (cp, (d, r)) in &drift {
            if known.get(cp) != Some(&(d.clone(), r.clone())) {
                msg += &format!(
                    "  new/changed U+{cp:04X}: duckdb {} rust {}\n",
                    hex(d),
                    hex(r)
                );
            }
        }
        for cp in known.keys() {
            if !drift.contains_key(cp) {
                msg += &format!("  no longer differs: U+{cp:04X}\n");
            }
        }
        msg += "current list:\n";
        for (cp, (d, r)) in &drift {
            msg += &format!("    (0x{cp:04X}, \"{}\", \"{}\"),\n", esc(d), esc(r));
        }
        panic!("{msg}");
    }
}

/// Context rules a per-code-point scan cannot see, and the dotted capital
/// I, pinned as strings on both sides.
#[test]
fn context_rule_strings_fold_as_known() {
    let conn = Connection::open_in_memory().expect("open");
    // Final sigma: Rust applies the context rule, DuckDB does not, so
    // `ΟΔΟΣ` and `οδοσ` are one name to DuckDB and two to Rust.
    assert_eq!(duckdb_lower(&conn, "ΟΔΟΣ"), "οδοσ");
    assert_eq!(fold("ΟΔΟΣ"), "οδος");
    assert_eq!(duckdb_lower(&conn, "οδοσ"), "οδοσ");
    assert_eq!(fold("οδοσ"), "οδοσ");
    // İ (U+0130): DuckDB folds to plain i, Rust to i + U+0307.
    assert_eq!(duckdb_lower(&conn, "\u{130}"), "i");
    assert_eq!(fold("\u{130}"), "i\u{307}");
    assert_eq!(duckdb_lower(&conn, "İSTANBUL"), "istanbul");
    assert_eq!(fold("İSTANBUL"), "i\u{307}stanbul");
}
