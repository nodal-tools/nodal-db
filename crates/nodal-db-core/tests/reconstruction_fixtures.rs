//! Array-tier reconstruction conformance (rust_implementation_plan.md Part 5,
//! deliverable 2; specs/reconstruction.md). Each
//! `conformance/reconstruction_fixtures/*.json` gives a dense flat `DOUBLE[]`
//! payload + its axes (declared order) and the expected long rows
//! (index-tuple -> value). The runner reconstructs via
//! `nodal_db_core::reconstruct::reconstruct` and asserts equality — the
//! non-square shapes (2x3, 2x3x2) fail loudly on a column-major transpose bug.
//!
//! Fixtures flagged `sql_crosscheck` are additionally inserted as a real
//! `value_array` row into a fresh schema DB and expanded through the DuckDB
//! `value_array_cells` read view; the Rust reconstruction MUST match the SQL
//! expansion bit-for-bit (spec §2.1; CLAUDE.md invariant 2 applied to reads).

use std::fs;
use std::path::{Path, PathBuf};

use duckdb::Connection;
use serde::Deserialize;

use nodal_db_core::reconstruct::{expand_dense_row, reconstruct};
use nodal_db_core::types::ScalarValue;

#[derive(Deserialize)]
struct Fixture {
    description: String,
    #[serde(default)]
    sql_crosscheck: bool,
    axes: Vec<AxisSpec>,
    values: Vec<f64>,
    expected: Vec<ExpectedRow>,
}

#[derive(Deserialize)]
struct AxisSpec {
    length: usize,
    #[serde(default)]
    positions: Vec<i64>,
}

#[derive(Deserialize)]
struct ExpectedRow {
    index: Vec<i32>,
    value: f64,
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("conformance/reconstruction_fixtures")
}

fn fixture_files() -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(fixtures_dir())
        .expect("read reconstruction_fixtures dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    v.sort();
    v
}

#[test]
fn reconstruction_fixtures_match() {
    let files = fixture_files();
    assert!(
        files.len() >= 4,
        "expected >=4 fixtures, got {}",
        files.len()
    );
    let mut crosschecked = 0;

    for path in &files {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let raw = fs::read_to_string(path).expect("read fixture");
        let fx: Fixture =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"));

        let lengths: Vec<usize> = fx.axes.iter().map(|a| a.length).collect();
        let rows = reconstruct(&lengths, &fx.values)
            .unwrap_or_else(|e| panic!("{name}: reconstruct failed: {e} ({})", fx.description));

        assert_eq!(
            rows.len(),
            fx.expected.len(),
            "{name}: row count ({})",
            fx.description
        );
        for (i, (got, want)) in rows.iter().zip(fx.expected.iter()).enumerate() {
            assert_eq!(
                got.positions, want.index,
                "{name}: row {i} index ({})",
                fx.description
            );
            match got.value {
                ScalarValue::Float(x) => assert_eq!(
                    x.to_bits(),
                    want.value.to_bits(),
                    "{name}: row {i} value ({})",
                    fx.description
                ),
                ref other => panic!("{name}: expected float measure, got {other:?}"),
            }
        }

        if fx.sql_crosscheck {
            sql_crosscheck(&name, &fx);
            crosschecked += 1;
        }
    }

    assert!(
        crosschecked >= 1,
        "at least one sql_crosscheck fixture expected"
    );
    println!(
        "reconstruction fixtures: {} checked ({} with SQL cross-check)",
        files.len(),
        crosschecked
    );
}

/// The same fixtures run in the INVERSE (write) direction
/// (specs/arrow_ipc_bulk_format.md §6.3): treat each fixture's axes as a dense
/// wire row's coordinate lists, expand via `expand_dense_row`, and require the
/// (coordinate tuple, value) pairs to be exactly the fixture's expected long
/// rows mapped through the coordinate lists — then round-trip: the expansion's
/// positions must be `reconstruct`'s positions bit-for-bit.
#[test]
fn reconstruction_fixtures_expand_inverse_direction() {
    for path in fixture_files() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let raw = fs::read_to_string(&path).expect("read fixture");
        let fx: Fixture =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {name}: {e}"));

        // Coordinate lists: the fixture's explicit axis positions (fall back to
        // 0..length for any fixture that omits them).
        let dim_lists: Vec<Vec<i64>> = fx
            .axes
            .iter()
            .map(|a| {
                if a.positions.is_empty() {
                    (0..a.length as i64).collect()
                } else {
                    assert_eq!(
                        a.positions.len(),
                        a.length,
                        "{name}: axis positions vs length"
                    );
                    a.positions.clone()
                }
            })
            .collect();

        let expanded = expand_dense_row(&dim_lists, &fx.values)
            .unwrap_or_else(|e| panic!("{name}: expand failed: {e} ({})", fx.description));
        assert_eq!(expanded.len(), fx.expected.len(), "{name}: cell count");

        // Round-trip through reconstruct(): the read side's positions must pair
        // with exactly the coordinates the expansion emitted.
        let lengths: Vec<usize> = fx.axes.iter().map(|a| a.length).collect();
        let rows = reconstruct(&lengths, &fx.values).expect("reconstruct");

        for (k, ((coords, v), want)) in expanded.iter().zip(fx.expected.iter()).enumerate() {
            let want_coords: Vec<i64> = want
                .index
                .iter()
                .enumerate()
                .map(|(i, &p)| dim_lists[i][p as usize])
                .collect();
            assert_eq!(
                coords, &want_coords,
                "{name}: cell {k} coords ({})",
                fx.description
            );
            assert_eq!(
                v.to_bits(),
                want.value.to_bits(),
                "{name}: cell {k} value ({})",
                fx.description
            );
            let rt: Vec<i64> = rows[k]
                .positions
                .iter()
                .enumerate()
                .map(|(i, &p)| dim_lists[i][p as usize])
                .collect();
            assert_eq!(
                coords, &rt,
                "{name}: cell {k} expand/reconstruct round-trip"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// SQL cross-check: insert the payload as a real value_array row and expand it
// through the DuckDB value_array_cells view; assert it equals the Rust rows.
// Specialised to rank 2 (the crosscheck fixture) — enough to pin agreement.

fn schema_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("schema")
}

fn apply_schema(conn: &Connection) {
    let mut files: Vec<PathBuf> = fs::read_dir(schema_dir())
        .expect("read schema dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().map(|x| x == "sql").unwrap_or(false)
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| {
                        n.chars()
                            .next()
                            .map(|c| c.is_ascii_digit())
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
        })
        .collect();
    files.sort();
    for f in files {
        let sql = fs::read_to_string(&f).expect("read sql file");
        conn.execute_batch(&sql)
            .unwrap_or_else(|e| panic!("FAILED applying {}: {e}", f.display()));
    }
}

fn sql_crosscheck(name: &str, fx: &Fixture) {
    assert_eq!(
        fx.axes.len(),
        2,
        "{name}: SQL cross-check fixture must be rank 2"
    );
    let conn = Connection::open_in_memory().expect("in-memory duckdb");
    apply_schema(&conn);

    // The schema seeds commit id 1 (the genesis commit); reuse it for
    // valid_from_commit. Insert only the two integer parametric axes.
    for (i, ax) in fx.axes.iter().enumerate() {
        conn.execute(
            "INSERT INTO axis (id, kind, length, content_hash, p_start_int, p_step_int)
             VALUES (?, 'integer', ?, 0, 0, 1)",
            duckdb::params![201 + i as i64, ax.length as i32],
        )
        .expect("insert axis");
    }

    // The value_array row: dense DOUBLE[] payload, row-major (spec §2).
    let list_lit = format!(
        "[{}]::DOUBLE[]",
        fx.values
            .iter()
            .map(|v| format!("{v:?}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    conn.execute(
        &format!(
            "INSERT INTO value_array
               (entity_id, parameter_definition_id, alternative_id,
                valid_from_commit, version, axis1_id, axis2_id, values)
             VALUES (1, 1, 1, 1, 1, 201, 202, {list_lit})"
        ),
        [],
    )
    .expect("insert value_array");

    // Expand through the read view, flat order.
    let mut stmt = conn
        .prepare(
            "SELECT i1_pos, i2_pos, v_float FROM value_array_cells
             WHERE entity_id = 1 ORDER BY i1_pos, i2_pos",
        )
        .unwrap();
    let sql_rows: Vec<(i32, i32, f64)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i32>(0)?,
                r.get::<_, i32>(1)?,
                r.get::<_, f64>(2)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    // The Rust reconstruction, same order (reconstruct emits flat/row-major order).
    let lengths: Vec<usize> = fx.axes.iter().map(|a| a.length).collect();
    let rust_rows = reconstruct(&lengths, &fx.values).unwrap();

    assert_eq!(
        sql_rows.len(),
        rust_rows.len(),
        "{name}: SQL vs Rust row count"
    );
    for (sql, rust) in sql_rows.iter().zip(rust_rows.iter()) {
        let rv = match rust.value {
            ScalarValue::Float(x) => x,
            ref o => panic!("{name}: non-float rust value {o:?}"),
        };
        assert_eq!(
            (sql.0, sql.1),
            (rust.positions[0], rust.positions[1]),
            "{name}: SQL position != Rust reconstruction (transpose?)"
        );
        assert_eq!(
            sql.2.to_bits(),
            rv.to_bits(),
            "{name}: SQL value != Rust value at ({}, {})",
            sql.0,
            sql.1
        );
    }
}
