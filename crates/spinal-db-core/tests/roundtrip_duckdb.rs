//! §10.5 cross-engine round-trip fixture (spec MUST, §10.5 / §12).
//!
//! The founding two-canonicalizers lesson (CLAUDE.md invariant 2) means the SQL
//! `axis_position` view and the Rust reconstruction MUST agree bit-for-bit; the
//! spec's FMA/clamp pins (§10.2, §7.1.2.1) are only as good as a test that runs
//! BOTH engines and compares. This materializes parametric axes through the real
//! schema, reads their coordinates back through the DuckDB `axis_position` view,
//! and asserts they equal the Rust `encoding::datetime_coord` / `start+step*pos`
//! results bit-exactly (µs-exact for datetime, IEEE-bit-exact for float).
//!
//! It is the standing regression guard: it MUST fail loudly if a future DuckDB
//! build starts FMA-contracting `start + step*pos` (§10.2 [verified] note) or
//! changes month-clamp (`to_months`) semantics (§7.1.2.1).

use std::fs;
use std::path::{Path, PathBuf};

use duckdb::Connection;
use spinal_db_core::encoding::{datetime_coord, Duration};

fn schema_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("schema")
}

fn numbered_sql_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .expect("read schema dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension().map(|x| x == "sql").unwrap_or(false)
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false))
                    .unwrap_or(false)
        })
        .collect();
    files.sort();
    files
}

fn apply_schema(conn: &Connection) {
    for f in numbered_sql_files(&schema_dir()) {
        let sql = fs::read_to_string(&f).expect("read sql file");
        conn.execute_batch(&sql)
            .unwrap_or_else(|e| panic!("FAILED applying {}: {e}", f.display()));
    }
}

/// Read `(pos, epoch_us(ts))` for a datetime axis back through `axis_position`.
fn read_datetime_coords(conn: &Connection, axis_id: i64) -> Vec<(i32, i64)> {
    let mut stmt = conn
        .prepare(
            "SELECT pos, epoch_us(ts) FROM axis_position WHERE axis_id = ? ORDER BY pos",
        )
        .unwrap();
    let rows = stmt
        .query_map([axis_id], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, i64>(1)?)))
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

/// Read `(pos, coord)` for a float axis back through `axis_position`.
fn read_float_coords(conn: &Connection, axis_id: i64) -> Vec<(i32, f64)> {
    let mut stmt = conn
        .prepare(
            "SELECT pos, coord FROM axis_position WHERE axis_id = ? ORDER BY pos",
        )
        .unwrap();
    let rows = stmt
        .query_map([axis_id], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, f64>(1)?)))
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

#[test]
fn parametric_axes_roundtrip_bit_exact_through_axis_position() {
    let conn = Connection::open_in_memory().expect("in-memory duckdb");
    apply_schema(&conn);

    // ---- datetime, hourly micros-only step (vector 01 parameters) ----------
    // start 2020-01-01T00:00:00, step (0 months, 3_600_000_000 µs), length 24.
    let dt_hourly_start: i64 = 1_577_836_800_000_000;
    let dt_hourly_step = Duration { months: 0, micros: 3_600_000_000 };
    let dt_hourly_len: i32 = 24;
    conn.execute(
        "INSERT INTO axis (id, kind, length, content_hash, p_start_ts, p_step_months, p_step_micros)
         VALUES (101, 'datetime', ?, 0, make_timestamp(?), ?, ?)",
        duckdb::params![
            dt_hourly_len,
            dt_hourly_start,
            dt_hourly_step.months,
            dt_hourly_step.micros
        ],
    )
    .expect("insert hourly datetime axis");

    // ---- datetime, month-clamp step (vector 03 parameters) -----------------
    // start 2001-01-31T00:00:00, step (1 month, 0 µs), length 4. This is the
    // subtle one: pos 0..3 must clamp to [Jan31, Feb28, Mar31, Apr30] via the
    // single-multiply `to_months` semantics (§7.1.2.1).
    let dt_clamp_start: i64 = 980_899_200_000_000;
    let dt_clamp_step = Duration { months: 1, micros: 0 };
    let dt_clamp_len: i32 = 4;
    conn.execute(
        "INSERT INTO axis (id, kind, length, content_hash, p_start_ts, p_step_months, p_step_micros)
         VALUES (103, 'datetime', ?, 0, make_timestamp(?), ?, ?)",
        duckdb::params![
            dt_clamp_len,
            dt_clamp_start,
            dt_clamp_step.months,
            dt_clamp_step.micros
        ],
    )
    .expect("insert month-clamp datetime axis");

    // ---- float, exact-decimal step (vector 07 parameters) ------------------
    // start 0.0, step 0.25, length 4 -> [0, 0.25, 0.5, 0.75].
    let f_start: f64 = 0.0;
    let f_step: f64 = 0.25;
    let f_len: i32 = 4;
    conn.execute(
        "INSERT INTO axis (id, kind, length, content_hash, p_start_dbl, p_step_dbl)
         VALUES (107, 'float', ?, 0, ?, ?)",
        duckdb::params![f_len, f_start, f_step],
    )
    .expect("insert parametric float axis");

    // ---- float, FMA-divergence axis (vector 21 parameters) -----------------
    // start -4675.83329566021, step 7006.486065829911 (>0), length 6. At pos 5
    // the pinned non-FMA `start + (step*pos)` = 0x40dda52635cbf3d8, whereas a
    // fused multiply-add gives 0x40dda52635cbf3d7 (1 ULP lower). This axis is
    // the standing cross-engine guard that DuckDB does NOT FMA-contract §10.2.
    let fma_start: f64 = -4675.83329566021;
    let fma_step: f64 = 7006.486065829911;
    let fma_len: i32 = 6;
    conn.execute(
        "INSERT INTO axis (id, kind, length, content_hash, p_start_dbl, p_step_dbl)
         VALUES (108, 'float', ?, 0, ?, ?)",
        duckdb::params![fma_len, fma_start, fma_step],
    )
    .expect("insert FMA-divergence float axis");

    // ---- datetime hourly: µs-exact vs Rust datetime_coord ------------------
    let got = read_datetime_coords(&conn, 101);
    assert_eq!(got.len(), dt_hourly_len as usize, "hourly row count");
    for (pos, ts) in got {
        let want = datetime_coord(dt_hourly_start, &dt_hourly_step, pos as i64)
            .expect("no overflow");
        assert_eq!(
            ts, want,
            "hourly datetime pos {pos}: DuckDB {ts} != Rust {want} (µs)"
        );
    }

    // ---- datetime month-clamp: µs-exact, and the clamp actually happened ----
    let got = read_datetime_coords(&conn, 103);
    assert_eq!(got.len(), dt_clamp_len as usize, "clamp row count");
    // Vector 03 coordinates: Jan31, Feb28, Mar31, Apr30 (each 00:00:00).
    let expected_clamp = [
        980_899_200_000_000_i64,
        983_318_400_000_000,
        985_996_800_000_000,
        988_588_800_000_000,
    ];
    for (pos, ts) in got {
        let want = datetime_coord(dt_clamp_start, &dt_clamp_step, pos as i64)
            .expect("no overflow");
        assert_eq!(
            ts, want,
            "clamp datetime pos {pos}: DuckDB {ts} != Rust {want} (µs)"
        );
        assert_eq!(
            ts, expected_clamp[pos as usize],
            "clamp datetime pos {pos}: DuckDB {ts} != spec-pinned {} — to_months clamp drift?",
            expected_clamp[pos as usize]
        );
    }

    // ---- float: IEEE-bit-exact vs Rust `start + step*pos` (no FMA) ----------
    let got = read_float_coords(&conn, 107);
    assert_eq!(got.len(), f_len as usize, "float row count");
    for (pos, coord) in got {
        // The pinned reconstruction (§10.2): multiply first, then add; two
        // separately-rounded ops, NOT a fused multiply-add.
        let want = f_start + f_step * (pos as f64);
        assert_eq!(
            coord.to_bits(),
            want.to_bits(),
            "float pos {pos}: DuckDB {coord:?} (bits {:#018x}) != Rust {want:?} (bits {:#018x}) — FMA contraction?",
            coord.to_bits(),
            want.to_bits()
        );
    }

    // ---- float FMA-divergence: DuckDB MUST agree with non-FMA, NOT FMA ------
    let got = read_float_coords(&conn, 108);
    assert_eq!(got.len(), fma_len as usize, "FMA-axis row count");
    for (pos, coord) in got {
        let want = fma_start + fma_step * (pos as f64); // non-FMA (§10.2)
        assert_eq!(
            coord.to_bits(),
            want.to_bits(),
            "FMA-axis pos {pos}: DuckDB {coord:?} (bits {:#018x}) != Rust non-FMA {want:?} (bits {:#018x}) — DuckDB FMA-contraction?",
            coord.to_bits(),
            want.to_bits()
        );
        if pos == 5 {
            // The pinned divergence: DuckDB agrees with the two-rounding value
            // (0x40dda52635cbf3d8) and MUST differ from the fused result.
            let via_fma = fma_step.mul_add(pos as f64, fma_start);
            assert_eq!(coord.to_bits(), 0x40dda52635cbf3d8_u64, "FMA-axis pos 5: DuckDB drifted from the spec-pinned non-FMA bits");
            assert_ne!(
                coord.to_bits(),
                via_fma.to_bits(),
                "FMA-axis pos 5: DuckDB matched the FUSED multiply-add — it began FMA-contracting (spec §10.2 drift)"
            );
        }
    }
}
