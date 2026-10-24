//! Compaction size on a history-heavy long table
//! (`specs/compaction_growth_FINDINGS.md`): the shape where the first
//! clustering order (closed rows by key) made compaction GROW the file.
//!
//! Source: `CELLS` rank-1 long cells (`SERIES` entities × positions on one
//! integer axis), fully rewritten `ROUNDS` times, one commit per round,
//! appended round by round with a checkpoint after each (the write path's
//! append order and its per-commit checkpoints). Built with raw SQL:
//! compaction copies rows verbatim, so the write path adds nothing to what is
//! measured, and the build stays seconds instead of minutes.
//!
//! Measured: file bytes and `used_blocks` (× 256 KiB) of the source and of
//! the compacted copy (`compaction::compact`), the compaction wall time, and
//! the size guard's verdict (`warning` / `kept_original`) of a second,
//! automatic run on the compacted copy.
//!
//! Run: `cargo bench -p nodal-db-core --bench compaction_size` (≈ a minute;
//! `-- --quick` for the 1 M-row test-fixture size). Results:
//! `benches/RESULTS.md`, section "Compaction size". `test = false`: never
//! part of `cargo test --workspace`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use duckdb::Connection;

use nodal_db_core::compaction::{compact, compact_if_due, CommitBackstop, CompactionPolicy};
use nodal_db_core::schema_init::init_schema;

fn build(dir: &Path, series: i64, cells: i64, rounds: i64) -> PathBuf {
    let db = dir.join("history_heavy.duckdb");
    init_schema(&db).unwrap();
    let per = cells / series;
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(&format!(
        "INSERT INTO axis (kind, length, content_hash, p_start_int, p_step_int) \
         VALUES ('integer', {per}, 0, 0, 1);"
    ))
    .unwrap();
    let axis: i64 = conn
        .query_row("SELECT max(id) FROM axis", [], |r| r.get(0))
        .unwrap();
    let commits: Vec<i64> = (0..rounds)
        .map(|_| {
            conn.query_row(
                "INSERT INTO \"commit\" (user, message) VALUES ('b', 'round') RETURNING id",
                [],
                |r| r.get(0),
            )
            .unwrap()
        })
        .collect();
    for r in 0..rounds as usize {
        let to = commits
            .get(r + 1)
            .map_or_else(|| "NULL".to_string(), |c| c.to_string());
        conn.execute_batch(&format!(
            "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, \
               valid_from_commit, valid_to_commit, version, axis1_id, i1_pos, value_kind, v_float) \
             SELECT c // {per}, 1, 1, {from}, {to}, {ver}, {axis}, (c % {per})::INTEGER, \
                    'float', (hash(c, {r}) % 100000) / 7.0 \
             FROM range({cells}) t(c) ORDER BY c; \
             CHECKPOINT;",
            from = commits[r],
            ver = r + 1,
        ))
        .unwrap();
    }
    conn.close().unwrap();
    db
}

fn used_mib(db: &Path) -> f64 {
    let conn = Connection::open(db).unwrap();
    let (used, size): (i64, i64) = conn
        .query_row(
            "SELECT used_blocks, block_size FROM pragma_database_size()",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    conn.close().unwrap();
    (used * size) as f64 / (1024.0 * 1024.0)
}

fn mib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

fn main() {
    let quick = std::env::args().any(|a| a == "--quick");
    let (series, cells, rounds) = if quick {
        (50, 50_000, 21)
    } else {
        (500, 500_000, 21)
    };
    let dir = tempfile::tempdir().unwrap();
    println!(
        "source: {cells} long cells ({series} series) x {rounds} full rewrites = {} rows",
        cells * rounds
    );
    let t = Instant::now();
    let db = build(dir.path(), series, cells, rounds);
    println!("built in {:.1} s", t.elapsed().as_secs_f64());
    let src_used = used_mib(&db);

    let t = Instant::now();
    let r = compact(&db).unwrap();
    let t_compact = t.elapsed().as_secs_f64();
    let out_used = used_mib(&db);

    println!();
    println!("| file | file size | used | compaction |");
    println!("|---|---|---|---|");
    println!(
        "| source | {:.1} MiB | {src_used:.1} MiB | |",
        mib(r.bytes_before)
    );
    println!(
        "| compacted | {:.1} MiB | {out_used:.1} MiB | {t_compact:.1} s |",
        mib(r.bytes_after)
    );
    if let Some(w) = &r.warning {
        println!("explicit compact warning: {w}");
    }

    // The size guard: an automatic run on the freshly compacted file has
    // nothing to gain and must keep the original.
    let again = compact_if_due(
        &db,
        CompactionPolicy {
            growth_factor: 0.5,
            max_commits: CommitBackstop::Off,
        },
        None,
    )
    .unwrap();
    println!(
        "automatic re-run: swapped {}, kept_original {}, rebuilt {:.1} MiB",
        again.swapped,
        again.kept_original,
        mib(again.bytes_rebuilt)
    );
}
