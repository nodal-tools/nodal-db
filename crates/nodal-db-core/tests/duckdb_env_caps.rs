//! Guard for the DuckDB caps every `cargo test`, `cargo run` and `cargo
//! bench` in this repo runs under (`.cargo/config.toml [env]`; CONTRIBUTING
//! "Machine budget"; perf_gate_footprint_PLAN D6) and that the perf gate sets
//! per sample (`scripts/bench/ab/ab.py`): DuckDB sizes `threads` from
//! `SLURM_CPUS_ON_NODE` and `memory_limit` (80 % of it) from
//! `SLURM_MEM_PER_NODE` (MB) when it creates an instance. If a DuckDB bump
//! drops or renames that detection, the caps stop working silently (every
//! instance back at all cores and 80 % of RAM): this test then fails, and so
//! does the gate's start-up check.
//!
//! Run outside cargo (the variable unset) it fails, so that a lost `[env]`
//! entry cannot make it pass vacuously. An empty value is the explicit
//! opt-out (`just full`'s release-build guard step runs uncapped that way:
//! DuckDB ignores a value it cannot parse); then there is nothing to check.

use nodal_db_core::conn::with_read;
use nodal_db_core::schema_init::init_schema_with_default;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!(
            "{name} is not set: .cargo/config.toml [env] sets it for cargo test \
             (run the test through cargo, or set it explicitly)"
        )
    })
}

#[test]
fn a_core_connection_is_sized_from_the_slurm_env() {
    let cpus = env("SLURM_CPUS_ON_NODE");
    let mem_mb = env("SLURM_MEM_PER_NODE");
    if cpus.is_empty() || mem_mb.is_empty() {
        eprintln!("SLURM_* set empty: uncapped run, nothing to check");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("caps.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    let mem_mb: f64 = mem_mb.parse().expect("SLURM_MEM_PER_NODE in MB");
    let (threads, limit, want): (String, String, String) = with_read(&db, |c| {
        Ok(c.query_row(
            "SELECT current_setting('threads')::VARCHAR, \
             current_setting('memory_limit')::VARCHAR, \
             format_bytes((0.8 * ? * 1000000)::BIGINT)",
            [mem_mb],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?)
    })
    .unwrap();
    assert_eq!(
        threads, cpus,
        "DuckDB no longer sizes threads from SLURM_CPUS_ON_NODE: the test and \
         perf-gate caps are not in effect (CONTRIBUTING \"DuckDB upgrades\")"
    );
    assert_eq!(
        limit, want,
        "DuckDB no longer sizes memory_limit from SLURM_MEM_PER_NODE (80 % of it)"
    );
}
