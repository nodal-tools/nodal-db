# Pre-freeze benchmark gate — recorded results

Plan Part 5 deliverable 4 (`specs/rust_implementation_plan.md`); budgets from
`specs/value_model_plan.md` Part 6; methodology mirrors `specs/ddl_sketch.md`
§10. **Recording gate**: measurements are recorded against budgets; nothing
here fails the build (gating scope is Q6, deliberately open). Reproduce with
`cargo bench` (builds the synthetic DB through the core write path first —
budget ~2 h wall on this machine, dominated by the long-tier write findings
below).

## Environment

| | |
|---|---|
| Date | 2026-08-18 |
| CPU | Intel Core i3-6100 @ 3.70 GHz (2C/4T, Skylake) — **not** the NVMe-class reference machine the §10 baselines were measured on; absolute numbers are not directly comparable, budget ratios are the signal |
| RAM | 16 GB |
| Disk | SATA SSD (`/dev/sda`), ext4; DB in `/tmp` (same device) |
| DuckDB | v1.5.5 (duckdb-rs 1.10505.0, bundled) — §10 baselines were DuckDB 1.4.4 |
| Profile | `bench` (inherits release, opt-level 3) |
| criterion | 0.5.1, sample size 10 for slow benches (wall-clock accuracy over statistical polish) |

## Synthetic DB (ddl_sketch §10 scale, built THROUGH `api::write_parameter`/`ingest_segments`)

1 226 400 live long rows @ 17.6% dead (140 entities × 8 760 int cells; 30
entity-rewrites of churn), 1 000 × 8 760-cell float arrays (~70 MB, Tier A),
100 000 single-parameter scalars. On-disk: **213 MB** after CHECKPOINT.

**Build wall time — this IS the bulk-write benchmark number:**

| Stage | Wall | Per unit |
|---|---|---|
| 100 k scalars (10 commits × 10 k entities) | 1 125.2 s | **11.3 ms/entity** (7.6 ms/entity on the first 10 k, 12.8 ms/entity by the last — grows with entity-table size) |
| 1 000 × 8 760 float arrays (10 commits × 100) | 82.0 s | **82 ms/entity** |
| 140 × 8 760 int long rows (1 commit/entity) | 4 369.8 s | **31.2 s/entity ≈ 3.6 ms/cell** |
| churn: 30 long-entity rewrites | 1 006.7 s | 33.6 s/rewrite |
| **Total** | **6 586.5 s (109.8 min)** | |

Context: fresh read-only connection open + `SELECT 1` on the built 213 MB
file: **534.5 ms** (median of 5). The §10 methodology (and the read benches
below) hold a warm connection; the per-operation open-read-close discipline
(CLAUDE.md invariant 1) pays this per operation — worth its own look before
freeze.

## Read benches vs VMP Part 6 budgets (warm RO connection, end-to-end to Arrow)

| Read (budget) | §10 baseline (1.4.4, ref machine) | Measured (1.5.5, this machine) | Status |
|---|---|---|---|
| Point scalar, triple (< 5 ms) | 1.2 ms | **0.98 ms** | PASS |
| Point long cell, triple (< 5 ms) | 2.2 ms | **0.98 ms** | PASS |
| Point array, triple, full payload (< 5 ms) | 27–31 ms | **77.3 ms** | RECORDED-OVER — the known residual; see below |
| Point array, `v_min`/`v_max` only (mitigation context) | 0.6–0.8 ms | **0.56 ms** | (context) |
| 10⁵ scalars, `read_parameter_scalar` (< 20 ms) | 24.5 ms | **30.0 ms** | RECORDED-OVER (raw-scan parity residual, §10; scale ≈ machine ratio) |
| 1 000 × 8 760 (~70 MB), `read_parameter_dense` (< 300 ms warm) | 219 ms | **311.5 ms** | RECORDED-OVER (borderline; 219 ms on the ref machine — treat as machine-scaled PASS, re-measure on NVMe-class) |
| Same, canonical `read_parameter` (recorded; dense form is the budget path per VMP P6) | 4 240 ms | **7 351 ms** | recorded |
| Model-load context: 1.23 M long rows canonical (< 3 s) | 587 ms | **2 063 ms** | PASS |

**Point-array residual on 1.5.5 — re-measured, NOT improved.** 77 ms vs the
< 5 ms budget (was 27–31 ms on 1.4.4 on a faster machine). The cost is still
pure list-payload row-group decompression; `v_min`-only reads are 0.56 ms, so
no point-query shape avoids decompressing the containing `values` segment on
1.5.5 either. The §10 escalation stands: physical row-group tuning at build
time, client-side caching of hot arrays, or a VMP budget amendment — queued
for re-test again at the next DuckDB upgrade.

## From/to scan degradation vs dead fraction (raw current-rows long scan, Arrow end-to-end)

| Dead fraction | Scan | vs 0% dead |
|---|---|---|
| 0% (pre-churn, builder-measured, median of 5) | 16.1 ms | 1.00× |
| 17.6% | 22.8 ms | **1.41×** |
| 33.3% | 22.7 ms | **1.41×** |

Better than the research baseline (1.6× @ 20%, 2.5× @ 56%): degradation is
present but flat between 17.6% and 33% — churn-generated dead rows cluster
into whole row groups after CHECKPOINT, and `valid_to_commit IS NULL`
zonemaps skip fully-dead groups, exactly the VMP Part 6 prediction. PASS
(recorded).

## Write benches (per cycle, each = one `write_parameter` commit + CHECKPOINT)

| Bench | Measured | Baseline / note |
|---|---|---|
| Scalar replace cycle | **1.24 s** | dominated by CHECKPOINT + RW open/close on the 213 MB file, not the row write |
| Array replace cycle (8 760 cells, `_hist` pre-image copy) | **2.43 s** | research: 4.3 s/cycle for pre-image copy at larger scope |
| Long replace cycle (8 760 cells close-out + re-insert) | **38.1 s** | research churn baseline: 0.9 s per 10-cycle single-table churn — see the alarm below |
| 100 new scalar entities, one commit (no checkpoint) | **2.38 s → 23.8 ms/entity** | at the 100 k-entity table size; from-empty build average is 11.3 ms/entity |

The one-current generated-column unique indexes (`one_current_long` at
7-column width, `one_current_scalar`) are maintained by every write above —
their cost is inside these numbers, not separately measurable here.

## Alarming findings (write path; physical/implementation-level, logical model untouched)

1. **Long-tier writes cost ~3.6 ms/cell in release** (8 760-cell value ≈
   31–38 s to write or replace; the 1.23 M-row synthetic long build alone took
   73 min). The bottleneck is per-cell work in `history::write_long`: one
   un-cached `MAX(version)` probe + one un-cached 29-parameter INSERT
   prepare/execute per cell. Release opt barely helps — this is per-statement
   DuckDB overhead, not Rust. The fix the plan already flags (batched
   multi-row inserts, plus a set-based version computation) is
   logical-model-neutral and is REQUIRED before SpineOpt-scale long-tier
   writes.

   **RESOLVED — physical batching landed (logical model untouched).**
   `write_long` now issues ONE per-key `GROUP BY i*_pos` version scan (instead
   of a per-cell `MAX(version)` probe) and writes all cells with chunked
   multi-row `INSERT`s reusing one prepared statement per 512-row chunk (instead
   of a per-cell prepare/execute). Both run inside the same write transaction, so
   conflict-retry and rollback atomicity are unchanged; column values incl. the
   `make_timestamp` accelerator binds and NaN canonicalization are byte-identical.
   Measured on the focused `long_write` micro-bench (drives `history::write_value`
   directly, no CHECKPOINT / `api` overhead — this machine, release):

   | 8 760-cell op | before (per-cell) | after (batched) | per-cell before → after |
   |---|---|---|---|
   | first-write | 26.5 s | 2.11 s | 3.03 ms → **0.24 ms** (~12.6×) |
   | replace cycle | 29.5 s | 2.42 s | 3.37 ms → **0.28 ms** (~12.2×) |
2. **Per-entity write cost did not melt away in release.** The ~44 ms/entity
   DEBUG fact (F4 scale test, array tier) measures **82 ms/entity in release
   end-to-end** for the same 1000 × 8760 shape — the dominant cost is the
   8 760-placeholder `list_value(...)` INSERT parse/bind per entity plus
   per-entity resolve probes, all DuckDB-side statement work. Scalars:
   11–24 ms/entity (grows with entity-table size; per-entity `resolve_entity`
   name probes are un-indexed scans). Same follow-up as (1): batching.
3. **CHECKPOINT + open/close ≈ 1.2 s on a 213 MB file** puts a floor under
   every open-write-close commit cycle; and a fresh RO open costs ~535 ms.
   Fine for batch tools, material for interactive editors — worth measuring
   on NVMe-class hardware before drawing conclusions.

## Not covered here (unchanged scope)

- Scenario-resolved slice (unbudgeted; in the plan's bench list, not in this
  gate's first cut — add when `maintain`/compaction lands).
- Compaction wall time (needs `maintain`, out of scope — plan Part 5).
- Julia end-to-end read (Q6 flag; Julia impl out of scope, gate stays open).

## Bench wiring

`cargo bench -p nodal-db-core` runs `benches/gate.rs` (criterion,
`harness = false`); the synthetic builder is `benches/support/mod.rs` and
builds the DB in a `nodal-bench-<pid>` tempdir, removed at end of run (stale
dirs swept at start). `[[bench]] test = false` keeps all of this out of
`cargo test --workspace`.

`cargo bench -p nodal-db-core --bench long_write` runs the focused long-tier
write micro-bench (`benches/long_write.rs`): NO synthetic DB — it drives
`history::write_value` directly on a fresh schema DB, isolating per-cell
long-tier write cost (alarm #1 above). Two 8 760-cell shapes: `long_insert_8760`
(fresh key, pure batched insert) and `long_replace_8760` (close-out + reinsert).
