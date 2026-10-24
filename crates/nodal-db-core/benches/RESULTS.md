# Pre-freeze benchmark gate — recorded results

> **Re-baseline note (2026-10-24).** The 1.5.5 column was recorded on
> DuckDB 1.5.5 with 1.5-format files on an Intel i3-6100. The **2.0.0
> column** is current `main` (DuckDB 2.0.0, native-format files, schema 0.4
> `array_id` key, columnar-bind write path) measured on a **different
> machine**, an AMD Ryzen 9 5900XT. The two columns compare neither the
> engines nor the code cleanly: read each column against the budgets, not
> against the other. What changed in the code between them is listed under
> "What changed since the 1.5.5 baseline" below.

Plan Part 5 deliverable 4 (`specs/rust_implementation_plan.md`); budgets from
`specs/value_model_plan.md` Part 6; methodology mirrors `specs/ddl_sketch.md`
§10. **Recording gate**: measurements are recorded against budgets; nothing
here fails the build (gating scope is Q6, deliberately open). Reproduce with
`cargo bench` (builds the synthetic DB through the core write path first —
~2 h wall on the i3-6100 at 1.5.5, dominated by the long-tier write
findings below; ~18 min on the Ryzen host at 2.0.0 including a 3 min
compile, 11.5 min of it the build).

## Environment

**1.5.5 column.**

| | |
|---|---|
| Date | 2026-08-18 |
| CPU | Intel Core i3-6100 @ 3.70 GHz (2C/4T, Skylake) — **not** the NVMe-class reference machine the §10 baselines were measured on; absolute numbers are not directly comparable, budget ratios are the signal |
| RAM | 16 GB |
| Disk | SATA SSD (`/dev/sda`), ext4; DB in `/tmp` (same device) |
| DuckDB | v1.5.5 (duckdb-rs 1.10505.0, bundled) — §10 baselines were DuckDB 1.4.4 |
| Profile | `bench` (inherits release, opt-level 3) |
| criterion | 0.5.1, sample size 10 for slow benches (wall-clock accuracy over statistical polish) |

**2.0.0 column: run conditions.**

| | |
|---|---|
| Date | 2026-10-23 23:48 to 2026-10-24 00:06 (gate), 00:06 (`long_write`) |
| Code | `main` at `eaf2551` |
| CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads |
| RAM | 70 GB |
| Disk | WD_BLACK SN850X NVMe, ext4; DB in `/tmp` (same device) |
| OS / Rust | Linux 6.14.0-37-generic; rustc 1.98.0 |
| DuckDB | v2.0.0 (`v2.0.0-dev88614`; duckdb-rs 1.20000.0, bundled), native-format files |
| Load | 1-min load average 2.2-7.0 while criterion measured (00:00-00:06); 2.8-11 during the synthetic build after its own DuckDB compile finished (34 at the very start, from that compile). One other interactive process used about 0.7 of a core throughout. No other bench ran. |

The 1.5.5 column has not been re-run on the Ryzen host; there is no
same-machine 1.5.5 baseline.

**What changed since the 1.5.5 baseline** (code, not machine):

- DuckDB 1.5.5 → 2.0.0, and the files are native 2.0 format, so an array
  write no longer logs the whole list column to the WAL
  (`scripts/bench/RESULTS_results_path.md` §5).
- Schema 0.4: `value_array.array_id` single-column primary key;
  `read_parameter_dense` resolves the key triple on narrow columns and
  fetches the payload through the ART index (§4b there). The
  `array_point_dense_macro` and `array_point_by_array_id` benches were
  added with it.
- Long-tier writes: first the batched `write_long` (alarm #1 below), then
  the columnar-bind write path (one list parameter per bulk column,
  `prepare_cached` statements; section below).
- Value-float normalization on read (`canon::read_value`, about 0.25 ns
  per float).
- The dev profile now compiles the bundled DuckDB at opt-level 3. This
  does not affect these figures (the `bench` profile was already
  optimized).

## Synthetic DB (ddl_sketch §10 scale, built THROUGH `api::write_parameter`/`ingest_segments`)

1 226 400 live long rows @ 17.6% dead (140 entities × 8 760 int cells; 30
entity-rewrites of churn), 1 000 × 8 760-cell float arrays (~70 MB, Tier A),
100 000 single-parameter scalars. On-disk: **213 MB** after CHECKPOINT on
1.5.5; **165 MiB** on 2.0.0 (native format, current `main`).

**Build wall time — this IS the bulk-write benchmark number:**

| Stage | Wall (1.5.5) | Per unit (1.5.5) | 2.0.0 (wall, per unit) |
|---|---|---|---|
| 100 k scalars (10 commits × 10 k entities) | 1 125.2 s | **11.3 ms/entity** (7.6 ms/entity on the first 10 k, 12.8 ms/entity by the last — grows with entity-table size) | 619.3 s, **6.2 ms/entity** (5.0 ms/entity at best, 7.5 ms/entity on the last 10 k; grows with entity-table size). **Set-based scalar write (2026-10-24): 2.1 s, 0.021 ms/entity** |
| 1 000 × 8 760 float arrays (10 commits × 100) | 82.0 s | **82 ms/entity** | 14.4 s, **14.4 ms/entity** |
| 140 × 8 760 int long rows (1 commit/entity) | 4 369.8 s | **31.2 s/entity ≈ 3.6 ms/cell** | 39.6 s, **0.28 s/entity ≈ 32 µs/cell** |
| churn: 30 long-entity rewrites | 1 006.7 s | 33.6 s/rewrite | 13.0 s, 0.43 s/rewrite |
| **Total** | **6 586.5 s (109.8 min)** | | **687.4 s (11.5 min)**; scalars are now 90 % of it. **Set-based scalar write: 64.2 s** (scalars 2.1, arrays 10.2, long 37.7, churn 13.1) |

Context: fresh read-only connection open + `SELECT 1` on the built 213 MB
file: **534.5 ms** (median of 5) on 1.5.5; **203.3 ms** on the 165 MiB
2.0.0 file. The §10 methodology (and the read benches
below) hold a warm connection; the per-operation open-read-close discipline
(CLAUDE.md invariant 1) pays this per operation — worth its own look before
freeze.

## Read benches vs VMP Part 6 budgets (warm RO connection, end-to-end to Arrow)

| Read (budget) | §10 baseline (1.4.4, ref machine) | Measured (1.5.5, i3-6100) | Status (1.5.5) | 2.0.0 (Ryzen) | Status (2.0.0) |
|---|---|---|---|---|---|
| Point scalar, triple (< 5 ms) | 1.2 ms | **0.98 ms** | PASS | **0.97 ms** | PASS |
| Point long cell, triple (< 5 ms) | 2.2 ms | **0.98 ms** | PASS | **1.24 ms** | PASS |
| Point array, triple, full payload (< 5 ms) | 27–31 ms | **77.3 ms** | RECORDED-OVER — the known residual; see below | **50.6 ms** | RECORDED-OVER as the direct-table form; the budget is met through the macro (next rows) |
| Point array, `v_min`/`v_max` only (mitigation context) | 0.6–0.8 ms | **0.56 ms** | (context) | **0.70 ms** | (context) |
| Point array via `read_parameter_dense` (`array_point_dense_macro`, added with the `array_id` key, schema 0.4) | — | — (bench added after the 1.5.5 run) | new | **2.52 ms** | **PASS** (< 5 ms; the API path for a point array) |
| Point array by `array_id = ?` (`array_point_by_array_id`, the index-lookup floor) | — | — (bench added after the 1.5.5 run) | new | **0.61 ms** | (floor) |
| 10⁵ scalars, `read_parameter_scalar` (< 20 ms) | 24.5 ms | **30.0 ms** | RECORDED-OVER (raw-scan parity residual, §10; scale ≈ machine ratio) | **22.5 ms** | **OVER** (+12 %; the same raw-scan parity residual, see below) |
| 1 000 × 8 760 (~70 MB), `read_parameter_dense` (< 300 ms warm) | 219 ms | **311.5 ms** | RECORDED-OVER (borderline; 219 ms on the ref machine — treat as machine-scaled PASS, re-measure on NVMe-class) | **240.1 ms** | **PASS** |
| Same, canonical `read_parameter` (recorded; dense form is the budget path per VMP P6) | 4 240 ms | **7 351 ms** | recorded | **3 151 ms** | recorded |
| Model-load context: 1.23 M long rows canonical (< 3 s) | 587 ms | **2 063 ms** | PASS | **1 065 ms** | PASS |

**Point-array residual on 1.5.5 — re-measured, NOT improved.** 77 ms vs the
< 5 ms budget (was 27–31 ms on 1.4.4 on a faster machine). The cost is still
pure list-payload row-group decompression; `v_min`-only reads are 0.56 ms, so
no point-query shape avoids decompressing the containing `values` segment on
1.5.5 either. The §10 escalation stands: physical row-group tuning at build
time, client-side caching of hot arrays, or a VMP budget amendment — queued
for re-test again at the next DuckDB upgrade.

**Schema 0.4 (`array_id` surrogate key) changes the picture for the read
macros.** `array_triple_payload` stays as the documented slow form (a filter
on the key triple directly on the payload scan is still a sequential scan).
The two new benches read the same array through `read_parameter_dense`
(narrow key resolution, then the payload fetched by `array_id` through the
ART index) and directly by `array_id`. On 2.0.0 (Ryzen) the dense macro
reads one point array in **2.52 ms**, inside the < 5 ms budget, and the
bare `array_id` lookup in 0.61 ms; the direct triple read still takes
50.6 ms. The point-array budget is therefore met through the API path
(`read_parameter_dense`), not through an arbitrary triple filter on
`value_array`. The results-path fixture agrees (10 000 arrays: 2.35 ms
warm min, `scripts/bench/RESULTS_results_path.md` §6).

**10⁵-scalar budget on 2.0.0: over by 12 % (22.5 ms vs 20 ms).** Not
diagnosed further in this run. It is the residual `ddl_sketch.md` §10
already recorded: there the raw current-state scan alone took 20.1 ms
against the 20 ms budget and `read_parameter_scalar` added ~4 ms of name
joins (24.5 ms), and the 1.5.5 run here was over too (30.0 ms). The cost
is the DuckDB → Arrow materialization of 100 000 rows × 10 columns plus
the two name joins, not something the 2.0 upgrade or the write-path
changes touched (the macro is unchanged since 1.5.5). Closing it needs a
narrower export (fewer columns) or a budget amendment, both owner
decisions.

## From/to scan degradation vs dead fraction (raw current-rows long scan, Arrow end-to-end)

| Dead fraction | Scan (1.5.5) | vs 0% dead (1.5.5) | Scan (2.0.0) | vs 17.6% (2.0.0) |
|---|---|---|---|---|
| 0% (pre-churn, builder-measured, median of 5) | 16.1 ms | 1.00× | 27.3 ms | (not comparable, see below) |
| 17.6% | 22.8 ms | **1.41×** | 14.6 ms | 1.00× |
| 33.3% | 22.7 ms | **1.41×** | 14.3 ms | **0.98×** |

On 2.0.0 the builder's 0 %-dead figure (a median of 5 on a connection
opened right after the post-build CHECKPOINT, while the build had just
finished) is slower than both criterion figures (warm, 10 samples), so the
ratio against it says nothing about dead rows. The comparable pair is
17.6 % → 33.3 %: flat (0.98×), as on 1.5.5.

Better than the research baseline (1.6× @ 20%, 2.5× @ 56%): degradation is
present but flat between 17.6% and 33% — churn-generated dead rows cluster
into whole row groups after CHECKPOINT, and `valid_to_commit IS NULL`
zonemaps skip fully-dead groups, exactly the VMP Part 6 prediction. PASS
(recorded).

## Write benches (per cycle, each = one `write_parameter` commit + CHECKPOINT)

| Bench | Measured (1.5.5) | Baseline / note | 2.0.0 (Ryzen) |
|---|---|---|---|
| Scalar replace cycle | **1.24 s** | dominated by CHECKPOINT + RW open/close on the 213 MB file, not the row write | **499 ms**; 499 ms again with the set-based scalar write (the floor, not the row write) |
| Array replace cycle (8 760 cells, `_hist` pre-image copy) | **2.43 s** | research: 4.3 s/cycle for pre-image copy at larger scope | **967 ms** |
| Long replace cycle (8 760 cells close-out + re-insert) | **38.1 s** | research churn baseline: 0.9 s per 10-cycle single-table churn — see the alarm below | **803 ms** |
| 100 new scalar entities, one commit (no checkpoint) | **2.38 s → 23.8 ms/entity** | at the 100 k-entity table size; from-empty build average is 11.3 ms/entity | **1.128 s → 11.3 ms/entity** (2.0 build average 6.2 ms/entity). **Set-based scalar write: 380 ms → 3.8 ms/entity**, mostly the commit's open/close on the 165 MiB file |

The one-current generated-column unique indexes (`one_current_long` at
7-column width, `one_current_scalar`) are maintained by every write above —
their cost is inside these numbers, not separately measurable here.

On 2.0.0 the three replace cycles are now all under 1 s, so the cycle is
dominated by the fixed cost (CHECKPOINT and the RW open/close on the
165 MiB file), not the row writes: the long replace that took 38 s on
1.5.5 is 0.8 s. The array cycle (967 ms) is the slowest of the three;
its pre-image copy into `value_array_hist` was the composite-key
sequential scan on 1.5.5 (`RESULTS_results_path.md` §5) and was not
re-profiled here.

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

   **2.0.0 column (current `main`, columnar-bind path, Ryzen host, run
   conditions above; `long_write` ran 00:06, load average 2.8-3.2):**

   | 8 760-cell op | criterion estimate [95 % CI] | per cell |
   |---|---|---|
   | first-write (`long_insert_8760`) | **272 ms** [222, 299] | 31 µs |
   | replace cycle (`long_replace_8760`) | **499 ms** [474, 518] | 57 µs |

   Different machine from the before/after rows above, so no ratio is
   given. The insert figure is slower than the A/B harness's 143 ms for
   the same shape (Columnar-bind section below) and has a wide interval:
   this bench keeps writing fresh entities into one file across all
   iterations, so later iterations insert into a larger table.
2. **Per-entity write cost did not melt away in release.** The ~44 ms/entity
   DEBUG fact (F4 scale test, array tier) measures **82 ms/entity in release
   end-to-end** for the same 1000 × 8760 shape — the dominant cost is the
   8 760-placeholder `list_value(...)` INSERT parse/bind per entity plus
   per-entity resolve probes, all DuckDB-side statement work. Scalars:
   11–24 ms/entity (grows with entity-table size; per-entity `resolve_entity`
   name probes are un-indexed scans). Same follow-up as (1): batching.

   **2.0.0 (columnar-bind path):** arrays 14.4 ms/entity end to end in the
   gate build; scalars 5.0-7.5 ms/entity, still growing with the entity
   table, and now 90 % of the gate's build time. The remaining scalar cost
   is statement count, not insert mechanics (Columnar-bind section below).
3. **CHECKPOINT + open/close ≈ 1.2 s on a 213 MB file** puts a floor under
   every open-write-close commit cycle; and a fresh RO open costs ~535 ms.
   Fine for batch tools, material for interactive editors — worth measuring
   on NVMe-class hardware before drawing conclusions.

   **2.0.0 on NVMe (Ryzen host):** the scalar replace cycle, which is
   mostly this floor, is 0.50 s on the 165 MiB file, and a fresh RO open
   + `SELECT 1` is 203 ms. Smaller, but still material for an editor that
   opens per operation (CLAUDE.md invariant 1).

## Columnar-bind write path (2026-10-23, DuckDB 2.0.0)

`history` now binds each bulk column as ONE list parameter instead of one
placeholder per cell (`specs/appender_write_path_PLAN.md`): the long
insert is `INSERT … SELECT ?, unnest(?::T[]), …` per ≤ 16 384 cells, the
array INSERT / replace UPDATE bind one `?::DOUBLE[]`, axis child rows
are one statement per kind, and every hot-path write statement is
prepared once per transaction (`prepare_cached`, cache capacity 64;
scalars skip the two `axis_count` probes; the rare definition-default
and string-rewrite helpers, the axis dedup lookups and the axis parent
`INSERT … RETURNING` still use plain prepare). Stored rows are byte-identical
(`tests/write_path_golden.rs`, generated by the old path). This
supersedes the "batched multi-row INSERT" mechanism of alarm #1 and is
the "batching" follow-up of alarm #2.

Measured separately from the gate's 2.0 column:
paired, interleaved A/B, old (`40e16a5`, the base before the columnar
write path) and new core
each built in release against the bundled DuckDB 2.0.0, harness
`scripts/bench/ab_write_path/` (its `long_insert` / `long_replace`
modes are the `long_write` bench's two shapes; `api_100_scalars` is
`write_100_new_scalar_entities` on a fresh file). 21 pairs per row,
median per-pair ratio new/old, bootstrap 95 % CI; shared AMD Ryzen 9
5900XT, load average 2-37 (rows whose load moved > 2× were re-run at
load 2-7 and kept their ratio within 0.01). Per value, commit included:

| Row | old median | new median | ratio [95 % CI] |
|---|---|---|---|
| `long_insert` (8 760 cells) | 1 781 ms (203 µs/cell) | 143 ms (16 µs/cell) | 0.080 [0.080, 0.081] |
| `long_replace` (8 760 cells) | 1 851 ms | 239 ms | 0.127 [0.124, 0.129] |
| array insert (8 760 values, 20 per commit) | 111 ms | 8.0 ms | 0.073 [0.072, 0.074] |
| array replace (20 per commit) | 120 ms | 16.1 ms | 0.134 [0.133, 0.137] |
| long value on a new irregular 8 760-point datetime axis | 5 913 ms | 162 ms | 0.027 [0.027, 0.028] |
| scalar insert (400 per commit) | 7.0 ms | 2.9 ms | 0.42 [0.40, 0.45] |
| scalar replace (400 per commit) | 5.8 ms | 2.8 ms | 0.48 [0.47, 0.49] |
| `write_100_new_scalar_entities` shape, per entity | 9.0 ms | 5.5 ms | 0.62 [0.60, 0.65] |
| read: current cells of one 876-cell long value | 0.61 ms | 0.60 ms | 1.01 [0.97, 1.03] (unchanged) |

The remaining per-value cost is statement count, not insert mechanics:
a scalar still issues ~5 statements at the ~0.3-0.8 ms DuckDB 2.0 floor,
and a long replace closes out and versions key by key. The set-based
per-segment write (plan D6(c), D10) is the follow-up. Scalars: done, see
the next section; the long tier (D10) remains.

## Set-based scalar write (2026-10-24, DuckDB 2.0.0)

`history::write_scalars` writes all scalars of a commit in rounds of
<= 16 384 keys (one live + version probe, a long/array live probe for
keys not live as scalars, one close-out per retention mode, one columnar
insert; point-filter forms for a one-key round), and each segment's
entity names resolve in one statement, reused across the commit's
segments. Paired A/B (`scripts/bench/ab_write_path`, old = the per-key
writer, 21 pairs per row, 11 for `api_10k_scalars`; load 2.7-10.8, the
`api_20x1` row re-run at 4.6-8.0 after its load moved > 2x). Whole
commits; the 10^5-scalar file was built once (2.9 s) and copied per
sample.

| Row | old median | new median | ratio [95 % CI] | bound |
|---|---|---|---|---|
| `api_10k_scalars` (10 000 new, one commit, per value) | 4.98 ms | 0.020 ms | 0.004 [0.004, 0.004] | <= 0.05 |
| `api_100_scalars_100k` (100 new on a 10^5 file, per value) | 8.75 ms | 0.99 ms | 0.113 [0.109, 0.115] | <= 0.30 |
| `api_100_scalars` (fresh file, per value) | 5.01 ms | 0.72 ms | 0.146 [0.143, 0.148] | |
| `api_20x1_scalars_100k` (20 segments x 1 value, per commit) | 349 ms | 191 ms | 0.56 [0.54, 0.57] | <= 0.80 |
| `api_1_scalar_100k` (single-value save, per commit) | 94.2 ms | 95.3 ms | 1.00 [0.99, 1.02] | <= 1.15 |
| `scalar_insert` (`write_value`, 400 per commit) | 2.74 ms | 2.60 ms | 0.94 [0.91, 0.96] | <= 1.25 |
| `scalar_replace` (`write_value`, 400 per commit) | 2.81 ms | 2.51 ms | 0.89 [0.89, 0.90] | <= 1.25 |
| `long_insert` / `long_replace` | 148 / 237 ms | 149 / 238 ms | 1.01 [0.99, 1.04] / 1.01 [1.00, 1.03] | CI in [0.95, 1.05] |
| `array_insert` / `array_replace` | 8.5 / 16.5 ms | 8.4 / 16.6 ms | 0.99 [0.96, 1.03] / 1.01 [0.99, 1.02] | CI in [0.95, 1.05] |
| `axis_insert` / `read_long` | 169 / 0.57 ms | 169 / 0.57 ms | 1.00 [0.99, 1.02] / 1.00 [0.99, 1.02] | CI in [0.95, 1.05] |

Gate re-run on the branch (same host, `cargo bench -p nodal-db-core
--bench gate`): synthetic build 687.4 s -> 64.2 s (scalars 619.3 s ->
2.1 s); `write_100_new_scalar_entities` 1.128 s -> 380 ms;
`replace_scalar_cycle_ckpt` 499 ms (unchanged: the CHECKPOINT and
open/close floor); every read row within noise of the 2.0.0 column.
The single-value save stays at the per-commit floor (RW open ~30-40 ms,
close ~20 ms) that the set-based write does not touch.

## Value-float normalization (2026-10-23, `benches/canon.rs`)

Storing value floats canonical (`-0.0 → +0.0`, every NaN → `0x7ff8…`) adds one
compare-and-select per float cell in `canon::read_value`. Measured with the
pure pipeline bench (no DB), the baseline binary built from the commit before
the change (`--save-baseline pre_norm`) and the new one compared against it,
release, on a shared AMD Ryzen 9 5900XT (16C/32T) that other jobs were loading
(load average 6–40 during the runs).

| shape | pre | post | criterion change (95 % CI) |
|---|---|---|---|
| (a) rank-1 8 760-cell full grid (array) | 734 µs | 743 µs | +0.9 % [+0.1, +1.7] |
| (c) rank-2 8 760-cell sparse (long) | 1.759 ms | 1.773 ms | +2.0 % [+1.2, +3.0] |
| (b) rank-2 1 M-cell full grid, finite | 204 ms | 192 ms | −5.9 % [−7.5, −4.3] |
| (b) same, ~1 % `-0.0`/NaN | 204 ms | 190 ms | −6.9 % [−8.8, −5.1] |

That is the quietest of five alternating rounds; the others swung by −44 %
to +250 % in both directions with the host load, so end-to-end deltas here
are host noise, not the change. The tiebreaker measures the added work
directly: `canonical_value_f64` over 1 M floats takes **0.24–0.27 ns per
element**, under the ~1 ns/cell acceptance bound (against ~80–200 ns per cell
for the pipeline itself). No `gate` rerun: the change is confined to the step
this bench isolates.

`cargo bench -p nodal-db-core --bench canon` runs it; `test = false` keeps it
out of `cargo test --workspace`.

## History-free extract (2026-10-23, `benches/extract.rs`)

Source built through the core write path: 100 entities × 10 versions of an
8 760-cell float array (one commit per version; 90 % of the array tier is
superseded history) plus 10 long-tier int series × 336 cells × 10 versions
(one commit per entity-version). Three files measured: the source as
written, a compacted copy (`compaction::compact`), and the head extract.
Release build, DuckDB 2.0.0, on the shared AMD Ryzen 9 5900XT (16C/32T)
with other jobs loading it (load average about 17–67 during the session),
so the millisecond timings are indicative only. Medians of 15; open + cookie
uses a fresh read-only connection per sample.

| file | size | RO open + cookie | current long scan | array point read |
|---|---|---|---|---|
| source | 73.5 MiB | 155.4 ms | 1.4 ms | 2.7 ms |
| compacted source | 43.3 MiB | 45.4 ms | 1.4 ms | 2.9 ms |
| head extract | 16.8 MiB | 44.7 ms | 2.3 ms | 2.9 ms |

Compaction 2.6 s, extract 1.6 s; 31 141 rows left out, no axis dropped
(every version reused the same axes).

Reading, honestly scoped:

- **Size is the benefit.** Against the raw source the extract is 4.4×
  smaller, but compaction alone already gets 1.7× of that (free blocks of
  old checkpoints). Against the **compacted** source the extract's own gain
  is **2.6×** at a 90 % dead array tier. That is well below the 10× the
  dead fraction alone would suggest: a fixed per-file overhead (an empty
  schema file is about 2 MiB, and every table with rows claims its own
  blocks) plus the live payload stay.
- **No read-speed claim.** The array point read is unchanged (current array
  reads never touch `value_array_hist`), and RO open + cookie matches the
  compacted file (the raw source's 155 ms is the uncompacted file, not
  history). The long-tier scan here is tiny (3 360 live rows) and within
  noise; the larger long-tier scan gain seen in the plan's toy spike is not
  reproduced by this bench and is not claimed.

`cargo bench -p nodal-db-core --bench extract` runs it (a few minutes;
`-- --quick` for a smaller source); `test = false` keeps it out of
`cargo test --workspace`.

## Not covered here (unchanged scope)

- Scenario-resolved slice (unbudgeted; in the plan's bench list, not in this
  gate's first cut — add when `maintain`/compaction lands).
- Compaction wall time at gate scale (the extract bench above times it on
  its own 73 MiB source only).
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
