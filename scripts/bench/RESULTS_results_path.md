# Results-scale benchmark: recorded results

Bulk result ingest and read paths at solver-output scale (10 parameters ×
1 000 entities × 8 760 hourly floats = 87.6 million values), measured
through the nodal-db core, plus two raw-DuckDB companions that separate
storage-format effects from nodal-db's own schema. This is the "Set B"
benchmark of the design rationale, now reproducible from the repository.
It is a recording benchmark: nothing here gates a build.

| Harness | What it measures | Run |
|---|---|---|
| `crates/nodal-db-cli/benches/results_path.rs` | Ingest (core API loop and Arrow-IPC import), file size, bind cost of the array INSERT, read paths, the `value_array_cells` A/B gate (`cellsgate`), row-group steel-man, long-tier reads. DuckDB as bundled by the core. | `just bench-results` (full: ~30-40 min on 1.5.5, 21 min on current `main`, §6) or `just bench-results --quick` (~1 min) |
| `scripts/bench/raw_duckdb_layout.py` | Dense `DOUBLE[]` lists against a lean sorted `(k, pos, v)` long table in plain DuckDB, no nodal-db schema. Any DuckDB version. | runs after the Rust bench in `just bench-results`; alone: `just bench-raw-layout <python> [--quick]` |
| `scripts/bench/long_tier_size_breakdown.py` | Where the long tier's bytes go: `value_long` built from the real schema, then without its one-current unique index, its triple index, its foreign keys and its `i1_dt` column, against a lean table (§7.1). Optional cross-check on a core-written file. | `just bench-long-size [--core-db <file>]` (~15 s) |
| `scripts/bench/surrogate_key_variant.py` | Superseded by schema 0.4 (§4b); needs a 0.3-schema input file. The real `value_array` (composite primary key) against a copy with a single-column `array_id` key; the long tier's equivalent; commit cost after an array replace. Any DuckDB version. | `just bench-sk-variant <python> --db <file> [--long-db <file>] [--commit-probe] [--commit-probe-native]` (input files: see the script's docstring) |

All data is deterministic (splitmix64, fixed seed); the raw-DuckDB script
generates values bit-identical to the Rust bench's dense shape (verified
against a written database). File sizes reproduce byte for byte. A bare
`cargo bench` (the benchmark gate) does not start the results-scale run:
with no phase argument the bench prints its usage and exits.

## Environment

| | |
|---|---|
| Date | 2026-10-23 |
| CPU | AMD Ryzen 9 5900XT, 16 cores / 32 threads |
| RAM | 70 GB |
| Disk | WD_BLACK SN850X NVMe, ext4; databases in `/tmp` (same device) |
| OS / Rust | Linux 6.14.0-37-generic; rustc 1.98.0, `bench` profile |
| DuckDB (core) | 1.5.5 (duckdb-rs 1.10505.0, bundled) for §1-§2; 2.0.0 (duckdb-rs 1.20000.0, bundled) for §4b, §4c and §6 |
| DuckDB (Python) | 1.5.5 (project `.venv`), 2.0.0 (throwaway venv); Python 3.12.3, numpy 2.5, pyarrow 25 / 26 |

Section 6 is the full re-run on current `main` (DuckDB 2.0.0) and has its
own run conditions; it is the current set of figures. Section 7 adds the
long-tier size breakdown and a one-year-of-minutes array, measured later
the same day. Sections 1-5 are kept
as recorded.

**Load caveat (sections 1-5).** The machine was shared with other workloads during these
runs (load average 7 to 45 on 32 threads). Ingest walls are the noisiest
numbers; where a phase ran twice the better run is quoted and the spread
is given. Read timings are min / median of 15 warm or 5 cold repetitions;
the minimum is the more reliable figure under load. Ratios within one
table are the finding, absolutes less so.

Units: MiB = 2^20 bytes. "Net B/value" subtracts the size of an empty,
checkpointed database (3 158 016 B for the nodal-db schema, 274 432 B for a
one-table raw DuckDB file).

## 1. Ingest through the core (DuckDB 1.5.5)

Lane (a) is `write_parameter` in a loop, 100 entities per call (one
commit each). Lane (b) is one Arrow-IPC container ingested by
`nodal-db import` (one process, one commit). Sizes are after CHECKPOINT.

| Shape / lane | Wall | values/s | File | Gross / net B/value |
|---|---|---|---|---|
| dense 1 000 series, API loop (4 runs: 37.7 / 77.1* / 44.4 / 38.2 s) | 37.7 s | 232 k | 127.3 MiB | 15.2 / 14.9 |
| dense 1 000 series, IPC import (4 runs: 105.7* / 35.4 / 39.8 / 35.0 s) | 35.0 s | 250 k | 71.5 MiB | 8.56 / 8.20 |
| dense 10 000 series, API loop (1 run) | 659.8 s | 133 k | 1 591.8 MiB | 19.1 / 19.0 |
| dense 10 000 series, IPC import (1 run) | 497.8 s | 176 k | 804.8 MiB | 9.63 / 9.60 |
| long, 1 000 series × 876 integer cells, API loop (2 runs: 217.7 / 181.7 s) | 181.7 s | 4.8 k | 93.0-93.3 MiB | 111 / 108 |
| long, same, IPC import (2 runs: 222.0 / 215.0 s) | 215.0 s | 4.1 k | 79.0 MiB | 94.6 / 91.0 |

\* runs that overlapped heavy external load.

- The 10k API loop's per-commit time grows from 3.0 s (first chunk) to
  10.1 s (last), median 6.0 s. The one-commit lane does not degrade.
- Many-commit files are about twice the size of one-commit files for the
  same data (127 vs 71.5 MiB; 1 592 vs 805 MiB).
- One-commit dense storage costs 8.2 B/value at 1 000 series but 9.6 B/value
  at 10 000 series. The raw-DuckDB table below shows the same effect from
  row-group size, so it is a DuckDB list-storage effect, not nodal-db.
- The long tier ingests about 50 times slower per value than the array
  tier (4.1-4.8 k against 250 k values/s).
- IPC container build (producer side): 135.8 MiB in 0.1-0.8 s (1k),
  1 357.8 MiB in 2.7 s (10k), 21.2 MiB in < 0.1 s (long).

### Statement-building share of an array write

At the time of this section (1.5.5) the core wrote each array with one
INSERT carrying one SQL parameter per cell (`list_value(?, …)`); it now
binds the list as one parameter (§4c). Measured in isolation (in-memory
table):

| | min | median |
|---|---|---|
| prepare (parse) of the 8 761-placeholder INSERT | 13.2-13.3 ms | 13.5-13.7 ms |
| execute (bind 8 761 parameters + insert) | 9.0-9.3 ms | 9.2-9.8 ms |

About 23 ms of statement mechanics per 8 760-value series, against
35-38 ms per series end to end (best 1k runs): **61 % (API loop) and
67 % (IPC import) of the per-series write cost**.

## 2. Reads through the core (DuckDB 1.5.5)

On the 10 000-series files. All 10 000 `value_array` rows sit in one row
group; the `values` column spans 3 169 blocks. Cold = fresh read-only
connection per repetition, connection open excluded and listed
separately.

| Read | Warm min / median | Cold min / median |
|---|---|---|
| RO connection open, 805 MiB one-commit file | — | 21.1 / 21.7 ms |
| RO connection open, 1 592 MiB many-commit file | — | 111.2 / 111.9 ms |
| one series, whole value (8 760), `read_parameter_dense` | 110.3 / 118.4 ms | 229.7 / 232.5 ms |
| one series, one week `values[4001:4168]` | 110.0 / 115.7 ms | 226.0 / 228.3 ms |
| one series, one cell `values[4001]` | 111.2 / 118.3 ms | 223.4 / 229.0 ms |
| one series, min/max only (`v_min`, `v_max`) | 0.80 / 0.92 ms | 1.81 / 2.18 ms |
| one week × 20 series, (a) whole lists + client slice | 112.6 / 117.8 ms | 234.0 / 236.1 ms |
| one week × 20 series, (b) SQL list slice | 111.7 / 117.5 ms | 224.2 / 232.0 ms |
| one week × 20 series, (c) unnest + filter on position | 111.0 / 116.9 ms | 224.9 / 230.7 ms |
| one week × 20 series, (d) `read_parameter` + timestamp filter | 235.9 / 245.9 ms | 360.3 / 363.5 ms |

The many-commit file gives the same read times within noise (whole value
110.0 / 114.8 ms warm; (d) 221.0 / 246.2 ms); only the connection open
differs (5×).

**Row-group steel-man.** `value_array` copied (direct table, no macro)
into scratch files:

| Copy | Row groups | File | One series whole / week / cell (warm min) | min/max only | 20 series week |
|---|---|---|---|---|---|
| default ROW_GROUP_SIZE | 1 | 792.5 MiB | 41.3 / 40.5 / 40.8 ms | 0.19 ms | 41.8 ms |
| ROW_GROUP_SIZE 2048 | 5 | 594.8 MiB | 95.7 / 96.8 / 97.4 ms | 0.20 ms | 98.3 ms |

Smaller row groups shrink the file 25 % but do not make point reads
cheaper. (A first run of this phase under heavy external load measured
both copies at 190-400 ms; the rerun above is the quieter one.)

**Long tier** (`value_long`, 876 000 live integer cells, 8 row groups):

| Read | Warm min / median | Cold min / median |
|---|---|---|
| RO connection open (79 MiB) | — | 27.2 / 29.2 ms |
| one cell | 0.37 / 0.44 ms | 1.29 / 1.37 ms |
| one series, week window (17 points) | 0.38 / 0.41 ms | 0.98 / 1.41 ms |
| one series, whole (876 points) | 0.34 / 0.39 ms | 1.18 / 1.67 ms |
| one series, min/max aggregate | 0.41 / 0.63 ms | 1.57 / 1.78 ms |
| 20 series, week window | 1.63 / 1.92 ms | 3.52 / 3.94 ms |
| `read_parameter`, 20 series, timestamp window | 26.0 / 28.1 ms | 36.1 / 39.2 ms |

## 3. Raw DuckDB: dense lists against a lean long table

No nodal-db schema: `dense(k INTEGER, vmin, vmax, v DOUBLE[])` and
`long(k INTEGER, pos INTEGER, v DOUBLE)`, both sorted by key, no index,
the same 87.6 million values as the 10k files above. `long_int` is the
long-tier shape of section 1 (1 000 series × 876 integers).

### Sizes (identical on 1.5.5 and 2.0.0 within 0.1 %)

| Table | File | B/value (net) | Row groups |
|---|---|---|---|
| dense, default ROW_GROUP_SIZE | 832.8 MiB | 9.97 | 2 |
| dense, ROW_GROUP_SIZE 2048 | 595.3 MiB | 7.12 | 5 |
| long (k, pos, v DOUBLE) | 650.5 MiB (650.0 on 2.0.0) | 7.78 | 720 |
| long_int, sparse integer shape | 3.8 MiB | 4.19 | 8 |

- A sorted narrow long table is **no larger than the dense lists** for
  hard-to-compress floats (7.8 against 7.1-10.0 B/value): sorted keys and
  positions compress to almost nothing.
- nodal-db's long tier stores the same integer shape at 91 B/cell (one
  commit) against 4.2 B/cell raw: the **long-tier overhead is ~20×**, and
  it comes from the schema (history and version columns, generated key
  columns, the one-current unique index and the triple index), not from
  the long format.
- nodal-db's one-commit dense file (9.6 B/value) is within 4 % of the raw
  default-row-group dense table: the array tier adds little on top of the
  format.

### Reads (warm min / median, ms; the script also prints cold timings)

| Read | dense default 1.5.5 | dense rg 2048 1.5.5 | long 1.5.5 | dense default 2.0.0 | dense rg 2048 2.0.0 | long 2.0.0 |
|---|---|---|---|---|---|---|
| RO open | 7.1 / 7.4 | 8.6 / 9.8 | 8.3 / 10.2 | 16.3 / 17.6 | 16.8 / 17.2 | 17.0 / 19.9 |
| one cell | 105.6 / 107.8 | 114.3 / 119.2 | 0.77 / 0.86 | 96.0 / 101.8 | 93.0 / 95.8 | 0.85 / 1.01 |
| one week (168) | 104.4 / 116.6 | 109.9 / 114.2 | 0.80 / 0.90 | 94.7 / 101.4 | 93.8 / 98.5 | 0.89 / 1.01 |
| one whole series (8 760) | 100.6 / 106.5 | 103.2 / 107.4 | 0.73 / 0.79 | 98.0 / 100.5 | 94.1 / 99.1 | 0.76 / 0.81 |
| min/max only | 0.37 / 0.50 | 0.41 / 0.51 | 1.14 / 1.20 (aggregate) | 0.46 / 0.52 | 0.44 / 0.48 | 1.33 / 1.46 (aggregate) |
| one week × 20 series | 344 / 361 | 105.0 / 110.1 | 1.47 / 1.73 | 328 / 376 | 95.8 / 103.5 | 1.86 / 2.07 |
| full-table aggregate | 740 / 884 | 785 / 808 | 22.9 / 23.5 | 681 / 700 | 755 / 764 | 22.7 / 23.0 |

- Point, slice and whole-series reads of a list row cost ~100 ms whatever
  is requested, on both engines; a sorted long table answers the same
  reads in under 1 ms (zone maps on the sorted key skip almost all row
  groups). DuckDB 2.0.0 does not change this.
- A full scan is ~35× faster on the long table (23 ms against 700-880 ms).
- The per-row min/max columns are the one cheap dense read (0.4-0.5 ms).

## 4. Surrogate key on the real schema (1.5.5 and 2.0.0)

> The committed schema adopted this key in 0.4; §4b measures it through
> the real macros. This section is the earlier probe on 0.3-schema files.

Hypothesis: slow dense point reads come from `value_array`'s composite
primary key `(entity_id, parameter_definition_id, alternative_id)`.
DuckDB uses an index scan only for a single-column key, so a point read
falls back to a sequential scan that decodes the list data around the
row. Test on the 805 MiB one-commit 10k file: the real file and macro;
a copy with the same composite key (control); and a copy with
`array_id BIGINT PRIMARY KEY` plus `UNIQUE (entity, parameter,
alternative)`, read through a variant of `read_parameter_dense` that
resolves keys on narrow columns and then joins to the payload through
`array_id`. The scan type comes from `EXPLAIN ANALYZE`.

Warm min / median, ms:

| Read | real file, macro | composite-key copy | surrogate key, macro variant | surrogate key, lookup + self-join |
|---|---|---|---|---|
| **1.5.5** one cell | 114.2 / 120.7 | 114.7 / 120.4 | 1.88 / 2.20 | 0.87 / 1.04 |
| one week (168) | 116.3 / 123.0 | 114.8 / 122.6 | 1.74 / 1.99 | 0.88 / 1.19 |
| one whole series | 115.3 / 122.4 | 115.6 / 122.0 | 1.75 / 1.96 | 0.77 / 1.02 |
| min/max only | 1.10 / 1.30 | 1.49 / 1.70 | 1.60 / 1.86 | 0.74 / 0.94 |
| one week × 20 series | 115.8 / 123.7 | 140.3 / 155.9 | 3.26 / 3.69 | 2.29 / 2.44 |
| scan type | sequential | sequential | index + sequential | index + sequential |
| **2.0.0** one cell | 124.2 / 139.4 | 136.9 / 155.1 | 2.22 / 2.85 | 1.18 / 1.74 |
| one week (168) | 140.3 / 192.1 | 99.9 / 109.2 | 2.39 / 3.02 | 1.35 / 1.83 |
| one whole series | 129.8 / 139.9 | 92.9 / 109.8 | 2.26 / 3.00 | 1.10 / 1.38 |
| min/max only | 1.93 / 2.27 | 1.63 / 1.85 | 2.01 / 2.67 | 1.16 / 1.66 |
| one week × 20 series | 132.4 / 144.0 | 94.4 / 100.5 | 4.82 / 7.63 | 3.44 / 6.10 |

Cold (fresh connection, open excluded), 1.5.5: real file 231-238 ms
min; surrogate variant 5-7 ms for one series, 13-15 ms for 20 series.

**Confirmed on the real schema, both engines:** the surrogate key turns
point, slice and whole-series reads from ~115 ms into ~1-2 ms (50-100×),
and the 20-series week into ~2.5-3.5 ms. The composite-key copy behaves
like the real file, so the gain is the key shape, not the copy. The
canonical `read_parameter` path was not re-plumbed in this test (its
`value_rows` view would need the same lookup-then-join rewrite).

**Long tier: no analogous effect.** `value_long` reads use sequential
scans despite the composite unique index, yet already take 0.4-0.6 ms
(1.5.5) and 0.7-1.1 ms (2.0.0): rows are written series by series, so the
sorted key's zone maps skip almost everything. A copy with a single-column
`series_id` and an index on it changes nothing material (cell 0.39 ms,
window 0.42 ms, whole series 0.77 ms, the last via index scan).

## 4b. The `array_id` key on the committed schema (2.0.0)

Schema 0.4 adopts §4's variant: `value_array.array_id BIGINT PRIMARY KEY`
(sequence stride 2) plus `UNIQUE` on the key triple; `read_parameter_dense`
and `value_array_cells` (so `value_cells`) resolve `array_id` on narrow
columns and fetch the payload through a self-join on it. Measured with
`results_path.rs` (`dense10k-ipc reads cellsgate`) on 2026-10-23: the AMD
Ryzen 9 5900XT host, shared, load average about 2-8 during the runs;
bundled DuckDB 2.0.0 (`v2.0.0-dev88614`, source d54cbbe3df). "Before" is
`main` at 4eea433 (composite key) built in its own target directory; same
data, same harness, one commit, IPC lane. Warm min / median, ms (cold min
in brackets, open excluded):

| Read | before (4eea433) | after (`array_id`) |
|---|---|---|
| one whole series, dense macro | 84.78 / 86.46 [182.05] | 2.36 / 2.55 [6.08] |
| week slice `values[4001:4168]` | 84.98 / 86.32 [133.66] | 2.35 / 3.02 [6.34] |
| single cell `values[4001]` | 84.85 / 86.42 [184.56] | 2.46 / 2.65 [6.16] |
| min/max only | 1.96 / 2.06 [3.37] | 2.33 / 2.52 [4.07] |
| week × 20 series, (a) dense + client slice | 86.53 / 90.20 [185.86] | 3.60 / 4.15 [11.73] |
| week × 20 series, (b) SQL list slice | 85.95 / 87.37 [139.35] | 3.91 / 4.19 [10.61] |
| week × 20 series, (c) unnest on `value_array` by triple | 86.02 / 87.93 | 85.20 / 86.96 (the direct table read; unchanged by design) |
| week × 20 series, (d) `read_parameter` + `idx1_ts` | 2 751 / 2 795 | 2 946 / 3 005 |
| 2 adjacent series, dense macro (new row) | — | 2.52 / 2.61 [6.54] |
| 51 series, dense macro (new row; past the 50-key limit) | — | 88.37 / 89.68 [194.57] |
| `read_parameter`, whole parameter (new row) | — | 4 041 / 4 108 [4 506] |
| IPC import, 87.6 M values | 1 200.4 s | 1 186.9 s, 1 218.6 s (two runs) |
| file size | 735 064 064 B | 735 326 208 B (+256 KiB) |

`EXPLAIN ANALYZE` on the after-file (Python duckdb 2.0.0): the dense macro
and `value_cells` point reads run an `Index Scan` on the payload side
(`Dynamic Filters: array_id = 23`, 1 row scanned); the direct triple read
and the 51-key read run a `Sequential Scan` over all 10 000 rows (~80 ms
of the scan time).

**`value_array_cells` gate** (`cellsgate`, paired method, threshold 1.10): the
whole-parameter `read_parameter` (8.76 M rows, `SELECT *`, Arrow), new
single-branch self-join cells view vs the old two-branch view installed as
TEMP views, one process, one file, threads = 8, 21 randomized-order pairs.
Three runs: the first two were flagged discarded (1-min load average drift
2.3× and 5.0×; the second because the bench's own 8 threads were counted —
since fixed: the start sample is taken after a 60 s warm-up). Valid run:
load 4.92 → 3.58 (1.37×); old median 7 579 ms, new 7 591 ms; per-pair
ratio median **1.041**, 95 % bootstrap CI **[1.003, 1.059]** → **PASS**
(≤ 1.10). The discarded runs gave 1.058 [1.048, 1.062] and 1.016
[0.958, 1.052]. The new view stays.

The IPC import is about 2.4× slower than the 1.5.5 figure in §1 (498 s),
for both schemas alike: the slowdown is not the key. (Explained and fixed
in §4c: DuckDB 2.0 parses the per-element `list_value(?, …)` INSERT
several times slower than 1.5.5.)

## 4c. Columnar-bind write path (2.0.0)

The core now binds each bulk column as ONE list parameter: a long value
is `INSERT … SELECT ?, unnest(?::T[]), …` per ≤ 16 384 cells, an array
payload one `?::DOUBLE[]`, a materialized axis's child rows one such
statement per kind (`specs/Done/appender_write_path_PLAN.md`; the stored
rows are byte-identical, pinned by `tests/write_path_golden.rs`).
Measured with `results_path.rs` (`dense10k-ipc long bindprobe`, full
scale, one run) on 2026-10-23: the AMD Ryzen 9 5900XT host, shared, load
average 4-12 during the run; bundled DuckDB 2.0.0 (`v2.0.0-dev88614`),
release.

| | before (§4b, `array_id`) | after |
|---|---|---|
| IPC import, 87.6 M dense values, one commit | 1 200.4 s (1 186.9 / 1 218.6 s) | **97.3 s** (900 k values/s) |
| file size after it | 735 326 208 B | 735 326 208 B (identical) |
| long, 876 000 cells, core API loop (2 runs) | not re-measured on 2.0 | 13.9 / 13.9 s (63 k values/s) |
| long, same, IPC import (2 runs) | 215.0 s on 1.5.5 (§1) | 12.9 / 12.8 s (68 k values/s) |

Bind probe on the same run (in-memory table, 8 760 values): the former
`list_value(?, …)` INSERT costs 86.5 ms to prepare (median, under this
run's load; 13.5 ms on 1.5.5 in §1) and 17.8 ms to execute; the bound
`?::DOUBLE[]` INSERT costs 0.27 ms to prepare and 1.42 ms to execute.

Paired, interleaved A/B of the write paths (old and new core in release,
21 pairs per row, median per-pair ratio new/old with a bootstrap 95 %
CI; harness `scripts/bench/ab_write_path/`): new 8 760-cell long value
0.080 [0.080, 0.081] (1 781 → 143 ms), long replace 0.127 [0.124,
0.129], new 8 760-value array 0.073 [0.072, 0.074] (111 → 8.0 ms),
array replace 0.134 [0.133, 0.137], a new irregular 8 760-point datetime
axis 0.027 [0.027, 0.028] (5.9 s → 0.16 s), scalar insert 0.42 [0.40,
0.45], scalar replace 0.48 [0.47, 0.49], `write_parameter` with 100 new
scalar entities 0.62 [0.60, 0.65]; a current-cell read of a long value
1.01 [0.97, 1.03] (unchanged). The import slowdown queued in §4b was
the 2.0 statement-parse cost of the per-element placeholders.

## 5. Commit cost after an array replace

The core replaces an array with a pre-image copy into `value_array_hist`
followed by an in-place UPDATE of the `value_array` row. Replayed on a
scratch copy of the 805 MiB file, 10 replaces, each its own transaction.

| Engine / file | Setting | pre-image copy (median) | write (median) | COMMIT each (ms) | WAL after each commit |
|---|---|---|---|---|---|
| 1.5.5, 1.5.5 file | default (checkpoint_threshold 16 MiB) | 124 ms | 12 ms | 493, 4503, 466, 3852, 250, 3731, 383, 3657, 239, 3753 | 121 MiB, 0, 121, 0, ... |
| 1.5.5, 1.5.5 file | checkpoint_threshold 100 GB | 119 ms | 11 ms | 232-455 (median 266) | +121 MiB per commit; close 4.7 s |
| 2.0.0, same 1.5.5-format file | default | 124 ms | 9 ms | 458, 3721, 391, 2942, ... (median 1 730) | 121 MiB, 0, 121, 0, ... |
| 2.0.0, same 1.5.5-format file | checkpoint_threshold 100 GB | 120 ms | 9 ms | 231-514 (median 357) | +121 MiB per commit; close 5.3 s |
| 2.0.0, file re-written in 2.0 format | default | 126 ms | 13 ms | 3-7 (median 4.3) | 0.1-2.8 MiB total; close 0.9 s |
| 2.0.0, file re-written in 2.0 format | checkpoint_threshold 100 GB | 107 ms | 11 ms | 3-9 (median 4.4) | same |

Replacing by DELETE + INSERT instead of UPDATE gives the same numbers in
every row.

- **Reproduced, and it is two effects.** On a 1.5.5-format file, every
  single-row write to `value_array` (UPDATE, or INSERT of one row, even an
  UPDATE of `v_min` alone) writes **~121 MiB to the WAL** instead of the
  ~70 KB the row holds. Writing that WAL is the 230-500 ms COMMIT floor.
  Because 121 MiB exceeds the default 16 MiB `checkpoint_threshold`,
  every second commit also runs an **automatic checkpoint** (3-4.5 s; the
  WAL drops to 0).
- **A checkpoint setting explains only the second part.** Raising
  `checkpoint_threshold` removes the multi-second commits but not the
  WAL write, and the deferred checkpoint is paid at close (4-5 s after
  10 replaces).
- **It is an index plus list-column effect of the old storage format.**
  On toy tables in 1.5.5, a one-row INSERT writes 64-121 MiB of WAL when
  the table has any primary key (single- or multi-column) and a list
  column, and 0.07 MiB without a key or without a list. DuckDB 2.0.0
  writing to a file it created itself logs 0.07 MiB and commits in 3-9 ms;
  2.0.0 writing to a file created by 1.5.5 behaves exactly like 1.5.5.
  An upgrade therefore only helps after the file is re-written
  (`COPY FROM DATABASE`, i.e. a compaction under 2.0).
- The pre-image copy itself (~120 ms) is the composite-key sequential
  scan of section 4, on both engines.

## 6. Current `main` on DuckDB 2.0.0 (full run, 2026-10-24)

`just bench-results` at full scale (no `--quick`): `results_path.rs -- all`
then `raw_duckdb_layout.py`, one run each, on `main` at `eaf2551`, which
has DuckDB 2.0.0, the `array_id` key (§4b) and the columnar-bind write
path (§4c).

| | |
|---|---|
| Date | 2026-10-24, 00:07 to 00:28 (Rust bench 1 138 s, raw-DuckDB script 102 s) |
| Machine | the same AMD Ryzen 9 5900XT host as sections 1-5 (16C/32T, 70 GB, WD_BLACK SN850X NVMe, databases in `/tmp`); Linux 6.14.0-37-generic, rustc 1.98.0, `bench` profile |
| DuckDB | core: bundled `v2.0.0-dev88614` (duckdb-rs 1.20000.0); Python: duckdb 2.0.0 (project `.venv`), numpy 2.5.4, pyarrow 26.0.0, Python 3.12.3 |
| Load | 1-min load average 1.8-3.0 for most of the run, one excursion to 13.5 around 00:18 (during the read phases; it fell back to 3-4 by 00:21). One other interactive process used about 0.7 of a core throughout. No other bench ran. The `cellsgate` phase logged 3.57 at its start and 3.35 at its end. |

Much quieter than sections 1-5 (load 7-45). The code also differs, so the
1.5.5 column below mixes both effects; it is the same machine, though.

### 6.1 Ingest

| Shape / lane | Wall (2.0.0, current `main`) | values/s | File | Gross / net B/value | §1 (1.5.5, best run) |
|---|---|---|---|---|---|
| dense 1 000 series, API loop (2 runs: 11.5 / 11.3 s) | 11.3 s | 774 k | 126.3 MiB | 15.11 / 14.87 | 37.7 s, 127.3 MiB |
| dense 1 000 series, IPC import (2 runs: 10.0 / 10.1 s) | 10.0 s | 875 k | 69.8 MiB | 8.35 / 8.11 | 35.0 s, 71.5 MiB |
| dense 10 000 series, API loop (1 run) | 127.1 s | 689 k | 957.5 MiB | 11.46 / 11.44 | 659.8 s, 1 591.8 MiB |
| dense 10 000 series, IPC import (1 run) | 86.4 s | 1.01 M | 701.3 MiB | 8.39 / 8.37 | 497.8 s, 804.8 MiB |
| long, 1 000 × 876 integer cells, API loop (2 runs: 13.4 / 13.4 s) | 13.4 s | 65.5 k | 71.5 MiB | 85.6 / 83.2 | 181.7 s, 93.0-93.3 MiB |
| long, same, IPC import (2 runs: 12.3 / 12.0 s) | 12.0 s | 73.3 k | 67.3 MiB | 80.5 / 78.1 | 215.0 s, 79.0 MiB |

Empty-schema baseline for "net": 2 109 440 B (2.0 MiB; 3 158 016 B on 1.5.5).

- The 10k API loop's per-100-entity commit now runs 0.81-1.55 s (median
  1.18 s, first 0.92 s, last 1.55 s), against 3.0 → 10.1 s on 1.5.5. It
  still grows with table size, more slowly.
- The many-commit penalty on file size shrinks: 957.5 against 701.3 MiB
  (1.37×) at 10k, against about 2× on 1.5.5. At 1k it is unchanged
  (126.3 against 69.8 MiB).
- The long tier now ingests at 65-73 k values/s against 0.87-1.01 M for the
  array tier, about 14× slower per value (was ~50×). Its storage is
  78-83 B/cell net, still ~20× the raw 4.2 B/cell (§3; the schema
  overhead is untouched by the write path).
- The IPC import of 87.6 M values (86.4 s) matches §4c's 97.3 s within
  that run's load.

**Bind probe** (in-memory table, 8 760 values): the former
`list_value(?, …)` INSERT, 63.8 / 65.3 ms to prepare (min / median),
15.6 / 16.4 ms to execute; the bound `?::DOUBLE[]` INSERT the core uses
now, 0.25 / 0.26 ms to prepare and 1.34 / 1.38 ms to execute. The bench's
"statement-building share" line (81.7 ms of former-mechanism statement
work against 10-11 ms/series end to end, "722-817 %") no longer measures
anything: the core does not issue that statement any more.

### 6.2 Reads through the core (10 000-series files)

Warm min / median, ms (cold min / median in brackets, fresh RO connection,
open excluded; n = 15 warm, 5 cold).

| Read | IPC-built, one commit (701.3 MiB, 1 row group) | API-built, 100 commits (957.5 MiB, 2 row groups) |
|---|---|---|
| RO connection open | [27.6 / 31.9] | [114.1 / 116.1] |
| one series, whole value, `read_parameter_dense` | 2.35 / 2.48 [6.44 / 7.12] | 2.42 / 2.57 [6.74 / 7.14] |
| one series, week slice `values[4001:4168]` | 2.47 / 2.62 [6.06 / 7.96] | 2.48 / 2.67 [6.94 / 7.02] |
| one series, single cell `values[4001]` | 2.48 / 2.54 [6.23 / 6.69] | 2.35 / 2.51 [6.86 / 7.06] |
| one series, min/max only | 2.38 / 2.48 [4.01 / 4.34] | 2.34 / 2.46 [4.50 / 4.62] |
| week × 20 series, (a) dense + client slice | 3.78 / 4.33 [9.83 / 15.90] | 3.97 / 4.27 [15.32 / 15.40] |
| week × 20 series, (b) SQL list slice | 3.77 / 4.00 [10.84 / 15.24] | 3.73 / 3.93 [14.63 / 14.83] |
| week × 20 series, (c) unnest on `value_array` by triple | 84.56 / 86.93 [181.71 / 186.14] | 34.54 / 35.76 [177.01 / 181.67] |
| week × 20 series, (d) `read_parameter` + `idx1_ts` | 3 010 / 3 028 [3 414 / 3 529] | 3 133 / 3 149 [3 323 / 3 344] |
| 2 adjacent series, dense macro | 2.48 / 2.58 [7.11 / 7.96] | 3.03 / 3.31 [8.27 / 9.78] |
| 51 series, dense macro (past the 50-key index limit) | 24.04 / 25.33 [178.8 / 182.6] | 48.82 / 53.98 [237.6 / 264.2] |
| `read_parameter`, whole parameter (8.76 M rows) | 4 069 / 4 107 [4 288 / 4 421] | 4 251 / 4 292 [5 810 / 5 998] |

- Point, slice and whole-series reads through the dense macro stay at
  ~2.4 ms warm, ~6-7 ms cold, on both files (§4b measured 2.36 ms).
  On 1.5.5 with the composite key (§2) the same reads took 110-118 ms.
- The 51-key read, which falls back to a sequential scan, measured 24 ms
  warm here against 88 ms in §4b; its cold figure (179 ms) matches §4b
  (195 ms). The warm gain is not explained here.
- (d) and the whole-parameter `read_parameter` remain the slow path
  (3-4 s): the canonical long form at 8.76 M rows, as in §4b.

**`value_array_cells` gate** (`cellsgate`, threads = 8, 21 randomized
pairs, threshold 1.10): old view median 7 537 ms, new 7 945 ms; per-pair
ratio median **1.052**, 95 % bootstrap CI **[1.043, 1.062]** →
**PASS**. Load 3.57 → 3.35 (1-min drift 1.07×, valid). Consistent with
§4b's valid run (1.041 [1.003, 1.059]).

**Row-group steel-man** (`value_array` copied into scratch files, direct
table reads, warm min / median ms):

| Copy | Row groups | File | One series whole / week / cell (warm min) | min/max only | 20 series week |
|---|---|---|---|---|---|
| default ROW_GROUP_SIZE | 1 | 690.5 MiB | 20.6 / 20.3 / 20.1 ms | 0.66 ms | 20.8 ms |
| ROW_GROUP_SIZE 2048 | 5 | 594.8 MiB | 34.3 / 32.4 / 32.2 ms | 0.65 ms | 34.4 ms |

Cold reads are ~170-200 ms for both copies. Smaller row groups still do
not help; the `array_id` macro (2.4 ms) beats both direct-table copies.

**Long tier** (`value_long`, 876 000 live integer cells, 8 row groups,
67.3 MiB IPC-built file):

| Read | Warm min / median | Cold min / median |
|---|---|---|
| RO connection open | — | 27.9 / 30.2 ms |
| one cell | 1.58 / 2.92 ms | 1.96 / 2.92 ms |
| one series, week window (17 points) | 0.93 / 0.99 ms | 1.86 / 2.39 ms |
| one series, whole (876 points) | 0.77 / 0.79 ms | 1.90 / 2.13 ms |
| one series, min/max aggregate | 1.07 / 1.14 ms | 2.29 / 2.47 ms |
| 20 series, week window | 3.50 / 3.92 ms | 6.07 / 7.52 ms |
| `read_parameter`, 20 series, timestamp window | 50.4 / 53.5 ms | 65.0 / 70.6 ms |

These are about 2× the 1.5.5 figures of §2 (e.g. 20-series window 1.63 →
3.50 ms, `read_parameter` 26.0 → 50.4 ms warm min), under much lower load,
with a different engine and write path (the stored rows are byte-identical,
§4c). Not diagnosed in this run; the §4 note already saw the long-tier
point reads at 0.7-1.1 ms on 2.0.0 against 0.4-0.6 ms on 1.5.5, so a 2.0
engine effect is the likely candidate.

### 6.3 Raw DuckDB layout (duckdb 2.0.0, Python)

Sizes match §3 within 0.3 %: dense default 832.8 MiB (9.97 B/value), dense
ROW_GROUP_SIZE 2048 595.3 MiB (7.12), long 649.0 MiB (7.77), long_int
3.8 MiB (4.19 net). Built in 26.4 s.

| Read (warm min / median, ms) | dense default | dense rg 2048 | long |
|---|---|---|---|
| RO open | 16.6 / 20.1 | 15.7 / 17.7 | 15.9 / 18.4 |
| one cell | 98.8 / 101.6 | 94.2 / 97.7 | 0.84 / 0.93 |
| one week (168) | 101.5 / 105.6 | 94.2 / 98.8 | 0.85 / 0.93 |
| one whole series (8 760) | 98.2 / 103.4 | 92.9 / 98.3 | 0.79 / 0.90 |
| min/max only | 0.47 / 0.57 | 0.54 / 0.63 | 1.44 / 1.53 (aggregate) |
| one week × 20 series | 336 / 348 | 95.9 / 101.3 | 1.74 / 2.02 |
| full-table aggregate | 699 / 707 | 750 / 765 | 23.2 / 24.2 |

Reproduces §3's 2.0.0 column within a few percent: the conclusions of §3
stand unchanged.

## 7. Long-tier size breakdown and one very long array (2026-10-24)

Two additions on `main` at `257812a` (DuckDB 2.0.0), on the same AMD Ryzen
9 5900XT host as §6 (Linux 6.14.0-37-generic). The machine was shared:
1-min load average 3.4-5.5 during §7.1 and 9-14 during §7.2.

### 7.1 Where the long tier's bytes go

`scripts/bench/long_tier_size_breakdown.py` (`just bench-long-size`;
Python duckdb 2.0.0 from the project `.venv`, numpy 2.5.4, pyarrow
26.0.0, Python 3.12.3). It builds the real schema with `schema/build.py`,
writes the long-tier shape of §6.1 (1 000 series × 876 integer cells, the
same values as `results_path.rs`) into `value_long` the way the one-commit
import does (one transaction, one INSERT per series, `SET threads = 1`),
and removes one part of the schema per variant. Sizes are DuckDB's used
blocks after CHECKPOINT, net of the same file before the insert; one
block is 256 KiB, so the resolution is about 0.3 B/cell. Three runs gave
identical figures.

| Variant | Used B/cell |
|---|---|
| A: schema as built (foreign keys, `one_current_long`, `ix_value_long_triple`) | 69.13 |
| B: A without `one_current_long` | 11.67 |
| C: B without `ix_value_long_triple` | 11.07 |
| D: C without the foreign keys (`value_long` re-created from the catalog DDL, REFERENCES removed) | 10.47 |
| E: D without the `i1_dt` accelerator column | 4.19 |
| L: lean `(k INTEGER, pos INTEGER, v BIGINT)`, no nodal-db schema | 4.19 |

| Part | B/cell | Share of 69.13 |
|---|---|---|
| `one_current_long`, the 7-column one-current unique index | 57.46 | 83 % |
| `i1_dt`, the copy of the time coordinate | 6.28 | 9 % |
| keys, positions, values and all other columns (variant E) | 4.19 | 6 % |
| `ix_value_long_triple` | 0.60 | 1 % |
| foreign keys (commit, axis) | 0.60 | 1 % |

- With the indexes, foreign keys and `i1_dt` removed, `value_long` stores
  this shape in the same 4.19 B/cell as the lean table (§3, §6.3). The
  history columns, the empty typed value columns and the unused index
  columns cost nothing measurable: every one of them is stored as a
  constant segment (`pragma_storage_info` compression `Constant`).
- **Cross-check on a file written by the core** (`--core-db` on the
  `ipc_long_r0.db` kept from a `results_path.rs -- long` run, net of an
  empty schema): 77.51 B/cell as written; 20.05 without
  `one_current_long` (the same 57.46 B/cell); 19.45 also without
  `ix_value_long_triple`; 8.38 with `value_long` dropped altogether. So
  `value_long` and its indexes account for 69.13 B/cell in the core's
  file too, the same as variant A. The remaining 8.4 B/cell are the other
  tables the import fills (1 000 entities, the commit, the parameter
  definition, the axis, ...) and their indexes: about 7 MiB in total for
  this file, which does not grow with the number of cells per series.
  This is the difference between 69 B/cell here and the 78 B/cell net
  file size of §6.1.

### 7.2 One very long array through the core

`results_path.rs` phase `bigarray` (`cargo bench -p nodal-db-cli --bench
results_path -- bigarray`; also part of `all`; bundled DuckDB
`v2.0.0-dev88614`, `bench` profile). One float series of a year at
one-minute resolution (525 600 values, full-precision noise) is written
through `api::write_parameter` into a fresh file, then replaced once.
Arrays are one row, so the whole list is canonicalized and bound as one
parameter. Peak memory is the resident-set high-water mark during the call
minus the RSS at its start (Linux `/proc/self/clear_refs`), with the input
batch built before the call; it includes DuckDB's own allocations. Two
invocations of three fresh files each, load average 9-14:

| | Wall per call, commit included | Peak RSS above the start |
|---|---|---|
| First write | 587-796 ms | 165-190 MiB |
| Replace (pre-image copy to `value_array_hist`, in-place update) | 1 309-1 560 ms | 138-180 MiB |

That is about 1.1-1.5 µs per value for the first write and 2.5-3.0 µs per
value for the replace, somewhat more per value than the 8 760-value
array figures of §4c (0.9 and 1.8 µs, 20 values per commit, at lower
load). An earlier probe of the bind step alone, outside the core (176 ms,
135 MB peak, `specs/Done/appender_write_path_PLAN.md` C7), is superseded
by these end-to-end figures.

## 8. Set-based long write (2026-10-24, DuckDB 2.0.0)

`cargo bench -p nodal-db-cli --bench results_path -- long`, full scale,
two runs per arm, run back to back on the same host (load average
4.6-5.3): old = `ee3f369` (per-key long writer), new = the set-based
long write (`specs/Done/set_based_long_and_delete_PLAN.md`; the long insert
moved its `unnest`s into a FROM subquery).

| Shape / lane | old | new | ratio |
|---|---|---|---|
| long, 1 000 × 876 integer cells, API loop (10 commits) | 11.8 / 11.6 s (75 k values/s) | 5.7 / 5.6 s (156 k values/s) | 0.48 |
| long, same, IPC import (one commit) | 10.4 / 10.3 s (85 k values/s) | 4.4 / 4.5 s (197 k values/s) | 0.43 |
| file size (API / IPC) | 74 985 472 / 70 529 024 B | identical | |

The API loop's ratio is against the appender plan's D10 gate of
<= 0.85. The old arm here is faster than §6.1's 13.4 s (lower load).
The long tier now ingests at about 150-200 k values/s against 0.9-1.0 M
for the array tier, about 5x slower per value (§6.1: 14x).

## 9. Save path: staleness check and commit backstop (2026-10-24, DuckDB 2.0.0)

Harness `scripts/bench/ab_write_path` (`src/save_path.rs`), release
builds, three arms: old = `c92ac18`; nofeat = the new tree without the
`save_api` feature (the editor's save today: RO head poll, then
`commit_changes`); new = the new tree with it (`commit_changes` with
`WriteOptions::expect_commit`). Fixture: 5 classes × 40 entities × 10
scalar parameters; a save replaces one scalar. The post-save reload
(`model_slurp`, a copy of the editor's `ModelCache::load` statement
sequence without its decode) is timed on its own: it dominates a save and
is the same in both arms. Paired, interleaved `ab.py`; medians and
bootstrap 95 % CIs. The first pass ran at load average 18-22 (shared
host); the rows marked † were re-run at load 2.8-3.5.

| Mode | arms | old | new | ratio (95 % CI) |
|---|---|---|---|---|
| `editor_save` † (commit span, ms/save) | nofeat → new | 125.4 | 79.0 | 0.630 (0.625-0.636) |
| `editor_save` (same, load ≈21) | nofeat → new | 166.8 | 105.3 | 0.636 (0.619-0.647) |
| `editor_reload` (ms) | nofeat → new | 354.9 | 354.6 | 0.995 (0.989-1.031) |
| `editor_session_100` † (growth slope, ms/save) | nofeat → new | 2.73 | 1.85 | 0.70 (0.66-0.90) |
| `editor_session_100` (load 18 → 8: discard by the 2× rule) | nofeat → new | 2.84 | 1.81 | 0.64 (0.48-0.97) |
| `editor_save` † | old → nofeat | 123.8 | 125.0 | 1.011 (0.995-1.030) |
| `scalar_replace` (400 values/txn, ms/value) | old → nofeat | 2.516 | 2.499 | 1.000 (0.991-1.011) |
| `txn_1_scalar_replace` (ms/commit) | nofeat → new (`expect_commit = Some`) | 75.29 | 75.90 | 1.010 (1.000-1.019) |

- One RO open less per save: −46 ms (load 3) / −61 ms (load 21) on the
  commit span. The growth of the per-save time falls from ≈2.7-2.8 to
  ≈1.8-1.9 ms per save (three opens per save → two).
- With `expect_commit = None` the new tree is the old one (both CIs
  include 1.0). `Some` adds the in-transaction `SELECT max(id)`: +0.6 ms
  per commit.

**Commit backstop sessions** (`growth_session`, new arm, one binary, load
≈4-12). Each save: `commit(expect)` → `compact_if_due(Some(id))` →
reload; the file is compacted once before the series. "Save" = commit +
reload, excluding the compaction stall; "first10"/"last10" are means of
the first and last ten saves.

| Saves | backstop | compactions | stall (ms) | save mean incl. stalls | first10 / last10 | commit first10 / last10 | RO open+close at end |
|---|---|---|---|---|---|---|---|
| 100 | off | 0 | – | 432.8 | 343.7 / 513.0 | 78.7 / 169.7 | 132.2 |
| 100 | auto (N = 50) | 2 | 1 137 | 407.2 | 343.3 / 419.8 | 77.5 / 123.6 | 41.3 |
| 200 | off | 0 | – | 527.6 | 345.3 / 714.4 | 79.2 / 274.5 | 222.6 |
| 200 | 30 | 6 | 1 086 | **388.5** | 343.2 / 354.3 | 78.3 / 86.2 | 61.5 |
| 200 | 50 | 4 | 1 112 | 395.6 | 332.6 / 408.3 | 74.5 / 118.8 | 38.3 |
| 200 | 100 | 2 | 1 190 | 431.6 | 336.6 / 489.6 | 76.8 / 163.9 | 40.3 |
| 60, m1k (25 MiB) | auto (N = 50) | 1 | 1 394 | 3 153.6 | 3 074 / 3 123 | 80.4 / 84.7 | 48.9 |

- Without the backstop the save keeps slowing (+1.9 ms per save; after
  200 saves a save is 2× slower and an empty RO open costs 223 ms).
- N = 30 and N = 50 are within 2 % of each other in mean save time
  (388.5 vs 395.6 ms); N = 100 is 9 % slower. This matches the plan's
  estimate (optimum ≈35 for a two-open save, flat between 30 and 100).
- The stall is ≈1.1 s on the fixture-sized file and 1.4 s on m1k (plan
  §11: 1.05 s and 1.34 s).
- On m1k the reload (≈3.0 s) dwarfs everything else; see the editor's
  reload quick win and incremental-refresh issue.

## Mapping to the earlier Set B figures

The earlier figures came from the same harness (restored here unchanged
apart from the additions listed in the bench's module docs) on the same
machine, in a quiet session; the recorded `nodal-db` code has changed since.

| Earlier figure | Now | Status |
|---|---|---|
| Dense ingest, one commit, ~280 k values/s (10k: 312 s) | 250 k values/s at 1k (35 s); 176 k at 10k (498 s, under load) | Reproduced at 1k within machine noise; the 10k run was slowed by external load (load average 20-25 during it) |
| Dense 1k API loop 33.5 s, 262 k values/s | 37.7-38.2 s, 229-232 k values/s | Reproduced (~12 % slower, consistent with the load) |
| Dense 10k API loop 491-582 s, 1 594.3 MB | 659.8 s, 1 591.8 MiB | Size reproduced (0.2 %); time slower under load |
| Long ingest ~5 400 values/s, ~50× slower | 4 100-4 800 values/s, ~52× slower | Ratio reproduced; absolute ~15-25 % slower |
| Statement share about two thirds, ~22 ms/series | 61-67 %, ~23 ms/series | Reproduced |
| 72.5 MB for 8.76 M dense values, ~8.3 B/value | 71.5 MiB (74 985 472 B), 8.56 B/value gross, 8.20 net | Reproduced; note the earlier "MB" were MiB, so the earlier 8.3 B/value mixed units (gross is 8.6) |
| 127 MB for the same data in 10 commits | 127.3 MiB | Reproduced |
| 80 MB long, ~90 B/cell | 79.0 MiB, 94.6 B/cell gross, 91.0 net | Reproduced (the ~90 figure is net of the empty schema) |
| Week × 20 series ~110 ms warm, ~220 ms cold | 111-113 ms warm min, 224-234 ms cold min | Reproduced |
| `read_parameter` week ~245 ms warm | 236 / 246 ms | Reproduced |
| One whole series or one week ~116-120 ms | 110-111 ms warm min, 116-118 ms median | Reproduced; single cell and min/max added (115 ms and 0.8 ms) |
| RO open 19.5-20.4 ms (805 MB), 103-107 ms (1 594 MB) | 21.1-21.7 ms, 111.2-111.9 ms | Reproduced |
| Smaller row groups did not help | 2048-row groups: 96-98 ms vs 41 ms default copy | Reproduced (no help), with a caveat: the default-size direct-table copy read faster than the real file (41 vs 110 ms); not explained here |
| All 10 000 arrays in one row group | 1 row group, 3 169 blocks | Reproduced |

Not reproduced: nothing outright. The earlier explanation of the slow
point read ("DuckDB scans the list column regardless of selectivity") is
superseded by section 4: the scan happens because the composite key
cannot be used as an index, and a single-column key avoids it.
