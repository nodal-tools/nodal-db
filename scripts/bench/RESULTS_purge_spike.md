# Purge spike: whole-database close-outs under the DuckDB cap

A purge closes every row of a scope in one transaction. This spike
measures whether the set-based statements a purge runs fit the budget
every core connection gets in this repo: DuckDB with 4 threads and a
memory limit of 80 % of 8 000 MB (`.cargo/config.toml`), caching
operators off (`conn::core_config`). It is a recording benchmark: nothing
here gates a build.

| | |
|---|---|
| Harness | `scripts/bench/purge_spike.py` (`all --dir DIR`; fixtures generated once into `DIR`, a few GB) |
| Date | 2026-11-17 |
| CPU / RAM | AMD Ryzen 9 5900XT, 16 cores / 32 threads; 70 GB |
| Disk | NVMe, ext4; databases in `/tmp` (same device) |
| DuckDB | 2.0.0 (project `.venv`, the version the core bundles) |
| Connection | `threads = 4`, `memory_limit = 6400MB`, `debug_enable_caching_operators = false` |

Each scenario runs in a fresh child process on a fresh copy of its
fixture; the parent records the child's peak RSS (`wait4`). The
statements are the shapes the purge uses: the scope and survivor sets are
transaction-local `TEMP` tables; every close carries `valid_from_commit <
c AND valid_to_commit IS NULL`; values are split by retention through a
`purge_retention(pd_id, retained)` table filled before any close; arrays
resolve `array_id`s on narrow columns first, then copy the pre-image to
`value_array_hist` and delete by `array_id`. One `BEGIN`, one commit row,
`COMMIT`, then `CHECKPOINT`.

## Results

| Scenario | Rows closed | Statements (s) | COMMIT (s) | Total (s) | Peak RSS (MB) | File before → after (MB) |
|---|---:|---:|---:|---:|---:|---:|
| `Values{}`, 10 M scalars, retained | 10 000 000 | 30.6 | 7.4 | 38.0 | 424 | 169 → 243 |
| `Values{}`, 10 M scalars, stub | 10 000 000 | 32.0 | 7.5 | 39.5 | 351 | 169 → 175 |
| `Values{}`, 105 M long cells, retained | 105 120 000 | 453.4 | 142.2 | 595.6 | 3 433 | 1 625 → 3 061 |
| `Values{}`, 105 M long cells, stub | 105 120 000 | 469.4 | 142.5 | 611.9 | 2 828 | 1 625 → 2 419 |
| `Values{}`, 1 000 × 8 760 arrays, retained | 1 000 | 0.25 | 0.26 | 0.71 | 693 | 67 → 125 |
| `Values{}`, 1 000 × 8 760 arrays, stub | 1 000 | 0.01 | 0.01 | 0.17 | 92 | 67 → 68 |
| `Entities{}`, 100 k 0-D + 20 k 2-D entities, every leg | 980 000 | 3.3 | 2.5 | 5.8 | 280 | 33 → 44 |
| `Values{class}`, 5 M scalars, 50 % survivor anti-join | 2 500 000 | 9.0 | 2.3 | 12.4 | 1 344 | 169 → 255 |

The `Entities{}` scenario closes 600 000 scalar values, 120 000
activations, 50 000 locations, 50 000 memberships, 40 000 element rows and
120 000 entity rows; its B1 frontier (all entities are already in scope)
needs one round.

`EXPLAIN` of a set-form leg (`UPDATE entity_alternative … WHERE entity_id
IN (SELECT id FROM <set>) AND <guard>`), in both bind forms (a `TEMP`
table and a bound `BIGINT[]` list through `unnest`), and of the scalar
close with a survivor anti-join (`NOT EXISTS`): a hash join (SEMI, and
ANTI for the survivors) in every case, no nested loop, blockwise,
piecewise-merge or cross-product operator.

## Verdict

**The kill criterion is not met: a whole-database close-out fits the
cap.** The largest case, 105 M long cells in one transaction, peaks at
3.4 GB RSS against a 6.4 GB DuckDB limit, with no spill failure. It is
slow: about 10 minutes, of which the `UPDATE` is 7.5 (it runs on about one
core: an update of an indexed column is a delete plus an insert per row)
and the `COMMIT` 2.4. The file grows by the closed rows' new versions (1.6
→ 3.1 GB retained) until compaction reclaims it.

Consequences for the plan:

- **Bind form: temp tables.** The purge's sets are computed in SQL from
  the scope (a class's entities, every live alternative), so no id list
  passes through Rust and a `TEMP` table is the natural carrier; both
  forms plan as the same hash join. The `unnest` form stays available to
  the leg templates (the render guard test renders it).
- **No chunking, no unscoped-purge limit.** One statement per leg and
  tier is fine at these sizes.
- **Documentation.** A purge of a results-sized long tier is a
  minutes-long transaction that doubles the tier on disk until
  compaction; the usage guide recommends a new file or a history-free
  extract for that case rather than a purge.
