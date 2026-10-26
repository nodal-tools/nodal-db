# Engine rules: DuckDB storage, ids and indexes

nodal-db relies on a few DuckDB behaviours that are easy to break by
accident. When one breaks, nothing raises an error: the file gets bigger,
a commit gets slower, or a query falls back to a full scan. This page
lists the rules that keep those failures from happening, why each one
exists, and the test that guards it.

**Read this before you change** any of:

- the schema DDL (`schema/*.sql`), including indexes and sequences;
- SQL statements in the core (`crates/nodal-db-core/src/`), especially
  ones that read or write `value_array` or look up entities by name;
- how database files are created, rebuilt or compacted, or how
  connections to them are opened;
- the bundled DuckDB version (see also
  [DuckDB upgrades](https://github.com/nodal-tools/nodal-db/blob/main/CONTRIBUTING.md#duckdb-upgrades)
  in `CONTRIBUTING.md`).

A change to any of these is also performance-sensitive: run the A/B perf
gate (`just perf-gate`, see "Performance gates" in `CONTRIBUTING.md`)
before merging it.

The rules describe DuckDB 2.0. Several of them depend on optimizer or
storage behaviour that DuckDB does not document as a contract, so the
guard tests are the real contract: if a DuckDB upgrade changes the
behaviour, a guard fails, and the rule has to be re-evaluated. For the
wider system design, see [Architecture](architecture.md).

## 1. Files use DuckDB's native 2.0 storage format

**Rule.** Every database file is created in DuckDB's native 2.0 storage
format (tag `v2.0.0+` in `duckdb_databases()`). Never set
`storage_compatibility_version`. Readers therefore need DuckDB 2.0 or
newer.

**Why.** On a file in the older 1.5 format, every write to `value_array`
logs the whole list column to the write-ahead log (about 121 MiB for a
single-array write at the scale of a model's results), and every second
commit triggers a checkpoint that takes seconds. On a native 2.0 file the
same commit takes milliseconds. The measurements are in
[`scripts/bench/RESULTS_results_path.md`](https://github.com/nodal-tools/nodal-db/blob/main/scripts/bench/RESULTS_results_path.md).
DuckDB 2.0 keeps writing an existing 1.5-format file in the old format,
so a stale file stays slow until it is rebuilt (compaction always writes
a native file).

**Guards.**
[`crates/nodal-db-core/tests/storage_format.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/storage_format.rs)
and the storage-format check in `schema/smoke_test.py`.

**Related: open time grows with commits.** Each read-write
commit-and-checkpoint cycle adds a roughly constant cost (about 0.9 ms)
to every later open of the file, while barely changing its size. This is
a DuckDB catalog growth issue tied to foreign keys
([duckdb/duckdb#27808](https://github.com/duckdb/duckdb/issues/27808)).
Compaction resets it, which is why `compact_if_due` has a commit-count
backstop next to its byte-growth gate (see
[Architecture, §8 Compaction](architecture.md#8-compaction)). Do not
remove the backstop because a file's size looks fine: size is not what
it protects.

## 2. Array payloads are read through `array_id`, and ids stay odd

**Rule.** A statement that reads or writes `value_array`'s list columns
is keyed by `array_id`, never by the key triple (entity, parameter,
alternative). Resolve the `array_id` first, on narrow columns, then
touch the payload. Every `array_id` comes from the sequence
`seq_value_array`, which steps by 2: either through the column default
or by drawing ids with `nextval` ahead of a set-based insert. The only
other source is a verbatim copy of existing ids (compaction, extract).
No code path invents ids, and any re-creation of the sequence keeps its
full specification, increment included, so the stride and the parity of
the ids survive.

**Why.** Given a dense, contiguous set of ids, DuckDB rewrites the
lookup into a range filter, and the read silently falls back to a full
scan of the payload, about 100 times slower. The stride keeps id sets
non-contiguous so lookups stay on the primary-key index. Keying a
payload statement by the key triple has the same effect: the scan reads
the list columns of every row it considers. There is no error in either
case, only a slow read.

**Inside a macro, fold the key set into one scalar list.** A macro that
resolves its keys through joins (`read_values`, `value_rows_for`) fetches
the payloads with

```sql
WHERE p.array_id = ANY(coalesce((SELECT list(DISTINCT va.array_id) FROM … ), [-1]))
```

DuckDB plans this as a SEMI join of the payload scan against the
unnested list with a dynamic IN filter, which is an index scan for 0 to
50 keys, in every case measured: a hit, a miss, a parameter with no
arrays at all. Three details matter:

- `[-1]` stands in **only for an empty list** (`coalesce`). Appended to a
  non-empty list, a sentinel takes one of the 50 dynamic-filter slots, so
  50 real keys become 51 and the scan goes sequential. -1 is below every
  zone map, so the empty case prunes to nothing.
- `DISTINCT`: a repeated input key would take another slot (50 ids listed
  twice became a sequential scan).
- Never a `UNION ALL SELECT -1` sentinel: inside a macro whose keys come
  through joins it fell back to a full payload scan even on hits.

Above 50 keys (`dynamic_or_filter_threshold`, a user setting) the read is
a sequential scan: correct, but as slow as a whole-parameter read. The
user docs state the limit in value hits, not names.

**Guards.**
[`crates/nodal-db-core/tests/array_id_index_scan.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/array_id_index_scan.rs)
(checks the plans of the production statements with `EXPLAIN ANALYZE`)
and
[`crates/nodal-db-core/tests/point_read_index_scan.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/point_read_index_scan.rs)
(the point-read macros: 0 to 50 keys, duplicates, the omitted-alternative
default, a time-window filter on top, and a 51-key negative control that
must stay sequential).

## 3. An index is an access path only for a lone single-key filter

**Rule.** DuckDB 2.0 scans through an index only when:

- the index key references exactly one base column. This excludes every
  multi-column index, and every index on this schema's `cur_*` and
  `ce`/`cp`/`ca` virtual columns, because those all reference
  `valid_to_commit` as well as a key; and
- the scan carries that one filter and no other filter that statistics
  cannot drop (DuckDB's `table_scan.cpp` falls back to a sequential scan
  when `FilterCount() != 1`).

So:

- Never add a plain (non-unique) index without an `EXPLAIN ANALYZE`
  guard test on a realistic fixture.
- Query a single-key index behind an `OFFSET 0` fence, which stops the
  optimizer from pushing the other filters into the indexed scan:

  ```sql
  SELECT … FROM (SELECT … FROM t WHERE key = ? OFFSET 0)
  WHERE <other filters>
  ```

**Why.** Small fixtures (one class, no history) hide the problem:
statistics prove the extra filters always true, drop them, and the index
is used. On a real file the same query is a sequential scan, with no
error. The fence is an optimizer behaviour, not a documented DuckDB
contract, so the guard test is the contract. If a DuckDB upgrade breaks
the fence, the fallbacks to try, in order, are listed in the doc comment
on `ENTITY_NAME_LOOKUP_SQL` in
[`crates/nodal-db-core/src/api.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/src/api.rs).

**Guards.**
[`crates/nodal-db-core/tests/entity_name_index_scan.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/entity_name_index_scan.rs)
(production SQL, a fixture with several classes and closed-out rows, and
an unfenced negative control),
[`crates/nodal-db-core/tests/array_id_index_scan.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/array_id_index_scan.rs),
and the index census in `schema/smoke_test.py`, which lists the plain
indexes exactly, so adding one requires a deliberate edit there.

## 4. Unique-index key order is a size decision

**Rule.** In a unique index, no column that can be constant within a
value may trail. Put the columns that are constant within a value first
and the column that varies most last.

**Why.** DuckDB's index (an adaptive radix tree) shares the key bytes
that rows have in common and stores each row's remaining bytes after its
last branch point. Trailing constants (positions beyond the value's
declared rank, the high zero bytes of each position, a per-value id) are
therefore paid on every row. Uniqueness does not depend on column order,
so the order is purely a size choice. The long tier's `one_current_long`
index cost 58–86 bytes per current row in the order
`(ce, cp, ca, c1, c2, c3, c4)` and 10–18 bytes in the current order
`(c4, c3, c2, ca, cp, ce, c1)`, with identical semantics. A wrong order
fails silently: the file is just bigger. The measurements are in
[Design rationale: value storage](design_rationale.md).

**Guards.**
[`crates/nodal-db-core/tests/long_index_size.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/long_index_size.rs)
(fails when the order or a DuckDB upgrade makes the index grow) and the
key-order checks in `schema/smoke_test.py`.

## 5. Every connection turns off DuckDB's caching operators

**Rule.** Every connection the core opens (and the CLI's one direct
open) gets its configuration from `conn::core_config`, which sets
`debug_enable_caching_operators = false`. Never open a connection with a
bare `Config::default()` or `Connection::open` in library code.

**Why.** DuckDB 2.0 has a bug in its caching operators. The query shape
that hits it is a join over dense ids that DuckDB runs as a perfect hash
join, then `unnest` of a list longer than 2048 elements whose length is
not a multiple of 2048, then a selective filter. In nodal-db that is
`read_parameter` or `value_rows` on 8760-long array values with a time
window (`WHERE idx1_ts BETWEEN …`) or a threshold (`WHERE v_float > …`),
on a file with several hundred such arrays. The query fails with
`INTERNAL Error: dict-surviving cache: global dictionary column 0
changed identity …`, and the error invalidates the whole database
instance, so every other connection on it fails too. No data is
damaged. DuckDB 1.5.5 is not affected; 2.0.0 and the 2.0.1 pre-releases
up to 2026-10-21 are. DuckDB issue: pending upstream report.

Turning the caching operators off avoids the bug, at no measurable
cost: the A/B perf gate passed every metric against the code without
the setting (quick scale, 2026-10-26). In the DuckDB source the
setting also stops pipelines from reusing operator and sink state when
they re-run (as in the recursive CTEs of the resolution views): fresh
state is built instead, which changes speed, not results. The setting does not
reach users who open the file with their own DuckDB connection; the
[usage guide](../usage.md#duckdb-version) tells them to set it
themselves. The setting is tagged `debug_` in DuckDB, so a release may
rename or remove it; every core open then fails with a clear error, and
the upgrade has to deal with it.

**Guards.**
[`crates/nodal-db-core/tests/duckdb_dict_cache_guard.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/duckdb_dict_cache_guard.rs):

- `raw_duckdb_still_has_the_bug` is a deliberate tripwire. It runs a
  three-statement repro on a plain connection and asserts that DuckDB
  still fails. When a DuckDB upgrade fixes the bug, it fails with "the
  DuckDB dict-cache bug appears fixed; remove the
  debug_enable_caching_operators workaround".
- `core_connection_reads_filtered_arrays_correctly` builds a file with
  800 arrays of 8760 values, checks that a time-window `read_parameter`
  fails on a plain connection, then checks the exact result through the
  core's read connection and that the core's read and write
  connections carry the setting.

**Removing the workaround,** once the tripwire fires on an upgrade:

1. Drop the `.with("debug_enable_caching_operators", "false")` line from
   `conn::core_config` (keep the function: it is the one place every
   open gets its configuration).
2. Delete `duckdb_dict_cache_guard.rs`, or keep its core test as a plain
   regression test of filtered array reads.
3. Remove this section and the warning in the usage guide (or narrow it
   to the DuckDB versions that still have the bug).
4. Run the perf gate (`just perf-gate`) with the DuckDB bump.

## 6. No build-side column through an UNNEST

**Rule.** In a statement that unnests array payloads, no column produced
by a join's build side may pass through the UNNEST. In practice:

- fetch the payloads only through a SEMI join (`= ANY(list)` or `IN`
  with a subquery), whose output is its probe side, the payload scan;
- take every column that is unnested or passed through from the payload
  scan itself;
- do every other lookup (axis lengths, names) after the unnest.

The self-join form `k JOIN value_array p ON p.array_id = k.array_id`
with `k`'s columns passed through, which `value_array_cells` (and through
it `value_cells`, `value_rows` and `read_parameter`) uses, is excluded
for new code.

**Why.** This is the shape of the DuckDB 2.0 dict-cache bug of §5: the
`array_id` self-join runs as a perfect hash join (the ids are dense) and
emits its build-side columns as a global dictionary; passed through an
UNNEST of lists whose length is not a multiple of 2 048 and followed by
a selective filter, they raise `INTERNAL Error: dict-surviving cache …`
and invalidate the database instance. §5 protects the core's own
connections, but not a user's: the SQL macros are the public read
surface, and a direct-SQL user opens the file with DuckDB's defaults. A
macro that follows this rule works on any connection, which is why the
user docs point direct-SQL users to `read_value` / `read_values` for
filtered reads of one or a few series. The rule also keeps new code
correct when the §5 workaround is removed after an upgrade.

**Guards.**
[`crates/nodal-db-core/tests/point_read_index_scan.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/point_read_index_scan.rs):

- a structural check over the macros' `EXPLAIN` plans: below every
  UNNEST, the only joins on the column path are SEMI, ANTI or MARK (only
  their probe side is followed; aggregates and CTE scans are barriers).
  It flags `value_array_cells` as a self-check;
- a data guard on a 1 000 x 8 760-cell fixture with dense ids, on a raw
  DuckDB connection with default settings (never the core's open, which
  turns caching off, §5): the macros return the exact counts under both
  failing filters, while `value_rows` under the same filter still
  raises (the positive control). When an upgrade fixes the bug, the
  positive control fails together with §5's tripwire: re-evaluate this
  rule then (it costs nothing to keep).

## 7. Read paths never expand parametric axes

**Rule.** A read path (`value_rows`, `read_parameter`, the point-read
macros, anything that renders coordinates) never joins an expansion of
every axis in the file. In practice:

- render a parametric dimension through its one `axis_parametric` row
  and the scalar macros `axis_param_ts`, `axis_param_num` and
  `axis_param_coord`, applied to the cell's own position, with the
  expansion's range as a join condition
  (`pos >= 0 AND pos < length`, so an out-of-range position renders
  NULL and the row is kept);
- join stored (child-table) positions only through
  `axis_position_stored_in_use(pd_id)`, which filters the `axis` scan and
  the child-table scan to the axes current values reference (of one
  parameter, or of every parameter with `NULL`);
- never put a pre-filter in front of an `unnest(range(length))` and rely
  on it reaching the axis scan;
- `axis_position` (every position of every axis) is for audits, extract
  and users, not for read paths.

The scalar macros are the only definition of the parametric arithmetic;
`encoding.rs` mirrors them and `roundtrip_duckdb.rs` pins the two
bit-for-bit through `axis_position`.

**Why.** `axis_position` expands every parametric axis with
`unnest(range(length))` and unions every stored position. `value_rows`
joined it once per dimension, and DuckDB 2.0's `common_subplan`
optimizer materialized the expansion once and built five hash tables of
it, whatever the read touched: with 500 extra 8 760-step axes (unused, or
used by another parameter) every `value_rows` or `read_parameter` call
cost 0.5 to 1.5 s more, scalar reads included. A pre-filter in front of
the expansion (`WHERE axis_id IN (…)`) is pushed below the UNNEST only
when the optimizer estimates the UNNEST as large; in one measured plan it
was not, and the expansion ran in full. The numbers are in
`specs/axis_position_cost_PLAN.md`.

**What it costs.** `value_rows` is a view and cannot see the caller's
filter, so its in-use axis set covers every current value: each call
scans the long and array tiers' key columns once (the set is evaluated
once per query) and hash-builds the stored positions of every axis in
use. On a one-axis file that is a few tens of milliseconds per call, and
it grows with the current long tier (about one key-column scan of it per
call). On a file whose values own many stored axes (irregular series,
each with its own axis) a `value_rows` point read pays for all of their
positions: slower than the expansion it replaced in that one shape.
Engine reads go through `read_parameter` (parameter-scoped) and the
point-read macros (key-scoped), not `value_rows`.

**Exception: the point-read macros' `ap`.** `value_rows_for` (behind
`read_value` and `read_values`) still filters `axis_position` by the
read value's own axes (`ap`). The filter is key-scoped, which a point
read needs; it works through a runtime join filter that reaches the
`axis` scans, which is an optimizer outcome, so it is guarded:
`point_read_axis_positions_are_limited_to_the_values_axes` in
`point_read_index_scan.rs` fails when the expansion stops being limited
to the value's axes. Moving it to the arithmetic blocks with a key-scoped
stored filter is a measured follow-up (plan §6, D12); a parameter-scoped
set (`axis_position_stored_in_use(pd)`) is not an option there, it made
point reads 2.5 to 19 times slower.

**Guards.**
[`crates/nodal-db-core/tests/axis_render_scaling.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/axis_render_scaling.rs)
(work counters of each read before and after adding 200 x 2 000-position
axes of each sort: unused parametric, unused stored, parametric in use;
no read plan contains an UNNEST over `axis`) and
[`crates/nodal-db-core/tests/axis_render_equality.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/axis_render_equality.rs)
(the rendering, bit for bit, against the pre-change text frozen in
`tests/fixtures/axis_render_oracle.sql`; it also covers the two copies
of the stored-position legs, in `axis_position_stored` and
`axis_position_stored_in_use`, which must not drift apart). The perf
gate's `synth_axes` metrics measure the same reads on a file with 500
axes of each sort.
