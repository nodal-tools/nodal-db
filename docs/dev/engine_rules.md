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
- how database files are created, rebuilt or compacted;
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

**Guard.**
[`crates/nodal-db-core/tests/array_id_index_scan.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/array_id_index_scan.rs)
(checks the plans of the production statements with `EXPLAIN ANALYZE`).

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
