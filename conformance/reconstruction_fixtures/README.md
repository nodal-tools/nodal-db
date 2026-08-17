# Array-tier reconstruction conformance fixtures

Language-neutral fixtures for the array (dense) tier's **flat `DOUBLE[]` payload
→ logical long table** mapping, as pinned by `spec/reconstruction.md`. Any
implementation (the Rust core, the SQL read view, a future engine skin) MUST
reproduce `expected` exactly. Authoritative runner:
`crates/spinal-db-core/tests/reconstruction_fixtures.rs`; Rust implementation:
`crates/spinal-db-core/src/reconstruct.rs`.

The canonical layout is **row-major in declared-dimension order** (C order: the
last declared dimension varies fastest). The non-square shapes (2×3, 2×3×2)
exist to catch a column-major (Julia/Fortran) transpose bug — see
`spec/reconstruction.md` §3.

## Fixture schema

Each `*.json` file is one object:

| field | type | meaning |
|-------|------|---------|
| `description` | string | what the fixture exercises |
| `declared_dims` | array | one entry per dimension, declared order: `{ "position": u8 (1-based), "kind": IndexKind }`. Metadata for readers; the mapping depends only on axis lengths and order. |
| `axes` | array | one entry per dimension, declared order: `{ "length": N, "positions": [..] }`. `positions` lists the axis's ordered coordinate labels (documentation only; reconstruction uses `length`). |
| `values` | array of number | the flat `DOUBLE[]` payload, row-major; `len(values) == ∏ axes[i].length`. |
| `expected` | array | the golden long rows: `{ "index": [p1..pn], "value": x }`, one per cell, in flat (row-major) order. |
| `sql_crosscheck` | bool (default `false`) | if `true`, the runner also inserts this payload as a `value_array` row into a fresh schema DB and asserts the DuckDB `value_array_cells` expansion equals `expected` (spec §2.1). |

### `IndexKind`

Matches `spinal_db_core::types::IndexKind`: `Datetime`, `Integer`, `Label`,
`Period`, `TimePattern`. The reconstruction mapping is kind-agnostic (positions
are 0-based integers into the axis); the kind is carried only so a fixture reads
as a concrete parameter shape.

## Fixtures

| file | shape | catches |
|------|-------|---------|
| `01_rank1_sanity.json` | rank-1, length 4 | identity mapping |
| `02_rank2_2x3.json` | rank-2, 2×3 | row/column-major transpose (non-square) |
| `03_rank3_2x3x2.json` | rank-3, 2×3×2 | row/column-major transpose (non-square) |
| `04_rank2_2x3_sql_crosscheck.json` | rank-2, 2×3 | Rust ↔ SQL `value_array_cells` agreement |
