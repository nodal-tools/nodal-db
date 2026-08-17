# Array-tier reconstruction (normative)

Status: **normative**, written 2026-08-17. Deliverable 2 of the conformance
suite (`specs/rust_implementation_plan.md` Part 5). Companion data:
`conformance/reconstruction_fixtures/*.json`; runner
`crates/spinal-db-core/tests/reconstruction_fixtures.rs`; Rust implementation
`crates/spinal-db-core/src/reconstruct.rs`.

This document pins the mapping between the **array tier's flat payload** and the
**logical long form** (index-tuple → value). It is the inverse of what the
canonicalization pipeline's tier step (`crate::tier`, VMP Part 3) produced, and
it MUST agree bit-for-bit with the SQL read view that performs the same
expansion (`schema/080_views_read.sql`, view `value_array_cells`). Two
independent decoders that disagree on layout silently transpose data — the
project's founding two-canonicalizers lesson (`CLAUDE.md` invariant 2) applied
to reads.

## 1. The array tier's storage form

The array (dense) tier stores one value as a single flat payload
(`value_array.values DOUBLE[]`, or `values_int BIGINT[]` for the integer door)
plus up to four axis references `axis1_id..axis4_id` in declared-dimension
order (`schema/050_values.sql`). The payload is **dense**: it holds exactly one
cell for every point of the Cartesian product of the axes, with no NULLs (NULL
elimination is canonicalization step 1). Therefore

```
len(values) = |A1| · |A2| · … · |An|
```

where `Ai` is the axis bound to dimension `i` (1-based) and `|Ai|` is its
length (`axis.length`). `n` is the rank (1..4). The tier stores only lengths and
order — **rank is metadata over flat storage** (VMP Part 3); the shape is not
carried per cell.

## 2. Canonical layout: row-major, declared-dimension order (C order)

> **The flat payload is laid out row-major in declared-dimension order: the
> last (highest-numbered) declared dimension varies fastest, the first varies
> slowest.** This is C / NumPy-default order, *not* Fortran/Julia column-major
> order.

Given dimensions `A1..An` (each with ordered 0-based positions `0..|Ai|-1`), the
cell at index tuple `(p1, p2, …, pn)` lives at flat index

```
k = ( … ((p1·|A2| + p2)·|A3| + p3)·|A4| + … )·|An| + pn      (row-major / C)
  = Σ_{i=1..n}  p_i · stride_i,   where  stride_i = ∏_{j>i} |A_j|   (stride_n = 1)
```

The inverse — the reconstruction a reader performs — decomposes a flat index `k`
(`0 ≤ k < len(values)`) back into its tuple by the row-major strides:

```
p_i = (k / stride_i) mod |A_i|,   stride_i = ∏_{j>i} |A_j|,   stride_n = 1
```

with integer (floor) division. Reconstruction emits one long row per flat index,
in flat order, so cell `k` carries value `values[k]`.

Equivalently, decoding from the last dimension inward (the form the SQL view
uses):

```
p_n     = k mod |A_n|
p_{n-1} = (k / |A_n|) mod |A_{n-1}|
p_{n-2} = (k / (|A_n|·|A_{n-1}|)) mod |A_{n-2}|
…
p_1     =  k / (|A_2|·…·|A_n|)                    ( the leading quotient )
```

### 2.1 Agreement with the SQL read view (normative cross-reference)

`schema/080_views_read.sql` view `value_array_cells` expands the payload with
`unnest(values)` zipped against `unnest(range(len(values)))` as `flat_pos`, and
computes the per-dimension positions as (missing axes coalesced to length 1, so
`n3 = n4 = 1` for a rank-2 value):

```sql
i1_pos = (flat_pos //  (n2 * n3 * n4))       -- p1 = k / stride_1
i2_pos = ((flat_pos // (n3 * n4)) % n2)      -- p2 = (k / stride_2) mod |A2|
i3_pos = ((flat_pos //  n4)       % n3)      -- p3 = (k / stride_3) mod |A3|
i4_pos =  (flat_pos              % n4)       -- p4 =  k             mod |A4|
```

These are exactly the row-major strides of §2 (`stride_1 = n2·n3·n4`,
`stride_2 = n3·n4`, `stride_3 = n4`, `stride_4 = 1`). The Rust `reconstruct`
function MUST produce the identical `(p1..pn) → value` mapping; the fixture
`04_rank2_2x3_sql_crosscheck.json` is expanded through both engines and compared
by the runner.

## 3. The column-major (Julia / Fortran) transpose hazard

Julia, Fortran, MATLAB, R, and NumPy's `order='F'` store multidimensional
arrays **column-major**: the *first* dimension varies fastest. A writer that
flattens a column-major array and hands the raw buffer to the array tier without
transposing would store the transpose of the intended value — and because the
tier records only lengths, nothing downstream can detect it: the payload length
still equals `∏|Ai|`, every axis position is still in range, and the value reads
back as a clean (but transposed) array. The corruption is silent.

**Normative requirement.** The canonical layout is row-major (§2). A
column-major producer (a native Julia/Fortran writer emitting a raw buffer) MUST
transpose its array to row-major **before** ingest — equivalently, permute the
declared dimensions to reverse order, or materialize the payload by iterating
the last declared dimension fastest. The `spinal-db import` Arrow-IPC boundary
carries logical `(entity, dim.., value)` long rows or an explicitly declared
dense payload; it never accepts an unlabelled column-major buffer. No writer
path may store a column-major buffer as-is.

### 3.1 Why a rank-2 non-square example catches the bug

For a square array (`|A1| = |A2|`) a transpose is a same-length permutation and
a length check cannot see it; only the *values* differ. The conformance fixtures
therefore use **non-square** shapes (2×3, 2×3×2). Under a column-major misread of
a 2×3 payload the decoder would compute `p1 = k mod 2`, `p2 = k / 2` — producing
index tuples that a row-major decoder never emits (e.g. flat index 1 → `(1,0)`
column-major vs the correct `(0,1)` row-major). Any decoder that transposes is
caught by an exact-tuple comparison against the golden.

## 4. Worked examples

### 4.1 Rank-2, 2×3 (row-major vs column-major differ)

Axes `A1` (length 2, positions 0,1) and `A2` (length 3, positions 0,1,2).
Strides: `stride_1 = |A2| = 3`, `stride_2 = 1`. Payload
`values = [10, 11, 12, 13, 14, 15]` (flat index 0..5):

| k | `p1 = k/3` | `p2 = k mod 3` | tuple | value | column-major would give |
|---|-----------|----------------|-------|-------|--------------------------|
| 0 | 0 | 0 | (0,0) | 10 | (0,0) |
| 1 | 0 | 1 | (0,1) | 11 | **(1,0)** |
| 2 | 0 | 2 | (0,2) | 12 | **(0,1)** |
| 3 | 1 | 0 | (1,0) | 13 | **(1,1)** |
| 4 | 1 | 1 | (1,1) | 14 | **(0,2)** |
| 5 | 1 | 2 | (1,2) | 15 | (1,2) |

The bolded column-major tuples differ from the canonical row-major ones at every
interior index — a transpose bug is impossible to miss.

### 4.2 Rank-3, 2×3×2

Axes `A1`(2), `A2`(3), `A3`(2). Strides: `stride_1 = |A2|·|A3| = 6`,
`stride_2 = |A3| = 2`, `stride_3 = 1`. Payload
`values = [0,1,2,3,4,5,6,7,8,9,10,11]` (value == flat index for clarity):

| k | `p1 = k/6` | `p2 = (k/2) mod 3` | `p3 = k mod 2` | tuple |
|----|-----------|--------------------|----------------|-------|
| 0  | 0 | 0 | 0 | (0,0,0) |
| 1  | 0 | 0 | 1 | (0,0,1) |
| 2  | 0 | 1 | 0 | (0,1,0) |
| 3  | 0 | 1 | 1 | (0,1,1) |
| 4  | 0 | 2 | 0 | (0,2,0) |
| 5  | 0 | 2 | 1 | (0,2,1) |
| 6  | 1 | 0 | 0 | (1,0,0) |
| 7  | 1 | 0 | 1 | (1,0,1) |
| 8  | 1 | 1 | 0 | (1,1,0) |
| 9  | 1 | 1 | 1 | (1,1,1) |
| 10 | 1 | 2 | 0 | (1,2,0) |
| 11 | 1 | 2 | 1 | (1,2,1) |

The last axis (`A3`, length 2) toggles every step; the middle axis advances
every 2; the first advances every 6 — the defining signature of row-major order.

## 5. Byte-level note

The payload is a DuckDB `DOUBLE[]` (`BIGINT[]` for the integer door); the wire
form is an Arrow `List<Float64>` (`List<Int64>`) whose child buffer holds the
`f64`/`i64` values contiguously in flat order. "Row-major" therefore describes
the child buffer directly: element `k` of the child buffer is cell `k`, decoded
by §2. Each `f64` is IEEE-754 little-endian per the Arrow memory layout; no
value transform happens during reconstruction (canonicalization already ran on
write), so `values[k]` reaches the long row bit-for-bit.
