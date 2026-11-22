# Canonical encoding and content hash

This page is the normative specification of the canonical byte encoding
and the 64-bit content hash of axis objects and values. It pins them
precisely enough that **two independent implementations produce
byte-identical output and therefore bit-identical hashes.** nodal-db has
two such implementations, and they are checked against each other: the
Rust core
([`crates/nodal-db-core/src/encoding.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/src/encoding.rs))
and an independent Python oracle
([`conformance/oracle/`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/oracle)),
which shares no code with the core. The machine-checkable form of this
contract is the conformance vectors
([`conformance/encoding_vectors/`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/encoding_vectors),
[`conformance/value_vectors/`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/value_vectors),
[`conformance/array_hash_vectors/`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/array_hash_vectors)).

Why this matters: every write goes through one canonicalization pipeline,
and two canonicalizers that drift apart silently split or merge data. This
page is the definition both sides are held to.

The key words **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, and **MAY** are
used as in RFC 2119.

The tables the encodings describe are defined in
[`schema/040_axes.sql`](https://github.com/nodal-tools/nodal-db/blob/main/schema/040_axes.sql)
(the axis and its child tables) and
[`schema/050_values.sql`](https://github.com/nodal-tools/nodal-db/blob/main/schema/050_values.sql)
(the value tiers).

---

## 1. Scope and layering

1.1. This spec defines two related but distinct canonical byte encodings:

- the **axis encoding** (§4–§7), whose hash is stored in `axis.content_hash`
  and drives content-verified axis dedup (two values that use the same
  axis share one stored axis object);
- the **value encoding** (§9), the canonical byte form of a single value,
  used for cross-writer equality, history diffs, the conformance
  vectors, and any future value-level content hash.

The stored array payload hash (§13) is not a value-level hash: it covers
one column's numbers only, as a storage accelerator of the unchanged-write
check. §9 stays reserved for a value-level hash.

1.2. Both encodings are pure, deterministic functions of **canonicalized
content only** (after the canonicalization pipeline: NULL elimination,
axis derivation, sort, tier decision). The encoder MUST be fed canonical
content; it MUST NOT re-derive the tier or the axis form. Two writers that
canonicalize the same logical data to the same content MUST produce
identical bytes.

1.3. The hash is an **accelerator, never the truth**. Equality of two axes
(or values) is decided by **byte comparison of the canonical encoding**,
not by hash equality. `axis.content_hash` is deliberately indexed, not
`UNIQUE`; a hash collision costs one extra comparison and never corrupts
dedup. Implementations MUST NOT treat a hash match as proof of equality.

---

## 2. The content hash

2.1. The hash function is **XXH3-64 with seed 0**. It is computed over the
complete canonical byte string (§4 for axes, §9 for values) as a single
contiguous input.

2.2. XXH3-64 returns a 64-bit result. That result MUST be interpreted as an
**unsigned 64-bit integer** and stored directly, by numeric value, into a
`content_hash` column (see §2.5 for engines without an unsigned type). No
byte reinterpretation, sign flip, or truncation is applied: the number the
algorithm returns is the number stored.

2.3. **Endianness of the stored value.** The 64-bit hash is a *numeric*
value, not a byte layout; endianness applies only to the encoding of the
*input* byte string (§3.1), never to the hash result itself. Two
implementations MUST agree on the same `u64` integer; how the storage
engine physically persists it on disk is invisible at the SQL value level
and out of scope here. Conformance vectors record the hash as a decimal
`u64`.

2.4. The hash function and seed are effectively a **one-way door**:
changing either rehashes every axis in every database. Any change is a
breaking format revision, not a routine edit. The same holds for the
stored array payload hash (§13): changing its definition makes every
stored `value_array.content_hash` stale (extra versions, never lost
writes).

2.5. **Stored column type.** The stored hashes (`axis.content_hash`,
`value_array.content_hash`, `value_array_hist.content_hash`) are
`UBIGINT` in DuckDB. An engine without an unsigned 64-bit integer type
stores the same 64 bits as a signed `BIGINT`, in two's complement: a value
`v >= 2^63` is stored as `v - 2^64`. Hashes are compared for equality
only; they are never ordered or range-filtered, so the signed and unsigned
representations are interchangeable. Conformance vectors stay decimal
`u64`. The hashes are accelerators that can always be recomputed from the
stored content; they are never identities.

---

## 3. Primitive byte encodings

All composite encodings are built from these primitives. Unless a field's
spec says otherwise, these rules are absolute.

### 3.1. Integers

3.1.1. All integer fields are encoded as **fixed-width, little-endian,
two's-complement** for signed types and **fixed-width little-endian** for
unsigned types. The width is stated explicitly at every use site; there is
no variable-length integer encoding anywhere in this spec.

3.1.2. The widths used by this spec:

- `u8` (1 byte): the form tag (§5.2); property ids and the property-count
  prefix (§6); the value tier tag, the rank byte, component-slot ids and the
  component-count prefix (§9); `bool` and presence bytes;
- `i16` (2 bytes): time-pattern `range_start`, `range_end`;
- `i32` (4 bytes): **signed value fields** whose column is `INTEGER`:
  `axis.length` (§5.3), per-position `ordinal` (§7.5, §7.7),
  `p_step_months` (§3.5), and long-tier cell positions (§9.3). These are
  values, not counts, and mirror their signed `INTEGER` columns exactly;
- `u32` (4 bytes): **all length and count prefixes, everywhere they
  appear**: the string byte-length prefix (§3.4), the time-pattern
  `clause_count` and `term_count` (§7.7), the array element-count prefix
  (§9.5), and the long-tier cell count (§9.3). A length or count is never
  negative, so every such prefix is unsigned;
- `i64` (8 bytes): timestamps (µs, §3.2), integer axis coordinates,
  duration micros, the value key fields, integer measures and integer
  array payload elements;
- `u64` (8 bytes): axis content hashes inside the value shape block (§9.3).

A `u32` prefix (up to about 4.29 billion) is sufficient: an axis or array
longer than that is not a feasible object.

### 3.2. Timestamps

3.2.1. Every `TIMESTAMP` is naive: all stored timestamps are naive, and
calendar and timezone are expressed as axis properties, never asserted. A
timestamp is encoded as its value in **microseconds since the Unix epoch
`1970-01-01T00:00:00`**, as a signed `i64` little-endian (§3.1). DuckDB
`TIMESTAMP` has microsecond precision, so this is exact and lossless.

3.2.2. No timezone offset, calendar conversion, or normalization is applied
to the timestamp value itself. `calendar` and `timezone` participate in the
hash only as separate axis properties (§6).

### 3.3. Doubles (`DOUBLE` / IEEE 754 binary64)

3.3.1. A `DOUBLE` is encoded as its **8 IEEE-754 binary64 bytes,
little-endian**, after the following canonicalization:

- **Negative zero is normalized to positive zero**: any bit pattern equal
  to `-0.0` MUST be encoded as `+0.0` (`0x0000000000000000`). This mirrors
  the writer-side `-0.0 → +0.0` rule for `axis_float.coord` and
  `years_presented`.
- **NaN is split by role (see §3.3.3):**
    - For **axis** floats (`p_start_dbl`, `p_step_dbl`, `axis_float.coord`,
      `years_presented`) NaN is **forbidden**: it MUST be rejected at the
      writer boundary before encoding (the DDL enforces `NOT isnan(...)`;
      DuckDB's total order makes `x = x` true for NaN, so `isnan()` is the
      only correct predicate). The encoder MUST NOT emit bytes for a NaN
      axis float; encountering one is an error, not an encodable
      coordinate. Axis coordinates must be orderable, and NaN has no
      ordering.
    - For **value** floats (`value_scalar.v_float`, `value_long.v_float`,
      `value_array.values`) NaN is a **legal stored value**, canonicalized
      to the **single quiet-NaN bit pattern `0x7ff8000000000000`**
      (positive quiet NaN, zero payload). Every incoming NaN, whatever its
      sign, quiet or payload bits, MUST encode as these same 8 bytes,
      exactly analogous to `-0.0 → +0.0`.
- All other values (including the two infinities and subnormals) are
  encoded verbatim as their binary64 bytes.

3.3.2. There is exactly **one** canonical bit pattern per encodable value:
positive zero for both zeros; for *value* floats the one quiet NaN
`0x7ff8000000000000` for every NaN; for *axis* floats no NaN at all. Byte
equality therefore tracks numeric equality for every representable
coordinate or value, with one deliberate exception: collapsing every NaN
bit pattern to one is a many-to-one map, so byte equality of two values
containing NaN does not track IEEE `==` (`NaN != NaN`). This is
intentional and safe because (a) it tracks DuckDB's **total order**, under
which `NaN = NaN` is true and every NaN collates equal, so the canonical
form is exactly the equality the engine itself uses; and (b) there is **no
cross-key value dedup** (the value encoding folds in the key and shape,
§9.6, and value equality is used only for history diffs and any future
value-level hash, never to merge two distinct values), so a
one-canonical-NaN rule cannot cause a wrong merge. The exception does not
apply to axes, where NaN cannot arise.

3.3.3. **Role split: axes reject NaN, values canonicalize it.** NaN
handling is deliberately asymmetric by role:

- **Axis floats** (`p_start_dbl`, `p_step_dbl`, `axis_float.coord`,
  `years_presented`): NaN is **rejected**. The DDL enforces
  `NOT isnan(...)` and the writer applies `-0.0 → +0.0`, so the invariant is
  enforced twice. An axis coordinate must be orderable; NaN is not.
- **Value floats** (`v_float`, `value_array.values`): NaN is a **legal
  stored value**, canonicalized to `0x7ff8000000000000` at the writer
  boundary before encoding, and stored as that same quiet NaN (DuckDB
  preserves NaN through `DOUBLE`, `DOUBLE[]`, and Arrow export). There is
  **no CHECK** on the value float columns (DuckDB cannot cheaply express
  `NOT isnan` over a `DOUBLE[]`), so both the `-0.0 → +0.0` normalization
  and the NaN canonicalization are enforced **only at the writer
  boundary** for values. A value written by bypassing the core could store
  a non-canonical `-0.0` or NaN bit pattern that no CHECK would catch.
  This asymmetry is the point: a NaN cell is **present, not absent**
  (absence is NULL or omission), so a NaN-bearing dense grid stays a
  complete Cartesian grid and keeps the efficient **array tier** instead of
  being rejected or demoted to the long tier.

### 3.4. Strings

3.4.1. An identifier string (label, tag, name; string *values* are raw,
§3.4.4) is encoded as **UTF-8 in Unicode Normalization Form C (NFC)**,
prefixed by its **byte length** (after NFC normalization) as a `u32`
little-endian (§3.1), followed by the NFC byte sequence. The prefix is the
byte length, not the code-point or grapheme count.

3.4.1.1. **NFC is pinned to Unicode 16.0.0.** NFC's decomposition and
composition tables evolve between Unicode releases, so "NFC" without a
version is under-specified: two implementations linking different
normalization libraries could normalize the same input differently and
split a dedup. The normalization form is therefore pinned to **Unicode
16.0.0**, and the conformance suite vendors a frozen normalization table
([`conformance/unicode/nfc_table_16.0.0.txt`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/unicode/nfc_table_16.0.0.txt),
generated by
[`gen_nfc_table.py`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/unicode/gen_nfc_table.py)
from the Unicode Character Database). The Rust core normalizes with a
pinned `unicode-normalization` crate that a guard test checks against this
table for every code point; the Python oracle normalizes with the table
itself
([`nfc16.py`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/oracle/nfc16.py)).
Neither depends on whatever Unicode version a platform library happens to
ship, and SQL never normalizes (see
[Engine rules §10](engine_rules.md#10-identifiers-reach-sql-in-nfc-sql-never-normalizes)).
Bumping the pinned Unicode version is a rehash-class change (§2.4), not a
routine dependency update.

3.4.2. NFC normalization MUST be applied to every encoded **identifier**
string: axis labels (`axis_label.label`), unit tags
(`axis_time_pattern.unit`), the `calendar`, `timezone` and `crs` property
strings (§6), the axis kind tag (§5.1) and the `value_kind` tag (§9).
Applying one normalization form uniformly removes the "same text, two byte
sequences" dedup hazard: a non-normalized `calendar` or `crs` tag would
otherwise split axes. String *values* are the one exception (§3.4.4).

3.4.3. **Case is significant.** NFC does **not** case-fold. The
`calendar`, `timezone`, `crs` (§6) and `unit` (§7.7) tags are compared and
hashed **case-sensitively**: `noleap` and `NoLeap`, `EPSG:4326` and
`epsg:4326`, `Europe/Helsinki` and `europe/helsinki` are **distinct** and
produce distinct axes. This is deliberate: case-folding rules are
themselves locale- and version-dependent and would reintroduce the
ambiguity NFC removes. Importers SHOULD canonicalize tag case **upstream**
(for example, CRS authority codes to their registered casing, IANA
timezone names to their canonical form) before handing data to the
writer, so a mere casing difference does not split axes.

3.4.4. **String values are encoded raw.** A string *value* (`v_str`,
scalar or long tier, §9.5) is encoded as its UTF-8 bytes **exactly as
written**, prefixed by their byte length as a `u32` LE, and is **never**
NFC-normalized. String values are data: storage keeps them byte for byte
and compares them by bytes (an NFC-equal but byte-different string is a
change), so the value encoding must treat two strings as equal exactly
when storage does. The rest of §3.4 (NFC, the Unicode pin) applies to
identifier strings only (§3.4.2). Vectors `value_vectors/v13` (scalar)
and `v14` (long-tier cells) pin this.

### 3.5. Durations (the `(months, micros)` pair)

3.5.1. `INTERVAL` appears nowhere. A duration, whether a scalar `duration`
value (`v_dur_months`, `v_dur_micros`) or a parametric datetime axis step
(`p_step_months`, `p_step_micros`), is the exact pair
`(months: i32, micros: i64)`. It is encoded as `months` (`i32` LE)
followed by `micros` (`i64` LE).

3.5.2. **Normalization rule.** The pair is stored and encoded exactly as
its two component fields; there is **no cross-normalization** between the
two components. Specifically:

- **There is no day unit.** A day is always `86_400_000_000` µs carried in
  the `micros` field; `months` carries only calendar months. The
  `1 day` vs `24 hours` ambiguity of `INTERVAL` is therefore
  **unrepresentable by construction**: both denote the same `micros` value
  and hash identically.
- A **month-bearing** duration (`months != 0`) is a visibly distinct class
  from a micros-only duration and never collapses into one: months are not
  convertible to micros (a month has no fixed length in µs), so
  `(months=1, micros=0)` and `(months=0, micros=N)` are always distinct
  encodings. The writer MUST NOT fold months into micros or vice versa.

3.5.3. **The pair is always fully materialized.** Both components are
always encoded; an **absent** component is the integer **`0`**, never a
NULL or an omitted field. There is therefore no presence flag on a
duration and no way for `(NULL, N)` and `(0, N)` to both occur: `(0, N)`
is the one canonical encoding of "micros only", and `(M, 0)` the one
canonical encoding of "months only". This makes the duration encoding
fixed-width (`i32` + `i64` = 12 bytes) and unambiguous, and it is why the
duration is a *single* logical component (§9.5) even though it occupies
two physical fields.

---

## 4. Axis canonical encoding: overall structure

4.1. The canonical byte string of an axis is the concatenation, **in this
exact order**, of:

1. **Kind tag** (§5.1)
2. **Form tag** (§5.2)
3. **Length** (§5.3)
4. **Property block** (§6): a `u8` count of present properties, then
   `calendar`, `timezone`, `crs`, and any future expressed property,
   present-only, in ascending property-id order
5. **Kind body** (§7): parametric fields *or* materialized child rows,
   determined by kind and form

4.2. The hash of the axis (§2) is XXH3-64 with seed 0 over this complete
string.

4.3. This structure is fixed. New *properties* extend only the property
block (§6) under the rehash-stable rule (§8); new *kinds* add a new kind
tag and a new kind body (§7) without disturbing existing kinds.

---

## 5. Axis header fields

### 5.1. Kind tag

5.1.1. The `axis.kind` value is encoded as a **string** (§3.4): its exact
lowercase ASCII tag, NFC-normalized (a no-op for ASCII),
`u32`-length-prefixed. The exact spellings are pinned in this table; an
implementation MUST emit these byte for byte (case-sensitive, §3.4.3):

| `axis.kind` tag | bytes | body encoding |
|-----------------|-------|---------------|
| `datetime`      | `datetime` (8) | §7.2 |
| `period`        | `period` (6)   | §7.6 |
| `label`         | `label` (5)    | §7.5 |
| `integer`       | `integer` (7)  | §7.3 |
| `time_pattern`  | `time_pattern` (12) | §7.7 |
| `float`         | `float` (5)    | §7.4 |

The kind is a string tag rather than an integer discriminant because
`axis.kind` is an open, additive vocabulary (no CHECK): a string tag needs
no central integer registry, and a future kind is self-describing, at a
cost of a few bytes per axis.

5.1.2. **Pitfall: `datetime` (axis kind) vs `date_time` (value kind).**
The *axis* kind tag is `datetime` (no underscore, §5.1.1); the *value*
kind tag for a datetime measure is `date_time` (**with** underscore, §9.3).
These are two different tag spaces, `axis.kind` and `value_kind`, and
their datetime spellings deliberately do not coincide. An implementation
MUST NOT normalize one to the other or share a single constant between
them; emitting `date_time` for an axis (or `datetime` for a value) is a
silent hash divergence. The conformance vectors include both spellings to
catch it.

### 5.2. Form tag

5.2.1. A single byte discriminating the storage form:

- `0x00`: **parametric** (regular grid stored as `(start, step, length)`);
- `0x01`: **materialized** (irregular grid stored in a child table).

5.2.2. The form tag MUST reflect the **canonical** form actually stored
under the parametric-first rule (§7.1). It is emitted for **every** kind,
including kinds that have only one form (`label`, `period`, `time_pattern`
are always `0x01`; see §7). It exists so a parametric encoding can never
byte-collide with a materialized one of the same kind, independent of any
coincidence in the numeric payload (§11.3).

### 5.3. Length

5.3.1. `axis.length` (number of positions, `>= 1`) as an `i32`
little-endian (§3.1). This is a signed **value** field mirroring the
`INTEGER` `axis.length` column, not a length *prefix* (those are `u32`,
§3.1.2). Length is encoded explicitly even though it is derivable from a
materialized child-row count, so parametric and materialized forms carry
it uniformly and a length mismatch is a byte mismatch.

---

## 6. Property block (calendar / timezone / crs / future)

6.1. `calendar`, `timezone`, and `crs` are the three **expressed, never
asserted** axis properties. They **MUST participate in the hash**, so that
axes differing only in a property never dedup into one object with one
writer's tag silently winning. A `noleap` representative axis and a
real-calendar axis with identical nominal timestamps are **distinct**
axes.

6.2. Each property has a stable, **append-only property id**:

| id | property | column | applies to |
|----|----------|--------|------------|
| 1 | calendar | `axis.calendar` | any kind (chiefly `datetime`/`period`) |
| 2 | timezone | `axis.timezone` | any kind (chiefly `datetime`/`period`) |
| 3 | crs      | `axis.crs`      | any kind (chiefly `float`), NULL elsewhere |

6.3. **Encoding: count prefix, then present-only, ascending id.** The
property block is:

- a **`u8` property-count prefix** `k` = the number of **non-NULL**
  properties present, then
- `k` entries, in **ascending property-id order**, one per non-NULL
  property. An entry is:
    - the property id as a `u8`, then
    - the property string value encoded per §3.4 (NFC,
      `u32`-length-prefixed, case-sensitive per §3.4.3).

A **NULL (absent) property contributes nothing**: it is neither counted
nor emitted (not even its id byte). An empty (all-NULL) property block is
therefore the single byte `0x00`.

6.3.1. **Why the count prefix is required (self-delimitation).** The
property strings are variable-length, so without a count the property
block's boundary with the following kind body (§7) is not pinned: for
kinds whose body is itself a variable-length string sequence (`label`,
`period`, `time_pattern`), a decoder could not tell where the last
property ends and the first body element begins, and two logically
different axes could produce the same byte string, a silent
**false merge**. The `u8` count makes the block self-delimiting: the
decoder reads exactly `k` entries and the body starts immediately after.
(`u8` suffices; the property registry grows by single digits.)

6.3.2. **The count prefix preserves §8 rehash stability.** The count is
the number of *present* (non-NULL) properties only. Adding a new nullable
property to the schema does **not** change the count on any existing axis
(the new property is NULL there, so it is not counted and emits nothing):
the count byte, and every other byte, is unchanged. The count rises only
when a property is genuinely present, and an axis that carries a property
a second axis lacks is already a *different* axis (§6.1).

6.4. Because each present entry is tagged with its property id and
entries are emitted in ascending id order behind the count, the block is
unambiguous: "calendar present, timezone absent" and "calendar absent,
timezone present" produce different bytes (different id tags), so they
can never collide.

---

## 7. Axis kind bodies

The kind body follows the property block. Its shape is chosen by
`(kind, form)`. For all materialized bodies, child rows are encoded in
**ascending `pos` order** (`pos` is dense `0..length-1`); `pos` itself is
**not** encoded, because the canonical axis is always dense and 0-based,
so it is implicit in the sequence position.

### 7.1. Parametric-first rule (normative precondition)

7.1.1. A `datetime`, `integer`, or `float` axis whose distinct coordinates
form a **regular progression** MUST be stored and encoded in parametric
form (`form = 0x00`); it MUST NOT be materialized. This is what makes a
regular grid's hash unique: the materialization never happens. Uniqueness
of a regular grid across writers rests entirely on every writer obeying
this rule (§11).

7.1.2. Regularity for `float` axes is decided by the bit-exact test in
§10. Regularity for `datetime` and `integer` axes is decided by the
**exact reconstruction test** below (§7.1.2.1–§7.1.2.3), framed
identically to the float test (§10.3): an axis is regular **iff** the
pinned reconstruction, seeded from `c[0]` and one step, reproduces
**every** materialized coordinate exactly; otherwise it MUST be
materialized. The datetime and integer reconstructions use exact integer
and µs arithmetic (no floating point), but the framing (reproduce every
coordinate or materialize) is the same, and it is what makes the store
decision a deterministic function that the SQL view and the Rust writer
both compute.

7.1.2.1. **`datetime` reconstruction, matching the SQL view exactly.** A
`datetime` axis is regular, and therefore MUST be stored parametric,
**iff** there is a step `(p_step_months, p_step_micros)` such that, with
`p_start_ts = c[0]`,

```
coord(pos) = p_start_ts  ⊕months  (p_step_months · pos)  ⊕micros  (p_step_micros · pos)
```

reproduces **every** materialized coordinate `c[pos]` **µs-exactly**
(§3.2) for `pos = 0 .. length-1`. This is precisely the expression the
`axis_position` view evaluates,
`p_start_ts + to_months(p_step_months*pos) + to_microseconds(p_step_micros*pos)`
([`schema/080_views_read.sql`](https://github.com/nodal-tools/nodal-db/blob/main/schema/080_views_read.sql)),
so the SQL reconstruction and the Rust store decision agree by
construction. If any position diverges by even one microsecond, the axis
is **not** regular and MUST be materialized (`form = 0x01`).

**Pinned semantics (binding):**

- **Single month multiply, applied once, not iterated steps.** The month
  offset is the exact integer product `p_step_months · pos`, applied to
  `p_start_ts` as a **single** month interval. It is **not** `pos`
  successive additions of a one-step interval. The two diverge at
  month-length clamp boundaries: from `2001-01-31`, a single `+2 months`
  clamps against *March* and lands on `2001-03-31`, whereas iterating
  `+1 month` twice lands on `2001-03-28` (February clamps to the 28th, then
  March keeps the 28th). Only the single-multiply form matches the view;
  the iterated form is forbidden.
- **Evaluation order is pinned:** apply the **month** interval to
  `p_start_ts` **first** (with the clamp rule below), **then** add the
  microsecond offset `p_step_micros · pos`. This mirrors the view's
  left-to-right `p_start_ts + to_months(…) + to_microseconds(…)`.
- **Month clamp rule (DuckDB interval-add / `to_months` semantics).**
  Adding `k` months lands on the same day of the month `k` months later;
  if that day does not exist in the target month, it **clamps to the last
  day of the target month**. Examples: `2001-01-31 ⊕ 1 month → 2001-02-28`,
  `2001-01-31 ⊕ 2 months → 2001-03-31`, `⊕ 3 months → 2001-04-30`.
  Time-of-day and sub-day components are carried through the month add
  unchanged; the microsecond offset is added afterwards.
- **The `micros` offset is exact.** `p_step_micros · pos` is an exact `i64`
  product added as microseconds; no rounding, no calendar interaction.

7.1.2.2. **Required conformance fixtures: month-clamp behaviour.** Because
the single-multiply-with-clamp semantics are subtle, the conformance
vectors MUST pin both sides of the divergence:

- **(a) Reproduce case → PARAMETRIC.** Coordinates
  `[2001-01-31, 2001-02-28, 2001-03-31, 2001-04-30]` (each at 00:00:00)
  MUST be stored **parametric** with `p_start_ts = 2001-01-31`,
  `p_step = (1 month, 0 µs)`: the single-multiply reconstruction
  reproduces all four positions exactly. An implementation that wrongly
  uses *iterated* stepping would compute position 2 as `2001-03-28`, fail
  to reproduce, and mis-decide this axis as irregular.
- **(b) Divergence case → MATERIALIZE.** Coordinates
  `[2001-01-31, 2001-02-28, 2001-03-28, 2001-04-28]` (the *iterated*
  `+1 month` sequence) MUST be **materialized** (`form = 0x01`) and MUST
  NOT be collapsed to parametric `p_start_ts = 2001-01-31`,
  `p_step = 1 month`: the pinned single-multiply reconstruction gives
  position 2 = `2001-03-31 ≠ 2001-03-28`, so no single
  `(start, months, micros)` reproduces it. An implementation that wrongly
  reads regularity as "every successive difference is the same `+1 month`
  step" would collapse this to a parametric axis whose read-back disagrees
  with storage.

These are vectors `03_datetime_monthclamp_parametric.json` and
`04_datetime_monthclamp_materialize.json`.

7.1.2.3. **`integer` reconstruction.** Same reproduce-or-materialize
framing. An `integer` axis is regular, and MUST be stored parametric,
**iff** with `p_start_int = c[0]` there is an integer step `p_step_int`
such that

```
coord(pos) = p_start_int + p_step_int · pos        (exact i64 arithmetic)
```

reproduces **every** `c[pos]` exactly for `pos = 0 .. length-1` (matching
the view's `p_start_int + p_step_int * pos`); otherwise it MUST be
materialized. Integer arithmetic has no clamp or rounding subtlety, so the
reproduce test reduces to the constant-successive-difference test; it is
stated as a reproduction to keep all three parametric kinds under one
framing. The writer MUST use checked `i64` arithmetic; a step that would
overflow `i64` across the axis makes the axis non-parametric
(materialize).

### 7.2. `datetime`

- **Parametric** (`0x00`): `p_start_ts` (timestamp → `i64` µs, §3.2),
  then the step as the duration pair `p_step_months` (`i32`),
  `p_step_micros` (`i64`) (§3.5).
- **Materialized** (`0x01`): the `axis_datetime.ts` sequence, each a
  timestamp `i64` µs (§3.2), in `pos` order. (Length is the header's; it
  is not repeated.)

### 7.3. `integer`

- **Parametric** (`0x00`): `p_start_int` (`i64`), then `p_step_int`
  (`i64`).
- **Materialized** (`0x01`): the `axis_integer.num` sequence, each `i64`,
  in `pos` order.

### 7.4. `float`

- **Parametric** (`0x00`): `p_start_dbl` (canonical DOUBLE, §3.3), then
  `p_step_dbl` (canonical DOUBLE). `p_step_dbl > 0` is enforced by the DDL
  (strictly increasing).
- **Materialized** (`0x01`): the `axis_float.coord` sequence, each a
  canonical DOUBLE (§3.3), in `pos` order.
- See §10 and §11.3 for the store decision and the non-byte-identity of
  the two forms.

### 7.5. `label` (always `form = 0x01`)

7.5.1. For each position in `pos` order, encode:

- `ordinal` (`i32` LE): the stored semantic order (`axis_label.ordinal`),
  which is **scoped to the axis**;
- `label` string (§3.4, NFC, `u32`-length-prefixed).

7.5.2. Both `pos` order and the explicit `ordinal` are encoded: two axes
with the same labels in the same physical slots but different semantic
ordinals are distinct axes and MUST hash differently.

### 7.6. `period` (always `form = 0x01`)

7.6.1. For each position in `pos` order, encode:

- `start_ts` (`i64` µs, §3.2);
- `end_ts` (`i64` µs, §3.2): stored explicitly, never derived;
- `years_presented`: an **optional** DOUBLE. Because it is nullable per row
  (`axis_period.years_presented`), encode a **presence byte** `u8`
  (`0x00` = absent, `0x01` = present); when present, follow it with the
  canonical DOUBLE (§3.3).

7.6.2. `years_presented` is a **per-position** optional, not an axis-level
property, which is why it uses a per-row presence byte rather than the §6
property block. The DDL permits per-row NULLs (no all-or-nothing
constraint), so an axis-level flag would misrepresent a legitimately mixed
axis.

### 7.7. `time_pattern` (always `form = 0x01`)

7.7.1. **Model.** A time-pattern axis is a sequence of positions. Each
position is a **union of clauses**; each clause is an **intersection**
(AND) of one or more **terms**; each term is a unit and an inclusive range
(`unit`, `range_start`, `range_end`). The units are `Y|M|D|WD|h|m|s`
(year, month, day, weekday, hour, minute, second; ranges are checked per
unit by the DDL). A clause has at least one term.

7.7.2. **Storage.** One `axis_time_pattern` row per term, primary key
`(axis_id, pos, term_no)`, with `clause_no INTEGER NOT NULL
CHECK (clause_no >= 0)` naming the clause the term belongs to:

- `term_no` runs `1..n` over the whole position, in order;
- clauses are numbered `0..k-1` contiguously in `term_no` order: a
  clause's terms are consecutive in `term_no`, so `clause_no` starts at 0
  on `term_no = 1` and is non-decreasing, increasing by exactly 1 at each
  clause boundary.

7.7.3. **Encoding.** For each position in `pos` order, encode:

- `ordinal` (`i32` LE): the explicit per-position ordinal (patterns are
  set-like and have no natural order);
- `clause_count` (`u32` LE): the number of clauses at this position;
- then, for each clause in **ascending `clause_no` order**:
    - `term_count` (`u32` LE): the number of terms in this clause
      (`>= 1`);
    - then, for each term of the clause in **ascending `term_no` order**:
        - `unit` string (§3.4, NFC, `u32`-length-prefixed, case-sensitive
          §3.4.3);
        - `range_start` (`i16` LE);
        - `range_end` (`i16` LE).

Clauses and terms MUST be emitted in these orders, so a position has one
canonical serialization.

7.7.4. **Ordinal is constant per `pos` (a writer invariant).** The schema
stores `ordinal` on every term row but does **not** constrain it to be
equal across the terms of one `pos`; the read view takes
`any_value(ordinal)`. The canonical encoder encodes **one** `ordinal` per
position, so the writer MUST write the **same** `ordinal` for every
`term_no` at a given `(axis_id, pos)`: `ordinal` is a property of the
*position*, not the term. The encoder reads it from **`term_no = 1`**. A
writer that emitted divergent per-term ordinals for one position would
make the canonical bytes depend on which term the encoder happened to
read; this invariant forecloses that.

7.7.5. **Rendering.** Read views (and `idxN_text`) render a position as
text: the terms of a clause are joined with `;`, and the clauses with
`,` (`,` = union, `;` = intersection, the syntax Spine Toolbox uses for
time patterns). Each term renders as `<unit><range_start>-<range_end>`.
For example, `M1-4;WD1-5,M9-10` is (months 1–4 AND weekdays 1–5) OR
months 9–10.

7.7.6. **Positions without intersections.** A union of single ranges is
one clause per term (a single-range position is clause 0) and renders as
a plain comma list (`M1-3,h9-17`), the form every time pattern had before
clauses were added (schema versions before 7).

7.7.7. Time-pattern axes have no Arrow import path yet; the encoding and
storage are defined so that adding one is purely additive.

---

## 8. Rehash-stable default rule for future properties

8.1. **A future expressed axis property, when NULL or absent, MUST
contribute zero bytes to the encoding.** Adding a new nullable property to
the `axis` table (and this spec) therefore leaves the canonical bytes, and
thus the hash, of every existing axis **unchanged**. This is how `crs`
was added without rehashing existing axes.

8.2. The mechanism is the present-only, id-tagged property block (§6.3): a
new property is assigned the next append-only property id (4, 5, …); it is
emitted only when non-NULL; on every pre-existing axis it is NULL and
emits nothing. Existing conformance vectors continue to pass without
modification; new vectors are *added* for the new property. The `u8`
property-count prefix (§6.3) does **not** weaken this: the count is over
*present* properties only, so a newly added NULL property leaves the
count, and thus every byte, unchanged (§6.3.2).

8.3. Implementations MUST NOT:

- reserve or emit placeholder bytes for a NULL property;
- reorder property ids (order is ascending by id, fixed);
- fold a new property into the fixed header (§5): the fixed header is
  frozen; all growth is in the property block.

8.4. This rule is the single most important forward-compatibility property
in this document. Getting it wrong (emitting even one byte for an absent
property) silently rehashes the entire corpus on the next schema addition.

---

## 9. Value canonical encoding: component-wise, populated columns only

9.1. **Motivation.** A value's canonical encoding MUST encode **only the
populated component columns**, so that adding a future second value
component column (for example `v_imag` on the scalar and long tiers,
`values_imag` on the array tier) does **not** change the encoding of any
existing value. This is cheap to guarantee from the start and impossible
to retrofit once value hashes are stored.

9.2. No value tier stores a value-level `content_hash` (the array tier's
stored hash is the payload hash of §13, not this encoding). The value
encoding is nonetheless defined, because it governs (a) the value
conformance vectors, (b) cross-writer value equality and history diffs,
and (c) any future value-level content hash.

9.3. **Structure.** The canonical byte string of a single canonicalized
value is the concatenation of:

1. **Key block**: the identity triple, three `i64` little-endian fields in
   this order: `entity_id`, `parameter_definition_id`, `alternative_id`.
   Identical measures under different keys therefore encode differently
   (§9.6).
2. **`value_kind` tag**: a string (§3.4, NFC, `u32`-length-prefixed,
   case-sensitive §3.4.3); the open tag from `value_scalar.value_kind` etc.
   (`'float'|'int'|'bool'|'string'|'date_time'|'duration'|…`). Note the
   `date_time` spelling (underscore) is the **value_kind** tag, distinct
   from the `datetime` **axis kind** tag (§5.1.2). For an array-tier value
   the tag is the array's value kind: `float` for a `values` (`DOUBLE[]`)
   payload, `int` for a `values_int` (`BIGINT[]`) payload. (The core
   writes only float arrays today: an int-valued indexed value goes to the
   long tier. The `int` array encoding is specified so that int arrays can
   be added without changing it.)
3. **Tier tag**: a single `u8` discriminating the physical tier: `0x00`
   **scalar** (rank 0), `0x01` **array** (dense payload), `0x02` **long**.
   It is emitted immediately after the `value_kind` tag and before the
   shape block. This is the value-side analogue of the axis form byte
   (§5.2): it ensures a scalar, an array and a long encoding can never
   byte-collide, independent of any coincidence in the shape or measure
   block. New tiers take new tag values, append-only, without disturbing
   existing ones.
4. **Shape block**: a **`u8` rank** (the number of axes; `0` for a rank-0
   scalar), then that many **axis content hashes** (`u64` little-endian
   each, §2), in declared-dimension order. This binds the value to the
   exact axis objects it indexes. A scalar emits rank `0` and no hashes.
5. **Measure block**, per tier:
    - **scalar**: one **component block** (below) carrying the slot-1
      measure;
    - **array**: one **component block** whose slot-1 bytes are a `u32`
      element-count prefix followed by each element (§9.5), row-major
      flattened in declared-dimension order. Positions are implicit (dense
      Cartesian) and pinned by the axis hashes and row-major order;
    - **long**: a `u32` **cell count**, then per cell, in canonical
      row-major order, the **positions** (`i32` little-endian per axis, in
      declared order) followed by a **component block** of that cell's
      measure.

A **component block** is a **`u8` component-count prefix** `k` = the
number of **populated** component columns, then `k` entries, one per
populated component, in **ascending component-slot order**, present-only
(the same mechanism as the axis property block, §6.3):

- the component-slot id as `u8`, then
- the component's value encoded by its physical type (§9.5).

The count prefix makes the component block self-delimiting for the same
reason as the property block (§6.3.1): the primary measure can itself be
variable-length (`v_str`, or an array payload), so without a count the
boundary after the last component is not pinned. Today `k = 1` for every
value (slot 1 only); it rises to `2` only when a slot-2 component is
actually populated, leaving every existing single-component value's bytes
unchanged: the §8 rehash-stable rule applied to values.

9.4. **Component slots** are append-only:

| slot | component | scalar/long column(s) | array column |
|------|-----------|-----------------------|--------------|
| 1 | primary measure | the single populated `v_*` for `value_kind` | `values` or `values_int` |
| 2 | (reserved, future) | e.g. `v_imag` | e.g. `values_imag` |

Slot 1 (the primary measure) is always present. Slot 2 and later are
emitted **only if populated**; on every existing single-component value
they are NULL and emit nothing, so adding them leaves existing value bytes
and hashes unchanged. NULL and absent are the same thing in canonical
content, which is what makes an absent component contribute nothing.

9.5. **Encoding of the primary measure (slot 1), by physical type:**

- `v_float` → canonical DOUBLE (§3.3, value role: NaN canonicalized);
- `v_int` → `i64` LE (§3.1);
- `v_bool` → one byte, `0x00` false / `0x01` true;
- `v_str` → raw string: its UTF-8 bytes exactly as written,
  `u32`-length-prefixed, **not** NFC-normalized (§3.4.4);
- `v_datetime` → `i64` µs (§3.2);
- duration (`v_dur_months`, `v_dur_micros`) → the pair `(i32, i64)` (§3.5);
  a duration is a single logical component encoded as its two fields;
- **array `values` (`DOUBLE[]`)** → a `u32` element-count prefix, then each
  element as a canonical DOUBLE (§3.3, value role), in **row-major
  flattened, declared-dimension order**;
- **array `values_int` (`BIGINT[]`)** → a `u32` element-count prefix, then
  each element as `i64` LE, in the same flattened order. Exactly one of
  `values` and `values_int` is populated (a CHECK on `value_array`);
  whichever is populated is the slot-1 component. No conformance vector
  covers `values_int` yet, because the core does not write int arrays.

The array element-count prefix is required for injectivity: once a second
component slot exists, the array payload is no longer the last bytes of
the encoding, and without a count the boundary between two
variable-length arrays would not be pinned. It also makes a mismatch
between the axis-length product and the payload length a byte mismatch.

9.6. **Value equality = key + shape + measure.** The value encoding folds
in the identity key (§9.3 step 1) and the position and shape context: the
axis content hashes (§9.3 step 4) and, for the long tier, the per-cell
positions (§9.3 step 5). Two values are equal iff they share key, shape,
and measure. This is the conservative, collision-avoiding definition; the
only thing it gives up is cross-key value dedup, which is not a use case.
The component-wise, populated-columns-only rule (§9.1, §9.4) applies
independently to the measure block, so a future second component slot
never changes an existing value's encoding.

---

## 10. Float parametric store rule: bit-exact coordinate arithmetic

10.1. This section pins the arithmetic that both implementations (the
`axis_position` SQL view and the Rust writer) MUST use, so they agree bit
for bit on (a) reconstructing a parametric float axis and (b) the
reverse: deciding whether a set of coordinates is a regular progression
storable parametrically (§7.1). Disagreement here splits a dedup.

10.2. The coordinate at position `pos` is computed as, and only as:

```
coord(pos) = p_start_dbl + (p_step_dbl * pos_as_f64)
```

with these binding requirements:

- **Operand order is exactly `start + (step * pos)`**: the multiplication
  `step * pos` is evaluated first, then added to `start`. This matches the
  SQL view expression `p_start_dbl + p_step_dbl * pos` (the
  parametric-float branch of
  [`schema/080_views_read.sql`](https://github.com/nodal-tools/nodal-db/blob/main/schema/080_views_read.sql)).
- **No fused multiply-add (FMA).** The multiply and the add are two
  separate IEEE-754 binary64 operations, each independently rounded. An
  implementation MUST NOT contract `start + step*pos` into a single FMA
  (which rounds once and can give a result one ULP off).

    DuckDB does not contract this expression to an FMA. A probe found the
    triple `a = -94910827.80130784` (`0xc196a0e5af348a0b`),
    `b = 3058998.3033553627` (`0x4147569b26d45939`), `c = 2601.0`, for
    which the two-rounding result `a + (b·c)` and the correctly rounded
    `fma(b, c, a)` differ by one ULP (`0x41fd495a34f39da9` vs
    `0x41fd495a34f39da8`). DuckDB returned `0x41fd495a34f39da9`, the
    separate two-rounding result, both for scalar literals and for
    `a + b*pos` over a table column (the `axis_position` shape). This is a
    per-build fact: a future or differently compiled DuckDB could
    contract. The §10.4 and §10.5 fixtures are the standing regression
    guard.

- **Rounding mode is round-to-nearest, ties-to-even** (the IEEE-754
  default) for both the multiply and the add.
- **`pos` → `f64` cast:** `pos` (a non-negative `INTEGER`, `< length`) is
  converted to `f64` exactly (every `INTEGER` value is exactly
  representable in binary64), then used as the multiplicand.

10.3. The **store decision** (is this coordinate set regular?) is: a set
of coordinates `c[0..n-1]` is stored parametrically with
`p_start_dbl = c[0]` and some `p_step_dbl` iff `coord(pos)` under §10.2
reproduces **every** `c[pos]` bit-exactly for `pos = 0..n-1`. If any
position diverges by even one ULP, the axis is **not** regular and MUST be
materialized (`form = 0x01`). This makes the store decision itself a
deterministic function pinned by §10.2.

10.4. **ULP-divergence fixtures are required in the conformance
vectors**, in two complementary forms:

- an "almost regular" coordinate set, reproduced by `start + step*pos` at
  every position except one, which is off by one ULP, asserting the axis
  materializes and does not collapse to parametric
  (`09_float_ulp_materialize.json`); and
- an **FMA-discriminating** fixture with `start ≠ 0` where
  `start + (step*pos)` and `fma(step, pos, start)` differ by one ULP at
  some position (`21_float_fma_divergence_parametric.json`). This is the
  one that catches an FMA-contracting or wrong-operand-order
  implementation: with `start = 0` a fused and a two-rounding evaluation
  coincide (`fma(step, pos, 0) == step*pos`), so a `start = 0` fixture
  does not exercise the no-FMA rule. The fixture pins the bit-exact
  non-FMA result at the divergent position and asserts the two evaluations
  differ, so a reconstruction that used `mul_add` fails loudly.

10.5. **A cross-engine bit-exactness fixture is required.** Agreement
between the SQL `axis_position` view and the Rust writer is probed, not
proved, so the test suite carries a **round-trip fixture across both
engines**
([`crates/nodal-db-core/tests/roundtrip_duckdb.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/tests/roundtrip_duckdb.rs)):
the Rust writer stores parametric axes (float, and datetime axes including
the month-clamp reconstruction of §7.1.2), the fixture reads the
coordinates **back through the `axis_position` SQL view**, and asserts
they are **bit-exact** (µs-exact for datetime, IEEE-bit-exact for float)
against the writer's own reconstruction. It fails loudly if a DuckDB build
begins FMA-contracting (§10.2) or changes month-clamp semantics
(§7.1.2.1).

---

## 11. Uniqueness caveats (normative notes)

11.1. **Needlessly distinct axes.** The same hourly grid stamped with
nominal year 1995 by one importer and 2018 by another yields two axis
objects. This is *correct* (different timestamps, different content,
different bytes) and cheap (regular grids are parametric, a few bytes).
Importers SHOULD prefer a **project-consistent nominal year** so shared
grids actually share. This is guidance, not an encoding rule.

11.2. **Never needlessly merged axes (§6).** Because `calendar`,
`timezone`, `crs` (and every future property) are hash input, axes
differing only in a property never dedup. This is enforced by §6, not
optional.

11.3. **Parametric and materialized float encodings are not
byte-identical.** A parametric float axis encodes
`(p_start_dbl, p_step_dbl)`, two doubles under `form = 0x00`; a
materialized float axis encodes `length` coordinates under
`form = 0x01`. These are different byte strings and hash differently
even when they denote the same coordinate set. Uniqueness of a regular
float grid across all writers therefore rests **entirely** on the
**mandatory parametric-first store rule** (§7.1, §10.3): every writer that
sees a regular float grid MUST store it parametrically, or a duplicate
(materialized) axis with a different hash results. Float regularity is
decided by floating-point arithmetic (§10) rather than exact integer
arithmetic, which makes the store rule the sole guarantee.

11.4. The same non-byte-identity holds for `datetime` and `integer`
parametric vs materialized forms; the same parametric-first rule (§7.1,
exact arithmetic) is the guarantee there. The form byte (§5.2) makes the
two forms structurally non-colliding regardless.

---

## 12. Conformance-vector coverage

The conformance vectors
([`conformance/encoding_vectors/`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/encoding_vectors)
for axes,
[`conformance/value_vectors/`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/value_vectors)
for values) MUST cover, at minimum:

- parametric vs irregular `datetime`, **including the §7.1.2.2 month-clamp
  pair**: the reproduce case `[Jan-31, Feb-28, Mar-31, Apr-30]` asserting
  PARAMETRIC and the divergence case `[Jan-31, Feb-28, Mar-28, Apr-28]`
  asserting MATERIALIZE;
- parametric vs irregular `integer`;
- parametric vs irregular `float`, including the §10.4 ULP-divergence and
  FMA-discriminating fixtures;
- `label` with non-trivial ordinals;
- `period` with and without `years_presented` (a per-row mix, §7.6);
- `time_pattern` with multi-term positions **and a constant per-`pos`
  ordinal** (§7.7.4);
- `calendar`, `timezone` and `crs` variants proving non-dedup (§6, §11.2),
  **including a case-only difference proving case sensitivity** (§3.4.3);
- NFC edge cases against the pinned Unicode 16.0.0 table (§3.4);
- `-0.0` normalization, the **rejected axis NaN** and the **canonicalized
  value NaN** (`0x7ff8000000000000`, §3.3);
- value encodings for each `value_kind` (including the
  `date_time`/`datetime` spelling pair, §5.1.2), month-bearing and
  micros-only durations (§3.5), and array payloads (§9.5).

The §10.5 cross-engine round trip is a Rust integration test rather than a
vector. The rehash stability of a NULL slot-2 component (§9.4, §8) is
pinned by a unit test in `encoding.rs`.

---

## 13. Array payload hash (stored, `value_array.content_hash`)

`array_payload_hash(payload) = XXH3-64(seed 0, B)`. `B` is the
concatenation, in stored (row-major) order, of each payload element's 64
bits as 8 bytes little-endian: a `DOUBLE`'s raw IEEE-754 pattern, a
`BIGINT`'s two's-complement value. Nothing else: no length prefix (the
byte length is part of the XXH3 input), no kind tag, no key, no axes. The
empty payload hashes the empty input. No canonicalization inside the
hash: `-0.0` and every NaN pattern hash as their own bits.

It is a storage accelerator over one column, not a value hash (§9): the
core uses a stored hash that differs from a new payload's hash to prove
the payload changed; a match proves nothing (the bytes decide). `NULL`
means unknown. The stored column type follows §2.5.

Implementations: `encoding::array_payload_hash` and
`array_payload_hash_i64` in
[`encoding.rs`](https://github.com/nodal-tools/nodal-db/blob/main/crates/nodal-db-core/src/encoding.rs)
(Rust), and
[`conformance/oracle/array_hash.py`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/oracle/array_hash.py)
(independent). Vectors:
[`conformance/array_hash_vectors/`](https://github.com/nodal-tools/nodal-db/blob/main/conformance/array_hash_vectors)
(11 vectors: empty, ±0, canonical and non-canonical NaN, subnormals and
infinities, an 8 760-element ramp, a `BIGINT` payload with a float
vector's bits (equal hash), two's-complement extremes, element order). The
one-way-door rule of §2.4 applies: changing the definition is a
rehash-class event (every stored hash becomes stale: extra versions, never
lost writes).
