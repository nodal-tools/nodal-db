# Canonical encoding and content hash — normative specification

Status: **draft normative spec, 2026-08-17.** This is Deliverable 1 of the
Rust core (`rust_implementation_plan.md` Part 5). It pins the canonical byte
encoding and the 64-bit content hash for axis objects and values precisely
enough that **two independent implementations produce byte-identical output
and therefore bit-identical hashes.** Drift between two canonicalizers is the
project's founding failure (`CLAUDE.md` invariant 2; `value_model_plan.md`
(VMP) Part 3). This document is that contract; the machine-checkable form is
`conformance/encoding_vectors/*.json`.

The key words **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, and **MAY** are
used as in RFC 2119.

Authoritative inputs: VMP Part 1 (index kinds, duration pair, calendar/tz),
Part 3 (canonicalization pipeline, tier rule), Part 4 (shared axes,
content-verified dedup), Part 9 (this deliverable); `schema/040_axes.sql`
(axis + child tables — the hash-input targets); `schema/050_values.sql` (the
value tiers); HANDOFF §6.6 (hash decision, populated-columns value encoding)
and §6.7 (float axis / CRS / F3/F6 follow-ups).

---

## 1. Scope and layering

1.1. This spec defines two related but distinct canonical byte encodings:
- the **axis encoding** (§4–§7), whose hash is stored in `axis.content_hash`
  and drives content-verified dedup (VMP Part 4);
- the **value encoding** (§9), the canonical byte form of a single value,
  used for cross-writer equality, history diffs, the conformance
  `pipeline_cases`, and any future value-level content hash.

1.2. Both encodings are pure, deterministic functions of **canonicalized
content only** (VMP Part 3 pipeline: NULL elimination, axis derivation, sort,
tier). The encoder MUST be fed canonical content; it MUST NOT re-derive tier
or axis form. Two writers that canonicalize the same logical data to the same
content MUST produce identical bytes.

1.3. The hash is an **accelerator, never the truth** (VMP Part 4, critique
#7). Equality of two axes (or values) is decided by **byte comparison of the
canonical encoding**, not by hash equality. `axis.content_hash` is
deliberately indexed, not `UNIQUE`; a hash collision costs one extra byte
comparison and never corrupts dedup. Implementations MUST NOT treat a hash
match as proof of equality.

---

## 2. The content hash

2.1. The hash function is **XXH3-64 with seed 0** (Juha's decision, HANDOFF
§6.6-C / decision (C)). It is computed over the complete canonical byte
string (§4 for axes, §9 for values) as a single contiguous input.

2.2. XXH3-64 returns a 64-bit result. That result MUST be interpreted as an
**unsigned 64-bit integer** and stored directly, by numeric value, into the
`content_hash UBIGINT` column. No byte reinterpretation, sign flip, or
truncation is applied: the number the algorithm returns is the number stored.

2.3. **Endianness of the stored value.** The 64-bit hash is a *numeric* value,
not a byte layout; endianness applies only to the encoding of the *input*
byte string (§3.1), never to the hash result itself. Two implementations MUST
agree on the same `u64` integer; how DuckDB physically persists that `UBIGINT`
on disk is a storage-engine concern that is invisible at the SQL value level
and out of scope here. Conformance vectors record the hash as a decimal `u64`.

2.4. The hash function and seed are effectively a **one-way door**: changing
either rehashes every axis in every database. Any change is a
breaking schema/format revision, not a routine edit.

---

## 3. Primitive byte encodings

All composite encodings are built from these primitives. Unless a field's
spec says otherwise, these rules are absolute.

### 3.1. Integers

3.1.1. All integer fields are encoded as **fixed-width, little-endian,
two's-complement** for signed types and **fixed-width little-endian** for
unsigned types. The width is stated explicitly at every use site; there is no
variable-length integer encoding anywhere in this spec.

3.1.2. The widths used by this spec:
- `u8` (1 byte) — the form tag (§5.2); property-ids and the property-count
  prefix (§6); component-slot ids and the component-count prefix (§9);
  `bool`/presence bytes;
- `i16` (2 bytes) — time-pattern `range_start`, `range_end`;
- `i32` (4 bytes) — **signed value fields** whose DDL column is `INTEGER`:
  `axis.length` (§5.3), per-position `ordinal` (§7.5, §7.7), and
  `p_step_months` (§3.5). These are values, not counts, and mirror their
  signed `INTEGER` columns exactly;
- `u32` (4 bytes) — **all length/count prefixes, everywhere they appear**: the
  string byte-length prefix (§3.4), the `time_pattern` `term_count` (§7.7),
  and the array element-count prefix (§9.5). A length or element count is
  never negative, so every such prefix is unsigned `u32` — there is no
  `i32`-vs-`u32` prefix left in this spec;
- `i64` (8 bytes) — timestamps (µs, §3.2), integer axis coordinates,
  duration micros, integer array payload elements.

**OPEN-1 [resolved]:** the widths above are pinned as stated. `axis.length`
and per-position `ordinal` stay `i32` to mirror their signed `INTEGER` DDL
columns; every length/count *prefix* is `u32`. A `u32` prefix (max ~4.29
billion) is sufficient — an axis or array longer than that is already
infeasible under the rank/row budgets — and the values/prefixes split removes
the earlier `i32`-vs-`u32` inconsistency (M5).

### 3.2. Timestamps

3.2.1. Every `TIMESTAMP` is naive (VMP Part 1: all stored timestamps are
naive; calendar/timezone are expressed, never asserted). It is encoded as its
value in **microseconds since the Unix epoch `1970-01-01T00:00:00`**, as a
signed `i64` little-endian (§3.1). DuckDB `TIMESTAMP` is microsecond
precision, so this is exact and lossless.

3.2.2. No timezone offset, calendar conversion, or normalization is applied to
the timestamp value itself. `calendar`/`timezone` participate in the hash only
as separate axis properties (§6).

### 3.3. Doubles (`DOUBLE` / IEEE 754 binary64)

3.3.1. A `DOUBLE` is encoded as its **8 IEEE-754 binary64 bytes,
little-endian**, after the following canonicalization:
- **Negative zero is normalized to positive zero**: any bit pattern equal to
  `-0.0` MUST be encoded as `+0.0` (`0x0000000000000000`). This mirrors the
  writer-side `-0.0 → +0.0` rule for `axis_float.coord` and `years_presented`
  (VMP Part 9; `schema/040_axes.sql`).
- **NaN is split by role (see §3.3.3):**
  - For **axis** floats (`p_start_dbl`, `p_step_dbl`, `axis_float.coord`,
    `years_presented`) NaN is **forbidden**: it MUST be rejected at the writer
    boundary before encoding (the DDL enforces `NOT isnan(...)`; see the
    critique-F1 note in `040_axes.sql` — DuckDB's total order makes `x = x` true
    for NaN, so `isnan()` is the only correct predicate). The encoder MUST NOT
    emit bytes for a NaN axis float; encountering one is a programming error, not
    an encodable coordinate. Axis coordinates must be orderable/parametric, and
    NaN has no ordering.
  - For **value** floats (`v_float`; the array payloads `value_scalar.v_float`,
    `value_long.v_float`, `value_array.values`) NaN is a **legal stored value**,
    canonicalized to the **single quiet-NaN bit pattern `0x7ff8000000000000`**
    (positive quiet NaN, zero payload). Every incoming NaN — whatever its
    sign/quiet/payload bits, signaling or not — MUST encode as these same 8
    bytes, exactly analogous to `-0.0 → +0.0`.
- All other values (including the two infinities and subnormals) are encoded
  verbatim as their binary64 bytes.

3.3.2. There is exactly **one** canonical bit pattern per encodable value:
positive zero for both zeros; for *value* floats the one quiet NaN
`0x7ff8000000000000` for every NaN; for *axis* floats no NaN at all. Byte
equality therefore tracks numeric equality for every representable
coordinate/value. **Deliberate, justified exception for the value-NaN
canonicalization:** collapsing every NaN bit pattern to one is a many-to-one map
(like `-0.0 → +0.0`), so byte-equality of two values with NaN measures no longer
tracks *IEEE* `==` (`NaN != NaN`). This is intentional and safe because (a) it
tracks DuckDB's **total order**, under which `NaN = NaN` is TRUE and every NaN
collates equal (critique F1, probe-confirmed) — so the canonical form is exactly
the equality the engine itself uses; and (b) there is **no cross-key value
dedup** (OPEN-7: the value encoding folds in key + shape, and value equality is
used only for history diffs and any future value-level content hash, never to
merge two distinct values), so a one-canonical-NaN rule cannot cause a wrong
merge. The exception does not apply to axes, where NaN cannot arise.

3.3.3. **Role split — axis rejects NaN, value canonicalizes it (M2).** NaN
handling is deliberately asymmetric by role:
- **Axis floats** (`p_start_dbl`, `p_step_dbl`, `axis_float.coord`,
  `years_presented`): NaN is **rejected**. The DDL enforces `NOT isnan(...)` and
  the writer applies `-0.0 → +0.0`, so the invariant is belt-and-suspenders. An
  axis coordinate must be orderable; NaN is not. **Axis encoding bytes are
  unchanged by this amendment** — the axis conformance vectors, including the
  axis NaN-reject vector, still pass byte-for-byte.
- **Value floats** (`v_float`; `value_array.values`, `value_long.v_float`,
  `value_scalar.v_float`): NaN is a **legal stored value**, canonicalized to
  `0x7ff8000000000000` at the Rust writer boundary before encoding, and stored
  as that same quiet NaN (DuckDB preserves NaN through `DOUBLE`, `DOUBLE[]`, and
  Arrow export — probe-confirmed). There is **no DB CHECK** on the value float
  columns — DuckDB cannot cheaply express `NOT isnan` over a `DOUBLE[]` and these
  columns were deliberately left unconstrained — so both the `-0.0 → +0.0`
  normalization and the NaN canonicalization are enforced **only at the Rust
  writer boundary** for values (`CLAUDE.md` invariant 2). A value that bypasses
  the writer could store a non-canonical `-0.0` or a non-canonical NaN bit
  pattern that no CHECK would catch; implementations SHOULD back the writer rule
  with a periodic **audit view** over the value tiers (mirroring
  `audit_axis_integrity`) so a smuggled non-canonical `-0.0`/NaN is detectable
  after the fact. This asymmetry is the point: a NaN cell is **present, not
  absent** (absence is NULL/omission, VMP Part 3 step 1), so a NaN-bearing dense
  grid stays a complete Cartesian grid and keeps the efficient **array tier**
  instead of being rejected or demoted to the long tier.

> **Amendment note (pre-freeze, pre-public).** §3.3.1–§3.3.3 reopen the *value*
> half of the original NaN ban (values now permit a canonicalized NaN; axes are
> unchanged). This is a deliberate pre-freeze change made before any public
> database exists; no stored hashes change for axes, and the value encoding has
> no persisted `content_hash` column yet (§9.2), so no corpus is rehashed.

### 3.4. Strings

3.4.1. A string is encoded as **UTF-8 in Unicode Normalization Form C (NFC)**,
prefixed by its **byte length** (after NFC normalization) as a `u32`
little-endian (§3.1), followed by the NFC byte sequence. The prefix is the
byte length, not the code-point or grapheme count.

3.4.1.1. **NFC is pinned to Unicode 16.0.0 (M1).** NFC's decomposition and
composition tables evolve between Unicode releases, so "NFC" without a version
is under-specified: two implementations linking different ICU/`unicode-
normalization` builds could normalize the same input differently and split a
dedup. The normalization form is therefore pinned to **Unicode 16.0.0**, and
the conformance suite MUST **vendor a frozen normalization table** (a snapshot
of the composition/decomposition/canonical-combining-class data) that both the
SQL side and the Rust writer normalize against, independent of whatever Unicode
version their platform libraries happen to ship. Bumping the pinned Unicode
version is a rehash-class change (§2.4), not a routine dependency update.

3.4.2. NFC normalization MUST be applied to every encoded string: axis labels
(`axis_label.label`), unit tags (`axis_time_pattern.unit`), and the
`calendar`/`timezone`/`crs` property strings (§6), and the `value_kind` tag
(§9). Applying one normalization form uniformly removes the
"same text, two byte sequences" dedup hazard.

3.4.3. **Case is significant (M6).** NFC does **not** case-fold. The
`calendar`, `timezone`, `crs` (§6) and `unit` (§7.7) tags are compared and
hashed **case-sensitively**: `noleap` and `NoLeap`, `EPSG:4326` and
`epsg:4326`, `Europe/Helsinki` and `europe/helsinki` are **distinct** and
produce distinct axes. This is deliberate — case-folding rules are themselves
locale- and version-dependent and would reintroduce the ambiguity NFC removes.
Importers SHOULD canonicalize tag case **upstream** (e.g. normalize CRS
authority codes to their registered casing, IANA timezone names to their
canonical form) before handing data to the writer, so a mere casing difference
does not spuriously split axes.

**OPEN-2 [resolved]:** NFC is applied to *all* encoded strings
(labels + calendar/timezone/crs/unit/value_kind), pinned to Unicode 16.0.0 per
§3.4.1.1. A non-normalized `calendar`/`crs` tag would otherwise split axes
spuriously, so uniform NFC is intended and required.

### 3.5. Durations (the `(months, micros)` pair)

3.5.1. `INTERVAL` appears nowhere. A duration — whether a scalar `duration`
value (`v_dur_months`, `v_dur_micros`) or a parametric datetime axis step
(`p_step_months`, `p_step_micros`) — is the tagged exact pair
`(months: i32, micros: i64)` (VMP Part 1, critique #9). It is encoded as
`months` (`i32` LE) followed by `micros` (`i64` LE).

3.5.2. **Normalization rule.** The pair is stored and encoded exactly as its
two component fields; there is **no cross-normalization** between the two
components. Specifically:
- **There is no day unit.** A day is always `86_400_000_000` µs carried in the
  `micros` field; `months` carries only calendar months. Consequently the
  `1 day` vs `24 hours` ambiguity that plagued `INTERVAL` is **unrepresentable
  by construction** (VMP Part 1, "Duration representation"): both denote the
  same `micros` value and hash identically.
- A **month-bearing** duration (`months != 0`) is a visibly distinct class
  from a micros-only duration and never collapses into one: months are not
  convertible to micros (a month has no fixed micro count), so `(months=1,
  micros=0)` and `(months=0, micros=N)` are always distinct encodings. The
  writer MUST NOT attempt to fold months into micros or vice versa.

3.5.3. **The pair is always fully materialized (M3).** Both components are
always encoded; an **absent** component is the integer **`0`**, never a NULL or
an omitted field. There is therefore no presence flag on a duration and no way
for `(NULL, N)` and `(0, N)` to both occur — the pair `(0, N)` is the one
canonical encoding of "micros-only", and `(M, 0)` the one canonical encoding of
"months-only". This makes the duration encoding fixed-width (`i32` + `i64` = 12
bytes) and unambiguous, and it is why the duration is a *single* logical
component (§9.5) even though it occupies two physical fields.

---

## 4. Axis canonical encoding — overall structure

4.1. The canonical byte string of an axis is the concatenation, **in this
exact order**:

1. **Kind tag** (§5.1)
2. **Form tag** (§5.2)
3. **Length** (§5.3)
4. **Property block** (§6) — a `u8` count of present properties, then
   `calendar`, `timezone`, `crs`, and any future expressed property,
   present-only, in ascending property-id order
5. **Kind body** (§7) — parametric fields *or* materialized child rows,
   determined by kind + form

4.2. The hash of the axis (§2) is XXH3-64/seed-0 over this complete string.

4.3. This structure is fixed at freeze. New *properties* extend only the
property block (§6) under the rehash-stable rule (§8); new *kinds* add a new
kind tag and a new kind body (§7) without disturbing existing kinds.

---

## 5. Axis header fields

### 5.1. Kind tag

5.1.1. The `axis.kind` enum value is encoded as a **string** (§3.4): its exact
lowercase ASCII tag, NFC-normalized (a no-op for ASCII), `u32`-length-prefixed.
The exact spellings are pinned in this table — an implementation MUST emit
these byte-for-byte (case-sensitive, §3.4.3):

| `axis.kind` tag | bytes | body encoding |
|-----------------|-------|---------------|
| `datetime`      | `datetime` (8) | §7.2 |
| `period`        | `period` (6)   | §7.6 |
| `label`         | `label` (5)    | §7.5 |
| `integer`       | `integer` (7)  | §7.3 |
| `time_pattern`  | `time_pattern` (12) | §7.7 |
| `float`         | `float` (5)    | §7.4 |

5.1.2. **⚠️ Footgun — `datetime` (axis kind) vs `date_time` (value_kind).**
The *axis* kind tag is `datetime` (no underscore, §5.1.1); the *value* kind tag
for a scalar/long datetime measure is `date_time` (**with** underscore, from
`value_scalar.value_kind`, §9.3). These are two different tag spaces —
`axis.kind` and `value_kind` — and their datetime spellings deliberately do NOT
coincide. An implementation MUST NOT normalize one to the other or share a
single constant between them; a copy-paste that emits `date_time` for an axis
(or `datetime` for a value) is a silent hash-divergence bug. The conformance
vectors MUST include both spellings to catch it.

**OPEN-3 [resolved]:** `kind` is encoded as its length-prefixed UTF-8 tag
string (not a fixed integer discriminant): the enum is "additive, no CHECK"
(`040_axes.sql`), so a string tag needs no central integer registry and a
future kind is self-describing, at a cost of a few bytes per axis. Exact
spellings are pinned in the §5.1.1 table; the `datetime`/`date_time` footgun is
called out in §5.1.2.

### 5.2. Form tag

5.2.1. A single byte discriminating the storage form:
- `0x00` — **parametric** (regular grid stored as `(start, step, length)`);
- `0x01` — **materialized** (irregular grid stored in a child table).

5.2.2. The form tag MUST reflect the **canonical** form actually stored under
the parametric-first rule (§7.1, VMP Part 4). It is emitted for **every** kind,
including kinds that have only one form (`label`, `period`, `time_pattern` are
always `0x01`; see §7). It exists so a parametric encoding can never
byte-collide with a materialized one of the same kind, independent of any
coincidence in the numeric payload.

**OPEN-4 [resolved]:** the explicit `u8` form-discriminator byte is **included**.
It closes the F6 concern structurally (parametric and materialized are NOT
byte-identical, §11.3) by making the two forms non-colliding regardless of
payload coincidence. It is defined *now*, before freeze, so it costs no rehash.
Single-form kinds (`label`, `period`, `time_pattern`) are fixed at `0x01`.

### 5.3. Length

5.3.1. `axis.length` (number of positions, `>= 1`) as an `i32` little-endian
(§3.1). This is a signed **value** field mirroring the `INTEGER` `axis.length`
column, not a length *prefix* (those are `u32`, §3.1.2) — the two are
intentionally distinct. Length is encoded explicitly even though it is
derivable from a materialized child-row count, so parametric and materialized
forms carry it uniformly and a length mismatch is a byte mismatch.

---

## 6. Property block (calendar / timezone / crs / future)

6.1. `calendar`, `timezone`, and `crs` are the three **expressed, never
asserted** axis properties (`040_axes.sql`; VMP Part 1). They **MUST
participate in the hash** so that axes differing only in a property never dedup
into one object with one writer's tag silently winning (VMP Part 1 "hash
caveat 2"; HANDOFF §6.7). A `noleap` representative axis and a real-calendar
axis with identical nominal timestamps are **distinct** axes.

6.2. Each property has a stable, **append-only property-id**:

| id | property | column | applies to |
|----|----------|--------|------------|
| 1 | calendar | `axis.calendar` | any kind (chiefly `datetime`/`period`) |
| 2 | timezone | `axis.timezone` | any kind (chiefly `datetime`/`period`) |
| 3 | crs      | `axis.crs`      | any kind (chiefly `float`), NULL elsewhere |

6.3. **Encoding — count prefix, then present-only, ascending id.** The property
block is:
- a **`u8` property-count prefix** `k` = the number of **non-NULL** properties
  present (H2), then
- `k` entries, in **ascending property-id order**, one per non-NULL property.
  An entry is:
  - the property-id as a `u8`, then
  - the property string value encoded per §3.4 (NFC, `u32`-length-prefixed,
    case-sensitive per §3.4.3).

A **NULL (absent) property contributes nothing** — it is neither counted nor
emitted (not even its id byte). An empty (all-NULL) property block is therefore
the single byte `0x00`.

6.3.1. **Why the count prefix is required (H2 — self-delimitation).** The
property strings are variable-length, so without a count the property block's
boundary with the following kind body (§7) is not pinned: for kinds whose body
is itself a variable-length string sequence (`label`, `period`,
`time_pattern`), a decoder could not tell where the last property ends and the
first body element begins, and two logically different axes could produce the
same byte string — a silent **false-merge**. The `u8` count makes the block
self-delimiting: the decoder reads exactly `k` entries and the body starts
immediately after. (`u8` suffices — the property registry grows by single
digits; a 256-property axis is not a real object.)

6.3.2. **The count prefix preserves §8 rehash-stability.** The count is the
number of *present* (non-NULL) properties only. Adding a new nullable property
to the schema does **not** change the count on any existing axis (the new
property is NULL there, so it is not counted and emits nothing) — the count
byte, and every other byte, is unchanged. The count rises only when a property
is **genuinely present**, and an axis that carries a property a second axis
lacks is already a *different* axis (different content → different bytes is
correct, §6.1). So the count prefix is fully consistent with the §8 rule that
an absent property contributes zero bytes.

6.4. Because each present entry is tagged with its property-id and entries are
emitted in ascending id order behind the count, the block is unambiguous:
"calendar present, timezone absent" and "calendar absent, timezone present"
produce different bytes (different id tags), so they can never collide.

---

## 7. Axis kind bodies

The kind body follows the property block. Its shape is chosen by
`(kind, form)`. For all materialized bodies, child rows are encoded in
**ascending `pos` order** (`pos` is dense `0..length-1`); `pos` itself is
**not** encoded — it is implicit in the sequence position — because the
canonical axis is always dense and 0-based (VMP Part 4).

### 7.1. Parametric-first rule (normative precondition)

7.1.1. A `datetime`, `integer`, or `float` axis whose distinct coordinates form
a **regular progression** MUST be stored and encoded in parametric form
(`form = 0x00`); it MUST NOT be materialized (VMP Part 3 step 2, Part 4). This
is what makes a regular grid's hash equal to its would-be materialization's —
*by making the materialization never happen*. Uniqueness of a regular grid
across writers rests entirely on every writer obeying this rule (§11).

7.1.2. Regularity for `float` axes is decided by the bit-exact test in §10.
Regularity for `datetime` and `integer` axes is decided by the **exact
reconstruction test** below (§7.1.2.1–§7.1.2.3), framed identically to the
float test (§10.3): an axis is regular **iff** the pinned reconstruction, seeded
from `c[0]` and one step, reproduces **every** materialized coordinate
bit-exactly; otherwise it MUST be materialized. The datetime and integer
reconstructions use exact integer/µs arithmetic (no floating point), but the
*framing* — reproduce-every-coordinate-or-materialize — is the same, and it is
what makes the store decision a deterministic function that the SQL view and
the Rust writer both compute.

7.1.2.1. **`datetime` reconstruction — matches the SQL view exactly.** A
`datetime` axis is regular, and therefore MUST be stored parametric, **iff**
there is a step `(p_step_months, p_step_micros)` such that, with
`p_start_ts = c[0]`,

```
coord(pos) = p_start_ts  ⊕months  (p_step_months · pos)  ⊕micros  (p_step_micros · pos)
```

reproduces **every** materialized coordinate `c[pos]` **µs-exactly** (§3.2) for
`pos = 0 .. length-1`. This is precisely the expression the `axis_position`
view evaluates —
`p_start_ts + to_months(p_step_months*pos) + to_microseconds(p_step_micros*pos)`
(`080_views_read.sql`) — so the SQL reconstruction and the Rust store decision
agree by construction. If any position diverges by even one microsecond, the
axis is **not** regular and MUST be materialized (`form = 0x01`).

**Pinned semantics (binding):**
- **Single month multiply, applied once — NOT iterated steps.** The month
  offset is the exact integer product `p_step_months · pos`, applied to
  `p_start_ts` as a **single** month interval. It is **not** `pos` successive
  additions of a one-step interval. The two diverge at month-length/clamp
  boundaries: from `2001-01-31`, a single `+2 months` clamps against *March*
  and lands on `2001-03-31`, whereas iterating `+1 month` twice lands on
  `2001-03-28` (Feb clamps to the 28th, then March keeps the 28th). Only the
  single-multiply form matches the view; the iterated form is forbidden.
- **Evaluation order is pinned:** apply the **month** interval to `p_start_ts`
  **first** (with the clamp rule below), **then** add the microsecond offset
  `p_step_micros · pos`. This mirrors the view's left-to-right
  `p_start_ts + to_months(…) + to_microseconds(…)`.
- **Month clamp rule (DuckDB interval-add / `to_months` semantics, pinned,
  probe-verified on 1.4.4).** Adding `k` months lands on the same day-of-month
  `k` months later; if that day does not exist in the target month, it
  **clamps to the last day of the target month**. Canonical example:
  `2001-01-31 ⊕ 1 month → 2001-02-28`, and `2001-01-31 ⊕ 2 months →
  2001-03-31`, `⊕ 3 months → 2001-04-30`. Time-of-day and sub-day components
  are carried through the month add unchanged; the microsecond offset is added
  afterward.
- **`micros` offset is exact.** `p_step_micros · pos` is an exact `i64` product
  added as microseconds; no rounding, no calendar interaction.

7.1.2.2. **REQUIRED conformance fixtures — month-clamp behaviour (H1).** Because
the single-multiply-with-clamp semantics are subtle, the conformance vectors
MUST pin both sides of the divergence:
- **(a) reproduce-case → PARAMETRIC.** Coords
  `[2001-01-31, 2001-02-28, 2001-03-31, 2001-04-30]` (each at 00:00:00) MUST be
  stored **parametric** `p_start_ts = 2001-01-31, p_step = (1 month, 0 µs)`:
  the single-multiply reconstruction reproduces all four positions exactly
  (probe-verified). An implementation that (wrongly) uses *iterated* stepping
  would compute position 2 as `2001-03-28` and fail to reproduce, mis-deciding
  this axis as irregular — this fixture catches that.
- **(b) divergence-case → MATERIALIZE.** Coords
  `[2001-01-31, 2001-02-28, 2001-03-28, 2001-04-28]` (the *iterated* `+1 month`
  sequence) MUST be **materialized** (`form = 0x01`) and MUST NOT be collapsed
  to parametric `p_start_ts = 2001-01-31, p_step = 1 month`: the pinned
  single-multiply reconstruction gives position 2 = `2001-03-31 ≠ 2001-03-28`,
  so no single `(start, months, micros)` pair reproduces it. An implementation
  that (wrongly) reads regularity as "every successive difference is the same
  `+1 month` step" would false-collapse this to a parametric axis whose read-back
  disagrees with storage — a dedup split. This is the regression the fixture
  guards.

7.1.2.3. **`integer` reconstruction.** Same reproduce-or-materialize framing. An
`integer` axis is regular, and MUST be stored parametric, **iff** with
`p_start_int = c[0]` there is an integer step `p_step_int` such that

```
coord(pos) = p_start_int + p_step_int · pos        (exact i64 arithmetic)
```

reproduces **every** `c[pos]` exactly for `pos = 0 .. length-1` (matching the
view's `p_start_int + p_step_int * pos`, `080_views_read.sql`); otherwise it MUST
be materialized. Integer arithmetic has no clamp or rounding subtlety, so the
reproduce test reduces to the constant-successive-difference test — but it is
stated as a reproduction to keep all three parametric kinds under one framing.
The writer MUST use wrapping-free (checked) `i64` arithmetic; a step that would
overflow `i64` across the axis makes the axis non-parametric (materialize).

### 7.2. `datetime`

- **Parametric** (`0x00`): `p_start_ts` (timestamp → `i64` µs, §3.2), then the
  step as the duration pair `p_step_months` (`i32`), `p_step_micros` (`i64`)
  (§3.5).
- **Materialized** (`0x01`): the `axis_datetime.ts` sequence, each a timestamp
  `i64` µs (§3.2), in `pos` order. (Length is the header's; not repeated.)

### 7.3. `integer`

- **Parametric** (`0x00`): `p_start_int` (`i64`), then `p_step_int` (`i64`).
- **Materialized** (`0x01`): the `axis_integer.num` sequence, each `i64`, in
  `pos` order.

### 7.4. `float`

- **Parametric** (`0x00`): `p_start_dbl` (canonical DOUBLE, §3.3), then
  `p_step_dbl` (canonical DOUBLE). Note `p_step_dbl > 0` is DDL-enforced
  (strictly increasing).
- **Materialized** (`0x01`): the `axis_float.coord` sequence, each a canonical
  DOUBLE (§3.3), in `pos` order.
- See §10 and §11.3 for the F3/F6 store-decision and non-byte-identity notes.

### 7.5. `label` (always `form = 0x01`)

7.5.1. For each position in `pos` order, encode:
- `ordinal` (`i32` LE) — the stored semantic order (`axis_label.ordinal`),
  which is **scoped to the axis** (GDX anti-lesson, VMP Part 4);
- `label` string (§3.4, NFC, `u32`-length-prefixed).

7.5.2. Both `pos`-order and the explicit `ordinal` are encoded: two axes with
the same labels in the same physical slots but different semantic ordinals are
distinct axes and MUST hash differently.

### 7.6. `period` (always `form = 0x01`)

7.6.1. For each position in `pos` order, encode:
- `start_ts` (`i64` µs, §3.2);
- `end_ts` (`i64` µs, §3.2) — stored explicitly, never derived (critique #10);
- `years_presented`: an **optional** DOUBLE. Because it is nullable
  (`axis_period.years_presented`), encode a **presence byte** `u8`
  (`0x00` = absent, `0x01` = present); when present, follow with the canonical
  DOUBLE (§3.3). (A per-row presence byte is used here rather than the §6
  property-block mechanism because this is a per-position optional, not an
  axis-level property.)

**OPEN-5 [resolved]:** `years_presented` is a **per-row** optional, encoded with
a per-position presence byte as in §7.6.1. The DDL (`axis_period.years_presented
DOUBLE`, nullable, no all-or-nothing constraint) permits per-row NULLs, so the
per-row optional is the safe, faithful reading; an all-or-nothing axis-level
flag would silently corrupt a legitimately mixed axis. Confirmed per-row.

### 7.7. `time_pattern` (always `form = 0x01`)

7.7.1. A time-pattern axis is a sequence of positions; each position is a
*set* of terms that union together (`axis_time_pattern`, PK
`(axis_id, pos, term_no)`). For each position in `pos` order, encode:
- `ordinal` (`i32` LE) — the explicit per-position ordinal (patterns are
  set-like, no natural order);
- `term_count` (`u32` LE) — the number of terms at this position (a count, so
  unsigned per §3.1.2);
- then, for each term in **ascending `term_no` order**:
  - `unit` string (§3.4, NFC, `u32`-length-prefixed, case-sensitive §3.4.3) —
    one of `Y|M|D|WD|h|m|s`;
  - `range_start` (`i16` LE);
  - `range_end` (`i16` LE).

7.7.2. Terms MUST be emitted in ascending `term_no` order so a position's term
set has one canonical serialization.

7.7.3. **Ordinal is constant per `pos` — a writer invariant (M4).** The schema
keys `axis_time_pattern` on `(axis_id, pos, term_no)` and stores `ordinal` on
every term row, but it does **not** constrain `ordinal` to be equal across the
terms of one `pos`. The read view papers over this with `any_value(t.ordinal)`
(`080_views_read.sql`). The canonical encoder encodes **one** `ordinal` per
position, so this MUST be pinned: the writer MUST write the **same** `ordinal`
for every `term_no` at a given `(axis_id, pos)` — `ordinal` is a property of the
*position*, not the term. The encoder reads it from **`term_no = 1`** (the
lowest term). A writer that emitted divergent per-term ordinals for one position
would make the canonical bytes depend on which term the encoder happened to read
— a non-determinism this invariant forecloses. (An audit view SHOULD assert
`count(distinct ordinal) = 1` per `(axis_id, pos)`.)

---

## 8. Rehash-stable default rule for future properties (CRITICAL)

8.1. **A future expressed axis property, when NULL/absent, MUST contribute zero
bytes to the encoding.** Adding a new nullable property to the `axis` table
(and this spec) therefore leaves the canonical bytes — and thus the hash — of
every existing axis **unchanged**. This is the guarantee that let `crs` be
added append-only (HANDOFF §6.7: "existing axis hashes unchanged") and is the
same additivity relied on throughout (VMP Part 1 hash caveat 2, Part 4).

8.2. The mechanism is the present-only, id-tagged property block (§6.3): a new
property is assigned the next append-only property-id (4, 5, …); it is emitted
only when non-NULL; on every pre-existing axis it is NULL and emits nothing.
Existing conformance vectors continue to pass without modification; new vectors
are *added* for the new property. The `u8` property-count prefix (§6.3) does
**not** weaken this: the count is over *present* properties only, so a
newly-added NULL property leaves the count — and thus every byte — unchanged
(§6.3.2). "Contributes zero bytes" therefore holds byte-for-byte for the whole
block, count included.

8.3. Implementations MUST NOT:
- reserve or emit placeholder bytes for a NULL property;
- reorder property-ids (order is ascending-by-id, fixed);
- fold a new property into the fixed header (§5) — the fixed header is frozen;
  all growth is in the property block.

8.4. This rule is the single most important forward-compatibility property in
this document. Getting it wrong (emitting even one byte for an absent property)
silently rehashes the entire corpus on the next schema addition.

---

## 9. Value canonical encoding — component-wise / populated-columns-only

9.1. **Motivation (HANDOFF §6.6; `dual_value_representation_FINDINGS.md`
D-5).** A value's canonical encoding MUST hash **only the populated component
columns**, so that adding a future second value-component column (e.g.
`v_imag` on the scalar/long tiers, `values_imag` on the array tier — the
recommended "second typed component column" door) does **not** rehash any
existing value. This is cheap to guarantee now and **impossible to retrofit
after freeze** (once hashes are stored, a full-tuple encoding would rehash
every value the moment the column is added).

9.2. **Today** no value tier stores a `content_hash` column (only `axis`
does). This section nonetheless defines the value encoding **now**, because it
governs (a) the `pipeline_cases` conformance vectors, (b) cross-writer value
equality and history diffs (VMP Part 3), and (c) any future value-level
content-hash column — all of which must be freeze-safe from day one.

9.3. **Structure (OPEN-7 [resolved]).** OPEN-7 was closed by jkiviluo
(2026-08-17): the value encoding folds in the **identity key and the
position/shape context**, not the measure alone (see §9.6). The canonical byte
string of a single canonicalized value is the concatenation:

1. **Key block** (OPEN-7) — the identity triple, three `i64` little-endian
   fields in this order: `entity_id`, `parameter_definition_id`,
   `alternative_id`. Identical measures under different keys therefore encode
   differently (§9.6).
2. **`value_kind` tag** — string (§3.4, NFC, `u32`-length-prefixed,
   case-sensitive §3.4.3); the open tag from `value_scalar.value_kind` etc.
   (`'float'|'int'|'bool'|'string'|'date_time'|'duration'|…`). Note the
   `date_time` spelling (underscore) is the **value_kind** tag, distinct from
   the `datetime` **axis-kind** tag (§5.1.2 footgun). For a Tier-A array the tag
   is `float` (the Tier-A rule is float-valued).
3. **Tier tag** — a single `u8` discriminating the physical tier: `0x00`
   **scalar** (rank 0), `0x01` **array** (Tier A, dense float payload), `0x02`
   **long** (Tier L). Emitted immediately after the `value_kind` tag and before
   the shape block. This is the value-side analogue of the axis **form byte**
   (§5.2): it exists so a scalar, array, and long encoding can never
   byte-collide, **independent of any coincidence** in the shape/measure block.
   Without it, an array and a long value differ only by the *structure* of the
   following block (a component block vs a `u32` cell count) — injectivity across
   tiers would rest on a structural coincidence rather than an explicit byte, the
   same gap the axis form byte closes (OPEN-4). New tiers take new tag values,
   append-only, without disturbing existing ones.
4. **Shape block** (OPEN-7) — a **`u8` rank** (the number of axes; `0` for a
   rank-0 scalar), then that many **axis content hashes** (`u64` little-endian
   each, spec §2), in declared-dimension order. This is the position/shape
   context: the value is bound to the exact axis objects it indexes. A scalar
   emits rank `0` and no hashes.
5. **Measure block** — per tier:
   - **scalar**: one **component block** (below) carrying the slot-1 measure;
   - **array** (Tier A): one **component block** whose slot-1 bytes are a `u32`
     element-count prefix then each canonical DOUBLE, row-major flattened in
     declared-dimension order (§9.5). Positions are implicit (dense Cartesian)
     and pinned by the axis hashes + row-major order;
   - **long** (Tier L): a `u32` **cell count**, then per cell — in canonical
     row-major order — the **positions** (`i32` little-endian per axis, declared
     order) followed by a **component block** of that cell's measure. The
     per-cell positions are the folded-in position context (OPEN-7).

A **component block** is a **`u8` component-count prefix** `k` = the number of
**populated** component columns (H2), then `k` entries, one per populated
component, in **ascending component-slot order**, present-only (identical
mechanism to the axis property block, §6.3):
- component-slot id as `u8`, then
- the component's value encoded by its physical type (§9.5).

The count prefix makes the component block self-delimiting for the same
injectivity reason as the property block (§6.3.1): the primary measure can
itself be variable-length (`v_str`, or an array payload), so without a count
the boundary after the last component is not pinned. Today `k = 1` for every
value (slot 1 only); it rises to `2` only when a slot-2 component is actually
populated — leaving every existing single-component value's bytes (and any
future hash) unchanged, exactly the §8 rehash-stable rule applied to values.

9.4. **Component slots** are append-only:

| slot | component | scalar/long column(s) | array column |
|------|-----------|-----------------------|--------------|
| 1 | primary measure | the single populated `v_*` for `value_kind` | `values` or `values_int` |
| 2 | (reserved, future) | e.g. `v_imag` | e.g. `values_imag` |

Slot 1 (the primary measure) is always present. Slot 2+ are emitted **only if
populated**; on every existing single-component value they are NULL and emit
nothing — so their addition leaves existing value bytes and hashes unchanged
(exactly the §8 rehash-stable rule applied to values). NULL≡absent (VMP Part 3
step 1) is what makes an absent component contribute nothing.

9.5. **Encoding of the primary measure (slot 1), by physical type:**
- `v_float` → canonical DOUBLE (§3.3);
- `v_int` → `i64` LE (§3.1);
- `v_bool` → one byte, `0x00` false / `0x01` true;
- `v_str` → string (§3.4, NFC, length-prefixed);
- `v_datetime` → `i64` µs (§3.2);
- duration (`v_dur_months`, `v_dur_micros`) → the pair `(i32, i64)` (§3.5)
  — note a duration is a single logical component encoded as its two fields;
- **array `values` (`DOUBLE[]`)** → a `u32` element-count prefix, then each
  element as a canonical DOUBLE (§3.3), in **row-major flattened, declared-
  dimension order** (VMP Part 3; the ordering is the subject of the separate
  reconstruction spec `spec/reconstruction.md`);
- **array `values_int` (`BIGINT[]`)** → a `u32` element-count prefix, then each
  element as `i64` LE, same flattened order. The `values`/`values_int` XOR
  (exactly one populated) is a writer invariant (`050_values.sql`); whichever
  is populated is the slot-1 component.

9.6. **Value equality = key + shape + measure (OPEN-7 [resolved]).** jkiviluo
(2026-08-17) closed OPEN-7 **against** the measure-only reading: the value
encoding **does** fold in the identity key (§9.3 step 1) and the position/shape
context — axis content hashes (§9.3 step 4) and, for the long tier, per-cell
positions (§9.3 step 5). Two values are "equal" iff they share key, shape, and
measure. Rationale (recorded in `specs/rust_impl_session_decisions.md`): the
conservative, collision-avoiding default; the only thing given up is a future
cross-key value-dedup, which is not a use case. The component-wise /
populated-columns-only rule (§9.1, §9.4) still applies **orthogonally** to the
measure block, so a future second component slot never rehashes an existing
value. (The array and long tiers already carry position/shape inherently via
the axis hashes and per-cell positions; this decision additionally binds in the
entity/parameter/alternative key.)

**OPEN-6 [resolved — keep].** The array-payload `u32` element-count prefix is
**kept**, and this is now load-bearing, not a nicety. Once a **second component
slot** exists (§9.4, the recommended `values_imag` door), the terminal array
payload is no longer the last bytes of the encoding: a following slot-2 payload
would begin immediately after it, and without an element count the boundary
between the two variable-length arrays is not pinned — the encoding becomes
non-injective and two different values can collide. The count makes each array
payload self-delimiting (and makes an axis-product/length mismatch a byte
mismatch). It is required for injectivity, so it stays.

**OPEN-7 [resolved — jkiviluo, 2026-08-17].** The value encoding carries the
axis/key context. The measure-only alternative was **rejected**: the value
encoding folds in the identity key (§9.3 step 1) and the position/shape (axis
content hashes + per-cell long positions, §9.3 steps 4-5), with the measure
still encoded component-wise / populated-columns-only (§9.4). "Two values are
equal" therefore means same key, same shape, same measure — the conservative,
collision-avoiding notion — for history diffs and any future value-level content
hash. See §9.6 and `specs/rust_impl_session_decisions.md`.

---

## 10. Float parametric store rule (F3) — bit-exact coordinate arithmetic

10.1. This section pins the arithmetic that BOTH implementations (the
`axis_position` SQL view and the future Rust writer) MUST use, so they agree
bit-for-bit on (a) reconstructing a parametric float axis and (b) the reverse:
deciding whether a set of coordinates IS a regular progression storable
parametrically (§7.1). Disagreement here is a dedup split — the founding
two-canonicalizers lesson (HANDOFF §6.7 F3; `float_axis_crs_int_plan.md` §9.2).

10.2. The coordinate at position `pos` is computed as, and ONLY as:

```
coord(pos) = p_start_dbl + (p_step_dbl * pos_as_f64)
```

with these binding requirements:
- **Operand order is exactly `start + (step * pos)`** — the multiplication
  `step * pos` is evaluated first, then added to `start`. This matches the SQL
  view expression `x.p_start_dbl + x.p_step_dbl * x.pos` (`080_views_read.sql`,
  the parametric-float branch).
- **No fused multiply-add (FMA).** The multiply and the add are two separate
  IEEE-754 binary64 operations, each independently rounded. An implementation
  MUST NOT contract `start + step*pos` into a single FMA (which rounds once and
  gives a different, off-by-a-ULP result).

  > **[verified] DuckDB 1.4.4 does NOT contract this expression to an FMA
  > (probed 2026-08-17).** The probe searched for a triple `(a, b, c)` where the
  > two-rounding result `a + (b·c)` differs from the correctly-rounded
  > `fma(b, c, a)`, and found
  > `a = -94910827.80130784` (`0xc196a0e5af348a0b`),
  > `b = 3058998.3033553627` (`0x4147569b26d45939`),
  > `c = 2601.0`, for which the two results differ by 1 ULP
  > (`0x41fd495a34f39da9` vs `0x41fd495a34f39da8`). `SELECT a + b*c` in DuckDB
  > 1.4.4 — both as scalar literals and as `a + b*pos` over a real table column
  > (the `axis_position` shape) — returned `0x41fd495a34f39da9`, i.e. the
  > **separate two-rounding** result, matching this spec's requirement and
  > **not** the FMA-contracted value. So the SQL view and a correct non-FMA Rust
  > writer agree here; there is **no STOP-class read-correctness risk** from FMA
  > contraction on this build. (This is a per-version fact: a future DuckDB, or a
  > differently-compiled build, could contract. The §10.4 / §10.5 fixtures are the
  > standing regression that would catch a drift.)
- **Rounding mode is round-to-nearest, ties-to-even** (IEEE-754 default) for
  both the multiply and the add.
- **`pos` → `f64` cast:** `pos` (a non-negative `INTEGER`, `< length`) is
  converted to `f64` exactly (all `INTEGER` values are exactly representable in
  binary64), then used as the multiplicand.

10.3. The **store decision** (is this coordinate set regular?) is: a set of
coordinates `c[0..n-1]` is stored parametrically with `p_start_dbl = c[0]` and
some `p_step_dbl` iff `coord(pos)` under §10.2 reproduces **every** `c[pos]`
bit-exactly for `pos = 0..n-1`. If any position diverges by even one ULP, the
axis is **not** regular and MUST be materialized (`form = 0x01`). This makes
the store decision itself a deterministic function pinned by §10.2.

10.4. **A ULP-divergence fixture is REQUIRED in the conformance vectors**
(HANDOFF §6.7 F3), in two complementary forms:
- an "almost regular" coordinate set — reproduced by `start + step*pos` at every
  position except one, which is off by one ULP — appearing as a vector asserting
  the axis materializes (does NOT collapse to parametric)
  (`09_float_ulp_materialize.json`); and
- an **FMA-discriminating** fixture with `start ≠ 0` where `start + (step*pos)`
  and `fma(step, pos, start)` genuinely differ by 1 ULP at some position
  (`21_float_fma_divergence_parametric.json`). This is the one that catches an
  FMA-contracting or wrong-operand-order implementation: with `start = 0` a fused
  and a two-rounding evaluation coincide (`fma(step,pos,0) == step*pos`), so a
  `start = 0` fixture does **not** exercise the no-FMA pin. The fixture pins the
  bit-exact non-FMA result at the divergent position and asserts the two
  evaluations differ, so a reconstruction that used `mul_add` fails loudly.

10.5. **A cross-engine bit-exactness fixture is REQUIRED (H3a).** Because §10.1
asserts the SQL `axis_position` view and the Rust writer agree bit-for-bit but
that agreement is only *probed*, not *proved*, the conformance suite MUST carry
a **round-trip fixture across both engines**: the Rust writer materializes a
parametric axis (float — and, for §7.1.2, at least one datetime axis exercising
the month-clamp reconstruction), the fixture reads the coordinates **back through
the `axis_position` SQL view** (via the project's DuckDB), and asserts they are
**bit-exact** (µs-exact for datetime, IEEE-bit-exact for float) against the
writer's own reconstruction. This fixture is the standing guard that the two
independent reconstruction implementations have not drifted — the §10.2/§7.1.2
arithmetic pins are only as good as a test that actually runs both engines and
compares. It MUST fail loudly if a future DuckDB build begins FMA-contracting
(§10.2 [verified] note) or changes month-clamp semantics (§7.1.2.1).

---

## 11. Uniqueness caveats (normative notes)

11.1. **Hash caveat 1 — needlessly distinct axes (VMP Part 1).** The same
hourly grid stamped nominal year 1995 by one importer and 2018 by another
yields two axis objects. This is *correct* (different timestamps → different
content → different bytes) and cheap (regular grids are parametric, a few
bytes). The importer SHOULD prefer a **project-consistent nominal year** so
shared grids actually share. This is guidance, not an encoding rule.

11.2. **Hash caveat 2 — needlessly merged axes (VMP Part 1; §6).** Because
`calendar`/`timezone`/`crs` (and every future property) are hash input, axes
differing only in a property never dedup. This is enforced by §6, not
optional.

11.3. **Parametric and materialized float encodings are NOT byte-identical
(F6).** A parametric float axis encodes `(p_start_dbl, p_step_dbl)` — two
doubles under `form = 0x00`; a materialized float axis encodes `length`
coordinates under `form = 0x01`. These are different byte strings and hash
differently even when they denote the same coordinate set. Uniqueness of a
regular float grid across all writers therefore rests **entirely** on the
**mandatory parametric-first store rule** (§7.1, §10.3): every writer that sees
a regular float grid MUST store it parametrically, or a duplicate (materialized)
axis with a different hash results. This is the float-axis analogue of the
datetime/integer parametric-first rule and is stated here because float
regularity is decided by floating-point arithmetic (§10) rather than exact
integer arithmetic, making the store rule the sole guarantee.

11.4. The same non-byte-identity holds for `datetime`/`integer` parametric vs
materialized forms; the same parametric-first rule (§7.1, exact arithmetic)
is the guarantee there. The `form` byte (§5.2) makes the two forms
structurally non-colliding regardless.

---

## 12. Conformance-vector coverage (informative pointer)

`conformance/encoding_vectors/*.json` (built in Phase 1, `rust_implementation
_plan.md` Part 5 Deliverable 1) MUST cover, at minimum: parametric vs
irregular `datetime`, **including the §7.1.2.2 month-clamp pair** — the
reproduce-case `[Jan-31, Feb-28, Mar-31, Apr-30]` asserting PARAMETRIC and the
divergence-case `[Jan-31, Feb-28, Mar-28, Apr-28]` asserting MATERIALIZE;
parametric vs irregular `integer`; parametric vs irregular `float` (incl. the
§10.4 ULP-divergence fixture); **the §10.5 cross-engine round-trip fixture**
(writer-materialized parametric axis read back bit-exact through the
`axis_position` view, for both float and a month-clamp datetime axis);
`label` with non-trivial ordinals; `period` with and without `years_presented`
(per-row mix, §7.6); `time_pattern` with multi-term positions **and constant
per-`pos` ordinal** (§7.7.3); `calendar`/`timezone`/`crs` variants proving
non-dedup (§6, §11.2) **including a case-only difference proving
case-sensitivity** (§3.4.3); month-bearing vs micros-only durations (§3.5);
NFC edge cases against the frozen Unicode 16.0.0 table (§3.4); `-0.0`
normalization, the **rejected axis NaN** and the **canonicalized value NaN**
(`0x7ff8000000000000`, §3.3); and value encodings for each
`value_kind` (incl. the `date_time`/`datetime` spelling pair, §5.1.2) and array
`values`/`values_int` (§9), with a fixture proving a NULL slot-2 component
leaves the hash unchanged (§9.4, §8) and one proving the property-count /
component-count prefixes make an otherwise-ambiguous variable-length body
self-delimiting (§6.3.1, §9.4 — the false-merge regression, H2).

---

## Open points for review

Following the adversarial critique (H1–H3, M1–M6), OPEN-1 through OPEN-6 are
**resolved in-text** as noted below; **OPEN-7 is now resolved** (jkiviluo,
2026-08-17), and the H3 FMA probe result is recorded.

- **OPEN-1 [resolved] (§3.1.2):** widths pinned — `i32` for signed value fields
  (`length`, `ordinal`, `p_step_months`); `u32` for every length/count prefix;
  `u8` for tags/counts/flags. The values-vs-prefixes split removes the earlier
  `i32`/`u32` contradiction (M5).
- **OPEN-2 [resolved] (§3.4):** NFC applied to *all* encoded strings, pinned to
  **Unicode 16.0.0** with a vendored frozen normalization table (M1);
  case-sensitive comparison (M6).
- **OPEN-3 [resolved] (§5.1):** `kind` encoded as a length-prefixed UTF-8 tag
  string; exact spellings tabulated; the `datetime`(axis) vs `date_time`
  (value_kind) footgun called out (§5.1.2).
- **OPEN-4 [resolved] (§5.2):** explicit `u8` `form` byte included; single-form
  kinds fixed at `0x01`.
- **OPEN-5 [resolved] (§7.6):** `years_presented` is a per-row optional
  (per-position presence byte).
- **OPEN-6 [resolved — keep] (§9.5):** array element-count prefix kept; now
  justified as **required for injectivity** once a second component slot exists
  (not merely a nicety).
- **OPEN-7 [resolved — jkiviluo, 2026-08-17] (§9.3, §9.6):** the value encoding
  folds in the identity **key** and the position/**shape** (axis content hashes +
  per-cell long positions), with the measure still component-wise /
  populated-columns-only. Value equality = same key + shape + measure — the
  conservative, collision-avoiding notion. The measure-only alternative was
  rejected.

**H3 FMA probe result (recorded, §10.2 [verified]):** DuckDB 1.4.4 does **not**
contract `p_start_dbl + p_step_dbl * pos` to a fused multiply-add — it returns
the separate two-rounding result, matching this spec's no-FMA requirement.
**No STOP-class read-correctness risk** on this build; the §10.4/§10.5 fixtures
stand as the regression guard against a future contracting build.
