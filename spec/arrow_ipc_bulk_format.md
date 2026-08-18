# Arrow-IPC bulk import format — normative specification

> **SIGNED OFF — jkiviluo, 2026-08-18.** All `OPEN-Q3-1..9` judgment calls
> are resolved as recommended (see §11 for the resolution notes and two
> clarifications folded in). This document is now **format version 1.0**, the
> SemVer'd interface of §3.3 door 3 (`language_and_architecture_research.md`).
> Not yet public — it goes public only when the DB itself does (jkiviluo:
> "this will not be public before it is stable"), so pre-1.0-public changes
> may still land under the §3.2 versioning rule.

Status: normative spec v1.0, signed off 2026-08-18. This is the wire format ingested by
`spinal-db import` (`rust_implementation_plan.md` Part 4; work-queue item 2),
the **declared write boundary for all non-core clients** — Julia/SpineOpt
today, any R/Rust/other tool tomorrow (`value_model_plan.md` (VMP) Part 8
decision 2; `julia_writer_strategy_research.md` §4 option i, §5). Producers
write this format with any stock Arrow implementation; the importer
deserializes it and calls the one Rust core — it **never re-canonicalizes**
(`CLAUDE.md` invariant 2).

The key words **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, and **MAY** are
used as in RFC 2119.

Authoritative inputs: `crates/spinal-db-core/src/api.rs` (the
`write_parameter` batch contract this format feeds); VMP Part 6 (interchange
encoding: logical long + `(entity, axes, array)` forms) and Part 8 decision 2;
`julia_writer_strategy_research.md` (the producer's workload);
`language_and_architecture_research.md` §3.3 (door 3);
`spec/reconstruction.md` (row-major law for the dense form);
`specs/rust_impl_session_decisions.md` (decision D: `spinal:label_domain`).

---

## 1. Scope and layering

1.1. This spec defines the **container**, **metadata**, and **payload batch
schemas** of one bulk-import input, plus the **error and atomicity semantics**
the importer MUST implement. It does not define canonicalization, tiering, or
the write protocol — those belong to the core (`spec/canonical_encoding.md`,
VMP Parts 3–5) and are invoked, not restated, here.

1.2. The format is deliberately **shaped by `write_parameter`'s existing
batch contract** (api.rs module docs): logical-long batches of
`(entity, dim…, value)` with Arrow types fixing index/measure kinds, and label
domains carried in Arrow field metadata. The importer is a thin adapter: parse
container → validate metadata → hand batches to the core inside one
transaction. Any format feature that would require importer-side data
interpretation beyond structural expansion (§6) is out of scope by design.

1.3. **v1.0 value/index coverage.** The format carries what the core accepts
today: measure kinds `float | int | bool | string | date_time`; index kinds
`datetime | integer | label`; rank 0–4. `duration` measures and
`period`/`time_pattern` dimensions have **no wire form in 1.0** (the core's
long-input derivation for them is deferred — `types.rs` note); they are
**reserved minor-version doors** (§3.4), not silently accepted.

---

## 2. Container

2.1. The container is the **Arrow IPC *stream* format** (encapsulated message
stream: schema message, dictionary/record-batch messages, end-of-stream
marker), NOT the IPC *file* (random-access / Feather) format.

Justification (**OPEN-Q3-1**): (a) the dominant producer's append path
requires it — SpineOpt's rolling runs accumulate result rows incrementally
(`Arrow.append`, `run_spineopt_basic.jl:1066-1098`), and Arrow.jl's append
supports only the stream format; (b) the importer reads strictly
sequentially into one transaction — the file format's seekable footer buys
nothing; (c) streams can be written to a pipe/stdout without seek-back.
*Alternative:* accept both formats by sniffing the `ARROW1` file magic.
Rejected for v1.0: two container paths double the parse surface for zero
producer need. A stream that begins with the file magic MUST be rejected with
an error naming the expected format (§9, E1).

2.2. **Segmented container (**OPEN-Q3-2**, the multi-parameter decision).**
One import input is an ordered sequence of **one or more complete IPC
streams, concatenated back-to-back in one file** ("segments"). Each segment
carries its own schema message (with the §4 metadata) and targets **exactly
one** `(class, parameter)` pair. The importer MUST, after each segment's
end-of-stream marker, check for further bytes and, if present, parse the next
segment; trailing bytes that do not begin a valid stream are an error (E6).

Why this shape: SpineOpt writes **many parameters per run** (tens of outputs
× entities), so one-parameter-per-file would force dozens of files and
invocations per run — hostile, per the task framing. The alternatives fail
structurally: an IPC stream admits exactly **one** schema, so parameters of
different rank/kinds cannot share a stream of uniform batches; per-batch
target metadata (Flatbuffers `Message.custom_metadata`) is not usably exposed
by arrow-rs, PyArrow, or Arrow.jl writers/readers; a discriminator column
would require a union-typed schema wide enough for every parameter's
dimensions. Concatenated streams need **no writer library support at all**: a
producer opens one file handle and runs its stock stream writer once per
parameter. On the read side the claim is narrower (critique F10, verified):
stock **writers** produce valid segments in all three ecosystems
(arrow-rs, PyArrow, Arrow.jl); stock **readers** delimit segments at exact
byte positions in arrow-rs 58.4.0 and PyArrow 25.0.1 (zero overshoot, even
on non-seekable sources) — but **Arrow.jl's stock reader cannot delimit
segments** (it buffers the whole source; `position(io)` stays 0 after
exhausting a stream), so Julia read-back of a multi-segment container needs
a manual byte-offset splitter. This is irrelevant to the format (the only
normative reader is the Rust importer; Julia is a producer) but the Julia
round-trip conformance test must account for it. Implementation note: in
arrow-ipc 58 the importer's resume-at-position loop MUST use the plain
`StreamReader::try_new` (or one shared reader) — `try_new_buffered` reads
ahead past the segment boundary and breaks the loop.
*Alternative:* one target per file, `spinal-db import <file>...` accepting
multiple file arguments in one invocation/transaction. Functionally
equivalent but pushes the multiplicity into the filesystem and the command
line; kept as the fallback if concatenated streams turn out to fight some
producer library.

2.2.1. **Truncation defense (normative — critique F1).** The stream format
has no footer, and every stock reader treats a bare EOF at a message
boundary as a clean end of stream (verified in arrow-rs, PyArrow, and
Arrow.jl: a stream with its end-of-stream marker stripped reads *silently*
as complete). A producer crash after flushing batches but before
`close()`/`finish()` — likewise disk-full and SIGKILL — produces exactly
that state, so an undefended importer would commit a silently partial
parameter with exit 0. Two compounding rules close this:

- **Per-segment EOS verification.** Every segment MUST terminate with the
  explicit end-of-stream marker (`ff ff ff ff 00 00 00 00`). The importer
  MUST distinguish EOS-terminated from bare-EOF-terminated streams and MUST
  fail the import on the latter (E10, "segment not terminated — truncation
  suspected"). Implementation note: stock `StreamReader` does **not** report
  this distinction — the importer needs a thin framing-aware wrapper over
  the byte stream it already controls (checking that the 8 bytes preceding
  each reader stop are the EOS sequence at a message boundary suffices).
- **Container completeness declaration.** Every segment MUST carry
  `spinal:segment_count` (§4.1): the total number of segments in the
  container, stamped identically on every segment. A count mismatch between
  segments, or an actual segment count differing from the declared one, is
  E10. This closes the between-segments truncation hole EOS checking alone
  cannot: truncation after segment k's EOS but before segment k+1's schema
  message yields a byte-perfect k-segment container no reader could
  otherwise distinguish from the intended one.

Producers SHOULD additionally write to a temporary path and rename after a
successful close (the standard atomic-file idiom).

2.2.2. **Producer hazard — `Arrow.append` (critique F11, verified).** The
rolling append path MUST target only a **single-stream, per-parameter**
file; the finished streams are then concatenated byte-level (`cat`) into
the container. Calling `Arrow.append` on a file that already holds a second
segment **succeeds without error and corrupts the container** (the appended
batches land inside the first stream's framing; the second segment becomes
unreadable). Never append to a multi-segment container.

2.3. Batches within a segment: a segment contains **zero or more record
batches**, all under the segment's schema (IPC guarantees this). Producers
MAY split rows across batches arbitrarily (e.g. one batch per rolling
window); a logical entity's rows MAY span batches. The importer MUST treat a
segment's batches as one logical table (concatenate before, or stream into,
the core call). A segment whose batches total **zero rows is an error**
(E6): the importer MUST NOT import it and MUST NOT auto-create anything
from it — consistent with the core, which rejects empty batches before they
can auto-create referents or burn a commit (api.rs, core critique F8:
"empty batch: no rows to write"). *Rejected alternative* (critique F6): a
schema-seeding empty segment that auto-creates the definition — a
legitimate-sounding feature, but it would contradict the shipped core's
empty-batch guard and make definition creation depend on a zero-data side
channel; if wanted later it is a core-first change and a minor door.

2.4. **Encodings the importer MUST accept:** dictionary-encoded columns
(Arrow.jl and PyArrow emit these for repeated strings; the importer decodes
to the plain type before the core call) and IPC body **buffer compression**
with codecs `ZSTD` and `LZ4_FRAME` (**OPEN-Q3-9**; producers MAY compress,
SpineOpt's existing pipeline uses zstd — `background_arrow_ipc_transition_plan.md`
Phase 0). Run-end encoding is **forbidden in 1.0**: it MUST NOT be produced
(Arrow.jl cannot read it; same precedent) and the importer **MUST reject**
it (E6) — a MAY-reject would make importability implementation-defined
(critique F15).

Implementation consequence (critique F12, plan-level): the workspace policy
"arrow only through `duckdb::arrow`" (root `Cargo.toml`) does **not** reach
IPC — duckdb 1.10505.0 pulls arrow 58.4.0 *without* the `ipc` feature, so
`duckdb::arrow::ipc` does not exist. The importer needs a **direct `arrow`
(or `arrow-ipc`) dependency with the `ipc` + `ipc_compression` features,
pinned exactly to the arrow version `duckdb::arrow` re-exports (58.4.0
today)**. This revives risk R2: a duckdb arrow-major bump now forces a
lockstep bump of the direct dependency — the Cargo.toml comment and the R2
mitigation must be amended to record this. (Verified: arrow 58.4.0 with
`ipc_compression` reads PyArrow-written zstd-compressed and
dictionary-encoded streams correctly, so this section's requirements are
implementable.)

2.5. One import invocation ingests **one container** (one file argument in
the Part-4 CLI sketch). A future multi-file invocation is a CLI concern, not
a format concern.

---

## 3. Format versioning (door 3 — SemVer'd interface)

3.1. Every segment's schema metadata MUST carry:

| key | value | rule |
|---|---|---|
| `spinal:format` | `spinal-db-bulk` | exact match, else E2 |
| `spinal:format_version` | `MAJOR.MINOR` decimal, no leading zeros | starting value **`1.0`** |
| `spinal:segment_count` | total segments in the container, positive decimal integer | integrity rule §2.2.1, else E10 |

The version is **per-segment** (each segment is a self-contained stream), but
all segments in one container MUST declare the same version (else E3).
Starting at `1.0` (not `0.x`): this boundary is declared stable for external
clients from its first shipped release; there is no "unstable 0.x" phase for
a format Julia tooling scripts against (**OPEN-Q3-3**; alternative: start at
`0.1` until a post-prototype freeze, at the cost of promising instability to
the first consumers).

3.2. **Compatibility policy (normative for every reader, forever):**

- **Different MAJOR than the reader supports → reject** before any database
  write (E3), naming both versions. A major bump means existing constructs
  changed meaning; no fallback parsing is attempted.
- **Same MAJOR, any MINOR (including newer than the reader) → attempt the
  import**, under the **structural-unrecognizability rule** (critique F2,
  normative for every future edit of this spec): a minor version MUST only
  add constructs an old reader **cannot misparse** — new Arrow types (an old
  reader fails at E6), new `spinal:form` values (E4), or new optional keys
  whose ignoring changes nothing about how the rest of the segment is
  interpreted. Any key or construct that **modifies the interpretation of an
  otherwise-valid construct** (an index-kind override on a valid Arrow type,
  a calendar or timezone annotation that changes what stored instants mean,
  …) is a **MAJOR** bump, full stop — because a warn-and-ignore reader would
  otherwise import the data under the old interpretation, silently. A stream
  that does not use a new minor construct therefore reads correctly on an
  old reader; a stream that does will fail at the specific unknown construct
  (E4/E6) with an error citing the version skew — a loud, located failure,
  never silent misreads.
- **Unknown `spinal:`-namespaced keys → warn and ignore** (safe *because of*
  the structural-unrecognizability rule: within a major, an ignorable key is
  by construction one whose ignoring cannot change meaning). Unknown keys
  outside the `spinal:` namespace MUST be ignored silently (foreign tools may
  stamp their own metadata).

*Rejected alternative* (critique F2): a `spinal:requires` required-features
key (JSON array; reader MUST reject a segment listing a feature it does not
implement), which would let interpretation-modifying additions stay minor at
the cost of one more mandatory check in every reader, forever. The
structural rule needs zero mechanism; prefer it.

3.3. Corollary for writers of this spec: adding a **required** key, changing
any §4–§6 semantics, or adding **any** construct that reinterprets an
existing valid one (§3.2) is a MAJOR bump; adding an optional key, a new
`form` value, or a new kind carriage on a **new** Arrow type is a MINOR
bump. The version history is maintained in this file.

3.4. Reserved for minor versions (declared now so names are stable):
`duration` measure carriage, `period` / `time_pattern` dimension carriage
(both need a multi-field wire form the core does not yet derive from long
input — new Arrow shapes, so structurally unrecognizable to a 1.0 reader,
hence legitimately minor), and an integer dense payload (§6.5). The
previously-reserved `spinal:index_kind` field-metadata override is
**removed from the minor list** (critique F2): overriding the kind of a
column whose Arrow type already maps to a kind is exactly the silent
reinterpretation §3.2 forbids — a 1.0 reader would warn, ignore the key,
and import a `period` column as `label`. If it is ever needed, it is a
MAJOR door. The same holds for any future `spinal:calendar` /
`spinal:timezone` axis-property carriage (§5.2).

---

## 4. Segment schema metadata

4.1. All keys live in the **`spinal:` namespace** (lower-case, colon
separator), matching the shipped `spinal:label_domain` precedent (decision D,
api.rs). Values are UTF-8 strings; keys marked JSON hold a JSON document.

| key | required | value |
|---|---|---|
| `spinal:format` | yes | `spinal-db-bulk` (§3.1) |
| `spinal:format_version` | yes | `1.0` (§3.1) |
| `spinal:segment_count` | yes | total segments in the container (§2.2.1, §3.1) |
| `spinal:class` | yes | target entity-class name |
| `spinal:parameter` | yes | target parameter-definition name |
| `spinal:alternative` | no | target alternative name (§4.3) |
| `spinal:form` | yes | `long` \| `dense` (§5, §6) |

Missing required key → E4. Unknown value of `spinal:form` → E4 (this is the
minor-version door of §3.2 doing its job).

4.2. **Targets are per-segment, not per-batch and not per-row**
(**OPEN-Q3-4**). Rationale in §2.2: per-batch custom metadata is not
practically writable/readable in the producer ecosystems, and a per-row
discriminator column cannot coexist with per-parameter dimension columns of
differing shape. The segmented container already gives multi-parameter
capability at stream granularity, which matches the core's natural write unit
(`write_parameter` = one class/parameter/alternative per call). A
`(class, parameter, alternative)` triple MUST NOT be targeted by more than
one segment in a container (else E7): allowing it would mean two same-commit
writes to the same value keys, which the history protocol has no defined
semantics for. The E7 comparison MUST be **case-insensitive** (critique F5):
every core name resolver matches `cur_name = lower(?)`, so segments
targeting `Flow` and `flow` resolve to the same parameter definition —
comparing raw metadata strings would pass E7 while producing the precise
double-write E7 exists to prevent. The fold is DuckDB `lower()` semantics —
the fold resolution itself uses (over the restricted name charset this is
ASCII lowering, the same fold the core's entity dedup applies, api.rs) — so
the importer's pre-check cannot drift from the core's resolution.

4.3. **Alternative resolution** (**OPEN-Q3-5**). The effective alternative
for a segment is: `spinal:alternative` if present, else the CLI
`--alternative` flag; neither present → E4. If **both** are present they MUST
be equal (case-insensitively, matching core name resolution), else E4 — a
conflict between file and flag is always a mistake worth stopping on, not a
precedence puzzle. This keeps the SpineOpt case trivial (one alternative per
run, passed once on the CLI, absent from the file) while letting a
multi-alternative container exist (each segment stamps its own).
*Alternative:* CLI flag strictly overrides file metadata. Rejected: silent
override is how a re-run clobbers the wrong alternative.

4.4. Dimension **names and kinds are carried by the payload columns
themselves**, not by metadata: field name = dimension name, Arrow field type
= index kind (§5.2), exactly as `write_parameter` infers them today. No
`spinal:dimensions` metadata key exists in 1.0 — duplicating the schema in
metadata creates a disagreement channel with nothing to arbitrate it. (A
`spinal:index_kind` per-field override, should kinds ever stop being
type-inferable, is a **major**-version construct — §3.4.) Dimension names
are **normative when the parameter
definition is auto-created** (they become `parameter_type_dimension` rows)
and informative otherwise (the core reads the declared type of an existing
definition from the DB — critique S2 — and validates kinds positionally, not
by name).

---

## 5. Payload batch schema — `long` form

5.1. `spinal:form` = `long` is the logical-long shape, **identical to the
`write_parameter` batch contract** (api.rs module docs). Columns, in order
(order is normative; names of the entity/value columns are conventional):

| position | field name | Arrow type | content |
|---|---|---|---|
| 0 | `entity` (SHOULD) | `Utf8` | entity **name** (decision B: resolve-by-name; auto-created if absent) |
| 1 … rank | *the dimension name* (normative on auto-create, §4.4) | per kind, §5.2 | index coordinate |
| last | `value` (SHOULD) | per kind, §5.3 | the measure |

Rank = column count − 2, and MUST be ≤ 4 (E6). Rank 0 (scalar) is the
two-column batch `(entity, value)`; one row per entity.

5.1.1. **Name tripwires (critique F8).** Because columns are identified by
position and names are only SHOULD, a rank-0 batch with a **string** measure
has two `Utf8` columns — a producer emitting `(value, entity)` would import
cleanly with entities and values interchanged. Therefore: the importer MUST
reject (E6) any batch in which a column literally named `entity` or `value`
(case-insensitively) appears at a position other than its §5.1/§6.1 slot —
the SHOULD-names become tripwires whenever present. The importer SHOULD warn
when both names are absent entirely.

5.2. **Dimension column types** (fixes the index kind; any other type → E6):

| Arrow field type | index kind |
|---|---|
| `Timestamp(Second \| Millisecond \| Microsecond, tz optional)` | `datetime` (stored naive, µs — canonical_encoding §3.2; §5.2.1–.2) |
| `Int64` | `integer` |
| `Utf8` | `label` |

5.2.1. **Timestamp units (critique F4, verified).** The core stores µs, but
the wire accepts `Second`, `Millisecond`, and `Microsecond`: Arrow.jl
serializes a `Dates.DateTime` column as `timestamp[ms]` and pandas defaults
to `ns` — µs-only would reject the dominant producers' natural output.
The importer MUST upcast `Second`/`Millisecond` values to `Microsecond`
**losslessly and structurally** (integer multiply — unit conversion is
arithmetic on the same instant, not interpretation, so CLAUDE.md
invariant 2 is untouched) before the core call. `Timestamp(Nanosecond, …)`
is **rejected** (E6) with an error text naming the fix (cast to µs
producer-side; pandas: `.astype("datetime64[us]")`): truncation is lossy
and lossy conversion must never be silent.

5.2.2. **Timezones.** Producers SHOULD emit timezone-naive timestamps. A
`tz`-annotated column is **not an error**: an Arrow tz-bearing timestamp is
a well-defined UTC-epoch instant, and the core accepts it, storing the
UTC-µs instant unchanged and surfacing a **`TimezoneNormalized` warning per
column** (api.rs; core critique F6 fix). The importer MUST pass such
columns through (after §5.2.1 unit upcast, which preserves the instant) and
MUST surface the core's warning — never strip or normalize silently. Stored
timestamps remain naive (VMP Part 1); calendar/timezone as *axis
properties* have no wire carriage in 1.0, and any future carriage is a
MAJOR door (§3.4).

5.3. **Value column types** (fixes the measure kind; any other → E6):
`Float64` → `float`, `Int64` → `int`, `Boolean` → `bool`, `Utf8` → `string`,
`Timestamp(Second | Millisecond | Microsecond, tz optional)` → `date_time`
(same unit-upcast and timezone rules as §5.2.1–.2). (No
`Duration`/`Interval` type is accepted — §1.3; `INTERVAL` appears nowhere
in the system, VMP Part 8 res. 7.)

5.3.1. **Float value semantics (critique F7; NaN value amendment).** For
`Float64` values, in both forms: **NaN is an accepted stored value.** A NaN
value float is canonicalized by the core's writer boundary to the one quiet
NaN `0x7ff8000000000000` (canonical_encoding §3.3.1, value half) and stored
as such — the importer passes it through unrejected. This is a v1.0
pre-public amendment (allowed under §3.2's additive-later / reader-safe rule):
the *value* half of the original NaN ban is reopened; the *axis* half is
unchanged (a NaN in a dimension/coordinate column is still illegal, but no
value-column NaN is). **NaN is still NOT absence:** absence is a NULL value
cell (`long`, §5.4) or an unemitted cell — a NaN is a real value, not a hole,
so **NaN-as-absent padding remains forbidden** in the sense that a producer
must not use NaN to *mean* "missing"; and a dense grid stays a **complete
grid** whether or not a cell is NaN (a NaN cell keeps the entity on the array
tier — canonical_encoding §3.3.3). **±Inf are legal stored values.** **−0.0
is accepted** on the wire and normalized to `+0.0` by the core's writer
boundary — producers need not pre-normalize.

5.4. **Null rules** (mirror the core exactly): a null in the `entity` column
or any dimension column → E6 (index positions must be present — canon.rs).
A null in the **value** column is legal and means **absent**: the core's
canonicalization step 1 (NULL ≡ absent, VMP Part 3) drops that index tuple.
An entity all of whose value cells are null → E6 (surfaced from the core:
"indexed value has no non-null cells"). At **rank 0**, an entity appearing
in more than one row (more than one non-null value) is likewise E6,
surfaced from the core's distinct duplicate-entity guard ("rank-0 scalar
must have exactly one non-null value, got {n}" — canon.rs; critique F16).
A segment whose batches total zero rows is an error (§2.3) — there is no
zero-row write and no metadata-only auto-create.

5.5. Duplicate index tuples within one entity are **not** a format-level
check; the core's pipeline rejects/handles them per VMP Part 3, and the
importer MUST surface that error verbatim (E6 class).

5.6. Sortedness is NOT required: canonicalization sorts (step 3). Producers
SHOULD emit rows grouped by entity for locality, and MAY dictionary-encode
`entity` and label columns (§2.4).

---

## 6. Payload batch schema — `dense` form

6.1. `spinal:form` = `dense` is the `(entity, axes, flat array)` shape (VMP
Part 6; plan Part 4). **One row = one entity's complete value.** Columns, in
order:

| position | field name | Arrow type | content |
|---|---|---|---|
| 0 | `entity` (SHOULD) | `Utf8` | entity name |
| 1 … rank | *the dimension name* | `List<T>` where `T` is the §5.2 type for the kind | that dimension's **axis coordinates, in axis order** |
| last | `value` (SHOULD) | `List<Float64>` | the flat payload |

Rank MUST be 1–4 (a rank-0 dense segment is meaningless → E6; use `long`).

6.2. **Row-major law.** `len(value list) = ∏ len(dim lists)` for every row
(else E8), and the flat payload is laid out **row-major in declared-dimension
order — last dimension varies fastest** — exactly `spec/reconstruction.md`
§2, whose fixtures (including the rank-2/3 transpose traps) apply to
producers of this form verbatim. Julia producers: `Arrow.jl` will happily
serialize a column-major `vec(A)` — **transpose first**; this is the named
hazard. Additionally (critique F9), the entries within each dim list MUST
be **distinct**: a duplicate coordinate would expand to duplicate index
tuples carrying different values, which the core rejects far from the
actual mistake (`DuplicateTuple`, a confusing per-entity canonicalization
error) — the importer MUST instead fail E8-class, naming the row, the
dimension, and the offending coordinate.

6.3. **The importer expands, the core canonicalizes.** The importer MUST
expand each dense row into logical-long tuples (the mechanical inverse
walk of reconstruction §2 — structural, no interpretation) and feed the same
core path as `long`. The core then re-derives axes and tier from the data as
always — the dense form is a **transport optimization only** (it avoids
materializing `n_entities × n_cells` repeated coordinate cells on the wire),
never a tier instruction; the library, not the caller, decides the tier (VMP
Part 6). This is what keeps the no-second-canonicalizer invariant intact.
Three consequences to state explicitly (critique F17):

- **Producer-sent axis order is pairing-only, not stored order.** The dim
  lists exist to pair coordinates with payload cells; canonicalization may
  reorder the *stored* payload (sort step 3; domain-ordinal ordering for
  labels) while preserving the tuple→value mapping. Producers MUST NOT
  assume their list order is what reads back.
- **The expansion is a third implementation of the reconstruction law**
  (alongside `reconstruct.rs` and the SQL view) and MUST be validated
  against the same `conformance/reconstruction_fixtures/*` run in the
  **inverse direction** (dense wire row → expected long tuples), not only
  the producer direction §6.2 aims the fixtures at.
- **Per-row (per-entity) expansion is RECOMMENDED** over whole-segment
  materialization — otherwise the dense form's wire-size win is repaid as
  importer RAM.

6.4. **Null rules:** nulls are forbidden everywhere in dense form — entity,
dim lists, list slots, and value-list slots (E6/E8). Absence is expressed by
not being rectangular, and "not rectangular" is exactly what `long` is for
(NULL-elimination happens producer-side by simply not emitting the cell).
A null *cell* in a dense payload has no defined meaning and MUST NOT be
given one. **NaN is not absence, but it IS a legal value** (critique F7; NaN
value amendment, §5.3.1): a NaN payload cell is stored (canonicalized to the
one quiet NaN), so a dense block that legitimately contains NaN values is
fine and stays on the array tier. What remains forbidden is using NaN to
*mean* absence — a producer must not pad a **ragged** block with NaN to fake
rectangularity; genuinely absent cells make the block non-rectangular and
MUST use `long` form (where absence is simply an unemitted cell). Each entity
MUST appear in at most one row per segment (E6).

6.5. **Payload type is `List<Float64>` only in 1.0** (**OPEN-Q3-8**). The
dense wire form exists for the bulk-float result workload; int/bool/string
dense payloads have no producer today and int-dense storage is itself only a
door (`values_int`, reconstruction §1). `List<Int64>` is reserved as a minor
addition (§3.4). *Alternative:* admit all §5.3 measure kinds as list payloads
now — costs importer surface for no known producer.

6.6. **Ship `dense` in 1.0 at all?** (**OPEN-Q3-7**) Recommendation: yes.
Part 4's CLI sketch names both forms; the expansion is small, is pinned by an
existing normative spec + fixtures (reconstruction), and rectangular
producers (FlexTool-style ladders, 1 000 × 8 760 series) avoid materializing
millions of repeated coordinate cells on the wire. *Alternative:* freeze `form`
= `long` only for 1.0 and add `dense` as `1.1` (the §3.2 policy makes that
additive and clean); choose this if implementation time before the SpineOpt
integration milestone is the binding constraint — SpineOpt's own emission is
long-shaped rows today (`julia_writer_strategy_research.md` §1).

---

## 7. Label-domain carriage

7.1. A `label` dimension's **ordered domain** (the ordinal source, VMP
Part 3 step 2) rides as Arrow **field metadata** on that dimension's field:
key **`spinal:label_domain`**, value a **JSON array of the ordered labels**
(e.g. `["low","mid","high"]`) — byte-for-byte the channel `write_parameter`
already reads (decision D; api.rs `effective_domains`). This is the **only**
domain channel over the wire: the CLI passes an empty in-process `domains`
arg, so field metadata is not a fallback here but the path. Present labels
take their ordinal from this list. The domain MUST be a **superset of the
present labels**: it MAY contain labels not present in the data (that is
its point), but a present label absent from the domain is an error (E6,
surfaced from the core: `LabelNotInDomain`, canon.rs). Malformed JSON → E6
(the core's existing "bad spinal:label_domain" error, surfaced).

7.2. `spinal:label_domain` is **REQUIRED on every label dimension**, in both
`long` and `dense` form; a label dimension without it → E6. This aligns the
spec with the shipped core (critique F3): the core hard-errors on a label
dimension with no domain (`CanonError::MissingDomain` — "missing ordered
domain for label dimension at position {0}", canon.rs), and since §7.1
fixes the CLI's in-process `domains` arg as empty, there is no other source.
*Rejected alternative:* a derive-from-present-labels fallback — it would
require the core to grow a pinned derivation order (sorted?
first-appearance?), which is new canonicalization surface; if ever wanted,
it is a core-first change and a minor door.

7.3. In `dense` form the same key attaches to the `List<Utf8>` dimension
field (the outer field). Note the redundancy is only apparent: the per-row
dim list is the *axis* (which labels, in which axis order); the domain is
the *ordinal universe* — they differ exactly when the axis is a subset of
the domain.

---

## 8. Importer↔CLI seam (informative summary, normative where flagged)

8.1. Per the Part-4 sketch, `spinal-db import <file> --db <path>` with
`--alternative` (see §4.3), `--message` (commit message; may be empty),
`--force-replace` (maps to `WriteOptions.force_replace` for **all** segments)
and `[--class] [--parameter]`. With the segmented container carrying targets,
`--class`/`--parameter` become **filters**: when given, segments not matching
are an error, not skipped (**OPEN-Q3-6b**, minor: erroring keeps "I imported
what you pointed at" true; the alternative — skip-with-warning — makes them
selectors, which is a different feature).

8.2. The commit's `user` MUST be non-empty (core guard, jkiviluo
2026-08-18). The Part-4 sketch has no `--user` flag — the CLI SHOULD default
to the OS username with a `--user` override. **Flagged as a sketch gap**, not
resolved here (it is a CLI-surface question, not a format one).

---

## 9. Error semantics and atomicity (normative for the importer)

9.1. Error classes (stable identifiers for tests and for the conformance
fixtures; text is free, class is not):

| class | condition | when detected |
|---|---|---|
| E1 | input is not an IPC stream (e.g. `ARROW1` file magic, garbage, **or an empty/0-byte file** — critique F16) | before any DB touch |
| E2 | `spinal:format` missing or ≠ `spinal-db-bulk` | per segment, before its writes |
| E3 | `spinal:format_version` missing/unparseable, MAJOR unsupported, or segments disagree | per segment, before writes |
| E4 | required metadata key missing; unknown `spinal:form` value; alternative conflict/absence (§4.3) | per segment |
| E5 | **warning, not error**: unknown `spinal:` key (§3.2) | logged, import continues |
| E6 | malformed payload: wrong/unsupported Arrow type (incl. `Timestamp(Nanosecond)`, §5.2.1; run-end encoding, §2.4), rank > 4, null where forbidden (a NaN VALUE is **not** an error — §5.3.1), zero-row segment (§2.3), misplaced `entity`/`value` column name (§5.1.1), missing/invalid `spinal:label_domain` or present label not in domain (§7), duplicate dense entity, rank-0 duplicate entity (§5.4), trailing garbage between segments, core-surfaced canonicalization errors | per batch/row |
| E7 | duplicate `(class, parameter, alternative)` target across segments, compared case-insensitively (§4.2) | container validation |
| E8 | dense length ≠ product of dim-list lengths; duplicate entry within a dim list (§6.2) | per row |
| E9 | database busy / write-write conflict with retry budget exhausted (core `with_write_txn_retrying`) | commit |
| E10 | container truncation/integrity (§2.2.1): segment ends at bare EOF without the end-of-stream marker; `spinal:segment_count` values disagree across segments; actual segment count ≠ declared | per segment boundary / end of container |

Any error other than E5 aborts the import with a nonzero exit code and a
message naming the segment (by ordinal and `class:parameter`), batch, and —
where known — row. Auto-create events (decision B) and `TimezoneNormalized`
tz-instant normalizations (§5.2.2) are **warnings on stdout, exit 0**,
mirroring the core's `Warning{…}` reports.

9.2. **Atomicity: one transaction per import invocation** (**OPEN-Q3-6**,
the load-bearing one). The entire container — all segments — commits as
**one** core transaction with **one** commit row, or nothing persists. This
is what Part 4 already promises ("full canonicalizer + write protocol in one
transaction") and what the producer's failure model relies on: SpineOpt's
crash-recovery story is "the file is retryable"
(`julia_writer_strategy_research.md` §4.i), which only holds if a failed
import leaves **zero** state — half-imported parameters would make the re-run
semantics of an auto-creating, history-keeping write protocol genuinely
ambiguous. Validation SHOULD be front-loaded (E1–E4/E7/E10 for *all*
segments before the first write) so common failures never open the RW lock;
note (critique F14) that front-loading over a stream container requires a
**full sequential pre-scan** (streams cannot be skipped without decoding) —
i.e. a validate pass plus an ingest pass, or full buffering; the
implementation agent should budget for two passes. Even a late E6/E8 MUST
roll back everything. Two consequences of whole-invocation atomicity worth
stating plainly (critique F14): (a) one transaction per invocation holds
DuckDB's **process-exclusive RW lock for the entire canonicalize+write** of
the container — at SpineOpt scale (10⁶–10⁸ cells) that can be minutes
during which no editor or reader can open the file; this is accepted for a
once-per-run batch job, and stated so nobody discovers it as a bug. (b) On
a write-write conflict the core retries the **whole closure** (fresh state
per attempt, conn.rs); the importer MUST therefore be able to re-produce
every batch on retry. It SHOULD **re-read the file** (local and immutable
for the duration) rather than hold decoded batches — at container scale,
held batches are a multi-GB footprint.
*Alternative:* one transaction per **segment** — matches today's
`write_parameter` surface with zero core changes and bounds memory, at the
cost of partial-import states and one commit row per parameter (a SpineOpt
run would stamp ~30 commits instead of 1). Rejected for the retry/audit
reasons above, but note the **core-surface consequence**: whole-invocation
atomicity requires a new core entry point (`ingest`, Part 4's
`core::ingest_ipc`) that runs the per-parameter resolve+canon+write sequence
for many parameters **inside one** `with_write_txn_retrying` closure —
today's public `write_parameter` opens its own transaction per call and
cannot be composed into a larger one. This is a refactor (extract the inner
body), not a redesign; flagged for the implementation agent.

9.3. **No partial batch is ever silently dropped, no unknown construct is
ever silently skipped** (E5's warn-and-ignore for *optional metadata keys* is
the single sanctioned exception, and it must be visible in the output).

---

## 10. Worked example (informative)

A SpineOpt-style rolling-run result container with two segments: a rank-2
long segment (flows per unit__node over stochastic scenario × time) and a
rank-1 dense segment (an 8 760-cell price series, shown truncated).
Invocation: `spinal-db import results.arrows --db model.sdb --alternative
run_2026-08-18 --message "yearly dispatch"`.

**Segment 1** — stream 1 of the file. Schema:

```
schema metadata:
  "spinal:format"          = "spinal-db-bulk"
  "spinal:format_version"  = "1.0"
  "spinal:segment_count"   = "2"
  "spinal:class"           = "unit__node"
  "spinal:parameter"       = "flow"
  "spinal:form"            = "long"
  (no "spinal:alternative" — the CLI flag supplies run_2026-08-18)

fields:
  0: "entity"    Utf8                       (non-null)
  1: "scenario"  Utf8                       field metadata:
       "spinal:label_domain" = "[\"s1\",\"s2\",\"s3\"]"   (required — §7.2)
  2: "t"         Timestamp(Millisecond)     (naive; Arrow.jl's natural
                                             DateTime output — the importer
                                             upcasts ×1000 to µs, §5.2.1)
  3: "value"     Float64
```

One record batch (rows shown as JSON-ish tuples):

```
("plant_a__node_x", "s1", 2030-01-01T00:00:00, 12.5)
("plant_a__node_x", "s1", 2030-01-01T01:00:00, 13.0)
("plant_a__node_x", "s2", 2030-01-01T00:00:00, 11.9)
("plant_a__node_x", "s2", 2030-01-01T01:00:00, null)   # absent cell → dropped
("plant_b__node_x", "s1", 2030-01-01T00:00:00,  7.25)
...
```

The core derives per-entity axes (the hourly progression collapses to a
parametric datetime axis; `s1 < s2` ordinals from the domain), picks the tier
(ragged → long tier for plant_a because s2 lacks 01:00), and writes under the
one commit. `plant_b__node_x` not existing yet is an AutoCreated warning, not
an error.

**Segment 2** — begins immediately after segment 1's end-of-stream marker.
(Producer note: each segment was written as its own single-stream file —
SpineOpt's rolling `Arrow.append` runs against the per-parameter file only —
and the finished streams were concatenated byte-level; appending to the
concatenated container would corrupt it, §2.2.2.)

```
schema metadata:
  "spinal:format"          = "spinal-db-bulk"
  "spinal:format_version"  = "1.0"
  "spinal:segment_count"   = "2"
  "spinal:class"           = "node"
  "spinal:parameter"       = "price"
  "spinal:form"            = "dense"

fields:
  0: "entity"  Utf8
  1: "t"       List<Timestamp(Microsecond)>
  2: "value"   List<Float64>
```

Rows (one per entity; lists truncated):

```
("node_x", [2030-01-01T00:00, 2030-01-01T01:00, ... 8760 stamps],
           [31.7, 30.2, ... 8760 values])            # len(value) == len(t)
("node_y", [2030-01-01T00:00, ... 8760 stamps],
           [28.0, ... 8760 values])
```

The importer expands each row to `(entity, t, value)` tuples (row-major —
trivial at rank 1) and feeds the same core path; the hourly axis
canonicalizes parametric, the values land in the array tier — decided by the
core, not by the `dense` label. Before committing, the importer has checked
that both segments end in the explicit end-of-stream marker and that the
container holds exactly the 2 segments both declare (§2.2.1) — a producer
crash that lost segment 2, or segment 2's tail, is E10, not a silent
1-parameter import. Both segments, all warnings, one commit; any failure
anywhere → the database is untouched and the file is simply re-imported
after the fix.

---

## 11. Decisions for jkiviluo (collected OPEN-Q3 markers)

> **RESOLVED — jkiviluo, 2026-08-18: all of OPEN-Q3-1..9 signed off as
> recommended below.** Three clarifications captured during sign-off:
> - **Q3-1 (file layout):** accepting Arrow's file layout later is *additive*
>   — a reader-side widening (sniff the `ARROW1` magic), safe as a minor under
>   §3.2; existing stream files are unaffected. 1.0 is stream-only to keep the
>   first importer small.
> - **Q3-6 (transaction granularity):** ONE import invocation = ONE
>   all-or-nothing DB transaction/commit; the invocation is atomic and cannot
>   half-succeed. The **connecting tool chooses the transaction boundary** by
>   choosing how finely it batches its `import` calls: import once at run-end =
>   all-or-nothing (a mid-run producer crash loses everything, because nothing
>   was imported yet); import per solve/rolling-window = each window durably
>   committed before the next, so partial progress survives a later crash.
>   SpineOpt already emits Arrow per rolling window, so per-window import is the
>   natural granularity.
> - **Q3-8 (dense completeness + NaN):** "dense = every cell populated" is
>   *our* one-canonical-form rule, not an Arrow limitation (Arrow lists could
>   hold null slots); holey data uses `long`. **NaN-value amendment (post-Q3):**
>   the original "NaN banned" stance is superseded for *values* — a NaN value is
>   now accepted and canonicalized to one quiet NaN (`0x7ff8000000000000`),
>   which restores well-defined value equality (the many-bit-pattern /
>   `NaN != NaN` hazard is removed by collapsing to one pattern, aligned with
>   DuckDB's total order; no cross-key value dedup relies on it — see
>   canonical_encoding §3.3). "Missing" is still absence/NULL, never a stored
>   NaN. Same additive-later logic as Q3-1 applies to Q3-8's reserved
>   `List<Int64>` and Q3-9's REE support.

- **OPEN-Q3-1 — container = IPC *stream* format, file format rejected**
  (§2.1). *Rec:* stream only — Arrow.jl's append path (SpineOpt's rolling
  writer) is stream-only; sequential one-pass ingest never needs the footer.
  *Alt:* also sniff-accept the file format (`ARROW1` magic) — doubles the
  parse surface for no known producer.
- **OPEN-Q3-2 — multi-parameter via concatenated IPC streams ("segments"),
  one `(class,parameter)` per segment** (§2.2). *Rec:* segmented container —
  **empirically confirmed by the critique**: stock **writers** in all three
  ecosystems (arrow-rs, PyArrow, Arrow.jl — verified); stock **readers**
  verified to delimit segments exactly for arrow-rs 58.4.0 and PyArrow
  25.0.1, including non-seekable sources. Justification corrected per
  critique F10: Arrow.jl's stock *reader* cannot delimit segments (it
  buffers the source) — irrelevant to the importer (Rust is the only
  normative reader; Julia is a producer) but the Julia round-trip
  conformance test needs a byte-offset splitter. The design now includes
  the F1 integrity rules (per-segment EOS verification +
  `spinal:segment_count`, §2.2.1 — stock readers accept silent truncation,
  so the format must defend) and the F11 `Arrow.append` corruption warning
  (§2.2.2). *Alt:* one target per file + multi-file CLI invocation.
- **OPEN-Q3-3 — starting version `1.0`, MAJOR.MINOR only** (§3.1). *Rec:*
  `1.0` — the boundary is declared stable to external clients from day one —
  **contingent on the §3.2 structural-unrecognizability rule (critique
  F2)**: as previously drafted, the minor-version policy could silently
  reinterpret data (the reserved `spinal:index_kind` door), and freezing
  that at 1.0 would have poisoned the whole 1.x line; with the F2 rule in
  place (interpretation-modifying constructs are MAJOR, `spinal:index_kind`
  removed from the minor list), 1.0 is right. *Alt:* `0.1` until
  post-prototype freeze.
- **OPEN-Q3-4 — targets carried per-segment in schema metadata** (§4.2), and
  duplicate `(class,parameter,alternative)` across segments is an error,
  compared **case-insensitively** (critique F5 — core resolution folds via
  `lower()`, so `Flow`/`flow` are one target).
  *Rec:* per-segment — matches the core's write unit; per-batch/per-row
  targeting is impractical or shape-impossible. *Alt:* per-row discriminator
  columns (rejected: incompatible with heterogeneous dimension schemas).
- **OPEN-Q3-5 — alternative: segment metadata optional, CLI flag as the
  other source; both present ⇒ must match, else error** (§4.3). *Rec:* as
  stated — no silent precedence. *Alt:* CLI strictly overrides metadata.
- **OPEN-Q3-6 — atomicity: ONE transaction + ONE commit row per import
  invocation (whole container), all-or-nothing** (§9.2). *Rec:* whole-
  invocation — Part 4 already promises it, and "the file is retryable" (the
  producer's crash model) requires zero residual state. Requires the new
  `core::ingest` entry (per-parameter write sequence composed inside one
  retried transaction — `write_parameter` cannot be called as-is N times).
  **Consequences to accept knowingly (critique F14):** the process-exclusive
  RW lock is held for the entire canonicalize+write — minutes at SpineOpt
  scale (10⁶–10⁸ cells), during which no editor/reader can open the file
  (fine for a once-per-run batch job, now stated in §9.2); on conflict
  retry the importer re-reads the file rather than holding decoded batches
  (multi-GB otherwise); and front-loaded validation means a sequential
  validate pass before the ingest pass (streams cannot be skipped without
  decoding).
  *Alt:* one transaction per segment — no core change, but partial imports
  and ~30 commit rows per SpineOpt run.
  - **OPEN-Q3-6b** (minor, §8.1): `--class`/`--parameter` flags act as
    assertions (mismatching segment ⇒ error). *Alt:* selectors
    (skip-with-warning).
- **OPEN-Q3-7 — `dense` form ships in 1.0** (§6.6). *Rec:* ship it — the
  expansion is pinned by `spec/reconstruction.md` + fixtures; rectangular
  producers get a large wire-size win. The critique attacked the
  no-second-canonicalizer claim directly and it **holds** (F17): expansion
  is order-preserving pairing, so no transposition channel exists; §6.3 now
  states that producer axis order is pairing-only, requires the importer's
  expansion to run against the reconstruction fixtures in the inverse
  direction, and recommends per-row expansion (RAM). *Alt:* `long`-only
  1.0, `dense` as an additive 1.1 (clean under the §3.2 policy) if schedule
  is the constraint.
- **OPEN-Q3-8 — dense payload `List<Float64>` only in 1.0** (§6.5). *Rec:*
  float-only — the only producer workload is bulk float results; `List<Int64>`
  reserved as a minor door. Float semantics are now pinned (critique F7 +
  NaN-value amendment, §5.3.1/§6.4): a **NaN value is accepted and
  canonicalized** to `0x7ff8000000000000` (aligned with canonical_encoding
  §3.3's reopened value half), NaN-as-*absence* padding still forbidden (a NaN
  is a value, not a hole), ±Inf legal, −0.0 normalized by the core. *Alt:*
  admit all measure kinds as lists now.
- **OPEN-Q3-9 — importer MUST accept dictionary encoding + ZSTD and
  LZ4_FRAME buffer compression; REE forbidden and MUST-rejected** (§2.4;
  critique F15 hardened MAY-reject to MUST-reject so importability is not
  implementation-defined). *Rec:* as stated (matches the Toolbox transition
  plan's producer constraints; verified implementable — arrow 58.4.0 with
  `ipc_compression` reads PyArrow-written zstd + dictionary streams).
  **Dependency decision recorded (critique F12):** `duckdb::arrow` ships
  without the `ipc` feature, so the importer takes a direct `arrow` (or
  `arrow-ipc`) dep with `ipc` + `ipc_compression`, pinned to duckdb's arrow
  major (58.4.0) — R2 revived, Cargo.toml comment to be amended. *Alt:*
  uncompressed-only 1.0 (smaller importer dependency surface, but SpineOpt's
  existing pipeline already compresses).

**Sketch gaps surfaced (not format decisions, listed for the morning):**
(a) the core needs `ingest` (multi-parameter, one retried transaction) —
`write_parameter` self-opens its transaction and cannot compose (§9.2);
(b) the Part-4 CLI sketch lacks `--user`, but the core rejects empty `user`
(§8.2); (c) `ValueKind::Duration` exists in core types but has no accepted
Arrow measure type in `write_parameter` — hence duration's exclusion from
1.0 (§1.3) is inherited from the core surface, not chosen here;
(d) (critique F12) the root `Cargo.toml` "no direct arrow" comment and the
R2 mitigation must be amended for the importer's direct
`arrow`/`ipc`+`ipc_compression` dependency, version-locked to
`duckdb::arrow`'s major (58.4.0 today).

---

## 12. Revision log

- **2026-08-18 (NaN-value amendment)** — reopened the **value** half of the
  NaN ban (jkiviluo decision). A NaN VALUE is now **accepted** and canonicalized
  by the core to the one quiet NaN `0x7ff8000000000000`; it is no longer E6.
  Absence is still NULL/omission (a NaN is a value, not a hole), and dense stays
  a complete grid — a NaN cell keeps the entity on the array tier. Axis/
  coordinate NaN is unchanged (still illegal). Updated §5.3.1, §6.4, §9.1 E6
  table, and §11 OPEN-Q3-8. A v1.0 pre-public amendment, safe under §3.2
  (reader-safe / additive-later); see canonical_encoding §3.3.1–§3.3.3.
- **2026-08-18 (draft v2)** — applied the adversarial critique
  `specs/arrow_ipc_bulk_format_critique.md` (findings F1–F17, empirical
  verifications against arrow-rs 58.4.0 / PyArrow 25.0.1 / Arrow.jl 2.8.1).
  Changes by finding:
  - **F1** → §2.2.1 (new: per-segment EOS verification + required
    `spinal:segment_count` + temp-path/rename guidance), §3.1/§4.1 tables,
    §9.1 (new class E10), §10 closing paragraph.
  - **F2** → §3.2 (structural-unrecognizability rule; `spinal:requires`
    recorded as rejected alternative), §3.3, §3.4 (`spinal:index_kind`
    removed from the minor list — MAJOR door), §4.4, §5.2.2 (calendar/tz
    carriage is MAJOR).
  - **F3** → §7.1 (domain MUST be superset of present labels;
    `LabelNotInDomain` surfaced), §7.2 (`spinal:label_domain` REQUIRED on
    every label dimension, aligned with core `MissingDomain`), §9.1 E6, §10.
  - **F4** → §5.2/§5.2.1 (timestamps widened to s/ms/µs with mandatory
    lossless integer upcast to µs; ns rejected with fix-naming error),
    §5.3, §9.1 E6, §10 (example uses Arrow.jl's natural ms).
  - **F5** → §4.2 (E7 duplicate-target comparison case-insensitive, DuckDB
    `lower()` fold pinned), §9.1 E7, §11 Q3-4.
  - **F6** → §2.3 + §5.4 (zero-row segment: single normative verb — hard
    error, MUST NOT import or auto-create, matching the core's empty-batch
    rejection; schema-seeding recorded as rejected alternative), §9.1 E6.
  - **F7** → §5.3.1 (new: NaN rejected, NaN-as-absent forbidden, ±Inf
    legal, −0.0 normalized by core — aligned with canonical_encoding
    §3.3.1), §6.4, §9.1 E6, §11 Q3-8.
  - **F8** → §5.1.1 (new: `entity`/`value` name tripwires — misplaced
    literal names are E6), §9.1 E6.
  - **F9** → §6.2 (dim-list entries MUST be distinct, E8-class with
    row/dimension/coordinate in the error), §9.1 E8.
  - **F10** → §2.2 justification corrected (stock writers everywhere; stock
    readers verified arrow-rs + PyArrow; Arrow.jl read-back needs a
    splitter; `try_new` not `try_new_buffered`), §11 Q3-2.
  - **F11** → §2.2.2 (new: `Arrow.append` on a multi-segment container
    corrupts silently — append only to single-stream files, then `cat`),
    §10 producer note.
  - **F12** → §2.4 implementation consequence + §11 sketch gap (d): direct
    `arrow` dep with `ipc`+`ipc_compression` pinned to `duckdb::arrow`'s
    58.4.0; R2 revived.
  - **F13** → §5.2.2, §9.1: tz-annotated timestamps are no longer E6 — the
    core (post-critique F6 fix, api.rs) accepts them as UTC instants with a
    per-column `TimezoneNormalized` warning; the spec now matches, and the
    importer surfaces the warning. (Supersedes the critique's
    "importer-only check" note, which predated the core fix.)
  - **F14** → §9.2 (lock-hold duration stated; retry SHOULD re-read the
    file; front-loaded validation = two passes), §11 Q3-6.
  - **F15** → §2.4 (REE: MAY-reject hardened to MUST-reject), §11 Q3-9.
  - **F16** → §9.1 E1 (empty/0-byte file), §5.4 (rank-0 duplicate-entity
    `ScalarRows` condition named).
  - **F17** → §6.3 (axis order is pairing-only; inverse-direction
    reconstruction-fixture requirement; per-row expansion recommended),
    §11 Q3-7.

  OPEN-Q3-1..9 all survive per the critique's verdict; Q3-2 and Q3-3 texts
  amended (F10 justification; F2 contingency). No section numbering
  changed; new subsections: 2.2.1, 2.2.2, 5.1.1, 5.2.1, 5.2.2, 5.3.1.
