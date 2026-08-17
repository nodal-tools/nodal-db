# Axis canonical-encoding conformance vectors

Language-neutral test vectors for the axis canonical byte encoding and the
64-bit content hash, as pinned by `spec/canonical_encoding.md`. Any
implementation (the Rust core, the SQL side, a future Python/engine skin) MUST
reproduce `expected_canonical_hex` and `expected_hash_u64` byte-for-byte for
each vector. The authoritative runner is
`crates/spinal-db-core/tests/encoding_vectors.rs`.

Value encodings (spec §9) are **not** covered here — they live in
`conformance/value_vectors/` (OPEN-7 resolved 2026-08-17; runner
`crates/spinal-db-core/tests/value_vectors.rs`, oracle
`conformance/oracle/encode_value.py`).

## Vector schema

Each `*.json` file is one object:

| field | type | meaning |
|-------|------|---------|
| `description` | string | what the vector exercises |
| `axis` | object | the canonical axis (see below); the encoder input |
| `encode_error` | bool (default `false`) | if `true`, the encoder MUST reject this axis (a NaN double); `expected_*` are then absent |
| `expected_canonical_hex` | string | the canonical byte string, lower-case hex (absent iff `encode_error`) |
| `expected_hash_u64` | number | XXH3-64/seed-0 of those bytes, as an unsigned 64-bit decimal (absent iff `encode_error`) |
| `regularity` | object (optional) | a parametric-first *store decision* assertion (§7.1, §10) |

### `axis`

```jsonc
{
  "length": 4,                          // i32, axis.length (spec §5.3)
  "properties": {                       // optional; each key null/absent = property not present (§6)
    "calendar": "noleap", "timezone": null, "crs": null
  },
  "body": { "type": "IntegerParametric", "start": 0, "step": 5 }
}
```

`body.type` selects the kind + form (spec §7). Variants and their fields:

- `DatetimeParametric` `{ start_ts: i64 µs, step: {months:i32, micros:i64} }`
- `DatetimeMaterialized` `{ coords: [i64 µs, …] }`
- `IntegerParametric` `{ start: i64, step: i64 }`
- `IntegerMaterialized` `{ coords: [i64, …] }`
- `FloatParametric` `{ start: f64, step: f64 }`
- `FloatMaterialized` `{ coords: [f64, …] }`
- `Label` `{ positions: [{ ordinal: i32, label: str }, …] }`
- `Period` `{ positions: [{ start_ts: i64, end_ts: i64, years_presented: f64|null }, …] }`
- `TimePattern` `{ positions: [{ ordinal: i32, terms: [{ unit, range_start:i16, range_end:i16 }, …] }, …] }`

Timestamps are µs since the Unix epoch (naive). Doubles are JSON numbers; the
1-ULP fixture relies on shortest-round-trip decimals parsing back to the exact
`f64`.

### `regularity`

An optional store-decision assertion checked by the reproduce helpers, tagged by
`kind`:

- `{ "kind": "Integer", "coords": [...], "expect_parametric": bool }`
- `{ "kind": "Float", "coords": [...], "expect_parametric": bool }`
- `{ "kind": "Datetime", "coords": [...], "step": {months,micros}, "expect_parametric": bool }`

`expect_parametric = true` asserts the coordinates reproduce under the pinned
reconstruction (so the axis MUST be stored parametric); `false` asserts they do
not (MUST be materialized).

## The goldens are authoritative — no in-test regeneration

`expected_canonical_hex` / `expected_hash_u64` are **fixed literals**, not
values the test can rewrite. The runner (`encoding_vectors.rs`) only ever
*asserts* the encoder reproduces them; there is deliberately **no `REGEN`
path**, because a self-regenerating oracle would let a spec-divergent encoder
edit stay green (it would just overwrite its own goldens). Two independent
implementations must agree on these bytes:

- `crates/spinal-db-core/tests/encoding_vectors.rs` — the Rust encoder;
- `conformance/oracle/encode_axis.py` — an independent Python re-encoder that
  re-derives the same bytes and XXH3-64 hash from the spec, sharing no code with
  Rust (run in CI by `crates/spinal-db-core/tests/python_oracle.rs`).

Changing a golden is a **rehash-class event** (spec §2.4): edit the JSON by hand
against the spec, then confirm BOTH the Rust runner and the Python oracle agree
on the new value. Never machine-overwrite these fields from the encoder under
test.
