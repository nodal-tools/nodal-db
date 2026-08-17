# Value canonical-encoding conformance vectors

Language-neutral test vectors for the **value** canonical byte encoding and its
64-bit content hash (`spec/canonical_encoding.md` §9, OPEN-7). Any
implementation MUST reproduce `expected_canonical_hex` and `expected_hash_u64`
byte-for-byte. Runners:

- `crates/spinal-db-core/tests/value_vectors.rs` — the Rust `encode_value`;
- `conformance/oracle/encode_value.py` — an independent Python re-encoder that
  re-derives the same bytes and XXH3-64/seed-0 hash from the spec, sharing no
  code with Rust (run in CI by `crates/spinal-db-core/tests/python_oracle.rs`).

OPEN-7 (jkiviluo, 2026-08-17): the value encoding folds in the identity **key**
(entity/parameter/alternative) and the position/**shape** (axis content hashes +
per-cell long positions), then the measure component-wise / populated-columns-
only (§9.4). Value equality = same key + shape + measure.

## Vector schema

Each `*.json`:

| field | type | meaning |
|-------|------|---------|
| `description` | string | what the vector exercises |
| `key` | object | `{entity_id, parameter_definition_id, alternative_id}` (i64) |
| `content` | object | the value content, tagged by `type` (below) |
| `encode_error` | bool (optional) | if `true`, the encoder MUST error: the runners inject a NaN into the declared scalar `Float` measure and assert rejection (§3.3.1). Such a vector omits the `expected_*` fields |
| `expected_canonical_hex` | string | canonical bytes, lower-case hex |
| `expected_hash_u64` | number | XXH3-64/seed-0 of those bytes, unsigned decimal |

The canonical byte string carries an explicit **tier tag byte** (§9.3 step 3:
`0x00` scalar / `0x01` array / `0x02` long) right after the `value_kind` tag, so
scalar/array/long encodings can never byte-collide (the value-side analogue of
the axis form byte §5.2).

`content.type`:

- `Scalar` `{ value: ScalarValue }`
- `Array` `{ axis_hashes: [u64,…], values: [f64,…] }` (Tier A, float payload)
- `Long` `{ value_kind, axis_hashes: [u64,…], cells: [{ positions:[i32,…], value: ScalarValue }, …] }`

`ScalarValue` is externally tagged: `{"Float":1.5}`, `{"Int":7}`,
`{"Bool":true}`, `{"Str":"peak"}`, `{"DateTime":123}`,
`{"Duration":{"months":1,"micros":2}}`.

The goldens are authoritative literals — no in-test regeneration (same policy as
`../encoding_vectors/README.md`). Changing one is a rehash-class event
(spec §2.4): edit by hand, then confirm both the Rust runner and the Python
oracle agree.
