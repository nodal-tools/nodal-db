#!/usr/bin/env python3
"""Independent second oracle for the VALUE canonical encoding (spec §9, OPEN-7).

Sibling of `encode_axis.py`: it re-derives every `conformance/value_vectors/
*.json` vector's canonical byte string AND its XXH3-64/seed-0 hash *from the
spec* (`spec/canonical_encoding.md` §9), sharing NO code with the Rust
`encode_value`. Two independent implementations agreeing byte- and hash-for-hash
is the point (CLAUDE.md invariant 2).

The value encoding (OPEN-7, jkiviluo 2026-08-17) folds in the identity KEY and
the position/shape (axis content hashes + per-cell positions), then the measure
component-wise / populated-columns-only (§9.4).

If a vector is missing its `expected_*` fields, this prints the computed values
(a one-shot golden generator) instead of asserting; with goldens present it
asserts. Run:

    .venv/bin/python conformance/oracle/encode_value.py
"""
import json
import math
import pathlib
import struct
import sys

import xxhash

VECTORS_DIR = pathlib.Path(__file__).resolve().parents[1] / "value_vectors"

# value_kind tag per ScalarValue variant (spec §9.3).
_VALUE_KIND_TAG = {
    "Float": "float",
    "Int": "int",
    "Bool": "bool",
    "Str": "string",
    "DateTime": "date_time",
    "Duration": "duration",
}


def _u8(n: int) -> bytes:
    return struct.pack("<B", n)


def _u32(n: int) -> bytes:
    return struct.pack("<I", n)


def _i32(n: int) -> bytes:
    return struct.pack("<i", n)


def _i64(n: int) -> bytes:
    return struct.pack("<q", n)


def _u64(n: int) -> bytes:
    return struct.pack("<Q", n)


def _string(s: str) -> bytes:
    import unicodedata

    b = unicodedata.normalize("NFC", s).encode("utf-8")
    return _u32(len(b)) + b


def _double(x: float) -> bytes:
    if isinstance(x, float) and math.isnan(x):
        raise ValueError("NaN is not an encodable value (spec §3.3.1)")
    x = float(x)
    if x == 0.0:
        x = 0.0
    return struct.pack("<d", x)


def _scalar_variant(sv: dict):
    """A ScalarValue is externally tagged: {"Float": 1.5}, {"Duration": {...}}."""
    (tag,) = sv.keys()
    return tag, sv[tag]


def _measure_bytes(sv: dict) -> bytes:
    tag, payload = _scalar_variant(sv)
    if tag == "Float":
        return _double(payload)
    if tag == "Int":
        return _i64(payload)
    if tag == "Bool":
        return _u8(0x01 if payload else 0x00)
    if tag == "Str":
        return _string(payload)
    if tag == "DateTime":
        return _i64(payload)
    if tag == "Duration":
        return _i32(payload["months"]) + _i64(payload["micros"])
    raise ValueError(f"unknown ScalarValue variant {tag!r}")


def _component_block(components: list) -> bytes:
    """u8 count, then present-only (slot_id:u8, bytes) entries (spec §9.3)."""
    out = _u8(len(components))
    for slot, b in components:
        out += _u8(slot) + b
    return out


def _shape(axis_hashes: list) -> bytes:
    out = _u8(len(axis_hashes))
    for h in axis_hashes:
        out += _u64(h)
    return out


def encode_value(key: dict, content: dict) -> bytes:
    out = _i64(key["entity_id"]) + _i64(key["parameter_definition_id"]) + _i64(key["alternative_id"])
    t = content["type"]
    if t == "Scalar":
        sv = content["value"]
        tag, _ = _scalar_variant(sv)
        out += _string(_VALUE_KIND_TAG[tag])
        out += _shape([])
        out += _component_block([(1, _measure_bytes(sv))])
        return out
    if t == "Array":
        out += _string("float")
        out += _shape(content["axis_hashes"])
        payload = _u32(len(content["values"]))
        for x in content["values"]:
            payload += _double(x)
        out += _component_block([(1, payload)])
        return out
    if t == "Long":
        out += _string(_VALUE_KIND_TAG[content["value_kind"]])
        out += _shape(content["axis_hashes"])
        cells = content["cells"]
        out += _u32(len(cells))
        for cell in cells:
            for p in cell["positions"]:
                out += _i32(p)
            out += _component_block([(1, _measure_bytes(cell["value"]))])
        return out
    raise ValueError(f"unknown content type {t!r}")


def content_hash(data: bytes) -> int:
    return xxhash.xxh3_64_intdigest(data, seed=0)


def check_vector(path: pathlib.Path) -> bool:
    vector = json.loads(path.read_text())
    name = path.name
    desc = vector.get("description", "")
    got = encode_value(vector["key"], vector["content"])
    got_hex = got.hex()
    got_hash = content_hash(got)

    if "expected_canonical_hex" not in vector or "expected_hash_u64" not in vector:
        print(f"GENERATE {name}\n  hex  {got_hex}\n  hash {got_hash}")
        return True

    if got_hex != vector["expected_canonical_hex"]:
        raise AssertionError(
            f"{name}: canonical bytes mismatch ({desc})\n  oracle {got_hex}\n  golden {vector['expected_canonical_hex']}"
        )
    if got_hash != vector["expected_hash_u64"]:
        raise AssertionError(
            f"{name}: content hash mismatch ({desc})\n  oracle {got_hash}\n  golden {vector['expected_hash_u64']}"
        )
    return True


def main() -> int:
    files = sorted(VECTORS_DIR.glob("*.json"))
    if not files:
        print(f"no value vectors found under {VECTORS_DIR}", file=sys.stderr)
        return 1
    for path in files:
        check_vector(path)
    print(f"independent Python value oracle: {len(files)} vectors reproduced byte- and hash-for-hash")
    return 0


if __name__ == "__main__":
    sys.exit(main())
