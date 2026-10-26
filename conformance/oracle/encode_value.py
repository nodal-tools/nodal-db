#!/usr/bin/env python3
"""Independent second oracle for the VALUE canonical encoding (spec §9, OPEN-7).

Sibling of `encode_axis.py`: it re-derives every `conformance/value_vectors/
*.json` vector's canonical byte string AND its XXH3-64/seed-0 hash *from the
spec* (`specs/canonical_encoding.md` §9), sharing NO code with the Rust
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

from nfc16 import nfc

VECTORS_DIR = pathlib.Path(__file__).resolve().parents[1] / "value_vectors"

# value_kind tag per ScalarValue variant (spec §9.3), and per ValueKind
# name (a Long vector's `value_kind`; only the string kind is spelled
# differently: ScalarValue `Str`, ValueKind `String`).
_VALUE_KIND_TAG = {
    "Float": "float",
    "Int": "int",
    "Bool": "bool",
    "Str": "string",
    "String": "string",
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


def _tag(s: str) -> bytes:
    """An identifier string (the value_kind tag): NFC (Unicode 16.0.0, the
    oracle's own `nfc16`), then u32-length-prefixed UTF-8 (spec §3.4). The
    tags are ASCII, so NFC never changes them."""
    b = nfc(s).encode("utf-8")
    return _u32(len(b)) + b


def _str_value(s: str) -> bytes:
    """A string VALUE (v_str): its UTF-8 bytes exactly as written, u32-
    length-prefixed, never normalized (spec §9.5): the value encoding must
    treat two strings as equal exactly when storage does, and storage keeps
    string values byte for byte."""
    b = s.encode("utf-8")
    return _u32(len(b)) + b


# The one canonical quiet-NaN bit pattern for VALUE floats (spec §3.3.1):
# positive quiet NaN, zero payload. Analogous to -0.0 -> +0.0, this makes
# byte-equality well-defined for values (one canonical pattern per value).
_CANONICAL_NAN_BITS = 0x7FF8000000000000


def _double(x: float) -> bytes:
    """§3.3 VALUE double: NaN canonicalized to the one quiet NaN, -0.0 -> +0.0,
    then 8 IEEE-754 binary64 bytes LE. (The AXIS oracle, encode_axis.py, still
    REJECTS NaN — the deliberate axis-vs-value split, §3.3.3.)"""
    x = float(x)
    if math.isnan(x):
        return struct.pack("<Q", _CANONICAL_NAN_BITS)
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
        return _str_value(payload)
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


# Tier tag byte (spec §9.3 step 3): explicit discriminator so scalar / array /
# long encodings can never byte-collide (the value-side analogue of the axis
# form byte §5.2). Emitted right after the value_kind tag, before the shape.
_TIER_SCALAR = 0x00
_TIER_ARRAY = 0x01
_TIER_LONG = 0x02


def encode_value(key: dict, content: dict) -> bytes:
    out = _i64(key["entity_id"]) + _i64(key["parameter_definition_id"]) + _i64(key["alternative_id"])
    t = content["type"]
    if t == "Scalar":
        sv = content["value"]
        tag, _ = _scalar_variant(sv)
        out += _tag(_VALUE_KIND_TAG[tag])
        out += _u8(_TIER_SCALAR)
        out += _shape([])
        out += _component_block([(1, _measure_bytes(sv))])
        return out
    if t == "Array":
        out += _tag("float")
        out += _u8(_TIER_ARRAY)
        out += _shape(content["axis_hashes"])
        payload = _u32(len(content["values"]))
        for x in content["values"]:
            payload += _double(x)
        out += _component_block([(1, payload)])
        return out
    if t == "Long":
        out += _tag(_VALUE_KIND_TAG[content["value_kind"]])
        out += _u8(_TIER_LONG)
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


def _apply_nan_at(content: dict, indices: list) -> dict:
    """Inject NaN into the measure at the given flat indices before encoding
    (JSON has no NaN literal). A Scalar float's sole measure is index 0; an
    Array's payload is indexed by element. Mirrors the Rust value_vectors runner
    so both oracles encode the same NaN-bearing value (§3.3.1)."""
    t = content["type"]
    if t == "Scalar":
        if "Float" not in content["value"]:
            raise AssertionError("nan_at on a Scalar needs a Float measure")
        content["value"]["Float"] = float("nan")
    elif t == "Array":
        for i in indices:
            content["values"][i] = float("nan")
    else:
        raise AssertionError(f"nan_at not supported for content type {t!r}")
    return content


def _apply_nan_bits_at(content: dict, pairs: list) -> dict:
    """Set the measure at each ``[flat_index, "0x…"]`` to the double with exactly
    those bits (NaN sign/signaling/payload variants JSON cannot spell). Same
    indexing as ``nan_at``. Python floats keep the bits through ``struct``."""
    t = content["type"]
    for i, bits in pairs:
        x = struct.unpack("<d", struct.pack("<Q", int(bits, 16)))[0]
        if t == "Scalar":
            if i != 0 or "Float" not in content["value"]:
                raise AssertionError("nan_bits_at on a Scalar needs one Float index 0")
            content["value"]["Float"] = x
        elif t == "Array":
            content["values"][i] = x
        else:
            raise AssertionError(f"nan_bits_at not supported for content type {t!r}")
    return content


def check_vector(path: pathlib.Path) -> bool:
    vector = json.loads(path.read_text())
    name = path.name
    desc = vector.get("description", "")

    content = vector["content"]
    nan_at = vector.get("nan_at")
    if nan_at is not None:
        content = _apply_nan_at(content, nan_at)
    nan_bits_at = vector.get("nan_bits_at")
    if nan_bits_at is not None:
        content = _apply_nan_bits_at(content, nan_bits_at)

    got = encode_value(vector["key"], content)
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
