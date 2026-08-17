#!/usr/bin/env python3
"""Independent second oracle for the axis canonical encoding.

This is the "second canonicalizer for validation" the conformance suite is
designed around (CLAUDE.md invariant 2; VMP Part 3): it re-derives every
`conformance/encoding_vectors/*.json` vector's canonical byte string AND its
XXH3-64/seed-0 content hash *from the spec* (`spec/canonical_encoding.md`),
sharing NO code with the Rust encoder. Two independent implementations that
agree byte- and hash-for-hash is the whole point — a spec-divergent edit to the
Rust encoder that also regenerated its own goldens would still fail here.

It deliberately uses only:
  - CPython `unicodedata.normalize("NFC", ...)`  for §3.4 NFC (see the version
    caveat below),
  - `struct` little-endian packing               for §3.1/§3.2/§3.3/§3.5,
  - the `xxhash` PyPI package                     for §2 XXH3-64/seed-0.

Run standalone (exit 0 iff every vector reproduces):

    .venv/bin/python conformance/oracle/encode_axis.py

`xxhash` is required (`.venv/bin/pip install xxhash`); it is the only
third-party dependency. The Rust side runs this via `python_oracle.rs`.

NFC-version caveat: CPython's `unicodedata` tracks the interpreter's bundled
Unicode version (3.12 -> 15.1.0), whereas the spec pins Unicode 16.0.0
(§3.4.1.1). The committed NFC vector (18) is a stable basic composition
(U+0065 U+0301 -> U+00E9) unaffected by that skew, so this oracle validates it
correctly today; the version-pinning debt is tracked in
`specs/rust_impl_session_decisions.md` ("Deferred pre-freeze debt").
"""
import json
import math
import pathlib
import struct
import sys

import xxhash

VECTORS_DIR = pathlib.Path(__file__).resolve().parents[1] / "encoding_vectors"

# Property-id registry, ascending id (spec §6.2): calendar=1, timezone=2, crs=3.
_PROPERTY_IDS = (("calendar", 1), ("timezone", 2), ("crs", 3))

# axis.kind -> pinned lowercase ASCII tag (spec §5.1.1). Parametric/materialized
# variants share the kind tag; the form byte (§5.2) distinguishes them.
_KIND_TAG = {
    "DatetimeParametric": "datetime",
    "DatetimeMaterialized": "datetime",
    "IntegerParametric": "integer",
    "IntegerMaterialized": "integer",
    "FloatParametric": "float",
    "FloatMaterialized": "float",
    "Label": "label",
    "Period": "period",
    "TimePattern": "time_pattern",
}
_PARAMETRIC = {"DatetimeParametric", "IntegerParametric", "FloatParametric"}


class NanForbidden(ValueError):
    """A NaN reached the encoder (spec §3.3.1): reject, never hash."""


def _u8(n: int) -> bytes:
    return struct.pack("<B", n)


def _i16(n: int) -> bytes:
    return struct.pack("<h", n)


def _i32(n: int) -> bytes:
    return struct.pack("<i", n)


def _u32(n: int) -> bytes:
    return struct.pack("<I", n)


def _i64(n: int) -> bytes:
    return struct.pack("<q", n)


def _string(s: str) -> bytes:
    """§3.4: NFC (Unicode), UTF-8, u32-LE byte-length prefix, then the bytes."""
    nfc = unicodedata_nfc(s)
    b = nfc.encode("utf-8")
    return _u32(len(b)) + b


def unicodedata_nfc(s: str) -> str:
    # Isolated so the NFC source is obvious and swappable if the frozen
    # Unicode-16 table is later vendored (§3.4.1.1).
    import unicodedata

    return unicodedata.normalize("NFC", s)


def _double(x: float) -> bytes:
    """§3.3: reject NaN; normalize -0.0 -> +0.0; 8 IEEE-754 binary64 bytes LE."""
    if isinstance(x, float) and math.isnan(x):
        raise NanForbidden("NaN is not an encodable value (spec §3.3.1)")
    x = float(x)
    if x == 0.0:  # true for both +0.0 and -0.0
        x = 0.0
    return struct.pack("<d", x)


def _property_block(props: dict) -> bytes:
    """§6.3: u8 count of present (non-null) props, then present-only, asc id."""
    present = [(pid, props.get(name)) for name, pid in _PROPERTY_IDS
               if props.get(name) is not None]
    out = _u8(len(present))
    for pid, value in present:
        out += _u8(pid) + _string(value)
    return out


def _body(body: dict) -> bytes:
    t = body["type"]
    if t == "DatetimeParametric":
        step = body["step"]
        return _i64(body["start_ts"]) + _i32(step["months"]) + _i64(step["micros"])
    if t == "DatetimeMaterialized":
        return b"".join(_i64(ts) for ts in body["coords"])
    if t == "IntegerParametric":
        return _i64(body["start"]) + _i64(body["step"])
    if t == "IntegerMaterialized":
        return b"".join(_i64(n) for n in body["coords"])
    if t == "FloatParametric":
        return _double(body["start"]) + _double(body["step"])
    if t == "FloatMaterialized":
        return b"".join(_double(c) for c in body["coords"])
    if t == "Label":
        out = b""
        for p in body["positions"]:
            out += _i32(p["ordinal"]) + _string(p["label"])
        return out
    if t == "Period":
        out = b""
        for p in body["positions"]:
            out += _i64(p["start_ts"]) + _i64(p["end_ts"])
            yp = p.get("years_presented")
            if yp is None:
                out += _u8(0x00)
            else:
                out += _u8(0x01) + _double(yp)
        return out
    if t == "TimePattern":
        out = b""
        for p in body["positions"]:
            terms = p["terms"]
            out += _i32(p["ordinal"]) + _u32(len(terms))
            for term in terms:
                out += _string(term["unit"]) + _i16(term["range_start"]) + _i16(term["range_end"])
        return out
    raise ValueError(f"unknown body type {t!r}")


def encode_axis(axis: dict) -> bytes:
    """Canonical byte string of an axis (spec §4.1), from the JSON vector form."""
    body = axis["body"]
    t = body["type"]
    out = _string(_KIND_TAG[t])                      # (1) kind tag
    out += _u8(0x00 if t in _PARAMETRIC else 0x01)   # (2) form byte
    out += _i32(axis["length"])                      # (3) length
    out += _property_block(axis.get("properties") or {})  # (4) property block
    out += _body(body)                               # (5) kind body
    return out


def content_hash(data: bytes) -> int:
    """§2: XXH3-64, seed 0, as an unsigned 64-bit integer."""
    return xxhash.xxh3_64_intdigest(data, seed=0)


def _inject_nan(axis: dict) -> dict:
    """Mirror the Rust runner: put a NaN into the vector's float body (§3.3.1)."""
    body = axis["body"]
    if body["type"] == "FloatMaterialized":
        body["coords"][0] = float("nan")
    elif body["type"] == "FloatParametric":
        body["start"] = float("nan")
    else:
        raise AssertionError("encode_error vector needs a float body")
    return axis


def check_vector(path: pathlib.Path) -> None:
    vector = json.loads(path.read_text())
    name = path.name
    desc = vector.get("description", "")

    if vector.get("encode_error"):
        try:
            encode_axis(_inject_nan(vector["axis"]))
        except NanForbidden:
            return
        raise AssertionError(f"{name}: NaN axis must be rejected, not encoded ({desc})")

    got = encode_axis(vector["axis"])
    got_hex = got.hex()
    want_hex = vector["expected_canonical_hex"]
    if got_hex != want_hex:
        raise AssertionError(
            f"{name}: canonical bytes mismatch ({desc})\n  oracle {got_hex}\n  golden {want_hex}"
        )
    got_hash = content_hash(got)
    want_hash = vector["expected_hash_u64"]
    if got_hash != want_hash:
        raise AssertionError(
            f"{name}: content hash mismatch ({desc})\n  oracle {got_hash}\n  golden {want_hash}"
        )


def main() -> int:
    files = sorted(VECTORS_DIR.glob("*.json"))
    if not files:
        print(f"no vectors found under {VECTORS_DIR}", file=sys.stderr)
        return 1
    for path in files:
        check_vector(path)
    print(f"independent Python oracle: {len(files)} vectors reproduced byte- and hash-for-hash")
    return 0


if __name__ == "__main__":
    sys.exit(main())
