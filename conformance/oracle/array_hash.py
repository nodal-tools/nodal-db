#!/usr/bin/env python3
"""Independent second oracle for the ARRAY PAYLOAD hash (spec §13).

Re-derives every `conformance/array_hash_vectors/*.json` vector's
`expected_hash_u64` from the spec, sharing NO code with the Rust
`encoding::array_payload_hash`: the hash input is each payload element's 64
bits as 8 bytes little-endian, in stored order, and nothing else (no length
prefix, no kind tag, no key, no axes, no canonicalization); the hash is
XXH3-64 with seed 0. A DOUBLE contributes its raw IEEE-754 pattern
(`struct.pack('<d')`), a BIGINT its two's-complement value
(`struct.pack('<q')`).

If a vector is missing `expected_hash_u64`, this prints the computed value
(a one-shot golden generator) instead of asserting; with goldens present it
asserts. Run:

    .venv/bin/python conformance/oracle/array_hash.py
"""
import json
import pathlib
import struct
import sys

import xxhash

VECTORS_DIR = pathlib.Path(__file__).resolve().parents[1] / "array_hash_vectors"


def payload(vec: dict) -> bytes:
    """The hash input bytes of one vector's payload."""
    kind = vec["element_type"]
    if kind == "bigint":
        assert "bits_at" not in vec and "ramp" not in vec, "bigint vectors spell values only"
        return b"".join(struct.pack("<q", int(v)) for v in vec["values"])
    if kind != "double":
        raise ValueError(f"unknown element_type {kind!r}")
    ramp = vec.get("ramp")
    if ramp is not None:
        values = [ramp["start"] + i * ramp["step"] for i in range(ramp["len"])]
    else:
        values = [float(v) for v in vec["values"]]
    words = [struct.pack("<d", v) for v in values]
    for idx, bits in vec.get("bits_at", []):
        words[idx] = struct.pack("<Q", int(bits, 16))
    return b"".join(words)


def main() -> int:
    paths = sorted(VECTORS_DIR.glob("*.json"))
    if not paths:
        print(f"no vectors in {VECTORS_DIR}", file=sys.stderr)
        return 1
    hashes = {}
    failures = 0
    for path in paths:
        vec = json.loads(path.read_text())
        h = xxhash.xxh3_64_intdigest(payload(vec), seed=0)
        hashes[path.stem] = h
        want = vec.get("expected_hash_u64")
        if want is None:
            print(f"{path.name}: expected_hash_u64 = {h}  (no golden yet)")
            failures += 1
        elif want != h:
            print(f"FAIL {path.name}: oracle {h} != golden {want}")
            failures += 1
        else:
            print(f"ok   {path.name}")
    for path in paths:
        vec = json.loads(path.read_text())
        other = vec.get("same_hash_as")
        if other is not None and hashes[path.stem] != hashes.get(other):
            print(f"FAIL {path.name}: hash differs from {other}'s")
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
