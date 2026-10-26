#!/usr/bin/env python3
"""The oracle's own NFC (Unicode Normalization Form C), Unicode 16.0.0.

An independent implementation of UAX #15 for the conformance oracle
(spec `canonical_encoding.md` §3.4.1.1): it normalizes with the vendored
table `conformance/unicode/nfc_table_16.0.0.txt` and its own code, never
with CPython's `unicodedata` (whose Unicode version is the interpreter's:
15.0.0 on Python 3.12) and never with the core (`nodal_db`): the oracle is
the second implementation the Rust core is checked against, so it shares
the *data* (the table, itself checked against the official
NormalizationTest) and none of the *code* (CLAUDE.md invariant 2;
`crates/nodal-db-core/tests/python_oracle.rs` guards the import).

The algorithm, UAX #15 §§ 9-10 and Unicode §3.11-3.12:

1. full canonical decomposition (the table's one-level decompositions
   applied recursively; Hangul syllables algorithmically);
2. canonical ordering (a stable sort of every run of non-starters by
   combining class);
3. canonical composition: each character is composed with the last starter
   unless it is blocked (a character of equal or higher class sits between
   them) or the pair has no primary composite (excluded code points never
   recompose); Hangul L+V and LV+T algorithmically.

Run as a script, it checks itself against NormalizationTest-16.0.0 (every
NFC invariant of Part 0-3, and Part 1's rule that every code point it does
not list is unchanged).
"""
import gzip
import pathlib
import sys

UNICODE_DIR = pathlib.Path(__file__).resolve().parents[1] / "unicode"
TABLE = UNICODE_DIR / "nfc_table_16.0.0.txt"
NORMALIZATION_TEST = UNICODE_DIR / "NormalizationTest-16.0.0.txt.gz"

# Hangul (Unicode §3.12).
S_BASE, L_BASE, V_BASE, T_BASE = 0xAC00, 0x1100, 0x1161, 0x11A7
L_COUNT, V_COUNT, T_COUNT = 19, 21, 28
N_COUNT = V_COUNT * T_COUNT
S_COUNT = L_COUNT * N_COUNT


def _load():
    ccc, one_level, excluded = {}, {}, set()
    for line in TABLE.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        code, klass, decomp, excl = line.split(";")
        cp = int(code, 16)
        if int(klass):
            ccc[cp] = int(klass)
        if decomp:
            one_level[cp] = [int(x, 16) for x in decomp.split()]
        if excl == "1":
            excluded.add(cp)

    full = {}

    def expand(cp):
        if cp in full:
            return full[cp]
        d = one_level.get(cp)
        out = [cp] if d is None else [x for c in d for x in expand(c)]
        full[cp] = out
        return out

    for cp in one_level:
        expand(cp)
    # Primary composites: two-character canonical decompositions of code
    # points that are not excluded (singletons and non-starter
    # decompositions are excluded by definition).
    compose = {}
    for cp, d in one_level.items():
        if cp in excluded:
            continue
        if len(d) != 2:
            raise ValueError(f"U+{cp:04X}: a non-excluded decomposition of length {len(d)}")
        compose[(d[0], d[1])] = cp
    return ccc, full, compose


_CCC, _FULL, _COMPOSE = _load()


def _decompose(s):
    out = []
    for ch in s:
        cp = ord(ch)
        si = cp - S_BASE
        if 0 <= si < S_COUNT:
            out.append(L_BASE + si // N_COUNT)
            out.append(V_BASE + (si % N_COUNT) // T_COUNT)
            if si % T_COUNT:
                out.append(T_BASE + si % T_COUNT)
        else:
            out.extend(_FULL.get(cp, (cp,)))
    return out


def _reorder(cps):
    i, n = 0, len(cps)
    while i < n:
        if _CCC.get(cps[i], 0) == 0:
            i += 1
            continue
        j = i
        while j < n and _CCC.get(cps[j], 0) != 0:
            j += 1
        cps[i:j] = sorted(cps[i:j], key=lambda c: _CCC[c])  # stable
        i = j
    return cps


def _pair(a, b):
    li, vi = a - L_BASE, b - V_BASE
    if 0 <= li < L_COUNT and 0 <= vi < V_COUNT:
        return S_BASE + (li * V_COUNT + vi) * T_COUNT
    si, ti = a - S_BASE, b - T_BASE
    if 0 <= si < S_COUNT and si % T_COUNT == 0 and 0 < ti < T_COUNT:
        return a + ti
    return _COMPOSE.get((a, b))


def _compose(cps):
    if not cps:
        return cps
    out = [cps[0]]
    starter = 0 if _CCC.get(cps[0], 0) == 0 else None
    last_ccc = -1 if starter == 0 else _CCC.get(cps[0], 0)
    for cp in cps[1:]:
        c = _CCC.get(cp, 0)
        if starter is not None:
            blocked = last_ccc != -1 and (last_ccc == 0 or last_ccc >= c)
            # `last_ccc == -1`: the character follows the starter directly.
            if not blocked:
                composite = _pair(out[starter], cp)
                if composite is not None:
                    out[starter] = composite
                    continue
        if c == 0:
            starter = len(out)
            last_ccc = -1
        else:
            last_ccc = c
        out.append(cp)
    return out


def nfc(s: str) -> str:
    """`s` in Normalization Form C, Unicode 16.0.0."""
    if s.isascii():
        return s
    return "".join(map(chr, _compose(_reorder(_decompose(s)))))


def nfd(s: str) -> str:
    """`s` in Normalization Form D, Unicode 16.0.0."""
    return "".join(map(chr, _reorder(_decompose(s))))


def _self_test() -> int:
    """NormalizationTest-16.0.0: for every line `c1;c2;c3;c4;c5`,
    c2 == NFC(c1) == NFC(c2) == NFC(c3) and c4 == NFC(c4) == NFC(c5) (and
    the NFD columns, which check the decomposition half on its own); every
    code point Part 1 does not list is its own NFC and NFD."""
    failures, lines, part1 = [], 0, set()
    part = None
    with gzip.open(NORMALIZATION_TEST, "rt", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if line.startswith("@"):
                part = line
                continue
            cols = ["".join(chr(int(x, 16)) for x in f.split()) for f in line.split(";")[:5]]
            c1, c2, c3, c4, c5 = cols
            lines += 1
            if part == "@Part1":
                part1.add(ord(c1))
            checks = [
                ("NFC(c1)", nfc(c1), c2), ("NFC(c2)", nfc(c2), c2),
                ("NFC(c3)", nfc(c3), c2), ("NFC(c4)", nfc(c4), c4),
                ("NFC(c5)", nfc(c5), c4), ("NFD(c1)", nfd(c1), c3),
                ("NFD(c2)", nfd(c2), c3), ("NFD(c3)", nfd(c3), c3),
                ("NFD(c4)", nfd(c4), c5), ("NFD(c5)", nfd(c5), c5),
            ]
            for name, got, want in checks:
                if got != want:
                    failures.append(f"{part} {line}: {name} = "
                                    f"{' '.join(f'{ord(c):04X}' for c in got)}")
    if not part1:
        failures.append("NormalizationTest has no Part 1")
    unlisted = 0
    for cp in range(0x110000):
        if 0xD800 <= cp <= 0xDFFF or cp in part1:
            continue
        unlisted += 1
        ch = chr(cp)
        if nfc(ch) != ch or nfd(ch) != ch:
            failures.append(f"Part 1: U+{cp:04X} is not listed but changes under NFC/NFD")
    for f in failures[:20]:
        print(f"FAIL {f}", file=sys.stderr)
    if failures:
        print(f"nfc16: {len(failures)} failure(s)", file=sys.stderr)
        return 1
    print(f"nfc16: NormalizationTest-16.0.0 passed ({lines} lines, "
          f"{unlisted} unlisted code points unchanged)")
    return 0


if __name__ == "__main__":
    sys.exit(_self_test())
