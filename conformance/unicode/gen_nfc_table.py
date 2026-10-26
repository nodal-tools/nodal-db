#!/usr/bin/env python3
"""Generate the vendored Unicode normalization table (spec §3.4.1.1).

Usage:

    python3 conformance/unicode/gen_nfc_table.py UCD_DIR

`UCD_DIR` holds the four files of the Unicode Character Database release
`VERSION` below, downloaded unchanged from
`https://www.unicode.org/Public/<VERSION>/ucd/`:

    UnicodeData.txt  DerivedNormalizationProps.txt
    CompositionExclusions.txt  NormalizationTest.txt

The script checks that each file is the release it expects (the version line
of the three that carry one) and writes, next to itself:

- `nfc_table_<VERSION>.txt`: one line per code point that has a non-zero
  canonical combining class or a canonical decomposition, Hangul syllables
  excepted (their decomposition is algorithmic, Unicode §3.12, in every
  implementation), with its Full_Composition_Exclusion flag. The header
  records the SHA-256 of every source file.
- `NormalizationTest-<VERSION>.txt.gz`: the official conformance file,
  gzipped reproducibly (no name, mtime 0).

The table is the normative data both implementations are checked against:
`crates/nodal-db-core/tests/text_nfc.rs` compares the pinned
`unicode-normalization` crate with it for every scalar value, and the
conformance oracle (`conformance/oracle/nfc16.py`) normalizes with it. Both
also run NormalizationTest, so a bug in this generator cannot hide.
Regenerating is part of a Unicode version bump (CONTRIBUTING.md, "Unicode
version bumps"); the output is deterministic, so rerunning it on the same
files changes nothing.
"""
import gzip
import hashlib
import pathlib
import sys

VERSION = "16.0.0"
HERE = pathlib.Path(__file__).resolve().parent
SOURCES = (
    "UnicodeData.txt",
    "DerivedNormalizationProps.txt",
    "CompositionExclusions.txt",
    "NormalizationTest.txt",
)
HANGUL_FIRST, HANGUL_LAST = 0xAC00, 0xD7A3


def ranges(path, prop):
    """Code points carrying binary property `prop` in a UCD property file."""
    out = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        cps, name = (f.strip() for f in line.split(";")[:2])
        if name != prop:
            continue
        lo, _, hi = cps.partition("..")
        out.update(range(int(lo, 16), int(hi or lo, 16) + 1))
    return out


def unicode_data(path):
    """`{cp: (ccc, canonical decomposition)}` for every assigned code point
    with a non-zero ccc or a canonical (untagged) decomposition. Ranges
    (`<..., First>`/`<..., Last>`) have ccc 0 and no decomposition."""
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        f = line.split(";")
        cp, ccc, decomp = int(f[0], 16), int(f[3]), f[5]
        canonical = [] if not decomp or decomp.startswith("<") else [
            int(x, 16) for x in decomp.split()]
        if ccc or canonical:
            out[cp] = (ccc, canonical)
    return out


def main(argv):
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    ucd = pathlib.Path(argv[1])
    paths = {name: ucd / name for name in SOURCES}
    for name, path in paths.items():
        head = path.read_text(encoding="utf-8").splitlines()[0]
        stem = name.removesuffix(".txt")
        if name != "UnicodeData.txt" and head != f"# {stem}-{VERSION}.txt":
            raise SystemExit(f"{path}: expected the {VERSION} release, found {head!r}")
    data = unicode_data(paths["UnicodeData.txt"])
    # UnicodeData.txt has no version line: U+1C89 is new in 16.0.0, so its
    # presence (and the absence of a 17.0 addition) pins the release.
    text = paths["UnicodeData.txt"].read_text(encoding="utf-8")
    if "\n1C89;" not in text or "\n1ACF;" in text:
        raise SystemExit("UnicodeData.txt is not the 16.0.0 release")
    fce = ranges(paths["DerivedNormalizationProps.txt"], "Full_Composition_Exclusion")
    # Every Full_Composition_Exclusion code point has a canonical
    # decomposition (exclusions, singletons, non-starter decompositions).
    missing = sorted(cp for cp in fce if cp not in data or not data[cp][1])
    if missing:
        raise SystemExit(f"exclusions without a decomposition: {missing[:10]}")
    # The explicit exclusion list is a subset of the derived one.
    explicit = set()
    for line in paths["CompositionExclusions.txt"].read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            lo, _, hi = line.partition("..")
            explicit.update(range(int(lo, 16), int(hi or lo, 16) + 1))
    if not explicit or not explicit <= fce:
        raise SystemExit("CompositionExclusions.txt is not a subset of Full_Composition_Exclusion")
    hangul = [cp for cp in data if HANGUL_FIRST <= cp <= HANGUL_LAST]
    if hangul:
        raise SystemExit(f"UnicodeData.txt lists Hangul syllables: {hangul[:5]}")

    lines = [
        f"# nfc_table_{VERSION}.txt: the normalization data of Unicode {VERSION}",
        "# (spec canonical_encoding.md §3.4.1.1). Generated by",
        "# conformance/unicode/gen_nfc_table.py; do not edit by hand.",
        "#",
        "# Sources (https://www.unicode.org/Public/" + VERSION + "/ucd/), SHA-256:",
    ]
    for name in SOURCES:
        digest = hashlib.sha256(paths[name].read_bytes()).hexdigest()
        lines.append(f"#   {digest}  {name}")
    lines += [
        "#",
        "# One line per code point with a non-zero canonical combining class or a",
        "# canonical decomposition, ascending:",
        "#",
        "#   CODE;CCC;DECOMPOSITION;EXCLUDED",
        "#",
        "# CODE is hex; CCC decimal; DECOMPOSITION the one-level canonical",
        "# decomposition of UnicodeData.txt field 5 (hex, space-separated; empty",
        "# when there is none; apply it recursively for the full decomposition);",
        "# EXCLUDED is 1 when the code point is Full_Composition_Exclusion (it",
        "# never recomposes), else 0. Code points not listed have ccc 0 and no",
        "# canonical decomposition. The 11,172 Hangul syllables U+AC00..U+D7A3",
        "# are not listed: their decomposition and composition are algorithmic",
        "# (Unicode §3.12) in every implementation.",
    ]
    for cp in sorted(data):
        ccc, decomp = data[cp]
        d = " ".join(f"{x:04X}" for x in decomp)
        lines.append(f"{cp:04X};{ccc};{d};{1 if cp in fce else 0}")
    out = HERE / f"nfc_table_{VERSION}.txt"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    test = HERE / f"NormalizationTest-{VERSION}.txt.gz"
    raw = paths["NormalizationTest.txt"].read_bytes()
    with open(test, "wb") as fh:
        with gzip.GzipFile(filename="", mode="wb", fileobj=fh, compresslevel=9, mtime=0) as gz:
            gz.write(raw)
    print(f"wrote {out.name} ({len(data)} code points) and {test.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
