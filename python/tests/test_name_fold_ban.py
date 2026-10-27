"""The Python half of the one-fold rule (docs/dev/engine_rules.md §8).

nodal-db decides that two names are one name by the core's fold
(``nodal_db.fold_name``, an exact copy of DuckDB's ``lower()``) of their
NFC form. Python's ``str.lower``/``str.casefold`` are other folds: their
case tables follow the interpreter's Unicode version and ``lower`` applies
the final-sigma rule, so a collision check built on them disagrees with
the core on rare letters. Rust bans its raw case mappings with clippy
(``clippy.toml``); Python cannot, so this test greps the importer package.
A genuine non-name use (a type tag) carries a ``# not a name`` comment on
its line. Stdlib only.
"""

from __future__ import annotations

import re
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / "nodal_spine_import"
BANNED = re.compile(r"\.(lower|casefold|upper)\(")


def test_no_python_case_mapping_compares_names():
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        for no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            code = line.split("#", 1)[0]
            if BANNED.search(code) and "# not a name" not in line:
                offenders.append(f"{path.name}:{no}: {line.strip()}")
    assert offenders == [], (
        "compare names with the core's fold (importer._name_key / "
        "nodal_db.fold_name), never str.lower/casefold/upper; a non-name use "
        "needs a '# not a name' comment:\n" + "\n".join(offenders)
    )


def test_the_ban_sees_a_violation(tmp_path):
    """The pattern catches the forms it must (a guard that matches nothing
    passes vacuously)."""
    for bad in ('key = n.lower()', 'k = s.casefold()', 'x = (a or "").upper()'):
        assert BANNED.search(bad.split("#", 1)[0]), bad
