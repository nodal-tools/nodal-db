"""The Python half of the one-fold rule (docs/dev/engine_rules.md §8).

nodal-db decides that two names are one name by the core's fold
(``nodal_db.fold_name``, an exact copy of DuckDB's ``lower()``) of their
NFC form. Python's ``str.lower``/``str.casefold`` are other folds: their
case tables follow the interpreter's Unicode version and ``lower`` applies
the final-sigma rule, so a collision check built on them disagrees with
the core on rare letters. Rust bans its raw case mappings with clippy
(``clippy.toml``); Python cannot, so this test walks the importer
package's syntax trees: any attribute named like a case mapping, called or
not (``s.lower()``, ``key=str.lower``, ``map(str.casefold, xs)``, also
inside an f-string's ``{...}``), and any string constant that is exactly
such a name (``getattr(s, "lower")``, ``methodcaller("casefold")``).
Comments are not in the tree, and a docstring is never exactly a method
name. A genuine non-name use (a type tag) carries a ``# not a name``
comment on its line. Stdlib only.
"""

from __future__ import annotations

import ast
import io
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / "nodal_spine_import"
MAPPINGS = frozenset({"lower", "casefold", "upper", "swapcase", "capitalize", "title"})
WAIVER = "# not a name"


def offending_lines(source: str) -> list[int]:
    """Line numbers of ``source`` that use a case mapping without the waiver."""
    # The tokenizer's line split (str.splitlines also splits on form feeds
    # and Unicode separators, which would shift the numbers).
    lines = io.StringIO(source).readlines()
    hits: set[int] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Attribute) and node.attr in MAPPINGS:
            # The line of the attribute name itself (a continuation line
            # when the access is split across lines).
            hits.add(node.end_lineno or node.lineno)
        elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
              and node.value in MAPPINGS):
            hits.add(node.lineno)
    return sorted(n for n in hits if WAIVER not in lines[n - 1])


def test_no_python_case_mapping_compares_names():
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        lines = io.StringIO(source).readlines()
        for no in offending_lines(source):
            offenders.append(f"{path.name}:{no}: {lines[no - 1].strip()}")
    assert offenders == [], (
        "compare names with the core's fold (importer._name_key / "
        "nodal_db.fold_name), never str.lower/casefold/upper; a non-name use "
        "needs a '# not a name' comment:\n" + "\n".join(offenders)
    )


def test_the_ban_sees_a_violation():
    """The scan catches the forms it must (a guard that matches nothing
    passes vacuously), and leaves the waived line, comments and docstrings
    alone."""
    for bad in (
        "key = n.lower()",
        "k = s.casefold()",
        'x = (a or "").upper()',
        "names.sort(key=str.lower)",
        "folded = list(map(str.casefold, names))",
        "f = str.lower",
        "k = s.\\\n    lower()",
        'f = getattr(s, "lower")',
        "f = operator.methodcaller('casefold')",
        "t = s.title()",
        'k = f"{n.lower()}"',
        "k = f'{n!r:>{w}} {m.casefold()}'",
    ):
        assert len(offending_lines(bad + "\n")) == 1, bad
    for ok in (
        't = (t or "").lower()  # not a name: a Spine type tag',
        "# s.lower() in a comment",
        '"""Use ``str.lower`` here? No."""',
        "x = s.islower() or s.lower_bound",
        'msg = "lower the bar"',
        'k = f"lower {n}"',
    ):
        assert offending_lines(ok + "\n") == [], ok
