"""Import profiles: the tool-specific half of the Spine -> nodal importer.

The generic mechanism (importer.py) asks the profile exactly four things:

1. the template identity to stamp (``template_name``, ``role``),
2. where the source stores its schema version (``version_of``),
3. how to NAME the index dimensions of an observed value shape
   (``dim_names`` — the D3 fan-out recurrence at the import boundary:
   index-column names are the discriminator, so observed shapes must be
   normalized to the head template's declared dimension names or the final
   reconcile would see never-declared members and skip their narrowing),
4. results-section class renames (``rename_class`` — the L2 space-name map).

Everything else is generic. Tool-specific profiles subclass `Profile` and
live with the tool they serve, not in this package.
"""

from __future__ import annotations

import re


class Profile:
    """Base profile: no head template, generic dimension names."""

    #: Stamped as `template_name`; None means the importer requires an
    #: explicit name argument.
    template_name: str | None = None
    role: str = "input"

    def version_of(self, db) -> str | None:
        """Return the source DB's schema version as a raw string, or None.

        The base profile knows no version convention; importing without a
        version FAILS (the stamp is non-optional) unless a version is
        passed explicitly.
        """
        return None

    def dim_names(self, class_name: str, parameter: str, kinds: tuple[str, ...],
                  index_names: tuple[str, ...], labels: tuple) -> tuple[list[str], str | None]:
        """Names for an observed index shape.

        Returns ``(names, ambiguity_note)`` — the note is non-None when the
        choice was a guess worth surfacing in the residue report.
        """
        names = []
        for i, kind in enumerate(kinds):
            n = index_names[i] if i < len(index_names) and index_names[i] else f"i{i + 1}"
            if not re.fullmatch(r"[A-Za-z0-9_]+", n):
                n = f"i{i + 1}"
            names.append(n)
        # de-duplicate (batch columns must be unique)
        seen: dict[str, int] = {}
        out = []
        for n in names:
            if n in seen:
                seen[n] += 1
                out.append(f"{n}_{seen[n]}")
            else:
                seen[n] = 0
                out.append(n)
        return out, None

    def rename_class(self, class_name: str) -> str:
        """Results-section space renames; identity by default."""
        return class_name
