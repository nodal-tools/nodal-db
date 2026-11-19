"""Import profiles: the tool-specific half of the Spine -> nodal importer.

The generic mechanism (importer.py) asks the profile exactly five things:

1. the template identity to stamp (``template_name``, ``role``),
2. where the source stores its schema version (``version_of``),
3. how to NAME the index dimensions of an observed value shape
   (``dim_names``:
   index-column names are the discriminator, so observed shapes must be
   normalized to the head template's declared dimension names or the final
   reconcile would see never-declared members and skip their narrowing),
4. results-section class renames (``rename_class``),
5. which Spine metadata entries are the tool's own markers, imported as
   tool database properties instead of pairs (``tool_property``).

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

    def tool_property(self, name: str) -> str | None:
        """The tool database property (``<tool>:<name>``) a Spine metadata
        entry named ``name`` is a marker for, or None for an ordinary entry
        (imported as a pair). Routing is by name (a prefix rule is the
        profile's choice); the entry's value becomes the property's value.
        None by default."""
        return None
