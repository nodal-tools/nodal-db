"""nodal_spine_import — generic Spine-DB -> nodal-db importer.

Home and posture per specs/flextool_port_finale_plan.md §4 (decisions D-F2
and D-F6, jkiviluo 2026-09-22):

- The MECHANISM is Spine-generic (`SpineImporter` + `Profile`); everything
  tool-specific (template identity, dimension-name normalization against the
  head template, rename-alias tables, the results-section rename map) lives
  in a thin profile, a `Profile` subclass that lives with the tool it serves.
- Import type posture = OBSERVED-SHAPE DECLARATION: stored values are ground
  truth; each definition's declared member set at import is the union of the
  shapes actually observed in its values (bind-by-construction — zero
  type-rejection possible); value-less definitions import typeless;
  `parameter_type_list` is imported as drift-report metadata only.
- Reads via spinedb_api with ``upgrade=False`` pinned (the importer never
  upgrades its input; the oracle leg owns upgrading). Writes exclusively
  through the ``nodal_db`` skin (CLAUDE.md invariant 2).
- Loud residue: unconsumed constructs, unflattenable values and the type
  drift table are counted and named — zero silent drops.
"""

from .importer import SpineImporter, ImportError_, ImportReport, normalize_version
from .profiles import Profile

__all__ = [
    "SpineImporter",
    "ImportReport",
    "ImportError_",
    "Profile",
    "normalize_version",
]
