# Architecture

Stub. There is no implementation yet; the design source of truth is
[`specs/initial_ideas.md`](../../specs/initial_ideas.md).

When implementation starts, this document becomes the authoritative
description of system structure — every subsystem, and the full statement
(with the *why*) of the invariants summarized in `CLAUDE.md`'s
"Invariants that bite".

Expected initial subsystems (from the concept spec):
- Storage core: DuckDB file, tiered value representation (long format /
  LIST columns / shared time-grid dimension), scenario resolution as views.
- Interchange: Arrow in memory, Arrow IPC on any wire.
- Application layer: Python API; optional Polars for editor-side analytics;
  DuckDB.jl/Arrow.jl on the Julia side.
- Editor: FlexTool-profile DB editor (tables, entity graph view, plotting).
- Toolbox integration: project-item plugin exposing the DB URL as a resource.
