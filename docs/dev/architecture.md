# Architecture

Stub. There is no implementation yet; the design record lives in
[`specs/`](../../specs/) — start from
[`specs/HANDOFF.md`](../../specs/HANDOFF.md). The validated DuckDB DDL
is in [`schema/`](../../schema/).

When implementation starts, this document becomes the authoritative
description of system structure — every subsystem, and the full statement
(with the *why*) of the invariants summarized in `CLAUDE.md`'s
"Invariants that bite".

Expected initial subsystems (per the decided Rust + egui architecture,
2026-08-16/17 — see `specs/schema_draft_plan.md` Part 3):
- Storage core: DuckDB file, tiered value representation (long format /
  LIST columns / shared time-grid dimension), scenario resolution as views.
- Interchange: Arrow in memory, Arrow IPC on any wire; public API is
  Arrow-in/Arrow-out.
- Application layer: Rust writer library + `spinal-db` CLI; PyO3/maturin
  Python skin for FlexTool/engine interop; Python consumers read views
  directly via `duckdb` (no bindings needed).
- Editor: egui-based FlexTool-profile DB editor (tables, entity graph
  view, plotting) — separate `spinal-editor` repo when work starts.
- Toolbox integration: project-item plugin exposing the DB URL as a resource.
