# Building against nodal-db

> **STUB** — this file reserves the place for the client/implementer
> guide. It states the rules that already bind every client; the full
> read-surface contract (view/macro names, parameters, columns,
> semantics) is queued work and will land here or be linked from here.
> When this file and a spec disagree, the spec wins.
> **Created: 2026-09-17.**

Who this is for: anyone writing a tool that reads from or writes to a
nodal-db database — a script, a pipeline, an editor, a solver adapter —
in any language. System context: `docs/dev/architecture.md`; mapping
external domain schemas (CESM, SiennaSchemas, INES, FlexTool) into
nodal-db templates: `docs/dev/schema_interop.md`.

## The one rule that is never waived

**Never reimplement canonicalization client-side.** The canonical
encodings, the value tier decision, axis dedup, and the history protocol
are deterministic functions owned by the one Rust core
(`nodal-db-core`). Two independent implementations of them drift; that
is the project's founding lesson. Everything below follows from it.

## Writing: two doors, no third

1. **The core API** (`nodal-db-core`), directly from Rust or through
   bindings over it (a thin Python skin is planned). Bindings marshal
   data and translate errors; they contain no logic.
2. **`nodal-db import`** — the CLI's Arrow-IPC bulk boundary
   (`spec/arrow_ipc_bulk_format.md`, signed off v1.0). Build Arrow
   tables in any language, hand them to the CLI; the core does all
   canonical work.

There is no documented wire/write protocol to implement, by design. If
your use case needs interactive writes from a language without bindings,
raise it — the answer will be a served API, not a protocol document.

## Reading: SQL is the API

- Read through the **views and macros** (`schema/070…120_*.sql`), not
  the base tables. Base-table layout is not a contract; the read surface
  is.
- **Hold connections per-operation: open, query, close.** DuckDB's file
  lock is symmetric — an idle read-only handle (a notebook, a GUI tab)
  blocks every writer commit until it closes. This is invariant 1 in
  `CLAUDE.md`; the why is in `specs/value_model_plan.md` Part 6.
- To detect new commits, poll the commit cookie — itself a
  per-operation read — rather than holding a connection open to watch.
- Expect and handle busy/conflict outcomes: the core surfaces writer
  contention as a defined error (`DbError::Busy`), not a hang; readers
  may transiently fail to open while a write commits and should retry
  briefly.

## The executable spec

`conformance/` holds the language-neutral test vectors (canonical
encodings, value tiers, history protocol, reconstruction) plus an
independent Python oracle. Any implementation that touches canonical
data — including a future port — is correct iff it reproduces these
vectors exactly. They are the specification; the prose lives in
`spec/canonical_encoding.md` and `spec/reconstruction.md`.

## Not yet written (queued)

- The read-surface contract: every public view/macro with parameters,
  columns, types, and semantics.
- Per-language "read correctly" snippets (open-query-close + cookie
  poll).
- Error taxonomy reference beyond `Busy`.
