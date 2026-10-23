# Building against nodal-db

This page is for anyone writing a tool that reads from or writes to a
nodal-db database (a script, a pipeline, an editor, a solver adapter) in
any language. It states the rules that bind every client. For system
context see [Architecture](architecture.md); for worked examples see
[Using nodal-db](../usage.md#for-integrators).

## The one rule that is never waived

**Never reimplement canonicalization client-side.** The canonical
encodings, the value tier decision, axis deduplication and the history
protocol are deterministic functions owned by the Rust core
(`nodal-db-core`). Two independent implementations of them drift, and
drift silently breaks history diffs, axis sharing and byte equality
between writers. Everything below follows from this.

## Writing: two entry points, no third

1. **The core API** (`nodal-db-core`), directly from Rust or through
   bindings over it, such as the Python package `nodal_db`. Bindings
   marshal data and translate errors; they contain no logic.
2. **`nodal-db import`**, the CLI's Arrow-IPC bulk boundary. Build Arrow
   tables in any language, write them as described in the
   [Arrow-IPC import format](arrow_import_format.md), and hand the file
   to the CLI; the core does all canonical work in one atomic commit.

Do not write to the base tables with SQL. There is no wire protocol for
writes to implement, by design. If your use case needs interactive
writes from a language without bindings, raise it in an issue; the
answer will be a served API, not a protocol document.

## Reading: SQL is the API

- Read through the **views and table macros**
  ([`schema/070`–`120_*.sql`](https://github.com/nodal-tools/nodal-db/tree/main/schema)),
  not the base tables. The base-table layout is not a stable interface;
  the read surface is. The [schema page](../schema.md#views-and-table-macros)
  lists the views and macros with their purposes.
- **Hold connections per operation: open, query, close.** DuckDB's file
  lock covers readers too: an idle read-only handle (a notebook, a GUI
  tab) blocks every writer commit until it closes. See
  [connection discipline](../usage.md#connection-discipline).
- To detect new commits, poll the newest commit id with a short read
  rather than holding a connection open to watch.
- Expect busy and conflict outcomes. The core reports writer contention
  as a defined error (`DbError::Busy` in Rust, `nodal_db.Busy` in
  Python) after a bounded retry, not as a hang. Readers may briefly fail
  to open while a commit is in progress and should retry.
- Use DuckDB 1.5 or newer to read; see
  [DuckDB version](../usage.md#duckdb-version).

## The executable specification

[`conformance/`](https://github.com/nodal-tools/nodal-db/tree/main/conformance)
holds language-neutral test vectors (canonical encodings, value
encodings, pipeline cases, history protocol, reconstruction of array
values) and an independent Python oracle. Any implementation that
touches canonical data, including a future port, is correct only if it
reproduces these vectors exactly. The vectors are the specification;
the README files in the subdirectories explain the vector formats.

## Not yet written

- A reference for the read surface: every public view and macro with
  parameters, columns, types and semantics.
- Per-language snippets for reading correctly (open-query-close and
  commit polling).
- An error reference beyond `Busy`.
