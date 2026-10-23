# nodal-db-py — the thin Python skin (`import nodal_db`)

The **write-path SDK** for nodal-db: a **logic-free marshaling
layer** over the one Rust core. It marshals Arrow tables and scalars in,
calls `nodal-db-core`, and translates `DbError` into a Python exception
hierarchy out. It implements **no** canonicalization, tier, history, or
reconcile rule — a rule you cannot find in `nodal-db-core` does not
exist here either (one canonicalizer, never two). For reads beyond the
conveniences below, SQL is the API: any DuckDB client reads the
views/macros directly (`docs/dev/building_against_nodal-db.md`).

## Connection discipline (the rule the API shape enforces)

`nodal_db.open(path)` returns a handle that holds the **path only —
never a connection**. Every method opens a fresh per-operation
connection inside the core and closes it before returning (an idle
connection would starve every writer). Keeping a `Database`
object alive in a notebook is free and blocks nobody; there is no
`close()` because there is nothing to close. `query()` returns a
`pyarrow.RecordBatchReader` backed by already-materialized batches, not
by a live connection.

## Usage sketch

```python
import pyarrow as pa
import nodal_db

# A tool initializes its store once, then loads its template.
db = nodal_db.init("model.duckdb", default_alternative="Base")
db.template_apply("template.yaml", user="my_tool")

# Interactive writes: logical-long Arrow batches; the CORE decides the
# storage tier and auto-creates missing referents (with warnings).
capacity = pa.record_batch({
    "entity": ["coal_plant", "gas_plant"],
    "value":  pa.array([400.0, 250.0], pa.float64()),
})
report = db.write_parameter("unit", "capacity", "Base", capacity,
                            user="analyst", message="initial capacities")
assert report["tiers"] == ["scalar"]

# A time series: dimension columns between entity and value; Arrow
# types pick the index kind (timestamp[us] -> datetime).
flow = pa.record_batch({
    "entity": ["coal_plant"] * 3,
    "time":   pa.array([...], pa.timestamp("us")),
    "value":  pa.array([1.0, 2.0, 3.0], pa.float64()),
})
db.write_parameter("unit", "flow", "scenario_a", flow, user="analyst")

# An editor save: writes + deletes, atomically, ONE commit row.
db.commit_changes(
    segments=[{"class": "unit", "parameter": "capacity",
               "alternative": "Base", "batch": capacity}],
    deletes=[("unit", "capacity", "Base", "old_plant")],
    user="analyst",
)

# Reads: the convenience wrapper, or plain SQL against the read surface.
batch = db.read_parameter("unit", "capacity", "Base")     # pyarrow.RecordBatch
table = db.query("SELECT * FROM read_parameter('unit', 'capacity')").read_all()

# Rerun-overwrite lifecycle: hard-erase a superseded result alternative
# (the marker + as-of guard are the core's; expect_present is the
# ordering belt — the erase refuses unless the replacement run exists).
db.erase_alternative("run_2026_09_01", user="my_tool",
                     expect_present=["run_2026_09_22"])
```

### Migration programs (template `run:` steps)

Tenants register **plain Python callables** as migration programs for
the duration of one `template_apply` call. The IR names them; the
registry binds them; they write through the ordinary API:

```python
def backfill_efficiency(db_path, user):
    # Called BY the core mid-chain. Writing through the skin from here
    # is the intended use (see the reentrancy contract below).
    inner = nodal_db.open(db_path)
    inner.write_parameter("unit", "efficiency", "Base", some_batch, user=user)

db.template_apply("template_v2.yaml", user="my_tool",
                  programs={"backfill_efficiency": backfill_efficiency})
```

Contract (the core's Layer-3 rules, surfaced verbatim):

- **Programs must be idempotent.** A program commits through the public
  API on its own; the progress substamp lands in a separate commit, so a
  crash in that window re-runs the program on resume.
- A chain naming an **unregistered** program fails loudly before any
  step runs (`nodal_db.ApiError`).
- A program that **raises** aborts the apply; the **original Python
  exception** propagates to the `template_apply` caller (not a wrapped
  rendering). Steps already committed keep their substamps; re-apply
  resumes after them.

## GIL / reentrancy contract

1. Every skin entry point **releases the GIL** around the core call
   (other Python threads run during DB work).
2. A `run:` program is invoked by the core at a point where the core
   holds **no connection and no in-process path lock** — the `run:` step
   sits between per-operation commits by construction.
3. The wrapper re-acquires the GIL **only for the callable itself**; the
   program's own `nodal_db` calls release it again (rule 1). Hence a
   program calling back into the API cannot deadlock: no lock is held
   across the callback on either side.
4. Corollary for program authors: do all DB work through `nodal_db` (or
   fresh, short-lived connections you close yourself); never stash a
   long-lived connection — the file lock starves the chain's next
   commit, surfacing as `nodal_db.Busy`.
5. **A program must NOT call `template_apply` on the database it is
   migrating.** Its own step's progress substamp lands only after it
   returns, so a nested apply would re-execute the program itself. The
   core refuses this (process-locally, by canonical DB path) with
   `nodal_db.ApiError` naming the reentrancy; a second *process*
   applying concurrently cannot be detected and is the ordinary
   concurrent-writer race. Value writes, reads, plans, erases — all
   fine; re-applying the in-flight template is the one forbidden call.
6. **No detached writers.** Any thread (or process) a program spawns
   must finish its writes before the program returns: the per-program
   commit attribution in `run_programs` is a commit-id watermark range
   taken around the call, so a write landing after the program returns
   is misattributed to a later step (or to none) and, worse, races the
   chain's own commits. Join your threads.

## Errors

One hierarchy, mapped 1:1 from the core's `DbError` (message text is the
core's own): `NodalDbError` (base) with `Busy`, `Conflict`, `ApiError`,
`VersionTooOld`, `SchemaError`, `DatabaseError`, `CanonError`,
`ArrowError`, `InternalError`. `Busy` and `Conflict` are defined,
retryable outcomes, never hangs.

## Arrow interop and version coupling

- **In**: any Arrow PyCapsule producer — `pyarrow.RecordBatch`
  (`__arrow_c_array__`), `pyarrow.Table` / `RecordBatchReader` / polars
  (`__arrow_c_stream__`). Imported zero-copy over the C data interface.
- **Out**: `pyarrow.RecordBatch` (`read_parameter`) and
  `pyarrow.RecordBatchReader` (`query`), exported zero-copy.
- The bridge is the maintained `arrow-pyarrow` crate (split out of
  arrow-rs), **pinned `=58.4.0`** to the workspace `arrow` pin — one
  arrow in the tree, so imported batches ARE the core's `RecordBatch`
  type at compile time. Version coupling is three-way and matched-bump:
  duckdb-rs ⇒ `arrow` ⇒ `arrow-pyarrow` (whose pyo3 requirement then
  dictates the `pyo3` line; 58.4.0 ⇒ pyo3 ^0.28). See the root
  `Cargo.toml` comments. Runtime needs `pyarrow>=16` (PyCapsule
  interface both directions).

## Building

Wheels are abi3: **one wheel per platform, CPython ≥ 3.9** (abi3-py39 —
the documented floor; in practice `pyarrow>=16` raises the effective
floor to its own supported Pythons).

```sh
python -m venv .venv && . .venv/bin/activate
pip install maturin
maturin develop            # build + install into the venv (debug)
maturin build --release    # release wheel into target/wheels/
pytest crates/nodal-db-py/tests
```

`cargo test --workspace` needs no maturin and no libpython: this crate
builds only as an extension module (undefined Python symbols are the
normal extension-module linking model) and carries no Rust test harness
— its tests are the pytest suite, run against the maturin-built module.
CI note: the existing `cargo test --workspace` + schema smoke jobs run
unchanged; a wheel-gate job would add `maturin build` + this crate's
pytest suite.
