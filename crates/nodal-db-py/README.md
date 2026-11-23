# nodal-db (Python)

`import nodal_db`: the Python package of
[nodal-db](https://github.com/nodal-tools/nodal-db), a general-purpose
database format for structured, versioned modelling data, stored in
DuckDB files with append-only history.

The package is a thin layer over nodal-db's Rust core. It passes Arrow
tables and scalars to the core and turns the core's errors into Python
exceptions; every rule (how values are encoded and stored, how history
is kept) lives in the core, so a file written from Python is the same as
one written by the `nodal-db` CLI or from Rust. For reads beyond the
conveniences below, SQL is the interface: any DuckDB 2.0 client reads
the database's views and macros directly.

Documentation: <https://nodal-tools.github.io/nodal-db/> (the
[usage guide](https://nodal-tools.github.io/nodal-db/usage/) and the
[reference](https://nodal-tools.github.io/nodal-db/reference/)).

## Install

```sh
pip install nodal-db
```

Wheels are built for Linux x86_64 (manylinux_2_28) and macOS arm64, one
wheel per platform for CPython 3.11 and later. `pyarrow>=16` is
installed with it.

## Connection discipline

`nodal_db.open(path)` returns a handle that holds the **path only, never
a connection**. Every method opens a connection, does its work and
closes it before returning, because DuckDB's file lock means that an
idle connection, even a read-only one, blocks every writer. Keeping a
`Database` object alive in a notebook is free and blocks nobody; there
is no `close()` because there is nothing to close. `query()` returns a
`pyarrow.RecordBatchReader` backed by batches that were already read,
not by a live connection.

## Usage

```python
import pyarrow as pa
import nodal_db

# Create a database (optionally with a default alternative), then load a template.
db = nodal_db.init("model.duckdb", default_alternative="Base")
db.template_apply("template.yaml", user="my_tool")

# Writes take Arrow batches in long form: an entity column, any index
# columns, and a value column. The core decides how to store the value
# and creates missing classes, parameters, alternatives and entities
# (reported as warnings).
capacity = pa.record_batch({
    "entity": ["coal_plant", "gas_plant"],
    "value":  pa.array([400.0, 250.0], pa.float64()),
})
report = db.write_parameter("unit", "capacity", "Base", capacity,
                            user="analyst", message="initial capacities")
assert report["tiers"] == ["scalar"]

# A time series: index columns between entity and value; the Arrow type
# picks the index kind (timestamp[us] -> datetime).
flow = pa.record_batch({
    "entity": ["coal_plant"] * 3,
    "time":   pa.array([...], pa.timestamp("us")),
    "value":  pa.array([1.0, 2.0, 3.0], pa.float64()),
})
db.write_parameter("unit", "flow", "scenario_a", flow, user="analyst")

# One save: writes and deletes (and structural changes, purges and
# metadata) in one atomic commit.
db.commit(
    segments=[{"class": "unit", "parameter": "capacity",
               "alternative": "Base", "batch": capacity}],
    deletes=[("unit", "capacity", "Base", "old_plant")],
    user="analyst",
)

# Reads: the convenience wrapper, or plain SQL against the read surface.
batch = db.read_parameter("unit", "capacity", "Base")     # pyarrow.RecordBatch
table = db.query("SELECT * FROM read_parameter('unit', 'capacity')").read_all()

# Replace a superseded result alternative: erase it physically (an
# erasure marker records it). expect_present makes the erase refuse
# unless the replacement run's alternative exists.
db.erase_alternative("run_2026_09_01", user="my_tool",
                     expect_present=["run_2026_09_22"])
```

### Migration programs (template `run:` steps)

A template can name migration programs (`run:` steps). The caller
registers them as **plain Python callables** for the duration of one
`template_apply` call; they write through the ordinary API:

```python
def backfill_efficiency(db_path, user):
    # Called by the core during the apply. Writing through nodal_db from
    # here is the intended use (see the reentrancy rules below).
    inner = nodal_db.open(db_path)
    inner.write_parameter("unit", "efficiency", "Base", some_batch, user=user)

db.template_apply("template_v2.yaml", user="my_tool",
                  programs={"backfill_efficiency": backfill_efficiency})
```

- **Programs must be idempotent.** A program commits through the public
  API on its own, and the apply records the program's progress in a
  separate commit afterwards, so a crash between the two re-runs the
  program when the apply is resumed.
- A template naming a program that is **not registered** fails before
  any step runs (`nodal_db.ApiError`).
- A program that **raises** aborts the apply, and the **original Python
  exception** propagates to the `template_apply` caller. Steps already
  committed stay recorded; applying again resumes after them.

## Threads and reentrancy

1. Every method **releases the GIL** while the core works, so other
   Python threads run during database work.
2. The core calls a `run:` program at a point where it holds **no
   connection and no in-process lock**.
3. The GIL is taken again **only for the program itself**, and the
   program's own `nodal_db` calls release it again (rule 1). A program
   calling back into the API therefore cannot deadlock.
4. For program authors: do all database work through `nodal_db` (or
   short-lived connections you close yourself); never keep a connection
   open, or the next commit of the apply fails with `nodal_db.Busy`.
5. **A program must not call `template_apply` on the database it is
   migrating.** Its progress is recorded only after it returns, so a
   nested apply would run the program again. The core refuses this
   within one process with `nodal_db.ApiError`; a second *process*
   applying at the same time cannot be detected and is an ordinary
   concurrent-writer race. Value writes, reads, plans and erases are
   all fine.
6. **No detached writers.** Any thread or process a program starts must
   finish its writes before the program returns: the apply attributes
   commits to a program by the range of commit ids around the call, so a
   later write is attributed to the wrong step and races the apply's own
   commits.

## Errors

Every error derives from `nodal_db.NodalDbError`:

- `Busy` (the file is locked by another process) and `Conflict` (a write
  conflict outlasted the retry budget): defined, retryable outcomes,
  never hangs.
- `ApiError`: a batch, name or argument the core cannot accept. Its
  subclasses: `Stale` (an `expect_commit=` precondition failed; nothing
  was written; carries `expected` and `found`), `SchemaMismatch` (the
  file was made by another schema version; `found`, `expected`),
  `CoveredWrite` and `WriteAndDeleteSameKey` (a write and a delete that
  cover each other in one call), `UndeclaredValueType`,
  `ValueMetadataUnstated`, `ReadSurfaceStale`, `TemplateStampedRename`
  and `TemplateStampedPurge`.
- `VersionTooOld`, `SchemaError`, `DatabaseError`, `CanonError`,
  `ArrowError`, `InternalError`.
- `UnreplayableWal`: compaction cannot fold a leftover `<db>.wal` into
  the file; nothing was changed. Carries `wal` and `cause`. Move the WAL
  aside, then compact.
- `ShadowedBuiltin`: the file defines a macro named like a DuckDB
  built-in function in its `main` schema (for example `CREATE MACRO
  lower(x) AS ...` run with direct SQL), which DuckDB would use instead
  of the built-in. Carries `name`, `schema` and `kind`. Drop the macro
  with direct SQL (the message gives the statement), then compact the
  file.

`ReadSurfaceStaleWarning` (a `UserWarning`) is issued once per file when
its views are older than this package's. Each exception's docstring
lists its attributes; the
[reference](https://nodal-tools.github.io/nodal-db/reference/#exceptions)
shows the whole tree.

## Arrow interop

- **In**: any Arrow PyCapsule producer: `pyarrow.RecordBatch`,
  `pyarrow.Table`, `pyarrow.RecordBatchReader`, polars. Imported
  zero-copy over the Arrow C data interface.
- **Out**: `pyarrow.RecordBatch` (`read_parameter`, `read_values`) and
  `pyarrow.RecordBatchReader` (`query`), exported zero-copy.
- Requires `pyarrow>=16` (the PyCapsule interface in both directions).

## Building from source

From a checkout of the repository:

```sh
python -m venv .venv && . .venv/bin/activate
pip install maturin
cd crates/nodal-db-py
maturin develop            # build and install into the venv (debug)
maturin build --release    # a release wheel into target/wheels/
pip install pytest 'duckdb>=2.0' && pytest tests
```

The first build compiles the bundled DuckDB (several minutes). Version
coupling between duckdb-rs, `arrow`, `arrow-pyarrow` and `pyo3` is
described in the repository's root `Cargo.toml`.

## License

EUPL-1.2. See the repository's `LICENSE` and README.
