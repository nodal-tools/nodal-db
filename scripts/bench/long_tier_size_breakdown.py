#!/usr/bin/env python3
"""Where the long tier's bytes go: a size breakdown of `value_long`.

Companion to the results-path bench (`crates/nodal-db-cli/benches/results_path.rs`)
and `raw_duckdb_layout.py`. Those measure that nodal-db's long tier costs about
80 bytes per cell against about 4 bytes for a lean `(k, pos, v)` table in plain
DuckDB; this script attributes the difference to parts of the schema.

Every variant starts from the real schema, built by `schema/build.py` from
`schema/*.sql` (no hand copy of the DDL). Variants remove one part at a time:

  A  schema as built: foreign keys, the 7-column one-current unique index
     `one_current_long` (and, before 2026-10-24, the key-triple index
     `ix_value_long_triple`)
  B  A without `one_current_long`
  C  B without `ix_value_long_triple`. The schema has not had that index
     since 2026-10-24 (DuckDB 2.0 never scans through a multi-column index;
     specs/name_index_and_unused_indexes_PLAN.md), so C is B and its row in
     the attribution reads 0.00; the letter stays because RESULTS §7.1 and
     the long-tier findings cite A-E by letter. `--core-db` on a file
     written before that date still measures it.
  D  C with `value_long` re-created without its REFERENCES clauses (DDL taken
     from the built file's catalog, REFERENCES stripped)
  E  D without the `i1_dt` scan-accelerator column
  L  lean reference: plain `(k INTEGER, pos INTEGER, v BIGINT)`, no schema

Each part's cost is the difference between adjacent variants. Sizes are
DuckDB's used blocks after CHECKPOINT (`PRAGMA database_size`, used_blocks x
block_size), net of the same file before the insert; the net file-size
growth is printed next to it (it also counts free blocks).

Data: the results-path bench's long-tier shape, 1 000 series x 876 integer
cells (every tenth hour of 2025), values bit-identical to `results_path.rs`
`ival` (same splitmix64 generator and seed as `raw_duckdb_layout.py`). Rows
are written as the core's one-commit import writes them: one transaction,
one INSERT per series in position order, version 1, one shared parametric
datetime axis, `i1_dt` filled, the history close-out column empty.
Deterministic: fixed data, `SET threads = 1` on every connection.

Optional cross-check (`--core-db FILE`): the same attribution on a file the
core itself wrote, for example the results-path bench's long-tier file
(`NODAL_RESULTS_DIR=/tmp/nr NODAL_RESULTS_KEEP=1 cargo bench -p nodal-db-cli
--bench results_path -- long`, then `--core-db /tmp/nr/ipc_long_r0.db`).
It also reports what the other tables (entities, commit, ...) hold, which the
bench's per-cell file size includes.

Usage:
    long_tier_size_breakdown.py [--entities N] [--workdir DIR] [--keep]
                                [--json OUT.json] [--core-db FILE]

Needs duckdb >= 2.0 (the schema build refuses older), numpy and pyarrow.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import os
import pathlib
import platform
import re
import shutil
import sys
import tempfile
import time

import duckdb

try:
    import numpy as np
    import pyarrow as pa
except ImportError:  # pragma: no cover - reported to the user
    sys.exit("long_tier_size_breakdown.py needs numpy and pyarrow next to duckdb "
             "(pip install numpy pyarrow)")

REPO = pathlib.Path(__file__).resolve().parents[2]
CELLS = 8760
SPARSE_STRIDE = 10
SPARSE_CELLS = CELLS // SPARSE_STRIDE  # 876
SEED = 0x6E6F64616C_2025  # same seed as the Rust results-path bench
U64_MAX = (1 << 64) - 1
T0_US = 1_735_689_600 * 1_000_000  # 2025-01-01T00:00:00Z
HOUR_US = 3_600 * 1_000_000


def load_schema_build():
    spec = importlib.util.spec_from_file_location("schema_build", REPO / "schema" / "build.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --- deterministic data (bit-identical to results_path.rs ival) -------------

def _splitmix64(x: np.ndarray) -> np.ndarray:
    with np.errstate(over="ignore"):
        z = x + np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return z ^ (z >> np.uint64(31))


def ivals(entities: int) -> np.ndarray:
    """(entities, 876) int64 matrix: the sparse-int long shape (param 0)."""
    e = np.arange(entities, dtype=np.uint64)[:, None]
    k = np.arange(SPARSE_CELLS, dtype=np.uint64)[None, :]
    x = np.uint64(~SEED & U64_MAX) ^ (e << np.uint64(20)) ^ k
    return (_splitmix64(x) % np.uint64(100_000)).astype(np.int64)


def series_tables(entities: int, commit_id: int, axis_id: int) -> list[pa.Table]:
    """One Arrow table per series, holding exactly the columns the core
    fills for a rank-1 integer value on a datetime axis (the rest stay NULL
    or take their column DEFAULT)."""
    vals = ivals(entities)
    pos = np.arange(SPARSE_CELLS, dtype=np.int32)
    ts = pa.array(T0_US + pos.astype(np.int64) * SPARSE_STRIDE * HOUR_US, pa.timestamp("us"))
    n = SPARSE_CELLS
    out = []
    for e in range(entities):
        out.append(pa.table({
            "entity_id": np.full(n, e + 1, np.int64),
            "parameter_definition_id": np.ones(n, np.int64),
            "alternative_id": np.ones(n, np.int64),
            "parameter_type_id": np.ones(n, np.int64),
            "valid_from_commit": np.full(n, commit_id, np.int64),
            "version": np.ones(n, np.int16),
            "axis1_id": np.full(n, axis_id, np.int64),
            "i1_pos": pos,
            "i1_dt": ts,
            "value_kind": pa.array(["int"] * n),
            "v_int": vals[e],
        }))
    return out


# --- measurement helpers ----------------------------------------------------

def connect(path: str):
    con = duckdb.connect(path)
    con.execute("SET threads = 1")
    return con


def used_bytes(con) -> int:
    cur = con.execute("PRAGMA database_size")
    row = cur.fetchone()
    d = dict(zip([c[0] for c in cur.description], row))
    return int(d["used_blocks"]) * int(d["block_size"])


def build_template(workdir: str) -> tuple[str, int, int]:
    """Real schema plus the one commit row and one axis row the cells point to."""
    path = os.path.join(workdir, "template.db")
    sb = load_schema_build()
    with contextlib.redirect_stdout(io.StringIO()):
        sb.build(path, force=True)
    con = connect(path)
    commit_id = con.execute(
        "INSERT INTO commit (user, message) VALUES ('bench', 'long-tier size breakdown') "
        "RETURNING id").fetchone()[0]
    axis_id = con.execute(
        "INSERT INTO axis (kind, length, content_hash, p_start_ts, p_step_months, p_step_micros) "
        "VALUES ('datetime', ?, 0, make_timestamp(?), 0, ?) RETURNING id",
        [SPARSE_CELLS, T0_US, SPARSE_STRIDE * HOUR_US]).fetchone()[0]
    con.execute("CHECKPOINT")
    con.close()
    return path, commit_id, axis_id


def value_long_ddl_without_fks(con) -> str:
    sql = con.execute(
        "SELECT sql FROM duckdb_tables() WHERE table_name = 'value_long'").fetchone()[0]
    # The catalog renders each FK as a table constraint:
    #   , FOREIGN KEY (col) REFERENCES "commit"(id)
    stripped = re.sub(r',\s*FOREIGN KEY\s*\(\s*\w+\s*\)\s*REFERENCES\s*"?\w+"?\s*\(\s*\w+\s*\)',
                      "", sql, flags=re.IGNORECASE)
    assert "REFERENCES" not in stripped.upper(), "could not strip the REFERENCES clauses"
    return stripped


def prepare_variant(con, variant: str) -> None:
    if variant in ("B", "C", "D", "E"):
        con.execute("DROP INDEX one_current_long")
    if variant in ("C", "D", "E"):
        con.execute("DROP INDEX IF EXISTS ix_value_long_triple")
    if variant in ("D", "E"):
        ddl = value_long_ddl_without_fks(con)
        con.execute("DROP TABLE value_long")
        con.execute(ddl)
    if variant == "E":
        con.execute("ALTER TABLE value_long DROP COLUMN i1_dt")


def measure_schema_variant(variant: str, template: str, workdir: str,
                           tables: list[pa.Table]) -> dict:
    path = os.path.join(workdir, f"variant_{variant}.db")
    shutil.copyfile(template, path)
    con = connect(path)
    prepare_variant(con, variant)
    con.execute("CHECKPOINT")
    base_used, base_file = used_bytes(con), os.path.getsize(path)
    cols = [c for c in tables[0].column_names
            if not (variant == "E" and c == "i1_dt")]
    collist = ", ".join(cols)
    t = time.perf_counter()
    con.execute("BEGIN")
    for src in tables:  # one INSERT per series, as the core's import does
        con.execute(f"INSERT INTO value_long ({collist}) SELECT {collist} FROM src")
    con.execute("COMMIT")
    insert_s = time.perf_counter() - t
    con.execute("CHECKPOINT")
    used, fsize = used_bytes(con), os.path.getsize(path)
    n = con.execute("SELECT count(*) FROM value_long").fetchone()[0]
    comp = con.execute(
        "SELECT column_name, string_agg(DISTINCT compression, ',' ORDER BY compression) "
        "FROM pragma_storage_info('value_long') WHERE segment_type <> 'VALIDITY' "
        "GROUP BY column_id, column_name ORDER BY column_id").fetchall()
    con.close()
    return {"variant": variant, "cells": n,
            "net_used_bytes": used - base_used, "net_file_bytes": fsize - base_file,
            "used_B_per_cell": (used - base_used) / n,
            "file_B_per_cell": (fsize - base_file) / n,
            "insert_s": insert_s, "compression": comp}


def measure_lean(workdir: str, entities: int) -> dict:
    path = os.path.join(workdir, "variant_L.db")
    con = connect(path)
    con.execute("CREATE TABLE lean (k INTEGER, pos INTEGER, v BIGINT)")
    con.execute("CHECKPOINT")
    base_used, base_file = used_bytes(con), os.path.getsize(path)
    src = pa.table({
        "k": np.repeat(np.arange(entities, dtype=np.int32), SPARSE_CELLS),
        "pos": np.tile(np.arange(SPARSE_CELLS, dtype=np.int32), entities),
        "v": ivals(entities).ravel(),
    })
    con.execute("INSERT INTO lean SELECT * FROM src")
    con.execute("CHECKPOINT")
    used, fsize = used_bytes(con), os.path.getsize(path)
    n = src.num_rows
    con.close()
    return {"variant": "L", "cells": n,
            "net_used_bytes": used - base_used, "net_file_bytes": fsize - base_file,
            "used_B_per_cell": (used - base_used) / n,
            "file_B_per_cell": (fsize - base_file) / n}


def check_core_db(core_db: str, workdir: str) -> dict:
    """Cross-check on a file the core wrote (e.g. the results-path bench's
    kept long-tier file): used blocks as written, after dropping
    one_current_long, after also dropping ix_value_long_triple (present only
    in files written before 2026-10-24; a no-op otherwise), and after
    dropping value_long altogether (what the other tables hold). Works on a
    scratch copy; the input file is not modified."""
    path = os.path.join(workdir, "core_copy.db")
    shutil.copyfile(core_db, path)
    con = connect(path)
    n = con.execute("SELECT count(*) FROM value_long").fetchone()[0]
    steps = [("as written", None),
             ("without one_current_long", "DROP INDEX one_current_long"),
             ("also without ix_value_long_triple", "DROP INDEX IF EXISTS ix_value_long_triple"),
             ("without value_long (other tables only)", "DROP TABLE value_long")]
    out = []
    for label, sql in steps:
        if sql:
            con.execute(sql)
            con.execute("CHECKPOINT")
        out.append((label, used_bytes(con)))
    con.close()
    os.remove(path)
    return {"cells": n, "used_bytes": out}


LABELS = {
    "A": "schema as built (FKs + one_current_long)",
    "B": "A without one_current_long",
    "C": "B without ix_value_long_triple (absent since 2026-10-24)",
    "D": "C without REFERENCES (no foreign keys)",
    "E": "D without the i1_dt accelerator column",
    "L": "lean (k, pos, v BIGINT), no nodal-db schema",
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entities", type=int, default=1000,
                    help="series count (default 1000, the results-path long shape)")
    ap.add_argument("--workdir", help="directory for the variant files (default: a temp dir)")
    ap.add_argument("--keep", action="store_true", help="keep the variant files")
    ap.add_argument("--json", help="also write the results as JSON to this path")
    ap.add_argument("--core-db", help="optional cross-check on a long-tier file written by the "
                    "core (see the module docstring); read through a scratch copy")
    args = ap.parse_args()

    workdir = args.workdir or tempfile.mkdtemp(prefix="nodal-longsize-")
    os.makedirs(workdir, exist_ok=True)
    print(f"long-tier size breakdown: duckdb {duckdb.__version__}, "
          f"{args.entities} series x {SPARSE_CELLS} integer cells = "
          f"{args.entities * SPARSE_CELLS} cells; threads = 1; workdir {workdir}")

    template, commit_id, axis_id = build_template(workdir)
    tables = series_tables(args.entities, commit_id, axis_id)
    rows = [measure_schema_variant(v, template, workdir, tables) for v in "ABCDE"]
    rows.append(measure_lean(workdir, args.entities))
    by = {r["variant"]: r for r in rows}

    print("\nvariant sizes (net of the same file before the insert, after CHECKPOINT):")
    print(f"  {'':2} {'':58} {'used B/cell':>12} {'file B/cell':>12}")
    for r in rows:
        print(f"  {r['variant']:2} {LABELS[r['variant']]:58} "
              f"{r['used_B_per_cell']:12.2f} {r['file_B_per_cell']:12.2f}")

    parts = [
        ("one_current_long (7-column unique index)", "A", "B"),
        ("ix_value_long_triple (absent since 2026-10-24)", "B", "C"),
        ("foreign keys (commit, axis)", "C", "D"),
        ("i1_dt accelerator column", "D", "E"),
    ]
    print("\nattribution (used blocks, B/cell):")
    breakdown = {}
    for name, hi, lo in parts:
        v = by[hi]["used_B_per_cell"] - by[lo]["used_B_per_cell"]
        breakdown[name] = v
        print(f"  {name:46} {v:7.2f}")
    breakdown["remaining columns (variant E)"] = by["E"]["used_B_per_cell"]
    print(f"  {'remaining columns (variant E)':46} {by['E']['used_B_per_cell']:7.2f}")
    print(f"  {'total (variant A)':46} {by['A']['used_B_per_cell']:7.2f}")
    print(f"  {'lean reference (variant L)':46} {by['L']['used_B_per_cell']:7.2f}")

    print("\ncompression per column, variant D (no indexes, no FKs):")
    for col, comp in by["D"]["compression"]:
        print(f"  {col:24} {comp}")

    core = None
    if args.core_db:
        core = check_core_db(args.core_db, workdir)
        sb = load_schema_build()
        empty = os.path.join(workdir, "empty_schema.db")
        with contextlib.redirect_stdout(io.StringIO()):
            sb.build(empty, force=True)
        con = connect(empty)
        con.execute("CHECKPOINT")
        empty_used = used_bytes(con)
        con.close()
        os.remove(empty)
        n = core["cells"]
        print(f"\ncross-check on the core-written file {args.core_db} ({n} cells), "
              f"used blocks net of an empty schema ({empty_used} B):")
        for label, b in core["used_bytes"]:
            print(f"  {label:46} {(b - empty_used) / n:7.2f} B/cell")
        core["empty_schema_used_bytes"] = empty_used

    if args.json:
        res = {"duckdb": duckdb.__version__, "python": platform.python_version(),
               "numpy": np.__version__, "pyarrow": pa.__version__,
               "platform": platform.platform(), "entities": args.entities,
               "variants": rows, "breakdown_used_B_per_cell": breakdown,
               "core_db_check": core}
        with open(args.json, "w") as f:
            json.dump(res, f, indent=2)
    if not args.keep and not args.workdir:
        shutil.rmtree(workdir, ignore_errors=True)
    elif not args.keep:
        for name in os.listdir(workdir):
            if name.startswith(("variant_", "template")):
                os.remove(os.path.join(workdir, name))


if __name__ == "__main__":
    main()
