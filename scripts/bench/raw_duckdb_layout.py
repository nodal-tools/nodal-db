#!/usr/bin/env python3
"""Raw-DuckDB storage-layout comparison: dense DOUBLE[] lists vs a lean long
(key, pos, value) table, with no nodal-db schema in the way.

Companion to the results-path bench (`crates/nodal-db-cli/benches/results_path.rs`,
run with `just bench-results`). That bench measures nodal-db's own tables;
this script separates *format* effects (what DuckDB does with a list column
vs a sorted narrow table) from nodal-db's long-tier overhead (history
columns, generated key columns, the one-current unique index).

It uses only the `duckdb` Python package (plus numpy and pyarrow for data
generation), so it runs under any DuckDB version: point it at a throwaway
venv to compare engine releases, e.g.

    python3 -m venv /tmp/duck2 && /tmp/duck2/bin/pip install duckdb==2.0.0 numpy pyarrow
    /tmp/duck2/bin/python scripts/bench/raw_duckdb_layout.py

Data is bit-identical to the Rust bench's dense shape: the same splitmix64
generator and seed, full-precision noise in [-100, 100), 8 760 hourly values
per series, series key k = param * entities + entity. A second, small long
table reproduces the Rust bench's sparse-int long shape (every 10th hour,
integer values) for a per-cell size comparison with nodal-db's long tier.

Tables (each in its own file, CHECKPOINTed, so file size = format size):
  dense_<rg>.db  dense(k INTEGER, vmin DOUBLE, vmax DOUBLE, v DOUBLE[])
                 sorted by k; one file per ROW_GROUP_SIZE in --row-groups
  long.db        long(k INTEGER, pos INTEGER, v DOUBLE) sorted by (k, pos)
  long_int.db    long_int(k INTEGER, pos INTEGER, v BIGINT), sparse shape

Reads (end-to-end to Arrow): one cell, one week
(168 points), one whole series, min/max only, one week of 20 series, and a
full-table aggregate scan. Warm = one connection, 3 warmups; cold = fresh
read-only connection per rep with the open timed separately.

Usage:
    raw_duckdb_layout.py [--quick] [--workdir DIR] [--row-groups default,2048]
                         [--keep] [--json OUT.json]
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import sys
import tempfile
import time

import duckdb

try:
    import numpy as np
    import pyarrow as pa
except ImportError:  # pragma: no cover - reported to the user
    sys.exit("raw_duckdb_layout.py needs numpy and pyarrow next to duckdb "
             "(pip install numpy pyarrow)")

CELLS = 8760
SEED = 0x6E6F64616C_2025  # same seed as the Rust results-path bench
U64_MAX = (1 << 64) - 1
SPARSE_STRIDE = 10
WIN_LO = 4000  # 0-based hour; DuckDB list slices are 1-based inclusive
WIN_PTS = 168
READ_SERIES = 20


# --- deterministic data (bit-identical to results_path.rs fval/ival) --------

def _splitmix64(x: np.ndarray) -> np.ndarray:
    with np.errstate(over="ignore"):
        z = x + np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return z ^ (z >> np.uint64(31))


def fvals(p: int, entities: int) -> np.ndarray:
    """(entities, 8760) float64 matrix for parameter p."""
    e = np.arange(entities, dtype=np.uint64)[:, None]
    h = np.arange(CELLS, dtype=np.uint64)[None, :]
    x = np.uint64(SEED) ^ (np.uint64(p) << np.uint64(40)) ^ (e << np.uint64(20)) ^ h
    r = _splitmix64(x)
    return (r.astype(np.float64) / np.float64(U64_MAX)) * 200.0 - 100.0


def ivals(entities: int) -> np.ndarray:
    """(entities, 876) int64 matrix: the sparse-int long shape (param 0)."""
    e = np.arange(entities, dtype=np.uint64)[:, None]
    k = np.arange(CELLS // SPARSE_STRIDE, dtype=np.uint64)[None, :]
    x = np.uint64(~SEED & U64_MAX) ^ (e << np.uint64(20)) ^ k
    return (_splitmix64(x) % np.uint64(100_000)).astype(np.int64)


def _selfcheck() -> None:
    """Spot-check the vectorized generator against a scalar reference."""
    def ref(p: int, e: int, h: int) -> float:
        x = (SEED ^ (p << 40) ^ (e << 20) ^ h) & U64_MAX
        x = (x + 0x9E3779B97F4A7C15) & U64_MAX
        z = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & U64_MAX
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & U64_MAX
        z ^= z >> 31
        return (float(z) / float(U64_MAX)) * 200.0 - 100.0
    m = fvals(3, 40)
    for e, h in [(0, 0), (7, 4000), (39, 8759), (21, 123)]:
        assert m[e, h] == ref(3, e, h), "generator drift vs the Rust bench"


# --- helpers ----------------------------------------------------------------

def fetch(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Run to Arrow on the client; return the number of materialized points."""
    rel = con.sql(sql)
    # Relation API: to_arrow_table() on newer clients, fetch_arrow_table() on
    # older ones (no extra DB-API copy on either).
    t = rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table()
    col = t.column(0)
    if pa.types.is_list(col.type) or pa.types.is_large_list(col.type):
        return sum(len(c.flatten()) for c in col.chunks)
    return t.num_rows


def timed(fn, reps: int) -> tuple[float, float]:
    ts = []
    for _ in range(reps):
        t = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t) * 1e3)
    return min(ts), statistics.median(ts)


def time_query(path: str, sql: str, expect: int, cold_reps: int, warm_reps: int) -> dict:
    def cold_once():
        c = duckdb.connect(path, read_only=True)
        t = time.perf_counter()
        n = fetch(c, sql)
        m = (time.perf_counter() - t) * 1e3
        c.close()
        assert n == expect, (sql, n, expect)
        return m
    cold = [cold_once() for _ in range(cold_reps)]
    c = duckdb.connect(path, read_only=True)
    for _ in range(3):
        assert fetch(c, sql) == expect, sql
    wmin, wmed = timed(lambda: fetch(c, sql), warm_reps)
    c.close()
    return {"warm_min": wmin, "warm_med": wmed,
            "cold_min": min(cold), "cold_med": statistics.median(cold)}


def open_tax(path: str, reps: int) -> dict:
    def once():
        c = duckdb.connect(path, read_only=True)
        c.close()
    mn, md = timed(once, max(reps, 5))
    return {"min": mn, "med": md}


def storage_shape(path: str, table: str, col: str) -> tuple[int, int]:
    c = duckdb.connect(path, read_only=True)
    r = c.execute(
        f"SELECT count(DISTINCT row_group_id), "
        f"count(DISTINCT block_id) FILTER (WHERE column_name = '{col}' AND block_id >= 0) "
        f"FROM pragma_storage_info('{table}')").fetchone()
    c.close()
    return int(r[0]), int(r[1])


def empty_file_bytes(workdir: str) -> int:
    p = os.path.join(workdir, "empty.db")
    c = duckdb.connect(p)
    c.execute("CREATE TABLE t(k INTEGER)")
    c.execute("CHECKPOINT")
    c.close()
    b = os.path.getsize(p)
    os.remove(p)
    return b


# --- build ------------------------------------------------------------------

def build_all(workdir: str, params: int, entities: int, row_groups: list[str]) -> dict:
    """Write dense (one file per row-group size), long and long_int files;
    return paths and insert+checkpoint timings."""
    out: dict = {"dense": {}}
    mem = duckdb.connect()
    pos = np.arange(CELLS, dtype=np.int32)

    # Dense: one file per row-group size, filled param by param.
    for rg in row_groups:
        path = os.path.join(workdir, f"dense_{rg}.db")
        opt = "" if rg == "default" else f" (ROW_GROUP_SIZE {int(rg)})"
        mem.execute(f"ATTACH '{path}' AS d{opt}")
        mem.execute("CREATE TABLE d.dense(k INTEGER, vmin DOUBLE, vmax DOUBLE, v DOUBLE[])")
        t_ins = 0.0
        for p in range(params):
            m = fvals(p, entities)
            keys = np.arange(p * entities, (p + 1) * entities, dtype=np.int32)
            offsets = pa.array(np.arange(0, (entities + 1) * CELLS, CELLS, dtype=np.int32))
            lists = pa.ListArray.from_arrays(offsets, pa.array(m.ravel()))
            src = pa.table({"k": keys, "vmin": m.min(axis=1), "vmax": m.max(axis=1), "v": lists})
            mem.register("src", src)
            t = time.perf_counter()
            mem.execute("INSERT INTO d.dense SELECT * FROM src ORDER BY k")
            t_ins += time.perf_counter() - t
            mem.unregister("src")
        t = time.perf_counter()
        mem.execute("CHECKPOINT d")
        t_ins += time.perf_counter() - t
        mem.execute("DETACH d")
        out["dense"][rg] = {"path": path, "build_s": t_ins}

    # Long (same float values, one row per cell).
    lpath = os.path.join(workdir, "long.db")
    mem.execute(f"ATTACH '{lpath}' AS l")
    mem.execute("CREATE TABLE l.long(k INTEGER, pos INTEGER, v DOUBLE)")
    t_ins = 0.0
    for p in range(params):
        m = fvals(p, entities)
        keys = np.arange(p * entities, (p + 1) * entities, dtype=np.int32)
        src = pa.table({"k": np.repeat(keys, CELLS), "pos": np.tile(pos, entities), "v": m.ravel()})
        mem.register("src", src)
        t = time.perf_counter()
        mem.execute("INSERT INTO l.long SELECT * FROM src ORDER BY k, pos")
        t_ins += time.perf_counter() - t
        mem.unregister("src")
    t = time.perf_counter()
    mem.execute("CHECKPOINT l")
    t_ins += time.perf_counter() - t
    mem.execute("DETACH l")
    out["long"] = {"path": lpath, "build_s": t_ins}

    # Long-int sparse shape (the Rust bench's long-tier data, param 0).
    ipath = os.path.join(workdir, "long_int.db")
    mem.execute(f"ATTACH '{ipath}' AS i")
    mem.execute("CREATE TABLE i.long_int(k INTEGER, pos INTEGER, v BIGINT)")
    iv = ivals(entities)
    n_sp = CELLS // SPARSE_STRIDE
    src = pa.table({"k": np.repeat(np.arange(entities, dtype=np.int32), n_sp),
                    "pos": np.tile(np.arange(n_sp, dtype=np.int32), entities),
                    "v": iv.ravel()})
    mem.register("src", src)
    mem.execute("INSERT INTO i.long_int SELECT * FROM src ORDER BY k, pos")
    mem.unregister("src")
    mem.execute("CHECKPOINT i")
    mem.execute("DETACH i")
    out["long_int"] = {"path": ipath, "cells": entities * n_sp}
    mem.close()
    return out


# --- main -------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--quick", action="store_true", help="1 param x 1000 series instead of 10 x 1000")
    ap.add_argument("--workdir", help="where to write the DB files (default: a temp dir)")
    ap.add_argument("--row-groups", default="default,2048",
                    help="comma list of ROW_GROUP_SIZE values for the dense table ('default' = engine default)")
    ap.add_argument("--keep", action="store_true", help="keep the DB files")
    ap.add_argument("--json", help="also write the results as JSON to this path")
    a = ap.parse_args()

    _selfcheck()
    params, entities = (1, 1000) if a.quick else (10, 1000)
    series, values = params * entities, params * entities * CELLS
    cold_reps, warm_reps = (3, 7) if a.quick else (5, 15)
    row_groups = [r.strip() for r in a.row_groups.split(",") if r.strip()]
    workdir = a.workdir or tempfile.mkdtemp(prefix="nodal-rawlayout-")
    os.makedirs(workdir, exist_ok=True)

    res: dict = {
        "duckdb": duckdb.__version__,
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pyarrow": pa.__version__,
        "machine": platform.machine(),
        "platform": platform.platform(),
        "params": params, "entities": entities, "series": series, "values": values,
        "cold_reps": cold_reps, "warm_reps": warm_reps,
    }
    print(f"raw DuckDB layout comparison — duckdb {duckdb.__version__}, "
          f"{series} series x {CELLS} = {values} values; workdir {workdir}")
    t0 = time.perf_counter()
    built = build_all(workdir, params, entities, row_groups)
    empty = empty_file_bytes(workdir)
    res["empty_file_bytes"] = empty
    print(f"built in {time.perf_counter() - t0:.1f} s (empty one-table file: {empty} B)")

    # Deterministic picks, same as the Rust bench: entity (37*i+11) % entities
    # of param 0.
    picks = [(37 * i + 11) % entities for i in range(READ_SERIES)]
    one, ids = picks[0], ",".join(str(k) for k in picks)
    lo1, hi1 = WIN_LO + 1, WIN_LO + WIN_PTS

    def size_row(name: str, path: str, n: int) -> dict:
        b = os.path.getsize(path)
        return {"name": name, "bytes": b, "mib": b / 2**20,
                "bytes_per_value": b / n, "net_bytes_per_value": (b - empty) / n}

    res["sizes"] = []
    res["reads"] = {}
    for rg, d in built["dense"].items():
        sz = size_row(f"dense DOUBLE[] (ROW_GROUP_SIZE {rg})", d["path"], values)
        sz["row_groups"], sz["list_blocks"] = storage_shape(d["path"], "dense", "v")
        sz["build_s"] = d["build_s"]
        res["sizes"].append(sz)
    lsz = size_row("long (k, pos, v DOUBLE)", built["long"]["path"], values)
    lsz["row_groups"], _ = storage_shape(built["long"]["path"], "long", "v")
    lsz["build_s"] = built["long"]["build_s"]
    res["sizes"].append(lsz)
    isz = size_row("long_int sparse (k, pos, v BIGINT)", built["long_int"]["path"], built["long_int"]["cells"])
    isz["row_groups"], _ = storage_shape(built["long_int"]["path"], "long_int", "v")
    res["sizes"].append(isz)

    print("\nsizes (after CHECKPOINT):")
    for s in res["sizes"]:
        print(f"  {s['name']:42} {s['bytes']:>12} B ({s['mib']:8.1f} MiB)  "
              f"{s['bytes_per_value']:6.2f} B/value  {s['net_bytes_per_value']:6.2f} net  "
              f"row groups {s['row_groups']}" + (f"  list blocks {s['list_blocks']}" if "list_blocks" in s else ""))

    dense_q = [
        ("one cell", f"SELECT v[{lo1}] FROM dense WHERE k = {one}", 1),
        ("one week (168)", f"SELECT v[{lo1}:{hi1}] FROM dense WHERE k = {one}", WIN_PTS),
        ("one whole series (8760)", f"SELECT v FROM dense WHERE k = {one}", CELLS),
        ("min/max only", f"SELECT vmin, vmax FROM dense WHERE k = {one}", 1),
        ("one week x 20 series", f"SELECT v[{lo1}:{hi1}] FROM dense WHERE k IN ({ids})", READ_SERIES * WIN_PTS),
        ("full scan aggregate", "SELECT sum(list_sum(v)) FROM dense", 1),
    ]
    long_q = [
        ("one cell", f"SELECT v FROM long WHERE k = {one} AND pos = {WIN_LO}", 1),
        ("one week (168)", f"SELECT v FROM long WHERE k = {one} AND pos BETWEEN {WIN_LO} AND {WIN_LO + WIN_PTS - 1}", WIN_PTS),
        ("one whole series (8760)", f"SELECT v FROM long WHERE k = {one}", CELLS),
        ("min/max only", f"SELECT min(v), max(v) FROM long WHERE k = {one}", 1),
        ("one week x 20 series",
         f"SELECT v FROM long WHERE k IN ({ids}) AND pos BETWEEN {WIN_LO} AND {WIN_LO + WIN_PTS - 1}",
         READ_SERIES * WIN_PTS),
        ("full scan aggregate", "SELECT sum(v) FROM long", 1),
    ]
    targets = [(f"dense rg={rg}", d["path"], dense_q) for rg, d in built["dense"].items()]
    targets.append(("long", built["long"]["path"], long_q))
    for name, path, qs in targets:
        print(f"\nreads: {name}  (RO open: ", end="")
        ot = open_tax(path, cold_reps)
        print(f"min {ot['min']:.2f} / med {ot['med']:.2f} ms)")
        rows = {"open": ot}
        for label, sql, expect in qs:
            r = time_query(path, sql, expect, cold_reps, warm_reps)
            rows[label] = r
            print(f"  {label:26} warm min {r['warm_min']:8.2f} / med {r['warm_med']:8.2f} ms   "
                  f"cold min {r['cold_min']:8.2f} / med {r['cold_med']:8.2f} ms")
        res["reads"][name] = rows

    res["total_s"] = time.perf_counter() - t0
    print(f"\ntotal {res['total_s']:.1f} s")
    if a.json:
        with open(a.json, "w") as f:
            json.dump(res, f, indent=2)
    if not a.keep and not a.workdir:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
