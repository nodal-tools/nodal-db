#!/usr/bin/env python3
"""Surrogate-key variant of `value_array`, measured against the real schema.

SUPERSEDED (schema 0.4-array-id): the committed schema now carries the
`array_id` surrogate key, and the results-path bench measures it through
the real macros (`results_path.rs` phases `reads` and `cellsgate`). This
script is kept as the historical before/after on the 0.3 schema (composite
primary key): it expects an input file built with that schema, and its
commit probe's positional INSERTs do not fit the 0.4 column layout.

Hypothesis under test: dense-array point / slice / whole reads are slow
(~80-120 ms at results scale) not because of list length but because
`value_array`'s primary key is composite (entity, parameter, alternative).
DuckDB uses an index scan only for a single-column key; with a composite
key a point read falls back to a sequential scan that decodes the list data
of the whole vector around the row. A single-column `array_id` key, with
readers joining through it, should turn those reads into index scans.

The script never changes the committed schema. It reads a nodal-db file
built by the results-path bench and measures:

1. **Baseline**: the real file, real `read_parameter_dense` macro and direct
   `value_array` reads keyed by the triple.
2. **Composite-key control**: `value_array` copied into a scratch file with
   the same composite PRIMARY KEY (isolates "copy/layout" from "key shape").
3. **Surrogate-key variant**: the same rows with `array_id BIGINT PRIMARY KEY`
   plus `UNIQUE (entity_id, parameter_definition_id, alternative_id)`, and a
   variant of `read_parameter_dense` that resolves keys on narrow columns
   first and then joins to the payload through `array_id`.
4. **Long tier** (optional `--long-db`): the same reads on `value_long`
   (composite unique index on generated columns), with the physical
   operator each read uses, plus a variant with a single-column series key.
5. **Commit cost after an array replace** (`--commit-probe`, and
   `--commit-probe-native` for a copy re-written in the running engine's own
   storage format): the
   core's replace statements (pre-image copy to `value_array_hist`, then an
   in-place UPDATE) on a scratch copy of the file, timing each statement and
   COMMIT, under the default checkpoint threshold and with automatic
   checkpoints pushed out of reach, recording the WAL size after each commit.

Runs under any `duckdb` Python (plus pyarrow), so the same file can be
measured on several engine releases. Build the input once with:

    NODAL_RESULTS_DIR=/tmp/nr NODAL_RESULTS_KEEP=1 \\
      cargo bench -p nodal-db-cli --bench results_path -- dense10k long
    python scripts/bench/surrogate_key_variant.py --db /tmp/nr/ipc_dense_10p_r0.db \\
      --long-db /tmp/nr/ipc_long_r0.db --commit-probe

(`--quick` on the bench gives a small file that exercises the script but
does not show the effect: it needs more rows than one 2 048-row vector.)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import sys
import tempfile
import time

import duckdb

try:
    import pyarrow as pa
except ImportError:  # pragma: no cover
    sys.exit("surrogate_key_variant.py needs pyarrow next to duckdb (pip install pyarrow)")

CLASS_DENSE, PARAM_DENSE = "res_dense", "flow00"
CLASS_LONG, PARAM_LONG = "res_long", "state"
CELLS = 8760
WIN_LO, WIN_PTS, READ_SERIES = 4000, 168, 20
SPARSE_STRIDE = 10
TRIPLE = "entity_id, parameter_definition_id, alternative_id"


# --- helpers ----------------------------------------------------------------

def fetch(con, sql: str) -> int:
    rel = con.sql(sql)
    t = rel.to_arrow_table() if hasattr(rel, "to_arrow_table") else rel.fetch_arrow_table()
    col = t.column(0)
    if pa.types.is_list(col.type) or pa.types.is_large_list(col.type):
        return sum(len(c.flatten()) for c in col.chunks)
    return t.num_rows


def time_query(path: str, sql: str, expect: int, cold_reps: int, warm_reps: int) -> dict:
    cold = []
    for _ in range(cold_reps):
        c = duckdb.connect(path, read_only=True)
        t = time.perf_counter()
        n = fetch(c, sql)
        cold.append((time.perf_counter() - t) * 1e3)
        c.close()
        assert n == expect, (sql, n, expect)
    c = duckdb.connect(path, read_only=True)
    for _ in range(3):
        assert fetch(c, sql) == expect, sql
    warm = []
    for _ in range(warm_reps):
        t = time.perf_counter()
        fetch(c, sql)
        warm.append((time.perf_counter() - t) * 1e3)
    c.close()
    return {"warm_min": min(warm), "warm_med": statistics.median(warm),
            "cold_min": min(cold), "cold_med": statistics.median(cold)}


def scan_ops(path: str, sql: str) -> str:
    """Scan types the executed plan used (EXPLAIN ANALYZE: the index scan is
    chosen at run time, e.g. from a join's dynamic filter)."""
    c = duckdb.connect(path, read_only=True)
    plan = "\n".join(str(x) for r in c.execute("EXPLAIN ANALYZE " + sql).fetchall() for x in r)
    c.close()
    kinds = []
    for k in re.findall(r"Type: (Index Scan|Sequential Scan)", plan):
        k = "index" if k.startswith("Index") else "seq"
        if k not in kinds:
            kinds.append(k)
    return "scan: " + ("+".join(kinds) or "?")


def report(label: str, r: dict, ops: str = "") -> None:
    print(f"  {label:50} warm min {r['warm_min']:8.2f} / med {r['warm_med']:8.2f} ms   "
          f"cold min {r['cold_min']:8.2f} / med {r['cold_med']:8.2f} ms  {ops}")


def columns(con, table: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(f"DESCRIBE {table}").fetchall()]


def ids(path: str, cls: str, param: str) -> tuple[int, int, list[int], int]:
    """(parameter_definition_id, alternative_id, 20 picked entity ids, n entities)."""
    c = duckdb.connect(path, read_only=True)
    pd = c.execute(
        "SELECT pd.id FROM parameter_definition pd JOIN entity_class ec ON ec.id = pd.entity_class_id "
        "WHERE lower(ec.name)=lower(?) AND lower(pd.name)=lower(?) AND pd.valid_to_commit IS NULL",
        [cls, param]).fetchone()[0]
    alt = c.execute("SELECT id FROM alternative WHERE valid_to_commit IS NULL ORDER BY id LIMIT 1").fetchone()[0]
    n = c.execute("SELECT count(*) FROM entity WHERE valid_to_commit IS NULL").fetchone()[0]
    picks = []
    for k in range(READ_SERIES):
        name = f"e{(37 * k + 11) % n:04d}"
        picks.append(c.execute("SELECT id FROM entity WHERE name = ? AND valid_to_commit IS NULL",
                               [name]).fetchone()[0])
    c.close()
    return pd, alt, picks, n


# --- variant build ------------------------------------------------------------

PD_PICK = """
WITH pd_pick AS (
  SELECT pd.id FROM parameter_definition pd
  JOIN entity_class ec ON ec.id = pd.entity_class_id AND ec.valid_to_commit IS NULL
  WHERE pd.valid_to_commit IS NULL
    AND lower(ec.name) = lower(class_name) AND lower(pd.name) = lower(parameter_name))"""


def build_variants(src: str, workdir: str) -> dict:
    """Scratch file with value_array copied twice (composite PK control and
    surrogate PK variant) plus the catalog tables the macros need."""
    dst = os.path.join(workdir, f"sk_variant_{duckdb.__version__}.db")
    if os.path.exists(dst):
        os.remove(dst)
    con = duckdb.connect(dst)
    con.execute(f"ATTACH '{src}' AS s (READ_ONLY)")
    for t in ("entity_class", "parameter_definition", "entity", "alternative"):
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM s.{t}")
    cols = columns(con, "s.value_array")
    coldefs = ", ".join(f'"{n}" {ty}' for n, ty in cols)
    names = ", ".join(f'"{n}"' for n, _ in cols)
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE va_ck ({coldefs}, PRIMARY KEY ({TRIPLE}))")
    con.execute(f"INSERT INTO va_ck SELECT {names} FROM s.value_array ORDER BY parameter_definition_id, entity_id")
    t_ck = time.perf_counter() - t0
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE va_sk (array_id BIGINT PRIMARY KEY, {coldefs}, UNIQUE ({TRIPLE}))")
    con.execute(f"INSERT INTO va_sk SELECT row_number() OVER (ORDER BY parameter_definition_id, entity_id), "
                f"{names} FROM s.value_array ORDER BY parameter_definition_id, entity_id")
    t_sk = time.perf_counter() - t0
    # The real macro, verbatim apart from the table name (control) ...
    con.execute(f"""CREATE MACRO read_parameter_dense_ck(class_name, parameter_name) AS TABLE {PD_PICK}
        SELECT va.entity_id, va.alternative_id, va.axis1_id, va.axis2_id, va.axis3_id, va.axis4_id,
               va.values, va.values_int, va.v_min, va.v_max
        FROM va_ck va JOIN pd_pick ON pd_pick.id = va.parameter_definition_id""")
    # ... and the surrogate variant: keys resolved on narrow columns, payload
    # reached through the single-column key.
    con.execute(f"""CREATE MACRO read_parameter_dense_sk(class_name, parameter_name) AS TABLE {PD_PICK},
        k AS (SELECT va.array_id, va.entity_id, va.alternative_id
              FROM va_sk va JOIN pd_pick ON pd_pick.id = va.parameter_definition_id)
        SELECT k.entity_id, k.alternative_id, v.axis1_id, v.axis2_id, v.axis3_id, v.axis4_id,
               v.values, v.values_int, v.v_min, v.v_max
        FROM k JOIN va_sk v ON v.array_id = k.array_id""")
    con.execute("CHECKPOINT")
    con.close()
    return {"path": dst, "copy_ck_s": t_ck, "copy_sk_s": t_sk}


# --- dense reads ----------------------------------------------------------------

def dense_reads(db: str, var: str, cold: int, warm: int) -> dict:
    pd, alt, picks, _ = ids(db, CLASS_DENSE, PARAM_DENSE)
    one, inlist = picks[0], ",".join(map(str, picks))
    lo1, hi1 = WIN_LO + 1, WIN_LO + WIN_PTS
    trip = f"entity_id = {one} AND parameter_definition_id = {pd} AND alternative_id = {alt}"
    out: dict = {}

    def shapes(src: str, where_one: str, where_20: str):
        return [
            ("one cell", f"SELECT \"values\"[{lo1}] {src} WHERE {where_one}", 1),
            ("one week (168)", f"SELECT \"values\"[{lo1}:{hi1}] {src} WHERE {where_one}", WIN_PTS),
            ("one whole series (8760)", f"SELECT \"values\" {src} WHERE {where_one}", CELLS),
            ("min/max only", f"SELECT v_min, v_max {src} WHERE {where_one}", 1),
            ("one week x 20 series", f"SELECT \"values\"[{lo1}:{hi1}] {src} WHERE {where_20}",
             READ_SERIES * WIN_PTS),
        ]

    macro = "read_parameter_dense('{c}','{p}')".format(c=CLASS_DENSE, p=PARAM_DENSE)
    groups = [
        ("baseline: real file, read_parameter_dense", db,
         shapes(f"FROM {macro}", f"entity_id = {one}", f"entity_id IN ({inlist})")),
        ("baseline: real file, value_array by triple", db,
         shapes("FROM value_array", trip,
                f"entity_id IN ({inlist}) AND parameter_definition_id = {pd} AND alternative_id = {alt}")),
        ("control: composite-PK copy, macro", var,
         shapes(f"FROM {macro.replace('read_parameter_dense', 'read_parameter_dense_ck')}",
                f"entity_id = {one}", f"entity_id IN ({inlist})")),
        ("variant: surrogate array_id, macro (join through key)", var,
         shapes(f"FROM {macro.replace('read_parameter_dense', 'read_parameter_dense_sk')}",
                f"entity_id = {one}", f"entity_id IN ({inlist})")),
        ("variant: surrogate array_id, lookup + self-join", var,
         shapes(f"FROM (SELECT array_id FROM va_sk WHERE {trip}) k JOIN va_sk v USING (array_id)",
                "TRUE", "TRUE")[:4]
         + [("one week x 20 series",
             f"SELECT v.\"values\"[{lo1}:{hi1}] FROM (SELECT array_id FROM va_sk WHERE entity_id IN ({inlist}) "
             f"AND parameter_definition_id = {pd} AND alternative_id = {alt}) k JOIN va_sk v USING (array_id)",
             READ_SERIES * WIN_PTS)]),
    ]
    for title, path, qs in groups:
        print(f"\n{title}")
        out[title] = {}
        for label, sql, expect in qs:
            r = time_query(path, sql, expect, cold, warm)
            r["scan"] = scan_ops(path, sql)
            out[title][label] = r
            report(label, r, r["scan"])
    return out


# --- long reads -----------------------------------------------------------------

def long_reads(db: str, workdir: str, cold: int, warm: int) -> dict:
    pd, alt, picks, _ = ids(db, CLASS_LONG, PARAM_LONG)
    one, inlist = picks[0], ",".join(map(str, picks))
    lo, hi = WIN_LO // SPARSE_STRIDE, (WIN_LO + WIN_PTS - 1) // SPARSE_STRIDE
    wpts = hi - lo + 1
    n_series_pts = CELLS // SPARSE_STRIDE
    base = f"FROM value_long WHERE parameter_definition_id = {pd} AND alternative_id = {alt} AND valid_to_commit IS NULL"
    # Variant: same rows with a single-column series key and an index on it.
    var = os.path.join(workdir, f"long_variant_{duckdb.__version__}.db")
    if os.path.exists(var):
        os.remove(var)
    con = duckdb.connect(var)
    con.execute(f"ATTACH '{db}' AS s (READ_ONLY)")
    con.execute(f"CREATE TABLE vl AS SELECT dense_rank() OVER (ORDER BY {TRIPLE}) AS series_id, "
                f"entity_id, parameter_definition_id, alternative_id, i1_pos, v_int "
                f"FROM s.value_long WHERE valid_to_commit IS NULL ORDER BY {TRIPLE}, i1_pos")
    con.execute("CREATE INDEX ix_vl_series ON vl (series_id)")
    sid = con.execute(f"SELECT series_id FROM vl WHERE entity_id = {one} LIMIT 1").fetchone()[0]
    con.execute("CHECKPOINT")
    con.close()
    groups = [
        ("long baseline: real value_long (composite unique index)", db, [
            ("one cell", f"SELECT v_int {base} AND entity_id = {one} AND i1_pos = {lo}", 1),
            (f"one series window ({wpts})", f"SELECT v_int {base} AND entity_id = {one} AND i1_pos BETWEEN {lo} AND {hi}", wpts),
            (f"one whole series ({n_series_pts})", f"SELECT v_int {base} AND entity_id = {one}", n_series_pts),
            ("20 series window", f"SELECT v_int {base} AND entity_id IN ({inlist}) AND i1_pos BETWEEN {lo} AND {hi}",
             READ_SERIES * wpts),
        ]),
        ("long variant: single-column series_id + index", var, [
            ("one cell", f"SELECT v_int FROM vl WHERE series_id = {sid} AND i1_pos = {lo}", 1),
            (f"one series window ({wpts})", f"SELECT v_int FROM vl WHERE series_id = {sid} AND i1_pos BETWEEN {lo} AND {hi}", wpts),
            (f"one whole series ({n_series_pts})", f"SELECT v_int FROM vl WHERE series_id = {sid}", n_series_pts),
        ]),
    ]
    out: dict = {}
    for title, path, qs in groups:
        print(f"\n{title}")
        out[title] = {}
        for label, sql, expect in qs:
            r = time_query(path, sql, expect, cold, warm)
            r["scan"] = scan_ops(path, sql)
            out[title][label] = r
            report(label, r, r["scan"])
    os.remove(var)
    return out


# --- commit after replace ---------------------------------------------------------

def native_copy(src: str, workdir: str) -> str:
    """Re-write `src` with COPY FROM DATABASE into a fresh file in the running
    engine's own storage format (a file written by an older engine keeps its
    older format when merely opened)."""
    dst = os.path.join(workdir, f"native_{duckdb.__version__}.db")
    for f in (dst, dst + ".wal"):
        if os.path.exists(f):
            os.remove(f)
    con = duckdb.connect()
    con.execute(f"ATTACH '{src}' AS s (READ_ONLY)")
    con.execute(f"ATTACH '{dst}' AS d")
    try:
        con.execute("COPY FROM DATABASE s TO d")
        con.execute("CHECKPOINT d")
        con.close()
        return dst
    except duckdb.Error as e:
        # DuckDB 1.5.x's COPY FROM DATABASE can trip over the schema's
        # foreign-key load order; on such an engine the source file is
        # already in its own format, so the plain commit probe covers it.
        con.close()
        for f in (dst, dst + ".wal"):
            if os.path.exists(f):
                os.remove(f)
        print(f"\nnative copy not possible on duckdb {duckdb.__version__} ({str(e).splitlines()[0][:120]}); "
              "skipping the native-format commit probe")
        return ""


def commit_probe(src: str, workdir: str, reps: int, tag: str = "") -> dict:
    """Mimic an array replace on a scratch copy of `src` and time each
    statement and COMMIT. Two replace shapes: the core's (pre-image copy to
    value_array_hist + in-place UPDATE of the list row) and DELETE + INSERT
    of the row; each under the default checkpoint threshold and with
    automatic checkpoints pushed out of reach. Records the WAL size after
    every commit (a WAL that drops to 0 means a checkpoint ran)."""
    pd, alt, picks, _ = ids(src, CLASS_DENSE, PARAM_DENSE)
    out: dict = {}
    modes = ["update in place (core shape)", "delete + insert"]
    settings = [("default threshold", None),
                ("checkpoint_threshold 100GB", "SET checkpoint_threshold = '100GB'")]
    for mode in modes:
        for slabel, setting in settings:
            label = f"{tag}{mode}, {slabel}"
            dst = os.path.join(workdir, f"commit_probe_{duckdb.__version__}.db")
            shutil.copyfile(src, dst)
            con = duckdb.connect(dst)
            if setting:
                con.execute(setting)
            thr = con.execute("SELECT current_setting('checkpoint_threshold')").fetchone()[0]
            times: dict = {}
            wal = []
            for i in range(reps):
                e = picks[i % len(picks)]
                trip = f"entity_id = {e} AND parameter_definition_id = {pd} AND alternative_id = {alt}"
                newvals = [float((e * 7919 + h + i) % 1000) for h in range(CELLS)]

                def step(name, fn):
                    t = time.perf_counter()
                    r = fn()
                    times.setdefault(name, []).append((time.perf_counter() - t) * 1e3)
                    return r

                step("begin", lambda: con.execute("BEGIN"))
                cid = step("commit row", lambda: con.execute(
                    "INSERT INTO commit (user, message) VALUES ('bench', 'replace') RETURNING id").fetchone()[0])
                step("pre-image copy", lambda: con.execute(
                    f"INSERT INTO value_array_hist SELECT *, {cid}, TRUE FROM value_array WHERE {trip}"))
                if mode.startswith("update"):
                    step("write new value", lambda: con.execute(
                        f"UPDATE value_array SET valid_from_commit = {cid}, version = version + 1, "
                        f"\"values\" = ?::DOUBLE[], v_min = ?, v_max = ? WHERE {trip}",
                        [newvals, min(newvals), max(newvals)]))
                else:
                    def del_ins():
                        con.execute(f"DELETE FROM value_array WHERE {trip}")
                        con.execute(
                            f"INSERT INTO value_array SELECT entity_id, parameter_definition_id, alternative_id, "
                            f"parameter_type_id, {cid}, version + 1, axis1_id, axis2_id, axis3_id, axis4_id, "
                            f"?::DOUBLE[], NULL, ?, ? FROM value_array_hist WHERE {trip} AND valid_to_commit = {cid}",
                            [newvals, min(newvals), max(newvals)])
                    step("write new value", del_ins)
                step("COMMIT", lambda: con.execute("COMMIT"))
                w = dst + ".wal"
                wal.append(os.path.getsize(w) if os.path.exists(w) else 0)
            t = time.perf_counter()
            con.close()
            close_ms = (time.perf_counter() - t) * 1e3
            for f in (dst, dst + ".wal"):
                if os.path.exists(f):
                    os.remove(f)
            res = {"checkpoint_threshold": thr,
                   "median_ms": {k: statistics.median(v) for k, v in times.items()},
                   "max_ms": {k: max(v) for k, v in times.items()},
                   "commit_ms_each": times["COMMIT"], "wal_bytes_after_commit": wal,
                   "close_ms": close_ms}
            out[label] = res
            print(f"\ncommit probe [{label}] (checkpoint_threshold = {thr}), {reps} replaces:")
            for k in times:
                print(f"  {k:18} median {res['median_ms'][k]:8.2f} ms   max {res['max_ms'][k]:8.2f} ms")
            print(f"  COMMIT each (ms): {', '.join(f'{x:.0f}' for x in times['COMMIT'])}")
            print(f"  WAL MiB after each commit: {', '.join(f'{x / 2**20:.1f}' for x in wal)}")
            print(f"  close (checkpoints the WAL): {close_ms:.0f} ms")
    return out


# --- main --------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Surrogate-key value_array variant vs the real schema")
    ap.add_argument("--db", required=True, help="dense nodal-db file from the results-path bench")
    ap.add_argument("--long-db", help="long-shape nodal-db file from the results-path bench")
    ap.add_argument("--commit-probe", action="store_true", help="also time COMMIT after array replaces")
    ap.add_argument("--commit-probe-native", action="store_true",
                    help="also run the commit probe on a copy re-written in this engine's storage format")
    ap.add_argument("--skip-reads", action="store_true", help="skip the dense/long read comparison")
    ap.add_argument("--workdir", help="scratch directory (default: a temp dir)")
    ap.add_argument("--cold", type=int, default=5)
    ap.add_argument("--warm", type=int, default=15)
    ap.add_argument("--reps", type=int, default=10, help="replaces in the commit probe")
    ap.add_argument("--json", help="write results as JSON")
    a = ap.parse_args()
    workdir = a.workdir or tempfile.mkdtemp(prefix="nodal-skvariant-")
    os.makedirs(workdir, exist_ok=True)
    res: dict = {"duckdb": duckdb.__version__, "db": a.db, "db_bytes": os.path.getsize(a.db)}
    print(f"surrogate-key variant — duckdb {duckdb.__version__}; source {a.db} "
          f"({os.path.getsize(a.db) / 2**20:.1f} MiB)")
    if not a.skip_reads:
        t = time.perf_counter()
        v = build_variants(a.db, workdir)
        print(f"variant file built in {time.perf_counter() - t:.1f} s "
              f"(composite copy {v['copy_ck_s']:.1f} s, surrogate copy {v['copy_sk_s']:.1f} s)")
        res["dense"] = dense_reads(a.db, v["path"], a.cold, a.warm)
        os.remove(v["path"])
        if a.long_db:
            res["long"] = long_reads(a.long_db, workdir, a.cold, a.warm)
    if a.commit_probe:
        res["commit"] = commit_probe(a.db, workdir, a.reps, "source file: ")
    if a.commit_probe_native:
        native = native_copy(a.db, workdir)
        if native:
            res["commit_native"] = commit_probe(native, workdir, a.reps,
                                                f"native {duckdb.__version__} copy: ")
            os.remove(native)
    if a.json:
        with open(a.json, "w") as f:
            json.dump(res, f, indent=2)
    if not a.workdir:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
