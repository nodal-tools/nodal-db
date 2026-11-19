#!/usr/bin/env python3
"""Purge feasibility spike: whole-database close-outs under the DuckDB cap.

A purge closes out every row in a scope in ONE transaction (one commit).
This spike measures whether the set-based statements a purge runs fit the
machine budget every core connection gets in this repo: DuckDB with 4
threads and a memory_limit of 80 % of 8 000 MB (`.cargo/config.toml`),
caching operators off (`conn::core_config`).

Scenarios (each runs in a fresh child process on a fresh copy of its
fixture; the parent records the child's peak RSS with `wait4`):

  scalar_retained / scalar_stub  Values{} on 10 M scalar values
  long_retained / long_stub      Values{} on ~100 M long cells
  array_retained / array_stub    Values{} on 1 000 x 8 760 arrays
  entities                       Entities{} on 100 k entities (+ 20 k N-D),
                                 every dependent leg, iterative B1 frontier
  values_class_survivors         Values{class} on 5 M scalars with a 50 %
                                 survivor anti-join (2.5 M survivor keys)
  explain                        EXPLAIN of set-form legs (temp-table and
                                 unnest bind forms): SEMI/hash join, no
                                 nested loop

Statements are the shapes the purge plan specifies (§3.6): the scope sets
and the survivor keys are txn-local TEMP tables; every close carries
`valid_from_commit < c AND valid_to_commit IS NULL`; values are split by
retention through a `purge_retention(pd_id, retained)` table filled before
any close; arrays resolve `array_id`s on narrow columns first, then copy the
pre-image and delete by `array_id`.

Usage (project venv, DuckDB 2.0.0):
    .venv/bin/python scripts/bench/purge_spike.py all --dir /tmp/purge_spike
    .venv/bin/python scripts/bench/purge_spike.py run SCENARIO --dir DIR

Fixtures are generated once into DIR (a few GB) and reused.
"""
import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time

import duckdb

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "schema"))
import build as schema_build  # noqa: E402

CAP = {
    "threads": 4,
    "memory_limit": "6400MB",
    "debug_enable_caching_operators": False,
}

N_SCALAR_ENT = 50_000      # per class, 2 classes x 100 params -> 10 M
N_SCALAR_PAR = 100
N_LONG_ENT = 1_000         # 1 000 entities x 12 params x 8 760 -> 105 M cells
N_LONG_PAR = 12
STEPS = 8_760
N_ARRAY = 1_000            # 1 000 entities x 1 param x 8 760
N_ENT = 100_000            # 0-D entities, 2 classes
N_ND = 20_000              # 2-D entities over pairs of 0-D ones


def connect(path, cap=True):
    return duckdb.connect(str(path), config=CAP if cap else {"threads": 8,
                          "memory_limit": "12GB",
                          "debug_enable_caching_operators": False})


def base(path):
    if path.exists():
        path.unlink()
    schema_build.build(str(path), force=True)


def setup_common(con, classes):
    """One fixture commit (2), one alternative, the given classes."""
    con.execute("INSERT INTO commit (user, message) VALUES ('spike', 'fixture')")
    con.execute("INSERT INTO alternative (name, valid_from_commit) VALUES ('base', 2)")
    for c in classes:
        con.execute("INSERT INTO entity_class (name, valid_from_commit) VALUES (?, 2)", [c])


def gen_scalar(path):
    base(path)
    con = connect(path, cap=False)
    setup_common(con, ["a", "b"])
    for ci, c in enumerate(["a", "b"]):
        cid = con.execute("SELECT id FROM entity_class WHERE name = ?", [c]).fetchone()[0]
        con.execute(
            "INSERT INTO entity (entity_class_id, name, valid_from_commit) "
            "SELECT ?, 'e' || i, 2 FROM range(?) t(i)", [cid, N_SCALAR_ENT])
        con.execute(
            "INSERT INTO parameter_definition (entity_class_id, name, retain_content, "
            "valid_from_commit) SELECT ?, 'p' || i, TRUE, 2 FROM range(?) t(i)",
            [cid, N_SCALAR_PAR])
    con.execute(
        "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id, "
        "valid_from_commit, version, value_kind, v_float) "
        "SELECT e.id, p.id, 1, 2, 1, 'float', (e.id * 31 + p.id) % 1000 / 7.0 "
        "FROM entity e JOIN parameter_definition p ON p.entity_class_id = e.entity_class_id")
    con.execute("CHECKPOINT")
    con.close()


def gen_long(path):
    base(path)
    con = connect(path, cap=False)
    setup_common(con, ["a"])
    con.execute(
        "INSERT INTO axis (kind, length, content_hash, p_start_ts, p_step_months, "
        "p_step_micros) VALUES ('datetime', ?, 1, TIMESTAMP '2030-01-01', 0, 3600000000)",
        [STEPS])
    con.execute(
        "INSERT INTO entity (entity_class_id, name, valid_from_commit) "
        "SELECT 1, 'e' || i, 2 FROM range(?) t(i)", [N_LONG_ENT])
    con.execute(
        "INSERT INTO parameter_definition (entity_class_id, name, retain_content, "
        "valid_from_commit) SELECT 1, 'p' || i, TRUE, 2 FROM range(?) t(i)", [N_LONG_PAR])
    pids = [r[0] for r in con.execute("SELECT id FROM parameter_definition ORDER BY id").fetchall()]
    for pid in pids:  # ~8.8 M rows per INSERT
        con.execute(
            "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id, "
            "valid_from_commit, version, axis1_id, i1_pos, i1_dt, value_kind, v_float) "
            "SELECT e.id, ?, 1, 2, 1, 1, s.i, TIMESTAMP '2030-01-01' + to_hours(s.i), "
            "'float', ((e.id * 131 + s.i) % 997) / 3.0 "
            "FROM entity e, range(?) s(i)", [pid, STEPS])
        con.execute("CHECKPOINT")
    con.close()


def gen_array(path):
    base(path)
    con = connect(path, cap=False)
    setup_common(con, ["a"])
    con.execute(
        "INSERT INTO axis (kind, length, content_hash, p_start_ts, p_step_months, "
        "p_step_micros) VALUES ('datetime', ?, 1, TIMESTAMP '2030-01-01', 0, 3600000000)",
        [STEPS])
    con.execute(
        "INSERT INTO entity (entity_class_id, name, valid_from_commit) "
        "SELECT 1, 'e' || i, 2 FROM range(?) t(i)", [N_ARRAY])
    con.execute(
        "INSERT INTO parameter_definition (entity_class_id, name, retain_content, "
        "valid_from_commit) VALUES (1, 'p', TRUE, 2)")
    con.execute(
        "INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id, "
        "valid_from_commit, version, axis1_id, values, v_min, v_max) "
        "SELECT e.id, 1, 1, 2, 1, 1, "
        "  list_transform(range(?), lambda x: ((e.id * 7 + x) % 1000) / 9.0), 0, 111 "
        "FROM entity e", [STEPS])
    con.execute("CHECKPOINT")
    con.close()


def gen_entities(path):
    base(path)
    con = connect(path, cap=False)
    setup_common(con, ["node", "unit", "link"])
    con.execute(
        "INSERT INTO entity_class_dimension (entity_class_id, position, dimension_class_id, "
        "valid_from_commit) VALUES (3, 1, 1, 2), (3, 2, 2, 2)")
    half = N_ENT // 2
    con.execute(
        "INSERT INTO entity (entity_class_id, name, valid_from_commit) "
        "SELECT 1 + (i >= ?)::INT, 'e' || i, 2 FROM range(?) t(i)", [half, N_ENT])
    # 2-D links: (node i, unit i) for i < N_ND
    con.execute(
        "INSERT INTO entity (entity_class_id, name, valid_from_commit) "
        "SELECT 3, 'l' || i, 2 FROM range(?) t(i)", [N_ND])
    con.execute(
        "INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit) "
        "SELECT l.id, 1, n.id, 2 FROM entity l JOIN entity n "
        "  ON n.name = 'e' || substr(l.name, 2) WHERE l.entity_class_id = 3")
    con.execute(
        "INSERT INTO entity_element (entity_id, position, element_entity_id, valid_from_commit) "
        "SELECT l.id, 2, u.id, 2 FROM entity l JOIN entity u "
        "  ON u.name = 'e' || (substr(l.name, 2)::INT + ?) WHERE l.entity_class_id = 3", [half])
    for c in (1, 2, 3):
        con.execute(
            "INSERT INTO parameter_definition (entity_class_id, name, retain_content, "
            "valid_from_commit) SELECT ?, 'p' || i, TRUE, 2 FROM range(5) t(i)", [c])
    con.execute(
        "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id, "
        "valid_from_commit, version, value_kind, v_float) "
        "SELECT e.id, p.id, 1, 2, 1, 'float', e.id % 100 "
        "FROM entity e JOIN parameter_definition p ON p.entity_class_id = e.entity_class_id")
    con.execute(
        "INSERT INTO entity_alternative (entity_id, alternative_id, active, valid_from_commit) "
        "SELECT id, 1, TRUE, 2 FROM entity")
    con.execute(
        "INSERT INTO entity_location (entity_id, lat, lon, valid_from_commit) "
        "SELECT id, 60, 25, 2 FROM entity WHERE entity_class_id = 1")
    # memberships: unit i (group) has member node i, base rows
    con.execute(
        "INSERT INTO membership (group_entity_id, member_entity_id, alternative_id, "
        "valid_from_commit) SELECT u.id, n.id, NULL, 2 FROM entity u JOIN entity n "
        "ON n.name = 'e' || (substr(u.name, 2)::INT - ?) WHERE u.entity_class_id = 2", [half])
    con.execute("CHECKPOINT")
    con.close()


FIXTURES = {
    "scalar": gen_scalar,
    "long": gen_long,
    "array": gen_array,
    "entities": gen_entities,
}

SCENARIO_FIXTURE = {
    "scalar_retained": "scalar",
    "scalar_stub": "scalar",
    "long_retained": "long",
    "long_stub": "long",
    "array_retained": "array",
    "array_stub": "array",
    "entities": "entities",
    "values_class_survivors": "scalar",
    "explain": "entities",
}

STUB_SET = ("content_retained = FALSE, v_float = NULL, v_int = NULL, v_bool = NULL, "
            "v_str = NULL, v_datetime = NULL, v_dur_months = NULL, v_dur_micros = NULL")


def retention(con, retained):
    con.execute(
        "CREATE TEMP TABLE purge_retention AS SELECT id AS pd_id, ? AS retained "
        "FROM parameter_definition WHERE valid_to_commit IS NULL", [retained])


def close_values_tier(con, c, table, scope, retained, survivors=False):
    set_list = "valid_to_commit = ?" + ("" if retained else ", " + STUB_SET)
    anti = ""
    if survivors:
        anti = (" AND NOT EXISTS (SELECT 1 FROM purge_survivor_value s WHERE "
                f"s.e = {table}.entity_id AND s.p = {table}.parameter_definition_id "
                f"AND s.a = {table}.alternative_id)")
    sql = (f"UPDATE {table} SET {set_list} WHERE {scope} "
           f"AND parameter_definition_id IN (SELECT pd_id FROM purge_retention "
           f"WHERE retained = {str(retained).upper()}) "
           f"AND valid_from_commit < ? AND valid_to_commit IS NULL{anti}")
    return con.execute(sql, [c, c]).fetchone()[0]


def close_arrays(con, c, scope, retained):
    con.execute(
        "CREATE TEMP TABLE purge_array_ids AS SELECT array_id FROM value_array "
        f"WHERE {scope} AND valid_from_commit < ? AND parameter_definition_id IN "
        f"(SELECT pd_id FROM purge_retention WHERE retained = {str(retained).upper()})", [c])
    if retained:
        copy = ("INSERT INTO value_array_hist SELECT *, ?, TRUE FROM value_array "
                "WHERE array_id IN (SELECT array_id FROM purge_array_ids)")
    else:
        copy = ("INSERT INTO value_array_hist (array_id, entity_id, parameter_definition_id, "
                "alternative_id, parameter_type_id, valid_from_commit, version, axis1_id, "
                "axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max, content_hash, "
                "valid_to_commit, content_retained) SELECT array_id, entity_id, "
                "parameter_definition_id, alternative_id, parameter_type_id, valid_from_commit, "
                "version, axis1_id, axis2_id, axis3_id, axis4_id, NULL, NULL, v_min, v_max, "
                "NULL, ?, FALSE FROM value_array "
                "WHERE array_id IN (SELECT array_id FROM purge_array_ids)")
    copied = con.execute(copy, [c]).fetchone()[0]
    deleted = con.execute("DELETE FROM value_array WHERE array_id IN "
                          "(SELECT array_id FROM purge_array_ids)").fetchone()[0]
    assert copied == deleted, (copied, deleted)
    return deleted


def timed(log, label, f):
    t = time.perf_counter()
    out = f()
    log.append({"step": label, "s": round(time.perf_counter() - t, 3), "rows": out})
    return out


ENTITY_LEGS = [
    ("entity_location", "entity_id"),
    ("entity_alternative", "entity_id"),
    ("membership", "group_entity_id"),
    ("membership", "member_entity_id"),
    ("entity_parameter_unit", "entity_id"),
    ("entity_metadata", "entity_id"),
    ("parameter_value_metadata", "entity_id"),
]


def scenario(name, work):
    c = None
    log = []
    con = connect(work)
    t0 = time.perf_counter()
    con.execute("BEGIN")
    c = con.execute("INSERT INTO commit (user, message) VALUES ('spike', ?) RETURNING id",
                    [name]).fetchone()[0]
    if name in ("scalar_retained", "scalar_stub"):
        r = name == "scalar_retained"
        timed(log, "retention", lambda: retention(con, r))
        timed(log, "close value_scalar", lambda: close_values_tier(con, c, "value_scalar", "TRUE", r))
    elif name in ("long_retained", "long_stub"):
        r = name == "long_retained"
        timed(log, "retention", lambda: retention(con, r))
        timed(log, "close value_long", lambda: close_values_tier(con, c, "value_long", "TRUE", r))
    elif name in ("array_retained", "array_stub"):
        r = name == "array_retained"
        timed(log, "retention", lambda: retention(con, r))
        timed(log, "close value_array", lambda: close_arrays(con, c, "TRUE", r))
    elif name == "values_class_survivors":
        timed(log, "retention", lambda: retention(con, True))
        timed(log, "survivor keys (2.5 M)", lambda: con.execute(
            "CREATE TEMP TABLE purge_survivor_value AS SELECT entity_id AS e, "
            "parameter_definition_id AS p, alternative_id AS a FROM value_scalar s "
            "JOIN entity e ON e.id = s.entity_id WHERE e.entity_class_id = 1 "
            "AND s.entity_id % 2 = 0 AND s.valid_to_commit IS NULL").fetchone())
        scope = ("parameter_definition_id IN (SELECT id FROM parameter_definition "
                 "WHERE entity_class_id = 1 AND valid_to_commit IS NULL)")
        timed(log, "close value_scalar (class a, anti-join)",
              lambda: close_values_tier(con, c, "value_scalar", scope, True, survivors=True))
    elif name == "entities":
        timed(log, "retention", lambda: retention(con, True))
        timed(log, "dependent set", lambda: con.execute(
            "CREATE TEMP TABLE purge_dep_entity AS SELECT id FROM entity "
            "WHERE valid_to_commit IS NULL AND valid_from_commit < ?", [c]).fetchone())

        def frontier():
            rounds = 0
            while True:
                rounds += 1
                n = con.execute(
                    "INSERT INTO purge_dep_entity SELECT DISTINCT ee.entity_id FROM entity_element ee "
                    "JOIN entity e ON e.id = ee.entity_id AND e.valid_to_commit IS NULL "
                    "WHERE ee.valid_to_commit IS NULL AND ee.element_entity_id IN "
                    "(SELECT id FROM purge_dep_entity) AND ee.entity_id NOT IN "
                    "(SELECT id FROM purge_dep_entity)").fetchone()[0]
                if n == 0:
                    return rounds
        timed(log, "B1 frontier (rounds)", frontier)
        timed(log, "close set", lambda: con.execute(
            "CREATE TEMP TABLE purge_close_entity AS SELECT id FROM purge_dep_entity").fetchone())
        scope = "entity_id IN (SELECT id FROM purge_dep_entity)"
        for t in ("value_scalar", "value_long"):
            timed(log, f"close {t}", lambda t=t: close_values_tier(con, c, t, scope, True))
        timed(log, "close value_array", lambda: close_arrays(con, c, scope, True))
        for t, col in ENTITY_LEGS:
            timed(log, f"close {t}.{col}", lambda t=t, col=col: con.execute(
                f"UPDATE {t} SET valid_to_commit = ? WHERE {col} IN "
                "(SELECT id FROM purge_dep_entity) AND valid_from_commit < ? "
                "AND valid_to_commit IS NULL", [c, c]).fetchone()[0])
        timed(log, "close semantic_annotation", lambda: con.execute(
            "UPDATE semantic_annotation SET valid_to_commit = ? WHERE object_id IN "
            "(SELECT id FROM purge_dep_entity) AND object_type = 'entity' "
            "AND valid_from_commit < ? AND valid_to_commit IS NULL", [c, c]).fetchone()[0])
        timed(log, "close entity_element (identity)", lambda: con.execute(
            "UPDATE entity_element SET valid_to_commit = ? WHERE entity_id IN "
            "(SELECT id FROM purge_close_entity) AND valid_from_commit < ? "
            "AND valid_to_commit IS NULL", [c, c]).fetchone()[0])
        timed(log, "close entity (object rows)", lambda: con.execute(
            "UPDATE entity SET valid_to_commit = ? WHERE id IN "
            "(SELECT id FROM purge_close_entity) AND valid_from_commit < ? "
            "AND valid_to_commit IS NULL", [c, c]).fetchone()[0])
    else:
        raise SystemExit(f"unknown scenario {name}")
    t1 = time.perf_counter()
    con.execute("COMMIT")
    t2 = time.perf_counter()
    con.execute("CHECKPOINT")
    t3 = time.perf_counter()
    con.close()
    return {
        "scenario": name,
        "steps": log,
        "statements_s": round(t1 - t0, 3),
        "commit_s": round(t2 - t1, 3),
        "checkpoint_s": round(t3 - t2, 3),
        "total_s": round(t3 - t0, 3),
    }


def explain(work):
    con = connect(work)
    con.execute("CREATE TEMP TABLE purge_dep_entity AS SELECT id FROM entity "
                "WHERE valid_to_commit IS NULL")
    out = {}
    ids = [r[0] for r in con.execute("SELECT id FROM entity LIMIT 50000").fetchall()]
    forms = {
        "temp_table": ("UPDATE entity_alternative SET valid_to_commit = 99 WHERE entity_id IN "
                       "(SELECT id FROM purge_dep_entity) AND valid_from_commit < 99 "
                       "AND valid_to_commit IS NULL", []),
        "unnest_bind": ("UPDATE entity_alternative SET valid_to_commit = 99 WHERE entity_id IN "
                        "(SELECT unnest(?::BIGINT[])) AND valid_from_commit < 99 "
                        "AND valid_to_commit IS NULL", [ids]),
        "scalar_anti_join": ("UPDATE value_scalar SET valid_to_commit = 99 WHERE entity_id IN "
                             "(SELECT id FROM purge_dep_entity) AND valid_from_commit < 99 "
                             "AND valid_to_commit IS NULL AND NOT EXISTS (SELECT 1 FROM "
                             "purge_dep_entity s WHERE s.id = value_scalar.entity_id)", []),
    }
    for k, (sql, args) in forms.items():
        plan = "\n".join(r[1] for r in con.execute("EXPLAIN " + sql, args).fetchall())
        # DuckDB 2.0 labels operators "Hash Join", older releases "HASH_JOIN".
        out[k] = {
            "hash_join": "HASH_JOIN" in plan or "Hash Join" in plan,
            "semi_or_anti": ("SEMI" in plan) or ("ANTI" in plan) or ("MARK" in plan),
            "nested_loop": any(x in plan for x in (
                "NESTED_LOOP", "BLOCKWISE_NL", "PIECEWISE_MERGE", "CROSS_PRODUCT",
                "Nested Loop", "Blockwise NL", "Piecewise Merge", "Cross Product")),
            "plan": plan,
        }
    con.close()
    return out


def run_child(name, d):
    fixture = d / f"{SCENARIO_FIXTURE[name]}.duckdb"
    work = d / f"work_{name}.duckdb"
    for p in (work, work.with_name(work.name + ".wal")):
        if p.exists():
            p.unlink()
    shutil.copy(fixture, work)
    size_before = work.stat().st_size
    res = explain(work) if name == "explain" else scenario(name, work)
    if name != "explain":
        res["file_mb_before"] = round(size_before / 2**20, 1)
        res["file_mb_after"] = round(work.stat().st_size / 2**20, 1)
    work.unlink()
    print(json.dumps(res))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["gen", "run", "all"])
    ap.add_argument("scenario", nargs="?")
    ap.add_argument("--dir", default="/tmp/purge_spike")
    ap.add_argument("--out", default=None, help="JSON results file (all)")
    a = ap.parse_args()
    d = pathlib.Path(a.dir)
    d.mkdir(parents=True, exist_ok=True)
    if a.cmd == "gen":
        for k, g in FIXTURES.items():
            p = d / f"{k}.duckdb"
            if not p.exists():
                t = time.perf_counter()
                g(p)
                print(f"generated {k} in {time.perf_counter() - t:.0f} s", file=sys.stderr)
        return
    if a.cmd == "run":
        run_child(a.scenario, d)
        return
    results = []
    for name in (a.scenario.split(",") if a.scenario else SCENARIO_FIXTURE):
        p = subprocess.Popen([sys.executable, __file__, "run", name, "--dir", str(d)],
                             stdout=subprocess.PIPE, text=True)
        out = p.stdout.read()
        _, status, ru = os.wait4(p.pid, 0)
        try:
            res = json.loads(out.strip().splitlines()[-1])
        except Exception:
            res = {"scenario": name, "error": out[-2000:]}
        res["exit_status"] = status
        res["peak_rss_mb"] = round(ru.ru_maxrss / 1024, 1)
        results.append(res)
        print(json.dumps({k: v for k, v in res.items() if k != "plan"}), flush=True)
    if a.out:
        pathlib.Path(a.out).write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
