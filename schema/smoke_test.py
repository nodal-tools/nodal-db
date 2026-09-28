#!/usr/bin/env python3
"""Smoke test for the nodal_db DDL sketch.

Builds the schema into a scratch file, populates a small demo model
(including the two worked examples from value_model_plan.md Part 2), and
exercises: the tier function, axis dedup, scenario resolution, two-pass
membership/activation, scenario-contents/diff, history on both mechanisms
(from/to close-out + array `_hist` pre-image), as-of-commit reads, and the
audit views. A hostile-data regression section then replays every finding
of specs/ddl_critique.md (F1-F10 + minors): corruption is planted raw, the
right constraint or audit is asserted to fire, cleanup follows, and the
suite ends by re-asserting every audit clean.
Prints PASS/FAIL per check; exit code 1 on any FAIL.

Usage: python smoke_test.py [workdir]  (default: a temp directory)

The writes below follow the normative write protocol of value_model_plan.md
Part 5 by hand — they are what the client library will own.
"""
import datetime
import pathlib
import re
import sys
import tempfile

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import build as schema_build

H = 3_600_000_000  # one hour in microseconds

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail and not ok else ""))


def nextval(con, seq):
    return con.execute(f"SELECT nextval('{seq}')").fetchone()[0]


def new_commit(con, message):
    return con.execute(
        "INSERT INTO commit (user, message) VALUES ('smoke_test', ?) RETURNING id",
        [message]).fetchone()[0]


# --- axis creation with content-verified dedup (VMP Part 4: hash is an
# accelerator, content is the truth; the canonical byte-level encoding spec
# is a separate deliverable — this uses a demo-grade canonical string).
def _axis_lookup(con, kind, key):
    h = con.execute("SELECT hash(?)", [key]).fetchone()[0]
    ids = [r[0] for r in con.execute(
        "SELECT id FROM axis WHERE content_hash = ? AND kind = ?", [h, kind]).fetchall()]
    return h, ids


def axis_datetime_parametric(con, start, step_micros, n, step_months=0):
    key = f"datetime|parametric|{start}|{step_months}|{step_micros}|{n}||"
    h, ids = _axis_lookup(con, "datetime", key)
    for aid in ids:  # content verification
        row = con.execute(
            "SELECT p_start_ts, p_step_months, p_step_micros, length FROM axis WHERE id = ?",
            [aid]).fetchone()
        if row == (start, step_months, step_micros, n):
            return aid
    aid = nextval(con, "seq_axis")
    con.execute(
        "INSERT INTO axis (id, kind, length, content_hash, p_start_ts, p_step_months, p_step_micros)"
        " VALUES (?, 'datetime', ?, ?, ?, ?, ?)",
        [aid, n, h, start, step_months, step_micros])
    return aid


def axis_integer_parametric(con, start, step, n):
    key = f"integer|parametric|{start}|{step}|{n}||"
    h, ids = _axis_lookup(con, "integer", key)
    for aid in ids:
        row = con.execute(
            "SELECT p_start_int, p_step_int, length FROM axis WHERE id = ?", [aid]).fetchone()
        if row == (start, step, n):
            return aid
    aid = nextval(con, "seq_axis")
    con.execute(
        "INSERT INTO axis (id, kind, length, content_hash, p_start_int, p_step_int)"
        " VALUES (?, 'integer', ?, ?, ?, ?)", [aid, n, h, start, step])
    return aid


def axis_label(con, labels):
    """labels: ordered list of names; ordinal = 1-based caller order."""
    key = "label|" + "|".join(labels) + "||"
    h, ids = _axis_lookup(con, "label", key)
    for aid in ids:
        got = [r[0] for r in con.execute(
            "SELECT label FROM axis_label WHERE axis_id = ? ORDER BY pos", [aid]).fetchall()]
        if got == list(labels):
            return aid
    aid = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash) VALUES (?, 'label', ?, ?)",
                [aid, len(labels), h])
    for pos, lab in enumerate(labels):
        con.execute(
            "INSERT INTO axis_label (id, axis_id, pos, label, ordinal) VALUES (?, ?, ?, ?, ?)",
            [nextval(con, "seq_axis_label"), aid, pos, lab, pos + 1])
    return aid


def axis_period(con, periods):
    """periods: list of (start_ts, end_ts, years_presented)."""
    key = "period|" + "|".join(f"{s}~{e}~{y}" for s, e, y in periods) + "||"
    h, ids = _axis_lookup(con, "period", key)
    for aid in ids:
        got = con.execute(
            "SELECT start_ts, end_ts, years_presented FROM axis_period WHERE axis_id = ? ORDER BY pos",
            [aid]).fetchall()
        if got == [tuple(p) for p in periods]:
            return aid
    aid = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash) VALUES (?, 'period', ?, ?)",
                [aid, len(periods), h])
    for pos, (s, e, y) in enumerate(periods):
        con.execute(
            "INSERT INTO axis_period (axis_id, pos, start_ts, end_ts, years_presented)"
            " VALUES (?, ?, ?, ?, ?)", [aid, pos, s, e, y])
    return aid


def axis_float_parametric(con, start, step, n):   # regular real-valued grid
    key = f"float|parametric|{start}|{step}|{n}||"
    h, ids = _axis_lookup(con, "float", key)
    for aid in ids:                      # content verification (dedup)
        row = con.execute(
            "SELECT p_start_dbl, p_step_dbl, length FROM axis WHERE id = ?", [aid]).fetchone()
        if row == (start, step, n):
            return aid
    aid = nextval(con, "seq_axis")
    con.execute(
        "INSERT INTO axis (id, kind, length, content_hash, p_start_dbl, p_step_dbl)"
        " VALUES (?, 'float', ?, ?, ?, ?)", [aid, n, h, start, step])
    return aid


def axis_float(con, coords):             # irregular, materialized
    key = "float|" + "|".join(str(c) for c in coords) + "||"
    h, ids = _axis_lookup(con, "float", key)
    for aid in ids:
        got = [r[0] for r in con.execute(
            "SELECT coord FROM axis_float WHERE axis_id = ? ORDER BY pos", [aid]).fetchall()]
        if got == list(coords):
            return aid
    aid = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash) VALUES (?, 'float', ?, ?)",
                [aid, len(coords), h])
    for pos, c in enumerate(coords):
        con.execute("INSERT INTO axis_float (axis_id, pos, coord) VALUES (?, ?, ?)", [aid, pos, c])
    return aid


# --- write protocol (VMP Part 5): close-out + insert on from/to tiers;
# SQL-side pre-image copy on the array tier. Caller opens the transaction.
def write_scalar(con, c, e, p, a, kind, col, value):
    cur = con.execute(
        "SELECT version FROM value_scalar WHERE entity_id=? AND parameter_definition_id=?"
        " AND alternative_id=? AND valid_to_commit IS NULL", [e, p, a]).fetchone()
    ver = 1
    if cur:
        ver = cur[0] + 1
        con.execute(
            "UPDATE value_scalar SET valid_to_commit=? WHERE entity_id=? AND"
            " parameter_definition_id=? AND alternative_id=? AND valid_to_commit IS NULL",
            [c, e, p, a])
    con.execute(
        f"INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,"
        f" valid_from_commit, version, value_kind, {col}) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [e, p, a, c, ver, kind, value])


def write_array(con, c, e, p, a, axes, values, int_kind=False):
    axes = (axes + [None] * 4)[:4]
    # route payload: exactly one of values/values_int populated (XOR CHECK);
    # v_min/v_max are DOUBLE for both kinds (int cast is exact at demo scale)
    fvals = None if int_kind else values
    ivals = values if int_kind else None
    vmin, vmax = float(min(values)), float(max(values))
    cur = con.execute(
        "SELECT version FROM value_array WHERE entity_id=? AND parameter_definition_id=?"
        " AND alternative_id=?", [e, p, a]).fetchone()
    if cur:
        # SQL-side pre-image copy — never client-buffered (VMP Part 5)
        con.execute(
            "INSERT INTO value_array_hist SELECT *, ?, TRUE FROM value_array"
            " WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?",
            [c, e, p, a])
        # set BOTH payload columns so the inactive one is NULLed on a kind switch
        # (critique F4 — else the live XOR CHECK rejects the UPDATE)
        con.execute(
            "UPDATE value_array SET valid_from_commit=?, version=?, axis1_id=?, axis2_id=?,"
            " axis3_id=?, axis4_id=?, values=?, values_int=?, v_min=?, v_max=?"
            " WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?",
            [c, cur[0] + 1, *axes, fvals, ivals, vmin, vmax, e, p, a])
    else:
        con.execute(
            "INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id,"
            " valid_from_commit, version, axis1_id, axis2_id, axis3_id, axis4_id, values,"
            " values_int, v_min, v_max) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)",
            [e, p, a, c, *axes, fvals, ivals, vmin, vmax])


def write_long_cell(con, c, e, p, a, axes, poss, kind, col, value, accels=None):
    axes = (axes + [None] * 4)[:4]
    poss = (poss + [0] * 4)[:4]
    acc = {f"i{i}_{k}": None for i in (1, 2, 3, 4) for k in ("dt", "num")}
    acc.update(accels or {})
    con.execute(
        f"INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id,"
        f" valid_from_commit, version, axis1_id, axis2_id, axis3_id, axis4_id,"
        f" i1_pos, i2_pos, i3_pos, i4_pos, i1_dt, i1_num, i2_dt, i2_num, i3_dt, i3_num,"
        f" i4_dt, i4_num, value_kind, {col})"
        " VALUES (?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [e, p, a, c, *axes, *poss,
         acc["i1_dt"], acc["i1_num"], acc["i2_dt"], acc["i2_num"],
         acc["i3_dt"], acc["i3_num"], acc["i4_dt"], acc["i4_num"], kind, value])


def main():
    workdir = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(tempfile.mkdtemp())
    db = workdir / "smoke.duckdb"
    schema_build.build(str(db), force=True)
    con = duckdb.connect(str(db))
    ts = datetime.datetime

    # ---- DB-identity UUID (schema_interop.md §9): minted once at init by
    # 900_bootstrap.sql. Captured BEFORE any writes; asserted well-formed here,
    # distinct per init, and (at suite end) unchanged across the whole flow.
    ident_sql = ("SELECT value FROM metadata WHERE lower(name) = 'db_identity'"
                 " AND valid_to_commit IS NULL")
    dbid = con.execute(ident_sql).fetchone()[0]
    check("db_identity: minted at init with RFC-4122 shape",
          bool(re.fullmatch(
              r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
              dbid or "")), str(dbid))
    twin_path = workdir / "smoke_identity_twin.duckdb"
    schema_build.build(str(twin_path), force=True)
    twin = duckdb.connect(str(twin_path))
    twin_id = twin.execute(ident_sql).fetchone()[0]
    twin.close()
    check("db_identity: a second init mints a different identity",
          twin_id is not None and twin_id != dbid, f"{dbid} vs {twin_id}")

    # ---- demo model, one commit (VMP Part 5: one transaction incl. commit row)
    con.execute("BEGIN")
    c2 = new_commit(con, "demo model")

    def insert(table, cols, vals):
        q = ", ".join("?" * len(vals))
        con.execute(f"INSERT INTO {table} ({cols}) VALUES ({q})", vals)
        return vals[0]

    def new_class(name, **kw):
        cid = nextval(con, "seq_entity_class")
        insert("entity_class",
               "id, name, required, valid_from_commit",
               [cid, name, kw.get("required", False), c2])
        return cid

    node = new_class("node")
    unit = new_class("unit")
    commodity = new_class("commodity")
    node_group = new_class("node_group")
    link = new_class("link")

    # class-set {node, unit}, and a 2-D class with a class-set-typed dimension
    nou = nextval(con, "seq_class_set")
    insert("class_set", "id, name, valid_from_commit", [nou, "node_or_unit", c2])
    insert("class_set_member", "class_set_id, member_class_id, valid_from_commit", [nou, node, c2])
    insert("class_set_member", "class_set_id, member_class_id, valid_from_commit", [nou, unit, c2])
    insert("entity_class_dimension",
           "entity_class_id, position, dimension_class_id, valid_from_commit", [link, 1, node, c2])
    con.execute("INSERT INTO entity_class_dimension (entity_class_id, position,"
                " dimension_class_set_id, valid_from_commit) VALUES (?, ?, ?, ?)",
                [link, 2, nou, c2])

    def new_entity(cls, name):
        eid = nextval(con, "seq_entity")
        insert("entity", "id, entity_class_id, name, valid_from_commit", [eid, cls, name, c2])
        return eid

    node_a = new_entity(node, "node_a")
    node_b = new_entity(node, "node_b")
    node_c = new_entity(node, "node_c")
    unit_1 = new_entity(unit, "unit_1")
    coal = new_entity(commodity, "coal")
    gas = new_entity(commodity, "gas")
    west = new_entity(node_group, "west")
    link_1 = new_entity(link, "link_1")
    insert("entity_element", "entity_id, position, element_entity_id, valid_from_commit",
           [link_1, 1, node_a, c2])
    insert("entity_element", "entity_id, position, element_entity_id, valid_from_commit",
           [link_1, 2, unit_1, c2])  # set-typed dimension holds a unit

    # units registry + one per-entity override (Decision 9)
    mw = nextval(con, "seq_units_of_measure")
    insert("units_of_measure", "id, name, valid_from_commit", [mw, "MW", c2])
    tonne = nextval(con, "seq_units_of_measure")
    insert("units_of_measure", "id, name, valid_from_commit", [tonne, "tonne", c2])

    def new_pd(cls, name, unit_id=None, required=False):
        pid = nextval(con, "seq_parameter_definition")
        insert("parameter_definition",
               "id, entity_class_id, name, unit_id, required, valid_from_commit",
               [pid, cls, name, unit_id, required, c2])
        return pid

    demand = new_pd(node, "demand", unit_id=mw, required=True)
    inflow = new_pd(node, "inflow_forecast")
    ladder = new_pd(commodity, "price_ladder")
    insert("entity_parameter_unit",
           "entity_id, parameter_definition_id, unit_id, valid_from_commit",
           [node_b, demand, tonne, c2])

    def new_type(pd, kind, rank, dims=(), expected=None):
        tid = nextval(con, "seq_parameter_type")
        insert("parameter_type",
               "id, parameter_definition_id, value_kind, rank, expected_tier, valid_from_commit",
               [tid, pd, kind, rank, expected, c2])
        for pos, (dname, dkind) in enumerate(dims, start=1):
            insert("parameter_type_dimension",
                   "parameter_type_id, position, dimension_name, kind, valid_from_commit",
                   [tid, pos, dname, dkind, c2])
        return tid

    new_type(demand, "float", 0)
    new_type(inflow, "indexed", 3,
             [("forecast", "label"), ("forecast_time", "datetime"),
              ("realization_time", "datetime")], expected="array")
    new_type(ladder, "indexed", 3,
             [("field", "label"), ("period", "period"), ("tier", "integer")], expected="array")

    # D3: a fresh DB no longer bootstraps a `Base` alternative — the smoke
    # seeds it itself (mirroring the `high` insert below), same as any client.
    base = nextval(con, "seq_alternative")
    insert("alternative", "id, name, valid_from_commit", [base, "Base", c2])
    high = nextval(con, "seq_alternative")
    insert("alternative", "id, name, valid_from_commit", [high, "high", c2])
    s0 = nextval(con, "seq_scenario")
    insert("scenario", "id, name, valid_from_commit", [s0, "s0", c2])
    s1 = nextval(con, "seq_scenario")
    insert("scenario", "id, name, valid_from_commit", [s1, "s1", c2])
    for sc, alt, rank in [(s0, base, 1), (s1, base, 1), (s1, high, 2)]:
        insert("scenario_alternative", "scenario_id, alternative_id, rank, valid_from_commit",
               [sc, alt, rank, c2])

    # bag with alternative-scoped membership (Decision 3) + deactivation
    for member in (node_a, node_b):
        insert("membership", "group_entity_id, member_entity_id, included, valid_from_commit",
               [west, member, True, c2])
    con.execute("INSERT INTO membership (group_entity_id, member_entity_id, alternative_id,"
                " included, valid_from_commit) VALUES (?, ?, ?, FALSE, ?)",
                [west, node_b, high, c2])
    insert("entity_alternative", "entity_id, alternative_id, active, valid_from_commit",
           [west, high, False, c2])

    # scalars
    write_scalar(con, c2, node_a, demand, base, "float", "v_float", 10.0)
    write_scalar(con, c2, node_a, demand, high, "float", "v_float", 20.0)
    write_scalar(con, c2, node_b, demand, base, "float", "v_float", 5.0)

    # worked example 1 (VMP Part 2): named-forecast stochastics, rank 3,
    # two entities sharing one grid (axis dedup)
    ax_f = axis_label(con, ["f_central", "f_wet", "f_dry"])
    ax_ft = axis_datetime_parametric(con, ts(2030, 1, 1), 6 * H, 4)
    ax_rt = axis_datetime_parametric(con, ts(2030, 1, 1), H, 48)
    inflow_vals = [f * 10000 + t * 100 + r
                   for f in range(3) for t in range(4) for r in range(48)]
    write_array(con, c2, node_a, inflow, base, [ax_f, ax_ft, ax_rt],
                [float(v) for v in inflow_vals])
    ax_ft2 = axis_datetime_parametric(con, ts(2030, 1, 1), 6 * H, 4)  # dedup hit
    write_array(con, c2, node_b, inflow, base, [ax_f, ax_ft2, ax_rt],
                [float(v + 1) for v in inflow_vals])

    # worked example 2 (VMP Part 2): commodity price ladder, rank 3.
    # coal is a full rectangle -> Tier A; gas is ragged -> Tier L (the tier
    # function is content-determined, VMP Part 3).
    ax_field = axis_label(con, ["price", "max_volume"])
    ax_p = axis_period(con, [(ts(2030, 1, 1), ts(2035, 1, 1), 5.0),
                             (ts(2035, 1, 1), ts(2040, 1, 1), 5.0)])
    ax_tier = axis_integer_parametric(con, 1, 1, 5)
    coal_vals = [float((f + 1) * 100 + (p + 1) * 10 + (t + 1))
                 for f in range(2) for p in range(2) for t in range(5)]
    write_array(con, c2, coal, ladder, base, [ax_field, ax_p, ax_tier], coal_vals)
    p_starts = [ts(2030, 1, 1), ts(2035, 1, 1)]
    for f in range(2):
        for p, ntiers in [(0, 5), (1, 3)]:  # p2035 has only 3 tiers -> ragged
            for t in range(ntiers):
                write_long_cell(con, c2, gas, ladder, base,
                                [ax_field, ax_p, ax_tier], [f, p, t], "float", "v_float",
                                float((f + 1) * 1000 + (p + 1) * 10 + (t + 1)),
                                accels={"i2_dt": p_starts[p], "i3_num": 1 + t})

    # authored display mode (Decision 12): greys one class under 'creator'
    creator = nextval(con, "seq_display_mode")
    insert("display_mode", "id, name, valid_from_commit", [creator, "creator", c2])
    insert("entity_class_display_mode",
           "display_mode_id, entity_class_id, display_status, display_order,"
           " font_color, background_color, valid_from_commit",
           [creator, unit, "greyed_out", 7, "FF0000", "00FF00", c2])

    # --- fitness round 2 additions -------------------------------------------
    # semantic seams (T1.2): a QUDT quantity-kind on MW, a codelist member IRI
    con.execute("UPDATE units_of_measure SET quantity_kind_link = ? WHERE id = ?",
                ["http://qudt.org/vocab/quantitykind/Power", mw])
    carrier = nextval(con, "seq_parameter_value_list")
    insert("parameter_value_list", "id, name, valid_from_commit", [carrier, "carrier", c2])
    elec = nextval(con, "seq_list_value")
    con.execute("INSERT INTO list_value (id, parameter_value_list_id, ordinal, value_kind,"
                " v_str, semantic_link, valid_from_commit) VALUES (?, ?, 1, 'string', ?, ?, ?)",
                [elec, carrier, "electricity",
                 "http://openenergy-platform.org/ontology/oeo/OEO_00000139", c2])

    # provenance (Decision 13): a tool entity produced an output alternative
    tool_cls = new_class("tool")
    ce_tool = new_entity(tool_cls, "capacity_expansion")
    ce_out = nextval(con, "seq_alternative")
    insert("alternative", "id, name, valid_from_commit", [ce_out, "ce_out", c2])
    run_id = nextval(con, "seq_run")
    insert("run", "id, tool_entity_id, result_alternative_id, input_commit_id,"
           " produced_at_commit_id, solver, status",
           [run_id, ce_tool, ce_out, c2, c2, "highs", "optimal"])

    # realizes refinement (Decision 13 option c): a coarse plant realized by a
    # fine component; deactivating the plant must propagate to the component.
    plant_cls = new_class("power_plant")
    comp_cls = new_class("component")
    plant_1 = new_entity(plant_cls, "plant_1")
    gen_1 = new_entity(comp_cls, "gen_1")
    realizes_cls = new_class("realizes_plant")  # reserved-name marker prefix
    con.execute("INSERT INTO entity_class_dimension (entity_class_id, position,"
                " dimension_class_id, valid_from_commit) VALUES (?, 1, ?, ?), (?, 2, ?, ?)",
                [realizes_cls, plant_cls, c2, realizes_cls, comp_cls, c2])
    r_link = new_entity(realizes_cls, "plant_1__gen_1")
    insert("entity_element", "entity_id, position, element_entity_id, valid_from_commit",
           [r_link, 1, plant_1, c2])
    insert("entity_element", "entity_id, position, element_entity_id, valid_from_commit",
           [r_link, 2, gen_1, c2])
    insert("entity_alternative", "entity_id, alternative_id, active, valid_from_commit",
           [plant_1, high, False, c2])  # plant off in `high` (=> s1)

    # semantic_annotation (T1.4): crosswalk objects to external ontology terms
    for otype, oid, rel, iri, vocab in [
        ("entity_class", node, "closeMatch", "cim:ConnectivityNode", "CIM16"),
        ("unit", mw, "qudt:hasQuantityKind", "http://qudt.org/vocab/quantitykind/Power", "QUDT"),
        ("list_value", elec, "exactMatch",
         "http://openenergy-platform.org/ontology/oeo/OEO_00000139", "OEO")]:
        sa = nextval(con, "seq_semantic_annotation")
        con.execute("INSERT INTO semantic_annotation (id, object_type, object_id, relation,"
                    " term_iri, vocabulary, valid_from_commit) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [sa, otype, oid, rel, iri, vocab, c2])
    con.execute("COMMIT")

    # ---- checks -----------------------------------------------------------
    one = lambda q, *a: con.execute(q, list(a)).fetchone()
    allrows = lambda q, *a: con.execute(q, list(a)).fetchall()

    # one-current unique index rejects a second current row
    try:
        con.execute("BEGIN")
        con.execute(
            "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,"
            " valid_from_commit, version, value_kind, v_float) VALUES (?, ?, ?, ?, 9, 'float', 1.0)",
            [node_a, demand, base, c2])
        con.execute("ROLLBACK")
        check("one-current index rejects duplicate current row", False)
    except duckdb.ConstraintException:
        con.execute("ROLLBACK")
        check("one-current index rejects duplicate current row", True)

    check("axis dedup: shared grid resolves to one axis object", ax_ft == ax_ft2,
          f"{ax_ft} vs {ax_ft2}")
    n_axes = one("SELECT count(*) FROM axis")[0]
    check("no duplicate axes minted (6 distinct axes)", n_axes == 6, f"n={n_axes}")

    tiers = dict(allrows("SELECT entity_id, tier FROM value_keys WHERE parameter_definition_id = ?",
                         ladder))
    check("tier function: rectangular ladder (coal) -> array tier", tiers.get(coal) == "array")
    check("tier function: ragged ladder (gas) -> long tier", tiers.get(gas) == "long")

    rows = allrows("SELECT entity, tier, v_float FROM read_parameter('commodity', 'price_ladder')")
    check("canonical read unifies tiers (straddling fix): 20 + 16 rows",
          len(rows) == 36 and {r[1] for r in rows} == {"array", "long"}, f"n={len(rows)}")
    cell = one("SELECT v_float FROM read_parameter('commodity', 'price_ladder')"
               " WHERE entity = 'coal' AND idx1_text = 'price'"
               " AND idx2_ts = TIMESTAMP '2030-01-01' AND idx3_num = 3")
    check("array cell decode (row-major, axis-rendered): coal/price/p2030/tier3 = 113",
          cell is not None and cell[0] == 113.0, str(cell))
    cell = one("SELECT v_float FROM read_parameter('node', 'inflow_forecast')"
               " WHERE entity = 'node_a' AND idx1_text = 'f_wet'"
               " AND idx2_ts = TIMESTAMP '2030-01-01 12:00' AND idx3_ts = TIMESTAMP '2030-01-01 05:00'")
    check("rank-3 stochastics cell: node_a/f_wet/12:00/05:00 = 10205",
          cell is not None and cell[0] == 10205.0, str(cell))
    n = one("SELECT count(*) FROM read_parameter('node', 'inflow_forecast')")[0]
    check("rank-3 stochastics: 2 entities x 576 cells", n == 1152, f"n={n}")

    r = {(e, s): v for e, s, v in allrows(
        "SELECT entity, alternative, v_float FROM resolved_value_rows('s1')"
        " WHERE parameter = 'demand'")}
    check("scenario resolution s1: high (rank 2) wins for node_a",
          r.get(("node_a", "high")) == 20.0 and ("node_a", "Base") not in r, str(r))
    check("scenario resolution s1: Base survives where high is silent (node_b)",
          r.get(("node_b", "Base")) == 5.0, str(r))
    r0 = one("SELECT v_float FROM resolved_value_rows('s0')"
             " WHERE parameter = 'demand' AND entity = 'node_a'")
    check("scenario resolution s0: Base only", r0[0] == 10.0, str(r0))
    conf = allrows("SELECT entity_id, n_candidates FROM scenario_value_conflict WHERE scenario_id = ?", s1)
    check("pre-collapse conflict view exposes node_a demand under 2 alternatives",
          len(conf) == 2 and all(c[1] == 2 and c[0] == node_a for c in conf), str(conf))

    m1 = {m for (m,) in allrows(
        "SELECT member_entity_id FROM membership_resolved WHERE scenario_id = ? AND included", s1)}
    m0 = {m for (m,) in allrows(
        "SELECT member_entity_id FROM membership_resolved WHERE scenario_id = ? AND included", s0)}
    check("membership pass 1: alternative-scoped exclusion (s1 = {node_a})", m1 == {node_a}, str(m1))
    check("membership pass 1: base membership (s0 = {node_a, node_b})",
          m0 == {node_a, node_b}, str(m0))

    act = {e: (a, src) for e, a, src in allrows(
        "SELECT entity_id, active, active_source FROM entity_activation_resolved"
        " WHERE scenario_id = ?", s1)}
    check("membership pass 2: own row deactivates west in s1", act[west] == (False, "own"))
    check("membership pass 2: deactivation propagates to node_a",
          act[node_a] == (False, "propagated"))
    check("membership pass 2: excluded node_b stays on class default",
          act[node_b] == (True, "default"))
    act0 = {e: a for e, a in allrows(
        "SELECT entity_id, active FROM entity_activation_resolved WHERE scenario_id = ?", s0)}
    check("membership pass 2: everything active in s0", all(act0.values()))

    sc = allrows("SELECT entity, active, in_group FROM scenario_contents('s1')"
                 " WHERE entity = 'node_a'")
    check("scenario_contents: node_a shown in west, inactive",
          sc == [("node_a", False, "west")], str(sc))
    diff = {(k, g, e): (ia, ib, aa, ab) for k, g, e, ia, ib, aa, ab in allrows(
        "SELECT * FROM scenario_diff('s1', 's0')")}
    check("scenario_diff: membership delta (west, node_b)",
          diff.get(("membership", "west", "node_b")) == (False, True, None, None), str(diff))
    check("scenario_diff: activation deltas (west, node_a)",
          diff.get(("activation", None, "west")) == (None, None, False, True)
          and diff.get(("activation", None, "node_a")) == (None, None, False, True), str(diff))

    # ---- update commit: history on both mechanisms ------------------------
    con.execute("BEGIN")
    c3 = new_commit(con, "revise demand and coal ladder")
    write_scalar(con, c3, node_a, demand, base, "float", "v_float", 12.0)
    write_array(con, c3, coal, ladder, base, [ax_field, ax_p, ax_tier],
                [v * 1.1 for v in coal_vals])
    con.execute("COMMIT")

    dead = one("SELECT version, valid_to_commit, v_float FROM value_scalar"
               " WHERE entity_id = ? AND alternative_id = ? AND parameter_definition_id = ?"
               " AND valid_to_commit IS NOT NULL", node_a, base, demand)
    cur = one("SELECT version, v_float FROM value_scalar WHERE entity_id = ? AND"
              " alternative_id = ? AND parameter_definition_id = ? AND valid_to_commit IS NULL",
              node_a, base, demand)
    check("from/to close-out: superseded scalar row stamped (v1, closed at c3)",
          dead == (1, c3, 10.0) and cur == (2, 12.0), f"{dead} / {cur}")
    hist = one("SELECT version, valid_to_commit, values[1] FROM value_array_hist"
               " WHERE entity_id = ?", coal)
    cur = one("SELECT version, values[1] FROM value_array WHERE entity_id = ?", coal)
    check("array _hist pre-image: v1 payload kept, current v2 replaced",
          hist == (1, c3, 111.0) and cur[0] == 2 and abs(cur[1] - 122.1) < 1e-9,
          f"{hist} / {cur}")

    asof = one("SELECT v_float FROM value_scalar_asof(?) WHERE entity_id = ?"
               " AND parameter_definition_id = ? AND alternative_id = ?", c2, node_a, demand, base)
    check("as-of-commit read: scalar at c2 = 10.0", asof == (10.0,), str(asof))
    asof = one("SELECT values[1], version FROM value_array_asof(?) WHERE entity_id = ?", c2, coal)
    check("as-of-commit read: coal array at c2 is the pre-image", asof == (111.0, 1), str(asof))
    now = one("SELECT v_float FROM resolved_value_rows('s0') WHERE entity = 'node_a'"
              " AND parameter = 'demand'")
    check("resolution reads current state after update", now == (12.0,), str(now))

    tl = allrows("SELECT tier, version, valid_to_commit FROM value_timeline"
                 " WHERE entity_id = ? ORDER BY version", coal)
    check("value_timeline: one logical timeline across current + hist",
          tl == [("array", 1, c3), ("array", 2, None)], str(tl))

    # ---- audits -----------------------------------------------------------
    for view in ["audit_dangling_ref", "audit_history_tiling", "audit_array_integrity",
                 "audit_type_conformance", "audit_index_accelerator", "audit_axis_integrity",
                 "audit_array_payload_xor", "audit_cross_tier_duplicate", "audit_membership_cycle",
                 "audit_class_set_entity_name_dup", "audit_element_class_mismatch",
                 "audit_value_class_mismatch", "audit_value_list_membership",
                 "audit_scenario_empty", "audit_required_class_empty",
                 "audit_realizes_cycle", "audit_dangling_semantic_ref"]:
        n = one(f"SELECT count(*) FROM {view}")[0]
        check(f"{view} clean", n == 0, f"n={n}")

    ti = allrows("SELECT entity_id, actual_tier, expected_tier FROM audit_tier_intent")
    check("audit_tier_intent surfaces gas raggedness (long vs expected array)",
          ti == [(gas, "long", "array")], str(ti))
    missing = {(s, e) for s, e in allrows(
        "SELECT scenario, entity FROM audit_missing_required_value")}
    check("completeness: node_c missing required demand in both scenarios",
          missing == {("s0", "node_c"), ("s1", "node_c")}, str(missing))
    eff = dict(allrows(
        "SELECT e.name, u.name FROM parameter_effective_unit p"
        " JOIN entity_current e ON e.id = p.entity_id"
        " JOIN units_of_measure_current u ON u.id = p.unit_id"
        " WHERE p.parameter_definition_id = ?", demand))
    check("effective unit: override (node_b -> tonne) else default (MW)",
          eff == {"node_a": "MW", "node_b": "tonne", "node_c": "MW"}, str(eff))
    ver = one("SELECT version FROM dataset_version")[0]
    check("dataset_version cookie = last commit id", ver == c3, f"{ver} vs {c3}")

    # ---- display mode (Decision 12): round-trip, uniqueness, FK intent
    disp = {ec: (st, order, fc, bc) for ec, st, order, fc, bc in allrows(
        "SELECT entity_class_id, display_status, display_order, font_color, background_color"
        " FROM entity_class_display('creator')")}
    check("display mode: greyed class returns status/order/color under creator",
          disp.get(unit) == ("greyed_out", 7, "FF0000", "00FF00"), str(disp.get(unit)))
    check("display mode: class with no ecdm row defaults to visible + own display_order",
          disp.get(node) == ("visible", None, None, None), str(disp.get(node)))
    try:
        con.execute("BEGIN")
        con.execute(
            "INSERT INTO entity_class_display_mode (display_mode_id, entity_class_id,"
            " display_status, valid_from_commit, version) VALUES (?, ?, 'hidden', ?, 9)",
            [creator, unit, c2])
        con.execute("ROLLBACK")
        check("one_current_ecdm rejects duplicate current (mode, class) row", False)
    except duckdb.ConstraintException:
        con.execute("ROLLBACK")
        check("one_current_ecdm rejects duplicate current (mode, class) row", True)
    con.execute("BEGIN")
    con.execute("INSERT INTO entity_class_display_mode (display_mode_id, entity_class_id,"
                " display_status, valid_from_commit) VALUES (?, 99999, 'visible', ?)", [creator, c3])
    con.execute("COMMIT")
    n = one("SELECT count(*) FROM audit_dangling_ref WHERE src_table = 'entity_class_display_mode'"
            " AND ref_table = 'entity_class' AND ref_id = 99999")[0]
    check("display mode: ecdm pointing at missing entity_class fires audit_dangling_ref",
          n == 1, f"n={n}")
    con.execute("DELETE FROM entity_class_display_mode WHERE entity_class_id = 99999")

    # ---- fitness round 2 additions: round-trip + FK-intent -----------------
    # semantic seams (T1.2)
    qk = one("SELECT quantity_kind_link FROM units_of_measure WHERE id = ?", mw)[0]
    check("semantic seam: MW unit carries a QUDT quantity-kind IRI",
          qk == "http://qudt.org/vocab/quantitykind/Power", str(qk))
    sl = one("SELECT semantic_link FROM list_value WHERE id = ?", elec)[0]
    check("semantic seam: list_value member carries a concept IRI",
          sl is not None and "OEO" in sl, str(sl))

    # provenance run (Decision 13)
    rr = one("SELECT solver, status, input_commit_id FROM run WHERE id = ?", run_id)
    check("run: provenance row round-trips (solver/status/input commit)",
          rr == ("highs", "optimal", c2), str(rr))
    bad_run = nextval(con, "seq_run")
    con.execute("INSERT INTO run (id, tool_entity_id, result_alternative_id, input_commit_id)"
                " VALUES (?, 99999, ?, ?)", [bad_run, ce_out, c2])
    n = one("SELECT count(*) FROM audit_dangling_ref WHERE src_table = 'run'"
            " AND ref_table = 'entity' AND ref_id = 99999")[0]
    check("run: tool_entity_id at a missing entity fires audit_dangling_ref", n == 1, f"n={n}")
    con.execute("DELETE FROM run WHERE id = ?", [bad_run])

    # realizes propagation (Decision 13 option c): plant off in `high` => s1
    ra1 = {e: (a, src) for e, a, src in allrows(
        "SELECT entity_id, active, active_source FROM entity_activation_resolved"
        " WHERE scenario_id = ?", s1)}
    check("realizes: coarse plant deactivated by its own row in s1",
          ra1.get(plant_1) == (False, "own"), str(ra1.get(plant_1)))
    check("realizes: deactivation propagates coarse->fine (gen_1 inactive in s1)",
          ra1.get(gen_1) == (False, "propagated"), str(ra1.get(gen_1)))
    ra0 = {e: a for e, a in allrows(
        "SELECT entity_id, active FROM entity_activation_resolved WHERE scenario_id = ?", s0)}
    check("realizes: fine stays active where coarse is active (gen_1 on in s0)",
          ra0.get(gen_1) is True, str(ra0.get(gen_1)))

    # semantic_annotation (T1.4)
    n = one("SELECT count(*) FROM semantic_annotation_current WHERE object_type = 'unit'"
            " AND object_id = ? AND relation = 'qudt:hasQuantityKind'", mw)[0]
    check("semantic_annotation: unit->QUDT crosswalk round-trips", n == 1, f"n={n}")
    sa_bad = nextval(con, "seq_semantic_annotation")
    con.execute("INSERT INTO semantic_annotation (id, object_type, object_id, term_iri,"
                " valid_from_commit) VALUES (?, 'entity_class', 99999, 'x:bad', ?)", [sa_bad, c2])
    n = one("SELECT count(*) FROM audit_dangling_semantic_ref WHERE object_type = 'entity_class'"
            " AND object_id = 99999")[0]
    check("semantic_annotation: annotation at a missing object fires audit_dangling_semantic_ref",
          n == 1, f"n={n}")
    con.execute("DELETE FROM semantic_annotation WHERE id = ?", [sa_bad])

    # =======================================================================
    # Hostile-data regression checks: every DDL-critique finding (F1..F10 +
    # minors) gets a check that would have caught it. Corruption is planted
    # raw (bypassing the write protocol on purpose), asserted to fire the
    # right audit/constraint, then cleaned up; the suite ends by re-asserting
    # every audit clean.
    # =======================================================================

    def expect_constraint(name, sql, params=None):
        try:
            con.execute("BEGIN")
            con.execute(sql, params or [])
            con.execute("ROLLBACK")
            check(name, False, "accepted")
        except duckdb.ConstraintException:
            con.execute("ROLLBACK")
            check(name, True)

    # run temporal coherence: produced_at (c2) before input (c3) is rejected
    expect_constraint(
        "run: produced_at before input commit is rejected (temporal coherence)",
        "INSERT INTO run (id, tool_entity_id, result_alternative_id, input_commit_id,"
        " produced_at_commit_id) VALUES (900001, ?, ?, ?, ?)", [ce_tool, ce_out, c3, c2])

    # ---- F7: alternative id 0 collides with the D3 base-membership sentinel
    expect_constraint(
        "F7: alternative id=0 rejected (D3 sentinel guard)",
        "INSERT INTO alternative (id, name, valid_from_commit) VALUES (0, 'zero', ?)", [c3])

    # ---- F2: reversed validity spans rejected at entry, on value and narrow tables
    expect_constraint(
        "F2: reversed span rejected on value_scalar (to <= from)",
        "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, valid_to_commit, version, value_kind, v_float)"
        " VALUES (?, ?, ?, ?, ?, 1, 'float', 1.0)", [node_c, demand, base, c3, c2])
    expect_constraint(
        "F2: reversed span rejected on entity (narrow table)",
        "INSERT INTO entity (id, entity_class_id, name, valid_from_commit, valid_to_commit)"
        " VALUES (nextval('seq_entity'), ?, 'revspan', ?, ?)", [node, c3, c2])
    expect_constraint(
        "F2: zero-length span rejected on value_array_hist",
        "INSERT INTO value_array_hist (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, version, axis1_id, values, valid_to_commit)"
        " VALUES (?, ?, ?, ?, 1, ?, [1.0], ?)", [node_c, demand, base, c2, ax_f, c2])

    # ---- F2: overlapping dead spans with clean version tiling -> audit fires
    con.execute("BEGIN")
    con.execute(
        "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, valid_to_commit, version, value_kind, v_float)"
        " VALUES (?, ?, ?, ?, ?, 1, 'float', 1.0)", [node_c, demand, base, 1, c3])
    con.execute(
        "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, valid_to_commit, version, value_kind, v_float)"
        " VALUES (?, ?, ?, ?, ?, 2, 'float', 2.0)", [node_c, demand, base, c2, c3])
    con.execute("COMMIT")
    probs = {p for (p,) in allrows(
        "SELECT problem FROM audit_history_tiling WHERE entity_id = ?", node_c)}
    check("F2: overlapping spans (clean version tiling) flagged by audit_history_tiling",
          "validity spans overlap within key" in probs, str(probs))
    n = one("SELECT count(*) FROM value_scalar_asof(?) WHERE entity_id = ?", c2, node_c)[0]
    check("F2: overlap still poisons as-of (2 rows for one key) until repaired", n == 2, f"n={n}")
    con.execute("DELETE FROM value_scalar WHERE entity_id = ?", [node_c])

    # ---- F1: half-deleted alternative no longer swallows values in resolution
    con.execute("BEGIN")
    ch = new_commit(con, "hostile: close out ONLY the alternative row for high")
    con.execute("UPDATE alternative SET valid_to_commit = ? WHERE id = ? AND valid_to_commit IS NULL",
                [ch, high])
    con.execute("COMMIT")
    r = allrows("SELECT alternative, v_float FROM resolved_value_rows('s1')"
                " WHERE entity = 'node_a' AND parameter = 'demand'")
    check("F1: phantom winner self-heals — Base value resurfaces in s1",
          r == [("Base", 12.0)], str(r))
    n = one("SELECT count(*) FROM scenario_value_resolved WHERE alternative_id = ?", high)[0]
    check("F1: closed-out alternative drops out of candidacy", n == 0, f"n={n}")
    n = one("SELECT count(*) FROM audit_dangling_ref WHERE ref_table = 'alternative'"
            " AND ref_id = ?", high)[0]
    check("F1: leftovers of the half-delete surface in audit_dangling_ref", n > 0, f"n={n}")
    act = one("SELECT active, active_source FROM entity_activation_resolved act"
              " JOIN scenario s ON s.id = act.scenario_id AND s.name = 's1'"
              " WHERE act.entity_id = ?", node_a)
    check("F1: activation resolution also ignores the closed alternative (node_a active again)",
          act == (True, "default"), str(act))
    con.execute("BEGIN")
    con.execute("INSERT INTO alternative (id, name, valid_from_commit, version)"
                " VALUES (?, 'high', ?, 2)", [high, ch])
    con.execute("COMMIT")

    # ---- F8: scenario_diff against a misspelled scenario is empty, not a mass removal
    n = one("SELECT count(*) FROM scenario_diff('s0', 'nope')")[0]
    check("F8: scenario_diff vs nonexistent name -> empty (matches scenario_contents)",
          n == 0, f"n={n}")
    n = one("SELECT count(*) FROM scenario_diff('s1', 's0')")[0]
    check("F8: scenario_diff between real scenarios still reports", n > 0, f"n={n}")

    # ---- F5: forked reserved metadata key fires the reserved-dup audit
    con.execute("BEGIN")
    con.execute("INSERT INTO metadata (id, name, value, valid_from_commit)"
                " VALUES (nextval('seq_metadata'), 'schema_version', 'CONFLICTING', ?)", [c3])
    con.execute("COMMIT")
    dup = allrows("SELECT name, n FROM audit_metadata_reserved_dup")
    check("F5: second current schema_version row fires audit_metadata_reserved_dup",
          dup == [("schema_version", 2)], str(dup))
    con.execute("DELETE FROM metadata WHERE value = 'CONFLICTING'")
    # audit_rules is a reserved single-valued key too (template_ir_plan D4 —
    # the one-string additive allowlist edit named in its Appendix)
    con.execute("BEGIN")
    for v in ("{}", '{"conditional_requirements": []}'):
        con.execute("INSERT INTO metadata (id, name, value, valid_from_commit)"
                    " VALUES (nextval('seq_metadata'), 'audit_rules', ?, ?)", [v, c3])
    con.execute("COMMIT")
    dup = allrows("SELECT name, n FROM audit_metadata_reserved_dup")
    check("D4: forked audit_rules key fires audit_metadata_reserved_dup",
          dup == [("audit_rules", 2)], str(dup))
    con.execute("DELETE FROM metadata WHERE lower(name) = 'audit_rules'")
    check("F10: bootstrap schema_version matches build.py constant",
          one("SELECT value FROM metadata_current WHERE lower(name) = 'schema_version'")[0]
          == schema_build.SCHEMA_VERSION)

    # ---- F6: element arity — missing / undeclared / zero-element entities
    con.execute("BEGIN")
    l_bad = nextval(con, "seq_entity")
    insert("entity", "id, entity_class_id, name, valid_from_commit", [l_bad, link, "l_bad", c3])
    insert("entity_element", "entity_id, position, element_entity_id, valid_from_commit",
           [l_bad, 1, node_a, c3])
    insert("entity_element", "entity_id, position, element_entity_id, valid_from_commit",
           [l_bad, 7, node_b, c3])   # undeclared position; declared pos 2 missing
    l_empty = nextval(con, "seq_entity")
    insert("entity", "id, entity_class_id, name, valid_from_commit", [l_empty, link, "l_empty", c3])
    con.execute("COMMIT")
    arity = {(e, p, prob) for e, p, prob in allrows(
        "SELECT entity_id, position, problem FROM audit_element_arity")}
    check("F6: audit_element_arity reports missing pos 2 and undeclared pos 7",
          (l_bad, 2, "missing element at declared position") in arity
          and (l_bad, 7, "element at undeclared position") in arity, str(arity))
    check("F6: zero-element entity of a 2-D class reported",
          {(l_empty, 1, "missing element at declared position"),
           (l_empty, 2, "missing element at declared position")} <= arity, str(arity))
    mm = allrows("SELECT entity_id, position FROM audit_element_class_mismatch"
                 " WHERE undeclared_position")
    check("F6: class-mismatch audit no longer blind to undeclared positions",
          mm == [(l_bad, 7)], str(mm))
    con.execute("DELETE FROM entity_element WHERE entity_id = ?", [l_bad])
    con.execute("DELETE FROM entity WHERE id IN (?, ?)", [l_bad, l_empty])

    # ---- F3: axis-content corruption fires the new axis audits
    con.execute("BEGIN")
    ax_short = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash) VALUES (?, 'label', 3, 42)",
                [ax_short])
    con.execute("INSERT INTO axis_label (id, axis_id, pos, label, ordinal)"
                " VALUES (nextval('seq_axis_label'), ?, 0, 'only_one', 1)", [ax_short])
    ax_rev = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash) VALUES (?, 'datetime', 2, 43)",
                [ax_rev])
    con.execute("INSERT INTO axis_datetime VALUES (?, 0, TIMESTAMP '2030-06-01')", [ax_rev])
    con.execute("INSERT INTO axis_datetime VALUES (?, 1, TIMESTAMP '2030-01-01')", [ax_rev])
    ax_nostep = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash, p_start_ts)"
                " VALUES (?, 'datetime', 4, 44, TIMESTAMP '2030-01-01')", [ax_nostep])
    con.execute(
        "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, version, axis1_id, i1_pos, value_kind, v_float)"
        " VALUES (?, ?, ?, ?, 1, ?, 2, 'float', 5.0)", [node_c, demand, base, c3, ax_short])
    con.execute(
        "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, version, axis1_id, i1_pos, value_kind, v_float)"
        " VALUES (?, ?, ?, ?, 1, ?, 99, 'float', 6.0)", [node_b, inflow, high, c3, ax_rev])
    con.execute("COMMIT")
    probs = {p for (p,) in allrows("SELECT problem FROM audit_axis_integrity")}
    check("F3: short child table fires audit_axis_integrity",
          any("do not tile" in p for p in probs), str(probs))
    check("F3: non-monotonic datetime axis fires audit_axis_integrity",
          "sortable axis children not strictly increasing" in probs, str(probs))
    check("F3/M7: parametric datetime with NULL steps fires audit_axis_integrity",
          "parametric datetime axis missing step field(s)" in probs, str(probs))
    pr = allrows("SELECT axis_id, pos, length FROM audit_position_range")
    check("F3: i1_pos >= axis.length fires audit_position_range",
          pr == [(ax_rev, 99, 2)], str(pr))
    acc = {(a, p) for a, p in allrows(
        "SELECT axis_id, pos FROM audit_index_accelerator")}
    check("F3: unrenderable positions fire audit_index_accelerator (LEFT JOIN fix)",
          {(ax_short, 2), (ax_rev, 99)} <= acc, str(acc))
    con.execute("DELETE FROM value_long WHERE axis1_id IN (?, ?)", [ax_short, ax_rev])
    con.execute("DELETE FROM axis_label WHERE axis_id = ?", [ax_short])
    con.execute("DELETE FROM axis_datetime WHERE axis_id = ?", [ax_rev])
    con.execute("DELETE FROM axis WHERE id IN (?, ?, ?)", [ax_short, ax_rev, ax_nostep])

    # ---- F9: indexed date_time and duration values round-trip via the long tier
    con.execute("BEGIN")
    c5 = new_commit(con, "F9: maintenance windows (indexed date_time/duration)")
    window = new_pd(node, "window_start")
    outage = new_pd(node, "outage_duration")
    con.execute(
        "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, version, axis1_id, i1_pos, value_kind, v_datetime)"
        " VALUES (?, ?, ?, ?, 1, ?, 0, 'date_time', TIMESTAMP '2030-06-01 08:00')",
        [node_a, window, base, c5, ax_f])
    con.execute(
        "INSERT INTO value_long (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, version, axis1_id, i1_pos, value_kind, v_dur_months, v_dur_micros)"
        " VALUES (?, ?, ?, ?, 1, ?, 0, 'duration', 0, ?)", [node_a, outage, base, c5, ax_f, 36 * H])
    con.execute("COMMIT")
    r = one("SELECT idx1_text, v_datetime FROM read_parameter('node', 'window_start')")
    check("F9: long-tier date_time value reads back with its index",
          r == ("f_central", ts(2030, 6, 1, 8)), str(r))
    r = one("SELECT v_dur_months, v_dur_micros FROM read_parameter('node', 'outage_duration')")
    check("F9: long-tier duration value reads back", r == (0, 36 * H), str(r))
    n = one("SELECT count(*) FROM audit_type_conformance")[0]
    check("F9: extended type-conformance audit accepts the new kinds", n == 0, f"n={n}")

    # ---- float coordinate axis + values_int (float_axis_crs_int_plan §9) -----
    con.execute("BEGIN")
    cfa = new_commit(con, "float axis + values_int additions")
    # Canonical rank>=1 declarations are value_kind='indexed' (030_parameters:
    # "scalar kinds or 'indexed'" — the writer's convention; the payload
    # measure kind lives on the value rows, not on the declaration).
    level_par = new_pd(node, "level_profile")   # rank-1 float array (values payload)
    new_type(level_par, "indexed", 1, [("level", "float")], expected="array")
    band_par = new_pd(node, "band_response")
    new_type(band_par, "indexed", 1, [("band", "float")], expected="array")
    landuse = new_pd(node, "land_use")          # rank-1 int array (values_int payload)
    new_type(landuse, "indexed", 1, [("level", "float")], expected="array")

    # (1) parametric float grid (vertical levels 0..50) shared by two entities
    ax_lvl = axis_float_parametric(con, 0.0, 10.0, 6)
    write_array(con, cfa, node_a, level_par, base, [ax_lvl], [1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    write_array(con, cfa, node_b, level_par, base, [ax_lvl], [6.0, 5.0, 4.0, 3.0, 2.0, 1.0])
    # (2) irregular materialized float axis (spectral bands, µm)
    ax_band = axis_float(con, [0.40, 0.55, 0.70, 2.10])
    write_array(con, cfa, node_a, band_par, base, [ax_band], [0.9, 0.8, 0.7, 0.2])
    # (3) dense int array (land-use classes) on the 6-level grid
    write_array(con, cfa, node_a, landuse, base, [ax_lvl], [11, 22, 33, 22, 11, 44], int_kind=True)
    con.execute("COMMIT")

    ax_lvl2 = axis_float_parametric(con, 0.0, 10.0, 6)   # dedup hit
    check("float axis: parametric grid dedups to one axis object", ax_lvl == ax_lvl2,
          f"{ax_lvl} vs {ax_lvl2}")
    nfloat = one("SELECT count(*) FROM axis WHERE kind = 'float'"
                 " AND p_start_dbl = 0.0 AND p_step_dbl = 10.0 AND length = 6")[0]
    check("float axis: one parametric float axis minted for the shared grid", nfloat == 1, f"n={nfloat}")
    lvl0 = one("SELECT idx1_coord, v_float FROM read_parameter('node', 'level_profile')"
               " WHERE entity = 'node_a' AND idx1_coord = 0.0")
    lvl5 = one("SELECT v_float FROM read_parameter('node', 'level_profile')"
               " WHERE entity = 'node_a' AND idx1_coord = 50.0")
    check("float axis: parametric coord = p_start_dbl + p_step_dbl*pos renders (0.0->1.0, 50.0->6.0)",
          lvl0 == (0.0, 1.0) and lvl5 == (6.0,), f"{lvl0} / {lvl5}")

    coords = [r[0] for r in allrows(
        "SELECT idx1_coord FROM read_parameter('node', 'band_response')"
        " WHERE entity = 'node_a' ORDER BY idx1_ordinal")]
    check("float axis: irregular axis_float coords round-trip (read_parameter/value_rows idx1_coord)",
          coords == [0.40, 0.55, 0.70, 2.10], str(coords))

    lu = allrows("SELECT value_kind, v_int, v_float FROM read_parameter('node', 'land_use')"
                 " WHERE entity = 'node_a' ORDER BY idx1_coord")
    check("values_int: dense int array reads back (value_kind='int', v_int cells, v_float NULL)",
          [r[0] for r in lu] == ['int'] * 6
          and [r[1] for r in lu] == [11, 22, 33, 22, 11, 44]
          and all(r[2] is None for r in lu), str(lu))
    lut = one("SELECT tier FROM value_keys WHERE entity_id = ? AND parameter_definition_id = ?",
              node_a, landuse)
    check("values_int: int array lands in the array tier", lut == ('array',), str(lut))
    di = one("SELECT values_int FROM read_parameter_dense('node', 'land_use')")
    check("values_int: read_parameter_dense returns the int payload",
          di == ([11, 22, 33, 22, 11, 44],), str(di))
    for view in ["audit_axis_integrity", "audit_array_integrity", "audit_index_accelerator",
                 "audit_type_conformance", "audit_array_payload_xor"]:
        n = one(f"SELECT count(*) FROM {view}")[0]
        check(f"float/int additions: {view} clean", n == 0, f"n={n}")

    # ---- D2(a) declared allowed-type SETS: set-aware audits -------------------
    # (template_ir_plan §3.3 / VMP Part 7 as amended 2026-09-18: parameter_type
    # is multi-row — a declared allowed-type set; the writer enforces
    # exactly-one-member instantiation at entry, and these views must judge a
    # value against the SET, not against an arbitrary single row.)
    con.execute("BEGIN")
    cd2 = new_commit(con, "D2(a): multi-type declaration + one value per member check")
    mixed = new_pd(node, "mixed_profile")     # declared SET: two 'indexed' members,
    # distinguishable by their dimension tuples, with different expected tiers.
    new_type(mixed, "indexed", 1, [("level", "float")], expected="array")
    new_type(mixed, "indexed", 1, [("t", "datetime")], expected="long")
    write_array(con, cd2, node_b, mixed, base, [ax_lvl], [7, 8, 9, 7, 8, 9], int_kind=True)
    con.execute("COMMIT")
    n = one("SELECT count(*) FROM audit_type_conformance WHERE parameter_definition_id = ?",
            mixed)[0]
    check("D2(a): array payload licensed by an 'indexed' member is clean", n == 0, f"n={n}")
    ti = allrows("SELECT actual_tier FROM audit_tier_intent WHERE parameter_definition_id = ?",
                 mixed)
    check("D2(a): actual tier matching ONE member's expected_tier is clean"
          " (no row-multiplication over the set)", ti == [], str(ti))
    # negative control (the pre-D2(a) whitewash false negative, closed): an
    # array payload on a definition whose declared set licenses NO array at
    # all (a scalar-only 'int' member) must fire the array branch.
    con.execute("BEGIN")
    cneg = new_commit(con, "D2(a): negative control, array payload on a scalar-only set")
    fonly = new_pd(node, "scalar_only")
    new_type(fonly, "int", 0)
    write_array(con, cneg, node_b, fonly, base, [ax_lvl], [1, 2, 3, 4, 5, 6], int_kind=True)
    con.execute("COMMIT")
    rows = allrows("SELECT src_table, value_kind FROM audit_type_conformance"
                   " WHERE parameter_definition_id = ?", fonly)
    check("D2(a): array payload on a set licensing no array fires audit_type_conformance",
          rows == [("value_array", "int")], str(rows))
    con.execute("DELETE FROM value_array WHERE parameter_definition_id = ?", [fonly])
    # defaults leg (critique m5): a definition-scoped default violating the
    # definition's own value list is now visible; fixing it clears the audit.
    fuel = new_pd(commodity, "fuel_carrier")
    con.execute("UPDATE parameter_definition SET parameter_value_list_id = ?"
                " WHERE id = ?", [carrier, fuel])
    con.execute("BEGIN")
    cdef = new_commit(con, "D2(a): default off the value list")
    con.execute("INSERT INTO definition_value_scalar (parameter_definition_id, slot,"
                " valid_from_commit, version, value_kind, v_str)"
                " VALUES (?, 'default_value', ?, 1, 'string', 'hydrogen')", [fuel, cdef])
    con.execute("COMMIT")
    rows = allrows("SELECT src_table, parameter_definition_id"
                   " FROM audit_value_list_membership")
    check("D2(a)/m5: a default violating its value list fires the defaults leg",
          rows == [("definition_value_scalar:default_value", fuel)], str(rows))
    con.execute("UPDATE definition_value_scalar SET v_str = 'electricity'"
                " WHERE parameter_definition_id = ?", [fuel])
    n = one("SELECT count(*) FROM audit_value_list_membership")[0]
    check("D2(a)/m5: a default on the list is clean", n == 0, f"n={n}")

    # ---- D11 (2026-09-18): persisted member binding (parameter_type_id) ------
    # Column exists on all seven payload tables (the probe fails loudly if any
    # is missing); every smoke-written row is unbound (NULL — the smoke writes
    # by hand, below the entry check; the Rust writer always binds).
    nonnull = one(
        "SELECT count(*) FROM ("
        " SELECT parameter_type_id FROM value_scalar"
        " UNION ALL SELECT parameter_type_id FROM value_long"
        " UNION ALL SELECT parameter_type_id FROM value_array"
        " UNION ALL SELECT parameter_type_id FROM value_array_hist"
        " UNION ALL SELECT parameter_type_id FROM definition_value_scalar"
        " UNION ALL SELECT parameter_type_id FROM definition_value_long"
        " UNION ALL SELECT parameter_type_id FROM definition_value_array)"
        " WHERE parameter_type_id IS NOT NULL")[0]
    check("D11: parameter_type_id exists on all seven payload tables"
          " (all NULL before any bound write)", nonnull == 0, f"n={nonnull}")
    n = one("SELECT count(*) FROM audit_dangling_ref WHERE ref_table = 'parameter_type'"
            " AND src_table LIKE 'value%'")[0]
    check("D11: NULL bindings are silent in the referential audit", n == 0, f"n={n}")
    # A binding to ANOTHER definition's member is damage and fires; rebinding
    # to the row's own definition's member clears it.
    wrong_pt = one("SELECT id FROM parameter_type WHERE parameter_definition_id = ?", inflow)[0]
    con.execute("UPDATE value_scalar SET parameter_type_id = ?"
                " WHERE parameter_definition_id = ? AND valid_to_commit IS NULL",
                [wrong_pt, demand])
    n = one("SELECT count(*) FROM audit_dangling_ref WHERE ref_table = 'parameter_type'"
            " AND src_table = 'value_scalar'")[0]
    check("D11: a cross-definition binding fires the referential audit", n >= 1, f"n={n}")
    own_pt = one("SELECT id FROM parameter_type WHERE parameter_definition_id = ?", demand)[0]
    con.execute("UPDATE value_scalar SET parameter_type_id = ?"
                " WHERE parameter_definition_id = ? AND valid_to_commit IS NULL",
                [own_pt, demand])
    n = one("SELECT count(*) FROM audit_dangling_ref WHERE ref_table = 'parameter_type'")[0]
    check("D11: a same-definition binding is clean", n == 0, f"n={n}")

    # ---- float axis / values_int hostile checks (float_axis_crs_int_plan §6) --
    # non-monotonic float axis fires the monotonicity audit
    con.execute("BEGIN")
    ax_fbad = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash) VALUES (?, 'float', 2, 46)", [ax_fbad])
    con.execute("INSERT INTO axis_float (axis_id, pos, coord) VALUES (?, 0, 0.70), (?, 1, 0.40)",
                [ax_fbad, ax_fbad])
    con.execute("COMMIT")
    probs = {p for (p,) in allrows("SELECT problem FROM audit_axis_integrity WHERE axis_id = ?", ax_fbad)}
    check("float hostile: non-monotonic axis_float fires audit_axis_integrity",
          "sortable axis children not strictly increasing" in probs, str(probs))
    con.execute("DELETE FROM axis_float WHERE axis_id = ?", [ax_fbad])
    con.execute("DELETE FROM axis WHERE id = ?", [ax_fbad])

    # NaN coord rejected by the entry CHECK — critique F1 REGRESSION: `coord = coord`
    # would have ADMITTED NaN in DuckDB (NaN = NaN is TRUE); NOT isnan() must reject.
    ax_fnan = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash) VALUES (?, 'float', 1, 47)", [ax_fnan])
    expect_constraint(
        "float hostile/F1: NaN coord REJECTED by axis_float CHECK (NOT isnan)",
        "INSERT INTO axis_float (axis_id, pos, coord) VALUES (?, 0, 'nan'::DOUBLE)", [ax_fnan])
    con.execute("DELETE FROM axis WHERE id = ?", [ax_fnan])
    # NaN parametric field on axis rejected by the axis CHECK too
    expect_constraint(
        "float hostile/F1: NaN p_step_dbl REJECTED by axis CHECK (NOT isnan)",
        "INSERT INTO axis (id, kind, length, content_hash, p_start_dbl, p_step_dbl)"
        " VALUES (nextval('seq_axis'), 'float', 4, 50, 0.0, 'nan'::DOUBLE)")

    # both / neither payload rejected by the value_array XOR CHECK
    expect_constraint(
        "values_int hostile: both payloads populated rejected by XOR CHECK",
        "INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, version, axis1_id, values, values_int)"
        " VALUES (?, ?, ?, ?, 1, ?, [1.0], [1])", [node_c, demand, base, c3, ax_f])
    expect_constraint(
        "values_int hostile: neither payload populated rejected by XOR CHECK",
        "INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id,"
        " valid_from_commit, version, axis1_id, values, values_int)"
        " VALUES (?, ?, ?, ?, 1, ?, NULL, NULL)", [node_c, demand, base, c3, ax_f])

    # parametric float with missing / zero p_step_dbl fires the completeness audit
    con.execute("BEGIN")
    ax_fns = nextval(con, "seq_axis")   # p_step_dbl NULL
    con.execute("INSERT INTO axis (id, kind, length, content_hash, p_start_dbl)"
                " VALUES (?, 'float', 4, 48, 0.0)", [ax_fns])
    ax_fzs = nextval(con, "seq_axis")   # zero step (breaks strict-increasing)
    con.execute("INSERT INTO axis (id, kind, length, content_hash, p_start_dbl, p_step_dbl)"
                " VALUES (?, 'float', 4, 49, 0.0, 0.0)", [ax_fzs])
    con.execute("COMMIT")
    probs = {p for (p,) in allrows("SELECT problem FROM audit_axis_integrity"
                                   " WHERE axis_id IN (?, ?)", ax_fns, ax_fzs)}
    check("float hostile: parametric float with missing/zero p_step_dbl fires audit",
          probs == {"parametric float axis missing/invalid p_step_dbl"}, str(probs))
    con.execute("DELETE FROM axis WHERE id IN (?, ?)", [ax_fns, ax_fzs])

    # ---- F10 gap: time_pattern axis round-trip through the long tier
    con.execute("BEGIN")
    c6 = new_commit(con, "F10: time-pattern indexed parameter")
    ax_tp = nextval(con, "seq_axis")
    con.execute("INSERT INTO axis (id, kind, length, content_hash)"
                " VALUES (?, 'time_pattern', 2, 45)", [ax_tp])
    for pos, ordv, terms in [(0, 1, [("M", 1, 6), ("h", 1, 24)]), (1, 2, [("M", 7, 12)])]:
        for tno, (u, a, b) in enumerate(terms, start=1):
            con.execute("INSERT INTO axis_time_pattern VALUES (?, ?, ?, ?, ?, ?, ?)",
                        [ax_tp, pos, ordv, tno, u, a, b])
    fee = new_pd(node, "seasonal_fee")
    for pos, v in [(0, 10.0), (1, 20.0)]:
        write_long_cell(con, c6, node_a, fee, base, [ax_tp], [pos], "float", "v_float", v)
    con.execute("COMMIT")
    r = dict(allrows("SELECT idx1_text, v_float FROM read_parameter('node', 'seasonal_fee')"))
    check("F10: time-pattern cells render as unioned pattern text",
          r == {"M1-6,h1-24": 10.0, "M7-12": 20.0}, str(r))
    expect_constraint(
        "F10: out-of-range pattern term h17-26 rejected (#1569)",
        "INSERT INTO axis_time_pattern VALUES (?, 1, 2, 2, 'h', 17, 26)", [ax_tp])
    expect_constraint(
        "F10: unknown pattern unit 'q' rejected",
        "INSERT INTO axis_time_pattern VALUES (?, 1, 2, 2, 'q', 1, 2)", [ax_tp])

    # ---- F10 gap: default-value fallback in completeness
    before = one("SELECT count(*) FROM audit_missing_required_value WHERE entity = 'node_c'")[0]
    con.execute("BEGIN")
    con.execute(
        "INSERT INTO definition_value_scalar (parameter_definition_id, slot,"
        " valid_from_commit, version, value_kind, v_float)"
        " VALUES (?, 'default_value', ?, 1, 'float', 0.0)", [demand, c6])
    con.execute("COMMIT")
    after = one("SELECT count(*) FROM audit_missing_required_value WHERE entity = 'node_c'")[0]
    check("F10: writing a default_value clears missing-required (fallback honored)",
          before == 2 and after == 0, f"{before} -> {after}")
    con.execute("DELETE FROM definition_value_scalar WHERE parameter_definition_id = ?", [demand])

    # ---- F10 gap: long-tier close-out (crossing the 7-column one-current index)
    con.execute("BEGIN")
    c7 = new_commit(con, "F10: revise one gas ladder cell")
    con.execute(
        "UPDATE value_long SET valid_to_commit = ? WHERE entity_id = ? AND"
        " parameter_definition_id = ? AND alternative_id = ? AND i1_pos = 0 AND i2_pos = 0"
        " AND i3_pos = 0 AND valid_to_commit IS NULL", [c7, gas, ladder, base])
    write_long_cell(con, c7, gas, ladder, base, [ax_field, ax_p, ax_tier], [0, 0, 0],
                    "float", "v_float", 9999.0,
                    accels={"i2_dt": p_starts[0], "i3_num": 1})
    con.execute("UPDATE value_long SET version = 2 WHERE entity_id = ? AND i1_pos = 0"
                " AND i2_pos = 0 AND i3_pos = 0 AND valid_to_commit IS NULL"
                " AND parameter_definition_id = ?", [gas, ladder])
    con.execute("COMMIT")
    cur = one("SELECT v_float FROM read_parameter('commodity', 'price_ladder')"
              " WHERE entity = 'gas' AND idx1_text = 'price' AND idx3_num = 1"
              " AND idx2_ts = TIMESTAMP '2030-01-01'")
    old = one("SELECT v_float FROM value_long_asof(?) WHERE entity_id = ? AND i1_pos = 0"
              " AND i2_pos = 0 AND i3_pos = 0", c6, gas)
    check("F10: long-tier cell close-out — new current reads, as-of returns pre-image",
          cur == (9999.0,) and old == (1011.0,), f"{cur} / {old}")
    n = one("SELECT count(*) FROM audit_history_tiling")[0]
    check("F10: long-tier update leaves version tiling clean", n == 0, f"n={n}")

    # ---- F10 gap: value delete (close-out with no successor) + as-of
    con.execute("BEGIN")
    c8 = new_commit(con, "F10: delete node_b demand")
    con.execute("UPDATE value_scalar SET valid_to_commit = ? WHERE entity_id = ?"
                " AND parameter_definition_id = ? AND valid_to_commit IS NULL", [c8, node_b, demand])
    con.execute("COMMIT")
    now_n = one("SELECT count(*) FROM value_scalar_current WHERE entity_id = ?", node_b)[0]
    past = one("SELECT v_float FROM value_scalar_asof(?) WHERE entity_id = ?"
               " AND parameter_definition_id = ?", c7, node_b, demand)
    tiling = one("SELECT count(*) FROM audit_history_tiling")[0]
    check("F10: delete = close-out; as-of still sees the value; tiling stays clean",
          now_n == 0 and past == (5.0,) and tiling == 0, f"{now_n} / {past} / {tiling}")
    con.execute("BEGIN")
    write_scalar(con, c8, node_b, demand, base, "float", "v_float", 5.0)
    con.execute("UPDATE value_scalar SET version = 2 WHERE entity_id = ?"
                " AND parameter_definition_id = ? AND valid_to_commit IS NULL", [node_b, demand])
    con.execute("COMMIT")

    # ---- F10 gap: rank-4 long value renders all four indices
    con.execute("BEGIN")
    c9 = new_commit(con, "F10: rank-4 value")
    quad = new_pd(node, "quad_par")
    write_long_cell(con, c9, node_a, quad, base,
                    [ax_f, ax_p, ax_tier, ax_ft], [1, 1, 2, 3], "float", "v_float", 4444.0,
                    accels={"i2_dt": p_starts[1], "i3_num": 3,
                            "i4_dt": ts(2030, 1, 1, 18)})
    con.execute("COMMIT")
    r = one("SELECT idx1_text, idx2_ts, idx3_num, idx4_ts, v_float"
            " FROM read_parameter('node', 'quad_par')")
    check("F10: rank-4 cell renders all four axis indices",
          r == ("f_wet", ts(2035, 1, 1), 3, ts(2030, 1, 1, 18), 4444.0), str(r))

    # ---- F10 gap: as-of on a narrow table (entity rename)
    con.execute("BEGIN")
    c10 = new_commit(con, "F10: rename node_a")
    con.execute("UPDATE entity SET valid_to_commit = ? WHERE id = ? AND valid_to_commit IS NULL",
                [c10, node_a])
    con.execute("INSERT INTO entity (id, entity_class_id, name, valid_from_commit, version)"
                " VALUES (?, ?, 'node_a_renamed', ?, 2)", [node_a, node, c10])
    con.execute("COMMIT")
    was = one("SELECT name FROM entity_asof(?) WHERE id = ?", c9, node_a)
    is_now = one("SELECT name FROM entity_current WHERE id = ?", node_a)
    check("F10/M4: narrow-table as-of — rename keeps id, as-of returns old name",
          was == ("node_a",) and is_now == ("node_a_renamed",), f"{was} / {is_now}")
    con.execute("BEGIN")
    c11 = new_commit(con, "F10: revert rename")
    con.execute("UPDATE entity SET valid_to_commit = ? WHERE id = ? AND valid_to_commit IS NULL",
                [c11, node_a])
    con.execute("INSERT INTO entity (id, entity_class_id, name, valid_from_commit, version)"
                " VALUES (?, ?, 'node_a', ?, 3)", [node_a, node, c11])
    con.execute("COMMIT")

    # ---- F10 gap: case-collision rejections (Decision 11 surfaces)
    expect_constraint("F10: case collision rejected — entity 'NODE_A' vs 'node_a'",
                      "INSERT INTO entity (id, entity_class_id, name, valid_from_commit)"
                      " VALUES (nextval('seq_entity'), ?, 'NODE_A', ?)", [node, c3])
    expect_constraint("F10: case collision rejected — class 'Node' vs 'node'",
                      "INSERT INTO entity_class (id, name, valid_from_commit)"
                      " VALUES (nextval('seq_entity_class'), 'Node', ?)", [c3])
    expect_constraint("F10: case collision rejected — alternative 'BASE' vs 'Base'",
                      "INSERT INTO alternative (id, name, valid_from_commit)"
                      " VALUES (nextval('seq_alternative'), 'BASE', ?)", [c3])

    # ---- #2373 relaxed: free-text names (hyphen/space) insert and round-trip.
    # The charset CHECK is gone; the DB now accepts any non-null name. The
    # hygiene floor (non-empty/trimmed/no-control-chars) lives in the writer,
    # not the DB — so only positive DB acceptance is asserted here.
    con.execute("BEGIN")
    hc = new_commit(con, "#2373: free-text names")
    hcls = nextval(con, "seq_entity_class")
    con.execute("INSERT INTO entity_class (id, name, valid_from_commit)"
                " VALUES (?, '5week-invest', ?)", [hcls, hc])
    hent = nextval(con, "seq_entity")
    con.execute("INSERT INTO entity (id, entity_class_id, name, valid_from_commit)"
                " VALUES (?, ?, 'gen-1 (wind)', ?)", [hent, hcls, hc])
    con.execute("COMMIT")
    check("#2373 relaxed: hyphenated/spaced class + entity names insert and round-trip",
          one("SELECT name FROM entity_class WHERE id = ?", hcls) == ("5week-invest",)
          and one("SELECT name FROM entity WHERE id = ?", hent) == ("gen-1 (wind)",))

    # ---- F10/M2: membership cycle surfaces, incl. the scenario-free base branch
    con.execute("BEGIN")
    con.execute("INSERT INTO membership (group_entity_id, member_entity_id, included,"
                " valid_from_commit) VALUES (?, ?, TRUE, ?)", [node_a, west, c3])
    con.execute("COMMIT")
    cyc = allrows("SELECT DISTINCT scenario_id IS NULL AS base_branch FROM audit_membership_cycle")
    check("F10/M2: 2-cycle west<->node_a fires audit_membership_cycle in both branches",
          sorted(x[0] for x in cyc) == [False, True], str(cyc))
    n = one("SELECT count(*) FROM membership_base_flat")[0]
    check("M2: base membership flattening not gated on scenarios", n > 0, f"n={n}")
    con.execute("DELETE FROM membership WHERE group_entity_id = ?", [node_a])

    # ---- M1: parameter_value_metadata dangling entity / dangling triple
    meta_id = one("SELECT id FROM metadata_current WHERE lower(name) = 'schema_version'")[0]
    con.execute("BEGIN")
    con.execute("INSERT INTO parameter_value_metadata (entity_id, parameter_definition_id,"
                " alternative_id, metadata_id, valid_from_commit)"
                " VALUES (99999, ?, ?, ?, ?)", [demand, base, meta_id, c3])
    con.execute("COMMIT")
    refs = {r for (r,) in allrows("SELECT ref_table FROM audit_dangling_ref"
                                  " WHERE src_table = 'parameter_value_metadata'")}
    check("M1: value-metadata link with dead entity fires entity AND triple branches",
          refs == {"entity", "value_keys (triple)"}, str(refs))
    con.execute("DELETE FROM parameter_value_metadata WHERE entity_id = 99999")

    # ---- M3: an empty scenario does not flood the missing-required audit
    con.execute("BEGIN")
    s_void = nextval(con, "seq_scenario")
    insert("scenario", "id, name, valid_from_commit", [s_void, "void", c3])
    con.execute("COMMIT")
    flagged = one("SELECT count(*) FROM audit_scenario_empty WHERE scenario = 'void'")[0]
    noise = one("SELECT count(*) FROM audit_missing_required_value WHERE scenario = 'void'")[0]
    check("M3: empty scenario -> audit_scenario_empty only, no missing-value noise",
          flagged == 1 and noise == 0, f"flag={flagged} noise={noise}")
    con.execute("DELETE FROM scenario WHERE id = ?", [s_void])

    # ---- M12: hostile scenario-name strings pass safely through the macros
    for weird in ["we'ird%", 'a"b\\c']:
        n = con.execute("SELECT count(*) FROM scenario_contents(?)", [weird]).fetchone()[0]
        check(f"M12: scenario_contents({weird!r}) -> empty, no error", n == 0, f"n={n}")

    # =======================================================================
    # Erasure marker + the as-of guard (erasure_compaction_plan §1, decisions
    # D-E1/D-E3 2026-09-19). The marker CHECK trio is exercised, then a marker
    # is planted raw and the FULL consumer-shape sweep of acceptance 3 runs
    # over all 31 as-of macros: the guard must fire under a bare SELECT *,
    # count(*), a pruned projection, AND a join — and must stay silent without
    # a marker and at/after the marker commit. This sweep is the PERMANENT
    # engine-upgrade tripwire (the guard's soundness rests on optimizer
    # behavior: filter expressions are not prunable, projection-riding guards
    # are — see 110_views_history.sql); never remove it.
    # =======================================================================

    # CHECK trio: exactly one scope column, matching the kind tag.
    expect_constraint(
        "erasure_marker: kind='alternative' without alternative_id rejected",
        "INSERT INTO erasure_marker (kind, at_commit, rows_removed)"
        " VALUES ('alternative', ?, 0)", [c3])
    expect_constraint(
        "erasure_marker: alternative_id on a kind='horizon' marker rejected",
        "INSERT INTO erasure_marker (kind, alternative_id, horizon_commit, at_commit, rows_removed)"
        " VALUES ('horizon', ?, ?, ?, 0)", [base, c3, c3])
    expect_constraint(
        "erasure_marker: kind='entity' without entity_id rejected",
        "INSERT INTO erasure_marker (kind, at_commit, rows_removed)"
        " VALUES ('entity', ?, 0)", [c3])
    # The kind tag is extensible by DDL edit only (widen the IN-list when a
    # grain ships) — an unknown string is unrepresentable, never silently inert.
    expect_constraint(
        "erasure_marker: unknown kind tag rejected (extensible by DDL edit only)",
        "INSERT INTO erasure_marker (kind, at_commit, rows_removed)"
        " VALUES ('poison', ?, 0)", [c3])

    asof_macros = [
        "metadata_asof", "entity_metadata_asof", "parameter_value_metadata_asof",
        "entity_class_asof", "class_set_asof", "class_set_member_asof",
        "entity_class_dimension_asof", "entity_asof", "entity_location_asof",
        "entity_element_asof", "alternative_asof", "scenario_asof",
        "scenario_alternative_asof", "entity_alternative_asof", "membership_asof",
        "units_of_measure_asof", "parameter_value_list_asof", "list_value_asof",
        "parameter_definition_asof", "entity_parameter_unit_asof",
        "parameter_type_asof", "parameter_type_dimension_asof", "display_mode_asof",
        "entity_class_display_mode_asof", "semantic_annotation_asof",
        "value_scalar_asof", "value_long_asof", "value_array_asof",
        "definition_value_scalar_asof", "definition_value_long_asof",
        "definition_value_array_asof"]

    def guard_shapes(m, c):
        # The B1 consumer shapes: bare, aggregate (projection fully pruned),
        # column-pruned constant projection, join into another relation
        # (every macro exposes valid_from_commit).
        return [
            f"SELECT * FROM {m}({c})",
            f"SELECT count(*) FROM {m}({c})",
            f"SELECT 1 FROM {m}({c})",
            f'SELECT count(*) FROM {m}({c}) t JOIN "commit" cm'
            f" ON cm.id = t.valid_from_commit"]

    def guard_sweep(c):
        """Per macro: 'error' if all shapes fail loudly on the guard,
        'silent' if all succeed, else the offending shape."""
        out = {}
        for m in asof_macros:
            verdicts = set()
            for shape in guard_shapes(m, c):
                try:
                    con.execute(shape).fetchall()
                    verdicts.add("silent")
                except duckdb.Error as e:
                    verdicts.add("error" if "unreliable" in str(e) else f"BAD:{e}")
            out[m] = verdicts.pop() if len(verdicts) == 1 else f"mixed:{verdicts}"
        return out

    # The hardcoded sweep list must equal the live catalog's census of *_asof
    # table macros — a new as-of macro that skips the guard sweep (or a
    # renamed one that silently drops out of it) fails HERE, not never.
    # `value_timeline` / `history_untrusted_span` are views, not macros, and
    # do not match the suffix; there are no other exclusions.
    census = sorted(r[0] for r in allrows(
        "SELECT DISTINCT function_name FROM duckdb_functions()"
        " WHERE NOT internal AND function_type = 'table_macro'"
        " AND ends_with(function_name, '_asof')"))
    check("erasure guard: sweep list == catalog census of *_asof table macros"
          " (31, no stragglers)",
          census == sorted(asof_macros), str(set(census) ^ set(asof_macros)))

    c_latest = one('SELECT max(id) FROM "commit"')[0]
    sweep = guard_sweep(c_latest)
    check("erasure guard: all 31 as-of macros silent in every shape on a"
          " marker-free database",
          all(v == "silent" for v in sweep.values()),
          str({m: v for m, v in sweep.items() if v != "silent"}))

    # Plant an erasure marker raw (the marker is append-only by convention —
    # this is the smoke suite's usual plant-assert-clean pattern; the Rust
    # suite exercises the real erase primitive end-to-end).
    con.execute("BEGIN")
    c_erase = new_commit(con, "hostile: planted erasure marker")
    con.execute("INSERT INTO erasure_marker (kind, alternative_id, at_commit,"
                " rows_removed, bytes_estimate, note)"
                " VALUES ('alternative', ?, ?, 0, 0, 'smoke plant')", [high, c_erase])
    con.execute("COMMIT")

    sweep = guard_sweep(c_erase - 1)
    for m in asof_macros:
        check(f"erasure guard: {m} fails loudly before the marker in ALL"
              f" consumer shapes", sweep[m] == "error", str(sweep[m]))
    sweep = guard_sweep(c_erase)
    check("erasure guard: all 31 macros exact and silent AT the marker commit",
          all(v == "silent" for v in sweep.values()),
          str({m: v for m, v in sweep.items() if v != "silent"}))

    ae = allrows("SELECT kind, scope_name, scope_unresolved, at_commit,"
                 " last_unreliable_asof FROM audit_erasure")
    check("audit_erasure: marker inventory resolves scope to the retained"
          " identity and reports the unreliable as-of range",
          ae == [("alternative", "high", False, c_erase, c_erase - 1)], str(ae))

    # The marker is deliberately KEPT (not cleaned up like the hostile plants):
    # the final all-audits sweep below must hold on a database in the erased
    # state — markers are permanent by contract, so "clean" must mean clean
    # WITH a marker present. (The marker-free direction was asserted by the
    # control sweep above, before the plant.)

    # ---- everything green again after the hostile section's cleanups
    for view in ["audit_dangling_ref", "audit_history_tiling", "audit_array_integrity",
                 "audit_type_conformance", "audit_index_accelerator",
                 "audit_cross_tier_duplicate", "audit_membership_cycle",
                 "audit_class_set_entity_name_dup", "audit_element_class_mismatch",
                 "audit_element_arity", "audit_value_class_mismatch",
                 "audit_value_list_membership", "audit_scenario_empty",
                 "audit_required_class_empty", "audit_axis_integrity",
                 "audit_array_payload_xor", "audit_position_range",
                 "audit_metadata_reserved_dup",
                 "audit_realizes_cycle", "audit_dangling_semantic_ref"]:
        n = one(f"SELECT count(*) FROM {view}")[0]
        check(f"final: {view} clean after hostile section", n == 0, f"n={n}")

    # ---- the erasure marker survived to the end WITH every audit clean above,
    # and the guard is still armed (permanent-marker contract).
    n_mark = one("SELECT count(*) FROM erasure_marker")[0]
    try:
        con.execute(f"SELECT count(*) FROM value_scalar_asof({c_erase - 1})").fetchall()
        armed = False
    except duckdb.Error as e:
        armed = "unreliable" in str(e)
    check("erasure guard: marker retained through the final audit sweep and"
          " still refusing pre-marker as-of", n_mark == 1 and armed,
          f"markers={n_mark} armed={armed}")

    # ---- DB-identity: stable for the lifetime of the database — still the one
    # current row minted at init, byte-identical after the whole smoke flow.
    rows = allrows(ident_sql)
    check("db_identity: single current row, unchanged across the whole suite",
          rows == [(dbid,)], str(rows))

    con.close()
    n_fail = RESULTS.count(False)
    print(f"\n{len(RESULTS)} checks, {n_fail} failures")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
