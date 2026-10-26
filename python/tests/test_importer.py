"""Generic Spine -> nodal-db importer suite, on SYNTHETIC Spine databases.

Everything here is made up (classes ``widget`` / ``hub`` / ``widget__hub``,
a ``run_modes`` list, …) and imports through the generic ``Profile`` with an
explicit template name and version — no tool-specific profile or template.

Covered:

- version-string normalization and the ``upgrade=False`` open contract;
- the identity stamp is non-optional (no version / no name => FAIL), and is
  present on the imported DB with the explicit name/version;
- the ``Profile.version_of`` hook supplies the version when none is passed;
- OBSERVED-SHAPE DECLARATION: each definition's declared member set is the
  union of shapes seen in its stored values (scalar float/string, label Map,
  datetime time series, integer-position Array); value-less definitions
  import typeless; list bindings carry over;
- the ``parameter_type_list`` drift table (declared vs observed);
- LOUD residue: time patterns, entity/scenario descriptions, entity groups,
  metadata items, non-literal defaults, ragged multi-shape values;
- structural sections (alternatives, scenarios, entities incl. 2-D,
  activation) and value fidelity read back against the Spine source;
- case-colliding names refuse loudly;
- names with leading/trailing whitespace import trimmed with a warning per
  name, and every reference follows; trimming into another name, or to
  nothing, refuses.

Run (project venv carries spinedb_api, pytest and the ``nodal_db`` skin;
rebuild the skin with ``maturin develop`` in ``crates/nodal-db-py`` after core
changes):

    .venv/bin/python -m pytest python/tests -q
"""

from __future__ import annotations

import pytest

pytest.importorskip("spinedb_api")
pytest.importorskip("nodal_db")

from nodal_spine_import import (  # noqa: E402
    ImportError_,
    Profile,
    SpineImporter,
    normalize_version,
)
from nodal_spine_import.importer import OPEN_KWARGS  # noqa: E402

TEMPLATE_NAME = "synthetic_model"


class SyntheticProfile(Profile):
    """The generic profile with an explicit template name (nothing else)."""

    template_name = TEMPLATE_NAME


# ---------------------------------------------------------------------------
# Synthetic Spine builders
# ---------------------------------------------------------------------------


def _url(path):
    return "sqlite:///" + str(path)


def _value(db, cls, byname, param, alt, value):
    from spinedb_api import to_database

    v, t = to_database(value)
    db.add_update_item(
        "parameter_value", entity_class_name=cls, entity_byname=tuple(byname),
        parameter_definition_name=param, alternative_name=alt, value=v, type=t,
    )


def make_basic_spine(tmp_path, name="basic.sqlite"):
    """A small, clean synthetic source: every construct is importable."""
    from spinedb_api import (
        Array,
        DatabaseMapping,
        Map,
        TimeSeriesVariableResolution,
        to_database,
    )

    path = tmp_path / name
    with DatabaseMapping(_url(path), create=True) as db:
        db.add_update_item("alternative", name="high")
        for cls in ("widget", "hub"):
            db.add_update_item("entity_class", name=cls)
        db.add_update_item(
            "entity_class", name="widget__hub", dimension_name_list=("widget", "hub"),
        )
        db.add_update_item("parameter_value_list", name="run_modes")
        for i, member in enumerate(("idle", "busy", "boost")):
            v, t = to_database(member)
            db.add_update_item(
                "list_value", parameter_value_list_name="run_modes", value=v, type=t,
                index=i,
            )

        # widget: float scalar, list-bound string, free string, Array, and a
        # value-less definition.
        db.add_update_item("parameter_definition", entity_class_name="widget",
                           name="capacity")
        dv, dt = to_database("idle")
        db.add_update_item("parameter_definition", entity_class_name="widget",
                           name="mode", parameter_value_list_name="run_modes",
                           default_value=dv, default_type=dt)
        db.add_update_item("parameter_definition", entity_class_name="widget",
                           name="tag")
        db.add_update_item("parameter_definition", entity_class_name="widget",
                           name="stages")
        db.add_update_item("parameter_definition", entity_class_name="widget",
                           name="unused_knob")
        # hub: a label Map and a datetime time series.
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="demand")
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="inflow")
        # widget__hub: a 2-D class scalar.
        db.add_update_item("parameter_definition", entity_class_name="widget__hub",
                           name="share")

        for w in ("w1", "w2"):
            db.add_update_item("entity", entity_class_name="widget", entity_byname=(w,))
        db.add_update_item("entity", entity_class_name="hub", entity_byname=("h1",))
        db.add_update_item("entity", entity_class_name="widget__hub",
                           entity_byname=("w1", "h1"))

        _value(db, "widget", ["w1"], "capacity", "Base", 10.0)
        _value(db, "widget", ["w2"], "capacity", "Base", 20.0)
        _value(db, "widget", ["w1"], "capacity", "high", 15.0)
        _value(db, "widget", ["w1"], "mode", "Base", "busy")
        _value(db, "widget", ["w2"], "tag", "Base", "blue")
        _value(db, "widget", ["w1"], "stages", "Base", Array([1.0, 2.0, 3.0]))
        _value(db, "hub", ["h1"], "demand", "Base",
               Map(["p1", "p2"], [5.0, 6.0], index_name="period"))
        _value(db, "hub", ["h1"], "inflow", "Base", TimeSeriesVariableResolution(
            ["2030-01-01T00:00", "2030-01-01T01:00"], [1.5, 2.5], False, False))
        _value(db, "widget__hub", ["w1", "h1"], "share", "Base", 0.25)

        db.add_update_item("scenario", name="sc_high")
        db.add_update_item("scenario_alternative", scenario_name="sc_high",
                           alternative_name="Base", rank=1)
        db.add_update_item("scenario_alternative", scenario_name="sc_high",
                           alternative_name="high", rank=2)
        db.add_update_item("entity_alternative", entity_class_name="widget",
                           entity_byname=("w2",), alternative_name="high", active=False)
        db.commit_session("synthetic basic source")
    return path


def run_import(tmp_path, spine, *, version="7", out="out.duckdb", profile=None):
    imp = SpineImporter(str(spine), str(tmp_path / out),
                        profile or SyntheticProfile(), user="t", version=version)
    return imp.run(), tmp_path / out


def _nodal(path):
    import nodal_db

    return nodal_db.open(str(path))


def _rows(db, sql):
    return db.query(sql).read_all().to_pylist()


def _meta(db):
    return {
        r["name"]: r["value"]
        for r in _rows(db, "SELECT name, value FROM metadata WHERE valid_to_commit IS NULL")
    }


def _declared(db, cls, param):
    """Current declared member signatures of (cls, param): sorted
    (value_kind-ish, rank) tuples straight from parameter_type."""
    return _rows(
        db,
        "SELECT pt.* FROM parameter_type pt "
        "JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id "
        "JOIN entity_class c ON c.id = pd.cur_cls "
        f"WHERE pt.valid_to_commit IS NULL AND c.cur_name = '{cls}' "
        f"AND pd.cur_name = '{param}'",
    )


# ---------------------------------------------------------------------------
# Pins: version normalization, open contract, identity stamp
# ---------------------------------------------------------------------------


def test_version_normalization_pin():
    assert normalize_version("7.0") == "7"
    assert normalize_version(7.0) == "7"
    assert normalize_version(" 12 ") == "12"
    with pytest.raises(ImportError_):
        normalize_version("7.5")
    with pytest.raises(ImportError_):
        normalize_version("seven")
    with pytest.raises(ImportError_, match="non-optional"):
        normalize_version(None)


def test_upgrade_false_pin():
    # The importer NEVER upgrades or creates its input.
    assert OPEN_KWARGS == {"create": False, "upgrade": False}


def test_stamp_is_non_optional_without_version(tmp_path):
    spine = make_basic_spine(tmp_path)
    with pytest.raises(ImportError_, match="non-optional"):
        run_import(tmp_path, spine, version=None)
    assert not (tmp_path / "out.duckdb").exists(), "nothing written before the refusal"


def test_profile_without_template_name_refuses(tmp_path):
    spine = make_basic_spine(tmp_path)
    with pytest.raises(ImportError_, match="no template name"):
        run_import(tmp_path, spine, profile=Profile())


def test_explicit_identity_is_stamped(tmp_path):
    rep, out = run_import(tmp_path, make_basic_spine(tmp_path), version="7.0")
    assert (rep.template_name, rep.template_version) == (TEMPLATE_NAME, "7")
    meta = _meta(_nodal(out))
    assert meta.get("template_name") == TEMPLATE_NAME
    assert meta.get("template_version") == "7"


def test_profile_version_hook_supplies_the_version(tmp_path):
    class Versioned(SyntheticProfile):
        def version_of(self, db):
            return "3.0"

    rep, out = run_import(tmp_path, make_basic_spine(tmp_path), version=None,
                          profile=Versioned())
    assert rep.template_version == "3"
    assert _meta(_nodal(out)).get("template_version") == "3"


# ---------------------------------------------------------------------------
# A clean source: counts, observed-shape declaration, zero residue
# ---------------------------------------------------------------------------


def test_clean_source_imports_with_zero_residue(tmp_path):
    rep, out = run_import(tmp_path, make_basic_spine(tmp_path))
    assert rep.residue == [], rep.summary()
    assert rep.drift_table == []
    assert rep.write_warnings == [], "everything is pre-declared: no auto-creates"
    assert rep.counts["classes"] == 3
    assert rep.counts["parameter_definitions"] == 8
    assert rep.counts["value_lists"] == 1
    assert rep.counts["entities"] == 4
    assert rep.counts["alternatives"] == 2
    assert rep.counts["scenarios"] == 1
    assert rep.counts["activation_rows"] == 1
    # values_written counts (entity, alternative) values, not cells:
    # 3 capacity + mode + tag + stages (Array) + demand (Map) + inflow (time
    # series) + share = 9.
    assert rep.counts["values_written"] == 9
    kinds = [k for k, _ in rep.commits]
    assert kinds[:2] == ["vocabulary", "structural"]
    assert set(kinds[2:]) == {"values"}


def test_observed_shape_declaration(tmp_path):
    _rep, out = run_import(tmp_path, make_basic_spine(tmp_path))
    db = _nodal(out)
    # one member per observed shape; nothing for a value-less definition
    assert len(_declared(db, "widget", "capacity")) == 1
    assert len(_declared(db, "widget", "tag")) == 1
    assert len(_declared(db, "widget", "stages")) == 1
    assert len(_declared(db, "hub", "demand")) == 1
    assert len(_declared(db, "hub", "inflow")) == 1
    assert len(_declared(db, "widget__hub", "share")) == 1
    assert _declared(db, "widget", "unused_knob") == [], "value-less imports typeless"

    # the dims of the indexed members carry the observed index names/kinds
    dims = _rows(
        db,
        "SELECT c.cur_name AS cls, pd.cur_name AS param, pt.value_kind, pt.rank, "
        "d.position, d.dimension_name, d.kind FROM parameter_type pt "
        "JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id "
        "JOIN entity_class c ON c.id = pd.cur_cls "
        "JOIN parameter_type_dimension d ON d.parameter_type_id = pt.id "
        "AND d.valid_to_commit IS NULL "
        "WHERE pt.valid_to_commit IS NULL ORDER BY cls, param, d.position",
    )
    got = {(r["cls"], r["param"]): (r["rank"], r["dimension_name"], r["kind"])
           for r in dims}
    assert got == {
        ("hub", "demand"): (1, "period", "label"),
        ("hub", "inflow"): (1, "time", "datetime"),
        ("widget", "stages"): (1, "i", "integer"),
    }, dims
    scalars = {
        (r["cls"], r["param"]): r["value_kind"]
        for r in _rows(
            db,
            "SELECT c.cur_name AS cls, pd.cur_name AS param, pt.value_kind "
            "FROM parameter_type pt "
            "JOIN parameter_definition pd ON pd.id = pt.parameter_definition_id "
            "JOIN entity_class c ON c.id = pd.cur_cls "
            "WHERE pt.valid_to_commit IS NULL AND pt.rank = 0",
        )
    }
    assert scalars == {
        ("widget", "capacity"): "float",
        ("widget", "mode"): "string",
        ("widget", "tag"): "string",
        ("widget__hub", "share"): "float",
    }, scalars

    # list binding carried over, with its members and default
    bound = _rows(
        db,
        "SELECT l.name AS list FROM parameter_definition pd "
        "JOIN parameter_value_list l ON l.id = pd.parameter_value_list_id "
        "AND l.valid_to_commit IS NULL "
        "WHERE pd.valid_to_commit IS NULL AND pd.cur_name = 'mode'",
    )
    assert [r["list"] for r in bound] == ["run_modes"]
    members = _rows(
        db,
        "SELECT lv.v_str FROM list_value lv "
        "JOIN parameter_value_list l ON l.id = lv.parameter_value_list_id "
        "WHERE lv.valid_to_commit IS NULL AND l.cur_name = 'run_modes' "
        "ORDER BY lv.ordinal",
    )
    assert [m["v_str"] for m in members] == ["idle", "busy", "boost"]


def test_structural_sections_land(tmp_path):
    _rep, out = run_import(tmp_path, make_basic_spine(tmp_path))
    db = _nodal(out)
    alts = {r["name"] for r in _rows(
        db, "SELECT name FROM alternative WHERE valid_to_commit IS NULL")}
    assert {"Base", "high"} <= alts
    ents = {r["name"] for r in _rows(
        db, "SELECT name FROM entity WHERE valid_to_commit IS NULL")}
    assert {"w1", "w2", "h1", "w1__h1"} <= ents
    dims = _rows(
        db,
        "SELECT count(*) AS n FROM entity_class_dimension d "
        "JOIN entity_class c ON c.id = d.entity_class_id "
        "WHERE d.valid_to_commit IS NULL AND c.cur_name = 'widget__hub'",
    )
    assert dims == [{"n": 2}]


# ---------------------------------------------------------------------------
# Value fidelity: imported content == the Spine source, read independently
# ---------------------------------------------------------------------------


def _cell(row):
    return {
        "float": row["v_float"],
        "int": row["v_int"],
        "integer": row["v_int"],
        "bool": row["v_bool"],
        "string": row["v_str"],
    }[row["value_kind"]]


def _nodal_values(db, cls, param):
    rows = _rows(
        db,
        "SELECT entity, alternative, value_kind, idx1_text, "
        "v_float, v_int, v_bool, v_str "
        f"FROM read_parameter('{cls}', '{param}')",
    )
    grouped: dict = {}
    for r in rows:
        grouped.setdefault((r["entity"], r["alternative"]), []).append(r)
    out = {}
    for key, group in grouped.items():
        if len(group) == 1 and group[0]["idx1_text"] is None:
            out[key] = ("scalar", _cell(group[0]))
        else:
            out[key] = ("map", sorted((g["idx1_text"], _cell(g)) for g in group))
    return out


def test_imported_values_match_the_spine_source(tmp_path):
    from spinedb_api import DatabaseMapping, Map, from_database

    spine = make_basic_spine(tmp_path)
    _rep, out = run_import(tmp_path, spine)
    want = {("widget", "capacity"), ("widget", "mode"), ("widget", "tag"),
            ("hub", "demand"), ("widget__hub", "share")}

    source = {}
    with DatabaseMapping(_url(spine), upgrade=False) as sdb:
        for pv in sdb.find_parameter_values():
            key = (pv["entity_class_name"], pv["parameter_definition_name"])
            if key not in want:
                continue
            val = from_database(pv["value"], pv["type"])
            norm = (
                ("map", sorted((str(i), v) for i, v in zip(val.indexes, val.values)))
                if isinstance(val, Map) else ("scalar", val)
            )
            entity = "__".join(pv["entity_byname"])
            source[(key[0], entity, pv["alternative_name"], key[1])] = norm

    db = _nodal(out)
    imported = {}
    for cls, param in want:
        for (entity, alt), norm in _nodal_values(db, cls, param).items():
            imported[(cls, entity, alt, param)] = norm

    assert ("widget", "w1", "high", "capacity") in source  # non-Base covered
    assert source[("hub", "h1", "Base", "demand")] == ("map", [("p1", 5.0), ("p2", 6.0)])
    assert imported == source


def test_array_and_time_series_cells_land(tmp_path):
    _rep, out = run_import(tmp_path, make_basic_spine(tmp_path))
    db = _nodal(out)
    stages = _rows(db, "SELECT v_float FROM read_parameter('widget', 'stages')")
    assert sorted(r["v_float"] for r in stages) == [1.0, 2.0, 3.0]
    inflow = _rows(db, "SELECT v_float FROM read_parameter('hub', 'inflow')")
    assert sorted(r["v_float"] for r in inflow) == [1.5, 2.5]


# ---------------------------------------------------------------------------
# Drift table and loud residue
# ---------------------------------------------------------------------------


def test_type_drift_imports_by_construction_with_a_drift_row(tmp_path):
    """Declared ``float`` but stored as a mix of string scalar + Array: both
    land (observed-shape declaration => zero type rejections) and the drift
    table names the mismatch."""
    from spinedb_api import Array, DatabaseMapping

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="routing", parameter_type_list=("float",))
        _value(db, "hub", ["h1"], "routing", "Base", "direct")
        db.add_update_item("entity", entity_class_name="hub", entity_byname=("h2",))
        _value(db, "hub", ["h2"], "routing", "Base", Array(["via_a", "via_b"]))
        db.commit_session("drift payload")

    rep, out = run_import(tmp_path, spine)
    assert not [r for r in rep.residue if "unrepresentable" in r or "unparseable" in r]
    assert any("hub.routing" in row and "chain will repair" in row
               for row in rep.drift_table), rep.drift_table
    got = _rows(_nodal(out), "SELECT count(*) AS n FROM read_parameter('hub', 'routing')")
    assert got == [{"n": 3}]  # 1 scalar + 2 array cells


def test_hostile_source_reports_loud_residue(tmp_path):
    from spinedb_api import DatabaseMapping, Map, TimePattern, to_database

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        # unflattenable value
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="seasonality")
        _value(db, "hub", ["h1"], "seasonality", "Base", TimePattern(["M1-6"], [1.0]))
        # a typeless definition's default cannot be carried
        dv, dt = to_database(4.5)
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="fallback_level", default_value=dv, default_type=dt)
        # ragged value: two leaf shapes in one Map on one entity
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="mixed")
        _value(db, "hub", ["h1"], "mixed", "Base",
               Map(["a", "b", "c"], [1.0, 2.0, Map(["x"], [3.0])]))
        # descriptions with no store slot + an entity group
        db.add_update_item("entity", entity_class_name="widget", entity_byname=("w3",),
                           description="a described widget")
        db.add_update_item("scenario", name="sc_described", description="has text")
        db.add_update_item("entity_group", entity_class_name="widget",
                           group_name="w1", member_name="w2")
        db.commit_session("hostile payload")

    rep, _out = run_import(tmp_path, spine)
    res = "\n".join(rep.residue)
    assert "hub.seasonality" in res and "time_pattern" in res, res
    assert "hub.fallback_level" in res and "not carried" in res, res
    assert "hub.mixed" in res and "ragged" in res, res
    assert "entity widget/w3: description not carried" in res, res
    assert "scenario sc_described: description not carried" in res, res
    assert "entity_group row(s) present, not imported" in res, res
    # the ragged value's largest group still landed (+1 over the clean 9)
    assert rep.counts["values_written"] == 10


def test_case_colliding_names_refuse(tmp_path):
    from spinedb_api import DatabaseMapping

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("entity", entity_class_name="widget", entity_byname=("W1",))
        db.commit_session("case collision")
    with pytest.raises(ImportError_, match="case-colliding"):
        run_import(tmp_path, spine)


def test_unnamed_map_index_gets_a_generic_dim_name(tmp_path):
    """The base profile names an unnamed index ``i1`` (never an empty or
    illegal column name)."""
    from spinedb_api import DatabaseMapping, Map

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="levels")
        _value(db, "hub", ["h1"], "levels", "Base", Map(["lo", "hi"], [1.0, 9.0],
                                                        index_name=""))
        db.commit_session("unnamed index")
    profile = SyntheticProfile()
    names, note = profile.dim_names("hub", "levels", ("label",), ("",), ("lo", "hi"))
    assert (names, note) == (["i1"], None)
    names, _ = profile.dim_names("hub", "x", ("label", "label"), ("t", "t"), ())
    assert names == ["t", "t_1"], "duplicate index names de-duplicated"

    rep, out = run_import(tmp_path, spine)
    assert rep.residue == [], rep.summary()
    vals = _nodal_values(_nodal(out), "hub", "levels")
    assert vals[("h1", "Base")] == ("map", [("hi", 9.0), ("lo", 1.0)])


# ---------------------------------------------------------------------------
# Duplicate keys in source values: keep the last entry, report the problem
# ---------------------------------------------------------------------------


def _add_param_value(spine, cls, entity, param, value, alt="Base"):
    from spinedb_api import DatabaseMapping

    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("parameter_definition", entity_class_name=cls, name=param)
        _value(db, cls, [entity], param, alt, value)
        db.commit_session(f"{cls}.{param}")


def _cells(db, cls, param, rank):
    cols = ", ".join(f"idx{i}_text" for i in range(1, rank + 1))
    rows = _rows(db, f"SELECT {cols}, v_float FROM read_parameter('{cls}', '{param}')")
    return sorted((tuple(r[f"idx{i}_text"] for i in range(1, rank + 1)), r["v_float"])
                  for r in rows)


def test_duplicate_top_level_key_keeps_last_and_warns(tmp_path, caplog):
    from spinedb_api import Map

    spine = make_basic_spine(tmp_path)
    _add_param_value(spine, "hub", "h1", "settings",
                     Map(["a", "b", "a"], [1.0, 2.0, 3.0], index_name="key"))
    with caplog.at_level("WARNING", logger="nodal_spine_import"):
        rep, out = run_import(tmp_path, spine)

    assert _cells(_nodal(out), "hub", "settings", 1) == [(("a",), 3.0), (("b",), 2.0)]
    assert len(rep.duplicate_keys) == 1, rep.summary()
    dup = rep.duplicate_keys[0]
    assert (dup.entity_class, dup.entity, dup.parameter, dup.alternative) == (
        "hub", "h1", "settings", "Base")
    assert dup.path == ()
    assert dup.key == "a"
    assert dup.entries == [(0, "1.0"), (2, "3.0")]
    assert dup.kept == 2
    text = dup.describe()
    assert text == (
        "class 'hub', entity 'h1', parameter 'settings', alternative 'Base': key "
        "'a' appears 2 times at the top level of the value (entry 0: 1.0; entry 2: "
        "3.0); kept the last, entry 2. A map holds one value per key: fix the "
        "source so the key appears once."
    )
    assert "duplicate keys: 1 (last entry kept)" in rep.summary()
    assert text in rep.summary()
    assert any(text in r.getMessage() for r in caplog.records), caplog.text
    # The kept entry is not residue: the value imported.
    assert not [r for r in rep.residue if "settings" in r], rep.residue


def test_duplicate_outer_key_keeps_the_last_whole_sub_map(tmp_path):
    """A repeated key in an outer map keeps the last entry's whole sub-map;
    the earlier sub-map is not merged in (as a ``dict`` built from the map
    would read it)."""
    from spinedb_api import Map

    spine = make_basic_spine(tmp_path)
    _add_param_value(spine, "hub", "h1", "nested", Map(
        ["a", "a", "b"],
        [Map(["x"], [1.0]), Map(["y"], [2.0]), Map(["x"], [5.0])],
    ))
    rep, out = run_import(tmp_path, spine)

    assert _cells(_nodal(out), "hub", "nested", 2) == [
        (("a", "y"), 2.0), (("b", "x"), 5.0)]
    [dup] = rep.duplicate_keys
    assert (dup.path, dup.key, dup.kept) == ((), "a", 1)
    assert dup.entries == [(0, "a map of 1 entries"), (1, "a map of 1 entries")]


def test_duplicate_inner_key_keeps_last_and_names_the_path(tmp_path):
    from spinedb_api import Map

    spine = make_basic_spine(tmp_path)
    _add_param_value(spine, "hub", "h1", "nested", Map(
        ["a", "b"],
        [Map(["x", "y", "x"], [1.0, 2.0, 3.0]), Map(["x"], [5.0])],
    ))
    rep, out = run_import(tmp_path, spine)

    assert _cells(_nodal(out), "hub", "nested", 2) == [
        (("a", "x"), 3.0), (("a", "y"), 2.0), (("b", "x"), 5.0)]
    [dup] = rep.duplicate_keys
    assert (dup.path, dup.key, dup.kept) == (("a",), "x", 2)
    assert "in the map at key path ('a')" in dup.describe()


def test_duplicate_time_stamp_keeps_last_and_warns(tmp_path):
    """spinedb_api writes a time series as a JSON object (so it cannot
    repeat a stamp itself), but it reads the list-of-pairs form too, which
    can: store that form directly."""
    from spinedb_api import DatabaseMapping

    spine = make_basic_spine(tmp_path)
    raw = (b'{"data": [["2030-01-01T00:00", 1.0], ["2030-01-01T01:00", 2.0], '
           b'["2030-01-01T01:00", 4.0]]}')
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("parameter_definition", entity_class_name="hub",
                           name="profile")
        db.add_update_item(
            "parameter_value", entity_class_name="hub", entity_byname=("h1",),
            parameter_definition_name="profile", alternative_name="Base",
            value=raw, type="time_series",
        )
        db.commit_session("raw time series")
    rep, out = run_import(tmp_path, spine)

    vals = _rows(_nodal(out), "SELECT v_float FROM read_parameter('hub', 'profile')")
    assert sorted(r["v_float"] for r in vals) == [1.0, 4.0]
    [dup] = rep.duplicate_keys
    assert dup.kept == 2
    assert "key 2030-01-01T01:00:00 appears 2 times" in dup.describe()


def test_clean_source_reports_no_duplicate_keys(tmp_path):
    rep, _out = run_import(tmp_path, make_basic_spine(tmp_path))
    assert rep.duplicate_keys == []
    assert "duplicate keys: 0" in rep.summary()


# ---------------------------------------------------------------------------
# Atomic import: a failed import leaves no database behind
# ---------------------------------------------------------------------------


def _leftovers(tmp_path, out_name="out.duckdb"):
    return sorted(p.name for p in tmp_path.iterdir()
                  if p.name.startswith(out_name) or p.name.startswith(f".{out_name}"))


def test_failed_write_leaves_no_target_and_no_temporary(tmp_path, monkeypatch):
    """A failure after the vocabulary and structural commits (the values
    write fails) leaves nothing at the target and removes the partial
    build."""
    import nodal_db

    real_init = nodal_db.init

    class FailingValues:
        def __init__(self, db):
            self._db = db

        def __getattr__(self, name):
            return getattr(self._db, name)

        def commit_changes(self, **kw):
            if kw.get("segments"):
                raise nodal_db.ApiError("injected values failure")
            return self._db.commit_changes(**kw)

    monkeypatch.setattr(nodal_db, "init", lambda path, **kw: FailingValues(real_init(path, **kw)))
    spine = make_basic_spine(tmp_path)
    with pytest.raises(nodal_db.ApiError, match="injected"):
        run_import(tmp_path, spine)
    assert _leftovers(tmp_path) == []


def test_successful_import_leaves_only_the_target(tmp_path):
    _rep, out = run_import(tmp_path, make_basic_spine(tmp_path))
    assert _leftovers(tmp_path) == ["out.duckdb"]
    assert _meta(_nodal(out))["template_name"] == TEMPLATE_NAME


def test_existing_target_is_refused_and_left_untouched(tmp_path):
    spine = make_basic_spine(tmp_path)
    target = tmp_path / "out.duckdb"
    target.write_bytes(b"not mine to overwrite")
    with pytest.raises(ImportError_, match="already exists"):
        run_import(tmp_path, spine)
    assert target.read_bytes() == b"not mine to overwrite"
    assert _leftovers(tmp_path) == ["out.duckdb"]


# ---------------------------------------------------------------------------
# Names with leading/trailing whitespace: imported trimmed, with a warning
# ---------------------------------------------------------------------------


def make_padded_spine(tmp_path):
    """The basic source plus one whitespace-padded name of every kind, each
    referenced the way the source refers to it: a 2-D class and entity over
    a padded class and entity, a scenario over a padded alternative, an
    activation row and values on the padded entity, a list binding to the
    padded list."""
    from spinedb_api import DatabaseMapping, Map, to_database

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("alternative", name=" peak")
        db.add_update_item("entity_class", name="gadget ")
        db.add_update_item("entity_class", name="gadget__hub",
                           dimension_name_list=("gadget ", "hub"))
        db.add_update_item("parameter_value_list", name="speeds\t")
        for i, member in enumerate(("slow", "fast")):
            v, t = to_database(member)
            db.add_update_item("list_value", parameter_value_list_name="speeds\t",
                               value=v, type=t, index=i)
        db.add_update_item("parameter_definition", entity_class_name="gadget ",
                           name=" power")
        db.add_update_item("parameter_definition", entity_class_name="gadget ",
                           name="speed", parameter_value_list_name="speeds\t")
        db.add_update_item("parameter_definition", entity_class_name="gadget__hub",
                           name="weight")
        db.add_update_item("entity", entity_class_name="gadget ",
                           entity_byname=("g1 ",))
        db.add_update_item("entity", entity_class_name="gadget__hub",
                           entity_byname=("g1 ", "h1"), name="g1-h1")
        _value(db, "gadget ", ["g1 "], " power", "Base", 3.0)
        _value(db, "gadget ", ["g1 "], " power", " peak", 7.0)
        _value(db, "gadget ", ["g1 "], "speed", "Base", "fast")
        _value(db, "gadget__hub", ["g1 ", "h1"], "weight", " peak",
               Map(["a ", "b"], [1.0, 2.0], index_name="slot"))
        db.add_update_item("scenario", name="sc_peak ")
        db.add_update_item("scenario_alternative", scenario_name="sc_peak ",
                           alternative_name="Base", rank=1)
        db.add_update_item("scenario_alternative", scenario_name="sc_peak ",
                           alternative_name=" peak", rank=2)
        db.add_update_item("entity_alternative", entity_class_name="gadget ",
                           entity_byname=("g1 ",), alternative_name=" peak",
                           active=False)
        db.commit_session("padded names")
    return spine


def test_padded_names_import_trimmed_with_a_warning_each(tmp_path, caplog):
    spine = make_padded_spine(tmp_path)
    with caplog.at_level("WARNING", logger="nodal_spine_import"):
        rep, out = run_import(tmp_path, spine)

    got = sorted((t.kind, t.scope or "", t.original, t.imported_as)
                 for t in rep.trimmed_names)
    assert got == sorted([
        ("alternative", "", " peak", "peak"),
        ("entity_class", "", "gadget ", "gadget"),
        ("value_list", "", "speeds\t", "speeds"),
        ("parameter_definition", "gadget", " power", "power"),
        ("entity", "gadget", "g1 ", "g1"),
        ("scenario", "", "sc_peak ", "sc_peak"),
    ]), rep.summary()
    entity_warning = next(t for t in rep.trimmed_names if t.kind == "entity")
    text = entity_warning.describe()
    assert text == (
        "entity 'g1 ' in class 'gadget' has leading or trailing whitespace; "
        "imported as 'g1'. Names in nodal-db are trimmed: fix the source name "
        "to stop this warning."
    )
    assert "trimmed names: 6 (imported without leading/trailing whitespace)" in (
        rep.summary())
    assert f"  trimmed name: {text}" in rep.summary()
    for t in rep.trimmed_names:
        assert any(t.describe() in r.getMessage() for r in caplog.records), t
    assert rep.residue == [], rep.summary()

    db = _nodal(out)
    assert "peak" in {r["name"] for r in _rows(db, "SELECT name FROM alternative_current")}
    assert {"gadget", "gadget__hub"} <= {
        r["name"] for r in _rows(db, "SELECT name FROM entity_class_current")}
    assert {"g1", "g1-h1"} <= {r["name"] for r in _rows(db, "SELECT name FROM entity_current")}
    assert "sc_peak" in {r["name"] for r in _rows(db, "SELECT name FROM scenario_current")}
    assert "speeds" in {
        r["name"] for r in _rows(db, "SELECT name FROM parameter_value_list_current")}
    assert _declared(db, "gadget", "power"), "parameter declared under its trimmed name"


def test_references_to_padded_names_use_the_trimmed_names(tmp_path):
    _rep, out = run_import(tmp_path, make_padded_spine(tmp_path))
    db = _nodal(out)

    # scenario -> the trimmed alternative, in rank order
    assert _rows(db, (
        "SELECT a.name FROM scenario_alternative_current sa "
        "JOIN scenario_current s ON s.id = sa.scenario_id "
        "JOIN alternative_current a ON a.id = sa.alternative_id "
        "WHERE s.name = 'sc_peak' ORDER BY sa.rank")) == [
            {"name": "Base"}, {"name": "peak"}]
    # values on the trimmed entity, parameter and alternative
    assert _nodal_values(db, "gadget", "power") == {
        ("g1", "Base"): ("scalar", 3.0), ("g1", "peak"): ("scalar", 7.0)}
    assert _nodal_values(db, "gadget", "speed") == {("g1", "Base"): ("scalar", "fast")}
    # the 2-D class over the trimmed class; map labels are data and keep
    # their whitespace
    assert _rows(db, (
        "SELECT c.name FROM entity_class_dimension d "
        "JOIN entity_class_current o ON o.id = d.entity_class_id "
        "JOIN entity_class_current c ON c.id = d.dimension_class_id "
        "WHERE d.valid_to_commit IS NULL AND o.name = 'gadget__hub' "
        "ORDER BY d.position")) == [{"name": "gadget"}, {"name": "hub"}]
    assert _cells(db, "gadget__hub", "weight", 1) == [(("a ",), 1.0), (("b",), 2.0)]
    # the activation row on the trimmed entity and alternative
    assert _rows(db, (
        "SELECT ea.active FROM entity_alternative_current ea "
        "JOIN entity_current e ON e.id = ea.entity_id "
        "JOIN alternative_current a ON a.id = ea.alternative_id "
        "WHERE e.name = 'g1' AND a.name = 'peak'")) == [{"active": False}]
    # the list binding to the trimmed list
    assert _rows(db, (
        "SELECT l.name FROM parameter_definition_current pd "
        "JOIN parameter_value_list_current l ON l.id = pd.parameter_value_list_id "
        "WHERE pd.name = 'speed'")) == [{"name": "speeds"}]


def test_trimming_into_an_existing_name_refuses(tmp_path):
    from spinedb_api import DatabaseMapping

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("entity", entity_class_name="widget",
                           entity_byname=("w1 ",))
        db.add_update_item("alternative", name=" HIGH")
        db.commit_session("collisions")
    with pytest.raises(ImportError_) as err:
        run_import(tmp_path, spine)
    msg = str(err.value)
    assert "would merge distinct source names" in msg
    assert ("entity names 'w1', 'w1 ' in class 'widget' are the same name once "
            "trimmed ('w1')") in msg
    assert ("alternative names 'high', ' HIGH' are the same name once trimmed "
            "(case-insensitively, as nodal-db resolves names)") in msg
    assert _leftovers(tmp_path) == []


def test_name_empty_after_trimming_refuses(tmp_path):
    from spinedb_api import DatabaseMapping

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("parameter_definition", entity_class_name="hub", name="  ")
        db.commit_session("blank name")
    with pytest.raises(ImportError_, match=(
            r"parameter_definition name '  ' in class 'hub' is empty once its "
            r"whitespace is trimmed")):
        run_import(tmp_path, spine)
    assert _leftovers(tmp_path) == []


def test_interior_control_character_still_refuses(tmp_path):
    """Trimming covers leading/trailing whitespace only: the other name
    rules stay errors."""
    from spinedb_api import DatabaseMapping

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        db.add_update_item("alternative", name=" bad\x01name ")
        db.commit_session("control character")
    with pytest.raises(ImportError_, match="free of control characters"):
        run_import(tmp_path, spine)


def test_clean_source_reports_no_trimmed_names(tmp_path):
    rep, _out = run_import(tmp_path, make_basic_spine(tmp_path))
    assert rep.trimmed_names == []
    assert "trimmed names: 0" in rep.summary()


# ---------------------------------------------------------------------------
# The text rule: nodal-db stores names and map keys in Unicode NFC
# ---------------------------------------------------------------------------

E_NFC, E_NFD = "é", "é"


def test_nfd_map_key_imports_in_nfc_and_is_reported(tmp_path):
    """The importer converts nothing itself; the core stores the key in
    NFC and the report counts it."""
    from spinedb_api import Map

    spine = make_basic_spine(tmp_path)
    _add_param_value(spine, "hub", "h1", "levels", Map([E_NFD, "b"], [1.0, 2.0]))
    rep, out = run_import(tmp_path, spine)
    vals = _nodal_values(_nodal(out), "hub", "levels")
    assert vals[("h1", "Base")] == ("map", [("b", 2.0), (E_NFC, 1.0)])
    normalized = [w for w in rep.write_warnings if w["kind"] == "text_normalized"]
    assert [(w["what"], w["written"], w["stored"]) for w in normalized] == [
        ("label", E_NFD, E_NFC)
    ]
    assert "normalized identifiers: 1 (stored in Unicode NFC)" in rep.summary()


def test_names_differing_only_in_normalization_refuse(tmp_path):
    from spinedb_api import DatabaseMapping

    spine = make_basic_spine(tmp_path)
    with DatabaseMapping(_url(spine), upgrade=False) as db:
        for name in (f"w{E_NFC}", f"w{E_NFD}"):
            db.add_update_item("entity", entity_class_name="widget", entity_byname=(name,))
        db.commit_session("normalization collision")
    with pytest.raises(ImportError_, match="differ only in Unicode normalization"):
        run_import(tmp_path, spine)
