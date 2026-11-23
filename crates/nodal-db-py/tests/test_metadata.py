"""The metadata channel through the skin: `metadata=` op dicts, the
`value_metadata=` shorthand on `commit` and `write_parameter`, the
report's `metadata` / `value_links`, the typed `ValueMetadataUnstated`, and
the property reads. Marshaling only: the rules are the core's."""

import pyarrow as pa
import pytest

import nodal_db


@pytest.fixture
def db(tmp_path):
    return nodal_db.init(str(tmp_path / "m.duckdb"), default_alternative="Base")


def batch(entities, values):
    return pa.record_batch(
        {"entity": pa.array(entities, pa.string()), "value": pa.array(values, pa.float64())}
    )


VALUE_A = {"class": "node", "parameter": "p", "alternative": "Base", "entity": "a"}


def one(db, sql):
    return db.query(sql).read_all().to_pylist()[0]


def annotated(db):
    db.write_parameter("node", "p", "Base", batch(["a", "b"], [1.0, 2.0]), user="t")
    rep = db.commit(
        metadata=[
            {"op": "put_source", "source": "IEA",
             "fields": {"title": "World Energy Outlook", "issued": "2024-10-16"}},
            {"op": "put_license", "license": "CC-BY-4.0", "fields": {"spdx": True}},
            {"op": "set_source_licenses", "source": "IEA", "licenses": ["CC-BY-4.0"]},
            {"op": "put_role", "role": "primary_source"},
            {"op": "link", "subject": {"dataset": True}, "target": {"source": "IEA"},
             "role": "primary_source"},
            {"op": "link_value", "value": VALUE_A, "target": {"source": "IEA"}},
            {"op": "create_pair", "name": "keyword", "value": "grid"},
            {"op": "link", "subject": {"entity": ["node", "a"]},
             "target": {"pair": ["keyword", "grid"]}},
            {"op": "set_property", "key": "title", "value": "Nordic grid"},
        ],
        user="t",
    )
    return rep


def test_ops_and_report(db):
    rep = annotated(db)
    assert rep["commit_id"] > 0
    effects = [m["effect"] for m in rep["metadata"]]
    assert effects == ["created", "created", "updated"] + ["created"] * 6
    assert rep["metadata"][0]["kind"] == "source"
    assert rep["value_links"]["set"] == 1
    assert one(db, "SELECT target_name FROM value_metadata_named")["target_name"] == "IEA"
    assert one(db, "SELECT license FROM value_licenses")["license"] == "CC-BY-4.0"
    assert db.database_property("TITLE") == "Nordic grid"
    owners = {p["key"]: p["owner"] for p in db.database_properties()}
    assert owners["title"] == "user" and owners["schema_version"] == "core"
    # A repeat of the same items changes nothing: no commit.
    again = db.commit(
        metadata=[{"op": "put_source", "source": "IEA",
                   "fields": {"title": "World Energy Outlook", "issued": "2024-10-16"}}],
        user="t",
    )
    assert again["commit_id"] == 0
    assert again["metadata"][0]["effect"] == "no_op"
    assert [m["item_index"] for m in rep["metadata"]] == list(range(9))
    assert "index" not in rep["metadata"][0]


def test_effect_tags_are_one_vocabulary(db):
    """Structural and metadata outcomes spell every effect the same way
    (the documented set), "no_op" included."""
    documented = {"created", "renamed", "updated", "no_op", "cleared", "deleted",
                  "restricted", "dropped_by_delete"}
    rep = db.commit(
        structural=[{"op": "create_alternative", "name": "alt"},
                    {"op": "create_alternative", "name": "alt"}],
        metadata=[{"op": "put_role", "role": "r"}],
        user="t",
    )
    again = db.commit(metadata=[{"op": "put_role", "role": "r"}], user="t")
    s = [o["effect"] for o in rep["structural"]]
    m = [o["effect"] for o in rep["metadata"] + again["metadata"]]
    assert s == ["created", "no_op"] and m == ["created", "no_op"]
    assert set(s) | set(m) <= documented


def test_unstated_rewrite_is_a_typed_error_and_the_shorthand_states_it(db):
    annotated(db)
    with pytest.raises(nodal_db.ValueMetadataUnstated) as ei:
        db.write_parameter("node", "p", "Base", batch(["a"], [5.0]), user="t")
    assert ei.value.count == 1
    assert ei.value.sample == ["node.a / p / Base"]
    assert isinstance(ei.value, nodal_db.ApiError)
    rep = db.write_parameter("node", "p", "Base", batch(["a"], [5.0]), user="t",
                             value_metadata="keep")
    assert rep["value_links"]["kept"] == 1
    rep = db.commit(
        segments=[{"class": "node", "parameter": "p", "alternative": "Base",
                   "batch": batch(["a"], [6.0])}],
        value_metadata={"set": [{"source": "IEA"}, {"pair": ["keyword", "grid"]}]},
        user="t",
    )
    assert rep["value_links"]["kept"] == 1 and rep["value_links"]["set"] == 1
    rep = db.commit(
        segments=[{"class": "node", "parameter": "p", "alternative": "Base",
                   "batch": batch(["a"], [7.0])}],
        metadata=[{"op": "value_metadata", "scope": VALUE_A, "action": "clear"}],
        user="t",
    )
    assert rep["value_links"]["cleared"] == 2


def test_deletes_and_refusals(db):
    annotated(db)
    rep = db.commit(metadata=[{"op": "delete_source", "source": "IEA"}], user="t")
    out = rep["metadata"][0]
    assert out["effect"] == "restricted" and out["blockers"]
    rep = db.commit(
        metadata=[{"op": "delete_source", "source": "IEA", "cascade": True}], user="t")
    assert rep["metadata"][0]["effect"] == "deleted"
    assert rep["metadata"][0]["links_closed"] >= 2
    with pytest.raises(nodal_db.ApiError, match="PutRole"):
        db.commit(
            metadata=[{"op": "link", "subject": {"dataset": True},
                       "target": {"pair": ["keyword", "grid"]}, "role": "nope"}],
            user="t")
    with pytest.raises(nodal_db.ApiError, match="unknown op"):
        db.commit(metadata=[{"op": "frobnicate"}], user="t")
    with pytest.raises(nodal_db.ApiError, match="core-owned"):
        db.commit(metadata=[{"op": "set_property", "key": "schema_version",
                                     "value": "x"}], user="t")
    # The `nodal:` namespace is reserved for future core keys.
    with pytest.raises(nodal_db.ApiError, match="core-owned"):
        db.commit(metadata=[{"op": "set_property", "key": "Nodal:anything",
                                     "value": "x"}], user="t")


def test_created_by_and_refinement_classes(db):
    created_by = db.database_property("created_by")
    assert created_by.startswith("nodal-db-core " + nodal_db.__version__), created_by
    db.ensure_class("plant", user="t")
    db.ensure_class("gen", user="t")
    db.ensure_class("plant_gen", ["plant", "gen"], user="t")
    db.set_class_refinement("plant_gen", True, user="t")
    cid = one(db, "SELECT id FROM entity_class_current WHERE name = 'plant_gen'")["id"]
    assert db.database_property("refinement_classes") == f"[{cid}]"
    with pytest.raises(nodal_db.ApiError, match="exactly two"):
        db.set_class_refinement("plant", True, user="t")
    db.set_class_refinement("plant_gen", False, user="t")
    assert db.database_property("refinement_classes") is None


def test_misspelled_keys_are_refused(db):
    annotated(db)
    bad = [
        {"op": "put_source", "source": "IEA", "feilds": {"title": "x"}},
        {"op": "put_source", "source": "IEA", "fields": {"titel": "x"}},
        {"op": "put_license", "license": "CC-BY-4.0", "fields": {"sdpx": True}},
        {"op": "link", "subject": {"dataset": True}, "target": {"source": "IEA"},
         "rol": "primary_source"},
        {"op": "link", "subject": {"dataset": True, "class": "node"},
         "target": {"source": "IEA"}},
        {"op": "link_value", "value": dict(VALUE_A, entty="a"), "target": {"source": "IEA"}},
        {"op": "value_metadata", "scope": "save",
         "action": {"set": [{"source": "IEA", "rol": "x"}]}},
        {"op": "set_property", "key": "title"},
        {"op": "link", "subject": {"dataset": True},
         "target": {"source": "IEA", "pair": ["keyword", "grid"]}},
        {"op": "value_metadata", "scope": "save",
         "action": {"set": [{"source": "IEA"}], "keep": True}},
    ]
    for item in bad:
        with pytest.raises(nodal_db.ApiError):
            db.commit(metadata=[item], user="t")
    assert db.database_property("title") == "Nordic grid"
    rep = db.commit(metadata=[{"op": "set_property", "key": "title", "value": None}],
                            user="t")
    assert rep["metadata"][0]["effect"] == "cleared"
