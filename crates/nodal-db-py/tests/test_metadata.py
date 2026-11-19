"""The metadata channel through the skin: `metadata=` op dicts, the
`value_metadata=` shorthand on `commit_changes` and `write_parameter`, the
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
    rep = db.commit_changes(
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
    again = db.commit_changes(
        metadata=[{"op": "put_source", "source": "IEA",
                   "fields": {"title": "World Energy Outlook", "issued": "2024-10-16"}}],
        user="t",
    )
    assert again["commit_id"] == 0
    assert again["metadata"][0]["effect"] == "noop"


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
    rep = db.commit_changes(
        segments=[{"class": "node", "parameter": "p", "alternative": "Base",
                   "batch": batch(["a"], [6.0])}],
        value_metadata={"set": [{"source": "IEA"}, {"pair": ["keyword", "grid"]}]},
        user="t",
    )
    assert rep["value_links"]["kept"] == 1 and rep["value_links"]["set"] == 1
    rep = db.commit_changes(
        segments=[{"class": "node", "parameter": "p", "alternative": "Base",
                   "batch": batch(["a"], [7.0])}],
        metadata=[{"op": "value_metadata", "scope": VALUE_A, "action": "clear"}],
        user="t",
    )
    assert rep["value_links"]["cleared"] == 2


def test_deletes_and_refusals(db):
    annotated(db)
    rep = db.commit_changes(metadata=[{"op": "delete_source", "source": "IEA"}], user="t")
    out = rep["metadata"][0]
    assert out["effect"] == "restricted" and out["blockers"]
    rep = db.commit_changes(
        metadata=[{"op": "delete_source", "source": "IEA", "cascade": True}], user="t")
    assert rep["metadata"][0]["effect"] == "deleted"
    assert rep["metadata"][0]["links_closed"] >= 2
    with pytest.raises(nodal_db.ApiError, match="PutRole"):
        db.commit_changes(
            metadata=[{"op": "link", "subject": {"dataset": True},
                       "target": {"pair": ["keyword", "grid"]}, "role": "nope"}],
            user="t")
    with pytest.raises(nodal_db.ApiError, match="unknown op"):
        db.commit_changes(metadata=[{"op": "frobnicate"}], user="t")
    with pytest.raises(nodal_db.ApiError, match="core-owned"):
        db.commit_changes(metadata=[{"op": "set_property", "key": "schema_version",
                                     "value": "x"}], user="t")


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
    ]
    for item in bad:
        with pytest.raises(nodal_db.ApiError):
            db.commit_changes(metadata=[item], user="t")
    assert db.database_property("title") == "Nordic grid"
    rep = db.commit_changes(metadata=[{"op": "set_property", "key": "title", "value": None}],
                            user="t")
    assert rep["metadata"][0]["effect"] == "cleared"
