"""Pytest suite for the thin PyO3 skin (crates/nodal-db-py).

Run with the maturin-built module importable (``maturin develop`` into the
active venv). The tests exercise the skin as marshaling only: every behavior
asserted here is the CORE's behavior observed through Python.

The centerpiece is ``test_run_program_writes_through_the_skin``: the D13
acceptance seed — a template migration chain whose ``run:`` step is a plain
Python function that writes back through the skin (reverse-direction PyO3,
reentrancy included).
"""

import os
import subprocess
import sys

import pyarrow as pa
import pytest

import nodal_db


@pytest.fixture
def db(tmp_path):
    return nodal_db.init(str(tmp_path / "t.duckdb"), default_alternative="Base")


def scalar_batch(entities, values, value_type=None):
    value = pa.array(values, value_type) if value_type is not None else pa.array(values)
    return pa.record_batch({"entity": pa.array(entities, pa.string()), "value": value})


# ---------------------------------------------------------------------------
# open / init / identity
# ---------------------------------------------------------------------------


def test_open_missing_file_is_typed_error(tmp_path):
    with pytest.raises(nodal_db.ApiError):
        nodal_db.open(str(tmp_path / "nope.duckdb"))


def test_identity_is_stable_uuid(db):
    ident = db.db_identity()
    assert len(ident) == 36
    assert ident == nodal_db.open(db.path).db_identity()


# ---------------------------------------------------------------------------
# write / read round trip, Arrow zero-copy, dtype fidelity
# ---------------------------------------------------------------------------


def test_scalar_roundtrip_dtype_fidelity(db):
    rep = db.write_parameter(
        "unit", "capacity", "Base", scalar_batch(["u1", "u2"], [1.5, 2.5], pa.float64()),
        user="tester",
    )
    assert rep["values_written"] == 2
    assert rep["tiers"] == ["scalar"]
    assert {(w["kind"], w["object_type"]) for w in rep["warnings"]} == {
        ("auto_created", "entity_class"),
        ("auto_created", "parameter_definition"),
        ("auto_created", "entity"),
    }

    rb = db.read_parameter("unit", "capacity", "Base")
    assert isinstance(rb, pa.RecordBatch)
    assert rb.num_rows == 2
    assert rb.schema.field("entity").type == pa.string()
    # dtype fidelity: the float measure comes back as float64, not a string
    # rendering or a downcast.
    assert rb.schema.field("v_float").type == pa.float64()
    got = dict(zip(rb.column("entity").to_pylist(), rb.column("v_float").to_pylist()))
    assert got == {"u1": 1.5, "u2": 2.5}


def test_indexed_timestamp_roundtrip(db):
    ts = pa.array(
        [1_700_000_000_000_000, 1_700_003_600_000_000, 1_700_007_200_000_000],
        pa.timestamp("us"),
    )
    batch = pa.record_batch(
        {
            "entity": pa.array(["u1", "u1", "u1"], pa.string()),
            "time": ts,
            "value": pa.array([1.0, 2.0, 3.0], pa.float64()),
        }
    )
    rep = db.write_parameter("unit", "flow", "Base", batch, user="tester")
    assert rep["values_written"] == 1

    rb = db.read_parameter("unit", "flow", "Base")
    assert rb.num_rows == 3
    # dtype fidelity through write -> canon -> storage -> read: the datetime
    # index column (position 1; the macro exposes idx slots for all four
    # possible positions) is timestamp[us] again, values identical. (The
    # period end idx1_end_ts is timestamp[us] too, and NULL here.)
    (dt_field,) = [
        f
        for f in rb.schema
        if f.name.startswith("idx1") and not f.name.endswith("_end_ts")
        and f.type == pa.timestamp("us")
    ]
    assert rb.column("idx1_end_ts").null_count == 3
    assert rb.column(dt_field.name).to_pylist() == ts.to_pylist()
    assert rb.column("v_float").to_pylist() == [1.0, 2.0, 3.0]


def _series(entity, values):
    n = len(values)
    return pa.record_batch(
        {
            "entity": pa.array([entity] * n, pa.string()),
            "time": pa.array(
                [1_700_000_000_000_000 + i * 3_600_000_000 for i in range(n)],
                pa.timestamp("us"),
            ),
            "value": pa.array(values, pa.float64()),
        }
    )


def _sorted_rows(rb):
    return sorted(rb.to_pylist(), key=repr)


def test_read_values_matches_filtered_read_parameter(db):
    # fast_point_reads_PLAN D4 (a'): the skin mirrors api::read_values.
    for alt, k in [("Base", 1.0), ("high", 2.0), ("low", 3.0)]:
        for e in ["u1", "u2", "u3"]:
            db.write_parameter("unit", "flow", alt, _series(e, [k, k + 1, k + 2]), user="t")
    full = {
        alt: db.read_parameter("unit", "flow", alt) for alt in ["Base", "high", "low"]
    }

    rb = db.read_values("unit", "flow", ["U1", "u3"], ["base", "LOW"])
    assert rb.schema == full["Base"].schema
    want = [
        r for alt in ["Base", "low"] for r in full[alt].to_pylist() if r["entity"] in ("u1", "u3")
    ]
    assert _sorted_rows(rb) == sorted(want, key=repr)

    # alternatives=None (the default): one entity in every alternative.
    rb = db.read_values("unit", "flow", ["u2"])
    want = [r for alt in full for r in full[alt].to_pylist() if r["entity"] == "u2"]
    assert rb.num_rows == 9
    assert _sorted_rows(rb) == sorted(want, key=repr)

    # Empty lists and misses read nothing (no error), with the same columns.
    for args in [(["u1"], []), ([], None), (["nope"], None), (["u1"], ["nope"])]:
        rb = db.read_values("unit", "flow", *args)
        assert rb.num_rows == 0 and rb.schema == full["Base"].schema
    assert db.read_values("nope", "flow", ["u1"]).num_rows == 0

    # A scalar parameter reads through the same call.
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1", "u2"], [5.0, 6.0]), user="t")
    rb = db.read_values("unit", "capacity", ["u2"], ["Base"])
    assert rb.column("v_float").to_pylist() == [6.0]
    assert rb.column("tier").to_pylist() == ["scalar"]

    # A bare string is not a list of names.
    with pytest.raises(TypeError):
        db.read_values("unit", "flow", "u1")


def test_table_input_via_capsule_stream(db):
    # pyarrow.Table travels the __arrow_c_stream__ path (multiple batches in
    # one Segment).
    table = pa.table({"entity": ["a", "b"], "value": [1.0, 2.0]})
    rep = db.write_parameter("node", "demand", "Base", table, user="tester")
    assert rep["values_written"] == 2


def test_identical_write_is_unchanged_and_commits_nothing(db):
    # skip_unchanged_writes_PLAN D1/D2: an identical write_parameter writes
    # no row and makes no commit; the report says so.
    first = db.write_parameter(
        "unit", "capacity", "Base", scalar_batch(["u1"], [1.5]), user="t"
    )
    assert first["commit_id"] > 0 and first["values_unchanged"] == 0
    head = db.head_commit()
    rep = db.write_parameter(
        "unit", "capacity", "Base", scalar_batch(["u1"], [1.5]), user="t"
    )
    assert rep["commit_id"] == 0
    assert (rep["values_written"], rep["values_unchanged"]) == (0, 1)
    assert rep["tiers"] == []
    assert db.head_commit() == head
    # A delete that finds nothing commits nothing either.
    db.delete_values([("unit", "capacity", "Base", "u1")], user="t")
    rep = db.delete_values([("unit", "capacity", "Base", "u1")], user="t")
    assert rep["commit_id"] == 0
    # A write and a delete of one value in one call is refused up front
    # (D9), as the typed `WriteAndDeleteSameKey` (an `ApiError` subclass,
    # so `except ApiError` still catches it) carrying the key.
    with pytest.raises(nodal_db.WriteAndDeleteSameKey) as ei:
        db.commit_changes(
            segments=[{"class": "unit", "parameter": "capacity", "alternative": "Base",
                       "batch": scalar_batch(["u1"], [2.0])}],
            deletes=[("unit", "capacity", "Base", "u1")],
            user="t",
        )
    assert isinstance(ei.value, nodal_db.ApiError)
    assert (ei.value.key_index, ei.value.class_name, ei.value.parameter) == (
        0, "unit", "capacity",
    )
    assert (ei.value.alternative, ei.value.entity) == ("Base", "u1")
    assert ei.value.resolved is None  # the up-front name check, not resolved ids


def test_query_returns_arrow_reader(db):
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="t")
    reader = db.query("SELECT count(*) AS n FROM entity")
    assert isinstance(reader, pa.RecordBatchReader)
    t = reader.read_all()
    assert t.column("n").to_pylist() == [1]
    # The query connection is read-only: a write through it must fail.
    with pytest.raises(nodal_db.DatabaseError):
        db.query("INSERT INTO commit (user, message) VALUES ('x', 'y')")


def test_commit_changes_write_plus_object_delete_is_covered_write(db):
    # A value write and an object delete that cascades over it in one call
    # (D10) is refused up front as the typed `CoveredWrite` (an `ApiError`
    # subclass), carrying the covering object-delete key.
    with pytest.raises(nodal_db.CoveredWrite) as ei:
        db.commit_changes(
            segments=[{"class": "unit", "parameter": "capacity", "alternative": "Base",
                       "batch": scalar_batch(["u1"], [1.0])}],
            object_deletes=[{"kind": "alternative", "name": "Base", "cascade": True}],
            user="t",
        )
    assert isinstance(ei.value, nodal_db.ApiError)
    assert (ei.value.key_index, ei.value.kind, ei.value.target) == (0, "alternative", "Base")
    assert ei.value.class_name is None
    assert ei.value.resolved is None  # the up-front name check, not resolved ids


def test_commit_changes_write_plus_delete_one_commit(db):
    db.write_parameter(
        "unit", "capacity", "Base", scalar_batch(["u1", "u2"], [1.0, 2.0]), user="t"
    )
    rep = db.commit_changes(
        segments=[
            {
                "class": "unit",
                "parameter": "capacity",
                "alternative": "Base",
                "batch": scalar_batch(["u3"], [3.0]),
            }
        ],
        deletes=[("unit", "capacity", "Base", "u1")],
        user="t",
    )
    rb = db.read_parameter("unit", "capacity", "Base")
    assert sorted(rb.column("entity").to_pylist()) == ["u2", "u3"]
    assert rep["rows_closed"] >= 1


def test_commit_changes_covered_writes_delete_wins(db):
    # WriteOptions::covered_writes: refused by default, dropped with a
    # warning under "delete_wins"; the uncovered write proceeds.
    db.write_parameter(
        "unit", "capacity", "Base", scalar_batch(["u1", "u2"], [1.0, 2.0]), user="t"
    )
    segments = [
        {
            "class": "unit",
            "parameter": "capacity",
            "alternative": "Base",
            "batch": scalar_batch(["u1", "u2"], [5.0, 6.0]),
        }
    ]
    deletes = [("unit", "capacity", "Base", "U1")]
    with pytest.raises(nodal_db.ApiError):
        db.commit_changes(segments=segments, deletes=deletes, user="t")
    with pytest.raises(nodal_db.ApiError):
        db.commit_changes(
            segments=segments, deletes=deletes, user="t", covered_writes="refuse"
        )
    with pytest.raises(ValueError):
        db.commit_changes(
            segments=segments, deletes=deletes, user="t", covered_writes="nope"
        )
    rep = db.commit_changes(
        segments=segments, deletes=deletes, user="t", covered_writes="delete_wins"
    )
    assert rep["commit_id"] > 0
    assert rep["warnings"] == [
        {
            "kind": "dropped_by_delete",
            "segment_index": 0,
            "by": "deletes",
            "key_index": 0,
            "object_type": "value",
            "name": "unit:capacity/Base/u1",
            "id": 0,
        }
    ]
    rb = db.read_parameter("unit", "capacity", "Base")
    assert rb.column("entity").to_pylist() == ["u2"]
    assert rb.column("v_float").to_pylist() == [6.0]
    # An object delete that finds nothing beside a write it covers: the
    # write is dropped and nothing is committed (no entity auto-created).
    head = db.head_commit()
    rep = db.commit_changes(
        segments=[
            {
                "class": "unit",
                "parameter": "capacity",
                "alternative": "Base",
                "batch": scalar_batch(["u9"], [9.0]),
            }
        ],
        object_deletes=[{"kind": "entity", "class": "unit", "name": "u9", "cascade": True}],
        user="t",
        covered_writes="delete_wins",
    )
    assert rep["commit_id"] == 0
    assert [w["by"] for w in rep["warnings"]] == ["object_deletes"]
    assert db.head_commit() == head


def test_delete_wins_refuses_when_the_delete_does_not_take_effect(db):
    # A cascade=False delete blocked by the entity's value does not take
    # effect: dropping the write would lose it, so the call is refused.
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="t")
    head = db.head_commit()
    with pytest.raises(nodal_db.ApiError, match="did not take effect"):
        db.commit_changes(
            segments=[{"class": "unit", "parameter": "capacity", "alternative": "Base",
                       "batch": scalar_batch(["u1"], [2.0])}],
            object_deletes=[{"kind": "entity", "class": "unit", "name": "u1",
                             "cascade": False}],
            user="t",
            covered_writes="delete_wins",
        )
    assert db.head_commit() == head


def test_object_delete_preview_and_commit(db):
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="t")
    key = {"kind": "entity", "class": "unit", "name": "u1"}
    (preview,) = db.preview_object_deletes([key])
    assert preview["found"] and preview["cascade"]["total_rows"] >= 1
    rep = db.commit_changes(object_deletes=[dict(key, cascade=True)], user="t")
    (outcome,) = rep["object_deletes"]
    assert outcome["status"] == "deleted"
    assert db.read_parameter("unit", "capacity", "Base").num_rows == 0


# ---------------------------------------------------------------------------
# error mapping
# ---------------------------------------------------------------------------


def test_exception_hierarchy(db):
    for exc in (
        nodal_db.Busy,
        nodal_db.Conflict,
        nodal_db.ApiError,
        nodal_db.VersionTooOld,
        nodal_db.SchemaError,
        nodal_db.DatabaseError,
        nodal_db.CanonError,
        nodal_db.ArrowError,
        nodal_db.InternalError,
    ):
        assert issubclass(exc, nodal_db.NodalDbError)
    assert issubclass(nodal_db.NodalDbError, Exception)


def test_api_errors_are_typed(db):
    with pytest.raises(nodal_db.ApiError, match="user must be non-empty"):
        db.write_parameter("c", "p", "a", scalar_batch(["e"], [1.0]), user="  ")
    # A one-column batch violates the batch contract.
    bad = pa.record_batch({"entity": pa.array(["e"], pa.string())})
    with pytest.raises(nodal_db.ApiError, match="entity column and a value column"):
        db.write_parameter("c", "p", "a", bad, user="t")
    with pytest.raises(nodal_db.ApiError, match="nothing to do"):
        db.commit_changes(user="t")


def test_busy_on_externally_locked_file(db):
    # This test proves the taxonomy's headline claim (Busy is a typed,
    # surfaced outcome) — it needs the python 'duckdb' package, DECLARED as
    # the [test] extra (pyproject.toml): pip install 'nodal-db[test]'.
    # The skip below is a fallback for stripped-down envs, and it is loud.
    pytest.importorskip(
        "duckdb",
        reason="python 'duckdb' package missing — install the declared test "
        "deps (pip install 'nodal-db[test]'); without it the Busy error "
        "mapping (a headline claim of the exception taxonomy) is UNTESTED",
    )
    # Hold a read-write DuckDB connection from ANOTHER process (DuckDB's file
    # lock is cross-process; in-process opens are serialized by the core's
    # path lock instead). The skin must surface DbError::Busy as
    # nodal_db.Busy after the core's bounded backoff, never hang or segfault.
    code = (
        "import duckdb, sys, time\n"
        "con = duckdb.connect(sys.argv[1])\n"
        "print('locked', flush=True)\n"
        "time.sleep(30)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code, db.path], stdout=subprocess.PIPE
    )
    try:
        assert proc.stdout.readline().strip() == b"locked"
        with pytest.raises(nodal_db.Busy):
            db.db_identity()
    finally:
        proc.kill()
        proc.wait()


def test_init_creates_native_storage_format(db):
    # nodal_db.init must create DuckDB's native 2.0 storage format (a
    # 1.5-format file logs the whole list column to the WAL on every array
    # write). Reading the tag needs a duckdb >= 2.0 client: the [test] extra.
    duckdb = pytest.importorskip(
        "duckdb",
        reason="python 'duckdb' package missing — install the declared test "
        "deps (pip install 'nodal-db[test]')",
    )
    con = duckdb.connect(db.path, read_only=True)
    try:
        (tag,) = con.execute(
            "SELECT tags['storage_version'] FROM duckdb_databases() "
            "WHERE database_name = current_database()"
        ).fetchone()
    finally:
        con.close()
    assert tag == "v2.0.0+"


# ---------------------------------------------------------------------------
# template: load / plan / apply through the skin, and the D13 centerpiece
# ---------------------------------------------------------------------------

TEMPLATE_V1 = """\
template: {name: pyskin, version: "1"}
classes:
  - name: unit
    parameters:
      - name: capacity
        types: [{kind: float}]
"""

TEMPLATE_V2 = """\
template: {name: pyskin, version: "2"}
classes:
  - name: unit
    parameters:
      - name: capacity
        types: [{kind: float}]
migrations:
  - from: "1"
    to: "2"
    steps:
      - run: backfill
"""


def write_template(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(text)
    return str(p)


def meta(db, key):
    t = db.query(
        f"SELECT value FROM metadata WHERE lower(name) = '{key}' AND valid_to_commit IS NULL"
    ).read_all()
    vals = t.column("value").to_pylist()
    return vals[0] if vals else None


def test_template_load_plan_apply(db, tmp_path):
    v1 = write_template(tmp_path, "v1.yaml", TEMPLATE_V1)
    plan = db.template_plan(v1)
    assert plan["template_name"] == "pyskin"
    assert not plan["blocked"]
    assert plan["actions"]  # the fresh load has work to do
    outcome = db.template_apply(v1, user="tester", expect_commit=plan["db_commit"])
    assert outcome["converged"] and not outcome["no_op"]
    assert meta(db, "template_name") == "pyskin"
    assert meta(db, "template_version") == "1"
    # Idempotent re-apply short-circuits.
    again = db.template_apply(v1, user="tester")
    assert again["no_op"]


def test_run_program_writes_through_the_skin(db, tmp_path):
    """The D13 centerpiece: a run: migration program is a plain Python
    callable that writes back through the skin while the core's apply is on
    the Rust stack — reverse-direction PyO3 with reentrant API use."""
    db.template_apply(write_template(tmp_path, "v1.yaml", TEMPLATE_V1), user="tester")

    calls = []

    def backfill(db_path, user):
        calls.append((db_path, user))
        # Reentrancy: open the SAME database through the skin, from inside a
        # core callback, and write through the one write path.
        inner = nodal_db.open(db_path)
        inner.write_parameter(
            "unit", "capacity", "Base", scalar_batch(["u1"], [42.0]), user=user
        )

    v2 = write_template(tmp_path, "v2.yaml", TEMPLATE_V2)
    outcome = db.template_apply(v2, user="tester", programs={"backfill": backfill})

    assert calls == [(db.path, "tester")]
    (run,) = outcome["run_programs"]
    assert run["program"] == "backfill"
    assert len(run["commits"]) == 1  # the program's own write commit
    assert outcome["converged"]
    assert meta(db, "template_version") == "2"
    rb = db.read_parameter("unit", "capacity", "Base")
    assert rb.column("v_float").to_pylist() == [42.0]


def test_run_program_missing_registration_is_loud(db, tmp_path):
    db.template_apply(write_template(tmp_path, "v1.yaml", TEMPLATE_V1), user="tester")
    v2 = write_template(tmp_path, "v2.yaml", TEMPLATE_V2)
    with pytest.raises(nodal_db.ApiError, match="not registered"):
        db.template_apply(v2, user="tester")


def test_run_program_exception_propagates_unwrapped(db, tmp_path):
    db.template_apply(write_template(tmp_path, "v1.yaml", TEMPLATE_V1), user="tester")

    class Boom(RuntimeError):
        pass

    def bad(db_path, user):
        raise Boom("original python exception")

    v2 = write_template(tmp_path, "v2.yaml", TEMPLATE_V2)
    with pytest.raises(Boom, match="original python exception"):
        db.template_apply(v2, user="tester", programs={"backfill": bad})
    # The chain aborted before the version stamp.
    assert meta(db, "template_version") == "1"


def test_run_program_nested_self_apply_refused(db, tmp_path):
    """A run: program re-applying its own in-flight template would re-execute
    itself (unbounded recursion); the core's in-flight guard refuses the
    nested apply with a typed error (README reentrancy contract, rule 5)."""
    db.template_apply(write_template(tmp_path, "v1.yaml", TEMPLATE_V1), user="tester")
    v2 = write_template(tmp_path, "v2.yaml", TEMPLATE_V2)

    def recurse(db_path, user):
        nodal_db.open(db_path).template_apply(
            v2, user=user, programs={"backfill": recurse}
        )

    with pytest.raises(nodal_db.ApiError, match="already in flight"):
        db.template_apply(v2, user="tester", programs={"backfill": recurse})
    # The chain aborted before the version stamp; the DB is not wedged: a
    # sane program afterwards completes the migration.
    assert meta(db, "template_version") == "1"
    ok = db.template_apply(
        v2,
        user="tester",
        programs={
            "backfill": lambda p, u: nodal_db.open(p).write_parameter(
                "unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user=u
            )
        },
    )
    assert ok["converged"] and meta(db, "template_version") == "2"


def test_template_apply_stale_plan_refused(db, tmp_path):
    v1 = write_template(tmp_path, "v1.yaml", TEMPLATE_V1)
    plan = db.template_plan(v1)
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u9"], [9.0]), user="t")
    # `Stale` is an ApiError subclass, so `except ApiError` code keeps working.
    with pytest.raises(nodal_db.ApiError, match="stale") as ei:
        db.template_apply(v1, user="tester", expect_commit=plan["db_commit"])
    assert isinstance(ei.value, nodal_db.Stale)
    assert ei.value.expected == plan["db_commit"]
    assert ei.value.found == db.head_commit()


def test_other_schema_version_is_schema_mismatch(db):
    # A file of another schema version: compaction refuses it with the typed
    # SchemaMismatch (an ApiError subclass) carrying both versions.
    duckdb = pytest.importorskip(
        "duckdb",
        reason="python 'duckdb' package missing — install the declared test "
        "deps (pip install 'nodal-db[test]')",
    )
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="t")
    con = duckdb.connect(db.path)
    try:
        (expected,) = con.execute(
            "SELECT value FROM metadata "
            "WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL"
        ).fetchone()
        con.execute(
            "UPDATE metadata SET value = '0.3-test' "
            "WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL"
        )
    finally:
        con.close()
    with pytest.raises(nodal_db.ApiError, match="schema_version") as ei:
        db.compact()
    assert isinstance(ei.value, nodal_db.SchemaMismatch)
    assert (ei.value.found, ei.value.expected) == ("0.3-test", expected)


# ---------------------------------------------------------------------------
# expect_commit / Stale / head_commit
# ---------------------------------------------------------------------------


def test_expect_commit_at_head_commits(db):
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="t")
    head = db.head_commit()
    rep = db.write_parameter(
        "unit", "capacity", "Base", scalar_batch(["u1"], [2.0]), user="t", expect_commit=head
    )
    assert rep["commit_id"] > head
    assert db.head_commit() == rep["commit_id"]


def test_expect_commit_stale_raises_and_writes_nothing(db):
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="t")
    loaded = db.head_commit()
    other = db.write_parameter("unit", "capacity", "Base", scalar_batch(["u2"], [5.0]), user="x")
    with pytest.raises(nodal_db.Stale) as ei:
        db.write_parameter(
            "unit", "capacity", "Base", scalar_batch(["u1"], [2.0]), user="t", expect_commit=loaded
        )
    assert isinstance(ei.value, nodal_db.ApiError)
    assert (ei.value.expected, ei.value.found) == (loaded, other["commit_id"])
    assert db.head_commit() == other["commit_id"]
    for call in (
        lambda: db.commit_changes(
            deletes=[("unit", "capacity", "Base", "u1")], user="t", expect_commit=loaded
        ),
        lambda: db.delete_values([("unit", "capacity", "Base", "u1")], user="t", expect_commit=loaded),
        lambda: db.commit_structural(
            [{"op": "create_alternative", "name": "A2"}], user="t", expect_commit=loaded
        ),
    ):
        with pytest.raises(nodal_db.Stale):
            call()
    assert db.head_commit() == other["commit_id"]
    rep = db.delete_values(
        [("unit", "capacity", "Base", "u1")], user="t", expect_commit=other["commit_id"]
    )
    assert rep["commit_id"] == other["commit_id"] + 1


# ---------------------------------------------------------------------------
# erasure
# ---------------------------------------------------------------------------


def test_erase_alternative_preview_and_erase(db):
    db.write_parameter("unit", "capacity", "scratch", scalar_batch(["u1"], [1.0]), user="t")
    preview = db.preview_erase_alternative("scratch")
    assert preview["found"] and preview["rows_total"] >= 1 and preview["still_current"]

    report = db.erase_alternative("scratch", user="t", note="pytest erase")
    assert report["erased"] and report["marker_id"] > 0 and report["identity_closed"]
    assert report["rows_total"] == preview["rows_total"]
    assert db.read_parameter("unit", "capacity", "scratch").num_rows == 0
    # Idempotent second erase: full no-op.
    again = db.erase_alternative("scratch", user="t")
    assert not again["erased"] and again["commit_id"] == 0


def test_erase_expect_present_belt(db):
    db.write_parameter("unit", "capacity", "old_run", scalar_batch(["u1"], [1.0]), user="t")
    with pytest.raises(nodal_db.ApiError):
        db.erase_alternative("old_run", user="t", expect_present=["replacement_run"])


# ---------------------------------------------------------------------------
# history-free extract / compaction
# ---------------------------------------------------------------------------


def test_extract_head_and_as_of(db, tmp_path):
    for v in (1.0, 2.0, 3.0):
        db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [v]), user="t")
    dest = tmp_path / "slim.duckdb"
    rep = db.extract(str(dest), user="t", note="pytest")
    assert rep["source_identity"] == db.db_identity()
    assert rep["extract_commit"] > rep["horizon_commit"]
    assert rep["rows_removed"] >= 2 and rep["rows_per_table"]["value_scalar"] == 1
    slim = nodal_db.open(str(dest))
    assert slim.db_identity() == rep["extract_identity"] != db.db_identity()
    assert slim.read_parameter("unit", "capacity", "Base").column("v_float").to_pylist() == [3.0]
    assert one_value(slim, "SELECT count(*) FROM value_scalar") == 1
    assert one_value(slim, "SELECT kind FROM erasure_marker") == "horizon"
    # as of the commit before the last write: the previous value.
    past = db.extract(str(tmp_path / "past.duckdb"), user="t", as_of=rep["horizon_commit"] - 1)
    assert nodal_db.open(str(tmp_path / "past.duckdb")).read_parameter(
        "unit", "capacity", "Base").column("v_float").to_pylist() == [2.0]
    assert past["horizon_commit"] == rep["horizon_commit"] - 1
    # Refusals are typed; user is required and keyword-only.
    with pytest.raises(nodal_db.ApiError):
        db.extract(str(dest), user="t")
    with pytest.raises(nodal_db.ApiError):
        db.extract(str(tmp_path / "x.duckdb"), user="t", as_of=10_000)
    with pytest.raises(TypeError):
        db.extract(str(tmp_path / "y.duckdb"))
    with pytest.raises(TypeError):
        db.extract(str(tmp_path / "z.duckdb"), "t")
    assert not (tmp_path / "x.duckdb").exists()


def test_compact(db):
    for v in (1.0, 2.0):
        db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [v]), user="t")
    ident = db.db_identity()
    rep = db.compact()
    assert rep["swapped"] and rep["rows_per_table"]["value_scalar"] == 2
    assert db.db_identity() == ident
    assert not db.compact(if_needed=True)["swapped"]
    # The growth gate fires, but the file was just compacted: the rebuild is
    # not smaller, so the automatic trigger keeps the original.
    rep = db.compact(if_needed=True, growth=0.5)
    assert rep["trigger"] == "growth" and rep["kept_original"]
    assert not rep["swapped"] and rep["warning"] and rep["bytes_rebuilt"] > 0
    # An explicit compact swaps anyway and warns.
    rep = db.compact()
    assert rep["swapped"] and not rep["kept_original"] and rep["warning"]
    with pytest.raises(nodal_db.ApiError):
        db.compact(growth=2.0)
    with pytest.raises(nodal_db.ApiError):
        db.compact(max_commits=2)
    with pytest.raises(nodal_db.ApiError):
        db.compact(if_needed=True, max_commits=-1)
    with pytest.raises(nodal_db.ApiError):
        db.compact(if_needed=True, max_commits="sometimes")
    # A bool is an int to Python; it is refused, not read as N = 1 / 0.
    for flag in (True, False):
        with pytest.raises(TypeError, match="not a bool"):
            db.compact(if_needed=True, max_commits=flag)


def test_compact_if_needed_fires_on_commit_count(db):
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [0.0]), user="t")
    assert db.compact()["trigger"] == "requested"
    for v in (1.0, 2.0):
        db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [v]), user="t")
    # Two commits since the baseline: not due by default ("auto", N = 50) or
    # with the count disabled ...
    assert not db.compact(if_needed=True)["swapped"]
    assert not db.compact(if_needed=True, max_commits=None)["swapped"]
    assert not db.compact(if_needed=True, max_commits=0)["swapped"]
    # ... but due at max_commits=2.
    rep = db.compact(if_needed=True, max_commits=2)
    assert rep["trigger"] == "commits"
    assert rep["swapped"] != rep["kept_original"]
    assert not db.compact(if_needed=True, max_commits=2)["swapped"]


# ---------------------------------------------------------------------------
# G8 migration verbs (flextool_port_finale_plan.md section 1, D-F4): thin 1:1
# exposure — every rule asserted here is core behavior observed through
# Python. Idempotent no-ops burn no commit (asserted via the commit count).
# ---------------------------------------------------------------------------


def commit_count(db):
    return db.query("SELECT count(*) AS n FROM commit").read_all()["n"][0].as_py()


def one_value(db, sql):
    t = db.query(sql).read_all()
    return t.column(0)[0].as_py() if t.num_rows else None


def test_verb_ensure_alternative(db):
    alt = db.ensure_alternative("floor", description="the v38 floor", user="t")
    assert alt > 0
    n = commit_count(db)
    assert db.ensure_alternative("Floor", user="t") == alt  # case-insensitive
    assert commit_count(db) == n  # no-op burns no commit
    with pytest.raises(nodal_db.ApiError):
        db.ensure_alternative("  ", user="t")


def test_verb_set_scenario_alternatives(db):
    for a in ("floor", "high"):
        db.ensure_alternative(a, user="t")
    sid = db.set_scenario_alternatives("s1", ["Base", "high"], user="t")
    assert sid > 0

    def order():
        t = db.query(
            "SELECT a.name FROM scenario_alternative sa "
            "JOIN alternative a ON a.cur_id = sa.alternative_id "
            "WHERE sa.valid_to_commit IS NULL ORDER BY sa.rank"
        ).read_all()
        return t["name"].to_pylist()

    assert order() == ["Base", "high"]
    n = commit_count(db)
    db.set_scenario_alternatives("s1", ["Base", "high"], user="t")
    assert commit_count(db) == n  # order-equal: no commit
    # The v38 shape: prepend the floor, whole-list replace, ranks renumber.
    db.set_scenario_alternatives("s1", ["floor", "Base", "high"], user="t")
    assert order() == ["floor", "Base", "high"]
    with pytest.raises(nodal_db.ApiError, match="ghost"):
        db.set_scenario_alternatives("s1", ["ghost"], user="t")


def test_verb_activation_and_active_by_default(db):
    db.ensure_entity_class("constraint", user="t")
    db.ensure_entity("constraint", "c1", user="t")
    db.set_entity_activation("constraint", "c1", "Base", True, user="t")
    assert (
        one_value(
            db,
            "SELECT count(*) AS n FROM entity_alternative "
            "WHERE valid_to_commit IS NULL AND active",
        )
        == 1
    )
    n = commit_count(db)
    db.set_entity_activation("constraint", "c1", "Base", True, user="t")
    assert commit_count(db) == n  # equal state: no commit
    db.set_entity_activation("constraint", "c1", "Base", None, user="t")  # close
    assert (
        one_value(
            db,
            "SELECT count(*) AS n FROM entity_alternative WHERE valid_to_commit IS NULL",
        )
        == 0
    )
    with pytest.raises(nodal_db.ApiError, match="ghost"):
        db.set_entity_activation("constraint", "ghost", "Base", True, user="t")

    db.set_class_active_by_default("constraint", False, user="t")
    assert (
        one_value(
            db,
            "SELECT CAST(active_by_default AS BIGINT) AS n FROM entity_class "
            "WHERE cur_name = 'constraint'",
        )
        == 0
    )
    n = commit_count(db)
    db.set_class_active_by_default("constraint", False, user="t")
    assert commit_count(db) == n


def test_verb_vocabulary_creation(db):
    fg = db.ensure_entity_class("flowGroup", user="t")
    db.ensure_entity_class("nodeX", user="t")
    nd = db.ensure_entity_class("flowGroup__nodeX", ["flowGroup", "nodeX"], user="t")
    assert fg > 0 and nd > 0
    # Idempotent with matching dims; mismatch is loud (no reshape door).
    assert db.ensure_entity_class("flowGroup__nodeX", ["flowGroup", "nodeX"], user="t") == nd
    with pytest.raises(nodal_db.ApiError, match="reshape"):
        db.ensure_entity_class("flowGroup__nodeX", ["nodeX"], user="t")

    # Properly typed definition: IR member declarations in.
    pd = db.ensure_parameter_definition(
        "flowGroup",
        "limit",
        types=[
            {"kind": "float"},
            {"kind": "indexed", "dims": [{"name": "period", "kind": "label"}]},
        ],
        description="a limit",
        user="t",
    )
    assert (
        one_value(
            db,
            f"SELECT count(*) AS n FROM parameter_type "
            f"WHERE parameter_definition_id = {pd} AND valid_to_commit IS NULL",
        )
        == 2
    )
    # Existing: as-is, never widened (D-F4).
    n = commit_count(db)
    assert db.ensure_parameter_definition(
        "flowGroup", "limit", types=[{"kind": "string"}], user="t"
    ) == pd
    assert commit_count(db) == n

    # Entities: 0-D and N-D with the derived __-joined name.
    db.ensure_entity("flowGroup", "g1", user="t")
    db.ensure_entity("nodeX", "n1", user="t")
    e = db.ensure_entity("flowGroup__nodeX", elements=["g1", "n1"], user="t")
    assert (
        one_value(db, f"SELECT name FROM entity WHERE cur_id = {e}") == "g1__n1"
    )
    with pytest.raises(nodal_db.ApiError):
        db.ensure_entity("flowGroup__nodeX", elements=["g1", "ghost"], user="t")


def test_verb_stamp_template_identity(db):
    db.stamp_template_identity("flextool", "40", user="t")
    assert (
        one_value(db, "SELECT value FROM metadata WHERE cur_name = 'template_name'")
        == "flextool"
    )
    n = commit_count(db)
    db.stamp_template_identity("FlexTool", "40", user="t")  # identical: no-op
    assert commit_count(db) == n
    with pytest.raises(nodal_db.ApiError, match="overwrite"):
        db.stamp_template_identity("flextool", "41", user="t")
    db.stamp_template_identity("flextool", "41", overwrite=True, user="t")
    assert (
        one_value(db, "SELECT value FROM metadata WHERE cur_name = 'template_version'")
        == "41"
    )


def test_stamped_db_refuses_unregistered_ladder_program(db, tmp_path):
    """A mid-version stamp arms the template's migration ladder, and
    template_apply refuses loudly while a pending `run:` program is
    unregistered (synthetic three-span ladder)."""
    ladder = tmp_path / "ladder.yaml"
    ladder.write_text(
        "template: {name: synth, version: '4'}\n"
        "classes: [{name: widget, parameters: [{name: size, types: [{kind: float}]}]}]\n"
        "migrations:\n"
        "  - {from: '1', to: '2', steps: []}\n"
        "  - {from: '2', to: '3', steps: [{run: synth_v3_backfill}]}\n"
        "  - {from: '3', to: '4', steps: []}\n"
    )
    db.stamp_template_identity("synth", "2", user="t")
    with pytest.raises(nodal_db.ApiError, match="synth_v3_backfill"):
        db.template_apply(str(ladder), user="t")


def test_out_of_range_timestamp_value_is_canon_error(db):
    """A µs timestamp below the store's range (DuckDB would write it and then
    fail every read of the column) is rejected by the core pipeline."""
    batch = scalar_batch(["n1"], [-(2**63)], pa.timestamp("us"))
    with pytest.raises(nodal_db.CanonError, match="outside the supported range"):
        db.write_parameter("node", "when", "Base", batch, user="tester")


def test_write_after_file_removed_is_api_error_and_creates_nothing(tmp_path):
    """A write whose database file has gone away is refused before any open:
    DuckDB's read-write open would otherwise recreate an empty file."""
    path = tmp_path / "gone.duckdb"
    handle = nodal_db.init(str(path), default_alternative="Base")
    path.unlink()
    wal = tmp_path / "gone.duckdb.wal"
    if wal.exists():
        wal.unlink()
    with pytest.raises(nodal_db.ApiError, match="no database file"):
        handle.write_parameter(
            "unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="tester"
        )
    assert not path.exists()


def test_value_floats_are_stored_canonical(db):
    """-0.0 is stored as +0.0 and every NaN as the one quiet NaN
    0x7ff8000000000000, whatever sign or payload bits it arrived with."""
    import struct

    import duckdb

    patterns = [
        (0x8000000000000000, 0x0000000000000000),  # -0.0
        (0xFFF8000000000000, 0x7FF8000000000000),  # negative quiet NaN
        (0x7FF0000000000001, 0x7FF8000000000000),  # signaling NaN
        (0x7FFFFFFFFFFFFFFF, 0x7FF8000000000000),  # quiet NaN, max payload
        (0x3FF8000000000000, 0x3FF8000000000000),  # 1.5
    ]
    n = len(patterns)
    raw = struct.pack(f"<{n}Q", *(i for i, _ in patterns))
    values = pa.Array.from_buffers(pa.float64(), n, [None, pa.py_buffer(raw)])
    entities = [f"e{i}" for i in range(n)]
    db.write_parameter(
        "node", "x", "Base", pa.record_batch({"entity": pa.array(entities), "value": values}),
        user="tester",
    )
    con = duckdb.connect(db.path, read_only=True)
    try:
        rows = con.execute(
            "SELECT e.name, vs.v_float FROM value_scalar vs "
            "JOIN entity e ON e.id = vs.entity_id ORDER BY e.name"
        ).fetchall()
    finally:
        con.close()
    got = [struct.unpack("<Q", struct.pack("<d", v))[0] for _, v in rows]
    assert got == [e for _, e in patterns]


# ---------------------------------------------------------------------------
# the read surface: status, refresh, stale files
# ---------------------------------------------------------------------------


def test_read_surface_types():
    assert issubclass(nodal_db.ReadSurfaceStale, nodal_db.ApiError)
    assert issubclass(nodal_db.ReadSurfaceStaleWarning, UserWarning)


def test_read_surface_status_and_refresh_on_current_file(db):
    st = db.read_surface_status()
    assert st["status"] == "current"
    assert st["found"] == st["expected"]
    assert set(st["expected"]) == {"contract_major", "revision", "ddl_hash"}
    assert st["refreshed"] is False
    rep = db.refresh_read_surface()
    assert rep["refreshed"] is False and rep["status"] == "current"


def _age(path):
    """Drop the version marker and read_value(s): a file of a build before
    versioning, as far as these reads can tell."""
    duckdb = pytest.importorskip(
        "duckdb",
        reason="python 'duckdb' package missing — install the declared test "
        "deps (pip install 'nodal-db[test]')",
    )
    con = duckdb.connect(path)
    try:
        con.execute("DROP MACRO TABLE nodal_read_surface")
        con.execute("DROP MACRO TABLE read_value")
        con.execute("DROP MACRO TABLE read_values")
    finally:
        con.close()


def test_stale_file_reads_warn_once_and_refresh(tmp_path):
    import warnings

    db = nodal_db.init(str(tmp_path / "stale.duckdb"), default_alternative="Base")
    db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1", "u2"], [1.0, 2.0]),
                       user="t")
    _age(db.path)
    assert db.read_surface_status()["status"] == "missing"
    assert db.read_surface_status()["found"] is None
    with pytest.warns(nodal_db.ReadSurfaceStaleWarning, match="refresh-views"):
        got = db.read_values("unit", "capacity", ["u1"])
    assert got.num_rows == 1
    # once per file and process: no second warning
    with warnings.catch_warnings():
        warnings.simplefilter("error", nodal_db.ReadSurfaceStaleWarning)
        assert db.read_values("unit", "capacity", ["u2"]).num_rows == 1
        # db.query falls back to this build's objects on a missing one
        t = db.query("SELECT * FROM read_values('unit', 'capacity', ['u1', 'u2'])").read_all()
        assert t.num_rows == 2
    rep = db.refresh_read_surface()
    assert rep["refreshed"] is True and rep["status"] == "missing"
    assert db.read_surface_status()["status"] == "current"


def test_a_write_refreshes_a_stale_file(tmp_path):
    db = nodal_db.init(str(tmp_path / "w.duckdb"), default_alternative="Base")
    _age(db.path)
    rep = db.write_parameter("unit", "capacity", "Base", scalar_batch(["u1"], [1.0]), user="t")
    assert rep["read_surface_warning"] is None
    assert db.read_surface_status()["status"] == "current"


# ---------------------------------------------------------------------------
# the text rule: names and labels in NFC, string values as written
# ---------------------------------------------------------------------------

E_NFC, E_NFD = "\u00e9", "e\u0301"


def test_normalize_identifier_is_the_cores_unicode_16_nfc():
    assert nodal_db.UNICODE_VERSION == "16.0.0"
    assert nodal_db.normalize_identifier(E_NFD) == E_NFC
    assert nodal_db.normalize_identifier("node_1") == "node_1"
    # A Unicode 16.0 composite: CPython 3.12's unicodedata (15.0) leaves
    # the pair unchanged, the core composes it.
    assert nodal_db.normalize_identifier("\U00011382\U000113c9") == "\U00011383"


def test_nfd_names_and_labels_read_back_in_nfc(db):
    batch = pa.record_batch({
        "entity": pa.array([f"n{E_NFD}", f"n{E_NFD}"], pa.string()),
        "mode": pa.array([E_NFD, "b"], pa.string()),
        "value": pa.array([E_NFD + "!", "plain"], pa.string()),
    })
    rep = db.write_parameter(
        f"cl{E_NFD}", "p", "Base", batch, domains={1: [E_NFD, "b"]}, user="tester",
    )
    text = sorted(
        (w["what"], w["count"], w["written"], w["stored"], w["name"])
        for w in rep["warnings"] if w["kind"] == "text_normalized"
    )
    assert text == [
        ("label", 1, E_NFD, E_NFC, f"cl{E_NFC}:p/Base"),
        ("name", 2, f"cl{E_NFD}", f"cl{E_NFC}", f"cl{E_NFC}:p/Base"),
    ]
    # Either spelling reads it; names and labels come back in NFC, the
    # string value exactly as written.
    for cls in (f"cl{E_NFC}", f"cl{E_NFD}"):
        rb = db.read_parameter(cls, "p", "Base")
        rows = sorted(zip(rb.column("entity").to_pylist(),
                          rb.column("idx1_text").to_pylist(),
                          rb.column("v_str").to_pylist()))
        assert rows == [(f"n{E_NFC}", "b", "plain"), (f"n{E_NFC}", E_NFC, E_NFD + "!")]

