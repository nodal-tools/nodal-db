//! The A/B harness of the perf gate (scripts/bench/ab/ab.py;
//! ab_ratio_gates_PLAN §3): one invocation = one measurement of one mode.
//! The runner builds it twice (base core / candidate core, the same harness
//! source in both) and drives the two binaries interleaved. DB init,
//! seeding, fixture copies and result checks are never timed.
//!
//! **Gate modes** (`metrics.toml`; `gate.rs`, `synth.rs`, `import.rs`,
//! `maint.rs`, and the gate wrappers in `save_path.rs` / `longbig.rs`) go
//! through the public surface only (`api::*`, the SQL read macros, the
//! `nodal-db` CLI) and print a `digest k=v ...` line (the logical result,
//! compared across the arms) before the value on the last line; a failed
//! result check exits with code 86 (`check.rs`). Fixture builders:
//! `build_100k`, `build_big`, `build_longbig`, `build_synth <path>
//! <quick|full>`, `build_hheavy <path> <quick|full>`, `build_xsrc <path>
//! <quick|full>`, `build_ipc_dense <path> <entities>`. `threads` prints the
//! DuckDB default thread count (pinning study, plan §6.2).
//!
//! **Diagnostic modes** (below; not gated, no digest, the value only) keep
//! their names for ad-hoc studies (`ab.py --bins OLD NEW --modes ...`);
//! some use crate-internal APIs (`history::write_value`).
//!
//! Modes: long_insert, long_replace (8 760-cell long values), array_insert,
//! array_replace (8 760-element arrays, 20 per transaction), axis_insert (a
//! long value on a new irregular 8 760-point datetime axis, whole commit),
//! scalar_insert, scalar_replace (400 per transaction), api_100_scalars (the
//! gate's write_100_new_scalar_entities shape), read_long (current-cell scan
//! of 100 x 876-cell long values written by the same build).
//!
//! Set-based scalar write modes (set_based_scalar_write_PLAN §6), all through
//! the API and timed per whole commit (open, write, commit, close):
//! api_10k_scalars (one 10 000-entity commit into a fresh file, per value),
//! and, on a COPY of a pre-built 10^5-scalar file named by the env var
//! `NODAL_AB_BASE`: api_100_scalars_100k (100 new entities, per value),
//! api_1_scalar_100k (a single-value replace, per commit; the editor save)
//! and api_20x1_scalars_100k (20 segments x 1 replaced value, per commit).
//! Build the base once with `ab_harness build_100k <path>` (prints the
//! build time; build it with the faster arm, the file format is the same).
//!
//! Set-based long, array and delete modes (set_based_long_and_delete_PLAN
//! §6), all through the API and timed per whole commit (open, write,
//! commit, close), in ms per commit. The `_big` modes run on a COPY of the
//! file named by `NODAL_AB_BIG`, built once with `ab_harness build_big
//! <path>` (plan §1.1: 10^5 scalar entities with parameter `p`; `bg24` =
//! 10 000 x 24-cell int long values; `bg8760` = 100 x 8 760-cell int long
//! values; `arr24` = 2 000 x 24-element float arrays; `lq00..lq19` = one
//! 24-cell int long value each on `s000000`). Modes:
//! api_delete_1000_scalar_big, api_delete_1000_long_big,
//! api_delete_500_array_big, api_long_1000x24_replace_big,
//! api_array_1000x24_replace_big, api_20x1_long24_big, api_1_long24_big,
//! api_1_delete_big and api_1_array24_big (the editor shapes: mean of 3
//! commits); on a fresh file: api_long_1000x24_new, api_array_1000x24_new
//! and api_long_10x8760_replace (10 keys seeded, then replaced in one
//! commit; only the replace is timed).
//!
//! Single-name lookup modes (name_index_and_unused_indexes_PLAN §6), on a
//! COPY of `NODAL_AB_BASE`: `lookup_raw_100k` (the core's single-name entity
//! lookup statement, 200 prepared executions on one RO connection, ms per
//! lookup) and `ensure_entity_100k` (the public `ensure_entity` verb on an
//! existing entity: RO open + lookups, mean of 20 calls). The statement is
//! the core's `api::ENTITY_NAME_LOOKUP_SQL` (default feature
//! `lookup_const`); an old arm whose core predates that const builds with
//! `--no-default-features` and runs the pre-2026-10-24 `cur_cls`/`cur_name`
//! statement it replaced.
//!
//! Skip-unchanged modes (skip_unchanged_writes_PLAN §6.4), on a fresh file,
//! ms per timed commit (the seed commit is untimed): `api_scalar_10k_
//! unchanged` / `_changed` (10 000 scalars re-written identical / changed),
//! `api_long_10x8760_unchanged` (10 x 8 760-cell long values re-written
//! identical; the changed twin is `api_long_10x8760_replace`), and
//! `api_array_200x8760_unchanged` / `_changed` (every value shifted, so the
//! v_min/v_max prefilter already proves the change) / `_changed_pinned`
//! (min and max kept, interior changed: the payload is fetched and
//! compared before the write — the worst case).
//!
//! Large-long-value modes (skip_unchanged_writes_PLAN P2): see
//! `longbig.rs` (`build_longbig`, `api_longbig_unchanged`, `_1cell`,
//! `_10pct`, `_changed`, on a copy of `NODAL_AB_LONGBIG`).
//!
//! Save-path modes (save_path_open_reduction plan §6.2): see `save_path.rs`
//! (`editor_save`, `editor_reload`, `editor_session_100`,
//! `txn_1_scalar_replace`, `growth_session`, `build_m1k`). The new arm is
//! built with `--features save_api`.

mod check;
mod gate;
mod longbig;
mod save_path;

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::{commit_changes, write_parameter, DeleteKey, Segment};
use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::{with_read, with_write_txn};
use nodal_db_core::duckdb;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis, Duration};
use nodal_db_core::history::write_value;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions,
};

/// The single-name entity lookup statement under test (`?1`/first: class
/// id, `?2`/second: name).
#[cfg(feature = "lookup_const")]
const ENTITY_LOOKUP: &str = nodal_db_core::api::ENTITY_NAME_LOOKUP_SQL;
#[cfg(not(feature = "lookup_const"))]
const ENTITY_LOOKUP: &str = "SELECT id FROM entity WHERE cur_cls = ? AND cur_name = lower(?)";

const T0: i64 = 1_735_689_600 * 1_000_000;
const H: i64 = 3_600_000_000;

fn key(e: i64) -> ValueKey {
    ValueKey {
        entity_id: e,
        parameter_definition_id: 1,
        alternative_id: 1,
    }
}

fn hourly(n: i32) -> CanonicalAxis {
    CanonicalAxis {
        length: n,
        properties: AxisProperties::default(),
        body: AxisBody::DatetimeParametric {
            start_ts: T0,
            step: Duration {
                months: 0,
                micros: H,
            },
        },
    }
}

fn long_v(seed: i64, n: i32, axis: CanonicalAxis) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Long,
        value_kind: ValueKind::Float,
        axes: vec![axis],
        shape: ShapedData::Long(
            (0..n)
                .map(|h| LongRow {
                    positions: vec![h],
                    value: ScalarValue::Float((seed + h as i64) as f64 * 1.5),
                })
                .collect(),
        ),
    }
}

fn array_v(seed: i64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Array,
        value_kind: ValueKind::Float,
        axes: vec![hourly(8760)],
        shape: ShapedData::Array((0..8760).map(|h| (seed + h) as f64 * 0.25).collect()),
    }
}

fn scalar_v(x: f64) -> CanonicalValue {
    CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(x)),
    }
}

/// Write `vals` (key, value) in ONE transaction; returns elapsed ms.
fn txn(db: &Path, vals: &[(ValueKey, CanonicalValue)]) -> f64 {
    let t = Instant::now();
    with_write_txn(db, "ab", "ab", |conn, c| {
        let mut cache = AxisCache::new();
        for (k, v) in vals {
            write_value(conn, c, *k, v, &mut cache, &WriteOptions::default(), None)?;
        }
        Ok(())
    })
    .expect("write");
    t.elapsed().as_secs_f64() * 1e3
}

fn scalar_batch(names: &[String], vals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(vals.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// One `write_parameter` of a rank-0 float batch on class `node`; returns
/// the elapsed ms of the whole commit.
fn api_scalars(db: &Path, parameter: &str, names: &[String], vals: &[f64]) -> f64 {
    let t = Instant::now();
    write_parameter(
        db,
        "node",
        parameter,
        "Base",
        scalar_batch(names, vals),
        &[],
        WriteOptions::default(),
        "ab",
        "ab",
    )
    .unwrap();
    t.elapsed().as_secs_f64() * 1e3
}

/// The 20 editor-shaped parameters of the 10^5 base file.
fn q_param(i: usize) -> String {
    format!("q{i:02}")
}

/// Build the 10^5-scalar base file: parameter `p` over entities
/// `s000000..s099999` (10 commits of 10 000, the gate's shape), then one
/// value on `s000000` for each of the 20 parameters `q00..q19`.
fn build_100k(path: &Path) {
    let t = Instant::now();
    init_schema_with_default(path, Some("Base")).unwrap();
    for chunk in 0..10 {
        let names: Vec<String> = (0..10_000)
            .map(|i| format!("s{:06}", chunk * 10_000 + i))
            .collect();
        let vals: Vec<f64> = (0..10_000)
            .map(|i| (chunk * 10_000 + i) as f64 * 0.5)
            .collect();
        api_scalars(path, "p", &names, &vals);
    }
    let segs: Vec<Segment> = (0..20).map(|i| q_segment(i, "s000000", i as f64)).collect();
    commit_changes(
        path,
        segs,
        vec![],
        vec![],
        vec![],
        "ab",
        "q",
        WriteOptions::default(),
    )
    .unwrap();
    eprintln!(
        "built {} in {:.1} s",
        path.display(),
        t.elapsed().as_secs_f64()
    );
}

fn q_segment(i: usize, entity: &str, v: f64) -> Segment {
    Segment {
        class: "node".into(),
        parameter: q_param(i),
        alternative: "Base".into(),
        batches: vec![scalar_batch(&[entity.to_string()], &[v])],
        domains: vec![],
    }
}

/// A private copy of the `NODAL_AB_BASE` file in `dir` (never timed).
fn copy_base(dir: &Path) -> std::path::PathBuf {
    let base = std::env::var("NODAL_AB_BASE").expect("set NODAL_AB_BASE to a build_100k file");
    let db = dir.join("base.duckdb");
    std::fs::copy(base, &db).expect("copy base");
    db
}

/// A private copy of the `NODAL_AB_BIG` file in `dir` (never timed).
fn copy_big(dir: &Path) -> std::path::PathBuf {
    let base = std::env::var("NODAL_AB_BIG").expect("set NODAL_AB_BIG to a build_big file");
    let db = dir.join("big.duckdb");
    std::fs::copy(base, &db).expect("copy big");
    db
}

/// The name of scalar entity `i` of the big and 10^5 files.
fn sname(i: usize) -> String {
    format!("s{i:06}")
}

fn names(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(sname).collect()
}

/// A rank-1 batch `(entity, t: Int64, value)`: `cells` cells (t = 0..cells)
/// per entity of `names`; `int` picks an Int64 measure (long tier) over a
/// Float64 one (array tier, the grid being dense). `seed` varies values.
fn series_batch(names: &[String], cells: usize, int: bool, seed: i64) -> RecordBatch {
    let n = names.len() * cells;
    let mut ent: Vec<&str> = Vec::with_capacity(n);
    let mut t: Vec<i64> = Vec::with_capacity(n);
    let mut v: Vec<i64> = Vec::with_capacity(n);
    for (k, name) in names.iter().enumerate() {
        for c in 0..cells {
            ent.push(name);
            t.push(c as i64);
            v.push(seed + (k * cells + c) as i64);
        }
    }
    let value: ArrayRef = if int {
        Arc::new(Int64Array::from(v))
    } else {
        Arc::new(Float64Array::from(
            v.iter().map(|&x| x as f64 * 0.5).collect::<Vec<_>>(),
        ))
    };
    let vtype = value.data_type().clone();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Int64, false),
            Field::new("value", vtype, false),
        ])),
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(t)) as ArrayRef,
            value,
        ],
    )
    .unwrap()
}

fn series_seg(parameter: &str, names: &[String], cells: usize, int: bool, seed: i64) -> Segment {
    Segment {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        batches: vec![series_batch(names, cells, int, seed)],
        domains: vec![],
    }
}

/// The float series of `series_seg(parameter, names, cells, false, 0)`
/// with every interior element moved by 0.25: each array keeps its first
/// (minimum) and last (maximum) element, so its `v_min` / `v_max` match the
/// stored ones and only the payload differs.
fn pinned_seg(parameter: &str, names: &[String], cells: usize) -> Segment {
    let base = series_batch(names, cells, false, 0);
    let vals = base
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let moved: Vec<f64> = (0..vals.len())
        .map(|i| {
            let c = i % cells;
            let v = vals.value(i);
            if c == 0 || c == cells - 1 {
                v
            } else {
                v + 0.25
            }
        })
        .collect();
    let batch = RecordBatch::try_new(
        base.schema(),
        vec![
            base.column(0).clone(),
            base.column(1).clone(),
            Arc::new(Float64Array::from(moved)) as ArrayRef,
        ],
    )
    .unwrap();
    Segment {
        class: "node".into(),
        parameter: parameter.into(),
        alternative: "Base".into(),
        batches: vec![batch],
        domains: vec![],
    }
}

/// One `commit_changes` of `segments` and `deletes`; returns elapsed ms.
fn api_commit(db: &Path, segments: Vec<Segment>, deletes: Vec<DeleteKey>) -> f64 {
    let t = Instant::now();
    commit_changes(
        db,
        segments,
        vec![],
        deletes,
        vec![],
        "ab",
        "ab",
        WriteOptions::default(),
    )
    .unwrap();
    t.elapsed().as_secs_f64() * 1e3
}

fn del_keys(parameter: &str, entities: impl Iterator<Item = String>) -> Vec<DeleteKey> {
    entities
        .map(|entity| DeleteKey {
            class: "node".into(),
            parameter: parameter.into(),
            alternative: "Base".into(),
            entity,
        })
        .collect()
}

/// Build the big base file (plan §1.1): `p` over 10^5 scalar entities (10
/// commits of 10 000), `bg24` over the first 10 000 (10 commits of 1 000),
/// `bg8760` over the first 100 (10 commits of 10), `arr24` over the first
/// 2 000 (one commit) and `lq00..lq19` on `s000000` (one commit).
fn build_big(path: &Path) {
    let t = Instant::now();
    init_schema_with_default(path, Some("Base")).unwrap();
    for chunk in 0..10 {
        let ns = names(chunk * 10_000..(chunk + 1) * 10_000);
        let vals: Vec<f64> = (0..10_000)
            .map(|i| (chunk * 10_000 + i) as f64 * 0.5)
            .collect();
        api_scalars(path, "p", &ns, &vals);
    }
    for chunk in 0..10 {
        let ns = names(chunk * 1_000..(chunk + 1) * 1_000);
        api_commit(path, vec![series_seg("bg24", &ns, 24, true, 0)], vec![]);
    }
    for chunk in 0..10 {
        let ns = names(chunk * 10..(chunk + 1) * 10);
        api_commit(path, vec![series_seg("bg8760", &ns, 8760, true, 0)], vec![]);
    }
    api_commit(
        path,
        vec![series_seg("arr24", &names(0..2_000), 24, false, 0)],
        vec![],
    );
    let segs: Vec<Segment> = (0..20)
        .map(|i| series_seg(&format!("lq{i:02}"), &names(0..1), 24, true, i))
        .collect();
    api_commit(path, segs, vec![]);
    eprintln!(
        "built {} in {:.1} s",
        path.display(),
        t.elapsed().as_secs_f64()
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args[1].as_str();
    if mode == "build_100k" {
        build_100k(Path::new(&args[2]));
        return;
    }
    if mode == "build_big" {
        build_big(Path::new(&args[2]));
        return;
    }
    if mode == "build_longbig" {
        longbig::build(Path::new(&args[2]));
        return;
    }
    if mode == "build_m1k" {
        save_path::build_m1k(Path::new(&args[2]));
        return;
    }
    if mode == "threads" {
        let conn = nodal_db_core::duckdb::Connection::open_in_memory().unwrap();
        let t: String = conn
            .query_row("SELECT current_setting('threads')::VARCHAR", [], |r| {
                r.get(0)
            })
            .unwrap();
        println!("{t}");
        return;
    }
    check::set_mode(mode);
    let dir = tempfile::tempdir().unwrap();
    if let Some(sample) = gate::run(mode, dir.path()) {
        sample.emit();
        return;
    }
    if mode == "growth_session" {
        save_path::growth_session(dir.path());
        return;
    }
    if let Some(ms) = longbig::run(mode, dir.path()) {
        println!("{ms:.6}");
        return;
    }
    if let Some(ms) = save_path::run(mode, dir.path()) {
        println!("{ms:.6}");
        return;
    }
    let db = dir.path().join("ab.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();

    let ms_per_unit = match mode {
        // 3 fresh 8 760-cell long values, one txn each.
        "long_insert" => {
            let mut tot = 0.0;
            for e in 1..=3 {
                tot += txn(&db, &[(key(e), long_v(e, 8760, hourly(8760)))]);
            }
            tot / 3.0
        }
        // seed one, then 3 full replaces (close-out 8 760 + reinsert).
        "long_replace" => {
            txn(&db, &[(key(1), long_v(0, 8760, hourly(8760)))]);
            let mut tot = 0.0;
            for s in 1..=3 {
                tot += txn(&db, &[(key(1), long_v(s, 8760, hourly(8760)))]);
            }
            tot / 3.0
        }
        // 20 fresh 8 760-element arrays in one txn.
        "array_insert" => {
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e))).collect();
            txn(&db, &v) / 20.0
        }
        // seed 20, then replace all 20 in one txn.
        "array_replace" => {
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e))).collect();
            txn(&db, &v);
            let v: Vec<_> = (1..=20).map(|e| (key(e), array_v(e + 7))).collect();
            txn(&db, &v) / 20.0
        }
        // one long value on a NEW irregular 8 760-coordinate datetime axis
        // (8 760 axis_datetime child rows), minus nothing: whole txn.
        "axis_insert" => {
            let coords: Vec<i64> = (0..8760).map(|h| T0 + h * H + (h % 7)).collect();
            let ax = CanonicalAxis {
                length: 8760,
                properties: AxisProperties::default(),
                body: AxisBody::DatetimeMaterialized { coords },
            };
            txn(&db, &[(key(1), long_v(1, 8760, ax))])
        }
        // 400 fresh scalars in one txn (µs/value reported as ms).
        "scalar_insert" => {
            let v: Vec<_> = (1..=400).map(|e| (key(e), scalar_v(e as f64))).collect();
            txn(&db, &v) / 400.0
        }
        "scalar_replace" => {
            let v: Vec<_> = (1..=400).map(|e| (key(e), scalar_v(e as f64))).collect();
            txn(&db, &v);
            let v: Vec<_> = (1..=400)
                .map(|e| (key(e), scalar_v(e as f64 + 0.5)))
                .collect();
            txn(&db, &v) / 400.0
        }
        // The gate's write_100_new_scalar_entities, through the API.
        "api_100_scalars" => {
            let mut tot = 0.0;
            for run in 0..3 {
                let names: Vec<String> = (0..100).map(|i| format!("w{run}_{i:03}")).collect();
                let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
                let t = Instant::now();
                write_parameter(
                    &db,
                    "node",
                    "p",
                    "Base",
                    scalar_batch(&names, &vals),
                    &[],
                    WriteOptions::default(),
                    "ab",
                    "ab",
                )
                .unwrap();
                tot += t.elapsed().as_secs_f64() * 1e3;
            }
            tot / 300.0
        }
        // The gate build's unit: one 10 000-new-entity scalar commit.
        "api_10k_scalars" => {
            let names: Vec<String> = (0..10_000).map(|i| format!("s{i:06}")).collect();
            let vals: Vec<f64> = (0..10_000).map(|i| i as f64).collect();
            api_scalars(&db, "p", &names, &vals) / 10_000.0
        }
        // 100 new scalar entities into the 10^5 file, 3 commits.
        "api_100_scalars_100k" => {
            let db = copy_base(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let names: Vec<String> = (0..100).map(|i| format!("w{run}_{i:03}")).collect();
                let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
                tot += api_scalars(&db, "p", &names, &vals);
            }
            tot / 300.0
        }
        // The editor save: one replaced value on the 10^5 file, per commit
        // (mean of 3 commits).
        "api_1_scalar_100k" => {
            let db = copy_base(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let names = vec![format!("s{:06}", 50_000 + run)];
                tot += api_scalars(&db, "p", &names, &[run as f64 + 0.25]);
            }
            tot / 3.0
        }
        // 20 segments x 1 replaced value in one commit on the 10^5 file, per
        // commit (mean of 3 commits).
        "api_20x1_scalars_100k" => {
            let db = copy_base(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let segs: Vec<Segment> = (0..20)
                    .map(|i| q_segment(i, "s000000", (run * 100 + i) as f64 + 0.5))
                    .collect();
                let t = Instant::now();
                commit_changes(
                    &db,
                    segs,
                    vec![],
                    vec![],
                    vec![],
                    "ab",
                    "ab",
                    WriteOptions::default(),
                )
                .unwrap();
                tot += t.elapsed().as_secs_f64() * 1e3;
            }
            tot / 3.0
        }
        // Read row: 100 keys x 876-cell long values written by THIS build,
        // then the current-cell scan of each key by triple (x5), per key.
        "read_long" => {
            let v: Vec<_> = (1..=100)
                .map(|e| (key(e), long_v(e, 876, hourly(876))))
                .collect();
            txn(&db, &v);
            let t = Instant::now();
            with_read(&db, |conn| {
                let mut stmt = conn.prepare(
                    "SELECT sum(v_float), count(*) FROM value_long \
                     WHERE entity_id = ? AND parameter_definition_id = 1 \
                       AND alternative_id = 1 AND valid_to_commit IS NULL",
                )?;
                for _ in 0..5 {
                    for e in 1..=100i64 {
                        let n: i64 = stmt.query_row([e], |r| r.get(1))?;
                        assert_eq!(n, 876);
                    }
                }
                Ok(())
            })
            .unwrap();
            t.elapsed().as_secs_f64() * 1e3 / 500.0
        }
        // --- set_based_long_and_delete_PLAN §6 (ms per commit) ---
        "api_delete_1000_scalar_big" => {
            let db = copy_big(dir.path());
            api_commit(&db, vec![], del_keys("p", (10_000..11_000).map(sname)))
        }
        "api_delete_1000_long_big" => {
            let db = copy_big(dir.path());
            api_commit(&db, vec![], del_keys("bg24", (0..1_000).map(sname)))
        }
        "api_delete_500_array_big" => {
            let db = copy_big(dir.path());
            api_commit(&db, vec![], del_keys("arr24", (0..500).map(sname)))
        }
        "api_long_1000x24_new" => api_commit(
            &db,
            vec![series_seg("l24", &names(0..1_000), 24, true, 0)],
            vec![],
        ),
        "api_long_1000x24_replace_big" => {
            let db = copy_big(dir.path());
            api_commit(
                &db,
                vec![series_seg("bg24", &names(1_000..2_000), 24, true, 7)],
                vec![],
            )
        }
        "api_array_1000x24_new" => api_commit(
            &db,
            vec![series_seg("a24", &names(0..1_000), 24, false, 0)],
            vec![],
        ),
        "api_array_1000x24_replace_big" => {
            let db = copy_big(dir.path());
            api_commit(
                &db,
                vec![series_seg("arr24", &names(0..1_000), 24, false, 7)],
                vec![],
            )
        }
        "api_20x1_long24_big" => {
            let db = copy_big(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let segs: Vec<Segment> = (0..20)
                    .map(|i| {
                        series_seg(&format!("lq{i:02}"), &names(0..1), 24, true, 100 * run + i)
                    })
                    .collect();
                tot += api_commit(&db, segs, vec![]);
            }
            tot / 3.0
        }
        "api_1_long24_big" => {
            let db = copy_big(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let ns = names(5_000 + run..5_001 + run);
                tot += api_commit(
                    &db,
                    vec![series_seg("bg24", &ns, 24, true, run as i64 + 3)],
                    vec![],
                );
            }
            tot / 3.0
        }
        "api_1_delete_big" => {
            let db = copy_big(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let keys = del_keys("p", std::iter::once(sname(50_000 + run)));
                tot += api_commit(&db, vec![], keys);
            }
            tot / 3.0
        }
        "api_1_array24_big" => {
            let db = copy_big(dir.path());
            let mut tot = 0.0;
            for run in 0..3 {
                let ns = names(500 + run..501 + run);
                tot += api_commit(
                    &db,
                    vec![series_seg("arr24", &ns, 24, false, run as i64 + 3)],
                    vec![],
                );
            }
            tot / 3.0
        }
        "api_long_10x8760_replace" => {
            let ns = names(0..10);
            api_commit(&db, vec![series_seg("l8760", &ns, 8760, true, 0)], vec![]);
            api_commit(&db, vec![series_seg("l8760", &ns, 8760, true, 5)], vec![])
        }
        // --- skip_unchanged_writes_PLAN §6.4 (ms per commit, the timed
        // commit only; the seed commit is untimed). `_unchanged` re-writes
        // identical values (the new core skips them and makes no commit);
        // `_changed` re-writes changed ones (the comparison is overhead);
        // `_changed_pinned` keeps every array's min and max and changes its
        // interior, so the array prefilter passes and the payload is
        // fetched and compared before the write (the worst case, e.g.
        // results pinned at 0 and capacity).
        "api_scalar_10k_unchanged" | "api_scalar_10k_changed" => {
            let ns = names(0..10_000);
            let vals: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.5).collect();
            api_scalars(&db, "p", &ns, &vals);
            let bump = if mode.ends_with("_changed") {
                0.25
            } else {
                0.0
            };
            let vals: Vec<f64> = vals.iter().map(|v| v + bump).collect();
            api_scalars(&db, "p", &ns, &vals)
        }
        "api_long_10x8760_unchanged" => {
            let ns = names(0..10);
            api_commit(&db, vec![series_seg("l8760", &ns, 8760, true, 0)], vec![]);
            api_commit(&db, vec![series_seg("l8760", &ns, 8760, true, 0)], vec![])
        }
        "api_array_200x8760_unchanged"
        | "api_array_200x8760_changed"
        | "api_array_200x8760_changed_pinned" => {
            let ns = names(0..200);
            api_commit(&db, vec![series_seg("a8760", &ns, 8760, false, 0)], vec![]);
            let seg = match mode {
                "api_array_200x8760_unchanged" => series_seg("a8760", &ns, 8760, false, 0),
                "api_array_200x8760_changed" => series_seg("a8760", &ns, 8760, false, 5),
                _ => pinned_seg("a8760", &ns, 8760),
            };
            api_commit(&db, vec![seg], vec![])
        }
        // Single-name lookups on the 10^5 file (see the module docs).
        "ensure_entity_100k" => {
            let db = copy_base(dir.path());
            let t = Instant::now();
            for i in 0..20 {
                let name = format!("s{:06}", 40_000 + i * 7);
                nodal_db_core::verbs::ensure_entity(&db, "node", Some(&name), &[], "ab").unwrap();
            }
            t.elapsed().as_secs_f64() * 1e3 / 20.0
        }
        "lookup_raw_100k" => {
            let db = copy_base(dir.path());
            with_read(&db, |conn| {
                let cid: i64 = conn.query_row(
                    "SELECT id FROM entity_class WHERE cur_name = 'node'",
                    [],
                    |r| r.get(0),
                )?;
                let mut stmt = conn.prepare(ENTITY_LOOKUP)?;
                for i in 0..20 {
                    let name = format!("S{i:06}");
                    let _: i64 = stmt.query_row(duckdb::params![cid, name], |r| r.get(0))?;
                }
                let t = Instant::now();
                for i in 0..200 {
                    let name = format!("S{:06}", 30_000 + i * 13);
                    let id: i64 = stmt.query_row(duckdb::params![cid, name], |r| r.get(0))?;
                    assert!(id > 0);
                }
                Ok(t.elapsed().as_secs_f64() * 1e3 / 200.0)
            })
            .unwrap()
        }
        other => panic!("unknown mode {other}"),
    };
    println!("{ms_per_unit:.6}");
}
