//! Save-path modes (save_path_open_reduction plan §6.2, as revised by its
//! critique: the commit span is timed separately from the post-save reload).
//!
//! One source, two arms, selected by the cargo feature `save_api`:
//! - without it (today's editor; also builds against older cores): a save is
//!   an RO head poll (`with_read(max id)`, the editor's `latest_commit_id`)
//!   followed by `commit_changes`: two opens in the commit span;
//! - with it: `commit_changes` with `WriteOptions::expect_commit = head`
//!   (the staleness check inside the write transaction): one open.
//!
//! The post-save reload (`model_slurp` on a fresh RO connection) is the same
//! in both arms and is timed on its own (`editor_reload`): it is 77-98 % of a
//! save on the editor's files and would drown the open saving in noise.
//!
//! Modes (ms; `ab.py` takes the last stdout line):
//! - `editor_save`: mean commit span (poll + commit, or commit(expect)) of 10
//!   one-scalar replaces on the 5 x 40 x 10 scalar fixture;
//! - `editor_reload`: mean `model_slurp` reload (open, queries, close) on
//!   the same fixture after the same 10 saves;
//! - `editor_session_100`: 100 full editor saves (commit span + reload);
//!   prints the least-squares growth slope of the per-save time, ms/save;
//! - `txn_1_scalar_replace`: mean of 10 one-scalar-replace transactions
//!   through `with_write_txn_retrying` (no feature) or
//!   `with_write_txn_retrying_opts` with `expect_commit = Some(head)` (with
//!   it): the cost of the in-transaction `SELECT max(id)`;
//! - `growth_session` (feature only; not an A/B mode): the editor's full
//!   save cycle `commit(expect) → compact_if_due(Some(id)) → reload`,
//!   repeated, printing one JSON summary. Env: `NODAL_AB_SAVES` (default
//!   100), `NODAL_AB_BACKSTOP` = `off` | `auto` | N (default `auto`),
//!   `NODAL_AB_FILE` = a `build_m1k` file to run on a copy of (the saves
//!   target its `cls0`/`s00`/`e0_000`; default: the fixture).
//! - `build_m1k <path>`: the critique probe's 1 000-entity model (4 classes
//!   x 250 entities; 16 scalar params x 2 alternatives, 2 hourly 8 760-pt
//!   series on 25 entities and 2 weekly 168-pt series on all, per class).

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::{commit_changes, Segment};
use nodal_db_core::conn::with_read;
use nodal_db_core::duckdb;
use nodal_db_core::duckdb::Connection;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

fn ms(t: Instant) -> f64 {
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

fn series_batch(names: &[String], cells: usize, seed: f64) -> RecordBatch {
    let n = names.len() * cells;
    let mut ent: Vec<&str> = Vec::with_capacity(n);
    let mut t: Vec<i64> = Vec::with_capacity(n);
    let mut v: Vec<f64> = Vec::with_capacity(n);
    for (k, name) in names.iter().enumerate() {
        for c in 0..cells {
            ent.push(name);
            t.push(c as i64);
            v.push(seed + ((k * 7919 + c * 31) % 10007) as f64 * 0.37);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("t", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn seg(class: &str, param: &str, alt: &str, b: RecordBatch) -> Segment {
    Segment {
        class: class.into(),
        parameter: param.into(),
        alternative: alt.into(),
        batches: vec![b],
        domains: vec![],
    }
}

fn build_commit(db: &Path, segs: Vec<Segment>) {
    commit_changes(
        db,
        segs,
        vec![],
        vec![],
        vec![],
        "ab",
        "build",
        WriteOptions::default(),
    )
    .unwrap();
}

/// The plan's commit-A/B fixture: 5 classes x 40 entities x 10 scalar
/// parameters (one commit per class; never timed).
fn build_fixture(db: &Path) {
    init_schema_with_default(db, Some("Base")).unwrap();
    for c in 0..5 {
        let class = format!("cls{c}");
        let names: Vec<String> = (0..40).map(|i| format!("e{c}_{i:02}")).collect();
        let segs = (0..10)
            .map(|p| {
                let vals: Vec<f64> = (0..40).map(|i| (i * 3 + p) as f64 * 0.5).collect();
                seg(
                    &class,
                    &format!("p{p:02}"),
                    "Base",
                    scalar_batch(&names, &vals),
                )
            })
            .collect();
        build_commit(db, segs);
    }
}

/// The critique probe's m1k model (plan §11).
pub fn build_m1k(db: &Path) {
    let t = Instant::now();
    init_schema_with_default(db, Some("Base")).unwrap();
    for c in 0..4 {
        let class = format!("cls{c}");
        let names: Vec<String> = (0..250).map(|i| format!("e{c}_{i:03}")).collect();
        for alt in ["Base", "High"] {
            let segs = (0..16)
                .map(|p| {
                    let vals: Vec<f64> = (0..250).map(|i| (i * 3 + p) as f64 * 0.5).collect();
                    seg(
                        &class,
                        &format!("s{p:02}"),
                        alt,
                        scalar_batch(&names, &vals),
                    )
                })
                .collect();
            build_commit(db, segs);
        }
        build_commit(
            db,
            vec![
                seg(
                    &class,
                    "ts_h0",
                    "Base",
                    series_batch(&names[..25], 8760, 1.0),
                ),
                seg(
                    &class,
                    "ts_h1",
                    "Base",
                    series_batch(&names[..25], 8760, 2.0),
                ),
            ],
        );
        build_commit(
            db,
            vec![
                seg(&class, "ts_w0", "Base", series_batch(&names, 168, 3.0)),
                seg(&class, "ts_w1", "Base", series_batch(&names, 168, 4.0)),
            ],
        );
    }
    eprintln!(
        "built {} in {:.1} s",
        db.display(),
        t.elapsed().as_secs_f64()
    );
}

// ------------------------------------------------- the editor's load pattern

fn query_rows(
    conn: &Connection,
    sql: &str,
) -> Result<Vec<RecordBatch>, nodal_db_core::error::DbError> {
    let mut stmt = conn.prepare(sql)?;
    let arrow = stmt.query_arrow([])?;
    Ok(arrow.collect())
}

fn first_strings(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        out.extend((0..b.num_rows()).map(|i| a.value(i).to_string()));
    }
    out
}

/// The SQL statement sequence of the editor's `ModelCache::load`
/// (nodal-editor `crates/nodal-editor/src/model.rs`, working tree of
/// 2026-10-24; copied from the save-path critique's probe), with the Arrow
/// fetch but without the editor's Fact decode. Keep it in step with the
/// editor by hand: the bench must not depend on the editor crate.
fn model_slurp(conn: &Connection) -> Result<usize, nodal_db_core::error::DbError> {
    Ok(model_slurp_digest(conn)?.0)
}

/// [`model_slurp`] plus the digest of everything it read (without the
/// file's storage-version tag, which an engine bump legitimately changes).
fn model_slurp_digest(
    conn: &Connection,
) -> Result<(usize, crate::check::RowHash), nodal_db_core::error::DbError> {
    let mut rows = 0usize;
    let mut hash = crate::check::RowHash::default();
    let mut q = |sql: &str| -> Result<Vec<RecordBatch>, nodal_db_core::error::DbError> {
        let b = query_rows(conn, sql)?;
        rows += b.iter().map(|x| x.num_rows()).sum::<usize>();
        if !sql.contains("storage_version") {
            hash.add(&b);
        }
        Ok(b)
    };
    let classes = first_strings(&q(
        "SELECT name FROM entity_class_current ORDER BY display_order NULLS LAST, name",
    )?);
    q("SELECT name FROM alternative_current ORDER BY name")?;
    q("SELECT COALESCE(max(id), 0) FROM commit")?;
    q(
        "SELECT COALESCE(tags['storage_version'], '') FROM duckdb_databases() \
       WHERE database_name = current_database()",
    )?;
    q(
        "SELECT DISTINCT lower(ec.name) FROM entity_class_dimension_current ecd \
       JOIN entity_class_current ec ON ecd.entity_class_id = ec.id",
    )?;
    q("SELECT name FROM scenario_current ORDER BY name")?;
    q(
        "SELECT s.name, a.name, sa.rank FROM scenario_alternative_current sa \
       JOIN scenario_current s ON sa.scenario_id = s.id \
       JOIN alternative_current a ON sa.alternative_id = a.id ORDER BY s.name, sa.rank",
    )?;
    q(
        "SELECT ec.name, e.name, a.name, ea.active FROM entity_alternative_current ea \
       JOIN entity_current e ON ea.entity_id = e.id \
       JOIN entity_class_current ec ON e.entity_class_id = ec.id \
       JOIN alternative_current a ON ea.alternative_id = a.id",
    )?;
    for class in &classes {
        q(&format!(
            "SELECT e.name FROM entity_current e \
             JOIN entity_class_current ec ON e.entity_class_id = ec.id \
             WHERE lower(ec.name) = lower('{class}') ORDER BY e.name"
        ))?;
        let rank0 = first_strings(&q(&format!(
            "SELECT pd.name, pt.value_kind FROM parameter_definition_current pd \
             JOIN entity_class_current ec ON pd.entity_class_id = ec.id \
             JOIN parameter_type_current pt ON pt.parameter_definition_id = pd.id \
             WHERE lower(ec.name) = lower('{class}') AND pt.rank = 0 \
             ORDER BY pd.display_order NULLS LAST, pd.name"
        ))?);
        let rank1 = first_strings(&q(&format!(
            "SELECT pd.name FROM parameter_definition_current pd \
             JOIN entity_class_current ec ON pd.entity_class_id = ec.id \
             JOIN parameter_type_current pt ON pt.parameter_definition_id = pd.id \
             WHERE lower(ec.name) = lower('{class}') AND pt.rank >= 1 \
             ORDER BY pd.display_order NULLS LAST, pd.name"
        ))?);
        for p in &rank1 {
            q(&format!(
                "SELECT ptd.dimension_name, ptd.kind \
                 FROM parameter_definition_current pd \
                 JOIN entity_class_current ec ON pd.entity_class_id = ec.id \
                 JOIN parameter_type_current pt ON pt.parameter_definition_id = pd.id \
                 JOIN parameter_type_dimension_current ptd \
                   ON ptd.parameter_type_id = pt.id AND ptd.position = 1 \
                 WHERE lower(ec.name) = lower('{class}') \
                   AND lower(pd.name) = lower('{p}') AND pt.rank >= 1"
            ))?;
        }
        for p in &rank0 {
            q(&format!(
                "SELECT entity, alternative, value_kind, v_float, v_int, v_bool, v_str \
                 FROM read_parameter_scalar('{class}','{p}')"
            ))?;
        }
        for p in &rank1 {
            q(&format!(
                "SELECT entity, idx1_num, idx1_ordinal, value_kind, v_float, v_int, alternative \
                 FROM read_parameter('{class}','{p}') ORDER BY idx1_ordinal, entity"
            ))?;
        }
    }
    Ok((rows, hash))
}

/// The editor's post-save reload: one RO open → `model_slurp` → close.
fn reload(db: &Path) -> f64 {
    let t = Instant::now();
    with_read(db, model_slurp).expect("reload");
    ms(t)
}

fn head(db: &Path) -> i64 {
    with_read(db, |c| {
        Ok(
            c.query_row("SELECT COALESCE(max(id), 0) FROM \"commit\"", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    })
    .unwrap()
}

/// One editor-shaped save (one scalar replace on `cls0`/`p00` of entity
/// `e0_00`, or `cls0`/`s00` on the m1k file) and its commit span in ms.
/// `epoch` is the editor's `cache.epoch_commit_id`, updated to the new
/// commit.
fn save(db: &Path, epoch: &mut i64, k: usize, param: &str, entity: &str) -> f64 {
    let segs = vec![seg(
        "cls0",
        param,
        "Base",
        scalar_batch(&[entity.to_string()], &[1000.0 + k as f64]),
    )];
    let t = Instant::now();
    #[cfg(not(feature = "save_api"))]
    let opts = {
        // Today's editor: the pre-commit RO poll (`Db::latest_commit_id`).
        let h = head(db);
        assert_eq!(h, *epoch, "stale (no other writer in the bench)");
        WriteOptions::default()
    };
    #[cfg(feature = "save_api")]
    let opts = WriteOptions {
        expect_commit: Some(*epoch),
        ..Default::default()
    };
    let r = commit_changes(db, segs, vec![], vec![], vec![], "ab", "save", opts).unwrap();
    let span = ms(t);
    *epoch = r.commit_id;
    span
}

/// Least-squares slope of `ys` against their index.
fn slope(ys: &[f64]) -> f64 {
    let n = ys.len() as f64;
    let mx = (n - 1.0) / 2.0;
    let my = ys.iter().sum::<f64>() / n;
    let (mut num, mut den) = (0.0, 0.0);
    for (i, y) in ys.iter().enumerate() {
        let dx = i as f64 - mx;
        num += dx * (y - my);
        den += dx * dx;
    }
    num / den
}

fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}

/// Dispatch a save-path mode; `None` if `mode` is not one of them.
pub fn run(mode: &str, dir: &Path) -> Option<f64> {
    let db = dir.join("save.duckdb");
    Some(match mode {
        "editor_save" => {
            build_fixture(&db);
            let mut epoch = head(&db);
            save(&db, &mut epoch, 0, "p00", "e0_00"); // warm-up, untimed
            let spans: Vec<f64> = (1..=10)
                .map(|k| save(&db, &mut epoch, k, "p00", "e0_00"))
                .collect();
            mean(&spans)
        }
        "editor_reload" => {
            build_fixture(&db);
            let mut epoch = head(&db);
            for k in 0..=10 {
                save(&db, &mut epoch, k, "p00", "e0_00");
            }
            reload(&db); // warm-up
            mean(&(0..5).map(|_| reload(&db)).collect::<Vec<_>>())
        }
        "editor_session_100" => {
            build_fixture(&db);
            let mut epoch = head(&db);
            let per_save: Vec<f64> = (0..100)
                .map(|k| save(&db, &mut epoch, k, "p00", "e0_00") + reload(&db))
                .collect();
            eprintln!(
                "mean {:.1} ms/save, first10 {:.1}, last10 {:.1}",
                mean(&per_save),
                mean(&per_save[..10]),
                mean(&per_save[90..])
            );
            slope(&per_save)
        }
        "txn_1_scalar_replace" => {
            build_fixture(&db);
            let mut tot = 0.0;
            for k in 0..10 {
                let h = head(&db);
                tot += one_scalar_txn(&db, h, k);
            }
            tot / 10.0
        }
        _ => return None,
    })
}

/// One single-value scalar replace through the transaction layer; ms.
fn one_scalar_txn(db: &Path, head: i64, k: usize) -> f64 {
    use nodal_db_core::axis::AxisCache;
    use nodal_db_core::canon::CanonicalValue;
    use nodal_db_core::history::write_value;
    use nodal_db_core::types::{ScalarValue, ShapedData, Tier, ValueKey, ValueKind};
    let key = ValueKey {
        entity_id: 1,
        parameter_definition_id: 1,
        alternative_id: 1,
    };
    let v = CanonicalValue {
        tier: Tier::Scalar,
        value_kind: ValueKind::Float,
        axes: vec![],
        shape: ShapedData::Scalar(ScalarValue::Float(k as f64 + 0.5)),
    };
    let body = |conn: &Connection, c: i64| {
        let mut cache = AxisCache::new();
        write_value(conn, c, key, &v, &mut cache, &WriteOptions::default(), None)?;
        Ok(())
    };
    let t = Instant::now();
    #[cfg(not(feature = "save_api"))]
    {
        let _ = head;
        nodal_db_core::conn::with_write_txn_retrying(db, "ab", "ab", body).unwrap();
    }
    #[cfg(feature = "save_api")]
    nodal_db_core::conn::with_write_txn_retrying_opts(
        db,
        "ab",
        "ab",
        nodal_db_core::conn::TxnOptions {
            expect_commit: Some(head),
        },
        body,
    )
    .unwrap();
    ms(t)
}

/// The growth session (feature only): prints one JSON line.
#[cfg(feature = "save_api")]
pub fn growth_session(dir: &Path) {
    use nodal_db_core::compaction::{
        compact_if_due, CommitBackstop, CompactionPolicy, CompactionTrigger,
    };
    let saves: usize = std::env::var("NODAL_AB_SAVES")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(100);
    let backstop_s = std::env::var("NODAL_AB_BACKSTOP").unwrap_or_else(|_| "auto".into());
    let backstop = match backstop_s.as_str() {
        "off" => CommitBackstop::Off,
        "auto" => CommitBackstop::SizeScaled,
        n => CommitBackstop::Fixed(n.parse().unwrap()),
    };
    let policy = CompactionPolicy {
        max_commits: backstop,
        ..Default::default()
    };
    let (db, param, entity) = match std::env::var("NODAL_AB_FILE") {
        Ok(src) => {
            let db = dir.join("session.duckdb");
            std::fs::copy(src, &db).unwrap();
            (db, "s00", "e0_000")
        }
        Err(_) => {
            let db = dir.join("session.duckdb");
            build_fixture(&db);
            (db, "p00", "e0_00")
        }
    };
    // Establish the baseline first (the editor's first save on a file with
    // no sidecar would compact; keep that out of the series).
    nodal_db_core::compaction::compact(&db).unwrap();
    let mut epoch = head(&db);
    let (mut commit_ms, mut compact_ms, mut reload_ms, mut total) =
        (vec![], vec![], vec![], vec![]);
    let mut stalls: Vec<f64> = vec![];
    let mut swaps_by: Vec<String> = vec![];
    for k in 0..saves {
        let c = save(&db, &mut epoch, k, param, entity);
        let t = Instant::now();
        let r = compact_if_due(&db, policy, Some(epoch)).unwrap();
        let g = ms(t);
        if r.swapped {
            stalls.push(g);
            swaps_by.push(format!(
                "{}@{k}",
                match r.trigger {
                    Some(CompactionTrigger::Commits) => "commits",
                    Some(CompactionTrigger::Growth) => "growth",
                    Some(CompactionTrigger::NoBaseline) => "nobaseline",
                    _ => "other",
                }
            ));
        }
        let l = reload(&db);
        commit_ms.push(c);
        compact_ms.push(g);
        reload_ms.push(l);
        total.push(c + g + l);
    }
    // An empty RO open+close at the end: the open cost a reader pays now.
    let t = Instant::now();
    with_read(&db, |_| Ok(())).unwrap();
    let ro_end = ms(t);
    let non_stall: Vec<f64> = total.iter().zip(&compact_ms).map(|(t, g)| t - g).collect();
    let bytes = std::fs::metadata(&db).unwrap().len();
    println!(
        "{{\"backstop\":\"{backstop_s}\",\"saves\":{saves},\"file_MiB_end\":{:.1},\
\"compactions\":{},\"swaps\":{:?},\"stall_ms_max\":{:.0},\"stall_ms_mean\":{:.0},\
\"commit_ms_mean\":{:.1},\"reload_ms_mean\":{:.1},\"save_ms_mean_incl_stalls\":{:.1},\
\"save_ms_first10\":{:.1},\"save_ms_last10\":{:.1},\"growth_slope_ms_per_save_excl_stalls\":{:.3},\
\"commit_first10\":{:.1},\"commit_last10\":{:.1},\"ro_open_close_end_ms\":{:.1}}}",
        bytes as f64 / 1048576.0,
        stalls.len(),
        swaps_by,
        stalls.iter().cloned().fold(0.0, f64::max),
        if stalls.is_empty() {
            0.0
        } else {
            mean(&stalls)
        },
        mean(&commit_ms),
        mean(&reload_ms),
        mean(&total),
        mean(&non_stall[..10]),
        mean(&non_stall[saves - 10..]),
        slope(&non_stall),
        mean(&commit_ms[..10]),
        mean(&commit_ms[saves - 10..]),
        ro_end,
    );
}

#[cfg(not(feature = "save_api"))]
pub fn growth_session(_dir: &Path) {
    panic!("growth_session needs --features save_api (compact_if_due)");
}

// ------------------------------------------------------------- gate modes

/// `wr_editor_save` (gate): the mean commit span of 10 one-scalar editor
/// saves (after one untimed warm-up save) on the 5 x 40 x 10 fixture, in
/// the gate's arm semantics (no `save_api`: RO head poll + commit). Checks:
/// 11 commits, and the saved cell reads back with the last value.
pub fn gate_editor_save(dir: &Path) -> crate::check::Sample {
    use crate::check::{check_commits, check_rows, read_hash, scalar_i64, Sample};
    let db = dir.join("save.duckdb");
    build_fixture(&db);
    let before = head(&db);
    let mut epoch = before;
    save(&db, &mut epoch, 0, "p00", "e0_00");
    let spans: Vec<f64> = (1..=10)
        .map(|k| save(&db, &mut epoch, k, "p00", "e0_00"))
        .collect();
    check_commits(&db, before, 11);
    check_rows(
        "saved value read back",
        scalar_i64(
            &db,
            "SELECT count(*) FROM read_parameter_scalar('cls0', 'p00') \
             WHERE entity = 'e0_00' AND v_float = 1010.0",
        ) as usize,
        1,
    );
    let h = read_hash(
        &db,
        "SELECT entity, alternative, v_float FROM read_parameter_scalar('cls0', 'p00')",
    );
    check_rows("cls0/p00 rows", h.rows as usize, 40);
    Sample::new(mean(&spans)).hash("readback", &h)
}

/// `rd_editor_reload` (gate): the mean of 5 editor reloads (`model_slurp`
/// on a fresh RO connection; one untimed warm-up) after 11 saves on the
/// fixture. Checks: every reload reads the same rows and content, 5
/// classes; the digest covers everything read.
pub fn gate_editor_reload(dir: &Path) -> crate::check::Sample {
    use crate::check::{check, Sample};
    let db = dir.join("save.duckdb");
    build_fixture(&db);
    let mut epoch = head(&db);
    for k in 0..=10 {
        save(&db, &mut epoch, k, "p00", "e0_00");
    }
    let once = || {
        let t = Instant::now();
        let r = with_read(&db, model_slurp_digest)
            .unwrap_or_else(|e| crate::check::query_failed(&format!("reload: {e}")));
        (ms(t), r)
    };
    let (_, first) = once();
    let mut spans = vec![];
    for _ in 0..5 {
        let (m, r) = once();
        check(r == first, "two reloads of one file read different content");
        spans.push(m);
    }
    let classes = with_read(&db, |c| {
        Ok(
            c.query_row("SELECT count(*) FROM entity_class_current", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    })
    .unwrap();
    crate::check::check_rows("classes", classes as usize, 5);
    Sample::new(mean(&spans))
        .with("rows", first.0)
        .hash("content", &first.1)
}
