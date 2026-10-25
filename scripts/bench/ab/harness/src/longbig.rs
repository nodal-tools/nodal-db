//! Large-long-value modes (skip_unchanged_writes_PLAN P2, §6.4): one sparse
//! 2-D label x label value of ~4.6 M cells (8 760 x 840 at 62 % fill, the
//! scale study's realistic shape), written through the API as the importer
//! and the editor do. The base file is built once with
//! `ab_harness build_longbig <path>`; the modes run on a COPY of the file
//! named by `NODAL_AB_LONGBIG` and time one whole commit (ms):
//!
//! - `api_longbig_unchanged`: the same value re-written;
//! - `api_longbig_1cell`: one cell changed (the headline editor/import case);
//! - `api_longbig_10pct`, `_50pct`, `_90pct`: that share of the cells
//!   changed, spread evenly (the cell-diff crossover, critique C5);
//! - `api_longbig_changed`: every cell changed (the whole-value case).
//!
//! With `NODAL_AB_RSS=1` a mode also prints, to stderr, the process's peak
//! resident set during the timed commit (the high-water mark is reset just
//! before it through `/proc/self/clear_refs`) and the resident set before.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::api::{commit_changes, Segment};
use nodal_db_core::canon::LabelDomain;
use nodal_db_core::duckdb;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

const OUTER: usize = 8760;
const INNER: usize = 840;
const FILL: f64 = 0.62;

fn mix(a: usize, b: usize, c: usize, r: u64) -> u64 {
    let mut z = (a as u64) << 24 ^ (b as u64) << 16 ^ (c as u64) ^ r << 40;
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn outer_labels() -> Vec<String> {
    (0..OUTER).map(|i| format!("t{i:05}")).collect()
}

fn inner_labels() -> Vec<String> {
    (0..INNER).map(|i| format!("r{i:04}")).collect()
}

/// The value's cells as a batch; `changed(cell index)` cells get the
/// round-1 value, the rest the round-0 value.
fn batch(changed: impl Fn(usize) -> bool) -> (RecordBatch, Vec<String>, Vec<String>) {
    let ol = outer_labels();
    let il = inner_labels();
    let thr = (FILL * 1_000_000.0) as u64;
    let (mut d1, mut d2, mut v) = (Vec::new(), Vec::new(), Vec::new());
    let mut n = 0usize;
    for (oi, o) in ol.iter().enumerate() {
        for (ii, i) in il.iter().enumerate() {
            if mix(oi, 7, ii, 0) % 1_000_000 >= thr && !(oi == 0 && ii == 0) {
                continue;
            }
            let r = u64::from(changed(n));
            d1.push(o.as_str());
            d2.push(i.as_str());
            v.push((mix(oi, 8, ii, r) % 1_000_000) as f64 / 1000.0);
            n += 1;
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Utf8, false),
        Field::new("rp", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let b = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["rp"; n])) as ArrayRef,
            Arc::new(StringArray::from(d1)) as ArrayRef,
            Arc::new(StringArray::from(d2)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap();
    (b, ol, il)
}

fn segment(changed: impl Fn(usize) -> bool) -> Segment {
    let (b, ol, il) = batch(changed);
    Segment {
        class: "timeset".into(),
        parameter: "weights".into(),
        alternative: "Base".into(),
        batches: vec![b],
        domains: vec![
            LabelDomain {
                position: 1,
                labels: ol,
            },
            LabelDomain {
                position: 2,
                labels: il,
            },
        ],
    }
}

fn commit(db: &Path, seg: Segment) -> f64 {
    let rss = std::env::var_os("NODAL_AB_RSS").is_some();
    let before = if rss {
        let _ = std::fs::write("/proc/self/clear_refs", "5");
        status_kib("VmRSS")
    } else {
        0
    };
    let t = Instant::now();
    commit_changes(
        db,
        vec![seg],
        vec![],
        vec![],
        vec![],
        "ab",
        "longbig",
        WriteOptions::default(),
    )
    .unwrap();
    let ms = t.elapsed().as_secs_f64() * 1e3;
    if rss {
        eprintln!(
            "rss_before_MiB={:.0} peak_MiB={:.0}",
            before as f64 / 1024.0,
            status_kib("VmHWM") as f64 / 1024.0
        );
    }
    ms
}

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(field))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

/// Build the base file: the round-0 value in one commit.
pub fn build(path: &Path) {
    let t = Instant::now();
    init_schema_with_default(path, Some("Base")).unwrap();
    let ms = commit(path, segment(|_| false));
    eprintln!(
        "built {} in {:.1} s (commit {:.1} s)",
        path.display(),
        t.elapsed().as_secs_f64(),
        ms / 1e3
    );
}

/// Run a `api_longbig_*` mode on a copy of `NODAL_AB_LONGBIG` in `dir`.
pub fn run(mode: &str, dir: &Path) -> Option<f64> {
    let changed: fn(usize) -> bool = match mode {
        "api_longbig_unchanged" => |_| false,
        "api_longbig_1cell" => |i| i == 1_234_567,
        "api_longbig_10pct" => |i| i % 10 == 3,
        "api_longbig_50pct" => |i| i % 2 == 1,
        "api_longbig_90pct" => |i| i % 10 != 3,
        "api_longbig_changed" => |_| true,
        _ => return None,
    };
    let base =
        std::env::var("NODAL_AB_LONGBIG").expect("set NODAL_AB_LONGBIG to a build_longbig file");
    let db = dir.join("longbig.duckdb");
    std::fs::copy(base, &db).expect("copy longbig");
    let seg = segment(changed);
    Some(commit(&db, seg))
}

/// `wr_longbig_1cell` (gate): one cell of the ~4.6 M-cell value changed,
/// one whole commit on a copy of `NODAL_AB_LONGBIG`. Checks: one commit;
/// the current cell count is unchanged and the changed cell reads back
/// with its new value; the digest is the cell count and an exact integer
/// checksum of the current values (values are multiples of 1/1000).
pub fn gate_1cell(dir: &Path) -> crate::check::Sample {
    use crate::check::{check_commits, check_rows, head, Sample};
    let base =
        std::env::var("NODAL_AB_LONGBIG").expect("set NODAL_AB_LONGBIG to a build_longbig file");
    let db = dir.join("longbig.duckdb");
    std::fs::copy(base, &db).expect("copy longbig");
    let target = 1_234_567;
    let (b, _, _) = batch(|i| i == target);
    let cells = b.num_rows();
    let new_v = b
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(target);
    let seg = segment(|i| i == target);
    let before = head(&db);
    let ms = commit(&db, seg);
    check_commits(&db, before, 1);
    let sql = format!(
        "SELECT count(*), sum(round(v_float * 1000)::BIGINT)::BIGINT, \
                count(*) FILTER (WHERE round(v_float * 1000)::BIGINT = {}) \
         FROM read_parameter('timeset', 'weights')",
        (new_v * 1000.0).round() as i64
    );
    let (count, sum, hit) = nodal_db_core::conn::with_read(&db, |c| {
        Ok(c.query_row(&sql, [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?)
    })
    .unwrap_or_else(|e| crate::check::query_failed(&format!("read-back: {e}")));
    check_rows("current cells", count as usize, cells);
    crate::check::check(hit >= 1, "the changed cell does not read back");
    Sample::new(ms)
        .with("cells", count)
        .with("value_sum_milli", sum)
}
