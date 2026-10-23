//! History-free extract benchmark: what the extract buys over the source and,
//! honestly, over a COMPACTED source (compaction already reclaims the free
//! blocks of old checkpoints; the extract's own gain is the dropped history).
//!
//! Source, built through the core write path (`api::write_parameter`):
//!   - `ARRAYS` entities × `VERSIONS` versions of an 8 760-cell float array
//!     (one commit per version, all entities per commit), so the dead
//!     fraction of the array tier is (VERSIONS − 1) / VERSIONS;
//!   - `LONGS` entities × `VERSIONS` versions of a `LONG_CELLS`-cell ragged
//!     int series (long tier; one commit per entity-version).
//!
//! Three files are measured: the source as written, a compacted copy of it,
//! and its head extract. Per file: size, RO open + commit cookie (median of
//! `REPS` fresh per-operation connections), a full current-state long-tier
//! scan, and an array point read (one entity's payload by its key). Plus the
//! wall time of the compaction and of the extract.
//!
//! Run: `cargo bench -p nodal-db-core --bench extract` (≈ a few minutes;
//! `-- --quick` for a smaller source). Results: `benches/RESULTS.md`,
//! section "History-free extract". `test = false`: never part of
//! `cargo test --workspace`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use duckdb::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{AccessMode, Config, Connection};

use nodal_db_core::api::write_parameter;
use nodal_db_core::compaction::compact;
use nodal_db_core::extract::{extract, ExtractOpts};
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::WriteOptions;

const CELLS: usize = 8760;
const REPS: usize = 15;

struct Size {
    arrays: usize,
    longs: usize,
    long_cells: usize,
    versions: usize,
}

fn array_batch(entities: &[String], version: usize) -> RecordBatch {
    let n = entities.len() * CELLS;
    let mut ent = Vec::with_capacity(n);
    let mut t = Vec::with_capacity(n);
    let mut v = Vec::with_capacity(n);
    for (k, e) in entities.iter().enumerate() {
        for h in 0..CELLS {
            ent.push(e.as_str());
            t.push(h as i64);
            // Varying, non-repeating payload (no free compression win).
            v.push(((k * 7919 + h * 31 + version * 104_729) % 1_000_003) as f64 * 0.137);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ent)) as ArrayRef,
            Arc::new(Int64Array::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(v)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// A ragged int series (int measure ⇒ long tier): every other position.
fn long_batch(entity: &str, cells: usize, version: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("t", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity; cells])) as ArrayRef,
            Arc::new(Int64Array::from(
                (0..cells as i64).map(|i| i * 2).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                (0..cells as i64)
                    .map(|i| i * 3 + version as i64)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write(db: &Path, class: &str, param: &str, batch: RecordBatch) {
    write_parameter(
        db,
        class,
        param,
        "Base",
        batch,
        &[],
        WriteOptions::default(),
        "bench",
        "extract bench",
    )
    .expect("write_parameter");
}

fn build_source(dir: &Path, s: &Size) -> PathBuf {
    let db = dir.join("source.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    let arrays: Vec<String> = (0..s.arrays).map(|i| format!("a{i}")).collect();
    for v in 0..s.versions {
        write(&db, "Unit", "profile", array_batch(&arrays, v));
        for i in 0..s.longs {
            write(
                &db,
                "Node",
                "ragged",
                long_batch(&format!("l{i}"), s.long_cells, v),
            );
        }
    }
    db
}

fn ro(db: &Path) -> Connection {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    Connection::open_with_flags(db, config).unwrap()
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn ms(d: Duration) -> String {
    format!("{:.1} ms", d.as_secs_f64() * 1e3)
}

fn mib(b: u64) -> String {
    format!("{:.1} MiB", b as f64 / (1024.0 * 1024.0))
}

struct Row {
    size: u64,
    open_cookie: Duration,
    long_scan: Duration,
    array_point: Duration,
}

fn measure(db: &Path) -> Row {
    let size = std::fs::metadata(db).unwrap().len();
    // RO open + commit cookie, per-operation (the editor's poll shape).
    let open_cookie = median(
        (0..REPS)
            .map(|_| {
                let t = Instant::now();
                let c = ro(db);
                let _: i64 = c
                    .query_row("SELECT max(id) FROM \"commit\"", [], |r| r.get(0))
                    .unwrap();
                c.close().unwrap();
                t.elapsed()
            })
            .collect(),
    );
    let c = ro(db);
    // Warm once, then time.
    let long_q = "SELECT count(*), sum(v_int) FROM value_long_current";
    let _: i64 = c.query_row(long_q, [], |r| r.get(0)).unwrap();
    let long_scan = median(
        (0..REPS)
            .map(|_| {
                let t = Instant::now();
                let _: i64 = c.query_row(long_q, [], |r| r.get(0)).unwrap();
                t.elapsed()
            })
            .collect(),
    );
    let point_q = "SELECT len(va.\"values\") FROM value_array va \
                   JOIN entity e ON e.id = va.entity_id AND e.valid_to_commit IS NULL \
                   WHERE e.name = 'a7'";
    let _: i64 = c.query_row(point_q, [], |r| r.get(0)).unwrap();
    let array_point = median(
        (0..REPS)
            .map(|_| {
                let t = Instant::now();
                let n: i64 = c.query_row(point_q, [], |r| r.get(0)).unwrap();
                assert_eq!(n as usize, CELLS);
                t.elapsed()
            })
            .collect(),
    );
    c.close().unwrap();
    Row {
        size,
        open_cookie,
        long_scan,
        array_point,
    }
}

fn main() {
    let quick = std::env::args().any(|a| a == "--quick");
    let s = if quick {
        Size {
            arrays: 20,
            longs: 4,
            long_cells: 168,
            versions: 5,
        }
    } else {
        Size {
            arrays: 100,
            longs: 10,
            long_cells: 336,
            versions: 10,
        }
    };
    let dir = tempfile::tempdir().unwrap();
    println!(
        "source: {} arrays x {} versions x {CELLS} cells; {} long series x {} cells x {} versions",
        s.arrays, s.versions, s.longs, s.long_cells, s.versions
    );
    let t = Instant::now();
    let src = build_source(dir.path(), &s);
    println!("built in {:.1} s", t.elapsed().as_secs_f64());

    let compacted = dir.path().join("compacted.duckdb");
    std::fs::copy(&src, &compacted).unwrap();
    let t = Instant::now();
    compact(&compacted).unwrap();
    let t_compact = t.elapsed();

    let slim = dir.path().join("extract.duckdb");
    let t = Instant::now();
    let r = extract(
        &src,
        &slim,
        &ExtractOpts {
            as_of: None,
            user: "bench",
            note: None,
        },
    )
    .unwrap();
    let t_extract = t.elapsed();

    let rows = [
        ("source", measure(&src)),
        ("compacted source", measure(&compacted)),
        ("head extract", measure(&slim)),
    ];
    println!();
    println!("| file | size | RO open + cookie | current long scan | array point read |");
    println!("|---|---|---|---|---|");
    for (name, m) in &rows {
        println!(
            "| {name} | {} | {} | {} | {} |",
            mib(m.size),
            ms(m.open_cookie),
            ms(m.long_scan),
            ms(m.array_point)
        );
    }
    let c = &rows[1].1;
    let e = &rows[2].1;
    println!();
    println!(
        "compaction {:.1} s; extract {:.1} s; extract vs compacted: size {:.1}x smaller; \
         rows left out {}, axes dropped {}",
        t_compact.as_secs_f64(),
        t_extract.as_secs_f64(),
        c.size as f64 / e.size as f64,
        r.rows_removed,
        r.axes_dropped
    );
}
