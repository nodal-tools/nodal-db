//! The Arrow-IPC import metric (`imp_ipc_dense`): the arm's own
//! `nodal-db import` binary (`NODAL_AB_CLI`) on one shared dense-form
//! container (`NODAL_AB_IPC`, built once by `build_ipc_dense`), wall time
//! including the process spawn (results_path lane (b)). The 87.6 M-value
//! import is not a gate metric (critique R1): ~50 s a sample.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, ListArray, StringArray, TimestampMicrosecondArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use nodal_db_core::schema_init::init_schema;

use crate::check::{check_commits, check_rows, elapsed_ms, head, read_hash, Sample};

const IPC_CLASS: &str = "res_dense";
const IPC_PARAM: &str = "flow00";
const T0_US: i64 = 1_735_689_600 * 1_000_000;
const HOUR_US: i64 = 3_600 * 1_000_000;
const CELLS: usize = 8760;

/// Entities of the IPC container at a scale: full 1 000 (8.76 M values,
/// ~5 s an import), quick 200.
pub fn entities_for(scale: &str) -> usize {
    match scale {
        "full" => 1_000,
        _ => 200,
    }
}

fn ipc_entities() -> usize {
    entities_for(&std::env::var("NODAL_AB_SCALE").unwrap_or_else(|_| "quick".into()))
}

fn ival(e: usize, h: usize) -> f64 {
    let mut z = ((e as u64) << 20 ^ h as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) % 2_000_000) as f64 / 1000.0 - 1000.0
}

/// `build_ipc_dense <path> <entities>`: a one-segment dense-form bulk
/// container (docs: arrow_import_format; results_path's dense shape), one
/// parameter over `entities` x 8 760 hourly float cells, batches of 100.
pub fn build_ipc_dense(path: &Path, entities: usize) {
    let ts_item = Arc::new(Field::new(
        "item",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    ));
    let val_item = Arc::new(Field::new("item", DataType::Float64, false));
    let md = std::collections::HashMap::from([
        ("spinal:format".to_string(), "spinal-db-bulk".to_string()),
        ("spinal:format_version".into(), "1.0".into()),
        ("spinal:segment_count".into(), "1".into()),
        ("spinal:class".into(), IPC_CLASS.into()),
        ("spinal:parameter".into(), IPC_PARAM.into()),
        ("spinal:alternative".into(), "Base".into()),
        ("spinal:form".into(), "dense".into()),
    ]);
    let schema = Arc::new(
        Schema::new(vec![
            Field::new("entity", DataType::Utf8, false),
            Field::new("time", DataType::List(ts_item.clone()), false),
            Field::new("value", DataType::List(val_item.clone()), false),
        ])
        .with_metadata(md),
    );
    let mut file = std::fs::File::create(path).expect("container create");
    let mut w = StreamWriter::try_new(&mut file, &schema).expect("stream writer");
    let mut lo = 0;
    while lo < entities {
        let hi = (lo + 100).min(entities);
        let rows = hi - lo;
        let ent: Vec<String> = (lo..hi).map(|e| format!("e{e:04}")).collect();
        let mut ts = Vec::with_capacity(rows * CELLS);
        let mut vals = Vec::with_capacity(rows * CELLS);
        for e in lo..hi {
            for h in 0..CELLS {
                ts.push(T0_US + h as i64 * HOUR_US);
                vals.push(ival(e, h));
            }
        }
        let offsets = OffsetBuffer::<i32>::from_lengths(std::iter::repeat_n(CELLS, rows));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ent)) as ArrayRef,
                Arc::new(ListArray::new(
                    ts_item.clone(),
                    offsets.clone(),
                    Arc::new(TimestampMicrosecondArray::from(ts)) as ArrayRef,
                    None,
                )) as ArrayRef,
                Arc::new(ListArray::new(
                    val_item.clone(),
                    offsets,
                    Arc::new(Float64Array::from(vals)) as ArrayRef,
                    None,
                )) as ArrayRef,
            ],
        )
        .unwrap();
        w.write(&batch).expect("ipc write");
        lo = hi;
    }
    w.finish().expect("ipc finish");
}

/// The arm's own `nodal-db import` on the shared container, wall time
/// including the process spawn (results_path lane (b)).
pub fn import_dense(dir: &Path) -> Sample {
    let cli = std::env::var("NODAL_AB_CLI").expect("set NODAL_AB_CLI to the arm's nodal-db");
    let container =
        std::env::var("NODAL_AB_IPC").expect("set NODAL_AB_IPC to a build_ipc_dense file");
    let entities = ipc_entities();
    let db = dir.join("import.duckdb");
    init_schema(&db).expect("schema init");
    let before = head(&db);
    let t = Instant::now();
    let out = Command::new(&cli)
        .args([
            "import",
            &container,
            "--db",
            db.to_str().unwrap(),
            "--user",
            "ab",
            "--message",
            "ab import",
        ])
        .output()
        .expect("run nodal-db import");
    let ms = elapsed_ms(t);
    if !out.status.success() {
        crate::check::query_failed(&format!(
            "nodal-db import failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    check_commits(&db, before, 1);
    let sql = format!(
        "SELECT e.name AS entity, d.values FROM read_parameter_dense('{IPC_CLASS}', '{IPC_PARAM}') d \
         JOIN entity e ON e.id = d.entity_id"
    );
    let h = read_hash(&db, &sql);
    check_rows("imported dense rows", h.rows as usize, entities);
    Sample::new(ms).hash("readback", &h)
}
