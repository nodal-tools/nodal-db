//! Result checks, digests and fault injection for the gate modes
//! (ab_ratio_gates_PLAN §3.4, critique M4; §9 items 8-9).
//!
//! Every gate mode returns a [`Sample`]: the timed value and a digest of
//! the *logical* result (names and values, never surrogate ids). `main`
//! prints the digest as `digest k=v ...` and the value on the last line;
//! the runner requires the two arms' digests to be equal on every sample.
//!
//! A failed fixed check prints `CHECK FAILED: ...` and exits with
//! [`CHECK_FAILED_EXIT`]; the runner turns that into WRONG RESULT (exit 4),
//! never a ratio. A query or core call that errors instead prints
//! `QUERY FAILED: ...` and exits with [`QUERY_FAILED_EXIT`] (ERROR, exit 3).
//!
//! `NODAL_AB_INJECT=<mode>:<spec>` (set by the runner for the candidate arm
//! in `--self-test` only): `<factor>` (e.g. `1.15`) busy-spins (factor - 1)
//! x the measured value right after the timed region and adds the spin's
//! measured duration to it (a regression without a code change); `wrong`
//! makes the mode's row-count checks see one row too many (the fixed check
//! fails); `wrongvalue` perturbs the digest (a right count with a wrong
//! value, caught only by the cross-arm digest).

use std::fmt::Write as _;
use std::sync::OnceLock;
use std::time::Instant;

use duckdb::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, StringArray,
    TimestampMicrosecondArray,
};
use duckdb::arrow::datatypes::{DataType, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use nodal_db_core::duckdb;
use nodal_db_core::duckdb::Connection;

pub const CHECK_FAILED_EXIT: i32 = 86;
/// A query, API call or the CLI failed (an error, not a wrong result).
pub const QUERY_FAILED_EXIT: i32 = 87;

static MODE: OnceLock<String> = OnceLock::new();
static SAMPLE_DIR: OnceLock<std::path::PathBuf> = OnceLock::new();

/// The sample's temp directory: removed before an early exit (an exit
/// skips the `TempDir` destructor and would leak fixture copies).
pub fn set_sample_dir(dir: &std::path::Path) {
    let _ = SAMPLE_DIR.set(dir.to_path_buf());
}

fn cleanup() {
    if let Some(d) = SAMPLE_DIR.get() {
        let _ = std::fs::remove_dir_all(d);
    }
}

pub fn set_mode(mode: &str) {
    let _ = MODE.set(mode.to_string());
}

/// The injection spec for the running mode, if any.
pub fn injection() -> Option<String> {
    let v = std::env::var("NODAL_AB_INJECT").ok()?;
    let (mode, spec) = v.split_once(':')?;
    (Some(mode) == MODE.get().map(String::as_str)).then(|| spec.to_string())
}

pub fn fail(msg: &str) -> ! {
    eprintln!("CHECK FAILED: {msg}");
    cleanup();
    std::process::exit(CHECK_FAILED_EXIT)
}

/// A read-back query, a core call or the CLI errored: the result cannot be
/// checked. The runner reports it as ERROR (exit 3), with this message.
pub fn query_failed(msg: &str) -> ! {
    eprintln!("QUERY FAILED: {msg}");
    cleanup();
    std::process::exit(QUERY_FAILED_EXIT)
}

/// A fixed check on a row (or value) count; `wrong` injection adds one.
pub fn check_rows(what: &str, got: usize, want: usize) {
    let got = if injection().as_deref() == Some("wrong") {
        got + 1
    } else {
        got
    };
    if got != want {
        fail(&format!("{what}: {got} rows, expected {want}"));
    }
}

pub fn check(cond: bool, what: &str) {
    if !cond {
        fail(what);
    }
}

/// One measurement and the digest of its logical result.
pub struct Sample {
    pub ms: f64,
    pub digest: Vec<(String, String)>,
}

impl Sample {
    pub fn new(ms: f64) -> Self {
        Sample { ms, digest: vec![] }
    }

    pub fn with(mut self, key: &str, value: impl std::fmt::Display) -> Self {
        self.digest.push((key.to_string(), value.to_string()));
        self
    }

    pub fn hash(self, key: &str, h: &RowHash) -> Self {
        self.with(&format!("{key}_rows"), h.rows)
            .with(&format!("{key}_hash"), format!("{:016x}", h.sum))
    }

    /// Print the digest line and the value (last line), applying any
    /// injection for this mode.
    pub fn emit(mut self) {
        match injection().as_deref() {
            Some("wrongvalue") => {
                if let Some(last) = self.digest.last_mut() {
                    last.1.push_str("~injected");
                } else {
                    self.digest.push(("injected".into(), "wrongvalue".into()));
                }
            }
            Some("wrong") | None => {}
            Some(f) => {
                let factor: f64 = f.parse().expect("NODAL_AB_INJECT factor");
                self.ms += spin_ms((factor - 1.0) * self.ms);
            }
        }
        let mut line = String::from("digest");
        for (k, v) in &self.digest {
            let _ = write!(line, " {k}={v}");
        }
        println!("{line}");
        println!("{:.6}", self.ms);
    }
}

/// Busy-spin for `ms` and return the measured duration.
fn spin_ms(ms: f64) -> f64 {
    let t = Instant::now();
    let mut x = 0u64;
    while t.elapsed().as_secs_f64() * 1e3 < ms {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        std::hint::black_box(x);
    }
    t.elapsed().as_secs_f64() * 1e3
}

pub fn elapsed_ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

pub fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

pub fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len() as f64
}

// ------------------------------------------------------------ row digest

/// An order-insensitive digest of query results: the row count and the
/// wrapping sum of per-row hashes over the *logical* columns (every column
/// except `id`, `*_id`, `*_commit` and `version`: surrogate ids and
/// physical versions may legitimately differ between the arms' fixtures).
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
pub struct RowHash {
    pub rows: u64,
    pub sum: u64,
}

fn logical(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    !(n == "id" || n.ends_with("_id") || n.ends_with("_commit") || n == "version")
}

const FNV: u64 = 0xcbf2_9ce4_8422_2325;

fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn cell(col: &ArrayRef, i: usize) -> u64 {
    if col.is_null(i) {
        return 0x6e75_6c6c;
    }
    let any = col.as_any();
    let h = match col.data_type() {
        DataType::Utf8 => fnv(
            FNV,
            any.downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .as_bytes(),
        ),
        DataType::Float64 => {
            let v = any.downcast_ref::<Float64Array>().unwrap().value(i);
            // -0.0 == 0.0 logically.
            fnv(
                FNV,
                &(if v == 0.0 { 0.0f64 } else { v }).to_bits().to_le_bytes(),
            )
        }
        DataType::Int64 => fnv(
            FNV,
            &any.downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int32 => fnv(
            FNV,
            &(any.downcast_ref::<Int32Array>().unwrap().value(i) as i64).to_le_bytes(),
        ),
        DataType::Boolean => fnv(
            FNV,
            &[any.downcast_ref::<BooleanArray>().unwrap().value(i) as u8],
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => fnv(
            FNV,
            &any.downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::List(_) => {
            let inner = any.downcast_ref::<ListArray>().unwrap().value(i);
            let mut h = fnv(FNV, &(inner.len() as u64).to_le_bytes());
            for j in 0..inner.len() {
                h = fnv(h, &cell(&inner, j).to_le_bytes());
            }
            h
        }
        _ => fnv(
            FNV,
            duckdb::arrow::util::display::array_value_to_string(col, i)
                .unwrap_or_default()
                .as_bytes(),
        ),
    };
    mix(h)
}

impl RowHash {
    pub fn add(&mut self, batches: &[RecordBatch]) {
        for b in batches {
            let schema = b.schema();
            // Keyed by the position among the *logical* columns: a
            // surrogate column (excluded) added to a view in one arm must
            // not shift the columns after it into a false WRONG RESULT.
            // (Not by name: an engine bump may rename expression columns.)
            let cols: Vec<&ArrayRef> = b
                .columns()
                .iter()
                .enumerate()
                .filter(|(k, _)| logical(schema.field(*k).name()))
                .map(|(_, c)| c)
                .collect();
            for i in 0..b.num_rows() {
                let mut h = FNV;
                for (k, col) in cols.iter().enumerate() {
                    h = fnv(h, &(k as u64).to_le_bytes());
                    h = fnv(h, &cell(col, i).to_le_bytes());
                }
                self.sum = self.sum.wrapping_add(mix(h));
                self.rows += 1;
            }
        }
    }

    pub fn of(batches: &[RecordBatch]) -> Self {
        let mut h = RowHash::default();
        h.add(batches);
        h
    }
}

// --------------------------------------------------------------- queries

pub fn batches(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>, duckdb::Error> {
    let mut stmt = conn.prepare(sql)?;
    let arrow = stmt.query_arrow([])?;
    Ok(arrow.collect())
}

pub fn rows(b: &[RecordBatch]) -> usize {
    b.iter().map(|x| x.num_rows()).sum()
}

/// The head commit id (0 on an empty history), on its own RO connection.
pub fn head(db: &std::path::Path) -> i64 {
    nodal_db_core::conn::with_read(db, |c| {
        Ok(
            c.query_row("SELECT COALESCE(max(id), 0) FROM \"commit\"", [], |r| {
                r.get::<_, i64>(0)
            })?,
        )
    })
    .expect("head commit")
}

/// A single integer from `sql`, on its own RO connection.
pub fn scalar_i64(db: &std::path::Path, sql: &str) -> i64 {
    nodal_db_core::conn::with_read(db, |c| Ok(c.query_row(sql, [], |r| r.get::<_, i64>(0))?))
        .unwrap_or_else(|e| query_failed(&format!("{sql}: {e}")))
}

/// The digest of `sql`'s result, on its own RO connection.
pub fn read_hash(db: &std::path::Path, sql: &str) -> RowHash {
    nodal_db_core::conn::with_read(db, |c| Ok(RowHash::of(&batches(c, sql)?)))
        .unwrap_or_else(|e| query_failed(&format!("{sql}: {e}")))
}

/// `'a', 'b', ...` for an `IN (...)` list (names are plain ASCII here).
pub fn in_list(names: &[String]) -> String {
    names
        .iter()
        .map(|n| format!("'{}'", n.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Check that exactly `n` commits were made since `before`.
pub fn check_commits(db: &std::path::Path, before: i64, n: i64) {
    let after = head(db);
    check(
        after - before == n,
        &format!("expected {n} new commit(s), head went {before} -> {after}"),
    );
}
