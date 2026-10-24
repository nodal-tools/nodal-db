//! The long-tier cell diff (skip_unchanged_writes_PLAN §4, P2) against an
//! independent model of per-cell history, plus targeted cases.
//!
//! The randomized test drives seeded sequences of direct `write_value` /
//! `delete_value` commits over a few keys: cell edits, adds and removes on
//! the same axes, identical rewrites, axis and kind changes (whole-value
//! rewrites), transitions to and from the scalar and array tiers, deletes
//! and rewrites, with and without content retention, interleaved with
//! commits that leave the long keys alone. The model keeps one row per
//! stored cell version (its span, version and whether a stub replaced its
//! content), applying the plan's rule: on the same axes, kind and binding a
//! changed cell closes and re-opens at its version + 1, a removed one
//! closes, an added one opens at its position's max version + 1, an equal
//! one stays open; anything else closes every current cell and opens every
//! new one. After the run, for EVERY commit c, the as-of reads must equal
//! the model's rows valid at c, the rows closed at c must be exactly the
//! model's, and `value_timeline`'s long leg must hold one span per change
//! of the value; after every commit the audits must be clean.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use duckdb::Connection;

use nodal_db_core::axis::AxisCache;
use nodal_db_core::canon::CanonicalValue;
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::encoding::{AxisBody, AxisProperties, CanonicalAxis};
use nodal_db_core::history::{delete_value, write_value};
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::types::{
    LongRow, ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions, WriteOutcome,
};

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

fn int_axis(len: i32) -> CanonicalAxis {
    CanonicalAxis {
        length: len,
        properties: AxisProperties::default(),
        body: AxisBody::IntegerParametric { start: 0, step: 1 },
    }
}

/// A long value's shape: the first axis length and an optional second.
type Shape = (i32, Option<i32>);
type Pos = [i32; 2];

/// One logical value of a key, as the model holds it.
#[derive(Clone, Debug, PartialEq)]
enum Val {
    /// Cells by position: the payload as `i64` (an int) or float bits.
    Long {
        shape: Shape,
        int: bool,
        cells: BTreeMap<Pos, i64>,
    },
    Array(Vec<u64>),
    Scalar(u64),
}

fn scalar_of(int: bool, x: i64) -> ScalarValue {
    if int {
        ScalarValue::Int(x)
    } else {
        ScalarValue::Float(f64::from_bits(x as u64))
    }
}

fn canonical(v: &Val) -> CanonicalValue {
    match v {
        Val::Long { shape, int, cells } => {
            let mut axes = vec![int_axis(shape.0)];
            if let Some(l) = shape.1 {
                axes.push(int_axis(l));
            }
            CanonicalValue {
                tier: Tier::Long,
                value_kind: if *int {
                    ValueKind::Int
                } else {
                    ValueKind::Float
                },
                axes,
                shape: ShapedData::Long(
                    cells
                        .iter()
                        .map(|(p, &x)| LongRow {
                            positions: match shape.1 {
                                Some(_) => vec![p[0], p[1]],
                                None => vec![p[0]],
                            },
                            value: scalar_of(*int, x),
                        })
                        .collect(),
                ),
            }
        }
        Val::Array(xs) => CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(xs.len() as i32)],
            shape: ShapedData::Array(xs.iter().map(|&b| f64::from_bits(b)).collect()),
        },
        Val::Scalar(b) => CanonicalValue {
            tier: Tier::Scalar,
            value_kind: ValueKind::Float,
            axes: vec![],
            shape: ShapedData::Scalar(ScalarValue::Float(f64::from_bits(*b))),
        },
    }
}

// ---------------------------------------------------------------------------
// The model: one row per stored long cell version
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct CellRow {
    pos: Pos,
    payload: i64,
    int: bool,
    shape: Shape,
    version: i16,
    from: i64,
    to: Option<i64>,
    stub: bool,
}

#[derive(Default)]
struct KeyModel {
    /// The current logical value (`None`: no value).
    cur: Option<Val>,
    /// Every long cell row ever written for the key.
    rows: Vec<CellRow>,
    /// The logical value right after each commit, by commit id.
    at: BTreeMap<i64, Option<Val>>,
    /// Long rows the model closed at each commit.
    closed: BTreeMap<i64, usize>,
}

impl KeyModel {
    fn open_rows(&mut self) -> impl Iterator<Item = &mut CellRow> {
        self.rows.iter_mut().filter(|r| r.to.is_none())
    }

    fn max_version(&self, p: Pos) -> i16 {
        self.rows
            .iter()
            .filter(|r| r.pos == p)
            .map(|r| r.version)
            .max()
            .unwrap_or(0)
    }

    /// Apply a write of `new` (`None`: a delete) at commit `c`.
    fn apply(&mut self, c: i64, new: Option<&Val>, retained: bool) {
        let mut closed = 0usize;
        let mut close = |r: &mut CellRow| {
            r.to = Some(c);
            r.stub = !retained;
            closed += 1;
        };
        match (&self.cur, new) {
            (
                Some(Val::Long {
                    shape: s0, int: i0, ..
                }),
                Some(Val::Long { shape, int, cells }),
            ) if s0 == shape && i0 == int => {
                // Same axes and kind: the cell diff.
                let mut keep = Vec::new();
                for r in self.rows.iter_mut().filter(|r| r.to.is_none()) {
                    if cells.get(&r.pos) == Some(&r.payload) {
                        keep.push(r.pos);
                    } else {
                        close(r);
                    }
                }
                for (&p, &x) in cells {
                    if !keep.contains(&p) {
                        let version = self.max_version(p) + 1;
                        self.rows.push(CellRow {
                            pos: p,
                            payload: x,
                            int: *int,
                            shape: *shape,
                            version,
                            from: c,
                            to: None,
                            stub: false,
                        });
                    }
                }
            }
            (_, new) => {
                // Whole value (other axes or kind, a tier change, a
                // delete): every current cell closes, every new cell opens.
                for r in self.open_rows() {
                    close(r);
                }
                if let Some(Val::Long { shape, int, cells }) = new {
                    {
                        for (&p, &x) in cells {
                            let version = self.max_version(p) + 1;
                            self.rows.push(CellRow {
                                pos: p,
                                payload: x,
                                int: *int,
                                shape: *shape,
                                version,
                                from: c,
                                to: None,
                                stub: false,
                            });
                        }
                    }
                }
            }
        }
        // An identical non-long write, or an unchanged long one, closes
        // nothing (handled above: equal values keep every row).
        *self.closed.entry(c).or_default() += closed;
        if let Some(v) = new {
            self.cur = Some(v.clone());
        } else {
            self.cur = None;
        }
    }

    /// The model's long rows valid at `c`, as the as-of read renders them.
    fn long_at(&self, c: i64) -> Vec<String> {
        let mut v: Vec<String> = self
            .rows
            .iter()
            .filter(|r| r.from <= c && r.to.is_none_or(|t| t > c))
            .map(|r| {
                format!(
                    "{:?} v{} {:?} {}",
                    r.pos,
                    r.version,
                    r.shape,
                    if r.stub {
                        "STUB".to_string()
                    } else if r.int {
                        format!("int:{}", r.payload)
                    } else {
                        format!("f:{:x}", r.payload as u64)
                    }
                )
            })
            .collect();
        v.sort();
        v
    }

    /// The commits at which the key's long value changed, in order (the
    /// starts of `value_timeline`'s long spans).
    fn long_span_starts(&self) -> Vec<i64> {
        let mut starts = Vec::new();
        let mut prev: Option<&Val> = None;
        for (&c, v) in &self.at {
            let long = v.as_ref().filter(|v| matches!(v, Val::Long { .. }));
            // Only a change of the stored long rows starts a span.
            let changed = match (prev, long) {
                (_, None) => false,
                (Some(a), Some(b)) => a != b,
                (None, Some(_)) => true,
            };
            if changed {
                starts.push(c);
            }
            prev = long;
        }
        starts
    }
}

// ---------------------------------------------------------------------------
// Database reads
// ---------------------------------------------------------------------------

fn key(e: i64) -> ValueKey {
    ValueKey {
        entity_id: e,
        parameter_definition_id: 1,
        alternative_id: 1,
    }
}

fn fresh_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.duckdb");
    init_schema(&path).unwrap();
    (dir, path)
}

fn db_long_at(conn: &Connection, c: i64, e: i64) -> Vec<String> {
    let mut stmt = conn
        .prepare(
            "SELECT l.i1_pos, l.i2_pos, l.version, a1.length, a2.length, l.content_retained, \
                    l.value_kind, l.v_int, l.v_float \
             FROM value_long_asof(?) l JOIN axis a1 ON a1.id = l.axis1_id \
             LEFT JOIN axis a2 ON a2.id = l.axis2_id WHERE l.entity_id = ?",
        )
        .unwrap();
    let mut v: Vec<String> = stmt
        .query_map(duckdb::params![c, e], |r| {
            let pos: Pos = [r.get(0)?, r.get(1)?];
            let version: i16 = r.get(2)?;
            let shape: Shape = (r.get(3)?, r.get(4)?);
            let retained: bool = r.get(5)?;
            let kind: String = r.get(6)?;
            let payload = if !retained {
                "STUB".to_string()
            } else if kind == "int" {
                format!("int:{}", r.get::<_, i64>(7)?)
            } else {
                format!("f:{:x}", r.get::<_, f64>(8)?.to_bits())
            };
            Ok(format!("{pos:?} v{version} {shape:?} {payload}"))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    v.sort();
    v
}

/// The key's scalar and array as of `c`: `None` when absent, `Some(None)`
/// for a stub (content not retained).
#[allow(clippy::type_complexity)]
fn db_other_at(
    conn: &Connection,
    c: i64,
    e: i64,
) -> (Option<Option<u64>>, Option<Option<Vec<u64>>>) {
    let s: Option<Option<f64>> = conn
        .query_row(
            "SELECT CASE WHEN content_retained THEN v_float END FROM value_scalar_asof(?) \
             WHERE entity_id = ?",
            duckdb::params![c, e],
            |r| r.get(0),
        )
        .ok();
    let a: Option<Option<String>> = conn
        .query_row(
            "SELECT CASE WHEN content_retained THEN values::VARCHAR END \
             FROM value_array_asof(?) WHERE entity_id = ?",
            duckdb::params![c, e],
            |r| r.get(0),
        )
        .ok();
    let a = a.map(|t| {
        t.map(|t| {
            t.trim_matches(|ch| ch == '[' || ch == ']')
                .split(", ")
                .filter(|s| !s.is_empty())
                .map(|s| s.parse::<f64>().unwrap().to_bits())
                .collect()
        })
    });
    (s.map(|s| s.map(f64::to_bits)), a)
}

const AUDITS: &[&str] = &[
    "audit_history_tiling",
    "audit_position_range",
    "audit_long_value_coherence",
    "audit_cross_tier_duplicate",
    "audit_index_accelerator",
    "audit_array_integrity",
];

fn assert_audits_clean(db: &Path, ctx: &str) {
    let conn = Connection::open(db).unwrap();
    for a in AUDITS {
        let n: i64 = conn
            .query_row(&format!("SELECT count(*) FROM {a}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "{a} must be empty ({ctx})");
    }
    conn.close().unwrap();
}

// ---------------------------------------------------------------------------
// The randomized run
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

fn payload(r: &mut Rng, int: bool) -> i64 {
    let x = r.below(5) as i64;
    if int {
        x
    } else {
        (x as f64 * 0.5).to_bits() as i64
    }
}

fn random_long(r: &mut Rng, shape: Shape, int: bool) -> Val {
    let mut cells = BTreeMap::new();
    for p in all_positions(shape) {
        if r.chance(60) {
            cells.insert(p, payload(r, int));
        }
    }
    if cells.is_empty() {
        cells.insert([0, 0], payload(r, int));
    }
    Val::Long { shape, int, cells }
}

fn all_positions(shape: Shape) -> Vec<Pos> {
    let mut v = Vec::new();
    for a in 0..shape.0 {
        for b in 0..shape.1.unwrap_or(1) {
            v.push([a, b]);
        }
    }
    v
}

fn random_shape(r: &mut Rng) -> Shape {
    (
        [4, 5][r.below(2) as usize],
        [None, Some(2)][r.below(2) as usize],
    )
}

/// The next value of a key whose current value is `cur`.
fn next_value(r: &mut Rng, cur: &Option<Val>) -> Option<Val> {
    let roll = r.below(100);
    match cur {
        Some(Val::Long { shape, int, cells }) if roll < 70 => {
            let (shape, int) = (*shape, *int);
            let mut cells = cells.clone();
            match roll {
                // Edit 1-3 cells (an edit may land on the same payload).
                0..=24 => {
                    let keys: Vec<Pos> = cells.keys().copied().collect();
                    for _ in 0..=r.below(3) {
                        let p = keys[r.below(keys.len() as u64) as usize];
                        cells.insert(p, payload(r, int));
                    }
                }
                // Add cells within the axes.
                25..=34 => {
                    for p in all_positions(shape) {
                        if !cells.contains_key(&p) && r.chance(40) {
                            cells.insert(p, payload(r, int));
                        }
                    }
                }
                // Remove cells (one stays).
                35..=44 => {
                    let keys: Vec<Pos> = cells.keys().copied().collect();
                    for p in keys.iter().skip(1) {
                        if r.chance(40) {
                            cells.remove(p);
                        }
                    }
                }
                // Identical rewrite.
                45..=54 => {}
                // Axis change: the cells move to another shape.
                55..=62 => {
                    let s = (if shape.0 == 4 { 5 } else { 4 }, shape.1);
                    cells.retain(|p, _| p[0] < s.0);
                    if cells.is_empty() {
                        cells.insert([0, 0], payload(r, int));
                    }
                    return Some(Val::Long {
                        shape: s,
                        int,
                        cells,
                    });
                }
                // Kind change with the same positions.
                _ => {
                    for v in cells.values_mut() {
                        *v = if int {
                            (*v as f64).to_bits() as i64
                        } else {
                            f64::from_bits(*v as u64) as i64
                        };
                    }
                    return Some(Val::Long {
                        shape,
                        int: !int,
                        cells,
                    });
                }
            }
            Some(Val::Long { shape, int, cells })
        }
        _ => match roll % 10 {
            0 => None,
            1 => Some(Val::Scalar((r.below(3) as f64).to_bits())),
            2 => Some(Val::Array(
                (0..3).map(|_| (r.below(3) as f64).to_bits()).collect(),
            )),
            3 if cur.is_some() => cur.clone(),
            _ => {
                let s = random_shape(r);
                let int = r.chance(30);
                Some(random_long(r, s, int))
            }
        },
    }
}

fn write(conn: &Connection, c: i64, k: ValueKey, v: Option<&Val>, retained: bool) -> WriteOutcome {
    let opts = WriteOptions {
        force_replace: !retained,
        ..WriteOptions::default()
    };
    match v {
        Some(v) => write_value(
            conn,
            c,
            k,
            &canonical(v),
            &mut AxisCache::new(),
            &opts,
            None,
        )
        .unwrap(),
        None => delete_value(conn, c, k, &opts).unwrap(),
    }
}

fn run_seed(seed: u64, commits: usize) {
    let (_d, db) = fresh_db();
    let mut r = Rng(seed);
    let longs = [1i64, 2];
    let other = 9i64;
    let mut models: BTreeMap<i64, KeyModel> =
        longs.iter().map(|&e| (e, KeyModel::default())).collect();
    let mut commit_ids = Vec::new();
    for step in 0..commits {
        // Which keys this commit writes: one long key, both, or only the
        // other key (a commit that leaves the long keys alone).
        let pick = r.below(10);
        let targets: Vec<i64> = match pick {
            0..=1 => vec![],
            2 => longs.to_vec(),
            _ => vec![longs[r.below(2) as usize]],
        };
        let mut plan: Vec<(i64, Option<Val>, bool)> = Vec::new();
        for &e in &targets {
            let m = &models[&e];
            let next = next_value(&mut r, &m.cur);
            // Deleting an absent value is a no-op the model need not track.
            if next.is_none() && m.cur.is_none() {
                continue;
            }
            plan.push((e, next, !r.chance(20)));
        }
        let other_val = (r.below(4) as f64).to_bits();
        let c = with_write_txn(&db, "t", &format!("step {step}"), |conn, c| {
            for (e, v, retained) in &plan {
                let o = write(conn, c, key(*e), v.as_ref(), *retained);
                // An unchanged long write reports it.
                if let (Some(Val::Long { .. }), Some(Val::Long { .. })) = (&models[e].cur, v) {
                    if models[e].cur.as_ref() == v.as_ref() {
                        assert!(o.unchanged, "seed {seed} step {step}: {o:?}");
                    }
                }
            }
            if targets.is_empty() || r.chance(30) {
                write(conn, c, key(other), Some(&Val::Scalar(other_val)), true);
            }
            Ok(c)
        })
        .unwrap();
        for (e, v, retained) in &plan {
            models.get_mut(e).unwrap().apply(c, v.as_ref(), *retained);
        }
        for m in models.values_mut() {
            let cur = m.cur.clone();
            m.at.insert(c, cur);
        }
        commit_ids.push(c);
        assert_audits_clean(&db, &format!("seed {seed} step {step}"));
    }

    // Every commit: the as-of reads, the closed rows, against the model.
    let conn = Connection::open(&db).unwrap();
    for &c in &commit_ids {
        for (&e, m) in &models {
            let ctx = format!("seed {seed} commit {c} key {e}");
            assert_eq!(db_long_at(&conn, c, e), m.long_at(c), "{ctx}: long as-of");
            let (s, a) = db_other_at(&conn, c, e);
            match &m.at[&c] {
                Some(Val::Scalar(b)) => {
                    assert!(
                        s == Some(Some(*b)) || s == Some(None),
                        "{ctx}: scalar {s:?}"
                    )
                }
                Some(Val::Array(xs)) => {
                    assert!(
                        a == Some(Some(xs.clone())) || a == Some(None),
                        "{ctx}: array {a:?}"
                    )
                }
                _ => assert_eq!((s, a), (None, None), "{ctx}: no other tier"),
            }
            let closed: i64 = conn
                .query_row(
                    "SELECT count(*) FROM value_long WHERE valid_to_commit = ? AND entity_id = ?",
                    duckdb::params![c, e],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                closed as usize,
                m.closed.get(&c).copied().unwrap_or(0),
                "{ctx}: long rows closed"
            );
        }
    }
    // value_timeline: one long span per change of the value, numbered from 1.
    for (&e, m) in &models {
        let spans: Vec<(i64, i64)> = conn
            .prepare(
                "SELECT version, valid_from_commit FROM value_timeline \
                 WHERE tier = 'long' AND entity_id = ? ORDER BY valid_from_commit",
            )
            .unwrap()
            .query_map([e], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let want: Vec<(i64, i64)> = m
            .long_span_starts()
            .into_iter()
            .enumerate()
            .map(|(i, c)| (i as i64 + 1, c))
            .collect();
        assert_eq!(
            spans, want,
            "seed {seed} key {e}: value_timeline long spans"
        );
    }
    conn.close().unwrap();
}

/// Seeds 1-6 x 60 commits; `LCD_SEED=n` runs one seed (to debug it).
#[test]
fn randomized_cell_diff_is_exact_as_of_every_commit() {
    let only: Option<u64> = std::env::var("LCD_SEED").ok().map(|s| s.parse().unwrap());
    for seed in (1..=6u64).filter(|s| only.is_none_or(|o| o == *s)) {
        run_seed(seed, 60);
    }
}

// ---------------------------------------------------------------------------
// Targeted cases
// ---------------------------------------------------------------------------

fn long_of(cells: &[(i32, f64)]) -> Val {
    Val::Long {
        shape: (5, None),
        int: false,
        cells: cells
            .iter()
            .map(|&(p, x)| ([p, 0], x.to_bits() as i64))
            .collect(),
    }
}

/// One direct write (or delete) in its own commit; returns (commit, outcome).
fn commit_one(db: &Path, e: i64, v: Option<&Val>, retained: bool) -> (i64, WriteOutcome) {
    with_write_txn(db, "t", "w", |conn, c| {
        Ok((c, write(conn, c, key(e), v, retained)))
    })
    .unwrap()
}

/// A long row of an entity: (pos, version, from, to, retained).
type LongRowText = (i32, i16, i64, Option<i64>, bool);

fn long_rows(db: &Path, e: i64) -> Vec<LongRowText> {
    let conn = Connection::open(db).unwrap();
    let v = conn
        .prepare(
            "SELECT i1_pos, version, valid_from_commit, valid_to_commit, content_retained \
             FROM value_long WHERE entity_id = ? ORDER BY ALL",
        )
        .unwrap()
        .query_map([e], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    conn.close().unwrap();
    v
}

/// A 1-cell edit closes and re-inserts exactly that cell and reports the
/// diff; with stub retention only the changed cell becomes a stub; an
/// added cell at a position with closed history gets its max version + 1.
#[test]
fn one_cell_edit_closes_one_row_and_stubs_only_it() {
    let (_d, db) = fresh_db();
    let v1 = long_of(&[(0, 1.0), (1, 2.0), (2, 3.0)]);
    let (c1, _) = commit_one(&db, 1, Some(&v1), true);
    let v2 = long_of(&[(0, 1.0), (1, 2.5), (2, 3.0)]);
    let (c2, o) = commit_one(&db, 1, Some(&v2), false);
    assert_eq!(
        (
            o.unchanged,
            o.cells_unchanged,
            o.rows_written,
            o.rows_closed,
            o.version
        ),
        (false, 2, 1, 1, 2)
    );
    assert_eq!(
        long_rows(&db, 1),
        vec![
            (0, 1, c1, None, true),
            (1, 1, c1, Some(c2), false),
            (1, 2, c2, None, true),
            (2, 1, c1, None, true),
        ]
    );
    // Remove cell 2, then add it back: version 2 over its closed version 1.
    let (c3, o) = commit_one(&db, 1, Some(&long_of(&[(0, 1.0), (1, 2.5)])), true);
    assert_eq!(
        (o.cells_unchanged, o.rows_written, o.rows_closed),
        (2, 0, 1)
    );
    let (c4, o) = commit_one(&db, 1, Some(&v2), true);
    assert_eq!(
        (o.cells_unchanged, o.rows_written, o.rows_closed),
        (2, 1, 0)
    );
    let rows = long_rows(&db, 1);
    assert!(rows.contains(&(2, 1, c1, Some(c3), true)), "{rows:?}");
    assert!(rows.contains(&(2, 2, c4, None, true)), "{rows:?}");
    // An identical write changes nothing.
    let (_, o) = commit_one(&db, 1, Some(&v2), true);
    assert_eq!((o.unchanged, o.cells_unchanged, o.version), (true, 3, 2));
    assert_eq!(long_rows(&db, 1), rows);
    assert_audits_clean(&db, "one cell edit");
}

/// A diff that closes more cells than it keeps (the "all but the kept
/// cells" close-out form) closes exactly the changed cells, and stubs only
/// them.
#[test]
fn a_diff_changing_most_cells_closes_exactly_those() {
    let (_d, db) = fresh_db();
    let v1 = long_of(&[(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0), (4, 5.0)]);
    let (c1, _) = commit_one(&db, 1, Some(&v1), true);
    let v2 = long_of(&[(0, 1.5), (1, 2.0), (2, 3.5), (3, 4.5)]);
    let (c2, o) = commit_one(&db, 1, Some(&v2), false);
    assert_eq!(
        (o.cells_unchanged, o.rows_written, o.rows_closed),
        (1, 3, 4)
    );
    assert_eq!(
        long_rows(&db, 1),
        vec![
            (0, 1, c1, Some(c2), false),
            (0, 2, c2, None, true),
            (1, 1, c1, None, true),
            (2, 1, c1, Some(c2), false),
            (2, 2, c2, None, true),
            (3, 1, c1, Some(c2), false),
            (3, 2, c2, None, true),
            (4, 1, c1, Some(c2), false),
        ]
    );
    assert_audits_clean(&db, "most cells changed");
}

/// A current long value whose cells carry two axis sets (damage the audit
/// reports) is never diffed: the next write rewrites it whole.
#[test]
fn an_incoherent_value_is_rewritten_whole() {
    let (_d, db) = fresh_db();
    commit_one(
        &db,
        1,
        Some(&long_of(&[(0, 1.0), (1, 2.0), (2, 3.0)])),
        true,
    );
    // Re-point one cell at another (valid, longer) axis.
    let other = with_write_txn(&db, "t", "axis", |conn, _| {
        nodal_db_core::axis::dedup_or_insert(conn, &mut AxisCache::new(), &int_axis(6))
    })
    .unwrap();
    let conn = Connection::open(&db).unwrap();
    conn.execute(
        "UPDATE value_long SET axis1_id = ? WHERE entity_id = 1 AND i1_pos = 2",
        [other],
    )
    .unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM audit_long_value_coherence", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(n, 1, "the audit reports the mixed-axis value");
    conn.close().unwrap();
    let v = long_of(&[(0, 1.0), (1, 2.5), (2, 3.0)]);
    let (_, o) = commit_one(&db, 1, Some(&v), true);
    assert_eq!(
        (o.cells_unchanged, o.rows_written, o.rows_closed),
        (0, 3, 3)
    );
    assert_audits_clean(&db, "after the whole rewrite");
}

/// `value_timeline`'s long leg: one row per change of the value (an edit,
/// an add, a remove), none for a commit that leaves it alone, a gap for a
/// delete, and `content_retained` FALSE for a span whose cells include a
/// stub.
#[test]
fn value_timeline_long_leg_is_value_level_change_spans() {
    let (_d, db) = fresh_db();
    let w = |v: &[(i32, f64)], retained: bool| commit_one(&db, 1, Some(&long_of(v)), retained).0;
    let c1 = w(&[(0, 1.0), (1, 2.0), (2, 3.0)], true);
    let c2 = w(&[(0, 1.0), (1, 2.5), (2, 3.0)], true);
    commit_one(&db, 7, Some(&Val::Scalar(1f64.to_bits())), true);
    let c3 = w(&[(0, 1.0), (1, 2.5), (2, 3.0), (3, 4.0)], true);
    let c4 = w(&[(1, 2.5), (2, 3.0), (3, 4.0)], true);
    let (c5, _) = commit_one(&db, 1, None, true);
    let c6 = w(&[(1, 2.5), (2, 3.0)], true);
    let c7 = w(&[(1, 2.5), (2, 3.5)], false);
    let conn = Connection::open(&db).unwrap();
    let rows: Vec<(i64, i64, Option<i64>, bool)> = conn
        .prepare(
            "SELECT version, valid_from_commit, valid_to_commit, content_retained \
             FROM value_timeline WHERE tier = 'long' AND entity_id = 1 ORDER BY version",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, c1, Some(c2), true),
            (2, c2, Some(c3), true),
            (3, c3, Some(c4), true),
            (4, c4, Some(c5), true),
            (5, c6, Some(c7), false),
            (6, c7, None, true),
        ]
    );
    let untrusted: Vec<(i64, i64)> = conn
        .prepare(
            "SELECT version, valid_from_commit FROM history_untrusted_span \
             WHERE tier = 'long' AND entity_id = 1",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(untrusted, vec![(5, c6)]);
    conn.close().unwrap();
}
