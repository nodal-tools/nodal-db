//! DB-facing content-verified axis dedup (VMP Part 4, critique #7).
//!
//! Axes are the design's CF coordinate variables: immutable, content-addressed,
//! shared by FK from every array- and long-tier dimension. This module persists
//! the [`CanonicalAxis`] values that `canon` already produced (CLAUDE.md
//! invariant 2 — it does **not** re-canonicalize) into the `axis` table and its
//! materialized child tables, deduping by content.
//!
//! **Hash is an accelerator, byte-equality is the truth.** `content_hash` is
//! indexed, deliberately NOT unique: two distinct axes may collide. So
//! [`dedup_or_insert`] narrows candidates by `content_hash + kind`, then
//! reconstructs each candidate's [`CanonicalAxis`] from the DB and compares
//! `encode_axis` bytes. A hash match is never trusted alone — v1's
//! `ON CONFLICT DO NOTHING` reuse silently mis-attached values on collision.
//!
//! F3 (Arrow-schema boundary): dedup touches **no** Arrow → `canon` path. The
//! only Arrow the `canon` pipeline consumes is the caller's write batch, whose
//! concrete types (`Int64`, `Utf8`, `TimestampMicrosecond`, `Float64`,
//! `Boolean`) canon downcasts strictly. Here, candidate reconstruction reads
//! scalar columns via typed row getters, and every timestamp crosses as an
//! `i64` µs count through `epoch_us(...)` on read and `make_timestamp(?)` on
//! write — so a DuckDB Arrow export's `Utf8View`/dictionary/non-µs-timestamp
//! quirks cannot reach this code. The full value-read Arrow boundary is Phase 3.

use std::collections::HashMap;

use duckdb::types::Value;
use duckdb::Connection;

use crate::encoding::{
    canonical_axis_f64, encode_axis, hash_axis, AxisBody, AxisProperties, CanonicalAxis, Duration,
    LabelPos, PeriodPos, TimePatternPos, TimePatternTerm,
};
use crate::error::DbError;
use crate::history::{clip, exec_columnar, list, opt_list};

/// A client-side dedup cache, scoped to one write session (Phase 3 threads it
/// across a batch so a handful of axes serving thousands of values are looked
/// up once). Keyed by content hash → the `(id, axis)` rows seen for that hash;
/// within a bucket, equality is still decided by byte-compare, never the hash.
#[derive(Debug, Default)]
pub struct AxisCache {
    by_hash: HashMap<u64, Vec<(i64, CanonicalAxis)>>,
}

impl AxisCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Byte-compare `input` (already encoded to `input_bytes`) against every
    /// cached axis sharing its hash; return a reused id on true equality.
    fn lookup(&self, hash: u64, input_bytes: &[u8]) -> Option<i64> {
        let bucket = self.by_hash.get(&hash)?;
        for (id, axis) in bucket {
            if encode_axis(axis).ok().as_deref() == Some(input_bytes) {
                return Some(*id);
            }
        }
        None
    }

    fn remember(&mut self, hash: u64, id: i64, axis: CanonicalAxis) {
        self.by_hash.entry(hash).or_default().push((id, axis));
    }
}

/// Reuse an existing axis with byte-identical content, else insert a new one.
/// Returns the `axis.id` to reference. `cache` accelerates repeats within the
/// write session; a cache miss falls back to a `content_hash + kind` candidate
/// query and per-candidate byte-compare (VMP Part 4).
pub fn dedup_or_insert(
    conn: &Connection,
    cache: &mut AxisCache,
    axis: &CanonicalAxis,
) -> Result<i64, DbError> {
    // F1 writer-boundary guard (Phase-2b critique): a materialized axis must
    // carry exactly `length` non-empty child rows. Checked here, at the axis
    // write boundary and BEFORE `encode_axis` (whose matching assert is
    // debug-only), so a malformed materialized axis is rejected in RELEASE and
    // can never be persisted — its child count is the audited round-trip identity.
    ensure_wellformed(axis)?;

    let input_bytes = encode_axis(axis)?;
    let hash = hash_axis(axis)?;
    let kind = axis.kind_tag();

    // 1. Client-side cache (byte-verified).
    if let Some(id) = cache.lookup(hash, &input_bytes) {
        return Ok(id);
    }

    // 2. DB candidates by hash + kind; reconstruct + byte-compare each.
    for candidate_id in candidate_ids(conn, hash, kind)? {
        let candidate = reconstruct_axis(conn, candidate_id, kind)?;
        if encode_axis(&candidate)? == input_bytes {
            cache.remember(hash, candidate_id, candidate);
            return Ok(candidate_id);
        }
    }

    // 3. No true match — insert a fresh axis (+ child rows for materialized).
    let new_id = insert_axis(conn, axis, hash)?;
    cache.remember(hash, new_id, axis.clone());
    Ok(new_id)
}

/// F1 guard: a materialized axis body must have a non-empty child list whose
/// length equals the declared `length` (parametric forms carry no child rows
/// and are exempt). Release-mode; the `encode_axis` equivalent is debug-only.
fn ensure_wellformed(axis: &CanonicalAxis) -> Result<(), DbError> {
    let children = match &axis.body {
        AxisBody::DatetimeMaterialized { coords } | AxisBody::IntegerMaterialized { coords } => {
            coords.len()
        }
        AxisBody::FloatMaterialized { coords } => coords.len(),
        AxisBody::Label { positions } => positions.len(),
        AxisBody::Period { positions } => positions.len(),
        AxisBody::TimePattern { positions } => positions.len(),
        // Parametric forms have no child rows — nothing to validate here.
        AxisBody::DatetimeParametric { .. }
        | AxisBody::IntegerParametric { .. }
        | AxisBody::FloatParametric { .. } => return Ok(()),
    };
    if children == 0 || children != axis.length as usize {
        return Err(DbError::MalformedAxis {
            kind: axis.kind_tag(),
            length: axis.length,
            children,
        });
    }
    Ok(())
}

/// Candidate `axis.id`s sharing the content hash and kind — the accelerated
/// narrowing before byte-compare.
fn candidate_ids(conn: &Connection, hash: u64, kind: &str) -> Result<Vec<i64>, DbError> {
    let mut stmt = conn.prepare("SELECT id FROM axis WHERE content_hash = ? AND kind = ?")?;
    let ids = stmt
        .query_map(duckdb::params![hash, kind], |r| r.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ids)
}

/// Reconstruct a candidate's [`CanonicalAxis`] from the DB — parametric columns
/// (+ calendar/timezone/crs) for parametric forms, child rows for materialized
/// kinds — so `encode_axis` can decide byte-equality. `kind` comes from the
/// candidate query, so the form is fixed by which `p_*` columns are non-NULL.
fn reconstruct_axis(conn: &Connection, id: i64, kind: &str) -> Result<CanonicalAxis, DbError> {
    // Parametric columns + properties + length, one row.
    let row = conn.query_row(
        "SELECT length,
                epoch_us(p_start_ts), p_step_months, p_step_micros,
                p_start_int, p_step_int,
                p_start_dbl, p_step_dbl,
                calendar, timezone, crs
         FROM axis WHERE id = ?",
        [id],
        |r| {
            Ok(AxisRow {
                length: r.get(0)?,
                p_start_ts: r.get(1)?,
                p_step_months: r.get(2)?,
                p_step_micros: r.get(3)?,
                p_start_int: r.get(4)?,
                p_step_int: r.get(5)?,
                p_start_dbl: r.get(6)?,
                p_step_dbl: r.get(7)?,
                calendar: r.get(8)?,
                timezone: r.get(9)?,
                crs: r.get(10)?,
            })
        },
    )?;

    let properties = AxisProperties {
        calendar: row.calendar,
        timezone: row.timezone,
        crs: row.crs,
    };

    let body = match kind {
        "datetime" => match row.p_start_ts {
            Some(start_ts) => AxisBody::DatetimeParametric {
                start_ts,
                step: Duration {
                    months: row.p_step_months.unwrap_or(0),
                    micros: row.p_step_micros.unwrap_or(0),
                },
            },
            None => AxisBody::DatetimeMaterialized {
                coords: read_i64s(
                    conn,
                    "SELECT epoch_us(ts) FROM axis_datetime WHERE axis_id = ? ORDER BY pos",
                    id,
                )?,
            },
        },
        "integer" => match row.p_start_int {
            Some(start) => AxisBody::IntegerParametric {
                start,
                step: row.p_step_int.unwrap_or(0),
            },
            None => AxisBody::IntegerMaterialized {
                coords: read_i64s(
                    conn,
                    "SELECT num FROM axis_integer WHERE axis_id = ? ORDER BY pos",
                    id,
                )?,
            },
        },
        "float" => match row.p_start_dbl {
            Some(start) => AxisBody::FloatParametric {
                start,
                step: row.p_step_dbl.unwrap_or(0.0),
            },
            None => AxisBody::FloatMaterialized {
                coords: read_f64s(
                    conn,
                    "SELECT coord FROM axis_float WHERE axis_id = ? ORDER BY pos",
                    id,
                )?,
            },
        },
        "label" => {
            let mut stmt = conn
                .prepare("SELECT ordinal, label FROM axis_label WHERE axis_id = ? ORDER BY pos")?;
            let positions = stmt
                .query_map([id], |r| {
                    Ok(LabelPos {
                        ordinal: r.get(0)?,
                        label: r.get(1)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            AxisBody::Label { positions }
        }
        "period" => {
            let mut stmt = conn.prepare(
                "SELECT epoch_us(start_ts), epoch_us(end_ts), years_presented
                 FROM axis_period WHERE axis_id = ? ORDER BY pos",
            )?;
            let positions = stmt
                .query_map([id], |r| {
                    Ok(PeriodPos {
                        start_ts: r.get(0)?,
                        end_ts: r.get(1)?,
                        years_presented: r.get(2)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            AxisBody::Period { positions }
        }
        "time_pattern" => AxisBody::TimePattern {
            positions: read_time_pattern(conn, id)?,
        },
        other => {
            return Err(DbError::UnknownAxisKind {
                id,
                kind: other.to_string(),
            })
        }
    };

    Ok(CanonicalAxis {
        length: row.length,
        properties,
        body,
    })
}

/// The `axis`-row scalar columns needed to reconstruct any kind.
struct AxisRow {
    length: i32,
    p_start_ts: Option<i64>,
    p_step_months: Option<i32>,
    p_step_micros: Option<i64>,
    p_start_int: Option<i64>,
    p_step_int: Option<i64>,
    p_start_dbl: Option<f64>,
    p_step_dbl: Option<f64>,
    calendar: Option<String>,
    timezone: Option<String>,
    crs: Option<String>,
}

fn read_i64s(conn: &Connection, sql: &str, axis_id: i64) -> Result<Vec<i64>, DbError> {
    let mut stmt = conn.prepare(sql)?;
    let v = stmt
        .query_map([axis_id], |r| r.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(v)
}

fn read_f64s(conn: &Connection, sql: &str, axis_id: i64) -> Result<Vec<f64>, DbError> {
    let mut stmt = conn.prepare(sql)?;
    let v = stmt
        .query_map([axis_id], |r| r.get::<_, f64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(v)
}

/// Reconstruct time-pattern positions: rows are `(pos, ordinal, term_no, ...)`,
/// ordered so consecutive terms of one `pos` group together.
fn read_time_pattern(conn: &Connection, id: i64) -> Result<Vec<TimePatternPos>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT pos, ordinal, unit, range_start, range_end
         FROM axis_time_pattern WHERE axis_id = ? ORDER BY pos, term_no",
    )?;
    let rows = stmt
        .query_map([id], |r| {
            Ok((
                r.get::<_, i32>(0)?, // pos
                r.get::<_, i32>(1)?, // ordinal
                r.get::<_, String>(2)?,
                r.get::<_, i16>(3)?,
                r.get::<_, i16>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut positions: Vec<TimePatternPos> = Vec::new();
    let mut cur_pos: Option<i32> = None;
    for (pos, ordinal, unit, range_start, range_end) in rows {
        if cur_pos != Some(pos) {
            positions.push(TimePatternPos {
                ordinal,
                terms: Vec::new(),
            });
            cur_pos = Some(pos);
        }
        positions.last_mut().unwrap().terms.push(TimePatternTerm {
            unit,
            range_start,
            range_end,
        });
    }
    Ok(positions)
}

/// Insert a new `axis` row (+ child rows for materialized kinds) and return its
/// generated id. Parametric columns are set only for the matching form; all
/// timestamps cross as `i64` µs through `make_timestamp(?)` (F3 boundary).
fn insert_axis(conn: &Connection, axis: &CanonicalAxis, hash: u64) -> Result<i64, DbError> {
    let kind = axis.kind_tag();

    // Parametric column values (NULL unless this form uses them).
    let mut p_start_ts: Option<i64> = None;
    let mut p_step_months: Option<i32> = None;
    let mut p_step_micros: Option<i64> = None;
    let mut p_start_int: Option<i64> = None;
    let mut p_step_int: Option<i64> = None;
    let mut p_start_dbl: Option<f64> = None;
    let mut p_step_dbl: Option<f64> = None;

    match &axis.body {
        AxisBody::DatetimeParametric { start_ts, step } => {
            p_start_ts = Some(*start_ts);
            p_step_months = Some(step.months);
            p_step_micros = Some(step.micros);
        }
        AxisBody::IntegerParametric { start, step } => {
            p_start_int = Some(*start);
            p_step_int = Some(*step);
        }
        // Axis floats are stored canonical (-0.0 -> +0.0). The NaN arm never
        // fires: encode_axis already rejected NaN earlier in dedup_or_insert.
        AxisBody::FloatParametric { start, step } => {
            p_start_dbl = Some(canonical_axis_f64(*start)?);
            p_step_dbl = Some(canonical_axis_f64(*step)?);
        }
        _ => {}
    }

    let props = &axis.properties;
    let new_id: i64 = conn.query_row(
        "INSERT INTO axis
           (kind, length, content_hash,
            p_start_ts, p_step_months, p_step_micros,
            p_start_int, p_step_int,
            p_start_dbl, p_step_dbl,
            calendar, timezone, crs)
         VALUES (?, ?, ?, make_timestamp(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING id",
        duckdb::params![
            kind,
            axis.length,
            hash,
            p_start_ts,
            p_step_months,
            p_step_micros,
            p_start_int,
            p_step_int,
            p_start_dbl,
            p_step_dbl,
            props.calendar.as_deref(),
            props.timezone.as_deref(),
            props.crs.as_deref(),
        ],
        |r| r.get(0),
    )?;

    // Child rows for materialized kinds: ONE columnar statement per kind
    // (appender_write_path_PLAN D8) instead of one prepare + execute per
    // coordinate: positions and coordinates bind as lists and are zipped by
    // `unnest`, chunked and length-checked like the long insert. Timestamps
    // still cross as µs through `make_timestamp`, and every float coordinate
    // still goes through `canonical_axis_f64` first (same function, same
    // place); dedup, the content hash and `ensure_wellformed` are upstream.
    let id = Value::BigInt(new_id);
    let positions = |n: usize| -> Vec<i32> { (0..n as i32).collect() };
    match &axis.body {
        AxisBody::DatetimeMaterialized { coords } => {
            let pos = positions(coords.len());
            exec_columnar(
                conn,
                "INSERT INTO axis_datetime (axis_id, pos, ts) \
                 SELECT ?, unnest(?::INTEGER[]), make_timestamp(unnest(?::BIGINT[]))",
                coords.len(),
                |r| {
                    vec![
                        id.clone(),
                        list(clip(&pos, &r), |&v| Value::Int(v)),
                        list(clip(coords, &r), |&v| Value::BigInt(v)),
                    ]
                },
            )?;
        }
        AxisBody::IntegerMaterialized { coords } => {
            let pos = positions(coords.len());
            exec_columnar(
                conn,
                "INSERT INTO axis_integer (axis_id, pos, num) \
                 SELECT ?, unnest(?::INTEGER[]), unnest(?::BIGINT[])",
                coords.len(),
                |r| {
                    vec![
                        id.clone(),
                        list(clip(&pos, &r), |&v| Value::Int(v)),
                        list(clip(coords, &r), |&v| Value::BigInt(v)),
                    ]
                },
            )?;
        }
        AxisBody::FloatMaterialized { coords } => {
            let pos = positions(coords.len());
            let canon: Vec<f64> = coords
                .iter()
                .map(|&c| canonical_axis_f64(c))
                .collect::<Result<_, _>>()?;
            exec_columnar(
                conn,
                "INSERT INTO axis_float (axis_id, pos, coord) \
                 SELECT ?, unnest(?::INTEGER[]), unnest(?::DOUBLE[])",
                canon.len(),
                |r| {
                    vec![
                        id.clone(),
                        list(clip(&pos, &r), |&v| Value::Int(v)),
                        list(clip(&canon, &r), |&v| Value::Double(v)),
                    ]
                },
            )?;
        }
        AxisBody::Label { positions: lp } => {
            let pos = positions(lp.len());
            // `axis_label.id` comes from its column DEFAULT, minted row by row
            // in position order (insertion order is preserved).
            exec_columnar(
                conn,
                "INSERT INTO axis_label (axis_id, pos, label, ordinal) \
                 SELECT ?, unnest(?::INTEGER[]), unnest(?::VARCHAR[]), unnest(?::INTEGER[])",
                lp.len(),
                |r| {
                    vec![
                        id.clone(),
                        list(clip(&pos, &r), |&v| Value::Int(v)),
                        list(clip(lp, &r), |p| Value::Text(p.label.clone())),
                        list(clip(lp, &r), |p| Value::Int(p.ordinal)),
                    ]
                },
            )?;
        }
        AxisBody::Period { positions: pp } => {
            let pos = positions(pp.len());
            let years: Vec<Option<f64>> = pp
                .iter()
                .map(|p| p.years_presented.map(canonical_axis_f64).transpose())
                .collect::<Result<_, _>>()?;
            exec_columnar(
                conn,
                "INSERT INTO axis_period (axis_id, pos, start_ts, end_ts, years_presented) \
                 SELECT ?, unnest(?::INTEGER[]), make_timestamp(unnest(?::BIGINT[])), \
                        make_timestamp(unnest(?::BIGINT[])), unnest(?::DOUBLE[])",
                pp.len(),
                |r| {
                    vec![
                        id.clone(),
                        list(clip(&pos, &r), |&v| Value::Int(v)),
                        list(clip(pp, &r), |p| Value::BigInt(p.start_ts)),
                        list(clip(pp, &r), |p| Value::BigInt(p.end_ts)),
                        opt_list(clip(&years, &r), |&v| Value::Double(v)),
                    ]
                },
            )?;
        }
        AxisBody::TimePattern { positions: tp } => {
            // One row per (position, term), terms numbered from 1 in order.
            let mut pos = Vec::new();
            let mut ordinal = Vec::new();
            let mut term_no = Vec::new();
            let mut terms = Vec::new();
            for (i, p) in tp.iter().enumerate() {
                for (t, term) in p.terms.iter().enumerate() {
                    pos.push(i as i32);
                    ordinal.push(p.ordinal);
                    term_no.push((t + 1) as i32);
                    terms.push(term);
                }
            }
            exec_columnar(
                conn,
                "INSERT INTO axis_time_pattern \
                   (axis_id, pos, ordinal, term_no, unit, range_start, range_end) \
                 SELECT ?, unnest(?::INTEGER[]), unnest(?::INTEGER[]), unnest(?::INTEGER[]), \
                        unnest(?::VARCHAR[]), unnest(?::SMALLINT[]), unnest(?::SMALLINT[])",
                terms.len(),
                |r| {
                    vec![
                        id.clone(),
                        list(clip(&pos, &r), |&v| Value::Int(v)),
                        list(clip(&ordinal, &r), |&v| Value::Int(v)),
                        list(clip(&term_no, &r), |&v| Value::Int(v)),
                        list(clip(&terms, &r), |t| Value::Text(t.unit.clone())),
                        list(clip(&terms, &r), |t| Value::SmallInt(t.range_start)),
                        list(clip(&terms, &r), |t| Value::SmallInt(t.range_end)),
                    ]
                },
            )?;
        }
        // Parametric forms have no child rows.
        AxisBody::DatetimeParametric { .. }
        | AxisBody::IntegerParametric { .. }
        | AxisBody::FloatParametric { .. } => {}
    }

    Ok(new_id)
}
