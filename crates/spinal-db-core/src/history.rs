//! The history write protocol (VMP Part 5) — the append-only versioning that
//! turns one already-canonicalized value into the correct
//! INSERT/UPDATE/close-out sequence across the three tiers. The drift-sensitive
//! heart of the write path (CLAUDE.md invariant 2): this module never
//! re-canonicalizes — it consumes exactly the [`CanonicalValue`] `canon`
//! produced and the axis ids `axis::dedup_or_insert` resolves.
//!
//! Every step here runs INSIDE the caller's transaction (opened by
//! `conn::with_write_txn` / `with_write_txn_retrying`, which also stamps the
//! `commit` row that is the validity edge). Autocommit is forbidden (VMP Part 5,
//! critique #1): the value rows and their commit row are atomic together.
//!
//! Tier mechanics:
//!   - **scalar / long**: single-table from/to-commit. The superseded row
//!     *becomes* the history row in place — `UPDATE … SET valid_to_commit = c`
//!     — then a fresh row is inserted with `version = n + 1`. No payload is ever
//!     copied.
//!   - **array**: the live table holds only current rows; the pre-image is
//!     copied to the `value_array_hist` twin **entirely in SQL**
//!     (`INSERT INTO value_array_hist SELECT *, c, TRUE FROM value_array …`) so
//!     snapshot consistency and crash atomicity stay engine-guaranteed, then the
//!     current row is replaced (or, on delete/transition, deleted).
//!
//! **Version counter (VMP Part 5, `audit_history_tiling`)**: the new version is
//! `max(existing versions for the key) + 1`, scoped to the tier-partition the
//! tiling audit uses — per triple within `value_scalar`; per triple across
//! `value_array` + `value_array_hist`; per `(triple, index tuple)` within
//! `value_long`. A tier transition therefore starts the *new* tier's version at
//! its own partition max + 1 (1 for a first-time tier): the audit partitions by
//! tier, and a continued cross-tier version would make the new tier's first row
//! read as "current version > 1 with no superseded rows".
//!
//! **Retention**: the superseded row (scalar/long close-out, array `_hist` copy)
//! keeps its content by default (`content_retained = TRUE`). `force_replace`
//! (or an explicit `retain_override = Some(false)`) writes a stub instead —
//! `content_retained = FALSE` with the payload columns nulled — so the prior
//! content is not preserved (VMP Part 5 retention).

use duckdb::types::Value;
use duckdb::{params, Connection};

use crate::axis::{dedup_or_insert, AxisCache};
use crate::canon::CanonicalValue;
use crate::encoding::{datetime_coord, AxisBody, CanonicalAxis};
use crate::error::DbError;
use crate::types::{ScalarValue, ShapedData, Tier, ValueKey, WriteOptions, WriteOutcome};

/// Should the superseded / pre-image row keep its content? `force_replace`
/// forces a stub; an explicit `retain_override` otherwise decides; default true.
fn keeps_content(opts: &WriteOptions) -> bool {
    !opts.force_replace && opts.retain_override.unwrap_or(true)
}

/// Write one canonicalized value for `key` at `commit_id`, applying the full
/// history protocol for its tier (and any tier transition from the currently
/// live tier). Runs inside the caller's write transaction.
pub fn write_value(
    conn: &Connection,
    commit_id: i64,
    key: ValueKey,
    value: &CanonicalValue,
    cache: &mut AxisCache,
    opts: &WriteOptions,
) -> Result<WriteOutcome, DbError> {
    let retained = keeps_content(opts);

    // --- 1. Resolve the value's axes (array/long tiers) via content-verified
    // dedup, threading the caller's cache. Count created vs reused by the axis
    // population delta (dedup itself returns only the id). ---
    let axes_before = axis_count(conn)?;
    let mut axis_ids: Vec<i64> = Vec::with_capacity(value.axes.len());
    for ax in &value.axes {
        axis_ids.push(dedup_or_insert(conn, cache, ax)?);
    }
    let axes_created = (axis_count(conn)? - axes_before) as usize;
    let axes_reused = value.axes.len() - axes_created;

    // --- 2. Current state: which tier (if any) holds a live row for the key. ---
    let current = live_tier(conn, &key)?;
    let target = value.tier;
    let same_tier_live = current == Some(target);

    // --- 3. Tier transition: close out / move the OLD tier's current row. ---
    let mut rows_closed = 0usize;
    if let Some(cur) = current {
        if cur != target {
            rows_closed += close_out_tier(conn, &key, commit_id, cur, retained)?;
        }
    }

    // --- 4. Write into the target tier. ---
    let (version, rows_written, closed_same) = match (&value.shape, target) {
        (ShapedData::Scalar(m), Tier::Scalar) => {
            write_scalar(conn, &key, commit_id, m, same_tier_live, retained)?
        }
        (ShapedData::Array(vals), Tier::Array) => {
            write_array(conn, &key, commit_id, &axis_ids, vals, same_tier_live, retained)?
        }
        (ShapedData::Long(rows), Tier::Long) => write_long(
            conn,
            &key,
            commit_id,
            &axis_ids,
            &value.axes,
            rows,
            same_tier_live,
            retained,
        )?,
        // canon guarantees the shape matches the tier.
        _ => unreachable!("shape/tier mismatch from canon"),
    };
    rows_closed += closed_same;

    Ok(WriteOutcome {
        tier: Some(target),
        version,
        rows_written,
        rows_closed,
        axes_created,
        axes_reused,
    })
}

/// Delete the value currently live for `key` at `commit_id`: close-out on the
/// scalar/long tiers, move-to-`_hist` on the array tier (VMP Part 5). A no-op
/// (no live tier) returns a zero outcome. `force_replace`/`retain_override`
/// control whether the superseded content is retained.
pub fn delete_value(
    conn: &Connection,
    commit_id: i64,
    key: ValueKey,
    opts: &WriteOptions,
) -> Result<WriteOutcome, DbError> {
    let retained = keeps_content(opts);
    let rows_closed = match live_tier(conn, &key)? {
        Some(cur) => close_out_tier(conn, &key, commit_id, cur, retained)?,
        None => 0,
    };
    Ok(WriteOutcome {
        tier: None,
        version: 0,
        rows_written: 0,
        rows_closed,
        axes_created: 0,
        axes_reused: 0,
    })
}

// ---------------------------------------------------------------------------
// Current-state detection
// ---------------------------------------------------------------------------

fn axis_count(conn: &Connection) -> Result<i64, DbError> {
    Ok(conn.query_row("SELECT count(*) FROM axis", [], |r| r.get(0))?)
}

/// The tier currently holding a live row for the key triple, or `None`. At most
/// one tier is live per triple (enforced by `audit_cross_tier_duplicate`); the
/// array table stores only live rows, so its mere presence is "live".
fn live_tier(conn: &Connection, key: &ValueKey) -> Result<Option<Tier>, DbError> {
    let triple = params![
        key.entity_id,
        key.parameter_definition_id,
        key.alternative_id
    ];
    let scalar: i64 = conn.query_row(
        "SELECT count(*) FROM value_scalar WHERE entity_id=? AND parameter_definition_id=? \
         AND alternative_id=? AND valid_to_commit IS NULL",
        triple,
        |r| r.get(0),
    )?;
    if scalar > 0 {
        return Ok(Some(Tier::Scalar));
    }
    let long: i64 = conn.query_row(
        "SELECT count(*) FROM value_long WHERE entity_id=? AND parameter_definition_id=? \
         AND alternative_id=? AND valid_to_commit IS NULL",
        triple,
        |r| r.get(0),
    )?;
    if long > 0 {
        return Ok(Some(Tier::Long));
    }
    let array: i64 = conn.query_row(
        "SELECT count(*) FROM value_array WHERE entity_id=? AND parameter_definition_id=? \
         AND alternative_id=?",
        triple,
        |r| r.get(0),
    )?;
    if array > 0 {
        return Ok(Some(Tier::Array));
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Scalar tier
// ---------------------------------------------------------------------------

fn write_scalar(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    m: &ScalarValue,
    same_tier_live: bool,
    retained: bool,
) -> Result<(u16, usize, usize), DbError> {
    let closed = if same_tier_live {
        close_out_scalar(conn, key, c, retained)?
    } else {
        0
    };
    let version = next_version_scalar(conn, key)?;
    let vc = VCols::of(m);
    conn.execute(
        "INSERT INTO value_scalar \
           (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version, \
            content_retained, value_kind, v_float, v_int, v_bool, v_str, v_datetime, \
            v_dur_months, v_dur_micros) \
         VALUES (?, ?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, make_timestamp(?), ?, ?)",
        params![
            key.entity_id,
            key.parameter_definition_id,
            key.alternative_id,
            c,
            version as i16,
            vc.kind,
            vc.f,
            vc.i,
            vc.b,
            vc.s,
            vc.dt,
            vc.dm,
            vc.du,
        ],
    )?;
    Ok((version, 1, closed))
}

fn close_out_scalar(conn: &Connection, key: &ValueKey, c: i64, retained: bool) -> Result<usize, DbError> {
    let sql = if retained {
        "UPDATE value_scalar SET valid_to_commit=? \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
           AND valid_to_commit IS NULL"
    } else {
        "UPDATE value_scalar SET valid_to_commit=?, content_retained=FALSE, \
           v_float=NULL, v_int=NULL, v_bool=NULL, v_str=NULL, v_datetime=NULL, \
           v_dur_months=NULL, v_dur_micros=NULL \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
           AND valid_to_commit IS NULL"
    };
    Ok(conn.execute(
        sql,
        params![c, key.entity_id, key.parameter_definition_id, key.alternative_id],
    )?)
}

fn next_version_scalar(conn: &Connection, key: &ValueKey) -> Result<u16, DbError> {
    let v: i32 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM value_scalar \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?",
        params![key.entity_id, key.parameter_definition_id, key.alternative_id],
        |r| r.get(0),
    )?;
    Ok(v as u16)
}

// ---------------------------------------------------------------------------
// Array tier
// ---------------------------------------------------------------------------

fn write_array(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    axis_ids: &[i64],
    vals: &[f64],
    same_tier_live: bool,
    retained: bool,
) -> Result<(u16, usize, usize), DbError> {
    let (vmin, vmax) = min_max(vals);
    let list_sql = list_value_sql(vals.len());

    if same_tier_live {
        // Pre-image copy to the twin — entirely in SQL (never client-buffered).
        copy_array_preimage(conn, key, c, retained)?;
        let version = next_version_array(conn, key)?;
        // Replace the current row in place (bump version, new payload). Both
        // payload columns are set so a stale values_int can't break the XOR.
        let sql = format!(
            "UPDATE value_array SET valid_from_commit=?, version=?, \
               axis1_id=?, axis2_id=?, axis3_id=?, axis4_id=?, \
               values={list_sql}, values_int=NULL, v_min=?, v_max=? \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?"
        );
        let mut p: Vec<Value> = vec![Value::BigInt(c), Value::SmallInt(version as i16)];
        push_axis4(&mut p, axis_ids);
        for v in vals {
            p.push(Value::Double(*v));
        }
        p.push(Value::Double(vmin));
        p.push(Value::Double(vmax));
        p.push(Value::BigInt(key.entity_id));
        p.push(Value::BigInt(key.parameter_definition_id));
        p.push(Value::BigInt(key.alternative_id));
        conn.execute(&sql, duckdb::params_from_iter(p.iter()))?;
        Ok((version, 1, 1))
    } else {
        let version = next_version_array(conn, key)?;
        let sql = format!(
            "INSERT INTO value_array \
               (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version, \
                axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, {list_sql}, NULL, ?, ?)"
        );
        let mut p: Vec<Value> = vec![
            Value::BigInt(key.entity_id),
            Value::BigInt(key.parameter_definition_id),
            Value::BigInt(key.alternative_id),
            Value::BigInt(c),
            Value::SmallInt(version as i16),
        ];
        push_axis4(&mut p, axis_ids);
        for v in vals {
            p.push(Value::Double(*v));
        }
        p.push(Value::Double(vmin));
        p.push(Value::Double(vmax));
        conn.execute(&sql, duckdb::params_from_iter(p.iter()))?;
        Ok((version, 1, 0))
    }
}

/// SQL-side pre-image copy of the live array row(s) into `value_array_hist`
/// (VMP Part 5). Retained → `SELECT *, c, TRUE`; stub → payload columns nulled
/// with `content_retained = FALSE`. The pre-image never leaves SQL.
fn copy_array_preimage(conn: &Connection, key: &ValueKey, c: i64, retained: bool) -> Result<usize, DbError> {
    let sql = if retained {
        "INSERT INTO value_array_hist SELECT *, ?, TRUE FROM value_array \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?"
    } else {
        "INSERT INTO value_array_hist \
           (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version, \
            axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max, \
            valid_to_commit, content_retained) \
         SELECT entity_id, parameter_definition_id, alternative_id, valid_from_commit, version, \
            axis1_id, axis2_id, axis3_id, axis4_id, NULL, NULL, v_min, v_max, ?, FALSE \
         FROM value_array \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?"
    };
    Ok(conn.execute(
        sql,
        params![c, key.entity_id, key.parameter_definition_id, key.alternative_id],
    )?)
}

fn next_version_array(conn: &Connection, key: &ValueKey) -> Result<u16, DbError> {
    let v: i32 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM ( \
            SELECT version FROM value_array \
              WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
            UNION ALL \
            SELECT version FROM value_array_hist \
              WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?)",
        params![
            key.entity_id,
            key.parameter_definition_id,
            key.alternative_id,
            key.entity_id,
            key.parameter_definition_id,
            key.alternative_id
        ],
        |r| r.get(0),
    )?;
    Ok(v as u16)
}

// ---------------------------------------------------------------------------
// Long tier
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn write_long(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    axis_ids: &[i64],
    axes: &[CanonicalAxis],
    rows: &[crate::types::LongRow],
    same_tier_live: bool,
    retained: bool,
) -> Result<(u16, usize, usize), DbError> {
    // Close out every live cell of the triple first; a cell that reappears in
    // the new value re-opens at its own (per-cell) version + 1, one absent from
    // the new value simply stays dead (VMP Part 5 per-(triple, index tuple)).
    let closed = if same_tier_live {
        close_out_long(conn, key, c, retained)?
    } else {
        0
    };

    let mut max_version = 0u16;
    for row in rows {
        let pos = pad4_pos(&row.positions);
        let version = next_version_long_cell(conn, key, &pos)?;
        max_version = max_version.max(version);

        // Per-dimension index accelerators, derived from the bound axis at the
        // cell's position (audited by audit_index_accelerator; never authoritative).
        let mut dt = [None::<i64>; 4];
        let mut num = [None::<i64>; 4];
        for (d, ax) in axes.iter().enumerate() {
            let (a_dt, a_num) = accel(ax, pos[d]);
            dt[d] = a_dt;
            num[d] = a_num;
        }
        let ax = |d: usize| axis_ids.get(d).copied();
        let vc = VCols::of(&row.value);
        conn.execute(
            "INSERT INTO value_long \
               (entity_id, parameter_definition_id, alternative_id, valid_from_commit, version, \
                axis1_id, axis2_id, axis3_id, axis4_id, i1_pos, i2_pos, i3_pos, i4_pos, \
                i1_dt, i1_num, i2_dt, i2_num, i3_dt, i3_num, i4_dt, i4_num, \
                value_kind, v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                     make_timestamp(?), ?, make_timestamp(?), ?, make_timestamp(?), ?, make_timestamp(?), ?, \
                     ?, ?, ?, ?, ?, make_timestamp(?), ?, ?)",
            params![
                key.entity_id,
                key.parameter_definition_id,
                key.alternative_id,
                c,
                version as i16,
                ax(0),
                ax(1),
                ax(2),
                ax(3),
                pos[0],
                pos[1],
                pos[2],
                pos[3],
                dt[0],
                num[0],
                dt[1],
                num[1],
                dt[2],
                num[2],
                dt[3],
                num[3],
                vc.kind,
                vc.f,
                vc.i,
                vc.b,
                vc.s,
                vc.dt,
                vc.dm,
                vc.du,
            ],
        )?;
    }
    Ok((max_version, rows.len(), closed))
}

fn close_out_long(conn: &Connection, key: &ValueKey, c: i64, retained: bool) -> Result<usize, DbError> {
    let sql = if retained {
        "UPDATE value_long SET valid_to_commit=? \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
           AND valid_to_commit IS NULL"
    } else {
        "UPDATE value_long SET valid_to_commit=?, content_retained=FALSE, \
           v_float=NULL, v_int=NULL, v_bool=NULL, v_str=NULL, v_datetime=NULL, \
           v_dur_months=NULL, v_dur_micros=NULL \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
           AND valid_to_commit IS NULL"
    };
    Ok(conn.execute(
        sql,
        params![c, key.entity_id, key.parameter_definition_id, key.alternative_id],
    )?)
}

fn next_version_long_cell(conn: &Connection, key: &ValueKey, pos: &[i32; 4]) -> Result<u16, DbError> {
    let v: i32 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM value_long \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
           AND i1_pos=? AND i2_pos=? AND i3_pos=? AND i4_pos=?",
        params![
            key.entity_id,
            key.parameter_definition_id,
            key.alternative_id,
            pos[0],
            pos[1],
            pos[2],
            pos[3]
        ],
        |r| r.get(0),
    )?;
    Ok(v as u16)
}

// ---------------------------------------------------------------------------
// Tier close-out dispatch (transition + delete)
// ---------------------------------------------------------------------------

fn close_out_tier(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    tier: Tier,
    retained: bool,
) -> Result<usize, DbError> {
    match tier {
        Tier::Scalar => close_out_scalar(conn, key, c, retained),
        Tier::Long => close_out_long(conn, key, c, retained),
        Tier::Array => {
            let moved = copy_array_preimage(conn, key, c, retained)?;
            conn.execute(
                "DELETE FROM value_array \
                 WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?",
                params![key.entity_id, key.parameter_definition_id, key.alternative_id],
            )?;
            Ok(moved)
        }
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// The typed value columns of one scalar measure — mostly-NULL, exactly one
/// (or the duration pair) populated. `v_datetime`/`i*_dt` bind as µs through
/// `make_timestamp(?)` at the call site.
struct VCols {
    kind: &'static str,
    f: Option<f64>,
    i: Option<i64>,
    b: Option<bool>,
    s: Option<String>,
    dt: Option<i64>,
    dm: Option<i32>,
    du: Option<i64>,
}

impl VCols {
    fn of(v: &ScalarValue) -> VCols {
        let mut c = VCols {
            kind: v.kind().tag(),
            f: None,
            i: None,
            b: None,
            s: None,
            dt: None,
            dm: None,
            du: None,
        };
        match v {
            ScalarValue::Float(x) => c.f = Some(*x),
            ScalarValue::Int(x) => c.i = Some(*x),
            ScalarValue::Bool(x) => c.b = Some(*x),
            ScalarValue::Str(x) => c.s = Some(x.clone()),
            ScalarValue::DateTime(x) => c.dt = Some(*x),
            ScalarValue::Duration { months, micros } => {
                c.dm = Some(*months);
                c.du = Some(*micros);
            }
        }
        c
    }
}

/// The `(v_datetime µs, v_num)` accelerator for a dimension's axis at `pos`.
/// Only datetime and integer axes carry an accelerator; label/float (and the
/// canon-unsupported period/time_pattern) leave both NULL — exactly what
/// `audit_index_accelerator` expects.
fn accel(axis: &CanonicalAxis, pos: i32) -> (Option<i64>, Option<i64>) {
    match &axis.body {
        AxisBody::DatetimeParametric { start_ts, step } => {
            (datetime_coord(*start_ts, step, pos as i64), None)
        }
        AxisBody::DatetimeMaterialized { coords } => (coords.get(pos as usize).copied(), None),
        AxisBody::IntegerParametric { start, step } => (None, Some(start + pos as i64 * step)),
        AxisBody::IntegerMaterialized { coords } => (None, coords.get(pos as usize).copied()),
        _ => (None, None),
    }
}

fn pad4_pos(positions: &[i32]) -> [i32; 4] {
    let mut p = [0i32; 4];
    for (i, v) in positions.iter().take(4).enumerate() {
        p[i] = *v;
    }
    p
}

/// Push four axis-id params (`Value::Null` beyond the declared rank).
fn push_axis4(p: &mut Vec<Value>, axis_ids: &[i64]) {
    for d in 0..4 {
        p.push(axis_ids.get(d).map(|&id| Value::BigInt(id)).unwrap_or(Value::Null));
    }
}

/// `list_value(?, …)::DOUBLE[]` with `n` element placeholders — the SQL-side
/// array constructor (a DOUBLE[] param cannot bind directly in duckdb-rs).
fn list_value_sql(n: usize) -> String {
    let mut s = String::from("list_value(");
    for i in 0..n {
        if i > 0 {
            s.push(',');
        }
        s.push('?');
    }
    s.push_str(")::DOUBLE[]");
    s
}

fn min_max(vals: &[f64]) -> (f64, f64) {
    let mut lo = f64::INFINITY;
    let mut hi = f64::NEG_INFINITY;
    for &v in vals {
        if v < lo {
            lo = v;
        }
        if v > hi {
            hi = v;
        }
    }
    (lo, hi)
}
