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

use std::collections::HashMap;

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
/// `parameter_type_id` is the declared member the value bound to at entry
/// (D11, 2026-09-18): the API always passes `Some` (resolve/materialize
/// guarantees a member); `None` is for callers below the entry check (tests,
/// out-of-contract tools) and lands as SQL NULL.
pub fn write_value(
    conn: &Connection,
    commit_id: i64,
    key: ValueKey,
    value: &CanonicalValue,
    cache: &mut AxisCache,
    opts: &WriteOptions,
    parameter_type_id: Option<i64>,
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
            write_scalar(conn, &key, commit_id, m, same_tier_live, retained, parameter_type_id)?
        }
        (ShapedData::Array(vals), Tier::Array) => write_array(
            conn,
            &key,
            commit_id,
            &axis_ids,
            vals,
            same_tier_live,
            retained,
            parameter_type_id,
        )?,
        (ShapedData::Long(rows), Tier::Long) => write_long(
            conn,
            &key,
            commit_id,
            &axis_ids,
            &value.axes,
            rows,
            same_tier_live,
            retained,
            parameter_type_id,
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
// Definition-scoped default values (template loader; #1598 mirrors)
// ---------------------------------------------------------------------------

/// Write a definition-scoped scalar `default_value` (the `#1598` mirror,
/// `definition_value_scalar`) under the same from/to close-out protocol as
/// entity-scoped scalars: close the current row (content retained — defaults
/// are small, low-churn authored content), insert the new one at
/// `max(version) + 1` for the `(definition, slot)` partition. A scalar default
/// is already canonical (rank 0: the canonicalization pipeline's NULL
/// elimination / axis derivation / tier rule are identity — VMP Part 3), so
/// this consumes the typed [`ScalarValue`] directly; `parameter_type_id` is
/// the D11 binding to the rank-0 declared member the default instantiates.
/// Returns `(rows_closed, rows_written)`; a no-change write still versions
/// (callers diff first — the template reconcile only calls this on change).
pub(crate) fn write_definition_default(
    conn: &Connection,
    commit_id: i64,
    pd_id: i64,
    value: &ScalarValue,
    parameter_type_id: Option<i64>,
) -> Result<(usize, usize), DbError> {
    let closed = close_definition_default(conn, commit_id, pd_id)?;
    let version: i32 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM definition_value_scalar \
         WHERE parameter_definition_id = ? AND slot = 'default_value'",
        params![pd_id],
        |r| r.get(0),
    )?;
    let vc = VCols::of(value);
    conn.execute(
        "INSERT INTO definition_value_scalar \
           (parameter_definition_id, slot, parameter_type_id, valid_from_commit, version, \
            content_retained, value_kind, v_float, v_int, v_bool, v_str, v_datetime, \
            v_dur_months, v_dur_micros) \
         VALUES (?, 'default_value', ?, ?, ?, TRUE, ?, ?, ?, ?, ?, make_timestamp(?), ?, ?)",
        params![
            pd_id,
            parameter_type_id,
            commit_id,
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
    Ok((closed, 1))
}

/// Close out the current scalar `default_value` row of a definition (content
/// retained). Returns the number of rows closed (0 or 1).
pub(crate) fn close_definition_default(
    conn: &Connection,
    commit_id: i64,
    pd_id: i64,
) -> Result<usize, DbError> {
    Ok(conn.execute(
        "UPDATE definition_value_scalar SET valid_to_commit = ? \
         WHERE parameter_definition_id = ? AND slot = 'default_value' \
           AND valid_to_commit IS NULL",
        params![commit_id, pd_id],
    )?)
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

#[allow(clippy::too_many_arguments)]
fn write_scalar(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    m: &ScalarValue,
    same_tier_live: bool,
    retained: bool,
    pt_id: Option<i64>,
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
           (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, \
            content_retained, value_kind, v_float, v_int, v_bool, v_str, v_datetime, \
            v_dur_months, v_dur_micros) \
         VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, make_timestamp(?), ?, ?)",
        params![
            key.entity_id,
            key.parameter_definition_id,
            key.alternative_id,
            pt_id,
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

#[allow(clippy::too_many_arguments)]
fn write_array(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    axis_ids: &[i64],
    vals: &[f64],
    same_tier_live: bool,
    retained: bool,
    pt_id: Option<i64>,
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
            "UPDATE value_array SET valid_from_commit=?, version=?, parameter_type_id=?, \
               axis1_id=?, axis2_id=?, axis3_id=?, axis4_id=?, \
               values={list_sql}, values_int=NULL, v_min=?, v_max=? \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?"
        );
        let mut p: Vec<Value> = vec![Value::BigInt(c), Value::SmallInt(version as i16)];
        p.push(pt_id.map(Value::BigInt).unwrap_or(Value::Null));
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
               (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
                valid_from_commit, version, \
                axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {list_sql}, NULL, ?, ?)"
        );
        let mut p: Vec<Value> = vec![
            Value::BigInt(key.entity_id),
            Value::BigInt(key.parameter_definition_id),
            Value::BigInt(key.alternative_id),
            pt_id.map(Value::BigInt).unwrap_or(Value::Null),
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
           (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, \
            axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max, \
            valid_to_commit, content_retained) \
         SELECT entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, \
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
    pt_id: Option<i64>,
) -> Result<(u16, usize, usize), DbError> {
    // Close out every live cell of the triple first; a cell that reappears in
    // the new value re-opens at its own (per-cell) version + 1, one absent from
    // the new value simply stays dead (VMP Part 5 per-(triple, index tuple)).
    let closed = if same_tier_live {
        close_out_long(conn, key, c, retained)?
    } else {
        0
    };

    // Physical batching (RESULTS.md alarm #1): the former per-cell `MAX(version)`
    // probe is replaced by ONE set-based query returning the current per-position
    // max, from which each cell's next version is computed in Rust. Long-tier
    // versions are per-cell (a key can hold pos0 at v2, pos1 at v1); canon
    // guarantees the cells of one write carry distinct position tuples, so this
    // snapshot (taken AFTER close-out, BEFORE any insert) yields exactly the
    // versions the per-cell probe did — each position advances independently.
    let cur_versions = current_long_versions(conn, key)?;

    // Encode every cell's row params up front, tracking the max new version.
    let mut max_version = 0u16;
    let mut rows_params: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    for row in rows {
        let pos = pad4_pos(&row.positions);
        let version = cur_versions.get(&pos).copied().unwrap_or(0) + 1;
        max_version = max_version.max(version);
        rows_params.push(long_row_params(key, pt_id, c, version, axis_ids, axes, &pos, &row.value));
    }

    // Batched multi-row INSERT (replaces the per-cell prepare/execute). Issued on
    // the caller's connection inside the open write transaction — no autocommit,
    // no separate connection; a write-write conflict or any error surfaces here
    // or at COMMIT exactly as before, and ROLLBACK unwinds every batched row, so
    // conflict-retry and crash atomicity are unchanged.
    insert_long_rows(conn, &rows_params)?;

    Ok((max_version, rows.len(), closed))
}

/// The current per-position `MAX(version)` for every position tuple the key
/// already holds (live OR superseded — versioning spans both, as the former
/// per-cell probe did: no `valid_to_commit` filter). One scan per key replaces
/// one probe per cell (RESULTS.md alarm #1).
fn current_long_versions(conn: &Connection, key: &ValueKey) -> Result<HashMap<[i32; 4], u16>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT i1_pos, i2_pos, i3_pos, i4_pos, MAX(version)::INTEGER \
         FROM value_long \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
         GROUP BY i1_pos, i2_pos, i3_pos, i4_pos",
    )?;
    let mut map = HashMap::new();
    let iter = stmt.query_map(
        params![key.entity_id, key.parameter_definition_id, key.alternative_id],
        |r| {
            let pos = [
                r.get::<_, i32>(0)?,
                r.get::<_, i32>(1)?,
                r.get::<_, i32>(2)?,
                r.get::<_, i32>(3)?,
            ];
            let v: i32 = r.get(4)?;
            Ok((pos, v as u16))
        },
    )?;
    for row in iter {
        let (pos, v) = row?;
        map.insert(pos, v);
    }
    Ok(map)
}

/// Rows per batched `value_long` INSERT. Chunked so the per-statement placeholder
/// count stays bounded (≤ CHUNK × 30); one prepared statement is reused across
/// all full-width chunks, so 8 760 cells cost ~17 prepares instead of 8 760.
const LONG_INSERT_CHUNK: usize = 512;

/// Insert the pre-encoded long-cell rows with as few statement prepares as
/// possible: a single reused prepared statement for the full-width chunks, plus
/// one tail statement for the remainder. All executes run on `conn` inside the
/// caller's transaction.
fn insert_long_rows(conn: &Connection, rows_params: &[Vec<Value>]) -> Result<(), DbError> {
    if rows_params.is_empty() {
        return Ok(());
    }
    let full = rows_params.len() / LONG_INSERT_CHUNK;
    if full > 0 {
        let sql = long_insert_sql(LONG_INSERT_CHUNK);
        let mut stmt = conn.prepare(&sql)?;
        for chunk in rows_params.chunks_exact(LONG_INSERT_CHUNK) {
            stmt.execute(duckdb::params_from_iter(chunk.iter().flatten()))?;
        }
    }
    let rem = &rows_params[full * LONG_INSERT_CHUNK..];
    if !rem.is_empty() {
        let sql = long_insert_sql(rem.len());
        conn.execute(&sql, duckdb::params_from_iter(rem.iter().flatten()))?;
    }
    Ok(())
}

/// A multi-row `value_long` INSERT with `n` row templates. Column order and the
/// `make_timestamp(?)` binds for the timestamp columns are identical to the
/// former per-cell statement — 30 columns (29 + the D11 `parameter_type_id`
/// binding), `content_retained` left to its TRUE default.
fn long_insert_sql(n: usize) -> String {
    const ROW: &str = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                        make_timestamp(?), ?, make_timestamp(?), ?, make_timestamp(?), ?, make_timestamp(?), ?, \
                        ?, ?, ?, ?, ?, make_timestamp(?), ?, ?)";
    let mut s = String::with_capacity(320 + n * (ROW.len() + 1));
    s.push_str(
        "INSERT INTO value_long \
           (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, \
            axis1_id, axis2_id, axis3_id, axis4_id, i1_pos, i2_pos, i3_pos, i4_pos, \
            i1_dt, i1_num, i2_dt, i2_num, i3_dt, i3_num, i4_dt, i4_num, \
            value_kind, v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros) \
         VALUES ",
    );
    for i in 0..n {
        if i > 0 {
            s.push(',');
        }
        s.push_str(ROW);
    }
    s
}

/// One long cell's 30 bind values, in the column order of [`long_insert_sql`].
/// The timestamp columns bind their raw µs (wrapped by `make_timestamp(?)` in
/// the SQL); the value columns come straight from [`VCols`], so the encoder
/// path — including its NaN canonicalization — is untouched.
#[allow(clippy::too_many_arguments)]
fn long_row_params(
    key: &ValueKey,
    pt_id: Option<i64>,
    c: i64,
    version: u16,
    axis_ids: &[i64],
    axes: &[CanonicalAxis],
    pos: &[i32; 4],
    value: &ScalarValue,
) -> Vec<Value> {
    // Per-dimension index accelerators, derived from the bound axis at the
    // cell's position (audited by audit_index_accelerator; never authoritative).
    let mut dt = [None::<i64>; 4];
    let mut num = [None::<i64>; 4];
    for (d, ax) in axes.iter().enumerate() {
        let (a_dt, a_num) = accel(ax, pos[d]);
        dt[d] = a_dt;
        num[d] = a_num;
    }
    let vc = VCols::of(value);
    let mut p: Vec<Value> = Vec::with_capacity(30);
    p.push(Value::BigInt(key.entity_id));
    p.push(Value::BigInt(key.parameter_definition_id));
    p.push(Value::BigInt(key.alternative_id));
    p.push(pt_id.map(Value::BigInt).unwrap_or(Value::Null));
    p.push(Value::BigInt(c));
    p.push(Value::SmallInt(version as i16));
    push_axis4(&mut p, axis_ids);
    for &ip in pos {
        p.push(Value::Int(ip));
    }
    for d in 0..4 {
        p.push(opt_bigint(dt[d]));
        p.push(opt_bigint(num[d]));
    }
    p.push(Value::Text(vc.kind.to_string()));
    p.push(opt_double(vc.f));
    p.push(opt_bigint(vc.i));
    p.push(opt_bool(vc.b));
    p.push(opt_text(vc.s));
    p.push(opt_bigint(vc.dt));
    p.push(opt_int(vc.dm));
    p.push(opt_bigint(vc.du));
    p
}

fn opt_bigint(v: Option<i64>) -> Value {
    v.map(Value::BigInt).unwrap_or(Value::Null)
}
fn opt_int(v: Option<i32>) -> Value {
    v.map(Value::Int).unwrap_or(Value::Null)
}
fn opt_double(v: Option<f64>) -> Value {
    v.map(Value::Double).unwrap_or(Value::Null)
}
fn opt_bool(v: Option<bool>) -> Value {
    v.map(Value::Boolean).unwrap_or(Value::Null)
}
fn opt_text(v: Option<String>) -> Value {
    v.map(Value::Text).unwrap_or(Value::Null)
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
