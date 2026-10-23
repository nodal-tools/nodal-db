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
//!     (`INSERT INTO value_array_hist SELECT *, c, TRUE FROM value_array
//!     WHERE array_id = ?`) so snapshot consistency and crash atomicity stay
//!     engine-guaranteed, then the current row is replaced in place (or, on
//!     delete/transition, deleted). Every statement that touches the list
//!     payload is keyed by the row's `array_id` (resolved narrowly by the key
//!     triple in `live_tier`), so DuckDB serves it with an index scan instead
//!     of scanning the payload of the whole table. `array_id` is a physical row
//!     handle, never canonical content: minted by the column DEFAULT on INSERT,
//!     never updated, copied verbatim into `_hist`; a delete or tier
//!     transition followed by a re-insert mints a new one.
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
//! **Physical write mechanism (appender_write_path_PLAN D1, D4)**: every
//! statement is plain SQL on the caller's connection, prepared once per
//! transaction through the connection's statement cache (`prepare_cached`; the
//! cache capacity is raised in `conn`). The bulk inserts bind each column
//! as ONE list parameter ("columnar bind"): a long value is
//! `INSERT … SELECT ?, unnest(?::T[]), …` per ≤ `LONG_INSERT_CHUNK` cells,
//! an array payload is one bound `?::DOUBLE[]`. The statement text no longer
//! depends on the cell count, so DuckDB parses it once per transaction, not
//! once per value. The encoder is still the one [`VCols::of`]; timestamps
//! still cross as µs through `make_timestamp`; floats arrive canonical. A
//! columnar insert zips its `unnest`s by position and NULL-pads a short list
//! silently, so a hard length check (all builds) guards every execution.
//!
//! **Retention**: the superseded row (scalar/long close-out, array `_hist` copy)
//! keeps its content by default (`content_retained = TRUE`). `force_replace`
//! (or an explicit `retain_override = Some(false)`) writes a stub instead —
//! `content_retained = FALSE` with the payload columns nulled — so the prior
//! content is not preserved (VMP Part 5 retention).

use std::collections::HashMap;

use duckdb::types::Value;
use duckdb::{params, Connection, OptionalExt};

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
    debug_assert_value_floats_canonical(value);
    let retained = keeps_content(opts);

    // --- 1. Resolve the value's axes (array/long tiers) via content-verified
    // dedup, threading the caller's cache. Count created vs reused by the axis
    // population delta (dedup itself returns only the id). A scalar has no
    // axes: both counts are skipped (two statements per scalar value). ---
    let mut axis_ids: Vec<i64> = Vec::with_capacity(value.axes.len());
    let axes_created = if value.axes.is_empty() {
        0
    } else {
        let axes_before = axis_count(conn)?;
        for ax in &value.axes {
            axis_ids.push(dedup_or_insert(conn, cache, ax)?);
        }
        (axis_count(conn)? - axes_before) as usize
    };
    let axes_reused = value.axes.len() - axes_created;

    // --- 2. Current state: which tier (if any) holds a live row for the key. ---
    let current = live_tier(conn, &key)?;
    let target = value.tier;
    let same_tier_live = current.map(Live::tier) == Some(target);

    // --- 3. Tier transition: close out / move the OLD tier's current row. ---
    let mut rows_closed = 0usize;
    if let Some(cur) = current {
        if cur.tier() != target {
            rows_closed += close_out_tier(conn, &key, commit_id, cur, retained)?;
        }
    }

    // --- 4. Write into the target tier. ---
    let (version, rows_written, closed_same) = match (&value.shape, target) {
        (ShapedData::Scalar(m), Tier::Scalar) => write_scalar(
            conn,
            &key,
            commit_id,
            m,
            same_tier_live,
            retained,
            parameter_type_id,
        )?,
        (ShapedData::Array(vals), Tier::Array) => write_array(
            conn,
            &key,
            commit_id,
            &axis_ids,
            vals,
            current.and_then(Live::array_id),
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
///
/// Scalar-only is SAFE by construction: the reconcile diff never emits a
/// `SetDefault`/`ClearDefault` action while a non-scalar (long/array) default
/// row exists — those are warn-and-skip (review item 6) — so this close can
/// never orphan a live default in another tier.
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

/// The `rename_list_member` value rewrite (template_ir_plan §4.2 Layer 2 —
/// the one data-touching hint), under the FULL history protocol: for every
/// CURRENT string value of `pd_id` equal to `from`, close the row at
/// `commit_id`, insert the successor (`version + 1`, D11 binding carried,
/// value = `to`), and — when the definition does not retain content
/// (`retained = false`, VMP Part 5's stub contract) — turn the closed row
/// into a stub (payload nulled, `content_retained = FALSE`). Sequenced
/// close → reinsert → stub so the reinsert can still read the pre-image.
/// Returns the number of values rewritten.
pub(crate) fn rewrite_scalar_string_values(
    conn: &Connection,
    commit_id: i64,
    pd_id: i64,
    from: &str,
    to: &str,
    retained: bool,
) -> Result<usize, DbError> {
    conn.execute(
        "UPDATE value_scalar SET valid_to_commit = ?1 \
         WHERE valid_to_commit IS NULL AND parameter_definition_id = ?2 \
           AND value_kind = 'string' AND v_str = ?3",
        params![commit_id, pd_id, from],
    )?;
    let written = conn.execute(
        "INSERT INTO value_scalar \
           (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, content_retained, value_kind, v_str) \
         SELECT entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
                ?1, version + 1, TRUE, 'string', ?2 \
         FROM value_scalar \
         WHERE valid_to_commit = ?1 AND parameter_definition_id = ?3 \
           AND value_kind = 'string' AND v_str = ?4",
        params![commit_id, to, pd_id, from],
    )?;
    if !retained {
        conn.execute(
            "UPDATE value_scalar SET content_retained = FALSE, \
               v_float=NULL, v_int=NULL, v_bool=NULL, v_str=NULL, v_datetime=NULL, \
               v_dur_months=NULL, v_dur_micros=NULL \
             WHERE valid_to_commit = ?1 AND parameter_definition_id = ?2 \
               AND value_kind = 'string' AND v_str = ?3",
            params![commit_id, pd_id, from],
        )?;
    }
    Ok(written)
}

/// The `unmapped_values: drop` arm of the G6 translation table
/// (template_ir_plan §4.2 v2, D14/D15 revision): close out every CURRENT
/// scalar value of `pd_id` that matches NO translation key (a non-string row
/// never matches a string key, so it drops too — the v65 semantics: anything
/// outside the table falls to the definition's default). Versioned close-out
/// through the history protocol — retention (`retained`) honored exactly like
/// [`rewrite_scalar_string_values`]. The caller guarantees the definition has
/// no current long/array rows (the scalar-only map guard). Returns rows
/// closed.
pub(crate) fn drop_scalar_values_not_matching(
    conn: &Connection,
    commit_id: i64,
    pd_id: i64,
    keys: &[&str],
    retained: bool,
) -> Result<usize, DbError> {
    // Build `NOT (value_kind = 'string' AND v_str IN (?, ?, …))` with one
    // placeholder per key (keys is never empty: map_values is validated
    // non-empty at load).
    let placeholders = vec!["?"; keys.len()].join(", ");
    let match_pred = format!("(value_kind = 'string' AND v_str IN ({placeholders}))");
    let mut binds: Vec<duckdb::types::Value> = vec![commit_id.into(), pd_id.into()];
    binds.extend(
        keys.iter()
            .map(|k| duckdb::types::Value::Text((*k).to_string())),
    );
    let closed = conn.execute(
        &format!(
            "UPDATE value_scalar SET valid_to_commit = ? \
             WHERE valid_to_commit IS NULL AND parameter_definition_id = ? \
               AND NOT {match_pred}"
        ),
        duckdb::params_from_iter(binds.iter()),
    )?;
    if !retained && closed > 0 {
        conn.execute(
            "UPDATE value_scalar SET content_retained = FALSE, \
               v_float=NULL, v_int=NULL, v_bool=NULL, v_str=NULL, v_datetime=NULL, \
               v_dur_months=NULL, v_dur_micros=NULL \
             WHERE valid_to_commit = ?1 AND parameter_definition_id = ?2 \
               AND content_retained",
            params![commit_id, pd_id],
        )?;
    }
    Ok(closed)
}

/// The defaults/prefills leg of the same rewrite (`definition_value_scalar`,
/// both slots). Defaults are authored definition content, always retained
/// (same stance as [`write_definition_default`]); the close-out keeps its
/// pre-image. Returns rows rewritten.
pub(crate) fn rewrite_definition_string_values(
    conn: &Connection,
    commit_id: i64,
    pd_id: i64,
    from: &str,
    to: &str,
) -> Result<usize, DbError> {
    conn.execute(
        "UPDATE definition_value_scalar SET valid_to_commit = ?1 \
         WHERE valid_to_commit IS NULL AND parameter_definition_id = ?2 \
           AND value_kind = 'string' AND v_str = ?3",
        params![commit_id, pd_id, from],
    )?;
    Ok(conn.execute(
        "INSERT INTO definition_value_scalar \
           (parameter_definition_id, slot, parameter_type_id, valid_from_commit, \
            version, content_retained, value_kind, v_str) \
         SELECT parameter_definition_id, slot, parameter_type_id, ?1, version + 1, \
                TRUE, 'string', ?2 \
         FROM definition_value_scalar \
         WHERE valid_to_commit = ?1 AND parameter_definition_id = ?3 \
           AND value_kind = 'string' AND v_str = ?4",
        params![commit_id, to, pd_id, from],
    )?)
}

// ---------------------------------------------------------------------------
// Current-state detection
// ---------------------------------------------------------------------------

fn axis_count(conn: &Connection) -> Result<i64, DbError> {
    Ok(conn
        .prepare_cached("SELECT count(*) FROM axis")?
        .query_row([], |r| r.get(0))?)
}

/// The live state of a key triple: the tier holding its current row and, for
/// the array tier, that row's `array_id` (the physical handle every
/// payload-touching statement is keyed by).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Live {
    Scalar,
    Long,
    Array(i64),
}

impl Live {
    fn tier(self) -> Tier {
        match self {
            Live::Scalar => Tier::Scalar,
            Live::Long => Tier::Long,
            Live::Array(_) => Tier::Array,
        }
    }

    fn array_id(self) -> Option<i64> {
        match self {
            Live::Array(id) => Some(id),
            _ => None,
        }
    }
}

/// The tier currently holding a live row for the key triple, or `None`. At most
/// one tier is live per triple (enforced by `audit_cross_tier_duplicate`); the
/// array table stores only live rows, so its mere presence is "live". The array
/// probe is narrow (it reads `array_id` by the triple, never the payload); the
/// UNIQUE key triple guarantees at most one row.
fn live_tier(conn: &Connection, key: &ValueKey) -> Result<Option<Live>, DbError> {
    let triple = params![
        key.entity_id,
        key.parameter_definition_id,
        key.alternative_id
    ];
    let scalar: i64 = conn
        .prepare_cached(
            "SELECT count(*) FROM value_scalar WHERE entity_id=? AND parameter_definition_id=? \
             AND alternative_id=? AND valid_to_commit IS NULL",
        )?
        .query_row(triple, |r| r.get(0))?;
    if scalar > 0 {
        return Ok(Some(Live::Scalar));
    }
    let long: i64 = conn
        .prepare_cached(
            "SELECT count(*) FROM value_long WHERE entity_id=? AND parameter_definition_id=? \
             AND alternative_id=? AND valid_to_commit IS NULL",
        )?
        .query_row(triple, |r| r.get(0))?;
    if long > 0 {
        return Ok(Some(Live::Long));
    }
    let array_id: Option<i64> = conn
        .prepare_cached(
            "SELECT array_id FROM value_array WHERE entity_id=? AND parameter_definition_id=? \
             AND alternative_id=?",
        )?
        .query_row(triple, |r| r.get(0))
        .optional()?;
    Ok(array_id.map(Live::Array))
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
    conn.prepare_cached(
        "INSERT INTO value_scalar \
           (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, \
            content_retained, value_kind, v_float, v_int, v_bool, v_str, v_datetime, \
            v_dur_months, v_dur_micros) \
         VALUES (?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, make_timestamp(?), ?, ?)",
    )?
    .execute(params![
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
    ])?;
    Ok((version, 1, closed))
}

fn close_out_scalar(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    retained: bool,
) -> Result<usize, DbError> {
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
    Ok(conn.prepare_cached(sql)?.execute(params![
        c,
        key.entity_id,
        key.parameter_definition_id,
        key.alternative_id
    ])?)
}

fn next_version_scalar(conn: &Connection, key: &ValueKey) -> Result<u16, DbError> {
    let v: i32 = conn
        .prepare_cached(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM value_scalar \
             WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?",
        )?
        .query_row(
            params![
                key.entity_id,
                key.parameter_definition_id,
                key.alternative_id
            ],
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
    live_array_id: Option<i64>,
    retained: bool,
    pt_id: Option<i64>,
) -> Result<(u16, usize, usize), DbError> {
    let (vmin, vmax) = min_max(vals);
    // The payload binds as ONE `?::DOUBLE[]` parameter (columnar bind, plan
    // D2): the canonical floats verbatim, so the statement text is the same
    // for every length and is prepared once per transaction. The explicit
    // cast types an empty list (duckdb-rs binds `[]` as VARCHAR[]).
    let payload = Value::List(vals.iter().map(|&v| Value::Double(v)).collect());

    if let Some(array_id) = live_array_id {
        // Pre-image copy to the twin — entirely in SQL (never client-buffered).
        copy_array_preimage(conn, array_id, c, retained)?;
        let version = next_version_array(conn, key)?;
        // Replace the current row in place (bump version, new payload), keyed
        // by its array_id, which is never updated. Both payload columns are set
        // so a stale values_int can't break the XOR.
        let mut p: Vec<Value> = vec![Value::BigInt(c), Value::SmallInt(version as i16)];
        p.push(pt_id.map(Value::BigInt).unwrap_or(Value::Null));
        push_axis4(&mut p, axis_ids);
        p.push(payload);
        p.push(Value::Double(vmin));
        p.push(Value::Double(vmax));
        p.push(Value::BigInt(array_id));
        conn.prepare_cached(
            "UPDATE value_array SET valid_from_commit=?, version=?, parameter_type_id=?, \
               axis1_id=?, axis2_id=?, axis3_id=?, axis4_id=?, \
               values=?::DOUBLE[], values_int=NULL, v_min=?, v_max=? \
             WHERE array_id=?",
        )?
        .execute(duckdb::params_from_iter(p))?;
        Ok((version, 1, 1))
    } else {
        let version = next_version_array(conn, key)?;
        // array_id is omitted: the column DEFAULT (seq_value_array) is its
        // only minter.
        let mut p: Vec<Value> = vec![
            Value::BigInt(key.entity_id),
            Value::BigInt(key.parameter_definition_id),
            Value::BigInt(key.alternative_id),
            pt_id.map(Value::BigInt).unwrap_or(Value::Null),
            Value::BigInt(c),
            Value::SmallInt(version as i16),
        ];
        push_axis4(&mut p, axis_ids);
        p.push(payload);
        p.push(Value::Double(vmin));
        p.push(Value::Double(vmax));
        conn.prepare_cached(
            "INSERT INTO value_array \
               (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
                valid_from_commit, version, \
                axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::DOUBLE[], NULL, ?, ?)",
        )?
        .execute(duckdb::params_from_iter(p))?;
        Ok((version, 1, 0))
    }
}

/// SQL-side pre-image copy of the live array row into `value_array_hist`
/// (VMP Part 5), keyed by its `array_id` (an index scan; the id is copied
/// verbatim). Retained → `SELECT *, c, TRUE` (column parity: `array_id` is the
/// first column of both tables); stub → payload columns nulled with
/// `content_retained = FALSE`. The pre-image never leaves SQL.
fn copy_array_preimage(
    conn: &Connection,
    array_id: i64,
    c: i64,
    retained: bool,
) -> Result<usize, DbError> {
    let sql = if retained {
        "INSERT INTO value_array_hist SELECT *, ?, TRUE FROM value_array WHERE array_id=?"
    } else {
        "INSERT INTO value_array_hist \
           (array_id, entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, \
            axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max, \
            valid_to_commit, content_retained) \
         SELECT array_id, entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
            valid_from_commit, version, \
            axis1_id, axis2_id, axis3_id, axis4_id, NULL, NULL, v_min, v_max, ?, FALSE \
         FROM value_array WHERE array_id=?"
    };
    Ok(conn.prepare_cached(sql)?.execute(params![c, array_id])?)
}

fn next_version_array(conn: &Connection, key: &ValueKey) -> Result<u16, DbError> {
    let v: i32 = conn
        .prepare_cached(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM ( \
                SELECT version FROM value_array \
                  WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
                UNION ALL \
                SELECT version FROM value_array_hist \
                  WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=?)",
        )?
        .query_row(
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

    // Encode every cell column-wise in one pass, tracking the max new version.
    let cols = LongColumns::build(rows, axes, &cur_versions);
    let max_version = cols.version.iter().copied().max().unwrap_or(0) as u16;

    // Columnar-bind INSERT (plan D1): one cached statement, one execution per
    // ≤ LONG_INSERT_CHUNK cells. Issued on the caller's connection inside the
    // open write transaction — no autocommit, no separate connection; a
    // write-write conflict or any error surfaces here or at COMMIT, and
    // ROLLBACK unwinds every row, so conflict-retry and crash atomicity are
    // unchanged. Rows land in canonical cell order (DuckDB's default
    // `preserve_insertion_order`, which the write connection never changes).
    let consts = LongConsts {
        entity_id: key.entity_id,
        parameter_definition_id: key.parameter_definition_id,
        alternative_id: key.alternative_id,
        parameter_type_id: pt_id,
        valid_from_commit: c,
        axis_ids,
    };
    insert_long_columns(conn, &consts, &cols)?;

    Ok((max_version, rows.len(), closed))
}

/// The current per-position `MAX(version)` for every position tuple the key
/// already holds (live OR superseded — versioning spans both, as the former
/// per-cell probe did: no `valid_to_commit` filter). One scan per key replaces
/// one probe per cell (RESULTS.md alarm #1).
fn current_long_versions(
    conn: &Connection,
    key: &ValueKey,
) -> Result<HashMap<[i32; 4], u16>, DbError> {
    let mut stmt = conn.prepare_cached(
        "SELECT i1_pos, i2_pos, i3_pos, i4_pos, MAX(version)::INTEGER \
         FROM value_long \
         WHERE entity_id=? AND parameter_definition_id=? AND alternative_id=? \
         GROUP BY i1_pos, i2_pos, i3_pos, i4_pos",
    )?;
    let mut map = HashMap::new();
    let iter = stmt.query_map(
        params![
            key.entity_id,
            key.parameter_definition_id,
            key.alternative_id
        ],
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

/// Cells per columnar `value_long` INSERT execution (plan D5, critique C7).
/// Every list element is a Rust `Value` plus a transient `duckdb_value` while
/// the bind copies it, so a whole 1 M-cell value bound at once peaks near
/// 1 GB; 16 384 cells keep the peak at ~200 MB (mostly DuckDB's
/// transaction-local storage) at the same speed. An 8 760-cell series is one
/// execution. The statement text does not depend on the chunk length.
const LONG_INSERT_CHUNK: usize = 16_384;

/// The per-key constants of a long write: bound as scalars, not lists.
struct LongConsts<'a> {
    entity_id: i64,
    parameter_definition_id: i64,
    alternative_id: i64,
    parameter_type_id: Option<i64>,
    valid_from_commit: i64,
    axis_ids: &'a [i64],
}

/// The per-cell columns of one long value, built column-wise in ONE pass over
/// the canonical rows (plan D4). Still the one encoder: the value columns come
/// from [`VCols::of`], the accelerators from [`accel`], the positions from
/// [`pad4_pos`] and the versions from the per-position version map. Each
/// column holds exactly one Rust type, so each binds as a list of one `Value`
/// variant (duckdb-rs rejects a list mixing variants).
#[derive(Default)]
struct LongColumns {
    version: Vec<i16>,
    pos: [Vec<i32>; 4],
    dt: [Vec<Option<i64>>; 4],
    num: [Vec<Option<i64>>; 4],
    kind: Vec<&'static str>,
    f: Vec<Option<f64>>,
    i: Vec<Option<i64>>,
    b: Vec<Option<bool>>,
    s: Vec<Option<String>>,
    vdt: Vec<Option<i64>>,
    dm: Vec<Option<i32>>,
    du: Vec<Option<i64>>,
}

impl LongColumns {
    fn build(
        rows: &[crate::types::LongRow],
        axes: &[CanonicalAxis],
        cur_versions: &HashMap<[i32; 4], u16>,
    ) -> LongColumns {
        let mut c = LongColumns::default();
        for row in rows {
            let pos = pad4_pos(&row.positions);
            let version = cur_versions.get(&pos).copied().unwrap_or(0) + 1;
            c.version.push(version as i16);
            // Per-dimension index accelerators, derived from the bound axis at
            // the cell's position (audited by audit_index_accelerator; never
            // authoritative). NULL beyond the declared rank.
            for (d, &p) in pos.iter().enumerate() {
                c.pos[d].push(p);
                let (a_dt, a_num) = axes.get(d).map_or((None, None), |ax| accel(ax, p));
                c.dt[d].push(a_dt);
                c.num[d].push(a_num);
            }
            let vc = VCols::of(&row.value);
            c.kind.push(vc.kind);
            c.f.push(vc.f);
            c.i.push(vc.i);
            c.b.push(vc.b);
            c.s.push(vc.s);
            c.vdt.push(vc.dt);
            c.dm.push(vc.dm);
            c.du.push(vc.du);
        }
        c
    }

    fn len(&self) -> usize {
        self.version.len()
    }

    /// Every per-cell column must hold exactly one entry per cell. Part of the
    /// hard length check (critique C2): a short column whose chunk is all
    /// NULL would otherwise bind as a plain NULL and pass the list check.
    fn check_column_lengths(&self) -> Result<(), DbError> {
        let n = self.len();
        let lens = [
            ("pos", self.pos.iter().map(Vec::len).collect::<Vec<_>>()),
            ("dt", self.dt.iter().map(Vec::len).collect()),
            ("num", self.num.iter().map(Vec::len).collect()),
            (
                "value",
                vec![
                    self.kind.len(),
                    self.f.len(),
                    self.i.len(),
                    self.b.len(),
                    self.s.len(),
                    self.vdt.len(),
                    self.dm.len(),
                    self.du.len(),
                ],
            ),
        ];
        for (group, ls) in &lens {
            if ls.iter().any(|&l| l != n) {
                return Err(DbError::Internal(format!(
                    "columnar insert: {group} column lengths {ls:?} differ from {n} cells"
                )));
            }
        }
        Ok(())
    }

    /// The 30 bind values of one execution over cells `r`, in the column order
    /// of [`LONG_INSERT_SQL`]: per-key constants as scalars, per-cell columns
    /// as lists. A column that is NULL in every cell of the chunk binds a
    /// plain NULL (the zip pads it).
    fn params(&self, k: &LongConsts, r: std::ops::Range<usize>) -> Vec<Value> {
        let mut p: Vec<Value> = Vec::with_capacity(30);
        p.push(Value::BigInt(k.entity_id));
        p.push(Value::BigInt(k.parameter_definition_id));
        p.push(Value::BigInt(k.alternative_id));
        p.push(k.parameter_type_id.map_or(Value::Null, Value::BigInt));
        p.push(Value::BigInt(k.valid_from_commit));
        p.push(list(clip(&self.version, &r), |&v| Value::SmallInt(v)));
        push_axis4(&mut p, k.axis_ids);
        for d in 0..4 {
            p.push(list(clip(&self.pos[d], &r), |&v| Value::Int(v)));
        }
        for d in 0..4 {
            p.push(opt_list(clip(&self.dt[d], &r), |&v| Value::BigInt(v)));
            p.push(opt_list(clip(&self.num[d], &r), |&v| Value::BigInt(v)));
        }
        p.push(list(clip(&self.kind, &r), |&v| Value::Text(v.to_string())));
        p.push(opt_list(clip(&self.f, &r), |&v| Value::Double(v)));
        p.push(opt_list(clip(&self.i, &r), |&v| Value::BigInt(v)));
        p.push(opt_list(clip(&self.b, &r), |&v| Value::Boolean(v)));
        p.push(opt_list(clip(&self.s, &r), |v| Value::Text(v.clone())));
        p.push(opt_list(clip(&self.vdt, &r), |&v| Value::BigInt(v)));
        p.push(opt_list(clip(&self.dm, &r), |&v| Value::Int(v)));
        p.push(opt_list(clip(&self.du, &r), |&v| Value::BigInt(v)));
        p
    }
}

/// The columnar `value_long` INSERT (plan Appendix A). Per-key constants are
/// scalar `?`; every per-cell column is `unnest(?::T[])`, zipped by position;
/// the timestamp columns cross as BIGINT µs through `make_timestamp`, the
/// same conversion as every other timestamp write. 30 columns (29 + the D11
/// `parameter_type_id` binding); `content_retained` keeps its TRUE default.
const LONG_INSERT_SQL: &str = "INSERT INTO value_long \
       (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
        valid_from_commit, version, \
        axis1_id, axis2_id, axis3_id, axis4_id, i1_pos, i2_pos, i3_pos, i4_pos, \
        i1_dt, i1_num, i2_dt, i2_num, i3_dt, i3_num, i4_dt, i4_num, \
        value_kind, v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros) \
     SELECT ?, ?, ?, ?, ?, unnest(?::SMALLINT[]), ?, ?, ?, ?, \
       unnest(?::INTEGER[]), unnest(?::INTEGER[]), unnest(?::INTEGER[]), unnest(?::INTEGER[]), \
       make_timestamp(unnest(?::BIGINT[])), unnest(?::BIGINT[]), \
       make_timestamp(unnest(?::BIGINT[])), unnest(?::BIGINT[]), \
       make_timestamp(unnest(?::BIGINT[])), unnest(?::BIGINT[]), \
       make_timestamp(unnest(?::BIGINT[])), unnest(?::BIGINT[]), \
       unnest(?::VARCHAR[]), unnest(?::DOUBLE[]), unnest(?::BIGINT[]), unnest(?::BOOLEAN[]), \
       unnest(?::VARCHAR[]), make_timestamp(unnest(?::BIGINT[])), \
       unnest(?::INTEGER[]), unnest(?::BIGINT[])";

/// Insert one long value's cells: one cached columnar statement, executed per
/// ≤ [`LONG_INSERT_CHUNK`] cells after the hard length check. All executes run
/// on `conn` inside the caller's transaction; a 0-cell chunk never executes.
fn insert_long_columns(
    conn: &Connection,
    k: &LongConsts,
    cols: &LongColumns,
) -> Result<(), DbError> {
    let n = cols.len();
    cols.check_column_lengths()?;
    if n == 0 {
        return Ok(());
    }
    let mut stmt = conn.prepare_cached(LONG_INSERT_SQL)?;
    let mut lo = 0;
    while lo < n {
        let hi = (lo + LONG_INSERT_CHUNK).min(n);
        let p = cols.params(k, lo..hi);
        check_list_lengths(&p, hi - lo)?;
        stmt.execute(duckdb::params_from_iter(p))?;
        lo = hi;
    }
    Ok(())
}

/// The columnar-bind guard (plan D4, critique C2), in EVERY build. The
/// `unnest`s of one SELECT zip by position and NULL-pad a shorter (or empty)
/// list silently, so a list of the wrong length would write NULL into the
/// nullable columns with no error. Every bound list must have exactly `n`
/// elements; a plain NULL parameter is the deliberate all-NULL column. Costs
/// a few dozen integer compares per execution.
pub(crate) fn check_list_lengths(params: &[Value], n: usize) -> Result<(), DbError> {
    for (i, v) in params.iter().enumerate() {
        if let Value::List(items) = v {
            if items.len() != n {
                return Err(DbError::Internal(format!(
                    "columnar insert: bind parameter {} has {} elements, expected {n}",
                    i + 1,
                    items.len()
                )));
            }
        }
    }
    Ok(())
}

/// The part of `col` inside `r` (never panics: a column shorter than the range
/// yields a shorter slice, which the length checks reject).
fn clip<'a, T>(col: &'a [T], r: &std::ops::Range<usize>) -> &'a [T] {
    let end = r.end.min(col.len());
    &col[r.start.min(end)..end]
}

/// A list parameter of one `Value` variant.
pub(crate) fn list<T>(col: &[T], f: impl Fn(&T) -> Value) -> Value {
    Value::List(col.iter().map(f).collect())
}

/// A list parameter whose elements may be NULL; a plain `NULL` when every
/// element is (the zip-unnest pads it, and an all-NULL list would carry no
/// element type).
pub(crate) fn opt_list<T>(col: &[Option<T>], f: impl Fn(&T) -> Value) -> Value {
    if col.iter().all(Option::is_none) {
        return Value::Null;
    }
    Value::List(
        col.iter()
            .map(|v| v.as_ref().map_or(Value::Null, &f))
            .collect(),
    )
}

fn close_out_long(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    retained: bool,
) -> Result<usize, DbError> {
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
    Ok(conn.prepare_cached(sql)?.execute(params![
        c,
        key.entity_id,
        key.parameter_definition_id,
        key.alternative_id
    ])?)
}

// ---------------------------------------------------------------------------
// Tier close-out dispatch (transition + delete)
// ---------------------------------------------------------------------------

fn close_out_tier(
    conn: &Connection,
    key: &ValueKey,
    c: i64,
    live: Live,
    retained: bool,
) -> Result<usize, DbError> {
    match live {
        Live::Scalar => close_out_scalar(conn, key, c, retained),
        Live::Long => close_out_long(conn, key, c, retained),
        Live::Array(array_id) => {
            let moved = copy_array_preimage(conn, array_id, c, retained)?;
            conn.prepare_cached("DELETE FROM value_array WHERE array_id=?")?
                .execute([array_id])?;
            Ok(moved)
        }
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// The typed value columns of one scalar measure — mostly-NULL, exactly one
/// (or the duration pair) populated. `v_datetime`/`i*_dt` bind as µs through
/// `make_timestamp(?)` at the call site. `pub(crate)` so the template
/// reconcile writes `list_value` members through the SAME scalar-column
/// encoding as every value row (one encoding site, review item on scalar
/// unification).
pub(crate) struct VCols {
    pub(crate) kind: &'static str,
    pub(crate) f: Option<f64>,
    pub(crate) i: Option<i64>,
    pub(crate) b: Option<bool>,
    pub(crate) s: Option<String>,
    pub(crate) dt: Option<i64>,
    pub(crate) dm: Option<i32>,
    pub(crate) du: Option<i64>,
}

/// The narrow fallible constructor for TEMPLATE literals: the v1 IR literal
/// kinds only (float/int/bool/string). `date_time`/`duration` scalars are
/// legal values but have no IR literal form — a loud error here, so the
/// template writers can never smuggle one in via a code path the validator
/// did not see.
pub(crate) fn literal_cols(v: &ScalarValue) -> Result<VCols, DbError> {
    match v {
        ScalarValue::Float(_)
        | ScalarValue::Int(_)
        | ScalarValue::Bool(_)
        | ScalarValue::Str(_) => Ok(VCols::of(v)),
        other => Err(DbError::Api(format!(
            "scalar kind {other:?} has no v1 template literal form"
        ))),
    }
}

impl VCols {
    pub(crate) fn of(v: &ScalarValue) -> VCols {
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
        p.push(
            axis_ids
                .get(d)
                .map(|&id| Value::BigInt(id))
                .unwrap_or(Value::Null),
        );
    }
}

/// Value floats reach `write_value` already canonical (the pipeline's
/// `canon::read_value` normalizes them); `history` never re-canonicalizes. A
/// hand-built `CanonicalValue` (tests, out-of-contract tools) holding `-0.0` or
/// a non-canonical NaN is caught here in debug builds.
fn debug_assert_value_floats_canonical(value: &CanonicalValue) {
    if !cfg!(debug_assertions) {
        return;
    }
    let ok = |x: f64| x.to_bits() == crate::encoding::canonical_value_f64(x).to_bits();
    let scalar_ok = |m: &ScalarValue| match m {
        ScalarValue::Float(x) => ok(*x),
        _ => true,
    };
    let all_ok = match &value.shape {
        ShapedData::Scalar(m) => scalar_ok(m),
        ShapedData::Array(vals) => vals.iter().all(|x| ok(*x)),
        ShapedData::Long(rows) => rows.iter().all(|r| scalar_ok(&r.value)),
    };
    debug_assert!(
        all_ok,
        "write_value got a non-canonical value float (-0.0 or a non-canonical NaN); \
         values must come from the canonicalization pipeline"
    );
}

/// `v_min` / `v_max` pruning metadata under DuckDB's total order, where NaN
/// sorts above every number: any NaN makes `v_max` NaN, and an all-NaN array
/// has `v_min` NaN too. (Skipping NaN would let a range-pruned query skip an
/// array whose NaN cells match, and `audit_array_integrity`, which compares
/// against `list_aggregate(values, 'min'/'max')`, would flag it.) Inputs are
/// canonical, so a NaN result is the canonical NaN.
fn min_max(vals: &[f64]) -> (f64, f64) {
    let mut lo = f64::INFINITY;
    let mut hi = f64::NEG_INFINITY;
    let mut any_nan = false;
    let mut any_num = false;
    for &v in vals {
        if v.is_nan() {
            any_nan = true;
            continue;
        }
        any_num = true;
        if v < lo {
            lo = v;
        }
        if v > hi {
            hi = v;
        }
    }
    let nan = f64::from_bits(crate::encoding::CANONICAL_NAN_BITS);
    if any_nan {
        hi = nan;
    }
    if !any_num {
        lo = nan;
    }
    (lo, hi)
}

/// Constraint rejection through the NEW statements, driven at the internal
/// entry points (appender_write_path_PLAN §7.2), plus the hard length check
/// (critique C2). Every failure must surface as the error it is, roll the
/// transaction back, and leave no rows and no `commit` row behind.
#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::conn::with_write_txn;
    use crate::encoding::AxisProperties;
    use crate::schema_init::init_schema;
    use crate::types::{LongRow, ValueKind};

    const KEY: ValueKey = ValueKey {
        entity_id: 1,
        parameter_definition_id: 2,
        alternative_id: 3,
    };

    fn fresh() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("t.duckdb");
        init_schema(&db).unwrap();
        (dir, db)
    }

    fn int_axis(len: i32) -> CanonicalAxis {
        CanonicalAxis {
            length: len,
            properties: AxisProperties::default(),
            body: AxisBody::IntegerParametric { start: 0, step: 1 },
        }
    }

    fn rows(n: i32) -> Vec<LongRow> {
        (0..n)
            .map(|p| LongRow {
                positions: vec![p],
                value: ScalarValue::Float(p as f64),
            })
            .collect()
    }

    fn long_value(n: i32) -> CanonicalValue {
        CanonicalValue {
            tier: Tier::Long,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(n)],
            shape: ShapedData::Long(rows(n)),
        }
    }

    /// Row counts of `commit` and every value table.
    fn counts(db: &Path) -> Vec<i64> {
        let conn = Connection::open(db).unwrap();
        let v = [
            "commit",
            "value_scalar",
            "value_long",
            "value_array",
            "value_array_hist",
        ]
        .iter()
        .map(|t| {
            conn.query_row(&format!("SELECT count(*) FROM {t}"), [], |r| r.get(0))
                .unwrap()
        })
        .collect();
        conn.close().unwrap();
        v
    }

    /// Run `f` in a write transaction; assert it fails with a DuckDB error
    /// whose message contains `needle`, and that nothing was persisted.
    fn assert_rejected<F>(db: &Path, needle: &str, f: F)
    where
        F: FnOnce(&Connection, i64) -> Result<(), DbError>,
    {
        let before = counts(db);
        let err = with_write_txn(db, "t", "reject", f).expect_err("must be rejected");
        assert!(
            matches!(err, DbError::Duckdb(_)),
            "expected DbError::Duckdb, got {err:?}"
        );
        assert!(
            err.to_string().contains(needle),
            "error {err} lacks {needle:?}"
        );
        assert_eq!(counts(db), before, "rollback left rows or a commit row");
    }

    fn consts(c: i64, axis_ids: &[i64]) -> LongConsts<'_> {
        LongConsts {
            entity_id: KEY.entity_id,
            parameter_definition_id: KEY.parameter_definition_id,
            alternative_id: KEY.alternative_id,
            parameter_type_id: None,
            valid_from_commit: c,
            axis_ids,
        }
    }

    fn axis_id(conn: &Connection, len: i32) -> i64 {
        dedup_or_insert(conn, &mut AxisCache::new(), &int_axis(len)).unwrap()
    }

    #[test]
    fn length_check_rejects_a_short_column() {
        // A shorter list would be NULL-padded silently by the zip-unnest; the
        // check must fail the write instead (in every build).
        let (_d, db) = fresh();
        let before = counts(&db);
        let err = with_write_txn(&db, "t", "short", |conn, c| {
            let ax = axis_id(conn, 3);
            let mut cols = LongColumns::build(&rows(3), &[int_axis(3)], &HashMap::new());
            cols.f.pop();
            insert_long_columns(conn, &consts(c, &[ax]), &cols)
        })
        .expect_err("a short column must be rejected");
        assert!(matches!(err, DbError::Internal(_)), "got {err:?}");
        assert_eq!(counts(&db), before);

        // An empty list against a non-empty chunk is caught too; a plain NULL
        // (the all-NULL column) is not a list and passes.
        assert!(check_list_lengths(&[Value::List(vec![])], 1).is_err());
        assert!(check_list_lengths(&[Value::Null, list(&[1i32], |&v| Value::Int(v))], 1).is_ok());
    }

    #[test]
    fn second_current_long_cell_violates_unique() {
        let (_d, db) = fresh();
        assert_rejected(&db, "UNIQUE constraint violation", |conn, c| {
            let ax = axis_id(conn, 3);
            let cols = LongColumns::build(&rows(3), &[int_axis(3)], &HashMap::new());
            insert_long_columns(conn, &consts(c, &[ax]), &cols)?;
            insert_long_columns(conn, &consts(c, &[ax]), &cols)
        });
    }

    #[test]
    fn bad_axis_on_long_violates_foreign_key() {
        let (_d, db) = fresh();
        assert_rejected(&db, "foreign key", |conn, c| {
            let cols = LongColumns::build(&rows(2), &[int_axis(2)], &HashMap::new());
            insert_long_columns(conn, &consts(c, &[999_999]), &cols)
        });
    }

    #[test]
    fn bad_axis_on_array_violates_foreign_key() {
        let (_d, db) = fresh();
        assert_rejected(&db, "foreign key", |conn, c| {
            write_array(conn, &KEY, c, &[999_999], &[1.0, 2.0], None, true, None).map(|_| ())
        });
    }

    #[test]
    fn fresh_array_insert_for_a_live_triple_violates_unique() {
        let (_d, db) = fresh();
        assert_rejected(&db, "UNIQUE constraint violation", |conn, c| {
            let ax = axis_id(conn, 2);
            write_array(conn, &KEY, c, &[ax], &[1.0, 2.0], None, true, None)?;
            write_array(conn, &KEY, c, &[ax], &[3.0, 4.0], None, true, None).map(|_| ())
        });
    }

    #[test]
    fn long_rewrite_in_one_commit_violates_the_span_check() {
        // Writing one key twice in one commit closes its cells at their own
        // valid_from_commit: a zero-length span.
        let (_d, db) = fresh();
        assert_rejected(&db, "CHECK constraint failed", |conn, c| {
            let mut cache = AxisCache::new();
            let o = WriteOptions::default();
            write_value(conn, c, KEY, &long_value(3), &mut cache, &o, None)?;
            write_value(conn, c, KEY, &long_value(3), &mut cache, &o, None).map(|_| ())
        });
    }

    #[test]
    fn empty_array_binds_as_an_empty_double_list() {
        // The bound empty list is typed by the `?::DOUBLE[]` cast (duckdb-rs
        // binds `[]` as VARCHAR[]). Canon never emits a 0-length array; this
        // pins the bind mechanics only.
        let (_d, db) = fresh();
        with_write_txn(&db, "t", "empty", |conn, c| {
            let ax = axis_id(conn, 1);
            write_array(conn, &KEY, c, &[ax], &[], None, true, None)
        })
        .unwrap();
        let conn = Connection::open(&db).unwrap();
        let (len, ty): (i64, String) = conn
            .query_row(
                "SELECT len(values), typeof(values) FROM value_array",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((len, ty.as_str()), (0, "DOUBLE[]"));
    }
}
