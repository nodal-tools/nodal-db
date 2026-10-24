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
//!     handle, never canonical content: minted by its sequence
//!     (`seq_value_array`, through the column DEFAULT for a one-key write,
//!     pre-minted and assigned in input order for a set-based round), never
//!     updated, copied verbatim into `_hist`; a delete or tier transition
//!     followed by a re-insert mints a new one.
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
//! statement is plain SQL on the caller's connection. The hot-path statements
//! (the `write_value` protocol: tier probes, version scans, close-outs, the
//! bulk inserts, the array update/delete and pre-image copies) are prepared
//! once per transaction through the connection's statement cache
//! (`prepare_cached`; the cache capacity is raised in `conn`); the rare paths
//! (definition defaults, the template string rewrites) use plain `execute`. The bulk inserts bind each column
//! as ONE list parameter ("columnar bind"): the long cells of a round are
//! `INSERT … SELECT … FROM (SELECT unnest(?::T[]), …)` per
//! ≤ `LONG_INSERT_CHUNK` cells, an array payload is one bound `?::DOUBLE[]`. The statement text no longer
//! depends on the cell count, so DuckDB parses it once per transaction, not
//! once per value. The encoder is still the one [`VCols::of`]; timestamps
//! still cross as µs through `make_timestamp`; floats arrive canonical. A
//! columnar insert zips its `unnest`s by position and NULL-pads a short list
//! silently, so a hard length check (all builds) guards every execution.
//!
//! **Set-based scalar write (set_based_scalar_write_PLAN)**: the scalar tier
//! has one writer, [`write_scalars`], which handles a batch of distinct keys
//! in rounds of ≤ `LONG_INSERT_CHUNK`: one grouped live + version probe, a
//! long/array live probe only for the keys not live in scalar, the per-key
//! `close_out_tier` for a tier transition, one close-out UPDATE per retention
//! mode, and one columnar INSERT. The API defers a commit's scalars into one
//! such call; [`write_value`] delegates a scalar to it with N = 1. Per-key
//! point probes cost a statement floor each, and DuckDB serves them with a
//! sequential scan rather than the compound indexes, so a set-based round is
//! what makes a scalar batch cost O(keys + table) per commit.
//!
//! **Set-based long write (set_based_long_and_delete_PLAN §3.2)**: the long
//! tier has one writer, [`write_longs`], on the scalar skeleton: per round
//! (≤ `LONG_INSERT_CHUNK` keys and cells, or one larger value) the scalar
//! live probe, one grouped long probe returning the per-position max
//! version and the current-cell count, the array live probe for the keys
//! live in neither, the per-key `close_out_tier` for a transition, one
//! close-out UPDATE per retention mode, and one columnar INSERT in the
//! key-ordinal form: the per-key columns bind as short lists that a
//! per-cell key ordinal indexes, and every `unnest` sits in a FROM subquery
//! (measured ~2x faster than SELECT-list `unnest`s for large values). The
//! API defers a commit's long values into one such call, with their axes
//! resolved at buffer time; [`write_value`] delegates a long value with
//! N = 1, which keeps the point probe and close-out statements.
//!
//! **Set-based array write (set_based_long_and_delete_PLAN §3.3)**: the array
//! tier has one writer, [`write_arrays`], on the same skeleton: per round
//! (≤ `LONG_INSERT_CHUNK` keys and ≤ `ARRAY_ROUND_ELEMENTS` payload
//! elements, or one larger value) the three live probes and one version
//! probe over `value_array` ∪ `value_array_hist`; for the keys whose
//! precedence tier is array, the pre-image copy by `array_id` and one
//! UPDATE in place with a nested `?::DOUBLE[][]` payload; for the others
//! one INSERT whose `array_id`s are pre-minted from the sequence, sorted
//! and assigned in input order (so they are exactly the ids sequential
//! inserts mint, with no reliance on insert or RETURNING order). The API
//! defers a commit's arrays like its long values; [`write_value`]
//! delegates with N = 1, which keeps today's per-key statements.
//!
//! **Set-based delete (set_based_long_and_delete_PLAN §3.4)**: deletes have
//! one implementation, [`delete_values`]: per round, the three live probes
//! (scalar; long with its current-cell count; array with its `array_id`)
//! under the per-key precedence, then per tier and retention mode one
//! close-out (the scalar and long UPDATEs; the array pre-image copy plus
//! DELETE by `array_id`). [`delete_value`] delegates with N = 1, which uses
//! the point statements.
//!
//! **Retention**: the superseded row (scalar/long close-out, array `_hist` copy)
//! keeps its content by default (`content_retained = TRUE`). `force_replace`
//! (or an explicit `retain_override = Some(false)`) writes a stub instead —
//! `content_retained = FALSE` with the payload columns nulled — so the prior
//! content is not preserved (VMP Part 5 retention).
//!
//! **Unchanged writes (skip_unchanged_writes_PLAN P1)**: a write whose stored
//! form equals the key's current stored value writes nothing — no close-out,
//! no insert, no `_hist` row — and reports [`WriteOutcome::unchanged`]. Equal
//! means: live in the same tier, the same binding (`parameter_type_id`, NULL
//! vs a member is a change), the same axis ids, and per row or cell the same
//! encoder output, compared on the RAW stored columns (`value_kind` and the
//! payload bits; arrays: `values` element bits and length, `values_int`
//! NULL) with `content_retained = TRUE`. The stored side is never
//! re-canonicalized and never compared by `content_hash`. Every doubt errs
//! toward "changed" (an extra version costs history; a false "unchanged"
//! would lose a write). The probes read the comparison columns in the same
//! statements as today (the scalar probe; the long tier's grouped
//! per-position probe, which is also its version source; the array live
//! probe's narrow columns as a prefilter, then the payload by `array_id`).
//!
//! **Long cell diff (skip_unchanged_writes_PLAN P2, §4)**: a long value is
//! merged with the key's current cells by position tuple. When every
//! current cell shares one set of axis ids, one `value_kind` and one
//! binding, equal to the new value's, a changed cell is closed out and
//! re-inserted at its version + 1, a removed cell is closed out, an added
//! cell is inserted at its position's max version + 1, and an equal cell is
//! left as it is (open span, same version); no difference at all is an
//! unchanged write. Any other key (other axes, kind or binding, or damaged
//! cells) is rewritten whole: every current cell closes, every new cell is
//! inserted, at the same per-position versions. Per-row spans keep as-of
//! reads exact; a value's current cells may come from several commits
//! (`value_timeline` reports value-level change spans,
//! `audit_long_value_coherence` checks the shared meta), and a diff appends
//! its inserted cells, so value rows have no physical-order contract.
//! Retention plays no part in equality: an unchanged write supersedes
//! nothing, so `force_replace` stubs nothing (D3), and a cell diff stubs
//! only the cells it closes. The no-commit rule (D1) lives in
//! `api::commit_changes`; a direct caller of these functions inside its own
//! `conn::with_write_txn` still commits its (possibly empty) commit row.

use std::borrow::Cow;
use std::collections::HashMap;

use duckdb::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray,
    PrimitiveArray, StringArray, StringViewArray,
};
use duckdb::arrow::datatypes::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::types::{TimeUnit, Value, ValueRef};
use duckdb::{params, Connection, OptionalExt};

use crate::axis::{dedup_or_insert_created, AxisCache};
use crate::canon::CanonicalValue;
use crate::encoding::{datetime_coord, AxisBody, CanonicalAxis};
use crate::error::DbError;
use crate::types::{
    ScalarValue, ShapedData, Tier, ValueKey, ValueKind, WriteOptions, WriteOutcome,
};

/// The comparison columns of a current value row (table alias `$t`), read
/// by the probes for the unchanged check: the 8 value columns, then the
/// row's binding and its `content_retained` (critique C3.4: a current stub
/// never compares equal). Plain columns, never aggregates: the probes read
/// the current rows in one UNION leg and the closed rows' max version in
/// another (an aggregate per column cost ~1-2 ms of planning per point
/// probe, measured).
macro_rules! cur_value_cols {
    ($t:literal) => {
        concat!(
            $t,
            ".value_kind, ",
            $t,
            ".v_float, ",
            $t,
            ".v_int, ",
            $t,
            ".v_bool, ",
            $t,
            ".v_str, ",
            $t,
            ".v_datetime, ",
            $t,
            ".v_dur_months, ",
            $t,
            ".v_dur_micros, ",
            $t,
            ".parameter_type_id, ",
            $t,
            ".content_retained"
        )
    };
}

/// The closed-rows leg's placeholders for [`cur_value_cols!`].
macro_rules! cur_value_nulls {
    () => {
        "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL"
    };
}

/// The long probes' current-cell columns: the cell's 4 axis ids, then its
/// [`cur_value_cols!`] block (table alias `l`).
macro_rules! long_cur_cols {
    () => {
        concat!(
            "l.axis1_id, l.axis2_id, l.axis3_id, l.axis4_id, ",
            cur_value_cols!("l")
        )
    };
}

/// The closed-rows leg's placeholders for [`long_cur_cols!`].
macro_rules! long_cur_nulls {
    () => {
        concat!("NULL, NULL, NULL, NULL, ", cur_value_nulls!())
    };
}

/// Should the superseded / pre-image row keep its content? `force_replace`
/// forces a stub; an explicit `retain_override` otherwise decides; default true.
pub(crate) fn keeps_content(opts: &WriteOptions) -> bool {
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

    // --- 0. A scalar goes through the one set-based scalar writer with
    // N = 1 (set_based_scalar_write_PLAN D2): it does its own live-tier
    // probe, transition and close-out, so no second scalar writer exists. ---
    if let ShapedData::Scalar(m) = &value.shape {
        let w = ScalarWrite {
            key,
            value: m.clone(),
            parameter_type_id,
            retained,
            entity_minted: false,
        };
        return Ok(write_scalars(conn, commit_id, std::slice::from_ref(&w))?[0]);
    }

    // --- 1. Resolve the value's axes via content-verified dedup, threading
    // the caller's cache (plan D8: dedup reports created vs reused). ---
    let (axis_ids, axes_created, axes_reused) = resolve_axes(conn, cache, &value.axes)?;

    // --- 2. A long or array value goes through the one set-based writer
    // of its tier with N = 1 (set_based_long_and_delete_PLAN §3.2-3.3). ---
    if value.tier == Tier::Long {
        let w = LongWrite {
            key,
            value: Cow::Borrowed(value),
            axis_ids,
            axes_created,
            axes_reused,
            parameter_type_id,
            retained,
            entity_minted: false,
        };
        return Ok(write_longs(conn, commit_id, std::slice::from_ref(&w))?[0]);
    }
    let w = ArrayWrite {
        key,
        value: Cow::Borrowed(value),
        axis_ids,
        axes_created,
        axes_reused,
        parameter_type_id,
        retained,
        entity_minted: false,
    };
    Ok(write_arrays(conn, commit_id, std::slice::from_ref(&w))?[0])
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
    let w = DeleteWrite {
        key,
        retained: keeps_content(opts),
    };
    Ok(delete_values(conn, commit_id, std::slice::from_ref(&w))?[0])
}

/// One key of a set-based delete and whether its closed rows keep their
/// content (`keeps_content`, resolved per key by the caller).
#[derive(Clone, Copy, Debug)]
pub(crate) struct DeleteWrite {
    pub(crate) key: ValueKey,
    pub(crate) retained: bool,
}

/// Delete the live value of every key of `w` at `commit_id` in set-based
/// rounds of ≤ [`LONG_INSERT_CHUNK`] keys (set_based_long_and_delete_PLAN
/// §3.4): the one delete implementation (`delete_value` delegates here with
/// N = 1). Per round: the live probes of the three tiers (scalar; long, for
/// the keys not live in scalar; array, for the keys live in neither), then
/// per tier and retention mode one close-out: the scalar and long UPDATEs,
/// and for the array tier the pre-image copy to `value_array_hist` plus the
/// DELETE, keyed by `array_id`. A round of one key uses the point
/// statements (`live_tier`, `close_out_tier`).
///
/// Semantics are those of one per-key `delete_value` per key: the live tier
/// by the per-key precedence (scalar, then long, then array), the same SET
/// lists and retention. A key with no live value closes nothing. Keys must
/// be distinct (the caller guarantees it); a repeated key or an affected-row
/// count that differs from the probes fails as [`DbError::Internal`].
/// Returns one [`WriteOutcome`] per input, in input order.
pub(crate) fn delete_values(
    conn: &Connection,
    commit_id: i64,
    w: &[DeleteWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    check_distinct_keys("delete batch", w.iter().map(|d| &d.key))?;
    let mut out = Vec::with_capacity(w.len());
    for chunk in w.chunks(LONG_INSERT_CHUNK) {
        out.extend(delete_round(conn, commit_id, chunk)?);
    }
    Ok(out)
}

/// One round of [`delete_values`] over ≤ [`LONG_INSERT_CHUNK`] distinct keys.
fn delete_round(
    conn: &Connection,
    c: i64,
    w: &[DeleteWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    let closed = if let [d] = w {
        // The point form of a one-key round (an editor's single delete):
        // today's statements, same rows.
        vec![match live_tier(conn, &d.key)? {
            Some(cur) => close_out_tier(conn, &d.key, c, cur, d.retained)?,
            None => 0,
        }]
    } else {
        let keys: Vec<&ValueKey> = w.iter().map(|d| &d.key).collect();
        let live = probe_live(conn, &keys)?;
        let targets: Vec<(&ValueKey, bool)> = w.iter().map(|d| (&d.key, d.retained)).collect();
        close_out_live(conn, c, &targets, &live)?
    };
    Ok(closed
        .into_iter()
        .map(|rows_closed| WriteOutcome {
            rows_closed,
            ..WriteOutcome::default()
        })
        .collect())
}

/// Fail as [`DbError::Internal`] when a batch names one key twice: the
/// set-based writers rely on distinct keys (a repeated key would be probed
/// once and written twice).
fn check_distinct_keys<'a>(
    what: &str,
    keys: impl Iterator<Item = &'a ValueKey>,
) -> Result<(), DbError> {
    let mut seen: std::collections::HashSet<Triple> = std::collections::HashSet::new();
    for k in keys {
        if !seen.insert(triple(k)) {
            return Err(DbError::Internal(format!("{what}: duplicate key {k:?}")));
        }
    }
    Ok(())
}

/// The live state of each key of `keys` that has one, by the per-key
/// precedence of [`live_tier`] (scalar, then long, then array), with the
/// number of its current rows (1 for scalar and array; the current cells
/// for long). The long probe reads only the keys not live in scalar, the
/// array probe only the keys live in neither.
fn probe_live(
    conn: &Connection,
    keys: &[&ValueKey],
) -> Result<HashMap<Triple, (Live, usize)>, DbError> {
    probe_live_meta(conn, keys, None)
}

/// [`probe_live`], and with `meta` the narrow comparison columns of every
/// array-live key's row too ([`ArrayMeta`], by `array_id`), read by the
/// same array probe (the array write's unchanged prefilter).
fn probe_live_meta(
    conn: &Connection,
    keys: &[&ValueKey],
    meta: Option<&mut HashMap<i64, ArrayMeta>>,
) -> Result<HashMap<Triple, (Live, usize)>, DbError> {
    let mut live: HashMap<Triple, (Live, usize)> = HashMap::new();
    for (t, _, _) in probe_keys(conn, SCALAR_LIVE_SQL, keys.iter().copied(), false)? {
        live.insert(t, (Live::Scalar, 1));
    }
    let rest: Vec<&ValueKey> = keys
        .iter()
        .copied()
        .filter(|k| !live.contains_key(&triple(k)))
        .collect();
    if rest.is_empty() {
        return Ok(live);
    }
    for (t, cells, _) in probe_keys(conn, LONG_CURRENT_SQL, rest.iter().copied(), false)? {
        live.insert(t, (Live::Long, cells as usize));
    }
    let rest: Vec<&ValueKey> = rest
        .into_iter()
        .filter(|k| !live.contains_key(&triple(k)))
        .collect();
    if rest.is_empty() {
        return Ok(live);
    }
    match meta {
        None => {
            for (t, array_id, _) in probe_keys(conn, ARRAY_LIVE_SQL, rest.iter().copied(), false)? {
                live.insert(t, (Live::Array(array_id), 1));
            }
        }
        Some(meta) => {
            let mut stmt =
                conn.prepare_cached(&format!("{SCALAR_KEYS_CTE}{ARRAY_LIVE_META_SQL}"))?;
            let rows = stmt.query_map(
                duckdb::params_from_iter(key_lists(rest.iter().copied())),
                |r| Ok(((r.get(0)?, r.get(1)?, r.get(2)?), ArrayMeta::read(r, 3)?)),
            )?;
            for row in rows {
                let (t, m): (Triple, ArrayMeta) = row?;
                live.insert(t, (Live::Array(m.array_id), 1));
                meta.insert(m.array_id, m);
            }
        }
    }
    Ok(live)
}

/// Close out the live rows of every key of `targets` (`(key, retained)`)
/// found in `live`: per tier and retention mode one statement (the scalar
/// and long UPDATEs; the array pre-image copy plus DELETE), each hard
/// checked against the probed row counts. Returns the rows closed per key,
/// in input order.
fn close_out_live(
    conn: &Connection,
    c: i64,
    targets: &[(&ValueKey, bool)],
    live: &HashMap<Triple, (Live, usize)>,
) -> Result<Vec<usize>, DbError> {
    let mut closed = vec![0usize; targets.len()];
    for retained in [true, false] {
        let mut scalar: Vec<usize> = Vec::new();
        let mut long: Vec<usize> = Vec::new();
        let mut array: Vec<usize> = Vec::new();
        for (i, (k, r)) in targets.iter().enumerate() {
            if *r != retained {
                continue;
            }
            match live.get(&triple(k)) {
                Some((Live::Scalar, _)) => scalar.push(i),
                Some((Live::Long, _)) => long.push(i),
                Some((Live::Array(_), _)) => array.push(i),
                None => {}
            }
        }
        let rows =
            |idx: &[usize]| -> usize { idx.iter().map(|&i| live[&triple(targets[i].0)].1).sum() };
        let lists = |idx: &[usize]| key_lists(idx.iter().map(|&i| targets[i].0));
        if !scalar.is_empty() {
            let sql = if retained {
                SCALAR_CLOSE_SQL
            } else {
                SCALAR_CLOSE_STUB_SQL
            };
            let [e, p, a] = lists(&scalar);
            let affected = conn
                .prepare_cached(sql)?
                .execute(duckdb::params_from_iter([Value::BigInt(c), e, p, a]))?;
            check_affected("scalar close-out", affected, rows(&scalar))?;
        }
        if !long.is_empty() {
            let sql = if retained {
                LONG_CLOSE_SQL
            } else {
                LONG_CLOSE_STUB_SQL
            };
            let [e, p, a] = lists(&long);
            let affected = conn
                .prepare_cached(sql)?
                .execute(duckdb::params_from_iter([Value::BigInt(c), e, p, a]))?;
            check_affected("long close-out", affected, rows(&long))?;
        }
        if !array.is_empty() {
            let ids = Value::List(
                array
                    .iter()
                    .map(|&i| match live[&triple(targets[i].0)].0 {
                        Live::Array(id) => Value::BigInt(id),
                        _ => unreachable!("array index of a non-array key"),
                    })
                    .collect(),
            );
            let sql = if retained {
                ARRAY_PREIMAGE_SET_SQL
            } else {
                ARRAY_PREIMAGE_STUB_SET_SQL
            };
            let copied = conn.prepare_cached(sql)?.execute(params![c, ids.clone()])?;
            check_affected("array pre-image copy", copied, array.len())?;
            let deleted = conn
                .prepare_cached(ARRAY_DELETE_SET_SQL)?
                .execute(params![ids])?;
            check_affected("array delete", deleted, array.len())?;
        }
        for i in scalar.into_iter().chain(long).chain(array) {
            closed[i] = live[&triple(targets[i].0)].1;
        }
    }
    Ok(closed)
}

/// The hard check (plan D12) of one set-based statement's affected rows.
fn check_affected(what: &str, affected: usize, expected: usize) -> Result<(), DbError> {
    if affected != expected {
        return Err(DbError::Internal(format!(
            "{what}: touched {affected} rows, expected {expected}"
        )));
    }
    Ok(())
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
/// (callers diff first — the template reconcile only calls this on change;
/// unlike the entity-scoped writers, which skip an unchanged value).
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
    live_below_scalar(conn, key)
}

/// [`live_tier`] for the single-key array write: the same precedence, and
/// for an array-live key the narrow comparison columns of its row
/// ([`ArrayMeta`]) from the same point read (never the payload).
fn live_tier_array_meta(
    conn: &Connection,
    key: &ValueKey,
) -> Result<Option<(Live, Option<ArrayMeta>)>, DbError> {
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
        return Ok(Some((Live::Scalar, None)));
    }
    let long: i64 = conn
        .prepare_cached(
            "SELECT count(*) FROM value_long WHERE entity_id=? AND parameter_definition_id=? \
             AND alternative_id=? AND valid_to_commit IS NULL",
        )?
        .query_row(triple, |r| r.get(0))?;
    if long > 0 {
        return Ok(Some((Live::Long, None)));
    }
    let m: Option<ArrayMeta> = conn
        .prepare_cached(ARRAY_LIVE_META_ONE_SQL)?
        .query_row(triple, |r| ArrayMeta::read(r, 0))
        .optional()?;
    Ok(m.map(|m| (Live::Array(m.array_id), Some(m))))
}

/// The long, then array, part of [`live_tier`] (for a key not live in
/// scalar).
fn live_below_scalar(conn: &Connection, key: &ValueKey) -> Result<Option<Live>, DbError> {
    let triple = params![
        key.entity_id,
        key.parameter_definition_id,
        key.alternative_id
    ];
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

/// One key of a set-based scalar write: the canonical scalar (owned, so a
/// buffer of these outlives the per-entity `canonicalize` result), the D11
/// member binding, and whether the superseded row keeps its content
/// (`keeps_content`, resolved per segment by the caller).
#[derive(Clone, Debug)]
pub(crate) struct ScalarWrite {
    pub(crate) key: ValueKey,
    pub(crate) value: ScalarValue,
    pub(crate) parameter_type_id: Option<i64>,
    pub(crate) retained: bool,
    /// The key's entity was minted by THIS commit, so no value row of any
    /// tier can exist for the key: the live-tier and version probes are
    /// skipped (version 1). Pass `false` when unsure; it only costs a probe.
    pub(crate) entity_minted: bool,
}

/// The zipped key lists every set-based scalar statement binds first
/// (`WITH k AS (SELECT unnest(e), unnest(p), unnest(a))`).
const SCALAR_KEYS_CTE: &str = "WITH k AS (SELECT unnest(?::BIGINT[]) AS e, \
     unnest(?::BIGINT[]) AS p, unnest(?::BIGINT[]) AS a) ";

/// Per key: the max version over all `value_scalar` rows of the triple
/// (current and closed: the `audit_history_tiling` partition), the count
/// of current rows, then the current row's version and comparison columns
/// (`any_value … FILTER (WHERE current)`: used only when exactly one row is
/// current, so `any_value` skipping NULLs cannot matter). One join with
/// aggregates: measured cheaper on a 10 000-key round than the two-leg
/// current/closed UNION the long probe uses (whose second join doubled the
/// probe), and bounded by the key count however long the histories are.
const SCALAR_PROBE_SQL: &str = "SELECT s.entity_id, s.parameter_definition_id, s.alternative_id, \
            max(s.version)::INTEGER, count(*) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.version::INTEGER) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.value_kind) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.v_float) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.v_int) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.v_bool) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.v_str) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.v_datetime) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.v_dur_months) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.v_dur_micros) FILTER (WHERE s.valid_to_commit IS NULL), \
            any_value(s.parameter_type_id) FILTER (WHERE s.valid_to_commit IS NULL), \
            bool_and(s.content_retained) FILTER (WHERE s.valid_to_commit IS NULL) \
     FROM value_scalar s JOIN k ON s.entity_id = k.e \
          AND s.parameter_definition_id = k.p AND s.alternative_id = k.a \
     GROUP BY ALL";

/// The keys with a current `value_long` row.
const LONG_LIVE_SQL: &str = "SELECT DISTINCT l.entity_id, l.parameter_definition_id, \
            l.alternative_id, 0::BIGINT \
     FROM value_long l JOIN k ON l.entity_id = k.e \
          AND l.parameter_definition_id = k.p AND l.alternative_id = k.a \
     WHERE l.valid_to_commit IS NULL";

/// The keys with a current `value_scalar` row.
const SCALAR_LIVE_SQL: &str = "SELECT s.entity_id, s.parameter_definition_id, s.alternative_id, \
            0::BIGINT \
     FROM value_scalar s JOIN k ON s.entity_id = k.e \
          AND s.parameter_definition_id = k.p AND s.alternative_id = k.a \
     WHERE s.valid_to_commit IS NULL";

/// The keys with current `value_long` cells and the number of those cells.
const LONG_CURRENT_SQL: &str = "SELECT l.entity_id, l.parameter_definition_id, l.alternative_id, \
            count(*) \
     FROM value_long l JOIN k ON l.entity_id = k.e \
          AND l.parameter_definition_id = k.p AND l.alternative_id = k.a \
     WHERE l.valid_to_commit IS NULL \
     GROUP BY ALL";

/// The set form of the retained `close_out_long`: every current cell of the
/// bound keys.
const LONG_CLOSE_SQL: &str = "UPDATE value_long SET valid_to_commit = ? \
     FROM (SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                  unnest(?::BIGINT[]) AS a) k \
     WHERE value_long.entity_id = k.e AND value_long.parameter_definition_id = k.p \
       AND value_long.alternative_id = k.a AND value_long.valid_to_commit IS NULL";

/// The stub form of [`LONG_CLOSE_SQL`]: the SET list of the stub
/// `close_out_long`.
const LONG_CLOSE_STUB_SQL: &str = "UPDATE value_long SET valid_to_commit = ?, \
       content_retained = FALSE, v_float = NULL, v_int = NULL, v_bool = NULL, v_str = NULL, \
       v_datetime = NULL, v_dur_months = NULL, v_dur_micros = NULL \
     FROM (SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                  unnest(?::BIGINT[]) AS a) k \
     WHERE value_long.entity_id = k.e AND value_long.parameter_definition_id = k.p \
       AND value_long.alternative_id = k.a AND value_long.valid_to_commit IS NULL";

/// The set form of the retained `copy_array_preimage`, keyed by the bound
/// `array_id`s.
const ARRAY_PREIMAGE_SET_SQL: &str = "INSERT INTO value_array_hist \
     SELECT *, ?, TRUE FROM value_array WHERE array_id IN (SELECT unnest(?::BIGINT[]))";

/// The set form of the stub `copy_array_preimage` (the same column list).
const ARRAY_PREIMAGE_STUB_SET_SQL: &str = "INSERT INTO value_array_hist \
       (array_id, entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
        valid_from_commit, version, \
        axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max, \
        valid_to_commit, content_retained) \
     SELECT array_id, entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
        valid_from_commit, version, \
        axis1_id, axis2_id, axis3_id, axis4_id, NULL, NULL, v_min, v_max, ?, FALSE \
     FROM value_array WHERE array_id IN (SELECT unnest(?::BIGINT[]))";

/// The set form of the array arm of `close_out_tier`'s DELETE.
const ARRAY_DELETE_SET_SQL: &str =
    "DELETE FROM value_array WHERE array_id IN (SELECT unnest(?::BIGINT[]))";

/// The keys with a `value_array` row (the live table holds only current
/// rows) and its `array_id`; never the payload.
const ARRAY_LIVE_SQL: &str = "SELECT v.entity_id, v.parameter_definition_id, v.alternative_id, \
            v.array_id \
     FROM value_array v JOIN k ON v.entity_id = k.e \
          AND v.parameter_definition_id = k.p AND v.alternative_id = k.a";

/// [`ARRAY_LIVE_SQL`] widened by the narrow comparison columns of the row
/// ([`ArrayMeta`]): never a list column (CLAUDE.md invariant 5), so it
/// stays a key-triple probe.
const ARRAY_LIVE_META_SQL: &str =
    "SELECT v.entity_id, v.parameter_definition_id, v.alternative_id, \
            v.array_id, v.version::INTEGER, v.parameter_type_id, \
            v.axis1_id, v.axis2_id, v.axis3_id, v.axis4_id, v.v_min, v.v_max \
     FROM value_array v JOIN k ON v.entity_id = k.e \
          AND v.parameter_definition_id = k.p AND v.alternative_id = k.a";

/// The single-key form of [`ARRAY_LIVE_META_SQL`].
const ARRAY_LIVE_META_ONE_SQL: &str = "SELECT array_id, version::INTEGER, parameter_type_id, \
            axis1_id, axis2_id, axis3_id, axis4_id, v_min, v_max \
     FROM value_array WHERE entity_id = ? AND parameter_definition_id = ? AND alternative_id = ?";

/// The stored payloads of the bound `array_id`s, for the unchanged check:
/// `(array_id, values, values_int IS NULL)`. Keyed by `array_id` alone
/// behind the `OFFSET 0` fence, so the payload is read through the
/// `array_id` index (CLAUDE.md invariants 5 and 6; guarded by
/// `tests/array_id_index_scan.rs`).
#[doc(hidden)]
pub const ARRAY_PAYLOAD_SQL: &str = "SELECT array_id, values, values_int IS NULL FROM \
       (SELECT array_id, values, values_int FROM value_array \
        WHERE array_id IN (SELECT unnest(?::BIGINT[])) OFFSET 0)";

/// The narrow columns of a live array row the unchanged prefilter compares:
/// its handle, version, binding, axes and pruning bounds.
#[derive(Clone, Copy, Debug)]
struct ArrayMeta {
    array_id: i64,
    version: u16,
    pt: Option<i64>,
    axis: [Option<i64>; 4],
    vmin: Option<f64>,
    vmax: Option<f64>,
}

impl ArrayMeta {
    /// Read `array_id, version, parameter_type_id, axis1..4_id, v_min,
    /// v_max` starting at column `o`.
    fn read(r: &duckdb::Row, o: usize) -> duckdb::Result<ArrayMeta> {
        Ok(ArrayMeta {
            array_id: r.get(o)?,
            version: r.get::<_, i32>(o + 1)? as u16,
            pt: r.get(o + 2)?,
            axis: [r.get(o + 3)?, r.get(o + 4)?, r.get(o + 5)?, r.get(o + 6)?],
            vmin: r.get(o + 7)?,
            vmax: r.get(o + 8)?,
        })
    }

    /// The prefilter: can the row hold the new value? A mismatch of the
    /// binding, any axis id or a pruning bound proves a change; a match
    /// proves nothing (the payload bytes decide).
    fn may_equal(&self, a: &ArrayWrite) -> bool {
        let (lo, hi) = min_max(a.vals());
        let axis: [Option<i64>; 4] = std::array::from_fn(|d| a.axis_ids.get(d).copied());
        self.pt == a.parameter_type_id
            && self.axis == axis
            && self.vmin.is_some_and(|x| same_bound(x, lo))
            && self.vmax.is_some_and(|x| same_bound(x, hi))
    }
}

/// The `array_id`s among `cands` (`(array_id, new payload)`) whose stored
/// payload equals the new one: `values` set with the same length and the
/// same bits per element (raw, no canonicalization; a NULL element never
/// matches), `values_int` NULL. Read through Arrow (no per-element
/// allocation), keyed by `array_id` ([`ARRAY_PAYLOAD_SQL`]).
fn array_payloads_equal(
    conn: &Connection,
    cands: &[(i64, &[f64])],
) -> Result<std::collections::HashSet<i64>, DbError> {
    use duckdb::arrow::array::{Array, AsArray};
    use duckdb::arrow::datatypes::Float64Type;
    let mut equal = std::collections::HashSet::new();
    if cands.is_empty() {
        return Ok(equal);
    }
    let want: HashMap<i64, &[f64]> = cands.iter().copied().collect();
    let ids = Value::List(cands.iter().map(|&(id, _)| Value::BigInt(id)).collect());
    let mut stmt = conn.prepare_cached(ARRAY_PAYLOAD_SQL)?;
    for batch in stmt.query_arrow(params![ids])? {
        let id_col = batch
            .column(0)
            .as_primitive::<duckdb::arrow::datatypes::Int64Type>();
        let no_int = batch.column(2).as_boolean();
        let vals = batch.column(1);
        for row in 0..batch.num_rows() {
            if id_col.is_null(row) || no_int.is_null(row) || !no_int.value(row) || vals.is_null(row)
            {
                continue;
            }
            let id = id_col.value(row);
            let Some(new) = want.get(&id) else {
                continue;
            };
            let stored = if let Some(l) = vals.as_list_opt::<i32>() {
                l.value(row)
            } else if let Some(l) = vals.as_list_opt::<i64>() {
                l.value(row)
            } else {
                return Err(DbError::Internal(format!(
                    "array payload read: unexpected Arrow type {:?}",
                    vals.data_type()
                )));
            };
            let Some(stored) = stored.as_primitive_opt::<Float64Type>() else {
                continue;
            };
            if stored.null_count() == 0
                && stored.len() == new.len()
                && stored
                    .values()
                    .iter()
                    .zip(new.iter())
                    .all(|(a, b)| a.to_bits() == b.to_bits())
            {
                equal.insert(id);
            }
        }
    }
    Ok(equal)
}

/// Same-tier close-out of the current rows of the bound keys, content
/// retained.
const SCALAR_CLOSE_SQL: &str = "UPDATE value_scalar SET valid_to_commit = ? \
     FROM (SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                  unnest(?::BIGINT[]) AS a) k \
     WHERE value_scalar.entity_id = k.e AND value_scalar.parameter_definition_id = k.p \
       AND value_scalar.alternative_id = k.a AND value_scalar.valid_to_commit IS NULL";

/// The stub form of [`SCALAR_CLOSE_SQL`]: the same SET list as the per-key
/// stub close-out (`content_retained = FALSE`, the 7 payload columns NULL).
const SCALAR_CLOSE_STUB_SQL: &str = "UPDATE value_scalar SET valid_to_commit = ?, \
       content_retained = FALSE, v_float = NULL, v_int = NULL, v_bool = NULL, v_str = NULL, \
       v_datetime = NULL, v_dur_months = NULL, v_dur_micros = NULL \
     FROM (SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                  unnest(?::BIGINT[]) AS a) k \
     WHERE value_scalar.entity_id = k.e AND value_scalar.parameter_definition_id = k.p \
       AND value_scalar.alternative_id = k.a AND value_scalar.valid_to_commit IS NULL";

/// The columnar `value_scalar` INSERT: the commit is a scalar bind, every
/// other column a zipped list; `content_retained` is the literal TRUE and
/// `v_datetime` crosses as µs through `make_timestamp`, as every timestamp
/// write does.
const SCALAR_INSERT_SQL: &str = "INSERT INTO value_scalar \
       (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
        valid_from_commit, version, content_retained, value_kind, \
        v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros) \
     SELECT unnest(?::BIGINT[]), unnest(?::BIGINT[]), unnest(?::BIGINT[]), unnest(?::BIGINT[]), \
       ?, unnest(?::SMALLINT[]), TRUE, unnest(?::VARCHAR[]), \
       unnest(?::DOUBLE[]), unnest(?::BIGINT[]), unnest(?::BOOLEAN[]), unnest(?::VARCHAR[]), \
       make_timestamp(unnest(?::BIGINT[])), unnest(?::INTEGER[]), unnest(?::BIGINT[])";

/// The single-key form of [`SCALAR_PROBE_SQL`]: every row of the triple
/// (one key's history), with the current flag and the comparison columns,
/// folded by [`ScalarProbe`]. Plain columns: the aggregate form cost ~1.7
/// ms more per point probe (planning and execution, measured), the
/// editor's single-value save.
const SCALAR_PROBE_ONE_SQL: &str = concat!(
    "SELECT s.version::INTEGER, (s.valid_to_commit IS NULL)::BIGINT, ",
    cur_value_cols!("s"),
    " FROM value_scalar s \
     WHERE s.entity_id = ? AND s.parameter_definition_id = ? AND s.alternative_id = ?"
);

type Triple = (i64, i64, i64);

/// One scalar key as its probe rows fold: the max version over all its
/// rows, the number of current rows and, when there is exactly one, that
/// row's version and comparison columns.
#[derive(Default)]
struct ScalarProbe {
    max_version: Option<i64>,
    current: i64,
    row: Option<(u16, CurRow)>,
}

impl ScalarProbe {
    /// Fold one probe row: version at `o`, current flag at `o + 1`, the
    /// comparison block from `o + 2`.
    fn push(&mut self, r: &duckdb::Row, o: usize) -> duckdb::Result<()> {
        let version: i32 = r.get(o)?;
        let cur: i64 = r.get(o + 1)?;
        self.max_version = Some(
            self.max_version
                .map_or(version as i64, |m| m.max(version as i64)),
        );
        if cur > 0 {
            self.current += cur;
            self.row = if self.current == 1 {
                Some((version as u16, CurRow::read(r, o + 2, 1)?))
            } else {
                None
            };
        }
        Ok(())
    }
}

/// A key triple as three scalar binds (the point-filter statements).
fn point(k: &ValueKey) -> [i64; 3] {
    [k.entity_id, k.parameter_definition_id, k.alternative_id]
}

fn triple(k: &ValueKey) -> Triple {
    (k.entity_id, k.parameter_definition_id, k.alternative_id)
}

/// The three key lists `(entity, parameter_definition, alternative)`.
fn key_lists<'a>(keys: impl Iterator<Item = &'a ValueKey> + Clone) -> [Value; 3] {
    [
        Value::List(keys.clone().map(|k| Value::BigInt(k.entity_id)).collect()),
        Value::List(
            keys.clone()
                .map(|k| Value::BigInt(k.parameter_definition_id))
                .collect(),
        ),
        Value::List(keys.map(|k| Value::BigInt(k.alternative_id)).collect()),
    ]
}

/// Run one keyed probe (`SCALAR_KEYS_CTE` + `sql`) whose rows are
/// `(triple, i64, i64)`; a probe without a second payload column returns 0.
fn probe_keys<'a>(
    conn: &Connection,
    sql: &str,
    keys: impl Iterator<Item = &'a ValueKey> + Clone,
    two: bool,
) -> Result<Vec<(Triple, i64, i64)>, DbError> {
    let mut stmt = conn.prepare_cached(&format!("{SCALAR_KEYS_CTE}{sql}"))?;
    let rows = stmt.query_map(duckdb::params_from_iter(key_lists(keys)), |r| {
        Ok((
            (r.get(0)?, r.get(1)?, r.get(2)?),
            r.get::<_, i64>(3)?,
            if two { r.get::<_, i64>(4)? } else { 0 },
        ))
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

/// Write every scalar of `w` at `commit_id` in set-based rounds of
/// ≤ [`LONG_INSERT_CHUNK`] keys (set_based_scalar_write_PLAN §3.2): the one
/// scalar history implementation (`write_value` delegates here with N = 1).
/// Per round: a grouped live + version probe of `value_scalar`, a live probe
/// of `value_long` and `value_array` for the keys the first probe found not
/// live, the per-key `close_out_tier` for each key live in another tier
/// (long close-out; array pre-image copy + DELETE, all in SQL), one
/// same-tier close-out UPDATE per retention mode, and one columnar INSERT.
/// The keys' entities minted by this commit skip the probes (version 1).
///
/// Semantics are those of one per-key `write_value` per key in input order:
/// the live tier by the per-key precedence (scalar, then long, then array),
/// `version = max(all rows of the triple) + 1`, the same close-out SQL and
/// retention. Keys must be distinct (the callers guarantee it); a repeated
/// key, a close-out count that differs from the probe, or a list of the
/// wrong length fails as [`DbError::Internal`], never as silent damage.
/// Returns one [`WriteOutcome`] per input, in input order.
pub(crate) fn write_scalars(
    conn: &Connection,
    commit_id: i64,
    w: &[ScalarWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    let mut seen: std::collections::HashSet<Triple> =
        std::collections::HashSet::with_capacity(w.len());
    for s in w {
        debug_assert_scalar_float_canonical(&s.value);
        if !seen.insert(triple(&s.key)) {
            return Err(DbError::Internal(format!(
                "scalar batch: duplicate key {:?}",
                s.key
            )));
        }
    }
    let mut out = Vec::with_capacity(w.len());
    for chunk in w.chunks(LONG_INSERT_CHUNK) {
        out.extend(write_scalar_round(conn, commit_id, chunk)?);
    }
    Ok(out)
}

/// One round of [`write_scalars`] over ≤ [`LONG_INSERT_CHUNK`] distinct keys.
fn write_scalar_round(
    conn: &Connection,
    c: i64,
    w: &[ScalarWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    let n = w.len();
    // A round of ONE key (an editor's single-value save, `write_value`)
    // binds its probes and close-out as plain point filters instead of list
    // joins (plan D3(b), a statement-form switch only): same rows, same
    // control flow, about half the per-statement cost at N = 1.
    let single = n == 1;
    // --- Probes (only for keys whose entity existed before this commit). ---
    // The scalar probe also reads the current row (version and comparison
    // columns) for the unchanged check (skip_unchanged_writes_PLAN §3.1).
    let mut max_version: HashMap<Triple, i64> = HashMap::new();
    let mut live: HashMap<Triple, Live> = HashMap::new();
    let mut current: HashMap<Triple, (u16, CurRow)> = HashMap::new();
    let probed = w.iter().filter(|s| !s.entity_minted).map(|s| &s.key);
    if single && probed.clone().next().is_some() {
        let k = &w[0].key;
        let mut probe = ScalarProbe::default();
        {
            let mut stmt = conn.prepare_cached(SCALAR_PROBE_ONE_SQL)?;
            let mut rows = stmt.query(point(k))?;
            while let Some(r) = rows.next()? {
                probe.push(r, 0)?;
            }
        }
        if let Some(v) = probe.max_version {
            max_version.insert(triple(k), v);
        }
        let cur = probe.current;
        if let Some(row) = probe.row {
            current.insert(triple(k), row);
        }
        let cur = if cur > 0 {
            Some(Live::Scalar)
        } else {
            live_below_scalar(conn, k)?
        };
        if let Some(l) = cur {
            live.insert(triple(k), l);
        }
    } else if probed.clone().next().is_some() {
        {
            let mut stmt = conn.prepare_cached(&format!("{SCALAR_KEYS_CTE}{SCALAR_PROBE_SQL}"))?;
            let mut rows = stmt.query(duckdb::params_from_iter(key_lists(probed.clone())))?;
            while let Some(r) = rows.next()? {
                let t: Triple = (r.get(0)?, r.get(1)?, r.get(2)?);
                max_version.insert(t, r.get(3)?);
                let cur: i64 = r.get(4)?;
                if cur > 0 {
                    live.insert(t, Live::Scalar);
                }
                if let Some(v) = r.get::<_, Option<i32>>(5)? {
                    current.insert(t, (v as u16, CurRow::read(r, 6, cur)?));
                }
            }
        }
        // Long and array only for the keys not live in scalar (zero keys for a
        // pure replace batch, so both statements are skipped). Precedence as
        // in `live_tier`: scalar, then long, then array.
        let rest: Vec<&ValueKey> = probed.filter(|k| !live.contains_key(&triple(k))).collect();
        if !rest.is_empty() {
            let long = probe_keys(conn, LONG_LIVE_SQL, rest.iter().copied(), false)?;
            let array = probe_keys(conn, ARRAY_LIVE_SQL, rest.iter().copied(), false)?;
            for (t, _, _) in long {
                live.insert(t, Live::Long);
            }
            for (t, array_id, _) in array {
                live.entry(t).or_insert(Live::Array(array_id));
            }
        }
    }

    // --- Unchanged keys: live in scalar with a current row equal to the
    // new one (kind, payload bits, binding; content retained). They write
    // nothing: no close-out, no insert (skip_unchanged_writes_PLAN §2.1). A
    // key live in another tier is always a change (the tier is a function
    // of the content). ---
    let unchanged: Vec<Option<u16>> = w
        .iter()
        .map(|s| {
            let t = triple(&s.key);
            match (live.get(&t), current.get(&t)) {
                (Some(Live::Scalar), Some((version, row)))
                    if row.equals(&s.value, s.parameter_type_id) =>
                {
                    Some(*version)
                }
                _ => None,
            }
        })
        .collect();

    // --- Tier transitions: the per-key close-out of the other tier, in
    // input order (plan D4). ---
    let mut closed = vec![0usize; n];
    for (i, s) in w.iter().enumerate() {
        if let Some(&cur @ (Live::Long | Live::Array(_))) = live.get(&triple(&s.key)) {
            closed[i] = close_out_tier(conn, &s.key, c, cur, s.retained)?;
        }
    }

    // --- Same-tier close-out: one UPDATE per retention mode. ---
    for retained in [true, false] {
        let idx: Vec<usize> = (0..n)
            .filter(|&i| {
                unchanged[i].is_none()
                    && w[i].retained == retained
                    && live.get(&triple(&w[i].key)) == Some(&Live::Scalar)
            })
            .collect();
        if idx.is_empty() {
            continue;
        }
        let affected = if single {
            // The point form: the per-key close-out, same SET lists.
            close_out_scalar(conn, &w[0].key, c, retained)?
        } else {
            let sql = if retained {
                SCALAR_CLOSE_SQL
            } else {
                SCALAR_CLOSE_STUB_SQL
            };
            let [e, p, a] = key_lists(idx.iter().map(|&i| &w[i].key));
            conn.prepare_cached(sql)?
                .execute(duckdb::params_from_iter([Value::BigInt(c), e, p, a]))?
        };
        // Hard check (plan D10): exactly one current row per scalar-live key
        // (`one_current_scalar` allows at most one).
        if affected != idx.len() {
            return Err(DbError::Internal(format!(
                "scalar batch: close-out touched {affected} rows for {} live keys",
                idx.len()
            )));
        }
        for i in idx {
            closed[i] += 1;
        }
    }

    // --- Insert the changed keys: every column built in one pass through
    // the one encoder. ---
    let mut versions: Vec<i16> = Vec::with_capacity(n);
    let mut cols = ScalarColumns::default();
    for (s, same) in w.iter().zip(&unchanged) {
        if let Some(v) = same {
            versions.push(*v as i16);
            continue;
        }
        let v = max_version.get(&triple(&s.key)).copied().unwrap_or(0) + 1;
        let v = i16::try_from(v)
            .map_err(|_| DbError::Internal(format!("scalar batch: version {v} out of range")))?;
        versions.push(v);
        cols.push(s, v);
    }
    cols.check_column_lengths()?;
    exec_columnar(conn, SCALAR_INSERT_SQL, cols.e.len(), |r| cols.params(c, r))?;

    Ok(versions
        .iter()
        .zip(closed)
        .zip(&unchanged)
        .map(|((&version, rows_closed), same)| match same {
            Some(v) => unchanged_outcome(Tier::Scalar, *v, (0, 0), 0),
            None => WriteOutcome {
                tier: Some(Tier::Scalar),
                version: version as u16,
                rows_written: 1,
                rows_closed,
                ..WriteOutcome::default()
            },
        })
        .collect())
}

/// The per-key columns of one scalar round, in [`SCALAR_INSERT_SQL`] order.
/// The value columns come from [`VCols::of`] (the one encoder); each column
/// holds one Rust type, so it binds as a list of one `Value` variant.
#[derive(Default)]
struct ScalarColumns {
    e: Vec<i64>,
    p: Vec<i64>,
    a: Vec<i64>,
    pt: Vec<Option<i64>>,
    version: Vec<i16>,
    kind: Vec<&'static str>,
    f: Vec<Option<f64>>,
    i: Vec<Option<i64>>,
    b: Vec<Option<bool>>,
    s: Vec<Option<String>>,
    dt: Vec<Option<i64>>,
    dm: Vec<Option<i32>>,
    du: Vec<Option<i64>>,
}

impl ScalarColumns {
    fn push(&mut self, w: &ScalarWrite, version: i16) {
        self.e.push(w.key.entity_id);
        self.p.push(w.key.parameter_definition_id);
        self.a.push(w.key.alternative_id);
        self.pt.push(w.parameter_type_id);
        self.version.push(version);
        let vc = VCols::of(&w.value);
        self.kind.push(vc.kind);
        self.f.push(vc.f);
        self.i.push(vc.i);
        self.b.push(vc.b);
        self.s.push(vc.s);
        self.dt.push(vc.dt);
        self.dm.push(vc.dm);
        self.du.push(vc.du);
    }

    /// Every column must hold one entry per key (a short all-NULL column
    /// would otherwise bind as a plain NULL and pass the list check).
    fn check_column_lengths(&self) -> Result<(), DbError> {
        let n = self.e.len();
        let lens = [
            self.p.len(),
            self.a.len(),
            self.pt.len(),
            self.version.len(),
            self.kind.len(),
            self.f.len(),
            self.i.len(),
            self.b.len(),
            self.s.len(),
            self.dt.len(),
            self.dm.len(),
            self.du.len(),
        ];
        if lens.iter().any(|&l| l != n) {
            return Err(DbError::Internal(format!(
                "scalar batch: column lengths {lens:?} differ from {n} keys"
            )));
        }
        Ok(())
    }

    fn params(&self, c: i64, r: std::ops::Range<usize>) -> Vec<Value> {
        vec![
            list(clip(&self.e, &r), |&v| Value::BigInt(v)),
            list(clip(&self.p, &r), |&v| Value::BigInt(v)),
            list(clip(&self.a, &r), |&v| Value::BigInt(v)),
            opt_list(clip(&self.pt, &r), |&v| Value::BigInt(v)),
            Value::BigInt(c),
            list(clip(&self.version, &r), |&v| Value::SmallInt(v)),
            list(clip(&self.kind, &r), |&v| Value::Text(v.to_string())),
            opt_list(clip(&self.f, &r), |&v| Value::Double(v)),
            opt_list(clip(&self.i, &r), |&v| Value::BigInt(v)),
            opt_list(clip(&self.b, &r), |&v| Value::Boolean(v)),
            opt_list(clip(&self.s, &r), |v| Value::Text(v.clone())),
            opt_list(clip(&self.dt, &r), |&v| Value::BigInt(v)),
            opt_list(clip(&self.dm, &r), |&v| Value::Int(v)),
            opt_list(clip(&self.du, &r), |&v| Value::BigInt(v)),
        ]
    }
}

/// The per-key close-out of a live scalar row: the scalar arm of
/// [`close_out_tier`] (deletes, and scalar → long/array transitions).
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

/// One key of a set-based array write (the array counterpart of
/// [`LongWrite`]; see there for the fields).
#[derive(Clone, Debug)]
pub(crate) struct ArrayWrite<'a> {
    pub(crate) key: ValueKey,
    pub(crate) value: Cow<'a, CanonicalValue>,
    pub(crate) axis_ids: Vec<i64>,
    pub(crate) axes_created: usize,
    pub(crate) axes_reused: usize,
    pub(crate) parameter_type_id: Option<i64>,
    pub(crate) retained: bool,
    pub(crate) entity_minted: bool,
}

impl ArrayWrite<'_> {
    fn vals(&self) -> &[f64] {
        match &self.value.shape {
            ShapedData::Array(vals) => vals,
            // canon guarantees the shape matches the tier.
            _ => unreachable!("array write of a non-array shape"),
        }
    }
}

/// Payload elements per set-based array round (plan D3): ~30 x 8 760, about
/// 2 MB of f64 plus the bind's transient `Value`s. A larger value is a round
/// of its own.
pub(crate) const ARRAY_ROUND_ELEMENTS: usize = 262_144;

/// Per key: the max version over its `value_array` and `value_array_hist`
/// rows (the set form of `next_version_array`).
const ARRAY_VERSION_SQL: &str = "SELECT v.entity_id, v.parameter_definition_id, v.alternative_id, \
            max(v.version)::BIGINT \
     FROM (SELECT entity_id, parameter_definition_id, alternative_id, version FROM value_array \
           UNION ALL \
           SELECT entity_id, parameter_definition_id, alternative_id, version \
           FROM value_array_hist) v \
     JOIN k ON v.entity_id = k.e AND v.parameter_definition_id = k.p \
          AND v.alternative_id = k.a \
     GROUP BY ALL";

/// The replace-in-place of a round's live array rows, keyed by their
/// `array_id` (which is never updated): the set form of `write_array`'s
/// UPDATE, the payload bound as one nested `?::DOUBLE[][]` (one `DOUBLE[]`
/// per key). Both payload columns are set, so a stale `values_int` cannot
/// break the XOR.
const ARRAY_UPDATE_SQL: &str =
    "UPDATE value_array SET valid_from_commit = ?, version = u.version, \
       parameter_type_id = u.pt, axis1_id = u.a1, axis2_id = u.a2, axis3_id = u.a3, \
       axis4_id = u.a4, values = u.vals, values_int = NULL, v_min = u.mn, v_max = u.mx \
     FROM (SELECT unnest(?::BIGINT[]) AS id, unnest(?::SMALLINT[]) AS version, \
             unnest(?::BIGINT[]) AS pt, unnest(?::BIGINT[]) AS a1, unnest(?::BIGINT[]) AS a2, \
             unnest(?::BIGINT[]) AS a3, unnest(?::BIGINT[]) AS a4, \
             unnest(?::DOUBLE[][]) AS vals, unnest(?::DOUBLE[]) AS mn, \
             unnest(?::DOUBLE[]) AS mx) u \
     WHERE value_array.array_id = u.id";

/// The insert of a round's new array rows with their pre-minted `array_id`s
/// (critique C3): the set form of `write_array`'s INSERT.
const ARRAY_INSERT_SQL: &str = "INSERT INTO value_array \
       (array_id, entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
        valid_from_commit, version, axis1_id, axis2_id, axis3_id, axis4_id, \
        values, values_int, v_min, v_max) \
     SELECT u.id, u.e, u.p, u.a, u.pt, ?, u.version, u.a1, u.a2, u.a3, u.a4, \
       u.vals, NULL, u.mn, u.mx \
     FROM (SELECT unnest(?::BIGINT[]) AS id, unnest(?::BIGINT[]) AS e, \
             unnest(?::BIGINT[]) AS p, unnest(?::BIGINT[]) AS a, unnest(?::BIGINT[]) AS pt, \
             unnest(?::SMALLINT[]) AS version, unnest(?::BIGINT[]) AS a1, \
             unnest(?::BIGINT[]) AS a2, unnest(?::BIGINT[]) AS a3, unnest(?::BIGINT[]) AS a4, \
             unnest(?::DOUBLE[][]) AS vals, unnest(?::DOUBLE[]) AS mn, \
             unnest(?::DOUBLE[]) AS mx) u";

/// `n` fresh `array_id`s from the column's own sequence (its only minter),
/// sorted: assigned to the new keys in input order they are exactly the ids
/// `n` sequential inserts would mint, and the sequence ends where they
/// would leave it. Hard check: `n` distinct ids.
const ARRAY_MINT_SQL: &str = "SELECT nextval('seq_value_array') FROM range(?)";

fn mint_array_ids(conn: &Connection, n: usize) -> Result<Vec<i64>, DbError> {
    let mut ids: Vec<i64> = conn
        .prepare_cached(ARRAY_MINT_SQL)?
        .query_map([n as i64], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != n {
        return Err(DbError::Internal(format!(
            "array batch: minted {} distinct array_ids for {n} new keys",
            ids.len()
        )));
    }
    Ok(ids)
}

/// Write every array value of `w` at `commit_id` in set-based rounds
/// (set_based_long_and_delete_PLAN §3.3): the one array history
/// implementation (`write_value` delegates here with N = 1). A round holds
/// ≤ [`LONG_INSERT_CHUNK`] keys and ≤ [`ARRAY_ROUND_ELEMENTS`] payload
/// elements, except a single larger value, which forms its own round (D3).
///
/// Per round: the scalar, long and array live probes under the per-key
/// precedence and the version probe over `value_array` ∪
/// `value_array_hist`; the per-key `close_out_tier` for each tier
/// transition (D6); for the keys whose PRECEDENCE tier is array (critique
/// C3: a damaged key live in long and array takes the INSERT and fails on
/// the UNIQUE triple, as before), the pre-image copy to `value_array_hist`
/// (one statement per retention mode, entirely in SQL) and one UPDATE in
/// place by `array_id`; for every other key one INSERT with `array_id`s
/// pre-minted from the sequence and assigned in input order. A round of one
/// key uses today's point statements (`live_tier`, `close_out_tier`, the
/// per-key array write).
///
/// Semantics are those of one per-key write per key in input order:
/// `version = max(value_array ∪ value_array_hist rows of the key) + 1`,
/// the same SET lists and retention, `array_id` minted by the sequence in
/// write order. Keys must be distinct (the callers guarantee it); a
/// repeated key, an affected-row count that differs from the probes or a
/// list of the wrong length fails as [`DbError::Internal`]. Returns one
/// [`WriteOutcome`] per input, in input order.
pub(crate) fn write_arrays(
    conn: &Connection,
    commit_id: i64,
    w: &[ArrayWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    check_distinct_keys("array batch", w.iter().map(|a| &a.key))?;
    let mut out = Vec::with_capacity(w.len());
    let (mut lo, mut elems) = (0usize, 0usize);
    for (i, a) in w.iter().enumerate() {
        debug_assert_value_floats_canonical(&a.value);
        let n = a.vals().len();
        if i > lo && (elems + n > ARRAY_ROUND_ELEMENTS || i - lo == LONG_INSERT_CHUNK) {
            out.extend(write_array_round(conn, commit_id, &w[lo..i])?);
            (lo, elems) = (i, 0);
        }
        elems += n;
    }
    if lo < w.len() {
        out.extend(write_array_round(conn, commit_id, &w[lo..])?);
    }
    Ok(out)
}

/// One round of [`write_arrays`] (see there for the bounds).
fn write_array_round(
    conn: &Connection,
    c: i64,
    w: &[ArrayWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    if let [a] = w {
        return Ok(vec![write_array_one(conn, c, a)?]);
    }
    let n = w.len();
    // --- Probes (only for keys whose entity existed before this commit). ---
    let probed: Vec<&ValueKey> = w
        .iter()
        .filter(|a| !a.entity_minted)
        .map(|a| &a.key)
        .collect();
    let mut meta: HashMap<i64, ArrayMeta> = HashMap::new();
    let (live, max_version) = if probed.is_empty() {
        (HashMap::new(), HashMap::new())
    } else {
        let live = probe_live_meta(conn, &probed, Some(&mut meta))?;
        let versions: HashMap<Triple, i64> =
            probe_keys(conn, ARRAY_VERSION_SQL, probed.iter().copied(), false)?
                .into_iter()
                .map(|(t, v, _)| (t, v))
                .collect();
        (live, versions)
    };

    // --- Unchanged keys: live in array, the narrow prefilter passes, and
    // the stored payload (fetched by array_id) equals the new one bit for
    // bit. They write nothing: no pre-image copy, no replace
    // (skip_unchanged_writes_PLAN §3.2). ---
    let mut cands: Vec<(i64, &[f64])> = Vec::new();
    for a in w {
        if let Some(&(Live::Array(id), _)) = live.get(&triple(&a.key)) {
            if meta.get(&id).is_some_and(|m| m.may_equal(a)) {
                cands.push((id, a.vals()));
            }
        }
    }
    let equal = array_payloads_equal(conn, &cands)?;
    let unchanged: Vec<Option<u16>> = w
        .iter()
        .map(|a| match live.get(&triple(&a.key)) {
            Some(&(Live::Array(id), _)) if equal.contains(&id) => meta.get(&id).map(|m| m.version),
            _ => None,
        })
        .collect();

    // --- Tier transitions: the per-key close-out of the other tier, in
    // input order (plan D6). ---
    let mut closed = vec![0usize; n];
    for (i, a) in w.iter().enumerate() {
        if let Some(&(cur @ (Live::Scalar | Live::Long), _)) = live.get(&triple(&a.key)) {
            closed[i] = close_out_tier(conn, &a.key, c, cur, a.retained)?;
        }
    }

    // --- Versions and the UPDATE / INSERT split by the precedence tier. ---
    let mut versions: Vec<i16> = Vec::with_capacity(n);
    for a in w {
        let v = max_version.get(&triple(&a.key)).copied().unwrap_or(0) + 1;
        versions
            .push(i16::try_from(v).map_err(|_| {
                DbError::Internal(format!("array batch: version {v} out of range"))
            })?);
    }
    let live_id = |i: usize| match live.get(&triple(&w[i].key)) {
        Some(&(Live::Array(id), _)) => Some(id),
        _ => None,
    };
    let replaced: Vec<usize> = (0..n)
        .filter(|&i| unchanged[i].is_none() && live_id(i).is_some())
        .collect();
    let fresh: Vec<usize> = (0..n).filter(|&i| live_id(i).is_none()).collect();

    // --- Replace in place: the pre-image copy (per retention mode), then
    // one UPDATE keyed by array_id. ---
    if !replaced.is_empty() {
        for retained in [true, false] {
            let ids: Vec<i64> = replaced
                .iter()
                .filter(|&&i| w[i].retained == retained)
                .filter_map(|&i| live_id(i))
                .collect();
            if ids.is_empty() {
                continue;
            }
            let sql = if retained {
                ARRAY_PREIMAGE_SET_SQL
            } else {
                ARRAY_PREIMAGE_STUB_SET_SQL
            };
            let copied = conn
                .prepare_cached(sql)?
                .execute(params![c, list(&ids, |&v| Value::BigInt(v))])?;
            check_affected("array pre-image copy", copied, ids.len())?;
        }
        let ids: Vec<i64> = replaced.iter().filter_map(|&i| live_id(i)).collect();
        let mut p = vec![Value::BigInt(c), list(&ids, |&v| Value::BigInt(v))];
        p.extend(array_cols(w, &replaced, &versions, false));
        check_list_lengths(&p, replaced.len())?;
        let updated = conn
            .prepare_cached(ARRAY_UPDATE_SQL)?
            .execute(duckdb::params_from_iter(p))?;
        check_affected("array update", updated, replaced.len())?;
        for &i in &replaced {
            closed[i] += 1;
        }
    }

    // --- Insert the new rows with pre-minted ids, in input order. ---
    if !fresh.is_empty() {
        let ids = mint_array_ids(conn, fresh.len())?;
        let mut p = vec![Value::BigInt(c), list(&ids, |&v| Value::BigInt(v))];
        p.extend(array_cols(w, &fresh, &versions, true));
        check_list_lengths(&p, fresh.len())?;
        let inserted = conn
            .prepare_cached(ARRAY_INSERT_SQL)?
            .execute(duckdb::params_from_iter(p))?;
        check_affected("array insert", inserted, fresh.len())?;
    }

    Ok(w.iter()
        .zip(versions)
        .zip(closed)
        .zip(&unchanged)
        .map(|(((a, version), rows_closed), same)| match same {
            Some(v) => unchanged_outcome(Tier::Array, *v, (a.axes_created, a.axes_reused), 0),
            None => WriteOutcome {
                tier: Some(Tier::Array),
                version: version as u16,
                rows_written: 1,
                rows_closed,
                axes_created: a.axes_created,
                axes_reused: a.axes_reused,
                ..WriteOutcome::default()
            },
        })
        .collect())
}

/// The per-key bind lists of the keys `idx` of an array round, in the
/// column order of [`ARRAY_UPDATE_SQL`] after `id` (or, with `key`, of
/// [`ARRAY_INSERT_SQL`] after `id`: the key triple first). The payload is
/// the canonical floats verbatim as one nested list; `v_min` / `v_max` come
/// from [`min_max`].
fn array_cols(w: &[ArrayWrite], idx: &[usize], versions: &[i16], key: bool) -> Vec<Value> {
    let col = |f: &dyn Fn(&ArrayWrite) -> Option<i64>| -> Value {
        let v: Vec<Option<i64>> = idx.iter().map(|&i| f(&w[i])).collect();
        opt_list(&v, |&x| Value::BigInt(x))
    };
    let mut p: Vec<Value> = Vec::with_capacity(12);
    if key {
        p.push(col(&|a| Some(a.key.entity_id)));
        p.push(col(&|a| Some(a.key.parameter_definition_id)));
        p.push(col(&|a| Some(a.key.alternative_id)));
        p.push(col(&|a| a.parameter_type_id));
        p.push(list(idx, |&i| Value::SmallInt(versions[i])));
    } else {
        p.push(list(idx, |&i| Value::SmallInt(versions[i])));
        p.push(col(&|a| a.parameter_type_id));
    }
    for d in 0..4 {
        p.push(col(&|a| a.axis_ids.get(d).copied()));
    }
    p.push(list(idx, |&i| {
        Value::List(w[i].vals().iter().map(|&x| Value::Double(x)).collect())
    }));
    let mm: Vec<(f64, f64)> = idx.iter().map(|&i| min_max(w[i].vals())).collect();
    p.push(list(&mm, |&(lo, _)| Value::Double(lo)));
    p.push(list(&mm, |&(_, hi)| Value::Double(hi)));
    p
}

/// The single-key form of an array round: today's point statements.
fn write_array_one(conn: &Connection, c: i64, a: &ArrayWrite) -> Result<WriteOutcome, DbError> {
    let current = if a.entity_minted {
        None
    } else {
        live_tier_array_meta(conn, &a.key)?
    };
    // The unchanged check (as in the set form): the prefilter on the
    // narrow columns, then the payload by array_id.
    if let Some((Live::Array(id), Some(m))) = current {
        if m.may_equal(a) && array_payloads_equal(conn, &[(id, a.vals())])?.contains(&id) {
            return Ok(unchanged_outcome(
                Tier::Array,
                m.version,
                (a.axes_created, a.axes_reused),
                0,
            ));
        }
    }
    let current = current.map(|(l, _)| l);
    let mut rows_closed = 0usize;
    if let Some(cur @ (Live::Scalar | Live::Long)) = current {
        rows_closed += close_out_tier(conn, &a.key, c, cur, a.retained)?;
    }
    let (version, rows_written, closed_same) = write_array(
        conn,
        &a.key,
        c,
        &a.axis_ids,
        a.vals(),
        current.and_then(Live::array_id),
        a.retained,
        a.parameter_type_id,
    )?;
    Ok(WriteOutcome {
        tier: Some(Tier::Array),
        version,
        rows_written,
        rows_closed: rows_closed + closed_same,
        axes_created: a.axes_created,
        axes_reused: a.axes_reused,
        ..WriteOutcome::default()
    })
}

// ---------------------------------------------------------------------------
// Long tier
// ---------------------------------------------------------------------------

/// One key of a set-based long write: the canonical value (borrowed from a
/// direct `write_value` caller, owned in the API's commit buffer), its axis
/// ids resolved at buffer time (in value order, so axis ids are minted as
/// before) with the created / reused counts of that resolution, the D11
/// member binding, whether the superseded cells keep their content, and
/// whether the key's entity was minted by this commit (no value row of any
/// tier can exist: the probes are skipped and every cell is version 1).
#[derive(Clone, Debug)]
pub(crate) struct LongWrite<'a> {
    pub(crate) key: ValueKey,
    pub(crate) value: Cow<'a, CanonicalValue>,
    pub(crate) axis_ids: Vec<i64>,
    pub(crate) axes_created: usize,
    pub(crate) axes_reused: usize,
    pub(crate) parameter_type_id: Option<i64>,
    pub(crate) retained: bool,
    pub(crate) entity_minted: bool,
}

impl LongWrite<'_> {
    fn rows(&self) -> &[crate::types::LongRow] {
        match &self.value.shape {
            ShapedData::Long(rows) => rows,
            // canon guarantees the shape matches the tier.
            _ => unreachable!("long write of a non-long shape"),
        }
    }
}

/// The axis ids of `axes`, in order, through the content-verified dedup
/// (threading the caller's cache), with the created and reused counts
/// (plan D8: one `axis` row per insert, so the counts equal the former
/// `axis` row-count delta).
pub(crate) fn resolve_axes(
    conn: &Connection,
    cache: &mut AxisCache,
    axes: &[CanonicalAxis],
) -> Result<(Vec<i64>, usize, usize), DbError> {
    let mut ids = Vec::with_capacity(axes.len());
    let mut created = 0usize;
    for ax in axes {
        let (id, new) = dedup_or_insert_created(conn, cache, ax)?;
        ids.push(id);
        created += usize::from(new);
    }
    Ok((ids, created, axes.len() - created))
}

/// Write every long value of `w` at `commit_id` in set-based rounds
/// (set_based_long_and_delete_PLAN §3.2): the one long history
/// implementation (`write_value` delegates here with N = 1). A round holds
/// ≤ [`LONG_INSERT_CHUNK`] keys and ≤ [`LONG_INSERT_CHUNK`] cells, except a
/// single larger value, which forms its own round (plan D3).
///
/// Per round: the scalar live probe; one grouped long probe returning, per
/// position of each key, the max version over all its rows and its current
/// cell (D5, skip_unchanged_writes_PLAN D7), streamed into a compact sorted
/// vector; the array live probe for the keys live in neither; the cell diff
/// per key ([`LongDiff`]); the per-key `close_out_tier` for each tier
/// transition (D6); per retention mode one key-level close-out UPDATE for
/// the keys rewritten whole and one position-joined UPDATE (chunked at
/// [`LONG_INSERT_CHUNK`] cells) for the diffed keys' changed and removed
/// cells; and one key-ordinal columnar INSERT of the changed and added
/// cells (D7 as amended by critique C1-C2), chunked at
/// [`LONG_INSERT_CHUNK`] cells. A round of one key binds its probes and
/// whole close-out as today's point statements (D4); the insert and the
/// cell close-out have one form for every N.
///
/// Semantics are those of one per-key write per key in input order: the
/// live tier by the per-key precedence (scalar, then long, then array), the
/// per-cell `version = max(all rows of the (key, positions)) + 1`, the cell
/// diff (an equal cell keeps its row; a cell absent from the new value
/// closes and stays dead), the same SET lists and retention. Keys must be distinct (the callers
/// guarantee it); a repeated key, a close-out count that differs from the
/// probe, or a list of the wrong length fails as [`DbError::Internal`].
/// Returns one [`WriteOutcome`] per input, in input order.
pub(crate) fn write_longs(
    conn: &Connection,
    commit_id: i64,
    w: &[LongWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    check_distinct_keys("long batch", w.iter().map(|l| &l.key))?;
    let mut out = Vec::with_capacity(w.len());
    let (mut lo, mut cells) = (0usize, 0usize);
    for (i, l) in w.iter().enumerate() {
        debug_assert_value_floats_canonical(&l.value);
        let n = l.rows().len();
        if i > lo && (cells + n > LONG_INSERT_CHUNK || i - lo == LONG_INSERT_CHUNK) {
            out.extend(write_long_round(conn, commit_id, &w[lo..i])?);
            (lo, cells) = (i, 0);
        }
        cells += n;
    }
    if lo < w.len() {
        out.extend(write_long_round(conn, commit_id, &w[lo..])?);
    }
    Ok(out)
}

/// Per key, in two legs: every current cell (its positions, version, the
/// flag 1, its axis ids and comparison columns, [`long_cur_cols!`]) and per
/// position tuple the max version over the closed rows (flag 0). Folded
/// per position ([`LongCells::finish`]) they give the max version over
/// every `value_long` row of the (key, positions), as `current_long_cells`
/// reads it, plus the current cell: the version source of the write and
/// the read of the cell diff in one statement (critique C6, D7). The
/// current leg reads plain columns, not one aggregate per column, which
/// measured ~2x the planning and execution cost of today's probe. Read
/// through a streamed Arrow result ([`read_long_probe`]), never
/// materialized: a 4.6 M-cell value would otherwise hold ~0.7 GB in
/// DuckDB's result buffer (critique C5).
const LONG_PROBE_SQL: &str = concat!(
    "SELECT l.entity_id, l.parameter_definition_id, l.alternative_id, \
            l.i1_pos, l.i2_pos, l.i3_pos, l.i4_pos, l.version::INTEGER, 1::BIGINT, ",
    long_cur_cols!(),
    " FROM value_long l JOIN k ON l.entity_id = k.e \
          AND l.parameter_definition_id = k.p AND l.alternative_id = k.a \
     WHERE l.valid_to_commit IS NULL \
     UNION ALL \
     SELECT l.entity_id, l.parameter_definition_id, l.alternative_id, \
            l.i1_pos, l.i2_pos, l.i3_pos, l.i4_pos, max(l.version)::INTEGER, 0::BIGINT, ",
    long_cur_nulls!(),
    " FROM value_long l JOIN k ON l.entity_id = k.e \
          AND l.parameter_definition_id = k.p AND l.alternative_id = k.a \
     WHERE l.valid_to_commit IS NOT NULL \
     GROUP BY ALL"
);

/// The single-key form of [`LONG_PROBE_SQL`] (the key binds twice).
const LONG_PROBE_ONE_SQL: &str = concat!(
    "SELECT l.i1_pos, l.i2_pos, l.i3_pos, l.i4_pos, l.version::INTEGER, 1::BIGINT, ",
    long_cur_cols!(),
    " FROM value_long l \
     WHERE l.entity_id = ? AND l.parameter_definition_id = ? AND l.alternative_id = ? \
       AND l.valid_to_commit IS NULL \
     UNION ALL \
     SELECT l.i1_pos, l.i2_pos, l.i3_pos, l.i4_pos, max(l.version)::INTEGER, 0::BIGINT, ",
    long_cur_nulls!(),
    " FROM value_long l \
     WHERE l.entity_id = ? AND l.parameter_definition_id = ? AND l.alternative_id = ? \
       AND l.valid_to_commit IS NOT NULL \
     GROUP BY l.i1_pos, l.i2_pos, l.i3_pos, l.i4_pos"
);

/// [`ProbeCell::cur`]: no current row at the position.
const CUR_NONE: u8 = 0;
/// [`ProbeCell::cur`]: exactly one current row, decoded and comparable.
const CUR_CMP: u8 = 1;
/// [`ProbeCell::cur`]: a current row that is not comparable (malformed, a
/// stub, or one of two current rows at the position).
const CUR_BAD: u8 = 2;

/// One position tuple of a key as the long probe read it, in 32 bytes
/// (critique C5: no `String` or heap cell per row): the max version over
/// all its rows and its current cell.
#[derive(Clone, Copy, Debug)]
struct ProbeCell {
    pos: [i32; 4],
    /// The current cell's payload ([`LongCells::pack`]), when `cur` is
    /// [`CUR_CMP`].
    payload: u64,
    version: u16,
    cur: u8,
}

/// What every current cell of a coherent key shares: its axis ids (padded
/// to 4), its binding and its value kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CellMeta {
    axes: [Option<i64>; 4],
    pt: Option<i64>,
    kind: ValueKind,
}

/// A key's probed long cells, sorted by position tuple once the probe is
/// read ([`LongCells::finish`]).
#[derive(Debug, Default)]
struct LongCells {
    cells: Vec<ProbeCell>,
    /// The payloads that do not fit a `u64` (strings, durations), indexed
    /// by [`ProbeCell::payload`].
    wide: Vec<StoredCell>,
    /// The meta of the first current cell read.
    meta: Option<CellMeta>,
    /// A current cell disagreed with `meta` (axis ids, binding or kind;
    /// critique C3.3: compared per cell) or was not comparable. Such a key
    /// is never diffed or judged unchanged: it is rewritten whole.
    incoherent: bool,
    /// Current rows over all positions.
    current: usize,
}

/// The kind of a decoded stored cell.
fn cell_kind(c: &StoredCell) -> ValueKind {
    match c {
        StoredCell::Float(_) => ValueKind::Float,
        StoredCell::Int(_) => ValueKind::Int,
        StoredCell::Bool(_) => ValueKind::Bool,
        StoredCell::Str(_) => ValueKind::String,
        StoredCell::DateTime(_) => ValueKind::DateTime,
        StoredCell::Duration(..) => ValueKind::Duration,
    }
}

impl LongCells {
    /// Pack a decoded cell into a `u64`: the bits of a fixed-width
    /// payload, or an index into `wide`.
    fn pack(&mut self, c: StoredCell) -> u64 {
        match c {
            StoredCell::Float(b) => b,
            StoredCell::Int(i) | StoredCell::DateTime(i) => i as u64,
            StoredCell::Bool(b) => u64::from(b),
            StoredCell::Str(_) | StoredCell::Duration(..) => {
                self.wide.push(c);
                (self.wide.len() - 1) as u64
            }
        }
    }

    /// Does the (comparable) current cell `c` of this coherent key hold
    /// exactly `new`, the decoded encoder output of a new cell? The kind
    /// is the key's shared kind, so it is compared once, then the payload
    /// bits (or the wide payload) per cell.
    fn holds(&self, c: &ProbeCell, new: &StoredCell) -> bool {
        let kind = match self.meta {
            Some(m) => m.kind,
            None => return false,
        };
        c.cur == CUR_CMP
            && kind == cell_kind(new)
            && match new {
                StoredCell::Float(b) => c.payload == *b,
                StoredCell::Int(i) | StoredCell::DateTime(i) => c.payload == *i as u64,
                StoredCell::Bool(b) => c.payload == u64::from(*b),
                StoredCell::Str(_) | StoredCell::Duration(..) => {
                    self.wide.get(c.payload as usize) == Some(new)
                }
            }
    }

    /// Fold one probe row: a closed-leg row (`current == None`: the
    /// position's max closed version) or a current cell with its meta and
    /// decoded payload (`None` when not comparable). A position may come
    /// in twice, once per leg; [`Self::finish`] merges.
    fn push(
        &mut self,
        pos: [i32; 4],
        version: u16,
        current: Option<(CellMeta, Option<StoredCell>)>,
    ) {
        let (cur, payload) = match current {
            None => (CUR_NONE, 0),
            Some((meta, cell)) => {
                self.current += 1;
                match self.meta {
                    None => self.meta = Some(meta),
                    Some(m) if m == meta => {}
                    Some(_) => self.incoherent = true,
                }
                match cell {
                    Some(c) => (CUR_CMP, self.pack(c)),
                    None => {
                        self.incoherent = true;
                        (CUR_BAD, 0)
                    }
                }
            }
        };
        self.cells.push(ProbeCell {
            pos,
            payload,
            version,
            cur,
        });
    }

    /// Sort by position tuple (in place) and merge the rows of one
    /// position in place: the max version of both legs, and its current
    /// cell (none, or not comparable when a damaged position has more than
    /// one, which also makes the key incoherent).
    fn finish(&mut self) {
        self.cells.sort_unstable_by_key(|c| c.pos);
        let mut w = 0usize;
        for r in 0..self.cells.len() {
            let c = self.cells[r];
            if w > 0 && self.cells[w - 1].pos == c.pos {
                let m = &mut self.cells[w - 1];
                m.version = m.version.max(c.version);
                if c.cur != CUR_NONE {
                    if m.cur == CUR_NONE {
                        m.cur = c.cur;
                        m.payload = c.payload;
                    } else {
                        m.cur = CUR_BAD;
                        self.incoherent = true;
                    }
                }
            } else {
                self.cells[w] = c;
                w += 1;
            }
        }
        self.cells.truncate(w);
        self.cells.shrink_to_fit();
    }
}

/// The Arrow columns of one long probe batch, typed once per batch.
struct ProbeCols<'a> {
    key: Option<[&'a Int64Array; 3]>,
    pos: [&'a Int32Array; 4],
    version: &'a Int32Array,
    flag: &'a Int64Array,
    axes: [&'a Int64Array; 4],
    kind: StrCol<'a>,
    f: &'a Float64Array,
    i: &'a Int64Array,
    b: &'a BooleanArray,
    s: StrCol<'a>,
    dt: TsCol<'a>,
    dm: &'a Int32Array,
    du: &'a Int64Array,
    pt: &'a Int64Array,
    retained: &'a BooleanArray,
}

/// A string column of either Arrow layout DuckDB may export.
enum StrCol<'a> {
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
    View(&'a StringViewArray),
}

impl<'a> StrCol<'a> {
    fn of(a: &'a ArrayRef) -> Result<StrCol<'a>, DbError> {
        if let Some(s) = a.as_string_opt::<i32>() {
            Ok(StrCol::Utf8(s))
        } else if let Some(s) = a.as_string_opt::<i64>() {
            Ok(StrCol::Large(s))
        } else if let Some(s) = a.as_string_view_opt() {
            Ok(StrCol::View(s))
        } else {
            Err(probe_type_error(a))
        }
    }

    fn get(&self, i: usize) -> Option<&'a str> {
        match self {
            StrCol::Utf8(s) => (!s.is_null(i)).then(|| s.value(i)),
            StrCol::Large(s) => (!s.is_null(i)).then(|| s.value(i)),
            StrCol::View(s) => (!s.is_null(i)).then(|| s.value(i)),
        }
    }
}

/// A timestamp column of any unit, read as exact epoch µs.
enum TsCol<'a> {
    S(&'a PrimitiveArray<TimestampSecondType>),
    Ms(&'a PrimitiveArray<TimestampMillisecondType>),
    Us(&'a PrimitiveArray<TimestampMicrosecondType>),
    Ns(&'a PrimitiveArray<TimestampNanosecondType>),
}

impl<'a> TsCol<'a> {
    fn of(a: &'a ArrayRef) -> Result<TsCol<'a>, DbError> {
        use duckdb::arrow::datatypes::{DataType, TimeUnit as AU};
        Ok(match a.data_type() {
            DataType::Timestamp(AU::Second, _) => TsCol::S(a.as_primitive()),
            DataType::Timestamp(AU::Millisecond, _) => TsCol::Ms(a.as_primitive()),
            DataType::Timestamp(AU::Microsecond, _) => TsCol::Us(a.as_primitive()),
            DataType::Timestamp(AU::Nanosecond, _) => TsCol::Ns(a.as_primitive()),
            _ => return Err(probe_type_error(a)),
        })
    }

    /// `Ok(None)`: NULL; `Err(())`: not exact in µs (critique C3.5).
    fn get(&self, i: usize) -> Result<Option<i64>, ()> {
        let (unit, v) = match self {
            TsCol::S(p) => (TimeUnit::Second, opt(*p, i)),
            TsCol::Ms(p) => (TimeUnit::Millisecond, opt(*p, i)),
            TsCol::Us(p) => (TimeUnit::Microsecond, opt(*p, i)),
            TsCol::Ns(p) => (TimeUnit::Nanosecond, opt(*p, i)),
        };
        match v {
            None => Ok(None),
            Some(v) => timestamp_us(unit, v).map(Some).ok_or(()),
        }
    }
}

fn probe_type_error(a: &ArrayRef) -> DbError {
    DbError::Internal(format!(
        "long probe: unexpected Arrow type {:?}",
        a.data_type()
    ))
}

/// An optional primitive value.
fn opt<T: duckdb::arrow::datatypes::ArrowPrimitiveType>(
    a: &duckdb::arrow::array::PrimitiveArray<T>,
    i: usize,
) -> Option<T::Native> {
    (!a.is_null(i)).then(|| a.value(i))
}

impl<'a> ProbeCols<'a> {
    /// Type the columns of `b`: the key triple first when `keyed` (the set
    /// form), then the four positions, version, current flag, the four
    /// axis ids and the [`cur_value_cols!`] block.
    fn of(b: &'a RecordBatch, keyed: bool) -> Result<ProbeCols<'a>, DbError> {
        use duckdb::arrow::datatypes::{Float64Type, Int32Type, Int64Type};
        let c = |i: usize| b.column(i);
        let i64c = |i: usize| {
            c(i).as_primitive_opt::<Int64Type>()
                .ok_or_else(|| probe_type_error(c(i)))
        };
        let i32c = |i: usize| {
            c(i).as_primitive_opt::<Int32Type>()
                .ok_or_else(|| probe_type_error(c(i)))
        };
        let boolc = |i: usize| c(i).as_boolean_opt().ok_or_else(|| probe_type_error(c(i)));
        let o = if keyed { 3 } else { 0 };
        if b.num_columns() != o + 20 {
            return Err(DbError::Internal(format!(
                "long probe: {} columns, expected {}",
                b.num_columns(),
                o + 20
            )));
        }
        Ok(ProbeCols {
            key: if keyed {
                Some([i64c(0)?, i64c(1)?, i64c(2)?])
            } else {
                None
            },
            pos: [i32c(o)?, i32c(o + 1)?, i32c(o + 2)?, i32c(o + 3)?],
            version: i32c(o + 4)?,
            flag: i64c(o + 5)?,
            axes: [i64c(o + 6)?, i64c(o + 7)?, i64c(o + 8)?, i64c(o + 9)?],
            kind: StrCol::of(c(o + 10))?,
            f: c(o + 11)
                .as_primitive_opt::<Float64Type>()
                .ok_or_else(|| probe_type_error(c(o + 11)))?,
            i: i64c(o + 12)?,
            b: boolc(o + 13)?,
            s: StrCol::of(c(o + 14))?,
            dt: TsCol::of(c(o + 15))?,
            dm: i32c(o + 16)?,
            du: i64c(o + 17)?,
            pt: i64c(o + 18)?,
            retained: boolc(o + 19)?,
        })
    }

    fn triple(&self, r: usize) -> Option<Triple> {
        self.key
            .map(|[e, p, a]| (e.value(r), p.value(r), a.value(r)))
    }

    /// Row `r` folded into `cells` (see [`LongCells::push`]): a current
    /// cell's meta, and its payload decoded from the RAW stored columns by
    /// the one decoder ([`StoredCell::decode`]; `None` for a malformed row
    /// or a stub, critique C3.4).
    fn push(&self, r: usize, cells: &mut LongCells) {
        let pos = [
            self.pos[0].value(r),
            self.pos[1].value(r),
            self.pos[2].value(r),
            self.pos[3].value(r),
        ];
        let version = self.version.value(r) as u16;
        if self.flag.value(r) == 0 {
            cells.push(pos, version, None);
            return;
        }
        let axes = [
            opt(self.axes[0], r),
            opt(self.axes[1], r),
            opt(self.axes[2], r),
            opt(self.axes[3], r),
        ];
        let pt = opt(self.pt, r);
        let cell = self.cell(r);
        let kind = cell.as_ref().map(cell_kind);
        // A cell that does not decode has no kind; any kind marks the meta,
        // the cell itself makes the key incoherent.
        let meta = CellMeta {
            axes,
            pt,
            kind: kind.unwrap_or(ValueKind::Float),
        };
        cells.push(pos, version, Some((meta, cell)));
    }

    fn cell(&self, r: usize) -> Option<StoredCell> {
        if self.retained.is_null(r) || !self.retained.value(r) {
            return None;
        }
        let kind = self.kind.get(r)?;
        let dt = self.dt.get(r).ok()?;
        StoredCell::decode(&RawCols {
            kind: kind.as_bytes(),
            f: opt(self.f, r),
            i: opt(self.i, r),
            b: (!self.b.is_null(r)).then(|| self.b.value(r)),
            s: self.s.get(r),
            dt,
            dm: opt(self.dm, r),
            du: opt(self.du, r),
        })
    }
}

/// Run a long probe statement (`sql` with `params`) as a streamed Arrow
/// result, handing every row to `each`. The batches are fetched with
/// [`duckdb::Statement::step`], which reports a failed fetch as an error
/// (the Arrow iterator would panic).
fn read_long_probe(
    conn: &Connection,
    sql: &str,
    params: impl duckdb::Params,
    keyed: bool,
    mut each: impl FnMut(&ProbeCols, usize),
) -> Result<(), DbError> {
    let mut stmt = conn.prepare_cached(sql)?;
    drop(stmt.stream_arrow(params)?);
    while let Some(sa) = stmt.step()? {
        let batch = RecordBatch::from(&sa);
        let cols = ProbeCols::of(&batch, keyed)?;
        for r in 0..batch.num_rows() {
            each(&cols, r);
        }
    }
    Ok(())
}

/// What one long write does to its key's cells (skip_unchanged_writes_PLAN
/// §4.1): the merge of the new value's cells with the probed ones, by
/// position tuple.
#[derive(Debug, Default)]
struct LongDiff {
    /// Current cells to close out (changed or removed), in position order.
    close: Vec<[i32; 4]>,
    /// New cells to insert (changed or added): the row index into the
    /// value's rows and the cell's version (its position's max + 1, or 1),
    /// in position order.
    insert: Vec<(u32, u16)>,
    /// Current cells left as they are (equal to the new cell).
    kept: usize,
    /// Their positions while there are at most [`LONG_INSERT_CHUNK`]
    /// (`None` beyond): the bound list of the "close all but these" form
    /// of a one-key close-out ([`LONG_CLOSE_EXCEPT_SQL`]).
    keep: Option<Vec<[i32; 4]>>,
    /// The highest version among the kept cells.
    kept_version: u16,
    /// Every current cell is closed: the close-out is the key-level
    /// statement (the whole-value path, or a diff that changed every cell;
    /// the same rows either way).
    whole: bool,
}

impl LongDiff {
    /// Merge `l`'s cells with the key's probed `cells` (both sorted by
    /// position tuple). With `whole`, no current cell is kept: every one
    /// closes, every new cell is inserted (the whole-value rewrite, with
    /// the same per-position versions). Otherwise a current cell equal to
    /// the new one at its position (the key's shared kind, the payload
    /// bits) is kept, a different one is closed and re-inserted at its
    /// version + 1, a current cell with no new cell is closed (removed),
    /// and a new cell with no current one is inserted at its position's
    /// max version + 1 (added; 1 if the position has no rows).
    fn of(cells: &LongCells, l: &LongWrite, whole: bool) -> LongDiff {
        let rows = l.rows();
        // Canon emits rows sorted by position tuple; a hand-built value
        // need not be, so merge through a sorted index then.
        let order: Option<Vec<u32>> = if rows
            .windows(2)
            .all(|w| pad4_pos(&w[0].positions) <= pad4_pos(&w[1].positions))
        {
            None
        } else {
            let mut o: Vec<u32> = (0..rows.len() as u32).collect();
            o.sort_by_key(|&r| pad4_pos(&rows[r as usize].positions));
            Some(o)
        };
        let row_at = |j: usize| order.as_ref().map_or(j as u32, |o| o[j]);
        let mut d = LongDiff {
            whole,
            keep: Some(Vec::new()),
            ..LongDiff::default()
        };
        let cs = &cells.cells;
        let (mut i, mut j) = (0usize, 0usize);
        while i < cs.len() || j < rows.len() {
            let r = (j < rows.len()).then(|| row_at(j));
            let np = r.map(|r| pad4_pos(&rows[r as usize].positions));
            match (cs.get(i), np) {
                (Some(c), Some(p)) if c.pos == p => {
                    let r = r.unwrap_or_default();
                    let same = !whole
                        && StoredCell::of_new(&rows[r as usize].value)
                            .is_some_and(|new| cells.holds(c, &new));
                    if same {
                        d.kept += 1;
                        d.kept_version = d.kept_version.max(c.version);
                        if d.kept > LONG_INSERT_CHUNK {
                            d.keep = None;
                        } else if let Some(k) = &mut d.keep {
                            k.push(c.pos);
                        }
                    } else {
                        if c.cur != CUR_NONE {
                            d.close.push(c.pos);
                        }
                        d.insert.push((r, c.version.saturating_add(1)));
                    }
                    i += 1;
                    j += 1;
                }
                (Some(c), p) if p.is_none_or(|p| c.pos < p) => {
                    if c.cur != CUR_NONE {
                        d.close.push(c.pos);
                    }
                    i += 1;
                }
                _ => {
                    d.insert.push((r.unwrap_or_default(), 1));
                    j += 1;
                }
            }
        }
        d
    }

    /// The version a write of this diff reports: the highest version among
    /// the key's current cells after it.
    fn version(&self) -> u16 {
        self.insert
            .iter()
            .map(|&(_, v)| v)
            .max()
            .unwrap_or(0)
            .max(self.kept_version)
    }
}

/// The per-key state the probes of a long round collect.
#[derive(Default)]
struct LongProbe {
    /// Per key, its probed cells (all positions it has rows at).
    cells: HashMap<Triple, LongCells>,
    /// The live tier per key (per-key precedence), with its current-row
    /// count (the current cells for long).
    live: HashMap<Triple, (Live, usize)>,
}

impl LongProbe {
    /// The single-key form: today's point statements (`live_tier`, then
    /// `current_long_cells`).
    fn point(conn: &Connection, k: &ValueKey) -> Result<LongProbe, DbError> {
        let mut p = LongProbe::default();
        let live = live_tier(conn, k)?;
        let cells = current_long_cells(conn, k)?;
        if let Some(l) = live {
            let n = if l == Live::Long { cells.current } else { 0 };
            p.live.insert(triple(k), (l, n));
        }
        p.cells.insert(triple(k), cells);
        Ok(p)
    }

    /// The set form over `keys` (none minted): the scalar live probe, the
    /// grouped long probe (versions and current cells) and the array live
    /// probe for the keys live in neither.
    fn set(conn: &Connection, keys: &[&ValueKey]) -> Result<LongProbe, DbError> {
        let mut p = LongProbe::default();
        for (t, _, _) in probe_keys(conn, SCALAR_LIVE_SQL, keys.iter().copied(), false)? {
            p.live.insert(t, (Live::Scalar, 1));
        }
        {
            let cells = &mut p.cells;
            read_long_probe(
                conn,
                &format!("{SCALAR_KEYS_CTE}{LONG_PROBE_SQL}"),
                duckdb::params_from_iter(key_lists(keys.iter().copied())),
                true,
                |cols, r| {
                    if let Some(t) = cols.triple(r) {
                        cols.push(r, cells.entry(t).or_default());
                    }
                },
            )?;
        }
        for (t, cells) in &mut p.cells {
            cells.finish();
            if cells.current > 0 {
                p.live.entry(*t).or_insert((Live::Long, cells.current));
            }
        }
        let rest: Vec<&ValueKey> = keys
            .iter()
            .copied()
            .filter(|k| !p.live.contains_key(&triple(k)))
            .collect();
        if !rest.is_empty() {
            for (t, array_id, _) in probe_keys(conn, ARRAY_LIVE_SQL, rest.iter().copied(), false)? {
                p.live.insert(t, (Live::Array(array_id), 1));
            }
        }
        Ok(p)
    }
}

/// The probed cells of one key: per position tuple the key already holds
/// (live OR superseded — versioning spans both, as the former per-cell
/// probe did: no `valid_to_commit` filter) its `MAX(version)` and its
/// current cell. The single-key form of [`LONG_PROBE_SQL`].
fn current_long_cells(conn: &Connection, key: &ValueKey) -> Result<LongCells, DbError> {
    let [e, p, a] = point(key);
    let mut cells = LongCells::default();
    read_long_probe(
        conn,
        LONG_PROBE_ONE_SQL,
        params![e, p, a, e, p, a],
        false,
        |cols, r| cols.push(r, &mut cells),
    )?;
    cells.finish();
    Ok(cells)
}

/// The close-out of a long cell diff: the current rows at the bound
/// (key, position tuple) rows, joined on the seven columns (one bound list
/// per column, ≤ [`LONG_INSERT_CHUNK`] rows per execution), content
/// retained.
const LONG_CLOSE_CELLS_SQL: &str = "UPDATE value_long SET valid_to_commit = ? \
     FROM (SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                  unnest(?::BIGINT[]) AS a, unnest(?::INTEGER[]) AS q1, \
                  unnest(?::INTEGER[]) AS q2, unnest(?::INTEGER[]) AS q3, \
                  unnest(?::INTEGER[]) AS q4) k \
     WHERE value_long.entity_id = k.e AND value_long.parameter_definition_id = k.p \
       AND value_long.alternative_id = k.a AND value_long.valid_to_commit IS NULL \
       AND value_long.i1_pos = k.q1 AND value_long.i2_pos = k.q2 \
       AND value_long.i3_pos = k.q3 AND value_long.i4_pos = k.q4";

/// The stub form of [`LONG_CLOSE_CELLS_SQL`] (the SET list of the stub
/// `close_out_long`).
const LONG_CLOSE_CELLS_STUB_SQL: &str = "UPDATE value_long SET valid_to_commit = ?, \
       content_retained = FALSE, v_float = NULL, v_int = NULL, v_bool = NULL, v_str = NULL, \
       v_datetime = NULL, v_dur_months = NULL, v_dur_micros = NULL \
     FROM (SELECT unnest(?::BIGINT[]) AS e, unnest(?::BIGINT[]) AS p, \
                  unnest(?::BIGINT[]) AS a, unnest(?::INTEGER[]) AS q1, \
                  unnest(?::INTEGER[]) AS q2, unnest(?::INTEGER[]) AS q3, \
                  unnest(?::INTEGER[]) AS q4) k \
     WHERE value_long.entity_id = k.e AND value_long.parameter_definition_id = k.p \
       AND value_long.alternative_id = k.a AND value_long.valid_to_commit IS NULL \
       AND value_long.i1_pos = k.q1 AND value_long.i2_pos = k.q2 \
       AND value_long.i3_pos = k.q3 AND value_long.i4_pos = k.q4";

/// The one-key close-out of a cell diff that closes most of the value's
/// cells: every current cell of the key but the bound kept positions (an
/// anti-join on ≤ [`LONG_INSERT_CHUNK`] positions), content retained. The
/// same rows as [`LONG_CLOSE_CELLS_SQL`] over the closed positions, at the
/// key-level UPDATE's per-row cost (critique C5: the position-joined form
/// costs ~7 µs per closed cell against ~5 µs for the key-level one, so a
/// re-import that changes all but a few cells of a large value would
/// otherwise be slower than the whole rewrite it replaces).
const LONG_CLOSE_EXCEPT_SQL: &str = "UPDATE value_long SET valid_to_commit = ? \
     WHERE entity_id = ? AND parameter_definition_id = ? AND alternative_id = ? \
       AND valid_to_commit IS NULL \
       AND NOT EXISTS (SELECT 1 FROM (SELECT unnest(?::INTEGER[]) AS q1, \
             unnest(?::INTEGER[]) AS q2, unnest(?::INTEGER[]) AS q3, \
             unnest(?::INTEGER[]) AS q4) k \
           WHERE value_long.i1_pos = k.q1 AND value_long.i2_pos = k.q2 \
             AND value_long.i3_pos = k.q3 AND value_long.i4_pos = k.q4)";

/// The stub form of [`LONG_CLOSE_EXCEPT_SQL`].
const LONG_CLOSE_EXCEPT_STUB_SQL: &str = "UPDATE value_long SET valid_to_commit = ?, \
       content_retained = FALSE, v_float = NULL, v_int = NULL, v_bool = NULL, v_str = NULL, \
       v_datetime = NULL, v_dur_months = NULL, v_dur_micros = NULL \
     WHERE entity_id = ? AND parameter_definition_id = ? AND alternative_id = ? \
       AND valid_to_commit IS NULL \
       AND NOT EXISTS (SELECT 1 FROM (SELECT unnest(?::INTEGER[]) AS q1, \
             unnest(?::INTEGER[]) AS q2, unnest(?::INTEGER[]) AS q3, \
             unnest(?::INTEGER[]) AS q4) k \
           WHERE value_long.i1_pos = k.q1 AND value_long.i2_pos = k.q2 \
             AND value_long.i3_pos = k.q3 AND value_long.i4_pos = k.q4)";

/// Close out every current cell of `key` but those at `keep` (one
/// statement, ≤ [`LONG_INSERT_CHUNK`] kept positions), checked against
/// `expected`, the number of cells the diff closes.
fn close_long_except(
    conn: &Connection,
    c: i64,
    retained: bool,
    key: &ValueKey,
    keep: &[[i32; 4]],
    expected: usize,
) -> Result<(), DbError> {
    let sql = if retained {
        LONG_CLOSE_EXCEPT_SQL
    } else {
        LONG_CLOSE_EXCEPT_STUB_SQL
    };
    let [e, p, a] = point(key);
    let mut v: Vec<Value> = vec![
        Value::BigInt(c),
        Value::BigInt(e),
        Value::BigInt(p),
        Value::BigInt(a),
    ];
    for d in 0..4 {
        v.push(list(keep, |p| Value::Int(p[d])));
    }
    check_list_lengths(&v[4..], keep.len())?;
    let affected = conn
        .prepare_cached(sql)?
        .execute(duckdb::params_from_iter(v))?;
    check_affected("long cell close-out", affected, expected)
}

/// Close out the current cells at `cells` ((key, positions) rows, each
/// holding exactly one current row: the diffed keys are coherent) at
/// commit `c`, in executions of ≤ [`LONG_INSERT_CHUNK`] rows, each checked
/// against its row count.
fn close_long_cells(
    conn: &Connection,
    c: i64,
    retained: bool,
    cells: &[(Triple, [i32; 4])],
) -> Result<(), DbError> {
    if cells.is_empty() {
        return Ok(());
    }
    let sql = if retained {
        LONG_CLOSE_CELLS_SQL
    } else {
        LONG_CLOSE_CELLS_STUB_SQL
    };
    let mut stmt = conn.prepare_cached(sql)?;
    for chunk in cells.chunks(LONG_INSERT_CHUNK) {
        let mut v: Vec<Value> = Vec::with_capacity(8);
        v.push(Value::BigInt(c));
        v.push(list(chunk, |(t, _)| Value::BigInt(t.0)));
        v.push(list(chunk, |(t, _)| Value::BigInt(t.1)));
        v.push(list(chunk, |(t, _)| Value::BigInt(t.2)));
        for d in 0..4 {
            v.push(list(chunk, |(_, p)| Value::Int(p[d])));
        }
        check_list_lengths(&v[1..], chunk.len())?;
        let affected = stmt.execute(duckdb::params_from_iter(v))?;
        check_affected("long cell close-out", affected, chunk.len())?;
    }
    Ok(())
}

/// One round of [`write_longs`] (see there for the bounds).
fn write_long_round(
    conn: &Connection,
    c: i64,
    w: &[LongWrite],
) -> Result<Vec<WriteOutcome>, DbError> {
    let n = w.len();
    let single = n == 1;
    // --- Probes (only for keys whose entity existed before this commit). ---
    let probed: Vec<&ValueKey> = w
        .iter()
        .filter(|l| !l.entity_minted)
        .map(|l| &l.key)
        .collect();
    let mut probe = match probed[..] {
        [] => LongProbe::default(),
        [k] if single => LongProbe::point(conn, k)?,
        _ => LongProbe::set(conn, &probed)?,
    };

    // --- The cell diff per key (skip_unchanged_writes_PLAN §4.1). A key
    // live in long whose current cells share one meta (axis ids, binding,
    // kind) equal to the new value's is merged cell by cell; any other key
    // is rewritten whole (every current cell closes, every new cell is
    // inserted). An empty diff is an unchanged write. Each key's probed
    // cells are dropped once diffed (critique C5: memory). ---
    let empty = LongCells::default();
    let diffs: Vec<LongDiff> = w
        .iter()
        .map(|l| {
            let t = triple(&l.key);
            let cells = probe.cells.remove(&t);
            let cells = cells.as_ref().unwrap_or(&empty);
            let live_long = matches!(probe.live.get(&t), Some((Live::Long, _)));
            let axes: [Option<i64>; 4] = std::array::from_fn(|d| l.axis_ids.get(d).copied());
            let whole = !live_long
                || cells.incoherent
                || cells.meta
                    != Some(CellMeta {
                        axes,
                        pt: l.parameter_type_id,
                        kind: l.value.value_kind,
                    });
            let mut d = LongDiff::of(cells, l, whole);
            if !live_long {
                // Not live in long: nothing of this tier closes (a damaged
                // doubly-live key keeps today's outcome).
                d.close.clear();
            } else if d.close.len() == cells.current {
                d.whole = true;
            }
            d
        })
        .collect();
    let unchanged: Vec<bool> = w
        .iter()
        .zip(&diffs)
        .map(|(l, d)| {
            matches!(probe.live.get(&triple(&l.key)), Some((Live::Long, _)))
                && !d.whole
                && d.close.is_empty()
                && d.insert.is_empty()
        })
        .collect();

    // --- Tier transitions: the per-key close-out of the other tier, in
    // input order (plan D6). ---
    let mut closed = vec![0usize; n];
    for (i, l) in w.iter().enumerate() {
        if let Some(&(cur @ (Live::Scalar | Live::Array(_)), _)) = probe.live.get(&triple(&l.key)) {
            closed[i] = close_out_tier(conn, &l.key, c, cur, l.retained)?;
        }
    }

    // --- Same-tier close-out, per retention mode: the keys closing every
    // current cell by the key-level UPDATE, the diffed keys' changed and
    // removed cells by the position-joined one. The close-out changes no
    // version, so the probe's versions equal a snapshot taken after it. ---
    for retained in [true, false] {
        let live_long = |i: usize| {
            w[i].retained == retained
                && matches!(probe.live.get(&triple(&w[i].key)), Some((Live::Long, _)))
        };
        let whole: Vec<usize> = (0..n).filter(|&i| live_long(i) && diffs[i].whole).collect();
        if !whole.is_empty() {
            if single {
                closed[0] += close_out_long(conn, &w[0].key, c, retained)?;
            } else {
                let sql = if retained {
                    LONG_CLOSE_SQL
                } else {
                    LONG_CLOSE_STUB_SQL
                };
                let [e, p, a] = key_lists(whole.iter().map(|&i| &w[i].key));
                let affected = conn
                    .prepare_cached(sql)?
                    .execute(duckdb::params_from_iter([Value::BigInt(c), e, p, a]))?;
                let expected: usize = whole
                    .iter()
                    .map(|&i| probe.live[&triple(&w[i].key)].1)
                    .sum();
                check_affected("long close-out", affected, expected)?;
                for i in whole {
                    closed[i] += probe.live[&triple(&w[i].key)].1;
                }
            }
        }
        let mut cells: Vec<(Triple, [i32; 4])> = Vec::new();
        for i in (0..n).filter(|&i| live_long(i) && !diffs[i].whole) {
            let d = &diffs[i];
            closed[i] += d.close.len();
            match &d.keep {
                // One key closing more cells than it keeps (with at most a
                // chunk kept): "all but the kept ones", one statement.
                Some(keep) if single && keep.len() < d.close.len() => {
                    close_long_except(conn, c, retained, &w[i].key, keep, d.close.len())?;
                }
                _ => {
                    let t = triple(&w[i].key);
                    cells.extend(d.close.iter().map(|&p| (t, p)));
                }
            }
        }
        close_long_cells(conn, c, retained, &cells)?;
    }

    // --- Insert the changed and added cells: every column built in one
    // pass through the one encoder. ---
    let mut keys = LongKeys::default();
    let mut cols = LongColumns::default();
    for ((l, d), &same) in w.iter().zip(&diffs).zip(&unchanged) {
        if same || d.insert.is_empty() {
            continue;
        }
        let ordinal = keys.push(l)?;
        cols.push_cells(l.rows(), &l.value.axes, &d.insert, ordinal)?;
    }
    insert_long_round(conn, c, &keys, &cols)?;

    Ok(w.iter()
        .zip(&diffs)
        .zip(closed)
        .zip(&unchanged)
        .map(|(((l, d), rows_closed), &same)| {
            if same {
                return unchanged_outcome(
                    Tier::Long,
                    d.kept_version,
                    (l.axes_created, l.axes_reused),
                    d.kept,
                );
            }
            WriteOutcome {
                tier: Some(Tier::Long),
                version: d.version(),
                cells_unchanged: d.kept,
                rows_written: d.insert.len(),
                rows_closed,
                axes_created: l.axes_created,
                axes_reused: l.axes_reused,
                ..WriteOutcome::default()
            }
        })
        .collect())
}

/// Rows per columnar-bind execution (plan D5, critique C7): the long insert
/// and the axis child rows.
/// Every list element is a Rust `Value` plus a transient `duckdb_value` while
/// the bind copies it, so a whole 1 M-cell value bound at once peaks near
/// 1 GB; 16 384 cells keep the peak at ~200 MB (mostly DuckDB's
/// transaction-local storage) at the same speed. An 8 760-cell series is one
/// execution. The statement text does not depend on the chunk length.
pub(crate) const LONG_INSERT_CHUNK: usize = 16_384;

/// The per-key columns of one long round, in key order: bound as short
/// per-key lists that the per-cell key ordinal indexes (critique C1-C2).
#[derive(Default)]
struct LongKeys {
    e: Vec<i64>,
    p: Vec<i64>,
    a: Vec<i64>,
    pt: Vec<Option<i64>>,
    axis: [Vec<Option<i64>>; 4],
}

impl LongKeys {
    /// Append one key; returns its 1-based ordinal.
    fn push(&mut self, l: &LongWrite) -> Result<i32, DbError> {
        self.e.push(l.key.entity_id);
        self.p.push(l.key.parameter_definition_id);
        self.a.push(l.key.alternative_id);
        self.pt.push(l.parameter_type_id);
        for (d, col) in self.axis.iter_mut().enumerate() {
            col.push(l.axis_ids.get(d).copied());
        }
        i32::try_from(self.e.len())
            .map_err(|_| DbError::Internal("long batch: key ordinal out of range".into()))
    }

    fn len(&self) -> usize {
        self.e.len()
    }

    /// The 8 per-key bind values: entity, definition, alternative, member
    /// binding, then the four axis ids, each a list of one entry per key
    /// (an all-NULL column binds a plain NULL, which indexes to NULL).
    fn params(&self) -> [Value; 8] {
        [
            list(&self.e, |&v| Value::BigInt(v)),
            list(&self.p, |&v| Value::BigInt(v)),
            list(&self.a, |&v| Value::BigInt(v)),
            opt_list(&self.pt, |&v| Value::BigInt(v)),
            opt_list(&self.axis[0], |&v| Value::BigInt(v)),
            opt_list(&self.axis[1], |&v| Value::BigInt(v)),
            opt_list(&self.axis[2], |&v| Value::BigInt(v)),
            opt_list(&self.axis[3], |&v| Value::BigInt(v)),
        ]
    }

    /// Every per-key column must hold one entry per key.
    fn check_column_lengths(&self) -> Result<(), DbError> {
        let n = self.len();
        let lens = [
            self.p.len(),
            self.a.len(),
            self.pt.len(),
            self.axis[0].len(),
            self.axis[1].len(),
            self.axis[2].len(),
            self.axis[3].len(),
        ];
        if lens.iter().any(|&l| l != n) {
            return Err(DbError::Internal(format!(
                "columnar insert: per-key column lengths {lens:?} differ from {n} keys"
            )));
        }
        Ok(())
    }
}

/// The per-cell columns of one long round, built column-wise in ONE pass
/// over the canonical rows (plan D4). Still the one encoder: the value
/// columns come from [`VCols::of`], the accelerators from [`accel`], the
/// positions from [`pad4_pos`] and the versions from the per-position
/// version map; `k` is the cell's 1-based key ordinal into [`LongKeys`].
/// Each column holds exactly one Rust type, so each binds as a list of one
/// `Value` variant (duckdb-rs rejects a list mixing variants).
#[derive(Default)]
struct LongColumns {
    k: Vec<i32>,
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
    /// Append the cells `inserts` of one value (key ordinal `k`): each a
    /// row index into `rows` and the cell's version ([`LongDiff`]).
    /// Returns the max version appended.
    fn push_cells(
        &mut self,
        rows: &[crate::types::LongRow],
        axes: &[CanonicalAxis],
        inserts: &[(u32, u16)],
        k: i32,
    ) -> Result<u16, DbError> {
        let mut max_version = 0u16;
        for &(r, version) in inserts {
            let row = &rows[r as usize];
            let pos = pad4_pos(&row.positions);
            let v16 = i16::try_from(version).map_err(|_| {
                DbError::Internal(format!("long batch: version {version} out of range"))
            })?;
            max_version = max_version.max(version);
            self.k.push(k);
            self.version.push(v16);
            // Per-dimension index accelerators, derived from the bound axis at
            // the cell's position (audited by audit_index_accelerator; never
            // authoritative). NULL beyond the declared rank.
            for (d, &p) in pos.iter().enumerate() {
                self.pos[d].push(p);
                let (a_dt, a_num) = axes.get(d).map_or((None, None), |ax| accel(ax, p));
                self.dt[d].push(a_dt);
                self.num[d].push(a_num);
            }
            let vc = VCols::of(&row.value);
            self.kind.push(vc.kind);
            self.f.push(vc.f);
            self.i.push(vc.i);
            self.b.push(vc.b);
            self.s.push(vc.s);
            self.vdt.push(vc.dt);
            self.dm.push(vc.dm);
            self.du.push(vc.du);
        }
        Ok(max_version)
    }

    fn len(&self) -> usize {
        self.version.len()
    }

    /// The hard length check (critique C2): every per-cell column holds
    /// exactly one entry per cell (a short column whose chunk is all NULL
    /// would otherwise bind as a plain NULL and pass the list check), and
    /// every key ordinal lies in `1..=keys` (an out-of-range `list[k]` reads
    /// NULL silently).
    fn check(&self, keys: usize) -> Result<(), DbError> {
        let n = self.len();
        let lens = [
            ("k", vec![self.k.len()]),
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
        if let Some(&k) = self
            .k
            .iter()
            .find(|&&k| k < 1 || usize::try_from(k).map_or(true, |k| k > keys))
        {
            return Err(DbError::Internal(format!(
                "columnar insert: key ordinal {k} outside 1..={keys}"
            )));
        }
        Ok(())
    }

    /// The 22 per-cell bind values of one execution over cells `r`, in the
    /// order of the FROM subquery of [`LONG_INSERT_SQL`]. A column that is
    /// NULL in every cell of the chunk binds a plain NULL (the zip pads it).
    fn params(&self, r: std::ops::Range<usize>) -> Vec<Value> {
        let mut p: Vec<Value> = Vec::with_capacity(22);
        p.push(list(clip(&self.k, &r), |&v| Value::Int(v)));
        p.push(list(clip(&self.version, &r), |&v| Value::SmallInt(v)));
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

/// The columnar `value_long` INSERT in the key-ordinal form (plan Appendix
/// A.3', critique C1-C2), the one long insert for every N. The per-key
/// columns bind as short per-key lists indexed by the cell's 1-based key
/// ordinal `u.k`; every per-cell column is an `unnest` inside the FROM
/// subquery (zipped by position; no join, so the rows keep list order, i.e.
/// keys in round order and each key's cells in canonical order). The
/// timestamp columns cross as BIGINT µs through `make_timestamp`, the same
/// conversion as every other timestamp write. 30 columns (29 + the D11
/// `parameter_type_id` binding); `content_retained` keeps its TRUE default.
const LONG_INSERT_SQL: &str = "INSERT INTO value_long \
       (entity_id, parameter_definition_id, alternative_id, parameter_type_id, \
        valid_from_commit, version, \
        axis1_id, axis2_id, axis3_id, axis4_id, i1_pos, i2_pos, i3_pos, i4_pos, \
        i1_dt, i1_num, i2_dt, i2_num, i3_dt, i3_num, i4_dt, i4_num, \
        value_kind, v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros) \
     SELECT (?::BIGINT[])[u.k], (?::BIGINT[])[u.k], (?::BIGINT[])[u.k], (?::BIGINT[])[u.k], \
       ?, u.version, \
       (?::BIGINT[])[u.k], (?::BIGINT[])[u.k], (?::BIGINT[])[u.k], (?::BIGINT[])[u.k], \
       u.i1_pos, u.i2_pos, u.i3_pos, u.i4_pos, \
       u.i1_dt, u.i1_num, u.i2_dt, u.i2_num, u.i3_dt, u.i3_num, u.i4_dt, u.i4_num, \
       u.value_kind, u.v_float, u.v_int, u.v_bool, u.v_str, u.v_datetime, \
       u.v_dur_months, u.v_dur_micros \
     FROM (SELECT unnest(?::INTEGER[]) AS k, unnest(?::SMALLINT[]) AS version, \
       unnest(?::INTEGER[]) AS i1_pos, unnest(?::INTEGER[]) AS i2_pos, \
       unnest(?::INTEGER[]) AS i3_pos, unnest(?::INTEGER[]) AS i4_pos, \
       make_timestamp(unnest(?::BIGINT[])) AS i1_dt, unnest(?::BIGINT[]) AS i1_num, \
       make_timestamp(unnest(?::BIGINT[])) AS i2_dt, unnest(?::BIGINT[]) AS i2_num, \
       make_timestamp(unnest(?::BIGINT[])) AS i3_dt, unnest(?::BIGINT[]) AS i3_num, \
       make_timestamp(unnest(?::BIGINT[])) AS i4_dt, unnest(?::BIGINT[]) AS i4_num, \
       unnest(?::VARCHAR[]) AS value_kind, unnest(?::DOUBLE[]) AS v_float, \
       unnest(?::BIGINT[]) AS v_int, unnest(?::BOOLEAN[]) AS v_bool, \
       unnest(?::VARCHAR[]) AS v_str, make_timestamp(unnest(?::BIGINT[])) AS v_datetime, \
       unnest(?::INTEGER[]) AS v_dur_months, unnest(?::BIGINT[]) AS v_dur_micros) u";

/// Insert one long round's cells: one cached statement, executed per
/// ≤ [`LONG_INSERT_CHUNK`] cells after the hard length checks (per-key
/// lists against the key count, per-cell lists against the chunk, ordinals
/// in range). All executes run on `conn` inside the caller's transaction,
/// so a write-write conflict or any error surfaces here or at COMMIT and
/// ROLLBACK unwinds every row; a 0-cell round never executes.
fn insert_long_round(
    conn: &Connection,
    c: i64,
    keys: &LongKeys,
    cols: &LongColumns,
) -> Result<(), DbError> {
    keys.check_column_lengths()?;
    cols.check(keys.len())?;
    let key_params = keys.params();
    check_list_lengths(&key_params, keys.len())?;
    let n = cols.len();
    if n == 0 {
        return Ok(());
    }
    let [e, p, a, pt, ax1, ax2, ax3, ax4] = key_params;
    let mut stmt = conn.prepare_cached(LONG_INSERT_SQL)?;
    let mut lo = 0;
    while lo < n {
        let hi = (lo + LONG_INSERT_CHUNK).min(n);
        let cells = cols.params(lo..hi);
        check_list_lengths(&cells, hi - lo)?;
        let mut v: Vec<Value> = Vec::with_capacity(31);
        v.extend([
            e.clone(),
            p.clone(),
            a.clone(),
            pt.clone(),
            Value::BigInt(c),
        ]);
        v.extend([ax1.clone(), ax2.clone(), ax3.clone(), ax4.clone()]);
        v.extend(cells);
        stmt.execute(duckdb::params_from_iter(v))?;
        lo = hi;
    }
    Ok(())
}

/// Run one columnar-bind statement (`INSERT … SELECT ?, unnest(?::T[]), …`)
/// over `n` rows: prepared once through the statement cache, executed per
/// ≤ [`LONG_INSERT_CHUNK`] rows with the bind values `params(range)`, each
/// execution gated by the hard length check. `n == 0` executes nothing.
/// Shared by the long insert and the axis child-row inserts (`axis`).
pub(crate) fn exec_columnar(
    conn: &Connection,
    sql: &str,
    n: usize,
    params: impl Fn(std::ops::Range<usize>) -> Vec<Value>,
) -> Result<(), DbError> {
    if n == 0 {
        return Ok(());
    }
    let mut stmt = conn.prepare_cached(sql)?;
    let mut lo = 0;
    while lo < n {
        let hi = (lo + LONG_INSERT_CHUNK).min(n);
        let p = params(lo..hi);
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
pub(crate) fn clip<'a, T>(col: &'a [T], r: &std::ops::Range<usize>) -> &'a [T] {
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

// ---------------------------------------------------------------------------
// Unchanged-write detection (skip_unchanged_writes_PLAN §2.1, critique C3)
// ---------------------------------------------------------------------------

/// The payload of one stored value row or long cell, decoded from its RAW
/// stored columns: floats as bits, strings as their exact bytes, timestamps
/// as µs. The stored side is never re-canonicalized (critique C3.1), and
/// equality is never decided by `encoding::content_hash` / `encode_value`,
/// whose string encoding is NFC-normalized while `v_str` is stored verbatim
/// (C3.2). A new value is decoded by the same function from its
/// [`VCols::of`] encoding, so equal cells mean equal encoder outputs: the
/// same `value_kind` and the same bits in every value column.
#[derive(Clone, Debug, PartialEq)]
enum StoredCell {
    Float(u64),
    Int(i64),
    Bool(bool),
    Str(Box<str>),
    DateTime(i64),
    Duration(i32, i64),
}

/// The value columns of one row: `value_kind`, then the 7 payload columns.
struct RawCols<'a> {
    kind: &'a [u8],
    f: Option<f64>,
    i: Option<i64>,
    b: Option<bool>,
    s: Option<&'a str>,
    dt: Option<i64>,
    dm: Option<i32>,
    du: Option<i64>,
}

impl StoredCell {
    /// The cell, or `None` when the columns are not exactly what the encoder
    /// writes for the row's kind (an unknown kind tag, a missing payload
    /// column, a second one set). Such a row can never equal a new value,
    /// so the write goes ahead: every doubt errs toward "changed".
    fn decode(c: &RawCols) -> Option<StoredCell> {
        let set = [
            c.f.is_some(),
            c.i.is_some(),
            c.b.is_some(),
            c.s.is_some(),
            c.dt.is_some(),
            c.dm.is_some(),
            c.du.is_some(),
        ]
        .iter()
        .filter(|&&x| x)
        .count();
        let is = |k: ValueKind| c.kind == k.tag().as_bytes();
        match set {
            1 if is(ValueKind::Float) => c.f.map(|x| StoredCell::Float(x.to_bits())),
            1 if is(ValueKind::Int) => c.i.map(StoredCell::Int),
            1 if is(ValueKind::Bool) => c.b.map(StoredCell::Bool),
            1 if is(ValueKind::String) => c.s.map(|s| StoredCell::Str(s.into())),
            1 if is(ValueKind::DateTime) => c.dt.map(StoredCell::DateTime),
            2 if is(ValueKind::Duration) => match (c.dm, c.du) {
                (Some(m), Some(u)) => Some(StoredCell::Duration(m, u)),
                _ => None,
            },
            _ => None,
        }
    }

    /// The cell a write of `v` stores (through the one encoder).
    fn of_new(v: &ScalarValue) -> Option<StoredCell> {
        let vc = VCols::of(v);
        StoredCell::decode(&RawCols {
            kind: vc.kind.as_bytes(),
            f: vc.f,
            i: vc.i,
            b: vc.b,
            s: vc.s.as_deref(),
            dt: vc.dt,
            dm: vc.dm,
            du: vc.du,
        })
    }

    /// Read the 8 value columns of a probe row starting at column `o`
    /// (`value_kind`, `v_float`, `v_int`, `v_bool`, `v_str`, `v_datetime`,
    /// `v_dur_months`, `v_dur_micros`) and decode them. A timestamp is
    /// converted to µs by its unit (critique C3.5); a column of an
    /// unexpected type, or a timestamp not exact in µs, yields `None`.
    fn read(r: &duckdb::Row, o: usize) -> duckdb::Result<Option<StoredCell>> {
        let kind = match r.get_ref(o)? {
            ValueRef::Text(t) => t,
            _ => return Ok(None),
        };
        let s = match r.get_ref(o + 4)? {
            ValueRef::Null => None,
            ValueRef::Text(t) => match std::str::from_utf8(t) {
                Ok(s) => Some(s),
                Err(_) => return Ok(None),
            },
            _ => return Ok(None),
        };
        let dt = match r.get_ref(o + 5)? {
            ValueRef::Null => None,
            ValueRef::Timestamp(unit, v) => match timestamp_us(unit, v) {
                Some(us) => Some(us),
                None => return Ok(None),
            },
            _ => return Ok(None),
        };
        Ok(StoredCell::decode(&RawCols {
            kind,
            f: r.get(o + 1)?,
            i: r.get(o + 2)?,
            b: r.get(o + 3)?,
            s,
            dt,
            dm: r.get(o + 6)?,
            du: r.get(o + 7)?,
        }))
    }
}

/// A duckdb-rs timestamp as epoch µs, exactly, or `None`.
fn timestamp_us(unit: TimeUnit, v: i64) -> Option<i64> {
    match unit {
        TimeUnit::Second => v.checked_mul(1_000_000),
        TimeUnit::Millisecond => v.checked_mul(1_000),
        TimeUnit::Microsecond => Some(v),
        TimeUnit::Nanosecond => (v % 1_000 == 0).then_some(v / 1_000),
    }
}

/// A key's (or a long position's) current row as a probe read it: its
/// decoded value (`None` when not comparable: no exactly-one current row,
/// a malformed or stub row), its binding and its version.
#[derive(Clone, Debug)]
struct CurRow {
    cell: Option<StoredCell>,
    pt: Option<i64>,
}

impl CurRow {
    /// Read the [`cur_value_cols!`] block of a probe row at column `o`,
    /// given the number of current rows the probe counted for it.
    fn read(r: &duckdb::Row, o: usize, current: i64) -> duckdb::Result<CurRow> {
        let cell = StoredCell::read(r, o)?;
        let pt: Option<i64> = r.get(o + 8)?;
        let retained: Option<bool> = r.get(o + 9)?;
        Ok(CurRow {
            cell: if current == 1 && retained == Some(true) {
                cell
            } else {
                None
            },
            pt,
        })
    }

    /// Does this current row equal a new write of `v` bound to `pt`? The
    /// binding compares as `IS NOT DISTINCT FROM` (a stored NULL against a
    /// new member is a change, which rebinds; critique C3.6, D8).
    fn equals(&self, v: &ScalarValue, pt: Option<i64>) -> bool {
        self.pt == pt
            && match (&self.cell, StoredCell::of_new(v)) {
                (Some(a), Some(b)) => *a == b,
                _ => false,
            }
    }
}

/// An unchanged write's outcome: the current tier and version, nothing
/// written or closed.
fn unchanged_outcome(tier: Tier, version: u16, axes: (usize, usize), cells: usize) -> WriteOutcome {
    WriteOutcome {
        tier: Some(tier),
        version,
        unchanged: true,
        cells_unchanged: cells,
        rows_written: 0,
        rows_closed: 0,
        axes_created: axes.0,
        axes_reused: axes.1,
    }
}

/// Two `v_min` / `v_max` values that do not prove a change: equal bits, or
/// both NaN (DuckDB orders NaN as one value). The array prefilter only
/// ever uses a mismatch, as evidence of a change; a match proves nothing
/// and the payload bytes decide (critique C3.5).
fn same_bound(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
}

/// The `(v_datetime µs, v_num)` accelerator for a dimension's axis at `pos`.
/// Datetime axes carry the timestamp, period axes the period's `start_ts`
/// (the `ts` that `axis_position` renders for a period), integer axes the
/// number; label/float/time_pattern leave both NULL — exactly what
/// `audit_index_accelerator` expects. (Canon does not emit period axes for
/// long values today; the period arm keeps a hand-built value audit-clean.)
fn accel(axis: &CanonicalAxis, pos: i32) -> (Option<i64>, Option<i64>) {
    match &axis.body {
        AxisBody::DatetimeParametric { start_ts, step } => {
            (datetime_coord(*start_ts, step, pos as i64), None)
        }
        AxisBody::DatetimeMaterialized { coords } => (coords.get(pos as usize).copied(), None),
        AxisBody::IntegerParametric { start, step } => (None, Some(start + pos as i64 * step)),
        AxisBody::IntegerMaterialized { coords } => (None, coords.get(pos as usize).copied()),
        AxisBody::Period { positions } => (positions.get(pos as usize).map(|p| p.start_ts), None),
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
    let scalar_ok = scalar_float_canonical;
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

fn scalar_float_canonical(m: &ScalarValue) -> bool {
    match m {
        ScalarValue::Float(x) => x.to_bits() == crate::encoding::canonical_value_f64(*x).to_bits(),
        _ => true,
    }
}

/// The [`debug_assert_value_floats_canonical`] check for one scalar of a
/// set-based batch.
fn debug_assert_scalar_float_canonical(m: &ScalarValue) {
    debug_assert!(
        scalar_float_canonical(m),
        "write_scalars got a non-canonical value float (-0.0 or a non-canonical NaN); \
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

    /// The per-key and per-cell columns of one round holding `KEY` with an
    /// `n`-cell long value on `axis_ids` (versions 1).
    fn long_round(axis_ids: &[i64], n: i32) -> (LongKeys, LongColumns) {
        let w = LongWrite {
            key: KEY,
            value: Cow::Owned(long_value(n)),
            axis_ids: axis_ids.to_vec(),
            axes_created: 0,
            axes_reused: 0,
            parameter_type_id: None,
            retained: true,
            entity_minted: true,
        };
        let mut keys = LongKeys::default();
        let mut cols = LongColumns::default();
        let k = keys.push(&w).unwrap();
        let inserts: Vec<(u32, u16)> = (0..n as u32).map(|r| (r, 1)).collect();
        cols.push_cells(w.rows(), &w.value.axes, &inserts, k)
            .unwrap();
        (keys, cols)
    }

    fn axis_id(conn: &Connection, len: i32) -> i64 {
        crate::axis::dedup_or_insert(conn, &mut AxisCache::new(), &int_axis(len)).unwrap()
    }

    #[test]
    fn length_check_rejects_a_short_column() {
        // A shorter list would be NULL-padded silently by the zip-unnest; the
        // check must fail the write instead (in every build).
        let (_d, db) = fresh();
        let before = counts(&db);
        let err = with_write_txn(&db, "t", "short", |conn, c| {
            let ax = axis_id(conn, 3);
            let (keys, mut cols) = long_round(&[ax], 3);
            cols.f.pop();
            insert_long_round(conn, c, &keys, &cols)
        })
        .expect_err("a short column must be rejected");
        assert!(matches!(err, DbError::Internal(_)), "got {err:?}");
        assert_eq!(counts(&db), before);

        // The key-ordinal form (critique C2): a short per-key list and a
        // key ordinal outside 1..=keys (which would read NULL) fail too.
        let err = with_write_txn(&db, "t", "short key", |conn, c| {
            let ax = axis_id(conn, 3);
            let (mut keys, cols) = long_round(&[ax], 3);
            keys.pt.pop();
            insert_long_round(conn, c, &keys, &cols)
        })
        .expect_err("a short per-key column must be rejected");
        assert!(matches!(err, DbError::Internal(_)), "got {err:?}");
        for bad in [0, 2] {
            let err = with_write_txn(&db, "t", "ordinal", |conn, c| {
                let ax = axis_id(conn, 3);
                let (keys, mut cols) = long_round(&[ax], 3);
                cols.k[1] = bad;
                insert_long_round(conn, c, &keys, &cols)
            })
            .expect_err("an out-of-range key ordinal must be rejected");
            assert!(
                matches!(&err, DbError::Internal(m) if m.contains("key ordinal")),
                "got {err:?}"
            );
        }
        assert_eq!(counts(&db), before);

        // An empty list against a non-empty chunk is caught too; a plain NULL
        // (the all-NULL column) is not a list and passes.
        assert!(check_list_lengths(&[Value::List(vec![])], 1).is_err());
        assert!(check_list_lengths(&[Value::Null, list(&[1i32], |&v| Value::Int(v))], 1).is_ok());
    }

    /// Every row of entity 1 (the `KEY` entity) in every value table, as
    /// text, in a stable order.
    fn key_rows(db: &Path) -> Vec<String> {
        let conn = Connection::open(db).unwrap();
        let mut out = Vec::new();
        for t in [
            "value_scalar",
            "value_long",
            "value_array",
            "value_array_hist",
        ] {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT CAST(t AS VARCHAR) FROM (SELECT * FROM {t} WHERE entity_id = 1 \
                     ORDER BY ALL) t"
                ))
                .unwrap();
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            out.extend(rows.into_iter().map(|r| format!("{t}: {r}")));
        }
        conn.close().unwrap();
        out
    }

    /// The point-filter statement form a round of ONE key uses (plan
    /// D3(b)) and the list-join form of a larger round give the same rows
    /// and outcome for the same key, over every prior state of the key
    /// (none, scalar live, scalar closed, long live, array live) and both
    /// retention modes, plus a minted key.
    #[test]
    fn single_key_round_matches_the_set_round() {
        const OTHER: ValueKey = ValueKey {
            entity_id: 9,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        let scalar = |x: f64| CanonicalValue {
            tier: Tier::Scalar,
            value_kind: ValueKind::Float,
            axes: vec![],
            shape: ShapedData::Scalar(ScalarValue::Float(x)),
        };
        let array = CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(3)],
            shape: ShapedData::Array(vec![1.0, 2.0, 3.0]),
        };
        let opts = WriteOptions::default();
        // Each prior state of KEY, as a sequence of per-commit steps
        // (Some(value) writes, None deletes).
        let states: Vec<(&str, Vec<Option<CanonicalValue>>)> = vec![
            ("none", vec![]),
            ("scalar live", vec![Some(scalar(1.0)), Some(scalar(2.0))]),
            ("scalar closed", vec![Some(scalar(1.0)), None]),
            ("long live", vec![Some(scalar(1.0)), Some(long_value(3))]),
            ("array live", vec![Some(array.clone())]),
            (
                "array replaced",
                vec![Some(array.clone()), Some(array.clone())],
            ),
        ];
        let setup = |db: &Path, steps: &[Option<CanonicalValue>]| {
            for step in steps {
                with_write_txn(db, "t", "setup", |conn, c| match step {
                    Some(v) => write_value(conn, c, KEY, v, &mut AxisCache::new(), &opts, None),
                    None => delete_value(conn, c, KEY, &opts),
                })
                .unwrap();
            }
            with_write_txn(db, "t", "other", |conn, c| {
                write_value(
                    conn,
                    c,
                    OTHER,
                    &scalar(5.0),
                    &mut AxisCache::new(),
                    &opts,
                    None,
                )
            })
            .unwrap();
        };
        for (name, steps) in &states {
            for retained in [true, false] {
                for minted in [false, true] {
                    if minted && !steps.is_empty() {
                        continue;
                    }
                    let w = ScalarWrite {
                        key: KEY,
                        value: ScalarValue::Float(7.5),
                        parameter_type_id: Some(4),
                        retained,
                        entity_minted: minted,
                    };
                    let other = ScalarWrite {
                        key: OTHER,
                        value: ScalarValue::Float(6.0),
                        parameter_type_id: None,
                        retained: !retained,
                        entity_minted: false,
                    };
                    let (_da, a) = fresh();
                    let (_db, b) = fresh();
                    setup(&a, steps);
                    setup(&b, steps);
                    let oa = with_write_txn(&a, "t", "w", |conn, c| {
                        write_scalars(conn, c, std::slice::from_ref(&w))
                    })
                    .unwrap();
                    let ob = with_write_txn(&b, "t", "w", |conn, c| {
                        write_scalars(conn, c, &[w.clone(), other.clone()])
                    })
                    .unwrap();
                    let case = format!("{name}, retained={retained}, minted={minted}");
                    assert_eq!(oa.len(), 1, "{case}");
                    assert_eq!(format!("{:?}", oa[0]), format!("{:?}", ob[0]), "{case}");
                    let rows = key_rows(&a);
                    assert!(!rows.is_empty(), "{case}");
                    assert_eq!(rows, key_rows(&b), "{case}");
                }
            }
        }
    }

    /// Plant a current row for `KEY` in `table` by direct SQL (a damaged,
    /// doubly-live key, which the writers never produce).
    fn plant(db: &Path, table: &str) {
        let conn = Connection::open(db).unwrap();
        let c = "(SELECT max(id) FROM \"commit\")";
        let sql = match table {
            "value_scalar" => format!(
                "INSERT INTO value_scalar (entity_id, parameter_definition_id, alternative_id, \
                   valid_from_commit, version, value_kind, v_float) \
                 VALUES (1, 2, 3, {c}, 1, 'float', 7.0)"
            ),
            "value_array" => format!(
                "INSERT INTO value_array (entity_id, parameter_definition_id, alternative_id, \
                   valid_from_commit, version, axis1_id, values, v_min, v_max) \
                 VALUES (1, 2, 3, {c}, 1, (SELECT min(id) FROM axis), [1.0], 1.0, 1.0)"
            ),
            other => panic!("plant: {other}"),
        };
        conn.execute(&sql, []).unwrap();
        conn.close().unwrap();
    }

    /// The point statements of a one-key delete round and the set
    /// statements of a larger round give the same rows and outcome for the
    /// same key, over every prior state of the key (none, each tier live,
    /// closed, replaced, and the two doubly-live damaged states, where the
    /// per-key precedence decides), in both retention modes.
    #[test]
    fn single_key_delete_round_matches_the_set_round() {
        const OTHER: ValueKey = ValueKey {
            entity_id: 9,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        let scalar = |x: f64| CanonicalValue {
            tier: Tier::Scalar,
            value_kind: ValueKind::Float,
            axes: vec![],
            shape: ShapedData::Scalar(ScalarValue::Float(x)),
        };
        let array = CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(3)],
            shape: ShapedData::Array(vec![1.0, 2.0, 3.0]),
        };
        let opts = WriteOptions::default();
        // Each prior state of KEY: per-commit steps (Some writes, None
        // deletes), then an optional planted row.
        type State = (
            &'static str,
            Vec<Option<CanonicalValue>>,
            Option<&'static str>,
        );
        let states: Vec<State> = vec![
            ("none", vec![], None),
            (
                "scalar live",
                vec![Some(scalar(1.0)), Some(scalar(2.0))],
                None,
            ),
            ("scalar closed", vec![Some(scalar(1.0)), None], None),
            (
                "long live",
                vec![Some(long_value(2)), Some(long_value(3))],
                None,
            ),
            ("long closed", vec![Some(long_value(3)), None], None),
            ("array live", vec![Some(array.clone())], None),
            (
                "array replaced",
                vec![Some(array.clone()), Some(array.clone())],
                None,
            ),
            (
                "scalar+long",
                vec![Some(long_value(3))],
                Some("value_scalar"),
            ),
            ("long+array", vec![Some(long_value(3))], Some("value_array")),
        ];
        let setup = |db: &Path, steps: &[Option<CanonicalValue>], planted: Option<&str>| {
            for step in steps {
                with_write_txn(db, "t", "setup", |conn, c| match step {
                    Some(v) => write_value(conn, c, KEY, v, &mut AxisCache::new(), &opts, None),
                    None => delete_value(conn, c, KEY, &opts),
                })
                .unwrap();
            }
            with_write_txn(db, "t", "other", |conn, c| {
                write_value(conn, c, OTHER, &array, &mut AxisCache::new(), &opts, None)
            })
            .unwrap();
            if let Some(t) = planted {
                plant(db, t);
            }
        };
        for (name, steps, planted) in &states {
            for retained in [true, false] {
                let w = DeleteWrite { key: KEY, retained };
                let other = DeleteWrite {
                    key: OTHER,
                    retained: !retained,
                };
                let (_da, a) = fresh();
                let (_db, b) = fresh();
                setup(&a, steps, *planted);
                setup(&b, steps, *planted);
                let oa = with_write_txn(&a, "t", "d", |conn, c| {
                    delete_values(conn, c, std::slice::from_ref(&w))
                })
                .unwrap();
                let ob =
                    with_write_txn(&b, "t", "d", |conn, c| delete_values(conn, c, &[other, w]))
                        .unwrap();
                let case = format!("{name}, retained={retained}");
                assert_eq!(oa.len(), 1, "{case}");
                assert_eq!(format!("{:?}", oa[0]), format!("{:?}", ob[1]), "{case}");
                assert_eq!(ob[0].rows_closed, 1, "{case}");
                assert_eq!(key_rows(&a), key_rows(&b), "{case}");
            }
        }
    }

    /// A rank-1 float long value over `positions` of a 4-long axis.
    fn sparse_long(positions: &[i32], base: f64) -> CanonicalValue {
        CanonicalValue {
            tier: Tier::Long,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(4)],
            shape: ShapedData::Long(
                positions
                    .iter()
                    .map(|&p| LongRow {
                        positions: vec![p],
                        value: ScalarValue::Float(base + p as f64),
                    })
                    .collect(),
            ),
        }
    }

    /// Every prior state of `KEY` the long and array equivalence tests run
    /// over: per-commit steps (Some writes, None deletes), then an optional
    /// planted row (a damaged, doubly-live key).
    type PriorState = (
        &'static str,
        Vec<Option<CanonicalValue>>,
        Option<&'static str>,
    );

    fn prior_states() -> Vec<PriorState> {
        let scalar = |x: f64| CanonicalValue {
            tier: Tier::Scalar,
            value_kind: ValueKind::Float,
            axes: vec![],
            shape: ShapedData::Scalar(ScalarValue::Float(x)),
        };
        let array = |x: f64| CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(3)],
            shape: ShapedData::Array(vec![x, 2.0, 3.0]),
        };
        vec![
            ("none", vec![], None),
            (
                "scalar live",
                vec![Some(scalar(1.0)), Some(scalar(2.0))],
                None,
            ),
            ("scalar closed", vec![Some(scalar(1.0)), None], None),
            (
                // pos0 v2 dead (stays dead), pos1 v3, pos2 v1 dead, pos3 v1.
                "long diverged",
                vec![
                    Some(sparse_long(&[0, 1, 2], 1.0)),
                    Some(sparse_long(&[0, 1], 2.0)),
                    Some(sparse_long(&[1, 3], 3.0)),
                ],
                None,
            ),
            (
                "long closed",
                vec![Some(sparse_long(&[1, 2], 1.0)), None],
                None,
            ),
            // The long equivalence test's target is sparse_long(&[0, 2, 3],
            // 10.0): equal to "long same", sharing cells 0 and 2 with "long
            // partly same" (1 removed, 3 added), and on another axis in
            // "long axis changed" (P2 cell diff).
            ("long same", vec![Some(sparse_long(&[0, 2, 3], 10.0))], None),
            (
                "long partly same",
                vec![Some(sparse_long(&[0, 1, 2], 10.0))],
                None,
            ),
            (
                "long axis changed",
                vec![Some(CanonicalValue {
                    axes: vec![int_axis(5)],
                    ..sparse_long(&[0, 2, 3], 10.0)
                })],
                None,
            ),
            ("array live", vec![Some(array(1.0))], None),
            (
                "array replaced",
                vec![Some(array(1.0)), Some(array(4.0))],
                None,
            ),
            ("array closed", vec![Some(array(1.0)), None], None),
            (
                "scalar+long",
                vec![Some(sparse_long(&[0, 1], 1.0))],
                Some("value_scalar"),
            ),
            (
                "long+array",
                vec![Some(sparse_long(&[0, 1], 1.0))],
                Some("value_array"),
            ),
            ("scalar+array", vec![Some(scalar(1.0))], Some("value_array")),
        ]
    }

    const OTHER: ValueKey = ValueKey {
        entity_id: 9,
        parameter_definition_id: 2,
        alternative_id: 3,
    };

    /// Build `KEY`'s prior state, plus a value for `OTHER` (`other`).
    fn setup_prior(
        db: &Path,
        steps: &[Option<CanonicalValue>],
        planted: Option<&str>,
        other: &CanonicalValue,
    ) {
        let opts = WriteOptions::default();
        for step in steps {
            with_write_txn(db, "t", "setup", |conn, c| match step {
                Some(v) => write_value(conn, c, KEY, v, &mut AxisCache::new(), &opts, None),
                None => delete_value(conn, c, KEY, &opts),
            })
            .unwrap();
        }
        with_write_txn(db, "t", "other", |conn, c| {
            write_value(conn, c, OTHER, other, &mut AxisCache::new(), &opts, None)
        })
        .unwrap();
        if let Some(t) = planted {
            plant(db, t);
        }
    }

    /// The outcome of one write, as text: the outcome, or the error class
    /// (DuckDB constraint errors name a row, which may differ).
    fn outcome_text(r: &Result<Vec<WriteOutcome>, DbError>, i: usize) -> String {
        match r {
            Ok(o) => format!("{:?}", o[i]),
            Err(DbError::Duckdb(e)) if e.to_string().contains("UNIQUE") => "UNIQUE".into(),
            Err(e) => format!("error {e}"),
        }
    }

    /// The point statements of a one-key long round and the set statements
    /// of a larger round give the same rows and outcome for the same key,
    /// over every prior state (each tier live, closed or replaced, per-cell
    /// versions diverged, and the doubly-live damaged states, where the
    /// per-key precedence decides, so a scalar+long key fails on the long
    /// UNIQUE as before), both retention modes, plus a minted key; and,
    /// with the target bound as the priors were (no binding), the cell diff
    /// of an equal, a partly equal and an other-axis prior (P2).
    #[test]
    fn single_key_long_round_matches_the_set_round() {
        let target = sparse_long(&[0, 2, 3], 10.0);
        let other = sparse_long(&[0, 1], 20.0);
        for (name, steps, planted) in &prior_states() {
            for (retained, pt) in [
                (true, Some(4)),
                (false, Some(4)),
                (true, None),
                (false, None),
            ] {
                for minted in [false, true] {
                    if minted && !steps.is_empty() {
                        continue;
                    }
                    let (_da, a) = fresh();
                    let (_db, b) = fresh();
                    setup_prior(&a, steps, *planted, &other);
                    setup_prior(&b, steps, *planted, &other);
                    let run = |db: &Path, with_other: bool| {
                        with_write_txn(db, "t", "w", |conn, c| {
                            let mut cache = AxisCache::new();
                            let mut w = Vec::new();
                            if with_other {
                                let (ids, cr, re) = resolve_axes(conn, &mut cache, &other.axes)?;
                                w.push(LongWrite {
                                    key: OTHER,
                                    value: Cow::Borrowed(&other),
                                    axis_ids: ids,
                                    axes_created: cr,
                                    axes_reused: re,
                                    parameter_type_id: None,
                                    retained: !retained,
                                    entity_minted: false,
                                });
                            }
                            let (ids, cr, re) = resolve_axes(conn, &mut cache, &target.axes)?;
                            w.push(LongWrite {
                                key: KEY,
                                value: Cow::Borrowed(&target),
                                axis_ids: ids,
                                axes_created: cr,
                                axes_reused: re,
                                parameter_type_id: pt,
                                retained,
                                entity_minted: minted,
                            });
                            write_longs(conn, c, &w)
                        })
                    };
                    let oa = run(&a, false);
                    let ob = run(&b, true);
                    let case = format!("{name}, retained={retained}, pt={pt:?}, minted={minted}");
                    assert_eq!(outcome_text(&oa, 0), outcome_text(&ob, 1), "{case}");
                    assert_eq!(oa.is_ok(), *name != "scalar+long", "{case}");
                    assert_eq!(key_rows(&a), key_rows(&b), "{case}");
                    // The diff itself, on the unbound target.
                    let o = oa.as_ref().ok().map(|o| &o[0]);
                    let diff = |o: &WriteOutcome| {
                        (
                            o.unchanged,
                            o.cells_unchanged,
                            o.rows_written,
                            o.rows_closed,
                        )
                    };
                    match (*name, pt) {
                        ("long same", None) => {
                            assert_eq!(o.map(diff), Some((true, 3, 0, 0)), "{case}")
                        }
                        ("long partly same", None) => {
                            assert_eq!(o.map(diff), Some((false, 2, 1, 1)), "{case}")
                        }
                        ("long same" | "long partly same" | "long axis changed", _) => {
                            assert_eq!(o.map(diff), Some((false, 0, 3, 3)), "{case}")
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn float_array(vals: Vec<f64>) -> CanonicalValue {
        CanonicalValue {
            tier: Tier::Array,
            value_kind: ValueKind::Float,
            axes: vec![int_axis(vals.len().max(1) as i32)],
            shape: ShapedData::Array(vals),
        }
    }

    /// An [`ArrayWrite`] of `v` for `key`, its axes resolved on `conn`.
    fn array_write<'a>(
        conn: &Connection,
        cache: &mut AxisCache,
        key: ValueKey,
        v: &'a CanonicalValue,
        retained: bool,
        minted: bool,
    ) -> Result<ArrayWrite<'a>, DbError> {
        let (ids, cr, re) = resolve_axes(conn, cache, &v.axes)?;
        Ok(ArrayWrite {
            key,
            value: Cow::Borrowed(v),
            axis_ids: ids,
            axes_created: cr,
            axes_reused: re,
            parameter_type_id: Some(4),
            retained,
            entity_minted: minted,
        })
    }

    /// The array counterpart of `single_key_long_round_matches_the_set_round`:
    /// a damaged key live in long and array (or scalar and array) takes the
    /// INSERT by its precedence tier and fails on the UNIQUE triple, in both
    /// forms (critique C3).
    #[test]
    fn single_key_array_round_matches_the_set_round() {
        let nan = f64::from_bits(crate::encoding::CANONICAL_NAN_BITS);
        let target = float_array(vec![nan, 0.0, 2.5]);
        let other = float_array(vec![1.0, 2.0]);
        for (name, steps, planted) in &prior_states() {
            for retained in [true, false] {
                for minted in [false, true] {
                    if minted && !steps.is_empty() {
                        continue;
                    }
                    let (_da, a) = fresh();
                    let (_db, b) = fresh();
                    setup_prior(&a, steps, *planted, &other);
                    setup_prior(&b, steps, *planted, &other);
                    let run = |db: &Path, with_other: bool| {
                        with_write_txn(db, "t", "w", |conn, c| {
                            let mut cache = AxisCache::new();
                            let mut w = Vec::new();
                            if with_other {
                                w.push(array_write(
                                    conn, &mut cache, OTHER, &other, !retained, false,
                                )?);
                            }
                            w.push(array_write(
                                conn, &mut cache, KEY, &target, retained, minted,
                            )?);
                            write_arrays(conn, c, &w)
                        })
                    };
                    let oa = run(&a, false);
                    let ob = run(&b, true);
                    let case = format!("{name}, retained={retained}, minted={minted}");
                    assert_eq!(outcome_text(&oa, 0), outcome_text(&ob, 1), "{case}");
                    let fails = matches!(*name, "long+array" | "scalar+array");
                    assert_eq!(oa.is_err(), fails, "{case}");
                    assert_eq!(key_rows(&a), key_rows(&b), "{case}");
                }
            }
        }
    }

    /// Every row of every value table (array ids included) and the
    /// sequences, as text.
    fn all_rows(db: &Path) -> Vec<String> {
        let conn = Connection::open(db).unwrap();
        let mut out = Vec::new();
        for sql in [
            "SELECT CAST(t AS VARCHAR) FROM (SELECT * FROM value_array ORDER BY ALL) t",
            "SELECT CAST(t AS VARCHAR) FROM (SELECT * FROM value_array_hist ORDER BY ALL) t",
            "SELECT CAST(t AS VARCHAR) FROM (SELECT sequence_name, last_value \
             FROM duckdb_sequences() ORDER BY ALL) t",
        ] {
            let mut stmt = conn.prepare(sql).unwrap();
            out.extend(
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
            );
        }
        conn.close().unwrap();
        out
    }

    /// A nested-list round with an empty array, a one-element array, a NaN
    /// and new and replaced keys writes the same rows, `array_id`s and
    /// sequence state as one per-key write per key in the same order: the
    /// pre-minted ids are those sequential inserts mint (critique C3, C8).
    #[test]
    fn array_round_matches_per_key_writes() {
        let nan = f64::from_bits(crate::encoding::CANONICAL_NAN_BITS);
        let vals = [
            float_array(vec![]),
            float_array(vec![1.5]),
            float_array(vec![nan, 0.0, -1.0]),
            float_array(vec![4.0, 5.0]),
            float_array(vec![]),
        ];
        let key = |e: i64| ValueKey {
            entity_id: e,
            parameter_definition_id: 2,
            alternative_id: 3,
        };
        let (_da, a) = fresh();
        let (_db, b) = fresh();
        for db in [&a, &b] {
            // Keys 2 and 4 already hold arrays (replaced below); key 7 holds
            // an unrelated array that bumps the sequence.
            with_write_txn(db, "t", "seed", |conn, c| {
                let mut cache = AxisCache::new();
                let o = WriteOptions::default();
                for e in [7, 2, 4] {
                    write_value(conn, c, key(e), &vals[3], &mut cache, &o, None)?;
                }
                Ok(())
            })
            .unwrap();
        }
        let order = [5, 2, 1, 4, 3];
        with_write_txn(&a, "t", "set", |conn, c| {
            let mut cache = AxisCache::new();
            let w = order
                .iter()
                .zip(&vals)
                .map(|(&e, v)| array_write(conn, &mut cache, key(e), v, e != 4, e == 5))
                .collect::<Result<Vec<_>, _>>()?;
            write_arrays(conn, c, &w)
        })
        .unwrap();
        with_write_txn(&b, "t", "per key", |conn, c| {
            let mut cache = AxisCache::new();
            let mut out = Vec::new();
            for (&e, v) in order.iter().zip(&vals) {
                let w = array_write(conn, &mut cache, key(e), v, e != 4, e == 5)?;
                out.extend(write_arrays(conn, c, &[w])?);
            }
            Ok(out)
        })
        .unwrap();
        assert_eq!(all_rows(&a), all_rows(&b));
    }

    #[test]
    fn array_batch_with_a_duplicate_key_is_an_internal_error() {
        let (_d, db) = fresh();
        let before = counts(&db);
        let v = float_array(vec![1.0]);
        let err = with_write_txn(&db, "t", "dup", |conn, c| {
            let w = array_write(conn, &mut AxisCache::new(), KEY, &v, true, false)?;
            write_arrays(conn, c, &[w.clone(), w]).map(|_| ())
        })
        .expect_err("a duplicate key must be rejected");
        assert!(
            matches!(&err, DbError::Internal(m) if m.contains("duplicate key")),
            "got {err:?}"
        );
        assert_eq!(counts(&db), before);
    }

    #[test]
    fn long_batch_with_a_duplicate_key_is_an_internal_error() {
        let (_d, db) = fresh();
        let before = counts(&db);
        let v = long_value(2);
        let err = with_write_txn(&db, "t", "dup", |conn, c| {
            let (ids, cr, re) = resolve_axes(conn, &mut AxisCache::new(), &v.axes)?;
            let w = LongWrite {
                key: KEY,
                value: Cow::Borrowed(&v),
                axis_ids: ids,
                axes_created: cr,
                axes_reused: re,
                parameter_type_id: None,
                retained: true,
                entity_minted: false,
            };
            write_longs(conn, c, &[w.clone(), w]).map(|_| ())
        })
        .expect_err("a duplicate key must be rejected");
        assert!(
            matches!(&err, DbError::Internal(m) if m.contains("duplicate key")),
            "got {err:?}"
        );
        assert_eq!(counts(&db), before);
    }

    #[test]
    fn delete_batch_with_a_duplicate_key_is_an_internal_error() {
        let (_d, db) = fresh();
        let before = counts(&db);
        let w = DeleteWrite {
            key: KEY,
            retained: true,
        };
        let err = with_write_txn(&db, "t", "dup", |conn, c| {
            delete_values(conn, c, &[w, w]).map(|_| ())
        })
        .expect_err("a duplicate key must be rejected");
        assert!(
            matches!(&err, DbError::Internal(m) if m.contains("duplicate key")),
            "got {err:?}"
        );
        assert_eq!(counts(&db), before);
    }

    #[test]
    fn scalar_batch_with_a_duplicate_key_is_an_internal_error() {
        // Callers guarantee distinct keys; the set-based writer checks the
        // invariant (plan D10) instead of writing a zero-length span.
        let (_d, db) = fresh();
        let before = counts(&db);
        let w = ScalarWrite {
            key: KEY,
            value: ScalarValue::Float(1.0),
            parameter_type_id: None,
            retained: true,
            entity_minted: false,
        };
        let err = with_write_txn(&db, "t", "dup", |conn, c| {
            write_scalars(conn, c, &[w.clone(), w.clone()]).map(|_| ())
        })
        .expect_err("a duplicate key must be rejected");
        assert!(
            matches!(&err, DbError::Internal(m) if m.contains("duplicate key")),
            "got {err:?}"
        );
        assert_eq!(counts(&db), before);
    }

    #[test]
    fn scalar_round_length_check_rejects_a_short_column() {
        let mut cols = ScalarColumns::default();
        let w = ScalarWrite {
            key: KEY,
            value: ScalarValue::Int(3),
            parameter_type_id: Some(1),
            retained: true,
            entity_minted: true,
        };
        cols.push(&w, 1);
        cols.push(&w, 2);
        assert!(cols.check_column_lengths().is_ok());
        cols.i.pop();
        assert!(matches!(
            cols.check_column_lengths(),
            Err(DbError::Internal(_))
        ));
    }

    #[test]
    fn second_current_long_cell_violates_unique() {
        let (_d, db) = fresh();
        assert_rejected(&db, "UNIQUE constraint violation", |conn, c| {
            let ax = axis_id(conn, 3);
            let (keys, cols) = long_round(&[ax], 3);
            insert_long_round(conn, c, &keys, &cols)?;
            insert_long_round(conn, c, &keys, &cols)
        });
    }

    #[test]
    fn bad_axis_on_long_violates_foreign_key() {
        let (_d, db) = fresh();
        assert_rejected(&db, "foreign key", |conn, c| {
            let (keys, cols) = long_round(&[999_999], 2);
            insert_long_round(conn, c, &keys, &cols)
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
        // valid_from_commit: a zero-length span. (The second value differs:
        // an identical one is an unchanged write, which closes nothing.)
        let (_d, db) = fresh();
        assert_rejected(&db, "CHECK constraint failed", |conn, c| {
            let mut cache = AxisCache::new();
            let o = WriteOptions::default();
            write_value(conn, c, KEY, &long_value(3), &mut cache, &o, None)?;
            write_value(conn, c, KEY, &long_value(4), &mut cache, &o, None).map(|_| ())
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

    fn period_axis() -> CanonicalAxis {
        CanonicalAxis {
            length: 2,
            properties: AxisProperties::default(),
            body: AxisBody::Period {
                positions: vec![
                    crate::encoding::PeriodPos {
                        start_ts: 1_577_836_800_000_000, // 2020-01-01
                        end_ts: 1_609_459_200_000_000,   // 2021-01-01
                        years_presented: None,
                    },
                    crate::encoding::PeriodPos {
                        start_ts: 1_609_459_200_000_000,
                        end_ts: 1_640_995_200_000_000, // 2022-01-01
                        years_presented: Some(1.0),
                    },
                ],
            },
        }
    }

    #[test]
    fn period_accelerator_is_the_period_start() {
        let ax = period_axis();
        assert_eq!(accel(&ax, 0), (Some(1_577_836_800_000_000), None));
        assert_eq!(accel(&ax, 1), (Some(1_609_459_200_000_000), None));
        assert_eq!(accel(&ax, 2), (None, None));
    }

    #[test]
    fn hand_built_long_period_value_is_accelerator_audit_clean() {
        // Canon does not emit long period dimensions; a hand-built value must
        // still satisfy `audit_index_accelerator` (iN_dt = period start).
        let (_d, db) = fresh();
        let value = CanonicalValue {
            tier: Tier::Long,
            value_kind: ValueKind::Float,
            axes: vec![period_axis()],
            shape: ShapedData::Long(rows(2)),
        };
        with_write_txn(&db, "t", "period", |conn, c| {
            let mut cache = AxisCache::new();
            write_value(
                conn,
                c,
                KEY,
                &value,
                &mut cache,
                &WriteOptions::default(),
                None,
            )
            .map(|_| ())
        })
        .unwrap();
        let conn = Connection::open(&db).unwrap();
        let (cells, bad): (i64, i64) = conn
            .query_row(
                "SELECT (SELECT count(*) FROM value_long WHERE i1_dt IS NOT NULL), \
                        (SELECT count(*) FROM audit_index_accelerator)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((cells, bad), (2, 0));
    }
}
