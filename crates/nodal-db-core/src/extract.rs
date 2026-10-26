//! History-free extract: build a NEW database file that holds exactly the
//! content valid at one commit H of a source file, without the superseded
//! history. The source is never written; it stays the complete history and
//! acts as the archive.
//!
//! > validate (dest absent, no stale `<dest>.wal`, dest ≠ src, one source
//! > identity) → `init_schema` a temp next to dest → `ATTACH` the source
//! > read-only → copy each base table's selection (by [`Selection`] class) in
//! > one transaction → restore the source's sequence state → self-check →
//! > write the extract commit E, the new `db_identity` row and a
//! > `kind='horizon'` erasure marker → CHECKPOINT → no-clobber publish.
//!
//! ## What the extract holds
//! - the `commit` log up to H verbatim, plus E (E > every source commit);
//! - every versioned row valid at H, verbatim (as of a past commit N, the
//!   rows valid at N with `valid_to_commit` set to NULL: their close-out
//!   edges point at commits outside the extract's lineage);
//! - `value_array`: the live rows (as of N: plus the `_hist` rows valid at N,
//!   promoted to live with their `array_id`); `value_array_hist` is empty;
//! - only the axes the carried value rows reference, with their children;
//! - all prior erasure markers and `run` rows (as of N: those that existed
//!   at N), plus the "trace closure": the latest version row of every
//!   alternative or entity a carried run or marker references, so the
//!   identity rows such trace rows point at never vanish;
//! - the source's metadata minus its `db_identity` row; the extract's own,
//!   freshly minted identity is written at E.
//!
//! The horizon marker (`horizon_commit` = H, `at_commit` = E,
//! `continuation_db` = the source identity) is the trace of what was left
//! out. With the as-of guard in `110_views_history.sql`, every as-of read at
//! a commit before E fails loudly on the extract; reads at E or later are
//! exact. `audit_history_tiling` is horizon-aware, so the surviving version
//! counters (kept verbatim, never renumbered) audit clean.
//!
//! ## Invariants
//! - **Invariant 2:** the extract computes no encodings, hashes or axis
//!   dedup. It copies canonical bytes and writes three bookkeeping rows.
//! - **Invariant 3:** nothing is removed from the source, and nothing in the
//!   extract's own history is rewritten: its lineage is "source ≤ H, then
//!   E", and the marker records what it omits. The file is invisible until
//!   that marker exists (the publish is the atomic point).
//! - **Lock discipline:** the per-path locks of source and dest are held for
//!   the whole operation (in-process exclusion), and the read-only ATTACH
//!   holds the source's file lock through the copy (cross-process exclusion:
//!   a concurrent writer sees `Busy` for the duration, as with compaction).
//! - **Table macros are never called through the attached catalog.** A
//!   table macro reached through the attached catalog
//!   (`nodal_rebuild_source.some_macro(…)`) binds its body's unqualified
//!   names in the caller's default catalog and silently reads the wrong
//!   database (pinned by a tripwire test). The derived-view self-check
//!   evaluates both sides through this build's definitions: the source
//!   under `USE <SRC>` with TEMP copies of the views, dropped before the
//!   extract side is read (`self_check_derived_views`).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use duckdb::{params, AccessMode, Connection};

use crate::compaction::{write_baseline, SCHEMA_VERSION};
use crate::conn::{canonical_path_key, open_with_retry, path_lock};
use crate::error::DbError;
use crate::rebuild::{
    assert_temp_schema_version, attach_source_ro, check_catalog, check_selection, cluster_order,
    discover_real_columns, fsync_dir, fsync_file, parent_dir, q, remove_temp, restore_sequences,
    selection_for, Selection, SRC, TABLE_ORDER,
};
use crate::schema_init::init_schema;

/// Options for [`extract`].
#[derive(Clone, Copy, Debug, Default)]
pub struct ExtractOpts<'a> {
    /// `None` = extract the source's current state (head). `Some(n)` =
    /// extract the state as it was right after commit `n`.
    pub as_of: Option<i64>,
    /// Author of the extract commit E (must be non-empty).
    pub user: &'a str,
    /// Free-form operator context, stored on the horizon marker.
    pub note: Option<&'a str>,
}

/// What one [`extract`] did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExtractReport {
    /// The source's `db_identity` (recorded as the marker's `continuation_db`).
    pub source_identity: String,
    /// The extract's own, freshly minted `db_identity`.
    pub extract_identity: String,
    /// H: the source commit whose state the extract holds.
    pub horizon_commit: i64,
    /// E: the extract's own commit (carries the marker and the identity row).
    pub extract_commit: i64,
    /// The new `kind='horizon'` erasure marker's id.
    pub marker_id: i64,
    /// Rows carried per base table (before E's three bookkeeping rows).
    pub rows_per_table: Vec<(String, u64)>,
    /// Source rows not carried, over all base tables (superseded history,
    /// unreferenced axes, and as of N also everything after N).
    pub rows_removed: u64,
    /// Source axes not carried (unreferenced by the carried value rows).
    pub axes_dropped: u64,
    /// Source file size in bytes.
    pub bytes_source: u64,
    /// Extract file size in bytes.
    pub bytes_extract: u64,
    /// Set when part of the self-check could not run: the derived-view
    /// comparison is skipped when this build's views do not bind on the
    /// source (the table-level check of every carried row still ran).
    pub warning: Option<String>,
}

/// The derived views the head-mode self-check compares between the source
/// and the extract. `*_current` views are left out on purpose: they apply the
/// same predicate as the copy, so comparing them would prove nothing. These
/// views JOIN across tables (resolution, membership and activation,
/// effective units, the tier-spanning key set), which is where a missing
/// row would show. The cell-exploding views (`value_rows`, `value_cells`,
/// `value_array_cells`) stay in the test suite: unnesting every array twice
/// would roughly double the extract's run time at results scale.
const DERIVED_VIEWS: &[&str] = &[
    "value_keys",
    "scenario_value_candidate",
    "scenario_value_resolved",
    "scenario_value_conflict",
    "membership_resolved",
    "membership_flat",
    "membership_base_flat",
    "realizes_edge",
    "entity_activation_own",
    "deact_edge",
    "entity_activation_resolved",
    "parameter_effective_unit",
];

/// Trace-closure targets: identity tables whose ids the append-only trace
/// tables reference, with the referencing (table, column) pairs.
const TRACE_REFS: &[(&str, &[(&str, &str)])] = &[
    (
        "alternative",
        &[
            ("run", "result_alternative_id"),
            ("erasure_marker", "alternative_id"),
        ],
    ),
    (
        "entity",
        &[("run", "tool_entity_id"), ("erasure_marker", "entity_id")],
    ),
];

/// Extract the content of `src` valid at its head (or at `opts.as_of`) into a
/// new database file at `dest`. See the module docs. `src` is only read.
///
/// Refuses (touching nothing) when `dest` exists, when `<dest>.wal` exists,
/// when `dest` is `src` (also through a symlink or relative alias), when the
/// source's schema version differs from this build's, or when the source has
/// not exactly one current `db_identity` row. As of N it also refuses when N
/// is not a commit, when the `schema_version` valid at N differs, when an
/// erasure marker was written after N, or when content valid at N was not
/// retained (a history stub).
pub fn extract(src: &Path, dest: &Path, opts: &ExtractOpts) -> Result<ExtractReport, DbError> {
    if opts.user.trim().is_empty() {
        return Err(DbError::Api("extract: user must be non-empty".into()));
    }
    if !src.is_file() {
        return Err(DbError::Api(format!(
            "no database file at {:?} (extract needs an existing source)",
            src.display().to_string()
        )));
    }
    let src_key = canonical_path_key(src);
    let dest_key = canonical_path_key(dest);
    if src_key == dest_key {
        return Err(DbError::Api(format!(
            "extract refused: destination {:?} is the source itself (an extract \
             always writes a new file)",
            dest.display().to_string()
        )));
    }

    // In-process exclusion for both paths, taken in a fixed order so two
    // concurrent extracts can never deadlock on each other.
    let (first, second) = if src_key < dest_key {
        (path_lock(src), path_lock(dest))
    } else {
        (path_lock(dest), path_lock(src))
    };
    let _g1 = first.lock().unwrap_or_else(|p| p.into_inner());
    let _g2 = second.lock().unwrap_or_else(|p| p.into_inner());

    check_dest_free(dest)?;
    sweep_orphan_extract_temps(dest);

    let bytes_source = fs::metadata(src).map(|m| m.len()).unwrap_or(0);
    let temp = temp_path_for(dest);
    init_schema(&temp)?;
    let conn = match open_with_retry(&temp, AccessMode::ReadWrite) {
        Ok(c) => c,
        Err(e) => {
            remove_temp(&temp);
            return Err(e);
        }
    };
    let built = build_into(&conn, src, opts);
    let mut report = match built {
        Ok(r) => r,
        Err(e) => {
            let _ = conn.close();
            remove_temp(&temp);
            return Err(e);
        }
    };
    if let Err((_, e)) = conn.close() {
        remove_temp(&temp);
        return Err(DbError::Duckdb(e));
    }

    // Durability, then the no-clobber publish (the atomic point: nothing
    // could see the temp before this).
    let published = (|| {
        fsync_file(&temp)?;
        check_dest_free(dest)?;
        publish_no_clobber(&temp, dest)
    })();
    if let Err(e) = published {
        remove_temp(&temp);
        return Err(e);
    }
    remove_temp(&temp);
    fsync_dir(parent_dir(dest));

    report.bytes_source = bytes_source;
    report.bytes_extract = fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
    // The extract is freshly packed: its size and its head (the extract's
    // own commit E) are the compaction baseline (save-path plan C8 ii).
    write_baseline(dest, report.bytes_extract, report.extract_commit);
    Ok(report)
}

/// Refuse an existing destination, or a stale `<dest>.wal` (which the first
/// read-write open of the new file would replay onto it). Never deletes user
/// files.
fn check_dest_free(dest: &Path) -> Result<(), DbError> {
    if fs::symlink_metadata(dest).is_ok() {
        return Err(DbError::Api(format!(
            "extract refused: destination {:?} already exists (remove it first; an \
             extract never overwrites a file)",
            dest.display().to_string()
        )));
    }
    let wal = wal_path(dest);
    if fs::symlink_metadata(&wal).is_ok() {
        return Err(DbError::Api(format!(
            "extract refused: a stale write-ahead log {:?} exists for the destination; \
             it would be replayed onto the new file (remove it first)",
            wal.display().to_string()
        )));
    }
    Ok(())
}

/// Publish `temp` as `dest` without ever replacing an existing `dest`:
/// `fs::rename` silently overwrites, a hard link does not. On a filesystem
/// without hard links this fails loudly and leaves no dest.
fn publish_no_clobber(temp: &Path, dest: &Path) -> Result<(), DbError> {
    match fs::hard_link(temp, dest) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(DbError::Api(format!(
            "extract refused: destination {:?} appeared while the extract ran; it was \
             left untouched",
            dest.display().to_string()
        ))),
        Err(e) => Err(DbError::Api(format!(
            "extract: could not publish {:?} as {:?}: {e}",
            temp.display().to_string(),
            dest.display().to_string()
        ))),
    }
}

/// Selection mode: the source's current state, or its state at commit N.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Head,
    AsOf(i64),
}

/// Everything after the temp exists: catalog checks, ATTACH, preflight, copy,
/// sequence restore, self-check, E. Runs on the temp's RW connection; any
/// error aborts (the caller removes the temp; the source was only read).
fn build_into(conn: &Connection, src: &Path, opts: &ExtractOpts) -> Result<ExtractReport, DbError> {
    assert_temp_schema_version(conn)?;
    let extract_identity: String = conn.query_row(
        "SELECT value FROM metadata \
         WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL",
        [],
        |r| r.get(0),
    )?;
    let real_cols = discover_real_columns(conn)?;
    check_catalog(conn, &real_cols)?;
    check_selection(&real_cols)?;
    // Wipe the temp's bootstrap rows (metadata before commit: FK order).
    conn.execute_batch("DELETE FROM metadata; DELETE FROM \"commit\";")?;

    attach_source_ro(conn, src)?;

    let source_identity = preflight_source(conn)?;
    let src_max: i64 =
        conn.query_row(&format!("SELECT max(id) FROM {SRC}.\"commit\""), [], |r| {
            r.get(0)
        })?;
    let mode = match opts.as_of {
        None => Mode::Head,
        Some(n) => Mode::AsOf(n),
    };
    let h = match mode {
        Mode::Head => src_max,
        Mode::AsOf(n) => {
            preflight_as_of(conn, n, &real_cols)?;
            n
        }
    };
    let sel = Sel {
        mode,
        h,
        real_cols: &real_cols,
    };

    // ---- the copy: one transaction, FK order, clustered -------------------
    conn.execute_batch("BEGIN")?;
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE _extract_axes AS {}",
        sel.axis_closure()?
    ))?;
    for table in TABLE_ORDER {
        let cols = sel.cols(table);
        let list = list(cols);
        let order = cluster_order(table, cols)?;
        conn.execute_batch(&format!(
            "INSERT INTO {t} ({list}) SELECT * FROM ({s}) s{order}",
            t = q(table),
            s = sel.select(table)?,
        ))?;
    }
    conn.execute_batch("COMMIT")?;

    restore_sequences(conn)?;

    // ---- self-check (before E, so E's rows are not in the comparison) ----
    let rows_per_table = self_check_selection(conn, &sel)?;
    let warning = if mode == Mode::Head {
        self_check_derived_views(conn)?
    } else {
        None
    };
    self_check_axes(conn, &real_cols)?;

    // ---- E, the new identity row, the horizon marker ----------------------
    let src_rows: u64 = TABLE_ORDER
        .iter()
        .map(|t| count(conn, &format!("SELECT count(*) FROM {SRC}.{}", q(t))))
        .sum::<Result<u64, DbError>>()?;
    let carried: u64 = rows_per_table.iter().map(|(_, n)| n).sum();
    let rows_removed = src_rows.saturating_sub(carried);
    let axes_src = count(conn, &format!("SELECT count(*) FROM {SRC}.axis"))?;
    let axes_kept = count(conn, "SELECT count(*) FROM axis")?;
    let cells = "coalesce(len(\"values\"), len(values_int), 0)";
    let cells_src = count(
        conn,
        &format!(
            "SELECT CAST((SELECT coalesce(sum({cells}), 0) FROM {SRC}.value_array) \
                  + (SELECT coalesce(sum({cells}), 0) FROM {SRC}.value_array_hist) \
                  + (SELECT coalesce(sum({cells}), 0) FROM {SRC}.definition_value_array) \
                  AS BIGINT)"
        ),
    )?;
    let cells_kept = count(
        conn,
        &format!(
            "SELECT CAST((SELECT coalesce(sum({cells}), 0) FROM value_array) \
                  + (SELECT coalesce(sum({cells}), 0) FROM definition_value_array) AS BIGINT)"
        ),
    )?;
    let bytes_estimate = cells_src.saturating_sub(cells_kept) * 8;

    let (message, note) = match mode {
        Mode::Head => (
            format!("history-free extract of {source_identity} at commit {h}"),
            opts.note.map(str::to_string),
        ),
        Mode::AsOf(n) => (
            format!("history-free extract of {source_identity} as of commit {n}"),
            Some(match opts.note {
                Some(t) => format!("as-of {n} extract; {t}"),
                None => format!("as-of {n} extract"),
            }),
        ),
    };

    conn.execute_batch("BEGIN")?;
    let written = (|| -> Result<(i64, i64), DbError> {
        let e: i64 = conn.query_row(
            "INSERT INTO \"commit\" (user, message) VALUES (?, ?) RETURNING id",
            params![opts.user, message],
            |r| r.get(0),
        )?;
        if e <= src_max {
            return Err(DbError::Api(format!(
                "internal: extract commit {e} is not above the source's last commit \
                 {src_max} (sequence restore failed)"
            )));
        }
        conn.execute(
            "INSERT INTO metadata (id, name, value, valid_from_commit) \
             VALUES (nextval('seq_metadata'), 'db_identity', ?, ?)",
            params![extract_identity, e],
        )?;
        let marker: i64 = conn.query_row(
            "INSERT INTO erasure_marker \
               (kind, horizon_commit, at_commit, continuation_db, rows_removed, \
                bytes_estimate, note) \
             VALUES ('horizon', ?, ?, ?, ?, ?, ?) RETURNING id",
            params![
                h,
                e,
                source_identity,
                rows_removed as i64,
                bytes_estimate as i64,
                note
            ],
            |r| r.get(0),
        )?;
        Ok((e, marker))
    })();
    let (extract_commit, marker_id) = match written {
        Ok(v) => {
            conn.execute_batch("COMMIT")?;
            v
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(e);
        }
    };

    conn.execute_batch(&format!(
        "DROP TABLE temp._extract_axes; DETACH {SRC}; CHECKPOINT;"
    ))?;

    Ok(ExtractReport {
        source_identity,
        extract_identity,
        horizon_commit: h,
        extract_commit,
        marker_id,
        rows_per_table,
        rows_removed,
        axes_dropped: axes_src.saturating_sub(axes_kept),
        bytes_source: 0,
        bytes_extract: 0,
        warning,
    })
}

/// Source-side refusals common to both modes: the schema version must be this
/// build's (named-column copies against a differently shaped source would
/// misfill), and there must be exactly one current `db_identity` (else the
/// marker's `continuation_db` would be NULL, which reads as "truncated", or
/// ambiguous). Returns the source identity.
fn preflight_source(conn: &Connection) -> Result<String, DbError> {
    let versions: Vec<String> = strings(
        conn,
        &format!(
            "SELECT value FROM {SRC}.metadata \
             WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL"
        ),
    )
    .map_err(|e| {
        DbError::Api(format!(
            "extract refused: the source does not look like a nodal-db database ({e})"
        ))
    })?;
    if versions.len() != 1 || versions[0] != SCHEMA_VERSION {
        return Err(DbError::SchemaMismatch {
            message: format!(
                "extract refused: source schema_version {versions:?} != current \
                 {SCHEMA_VERSION:?} (to migrate, re-init a fresh database and re-import)"
            ),
            found: found_version(versions),
            expected: SCHEMA_VERSION.to_string(),
        });
    }
    let ids = strings(
        conn,
        &format!(
            "SELECT value FROM {SRC}.metadata \
             WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL"
        ),
    )?;
    if ids.len() != 1 {
        return Err(DbError::Api(format!(
            "extract refused: the source has {} current db_identity rows (exactly one \
             is required; the extract records it as the archive's identity)",
            ids.len()
        )));
    }
    Ok(ids.into_iter().next().expect("one identity"))
}

/// The `found` of a [`DbError::SchemaMismatch`] from the `schema_version`
/// rows read: the one value, or the rendered list when there are none or
/// several (a damaged file).
fn found_version(versions: Vec<String>) -> String {
    match <[String; 1]>::try_from(versions) {
        Ok([v]) => v,
        Err(vs) => format!("{vs:?}"),
    }
}

/// As-of-N refusals, each loud and named:
/// - N is not a commit id of the source;
/// - the `schema_version` row valid at N is not this build's (a belt: no
///   in-place schema bump path exists, but a hand-edited file could carry
///   one, and it is the row the extract would copy);
/// - an erasure marker was written after N (content valid at N was later
///   removed; the same condition the as-of guard refuses on);
/// - a row valid at N is a history stub (`content_retained = FALSE`): its
///   content at N was not kept, and a promoted array stub would violate
///   `value_array`'s payload CHECK.
fn preflight_as_of(
    conn: &Connection,
    n: i64,
    real_cols: &[(String, Vec<String>)],
) -> Result<(), DbError> {
    if count(
        conn,
        &format!("SELECT count(*) FROM {SRC}.\"commit\" WHERE id = {n}"),
    )? != 1
    {
        return Err(DbError::Api(format!(
            "as-of extract refused: {n} is not a commit id of the source"
        )));
    }
    let versions = strings(
        conn,
        &format!(
            "SELECT value FROM {SRC}.metadata WHERE lower(name) = 'schema_version' \
             AND valid_from_commit <= {n} AND (valid_to_commit IS NULL OR valid_to_commit > {n})"
        ),
    )?;
    if versions.len() != 1 || versions[0] != SCHEMA_VERSION {
        return Err(DbError::SchemaMismatch {
            message: format!(
                "as-of extract refused: the schema_version valid at commit {n} is \
                 {versions:?}, not the current {SCHEMA_VERSION:?}"
            ),
            found: found_version(versions),
            expected: SCHEMA_VERSION.to_string(),
        });
    }
    let later = strings(
        conn,
        &format!(
            "SELECT CAST(at_commit AS VARCHAR) FROM {SRC}.erasure_marker \
             WHERE at_commit > {n} ORDER BY at_commit"
        ),
    )?;
    if !later.is_empty() {
        return Err(DbError::Api(format!(
            "as-of extract refused: content valid at commit {n} was erased later (erasure \
             marker(s) at commit {}); the state at {n} cannot be reconstructed",
            later.join(", ")
        )));
    }
    let mut legs = Vec::new();
    for (table, cols) in real_cols {
        if !cols.iter().any(|c| c == "content_retained") {
            continue;
        }
        let at_n = if selection_for(table)? == Selection::ArrayHist {
            format!("valid_from_commit <= {n} AND valid_to_commit > {n}")
        } else {
            format!(
                "valid_from_commit <= {n} AND (valid_to_commit IS NULL OR valid_to_commit > {n})"
            )
        };
        legs.push(format!(
            "SELECT parameter_definition_id FROM {SRC}.{} WHERE {at_n} AND NOT content_retained",
            q(table)
        ));
    }
    let stubbed = strings(
        conn,
        &format!(
            "SELECT DISTINCT ec.name || '.' || pd.name FROM ({}) s \
             JOIN {SRC}.parameter_definition pd ON pd.id = s.parameter_definition_id \
             JOIN {SRC}.entity_class ec ON ec.id = pd.entity_class_id \
             QUALIFY row_number() OVER (PARTITION BY s.parameter_definition_id \
                                        ORDER BY pd.version DESC, ec.version DESC) = 1 \
             ORDER BY 1",
            legs.join(" UNION ALL ")
        ),
    )?;
    if !stubbed.is_empty() {
        return Err(DbError::Api(format!(
            "as-of extract refused: content at commit {n} was not retained for \
             parameter(s) {} (history stubs)",
            stubbed.join(", ")
        )));
    }
    Ok(())
}

/// The per-table selection SQL generator (one place for both modes, used by
/// both the copy and the self-check).
struct Sel<'a> {
    mode: Mode,
    /// The horizon: the source head, or N.
    h: i64,
    real_cols: &'a [(String, Vec<String>)],
}

impl Sel<'_> {
    fn cols(&self, table: &str) -> &[String] {
        self.real_cols
            .iter()
            .find(|(t, _)| t == table)
            .map(|(_, c)| c.as_slice())
            .expect("every TABLE_ORDER entry is discovered")
    }

    /// "Valid at the horizon" for alias `a`.
    fn live(&self, a: &str) -> String {
        match self.mode {
            Mode::Head => format!("{a}.\"valid_to_commit\" IS NULL"),
            Mode::AsOf(n) => format!(
                "{a}.\"valid_from_commit\" <= {n} AND \
                 ({a}.\"valid_to_commit\" IS NULL OR {a}.\"valid_to_commit\" > {n})"
            ),
        }
    }

    /// The column list for a versioned table's main leg: as of N, the
    /// close-out edge is NULLed (it points outside the extract's lineage).
    fn versioned_list(&self, table: &str) -> String {
        let cols = self.cols(table);
        match self.mode {
            Mode::Head => list(cols),
            Mode::AsOf(_) => cols
                .iter()
                .map(|c| {
                    if c == "valid_to_commit" {
                        "NULL::BIGINT AS \"valid_to_commit\"".to_string()
                    } else {
                        q(c)
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    /// The full SELECT producing exactly the rows to carry for `table`, with
    /// the table's real columns in catalog order.
    fn select(&self, table: &str) -> Result<String, DbError> {
        let t = q(table);
        let plain = list(self.cols(table));
        let h = self.h;
        Ok(match selection_for(table)? {
            Selection::CommitLog => format!("SELECT {plain} FROM {SRC}.{t} t WHERE t.\"id\" <= {h}"),
            Selection::Trace if table == "run" => match self.mode {
                Mode::Head => format!("SELECT {plain} FROM {SRC}.{t} t"),
                // X14: a run whose outputs landed after N did not exist at N.
                // `run` has no creation edge, so this is an approximation,
                // narrowed by requiring its result alternative and tool entity
                // to have existed by N.
                Mode::AsOf(n) => format!(
                    "SELECT {plain} FROM {SRC}.{t} t \
                     WHERE t.\"input_commit_id\" <= {n} \
                       AND (t.\"produced_at_commit_id\" IS NULL OR t.\"produced_at_commit_id\" <= {n}) \
                       AND EXISTS (SELECT 1 FROM {SRC}.\"alternative\" a \
                                   WHERE a.\"id\" = t.\"result_alternative_id\" \
                                     AND a.\"valid_from_commit\" <= {n}) \
                       AND EXISTS (SELECT 1 FROM {SRC}.\"entity\" x \
                                   WHERE x.\"id\" = t.\"tool_entity_id\" \
                                     AND x.\"valid_from_commit\" <= {n})"
                ),
            },
            Selection::Trace => format!("SELECT {plain} FROM {SRC}.{t} t WHERE t.\"at_commit\" <= {h}"),
            Selection::Versioned => {
                let main_list = self.versioned_list(table);
                let live = self.live("t");
                // The source identity is replaced by the extract's own at E.
                let extra = if table == "metadata" {
                    " AND lower(t.\"name\") <> 'db_identity'"
                } else {
                    ""
                };
                let mut s = format!("SELECT {main_list} FROM {SRC}.{t} t WHERE {live}{extra}");
                if let Some(refs) = self.trace_refs(table)? {
                    let live_x = self.live("x");
                    s.push_str(&format!(
                        " UNION ALL SELECT {plain} FROM {SRC}.{t} t \
                         WHERE t.\"id\" IN ({refs}) AND t.\"valid_from_commit\" <= {h} \
                           AND NOT EXISTS (SELECT 1 FROM {SRC}.{t} x \
                                           WHERE x.\"id\" = t.\"id\" AND {live_x}) \
                         QUALIFY row_number() OVER \
                           (PARTITION BY t.\"id\" ORDER BY t.\"version\" DESC) = 1"
                    ));
                }
                s
            }
            Selection::ArrayLive => match self.mode {
                Mode::Head => format!("SELECT {plain} FROM {SRC}.{t} t"),
                // Promotion: the hist row valid at N becomes the live row,
                // keeping its array_id (exactly one row per key is valid at N).
                Mode::AsOf(n) => format!(
                    "SELECT {plain} FROM {SRC}.{t} t WHERE t.\"valid_from_commit\" <= {n} \
                     UNION ALL SELECT {plain} FROM {SRC}.\"value_array_hist\" t \
                     WHERE t.\"valid_from_commit\" <= {n} AND t.\"valid_to_commit\" > {n}"
                ),
            },
            Selection::ArrayHist => format!("SELECT {plain} FROM {SRC}.{t} t WHERE FALSE"),
            Selection::Axis => format!(
                "SELECT {plain} FROM {SRC}.{t} t \
                 WHERE t.\"id\" IN (SELECT axis_id FROM temp._extract_axes)"
            ),
            Selection::AxisChild => format!(
                "SELECT {plain} FROM {SRC}.{t} t \
                 WHERE t.\"axis_id\" IN (SELECT axis_id FROM temp._extract_axes)"
            ),
        })
    }

    /// The ids of `table` that carried trace rows reference (trace closure),
    /// or `None` if `table` is not a closure target.
    fn trace_refs(&self, table: &str) -> Result<Option<String>, DbError> {
        let Some((_, refs)) = TRACE_REFS.iter().find(|(t, _)| *t == table) else {
            return Ok(None);
        };
        let mut legs = Vec::with_capacity(refs.len());
        for (from, col) in *refs {
            legs.push(format!(
                "SELECT r.{c} FROM ({s}) r WHERE r.{c} IS NOT NULL",
                c = q(col),
                s = self.select(from)?
            ));
        }
        Ok(Some(legs.join(" UNION ")))
    }

    /// The axis ids the carried value rows reference: every non-hist table
    /// with an `axis1_id` column (discovered from the catalog, so a new
    /// axis-bearing table joins the closure automatically).
    fn axis_closure(&self) -> Result<String, DbError> {
        let mut legs = Vec::new();
        for (table, cols) in self.real_cols {
            if !cols.iter().any(|c| c == "axis1_id")
                || selection_for(table)? == Selection::ArrayHist
            {
                continue;
            }
            legs.push(format!(
                "SELECT unnest([axis1_id, axis2_id, axis3_id, axis4_id]) AS axis_id \
                 FROM (SELECT DISTINCT \"axis1_id\", \"axis2_id\", \"axis3_id\", \"axis4_id\" \
                       FROM ({s}) s) d",
                s = self.select(table)?
            ));
        }
        Ok(format!(
            "SELECT DISTINCT axis_id FROM ({}) WHERE axis_id IS NOT NULL",
            legs.join(" UNION ALL ")
        ))
    }
}

/// Self-check (a): per table, `count` + `bit_xor(hash(real columns))` of the
/// source selection against the temp. Order-independent, covers every column
/// of every carried row. Returns the carried counts.
fn self_check_selection(conn: &Connection, sel: &Sel) -> Result<Vec<(String, u64)>, DbError> {
    let mut out = Vec::with_capacity(TABLE_ORDER.len());
    for table in TABLE_ORDER {
        let args = list(sel.cols(table));
        let probe = |rel: &str| -> Result<(i64, String), DbError> {
            Ok(conn.query_row(
                &format!(
                    "SELECT count(*), CAST(COALESCE(bit_xor(hash({args})), 0) AS VARCHAR) \
                     FROM {rel}"
                ),
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )?)
        };
        let (sc, sh) = probe(&format!("({}) s", sel.select(table)?))?;
        let (tc, th) = probe(&q(table))?;
        if sc != tc || sh != th {
            return Err(DbError::Api(format!(
                "extract self-check FAILED for table {table:?}: source selection \
                 (count {sc}, hash {sh}) != extract (count {tc}, hash {th}); nothing published"
            )));
        }
        out.push((table.to_string(), tc as u64));
    }
    Ok(out)
}

/// Self-check (b), head mode: the derived (joining) views give the same
/// result on the source and the extract, both evaluated through THIS
/// build's definitions. The source's own views may be older (its read
/// surface is the one of the build that created or last refreshed it), and
/// comparing an old body with this build's would fail on identical content.
/// So the source side runs under `USE <SRC>` with TEMP copies of this
/// build's view closure installed (their unqualified names then bind to the
/// source's tables); the copies are DROPPED before switching back, which is
/// mandatory: temp objects shadow even inside an attached catalog's views,
/// so a leftover copy would make both sides read the same definitions and
/// the check pass vacuously. The extract side then reads its own
/// (this build's) views. Returns a note when the copies could not be
/// installed on the source (the check is then skipped; the table-level
/// self-check, which covers every carried row, still ran).
fn self_check_derived_views(conn: &Connection) -> Result<Option<String>, DbError> {
    let probe = |rel: &str| -> Result<(i64, String), DbError> {
        Ok(conn.query_row(
            &format!(
                "SELECT count(*), CAST(COALESCE(bit_xor(hash(v)), 0) AS VARCHAR) \
                 FROM (SELECT * FROM {rel}) v"
            ),
            [],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
        )?)
    };
    let own: String = conn.query_row("SELECT current_database()", [], |r| r.get(0))?;
    // ---- the source, through this build's definitions ----------------------
    conn.execute_batch(&format!("USE {SRC}"))?;
    let overlay = match crate::read_surface::install_overlay_unguarded(conn, Some(DERIVED_VIEWS)) {
        Ok(objs) => objs,
        Err(e) => {
            conn.execute_batch(&format!("USE {}", q(&own)))?;
            return Ok(Some(format!(
                "derived-view check skipped: this build's views do not bind on the \
                 source ({e}); the table-level check of every carried row ran"
            )));
        }
    };
    let source: Result<Vec<(i64, String)>, DbError> =
        DERIVED_VIEWS.iter().map(|v| probe(&q(v))).collect();
    // Mandatory, before anything reads the extract side (see above).
    crate::read_surface::drop_overlay(conn, &overlay)?;
    conn.execute_batch(&format!("USE {}", q(&own)))?;
    let source = source?;
    // ---- the extract, through its own (this build's) views -----------------
    for (view, a) in DERIVED_VIEWS.iter().zip(source) {
        let b = probe(&q(view))?;
        if a != b {
            return Err(DbError::Api(format!(
                "extract self-check FAILED for view {view:?}: source {a:?} != extract {b:?}; \
                 nothing published"
            )));
        }
    }
    Ok(None)
}

/// Self-check of the axis garbage collection: every axis id a carried row
/// references exists in the extract, and the extract holds no axis nothing
/// references (the axis set is exactly the referenced closure).
fn self_check_axes(conn: &Connection, real_cols: &[(String, Vec<String>)]) -> Result<(), DbError> {
    let mut legs = Vec::new();
    for (table, cols) in real_cols {
        if cols.iter().any(|c| c == "axis1_id") {
            legs.push(format!(
                "SELECT unnest([axis1_id, axis2_id, axis3_id, axis4_id]) AS axis_id FROM {}",
                q(table)
            ));
        }
    }
    let referenced = format!(
        "SELECT DISTINCT axis_id FROM ({}) WHERE axis_id IS NOT NULL",
        legs.join(" UNION ALL ")
    );
    let missing = count(
        conn,
        &format!(
            "SELECT count(*) FROM ({referenced}) r WHERE r.axis_id NOT IN (SELECT id FROM axis)"
        ),
    )?;
    let unreferenced = count(
        conn,
        &format!("SELECT count(*) FROM axis WHERE id NOT IN ({referenced})"),
    )?;
    if missing != 0 || unreferenced != 0 {
        return Err(DbError::Api(format!(
            "extract self-check FAILED for the axis closure: {missing} referenced axes \
             missing, {unreferenced} unreferenced axes carried; nothing published"
        )));
    }
    Ok(())
}

fn list(cols: &[String]) -> String {
    cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ")
}

fn count(conn: &Connection, sql: &str) -> Result<u64, DbError> {
    let n: i64 = conn.query_row(sql, [], |r| r.get(0))?;
    Ok(n.max(0) as u64)
}

fn strings(conn: &Connection, sql: &str) -> Result<Vec<String>, DbError> {
    let mut stmt = conn.prepare(sql)?;
    let v = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    Ok(v)
}

// ---------------------------------------------------------------------------
// Temp naming and the orphan sweep (extract-specific; compaction's own
// `<db>.compact.` prefix is left alone).
// ---------------------------------------------------------------------------

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// `<dest-filename>.extract.<pid>.<n>.tmp`, a sibling of dest (same
/// filesystem, so the hard-link publish works).
fn temp_path_for(dest: &Path) -> PathBuf {
    let dir = dest.parent().unwrap_or_else(|| Path::new("."));
    let name = dest.file_name().and_then(|n| n.to_str()).unwrap_or("db");
    let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("{name}.extract.{}.{n}.tmp", std::process::id()))
}

fn temp_prefix(dest: &Path) -> String {
    let name = dest.file_name().and_then(|n| n.to_str()).unwrap_or("db");
    format!("{name}.extract.")
}

/// Delete abandoned `<dest-filename>.extract.*.tmp` (and `.tmp.wal`) siblings
/// from a crashed run. Best-effort, under the dest's per-path lock. (A
/// concurrent extract to the SAME dest in another process would lose its
/// temp here and then fail loudly at publish; it never produces a wrong dest.)
pub fn sweep_orphan_extract_temps(dest: &Path) {
    let dir = parent_dir(dest);
    let prefix = temp_prefix(dest);
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with(&prefix) && (name.ends_with(".tmp") || name.ends_with(".tmp.wal")) {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

fn wal_path(db: &Path) -> PathBuf {
    let mut w = db.as_os_str().to_os_string();
    w.push(".wal");
    PathBuf::from(w)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rebuild::SELECTION;

    /// A dest that appears while the extract runs (after the step-1 check)
    /// is never replaced: the publish is a hard link, not a rename.
    #[test]
    fn publish_never_clobbers_a_dest_that_appeared() {
        let dir = tempfile::tempdir().unwrap();
        let temp = dir.path().join("x.duckdb.extract.1.0.tmp");
        let dest = dir.path().join("x.duckdb");
        fs::write(&temp, b"new").unwrap();
        fs::write(&dest, b"someone else's file").unwrap();
        let err = publish_no_clobber(&temp, &dest).expect_err("must refuse");
        assert!(err.to_string().contains("appeared"), "{err}");
        assert_eq!(fs::read(&dest).unwrap(), b"someone else's file");
        // And a free dest publishes.
        fs::remove_file(&dest).unwrap();
        publish_no_clobber(&temp, &dest).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"new");
    }

    /// Every base table is classed, and the classes fit the live catalog.
    #[test]
    fn selection_covers_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("sel.duckdb");
        init_schema(&db).unwrap();
        let conn = open_with_retry(&db, AccessMode::ReadWrite).unwrap();
        let real = discover_real_columns(&conn).unwrap();
        check_catalog(&conn, &real).unwrap();
        check_selection(&real).expect("SELECTION fits the catalog");
        for t in TABLE_ORDER {
            assert!(SELECTION.iter().any(|(s, _)| s == t), "{t} unclassed");
        }
        // The derived views of the runtime self-check all exist.
        for v in DERIVED_VIEWS {
            let n: i64 = conn
                .query_row(
                    "SELECT count(*) FROM duckdb_views() WHERE view_name = ?",
                    [v],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "derived view {v} is gone");
        }
        conn.close().unwrap();
    }
}
