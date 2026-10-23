//! Physical repack (Sense A) — reclaim dead *blocks* without dropping any
//! logical content (`specs/compaction_DESIGN.md`, including its overriding
//! "Critique resolutions" B1/B2/B3/R1/R2/R3).
//!
//! DuckDB's single-file block manager never returns the dead blocks of many
//! prior commit-checkpoints to a smaller file (§0/§1). There is no in-place
//! shrink, so [`compact`] rebuilds the whole file into a fresh, densely-packed
//! sibling and atomically renames it over the original:
//!
//! > `init_schema` a fresh temp → `ATTACH` the original read-only →
//! > `INSERT … SELECT` every base table's *real* columns in FK order →
//! > restore sequence state → full-content self-check → fsync + atomic rename.
//!
//! It is **non-destructive**: every history row (live AND superseded), the whole
//! `commit` log, all axes/metadata and sequence state survive byte-for-byte at
//! the query level. The catalog (75 views, 38 macros, 18 sequences, indexes) is
//! reconstructed by `init_schema`, never copied — only base-table data crosses.
//!
//! ## Clustering (erasure_compaction_plan §4.1, D-E4 — always-on)
//! The rebuild copy ORDER-BYs each table per [`CLUSTER_KEYS`]: current-first on
//! versioned tables, key/natural order elsewhere. Content is untouched (the
//! order-independent self-check still covers every row); only physical layout
//! changes — see the `CLUSTER_KEYS` docs for the verified mechanism.
//!
//! ## Crash safety (§3)
//! The original is only ever *read* (attached RO) until the final `rename`.
//! A crash before the rename leaves the intact original + an orphan
//! `*.compact.*.tmp` (swept on the next [`compact`]); a crash during/after the
//! rename leaves the complete new file (the temp is fsync'd, and the directory
//! entry, before the rename flips it). POSIX `rename` over an existing path is
//! atomic — a reader sees the old inode or the new one, never a splice.
//!
//! **Windows single-writer assumption (§3):** `rename`-over-an-open-file fails
//! on Windows while any handle to the target is open. Both connections (the temp
//! and the RO attach of the source) are closed before the rename, and the whole
//! op holds the exclusive per-path lock + the source's file lock, so within the
//! supported single-writer editor model no other handle is open. A sharing
//! violation surfaces as an error and leaves the original intact — a failed
//! compaction is never a lost DB.
//!
//! ## Lock discipline (invariants #1/#2, §4)
//! Compaction is an exclusive whole-DB rewrite: it holds [`conn::path_lock`] for
//! the entire op (init, copy, self-check, swap) and holds the source's file lock
//! via the RO `ATTACH` so no concurrent commit races the swap. A busy source
//! surfaces as [`DbError::Busy`]. It runs as a distinct locked phase (never
//! inside a write transaction) so a compaction failure can never roll back a
//! durable commit. It moves already-canonicalized bytes verbatim — it computes
//! no value encodings, hashes or axis dedup (invariant #2).

use std::fs;
use std::path::{Path, PathBuf};

use duckdb::{AccessMode, Connection};

use crate::conn::{open_with_retry, path_lock};
use crate::error::DbError;
use crate::schema_init::init_schema;

/// Report of one repack. `swapped == false` marks a [`compact_if_needed`] no-op
/// (the reclaimable-space signal was below the threshold), in which case the
/// `bytes_after`/`blocks_after` mirror the "before" values and `rows_per_table`
/// is empty (nothing was copied).
#[derive(Debug, Clone, Default)]
pub struct CompactionReport {
    pub bytes_before: u64,
    pub bytes_after: u64,
    /// `PRAGMA database_size.total_blocks` on the source before the repack.
    pub blocks_before: u64,
    /// `total_blocks` of the freshly-packed file (post-`CHECKPOINT`).
    pub blocks_after: u64,
    /// Copied row counts (live + dead) per base table — makes the self-check
    /// inspectable, not just pass/fail.
    pub rows_per_table: Vec<(String, u64)>,
    /// `false` ⇒ [`compact_if_needed`] skipped under the threshold.
    pub swapped: bool,
}

/// Current schema version the bundled DDL seeds (`900_bootstrap.sql`, mirrored
/// by `schema/build.py`'s `SCHEMA_VERSION`). Duplicated here only to gate a
/// source read *cheaply* before a temp is built (R2); it is cross-checked
/// against the freshly-init'd temp's own `metadata` row (see
/// [`assert_temp_schema_version`]) so it can never silently drift from the DDL.
///
/// **Bump rule:** bump iff the COPY CONTRACT changes — the base-table set or a
/// table's insertable column shape (what the named-column `INSERT..SELECT`
/// and the self-check hash range over). Bootstrap DATA rows and view/macro/
/// index-only changes do NOT bump: the catalog is rebuilt from the DDL, never
/// copied. Precedents: the DB-identity metadata row (20fe969) added bootstrap
/// data — no bump; the `erasure_marker` base table (this version) changed the
/// table set — bump. Consequence of a bump (erasure_compaction_plan §9 step
/// 1): existing dev DBs refuse compaction until re-inited/re-imported —
/// pre-freeze, re-init IS the migration story.
pub const SCHEMA_VERSION: &str = "0.3-erasure-marker";

/// Default byte-growth watermark for [`compact_if_needed`]: repack once the DB
/// file has grown to at least this multiple of its size right after the last
/// successful compaction (3.0 ⇒ compact when the file has tripled).
///
/// **Why byte-growth, not the free-block ratio (R1 CALIBRATION FIX,
/// empirical).** The first cut gated on `PRAGMA database_size`'s
/// `free_blocks / total_blocks`. Measured against the real 19 MB bloated
/// fixture — the exact case this feature targets — that ratio is only 0.25
/// (57 used / 76 total): DuckDB reports stranded dead blocks from old
/// checkpoints as `used`, not `free`, so the free-block signal saturates low
/// and *never* trips on true bloat. It is the wrong signal. The file's byte
/// size vs its last-compacted baseline (persisted in a sidecar, see
/// [`baseline_path`]) tracks accreted dead blocks directly, and the cheap gate
/// is two `stat`s + a tiny file read with NO DB open. Raise the factor to
/// compact less often, lower it to compact more eagerly.
pub const DEFAULT_GROWTH_FACTOR: f64 = 3.0;

/// Base tables in a safe insertion order (§2.3). The ONLY engine-enforced FKs
/// are `→ commit(id)` and `→ axis(id)` (verified across `001–065`), so
/// **`commit` first, then `axis`, then the rest in any order** satisfies every
/// referential constraint. `value_array_hist` carries no FK by design.
const TABLE_ORDER: &[&str] = &[
    // FK targets first.
    "commit",
    "axis",
    // References commit only (`at_commit`, `horizon_commit`); append-only
    // erasure trace (erasure_compaction_plan §1.1) — copied verbatim like
    // every base table (§4.2: erased rows are simply not there, so compaction
    // stays copy-verbatim-and-self-check).
    "erasure_marker",
    // The rest — no remaining engine FKs to order among.
    "metadata",
    "entity_metadata",
    "parameter_value_metadata",
    "run",
    "alternative",
    "scenario",
    "scenario_alternative",
    "entity_alternative",
    "membership",
    "axis_datetime",
    "axis_period",
    "axis_label",
    "axis_integer",
    "axis_float",
    "axis_time_pattern",
    "display_mode",
    "entity_class_display_mode",
    "entity_class",
    "class_set",
    "class_set_member",
    "entity_class_dimension",
    "entity",
    "entity_element",
    "entity_location",
    "units_of_measure",
    "parameter_value_list",
    "list_value",
    "parameter_definition",
    "entity_parameter_unit",
    "parameter_type",
    "parameter_type_dimension",
    "value_scalar",
    "value_long",
    "value_array",
    "value_array_hist",
    "definition_value_scalar",
    "definition_value_long",
    "definition_value_array",
    "semantic_annotation",
];

/// The authoritative per-table `GENERATED ALWAYS AS … VIRTUAL` column set
/// (design §2.2, verified table-by-table against the DDL). These recompute from
/// the base columns and CANNOT be inserted into, so they are excluded from both
/// the copy and the self-check hash. Tables absent from this list have no
/// generated columns.
///
/// **R3 (honest drift guard):** `information_schema.columns.is_generated` is
/// NULL for VIRTUAL columns on the bundled engine, so the runtime cross-check
/// ([`check_catalog`]) can only confirm these names STILL EXIST and that the set
/// of base tables is unchanged — it cannot confirm a listed column is still
/// *generated*, nor detect a NEW generated column added within an existing
/// table. Keyed to [`SCHEMA_VERSION`]; retire this map once a bundled DuckDB
/// reliably populates the standard flag.
const GENERATED_COLUMNS: &[(&str, &[&str])] = &[
    ("metadata", &["cur_id", "cur_name", "cur_val"]),
    ("entity_metadata", &["cur_e", "cur_m"]),
    (
        "parameter_value_metadata",
        &["cur_e", "cur_p", "cur_a", "cur_m"],
    ),
    ("display_mode", &["cur_id", "cur_name"]),
    ("entity_class_display_mode", &["cur_dm", "cur_ec"]),
    ("alternative", &["cur_id", "cur_name"]),
    ("scenario", &["cur_id", "cur_name"]),
    ("scenario_alternative", &["cur_s", "cur_a", "cur_r"]),
    ("entity_alternative", &["cur_e", "cur_a"]),
    ("membership", &["cur_g", "cur_m", "cur_a"]),
    ("value_scalar", &["ce", "cp", "ca"]),
    ("value_long", &["ce", "cp", "ca", "c1", "c2", "c3", "c4"]),
    ("definition_value_scalar", &["cp", "cs"]),
    (
        "definition_value_long",
        &["cp", "cs", "c1", "c2", "c3", "c4"],
    ),
    ("definition_value_array", &["cp", "cs"]),
    ("entity_class", &["cur_id", "cur_name"]),
    ("class_set", &["cur_id", "cur_name"]),
    ("class_set_member", &["cur_s", "cur_c"]),
    ("entity_class_dimension", &["cur_c", "cur_p"]),
    ("entity", &["cur_id", "cur_cls", "cur_name"]),
    ("entity_element", &["cur_e", "cur_p"]),
    ("entity_location", &["cur_e"]),
    ("units_of_measure", &["cur_id", "cur_name"]),
    ("parameter_value_list", &["cur_id", "cur_name"]),
    ("list_value", &["cur_id", "cur_l", "cur_o"]),
    ("parameter_definition", &["cur_id", "cur_cls", "cur_name"]),
    ("entity_parameter_unit", &["cur_e", "cur_p"]),
    ("parameter_type", &["cur_id"]),
    ("parameter_type_dimension", &["cur_t", "cur_p"]),
    ("semantic_annotation", &["cur_id"]),
];

/// Generated columns for one table (empty slice if none).
fn generated_for(table: &str) -> &'static [&'static str] {
    GENERATED_COLUMNS
        .iter()
        .find(|(t, _)| *t == table)
        .map(|(_, c)| *c)
        .unwrap_or(&[])
}

/// Per-table clustering key columns for the rebuild copy's `ORDER BY`
/// (erasure_compaction_plan §4.1, decision D-E4: clustering is ALWAYS-ON).
/// The rule, applied uniformly by [`cluster_order`]:
///
///   - table has a `valid_to_commit` column (from/to tiers and all narrow
///     versioned tables; also `value_array_hist`, whose liveness expression is
///     constant-false) ⇒ `ORDER BY (valid_to_commit IS NULL) DESC, <keys>,
///     valid_from_commit` — **current-first**, then key locality, then history
///     order;
///   - otherwise (append-only tables, axis children, the all-live
///     `value_array`) ⇒ `ORDER BY <keys>` — natural/key order, locality only.
///
/// **Mechanism, stated correctly** (critique M2, empirical on DuckDB 1.5.5):
/// the current-first win is **cheap contiguous validity-vector filtering**
/// (~2× faster current-state scans on a 2 M-row/50%-dead fixture), NOT
/// row-group skipping — 1.5.5 does not use null-count segment stats to
/// eliminate row groups on `valid_to_commit IS NULL`; both layouts scan every
/// row. Min/max zone maps DO prune on the sortable key columns (verified), so
/// the key part buys genuine skipping for keyed reads. Insertion order
/// verifiably persists through CHECKPOINT and reopen. The B3 self-check is
/// order-independent (`count` + `bit_xor(hash(...))`), so clustering needs
/// zero self-check changes; determinism costs no flag surface, and the sort
/// is bounded by compaction already being a whole-file rewrite.
///
/// Every listed column is validated against the live catalog at repack time
/// ([`cluster_order`] hard-errors on a missing column), so this map cannot
/// silently drift from the DDL. Every TABLE_ORDER entry must appear here.
const CLUSTER_KEYS: &[(&str, &[&str])] = &[
    // Append-only: id / natural key (verbatim order).
    ("commit", &["id"]),
    ("axis", &["id"]),
    ("erasure_marker", &["id"]),
    ("run", &["id"]),
    ("axis_datetime", &["axis_id", "pos"]),
    ("axis_period", &["axis_id", "pos"]),
    ("axis_label", &["axis_id", "pos"]),
    ("axis_integer", &["axis_id", "pos"]),
    ("axis_float", &["axis_id", "pos"]),
    ("axis_time_pattern", &["axis_id", "pos", "term_no"]),
    // All-live array tier: key triple (locality only — no validity column).
    (
        "value_array",
        &["entity_id", "parameter_definition_id", "alternative_id"],
    ),
    // History twin: key triple (+ valid_from via the uniform versioned rule).
    (
        "value_array_hist",
        &["entity_id", "parameter_definition_id", "alternative_id"],
    ),
    // From/to value tiers: key triple (long adds its position tuple).
    (
        "value_scalar",
        &["entity_id", "parameter_definition_id", "alternative_id"],
    ),
    (
        "value_long",
        &[
            "entity_id",
            "parameter_definition_id",
            "alternative_id",
            "i1_pos",
            "i2_pos",
            "i3_pos",
            "i4_pos",
        ],
    ),
    (
        "definition_value_scalar",
        &["parameter_definition_id", "slot"],
    ),
    (
        "definition_value_long",
        &[
            "parameter_definition_id",
            "slot",
            "i1_pos",
            "i2_pos",
            "i3_pos",
            "i4_pos",
        ],
    ),
    (
        "definition_value_array",
        &["parameter_definition_id", "slot"],
    ),
    // Narrow versioned tables: their identity/key columns.
    ("metadata", &["id"]),
    ("entity_metadata", &["entity_id", "metadata_id"]),
    (
        "parameter_value_metadata",
        &[
            "entity_id",
            "parameter_definition_id",
            "alternative_id",
            "metadata_id",
        ],
    ),
    ("alternative", &["id"]),
    ("scenario", &["id"]),
    ("scenario_alternative", &["scenario_id", "alternative_id"]),
    ("entity_alternative", &["entity_id", "alternative_id"]),
    (
        "membership",
        &["group_entity_id", "member_entity_id", "alternative_id"],
    ),
    ("display_mode", &["id"]),
    (
        "entity_class_display_mode",
        &["display_mode_id", "entity_class_id"],
    ),
    ("entity_class", &["id"]),
    ("class_set", &["id"]),
    ("class_set_member", &["class_set_id", "member_class_id"]),
    ("entity_class_dimension", &["entity_class_id", "position"]),
    ("entity", &["id"]),
    ("entity_element", &["entity_id", "position"]),
    ("entity_location", &["entity_id"]),
    ("units_of_measure", &["id"]),
    ("parameter_value_list", &["id"]),
    ("list_value", &["id"]),
    ("parameter_definition", &["id"]),
    (
        "entity_parameter_unit",
        &["entity_id", "parameter_definition_id"],
    ),
    ("parameter_type", &["id"]),
    (
        "parameter_type_dimension",
        &["parameter_type_id", "position"],
    ),
    ("semantic_annotation", &["id"]),
];

/// The `ORDER BY` clause for one table's rebuild copy (see [`CLUSTER_KEYS`]).
/// Hard-errors on a table missing from the map or a key column absent from the
/// live catalog (`real_cols` — the same drift discipline as [`check_catalog`]).
fn cluster_order(table: &str, real_cols: &[String]) -> Result<String, DbError> {
    let keys = CLUSTER_KEYS
        .iter()
        .find(|(t, _)| *t == table)
        .map(|(_, k)| *k)
        .ok_or_else(|| {
            DbError::Api(format!(
                "compaction catalog drift: table {table:?} has no CLUSTER_KEYS entry \
                 (a schema edit added a table; update compaction::CLUSTER_KEYS)"
            ))
        })?;
    for k in keys {
        if !real_cols.iter().any(|c| c == k) {
            return Err(DbError::Api(format!(
                "compaction catalog drift: cluster key column {k:?} of table {table:?} \
                 is gone (update compaction::CLUSTER_KEYS)"
            )));
        }
    }
    let mut parts: Vec<String> = Vec::with_capacity(keys.len() + 2);
    let versioned = real_cols.iter().any(|c| c == "valid_to_commit");
    if versioned {
        // Current-first (D-E4): live rows form one contiguous prefix.
        parts.push("(\"valid_to_commit\" IS NULL) DESC".to_string());
    }
    parts.extend(keys.iter().map(|k| q(k)));
    if versioned {
        parts.push(q("valid_from_commit"));
    }
    Ok(format!(" ORDER BY {}", parts.join(", ")))
}

/// Quote a SQL identifier (double-quote, embedded `"` doubled). Every table and
/// column name flows through this so reserved words (`commit`, `values`,
/// `timestamp`, `user`, …) are always safe.
fn q(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Physical repack of `db_path` (Sense A). See the module docs. Always performs
/// the rewrite (no gate); use [`compact_if_needed`] on the save path. On success
/// it refreshes the byte-growth baseline sidecar to the new file size.
pub fn compact(db_path: &Path) -> Result<CompactionReport, DbError> {
    compact_locked(db_path)
}

/// [`compact`] gated by a cheap byte-growth watermark (R1 CALIBRATION FIX). The
/// gate is pure filesystem — the DB file's current byte size vs the baseline
/// (its size right after the last successful compaction, persisted in the
/// [`baseline_path`] sidecar) — with **NO DB open**:
///
/// - no baseline (missing / zero / unparseable) ⇒ compact once to establish it;
/// - baseline `b` present ⇒ compact iff `current_bytes >= growth_factor * b`,
///   else no-op with `swapped == false`.
///
/// Only when it decides to compact does it open the source (schema_version gate,
/// repack, atomic swap). So the common no-op commit is two `stat`s plus a
/// tiny file read. Pass [`DEFAULT_GROWTH_FACTOR`] for the documented default.
/// The sidecar is advisory: a stale/missing/torn baseline costs at most one
/// extra or skipped compaction and self-corrects on the next run.
pub fn compact_if_needed(db_path: &Path, growth_factor: f64) -> Result<CompactionReport, DbError> {
    // Cheap gate FIRST — no lock, no DB open.
    let current = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let should_compact = match read_baseline(db_path) {
        None => true,    // establish a baseline on first run
        Some(0) => true, // degenerate/torn baseline ⇒ re-establish
        Some(b) => current >= (b as f64 * growth_factor) as u64,
    };
    if !should_compact {
        return Ok(CompactionReport {
            bytes_before: current,
            bytes_after: current,
            // Not probed on the no-op path (no DB open, by design).
            blocks_before: 0,
            blocks_after: 0,
            rows_per_table: Vec::new(),
            swapped: false,
        });
    }
    compact_locked(db_path)
}

/// The full locked repack: exclusive lock → orphan sweep → source probe (R2
/// schema gate + block metrics) → temp init + copy + self-check → atomic swap →
/// baseline sidecar refresh. Shared by [`compact`] and the fire path of
/// [`compact_if_needed`].
fn compact_locked(db_path: &Path) -> Result<CompactionReport, DbError> {
    // Exclusive for the WHOLE op (invariant #1, §4): held across init, copy,
    // self-check and swap. Non-reentrant — nothing below may call a `conn`
    // helper that re-takes this lock (that is why we use `open_with_retry`
    // directly, never `with_read`).
    let lock = path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());

    // Best-effort sweep of abandoned temps from a prior crashed run (§3). They
    // are always incomplete-and-orphaned; the live DB is authoritative.
    sweep_orphan_temps(db_path);

    let bytes_before = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);

    // ---- Cheap source probe: schema_version gate (R2) + block metrics -------
    // One per-operation RO handle (opened directly so it does not re-take the
    // per-path lock we already hold), closed immediately.
    let (src_version, blocks_before) = {
        let conn = open_with_retry(db_path, AccessMode::ReadOnly)?;
        let version: String = conn.query_row(
            "SELECT value FROM metadata \
             WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )?;
        let total: i64 =
            conn.query_row("SELECT total_blocks FROM pragma_database_size()", [], |r| {
                r.get(0)
            })?;
        conn.close().map_err(|(_, e)| DbError::Duckdb(e))?;
        (version, total as u64)
    };

    // R2: hard-error on any schema mismatch BEFORE building a temp or copying —
    // a named-column INSERT..SELECT against a differently-shaped source would
    // silently drop/misfill columns.
    if src_version != SCHEMA_VERSION {
        return Err(DbError::Api(format!(
            "compaction refused: source schema_version {src_version:?} != current {SCHEMA_VERSION:?} \
             (compaction copies named columns against the current schema; to migrate, \
             re-init a fresh database and re-import)"
        )));
    }

    // ---- Build the fresh temp and repack into it ---------------------------
    let temp_path = temp_path_for(db_path);
    // A stale temp with this exact name (same pid, astronomically unlikely) is
    // removed by the sweep above; init a clean file.
    init_schema(&temp_path)?;

    // The repack runs on ONE temp RW connection with the source ATTACHed RO.
    // Any failure inside must clean the temp and keep the original untouched.
    let conn = open_with_retry(&temp_path, AccessMode::ReadWrite)?;
    let outcome = repack_into(&conn, db_path, &temp_path);

    let (rows_per_table, blocks_after) = match outcome {
        Ok(v) => v,
        Err(e) => {
            // Abort: close, delete the temp (+ its WAL), keep the original.
            let _ = conn.close();
            remove_temp(&temp_path);
            return Err(e);
        }
    };

    // Close the temp handle BEFORE the swap (Windows caveat, §3).
    conn.close().map_err(|(_, e)| DbError::Duckdb(e))?;

    // Stale-WAL hygiene (erasure_compaction_plan §7 item 7 / critique m7b),
    // BEFORE the rename so the removal can only ever hit a WAL of the OLD
    // inode era: a `<db>.wal` present here was left by a writer that crashed
    // before this compaction (normal closes checkpoint and remove it; we hold
    // the per-path lock, and the source's file lock was held through the
    // copy). Its committed content is already IN the temp: the RO source
    // probe/ATTACH replays valid WALs (pinned by
    // `compact_carries_stale_wal_content_and_unlinks_it`), and a corrupt WAL
    // fails that open loudly, aborting long before this point. Left in place,
    // it would be replayed against the freshly-swapped file on the next RW
    // open — the hazard. Unlinking after the rename instead would race a
    // second process's NEW live WAL on the swapped-in file; before the
    // rename, no writer can have the new inode yet.
    let stale_wal = {
        let mut w = db_path.as_os_str().to_os_string();
        w.push(".wal");
        PathBuf::from(w)
    };
    let _ = fs::remove_file(&stale_wal);

    // Durability + atomic swap (§3): fsync the temp bytes and its directory
    // entry, rename over the original, fsync the directory again.
    fsync_file(&temp_path)?;
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    fsync_dir(dir);
    fs::rename(&temp_path, db_path).map_err(|source| DbError::SchemaDir {
        path: temp_path.display().to_string(),
        source,
    })?;
    fsync_dir(dir);

    let bytes_after = fs::metadata(db_path)
        .map(|m| m.len())
        .unwrap_or(bytes_before);

    // Refresh the byte-growth baseline (R1) to the just-compacted size — written
    // AFTER the DB rename so a crash in between leaves an under-estimate
    // baseline (benign: at worst an earlier next compaction). Best-effort and
    // atomic (temp sidecar + rename); a write failure only costs advisory
    // accuracy, never the compaction itself.
    write_baseline(db_path, bytes_after);

    Ok(CompactionReport {
        bytes_before,
        bytes_after,
        blocks_before,
        blocks_after,
        rows_per_table,
        swapped: true,
    })
}

/// The copy + restore + self-check, all on `conn` (a fresh RW temp connection).
/// Returns the per-table copied counts and the post-`CHECKPOINT` block count.
/// On ANY error the caller aborts and keeps the original (never swaps).
fn repack_into(
    conn: &Connection,
    src_path: &Path,
    temp_path: &Path,
) -> Result<(Vec<(String, u64)>, u64), DbError> {
    // Protect the SCHEMA_VERSION const against silent DDL drift: the temp we
    // just built must seed exactly it (R2 belt-and-braces).
    assert_temp_schema_version(conn)?;

    // Discover the real (insertable) columns per table from the temp's OWN
    // catalog, BEFORE attaching the source (so information_schema shows only the
    // temp). real = catalog_columns(t) MINUS GENERATED_COLUMNS(t) (§2.2).
    let real_cols = discover_real_columns(conn)?;

    // R3 runtime cross-check: base-table set unchanged + every mapped generated
    // column still exists. Hard-error on drift so a schema edit fails loudly.
    check_catalog(conn, &real_cols)?;

    // B1: init_schema replays 900_bootstrap.sql, which seeds commit(id=1) + a
    // metadata schema_version row. Both collide with the copy (PK on commit.id;
    // one_current_/uq_ indexes on metadata). Clear them first. metadata BEFORE
    // commit: metadata.valid_from_commit REFERENCES commit(id), so deleting
    // commit first would trip that engine FK.
    conn.execute_batch("DELETE FROM metadata; DELETE FROM \"commit\";")?;

    // ATTACH the source read-only — this acquires and HOLDS the source's file
    // lock for the whole copy (cross-process exclusion, §4). A busy source
    // surfaces as DbError::Busy, identical to a busy commit.
    attach_source_ro(conn, src_path)?;

    // Copy every base table's real columns, named (never positional), in FK
    // order, inside one transaction (belt-and-braces beyond the two-tier order).
    conn.execute_batch("BEGIN")?;
    for table in TABLE_ORDER {
        let cols = real_cols
            .iter()
            .find(|(t, _)| t == table)
            .map(|(_, c)| c)
            .expect("every TABLE_ORDER entry is discovered");
        let list = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        // Clustered copy (erasure_compaction_plan §4.1, always-on — D-E4):
        // current-first / key-ordered per CLUSTER_KEYS. Insertion order
        // persists through CHECKPOINT; the order-independent self-check is
        // unaffected.
        let order = cluster_order(table, cols)?;
        let sql = format!(
            "INSERT INTO {t} ({list}) SELECT {list} FROM src.{t}{order}",
            t = q(table)
        );
        conn.execute_batch(&sql)?;
    }
    conn.execute_batch("COMMIT")?;

    // Sequence restore (B2 / B2b) — must run for the fresh sequences to sit
    // where the source left off, else the next auto-id collides.
    restore_sequences(conn)?;

    // B3 self-check: full content hash over ALL real columns of ALL rows
    // (live + dead) of ALL base tables. This IS the destructive-swap guard.
    let rows_per_table = self_check(conn, &real_cols)?;

    // Detach the source, then settle blocks and read the final count.
    conn.execute_batch("DETACH src")?;
    conn.execute_batch("CHECKPOINT")?;
    let _ = temp_path; // (temp file identity is the caller's concern)
    let blocks_after: i64 =
        conn.query_row("SELECT total_blocks FROM pragma_database_size()", [], |r| {
            r.get(0)
        })?;

    Ok((rows_per_table, blocks_after as u64))
}

/// Confirm the freshly-init'd temp seeded [`SCHEMA_VERSION`] (guards the const
/// against DDL drift). Read before B1 wipes `metadata`.
fn assert_temp_schema_version(conn: &Connection) -> Result<(), DbError> {
    let v: String = conn.query_row(
        "SELECT value FROM metadata \
         WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL",
        [],
        |r| r.get(0),
    )?;
    if v != SCHEMA_VERSION {
        return Err(DbError::Api(format!(
            "internal: fresh temp seeded schema_version {v:?} != compaction::SCHEMA_VERSION \
             {SCHEMA_VERSION:?} — the const has drifted from the DDL"
        )));
    }
    Ok(())
}

/// Per-table ordered real (insertable) columns: catalog columns minus the
/// [`GENERATED_COLUMNS`] set, in `ordinal_position` order. Ordered so the copy's
/// INSERT list and SELECT list line up positionally.
fn discover_real_columns(conn: &Connection) -> Result<Vec<(String, Vec<String>)>, DbError> {
    let mut out = Vec::with_capacity(TABLE_ORDER.len());
    for table in TABLE_ORDER {
        let mut stmt = conn.prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )?;
        let names: Vec<String> = stmt
            .query_map([table], |r| r.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        let gen = generated_for(table);
        let real: Vec<String> = names
            .into_iter()
            .filter(|c| !gen.contains(&c.as_str()))
            .collect();
        out.push((table.to_string(), real));
    }
    Ok(out)
}

/// R3 cross-check against the live temp catalog: (a) the set of base tables is
/// exactly [`TABLE_ORDER`]; (b) every column named in [`GENERATED_COLUMNS`]
/// still exists on its table. Hard-error on any divergence.
fn check_catalog(conn: &Connection, real_cols: &[(String, Vec<String>)]) -> Result<(), DbError> {
    // (a) base-table set.
    let mut stmt = conn.prepare(
        "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'",
    )?;
    let mut found: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    found.sort();
    let mut expected: Vec<String> = TABLE_ORDER.iter().map(|t| t.to_string()).collect();
    expected.sort();
    if found != expected {
        return Err(DbError::Api(format!(
            "compaction catalog drift: base tables {found:?} != known set {expected:?} \
             (a schema edit added/removed a table; update compaction::TABLE_ORDER + \
             GENERATED_COLUMNS)"
        )));
    }
    // (b) each mapped generated column still exists on its table. Rebuild the
    // full column list per table (real + generated) from the catalog to check
    // presence.
    for (table, gens) in GENERATED_COLUMNS {
        let mut cstmt = conn
            .prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ?")?;
        let cols: Vec<String> = cstmt
            .query_map([table], |r| r.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        for g in *gens {
            if !cols.iter().any(|c| c == g) {
                return Err(DbError::Api(format!(
                    "compaction catalog drift: generated column {g:?} of table {table:?} is gone \
                     (update compaction::GENERATED_COLUMNS)"
                )));
            }
        }
    }
    // Also fail if any table produced an empty real-column list (impossible for
    // the real schema; catches a wholesale-generated map mistake).
    for (table, real) in real_cols {
        if real.is_empty() {
            return Err(DbError::Api(format!(
                "compaction: table {table:?} has no real columns to copy \
                 (GENERATED_COLUMNS over-claims)"
            )));
        }
    }
    Ok(())
}

/// ATTACH the source read-only on the temp connection, mapping a busy file lock
/// to [`DbError::Busy`] (same UX as a busy commit).
fn attach_source_ro(conn: &Connection, src_path: &Path) -> Result<(), DbError> {
    // Single-quote-escape the path for the SQL literal.
    let lit = src_path.display().to_string().replace('\'', "''");
    let sql = format!("ATTACH '{lit}' AS src (READ_ONLY)");
    conn.execute_batch(&sql).map_err(|e| {
        if e.to_string().contains("Could not set lock") {
            DbError::Busy { attempts: 1 }
        } else {
            DbError::Duckdb(e)
        }
    })
}

/// Restore each fresh sequence to the source's state (B2, confirmed on bundled
/// 1.5.5). The fresh sequences sit at their DDL start after `init_schema`; left
/// there, the next auto-created id would be 1 and collide with copied ids /
/// violate a `one_current_*` index.
///
/// **Mechanism (B2 — locked):** `CREATE OR REPLACE SEQUENCE … START WITH n`
/// alone FAILS (dependency error — the column DEFAULT `nextval('seq')` depends
/// on it) and `ALTER SEQUENCE … RESTART` is unimplemented. The working dance is
/// per sequence: DROP the column DEFAULT → `CREATE OR REPLACE … START WITH n` →
/// re-add the DEFAULT.
///
/// `n = last_value + increment_by` so the NEXT `nextval` equals it (mirrors the
/// source's last handed-out id; DuckDB `nextval` is non-transactional so
/// mirroring `last_value` — not `max(id)+1` — is the non-destructive bar). A
/// source sequence that is unused (`last_value` NULL) is left at its DDL start.
///
/// **B2b:** `duckdb_sequences()` returns rows for BOTH databases under ATTACH,
/// so the source state read is filtered `WHERE database_name = 'src'`.
fn restore_sequences(conn: &Connection) -> Result<(), DbError> {
    // (table, column, sequence) for every column whose DEFAULT is nextval('…').
    // Read from the temp's own catalog (main db).
    let mut stmt = conn.prepare(
        "SELECT table_name, column_name, column_default FROM information_schema.columns \
         WHERE column_default LIKE 'nextval(%'",
    )?;
    let mapping: Vec<(String, String, String)> = stmt
        .query_map([], |r| {
            let t: String = r.get(0)?;
            let c: String = r.get(1)?;
            let d: String = r.get(2)?;
            Ok((t, c, parse_seq_name(&d)))
        })?
        .collect::<Result<_, _>>()?;

    for (table, column, seq) in mapping {
        // Source state for THIS sequence (B2b: filter by database_name).
        let row: Option<(Option<i64>, i64)> = conn
            .query_row(
                "SELECT last_value, increment_by FROM duckdb_sequences() \
                 WHERE database_name = 'src' AND sequence_name = ?",
                [&seq],
                |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, i64>(1)?)),
            )
            .ok();
        let Some((Some(last), inc)) = row else {
            // Unused (NULL last_value) or missing ⇒ leave the fresh sequence at
            // its DDL start (matches an unused source sequence).
            continue;
        };
        let n = last + inc;
        // The confirmed drop-default → replace → re-add-default dance.
        conn.execute_batch(&format!(
            "ALTER TABLE {t} ALTER COLUMN {c} DROP DEFAULT; \
             CREATE OR REPLACE SEQUENCE {s} START WITH {n}; \
             ALTER TABLE {t} ALTER COLUMN {c} SET DEFAULT nextval('{seq}');",
            t = q(&table),
            c = q(&column),
            s = q(&seq),
            n = n,
            seq = seq.replace('\'', "''"),
        ))?;
    }
    Ok(())
}

/// Extract `seq` from a `nextval('seq')` DEFAULT expression.
fn parse_seq_name(default_expr: &str) -> String {
    default_expr
        .split_once('\'')
        .and_then(|(_, rest)| rest.split_once('\''))
        .map(|(name, _)| name.to_string())
        .unwrap_or_default()
}

/// B3 self-check: for every base table compare `(count(*), bit_xor(hash(<all
/// real cols>)))` on the source and the temp. `count(*)` includes superseded
/// (dead) rows — it defeats the XOR's duplicate-cancellation AND covers full
/// history. The hash covers every real column, so a swapped same-typed column,
/// a truncated value, or corruption confined to dead rows is caught. Any
/// mismatch aborts the swap (the caller keeps the original). Returns the copied
/// per-table counts for the report.
fn self_check(
    conn: &Connection,
    real_cols: &[(String, Vec<String>)],
) -> Result<Vec<(String, u64)>, DbError> {
    let mut counts = Vec::with_capacity(real_cols.len());
    for (table, cols) in real_cols {
        let hash_args = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
        // CAST the (unsigned) hash XOR to VARCHAR so it round-trips through a
        // String regardless of the duckdb-rs UBIGINT mapping.
        let probe = |rel: &str| -> Result<(i64, String), DbError> {
            let sql = format!(
                "SELECT count(*), CAST(COALESCE(bit_xor(hash({args})), 0) AS VARCHAR) FROM {rel}",
                args = hash_args,
                rel = rel,
            );
            Ok(conn.query_row(&sql, [], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?)
        };
        let (src_c, src_h) = probe(&format!("src.{}", q(table)))?;
        let (tmp_c, tmp_h) = probe(&q(table))?;
        if src_c != tmp_c || src_h != tmp_h {
            return Err(DbError::Api(format!(
                "compaction self-check FAILED for table {table:?}: \
                 source (count {src_c}, hash {src_h}) != temp (count {tmp_c}, hash {tmp_h}) — \
                 aborting swap, original left intact"
            )));
        }
        counts.push((table.clone(), tmp_c as u64));
    }
    Ok(counts)
}

// ---------------------------------------------------------------------------
// Temp-file naming, orphan sweep, and durability helpers (§3).
// ---------------------------------------------------------------------------

/// `<db-filename>.compact.<pid>.tmp`, a sibling in the SAME directory (so the
/// final `rename` is a same-filesystem atomic replace). The full DB filename
/// (with extension) is used as the prefix so two DBs that share a stem cannot
/// collide.
fn temp_path_for(db_path: &Path) -> PathBuf {
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let name = db_path.file_name().and_then(|n| n.to_str()).unwrap_or("db");
    dir.join(format!("{name}.compact.{}.tmp", std::process::id()))
}

/// The prefix a temp sibling of `db_path` starts with (for the orphan sweep).
fn temp_prefix(db_path: &Path) -> String {
    let name = db_path.file_name().and_then(|n| n.to_str()).unwrap_or("db");
    format!("{name}.compact.")
}

/// Delete any abandoned `<db-filename>.compact.*.tmp` (and matching `*.wal`)
/// siblings from a prior crashed run (§3 orphan cleanup). Best-effort: called
/// under the per-path lock, so no live temp of ours is ever swept.
///
/// The baseline sidecar (`<db-filename>.compaction-baseline`, R1) can never
/// match: its name begins `…compaction-baseline`, so `starts_with(prefix)` —
/// where `prefix` ends in the dot of `…compact.` — is false. The explicit
/// `compaction-baseline` exclusion below is belt-and-braces.
pub fn sweep_orphan_temps(db_path: &Path) {
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let prefix = temp_prefix(db_path);
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            if name.contains("compaction-baseline") {
                continue; // never touch the advisory baseline sidecar
            }
            if name.starts_with(&prefix) && (name.ends_with(".tmp") || name.ends_with(".tmp.wal")) {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

/// The byte-growth baseline sidecar for `db_path` (R1 CALIBRATION FIX):
/// `<db-filename>.compaction-baseline`, a sibling holding the plain-text byte
/// size of the DB file right after the last successful compaction. NOT stored
/// in the DB — a `metadata` row would need the write pipeline (invariant #2),
/// would mutate the `commit` log (breaking the verbatim-commit-log property the
/// editor's staleness cookie relies on), and would itself grow the file.
pub fn baseline_path(db_path: &Path) -> PathBuf {
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let name = db_path.file_name().and_then(|n| n.to_str()).unwrap_or("db");
    dir.join(format!("{name}.compaction-baseline"))
}

/// Read the baseline byte size. A missing / torn / unparseable sidecar yields
/// `None` (⇒ re-establish on the next [`compact_if_needed`]) — the sidecar is
/// advisory, never load-bearing for correctness.
fn read_baseline(db_path: &Path) -> Option<u64> {
    let text = fs::read_to_string(baseline_path(db_path)).ok()?;
    text.trim().parse::<u64>().ok()
}

/// Write the baseline atomically (temp sidecar + rename), best-effort. A failure
/// is swallowed — the worst case is one extra or skipped future compaction.
fn write_baseline(db_path: &Path, bytes: u64) {
    let final_path = baseline_path(db_path);
    // Distinct temp name (pid-tagged) that CANNOT match the orphan-temp sweep
    // (`…compaction-baseline.<pid>.tmp` does not start with `…compact.`).
    let tmp = {
        let mut s = final_path.as_os_str().to_os_string();
        s.push(format!(".{}.tmp", std::process::id()));
        PathBuf::from(s)
    };
    if fs::write(&tmp, bytes.to_string().as_bytes()).is_ok()
        && fs::rename(&tmp, &final_path).is_err()
    {
        let _ = fs::remove_file(&tmp);
    }
}

/// Remove a temp file and its WAL sidecar (abort cleanup).
fn remove_temp(temp_path: &Path) {
    let _ = fs::remove_file(temp_path);
    let mut wal = temp_path.as_os_str().to_os_string();
    wal.push(".wal");
    let _ = fs::remove_file(PathBuf::from(wal));
}

/// fsync a file's bytes to disk (durability before the rename, §3).
fn fsync_file(path: &Path) -> Result<(), DbError> {
    let f = fs::File::open(path).map_err(|source| DbError::SchemaDir {
        path: path.display().to_string(),
        source,
    })?;
    f.sync_all().map_err(|source| DbError::SchemaDir {
        path: path.display().to_string(),
        source,
    })
}

/// fsync a directory entry (POSIX durability step for the rename). Best-effort:
/// on non-Unix a directory handle cannot be fsync'd — the atomic-replace API
/// carries the durability guarantee there (design risk #2).
#[cfg(unix)]
fn fsync_dir(dir: &Path) {
    if let Ok(f) = fs::File::open(dir) {
        let _ = f.sync_all();
    }
}

#[cfg(not(unix))]
fn fsync_dir(_dir: &Path) {}

#[cfg(test)]
mod tests {
    //! In-module unit tests that need PRIVATE access: the generated-column
    //! drift guard (R3) and the self-check discrepancy guard (B3). End-to-end
    //! round-trip / sequence-continuity / schema-gate / crash-safety tests live
    //! in `tests/compaction.rs` (public API only).
    use super::*;
    use crate::schema_init::init_schema;

    fn fresh(dir: &tempfile::TempDir, name: &str) -> PathBuf {
        let db = dir.path().join(name);
        init_schema(&db).expect("init schema");
        db
    }

    /// R3: [`check_catalog`] fires when the generated-column map claims a column
    /// that does not exist on the live catalog (simulated drift).
    #[test]
    fn generated_column_guard_fires_on_drift() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh(&dir, "gen.duckdb");
        let conn = open_with_retry(&db, AccessMode::ReadWrite).unwrap();

        // The real map passes.
        let real = discover_real_columns(&conn).unwrap();
        check_catalog(&conn, &real).expect("real catalog passes");

        // Simulate schema drift by adding a base table the map does not know:
        // the base-table-set cross-check must reject it (an index depends on
        // every generated column, so a column cannot be dropped to simulate the
        // column-level case — R3 already notes the guard's name-existence
        // strength; the table-set arm is what fires here).
        conn.execute_batch("CREATE TABLE drift_extra (x BIGINT)")
            .expect("create extra table");
        let real2 = discover_real_columns(&conn).unwrap();
        let err = check_catalog(&conn, &real2).expect_err("drift must be rejected");
        assert!(
            format!("{err}").contains("catalog drift"),
            "unexpected error: {err}"
        );
        conn.close().unwrap();
    }

    /// B3: [`self_check`] catches a content discrepancy between source and temp
    /// (here: temp is missing a row the source has → the destructive-swap guard
    /// must refuse).
    #[test]
    fn self_check_catches_row_discrepancy() {
        let dir = tempfile::tempdir().unwrap();
        // "source": one extra alternative row beyond the temp.
        let src = fresh(&dir, "src.duckdb");
        {
            let c = open_with_retry(&src, AccessMode::ReadWrite).unwrap();
            c.execute_batch(
                "INSERT INTO \"commit\"(user, message) VALUES ('t','c'); \
                 INSERT INTO alternative(id, name, valid_from_commit) \
                 VALUES (nextval('seq_alternative'), 'Base', (SELECT max(id) FROM \"commit\"));",
            )
            .unwrap();
            c.close().unwrap();
        }
        // temp: empty (fresh schema, then wipe bootstrap like the real path).
        let temp = fresh(&dir, "temp.duckdb");
        let conn = open_with_retry(&temp, AccessMode::ReadWrite).unwrap();
        conn.execute_batch("DELETE FROM metadata; DELETE FROM \"commit\";")
            .unwrap();
        attach_source_ro(&conn, &src).unwrap();

        // Copy everything EXCEPT alternative → alternative differs.
        let real = discover_real_columns(&conn).unwrap();
        for (table, cols) in &real {
            if table == "alternative" {
                continue;
            }
            let list = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");
            let _ = conn.execute_batch(&format!(
                "INSERT INTO {t} ({list}) SELECT {list} FROM src.{t}",
                t = q(table)
            ));
        }
        let err = self_check(&conn, &real).expect_err("discrepancy must be caught");
        assert!(
            format!("{err}").contains("self-check FAILED"),
            "unexpected error: {err}"
        );
        conn.close().unwrap();
    }
}
