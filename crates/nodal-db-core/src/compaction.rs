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
//! The rebuild copy ORDER-BYs each table per `rebuild::CLUSTER_KEYS`: current-first on
//! versioned tables (closed rows grouped by close commit, D-E4 amended
//! 2026-10-24), key/natural order elsewhere. Content is untouched (the
//! order-independent self-check still covers every row); only physical layout
//! changes — see the `CLUSTER_KEYS` docs for the verified mechanism.
//!
//! ## Size guard (`specs/compaction_growth_FINDINGS.md` §5)
//! The rebuilt temp's on-disk size (after its `CHECKPOINT` and close, so its
//! WAL is empty) is compared with the source file's before the swap. When it
//! is **not smaller**:
//! - an **automatic** trigger ([`compact_if_due`] / [`compact_if_needed`]:
//!   no baseline, growth, commit count) keeps the original, deletes the temp
//!   and still resets the baseline (size and head), so the commit backstop
//!   waits another `N` commits instead of re-paying the repack on every save
//!   (`swapped == false`, [`CompactionReport::kept_original`]);
//! - an **explicit** [`compact`] swaps anyway (the caller asked for the
//!   repack) and reports it in [`CompactionReport::warning`].
//!
//! The kept-original path is the self-check-abort path: the original is never
//! written, so crash safety and invariant 3 are unchanged.
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

use crate::conn::{head_on, open_with_retry, path_lock};
use crate::error::DbError;
use crate::rebuild::{
    assert_temp_schema_version, attach_source_ro, check_catalog, cluster_order,
    discover_real_columns, fsync_dir, fsync_file, parent_dir, q, remove_temp, restore_sequences,
    SRC, TABLE_ORDER,
};
use crate::schema_init::init_schema;

/// Report of one repack. `swapped == false` marks either a gate no-op
/// ([`compact_if_needed`] / [`compact_if_due`] below their thresholds:
/// `trigger == None`, `rows_per_table` empty) or a kept original
/// (`kept_original == true`: a rebuild ran and was discarded). In both cases
/// the file is unchanged and `bytes_after` / `blocks_after` mirror the
/// "before" values.
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
    /// `false` ⇒ [`compact_if_needed`] / [`compact_if_due`] skipped under the
    /// threshold (or [`CompactionBackoff`] skipped it after a recent failure).
    pub swapped: bool,
    /// Why the repack ran; `None` when it did not (`swapped == false`).
    pub trigger: Option<CompactionTrigger>,
    /// `true` ⇒ [`CompactionBackoff`] skipped the gate entirely because a
    /// repack failed recently (no stat, no sidecar read, no DB open).
    pub backed_off: bool,
    /// `true` ⇒ an automatic trigger rebuilt the file, the rebuild was not
    /// smaller than the source, and the original was kept (the temp was
    /// deleted; the baseline was still reset). `swapped` is then `false`,
    /// `trigger` names the gate that fired, and `bytes_rebuilt` holds the
    /// discarded rebuild's size.
    pub kept_original: bool,
    /// On-disk bytes of the rebuilt temp, measured after its `CHECKPOINT`
    /// and close; `0` when nothing was rebuilt (a gate no-op).
    pub bytes_rebuilt: u64,
    /// Set when the rebuild was not smaller than the source: on a kept
    /// original (automatic trigger) and on an explicit [`compact`] that
    /// swapped anyway. Callers surface it (the CLI prints it to stderr).
    pub warning: Option<String>,
}

/// What made a repack run ([`CompactionReport::trigger`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionTrigger {
    /// An unconditional [`compact`] (the caller asked for it).
    Requested,
    /// No usable baseline: the sidecar is missing, torn, zero, or records a
    /// commit ABOVE the current head (a file restored or replaced under its
    /// old sidecar, plan C8 i). Compacting re-establishes it.
    NoBaseline,
    /// The byte-growth gate: the file reached `growth_factor` × its baseline.
    Growth,
    /// The commit-count backstop: at least `N` commits since the baseline.
    Commits,
}

/// The commit-count backstop's `N` (save_path_open_reduction plan §5, D5):
/// how many commits after the last compaction [`compact_if_due`] repacks
/// even when the byte gate has not tripped.
///
/// **Why a commit count.** Every RW load→checkpoint cycle adds a roughly
/// constant cost to every later open of the file (engine-FK catalog growth,
/// `commit_fixed_cost_FINDINGS.md`: ≈0.85-0.9 ms per commit per open), and
/// that growth barely moves the byte size, so the 3× byte gate almost never
/// trips on it. A repack resets it. The backstop works around a DuckDB
/// catalog growth issue, <https://github.com/duckdb/duckdb/issues/27808>;
/// revisit it when that is fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitBackstop {
    /// Byte gate only (the pre-backstop behavior of [`compact_if_needed`]).
    Off,
    /// A fixed `N`; `Fixed(0)` is the same as [`CommitBackstop::Off`].
    Fixed(u64),
    /// `N` scaled with the file size ([`size_scaled_max_commits`]): 50 for
    /// editor-sized files, up to 200 for very large ones.
    #[default]
    SizeScaled,
}

impl CommitBackstop {
    /// The effective `N` for a file of `bytes`, or `None` when disabled.
    pub fn threshold(self, bytes: u64) -> Option<u64> {
        match self {
            CommitBackstop::Off | CommitBackstop::Fixed(0) => None,
            CommitBackstop::Fixed(n) => Some(n),
            CommitBackstop::SizeScaled => Some(size_scaled_max_commits(bytes)),
        }
    }
}

/// The policy [`compact_if_due`] gates on. `Default` = byte growth
/// [`DEFAULT_GROWTH_FACTOR`] plus the size-scaled commit backstop.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionPolicy {
    pub growth_factor: f64,
    pub max_commits: CommitBackstop,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        CompactionPolicy {
            growth_factor: DEFAULT_GROWTH_FACTOR,
            max_commits: CommitBackstop::SizeScaled,
        }
    }
}

/// Floor of the size-scaled commit backstop (plan D5: N = 50, owner-approved).
pub const COMMIT_BACKSTOP_FLOOR: u64 = 50;
/// Cap of the size-scaled commit backstop (plan C9).
pub const COMMIT_BACKSTOP_CAP: u64 = 200;

/// The size-scaled `N` (plan C9/D5): the amortized-cost optimum
/// `N* = sqrt(2·C_est / g)`, clamped to
/// [`COMMIT_BACKSTOP_FLOOR`]..=[`COMMIT_BACKSTOP_CAP`].
///
/// - `g` = 1.75 ms: the per-commit open-cost growth an editor save pays (two
///   opens per save: the RW commit and the RO reload);
/// - `C_est` = 0.85 s + 19.7 ms/MiB: the repack's cost, fitted through the
///   measured 25 MiB (1.34 s) and 419 MiB (9.1 s) points.
///
/// Result: 50 up to ≈68 MiB, 57 at 100 MiB, ≈102 at 419 MiB, capped at 200
/// from ≈1.7 GiB. A pure function of the size the gate already `stat`s: no
/// extra I/O.
pub fn size_scaled_max_commits(bytes: u64) -> u64 {
    let mib = bytes as f64 / (1024.0 * 1024.0);
    let c_est_ms = 850.0 + 19.7 * mib;
    let n = (2.0 * c_est_ms / 1.75).sqrt().round() as u64;
    n.clamp(COMMIT_BACKSTOP_FLOOR, COMMIT_BACKSTOP_CAP)
}

/// How many commits a failed repack is skipped for by
/// [`CompactionBackoff`]: `max(5, N/5)` (plan C12), where `N` is the
/// backstop's effective threshold (the floor when the backstop is off).
pub fn failure_backoff_commits(n: u64) -> u64 {
    (n / 5).max(5)
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
/// data — no bump; the `erasure_marker` base table (0.3) changed the table set
/// — bump; the `array_id` surrogate key on `value_array` / `value_array_hist`
/// (this version) changed two tables' insertable columns — bump. Consequence of a bump (erasure_compaction_plan §9 step
/// 1): existing dev DBs refuse compaction until re-inited/re-imported —
/// pre-freeze, re-init IS the migration story.
pub const SCHEMA_VERSION: &str = "0.4-array-id";

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

/// Physical repack of `db_path` (Sense A). See the module docs. Always performs
/// the rewrite (no gate); use [`compact_if_due`] on the save path. On success
/// it refreshes the baseline sidecar to the new file size and head commit.
///
/// An explicit repack **always swaps**, even when the rebuild is not smaller
/// than the source (then [`CompactionReport::warning`] says so). Only the
/// automatic triggers keep the original on no gain (see the module docs).
pub fn compact(db_path: &Path) -> Result<CompactionReport, DbError> {
    compact_locked(db_path, CompactionTrigger::Requested)
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
///
/// The byte gate alone, kept for compatibility: it does not see the
/// per-commit open-cost growth. The save path, the CLI's `--if-needed` and
/// the skin's `compact(if_needed=True)` use [`compact_if_due`].
pub fn compact_if_needed(db_path: &Path, growth_factor: f64) -> Result<CompactionReport, DbError> {
    // Cheap gate FIRST — no lock, no DB open.
    let current = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let trigger = match read_baseline(db_path) {
        // No baseline (establish it on the first run), or a degenerate one.
        None | Some(Baseline { bytes: 0, .. }) => Some(CompactionTrigger::NoBaseline),
        Some(b) if current >= (b.bytes as f64 * growth_factor) as u64 => {
            Some(CompactionTrigger::Growth)
        }
        Some(_) => None,
    };
    match trigger {
        Some(t) => compact_locked(db_path, t),
        None => Ok(no_op_report(current)),
    }
}

/// The save-path gate (save_path_open_reduction plan §5.2): [`compact`] when
/// due under `policy`, else a no-op report (`swapped == false`). In order:
///
/// 1. no usable baseline (missing / torn / zero bytes) ⇒ compact;
/// 2. the byte gate (`current >= growth_factor × baseline bytes`, as
///    [`compact_if_needed`]) ⇒ compact;
/// 3. the commit backstop (`policy.max_commits`, threshold `N`):
///    - an old-format sidecar (bytes only, written by an older build) is
///      **adopted**: rewritten with the same bytes and `commit = head`, no
///      compaction (plan D6, C8 iii);
///    - `head < baseline commit` ⇒ the baseline belongs to another file
///      (restored or replaced under the sidecar) ⇒ compact (plan C8 i);
///    - `head − baseline commit >= N` ⇒ compact.
///
/// `head`: the caller's knowledge of the head commit. The editor passes the
/// commit id of the save it just made (`WriteReport::commit_id`), a lower
/// bound on the head: the gate can only fire late by the number of
/// concurrent commits, and the no-op path stays open-free (two `stat`s and a
/// sidecar read). `None` reads it with [`crate::api::head_commit`]: **one RO
/// open**, made only when the byte gate did not fire and the backstop is on
/// (fine for the CLI and the skin, not for a per-save path).
///
/// Every repack (from either gate, an unconditional [`compact`], or an
/// [`crate::extract::extract`]) records both the size and the head it copied
/// (read on the fresh file), so each gate resets the other. The commit count
/// is the `commit.id` delta, so it counts every writer's commits (editor,
/// skin, CLI, FlexTool): the growth is per RW cycle, whoever commits.
///
/// **A failed repack refreshes nothing**, so once the backstop is due every
/// later call retries the full repack (1-9 s) until the cause clears. A
/// caller on a per-save path must back off after a failure: hold a
/// [`CompactionBackoff`] (plan C12).
pub fn compact_if_due(
    db_path: &Path,
    policy: CompactionPolicy,
    head: Option<i64>,
) -> Result<CompactionReport, DbError> {
    let current = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let baseline = match read_baseline(db_path) {
        None | Some(Baseline { bytes: 0, .. }) => {
            return compact_locked(db_path, CompactionTrigger::NoBaseline)
        }
        Some(b) => b,
    };
    if current >= (baseline.bytes as f64 * policy.growth_factor) as u64 {
        return compact_locked(db_path, CompactionTrigger::Growth);
    }
    let Some(n) = policy.max_commits.threshold(current) else {
        return Ok(no_op_report(current));
    };
    let head = match head {
        Some(h) => h,
        None => crate::api::head_commit(db_path)?,
    };
    match baseline.commit {
        // Old-format sidecar: adopt, never compact on an upgrade.
        None => {
            write_baseline(db_path, baseline.bytes, head);
            Ok(no_op_report(current))
        }
        Some(c) if head < c => compact_locked(db_path, CompactionTrigger::NoBaseline),
        Some(c) if (head - c) as u64 >= n => compact_locked(db_path, CompactionTrigger::Commits),
        Some(_) => Ok(no_op_report(current)),
    }
}

/// In-memory failure back-off around [`compact_if_due`] for a per-save caller
/// (plan C12): after a repack fails at head `X`, calls are skipped (no
/// sidecar read, no open; `backed_off == true`) while
/// `X <= head < X + failure_backoff_commits(N)`, so a persistent failure
/// (disk full, a schema-version mismatch, a long reader holding the lock)
/// costs one repack attempt per back-off window, not one per save. Any
/// successful call clears it; so does a process restart (it is not
/// persisted). A head below `X` (the file was replaced) does not skip.
#[derive(Debug, Clone, Default)]
pub struct CompactionBackoff {
    failed_at: Option<i64>,
}

impl CompactionBackoff {
    pub fn new() -> Self {
        Self::default()
    }

    /// The head commit of the last failed attempt, if the back-off is armed.
    pub fn failed_at(&self) -> Option<i64> {
        self.failed_at
    }

    /// [`compact_if_due`] with `Some(head)`, unless a recent failure is
    /// still within its back-off window. An error is returned as is (and
    /// arms the back-off); the caller decides how to surface it.
    pub fn compact_if_due(
        &mut self,
        db_path: &Path,
        policy: CompactionPolicy,
        head: i64,
    ) -> Result<CompactionReport, DbError> {
        if let Some(x) = self.failed_at {
            let current = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
            let n = policy
                .max_commits
                .threshold(current)
                .unwrap_or(COMMIT_BACKSTOP_FLOOR);
            if head >= x && head < x + failure_backoff_commits(n) as i64 {
                return Ok(CompactionReport {
                    backed_off: true,
                    ..no_op_report(current)
                });
            }
        }
        match compact_if_due(db_path, policy, Some(head)) {
            Ok(r) => {
                self.failed_at = None;
                Ok(r)
            }
            Err(e) => {
                self.failed_at = Some(head);
                Err(e)
            }
        }
    }
}

/// The report of a gate that decided not to compact.
fn no_op_report(current: u64) -> CompactionReport {
    CompactionReport {
        bytes_before: current,
        bytes_after: current,
        // Not probed on the no-op path (no DB open, by design).
        blocks_before: 0,
        blocks_after: 0,
        rows_per_table: Vec::new(),
        swapped: false,
        trigger: None,
        backed_off: false,
        kept_original: false,
        bytes_rebuilt: 0,
        warning: None,
    }
}

/// The full locked repack: exclusive lock → orphan sweep → source probe (R2
/// schema gate + block metrics) → temp init + copy + self-check → atomic swap →
/// baseline sidecar refresh. Shared by [`compact`] and the fire paths of
/// [`compact_if_needed`] / [`compact_if_due`].
fn compact_locked(db_path: &Path, trigger: CompactionTrigger) -> Result<CompactionReport, DbError> {
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
        return Err(DbError::SchemaMismatch {
            message: format!(
                "compaction refused: source schema_version {src_version:?} != current \
                 {SCHEMA_VERSION:?} (compaction copies named columns against the current \
                 schema; to migrate, re-init a fresh database and re-import)"
            ),
            found: src_version,
            expected: SCHEMA_VERSION.to_string(),
        });
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

    let Repacked {
        rows_per_table,
        blocks_after,
        head,
    } = match outcome {
        Ok(v) => v,
        Err(e) => {
            // Abort: close, delete the temp (+ its WAL), keep the original.
            let _ = conn.close();
            remove_temp(&temp_path);
            return Err(e);
        }
    };

    // Close the temp handle BEFORE the swap (Windows caveat, §3).
    if let Err((_, e)) = conn.close() {
        remove_temp(&temp_path);
        return Err(DbError::Duckdb(e));
    }

    // ---- Size guard (compaction_growth_FINDINGS §5) ------------------------
    // Measured on disk after the temp's CHECKPOINT and close: its WAL is then
    // empty or gone, but it is counted anyway so a leftover WAL can never
    // make a rebuild look smaller than it is.
    let bytes_rebuilt = on_disk_bytes(&temp_path);
    let not_smaller = bytes_rebuilt >= bytes_before;
    let warning = not_smaller.then(|| {
        format!(
            "compaction did not shrink {}: rebuilt {bytes_rebuilt} bytes >= source \
             {bytes_before} bytes",
            db_path.display()
        )
    });
    if not_smaller && trigger != CompactionTrigger::Requested {
        // Automatic trigger, no gain: keep the original (the self-check-abort
        // path: the original is never written). Reset the baseline anyway, so
        // the commit backstop waits another N commits instead of retrying on
        // every save; the size is the original's, which is what stays.
        remove_temp(&temp_path);
        write_baseline(db_path, bytes_before, head);
        return Ok(CompactionReport {
            bytes_before,
            bytes_after: bytes_before,
            blocks_before,
            blocks_after: blocks_before,
            rows_per_table,
            swapped: false,
            trigger: Some(trigger),
            backed_off: false,
            kept_original: true,
            bytes_rebuilt,
            warning: warning.map(|w| format!("{w}; original kept")),
        });
    }

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
    let dir = parent_dir(db_path);
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
    // The head is the one read on the temp after the copy (plan C10), not
    // any caller's value: a commit that landed between the caller's commit
    // and this lock is counted.
    write_baseline(db_path, bytes_after, head);

    Ok(CompactionReport {
        bytes_before,
        bytes_after,
        blocks_before,
        blocks_after,
        rows_per_table,
        swapped: true,
        trigger: Some(trigger),
        backed_off: false,
        kept_original: false,
        bytes_rebuilt,
        warning: warning.map(|w| format!("{w}; swapped anyway (explicit compact)")),
    })
}

/// A database file's on-disk bytes: the main file plus its WAL, if any.
fn on_disk_bytes(path: &Path) -> u64 {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    let len = |p: &Path| fs::metadata(p).map(|m| m.len()).unwrap_or(0);
    len(path) + len(Path::new(&wal))
}

/// What [`repack_into`] produced: the per-table copied counts, the
/// post-`CHECKPOINT` block count, and the head commit of the copy.
struct Repacked {
    rows_per_table: Vec<(String, u64)>,
    blocks_after: u64,
    head: i64,
}

/// The copy + restore + self-check, all on `conn` (a fresh RW temp connection).
/// Returns the per-table copied counts, the post-`CHECKPOINT` block count,
/// and the head commit of the copy (for the baseline sidecar).
/// On ANY error the caller aborts and keeps the original (never swaps).
fn repack_into(conn: &Connection, src_path: &Path, temp_path: &Path) -> Result<Repacked, DbError> {
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
            "INSERT INTO {t} ({list}) SELECT {list} FROM {SRC}.{t}{order}",
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

    // The head the copy holds (the temp is the default catalog, so the
    // unqualified `commit` is the temp's): exact for the new file, since the
    // source's file lock was held through the copy.
    let head = head_on(conn)?;

    // Detach the source, then settle blocks and read the final count.
    conn.execute_batch(&format!("DETACH {SRC}"))?;
    conn.execute_batch("CHECKPOINT")?;
    let _ = temp_path; // (temp file identity is the caller's concern)
    let blocks_after: i64 =
        conn.query_row("SELECT total_blocks FROM pragma_database_size()", [], |r| {
            r.get(0)
        })?;

    Ok(Repacked {
        rows_per_table,
        blocks_after: blocks_after as u64,
        head,
    })
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
        let (src_c, src_h) = probe(&format!("{SRC}.{}", q(table)))?;
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
    let dir = parent_dir(db_path);
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

/// The compaction baseline sidecar for `db_path` (R1 CALIBRATION FIX; plan
/// §5.2): `<db-filename>.compaction-baseline`, a sibling recording the DB
/// file's byte size and its head commit right after the last successful
/// repack. NOT stored in the DB — a `metadata` row would need the write
/// pipeline (invariant #2), would mutate the `commit` log (breaking the
/// verbatim-commit-log property the editor's staleness cookie relies on),
/// and would itself grow the file.
///
/// Format (v2): two lines, `bytes=<u64>` and `commit=<i64>`. The v1 format
/// (written by older builds) is the bare byte size; it still parses, with
/// no commit, and [`compact_if_due`] adopts it. An older build reading a v2
/// sidecar fails to parse it, treats it as missing, compacts once and
/// rewrites v1; the new build then adopts that without compacting.
pub fn baseline_path(db_path: &Path) -> PathBuf {
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let name = db_path.file_name().and_then(|n| n.to_str()).unwrap_or("db");
    dir.join(format!("{name}.compaction-baseline"))
}

/// One parsed baseline sidecar ([`baseline_path`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Baseline {
    /// The DB file's byte size right after the last repack.
    pub bytes: u64,
    /// The head commit the repack copied; `None` for a v1 (bytes-only)
    /// sidecar written by an older build.
    pub commit: Option<i64>,
}

/// Read the baseline. A missing / torn / unparseable sidecar yields `None`
/// (⇒ re-establish on the next gated call) — the sidecar is advisory, never
/// load-bearing for correctness.
pub fn read_baseline(db_path: &Path) -> Option<Baseline> {
    let text = fs::read_to_string(baseline_path(db_path)).ok()?;
    parse_baseline(&text)
}

/// Parse v2 (`bytes=` / `commit=` lines, both required) or v1 (a bare u64).
fn parse_baseline(text: &str) -> Option<Baseline> {
    let text = text.trim();
    if let Ok(bytes) = text.parse::<u64>() {
        return Some(Baseline {
            bytes,
            commit: None,
        });
    }
    let (mut bytes, mut commit) = (None, None);
    for line in text.lines() {
        match line.trim().split_once('=') {
            Some(("bytes", v)) => bytes = Some(v.trim().parse::<u64>().ok()?),
            Some(("commit", v)) => commit = Some(v.trim().parse::<i64>().ok()?),
            _ => return None,
        }
    }
    Some(Baseline {
        bytes: bytes?,
        commit: Some(commit?),
    })
}

/// Write the v2 baseline atomically (temp sidecar + rename), best-effort. A
/// failure is swallowed — the worst case is one extra or skipped future
/// compaction. Both values are required, so every writer (the repack, the
/// extract, the adopt step) records the commit too (plan C8 ii).
pub(crate) fn write_baseline(db_path: &Path, bytes: u64, commit: i64) {
    let final_path = baseline_path(db_path);
    // Distinct temp name (pid-tagged) that CANNOT match the orphan-temp sweep
    // (`…compaction-baseline.<pid>.tmp` does not start with `…compact.`).
    let tmp = {
        let mut s = final_path.as_os_str().to_os_string();
        s.push(format!(".{}.tmp", std::process::id()));
        PathBuf::from(s)
    };
    let text = format!("bytes={bytes}\ncommit={commit}\n");
    if fs::write(&tmp, text.as_bytes()).is_ok() && fs::rename(&tmp, &final_path).is_err() {
        let _ = fs::remove_file(&tmp);
    }
}

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

    #[test]
    fn baseline_parses_v1_and_v2_and_rejects_torn() {
        assert_eq!(
            parse_baseline("123\n"),
            Some(Baseline {
                bytes: 123,
                commit: None
            })
        );
        assert_eq!(
            parse_baseline("bytes=123\ncommit=7\n"),
            Some(Baseline {
                bytes: 123,
                commit: Some(7)
            })
        );
        assert_eq!(parse_baseline("bytes=123\n"), None, "v2 needs both lines");
        assert_eq!(parse_baseline("bytes=12x\ncommit=7"), None);
        assert_eq!(parse_baseline(""), None);
        assert_eq!(parse_baseline("junk=1\nbytes=1\ncommit=1"), None);
    }

    /// Plan test 22: the size-scaled N is the floor for editor-sized files,
    /// grows past ≈68 MiB, is ≈100 at results scale, and is capped.
    #[test]
    fn size_scaled_n_floor_growth_and_cap() {
        const MIB: u64 = 1024 * 1024;
        for mib in [0, 1, 16, 25, 64] {
            assert_eq!(size_scaled_max_commits(mib * MIB), 50, "{mib} MiB");
        }
        assert_eq!(size_scaled_max_commits(100 * MIB), 57);
        assert_eq!(size_scaled_max_commits(419 * MIB), 102);
        assert_eq!(size_scaled_max_commits(2048 * MIB), 200);
        assert_eq!(size_scaled_max_commits(u64::MAX), 200);
        let mut prev = 0;
        for mib in (0..4096).step_by(16) {
            let n = size_scaled_max_commits(mib * MIB);
            assert!(n >= prev, "monotone");
            prev = n;
        }
        assert_eq!(CommitBackstop::Off.threshold(u64::MAX), None);
        assert_eq!(CommitBackstop::Fixed(0).threshold(1), None);
        assert_eq!(CommitBackstop::Fixed(3).threshold(u64::MAX), Some(3));
        assert_eq!(failure_backoff_commits(50), 10);
        assert_eq!(failure_backoff_commits(10), 5);
        assert_eq!(failure_backoff_commits(200), 40);
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
                "INSERT INTO {t} ({list}) SELECT {list} FROM {SRC}.{t}",
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
