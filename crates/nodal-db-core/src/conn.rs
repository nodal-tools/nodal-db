//! Connection discipline (VMP Part 6; CLAUDE.md invariant 1).
//!
//! DuckDB's lock model is symmetric: while any read-write connection is open,
//! a second process's RW *and* read-only connects both fail
//! (`IOException: Could not set lock`); and an idle read-only handle equally
//! starves a writer's RW acquisition. The consequence is non-negotiable —
//! **no long-lived connections anywhere.** Every helper here is open → work →
//! close, with the RW lock held only for the span of one commit.
//!
//! - [`with_write_txn`] opens RW (bounded-backoff retry on the lock error),
//!   `BEGIN`, inserts the `commit` row as the validity edge (VMP Part 5),
//!   runs the closure, `COMMIT`, and closes. No `CHECKPOINT` yet — a future
//!   option once churn/compaction lands (VMP Part 5).
//! - [`with_read`] opens a fresh read-only connection, runs, and closes; the
//!   commit-cookie poll (#2252) is itself one such per-operation read.
//! - **In-process serialization (api critique F1):** DuckDB's file lock
//!   excludes other *processes* only — two threads of one process opening the
//!   same file get two independent database instances whose commits silently
//!   diverge (lost or chimerically merged writes). Every open-use-close in
//!   this library therefore first takes a process-global per-path lock
//!   ([`path_lock`]), mirroring in-process the exclusion the file lock gives
//!   cross-process. Connections stay per-operation — this serializes them,
//!   it never shares them.
//!
//! NOTE for Phase 3 (documented, not implemented here): the write-conflict
//! retry loop the history protocol needs must match the DuckDB **1.5.5**
//! wording `Conflict on tuple deletion!` (1.4.4 said `Conflict on update!`),
//! so a robust matcher keys on the `Conflict` substring, not the full phrase.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::sleep;
use std::time::Duration;

use duckdb::{AccessMode, Config, Connection};

use crate::error::DbError;
use crate::MINIMUM_DUCKDB;

/// Bounded-backoff budget for a busy lock. Eight attempts, 20 ms doubling to a
/// 500 ms cap (~1.9 s worst case) — enough to ride out a short commit; a real
/// solve-length write is the launching GUI's serialization cost (VMP Part 6).
const MAX_ATTEMPTS: u32 = 8;
const BACKOFF_START: Duration = Duration::from_millis(20);
const BACKOFF_CAP: Duration = Duration::from_millis(500);

/// Process-global registry of per-path serialization locks (api critique F1).
static PATH_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

/// The canonical process-local identity of one DB path: the canonicalized
/// path, so symlink/relative aliases of one file share one key. For a
/// not-yet-created file the parent directory is canonicalized instead. The
/// key under [`path_lock`] AND the template module's in-flight-apply guard —
/// one canonicalization, so the two registries cannot disagree on identity.
pub(crate) fn canonical_path_key(db_path: &Path) -> PathBuf {
    db_path.canonicalize().unwrap_or_else(|_| {
        match (db_path.parent().and_then(|d| d.canonicalize().ok()), db_path.file_name()) {
            (Some(dir), Some(name)) => dir.join(name),
            _ => db_path.to_path_buf(),
        }
    })
}

/// The in-process serialization lock for one DB path, keyed by
/// [`canonical_path_key`].
pub(crate) fn path_lock(db_path: &Path) -> Arc<Mutex<()>> {
    let key = canonical_path_key(db_path);
    let mut map = PATH_LOCKS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    map.entry(key).or_default().clone()
}

/// Is this the DuckDB "another connection holds the lock" error? Matched on the
/// stable substring (`Could not set lock`) that both the RW-vs-RW and the
/// RW-vs-RO cases share.
fn is_lock_busy(e: &duckdb::Error) -> bool {
    e.to_string().contains("Could not set lock")
}

/// Open a connection in the given access mode, retrying with bounded backoff
/// while the file is locked by another connection (surfaced as [`DbError::Busy`]).
/// `Config` is not `Clone`, so it is rebuilt per attempt (cheap).
///
/// `pub(crate)` so [`crate::compaction`] can open the source read-only itself
/// (it already holds [`path_lock`] for the whole repack and must NOT re-take it
/// via [`with_read`], which would deadlock the non-reentrant per-path mutex).
pub(crate) fn open_with_retry(db_path: &Path, mode: AccessMode) -> Result<Connection, DbError> {
    let mut backoff = BACKOFF_START;
    for attempt in 1..=MAX_ATTEMPTS {
        let config = Config::default().access_mode(mode.clone())?;
        match Connection::open_with_flags(db_path, config) {
            Ok(conn) => return Ok(conn),
            Err(e) if is_lock_busy(&e) => {
                if attempt == MAX_ATTEMPTS {
                    return Err(DbError::Busy {
                        attempts: MAX_ATTEMPTS,
                    });
                }
                sleep(backoff);
                backoff = (backoff * 2).min(BACKOFF_CAP);
            }
            Err(e) => return Err(DbError::Duckdb(e)),
        }
    }
    unreachable!("loop returns on the final attempt")
}

/// Assert the bundled DuckDB is >= the line the schema was verified on (mirrors
/// `build.py`'s `MINIMUM_DUCKDB`; risk R1). Fails fast at connection open.
fn assert_min_version(conn: &Connection) -> Result<(), DbError> {
    let raw: String = conn.query_row("SELECT version()", [], |r| r.get(0))?;
    let trimmed = raw.trim_start_matches('v');
    let mut it = trimmed.split('.');
    let major: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u32 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    if (major, minor) < MINIMUM_DUCKDB {
        return Err(DbError::VersionTooOld { found: raw });
    }
    Ok(())
}

/// Open read-write for exactly one commit, run `f`, commit, and close.
///
/// Sequence: RW open (bounded-backoff retry on a busy lock) → version gate →
/// `BEGIN` → insert the `commit(user, message)` row `RETURNING id` → `f(conn,
/// commit_id)` → `COMMIT` → close. On any error from `f` (or `COMMIT`) the
/// transaction is rolled back and the connection is still closed, so the RW
/// lock is never leaked. `commit_id` is the validity edge Phase 3's history
/// writes stamp onto rows (VMP Part 5); axis writes ignore it (axes are
/// immutable and carry no validity span).
pub fn with_write_txn<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    f: F,
) -> Result<T, DbError>
where
    F: FnOnce(&Connection, i64) -> Result<T, DbError>,
{
    run_txn_once(db_path, user, message, f)
}

/// One open → BEGIN → `commit` row → closure → COMMIT → close cycle. Shared by
/// [`with_write_txn`] (once) and [`with_write_txn_retrying`] (per attempt); a
/// fresh RW connection and a fresh `commit` row are used every call, so a
/// rolled-back attempt leaves no orphan commit (VMP Part 5).
fn run_txn_once<T, F>(db_path: &Path, user: &str, message: &str, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection, i64) -> Result<T, DbError>,
{
    // In-process per-path serialization (critique F1): held across the whole
    // open-use-close so a sibling thread can never hold a second instance.
    let lock = path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = open_with_retry(db_path, AccessMode::ReadWrite)?;
    assert_min_version(&conn)?;

    conn.execute_batch("BEGIN")?;

    let result = (|| {
        let commit_id: i64 = conn.query_row(
            "INSERT INTO commit (user, message) VALUES (?, ?) RETURNING id",
            duckdb::params![user, message],
            |r| r.get(0),
        )?;
        let value = f(&conn, commit_id)?;
        // COMMIT is inside the fallible block: DuckDB can raise the write-write
        // `Conflict` here (not only at the conflicting statement), and the retry
        // loop must see it.
        conn.execute_batch("COMMIT")?;
        Ok(value)
    })();

    match result {
        Ok(value) => {
            conn.close().map_err(|(_, e)| DbError::Duckdb(e))?;
            Ok(value)
        }
        Err(e) => {
            // Best-effort rollback; propagate the original error either way.
            let _ = conn.execute_batch("ROLLBACK");
            let _ = conn.close();
            Err(e)
        }
    }
}

/// Is this a DuckDB write-write conflict (VMP Part 5 retry trigger)? Matched on
/// the robust `Conflict` substring, not the full phrase: DuckDB 1.5.5 says
/// `Conflict on tuple deletion!`, 1.4.4 said `Conflict on update!`.
fn is_conflict(e: &DbError) -> bool {
    matches!(e, DbError::Duckdb(_)) && e.to_string().contains("Conflict")
}

/// Bounded retry budget for the write-write conflict loop (VMP Part 5).
const MAX_CONFLICT_ATTEMPTS: u32 = 5;

/// Like [`with_write_txn`], but re-runs the *whole* closure from scratch on a
/// write-write conflict (VMP Part 5). Each attempt opens a fresh transaction and
/// a fresh `commit` row, so the closure re-reads every piece of state it depends
/// on — current versions, array pre-images, axis lookups — inside the new
/// transaction; nothing read before the conflict is reused. Hence `F: Fn` (must
/// be re-runnable), not `FnOnce`. After [`MAX_CONFLICT_ATTEMPTS`] the conflict is
/// surfaced as [`DbError::Conflict`]; any non-conflict error returns immediately.
pub fn with_write_txn_retrying<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    f: F,
) -> Result<T, DbError>
where
    F: Fn(&Connection, i64) -> Result<T, DbError>,
{
    for attempt in 1..=MAX_CONFLICT_ATTEMPTS {
        match run_txn_once(db_path, user, message, &f) {
            Ok(value) => return Ok(value),
            Err(e) if is_conflict(&e) => {
                if attempt == MAX_CONFLICT_ATTEMPTS {
                    return Err(DbError::Conflict {
                        attempts: MAX_CONFLICT_ATTEMPTS,
                    });
                }
                // Roll back happened in run_txn_once; loop re-executes fresh.
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("loop returns on the final attempt")
}

/// Open a fresh read-only connection, run `f`, and close. Per-operation only —
/// the handle must not outlive the call (CLAUDE.md invariant 1).
pub fn with_read<T, F>(db_path: &Path, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection) -> Result<T, DbError>,
{
    // Reads serialize too (critique F1): an in-process RO instance open during
    // a write has the same dual-instance hazard as a second writer.
    let lock = path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = open_with_retry(db_path, AccessMode::ReadOnly)?;
    let result = f(&conn);
    // Close regardless of the closure outcome so no RO handle lingers.
    match conn.close() {
        Ok(()) => result,
        Err((_, e)) => result.and(Err(DbError::Duckdb(e))),
    }
}
