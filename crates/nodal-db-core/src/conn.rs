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
//! retry loop the history protocol needs keys on the `Conflict` substring, not
//! a full phrase: DuckDB's wording depends on the operation, not the version
//! (`Conflict on update!` for an UPDATE, `Conflict on tuple deletion!` for a
//! DELETE; identical on 1.5.5 and 2.0.0).

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

/// Prepared-statement cache capacity of the write connection (see
/// `run_txn_once`).
const WRITE_STATEMENT_CACHE: usize = 64;

/// Process-global registry of per-path serialization locks (api critique F1).
static PATH_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

/// The canonical process-local identity of one DB path: the canonicalized
/// path, so symlink/relative aliases of one file share one key. For a
/// not-yet-created file the parent directory is canonicalized instead. The
/// key under [`path_lock`] AND the template module's in-flight-apply guard —
/// one canonicalization, so the two registries cannot disagree on identity.
pub(crate) fn canonical_path_key(db_path: &Path) -> PathBuf {
    db_path.canonicalize().unwrap_or_else(|_| {
        match (
            db_path.parent().and_then(|d| d.canonicalize().ok()),
            db_path.file_name(),
        ) {
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

/// The DuckDB configuration of every connection the core (and the CLI)
/// opens: the access mode plus the engine workarounds below. Every open goes
/// through this function, so a workaround cannot miss a path.
///
/// `debug_enable_caching_operators = false` works around a DuckDB 2.0
/// INTERNAL error ("dict-surviving cache: global dictionary column 0 changed
/// identity …") for a join over dense ids, then `unnest` of a list longer
/// than 2048 elements, then a selective filter: e.g. `read_parameter` on
/// 8760-long arrays with a time-window filter. The error invalidates the
/// whole database instance. DuckDB issue: pending upstream report; see
/// docs/dev/engine_rules.md §5 (bug, guard test, how to remove this).
/// The setting is debug-tagged: if a DuckDB release renames or drops it,
/// every open fails loudly here, which the upgrade checklist catches.
pub fn core_config(mode: AccessMode) -> Result<Config, duckdb::Error> {
    Config::default()
        .access_mode(mode)?
        .with("debug_enable_caching_operators", "false")
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
        let config = core_config(mode.clone())?;
        let before = file_stamp(db_path);
        match Connection::open_with_flags(db_path, config) {
            Ok(conn) => {
                // A file must not shadow a built-in (engine rule 8): before
                // any core SQL runs on it. On a refusal `conn` drops (closes).
                guard_builtins(&conn, db_path, before)?;
                return Ok(conn);
            }
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

/// The on-disk state of one database file, as the shadowing guard's cache
/// key: `(len, mtime, and on Unix dev, inode, ctime)` of the file and of its
/// `<db>.wal` (`None` when absent).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileStamp([Option<StatKey>; 2]);

#[derive(Clone, Debug, PartialEq, Eq)]
struct StatKey {
    len: u64,
    modified: Option<std::time::SystemTime>,
    #[cfg(unix)]
    unix: (u64, u64, i64, i64),
}

fn stat_key(path: &Path) -> Option<StatKey> {
    let m = std::fs::metadata(path).ok()?;
    Some(StatKey {
        len: m.len(),
        modified: m.modified().ok(),
        #[cfg(unix)]
        unix: {
            use std::os::unix::fs::MetadataExt;
            (m.dev(), m.ino(), m.ctime(), m.ctime_nsec())
        },
    })
}

/// `None` when the file itself cannot be stat'ed (then nothing is cached).
pub(crate) fn file_stamp(db_path: &Path) -> Option<FileStamp> {
    let mut wal = db_path.as_os_str().to_owned();
    wal.push(".wal");
    let db = stat_key(db_path)?;
    Some(FileStamp([Some(db), stat_key(Path::new(&wal))]))
}

/// Per-path state of the shadowing guard: the [`FileStamp`] the file had
/// when it was last found clean, and how many listings ran for it (the test
/// hook [`builtin_guard_listings`]).
#[derive(Default)]
struct GuardEntry {
    clean: Option<FileStamp>,
    listings: u64,
    /// Times a core commit re-stamped the file as clean ([`record_own_write`]).
    own_writes: u64,
}

/// The guard's cache, by [`canonical_path_key`]. Bounded: the core's own
/// temp files are forgotten when they go ([`forget_builtin_guard`]), and
/// past [`GUARD_CACHE_CAP`] paths the map starts over (a re-listing per
/// path, never a wrong answer).
static BUILTIN_GUARD: OnceLock<Mutex<HashMap<PathBuf, GuardEntry>>> = OnceLock::new();
const GUARD_CACHE_CAP: usize = 1024;

fn guard_cache() -> std::sync::MutexGuard<'static, HashMap<PathBuf, GuardEntry>> {
    BUILTIN_GUARD
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

fn store_clean(key: PathBuf, stamp: FileStamp) {
    let mut cache = guard_cache();
    if cache.len() >= GUARD_CACHE_CAP && !cache.contains_key(&key) {
        cache.clear();
    }
    cache.entry(key).or_default().clean = Some(stamp);
}

/// Drop `db_path` from the guard's cache: for the core's temp files
/// (compaction, extract), which are renamed away or removed.
pub(crate) fn forget_builtin_guard(db_path: &Path) {
    guard_cache().remove(&canonical_path_key(db_path));
}

/// Test hook: how many uncached listings the guard has run for `db_path`
/// in this process (a cached open adds none).
#[doc(hidden)]
pub fn builtin_guard_listings(db_path: &Path) -> u64 {
    guard_cache()
        .get(&canonical_path_key(db_path))
        .map_or(0, |e| e.listings)
}

/// Test hook: how many times a core commit re-stamped `db_path` as clean
/// (`record_own_write`) in this process.
#[doc(hidden)]
pub fn builtin_guard_own_writes(db_path: &Path) -> u64 {
    guard_cache()
        .get(&canonical_path_key(db_path))
        .map_or(0, |e| e.own_writes)
}

/// Refuse a database file that defines a macro shadowing a DuckDB built-in
/// function (engine rule 8, "A file must not shadow a built-in";
/// [`DbError::ShadowedBuiltin`]). Run by [`open_with_retry`] on every core
/// open, before any core SQL; `before` is the file's [`FileStamp`] taken
/// just before the open.
///
/// Why: DuckDB resolves an unqualified function name in the file's `main`
/// schema before the built-ins, on every later connection to the file
/// (read-only ones included) and inside the file's own generated columns,
/// indexes and views. A persisted `CREATE MACRO lower(x) AS …` silently
/// replaces the name fold everywhere (`cur_name`, its unique indexes, every
/// resolver), so the unique index stops catching duplicates and `types::fold`
/// no longer equals the SQL fold; `max`, `count`, `nextval`, `range`, `+`,
/// `||`, … are shadowed the same way. So every built-in name is guarded, not
/// a list.
///
/// Placement and cost: the catalog listing (`duckdb_functions()`, about
/// 3,200 rows) costs about 13 ms per call (release build, DuckDB 2.0.0,
/// measured 2026-11-03; the scan of the built-ins dominates and no filter is
/// pushed into it), against about 35 ms for a whole per-operation read
/// (`head_commit`: open, one query, close). Run on every open it would add
/// over a third to every read (invariant 1 makes opens per-operation), so
/// the answer is cached per process by path and file state ([`FileStamp`]):
/// an unchanged file costs a few `stat` calls. The stamp is taken before
/// and after the open and cached only when the two agree, so a change that
/// races the open is listed again next time. Any change to the file or its
/// WAL (another process's `CREATE MACRO`) re-runs the listing on the next
/// open. A commit through [`run_txn_once`] does not: [`stamp_own_write`] /
/// [`record_own_write`] re-stamp the file after it. That trade was measured
/// (release, 2026-11-03, medians of 200 single-value saves, 3 runs): +3.0 to
/// +5.3 ms per save (the explicit `CHECKPOINT`, about 3-5 %), -22 ms on the
/// next open (about 90 ms to 67 ms); the owner chose it. The core's other
/// writers (compaction and its WAL fold, extract, `schema_init`, the
/// read-surface refresh verb) do not re-stamp: the next open lists. A
/// refusal is never cached. The residual gap (recorded):
/// a change that leaves the size, mtime, inode and ctime of both files
/// equal, which needs a filesystem with coarse timestamps.
pub(crate) fn guard_builtins(
    conn: &Connection,
    db_path: &Path,
    before: Option<FileStamp>,
) -> Result<(), DbError> {
    let key = canonical_path_key(db_path);
    let after = file_stamp(db_path);
    if after.is_some() {
        let cache = guard_cache();
        if cache.get(&key).and_then(|e| e.clean.as_ref()) == after.as_ref() {
            return Ok(());
        }
    }
    let listed = check_no_shadowed_builtins(conn);
    guard_cache().entry(key.clone()).or_default().listings += 1;
    listed?;
    if let Some(after) = after.filter(|a| before.as_ref() == Some(a)) {
        store_clean(key, after);
    }
    Ok(())
}

/// After a committed write cycle on `conn`, before the close: `CHECKPOINT`
/// (so the close has nothing left to write) and return the file's stamp
/// then, for [`record_own_write`] after the close. `None` when the
/// checkpoint fails (nothing is recorded; the next open lists).
///
/// Why recording it as clean is sound: the guard passed at this
/// connection's open (cached or listed), the stamp is taken while this RW
/// connection holds the file's lock (no other process can change it), and
/// the core writes only its own rows and its own vetted DDL (the read
/// surface), never a user macro. [`record_own_write`] re-stats after the
/// close, so a change by anyone after the close is not recorded.
fn stamp_own_write(conn: &Connection, db_path: &Path) -> Option<FileStamp> {
    conn.execute_batch("CHECKPOINT").ok()?;
    file_stamp(db_path)
}

/// Record [`stamp_own_write`]'s stamp as clean once the connection is
/// closed, only if the file still has it (re-stat'ed now); otherwise leave
/// the cache as it is, and the next open lists.
fn record_own_write(db_path: &Path, stamp: Option<FileStamp>) {
    if let Some(stamp) = stamp {
        if file_stamp(db_path).as_ref() == Some(&stamp) {
            let key = canonical_path_key(db_path);
            store_clean(key.clone(), stamp);
            guard_cache().entry(key).or_default().own_writes += 1;
        }
    }
}

/// The uncached check of [`guard_builtins`], over every database attached to
/// `conn` (the rebuild's `ATTACH`ed source too: it reads it under `USE`):
/// a non-temporary macro in a `main` schema whose name is a built-in's of the
/// same family (a scalar macro against scalar, aggregate, window and macro
/// built-ins; a table macro against table functions and table macros) is
/// refused. Not counted as built-ins: pragmas (not called as functions) and
/// the comparison operators, which DuckDB binds as comparisons, never
/// through a macro of that name (verified on 2.0.0; `+`, `||`, `~~` (LIKE)
/// and `->>` are shadowed and stay guarded). Other schemas are not on
/// DuckDB's search path and are allowed; temporary objects are
/// connection-local, and the core never shares a connection. The listing is
/// read through `system.main.duckdb_functions()` (a file's own
/// `duckdb_functions` table macro cannot answer for it) and compared in
/// Rust, so no function the file could shadow takes part.
pub(crate) fn check_no_shadowed_builtins(conn: &Connection) -> Result<(), DbError> {
    use std::collections::HashSet;
    const NOT_SHADOWABLE: [&str; 9] = [
        "=",
        "!=",
        "<>",
        "<",
        "<=",
        ">",
        ">=",
        "is distinct from",
        "is not distinct from",
    ];
    let table_family = |kind: &str| matches!(kind, "table" | "table_macro");
    let mut builtins: HashSet<(bool, String)> = HashSet::new();
    let mut user: Vec<(String, String, String)> = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT database_name, schema_name, function_name, function_type, internal \
         FROM system.main.duckdb_functions()",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (database, schema): (String, String) = (row.get(0)?, row.get(1)?);
        let (name, kind, internal): (String, String, bool) =
            (row.get(2)?, row.get(3)?, row.get(4)?);
        // DuckDB matches catalog names ASCII-case-insensitively; these are
        // SQL function identifiers, not nodal-db names (engine rule 8).
        #[allow(clippy::disallowed_methods)]
        let folded = name.to_ascii_lowercase();
        if internal {
            if kind != "pragma" && !NOT_SHADOWABLE.contains(&folded.as_str()) {
                builtins.insert((table_family(&kind), folded));
            }
        } else if schema == "main" && database != "temp" {
            user.push((folded, name, kind));
        }
    }
    user.sort();
    for (folded, name, kind) in user {
        if builtins.contains(&(table_family(&kind), folded)) {
            return Err(DbError::ShadowedBuiltin {
                name,
                schema: "main".into(),
                kind: if table_family(&kind) {
                    "table macro".into()
                } else {
                    "macro".into()
                },
            });
        }
    }
    Ok(())
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
pub fn with_write_txn<T, F>(db_path: &Path, user: &str, message: &str, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection, i64) -> Result<T, DbError>,
{
    run_txn_once(db_path, user, message, TxnOptions::default(), |conn, c| {
        Ok((f(conn, c)?, TxnEnd::Commit))
    })
}

/// How a successful write cycle ends (skip_unchanged_writes_PLAN D1): a
/// closure that proved it wrote nothing asks for [`TxnEnd::Rollback`], so
/// the call leaves no `commit` row and the head (the polling cookie) does
/// not move. The `commit` row's sequence value stays consumed, as on a
/// conflict retry, so commit ids may skip. Only the `api` entry points
/// decide this (`commit_changes` and its wrappers); every other writer,
/// [`with_write_txn`] included, commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TxnEnd {
    Commit,
    Rollback,
}

/// Transaction-level options of one write cycle (save_path_open_reduction
/// plan §2.1): checks the transaction layer runs itself, so any writer can
/// use them, not only the `api` entry points.
///
/// `expect_commit = Some(c)`: an optimistic-concurrency precondition. Right
/// after `BEGIN` and **before** the `commit` row is inserted, the head
/// (`COALESCE(max(commit.id), 0)`) is read; if it is not exactly `c` the
/// transaction is rolled back and the call returns [`DbError::Stale`]. The
/// rule is `!=`, not `>`: a head *below* `c` means the file was replaced or
/// restored under the caller, which is also "not what I loaded". A `Stale`
/// writes nothing, inserts no commit row and uses no sequence value, and it
/// is never retried (it is not a `Conflict`).
///
/// Atomicity: the check and the write share one transaction under DuckDB's
/// exclusive file lock (cross-process) and the core's per-path lock
/// (in-process), so no commit can land between them. The guarantee holds
/// for **core-routed access only**: a second DuckDB instance opened in the
/// same process outside the core is invisible to both locks (fcntl locks are
/// per process; api critique F1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TxnOptions {
    pub expect_commit: Option<i64>,
}

/// The head commit id on an open connection: `COALESCE(max(id), 0)` over the
/// `commit` log (0 on an empty log). The one SQL spelling shared by
/// [`TxnOptions::expect_commit`], `api::head_commit`, the template's
/// staleness cursor and the compaction commit gate.
pub(crate) fn head_on(conn: &Connection) -> Result<i64, DbError> {
    Ok(
        conn.query_row("SELECT COALESCE(max(id), 0) FROM \"commit\"", [], |r| {
            r.get(0)
        })?,
    )
}

/// One open → BEGIN → `commit` row → closure → COMMIT → close cycle. Shared by
/// [`with_write_txn`] (once) and [`with_write_txn_retrying`] (per attempt); a
/// fresh RW connection and a fresh `commit` row are used every call, so a
/// rolled-back attempt leaves no orphan commit (VMP Part 5). A closure that
/// returns [`TxnEnd::Rollback`] ends the cycle with ROLLBACK instead of
/// COMMIT and still returns its value.
fn run_txn_once<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    txn: TxnOptions,
    f: F,
) -> Result<T, DbError>
where
    F: FnOnce(&Connection, i64) -> Result<(T, TxnEnd), DbError>,
{
    // In-process per-path serialization (critique F1): held across the whole
    // open-use-close so a sibling thread can never hold a second instance.
    require_db_file(db_path)?;
    let lock = path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = open_with_retry(db_path, AccessMode::ReadWrite)?;
    assert_min_version(&conn)?;
    // The engine's `lower()` must be the core's name fold (engine rule 8):
    // checked on the first write of the process, then a cached answer.
    crate::types::check_engine_fold(&conn)?;
    // An older read surface is replaced with this build's here, in its own
    // transaction before the caller's (a rollback of the caller's write must
    // not undo it, and the conflict loop must not repeat it). One version
    // read on a current file; never fails the write (read_surface docs).
    crate::read_surface::refresh_before_write(&conn, db_path);
    // The write path prepares its hot-path statements once per transaction
    // through this connection's statement cache (`prepare_cached`; the rare
    // paths, axis dedup lookups and the axis parent `INSERT … RETURNING` use
    // plain prepare). The history protocol alone has ~20 distinct cached
    // statements (3 live-tier probes, the 3 version scans, 4 close-out
    // variants, the scalar/long/array inserts, the array UPDATE/DELETE, 2
    // pre-image copies, the axis count), the axis writer 6 more (one child-row
    // insert per materialized kind), plus the api lookups; duckdb-rs's default LRU
    // capacity of 16 would thrash on a mixed commit and silently re-prepare
    // (appender_write_path_PLAN critique C11).
    conn.set_prepared_statement_cache_capacity(WRITE_STATEMENT_CACHE);
    // DuckDB's default `preserve_insertion_order` must stay on for this
    // connection: the columnar long insert relies on it to land a value's rows
    // in canonical cell order (series-ordered zone maps; critique C6).

    conn.execute_batch("BEGIN")?;

    // Set when the `commit` row insert itself fails: the cue to check (after
    // the rollback) whether the file is a nodal-db database at all.
    let mut commit_insert_failed = false;
    let result = (|| {
        // The staleness precondition (TxnOptions docs): after BEGIN, before
        // the commit row, so a refusal leaves no orphan commit and uses no
        // sequence value. A failing probe marks the commit-table check too:
        // on a non-nodal-db file it is the first statement to hit `commit`.
        if let Some(expected) = txn.expect_commit {
            let found = head_on(&conn).inspect_err(|_| commit_insert_failed = true)?;
            if found != expected {
                return Err(DbError::Stale { expected, found });
            }
        }
        let commit_id: i64 = conn
            .query_row(
                "INSERT INTO commit (user, message) VALUES (?, ?) RETURNING id",
                duckdb::params![user, message],
                |r| r.get(0),
            )
            .inspect_err(|_| commit_insert_failed = true)?;
        let (value, end) = f(&conn, commit_id)?;
        // COMMIT is inside the fallible block: DuckDB can raise the write-write
        // `Conflict` here (not only at the conflicting statement), and the retry
        // loop must see it.
        match end {
            TxnEnd::Commit => conn.execute_batch("COMMIT")?,
            TxnEnd::Rollback => conn.execute_batch("ROLLBACK")?,
        }
        Ok((value, end))
    })();

    match result {
        Ok((value, end)) => {
            // The shadowing guard's re-stamp after the core's own commit
            // (`stamp_own_write`): the next open of the file then needs no
            // catalog listing. A rolled-back cycle (nothing written) leaves
            // the stamp cached at the open as it is.
            let stamp = (end == TxnEnd::Commit)
                .then(|| stamp_own_write(&conn, db_path))
                .flatten();
            conn.close().map_err(|(_, e)| DbError::Duckdb(e))?;
            record_own_write(db_path, stamp);
            Ok(value)
        }
        Err(e) => {
            // Best-effort rollback; propagate the original error either way.
            let _ = conn.execute_batch("ROLLBACK");
            // Probe only on this failure path (a successful insert proves the
            // table exists), and only after the rollback: the failed statement
            // leaves the transaction aborted, which would reject the probe.
            let e = if commit_insert_failed && !has_commit_table(&conn) {
                DbError::Api(format!(
                    "{:?} is not an initialized nodal-db database (no `commit` table); \
                     create it with init first",
                    db_path.display().to_string()
                ))
            } else if let (DbError::Duckdb(_), Some(found)) = (&e, foreign_schema_version(&conn)) {
                // An engine error (typically a Binder error on a column this
                // build's statements expect) on a file of another schema
                // version: say so instead of surfacing the raw binder text.
                let expected = crate::compaction::SCHEMA_VERSION;
                DbError::SchemaMismatch {
                    message: format!(
                        "database schema version {found:?} is not this nodal-db's \
                         ({expected:?}); re-create the database (init + import) with this \
                         nodal-db, or use a nodal-db build matching its schema version \
                         (original error: {e})"
                    ),
                    found,
                    expected: expected.to_string(),
                }
            } else {
                e
            };
            let _ = conn.close();
            Err(e)
        }
    }
}

/// Refuse a missing database path BEFORE any open: DuckDB's RW open creates
/// the file, which would leave an empty database behind on a typo'd path.
/// Mirrors the Python skin's `open` check. (Not in `open_with_retry` itself:
/// compaction deliberately creates its temp file through it.)
fn require_db_file(db_path: &Path) -> Result<(), DbError> {
    if db_path.is_file() {
        return Ok(());
    }
    Err(DbError::Api(format!(
        "no database file at {:?} (create one with init first)",
        db_path.display().to_string()
    )))
}

/// Does the open database have nodal-db's `commit` table? A probe error
/// counts as "present", so the caller keeps the original error rather than
/// masking it.
fn has_commit_table(conn: &Connection) -> bool {
    conn.query_row(
        "SELECT count(*) FROM duckdb_tables() \
         WHERE database_name = current_database() AND schema_name = 'main' \
           AND table_name = 'commit'",
        [],
        |r| r.get::<_, i64>(0),
    )
    .map(|n| n > 0)
    .unwrap_or(true)
}

/// The file's current `schema_version` metadata value when it differs from
/// this build's [`crate::compaction::SCHEMA_VERSION`]; `None` when it matches
/// or the probe fails (the caller then keeps the original error). Only run on
/// a write failure, after the rollback.
fn foreign_schema_version(conn: &Connection) -> Option<String> {
    let found: String = conn
        .query_row(
            "SELECT value FROM metadata \
             WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )
        .ok()?;
    (found != crate::compaction::SCHEMA_VERSION).then_some(found)
}

/// Is this a DuckDB write-write conflict (VMP Part 5 retry trigger)? Matched on
/// the robust `Conflict` substring, not the full phrase: the wording depends on
/// the operation (`Conflict on update!` for an UPDATE, `Conflict on tuple
/// deletion!` for a DELETE; identical on DuckDB 1.5.5 and 2.0.0).
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
    with_write_txn_retrying_opts(db_path, user, message, TxnOptions::default(), f)
}

/// [`with_write_txn_retrying`] with transaction-level options ([`TxnOptions`]).
/// Every attempt re-runs the checks, so a conflict retry re-checks
/// `expect_commit` against the head inside its own fresh transaction. A
/// [`DbError::Stale`] is returned on the attempt that sees it, never retried.
pub fn with_write_txn_retrying_opts<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    txn: TxnOptions,
    f: F,
) -> Result<T, DbError>
where
    F: Fn(&Connection, i64) -> Result<T, DbError>,
{
    with_write_txn_retrying_ends(db_path, user, message, txn, |conn, c| {
        Ok((f(conn, c)?, TxnEnd::Commit))
    })
}

/// [`with_write_txn_retrying_opts`] for a closure that also says how its
/// cycle ends ([`TxnEnd`]): the `api` entry points, which roll back a call
/// that wrote nothing (skip_unchanged_writes_PLAN D1).
pub(crate) fn with_write_txn_retrying_ends<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    txn: TxnOptions,
    f: F,
) -> Result<T, DbError>
where
    F: Fn(&Connection, i64) -> Result<(T, TxnEnd), DbError>,
{
    for attempt in 1..=MAX_CONFLICT_ATTEMPTS {
        match run_txn_once(db_path, user, message, txn, &f) {
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
///
/// The read surface (`read_surface` module): the file's version is read
/// first (one query). A file of another contract major is read through
/// temporary copies of this build's views and macros (installed before
/// `f`); within one major the file's own surface is used. When `f` fails on
/// a missing view or macro of a stale file, the error becomes
/// [`DbError::ReadSurfaceStale`] (`f` is `FnOnce`, so it cannot be retried;
/// [`with_read_retrying`] retries once through the copies instead). A stale
/// file is noticed once per process (`read_surface::take_notices`).
pub fn with_read<T, F>(db_path: &Path, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection) -> Result<T, DbError>,
{
    use crate::read_surface::{map_read_error, prepare_read, ReadScope};
    with_read_plain(db_path, |conn| {
        let state = prepare_read(conn, db_path, ReadScope::Generic)?;
        f(conn).map_err(|e| map_read_error(e, &state))
    })
}

/// [`with_read`] for a re-runnable closure: when `f` fails on a missing view
/// or macro of a stale file, the full set of this build's surface objects
/// is installed as temporary copies and `f` runs once more (Python
/// `db.query`). Never overlays a connection that `f` attached another
/// database to: the error is returned instead.
pub fn with_read_retrying<T, F>(db_path: &Path, f: F) -> Result<T, DbError>
where
    F: Fn(&Connection) -> Result<T, DbError>,
{
    use crate::read_surface::{
        attaches_other_database, install_overlay, is_missing_object, map_read_error, prepare_read,
        ReadScope,
    };
    with_read_plain(db_path, |conn| {
        let state = prepare_read(conn, db_path, ReadScope::Generic)?;
        match f(conn) {
            Err(e)
                if !state.overlaid
                    && state.check.is_stale()
                    && is_missing_object(&e)
                    && !attaches_other_database(conn).unwrap_or(true) =>
            {
                if install_overlay(conn, None).is_err() {
                    return Err(map_read_error(e, &state));
                }
                f(conn).map_err(|e| map_read_error(e, &state))
            }
            r => r.map_err(|e| map_read_error(e, &state)),
        }
    })
}

/// [`with_read`] for one of the core's named reads (`api::read_parameter`,
/// `api::read_values`): on a stale file, or one of another contract major,
/// only the dependency closure of `objects` is installed as temporary copies
/// of this build's (a dozen objects, ~15 ms, instead of the whole surface).
pub(crate) fn with_read_named<T, F>(db_path: &Path, objects: &[&str], f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection) -> Result<T, DbError>,
{
    use crate::read_surface::{map_read_error, prepare_read, ReadScope};
    with_read_plain(db_path, |conn| {
        let state = prepare_read(conn, db_path, ReadScope::Named(objects))?;
        f(conn).map_err(|e| map_read_error(e, &state))
    })
}

/// One read-write connection for work that is not a commit (the read-surface
/// refresh verb): open (bounded-backoff retry), version gate, `f`, close,
/// under the per-path lock. `f` manages its own transactions.
pub(crate) fn with_rw_conn<T, F>(db_path: &Path, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection) -> Result<T, DbError>,
{
    require_db_file(db_path)?;
    let lock = path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = open_with_retry(db_path, AccessMode::ReadWrite)?;
    let result = assert_min_version(&conn).and_then(|()| f(&conn));
    match conn.close() {
        Ok(()) => result,
        Err((_, e)) => result.and(Err(DbError::Duckdb(e))),
    }
}

/// [`with_read`] without the read-surface step: for the core's own reads of
/// base tables (lookups, previews, the commit head), which use no view or
/// macro and so need no version read.
pub(crate) fn with_read_plain<T, F>(db_path: &Path, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection) -> Result<T, DbError>,
{
    // Reads serialize too (critique F1): an in-process RO instance open during
    // a write has the same dual-instance hazard as a second writer.
    require_db_file(db_path)?;
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
