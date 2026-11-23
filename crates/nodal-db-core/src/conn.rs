//! Connection discipline: every connection is per operation.
//!
//! DuckDB's lock model is symmetric: while any read-write connection is open,
//! a second process's RW *and* read-only connects both fail
//! (`IOException: Could not set lock`); and an idle read-only handle equally
//! starves a writer's RW acquisition. The consequence is non-negotiable —
//! **no long-lived connections anywhere.** Every helper here is open → work →
//! close, with the RW lock held only for the span of one commit.
//!
//! - [`with_write_txn`] opens RW (bounded-backoff retry on the lock error),
//!   `BEGIN`, inserts the `commit` row as the validity edge, runs the closure,
//!   `COMMIT`, and closes. The core's own write cycles also `CHECKPOINT`
//!   before the close, for the shadowing guard's re-stamp (engine rule 8);
//!   the public closure family does not.
//! - [`with_read`] opens a fresh read-only connection, runs, and closes; a
//!   poll of the head commit (the change cookie) is itself one such
//!   per-operation read.
//! - **In-process serialization:** DuckDB's file lock excludes other
//!   *processes* only — two threads of one process opening the same file get
//!   two independent database instances whose commits silently diverge (lost
//!   or chimerically merged writes). Every open-use-close in this library
//!   therefore first takes a process-global per-path lock, mirroring
//!   in-process the exclusion the file lock gives cross-process. Connections
//!   stay per-operation — this serializes them, it never shares them.
//!
//! The write-conflict retry loop keys on the `Conflict` substring, not a full
//! phrase: DuckDB's wording depends on the operation, not the version
//! (`Conflict on update!` for an UPDATE, `Conflict on tuple deletion!` for a
//! DELETE; identical on 1.5.5 and 2.0.0).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::sleep;
use std::time::Duration;

use duckdb::{AccessMode, Config, Connection, OptionalExt};

use crate::error::DbError;
use crate::MINIMUM_DUCKDB;

/// Bounded-backoff budget for a busy lock. Eight attempts, 20 ms doubling to a
/// 500 ms cap (~1.9 s worst case) — enough to ride out a short commit; a real
/// solve-length write is the launching GUI's serialization cost.
const MAX_ATTEMPTS: u32 = 8;
const BACKOFF_START: Duration = Duration::from_millis(20);
const BACKOFF_CAP: Duration = Duration::from_millis(500);

/// Prepared-statement cache capacity of the write connection (see
/// `run_txn_once`).
const WRITE_STATEMENT_CACHE: usize = 64;

/// Process-global registry of per-path serialization locks.
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
/// whole database instance. Upstream fix: duckdb/duckdb PR #27979
/// (<https://github.com/duckdb/duckdb/pull/27979>, open); see
/// `docs/dev/engine_rules.md` (the bug, its guard test, how to remove this).
/// The setting is debug-tagged: if a DuckDB release renames or drops it,
/// every open fails loudly here, which the upgrade checklist catches.
///
/// **Lock-down (engine rule 12, "A file's SQL runs without file, network or
/// extension access").** A database file carries its own views and macros,
/// and the core calls some of them by name on every open
/// (`nodal_read_surface()`, the read macros), so a file from someone else
/// runs its own SQL inside the core's process. Every core connection is
/// therefore opened with DuckDB's external access off (no file reads or
/// writes outside the database itself, no `glob`, no HTTP), extension
/// autoload and autoinstall off (no downloads), persistent secrets off and
/// community extensions off, and its configuration is locked right after
/// the open (`open_with_retry`), so no SQL on it can turn any of that back
/// on. The database file, its WAL and DuckDB's spill directory stay usable.
/// The one exception is [`Access::Trusted`], an explicit per-call opt-in
/// for a caller's own query on a file it trusts.
pub fn core_config(mode: AccessMode) -> Result<Config, duckdb::Error> {
    config_for(mode, Lockdown::Locked)
}

/// How much of DuckDB's file-system, network and extension machinery a
/// connection that runs a caller's own SQL may use ([`with_read_retrying_as`],
/// Python `db.query(..., trusted=)`). Every other core connection is
/// [`Access::Locked`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Access {
    /// External access, extension autoload/autoinstall, persistent secrets
    /// and community extensions off; configuration locked (the default; see
    /// [`core_config`]).
    #[default]
    Locked,
    /// DuckDB's own defaults (file and network access, extension autoload
    /// and autoinstall, persistent secrets), configuration not locked. The
    /// file's own views and macros then also run with that access: only
    /// for files you trust.
    Trusted,
}

/// The lock-down a core open applies ([`config_for`], [`lock_down`]).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Lockdown<'a> {
    /// Every core connection: external access off from the config on.
    Locked,
    /// The rebuild's temp connection (compaction, extract), which then
    /// `ATTACH`es the source: DuckDB refuses `allowed_paths` at config time
    /// and once external access is off, so the connection opens with access
    /// on (autoload, autoinstall and secrets already off), then allows
    /// exactly these paths, turns access off and locks, before any SQL of
    /// the file runs (verified on DuckDB 2.0.0).
    Attach(&'a str),
    /// [`Access::Trusted`].
    Trusted,
}

/// The [`Config`] of one core open under `lockdown` (see [`core_config`]).
pub(crate) fn config_for(
    mode: AccessMode,
    lockdown: Lockdown<'_>,
) -> Result<Config, duckdb::Error> {
    let mut config = Config::default()
        .access_mode(mode)?
        .with("debug_enable_caching_operators", "false")?;
    match lockdown {
        Lockdown::Locked | Lockdown::Attach(_) => {
            let external = if matches!(lockdown, Lockdown::Locked) {
                "false"
            } else {
                "true"
            };
            config = config
                .with("enable_external_access", external)?
                .with("autoload_known_extensions", "false")?
                .with("autoinstall_known_extensions", "false")?
                .with("allow_persistent_secrets", "false")?
                .with("allow_community_extensions", "false")?;
        }
        Lockdown::Trusted => {}
    }
    let limits = resource_limits();
    if let Some(m) = &limits.memory_limit {
        config = config.with("memory_limit", m)?;
    }
    if let Some(t) = limits.threads {
        config = config.with("threads", t.to_string())?;
    }
    Ok(config)
}

/// Right after a core open, before any SQL of the file runs: finish the
/// lock-down of [`config_for`] (the `ATTACH` path's ordered settings) and
/// lock the configuration. Nothing for [`Lockdown::Trusted`].
fn lock_down(conn: &Connection, lockdown: Lockdown<'_>) -> Result<(), duckdb::Error> {
    match lockdown {
        Lockdown::Trusted => return Ok(()),
        Lockdown::Locked => {}
        Lockdown::Attach(src) => {
            let lit = |p: &str| format!("'{}'", p.replace('\'', "''"));
            conn.execute_batch(&format!(
                "SET allowed_paths = [{}, {}]; SET enable_external_access = false",
                lit(src),
                lit(&format!("{src}.wal"))
            ))?;
        }
    }
    conn.execute_batch("SET lock_configuration = true")
}

/// DuckDB's memory and thread caps for every connection this process opens
/// ([`set_resource_limits`]). `None` leaves DuckDB's own default (80 % of
/// RAM, every core; or the SLURM allocation when `SLURM_*` is set).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceLimits {
    /// A DuckDB `memory_limit` value (`"4GiB"`, `"50%"`).
    pub memory_limit: Option<String>,
    /// DuckDB `threads`.
    pub threads: Option<u64>,
}

static LIMITS: std::sync::RwLock<ResourceLimits> = std::sync::RwLock::new(ResourceLimits {
    memory_limit: None,
    threads: None,
});

/// Set the [`ResourceLimits`] every later core connection of this process is
/// opened with (the CLI's `--memory-limit` / `--threads`). Validated by one
/// in-memory open, so a bad value fails here rather than on the next read.
pub fn set_resource_limits(limits: ResourceLimits) -> Result<(), DbError> {
    if limits.threads == Some(0) {
        return Err(DbError::Api("threads must be at least 1".into()));
    }
    let mut probe = Config::default();
    if let Some(m) = &limits.memory_limit {
        probe = probe.with("memory_limit", m)?;
    }
    if let Some(t) = limits.threads {
        probe = probe.with("threads", t.to_string())?;
    }
    Connection::open_in_memory_with_flags(probe)?;
    *LIMITS.write().unwrap_or_else(|p| p.into_inner()) = limits;
    Ok(())
}

/// The process's current [`ResourceLimits`].
pub fn resource_limits() -> ResourceLimits {
    LIMITS.read().unwrap_or_else(|p| p.into_inner()).clone()
}

/// A database path as the UTF-8 string DuckDB is given. A non-UTF-8 path is
/// refused (finding L2): a lossy conversion would name a *different* file
/// (U+FFFD substituted), e.g. in the rebuild's `ATTACH`.
pub(crate) fn utf8_path(db_path: &Path) -> Result<&str, DbError> {
    db_path.to_str().ok_or_else(|| {
        DbError::Api(format!(
            "the database path {:?} is not valid UTF-8; rename it (nodal-db \
             accepts UTF-8 paths only)",
            db_path.display().to_string()
        ))
    })
}

/// The read-write connection `schema_init` applies the schema on: a new
/// database only. Any existing entry at the path, or an existing
/// `<db>.wal`, is refused (finding L1: applying the DDL into someone's
/// DuckDB file, or half-applying it onto a nodal-db file, writes into a file
/// that is not new; DuckDB 2.0 cannot open a zero-byte file either).
/// Locked down like every core connection (engine rule 12), so the schema
/// SQL cannot reach files either.
pub(crate) fn open_fresh(db_path: &Path) -> Result<Connection, DbError> {
    utf8_path(db_path)?;
    // `symlink_metadata`: a symlink, dangling or not, is an existing entry.
    if std::fs::symlink_metadata(db_path).is_ok() {
        return Err(DbError::Api(format!(
            "refusing to initialize {:?}: it already exists (init creates a new \
             database; use a new path)",
            db_path.display().to_string()
        )));
    }
    let mut wal = db_path.as_os_str().to_owned();
    wal.push(".wal");
    if std::fs::symlink_metadata(&wal).is_ok() {
        return Err(DbError::Api(format!(
            "refusing to initialize {:?}: a write-ahead log {:?} exists next to it \
             (it would be replayed onto the new database; remove it first)",
            db_path.display().to_string(),
            Path::new(&wal).display().to_string()
        )));
    }
    let before = file_stamp(db_path);
    let conn = Connection::open_with_flags(
        db_path,
        config_for(AccessMode::ReadWrite, Lockdown::Locked)?,
    )?;
    lock_down(&conn, Lockdown::Locked)?;
    // The DDL's generated columns and indexes bind `lower()` (engine rule 8).
    guard_builtins(&conn, db_path, before)?;
    Ok(conn)
}

/// Open a connection in the given access mode, retrying with bounded backoff
/// while the file is locked by another connection (surfaced as [`DbError::Busy`]).
/// `Config` is not `Clone`, so it is rebuilt per attempt (cheap).
///
/// `pub(crate)` so [`crate::compaction`] can open the source read-only itself
/// (it already holds [`path_lock`] for the whole repack and must NOT re-take it
/// via [`with_read`], which would deadlock the non-reentrant per-path mutex).
pub(crate) fn open_with_retry(db_path: &Path, mode: AccessMode) -> Result<Connection, DbError> {
    open_with_retry_as(db_path, mode, Lockdown::Locked)
}

/// The rebuild's temp connection (compaction, extract): [`open_with_retry`]
/// read-write, locked down so that it may `ATTACH` exactly `src`
/// ([`Lockdown::Attach`]; `crate::rebuild::attach_source_ro` uses the same
/// spelling of the path, [`attach_path`]).
pub(crate) fn open_for_rebuild(temp: &Path, src: &Path) -> Result<Connection, DbError> {
    let src = attach_path(src)?;
    open_with_retry_as(temp, AccessMode::ReadWrite, Lockdown::Attach(&src))
}

/// The one spelling of a rebuild source's path: absolute (so DuckDB's
/// `allowed_paths` check and the `ATTACH` see the same string whatever the
/// working directory) and UTF-8 ([`utf8_path`]).
pub(crate) fn attach_path(src: &Path) -> Result<String, DbError> {
    utf8_path(src)?;
    let abs = std::path::absolute(src).map_err(|e| {
        DbError::Api(format!(
            "cannot resolve the path {:?}: {e}",
            src.display().to_string()
        ))
    })?;
    Ok(utf8_path(&abs)?.to_owned())
}

/// [`open_with_retry`] under an explicit [`Lockdown`].
pub(crate) fn open_with_retry_as(
    db_path: &Path,
    mode: AccessMode,
    lockdown: Lockdown<'_>,
) -> Result<Connection, DbError> {
    utf8_path(db_path)?;
    let mut backoff = BACKOFF_START;
    for attempt in 1..=MAX_ATTEMPTS {
        let config = config_for(mode.clone(), lockdown)?;
        let before = file_stamp(db_path);
        match Connection::open_with_flags(db_path, config) {
            Ok(conn) => {
                // The lock-down first (engine rule 12), then the shadowing
                // guard (engine rule 8): both before any core SQL runs on
                // the file. On a refusal `conn` drops (closes).
                lock_down(&conn, lockdown)?;
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
/// over a third to every read, so
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
/// Called only for the core's own write cycles ([`Restamp::Yes`]: the `api`
/// entry points, erasure, the verbs, the template apply), whose SQL is all
/// core-owned; never for the public closure family ([`with_write_txn`] and
/// its retrying forms), which runs a caller's SQL (a `CREATE MACRO lower`
/// there must be listed on the next open).
///
/// Why recording it as clean is sound under that precondition: the guard
/// passed at this connection's open (cached or listed), the core-owned SQL
/// writes only the core's rows and its vetted DDL (the read surface), never
/// a user macro, and the stamp is taken while this RW connection holds the
/// file's lock, so no other process can change it meanwhile.
/// [`record_own_write`] re-stats after the close, so a change by anyone
/// after the close is not recorded. The guarantee holds for core-routed
/// access only, like [`TxnOptions::expect_commit`]'s: a second DuckDB
/// instance in the same process (Python's own `duckdb` module on the same
/// file) shares the process's POSIX locks and could change the file between
/// the open and the stamp.
fn stamp_own_write(conn: &Connection, db_path: &Path) -> Option<FileStamp> {
    conn.execute_batch("CHECKPOINT").ok()?;
    file_stamp(db_path)
}

/// Record [`stamp_own_write`]'s stamp as clean once the connection is
/// closed, only if the file still has it (re-stat'ed now); otherwise leave
/// the cache as it is, and the next open lists.
fn record_own_write(db_path: &Path, stamp: Option<FileStamp>) {
    if let Some(stamp) = stamp {
        #[cfg(debug_assertions)]
        test_hooks::AFTER_CLOSE.with(|h| {
            if let Some(hook) = h.take() {
                hook(db_path);
            }
        });
        if file_stamp(db_path).as_ref() == Some(&stamp) {
            let key = canonical_path_key(db_path);
            store_clean(key.clone(), stamp);
            guard_cache().entry(key).or_default().own_writes += 1;
        }
    }
}

/// Test hooks of the guard's re-stamp (debug builds only; thread-local, so
/// parallel tests do not interfere: the write cycle runs on the calling
/// thread).
#[doc(hidden)]
pub mod test_hooks {
    #[cfg(debug_assertions)]
    thread_local! {
        pub(super) static AFTER_CLOSE: std::cell::Cell<Option<fn(&std::path::Path)>> =
            const { std::cell::Cell::new(None) };
    }

    /// Run `hook` once, on this thread's next re-stamping commit, between
    /// the close and the re-stat (e.g. to change the file there).
    #[cfg(debug_assertions)]
    pub fn after_next_close(hook: fn(&std::path::Path)) {
        AFTER_CLOSE.with(|h| h.set(Some(hook)));
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
/// lock is never leaked. `commit_id` is the validity edge the history
/// writes stamp onto rows; axis writes ignore it (axes are
/// immutable and carry no validity span).
pub fn with_write_txn<T, F>(db_path: &Path, user: &str, message: &str, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection, i64) -> Result<T, DbError>,
{
    run_txn_once(
        db_path,
        user,
        message,
        TxnOptions::default(),
        Restamp::No,
        |conn, c| Ok((f(conn, c)?, TxnEnd::Commit)),
    )
}

/// How a successful write cycle ends: a
/// closure that proved it wrote nothing asks for [`TxnEnd::Rollback`], so
/// the call leaves no `commit` row and the head (the polling cookie) does
/// not move. The `commit` row's sequence value stays consumed, as on a
/// conflict retry, so commit ids may skip. Only the `api` entry points
/// decide this (`commit` and its wrappers); every other writer,
/// [`with_write_txn`] included, commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TxnEnd {
    Commit,
    Rollback,
}

/// Transaction-level options of one write cycle: checks the transaction
/// layer runs itself, so any writer can
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
/// per process).
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
/// rolled-back attempt leaves no orphan commit. A closure that
/// returns [`TxnEnd::Rollback`] ends the cycle with ROLLBACK instead of
/// COMMIT and still returns its value.
fn run_txn_once<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    txn: TxnOptions,
    restamp: Restamp,
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
    // Set when the gate's read hit no `database_property` table: after the
    // rollback, the file's own (older-layout) version is read and named.
    let mut older_layout = false;
    let result = (|| {
        // The schema-version gate: a file of another `SCHEMA_VERSION` is
        // refused before anything is written, whatever the write (tier,
        // retention mode, structural or a caller's own SQL), so no core write
        // ever touches it. A failing probe (not a nodal-db file) marks the
        // commit-table check: the file may not be a nodal-db database.
        if let Some(found) = foreign_schema_version(&conn).inspect_err(|e| {
            commit_insert_failed = true;
            older_layout = crate::read_surface::is_missing_object(e);
        })? {
            return Err(schema_mismatch(found));
        }
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
            let stamp = (end == TxnEnd::Commit && restamp == Restamp::Yes)
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
            // A file without `database_property`: name its own version
            // (read in autocommit now, from whichever layout it has).
            let older = older_layout
                .then(|| crate::property::schema_version_any(&conn).ok().flatten())
                .flatten();
            let e = if let Some(found) = older {
                schema_mismatch(found)
            } else if commit_insert_failed && !has_commit_table(&conn) {
                DbError::Api(format!(
                    "{:?} is not an initialized nodal-db database (no `commit` table); \
                     create it with init first",
                    db_path.display().to_string()
                ))
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
/// this build's [`crate::compaction::SCHEMA_VERSION`] (an empty string when
/// the row is missing); `None` when it matches. Run by every write cycle
/// inside its transaction, before the `commit` row (one single-row read).
fn foreign_schema_version(conn: &Connection) -> Result<Option<String>, DbError> {
    // The 0.6 table only (no catalog listing on the hot path): a missing
    // table fails the probe, and the caller names the file's own version
    // (an older layout) or the missing nodal-db tables after its rollback.
    let found: Option<String> = conn
        .query_row(
            "SELECT value FROM database_property \
             WHERE lower(key) = 'schema_version' AND valid_to_commit IS NULL",
            [],
            |r| r.get(0),
        )
        .optional()?;
    let found = found.unwrap_or_default();
    Ok((!crate::compaction::is_current_schema_version(&found)).then_some(found))
}

/// The write cycle's refusal of a file of another schema version.
fn schema_mismatch(found: String) -> DbError {
    let expected = crate::compaction::SCHEMA_VERSION;
    DbError::SchemaMismatch {
        message: format!(
            "write refused: database schema version {found:?} is not this nodal-db's \
             ({expected:?}); re-create the database (init + import) with this nodal-db, \
             or use a nodal-db build matching its schema version"
        ),
        found,
        expected: expected.to_string(),
    }
}

/// Is this a DuckDB write-write conflict? Matched on
/// the robust `Conflict` substring, not the full phrase: the wording depends on
/// the operation (`Conflict on update!` for an UPDATE, `Conflict on tuple
/// deletion!` for a DELETE; identical on DuckDB 1.5.5 and 2.0.0).
fn is_conflict(e: &DbError) -> bool {
    matches!(e, DbError::Duckdb(_)) && e.to_string().contains("Conflict")
}

/// Bounded retry budget for the write-write conflict loop.
const MAX_CONFLICT_ATTEMPTS: u32 = 5;

/// Like [`with_write_txn`], but re-runs the *whole* closure from scratch on a
/// write-write conflict. Each attempt opens a fresh transaction and
/// a fresh `commit` row, so the closure re-reads every piece of state it depends
/// on — current versions, array pre-images, axis lookups — inside the new
/// transaction; nothing read before the conflict is reused. Hence `F: Fn` (must
/// be re-runnable), not `FnOnce`. After `MAX_CONFLICT_ATTEMPTS` the conflict is
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
///
/// The public closure family ([`with_write_txn`], [`with_write_txn_retrying`]
/// and this) runs the caller's SQL, so its commits never re-stamp the file
/// for the shadowing guard (`Restamp::No`): the next open lists it.
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
    retry_conflicts(db_path, user, message, txn, Restamp::No, |conn, c| {
        Ok((f(conn, c)?, TxnEnd::Commit))
    })
}

/// [`with_write_txn_retrying`] for the core's own write cycles (erasure, the
/// verbs, the template apply), whose SQL is all core-owned: a commit
/// re-stamps the file for the shadowing guard ([`Restamp::Yes`]).
pub(crate) fn core_write_txn_retrying<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    f: F,
) -> Result<T, DbError>
where
    F: Fn(&Connection, i64) -> Result<T, DbError>,
{
    core_write_txn_retrying_opts(db_path, user, message, TxnOptions::default(), f)
}

/// [`core_write_txn_retrying`] with [`TxnOptions`].
pub(crate) fn core_write_txn_retrying_opts<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    txn: TxnOptions,
    f: F,
) -> Result<T, DbError>
where
    F: Fn(&Connection, i64) -> Result<T, DbError>,
{
    retry_conflicts(db_path, user, message, txn, Restamp::Yes, |conn, c| {
        Ok((f(conn, c)?, TxnEnd::Commit))
    })
}

/// [`core_write_txn_retrying_opts`] for a closure that also says how its
/// cycle ends ([`TxnEnd`]): the `api` entry points, which roll back a call
/// that wrote nothing. Core-owned SQL, so a
/// commit re-stamps.
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
    retry_conflicts(db_path, user, message, txn, Restamp::Yes, f)
}

/// Whether a committed write cycle re-stamps the file as clean for the
/// shadowing guard ([`stamp_own_write`]). `Yes` only for the core's own
/// write cycles, whose SQL is core-owned; not settable from outside the
/// crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Restamp {
    Yes,
    No,
}

/// The conflict-retry loop shared by every retrying writer.
fn retry_conflicts<T, F>(
    db_path: &Path,
    user: &str,
    message: &str,
    txn: TxnOptions,
    restamp: Restamp,
    f: F,
) -> Result<T, DbError>
where
    F: Fn(&Connection, i64) -> Result<(T, TxnEnd), DbError>,
{
    for attempt in 1..=MAX_CONFLICT_ATTEMPTS {
        match run_txn_once(db_path, user, message, txn, restamp, &f) {
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
/// the handle must not outlive the call.
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
    with_read_retrying_as(db_path, Access::Locked, f)
}

/// [`with_read_retrying`] with an explicit [`Access`]: [`Access::Trusted`]
/// opens the connection with DuckDB's own file, network and extension
/// access (and an unlocked configuration) for a caller's own query on a
/// file it trusts (Python `db.query(sql, trusted=True)`). The file's views
/// and macros, the read-surface check included, then run with that access
/// too.
pub fn with_read_retrying_as<T, F>(db_path: &Path, access: Access, f: F) -> Result<T, DbError>
where
    F: Fn(&Connection) -> Result<T, DbError>,
{
    use crate::read_surface::{
        attaches_other_database, install_overlay, is_missing_object, map_read_error, prepare_read,
        ReadScope,
    };
    let lockdown = match access {
        Access::Locked => Lockdown::Locked,
        Access::Trusted => Lockdown::Trusted,
    };
    with_read_plain_as(db_path, lockdown, |conn| {
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
    with_read_plain_as(db_path, Lockdown::Locked, f)
}

fn with_read_plain_as<T, F>(db_path: &Path, lockdown: Lockdown<'_>, f: F) -> Result<T, DbError>
where
    F: FnOnce(&Connection) -> Result<T, DbError>,
{
    // Reads serialize too (critique F1): an in-process RO instance open during
    // a write has the same dual-instance hazard as a second writer.
    require_db_file(db_path)?;
    let lock = path_lock(db_path);
    let _serial = lock.lock().unwrap_or_else(|p| p.into_inner());
    let conn = open_with_retry_as(db_path, AccessMode::ReadOnly, lockdown)?;
    let result = f(&conn);
    // Close regardless of the closure outcome so no RO handle lingers.
    match conn.close() {
        Ok(()) => result,
        Err((_, e)) => result.and(Err(DbError::Duckdb(e))),
    }
}
