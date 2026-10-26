//! The read surface's version, refresh and overlay
//! (docs/dev/read_surface.md; `specs/read_surface_contract_PLAN.md` Part B).
//!
//! The read surface (every view and macro of `schema/070`-`130_*.sql`) lives
//! inside each database file, frozen at the build that created or last
//! refreshed it. `130_read_surface.sql` stamps it with a version,
//! `nodal_read_surface()` → `(contract_major, revision, ddl_hash)`, applied
//! last so the marker exists only if every object before it was created.
//! This module compares a file's version with this build's and acts:
//!
//! - **Writes** ([`refresh_before_write`], called by `conn::run_txn_once`
//!   right after the RW open): an older file (lower `(major, revision)`, the
//!   marker missing, or the same revision with another hash) of this build's
//!   `SCHEMA_VERSION` gets this build's surface in its own transaction, before
//!   the caller's (no `commit` row: a catalog refresh is not content). A newer
//!   file is never downgraded. A failed refresh rolls back, the write goes
//!   through, and the write report carries a warning; the explicit verb
//!   ([`refresh_read_surface`]) fails loudly instead.
//! - **Reads** (`conn::with_read*`): the version is read first, before any
//!   overlay (a temp object shadows the file's even when qualified `main.`).
//!   The core's named reads (`api::read_parameter`, `api::read_values`)
//!   install TEMP copies of only their dependency closure on a stale file;
//!   a generic read uses the file's own surface within one contract major
//!   (bodies within a major are by contract semantically equal) and falls
//!   back to a full overlay only on a missing-object error; a file of
//!   another major gets the full overlay up front. An overlay is never
//!   installed on a connection that attaches another database: temp bodies
//!   bind unqualified names to the default catalog and would shadow the
//!   attached file's views too.
//!
//! The refresh drops by the file's actual catalog (a view, a table macro or a
//! scalar macro of a surface name or a retired name), with `IF EXISTS` and
//! **never** `CASCADE`: DuckDB 2.0 records no dependencies of views or
//! macros, so user views over surface objects survive (they bind by name at
//! query time); a `CASCADE` would drop them the day DuckDB starts tracking
//! those dependencies. The whole refresh is one transaction: DDL is
//! transactional and the WAL replays all of it or none.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use duckdb::Connection;

use crate::compaction::SCHEMA_VERSION;
use crate::conn::canonical_path_key;
use crate::error::DbError;
use crate::schema_init::EMBEDDED_SCHEMA;

/// The schema files that hold the read surface (`070`-`130`): views and
/// macros only, no tables, indexes or data.
const SURFACE_FIRST: u32 = 70;
const SURFACE_LAST: u32 = 130;
/// The last file the DDL hash covers (`130` holds the hash itself).
const HASH_LAST: u32 = 120;
/// The file holding the version marker.
const VERSION_FILE: &str = "130_read_surface.sql";

/// The committed list of retired surface names (dropped from older files by
/// the refresh, so a removed object never lingers with a stale body).
static RETIRED_NAMES: &str = include_str!("../../../schema/read_surface_retired.txt");

// ---------------------------------------------------------------------------
// Versions and status
// ---------------------------------------------------------------------------

/// One read-surface version: `nodal_read_surface()`'s row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceVersion {
    pub contract_major: i32,
    pub revision: i32,
    /// xxh3-64 of the normalized `070`-`120` DDL, 16 lower-case hex digits.
    pub ddl_hash: String,
}

impl fmt::Display for SurfaceVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{} ({})",
            self.contract_major, self.revision, self.ddl_hash
        )
    }
}

/// How a file's read surface compares with this build's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SurfaceStatus {
    /// Same version and hash.
    Current,
    /// No `nodal_read_surface()`: the file predates versioning (contract
    /// major 0).
    Missing,
    /// A lower `(contract_major, revision)`.
    Older,
    /// The same version with another DDL hash: a development build whose
    /// surface DDL was edited without a revision bump. Treated as older (it
    /// is refreshed; two such builds alternating refresh each other).
    Drift,
    /// A higher `(contract_major, revision)`: never downgraded.
    Newer,
}

impl SurfaceStatus {
    /// The stable lower-case name (`current`, `missing`, `older`, `drift`,
    /// `newer`) the CLI and the Python skin report.
    pub fn as_str(self) -> &'static str {
        match self {
            SurfaceStatus::Current => "current",
            SurfaceStatus::Missing => "missing",
            SurfaceStatus::Older => "older",
            SurfaceStatus::Drift => "drift",
            SurfaceStatus::Newer => "newer",
        }
    }
}

/// A file's read-surface version against this build's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceCheck {
    /// The file's version; `None` when the marker is missing.
    pub found: Option<SurfaceVersion>,
    /// This build's version.
    pub expected: SurfaceVersion,
    pub status: SurfaceStatus,
}

impl SurfaceCheck {
    fn new(found: Option<SurfaceVersion>) -> Self {
        let expected = build_version().clone();
        let status = match &found {
            None => SurfaceStatus::Missing,
            Some(f) => {
                let a = (f.contract_major, f.revision);
                let b = (expected.contract_major, expected.revision);
                if a < b {
                    SurfaceStatus::Older
                } else if a > b {
                    SurfaceStatus::Newer
                } else if f.ddl_hash != expected.ddl_hash {
                    SurfaceStatus::Drift
                } else {
                    SurfaceStatus::Current
                }
            }
        };
        SurfaceCheck {
            found,
            expected,
            status,
        }
    }

    /// Older than this build (missing, a lower version, or dev drift): a
    /// write refreshes it.
    pub fn is_stale(&self) -> bool {
        matches!(
            self.status,
            SurfaceStatus::Missing | SurfaceStatus::Older | SurfaceStatus::Drift
        )
    }

    /// The file's contract major (0 when the marker is missing: versioning
    /// started at major 0).
    pub fn found_major(&self) -> i32 {
        self.found.as_ref().map_or(0, |v| v.contract_major)
    }

    /// The file's surface is of another contract major than this build's
    /// (older or newer): its objects' shapes may differ incompatibly.
    pub fn other_major(&self) -> bool {
        self.found_major() != self.expected.contract_major
    }

    /// The file's version for messages (`none (predates versioning)` when
    /// missing).
    pub fn found_label(&self) -> String {
        self.found.as_ref().map_or_else(
            || "none (predates versioning)".to_string(),
            |v| v.to_string(),
        )
    }
}

/// This build's read-surface version, parsed once from the embedded
/// `130_read_surface.sql` (the runtime never computes the hash; the gate
/// test does).
pub fn build_version() -> &'static SurfaceVersion {
    static V: OnceLock<SurfaceVersion> = OnceLock::new();
    V.get_or_init(|| {
        let text = embedded(VERSION_FILE).expect("130_read_surface.sql is embedded");
        parse_version_file(text).expect("130_read_surface.sql: unparseable version literals")
    })
}

/// Parse the three `CAST(<literal> AS <type>) AS <column>` literals of
/// `130_read_surface.sql`. `None` when the layout changed.
pub fn parse_version_file(text: &str) -> Option<SurfaceVersion> {
    fn literal<'a>(text: &'a str, column: &str) -> Option<&'a str> {
        let end = text.find(&format!(") AS {column}"))?;
        let start = text[..end].rfind("CAST(")? + "CAST(".len();
        let inner = &text[start..end];
        let lit = inner[..inner.rfind(" AS ")?].trim();
        Some(lit.trim_matches('\''))
    }
    Some(SurfaceVersion {
        contract_major: literal(text, "contract_major")?.parse().ok()?,
        revision: literal(text, "revision")?.parse().ok()?,
        ddl_hash: literal(text, "ddl_hash")?.to_string(),
    })
}

/// Read the file's version on `conn` and compare. A missing marker is
/// [`SurfaceStatus::Missing`] (detected from the Catalog error of the one
/// query, never through `duckdb_functions()`, which costs ~20 ms); any other
/// error is returned. Must run before any overlay is installed on `conn`.
pub fn check_on(conn: &Connection) -> Result<SurfaceCheck, DbError> {
    let found = conn.query_row(
        "SELECT contract_major, revision, ddl_hash FROM nodal_read_surface()",
        [],
        |r| {
            Ok(SurfaceVersion {
                contract_major: r.get(0)?,
                revision: r.get(1)?,
                ddl_hash: r.get(2)?,
            })
        },
    );
    match found {
        Ok(v) => Ok(SurfaceCheck::new(Some(v))),
        Err(e) if is_missing_object_error(&e) => Ok(SurfaceCheck::new(None)),
        Err(e) => Err(DbError::Duckdb(e)),
    }
}

/// Is this DuckDB's "object does not exist" Catalog error (a missing view,
/// table, table function or scalar function)? The error text is not a
/// DuckDB contract: this is the ONE place it is matched, and
/// `tests/read_surface.rs` pins it, so a DuckDB upgrade that rewords it
/// fails a test instead of making every file "current" or "missing".
pub fn is_missing_object_error(e: &duckdb::Error) -> bool {
    let s = e.to_string();
    s.contains("Catalog Error") && s.contains("does not exist")
}

/// [`is_missing_object_error`] on a [`DbError`].
pub fn is_missing_object(e: &DbError) -> bool {
    matches!(e, DbError::Duckdb(d) if is_missing_object_error(d))
}

/// The file's current `schema_version` metadata value; `None` when it cannot
/// be read (not a nodal-db file).
fn schema_version_on(conn: &Connection) -> Option<String> {
    conn.query_row(
        "SELECT value FROM metadata \
         WHERE lower(name) = 'schema_version' AND valid_to_commit IS NULL",
        [],
        |r| r.get(0),
    )
    .ok()
}

// ---------------------------------------------------------------------------
// The surface DDL: objects, dependencies, normalization and hash
// ---------------------------------------------------------------------------

/// The kind of a surface object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    View,
    TableMacro,
    ScalarMacro,
}

impl ObjectKind {
    fn drop_sql(self, name: &str, qualifier: &str) -> String {
        let what = match self {
            ObjectKind::View => "VIEW",
            ObjectKind::TableMacro => "MACRO TABLE",
            ObjectKind::ScalarMacro => "MACRO",
        };
        // Never CASCADE (module docs).
        format!("DROP {what} IF EXISTS {qualifier}\"{name}\";")
    }
}

/// One view or macro of the build's read surface.
#[derive(Debug)]
pub struct SurfaceObject {
    pub name: String,
    pub kind: ObjectKind,
    /// The schema file it is created in.
    pub file: &'static str,
    /// Its `-- @surface:` class (the line directly above the CREATE).
    pub class: Option<String>,
    /// The statement from `CREATE` to its end (no `;`).
    body: &'static str,
    /// Indices of the surface objects its body names.
    deps: Vec<usize>,
}

impl SurfaceObject {
    /// The statement as a TEMP object (an overlay copy).
    fn temp_sql(&self) -> String {
        format!("CREATE TEMP {};", &self.body["CREATE ".len()..])
    }
}

/// The build's read surface, parsed from the embedded `070`-`130` files.
#[derive(Debug)]
pub struct Surface {
    /// In creation (file) order.
    pub objects: Vec<SurfaceObject>,
    index: HashMap<String, usize>,
    /// Statements of the surface files that are not `CREATE VIEW|MACRO`
    /// (the census test requires none).
    pub other_statements: Vec<String>,
}

impl Surface {
    pub fn get(&self, name: &str) -> Option<&SurfaceObject> {
        self.index.get(name).map(|&i| &self.objects[i])
    }

    /// `names` and every surface object their bodies reach, in creation
    /// order. Unknown names are ignored.
    pub fn closure(&self, names: &[&str]) -> Vec<&SurfaceObject> {
        let mut seen = vec![false; self.objects.len()];
        let mut stack: Vec<usize> = names
            .iter()
            .filter_map(|n| self.index.get(*n).copied())
            .collect();
        while let Some(i) = stack.pop() {
            if !std::mem::replace(&mut seen[i], true) {
                stack.extend(self.objects[i].deps.iter().copied());
            }
        }
        self.objects
            .iter()
            .enumerate()
            .filter(|(i, _)| seen[*i])
            .map(|(_, o)| o)
            .collect()
    }
}

/// The parsed surface of this build (once per process).
pub fn surface() -> &'static Surface {
    static S: OnceLock<Surface> = OnceLock::new();
    S.get_or_init(|| parse_surface(surface_files()))
}

fn file_number(name: &str) -> Option<u32> {
    name.get(..3)?.parse().ok()
}

fn embedded(name: &str) -> Option<&'static str> {
    EMBEDDED_SCHEMA
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, s)| *s)
}

/// The embedded surface files (`070`-`130`), in application order.
pub fn surface_files() -> Vec<(&'static str, &'static str)> {
    EMBEDDED_SCHEMA
        .iter()
        .filter(|(n, _)| {
            file_number(n).is_some_and(|k| (SURFACE_FIRST..=SURFACE_LAST).contains(&k))
        })
        .copied()
        .collect()
}

/// The retired surface names (`schema/read_surface_retired.txt`).
pub fn retired_names() -> Vec<&'static str> {
    RETIRED_NAMES
        .lines()
        .map(|l| l.split('#').next().unwrap_or("").trim())
        .filter(|l| !l.is_empty())
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Lx {
    Word,
    Quoted,
    LineComment,
    BlockComment,
    Space,
    Punct,
}

#[derive(Clone, Copy, Debug)]
struct Lexeme {
    kind: Lx,
    start: usize,
    end: usize,
}

/// A small SQL lexer: words, quoted literals and identifiers (`'…'`, `"…"`,
/// doubled quotes inside), `--` and `/* */` comments, whitespace runs and
/// single punctuation bytes. Enough to split statements and normalize text
/// without being fooled by a `;` or `--` inside a literal.
fn lex(sql: &str) -> Vec<Lexeme> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let word = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80;
    while i < b.len() {
        let start = i;
        let c = b[i];
        let kind = if c.is_ascii_whitespace() {
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            Lx::Space
        } else if c == b'-' && b.get(i + 1) == Some(&b'-') {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            Lx::LineComment
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i < b.len() && !(b[i] == b'*' && b.get(i + 1) == Some(&b'/')) {
                i += 1;
            }
            i = (i + 2).min(b.len());
            Lx::BlockComment
        } else if c == b'\'' || c == b'"' {
            i += 1;
            while i < b.len() {
                if b[i] == c {
                    if b.get(i + 1) == Some(&c) {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            Lx::Quoted
        } else if word(c) {
            while i < b.len() && word(b[i]) {
                i += 1;
            }
            Lx::Word
        } else {
            i += 1;
            Lx::Punct
        };
        out.push(Lexeme {
            kind,
            start,
            end: i,
        });
    }
    out
}

/// The `-- @surface: <class>` tag of a line comment, if it is one.
fn surface_tag(comment: &str) -> Option<&str> {
    comment
        .strip_prefix("--")?
        .trim()
        .strip_prefix("@surface:")
        .map(str::trim)
}

/// The DDL text as the hash sees it: every comment except the `@surface:`
/// tags removed, every whitespace run outside literals collapsed to one
/// space. Comment-only and whitespace-only edits therefore keep the hash; a
/// class change (a tag edit) does not.
pub fn normalize_ddl(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() / 2);
    let mut gap = false;
    for l in lex(sql) {
        let t = &sql[l.start..l.end];
        match l.kind {
            Lx::Space | Lx::BlockComment => gap = true,
            Lx::LineComment => {
                if let Some(class) = surface_tag(t) {
                    if !out.is_empty() {
                        out.push(' ');
                    }
                    out.push_str("--@surface:");
                    out.push_str(class);
                }
                gap = true;
            }
            _ => {
                if gap && !out.is_empty() {
                    out.push(' ');
                }
                gap = false;
                out.push_str(t);
            }
        }
    }
    out
}

/// The DDL hash of the given surface files: xxh3-64 over each file's name
/// and normalized text, in order, for the files `070`-`120`.
pub fn ddl_hash_of(files: &[(&str, &str)]) -> String {
    let mut buf = String::new();
    for (name, sql) in files {
        if file_number(name).is_some_and(|k| (SURFACE_FIRST..=HASH_LAST).contains(&k)) {
            buf.push_str(name);
            buf.push('\n');
            buf.push_str(&normalize_ddl(sql));
            buf.push('\n');
        }
    }
    format!("{:016x}", xxhash_rust::xxh3::xxh3_64(buf.as_bytes()))
}

/// The DDL hash of this build's embedded surface files.
pub fn build_ddl_hash() -> String {
    ddl_hash_of(&surface_files())
}

fn parse_surface(files: Vec<(&'static str, &'static str)>) -> Surface {
    let mut objects: Vec<SurfaceObject> = Vec::new();
    let mut other = Vec::new();
    // body word lists, resolved to deps once every name is known
    let mut words: Vec<Vec<&'static str>> = Vec::new();
    for (file, sql) in files {
        let lx = lex(sql);
        let mut stmt_start = 0usize; // index into lx
        for (k, l) in lx.iter().enumerate() {
            let is_end = l.kind == Lx::Punct && &sql[l.start..l.end] == ";";
            if !is_end && k + 1 != lx.len() {
                continue;
            }
            let end = if is_end { k } else { k + 1 };
            let part = &lx[stmt_start..end];
            stmt_start = k + 1;
            let Some(ci) = part.iter().position(|l| l.kind == Lx::Word) else {
                continue; // comments and whitespace only
            };
            let ws: Vec<&Lexeme> = part[ci..].iter().filter(|l| l.kind == Lx::Word).collect();
            let text = move |l: &Lexeme| -> &'static str { &sql[l.start..l.end] };
            let body_end = part.last().map_or(part[ci].end, |l| l.end);
            let body = sql[part[ci].start..body_end].trim_end();
            let head: Vec<String> = ws.iter().take(2).map(|l| text(l).to_uppercase()).collect();
            let kind = match head.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                ["CREATE", "VIEW"] => ObjectKind::View,
                ["CREATE", "MACRO"] => {
                    // `name(params) AS TABLE` vs `name(params) AS <expr>`
                    let mut depth = 0i32;
                    let mut after = None;
                    for (j, l) in part[ci..].iter().enumerate() {
                        if l.kind == Lx::Punct {
                            match &sql[l.start..l.end] {
                                "(" => depth += 1,
                                ")" => {
                                    depth -= 1;
                                    if depth == 0 {
                                        after = Some(ci + j + 1);
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    let rest: Vec<String> = after
                        .map(|a| {
                            part[a..]
                                .iter()
                                .filter(|l| l.kind == Lx::Word)
                                .take(2)
                                .map(|l| text(l).to_uppercase())
                                .collect()
                        })
                        .unwrap_or_default();
                    if rest == ["AS", "TABLE"] {
                        ObjectKind::TableMacro
                    } else {
                        ObjectKind::ScalarMacro
                    }
                }
                _ => {
                    other.push(body.to_string());
                    continue;
                }
            };
            let Some(name_lx) = ws.get(2) else {
                other.push(body.to_string());
                continue;
            };
            // the tag: the line comment directly above the CREATE (one
            // newline between them)
            let class = ci
                .checked_sub(2)
                .filter(|&t| {
                    part[t].kind == Lx::LineComment
                        && part[t + 1].kind == Lx::Space
                        && sql[part[t + 1].start..part[t + 1].end]
                            .matches('\n')
                            .count()
                            == 1
                })
                .and_then(|t| surface_tag(&sql[part[t].start..part[t].end]))
                .map(str::to_string);
            words.push(ws.iter().skip(3).map(|l| text(l)).collect());
            objects.push(SurfaceObject {
                name: text(name_lx).to_string(),
                kind,
                file,
                class,
                body,
                deps: Vec::new(),
            });
        }
    }
    let index: HashMap<String, usize> = objects
        .iter()
        .enumerate()
        .map(|(i, o)| (o.name.clone(), i))
        .collect();
    for (i, ws) in words.iter().enumerate() {
        let mut deps: Vec<usize> = ws
            .iter()
            .filter_map(|w| index.get(&w.to_lowercase()).copied())
            .filter(|&d| d != i)
            .collect();
        deps.sort_unstable();
        deps.dedup();
        objects[i].deps = deps;
    }
    Surface {
        objects,
        index,
        other_statements: other,
    }
}

// ---------------------------------------------------------------------------
// Notices (once per process per file) and the write warning
// ---------------------------------------------------------------------------

/// A stale-surface notice for the skins (Python `warnings.warn`, CLI stderr,
/// the editor's banner). Issued at most once per process per file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleNotice {
    pub path: PathBuf,
    pub check: SurfaceCheck,
    pub message: String,
}

thread_local! {
    static NOTICES: RefCell<Vec<StaleNotice>> = const { RefCell::new(Vec::new()) };
    static WRITE_WARNING: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn notified() -> &'static Mutex<HashSet<PathBuf>> {
    static N: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    N.get_or_init(Default::default)
}

/// Files whose refresh failed in this process (with the message), so a
/// write does not retry the ~0.2 s refresh on every save. The explicit verb
/// ignores it.
fn refresh_failed() -> &'static Mutex<HashMap<PathBuf, String>> {
    static F: OnceLock<Mutex<HashMap<PathBuf, String>>> = OnceLock::new();
    F.get_or_init(Default::default)
}

fn notify_once(db_path: &Path, check: &SurfaceCheck, message: String) {
    let key = canonical_path_key(db_path);
    let first = notified()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key);
    if first {
        NOTICES.with(|n| {
            n.borrow_mut().push(StaleNotice {
                path: db_path.to_path_buf(),
                check: check.clone(),
                message,
            })
        });
    }
}

fn forget(db_path: &Path) {
    let key = canonical_path_key(db_path);
    notified()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&key);
    refresh_failed()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&key);
}

/// Take the notices this thread produced since the last call (the skins
/// call it after each core call; the core itself never prints).
pub fn take_notices() -> Vec<StaleNotice> {
    NOTICES.with(|n| std::mem::take(&mut *n.borrow_mut()))
}

pub(crate) fn clear_write_warning() {
    WRITE_WARNING.with(|w| *w.borrow_mut() = None);
}

/// The warning of the last write cycle on this thread (a failed refresh),
/// taken once.
pub(crate) fn take_write_warning() -> Option<String> {
    WRITE_WARNING.with(|w| w.borrow_mut().take())
}

// ---------------------------------------------------------------------------
// The refresh
// ---------------------------------------------------------------------------

/// The views and macros of the file's `main` schema (not temp), by kind.
fn file_objects(conn: &Connection) -> Result<Vec<(String, ObjectKind)>, DbError> {
    let mut out = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT view_name FROM duckdb_views() \
         WHERE database_name = current_database() AND schema_name = 'main' \
           AND NOT internal AND NOT temporary",
    )?;
    for name in stmt.query_map([], |r| r.get::<_, String>(0))? {
        out.push((name?, ObjectKind::View));
    }
    let mut stmt = conn.prepare(
        "SELECT DISTINCT function_name, function_type FROM duckdb_functions() \
         WHERE database_name = current_database() AND schema_name = 'main' \
           AND NOT internal AND function_type IN ('macro', 'table_macro')",
    )?;
    for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (name, ftype) = row?;
        let kind = if ftype == "table_macro" {
            ObjectKind::TableMacro
        } else {
            ObjectKind::ScalarMacro
        };
        out.push((name, kind));
    }
    Ok(out)
}

/// Replace the file's read surface with this build's in ONE transaction on
/// `conn` (read-write, no transaction open): drop every view or macro whose
/// name is a surface name of this build or a retired one (by the kind the
/// file has, `IF EXISTS`, never `CASCADE`), then apply `070`-`130`. On any
/// error the transaction is rolled back (the old catalog stays intact) and
/// the error returned. A user table with a surface view's name is the
/// expected failure ("Existing object … is of type Table").
pub(crate) fn refresh_on(conn: &Connection) -> Result<(), DbError> {
    let s = surface();
    let mut names: HashSet<&str> = s.objects.iter().map(|o| o.name.as_str()).collect();
    names.extend(retired_names());
    let existing = file_objects(conn)?;
    conn.execute_batch("BEGIN")?;
    let result = (|| -> Result<(), DbError> {
        let drops: String = existing
            .iter()
            .filter(|(n, _)| names.contains(n.as_str()))
            .map(|(n, k)| k.drop_sql(n, ""))
            .collect();
        conn.execute_batch(&drops)?;
        for (_, sql) in surface_files() {
            conn.execute_batch(sql)?;
        }
        Ok(())
    })();
    match result {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// The write-cycle hook (`conn::run_txn_once`, right after the RW open and
/// before the caller's `BEGIN`): refresh an older file of this build's
/// `SCHEMA_VERSION` in its own transaction. Never fails the write: a version
/// read that errors, a missing `schema_version` (not a nodal-db file: the
/// write's own path reports it) or another schema version (the
/// `SchemaMismatch` path decides) skip the refresh silently; a failed
/// refresh leaves a warning for the write report (and a notice) and is not
/// retried by later writes in this process.
pub(crate) fn refresh_before_write(conn: &Connection, db_path: &Path) {
    let warning = (|| {
        let check = check_on(conn).ok()?;
        if !check.is_stale() {
            return None;
        }
        if schema_version_on(conn)? != SCHEMA_VERSION {
            return None;
        }
        let key = canonical_path_key(db_path);
        if let Some(w) = refresh_failed()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
        {
            return Some(w.clone());
        }
        match refresh_on(conn) {
            Ok(()) => {
                forget(db_path);
                None
            }
            Err(e) => {
                let w = format!(
                    "the read surface of {} ({}) is older than this nodal-db's ({}) and \
                     could not be refreshed: {e}. The write itself went through; reads \
                     use temporary copies of this build's views meanwhile. Rename the \
                     object of yours that uses a reserved read-surface name (see the \
                     read-surface reference), then run `nodal-db maintain refresh-views {}`",
                    db_path.display(),
                    check.found_label(),
                    check.expected,
                    db_path.display()
                );
                refresh_failed()
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(key, w.clone());
                notify_once(db_path, &check, w.clone());
                Some(w)
            }
        }
    })();
    WRITE_WARNING.with(|w| *w.borrow_mut() = warning);
}

/// What [`read_surface_status`] and [`refresh_read_surface`] report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SurfaceReport {
    /// The file's `schema_version` (`None`: unreadable, not a nodal-db file).
    pub schema_version: Option<String>,
    /// The file's read-surface version against this build's, before any
    /// refresh.
    pub check: SurfaceCheck,
    /// `refresh_read_surface` replaced the file's surface with this build's.
    pub refreshed: bool,
}

/// The file's read-surface status, on one per-operation read-only connection
/// (no overlay, no notice). `nodal-db maintain refresh-views --check`.
pub fn read_surface_status(db: &Path) -> Result<SurfaceReport, DbError> {
    crate::conn::with_read_plain(db, |conn| {
        Ok(SurfaceReport {
            schema_version: schema_version_on(conn),
            check: check_on(conn)?,
            refreshed: false,
        })
    })
}

/// Refresh the file's read surface now (`nodal-db maintain refresh-views`,
/// Python `db.refresh_read_surface()`): one read-write cycle, the same
/// refresh a write runs, but loud: a failure is an error. A current file is
/// left alone; so is a newer one unless `force` (a downgrade; development
/// only). Refuses a file of another `SCHEMA_VERSION` (`SchemaMismatch`).
/// Writes no `commit` row (a catalog refresh is not content).
pub fn refresh_read_surface(db: &Path, force: bool) -> Result<SurfaceReport, DbError> {
    let report = crate::conn::with_rw_conn(db, |conn| {
        let Some(found) = schema_version_on(conn) else {
            return Err(DbError::Api(format!(
                "{:?} is not an initialized nodal-db database (no schema_version); \
                 create it with init first",
                db.display().to_string()
            )));
        };
        if found != SCHEMA_VERSION {
            return Err(DbError::SchemaMismatch {
                message: format!(
                    "refresh-views refused: database schema version {found:?} is not this \
                     nodal-db's ({SCHEMA_VERSION:?}); its tables differ, so this build's \
                     views would not fit them"
                ),
                found,
                expected: SCHEMA_VERSION.to_string(),
            });
        }
        let check = check_on(conn)?;
        let run = check.is_stale() || (force && check.status != SurfaceStatus::Current);
        if run {
            refresh_on(conn).map_err(|e| {
                DbError::Api(format!(
                    "refresh-views failed on {}: {e} (nothing changed; an object of yours \
                     may use a name the read surface reserves)",
                    db.display()
                ))
            })?;
        }
        Ok(SurfaceReport {
            schema_version: Some(found),
            check,
            refreshed: run,
        })
    })?;
    if report.refreshed {
        forget(db);
    }
    Ok(report)
}

// ---------------------------------------------------------------------------
// The read overlay
// ---------------------------------------------------------------------------

/// How a core read uses the surface.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ReadScope<'a> {
    /// An arbitrary closure (`conn::with_read`, Python `db.query`).
    Generic,
    /// One of the core's named reads, needing these objects.
    Named(&'a [&'a str]),
}

/// What [`prepare_read`] found and did.
#[derive(Clone, Debug)]
pub(crate) struct ReadState {
    pub check: SurfaceCheck,
    pub overlaid: bool,
}

/// Before a core read: read the file's version (before any overlay),
/// notice a stale file once, and install the overlay the scope needs.
pub(crate) fn prepare_read(
    conn: &Connection,
    db_path: &Path,
    scope: ReadScope<'_>,
) -> Result<ReadState, DbError> {
    let check = check_on(conn)?;
    if check.status == SurfaceStatus::Current
        || (check.status == SurfaceStatus::Newer && !check.other_major())
    {
        return Ok(ReadState {
            check,
            overlaid: false,
        });
    }
    let message = if check.status == SurfaceStatus::Newer {
        format!(
            "{}: its read surface ({}) is of a newer contract major than this nodal-db's \
             ({}); nodal-db's reads use this build's own views on it",
            db_path.display(),
            check.found_label(),
            check.expected
        )
    } else {
        format!(
            "{}: its read surface ({}) is older than this nodal-db's ({}); nodal-db's reads \
             use temporary copies of this build's views where they need them, which costs \
             time per read. Refresh the file: `nodal-db maintain refresh-views {}` (or any \
             write with this nodal-db)",
            db_path.display(),
            check.found_label(),
            check.expected,
            db_path.display()
        )
    };
    notify_once(db_path, &check, message);
    let overlay = match scope {
        ReadScope::Named(names) => Some(Some(names)),
        ReadScope::Generic if check.other_major() => Some(None),
        ReadScope::Generic => None,
    };
    if let Some(names) = overlay {
        install_overlay(conn, names)?;
    }
    Ok(ReadState {
        overlaid: overlay.is_some(),
        check,
    })
}

/// A missing-object error on a stale file becomes the defined
/// [`DbError::ReadSurfaceStale`]; anything else passes through.
pub(crate) fn map_read_error(e: DbError, state: &ReadState) -> DbError {
    if !(state.check.is_stale() && is_missing_object(&e)) {
        return e;
    }
    let found = state.check.found.as_ref().map(ToString::to_string);
    DbError::ReadSurfaceStale {
        message: format!(
            "the database file's read surface ({}) is older than this nodal-db's ({}) and \
             lacks an object this read needs ({e}); refresh it with `nodal-db maintain \
             refresh-views FILE` (or any write with this nodal-db)",
            state.check.found_label(),
            state.check.expected
        ),
        found,
        expected: state.check.expected.to_string(),
    }
}

/// Does the connection have a database attached besides its default one?
pub(crate) fn attaches_other_database(conn: &Connection) -> Result<bool, DbError> {
    let n: i64 = conn.query_row(
        "SELECT count(*) FROM duckdb_databases() \
         WHERE NOT internal AND database_name <> current_database()",
        [],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// Install TEMP copies of this build's surface objects (`None`: all of
/// them; `Some(names)`: their dependency closure) on `conn`. Refuses a
/// connection that attaches another database. Returns the objects created.
pub(crate) fn install_overlay(
    conn: &Connection,
    names: Option<&[&str]>,
) -> Result<Vec<&'static SurfaceObject>, DbError> {
    if attaches_other_database(conn)? {
        return Err(DbError::Api(
            "internal: refusing a read-surface overlay on a connection that attaches \
             another database (temporary objects would shadow its views too)"
                .into(),
        ));
    }
    install_overlay_unguarded(conn, names)
}

/// [`install_overlay`] without the attach guard: ONLY for the extract
/// self-check, which installs the closure under `USE <source>` and drops it
/// with [`drop_overlay`] before switching back (temp objects shadow even
/// inside an attached catalog's views, so a leftover overlay would make both
/// sides read the same definitions). On failure nothing is left installed.
pub(crate) fn install_overlay_unguarded(
    conn: &Connection,
    names: Option<&[&str]>,
) -> Result<Vec<&'static SurfaceObject>, DbError> {
    let s = surface();
    let objs: Vec<&'static SurfaceObject> = match names {
        None => s.objects.iter().collect(),
        Some(n) => s.closure(n),
    };
    let sql: String = objs
        .iter()
        .map(|o| o.temp_sql())
        .collect::<Vec<_>>()
        .join("\n");
    if let Err(e) = conn.execute_batch(&sql) {
        let _ = drop_overlay(conn, &objs);
        return Err(DbError::Duckdb(e));
    }
    Ok(objs)
}

/// Drop overlay objects (TEMP copies), in reverse creation order.
pub(crate) fn drop_overlay(conn: &Connection, objs: &[&SurfaceObject]) -> Result<(), DbError> {
    let sql: String = objs
        .iter()
        .rev()
        .map(|o| o.kind.drop_sql(&o.name, "temp.main."))
        .collect();
    conn.execute_batch(&sql)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_literals_parse() {
        let v = build_version();
        assert_eq!(v.ddl_hash.len(), 16, "{v}");
        assert!(v.contract_major >= 0 && v.revision >= 1, "{v}");
    }

    #[test]
    fn normalizer_keeps_tags_and_literals() {
        let a = "-- c\nSELECT  'a  -- b;' AS x -- tail\n;";
        let b = "SELECT 'a  -- b;'\n   AS x\n;";
        assert_eq!(normalize_ddl(a), normalize_ddl(b));
        let t1 = "-- @surface: stable\nCREATE VIEW v AS SELECT 1;";
        let t2 = "-- @surface: internal\nCREATE VIEW v AS SELECT 1;";
        assert_ne!(normalize_ddl(t1), normalize_ddl(t2));
        // whitespace inside a literal is significant
        assert_ne!(
            normalize_ddl("SELECT 'a b'"),
            normalize_ddl("SELECT 'a  b'")
        );
    }

    #[test]
    fn statement_parser_sees_kinds_and_tags() {
        let s = parse_surface(vec![(
            "080_x.sql",
            "-- head\n\n-- @surface: internal\nCREATE MACRO f(a) AS a + 1;\n\
             -- @surface: stable\nCREATE MACRO t(a) AS TABLE SELECT f(a) AS v FROM w;\n\
             -- note; with a semicolon\nCREATE VIEW w AS SELECT ';' AS s;",
        )]);
        let k: Vec<_> = s
            .objects
            .iter()
            .map(|o| (o.name.as_str(), o.kind, o.class.as_deref()))
            .collect();
        assert_eq!(
            k,
            vec![
                ("f", ObjectKind::ScalarMacro, Some("internal")),
                ("t", ObjectKind::TableMacro, Some("stable")),
                ("w", ObjectKind::View, None),
            ]
        );
        let c: Vec<_> = s.closure(&["t"]).iter().map(|o| o.name.as_str()).collect();
        assert_eq!(c, vec!["f", "t", "w"]);
        assert!(s.other_statements.is_empty());
    }
}
