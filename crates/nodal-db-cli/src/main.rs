//! `nodal-db` — the command-line tool over `nodal-db-core`.
//!
//! `init` creates a database file (optionally loading a template);
//! `import` ingests the Arrow-IPC bulk format through the core's one
//! canonicalizer (the CLI deserializes and expands, it never
//! re-canonicalizes); `erase-alternative`, `extract` and `purge` remove or
//! close out data; `maintain` compacts a file or refreshes its read
//! surface; `template` loads, plans, applies and exports templates.
//!
//! Exit codes: 0 success, 1 error, 75 busy (the database is locked or the
//! write-conflict retries ran out: retry later), and for
//! `maintain refresh-views --check` 3 (stale) and 4 (newer than this
//! nodal-db).

/// `println!` through [`clean`]: every line the CLI prints may carry names
/// from a database or a file someone else wrote.
macro_rules! say {
    ($($t:tt)*) => { println!("{}", $crate::clean(&format!($($t)*))) };
}

/// `eprintln!` through [`clean`].
macro_rules! say_err {
    ($($t:tt)*) => { eprintln!("{}", $crate::clean(&format!($($t)*))) };
}

mod import;
mod template;

use std::borrow::Cow;
use std::path::PathBuf;

use anyhow::bail;
use clap::{Parser, Subcommand};

/// The compile-time-embedded schema: ONE list, owned by the core
/// (`schema_init::EMBEDDED_SCHEMA`) and shared with every other embedder
/// (the Python skin), so the copies cannot drift. A census test in the core
/// asserts it matches `schema/*.sql` exactly.
use nodal_db_core::schema_init::EMBEDDED_SCHEMA;

#[derive(Parser)]
#[command(
    name = "nodal-db",
    about = "nodal-db: create, import into and maintain nodal-db database files",
    version,
    after_help = "Exit codes: 0 success; 1 error; 75 busy (the database is locked by \
another process or write conflicts persisted: retry later). \
`maintain refresh-views --check` also exits 3 (stale) or 4 (newer than this nodal-db)."
)]
struct Cli {
    /// DuckDB memory limit for every database this command opens, e.g.
    /// `4GiB` or `25%` (of RAM). Default: 50% of RAM, or DuckDB's own
    /// sizing under SLURM.
    #[arg(long, global = true, value_name = "SIZE")]
    memory_limit: Option<String>,
    /// DuckDB worker threads per database this command opens. Default: the
    /// CPU count, at most 8, or DuckDB's own sizing under SLURM.
    #[arg(long, global = true, value_name = "N")]
    threads: Option<u64>,
    #[command(subcommand)]
    command: Command,
}

/// The CLI's default memory limit (DuckDB's own is 80% of RAM).
const DEFAULT_MEMORY_LIMIT: &str = "50%";
/// The CLI's default thread cap (DuckDB's own is every core).
const DEFAULT_MAX_THREADS: u64 = 8;

/// Is a SLURM allocation variable set? DuckDB then sizes itself from it
/// (`SLURM_CPUS_ON_NODE`, `SLURM_MEM_PER_NODE`), and the CLI keeps that.
fn slurm(var: &str) -> bool {
    std::env::var_os(var).is_some_and(|v| !v.is_empty())
}

/// The CLI's DuckDB caps: the flags, else the defaults above (a shared
/// machine keeps headroom even when a file is huge or crafted).
fn resource_limits(cli: &Cli) -> nodal_db_core::conn::ResourceLimits {
    let memory_limit = cli
        .memory_limit
        .clone()
        .or_else(|| (!slurm("SLURM_MEM_PER_NODE")).then(|| DEFAULT_MEMORY_LIMIT.to_string()));
    let threads = cli.threads.or_else(|| {
        (!slurm("SLURM_CPUS_ON_NODE")).then(|| {
            std::thread::available_parallelism()
                .map_or(DEFAULT_MAX_THREADS, |n| n.get() as u64)
                .min(DEFAULT_MAX_THREADS)
        })
    });
    nodal_db_core::conn::ResourceLimits {
        memory_limit,
        threads,
    }
}

/// One line of CLI output with terminal control characters escaped: every
/// C0 control (newline and tab included: a name must not start a line that
/// looks like the CLI's own), DEL, C1, and the bidirectional overrides, so a
/// name from someone else's file cannot move the cursor, retitle the
/// terminal, forge a line or reorder one.
fn clean(s: &str) -> Cow<'_, str> {
    escape_controls(s, false)
}

/// [`clean`] for multi-line text the CLI lays out itself (a template plan,
/// an export on a terminal): newlines and tabs stay.
fn clean_text(s: &str) -> Cow<'_, str> {
    escape_controls(s, true)
}

fn escape_controls(s: &str, keep_lines: bool) -> Cow<'_, str> {
    let bad = |c: char| {
        (c.is_control() && !(keep_lines && (c == '\n' || c == '\t')))
            || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    };
    if !s.chars().any(bad) {
        return Cow::Borrowed(s);
    }
    Cow::Owned(
        s.chars()
            .map(|c| {
                if bad(c) {
                    c.escape_unicode().to_string()
                } else {
                    c.to_string()
                }
            })
            .collect(),
    )
}

#[derive(Subcommand)]
enum Command {
    /// Create a new database file, optionally loading a template.
    Init {
        /// Path of the DuckDB file to create.
        db: PathBuf,
        /// Create one alternative with this name. Without it the file has
        /// no alternative until the first write names one (writes create
        /// missing alternatives); there is no implicit `Base`.
        #[arg(long)]
        alternative: Option<String>,
        /// Load this template (YAML or JSON IR) right after the schema.
        #[arg(long)]
        template: Option<PathBuf>,
        /// Commit author for the template load; defaults to the OS username.
        #[arg(long)]
        user: Option<String>,
        /// Development builds only (a release build refuses it): apply
        /// schema/*.sql from a directory instead of the embedded schema.
        /// Without a value, uses the repository's schema/ directory.
        #[arg(long, num_args = 0..=1, hide = !cfg!(debug_assertions))]
        schema_dir: Option<Option<PathBuf>>,
    },
    /// Import an Arrow-IPC bulk file into a database in one commit.
    ///
    /// File format and error classes (E1–E10):
    /// <https://nodal-tools.github.io/nodal-db/dev/arrow_import_format/>
    Import {
        /// Arrow-IPC bulk container (concatenated IPC streams).
        file: PathBuf,
        /// Target database (must already exist; create it with `nodal-db init`).
        #[arg(long)]
        db: PathBuf,
        /// Target alternative for segments that do not name their own; if a
        /// segment also names one, the two must match (case-insensitively).
        #[arg(long)]
        alternative: Option<String>,
        /// Assertion, not a filter: every segment must target this class,
        /// else the import fails (E4).
        #[arg(long)]
        class: Option<String>,
        /// Assertion, not a filter: every segment must target this
        /// parameter, else the import fails (E4).
        #[arg(long)]
        parameter: Option<String>,
        /// Commit author; defaults to the OS username (must be non-empty).
        #[arg(long)]
        user: Option<String>,
        /// Commit message (may be empty).
        #[arg(long, default_value = "")]
        message: String,
        /// Keep no content for the versions this import replaces: each
        /// replaced value's previous version stays in history only as a
        /// content-less stub (as if the parameter had retain_content = FALSE).
        /// New versions are written either way; values equal to the stored
        /// ones are not rewritten (and so leave no stub). Applies to all
        /// segments.
        #[arg(long)]
        force_replace: bool,
        /// What the new versions of values that carry metadata links keep:
        /// `keep` (their links) or `clear` (none). Required when the import
        /// changes such a value; values without links, new values and
        /// unchanged values need nothing.
        #[arg(long, value_enum)]
        value_metadata: Option<import::ValueMetadataArg>,
    },
    /// Physically remove one alternative and all its data, leaving a trace.
    ///
    /// Closes the alternative, deletes every row of its data in every
    /// storage tier and writes an erasure marker, in one transaction. The
    /// space returns to the operating system at the next compaction. Meant
    /// for overwriting the results of a rerun in an output database;
    /// running it on a curated input database is almost certainly a
    /// mistake.
    EraseAlternative {
        /// Target DuckDB file.
        db: PathBuf,
        /// The alternative to erase (name; case-insensitive, resolves closed
        /// identities too).
        name: String,
        /// Report what WOULD be removed (rows per table, bytes estimate) and
        /// exit without touching the database.
        #[arg(long)]
        preview: bool,
        /// Free-form operator context, stored on the marker row.
        #[arg(long)]
        note: Option<String>,
        /// Refuse unless this alternative exists (repeatable): name the
        /// replacement run's alternative so the old one is never erased
        /// before the new one is written.
        #[arg(long = "expect-present")]
        expect_present: Vec<String>,
        /// Commit author for the erasure commit; defaults to the OS username.
        #[arg(long)]
        user: Option<String>,
    },
    /// Write a history-free copy of a database into a new file.
    ///
    /// The copy holds the content valid at the source's last commit (or at
    /// --as-of N), without the superseded history. The source is only read;
    /// it keeps the complete history. The copy gets its own database
    /// identity and a horizon marker naming the source; as-of reads at
    /// commits before the extract's own commit fail in it. Writers of the
    /// source wait (or get "busy") while the copy runs.
    Extract {
        /// The source database (read only).
        src: PathBuf,
        /// The new file to create (must not exist).
        dest: PathBuf,
        /// Extract the state as it was right after this commit instead of
        /// the current state.
        #[arg(long)]
        as_of: Option<i64>,
        /// Free-form operator context, stored on the horizon marker.
        #[arg(long)]
        note: Option<String>,
        /// Author of the extract's commit; defaults to the OS username.
        #[arg(long)]
        user: Option<String>,
    },
    /// Close out everything in one scope in one commit.
    ///
    /// A versioned close-out: history is kept, nothing is removed and the
    /// file does not shrink. Refuses a reach outside the scope unless
    /// --allow-outside-scope, and on a template-stamped database a classes,
    /// parameters or value-lists purge unless --allow-on-template.
    Purge {
        /// Target DuckDB file.
        db: PathBuf,
        /// values, entities, activations, memberships, alternatives,
        /// scenarios, classes, parameters or value-lists.
        #[arg(long)]
        scope: String,
        /// Class filter (values, entities, activations, parameters).
        #[arg(long)]
        class: Option<String>,
        /// Parameter filter (values; needs --class).
        #[arg(long)]
        parameter: Option<String>,
        /// Alternative filter (values, activations).
        #[arg(long)]
        alternative: Option<String>,
        /// Group class filter (memberships).
        #[arg(long)]
        group_class: Option<String>,
        /// Names to keep (alternatives, classes, value-lists); repeatable.
        #[arg(long = "except")]
        except: Vec<String>,
        /// Allow closing objects outside the scope that the cascade
        /// reaches, and removing bindings of purged value lists.
        #[arg(long)]
        allow_outside_scope: bool,
        /// Allow closing template vocabulary on a template-stamped database.
        #[arg(long)]
        allow_on_template: bool,
        /// Report what WOULD be closed and exit without writing.
        #[arg(long)]
        preview: bool,
        /// Commit author; defaults to the OS username.
        #[arg(long)]
        user: Option<String>,
        /// Commit message (may be empty).
        #[arg(long, default_value = "")]
        message: String,
    },
    /// Database maintenance.
    Maintain {
        #[command(subcommand)]
        command: MaintainCommand,
    },
    /// Load, plan, apply and export templates.
    Template {
        #[command(subcommand)]
        command: template::TemplateCommand,
    },
}

#[derive(Subcommand)]
enum MaintainCommand {
    /// Rewrite the database file densely packed to reclaim space.
    ///
    /// Reclaims the space of old checkpoints and erased data. Keeps every
    /// row, history included, and the database identity. Takes the
    /// database's lock for the duration.
    Compact {
        /// Target DuckDB file.
        db: PathBuf,
        /// Only compact when due: the file has grown past the growth factor
        /// times its size right after the last compaction, or enough commits
        /// have landed since then (see --max-commits). Always compacts the
        /// first time, to record the size and commit. Reading the head
        /// commit for the commit count opens the database read-only once.
        #[arg(long)]
        if_needed: bool,
        /// Growth factor for --if-needed (default 3.0).
        #[arg(long, requires = "if_needed")]
        growth: Option<f64>,
        /// Commit count for --if-needed: compact once this many commits have
        /// landed since the last compaction, even if the file has not grown.
        /// Default: 60, scaled up to 100 for files from about 560 MiB. 0 disables the
        /// commit count (growth only).
        #[arg(long, requires = "if_needed")]
        max_commits: Option<u64>,
    },
    /// Update the database's views and macros to this nodal-db's version.
    ///
    /// Replaces the read surface (views and macros) when it is older than
    /// this nodal-db's. Any write with this nodal-db does the same first;
    /// use this for a file that is only read. Data and history are
    /// untouched and no commit is recorded. A current file is left alone,
    /// and so is a newer one (unless --force). Fails, changing nothing,
    /// when the refresh cannot run (e.g. a table of yours holds a name the
    /// read surface reserves).
    RefreshViews {
        /// Target DuckDB file.
        db: PathBuf,
        /// Only report the schema version and the file's and this build's
        /// read-surface versions, changing nothing. Exit code 0 = up to
        /// date, 3 = stale (older, predating versioning, or the same version
        /// with other definitions), 4 = newer than this nodal-db (it cannot
        /// refresh it), 1 = error.
        #[arg(long)]
        check: bool,
        /// Also replace a NEWER read surface with this build's (a downgrade;
        /// for development only: the newer nodal-db refreshes it back on its
        /// next write).
        #[arg(long, conflicts_with = "check")]
        force: bool,
    },
}

/// Bytes as MiB with one decimal.
fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Exit code: success (and `refresh-views --check`: up to date).
const EXIT_OK: i32 = 0;
/// Exit code: an error.
const EXIT_ERROR: u8 = 1;
/// Exit code: busy or conflicted, retryable (BSD `EX_TEMPFAIL`): another
/// process holds the database's lock or write conflicts persisted.
const EXIT_BUSY: u8 = 75;
/// `refresh-views --check`: the read surface is stale (older, predating
/// versioning, or drifted); a refresh or any write updates it.
const EXIT_STALE: i32 = 3;
/// `refresh-views --check`: the read surface is newer than this nodal-db's,
/// which cannot refresh it.
const EXIT_NEWER: i32 = 4;

/// Is the error (or any error in its chain) a retryable busy / conflict?
fn is_busy(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        matches!(
            c.downcast_ref::<nodal_db_core::error::DbError>(),
            Some(nodal_db_core::error::DbError::Busy { .. })
                | Some(nodal_db_core::error::DbError::Conflict { .. })
        ) || c.downcast_ref::<import::Retryable>().is_some()
    })
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let result = nodal_db_core::conn::set_resource_limits(resource_limits(&cli))
        .map_err(|e| anyhow::anyhow!("--memory-limit / --threads: {e}"))
        .and_then(|()| run(cli));
    // The core never prints: a stale read surface met by a read, or a failed
    // refresh before a write, is reported here once per file.
    for n in nodal_db_core::read_surface::take_notices() {
        say_err!("warning: {}", n.message);
    }
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            // What `fn main() -> anyhow::Result<()>` printed, with each
            // message escaped on its own line: core error messages quote
            // names from the file.
            say_err!("Error: {e}");
            let causes: Vec<String> = e.chain().skip(1).map(|c| c.to_string()).collect();
            if !causes.is_empty() {
                eprintln!("\nCaused by:");
                for c in causes {
                    say_err!("    {c}");
                }
            }
            std::process::ExitCode::from(if is_busy(&e) { EXIT_BUSY } else { EXIT_ERROR })
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Init {
            db,
            alternative,
            template,
            user,
            schema_dir,
        } => {
            // Refuse an existing database (CLI critique F6): the schema applies
            // file-by-file under autocommit, so re-applying onto existing state
            // fails partway and can leave a mixed state. Only an absent path
            // counts as fresh (DuckDB 2.0 cannot open a zero-byte file). The
            // core's init refuses it too; this keeps the CLI's wording.
            if std::fs::symlink_metadata(&db).is_ok() {
                bail!(
                    "{}: already exists; refusing to re-apply schema \
                     (init targets a fresh path; use a new file)",
                    db.display()
                );
            }
            // The schema SQL of a directory is arbitrary SQL: a development
            // override, never something a released binary runs.
            if schema_dir.is_some() && !cfg!(debug_assertions) {
                bail!(
                    "--schema-dir is a development-only override; a release build \
                     applies only the schema embedded in it"
                );
            }
            let default_alt = alternative.as_deref();
            match schema_dir {
                None => nodal_db_core::schema_init::init_schema_from_sources_with_default(
                    &db,
                    EMBEDDED_SCHEMA,
                    default_alt,
                )?,
                Some(None) => nodal_db_core::schema_init::init_schema_from_dir_with_default(
                    &db,
                    &nodal_db_core::schema_init::schema_dir(),
                    default_alt,
                )?,
                Some(Some(dir)) => nodal_db_core::schema_init::init_schema_from_dir_with_default(
                    &db,
                    &dir,
                    default_alt,
                )?,
            }
            say!("initialized {}", db.display());
            if let Some(file) = template {
                template::load_into(&file, &db, user.as_deref())?;
            }
            Ok(())
        }
        Command::Import {
            file,
            db,
            alternative,
            class,
            parameter,
            user,
            message,
            force_replace,
            value_metadata,
        } => import::run(&import::ImportArgs {
            file,
            db,
            alternative,
            class,
            parameter,
            user,
            message,
            force_replace,
            value_metadata,
        }),
        Command::EraseAlternative {
            db,
            name,
            preview,
            note,
            expect_present,
            user,
        } => {
            import::require_db(&db)?;
            use nodal_db_core::erasure;
            use nodal_db_core::types::{ObjectDeleteKey, ObjectKind, ObjectRef};
            let key = ObjectDeleteKey {
                kind: ObjectKind::Alternative,
                class: None,
                target: ObjectRef::Name(name.clone()),
            };
            let expect: Vec<&str> = expect_present.iter().map(String::as_str).collect();
            let opts = erasure::EraseOpts {
                note: note.as_deref(),
                expect_present: &expect,
            };
            if preview {
                let p = erasure::preview_erase_alternative(&db, &key, &opts)?;
                if !p.found {
                    bail!("alternative '{name}' not found (no identity row)");
                }
                say!(
                    "would erase alternative '{}' (id {}, identity {}):",
                    p.alternative_name,
                    p.alternative_id,
                    if p.still_current {
                        "current — will be closed"
                    } else {
                        "already closed"
                    }
                );
                for tc in &p.per_table {
                    say!("  {:<24} {:>10} rows", tc.table, tc.rows);
                }
                say!(
                    "  total {} rows, ~{} payload bytes (advisory)",
                    p.rows_total,
                    p.bytes_estimate
                );
                for s in &p.referencing_scenarios {
                    say!(
                        "  note: scenario '{}' (id {}) loses a rank row",
                        s.name,
                        s.id
                    );
                }
                if !p.referencing_runs.is_empty() {
                    say!(
                        "  note: erases the results of run(s) {:?}",
                        p.referencing_runs
                    );
                }
                // The same --expect-present check the erase enforces, reported
                // read-only (core computes both from one helper).
                for n in &expect_present {
                    if !p
                        .expect_present_problems
                        .iter()
                        .any(|prob| prob.contains(&format!("'{n}'")))
                    {
                        say!("  expect-present '{n}': present — the erase would proceed");
                    }
                }
                for prob in &p.expect_present_problems {
                    say!("  expect-present PROBLEM: the erase would refuse — {prob}");
                }
                if note.is_some() {
                    say!("  (--note is recorded only by an actual erase; ignored in preview)");
                }
                return Ok(());
            }
            let user = import::resolve_user_from(user.as_deref())?;
            // D-E6: mechanism is DB-agnostic, policy is client-side — warn
            // loudly when the target does not look like a tool-produced
            // output DB. Heuristic: output DBs carry `run` provenance rows
            // (schema_interop.md §9); a DB with none is probably a curated
            // input. Limits, stated: a runner that never records `run` rows
            // makes its output DB trip this warning spuriously, and an input
            // DB that happens to contain run rows is not caught. Warning
            // only — never a refusal (no flag surface in v1).
            {
                // Through the core's read path: per-path lock, Busy retry
                // and the shadowing guard (engine rule 8).
                let runs: i64 = nodal_db_core::conn::with_read(&db, |conn| {
                    Ok(conn.query_row("SELECT count(*) FROM run", [], |r| r.get(0))?)
                })?;
                if runs == 0 {
                    say_err!(
                        "warning: {} has no `run` provenance rows — this looks like a \
                         curated INPUT database, not a tool-produced output DB. Erase is \
                         mechanism-agnostic and will proceed, but the sanctioned \
                         use is output-DB overwrite-on-rerun.",
                        db.display()
                    );
                }
            }
            let r = erasure::erase_alternative(&db, &key, &opts, &user)?;
            if !r.erased {
                say!(
                    "nothing to erase: alternative '{name}' (id {}) is already closed with no \
                     data rows (no commit, no marker written)",
                    r.alternative_id
                );
                return Ok(());
            }
            say!(
                "erased alternative '{name}' (id {}): {} rows across {} tables, \
                 ~{} payload bytes; marker {} at commit {}{}",
                r.alternative_id,
                r.rows_total,
                r.per_table.len(),
                r.bytes_estimate,
                r.marker_id,
                r.commit_id,
                if r.identity_closed {
                    "; identity row closed"
                } else {
                    ""
                }
            );
            for tc in &r.per_table {
                say!("  {:<24} {:>10} rows", tc.table, tc.rows);
            }
            say!("(bytes return to the OS at the next compaction)");
            Ok(())
        }
        Command::Extract {
            src,
            dest,
            as_of,
            note,
            user,
        } => {
            import::require_db(&src)?;
            let user = import::resolve_user_from(user.as_deref())?;
            let opts = nodal_db_core::extract::ExtractOpts {
                as_of,
                user: &user,
                note: note.as_deref(),
            };
            let r = nodal_db_core::extract::extract(&src, &dest, &opts)?;
            let state = match as_of {
                Some(n) => format!("the state as of commit {n}"),
                None => format!("the current state (commit {})", r.horizon_commit),
            };
            say!(
                "extracted {state} of {} into {}",
                src.display(),
                dest.display()
            );
            say!("  source identity   {}", r.source_identity);
            say!("  extract identity  {}", r.extract_identity);
            say!(
                "  horizon commit {}, extract commit {}, horizon marker {}",
                r.horizon_commit,
                r.extract_commit,
                r.marker_id
            );
            say!(
                "  size {} -> {}; {} rows left out, {} unreferenced axes dropped",
                mib(r.bytes_source),
                mib(r.bytes_extract),
                r.rows_removed,
                r.axes_dropped
            );
            say!(
                "note: the extract has a NEW database identity; as-of reads before commit \
                 {} fail in it (the source keeps the full history)",
                r.extract_commit
            );
            Ok(())
        }
        Command::Purge {
            db,
            scope,
            class,
            parameter,
            alternative,
            group_class,
            except,
            allow_outside_scope,
            allow_on_template,
            preview,
            user,
            message,
        } => {
            import::require_db(&db)?;
            use nodal_db_core::types::{PurgeRequest, PurgeScope};
            // A flag the scope does not take is refused: ignoring it would
            // silently widen the purge.
            let takes: &[&str] = match scope.as_str() {
                "values" => &["class", "parameter", "alternative"],
                "entities" | "parameters" => &["class"],
                "activations" => &["class", "alternative"],
                "memberships" => &["group-class"],
                "alternatives" | "classes" | "value-lists" | "value_lists" => &["except"],
                _ => &[],
            };
            for (flag, given) in [
                ("class", class.is_some()),
                ("parameter", parameter.is_some()),
                ("alternative", alternative.is_some()),
                ("group-class", group_class.is_some()),
                ("except", !except.is_empty()),
            ] {
                if given && !takes.contains(&flag) {
                    bail!("--scope {scope} does not take --{flag}");
                }
            }
            let s = match scope.as_str() {
                "values" => PurgeScope::Values {
                    class,
                    parameter,
                    alternative,
                },
                "entities" => PurgeScope::Entities { class },
                "activations" => PurgeScope::Activations { class, alternative },
                "memberships" => PurgeScope::Memberships { group_class },
                "alternatives" => PurgeScope::Alternatives { except },
                "scenarios" => PurgeScope::Scenarios,
                "classes" => PurgeScope::Classes { except },
                "parameters" => PurgeScope::Parameters { class },
                "value-lists" | "value_lists" => PurgeScope::ValueLists { except },
                other => bail!(
                    "unknown scope {other:?} (values, entities, activations, memberships, \
                     alternatives, scenarios, classes, parameters, value-lists)"
                ),
            };
            let req = PurgeRequest {
                scope: s,
                allow_outside_scope,
                allow_on_template,
            };
            if preview {
                let p = nodal_db_core::api::preview_purge(&db, vec![req])?;
                let p = &p[0];
                say!(
                    "would close {} object(s), {} row(s):",
                    p.objects,
                    p.cascade.total_rows
                );
                say!(
                    "  {:<28} {:>10} rows",
                    "objects + identity",
                    p.cascade.object_rows
                );
                for tc in &p.cascade.per_table {
                    say!("  {:<28} {:>10} rows", tc.table, tc.rows);
                }
                for b in &p.outside_scope {
                    say!(
                        "  outside the scope: {} {} of {} ({})",
                        b.count,
                        b.table,
                        b.column,
                        b.sample.join(", ")
                    );
                }
                if p.template_stamped {
                    say!("  closes template vocabulary (needs --allow-on-template)");
                }
                say!(
                    "  long-tier cells + array elements in the scope: {}",
                    p.cells
                );
                for w in &p.warnings {
                    if let nodal_db_core::types::WarningKind::PurgeLarge { threshold, .. } = w.kind
                    {
                        say!(
                            "  warning: more than {threshold} cells: one long transaction that \
                             holds the write lock (about 10 min and 3.4 GB per 105 M cells \
                             measured)"
                        );
                    }
                }
                say!("(an upper bound: nothing is written, so nothing is spared)");
                return Ok(());
            }
            let user = import::resolve_user_from(user.as_deref())?;
            let r = nodal_db_core::api::purge(
                &db,
                vec![req],
                &user,
                &message,
                nodal_db_core::types::WriteOptions::default(),
            )?;
            for w in &r.warnings {
                if let nodal_db_core::types::WarningKind::PurgeLarge {
                    cells, threshold, ..
                } = w.kind
                {
                    say_err!("warning: the purge covered {cells} cells (more than {threshold})");
                }
            }
            let o = &r.purges[0];
            if r.commit_id == 0 {
                say!("nothing to purge (no commit)");
                return Ok(());
            }
            say!(
                "purged {} object(s), {} row(s) at commit {}{}",
                o.objects,
                o.closed_out.total_rows,
                r.commit_id,
                if o.unbound > 0 {
                    format!("; {} value-list binding(s) removed", o.unbound)
                } else {
                    String::new()
                }
            );
            for tc in &o.closed_out.per_table {
                say!("  {:<28} {:>10} rows", tc.table, tc.rows);
            }
            Ok(())
        }
        Command::Maintain {
            command:
                MaintainCommand::Compact {
                    db,
                    if_needed,
                    growth,
                    max_commits,
                },
        } => {
            import::require_db(&db)?;
            use nodal_db_core::compaction::{self, CommitBackstop, CompactionPolicy};
            let r = if if_needed {
                let policy = CompactionPolicy {
                    growth_factor: growth.unwrap_or(compaction::DEFAULT_GROWTH_FACTOR),
                    max_commits: match max_commits {
                        None => CommitBackstop::SizeScaled,
                        Some(n) => CommitBackstop::Fixed(n),
                    },
                };
                compaction::compact_if_due(&db, policy, None)?
            } else {
                compaction::compact(&db)?
            };
            let why = match r.trigger {
                Some(compaction::CompactionTrigger::NoBaseline) => " (no baseline recorded)",
                Some(compaction::CompactionTrigger::Growth) => " (file growth)",
                Some(compaction::CompactionTrigger::Commits) => " (commit count)",
                Some(compaction::CompactionTrigger::Requested) | None => "",
            };
            if let Some(w) = &r.warning {
                say_err!("warning: {w}");
            }
            if r.kept_original {
                say!(
                    "{}: compaction ran{why} but did not shrink the file \
                     ({} rebuilt vs {}); original kept",
                    db.display(),
                    mib(r.bytes_rebuilt),
                    mib(r.bytes_before)
                );
            } else if r.swapped {
                say!(
                    "compacted {}: {} -> {}{why}",
                    db.display(),
                    mib(r.bytes_before),
                    mib(r.bytes_after)
                );
            } else {
                say!(
                    "{}: no compaction needed ({}; below the growth and commit thresholds)",
                    db.display(),
                    mib(r.bytes_before)
                );
            }
            Ok(())
        }
        Command::Maintain {
            command: MaintainCommand::RefreshViews { db, check, force },
        } => {
            import::require_db(&db)?;
            use nodal_db_core::read_surface::SurfaceStatus;
            if check {
                let r = nodal_db_core::api::read_surface_status(&db)?;
                say!("{}", db.display());
                say!(
                    "  schema version         {}",
                    r.schema_version.as_deref().unwrap_or("(unreadable)")
                );
                say!("  file read surface      {}", r.check.found_label());
                say!("  this nodal-db          {}", r.check.expected);
                say!("  status                 {}", r.check.status.as_str());
                let code = match r.check.status {
                    SurfaceStatus::Current => EXIT_OK,
                    SurfaceStatus::Older | SurfaceStatus::Missing | SurfaceStatus::Drift => {
                        EXIT_STALE
                    }
                    SurfaceStatus::Newer => EXIT_NEWER,
                };
                for n in nodal_db_core::read_surface::take_notices() {
                    say_err!("warning: {}", n.message);
                }
                std::process::exit(code);
            }
            let r = nodal_db_core::api::refresh_read_surface(&db, force)?;
            if r.refreshed {
                say!(
                    "refreshed the read surface of {}: {} -> {}",
                    db.display(),
                    r.check.found_label(),
                    r.check.expected
                );
            } else if r.check.status == SurfaceStatus::Newer {
                say!(
                    "{}: read surface {} is newer than this nodal-db's ({}); left alone \
                     (--force downgrades it)",
                    db.display(),
                    r.check.found_label(),
                    r.check.expected
                );
            } else {
                say!(
                    "{}: read surface {} is current; nothing to do",
                    db.display(),
                    r.check.found_label()
                );
            }
            Ok(())
        }
        Command::Template { command } => template::run(command),
    }
}
