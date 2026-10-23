//! `nodal-db` CLI — verb dispatch (`specs/rust_implementation_plan.md`
//! Part 4). `init` applies the schema (optionally loading a template — the
//! init-with-template path); `import` ingests the Arrow-IPC bulk format
//! (`specs/arrow_ipc_bulk_format.md`) through the one core canonicalizer — the
//! CLI deserializes and expands, it NEVER re-canonicalizes (CLAUDE.md
//! invariant 2). `template` is un-frozen (template_ir_plan phase 2/3:
//! load / plan / apply / export); `extract` writes a history-free copy of a
//! database into a new file; `maintain` is a subcommand group whose first
//! member is `compact` (the physical repack). `migrate` remains an explicit
//! not-implemented stub reserving the verb (§3.3 door 3) — the reconcile
//! runner lives under `template apply` in v1 (§4.4: `migrate` becomes the
//! full-chain runner as a later door).

mod import;
mod template;

use std::path::PathBuf;

use anyhow::bail;
use clap::{Parser, Subcommand};

/// The compile-time-embedded schema (plan Q4): ONE list, owned by the core
/// (`schema_init::EMBEDDED_SCHEMA`) and shared with every other embedder
/// (the Python skin), so the copies cannot drift. A census test in the core
/// asserts it matches `schema/*.sql` exactly.
use nodal_db_core::schema_init::EMBEDDED_SCHEMA;

#[derive(Parser)]
#[command(name = "nodal-db", about = "nodal_db core CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply the schema into a fresh DuckDB file (optionally loading a
    /// template — the init-with-template path).
    Init {
        /// Path of the DuckDB file to create.
        db: PathBuf,
        /// Seed one default alternative with this name. Omitted ⇒ no
        /// alternative is seeded (the value write path auto-creates on first
        /// use); the DB is never implicitly given a `Base`. A template's seed
        /// alternative maps HERE, never to template content.
        #[arg(long)]
        alternative: Option<String>,
        /// Load this template (YAML or JSON IR) right after the schema.
        #[arg(long)]
        template: Option<PathBuf>,
        /// Commit author for the template load; defaults to the OS username.
        #[arg(long)]
        user: Option<String>,
        /// Dev override: apply schema/*.sql from a directory instead of the
        /// compile-time-embedded schema. Without a value, uses the repo's
        /// schema/ directory as resolved by the core.
        #[arg(long, num_args = 0..=1)]
        schema_dir: Option<Option<PathBuf>>,
    },
    /// Import an Arrow-IPC bulk file into an existing database, atomically
    /// (one commit).
    ///
    /// File format and error classes (E1–E10):
    /// https://nodal-tools.github.io/nodal-db/dev/arrow_import_format/
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
        /// New versions are written either way. Applies to all segments.
        #[arg(long)]
        force_replace: bool,
    },
    /// Hard-erase one alternative's data (physical removal WITH a trace:
    /// closes the identity row, DELETEs every alternative-keyed row across
    /// all tiers, and writes an erasure_marker in the same transaction).
    /// Bytes return to the OS at the next compaction. NOTE: the mechanism
    /// works on any database; running it against a curated input database is
    /// almost certainly a mistake — the intended use is overwriting an output
    /// database on rerun.
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
        /// Rerun-overwrite ordering belt: each named alternative must have a
        /// current row (the replacement run) or the erase refuses.
        #[arg(long = "expect-present")]
        expect_present: Vec<String>,
        /// Commit author for the erasure commit; defaults to the OS username.
        #[arg(long)]
        user: Option<String>,
    },
    /// Write a history-free copy of a database into a NEW file: the content
    /// valid at the source's last commit (or at --as-of N), without the
    /// superseded history. The source is only read; it stays the complete
    /// history (the archive). The copy gets its own database identity and a
    /// horizon marker naming the source; as-of reads at commits before the
    /// extract's own commit fail loudly in it. Writers of the source wait (or
    /// get "busy") while the copy runs.
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
    /// Not implemented in v1 core (verb reserved; the v1 reconcile runner is
    /// `template apply`).
    Migrate,
    /// Database maintenance.
    Maintain {
        #[command(subcommand)]
        command: MaintainCommand,
    },
    /// Template IR operations: load / plan / apply / export.
    Template {
        #[command(subcommand)]
        command: template::TemplateCommand,
    },
}

#[derive(Subcommand)]
enum MaintainCommand {
    /// Rewrite the database file densely packed, reclaiming the space of
    /// old checkpoints and erased data. Keeps every row, history included,
    /// and the database identity. Takes the database's lock for the
    /// duration.
    Compact {
        /// Target DuckDB file.
        db: PathBuf,
        /// Only compact when the file has grown past the growth factor
        /// times its size right after the last compaction (always compacts
        /// the first time, to record that size).
        #[arg(long)]
        if_needed: bool,
        /// Growth factor for --if-needed (default 3.0).
        #[arg(long, requires = "if_needed")]
        growth: Option<f64>,
    },
}

/// Bytes as MiB with one decimal.
fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
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
            // fails partway and can leave a mixed state. Only an absent or
            // zero-byte path (a `touch`ed placeholder) counts as fresh.
            if std::fs::metadata(&db).map(|m| m.len() > 0).unwrap_or(false) {
                bail!(
                    "{}: already exists; refusing to re-apply schema \
                     (init targets a fresh path; use a new file)",
                    db.display()
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
            println!("initialized {}", db.display());
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
        } => import::run(&import::ImportArgs {
            file,
            db,
            alternative,
            class,
            parameter,
            user,
            message,
            force_replace,
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
                println!(
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
                    println!("  {:<24} {:>10} rows", tc.table, tc.rows);
                }
                println!(
                    "  total {} rows, ~{} payload bytes (advisory)",
                    p.rows_total, p.bytes_estimate
                );
                for s in &p.referencing_scenarios {
                    println!(
                        "  note: scenario '{}' (id {}) loses a rank row",
                        s.name, s.id
                    );
                }
                if !p.referencing_runs.is_empty() {
                    println!(
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
                        println!("  expect-present '{n}': present — the erase would proceed");
                    }
                }
                for prob in &p.expect_present_problems {
                    println!("  expect-present PROBLEM: the erase would refuse — {prob}");
                }
                if note.is_some() {
                    println!("  (--note is recorded only by an actual erase; ignored in preview)");
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
                use nodal_db_core::duckdb::{AccessMode, Config, Connection};
                let config = Config::default().access_mode(AccessMode::ReadOnly)?;
                let conn = Connection::open_with_flags(&db, config)?;
                let runs: i64 = conn.query_row("SELECT count(*) FROM run", [], |r| r.get(0))?;
                let _ = conn.close();
                if runs == 0 {
                    eprintln!(
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
                println!(
                    "nothing to erase: alternative '{name}' (id {}) is already closed with no \
                     data rows (no commit, no marker written)",
                    r.alternative_id
                );
                return Ok(());
            }
            println!(
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
                println!("  {:<24} {:>10} rows", tc.table, tc.rows);
            }
            println!("(bytes return to the OS at the next compaction)");
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
            println!(
                "extracted {state} of {} into {}",
                src.display(),
                dest.display()
            );
            println!("  source identity   {}", r.source_identity);
            println!("  extract identity  {}", r.extract_identity);
            println!(
                "  horizon commit {}, extract commit {}, horizon marker {}",
                r.horizon_commit, r.extract_commit, r.marker_id
            );
            println!(
                "  size {} -> {}; {} rows left out, {} unreferenced axes dropped",
                mib(r.bytes_source),
                mib(r.bytes_extract),
                r.rows_removed,
                r.axes_dropped
            );
            println!(
                "note: the extract has a NEW database identity; as-of reads before commit \
                 {} fail in it (the source keeps the full history)",
                r.extract_commit
            );
            Ok(())
        }
        Command::Migrate => bail!(
            "migrate: not implemented in v1 core (the v1 reconcile runner is \
             `template apply`)"
        ),
        Command::Maintain {
            command:
                MaintainCommand::Compact {
                    db,
                    if_needed,
                    growth,
                },
        } => {
            import::require_db(&db)?;
            use nodal_db_core::compaction;
            let r = if if_needed {
                compaction::compact_if_needed(
                    &db,
                    growth.unwrap_or(compaction::DEFAULT_GROWTH_FACTOR),
                )?
            } else {
                compaction::compact(&db)?
            };
            if r.swapped {
                println!(
                    "compacted {}: {} -> {}",
                    db.display(),
                    mib(r.bytes_before),
                    mib(r.bytes_after)
                );
            } else {
                println!(
                    "{}: no compaction needed ({}; below the growth threshold)",
                    db.display(),
                    mib(r.bytes_before)
                );
            }
            Ok(())
        }
        Command::Template { command } => template::run(command),
    }
}
