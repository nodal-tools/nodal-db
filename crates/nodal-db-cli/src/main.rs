//! `nodal-db` CLI — verb dispatch (`specs/rust_implementation_plan.md`
//! Part 4). `init` applies the schema (optionally loading a template — the
//! init-with-template path); `import` ingests the Arrow-IPC bulk format
//! (`spec/arrow_ipc_bulk_format.md`) through the one core canonicalizer — the
//! CLI deserializes and expands, it NEVER re-canonicalizes (CLAUDE.md
//! invariant 2). `template` is un-frozen (template_ir_plan phase 2/3:
//! load / plan / apply / export); `migrate`/`maintain` remain explicit
//! not-implemented stubs reserving the verbs (§3.3 door 3) — the reconcile
//! runner lives under `template apply` in v1 (§4.4: `migrate` becomes the
//! full-chain runner as a later door).

mod import;
mod template;

use std::path::PathBuf;

use anyhow::bail;
use clap::{Parser, Subcommand};

/// The schema embedded at compile time (plan Q4 decision: `include_str!`
/// embedding, so a deployed binary needs no schema directory). File names are
/// kept for lexical ordering — the same order `schema/build.py` and
/// `core::schema_init` apply. Adding/renaming a schema file is a compile
/// error here, loudly.
static EMBEDDED_SCHEMA: &[(&str, &str)] = &[
    ("001_meta.sql", include_str!("../../../schema/001_meta.sql")),
    ("010_entities.sql", include_str!("../../../schema/010_entities.sql")),
    ("020_alternatives.sql", include_str!("../../../schema/020_alternatives.sql")),
    ("030_parameters.sql", include_str!("../../../schema/030_parameters.sql")),
    ("040_axes.sql", include_str!("../../../schema/040_axes.sql")),
    ("050_values.sql", include_str!("../../../schema/050_values.sql")),
    ("055_provenance.sql", include_str!("../../../schema/055_provenance.sql")),
    ("060_display.sql", include_str!("../../../schema/060_display.sql")),
    ("065_semantic.sql", include_str!("../../../schema/065_semantic.sql")),
    ("070_views_current.sql", include_str!("../../../schema/070_views_current.sql")),
    ("080_views_read.sql", include_str!("../../../schema/080_views_read.sql")),
    ("090_views_resolution.sql", include_str!("../../../schema/090_views_resolution.sql")),
    ("100_views_completeness.sql", include_str!("../../../schema/100_views_completeness.sql")),
    ("110_views_history.sql", include_str!("../../../schema/110_views_history.sql")),
    ("120_views_audit.sql", include_str!("../../../schema/120_views_audit.sql")),
    ("900_bootstrap.sql", include_str!("../../../schema/900_bootstrap.sql")),
];

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
        /// Seed one default alternative with this name (decision D3). Omitted ⇒
        /// no alternative is seeded (the value write path auto-creates on first
        /// use); the DB is never implicitly given a `Base`. A template's seed
        /// alternative maps HERE, never to IR content (template_ir_plan §1.2).
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
    /// Ingest an Arrow-IPC bulk container (spec/arrow_ipc_bulk_format.md)
    /// through the one core canonicalizer, atomically (one commit).
    Import {
        /// Arrow-IPC bulk container (concatenated IPC streams).
        file: PathBuf,
        /// Target DuckDB file.
        #[arg(long)]
        db: PathBuf,
        /// Target alternative when segments do not stamp their own
        /// (spec §4.3: when both are present they must match).
        #[arg(long)]
        alternative: Option<String>,
        /// Assertion (not a selector, spec OPEN-Q3-6b): every segment must
        /// target this class, else the import errors.
        #[arg(long)]
        class: Option<String>,
        /// Assertion (not a selector): every segment must target this
        /// parameter, else the import errors.
        #[arg(long)]
        parameter: Option<String>,
        /// Commit author; defaults to the OS username (must be non-empty).
        #[arg(long)]
        user: Option<String>,
        /// Commit message (may be empty).
        #[arg(long, default_value = "")]
        message: String,
        /// Replace identical current values instead of no-op'ing them
        /// (core WriteOptions.force_replace, applied to ALL segments).
        #[arg(long)]
        force_replace: bool,
    },
    /// Hard-erase one alternative's data (physical removal WITH a trace:
    /// closes the identity row, DELETEs every alternative-keyed row across
    /// all tiers, and writes an erasure_marker in the same transaction —
    /// specs/erasure_compaction_plan.md). Bytes return to the OS at the next
    /// compaction. NOTE (D-E6): the mechanism is DB-agnostic; running it
    /// against a curated INPUT database is almost certainly a mistake — the
    /// sanctioned customer is output-DB overwrite-on-rerun.
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
    /// Not implemented in v1 core (verb reserved; the v1 reconcile runner is
    /// `template apply`).
    Migrate,
    /// Not implemented in v1 core (verb reserved).
    Maintain,
    /// Template IR operations: load / plan / apply / export
    /// (specs/template_ir_plan.md).
    Template {
        #[command(subcommand)]
        command: template::TemplateCommand,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init { db, alternative, template, user, schema_dir } => {
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
                Some(Some(dir)) => {
                    nodal_db_core::schema_init::init_schema_from_dir_with_default(
                        &db, &dir, default_alt,
                    )?
                }
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
        Command::EraseAlternative { db, name, preview, note, expect_present, user } => {
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
                    if p.still_current { "current — will be closed" } else { "already closed" }
                );
                for tc in &p.per_table {
                    println!("  {:<24} {:>10} rows", tc.table, tc.rows);
                }
                println!(
                    "  total {} rows, ~{} payload bytes (advisory)",
                    p.rows_total, p.bytes_estimate
                );
                for s in &p.referencing_scenarios {
                    println!("  note: scenario '{}' (id {}) loses a rank row", s.name, s.id);
                }
                if !p.referencing_runs.is_empty() {
                    println!("  note: erases the results of run(s) {:?}", p.referencing_runs);
                }
                // The same --expect-present check the erase enforces, reported
                // read-only (core computes both from one helper).
                for n in &expect_present {
                    if !p.expect_present_problems.iter().any(|prob| prob.contains(&format!("'{n}'"))) {
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
                let runs: i64 =
                    conn.query_row("SELECT count(*) FROM run", [], |r| r.get(0))?;
                let _ = conn.close();
                if runs == 0 {
                    eprintln!(
                        "warning: {} has no `run` provenance rows — this looks like a \
                         curated INPUT database, not a tool-produced output DB. Erase is \
                         mechanism-agnostic (D-E6) and will proceed, but the sanctioned \
                         use is output-DB overwrite-on-rerun (schema_interop.md §9).",
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
                if r.identity_closed { "; identity row closed" } else { "" }
            );
            for tc in &r.per_table {
                println!("  {:<24} {:>10} rows", tc.table, tc.rows);
            }
            println!("(bytes return to the OS at the next compaction)");
            Ok(())
        }
        Command::Migrate => bail!(
            "migrate: not implemented in v1 core (the v1 reconcile runner is \
             `template apply`)"
        ),
        Command::Maintain => bail!("maintain: not implemented in v1 core"),
        Command::Template { command } => template::run(command),
    }
}
