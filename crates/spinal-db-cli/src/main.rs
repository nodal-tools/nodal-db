//! `spinal-db` CLI — verb dispatch (`specs/rust_implementation_plan.md`
//! Part 4). `init` applies the schema; `import` ingests the Arrow-IPC bulk
//! format (`spec/arrow_ipc_bulk_format.md`) through the one core
//! canonicalizer — the CLI deserializes and expands, it NEVER
//! re-canonicalizes (CLAUDE.md invariant 2). `migrate`/`maintain`/`template`
//! are explicit not-implemented stubs freezing the verb surface (§3.3 door 3).

mod import;

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
#[command(name = "spinal-db", about = "spinal_db core CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply the schema into a fresh DuckDB file.
    Init {
        /// Path of the DuckDB file to create.
        db: PathBuf,
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
    /// Not implemented in v1 core (verb reserved).
    Migrate,
    /// Not implemented in v1 core (verb reserved).
    Maintain,
    /// Not implemented in v1 core (verb reserved).
    Template,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init { db, schema_dir } => {
            match schema_dir {
                None => spinal_db_core::schema_init::init_schema_from_sources(&db, EMBEDDED_SCHEMA)?,
                Some(None) => spinal_db_core::schema_init::init_schema_from_dir(
                    &db,
                    &spinal_db_core::schema_init::schema_dir(),
                )?,
                Some(Some(dir)) => spinal_db_core::schema_init::init_schema_from_dir(&db, &dir)?,
            }
            println!("initialized {}", db.display());
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
        Command::Migrate => bail!("migrate: not implemented in v1 core"),
        Command::Maintain => bail!("maintain: not implemented in v1 core"),
        Command::Template => bail!("template: not implemented in v1 core"),
    }
}
