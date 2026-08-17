//! `spinal-db` CLI — Phase 0 skeleton. Verb surface only; the `init` and
//! `import` bodies land in Phase 4 (they reuse spinal-db-core, never a
//! second canonicalizer).

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "spinal-db", about = "spinal_db core CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply schema/*.sql into a fresh DuckDB file.
    Init {
        /// Path of the DuckDB file to create.
        db: String,
    },
    /// Ingest an Arrow-IPC bulk file through the one canonicalizer.
    Import {
        /// Arrow-IPC bulk file to import.
        file: String,
        /// Target DuckDB file.
        #[arg(long)]
        db: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init { .. } => println!("init: not yet implemented"),
        Command::Import { .. } => println!("import: not yet implemented"),
    }
    Ok(())
}
