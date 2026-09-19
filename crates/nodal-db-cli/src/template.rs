//! `nodal-db template` — the template-IR verbs (template_ir_plan phases
//! 2b/3/6): `load` (fresh-init load), `plan` (read-only dry run), `apply`
//! (reconcile: migration chain + additive diff), `export` (round-trip
//! acceptance 8.1.7). All decisions live in the core — the CLI parses
//! arguments and prints reports; in particular the template-name retarget
//! guard is `template::apply`'s own refusal (one decision for every front
//! end), and the CLI deliberately does NOT expose the
//! `allow_retarget`/`allow_downgrade` overrides yet.
//!
//! Escape-hatch (`run:`) programs are LIBRARY-registered
//! (`template::MigrationRegistry`); the CLI ships an empty registry, so a
//! chain naming a program fails loudly here — tenants run such chains through
//! their own tool, which registers its migration code (§4.2 Layer 3).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Subcommand;
use nodal_db_core::template as tpl;

use crate::import::resolve_user_from;

#[derive(Subcommand)]
pub enum TemplateCommand {
    /// Load a template into a freshly initialized database (the core refuses
    /// a DB already stamped with a different template).
    Load {
        /// Template document (YAML 1.2 or JSON — one parser reads both).
        file: PathBuf,
        /// Target DuckDB file (created by `nodal-db init`).
        #[arg(long)]
        db: PathBuf,
        /// Commit author; defaults to the OS username.
        #[arg(long)]
        user: Option<String>,
        /// Commit message (defaults to a template/version line).
        #[arg(long, default_value = "")]
        message: String,
    },
    /// Dry run: print the pending migration chain and the additive reconcile
    /// plan without writing anything (quote the printed commit id to
    /// `apply --expect-commit` to pin it).
    Plan {
        /// Template document (YAML or JSON).
        file: PathBuf,
        #[arg(long)]
        db: PathBuf,
    },
    /// Reconcile the database to the template: run the pending migration
    /// chain, then apply the additive diff in one commit (plan-then-apply).
    Apply {
        /// Template document (YAML or JSON).
        file: PathBuf,
        #[arg(long)]
        db: PathBuf,
        #[arg(long)]
        user: Option<String>,
        #[arg(long, default_value = "")]
        message: String,
        /// Refuse unless the database is exactly at this commit id (the id
        /// `template plan` printed) — the plan→apply staleness guard.
        #[arg(long)]
        expect_commit: Option<i64>,
    },
    /// Re-emit the template IR from a loaded database (semantic round-trip:
    /// export and source compare equal under the normalized object model).
    Export {
        #[arg(long)]
        db: PathBuf,
        /// Output file; stdout when omitted.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Emit JSON instead of YAML.
        #[arg(long)]
        json: bool,
    },
}

pub fn run(cmd: TemplateCommand) -> Result<()> {
    match cmd {
        TemplateCommand::Load { file, db, user, message } => {
            apply_file(&file, &db, user.as_deref(), &message, None)
        }
        TemplateCommand::Plan { file, db } => {
            let t = parse(&file)?;
            let p = tpl::plan(&db, &t).map_err(anyhow::Error::from)?;
            print!("{p}");
            Ok(())
        }
        TemplateCommand::Apply { file, db, user, message, expect_commit } => {
            apply_file(&file, &db, user.as_deref(), &message, expect_commit)
        }
        TemplateCommand::Export { db, out, json } => {
            let (t, warnings) = tpl::export(&db).map_err(anyhow::Error::from)?;
            for w in &warnings {
                eprintln!("warning: {w}");
            }
            let text = if json {
                tpl::to_json(&t).map_err(anyhow::Error::from)?
            } else {
                tpl::to_yaml(&t).map_err(anyhow::Error::from)?
            };
            match out {
                Some(path) => {
                    std::fs::write(&path, &text)
                        .with_context(|| format!("writing {}", path.display()))?;
                    eprintln!(
                        "exported template {} version {} to {}",
                        t.template.name,
                        t.template.version,
                        path.display()
                    );
                }
                None => print!("{text}"),
            }
            Ok(())
        }
    }
}

/// The `init --template` path: load right after schema init.
pub fn load_into(file: &Path, db: &Path, user: Option<&str>) -> Result<()> {
    apply_file(file, db, user, "", None)
}

fn parse(file: &Path) -> Result<tpl::Template> {
    tpl::parse_file(file).with_context(|| format!("parsing template {}", file.display()))
}

/// Parse ONCE, apply, and report (the core validates/reads inside `apply`;
/// no CLI-side pre-plan — one parse, one snapshot chain).
fn apply_file(
    file: &Path,
    db: &Path,
    user: Option<&str>,
    message: &str,
    expect_commit: Option<i64>,
) -> Result<()> {
    let t = parse(file)?;
    let user = resolve_user_from(user)?;
    let registry = tpl::MigrationRegistry::new();
    let opts = tpl::ApplyOptions { expect_commit, ..Default::default() };
    let outcome =
        tpl::apply(db, &t, &registry, &user, message, &opts).map_err(anyhow::Error::from)?;
    if outcome.no_op {
        println!(
            "template {} version {}: already applied (content hash match) — no-op",
            t.template.name, t.template.version
        );
        return Ok(());
    }
    for w in &outcome.warnings {
        eprintln!("warning: {w}");
    }
    println!(
        "applied template {} version {} in {} commit(s): {} action(s){}",
        t.template.name,
        t.template.version,
        outcome.commits.len(),
        outcome.plan.actions.len(),
        if outcome.converged { "" } else { " [PARTIAL — content hash withheld]" }
    );
    for a in &outcome.plan.actions {
        println!("  + {a}");
    }
    for s in &outcome.plan.skipped {
        println!("  skip: {s}");
    }
    Ok(())
}
