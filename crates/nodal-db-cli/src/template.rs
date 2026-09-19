//! `nodal-db template` — the template-IR verbs (template_ir_plan phases
//! 2b/3/6): `load` (fresh-init load), `plan` (read-only dry run), `apply`
//! (reconcile: migration chain + additive diff), `export` (round-trip
//! acceptance 8.1.7). All writes go through the core's template module — the
//! CLI parses arguments and prints reports, nothing else.
//!
//! Escape-hatch (`run:`) programs are LIBRARY-registered
//! (`template::MigrationRegistry`); the CLI ships an empty registry, so a
//! chain naming a program fails loudly here — tenants run such chains through
//! their own tool, which registers its migration code (§4.2 Layer 3).

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Subcommand;
use nodal_db_core::template as tpl;

#[derive(Subcommand)]
pub enum TemplateCommand {
    /// Load a template into a freshly initialized database. Refuses a DB
    /// already stamped with a DIFFERENT template (use `apply` to reconcile
    /// or layer).
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
    /// plan without writing anything.
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
            // Fresh-load guard: a DB stamped with a different template is a
            // reconcile/layering situation — make the caller say `apply`.
            let (t, _) = parse(&file)?;
            let p = tpl::plan(&db, &t).map_err(anyhow::Error::from)?;
            if let Some(dbn) = &p.db_template_name {
                if dbn.to_lowercase() != t.template.name.to_lowercase() {
                    bail!(
                        "{}: already stamped with template {dbn:?}; `template load` is the \
                         fresh-init path — use `template apply` to reconcile or layer",
                        db.display()
                    );
                }
            }
            apply_and_report(&file, &db, user.as_deref(), &message)
        }
        TemplateCommand::Plan { file, db } => {
            let (t, _) = parse(&file)?;
            let p = tpl::plan(&db, &t).map_err(anyhow::Error::from)?;
            print!("{p}");
            Ok(())
        }
        TemplateCommand::Apply { file, db, user, message } => {
            apply_and_report(&file, &db, user.as_deref(), &message)
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
    apply_and_report(file, db, user, "")
}

fn parse(file: &Path) -> Result<(tpl::Template, ())> {
    let t = tpl::parse_file(file)
        .with_context(|| format!("parsing template {}", file.display()))?;
    Ok((t, ()))
}

fn apply_and_report(file: &Path, db: &Path, user: Option<&str>, message: &str) -> Result<()> {
    let (t, _) = parse(file)?;
    let user = resolve_user(user)?;
    let registry = tpl::MigrationRegistry::new();
    let outcome = tpl::apply(db, &t, &registry, &user, message).map_err(anyhow::Error::from)?;
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
        "applied template {} version {} in {} commit(s): {} action(s)",
        t.template.name,
        t.template.version,
        outcome.commits.len(),
        outcome.plan.actions.len()
    );
    for a in &outcome.plan.actions {
        println!("  + {a}");
    }
    Ok(())
}

/// Commit-author resolution, same contract as `import` (spec §8.2): `--user`
/// if given (non-empty), else the first non-blank of USER/USERNAME/LOGNAME.
fn resolve_user(user: Option<&str>) -> Result<String> {
    if let Some(u) = user {
        let u = u.trim();
        if u.is_empty() {
            bail!("--user must be non-empty");
        }
        return Ok(u.to_string());
    }
    for var in ["USER", "USERNAME", "LOGNAME"] {
        if let Ok(v) = std::env::var(var) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Ok(v);
            }
        }
    }
    bail!(
        "no commit author: --user was not given and none of USER/USERNAME/LOGNAME is set \
         to a non-blank value (pass --user <name>)"
    )
}
