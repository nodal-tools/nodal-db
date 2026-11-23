//! `nodal-db template` — the template verbs: `load` (fresh-init load), `plan`
//! (read-only dry run), `apply` (reconcile: migration chain + additive diff),
//! `export` (round-trip). All decisions live in the core — the CLI parses
//! arguments and prints reports; in particular the template-name retarget
//! guard is `template::apply`'s own refusal (one decision for every front
//! end), and the CLI deliberately does NOT expose the
//! `allow_retarget`/`allow_downgrade` overrides.
//!
//! Escape-hatch (`run:`) programs are LIBRARY-registered
//! (`template::MigrationRegistry`); the CLI ships an empty registry, so a
//! chain naming a program fails loudly here — a tool whose chain uses such
//! programs runs it through its own code, which registers them.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Subcommand;
use nodal_db_core::error::DbError;
use nodal_db_core::template as tpl;

use crate::import::{require_db, resolve_user_from};

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
        /// Plan as `apply --recreate-removed` would: re-create removed
        /// classes, parameters and value lists instead of blocking on them.
        #[arg(long)]
        recreate_removed: bool,
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
        /// Re-create the declared classes, parameters and value lists that
        /// were removed locally (by an object delete, a purge or a dropped
        /// list) instead of refusing on them.
        #[arg(long)]
        recreate_removed: bool,
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
        TemplateCommand::Load {
            file,
            db,
            user,
            message,
        } => apply_file(&file, &db, user.as_deref(), &message, None, false),
        TemplateCommand::Plan {
            file,
            db,
            recreate_removed,
        } => {
            require_db(&db)?;
            let t = parse(&file)?;
            let opts = tpl::ApplyOptions {
                recreate_removed,
                ..Default::default()
            };
            let p = tpl::plan_with(&db, &t, &opts).map_err(anyhow::Error::from)?;
            print!("{}", crate::clean_text(&p.to_string()));
            Ok(())
        }
        TemplateCommand::Apply {
            file,
            db,
            user,
            message,
            expect_commit,
            recreate_removed,
        } => apply_file(
            &file,
            &db,
            user.as_deref(),
            &message,
            expect_commit,
            recreate_removed,
        ),
        TemplateCommand::Export { db, out, json } => {
            require_db(&db)?;
            let (t, warnings) = tpl::export(&db).map_err(anyhow::Error::from)?;
            for w in &warnings {
                say_err!("warning: {w}");
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
                    say_err!(
                        "exported template {} version {} to {}",
                        t.template.name,
                        t.template.version,
                        path.display()
                    );
                }
                // The export is data: written to a pipe or a file it stays
                // byte-exact. On a terminal, controls the serializers leave
                // as they are (C1, bidi overrides) are escaped.
                None if std::io::IsTerminal::is_terminal(&std::io::stdout()) => {
                    print!("{}", crate::clean_text(&text))
                }
                None => print!("{text}"),
            }
            Ok(())
        }
    }
}

/// The `init --template` path: load right after schema init.
pub fn load_into(file: &Path, db: &Path, user: Option<&str>) -> Result<()> {
    apply_file(file, db, user, "", None, false)
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
    recreate_removed: bool,
) -> Result<()> {
    require_db(db)?;
    let t = parse(file)?;
    let user = resolve_user_from(user)?;
    let registry = tpl::MigrationRegistry::new();
    let opts = tpl::ApplyOptions {
        expect_commit,
        recreate_removed,
        ..Default::default()
    };
    let outcome = tpl::apply(db, &t, &registry, &user, message, &opts).map_err(|e| match e {
        // The core's Stale text says "reload and re-apply"; for a pinned
        // template apply the reload is a new plan.
        DbError::Stale { expected, found } => anyhow::anyhow!(
            "stale plan: the database is at commit {found} but the plan was made at \
             commit {expected} — re-plan and re-apply"
        ),
        other => anyhow::Error::from(other),
    })?;
    if outcome.no_op {
        say!(
            "template {} version {}: already applied (content hash match) — no-op",
            t.template.name,
            t.template.version
        );
        return Ok(());
    }
    for w in &outcome.warnings {
        say_err!("warning: {w}");
    }
    say!(
        "applied template {} version {} in {} commit(s): {} action(s){}",
        t.template.name,
        t.template.version,
        outcome.commits.len(),
        outcome.plan.actions.len(),
        if outcome.converged {
            ""
        } else {
            " [PARTIAL — content hash withheld]"
        }
    );
    for a in &outcome.plan.actions {
        say!("  + {a}");
    }
    for n in &outcome.notes {
        say!("  note: {n}"); // chain-step records (D14 cascade counts &c.)
    }
    for s in &outcome.plan.skipped {
        say!("  skip: {s}");
    }
    Ok(())
}
