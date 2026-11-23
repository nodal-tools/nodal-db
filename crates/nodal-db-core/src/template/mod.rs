//! Templates: parse the YAML-1.2/JSON template document, validate it, load it
//! into a fresh database, reconcile it against a populated one, and export it
//! back out (`nodal-db template export`).
//!
//! The loader is a CLIENT of the one write path: it lives in this crate,
//! writes structural rows through the same from/to protocol as `api.rs`'s
//! resolvers (declared type members via `api::create_parameter_type` — the one
//! insert path), and routes defaults — which ARE values — through
//! `history::write_definition_default` (rank-0 scalars are their own canonical
//! form). There is no second canonicalizer and no template-privileged writer.
//!
//! Module map:
//! - [`ir`] — the object model, parser (serde_norway, YAML 1.2 core schema),
//!   pure load-time validation, normalization + `template_content_hash`.
//! - [`dbvocab`] — snapshot the DB's current vocabulary (the diff's DB side,
//!   the export's source).
//! - [`reconcile`] — plan (read-only dry run) / apply (migration chain +
//!   additive diff, one commit), the rename/merge hint executor, the `run:`
//!   program registry.
//! - [`export`](mod@export) — DB → [`ir::Template`] (round-trip).

pub mod dbvocab;
pub mod export;
pub mod ir;
pub mod reconcile;

pub use export::export;
pub use ir::{parse_file, parse_str, template_content_hash, to_json, to_yaml, validate, Template};
pub use reconcile::{
    apply, plan, plan_with, ApplyOptions, ApplyOutcome, MigrationRegistry, ReconcilePlan,
    RunProgramReport,
};
