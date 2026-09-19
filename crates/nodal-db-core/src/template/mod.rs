//! The template loader (`specs/template_ir_plan.md`, phase 2 of HANDOFF
//! §8.4): parse the YAML-1.2/JSON template IR, validate it, load it into a
//! fresh (de-Based) database, reconcile it against a populated one, and
//! re-export it (`template export`, acceptance §8.1.7).
//!
//! The loader is a CLIENT of the one write path (CLAUDE.md invariant #2, plan
//! §1.3): it lives in this crate, writes structural rows through the same
//! from/to protocol as `api.rs`'s resolvers (declared type members via
//! `api::create_parameter_type` — the one insert path), and routes defaults —
//! which ARE values — through `history::write_definition_default` (rank-0
//! scalars are their own canonical form, VMP Part 3). There is no second
//! canonicalizer and no template-privileged writer.
//!
//! Module map:
//! - [`ir`] — the object model, parser (serde_norway, YAML 1.2 core schema —
//!   see its docs for the D1 crate rationale), pure load-time validation,
//!   normalization + `template_content_hash`.
//! - [`dbvocab`] — snapshot the DB's current vocabulary (the diff's DB side,
//!   the export's source).
//! - [`reconcile`] — plan (read-only dry run) / apply (migration chain +
//!   additive diff, one commit), the Layer-2 hint executor, the Layer-3
//!   `run:` program registry.
//! - [`export`] — DB → [`ir::Template`] (round-trip acceptance).

pub mod dbvocab;
pub mod export;
pub mod ir;
pub mod reconcile;

pub use export::export;
pub use ir::{
    parse_file, parse_str, template_content_hash, to_json, to_yaml, validate, Template,
};
pub use reconcile::{
    apply, plan, ApplyOptions, ApplyOutcome, MigrationRegistry, ReconcilePlan, RunProgramReport,
};
