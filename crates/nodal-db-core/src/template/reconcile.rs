//! Template reconcile: the three-layer migration model's v1 mechanics
//! (`specs/template_ir_plan.md` §4, D5) plus the fresh-init load, which is the
//! same operation against an empty vocabulary (§7 "idempotency &
//! re-application").
//!
//! - **Layer 1 — declarative reconcile.** [`plan`] diffs the DB's current
//!   vocabulary against the template and emits the minimal ADDITIVE write set
//!   ([`Action`]); [`apply`] executes it in one commit. Re-applying an
//!   unchanged template is a no-op (`template_content_hash` short-circuit,
//!   then an empty diff).
//! - **Layer 2 — identity hints.** The closed [`super::ir::MigrationStep`]
//!   vocabulary (renames, `rename_list_member` with its value rewrite,
//!   `move_parameter`) executes as ordinary from/to close-out versioning —
//!   one commit per step, so history shows each migration step.
//! - **Layer 3 — the `run:` escape hatch.** Named tool-side programs from the
//!   caller's [`MigrationRegistry`], executed in declared order **through the
//!   public write API** (each program commits via `api::commit_changes` etc.;
//!   the IR records identity/ordering only — invariant CLAUDE.md #2 by
//!   construction).
//!
//! ## Chain integrity (review items 1-4)
//! `apply` guards, in order, BEFORE any write: the staleness check
//! (`ApplyOptions::expect_commit`), the retarget guard (a DB stamped with a
//! DIFFERENT `template_name` refuses unless `allow_retarget` — ONE decision,
//! here, for every front end), the engine-schema note, the no-op
//! short-circuit, the downgrade guard (the stamped version is
//! forward-reachable FROM the template's own version through its chain), the
//! unreachable-chain guard (a stamped version the template's non-empty chain
//! cannot walk to the target refuses loudly; only a template with NO
//! `migrations:` at all bridges by plain reconcile, §4.2 L1), and the blocked
//! pre-check (the full simulated plan must carry no reshape blockers before
//! any span commits).
//!
//! **Crash resume.** Each declarative step's commit also writes the
//! `template_migration_step` progress substamp (same commit — atomic); the
//! span's version stamp clears it. An interrupted chain resumes by skipping
//! completed steps. A `run:` program commits through the public API on its
//! own, so its substamp lands AFTER it in a separate commit — a crash in
//! that window re-runs the program. **`run:` programs MUST therefore be
//! idempotent**; that is the Layer-3 contract (true cross-program atomicity
//! is impossible from outside the program's own commits, so the contract is
//! stated rather than pretended away).
//!
//! ## Partial-apply honesty (review item 14)
//! When anything the template declares could not be applied (a narrowed type
//! member kept because values bind it, a seed membership the user excluded, a
//! default withheld because a non-scalar default occupies the slot), the
//! apply does NOT certify convergence: `template_name`/`template_version`
//! stamp, but `template_content_hash` is withheld (and a stale one closed
//! out), so the no-op short-circuit can never certify unconverged state —
//! plan/apply keep replanning until the skips clear, then the hash lands.
//!
//! ## Layering (§7, doc note)
//! Layered composition is REDECLARATION-based: a second template composes by
//! declaring its own (disjoint) vocabulary in full; D6's element-closure
//! validation is per-document, so a layered template that seeds N-D entities
//! must declare their element entities itself. v1 keeps one primary template
//! per DB (last applied stamps identity).
//!
//! **Write-path discipline (CLAUDE.md invariant #2).** This module lives in
//! the one core and writes structural rows exactly the way `api.rs`'s
//! auto-create resolvers do (same tables, same from/to protocol); declared
//! type members go through `api::create_parameter_type` (the one insert
//! path); defaults and the rename-hint value rewrite are value writes and
//! land through `history` (retention-honoring close-outs, VMP Part 5). No
//! domain row is written outside this crate. Usage guards (the narrowing and
//! move checks) are plain COUNT aggregates — no `error()`-style side-effect
//! guards anywhere, so no projection-pushdown hazard class applies.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;

use duckdb::{params, Connection};
use serde_json::{json, Map, Value};

use crate::api::{
    create_parameter_type, lookup_class_set, lookup_entity, lookup_entity_class,
    lookup_parameter, lookup_unit, lookup_value_list, parse_index_kind,
};
use crate::conn::{with_read, with_write_txn_retrying};
use crate::error::DbError;
use crate::history::{
    close_definition_default, literal_cols, rewrite_definition_string_values,
    rewrite_scalar_string_values, write_definition_default,
};
use crate::object_delete::resolve_element as resolve_element_in_dim;
use crate::types::{fold, IndexKind, ScalarValue, ValueKind};

use super::dbvocab::{read_metadata_key, read_vocab, DbParam, DbVocab};
use super::ir::{
    self, effective_member_kind, Checked, Class, ListMember, Migration, MigrationStep, Parameter,
    Template, TypeDecl,
};

// ---------------------------------------------------------------------------
// The migration-program registry (Layer 3)
// ---------------------------------------------------------------------------

/// A named tool-side migration program (§4.2 Layer 3): runs with the DB path
/// and the commit author, and does ALL its writes through the public write API
/// (its own commits). The IR's `run:` step names it; the registry binds it.
/// **Contract: programs must be idempotent** (module docs, "Crash resume").
pub type MigrationProgram = Box<dyn Fn(&Path, &str) -> Result<(), DbError> + Send + Sync>;

/// The runner's program table. Tenants register their migration code here;
/// an IR `run:` step naming an unregistered program is a hard error (the
/// chain cannot silently skip an imperative step).
#[derive(Default)]
pub struct MigrationRegistry {
    programs: HashMap<String, MigrationProgram>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        name: &str,
        program: impl Fn(&Path, &str) -> Result<(), DbError> + Send + Sync + 'static,
    ) {
        self.programs.insert(name.to_string(), Box::new(program));
    }

    fn get(&self, name: &str) -> Option<&MigrationProgram> {
        self.programs.get(name)
    }
}

/// Caller-side switches for [`apply`] (review items 1, 2, 16). All default
/// off/None; the CLI deliberately does not expose the two `allow_*` overrides
/// yet.
#[derive(Clone, Copy, Debug, Default)]
pub struct ApplyOptions {
    /// Permit applying a template whose `template_name` differs from the
    /// name already stamped on the DB (identity keys are restamped).
    pub allow_retarget: bool,
    /// Permit applying a template whose version the DB's stamped version is
    /// AHEAD of (per the template's own chain) — a downgrade.
    pub allow_downgrade: bool,
    /// Refuse unless the DB is exactly at this commit id (the id `plan`
    /// printed) — the plan→apply staleness guard. Re-verified inside the
    /// final write transaction against the commits this apply itself made.
    pub expect_commit: Option<i64>,
}

// ---------------------------------------------------------------------------
// The plan model
// ---------------------------------------------------------------------------

/// A value-list member ready to write (kind + typed value resolved at plan
/// time). `ordinal` is the member's 1-based template position — ordinal IS
/// semantic (Decision 1 rider), so reconcile maintains template order.
#[derive(Clone, Debug)]
pub struct PlannedListMember {
    pub kind: String,
    pub value: ScalarValue,
    pub description: Option<String>,
    pub ordinal: i64,
}

/// One existing `list_value` row to revise (close-out + reinsert, same id) —
/// the row id is threaded from the diff's snapshot so the executor never
/// re-matches by value (review item 17: no float-equality SQL).
#[derive(Clone, Debug)]
pub struct MemberRevision {
    pub id: i64,
    pub ordinal: i64,
    pub description: Option<String>,
    /// Human rendering for the plan.
    pub display: String,
}

#[derive(Clone, Debug)]
pub struct PlannedDim {
    pub position: i64,
    pub referent: String,
    pub is_set: bool,
}

/// One planned (and then applied) reconcile write. Carries the full payload
/// needed to execute, so plan and apply cannot diverge by re-interpretation —
/// apply re-diffs inside its transaction and executes exactly these.
#[derive(Clone, Debug)]
pub enum Action {
    CreateUnit { name: String, description: Option<String>, quantity_kind: Option<String> },
    UpdateUnit { name: String, description: Option<String>, quantity_kind: Option<String> },
    CreateValueList { name: String, description: Option<String>, members: Vec<PlannedListMember> },
    UpdateValueList { name: String, description: Option<String> },
    /// Bring an existing list's members to the template's state: revisions
    /// (ordinal/description, two-phase close-then-reinsert so the
    /// `(list, ordinal)` unique index never sees a transient collision) and
    /// additions, in template order.
    SyncListMembers { list: String, revise: Vec<MemberRevision>, add: Vec<PlannedListMember> },
    CreateClass {
        name: String,
        description: Option<String>,
        active_by_default: bool,
        required: bool,
        display_order: Option<i64>,
    },
    UpdateClass {
        name: String,
        description: Option<String>,
        active_by_default: bool,
        required: bool,
        display_order: Option<i64>,
    },
    /// Dimension rows of a NEW class, emitted after every `CreateClass` so
    /// forward references between new classes resolve (§2.2 by-name).
    CreateClassDims { class: String, dims: Vec<PlannedDim> },
    CreateClassSet { name: String, description: Option<String> },
    UpdateClassSet { name: String, description: Option<String> },
    AddClassSetMember { set: String, member: String },
    CreateParameter {
        class: String,
        name: String,
        description: Option<String>,
        value_list: Option<String>,
        unit: Option<String>,
        required: bool,
        display_order: Option<i64>,
        types: Vec<TypeDecl>,
    },
    UpdateParameter {
        class: String,
        name: String,
        description: Option<String>,
        value_list: Option<String>,
        unit: Option<String>,
        required: bool,
        display_order: Option<i64>,
    },
    AddParameterType { class: String, parameter: String, member: TypeDecl },
    /// Close out a declared member the template no longer carries (§4.2:
    /// "narrowing is flagged when current data instantiates a removed
    /// member"). Apply skips-with-warning when any current value binds it —
    /// a skip withholds convergence (module docs).
    NarrowParameterType { class: String, parameter: String, pt_id: i64, describe: String },
    SetDefault { class: String, parameter: String, kind: String, value: ScalarValue },
    ClearDefault { class: String, parameter: String },
    CreateSeedEntity {
        class: String,
        name: String,
        description: Option<String>,
        elements: Vec<String>,
    },
    /// Insert a base membership row — unless one already exists: an
    /// `included = TRUE` row is a benign no-op, an `included = FALSE` row is
    /// a USER EXCLUSION the loader respects (skip + convergence withheld;
    /// review item 10 — the loader never overrides user data).
    CreateSeedMembership {
        group_class: String,
        group: String,
        member_class: String,
        member: String,
    },
    /// Replace the template-owned keys of the Decision-7 `presentation`
    /// document (D8); `doc` is the full merged document to store.
    SetPresentation { doc: String },
    SetAuditRules { doc: Option<String> },
    /// Stamp `template_name`/`template_version`; stamp `template_content_hash`
    /// ONLY when the apply converged (no skips), else close any stale hash
    /// (module docs, "Partial-apply honesty").
    StampIdentity { name: String, version: String, content_hash: String },
}

fn scalar_display(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Float(f) => format!("{f}"),
        ScalarValue::Int(i) => format!("{i}"),
        ScalarValue::Bool(b) => format!("{b}"),
        ScalarValue::Str(s) => format!("{s:?}"),
        ScalarValue::DateTime(t) => format!("datetime({t})"),
        ScalarValue::Duration { months, micros } => format!("duration({months}mo,{micros}us)"),
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::CreateUnit { name, .. } => write!(f, "create unit {name}"),
            Action::UpdateUnit { name, .. } => write!(f, "update unit {name}"),
            Action::CreateValueList { name, members, .. } => {
                write!(f, "create value_list {name} ({} members)", members.len())
            }
            Action::UpdateValueList { name, .. } => write!(f, "update value_list {name}"),
            Action::SyncListMembers { list, revise, add } => {
                write!(f, "sync value_list {list} members:")?;
                for a in add {
                    write!(f, " +{}@{}", scalar_display(&a.value), a.ordinal)?;
                }
                for r in revise {
                    write!(f, " ~{}@{}", r.display, r.ordinal)?;
                }
                Ok(())
            }
            Action::CreateClass { name, .. } => write!(f, "create class {name}"),
            Action::UpdateClass { name, .. } => write!(f, "update class {name}"),
            Action::CreateClassDims { class, dims } => {
                let d: Vec<String> = dims
                    .iter()
                    .map(|d| {
                        if d.is_set {
                            format!("{{class_set: {}}}", d.referent)
                        } else {
                            d.referent.clone()
                        }
                    })
                    .collect();
                write!(f, "declare dimensions of {class}: [{}]", d.join(", "))
            }
            Action::CreateClassSet { name, .. } => write!(f, "create class_set {name}"),
            Action::UpdateClassSet { name, .. } => write!(f, "update class_set {name}"),
            Action::AddClassSetMember { set, member } => {
                write!(f, "add class_set member {set}: {member}")
            }
            Action::CreateParameter { class, name, types, .. } => {
                write!(f, "create parameter {class}.{name} ({} type member(s))", types.len())
            }
            Action::UpdateParameter { class, name, .. } => {
                write!(f, "update parameter {class}.{name}")
            }
            Action::AddParameterType { class, parameter, member } => {
                write!(f, "widen type set of {class}.{parameter}: + {}", member.describe())
            }
            Action::NarrowParameterType { class, parameter, describe, .. } => {
                write!(f, "narrow type set of {class}.{parameter}: - {describe}")
            }
            Action::SetDefault { class, parameter, kind, value } => {
                write!(f, "set default of {class}.{parameter}: {kind} {}", scalar_display(value))
            }
            Action::ClearDefault { class, parameter } => {
                write!(f, "clear default of {class}.{parameter}")
            }
            Action::CreateSeedEntity { class, name, elements, .. } => {
                if elements.is_empty() {
                    write!(f, "seed entity {class}: {name}")
                } else {
                    write!(f, "seed entity {class}: {name} = ({})", elements.join(", "))
                }
            }
            Action::CreateSeedMembership { group_class, group, member_class, member } => {
                write!(f, "seed membership {group_class}:{group} <- {member_class}:{member}")
            }
            Action::SetPresentation { .. } => {
                write!(f, "store parameter groups (presentation document, D8)")
            }
            Action::SetAuditRules { doc } => match doc {
                Some(_) => write!(f, "store audit_rules document (D4)"),
                None => write!(f, "clear audit_rules document"),
            },
            Action::StampIdentity { name, version, content_hash } => write!(
                f,
                "stamp identity: template_name={name} template_version={version} \
                 template_content_hash={content_hash} (hash withheld if any skips)"
            ),
        }
    }
}

/// One pending migration span, rendered for the plan.
#[derive(Clone, Debug)]
pub struct PlannedSpan {
    pub from: String,
    pub to: String,
    pub steps: Vec<String>,
}

/// The dry-run output (§4.2 plan-then-apply): pending migration spans, the
/// diff's action list, blockers (apply refuses while any exist), skips
/// (declared state the apply will NOT converge on — convergence withheld),
/// warnings, and notes.
#[derive(Clone, Debug, Default)]
pub struct ReconcilePlan {
    pub template_name: String,
    pub template_version: String,
    pub content_hash: String,
    pub db_template_name: Option<String>,
    pub db_template_version: Option<String>,
    /// `max(commit.id)` at plan time — quote it to `ApplyOptions::expect_commit`
    /// to make apply refuse if anything moved in between (review item 16).
    pub db_commit: i64,
    pub pending_migrations: Vec<PlannedSpan>,
    pub actions: Vec<Action>,
    pub blocked: Vec<String>,
    /// Declared template state that will be skipped, not applied (narrow
    /// kept, membership excluded, non-scalar default in the way). Non-empty
    /// skips withhold `template_content_hash` (module docs).
    pub skipped: Vec<String>,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
    pub no_op: bool,
}

impl fmt::Display for ReconcilePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "template {} version {} (content hash {})",
            self.template_name, self.template_version, self.content_hash
        )?;
        match (&self.db_template_name, &self.db_template_version) {
            (Some(n), Some(v)) => {
                writeln!(f, "database: template {n} version {v} at commit {}", self.db_commit)?
            }
            (Some(n), None) => writeln!(
                f,
                "database: template {n} (no version stamped) at commit {}",
                self.db_commit
            )?,
            _ => writeln!(
                f,
                "database: no template stamped (fresh load) at commit {}",
                self.db_commit
            )?,
        }
        if self.no_op {
            writeln!(f, "NO-OP: the database already carries this template version")?;
            return Ok(());
        }
        for span in &self.pending_migrations {
            writeln!(f, "migration {} -> {}:", span.from, span.to)?;
            for s in &span.steps {
                writeln!(f, "  * {s}")?;
            }
        }
        if !self.actions.is_empty() {
            writeln!(f, "reconcile actions ({}):", self.actions.len())?;
            for a in &self.actions {
                writeln!(f, "  + {a}")?;
            }
        }
        for b in &self.blocked {
            writeln!(f, "  BLOCKED: {b}")?;
        }
        for s in &self.skipped {
            writeln!(f, "  skip (convergence withheld): {s}")?;
        }
        for w in &self.warnings {
            writeln!(f, "  warning: {w}")?;
        }
        for n in &self.notes {
            writeln!(f, "  note: {n}")?;
        }
        Ok(())
    }
}

/// One `run:` program execution, with the commit ids it produced through the
/// public API (derived from the commit-id watermark around the call).
#[derive(Clone, Debug)]
pub struct RunProgramReport {
    pub program: String,
    pub commits: Vec<i64>,
}

/// The outcome of [`apply`].
#[derive(Debug, Default)]
pub struct ApplyOutcome {
    /// True when nothing needed to change (content-hash short-circuit or an
    /// empty diff): no commit was made.
    pub no_op: bool,
    /// Commit ids THIS apply made directly: one per executed migration step /
    /// substamp / version stamp, plus the final reconcile commit.
    pub commits: Vec<i64>,
    /// `run:` program executions with their public-API commit ids.
    pub run_programs: Vec<RunProgramReport>,
    /// The plan as APPLIED (recomputed inside the final transaction).
    pub plan: ReconcilePlan,
    /// False when any skip occurred: `template_content_hash` was withheld
    /// (module docs, "Partial-apply honesty").
    pub converged: bool,
    pub warnings: Vec<String>,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

fn max_commit(conn: &Connection) -> Result<i64, DbError> {
    Ok(conn.query_row("SELECT max(id) FROM commit", [], |r| r.get(0))?)
}

/// Read-only dry run: validate, snapshot the DB vocabulary, simulate any
/// pending migration chain on the snapshot (renames applied in memory; `run:`
/// programs noted, not simulated), and diff. Guard conditions apply would
/// refuse on are surfaced as blockers. One per-operation RO connection
/// (CLAUDE.md invariant 1). Best-effort snapshot — [`apply`] recomputes
/// everything; quote `db_commit` to `expect_commit` to pin it.
pub fn plan(db: &Path, t: &Template) -> Result<ReconcilePlan, DbError> {
    let chk = ir::validate(t)?;
    with_read(db, |conn| {
        let mut warnings = chk.warnings.clone();
        let mut vocab = read_vocab(conn, &mut warnings)?;
        let db_commit = max_commit(conn)?;
        // Surface apply's own refusals read-only (retarget + chain guards) so
        // the dry run predicts them.
        let mut guard_blocks: Vec<String> = Vec::new();
        if let Some(dbn) = &vocab.template_name {
            if fold(dbn) != fold(&t.template.name) {
                guard_blocks.push(format!(
                    "apply will refuse: database is stamped with template {dbn:?} — \
                     applying {:?} is an identity retarget \
                     (ApplyOptions::allow_retarget)",
                    t.template.name
                ));
            }
        }
        let guard = chain_guards(&vocab, t, &ApplyOptions::default());
        let spans = pending_spans(&vocab, t);
        let mut planned_spans = Vec::new();
        for m in &spans {
            planned_spans.push(PlannedSpan {
                from: m.from.clone(),
                to: m.to.clone(),
                steps: m.steps.iter().map(|s| s.describe()).collect(),
            });
            simulate_span(&mut vocab, m, &mut warnings);
        }
        let mut p = diff(&vocab, &chk)?;
        p.db_commit = db_commit;
        p.pending_migrations = planned_spans;
        if let Err(e) = guard {
            guard_blocks.push(format!("apply will refuse: {e}"));
        }
        p.blocked.splice(0..0, guard_blocks);
        p.warnings.splice(0..0, warnings);
        Ok(p)
    })
}

/// Apply the template under the full guard sequence (module docs): the
/// pending migration chain (one commit per declarative step with its progress
/// substamp; `run:` programs through the registry and the public API; a
/// version stamp per span), then the final reconcile diff in ONE commit that
/// also stamps identity (§7).
pub fn apply(
    db: &Path,
    t: &Template,
    registry: &MigrationRegistry,
    user: &str,
    message: &str,
    opts: &ApplyOptions,
) -> Result<ApplyOutcome, DbError> {
    let chk = ir::validate(t)?;
    if user.trim().is_empty() {
        return Err(DbError::Api("user must be non-empty".into()));
    }
    let mut out = ApplyOutcome { converged: true, ..Default::default() };
    out.warnings.extend(chk.warnings.iter().cloned());

    // --- Pre-flight: metadata keys only (cheap; review item 21). ---
    struct Pre {
        name: Option<String>,
        version: Option<String>,
        hash: Option<String>,
        schema_version: Option<String>,
        substamp: Option<String>,
        commit: i64,
    }
    let pre = with_read(db, |conn| {
        Ok(Pre {
            name: read_metadata_key(conn, "template_name")?,
            version: read_metadata_key(conn, "template_version")?,
            hash: read_metadata_key(conn, "template_content_hash")?,
            schema_version: read_metadata_key(conn, "schema_version")?,
            substamp: read_metadata_key(conn, "template_migration_step")?,
            commit: max_commit(conn)?,
        })
    })?;

    // Staleness (review item 16): the caller pinned the plan's commit.
    if let Some(exp) = opts.expect_commit {
        if pre.commit != exp {
            return Err(DbError::Api(format!(
                "stale plan: the database is at commit {} but the plan was made at commit \
                 {exp} — re-plan and re-apply",
                pre.commit
            )));
        }
    }

    // Retarget guard (review item 1): ONE decision, in core, for every front
    // end. `allow_retarget` is the explicit opt-in (not CLI-exposed yet).
    if let Some(dbn) = &pre.name {
        if fold(dbn) != fold(&t.template.name) {
            if !opts.allow_retarget {
                return Err(DbError::Api(format!(
                    "database is stamped with template {dbn:?}; refusing to apply \
                     {:?} (identity retarget) — this is an explicit opt-in \
                     (ApplyOptions::allow_retarget), not a default",
                    t.template.name
                )));
            }
            out.warnings.push(format!(
                "retargeting: identity keys restamp from template {dbn:?} to {:?} \
                 (v1 layering is only sound for disjoint namespaces)",
                t.template.name
            ));
        }
    }

    // Engine-schema note (review item 12): BEFORE the short-circuit, so it is
    // never silenced by a no-op.
    if let (Some(engine), Some(dbv)) = (&t.template.engine_schema, &pre.schema_version) {
        if engine != dbv {
            out.warnings.push(format!(
                "template declares engine_schema {engine:?} but the database's \
                 schema_version is {dbv:?} (compatibility note only, not a block)"
            ));
        }
    }

    // Fast idempotency short-circuit (§7): same template, same version, same
    // certified content — nothing to do, no commit. An absent hash (a prior
    // PARTIAL apply) never matches, so unconverged state keeps replanning.
    if pre.name.as_deref() == Some(t.template.name.as_str())
        && pre.version.as_deref() == Some(t.template.version.as_str())
        && pre.hash.as_deref() == Some(chk.hash.as_str())
    {
        out.no_op = true;
        out.plan = ReconcilePlan {
            template_name: t.template.name.clone(),
            template_version: t.template.version.clone(),
            content_hash: chk.hash.clone(),
            db_template_name: pre.name,
            db_template_version: pre.version,
            db_commit: pre.commit,
            no_op: true,
            ..Default::default()
        };
        return Ok(out);
    }

    // --- Chain guards + blocked pre-check (review items 2, 3), one snapshot. ---
    let spans = with_read(db, |conn| {
        let mut w = Vec::new();
        let mut vocab = read_vocab(conn, &mut w)?;
        chain_guards(&vocab, t, opts)?;
        let spans = pending_spans(&vocab, t);
        for m in &spans {
            simulate_span(&mut vocab, m, &mut w);
        }
        let sim = diff(&vocab, &chk)?;
        if !sim.blocked.is_empty() {
            return Err(blocked_error(&sim));
        }
        Ok(spans)
    })?;

    // Every named program across the WHOLE chain must be registered before
    // any step runs (never leave a chain half-applied over a typo).
    for span in &spans {
        for step in &span.steps {
            if let MigrationStep::Run(p) = step {
                if registry.get(p).is_none() {
                    return Err(DbError::Api(format!(
                        "migration {} -> {}: run program {p:?} is not registered \
                         (register it in the MigrationRegistry)",
                        span.from, span.to
                    )));
                }
            }
        }
    }

    // Crash-resume position from the progress substamp (module docs).
    let resume = parse_substamp(pre.substamp.as_deref());
    // The commit-id cursor: what max(commit.id) should be after OUR latest
    // write — advanced by every commit we (or a run: program) make, verified
    // inside the final transaction when expect_commit pinned the base.
    let mut cursor = pre.commit;

    // --- Layer 2/3: the pending migration chain, in span/step order. ---
    for span in &spans {
        let done_through: i64 = match &resume {
            Some((f, to, idx)) if *f == span.from && *to == span.to => *idx,
            _ => -1,
        };
        for (i, step) in span.steps.iter().enumerate() {
            if (i as i64) <= done_through {
                continue; // completed before an interruption (substamp)
            }
            let substamp = format!("{}->{}:{i}", span.from, span.to);
            match step {
                MigrationStep::Run(p) => {
                    // Layer 3: tool-side program through the public write API.
                    let program = registry.get(p).unwrap();
                    program(db, user)?;
                    let after = with_read(db, max_commit)?;
                    out.run_programs.push(RunProgramReport {
                        program: p.clone(),
                        commits: ((cursor + 1)..=after).collect(),
                    });
                    // Progress substamp in its own commit (the program owns
                    // its commits) — hence the run:-idempotency contract.
                    let msg = format!(
                        "template {} migration {} -> {}: progress after run {p}",
                        t.template.name, span.from, span.to
                    );
                    let c = with_write_txn_retrying(db, user, &msg, |conn, c| {
                        set_metadata(conn, c, "template_migration_step", &substamp)?;
                        Ok(c)
                    })?;
                    out.commits.push(c);
                    cursor = c;
                }
                declarative => {
                    let desc = declarative.describe();
                    let msg = format!(
                        "template {} migration {} -> {}: {desc}",
                        t.template.name, span.from, span.to
                    );
                    let c = with_write_txn_retrying(db, user, &msg, |conn, c| {
                        execute_step(conn, c, declarative)?;
                        // Same-commit progress substamp: atomic with the step.
                        set_metadata(conn, c, "template_migration_step", &substamp)?;
                        Ok(c)
                    })?;
                    out.commits.push(c);
                    cursor = c;
                }
            }
        }
        // Span stamp: bump the version AND clear the substamp, atomically —
        // an interrupted chain resumes at the right span (§4.4).
        let msg = format!(
            "template {} migration {} -> {}: version stamp",
            t.template.name, span.from, span.to
        );
        let c = with_write_txn_retrying(db, user, &msg, |conn, c| {
            set_metadata(conn, c, "template_version", &span.to)?;
            clear_metadata(conn, c, "template_migration_step")?;
            Ok(c)
        })?;
        out.commits.push(c);
        cursor = c;
    }

    // --- Layer 1: the final reconcile, one commit, identity stamped. ---
    // Pre-check RO so an empty diff never burns a commit (§7 idempotency);
    // the write txn re-diffs, so a racing writer only costs one replan,
    // never a wrong apply.
    let pre_plan = with_read(db, |conn| {
        let mut w = Vec::new();
        let vocab = read_vocab(conn, &mut w)?;
        diff(&vocab, &chk)
    })?;
    if !pre_plan.blocked.is_empty() {
        return Err(blocked_error(&pre_plan));
    }
    if pre_plan.actions.is_empty() {
        out.no_op = out.commits.is_empty();
        out.converged = pre_plan.skipped.is_empty();
        out.plan = pre_plan;
        return Ok(out);
    }

    let msg = if message.is_empty() {
        format!("template {} version {}: apply", t.template.name, t.template.version)
    } else {
        message.to_string()
    };
    let expect_inside = opts.expect_commit.map(|_| cursor);
    let applied = with_write_txn_retrying(db, user, &msg, |conn, c| {
        // Staleness re-check INSIDE the transaction (review item 16): the
        // world outside our own commits must not have moved.
        if let Some(expected) = expect_inside {
            let outside: i64 = conn.query_row(
                "SELECT max(id) FROM commit WHERE id <> ?",
                params![c],
                |r| r.get(0),
            )?;
            if outside != expected {
                return Err(DbError::Api(format!(
                    "stale plan: a concurrent commit ({outside}) appeared after the \
                     expected commit {expected} — re-plan and re-apply"
                )));
            }
        }
        let mut w = Vec::new();
        let vocab = read_vocab(conn, &mut w)?;
        let mut p = diff(&vocab, &chk)?;
        if !p.blocked.is_empty() {
            return Err(blocked_error(&p));
        }
        let mut ctx = ExecCtx {
            skipped: p.skipped.clone(),
            warnings: Vec::new(),
            ids: IdCache::default(),
        };
        for a in &p.actions {
            execute_action(conn, c, a, &mut ctx)?;
        }
        p.skipped = ctx.skipped;
        p.warnings.extend(ctx.warnings);
        p.warnings.splice(0..0, w);
        Ok((c, p))
    })?;
    out.commits.push(applied.0);
    out.plan = applied.1;
    out.converged = out.plan.skipped.is_empty();
    if !out.converged {
        out.warnings.push(format!(
            "template_content_hash withheld: {} declared item(s) skipped — the apply \
             is PARTIAL and will replan until it converges",
            out.plan.skipped.len()
        ));
    }
    out.warnings.extend(out.plan.warnings.iter().cloned());
    Ok(out)
}

fn blocked_error(p: &ReconcilePlan) -> DbError {
    DbError::Api(format!(
        "template apply refused: {} blocking conflict(s) (the in-place reshape door is \
         deliberately deferred — see template_ir_plan §4.2):\n  - {}",
        p.blocked.len(),
        p.blocked.join("\n  - ")
    ))
}

/// Parse the `template_migration_step` progress substamp: `from->to:index`.
fn parse_substamp(v: Option<&str>) -> Option<(String, String, i64)> {
    let v = v?;
    let (span, idx) = v.rsplit_once(':')?;
    let (from, to) = span.split_once("->")?;
    Some((from.to_string(), to.to_string(), idx.parse().ok()?))
}

// ---------------------------------------------------------------------------
// Chain guards + selection + in-memory simulation
// ---------------------------------------------------------------------------

/// The chain-integrity guards (review item 2), evaluated on the snapshot:
/// downgrade (the stamped version is forward-reachable FROM the template's
/// version through its own chain) and unreachable (a non-empty chain that
/// cannot walk from the stamped version to the target). A template with NO
/// `migrations:` block bridges any same-name version gap by plain reconcile —
/// the §4.2 Layer-1 case, made explicit here.
fn chain_guards(vocab: &DbVocab, t: &Template, opts: &ApplyOptions) -> Result<(), DbError> {
    let Some(dbv) = &vocab.template_version else { return Ok(()) };
    // Chains are same-name lineage only.
    match &vocab.template_name {
        Some(n) if fold(n) == fold(&t.template.name) => {}
        _ => return Ok(()),
    }
    if dbv == &t.template.version {
        return Ok(());
    }
    // Downgrade: walk FORWARD from the template's version; reaching the
    // stamped version means the DB is ahead of this template.
    if !opts.allow_downgrade {
        let mut cur = t.template.version.clone();
        let mut seen = HashSet::new();
        while seen.insert(cur.clone()) {
            match t.migrations.iter().find(|m| m.from == cur) {
                Some(m) => {
                    if &m.to == dbv {
                        return Err(DbError::Api(format!(
                            "database is at template version {dbv:?}, AHEAD of this \
                             template's version {:?} (per its own migration chain) — \
                             refusing the downgrade (ApplyOptions::allow_downgrade to \
                             override)",
                            t.template.version
                        )));
                    }
                    cur = m.to.clone();
                }
                None => break,
            }
        }
    }
    if t.migrations.is_empty() {
        return Ok(()); // §4.2 L1: plain reconcile bridges; no chain declared.
    }
    // Unreachable: the declared chain must walk from the stamped version to
    // the template's version, else refuse loudly (never silently reconcile
    // across a gap the template itself says needs migrations).
    let mut cur = dbv.clone();
    let mut seen = HashSet::new();
    while cur != t.template.version {
        if !seen.insert(cur.clone()) {
            return Err(DbError::Api(format!(
                "template migration chain loops at version {cur:?} — the chain must be \
                 linear"
            )));
        }
        match t.migrations.iter().find(|m| m.from == cur) {
            Some(m) => cur = m.to.clone(),
            None => {
                return Err(DbError::Api(format!(
                    "database is at template version {dbv:?} but the template's \
                     migration chain has no path from there to {:?} (nearest dead end \
                     at {cur:?}) — this template cannot upgrade that database",
                    t.template.version
                )))
            }
        }
    }
    Ok(())
}

/// The spans to run: follow the chain from the DB's stamped version toward the
/// template's version. No stamped version (fresh DB), a different stamped
/// template name, or already-at-target ⇒ no spans. Gap/cycle handling lives in
/// [`chain_guards`] (which runs first); this walk mirrors it.
fn pending_spans(vocab: &DbVocab, t: &Template) -> Vec<Migration> {
    match &vocab.template_name {
        Some(n) if fold(n) == fold(&t.template.name) => {}
        _ => return Vec::new(),
    }
    let Some(mut cur) = vocab.template_version.clone() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    while cur != t.template.version {
        if !seen.insert(cur.clone()) {
            break;
        }
        match t.migrations.iter().find(|m| m.from == cur) {
            Some(m) => {
                cur = m.to.clone();
                out.push(m.clone());
            }
            None => break,
        }
    }
    out
}

/// Apply one span's renames/moves to the in-memory snapshot so the plan's diff
/// reflects post-migration names. `run:` steps are opaque (noted).
fn simulate_span(vocab: &mut DbVocab, m: &Migration, warnings: &mut Vec<String>) {
    for step in &m.steps {
        match step {
            MigrationStep::RenameClass { from, to } => {
                if let Some(mut c) = vocab.classes.remove(&fold(from)) {
                    c.name = to.clone();
                    vocab.classes.insert(fold(to), c);
                    // Propagate into every by-name cross-reference of the
                    // snapshot: dimension referents and class-set members
                    // (the physical rename keeps the id, so the DB needs no
                    // such touch-up — only this in-memory name view does).
                    for c in vocab.classes.values_mut() {
                        for d in &mut c.dims {
                            if let super::dbvocab::DbDimRef::Class(n) = d {
                                if fold(n) == fold(from) {
                                    *n = to.clone();
                                }
                            }
                        }
                    }
                    for s in vocab.class_sets.values_mut() {
                        for m in &mut s.members {
                            if fold(m) == fold(from) {
                                *m = to.clone();
                            }
                        }
                    }
                }
            }
            MigrationStep::RenameParameter { class, from, to } => {
                if let Some(c) = vocab.classes.get_mut(&fold(class)) {
                    if let Some(mut p) = c.params.remove(&fold(from)) {
                        p.name = to.clone();
                        c.params.insert(fold(to), p);
                    }
                }
            }
            MigrationStep::RenameValueList { from, to } => {
                if let Some(mut l) = vocab.value_lists.remove(&fold(from)) {
                    l.name = to.clone();
                    let new_name = l.name.clone();
                    vocab.value_lists.insert(fold(to), l);
                    for c in vocab.classes.values_mut() {
                        for p in c.params.values_mut() {
                            if p.value_list.as_deref().map(fold) == Some(fold(from)) {
                                p.value_list = Some(new_name.clone());
                            }
                        }
                    }
                }
            }
            MigrationStep::RenameListMember { list, from, to, .. } => {
                if let Some(l) = vocab.value_lists.get_mut(&fold(list)) {
                    for mem in &mut l.members {
                        if mem.value_kind == "string" {
                            if let ScalarValue::Str(s) = &mem.value {
                                if s == from {
                                    mem.value = ScalarValue::Str(to.clone());
                                }
                            }
                        }
                    }
                }
            }
            MigrationStep::MoveParameter { name, from_class, to_class } => {
                let taken = vocab
                    .classes
                    .get_mut(&fold(from_class))
                    .and_then(|c| c.params.remove(&fold(name)));
                if let (Some(p), Some(tc)) = (taken, vocab.classes.get_mut(&fold(to_class))) {
                    tc.params.insert(fold(name), p);
                }
            }
            MigrationStep::Run(p) => warnings.push(format!(
                "migration {} -> {}: run {p:?} is imperative — its effects are not \
                 simulated; the planned diff below may shift after it executes",
                m.from, m.to
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// The diff (Layer 1)
// ---------------------------------------------------------------------------

/// Compute the additive reconcile plan between a vocabulary snapshot and a
/// validated template. Pure — no DB access.
fn diff(vocab: &DbVocab, chk: &Checked<'_>) -> Result<ReconcilePlan, DbError> {
    let t = chk.template;
    let mut p = ReconcilePlan {
        template_name: t.template.name.clone(),
        template_version: t.template.version.clone(),
        content_hash: chk.hash.clone(),
        db_template_name: vocab.template_name.clone(),
        db_template_version: vocab.template_version.clone(),
        ..Default::default()
    };

    if let (Some(engine), Some(dbv)) = (&t.template.engine_schema, &vocab.schema_version) {
        if engine != dbv {
            p.warnings.push(format!(
                "template declares engine_schema {engine:?} but the database's \
                 schema_version is {dbv:?} (compatibility note only, not a block)"
            ));
        }
    }

    // --- units ---
    for u in &t.units {
        match vocab.units.get(&fold(&u.name)) {
            None => p.actions.push(Action::CreateUnit {
                name: u.name.clone(),
                description: u.description.clone(),
                quantity_kind: u.quantity_kind.clone(),
            }),
            Some(db) => {
                if db.description != u.description || db.quantity_kind != u.quantity_kind {
                    p.actions.push(Action::UpdateUnit {
                        name: u.name.clone(),
                        description: u.description.clone(),
                        quantity_kind: u.quantity_kind.clone(),
                    });
                }
            }
        }
    }

    // --- value lists (ordinal IS semantic: template order is maintained,
    //     extras re-numbered after the template members — review item 13) ---
    for l in &t.value_lists {
        match vocab.value_lists.get(&fold(&l.name)) {
            None => p.actions.push(Action::CreateValueList {
                name: l.name.clone(),
                description: l.description.clone(),
                members: l
                    .members
                    .iter()
                    .enumerate()
                    .map(|(i, m)| planned_member(m, i as i64 + 1))
                    .collect(),
            }),
            Some(db) => {
                if db.description != l.description {
                    p.actions.push(Action::UpdateValueList {
                        name: l.name.clone(),
                        description: l.description.clone(),
                    });
                }
                let mut db_index: HashMap<(String, String), &super::dbvocab::DbListMember> =
                    HashMap::new();
                for m in &db.members {
                    db_index.insert((m.value_kind.clone(), m.value.identity_string()), m);
                }
                let mut revise: Vec<MemberRevision> = Vec::new();
                let mut add: Vec<PlannedListMember> = Vec::new();
                let mut matched: HashSet<(String, String)> = HashSet::new();
                for (i, m) in l.members.iter().enumerate() {
                    let desired_ordinal = i as i64 + 1;
                    let kind = effective_member_kind(m)
                        .map_err(|e| DbError::Api(format!("value_list {:?}: {e}", l.name)))?;
                    let sv = m.value.resolve(&kind).map_err(DbError::Api)?;
                    let key = (kind.clone(), sv.identity_string());
                    matched.insert(key.clone());
                    match db_index.get(&key) {
                        None => add.push(PlannedListMember {
                            kind,
                            value: sv,
                            description: m.description.clone(),
                            ordinal: desired_ordinal,
                        }),
                        Some(dbm) => {
                            if dbm.ordinal != desired_ordinal
                                || dbm.description != m.description
                            {
                                revise.push(MemberRevision {
                                    id: dbm.id,
                                    ordinal: desired_ordinal,
                                    description: m.description.clone(),
                                    display: scalar_display(&sv),
                                });
                            }
                        }
                    }
                }
                // Extras (DB members not in the template): kept, re-homed to
                // tail ordinals AFTER the template members in their current
                // relative order.
                let mut next_tail = l.members.len() as i64;
                for m in &db.members {
                    let key = (m.value_kind.clone(), m.value.identity_string());
                    if !matched.contains(&key) {
                        next_tail += 1;
                        p.warnings.push(format!(
                            "value_list {:?}: member {} is not in the template — left in \
                             place (member removal is not additive; a rename is a \
                             rename_list_member hint)",
                            l.name,
                            scalar_display(&m.value)
                        ));
                        if m.ordinal != next_tail {
                            revise.push(MemberRevision {
                                id: m.id,
                                ordinal: next_tail,
                                description: m.description.clone(),
                                display: scalar_display(&m.value),
                            });
                        }
                    }
                }
                if !revise.is_empty() || !add.is_empty() {
                    p.actions.push(Action::SyncListMembers {
                        list: l.name.clone(),
                        revise,
                        add,
                    });
                }
            }
        }
    }

    // --- classes (header + field updates; dims for new classes; blockers for
    //     reshape) ---
    for c in &t.classes {
        match vocab.classes.get(&fold(&c.name)) {
            None => {
                p.actions.push(Action::CreateClass {
                    name: c.name.clone(),
                    description: c.description.clone(),
                    active_by_default: c.active_by_default,
                    required: c.required,
                    display_order: c.display_order,
                });
                if !c.dimensions.is_empty() {
                    p.actions.push(Action::CreateClassDims {
                        class: c.name.clone(),
                        dims: c
                            .dimensions
                            .iter()
                            .enumerate()
                            .map(|(i, d)| PlannedDim {
                                position: i as i64 + 1,
                                referent: d.referent_name().to_string(),
                                is_set: d.is_set(),
                            })
                            .collect(),
                    });
                }
            }
            Some(db) => {
                let t_dims: Vec<(bool, String)> = c
                    .dimensions
                    .iter()
                    .map(|d| (d.is_set(), fold(d.referent_name())))
                    .collect();
                let d_dims: Vec<(bool, String)> = db
                    .dims
                    .iter()
                    .map(|d| (d.is_set(), fold(d.referent_name())))
                    .collect();
                if t_dims != d_dims {
                    p.blocked.push(format!(
                        "class {:?}: declared dimensions differ from the database's \
                         (template: {:?}, db: {:?}) — re-dimensioning a live class is \
                         the deferred in-place reshape door",
                        c.name, t_dims, d_dims
                    ));
                    continue;
                }
                if db.description != c.description
                    || db.active_by_default != c.active_by_default
                    || db.required != c.required
                    || db.display_order != c.display_order
                {
                    p.actions.push(Action::UpdateClass {
                        name: c.name.clone(),
                        description: c.description.clone(),
                        active_by_default: c.active_by_default,
                        required: c.required,
                        display_order: c.display_order,
                    });
                }
            }
        }
    }

    // --- class sets ---
    for s in &t.class_sets {
        match vocab.class_sets.get(&fold(&s.name)) {
            None => {
                p.actions.push(Action::CreateClassSet {
                    name: s.name.clone(),
                    description: s.description.clone(),
                });
                for m in &s.members {
                    p.actions.push(Action::AddClassSetMember {
                        set: s.name.clone(),
                        member: m.clone(),
                    });
                }
            }
            Some(db) => {
                if db.description != s.description {
                    p.actions.push(Action::UpdateClassSet {
                        name: s.name.clone(),
                        description: s.description.clone(),
                    });
                }
                let existing: HashSet<String> = db.members.iter().map(|m| fold(m)).collect();
                for m in &s.members {
                    if !existing.contains(&fold(m)) {
                        p.actions.push(Action::AddClassSetMember {
                            set: s.name.clone(),
                            member: m.clone(),
                        });
                    }
                }
                let wanted: HashSet<String> = s.members.iter().map(|m| fold(m)).collect();
                for m in &db.members {
                    if !wanted.contains(&fold(m)) {
                        p.warnings.push(format!(
                            "class_set {:?}: member {m:?} is not in the template — left in \
                             place (member removal narrows entity acceptance; not additive)",
                            s.name
                        ));
                    }
                }
            }
        }
    }

    // --- parameters ---
    for (ci, c) in t.classes.iter().enumerate() {
        let db_class = vocab.classes.get(&fold(&c.name));
        for (pi, param) in c.parameters.iter().enumerate() {
            let eff = &chk.effective_types[&(ci, pi)];
            let db_param = db_class.and_then(|dc| dc.params.get(&fold(&param.name)));
            match db_param {
                None => {
                    p.actions.push(Action::CreateParameter {
                        class: c.name.clone(),
                        name: param.name.clone(),
                        description: param.description.clone(),
                        value_list: param.value_list.clone(),
                        unit: param.unit.clone(),
                        required: param.required,
                        display_order: param.display_order,
                        types: eff.clone(),
                    });
                    if let Some(dv) = &param.default {
                        p.actions.push(Action::SetDefault {
                            class: c.name.clone(),
                            parameter: param.name.clone(),
                            kind: dv.kind.clone(),
                            value: dv.value.resolve(&dv.kind).map_err(DbError::Api)?,
                        });
                    }
                }
                Some(dbp) => {
                    diff_parameter(&mut p, c, param, eff, dbp)?;
                }
            }
        }
    }

    // --- seed scaffolding ---
    // Creation order must respect element dependencies (an entity's elements
    // may themselves be seeded N-D entities, §8.2.2): fixpoint over the seed
    // list, creating an entity only once every element resolves against a
    // dimension-appropriate (class, name) — DB-existing or created earlier in
    // this plan (review item 9: keyed on (folded class, folded name), never
    // name alone). Validation guarantees elements reference declared seed
    // entities, so a stall is a cycle — surfaced as a blocker.
    if let Some(seed) = &t.seed {
        let mut available: HashSet<(String, String)> = vocab.entities.keys().cloned().collect();
        let mut pending: Vec<&ir::SeedEntity> = Vec::new();
        for e in &seed.entities {
            let name = e.effective_name();
            match vocab.entities.get(&(fold(&e.class), fold(&name))) {
                None => pending.push(e),
                Some(db) => {
                    let want: Vec<String> = e
                        .elements
                        .clone()
                        .unwrap_or_default()
                        .iter()
                        .map(|s| fold(s))
                        .collect();
                    let have: Vec<String> = db.elements.iter().map(|s| fold(s)).collect();
                    if want != have {
                        p.blocked.push(format!(
                            "seed entity {}:{name} exists with a different element tuple \
                             (template: {want:?}, db: {have:?}) — an N-D entity's identity \
                             is its tuple (reshape door)",
                            e.class
                        ));
                    }
                }
            }
        }
        // Can `el` serve as the element at dimension `dim` of `class`, given
        // what is available? Checked per the dimension's declared referent.
        let element_ok = |dim: &ir::DimRef, el: &str, avail: &HashSet<(String, String)>| {
            match dim {
                ir::DimRef::Class(cls) => avail.contains(&(fold(cls), fold(el))),
                ir::DimRef::Set { class_set } => chk
                    .set_ix
                    .get(&fold(class_set))
                    .map(|&sx| {
                        t.class_sets[sx]
                            .members
                            .iter()
                            .any(|m| avail.contains(&(fold(m), fold(el))))
                    })
                    .unwrap_or(false),
            }
        };
        while !pending.is_empty() {
            let ready: Vec<usize> = pending
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    let Some(&ci) = chk.class_ix.get(&fold(&e.class)) else { return false };
                    let dims = &t.classes[ci].dimensions;
                    e.elements
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .zip(dims.iter())
                        .all(|(el, dim)| element_ok(dim, el, &available))
                })
                .map(|(i, _)| i)
                .collect();
            if ready.is_empty() {
                p.blocked.push(format!(
                    "seed entities form an element-reference cycle: {:?}",
                    pending.iter().map(|e| e.effective_name()).collect::<Vec<_>>()
                ));
                break;
            }
            for &i in &ready {
                let e = pending[i];
                let name = e.effective_name();
                available.insert((fold(&e.class), fold(&name)));
                p.actions.push(Action::CreateSeedEntity {
                    class: e.class.clone(),
                    name,
                    description: e.description.clone(),
                    elements: e.elements.clone().unwrap_or_default(),
                });
            }
            for &i in ready.iter().rev() {
                pending.remove(i);
            }
        }
        // Memberships: an included=TRUE row is converged; an included=FALSE
        // row is a user exclusion — respected at execution (item 10); absence
        // gets a create action.
        let existing: HashMap<(String, String, String, String), bool> = vocab
            .memberships
            .iter()
            .map(|m| {
                (
                    (fold(&m.group_class), fold(&m.group), fold(&m.member_class), fold(&m.member)),
                    m.included,
                )
            })
            .collect();
        for m in &seed.memberships {
            let key = (
                fold(&m.group.class),
                fold(&m.group.entity),
                fold(&m.member.class),
                fold(&m.member.entity),
            );
            match existing.get(&key) {
                Some(true) => {}
                Some(false) => p.skipped.push(format!(
                    "seed membership {}:{} <- {}:{} is EXCLUDED by a user base row \
                     (included = FALSE) — the loader never overrides user data",
                    m.group.class, m.group.entity, m.member.class, m.member.entity
                )),
                None => p.actions.push(Action::CreateSeedMembership {
                    group_class: m.group.class.clone(),
                    group: m.group.entity.clone(),
                    member_class: m.member.class.clone(),
                    member: m.member.entity.clone(),
                }),
            }
        }
    }

    // --- presentation document (groups, D8) ---
    let desired_presentation = desired_presentation(t);
    let current_sub = vocab.presentation.as_ref().map(presentation_subtree);
    if desired_presentation != current_sub.unwrap_or(Value::Null) {
        let mut doc = match &vocab.presentation {
            Some(Value::Object(m)) => m.clone(),
            _ => Map::new(),
        };
        doc.remove("parameter_groups");
        doc.remove("parameter_group_membership");
        if let Value::Object(sub) = &desired_presentation {
            for (k, v) in sub {
                doc.insert(k.clone(), v.clone());
            }
        }
        p.actions.push(Action::SetPresentation {
            doc: serde_json::to_string(&Value::Object(doc)).expect("json"),
        });
    }

    // --- audit rules document (D4) ---
    let desired_rules = ir::normalized_audit_rules(t);
    let current_rules: Option<Value> = vocab
        .audit_rules
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    if desired_rules != current_rules {
        p.actions.push(Action::SetAuditRules {
            doc: desired_rules
                .as_ref()
                .map(|v| serde_json::to_string(v).expect("json")),
        });
    }

    // --- identity stamps. Also re-emitted when skips exist while a hash is
    // stamped: the executor then CLOSES the stale hash (a certified hash on
    // unconverged state is the one lie the short-circuit would believe). ---
    let identity_dirty = vocab.template_name.as_deref() != Some(t.template.name.as_str())
        || vocab.template_version.as_deref() != Some(t.template.version.as_str())
        || vocab.template_content_hash.as_deref() != Some(chk.hash.as_str());
    if identity_dirty || (!p.skipped.is_empty() && vocab.template_content_hash.is_some()) {
        p.actions.push(Action::StampIdentity {
            name: t.template.name.clone(),
            version: t.template.version.clone(),
            content_hash: chk.hash.clone(),
        });
    }

    // Execution-order pass: STABLE sort by kind precedence so every action's
    // referents exist when it runs (units/lists first, class headers before
    // sets, sets before set-typed dimension rows, definitions before
    // defaults, entities before memberships, identity stamps last). Within a
    // kind, template/diff order is preserved (incl. the seed fixpoint order).
    p.actions.sort_by_key(action_rank);
    p.no_op = p.actions.is_empty() && p.blocked.is_empty();
    Ok(p)
}

/// The apply precedence of an action kind (see the sort above).
fn action_rank(a: &Action) -> u8 {
    match a {
        Action::CreateUnit { .. } | Action::UpdateUnit { .. } => 0,
        Action::CreateValueList { .. }
        | Action::UpdateValueList { .. }
        | Action::SyncListMembers { .. } => 1,
        Action::CreateClass { .. } | Action::UpdateClass { .. } => 2,
        Action::CreateClassSet { .. }
        | Action::UpdateClassSet { .. }
        | Action::AddClassSetMember { .. } => 3,
        Action::CreateClassDims { .. } => 4,
        Action::CreateParameter { .. }
        | Action::UpdateParameter { .. }
        | Action::AddParameterType { .. }
        | Action::NarrowParameterType { .. } => 5,
        Action::SetDefault { .. } | Action::ClearDefault { .. } => 6,
        Action::CreateSeedEntity { .. } => 7,
        Action::CreateSeedMembership { .. } => 8,
        Action::SetPresentation { .. } | Action::SetAuditRules { .. } => 9,
        Action::StampIdentity { .. } => 10,
    }
}

/// Diff one existing parameter: field updates, type-set widening/narrowing,
/// default changes.
fn diff_parameter(
    p: &mut ReconcilePlan,
    c: &Class,
    param: &Parameter,
    eff: &[TypeDecl],
    dbp: &DbParam,
) -> Result<(), DbError> {
    let list_eq = opt_name_eq(&param.value_list, &dbp.value_list);
    let unit_eq = opt_name_eq(&param.unit, &dbp.unit);
    if dbp.description != param.description
        || !list_eq
        || !unit_eq
        || dbp.required != param.required
        || dbp.display_order != param.display_order
    {
        p.actions.push(Action::UpdateParameter {
            class: c.name.clone(),
            name: param.name.clone(),
            description: param.description.clone(),
            value_list: param.value_list.clone(),
            unit: param.unit.clone(),
            required: param.required,
            display_order: param.display_order,
        });
    }

    // Type set: skip entirely for a typeless template declaration (§2.2 —
    // typeless means unconstrained; auto-materialized members stay).
    if !(param.types.is_none() && param.value_list.is_none()) {
        let t_keys: Vec<_> = eff.iter().map(|m| m.match_key()).collect();
        let db_keys: Vec<_> = dbp.types.iter().map(|m| m.match_key()).collect();
        for (i, m) in eff.iter().enumerate() {
            if !db_keys.contains(&t_keys[i]) {
                p.actions.push(Action::AddParameterType {
                    class: c.name.clone(),
                    parameter: param.name.clone(),
                    member: m.clone(),
                });
            }
        }
        for (i, m) in dbp.types.iter().enumerate() {
            if !t_keys.contains(&db_keys[i]) {
                p.actions.push(Action::NarrowParameterType {
                    class: c.name.clone(),
                    parameter: param.name.clone(),
                    pt_id: m.pt_id,
                    describe: m.describe(),
                });
            }
        }
    }

    // Defaults.
    //
    // HOLD (jkiviluo ruling pending): the semantics of OMITTING `default:` in
    // the template. CURRENT behavior: omission = CLEAR (the template is
    // authoritative for its vocabulary, incl. the absence of a default). The
    // alternative under consideration: omission = no opinion, with an
    // explicit `default: null` spelling to clear. Either ruling lands
    // entirely inside this match (the `(None, Some)` arm below).
    //
    // Regardless of the ruling, a NON-SCALAR default row (long/array mirror)
    // blocks both directions conservatively (review item 6): v1 IR cannot
    // express one, so neither Set nor Clear may silently fight it — the item
    // is skipped and convergence withheld.
    match (&param.default, &dbp.default) {
        (Some(dv), maybe_db) => {
            if dbp.has_nonscalar_default {
                p.skipped.push(format!(
                    "parameter {}.{}: a non-scalar default occupies the slot — the \
                     template's scalar default is NOT applied (v1 IR cannot express or \
                     supersede it)",
                    c.name, param.name
                ));
            } else {
                let sv = dv.value.resolve(&dv.kind).map_err(DbError::Api)?;
                let changed = match maybe_db {
                    None => true,
                    Some(dbd) => {
                        dbd.value_kind != dv.kind
                            || dbd.value.identity_string() != sv.identity_string()
                    }
                };
                if changed {
                    p.actions.push(Action::SetDefault {
                        class: c.name.clone(),
                        parameter: param.name.clone(),
                        kind: dv.kind.clone(),
                        value: sv,
                    });
                }
            }
        }
        (None, Some(_)) => {
            // HOLD decision point (see above): omission currently CLEARS.
            p.actions.push(Action::ClearDefault {
                class: c.name.clone(),
                parameter: param.name.clone(),
            });
        }
        (None, None) => {
            if dbp.has_nonscalar_default {
                p.skipped.push(format!(
                    "parameter {}.{}: non-scalar default in the database — v1 IR cannot \
                     express it; left untouched",
                    c.name, param.name
                ));
            }
        }
    }
    Ok(())
}

fn opt_name_eq(a: &Option<String>, b: &Option<String>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => fold(x) == fold(y),
        _ => false,
    }
}

fn planned_member(m: &ListMember, ordinal: i64) -> PlannedListMember {
    let kind = effective_member_kind(m).expect("validated");
    let value = m.value.resolve(&kind).expect("validated");
    PlannedListMember { kind, value, description: m.description.clone(), ordinal }
}

/// The template-owned subtree of the presentation document (D8).
fn desired_presentation(t: &Template) -> Value {
    let mut sub = Map::new();
    if !t.parameter_groups.is_empty() {
        let mut groups: Vec<&ir::ParameterGroup> = t.parameter_groups.iter().collect();
        groups.sort_by_key(|g| fold(&g.name));
        let arr: Vec<Value> = groups
            .iter()
            .map(|g| {
                let mut m = Map::new();
                m.insert("name".into(), json!(g.name));
                if let Some(c) = &g.color {
                    m.insert("color".into(), json!(c));
                }
                if let Some(o) = g.order {
                    m.insert("order".into(), json!(o));
                }
                Value::Object(m)
            })
            .collect();
        sub.insert("parameter_groups".into(), Value::Array(arr));
    }
    let mut membership = Map::new();
    for c in &t.classes {
        for p in &c.parameters {
            if let Some(g) = &p.group {
                membership.insert(format!("{}.{}", c.name, p.name), json!(g));
            }
        }
    }
    if !membership.is_empty() {
        sub.insert("parameter_group_membership".into(), Value::Object(membership));
    }
    if sub.is_empty() {
        Value::Null
    } else {
        Value::Object(sub)
    }
}

/// Extract the template-owned subtree from a stored presentation document.
fn presentation_subtree(doc: &Value) -> Value {
    let mut sub = Map::new();
    if let Value::Object(m) = doc {
        if let Some(g) = m.get("parameter_groups") {
            sub.insert("parameter_groups".into(), g.clone());
        }
        if let Some(g) = m.get("parameter_group_membership") {
            sub.insert("parameter_group_membership".into(), g.clone());
        }
    }
    if sub.is_empty() {
        Value::Null
    } else {
        Value::Object(sub)
    }
}

// ---------------------------------------------------------------------------
// Action execution (apply side; runs inside the one reconcile transaction)
// ---------------------------------------------------------------------------

/// Per-apply execution context: name→id memoization (one lookup per name per
/// transaction), apply-time skips (convergence), and warnings.
#[derive(Default)]
struct ExecCtx {
    skipped: Vec<String>,
    warnings: Vec<String>,
    ids: IdCache,
}

/// Name→id cache for one transaction (review item 21). Safe: the transaction
/// is the only writer, and every id this cache stores was current when first
/// resolved inside it.
#[derive(Default)]
struct IdCache {
    class: HashMap<String, i64>,
    set: HashMap<String, i64>,
    list: HashMap<String, i64>,
    unit: HashMap<String, i64>,
    param: HashMap<(String, String), i64>,
}

fn require<T>(v: Option<T>, what: &str, name: &str) -> Result<T, DbError> {
    v.ok_or_else(|| {
        DbError::Api(format!("{what} {name:?} not found (reconcile ordering bug?)"))
    })
}

impl IdCache {
    fn class(&mut self, conn: &Connection, name: &str) -> Result<i64, DbError> {
        if let Some(&id) = self.class.get(&fold(name)) {
            return Ok(id);
        }
        let id = require(lookup_entity_class(conn, name)?, "class", name)?;
        self.class.insert(fold(name), id);
        Ok(id)
    }
    fn set(&mut self, conn: &Connection, name: &str) -> Result<i64, DbError> {
        if let Some(&id) = self.set.get(&fold(name)) {
            return Ok(id);
        }
        let id = require(lookup_class_set(conn, name)?, "class_set", name)?;
        self.set.insert(fold(name), id);
        Ok(id)
    }
    fn list(&mut self, conn: &Connection, name: &str) -> Result<i64, DbError> {
        if let Some(&id) = self.list.get(&fold(name)) {
            return Ok(id);
        }
        let id = require(lookup_value_list(conn, name)?, "value_list", name)?;
        self.list.insert(fold(name), id);
        Ok(id)
    }
    fn unit(&mut self, conn: &Connection, name: &str) -> Result<i64, DbError> {
        if let Some(&id) = self.unit.get(&fold(name)) {
            return Ok(id);
        }
        let id = require(lookup_unit(conn, name)?, "unit", name)?;
        self.unit.insert(fold(name), id);
        Ok(id)
    }
    fn param(&mut self, conn: &Connection, class: &str, name: &str) -> Result<i64, DbError> {
        let key = (fold(class), fold(name));
        if let Some(&id) = self.param.get(&key) {
            return Ok(id);
        }
        let cid = self.class(conn, class)?;
        let id = require(
            lookup_parameter(conn, cid, name)?,
            "parameter",
            &format!("{class}.{name}"),
        )?;
        self.param.insert(key, id);
        Ok(id)
    }
}

// --- per-table row specs + the ONE close-out-then-reinsert site per table
//     (review item 20: replaces the hand-written copies; RenameParameter /
//     MoveParameter / UpdateParameter share one parameter_definition INSERT) --

/// Close the current row(s) of `table` carrying `id` (versioned tables share
/// the id across versions; exactly one row is current). Returns rows closed.
fn close_row(conn: &Connection, c: i64, table: &str, id: i64) -> Result<usize, DbError> {
    let n = conn.execute(
        &format!("UPDATE {table} SET valid_to_commit = ? WHERE id = ? AND valid_to_commit IS NULL"),
        params![c, id],
    )?;
    if n != 1 {
        return Err(DbError::Api(format!(
            "revision of {table} id {id}: expected exactly one current row, closed {n}"
        )));
    }
    Ok(n)
}

struct PdRow {
    id: i64,
    entity_class_id: i64,
    name: String,
    description: Option<String>,
    parameter_value_list_id: Option<i64>,
    unit_id: Option<i64>,
    required: bool,
    display_order: Option<i64>,
    retain_content: bool,
    version: i64,
}

fn read_pd_row(conn: &Connection, pd_id: i64) -> Result<PdRow, DbError> {
    Ok(conn.query_row(
        "SELECT id, entity_class_id, name, description, parameter_value_list_id, unit_id, \
                required, display_order, retain_content, version \
         FROM parameter_definition WHERE cur_id = ?",
        params![pd_id],
        |r| {
            Ok(PdRow {
                id: r.get(0)?,
                entity_class_id: r.get(1)?,
                name: r.get(2)?,
                description: r.get(3)?,
                parameter_value_list_id: r.get(4)?,
                unit_id: r.get(5)?,
                required: r.get(6)?,
                display_order: r.get(7)?,
                retain_content: r.get(8)?,
                version: r.get(9)?,
            })
        },
    )?)
}

/// The one `parameter_definition` revision site: close the current row, insert
/// the successor (same id, `version + 1`) from `row`.
fn revise_pd_row(conn: &Connection, c: i64, row: &PdRow) -> Result<(), DbError> {
    close_row(conn, c, "parameter_definition", row.id)?;
    conn.execute(
        "INSERT INTO parameter_definition \
           (id, entity_class_id, name, description, parameter_value_list_id, unit_id, \
            required, display_order, retain_content, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            row.id,
            row.entity_class_id,
            row.name,
            row.description,
            row.parameter_value_list_id,
            row.unit_id,
            row.required,
            row.display_order,
            row.retain_content,
            c,
            row.version + 1
        ],
    )?;
    Ok(())
}

struct EcRow {
    id: i64,
    name: String,
    description: Option<String>,
    active_by_default: bool,
    required: bool,
    display_order: Option<i64>,
    version: i64,
}

fn read_ec_row(conn: &Connection, id: i64) -> Result<EcRow, DbError> {
    Ok(conn.query_row(
        "SELECT id, name, description, active_by_default, required, display_order, version \
         FROM entity_class WHERE cur_id = ?",
        params![id],
        |r| {
            Ok(EcRow {
                id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                active_by_default: r.get(3)?,
                required: r.get(4)?,
                display_order: r.get(5)?,
                version: r.get(6)?,
            })
        },
    )?)
}

fn revise_ec_row(conn: &Connection, c: i64, row: &EcRow) -> Result<(), DbError> {
    close_row(conn, c, "entity_class", row.id)?;
    conn.execute(
        "INSERT INTO entity_class \
           (id, name, description, active_by_default, required, display_order, \
            valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            row.id,
            row.name,
            row.description,
            row.active_by_default,
            row.required,
            row.display_order,
            c,
            row.version + 1
        ],
    )?;
    Ok(())
}

struct ListRow {
    id: i64,
    name: String,
    description: Option<String>,
    version: i64,
}

fn read_list_row(conn: &Connection, id: i64) -> Result<ListRow, DbError> {
    Ok(conn.query_row(
        "SELECT id, name, description, version FROM parameter_value_list WHERE cur_id = ?",
        params![id],
        |r| {
            Ok(ListRow {
                id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                version: r.get(3)?,
            })
        },
    )?)
}

fn revise_list_row(conn: &Connection, c: i64, row: &ListRow) -> Result<(), DbError> {
    close_row(conn, c, "parameter_value_list", row.id)?;
    conn.execute(
        "INSERT INTO parameter_value_list (id, name, description, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?)",
        params![row.id, row.name, row.description, c, row.version + 1],
    )?;
    Ok(())
}

/// One `list_value` member row (full payload — reads precede closes so the
/// pre-image is always available).
struct MemberRow {
    id: i64,
    list_id: i64,
    ordinal: i64,
    description: Option<String>,
    semantic_link: Option<String>,
    value_kind: String,
    v_float: Option<f64>,
    v_int: Option<i64>,
    v_bool: Option<bool>,
    v_str: Option<String>,
    version: i64,
}

fn read_member_row(conn: &Connection, id: i64) -> Result<MemberRow, DbError> {
    Ok(conn.query_row(
        "SELECT id, parameter_value_list_id, ordinal, description, semantic_link, \
                value_kind, v_float, v_int, v_bool, v_str, version \
         FROM list_value WHERE cur_id = ?",
        params![id],
        |r| {
            Ok(MemberRow {
                id: r.get(0)?,
                list_id: r.get(1)?,
                ordinal: r.get(2)?,
                description: r.get(3)?,
                semantic_link: r.get(4)?,
                value_kind: r.get(5)?,
                v_float: r.get(6)?,
                v_int: r.get(7)?,
                v_bool: r.get(8)?,
                v_str: r.get(9)?,
                version: r.get(10)?,
            })
        },
    )?)
}

fn insert_member_row(conn: &Connection, c: i64, row: &MemberRow) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO list_value \
           (id, parameter_value_list_id, ordinal, description, semantic_link, value_kind, \
            v_float, v_int, v_bool, v_str, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            row.id,
            row.list_id,
            row.ordinal,
            row.description,
            row.semantic_link,
            row.value_kind,
            row.v_float,
            row.v_int,
            row.v_bool,
            row.v_str,
            c,
            row.version + 1
        ],
    )?;
    Ok(())
}

fn insert_new_member(
    conn: &Connection,
    c: i64,
    list_id: i64,
    m: &PlannedListMember,
) -> Result<(), DbError> {
    // One scalar-column encoding site (history::VCols, review item 17).
    let vc = literal_cols(&m.value)?;
    conn.execute(
        "INSERT INTO list_value \
           (parameter_value_list_id, ordinal, description, value_kind, \
            v_float, v_int, v_bool, v_str, valid_from_commit) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![list_id, m.ordinal, m.description, vc.kind, vc.f, vc.i, vc.b, vc.s, c],
    )?;
    Ok(())
}

/// Resolve the rank-0 declared member matching a default's kind (the D11
/// binding for `definition_value_scalar.parameter_type_id`).
fn rank0_member_id(conn: &Connection, pd_id: i64, kind: &str) -> Result<Option<i64>, DbError> {
    let r = conn.query_row(
        "SELECT id FROM parameter_type WHERE parameter_definition_id = ? \
         AND valid_to_commit IS NULL AND value_kind = ? AND rank = 0",
        params![pd_id, kind],
        |r| r.get::<_, i64>(0),
    );
    match r {
        Ok(id) => Ok(Some(id)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn execute_action(
    conn: &Connection,
    c: i64,
    a: &Action,
    ctx: &mut ExecCtx,
) -> Result<(), DbError> {
    match a {
        Action::CreateUnit { name, description, quantity_kind } => {
            conn.execute(
                "INSERT INTO units_of_measure \
                   (name, description, quantity_kind_link, valid_from_commit) \
                 VALUES (?, ?, ?, ?)",
                params![name, description, quantity_kind, c],
            )?;
        }
        Action::UpdateUnit { name, description, quantity_kind } => {
            let id = ctx.ids.unit(conn, name)?;
            let (semantic_link, version): (Option<String>, i64) = conn.query_row(
                "SELECT semantic_link, version FROM units_of_measure WHERE cur_id = ?",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            close_row(conn, c, "units_of_measure", id)?;
            conn.execute(
                "INSERT INTO units_of_measure \
                   (id, name, description, semantic_link, quantity_kind_link, \
                    valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![id, name, description, semantic_link, quantity_kind, c, version + 1],
            )?;
        }
        Action::CreateValueList { name, description, members } => {
            let id: i64 = conn.query_row(
                "INSERT INTO parameter_value_list (name, description, valid_from_commit) \
                 VALUES (?, ?, ?) RETURNING id",
                params![name, description, c],
                |r| r.get(0),
            )?;
            ctx.ids.list.insert(fold(name), id);
            for m in members {
                insert_new_member(conn, c, id, m)?;
            }
        }
        Action::UpdateValueList { name, description } => {
            let id = ctx.ids.list(conn, name)?;
            let mut row = read_list_row(conn, id)?;
            row.description = description.clone();
            revise_list_row(conn, c, &row)?;
        }
        Action::SyncListMembers { list, revise, add } => {
            let lid = ctx.ids.list(conn, list)?;
            // Reads first (pre-images), then ALL closes, then ALL inserts —
            // the (list, ordinal) unique index never sees a transient state.
            let mut rows: Vec<MemberRow> = Vec::with_capacity(revise.len());
            for r in revise {
                let mut row = read_member_row(conn, r.id)?;
                row.ordinal = r.ordinal;
                row.description = r.description.clone();
                rows.push(row);
            }
            for row in &rows {
                close_row(conn, c, "list_value", row.id)?;
            }
            for row in &rows {
                insert_member_row(conn, c, row)?;
            }
            for m in add {
                insert_new_member(conn, c, lid, m)?;
            }
        }
        Action::CreateClass { name, description, active_by_default, required, display_order } => {
            let id: i64 = conn.query_row(
                "INSERT INTO entity_class \
                   (name, description, active_by_default, required, display_order, \
                    valid_from_commit) \
                 VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
                params![name, description, active_by_default, required, display_order, c],
                |r| r.get(0),
            )?;
            ctx.ids.class.insert(fold(name), id);
        }
        Action::UpdateClass { name, description, active_by_default, required, display_order } => {
            let id = ctx.ids.class(conn, name)?;
            let mut row = read_ec_row(conn, id)?;
            row.description = description.clone();
            row.active_by_default = *active_by_default;
            row.required = *required;
            row.display_order = *display_order;
            revise_ec_row(conn, c, &row)?;
        }
        Action::CreateClassDims { class, dims } => {
            let cid = ctx.ids.class(conn, class)?;
            for d in dims {
                let (dc, ds) = if d.is_set {
                    (None, Some(ctx.ids.set(conn, &d.referent)?))
                } else {
                    (Some(ctx.ids.class(conn, &d.referent)?), None)
                };
                conn.execute(
                    "INSERT INTO entity_class_dimension \
                       (entity_class_id, position, dimension_class_id, \
                        dimension_class_set_id, valid_from_commit) \
                     VALUES (?, ?, ?, ?, ?)",
                    params![cid, d.position, dc, ds, c],
                )?;
            }
        }
        Action::CreateClassSet { name, description } => {
            let id: i64 = conn.query_row(
                "INSERT INTO class_set (name, description, valid_from_commit) \
                 VALUES (?, ?, ?) RETURNING id",
                params![name, description, c],
                |r| r.get(0),
            )?;
            ctx.ids.set.insert(fold(name), id);
        }
        Action::UpdateClassSet { name, description } => {
            let id = ctx.ids.set(conn, name)?;
            let version: i64 = conn.query_row(
                "SELECT version FROM class_set WHERE cur_id = ?",
                params![id],
                |r| r.get(0),
            )?;
            close_row(conn, c, "class_set", id)?;
            conn.execute(
                "INSERT INTO class_set (id, name, description, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?)",
                params![id, name, description, c, version + 1],
            )?;
        }
        Action::AddClassSetMember { set, member } => {
            let sid = ctx.ids.set(conn, set)?;
            let mid = ctx.ids.class(conn, member)?;
            conn.execute(
                "INSERT INTO class_set_member (class_set_id, member_class_id, \
                    valid_from_commit) VALUES (?, ?, ?)",
                params![sid, mid, c],
            )?;
        }
        Action::CreateParameter {
            class,
            name,
            description,
            value_list,
            unit,
            required,
            display_order,
            types,
        } => {
            let cid = ctx.ids.class(conn, class)?;
            let lid = match value_list {
                Some(l) => Some(ctx.ids.list(conn, l)?),
                None => None,
            };
            let uid = match unit {
                Some(u) => Some(ctx.ids.unit(conn, u)?),
                None => None,
            };
            let pd_id: i64 = conn.query_row(
                "INSERT INTO parameter_definition \
                   (entity_class_id, name, description, parameter_value_list_id, unit_id, \
                    required, display_order, valid_from_commit) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
                params![cid, name, description, lid, uid, required, display_order, c],
                |r| r.get(0),
            )?;
            ctx.ids.param.insert((fold(class), fold(name)), pd_id);
            for m in types {
                insert_type_member(conn, c, pd_id, m)?;
            }
        }
        Action::UpdateParameter {
            class,
            name,
            description,
            value_list,
            unit,
            required,
            display_order,
        } => {
            let pd_id = ctx.ids.param(conn, class, name)?;
            let lid = match value_list {
                Some(l) => Some(ctx.ids.list(conn, l)?),
                None => None,
            };
            let uid = match unit {
                Some(u) => Some(ctx.ids.unit(conn, u)?),
                None => None,
            };
            let mut row = read_pd_row(conn, pd_id)?;
            row.description = description.clone();
            row.parameter_value_list_id = lid;
            row.unit_id = uid;
            row.required = *required;
            row.display_order = *display_order;
            revise_pd_row(conn, c, &row)?;
        }
        Action::AddParameterType { class, parameter, member } => {
            let pd_id = ctx.ids.param(conn, class, parameter)?;
            insert_type_member(conn, c, pd_id, member)?;
        }
        Action::NarrowParameterType { class, parameter, pt_id, describe } => {
            // §4.2: narrowing is flagged when current data instantiates the
            // removed member. Conservative: any current value bound to the
            // member — or any current value of the definition with a NULL
            // binding (pre-D11 rows whose member we cannot know) — keeps the
            // member, recorded as a SKIP (convergence withheld). Plain COUNT
            // aggregates, no side-effect guards (module docs).
            let pd_id = ctx.ids.param(conn, class, parameter)?;
            let bound: i64 = conn.query_row(
                "SELECT \
                   (SELECT count(*) FROM value_scalar WHERE parameter_type_id = ?1 \
                      AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM value_long WHERE parameter_type_id = ?1 \
                      AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM value_array WHERE parameter_type_id = ?1) + \
                   (SELECT count(*) FROM definition_value_scalar WHERE parameter_type_id = ?1 \
                      AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM definition_value_long WHERE parameter_type_id = ?1 \
                      AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM definition_value_array WHERE parameter_type_id = ?1 \
                      AND valid_to_commit IS NULL)",
                params![pt_id],
                |r| r.get(0),
            )?;
            let unbound: i64 = conn.query_row(
                "SELECT \
                   (SELECT count(*) FROM value_scalar WHERE parameter_definition_id = ?1 \
                      AND parameter_type_id IS NULL AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM value_long WHERE parameter_definition_id = ?1 \
                      AND parameter_type_id IS NULL AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM value_array WHERE parameter_definition_id = ?1 \
                      AND parameter_type_id IS NULL)",
                params![pd_id],
                |r| r.get(0),
            )?;
            if bound > 0 || unbound > 0 {
                ctx.skipped.push(format!(
                    "narrowing {class}.{parameter}: {describe} kept — {bound} current \
                     value row(s) bind it{}",
                    if unbound > 0 {
                        format!(" (+{unbound} unbound rows whose member is unknown)")
                    } else {
                        String::new()
                    }
                ));
            } else {
                conn.execute(
                    "UPDATE parameter_type SET valid_to_commit = ? WHERE id = ? \
                     AND valid_to_commit IS NULL",
                    params![c, pt_id],
                )?;
                conn.execute(
                    "UPDATE parameter_type_dimension SET valid_to_commit = ? \
                     WHERE parameter_type_id = ? AND valid_to_commit IS NULL",
                    params![c, pt_id],
                )?;
            }
        }
        Action::SetDefault { class, parameter, kind, value } => {
            let pd_id = ctx.ids.param(conn, class, parameter)?;
            let pt_id = rank0_member_id(conn, pd_id, kind)?;
            write_definition_default(conn, c, pd_id, value, pt_id)?;
        }
        Action::ClearDefault { class, parameter } => {
            let pd_id = ctx.ids.param(conn, class, parameter)?;
            close_definition_default(conn, c, pd_id)?;
        }
        Action::CreateSeedEntity { class, name, description, elements } => {
            let cid = ctx.ids.class(conn, class)?;
            let eid: i64 = conn.query_row(
                "INSERT INTO entity (entity_class_id, name, description, valid_from_commit) \
                 VALUES (?, ?, ?, ?) RETURNING id",
                params![cid, name, description, c],
                |r| r.get(0),
            )?;
            for (i, el) in elements.iter().enumerate() {
                let pos = i as i64 + 1;
                let el_id = resolve_seed_element(conn, cid, pos, el)?;
                conn.execute(
                    "INSERT INTO entity_element \
                       (entity_id, position, element_entity_id, valid_from_commit) \
                     VALUES (?, ?, ?, ?)",
                    params![eid, pos, el_id, c],
                )?;
            }
        }
        Action::CreateSeedMembership { group_class, group, member_class, member } => {
            let gc = ctx.ids.class(conn, group_class)?;
            let mc = ctx.ids.class(conn, member_class)?;
            let gid = require(
                lookup_entity(conn, gc, group)?,
                "seed membership group entity",
                &format!("{group_class}:{group}"),
            )?;
            let mid = require(
                lookup_entity(conn, mc, member)?,
                "seed membership member entity",
                &format!("{member_class}:{member}"),
            )?;
            // Item 10: consult ANY existing current base row first. TRUE ⇒
            // converged no-op; FALSE ⇒ a user exclusion, respected (skip);
            // absent ⇒ insert (never a second current row into the
            // (group, member, alt) one-current index).
            let existing: Option<bool> = match conn.query_row(
                "SELECT included FROM membership WHERE cur_g = ? AND cur_m = ? AND cur_a = 0",
                params![gid, mid],
                |r| r.get(0),
            ) {
                Ok(v) => Some(v),
                Err(duckdb::Error::QueryReturnedNoRows) => None,
                Err(e) => return Err(e.into()),
            };
            match existing {
                Some(true) => {}
                Some(false) => ctx.skipped.push(format!(
                    "seed membership {group_class}:{group} <- {member_class}:{member} is \
                     EXCLUDED by a user base row (included = FALSE) — respected, not \
                     overridden"
                )),
                None => {
                    conn.execute(
                        "INSERT INTO membership \
                           (group_entity_id, member_entity_id, alternative_id, included, \
                            valid_from_commit) \
                         VALUES (?, ?, NULL, TRUE, ?)",
                        params![gid, mid, c],
                    )?;
                }
            }
        }
        Action::SetPresentation { doc } => {
            set_metadata(conn, c, "presentation", doc)?;
        }
        Action::SetAuditRules { doc } => match doc {
            Some(d) => set_metadata(conn, c, "audit_rules", d)?,
            None => clear_metadata(conn, c, "audit_rules")?,
        },
        Action::StampIdentity { name, version, content_hash } => {
            set_metadata(conn, c, "template_name", name)?;
            set_metadata(conn, c, "template_version", version)?;
            if ctx.skipped.is_empty() {
                set_metadata(conn, c, "template_content_hash", content_hash)?;
            } else {
                // Partial apply: withhold certification (module docs). A
                // stale hash from an earlier converged apply is closed so the
                // short-circuit cannot believe it.
                clear_metadata(conn, c, "template_content_hash")?;
                ctx.warnings.push(format!(
                    "identity stamped WITHOUT template_content_hash ({} skip(s)); the \
                     template will replan until it converges",
                    ctx.skipped.len()
                ));
            }
        }
    }
    Ok(())
}

/// Resolve one seed element name against the dimension's declared referent,
/// through object_delete's shared [`resolve_element_in_dim`] (one resolution
/// semantics for elements everywhere; ambiguity across a set's members is a
/// clean error mirroring `audit_class_set_entity_name_dup`).
fn resolve_seed_element(
    conn: &Connection,
    class_id: i64,
    position: i64,
    element: &str,
) -> Result<i64, DbError> {
    let (dc, ds): (Option<i64>, Option<i64>) = conn.query_row(
        "SELECT dimension_class_id, dimension_class_set_id FROM entity_class_dimension \
         WHERE cur_c = ? AND cur_p = ?",
        params![class_id, position],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    require(
        resolve_element_in_dim(conn, dc, ds, element)?,
        "seed element",
        &format!("{element} (position {position})"),
    )
}

/// Insert one declared type member through the api's one insert path.
fn insert_type_member(conn: &Connection, c: i64, pd_id: i64, m: &TypeDecl) -> Result<(), DbError> {
    let dims: Vec<(String, IndexKind)> = m
        .dims
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(|d| Ok((d.name.clone(), parse_index_kind(&d.kind)?)))
        .collect::<Result<_, DbError>>()?;
    let vk = match m.kind.as_str() {
        "float" => ValueKind::Float,
        "int" => ValueKind::Int,
        "bool" => ValueKind::Bool,
        "string" => ValueKind::String,
        "date_time" => ValueKind::DateTime,
        "duration" => ValueKind::Duration,
        // "indexed": the value_kind argument is ignored when dims is
        // non-empty (create_parameter_type stores 'indexed').
        _ => ValueKind::Float,
    };
    create_parameter_type(conn, c, pd_id, &dims, vk)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Reserved-key metadata writes (close-out-then-insert; audit_metadata_reserved_dup)
// ---------------------------------------------------------------------------

/// Set a reserved single-valued metadata key: no-op when the current value is
/// already equal, else close-out-then-insert under the same id (the writer
/// rule the reserved-dup audit documents).
pub(crate) fn set_metadata(
    conn: &Connection,
    c: i64,
    key: &str,
    value: &str,
) -> Result<(), DbError> {
    let existing = conn.query_row(
        "SELECT id, value, version FROM metadata WHERE cur_name = lower(?)",
        params![key],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?)),
    );
    match existing {
        Ok((_, v, _)) if v == value => Ok(()),
        Ok((id, _, version)) => {
            conn.execute(
                "UPDATE metadata SET valid_to_commit = ? WHERE id = ? AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO metadata (id, name, value, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?)",
                params![id, key, value, c, version + 1],
            )?;
            Ok(())
        }
        Err(duckdb::Error::QueryReturnedNoRows) => {
            conn.execute(
                "INSERT INTO metadata (name, value, valid_from_commit) VALUES (?, ?, ?)",
                params![key, value, c],
            )?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn clear_metadata(conn: &Connection, c: i64, key: &str) -> Result<(), DbError> {
    conn.execute(
        "UPDATE metadata SET valid_to_commit = ? WHERE cur_name = lower(?)",
        params![c, key],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Layer-2 hint execution (one commit per step; apply side)
// ---------------------------------------------------------------------------

fn charset_ok(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn hint_class_id(conn: &Connection, step: &str, name: &str) -> Result<i64, DbError> {
    lookup_entity_class(conn, name)?
        .ok_or_else(|| DbError::Api(format!("{step}: class {name:?} not found")))
}

fn execute_step(conn: &Connection, c: i64, step: &MigrationStep) -> Result<(), DbError> {
    match step {
        MigrationStep::RenameClass { from, to } => {
            if !charset_ok(to) {
                return Err(DbError::Api(format!(
                    "rename_class: target {to:?} must match [A-Za-z0-9_]+"
                )));
            }
            let id = lookup_entity_class(conn, from)?
                .ok_or_else(|| DbError::Api(format!("rename_class: {from:?} not found")))?;
            if fold(from) != fold(to) && lookup_entity_class(conn, to)?.is_some() {
                return Err(DbError::Api(format!(
                    "rename_class: target name {to:?} already exists"
                )));
            }
            let mut row = read_ec_row(conn, id)?;
            row.name = to.clone();
            revise_ec_row(conn, c, &row)?;
        }
        MigrationStep::RenameParameter { class, from, to } => {
            if !charset_ok(to) {
                return Err(DbError::Api(format!(
                    "rename_parameter: target {to:?} must match [A-Za-z0-9_]+"
                )));
            }
            let cid = hint_class_id(conn, "rename_parameter", class)?;
            let pd_id = lookup_parameter(conn, cid, from)?
                .ok_or_else(|| DbError::Api(format!("rename_parameter: {from:?} not found")))?;
            if fold(from) != fold(to) && lookup_parameter(conn, cid, to)?.is_some() {
                return Err(DbError::Api(format!(
                    "rename_parameter: {class}.{to} already exists"
                )));
            }
            let mut row = read_pd_row(conn, pd_id)?;
            row.name = to.clone();
            revise_pd_row(conn, c, &row)?;
        }
        MigrationStep::RenameValueList { from, to } => {
            let id = lookup_value_list(conn, from)?
                .ok_or_else(|| DbError::Api(format!("rename_value_list: {from:?} not found")))?;
            if fold(from) != fold(to) && lookup_value_list(conn, to)?.is_some() {
                return Err(DbError::Api(format!(
                    "rename_value_list: target name {to:?} already exists"
                )));
            }
            let mut row = read_list_row(conn, id)?;
            row.name = to.clone();
            revise_list_row(conn, c, &row)?;
        }
        MigrationStep::RenameListMember { list, from, to, rewrite_values } => {
            let lid = lookup_value_list(conn, list)?
                .ok_or_else(|| DbError::Api(format!("rename_list_member: list {list:?} not found")))?;
            let member_id: i64 = conn
                .query_row(
                    "SELECT id FROM list_value WHERE cur_l = ? AND value_kind = 'string' \
                     AND v_str = ? AND valid_to_commit IS NULL",
                    params![lid, from],
                    |r| r.get(0),
                )
                .map_err(|e| match e {
                    duckdb::Error::QueryReturnedNoRows => DbError::Api(format!(
                        "rename_list_member: {list} has no string member {from:?}"
                    )),
                    e => e.into(),
                })?;
            let taken: i64 = conn.query_row(
                "SELECT count(*) FROM list_value WHERE cur_l = ? AND value_kind = 'string' \
                 AND v_str = ? AND valid_to_commit IS NULL",
                params![lid, to],
                |r| r.get(0),
            )?;
            if taken > 0 {
                return Err(DbError::Api(format!(
                    "rename_list_member: {list} already has a member {to:?}"
                )));
            }
            let mut row = read_member_row(conn, member_id)?;
            row.v_str = Some(to.clone());
            close_row(conn, c, "list_value", member_id)?;
            insert_member_row(conn, c, &row)?;
            if *rewrite_values {
                // The one data-touching hint (§4.2 Layer 2): rewrite matching
                // CURRENT stored strings of every definition bound to this
                // list — per definition, through the history protocol, which
                // honors the definition's retain_content (VMP Part 5 stub
                // contract; review item 5).
                let mut stmt = conn.prepare(
                    "SELECT id, retain_content FROM parameter_definition \
                     WHERE valid_to_commit IS NULL AND parameter_value_list_id = ?",
                )?;
                let defs: Vec<(i64, bool)> = stmt
                    .query_map(params![lid], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()?;
                for (pd_id, retain) in defs {
                    rewrite_scalar_string_values(conn, c, pd_id, from, to, retain)?;
                    rewrite_definition_string_values(conn, c, pd_id, from, to)?;
                }
            }
        }
        MigrationStep::MoveParameter { name, from_class, to_class } => {
            let from_cid = hint_class_id(conn, "move_parameter", from_class)?;
            let to_cid = hint_class_id(conn, "move_parameter", to_class)?;
            let pd_id = lookup_parameter(conn, from_cid, name)?
                .ok_or_else(|| DbError::Api(format!("move_parameter: {name:?} not found")))?;
            if lookup_parameter(conn, to_cid, name)?.is_some() {
                return Err(DbError::Api(format!(
                    "move_parameter: {to_class}.{name} already exists"
                )));
            }
            // v1 guard (§4.2): values do not carry — a value's entity belongs
            // to the OLD class, so a move with live values would strand them
            // (audit_value_class_mismatch). The carry case is the reshape door.
            let values: i64 = conn.query_row(
                "SELECT \
                   (SELECT count(*) FROM value_scalar WHERE parameter_definition_id = ?1 \
                      AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM value_long WHERE parameter_definition_id = ?1 \
                      AND valid_to_commit IS NULL) + \
                   (SELECT count(*) FROM value_array WHERE parameter_definition_id = ?1)",
                params![pd_id],
                |r| r.get(0),
            )?;
            if values > 0 {
                return Err(DbError::Api(format!(
                    "move_parameter: {from_class}.{name} holds {values} current value \
                     row(s); moving values between classes is the deferred reshape door \
                     (delete or migrate them first via a run: program)"
                )));
            }
            let mut row = read_pd_row(conn, pd_id)?;
            row.entity_class_id = to_cid;
            revise_pd_row(conn, c, &row)?;
        }
        MigrationStep::Run(_) => unreachable!("run steps execute through the registry"),
    }
    Ok(())
}
