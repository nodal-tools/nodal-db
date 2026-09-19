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
//! Plan-then-apply shape (migration-safety discussion): [`plan`] is a
//! read-only, human-readable dry run. Like `api::preview_object_deletes` it is
//! a best-effort snapshot — [`apply`] recomputes the diff inside its own write
//! transaction (fresh state per conflict retry), so the applied plan, not the
//! previewed one, is authoritative.
//!
//! **Write-path discipline (CLAUDE.md invariant #2).** This module lives in
//! the one core and writes structural rows exactly the way `api.rs`'s
//! auto-create resolvers do (same tables, same from/to protocol); declared
//! type members go through `api::create_parameter_type` (the one insert path);
//! defaults are values and land through `history::write_definition_default`
//! (a rank-0 scalar's canonical form is itself — VMP Part 3). No domain row
//! is written outside this crate.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;

use duckdb::{params, Connection};
use serde_json::{json, Map, Value};

use crate::api::{create_parameter_type, parse_index_kind};
use crate::conn::{with_read, with_write_txn_retrying};
use crate::error::DbError;
use crate::history::{close_definition_default, write_definition_default};
use crate::types::{IndexKind, ScalarValue, ValueKind};

use super::dbvocab::{read_vocab, DbParam, DbVocab};
use super::ir::{
    self, effective_member_kind, template_content_hash, Checked, Class, ListMember, Migration,
    MigrationStep, Parameter, Template, TypeDecl,
};

fn fold(s: &str) -> String {
    s.to_lowercase()
}

// ---------------------------------------------------------------------------
// The migration-program registry (Layer 3)
// ---------------------------------------------------------------------------

/// A named tool-side migration program (§4.2 Layer 3): runs with the DB path
/// and the commit author, and does ALL its writes through the public write API
/// (its own commits). The IR's `run:` step names it; the registry binds it.
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

// ---------------------------------------------------------------------------
// The plan model
// ---------------------------------------------------------------------------

/// One planned (and then applied) reconcile write. Carries the full payload
/// needed to execute, so plan and apply cannot diverge by re-interpretation —
/// apply re-diffs inside its transaction and executes exactly these.
#[derive(Clone, Debug)]
pub enum Action {
    CreateUnit { name: String, description: Option<String>, quantity_kind: Option<String> },
    UpdateUnit { name: String, description: Option<String>, quantity_kind: Option<String> },
    CreateValueList { name: String, description: Option<String>, members: Vec<PlannedListMember> },
    UpdateValueList { name: String, description: Option<String> },
    AddListMember { list: String, member: PlannedListMember },
    /// Description-only member update (member identity is its typed value).
    UpdateListMember { list: String, kind: String, value: ScalarValue, description: Option<String> },
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
    /// member"). Apply skips-with-warning when any current value binds it.
    NarrowParameterType { class: String, parameter: String, pt_id: i64, describe: String },
    SetDefault { class: String, parameter: String, kind: String, value: ScalarValue },
    ClearDefault { class: String, parameter: String },
    CreateSeedEntity {
        class: String,
        name: String,
        description: Option<String>,
        elements: Vec<String>,
    },
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
    StampIdentity { name: String, version: String, content_hash: String },
}

/// A list member ready to write (kind + typed value resolved at plan time).
#[derive(Clone, Debug)]
pub struct PlannedListMember {
    pub kind: String,
    pub value: ScalarValue,
    pub description: Option<String>,
    pub ordinal: i64,
}

#[derive(Clone, Debug)]
pub struct PlannedDim {
    pub position: i64,
    pub referent: String,
    pub is_set: bool,
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
            Action::AddListMember { list, member } => {
                write!(f, "add value_list member {list}: {}", scalar_display(&member.value))
            }
            Action::UpdateListMember { list, value, .. } => {
                write!(f, "update value_list member {list}: {}", scalar_display(value))
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
                 template_content_hash={content_hash}"
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
/// diff's action list, blockers (reshape-door conflicts — apply refuses while
/// any exist), warnings, and informational notes.
#[derive(Clone, Debug, Default)]
pub struct ReconcilePlan {
    pub template_name: String,
    pub template_version: String,
    pub content_hash: String,
    pub db_template_name: Option<String>,
    pub db_template_version: Option<String>,
    pub pending_migrations: Vec<PlannedSpan>,
    pub actions: Vec<Action>,
    pub blocked: Vec<String>,
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
            (Some(n), Some(v)) => writeln!(f, "database: template {n} version {v}")?,
            (Some(n), None) => writeln!(f, "database: template {n} (no version stamped)")?,
            _ => writeln!(f, "database: no template stamped (fresh load)")?,
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
        for w in &self.warnings {
            writeln!(f, "  warning: {w}")?;
        }
        for n in &self.notes {
            writeln!(f, "  note: {n}")?;
        }
        Ok(())
    }
}

/// The outcome of [`apply`].
#[derive(Debug, Default)]
pub struct ApplyOutcome {
    /// True when nothing needed to change (content-hash short-circuit or an
    /// empty diff): no commit was made.
    pub no_op: bool,
    /// Commit ids in order: one per executed migration step / version stamp,
    /// plus the final reconcile commit. Escape-hatch programs commit through
    /// the public API on their own; those ids are not tracked here.
    pub commits: Vec<i64>,
    /// The plan as APPLIED (recomputed inside the final transaction).
    pub plan: ReconcilePlan,
    pub warnings: Vec<String>,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Read-only dry run: validate, snapshot the DB vocabulary, simulate any
/// pending migration chain on the snapshot (renames applied in memory; `run:`
/// programs noted, not simulated), and diff. One per-operation RO connection
/// (CLAUDE.md invariant 1).
pub fn plan(db: &Path, t: &Template) -> Result<ReconcilePlan, DbError> {
    let chk = ir::validate(t)?;
    with_read(db, |conn| {
        let mut warnings = chk.warnings.clone();
        let mut vocab = read_vocab(conn, &mut warnings)?;
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
        p.pending_migrations = planned_spans;
        p.warnings.splice(0..0, warnings);
        Ok(p)
    })
}

/// Apply the template: content-hash short-circuit, then the pending migration
/// chain (one commit per declarative step; `run:` programs through the
/// registry and the public API; a version stamp per span), then the final
/// reconcile diff in ONE commit that also stamps identity (§7). Refuses while
/// the diff reports blockers (reshape-door conflicts).
pub fn apply(
    db: &Path,
    t: &Template,
    registry: &MigrationRegistry,
    user: &str,
    message: &str,
) -> Result<ApplyOutcome, DbError> {
    let chk = ir::validate(t)?;
    if user.trim().is_empty() {
        return Err(DbError::Api("user must be non-empty".into()));
    }
    let hash = template_content_hash(t);
    let mut out = ApplyOutcome::default();
    out.warnings.extend(chk.warnings.iter().cloned());

    // Fast idempotency short-circuit (§7): same template, same version, same
    // normalized content — nothing to do, no commit.
    let (db_name, db_version, db_hash, spans) = with_read(db, |conn| {
        let mut w = Vec::new();
        let v = read_vocab(conn, &mut w)?;
        let spans = pending_spans(&v, t);
        Ok((v.template_name.clone(), v.template_version.clone(), v.template_content_hash, spans))
    })?;
    if db_name.as_deref() == Some(t.template.name.as_str())
        && db_version.as_deref() == Some(t.template.version.as_str())
        && db_hash.as_deref() == Some(hash.as_str())
    {
        out.no_op = true;
        out.plan = ReconcilePlan {
            template_name: t.template.name.clone(),
            template_version: t.template.version.clone(),
            content_hash: hash,
            db_template_name: db_name,
            db_template_version: db_version,
            no_op: true,
            ..Default::default()
        };
        return Ok(out);
    }

    // --- Layer 2/3: the pending migration chain, in span/step order. ---
    for span in &spans {
        // Every named program must be registered BEFORE any step of the span
        // runs (never leave a chain half-applied over a typo).
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
        for step in &span.steps {
            match step {
                MigrationStep::Run(p) => {
                    // Layer 3: tool-side program through the public write API.
                    let program = registry.get(p).unwrap();
                    program(db, user)?;
                }
                declarative => {
                    let desc = declarative.describe();
                    let msg = format!(
                        "template {} migration {} -> {}: {desc}",
                        t.template.name, span.from, span.to
                    );
                    let commit = with_write_txn_retrying(db, user, &msg, |conn, c| {
                        execute_step(conn, c, declarative)?;
                        Ok(c)
                    })?;
                    out.commits.push(commit);
                }
            }
        }
        // Stamp the span's target version so an interrupted chain resumes at
        // the right span (§4.4: "in order between version stamps").
        let msg = format!(
            "template {} migration {} -> {}: version stamp",
            t.template.name, span.from, span.to
        );
        let commit = with_write_txn_retrying(db, user, &msg, |conn, c| {
            set_metadata(conn, c, "template_version", &span.to)?;
            Ok(c)
        })?;
        out.commits.push(commit);
    }

    // --- Layer 1: the final reconcile, one commit, identity stamped. ---
    // Pre-check RO so an empty diff never burns a commit (§7 idempotency);
    // the write txn re-diffs, so a racing writer only costs one empty-ish
    // commit, never a wrong apply.
    let pre = with_read(db, |conn| {
        let mut w = Vec::new();
        let vocab = read_vocab(conn, &mut w)?;
        diff(&vocab, &chk)
    })?;
    if !pre.blocked.is_empty() {
        return Err(blocked_error(&pre));
    }
    if pre.actions.is_empty() {
        out.no_op = out.commits.is_empty();
        out.plan = pre;
        return Ok(out);
    }

    let msg = if message.is_empty() {
        format!("template {} version {}: apply", t.template.name, t.template.version)
    } else {
        message.to_string()
    };
    let applied = with_write_txn_retrying(db, user, &msg, |conn, c| {
        let mut w = Vec::new();
        let vocab = read_vocab(conn, &mut w)?;
        let mut p = diff(&vocab, &chk)?;
        if !p.blocked.is_empty() {
            return Err(blocked_error(&p));
        }
        for a in &p.actions {
            execute_action(conn, c, a, &mut p.warnings)?;
        }
        p.warnings.splice(0..0, w);
        Ok((c, p))
    })?;
    out.commits.push(applied.0);
    out.plan = applied.1;
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

// ---------------------------------------------------------------------------
// Migration-chain selection + in-memory simulation (plan side)
// ---------------------------------------------------------------------------

/// The spans to run: follow the chain from the DB's stamped version toward the
/// template's version. No stamped version (fresh DB) or already-at-target ⇒
/// no spans. A cycle in the chain is a hard error.
fn pending_spans(vocab: &DbVocab, t: &Template) -> Vec<Migration> {
    let Some(mut cur) = vocab.template_version.clone() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    while cur != t.template.version {
        if !seen.insert(cur.clone()) {
            break; // cycle: refuse to loop; the diff surfaces the residue
        }
        match t.migrations.iter().find(|m| m.from == cur) {
            Some(m) => {
                cur = m.to.clone();
                out.push(m.clone());
            }
            None => break, // gap: plain reconcile bridges the rest (§4.2 L1)
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
        content_hash: template_content_hash(t),
        db_template_name: vocab.template_name.clone(),
        db_template_version: vocab.template_version.clone(),
        ..Default::default()
    };

    if let (Some(dbn), n) = (&vocab.template_name, &t.template.name) {
        if fold(dbn) != fold(n) {
            p.warnings.push(format!(
                "database is stamped with template {dbn:?}; applying {n:?} restamps the \
                 identity keys (v1 layers templates only when their namespaces are disjoint)"
            ));
        }
    }
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

    // --- value lists ---
    for l in &t.value_lists {
        let planned_members = |offset: i64| -> Vec<PlannedListMember> {
            l.members
                .iter()
                .enumerate()
                .map(|(i, m)| planned_member(m, offset + i as i64 + 1))
                .collect()
        };
        match vocab.value_lists.get(&fold(&l.name)) {
            None => p.actions.push(Action::CreateValueList {
                name: l.name.clone(),
                description: l.description.clone(),
                members: planned_members(0),
            }),
            Some(db) => {
                if db.description != l.description {
                    p.actions.push(Action::UpdateValueList {
                        name: l.name.clone(),
                        description: l.description.clone(),
                    });
                }
                // Match members by their typed value; existing members keep
                // their DB ordinals, new members append after the current max
                // (mid-list insertion order is not preserved on reconcile —
                // a documented v1 simplification; fresh loads preserve order).
                let mut db_index: HashMap<(String, String), &super::dbvocab::DbListMember> =
                    HashMap::new();
                for m in &db.members {
                    db_index.insert(
                        (m.value_kind.clone(), scalar_key(&m.value)),
                        m,
                    );
                }
                let mut next_ordinal =
                    db.members.iter().map(|m| m.ordinal).max().unwrap_or(0);
                let mut matched: HashSet<(String, String)> = HashSet::new();
                for m in &l.members {
                    let kind = effective_member_kind(m)
                        .map_err(|e| DbError::Api(format!("value_list {:?}: {e}", l.name)))?;
                    let sv = m.value.resolve(&kind).map_err(DbError::Api)?;
                    let key = (kind.clone(), scalar_key(&sv));
                    matched.insert(key.clone());
                    match db_index.get(&key) {
                        None => {
                            next_ordinal += 1;
                            p.actions.push(Action::AddListMember {
                                list: l.name.clone(),
                                member: PlannedListMember {
                                    kind,
                                    value: sv,
                                    description: m.description.clone(),
                                    ordinal: next_ordinal,
                                },
                            });
                        }
                        Some(dbm) => {
                            if dbm.description != m.description {
                                p.actions.push(Action::UpdateListMember {
                                    list: l.name.clone(),
                                    kind,
                                    value: sv,
                                    description: m.description.clone(),
                                });
                            }
                        }
                    }
                }
                for m in &db.members {
                    let key = (m.value_kind.clone(), scalar_key(&m.value));
                    if !matched.contains(&key) {
                        p.warnings.push(format!(
                            "value_list {:?}: member {} is not in the template — left in \
                             place (member removal is not additive; a rename is a \
                             rename_list_member hint)",
                            l.name,
                            scalar_display(&m.value)
                        ));
                    }
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
    // list, creating an entity only once every element name is available
    // (already in the DB or created earlier in this plan). Validation
    // guarantees elements reference declared seed entities, so a stall is a
    // cycle — surfaced as a blocker.
    if let Some(seed) = &t.seed {
        let mut available: HashSet<String> =
            vocab.entities.keys().map(|(_, n)| n.clone()).collect();
        let mut pending: Vec<&crate::template::ir::SeedEntity> = Vec::new();
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
        while !pending.is_empty() {
            let ready: Vec<usize> = pending
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    e.elements
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .all(|el| available.contains(&fold(el)))
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
            // Emit in template order within the wave; remove afterwards in
            // reverse index order so indices stay valid.
            for &i in &ready {
                let e = pending[i];
                let name = e.effective_name();
                available.insert(fold(&name));
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
        let existing: HashSet<(String, String, String, String)> = vocab
            .memberships
            .iter()
            .map(|m| {
                (fold(&m.group_class), fold(&m.group), fold(&m.member_class), fold(&m.member))
            })
            .collect();
        for m in &seed.memberships {
            let key = (
                fold(&m.group.class),
                fold(&m.group.entity),
                fold(&m.member.class),
                fold(&m.member.entity),
            );
            if !existing.contains(&key) {
                p.actions.push(Action::CreateSeedMembership {
                    group_class: m.group.class.clone(),
                    group: m.group.entity.clone(),
                    member_class: m.member.class.clone(),
                    member: m.member.entity.clone(),
                });
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
        if doc.is_empty() {
            // Whole doc reduces to nothing: represent as a clear via an empty
            // object (keeps the reserved key single-valued semantics simple).
            p.actions.push(Action::SetPresentation { doc: "{}".to_string() });
        } else {
            p.actions.push(Action::SetPresentation {
                doc: serde_json::to_string(&Value::Object(doc)).expect("json"),
            });
        }
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

    // --- identity stamps ---
    let hash = p.content_hash.clone();
    if vocab.template_name.as_deref() != Some(t.template.name.as_str())
        || vocab.template_version.as_deref() != Some(t.template.version.as_str())
        || vocab.template_content_hash.as_deref() != Some(hash.as_str())
    {
        p.actions.push(Action::StampIdentity {
            name: t.template.name.clone(),
            version: t.template.version.clone(),
            content_hash: hash,
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
        | Action::AddListMember { .. }
        | Action::UpdateListMember { .. } => 1,
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

    // Default: template-authoritative (a vocabulary fact).
    match (&param.default, &dbp.default) {
        (Some(dv), maybe_db) => {
            let sv = dv.value.resolve(&dv.kind).map_err(DbError::Api)?;
            let changed = match maybe_db {
                None => true,
                Some(dbd) => dbd.value_kind != dv.kind || scalar_key(&dbd.value) != scalar_key(&sv),
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
        (None, Some(_)) => {
            if dbp.has_nonscalar_default {
                p.warnings.push(format!(
                    "parameter {}.{}: non-scalar default in the database — v1 IR cannot \
                     express it; left untouched",
                    c.name, param.name
                ));
            } else {
                p.actions.push(Action::ClearDefault {
                    class: c.name.clone(),
                    parameter: param.name.clone(),
                });
            }
        }
        (None, None) => {}
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

/// A hashable identity string for a scalar (mirrors the IR's canonical
/// literal string; float text is Rust's shortest round-trip form).
fn scalar_key(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Float(f) => format!("f:{f}"),
        ScalarValue::Int(i) => format!("i:{i}"),
        ScalarValue::Bool(b) => format!("b:{b}"),
        ScalarValue::Str(s) => format!("s:{s}"),
        ScalarValue::DateTime(t) => format!("d:{t}"),
        ScalarValue::Duration { months, micros } => format!("u:{months}:{micros}"),
    }
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

fn lookup_id(
    conn: &Connection,
    sql: &str,
    name: &str,
    what: &str,
) -> Result<i64, DbError> {
    conn.query_row(sql, params![name], |r| r.get(0)).map_err(|e| match e {
        duckdb::Error::QueryReturnedNoRows => {
            DbError::Api(format!("{what} {name:?} not found (reconcile ordering bug?)"))
        }
        e => e.into(),
    })
}

fn class_id(conn: &Connection, name: &str) -> Result<i64, DbError> {
    lookup_id(conn, "SELECT id FROM entity_class WHERE cur_name = lower(?)", name, "class")
}

fn set_id(conn: &Connection, name: &str) -> Result<i64, DbError> {
    lookup_id(conn, "SELECT id FROM class_set WHERE cur_name = lower(?)", name, "class_set")
}

fn list_id(conn: &Connection, name: &str) -> Result<i64, DbError> {
    lookup_id(
        conn,
        "SELECT id FROM parameter_value_list WHERE cur_name = lower(?)",
        name,
        "value_list",
    )
}

fn unit_id(conn: &Connection, name: &str) -> Result<i64, DbError> {
    lookup_id(conn, "SELECT id FROM units_of_measure WHERE cur_name = lower(?)", name, "unit")
}

fn param_id(conn: &Connection, class: &str, name: &str) -> Result<i64, DbError> {
    let cid = class_id(conn, class)?;
    conn.query_row(
        "SELECT id FROM parameter_definition WHERE cur_cls = ? AND cur_name = lower(?)",
        params![cid, name],
        |r| r.get(0),
    )
    .map_err(|e| match e {
        duckdb::Error::QueryReturnedNoRows => {
            DbError::Api(format!("parameter {class}.{name} not found (ordering bug?)"))
        }
        e => e.into(),
    })
}

fn entity_id_in_class(conn: &Connection, class_id: i64, name: &str) -> Result<Option<i64>, DbError> {
    let r = conn.query_row(
        "SELECT id FROM entity WHERE cur_cls = ? AND cur_name = lower(?)",
        params![class_id, name],
        |r| r.get::<_, i64>(0),
    );
    match r {
        Ok(id) => Ok(Some(id)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The typed value columns of a scalar for INSERT binds (mirrors history's
/// private `VCols`; duplicated narrowly for `list_value`, which history does
/// not write).
struct ScalarCols {
    kind: &'static str,
    f: Option<f64>,
    i: Option<i64>,
    b: Option<bool>,
    s: Option<String>,
}

impl ScalarCols {
    fn of(v: &ScalarValue) -> Result<ScalarCols, DbError> {
        let mut c = ScalarCols { kind: "", f: None, i: None, b: None, s: None };
        match v {
            ScalarValue::Float(x) => {
                c.kind = "float";
                c.f = Some(*x);
            }
            ScalarValue::Int(x) => {
                c.kind = "int";
                c.i = Some(*x);
            }
            ScalarValue::Bool(x) => {
                c.kind = "bool";
                c.b = Some(*x);
            }
            ScalarValue::Str(x) => {
                c.kind = "string";
                c.s = Some(x.clone());
            }
            other => {
                return Err(DbError::Api(format!(
                    "scalar kind {other:?} has no v1 template literal form"
                )))
            }
        }
        Ok(c)
    }
}

fn insert_list_member(
    conn: &Connection,
    c: i64,
    list_id: i64,
    m: &PlannedListMember,
) -> Result<(), DbError> {
    let vc = ScalarCols::of(&m.value)?;
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
    warnings: &mut Vec<String>,
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
            let (id, semantic_link, version): (i64, Option<String>, i64) = conn.query_row(
                "SELECT id, semantic_link, version FROM units_of_measure \
                 WHERE cur_name = lower(?)",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            conn.execute(
                "UPDATE units_of_measure SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
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
            for m in members {
                insert_list_member(conn, c, id, m)?;
            }
        }
        Action::UpdateValueList { name, description } => {
            let (id, version): (i64, i64) = conn.query_row(
                "SELECT id, version FROM parameter_value_list WHERE cur_name = lower(?)",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            conn.execute(
                "UPDATE parameter_value_list SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO parameter_value_list (id, name, description, \
                    valid_from_commit, version) VALUES (?, ?, ?, ?, ?)",
                params![id, name, description, c, version + 1],
            )?;
        }
        Action::AddListMember { list, member } => {
            let lid = list_id(conn, list)?;
            insert_list_member(conn, c, lid, member)?;
        }
        Action::UpdateListMember { list, kind, value, description } => {
            let lid = list_id(conn, list)?;
            let vc = ScalarCols::of(value)?;
            let row = conn.query_row(
                "SELECT id, ordinal, semantic_link, version FROM list_value \
                 WHERE cur_l = ? AND value_kind = ? AND valid_to_commit IS NULL \
                   AND ((? = 'float' AND v_float = ?) OR (? = 'int' AND v_int = ?) \
                     OR (? = 'bool' AND v_bool = ?) OR (? = 'string' AND v_str = ?))",
                params![lid, kind, kind, vc.f, kind, vc.i, kind, vc.b, kind, vc.s],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )?;
            let (id, ordinal, semantic_link, version) = row;
            conn.execute(
                "UPDATE list_value SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO list_value \
                   (id, parameter_value_list_id, ordinal, description, semantic_link, \
                    value_kind, v_float, v_int, v_bool, v_str, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    id, lid, ordinal, description, semantic_link, vc.kind, vc.f, vc.i, vc.b,
                    vc.s, c, version + 1
                ],
            )?;
        }
        Action::CreateClass { name, description, active_by_default, required, display_order } => {
            conn.execute(
                "INSERT INTO entity_class \
                   (name, description, active_by_default, required, display_order, \
                    valid_from_commit) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![name, description, active_by_default, required, display_order, c],
            )?;
        }
        Action::UpdateClass { name, description, active_by_default, required, display_order } => {
            let (id, version): (i64, i64) = conn.query_row(
                "SELECT id, version FROM entity_class WHERE cur_name = lower(?)",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            conn.execute(
                "UPDATE entity_class SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO entity_class \
                   (id, name, description, active_by_default, required, display_order, \
                    valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![id, name, description, active_by_default, required, display_order, c,
                        version + 1],
            )?;
        }
        Action::CreateClassDims { class, dims } => {
            let cid = class_id(conn, class)?;
            for d in dims {
                let (dc, ds) = if d.is_set {
                    (None, Some(set_id(conn, &d.referent)?))
                } else {
                    (Some(class_id(conn, &d.referent)?), None)
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
            conn.execute(
                "INSERT INTO class_set (name, description, valid_from_commit) VALUES (?, ?, ?)",
                params![name, description, c],
            )?;
        }
        Action::UpdateClassSet { name, description } => {
            let (id, version): (i64, i64) = conn.query_row(
                "SELECT id, version FROM class_set WHERE cur_name = lower(?)",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            conn.execute(
                "UPDATE class_set SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO class_set (id, name, description, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?)",
                params![id, name, description, c, version + 1],
            )?;
        }
        Action::AddClassSetMember { set, member } => {
            let sid = set_id(conn, set)?;
            let mid = class_id(conn, member)?;
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
            let cid = class_id(conn, class)?;
            let lid = value_list.as_deref().map(|l| list_id(conn, l)).transpose()?;
            let uid = unit.as_deref().map(|u| unit_id(conn, u)).transpose()?;
            let pd_id: i64 = conn.query_row(
                "INSERT INTO parameter_definition \
                   (entity_class_id, name, description, parameter_value_list_id, unit_id, \
                    required, display_order, valid_from_commit) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
                params![cid, name, description, lid, uid, required, display_order, c],
                |r| r.get(0),
            )?;
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
            let pd_id = param_id(conn, class, name)?;
            let lid = value_list.as_deref().map(|l| list_id(conn, l)).transpose()?;
            let uid = unit.as_deref().map(|u| unit_id(conn, u)).transpose()?;
            let (cid, retain, version): (i64, bool, i64) = conn.query_row(
                "SELECT entity_class_id, retain_content, version FROM parameter_definition \
                 WHERE cur_id = ?",
                params![pd_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            conn.execute(
                "UPDATE parameter_definition SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, pd_id],
            )?;
            conn.execute(
                "INSERT INTO parameter_definition \
                   (id, entity_class_id, name, description, parameter_value_list_id, \
                    unit_id, required, display_order, retain_content, valid_from_commit, \
                    version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![pd_id, cid, name, description, lid, uid, required, display_order,
                        retain, c, version + 1],
            )?;
        }
        Action::AddParameterType { class, parameter, member } => {
            let pd_id = param_id(conn, class, parameter)?;
            insert_type_member(conn, c, pd_id, member)?;
        }
        Action::NarrowParameterType { class, parameter, pt_id, describe } => {
            // §4.2: narrowing is flagged when current data instantiates the
            // removed member. Conservative: any current value bound to the
            // member — or any current value of the definition with a NULL
            // binding (pre-D11 rows whose member we cannot know) — blocks the
            // close-out with a warning, never an error.
            let pd_id = param_id(conn, class, parameter)?;
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
                warnings.push(format!(
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
            let pd_id = param_id(conn, class, parameter)?;
            let pt_id = rank0_member_id(conn, pd_id, kind)?;
            write_definition_default(conn, c, pd_id, value, pt_id)?;
        }
        Action::ClearDefault { class, parameter } => {
            let pd_id = param_id(conn, class, parameter)?;
            close_definition_default(conn, c, pd_id)?;
        }
        Action::CreateSeedEntity { class, name, description, elements } => {
            let cid = class_id(conn, class)?;
            let eid: i64 = conn.query_row(
                "INSERT INTO entity (entity_class_id, name, description, valid_from_commit) \
                 VALUES (?, ?, ?, ?) RETURNING id",
                params![cid, name, description, c],
                |r| r.get(0),
            )?;
            for (i, el) in elements.iter().enumerate() {
                let pos = i as i64 + 1;
                let el_id = resolve_element(conn, cid, pos, el)?;
                conn.execute(
                    "INSERT INTO entity_element \
                       (entity_id, position, element_entity_id, valid_from_commit) \
                     VALUES (?, ?, ?, ?)",
                    params![eid, pos, el_id, c],
                )?;
            }
        }
        Action::CreateSeedMembership { group_class, group, member_class, member } => {
            let gc = class_id(conn, group_class)?;
            let mc = class_id(conn, member_class)?;
            let gid = entity_id_in_class(conn, gc, group)?.ok_or_else(|| {
                DbError::Api(format!("seed membership: entity {group_class}:{group} not found"))
            })?;
            let mid = entity_id_in_class(conn, mc, member)?.ok_or_else(|| {
                DbError::Api(format!("seed membership: entity {member_class}:{member} not found"))
            })?;
            conn.execute(
                "INSERT INTO membership \
                   (group_entity_id, member_entity_id, alternative_id, included, \
                    valid_from_commit) \
                 VALUES (?, ?, NULL, TRUE, ?)",
                params![gid, mid, c],
            )?;
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
            set_metadata(conn, c, "template_content_hash", content_hash)?;
        }
    }
    Ok(())
}

/// Resolve one seed element name against the dimension's declared referent —
/// a class (must hold the entity) or a class-set (exactly one member class
/// must hold it; ambiguity is an error, mirroring
/// `audit_class_set_entity_name_dup`).
fn resolve_element(
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
    if let Some(dcid) = dc {
        return entity_id_in_class(conn, dcid, element)?.ok_or_else(|| {
            DbError::Api(format!(
                "seed element {element:?} at position {position}: no such entity in the \
                 dimension's class"
            ))
        });
    }
    let dsid = ds.expect("dimension XOR check");
    let mut stmt = conn.prepare(
        "SELECT e.id FROM entity e \
         JOIN class_set_member m ON m.cur_c = e.cur_cls \
         WHERE m.cur_s = ? AND e.cur_name = lower(?)",
    )?;
    let ids: Vec<i64> = stmt
        .query_map(params![dsid, element], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    match ids.as_slice() {
        [id] => Ok(*id),
        [] => Err(DbError::Api(format!(
            "seed element {element:?} at position {position}: no entity of any member \
             class of the dimension's class_set"
        ))),
        _ => Err(DbError::Api(format!(
            "seed element {element:?} at position {position}: ambiguous — entities of \
             multiple member classes share the name (see audit_class_set_entity_name_dup)"
        ))),
    }
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

fn execute_step(conn: &Connection, c: i64, step: &MigrationStep) -> Result<(), DbError> {
    match step {
        MigrationStep::RenameClass { from, to } => {
            if !charset_ok(to) {
                return Err(DbError::Api(format!(
                    "rename_class: target {to:?} must match [A-Za-z0-9_]+"
                )));
            }
            let (id, description, abd, required, display_order, version) = conn
                .query_row(
                    "SELECT id, description, active_by_default, required, display_order, \
                            version \
                     FROM entity_class WHERE cur_name = lower(?)",
                    params![from],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Option<String>>(1)?,
                            r.get::<_, bool>(2)?,
                            r.get::<_, bool>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, i64>(5)?,
                        ))
                    },
                )
                .map_err(|e| step_lookup_err(e, "rename_class", from))?;
            if fold(from) != fold(to) {
                let taken: i64 = conn.query_row(
                    "SELECT count(*) FROM entity_class WHERE cur_name = lower(?)",
                    params![to],
                    |r| r.get(0),
                )?;
                if taken > 0 {
                    return Err(DbError::Api(format!(
                        "rename_class: target name {to:?} already exists"
                    )));
                }
            }
            conn.execute(
                "UPDATE entity_class SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO entity_class \
                   (id, name, description, active_by_default, required, display_order, \
                    valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![id, to, description, abd, required, display_order, c, version + 1],
            )?;
        }
        MigrationStep::RenameParameter { class, from, to } => {
            if !charset_ok(to) {
                return Err(DbError::Api(format!(
                    "rename_parameter: target {to:?} must match [A-Za-z0-9_]+"
                )));
            }
            let cid = class_id(conn, class)
                .map_err(|_| DbError::Api(format!("rename_parameter: class {class:?} not found")))?;
            let (id, description, lid, uid, required, display_order, retain, version) = conn
                .query_row(
                    "SELECT id, description, parameter_value_list_id, unit_id, required, \
                            display_order, retain_content, version \
                     FROM parameter_definition WHERE cur_cls = ? AND cur_name = lower(?)",
                    params![cid, from],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Option<String>>(1)?,
                            r.get::<_, Option<i64>>(2)?,
                            r.get::<_, Option<i64>>(3)?,
                            r.get::<_, bool>(4)?,
                            r.get::<_, Option<i64>>(5)?,
                            r.get::<_, bool>(6)?,
                            r.get::<_, i64>(7)?,
                        ))
                    },
                )
                .map_err(|e| step_lookup_err(e, "rename_parameter", from))?;
            if fold(from) != fold(to) {
                let taken: i64 = conn.query_row(
                    "SELECT count(*) FROM parameter_definition \
                     WHERE cur_cls = ? AND cur_name = lower(?)",
                    params![cid, to],
                    |r| r.get(0),
                )?;
                if taken > 0 {
                    return Err(DbError::Api(format!(
                        "rename_parameter: {class}.{to} already exists"
                    )));
                }
            }
            conn.execute(
                "UPDATE parameter_definition SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO parameter_definition \
                   (id, entity_class_id, name, description, parameter_value_list_id, \
                    unit_id, required, display_order, retain_content, valid_from_commit, \
                    version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![id, cid, to, description, lid, uid, required, display_order, retain,
                        c, version + 1],
            )?;
        }
        MigrationStep::RenameValueList { from, to } => {
            let (id, description, version) = conn
                .query_row(
                    "SELECT id, description, version FROM parameter_value_list \
                     WHERE cur_name = lower(?)",
                    params![from],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, Option<String>>(1)?,
                            r.get::<_, i64>(2)?,
                        ))
                    },
                )
                .map_err(|e| step_lookup_err(e, "rename_value_list", from))?;
            if fold(from) != fold(to) {
                let taken: i64 = conn.query_row(
                    "SELECT count(*) FROM parameter_value_list WHERE cur_name = lower(?)",
                    params![to],
                    |r| r.get(0),
                )?;
                if taken > 0 {
                    return Err(DbError::Api(format!(
                        "rename_value_list: target name {to:?} already exists"
                    )));
                }
            }
            conn.execute(
                "UPDATE parameter_value_list SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO parameter_value_list (id, name, description, \
                    valid_from_commit, version) VALUES (?, ?, ?, ?, ?)",
                params![id, to, description, c, version + 1],
            )?;
        }
        MigrationStep::RenameListMember { list, from, to, rewrite_values } => {
            let lid = list_id(conn, list)
                .map_err(|_| DbError::Api(format!("rename_list_member: list {list:?} not found")))?;
            let (id, ordinal, description, semantic_link, version) = conn
                .query_row(
                    "SELECT id, ordinal, description, semantic_link, version FROM list_value \
                     WHERE cur_l = ? AND value_kind = 'string' AND v_str = ? \
                       AND valid_to_commit IS NULL",
                    params![lid, from],
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, i64>(1)?,
                            r.get::<_, Option<String>>(2)?,
                            r.get::<_, Option<String>>(3)?,
                            r.get::<_, i64>(4)?,
                        ))
                    },
                )
                .map_err(|e| step_lookup_err(e, "rename_list_member", from))?;
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
            conn.execute(
                "UPDATE list_value SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, id],
            )?;
            conn.execute(
                "INSERT INTO list_value \
                   (id, parameter_value_list_id, ordinal, description, semantic_link, \
                    value_kind, v_str, valid_from_commit, version) \
                 VALUES (?, ?, ?, ?, ?, 'string', ?, ?, ?)",
                params![id, lid, ordinal, description, semantic_link, to, c, version + 1],
            )?;
            if *rewrite_values {
                // The one data-touching hint (§4.2 Layer 2): mechanically
                // rewrite matching CURRENT stored strings of definitions
                // bound to this list — proper close-out + re-insert, so
                // history keeps the pre-rename values.
                conn.execute(
                    "UPDATE value_scalar SET valid_to_commit = ?1 \
                     WHERE valid_to_commit IS NULL AND value_kind = 'string' AND v_str = ?2 \
                       AND parameter_definition_id IN \
                         (SELECT id FROM parameter_definition \
                          WHERE valid_to_commit IS NULL AND parameter_value_list_id = ?3)",
                    params![c, from, lid],
                )?;
                conn.execute(
                    "INSERT INTO value_scalar \
                       (entity_id, parameter_definition_id, alternative_id, \
                        parameter_type_id, valid_from_commit, version, content_retained, \
                        value_kind, v_str) \
                     SELECT entity_id, parameter_definition_id, alternative_id, \
                            parameter_type_id, ?1, version + 1, TRUE, 'string', ?2 \
                     FROM value_scalar \
                     WHERE valid_to_commit = ?1 AND value_kind = 'string' AND v_str = ?3 \
                       AND parameter_definition_id IN \
                         (SELECT id FROM parameter_definition \
                          WHERE valid_to_commit IS NULL AND parameter_value_list_id = ?4)",
                    params![c, to, from, lid],
                )?;
                // Defaults/prefills mirror (the §3.3.4 defaults leg).
                conn.execute(
                    "UPDATE definition_value_scalar SET valid_to_commit = ?1 \
                     WHERE valid_to_commit IS NULL AND value_kind = 'string' AND v_str = ?2 \
                       AND parameter_definition_id IN \
                         (SELECT id FROM parameter_definition \
                          WHERE valid_to_commit IS NULL AND parameter_value_list_id = ?3)",
                    params![c, from, lid],
                )?;
                conn.execute(
                    "INSERT INTO definition_value_scalar \
                       (parameter_definition_id, slot, parameter_type_id, \
                        valid_from_commit, version, content_retained, value_kind, v_str) \
                     SELECT parameter_definition_id, slot, parameter_type_id, ?1, \
                            version + 1, TRUE, 'string', ?2 \
                     FROM definition_value_scalar \
                     WHERE valid_to_commit = ?1 AND value_kind = 'string' AND v_str = ?3 \
                       AND parameter_definition_id IN \
                         (SELECT id FROM parameter_definition \
                          WHERE valid_to_commit IS NULL AND parameter_value_list_id = ?4)",
                    params![c, to, from, lid],
                )?;
            }
        }
        MigrationStep::MoveParameter { name, from_class, to_class } => {
            let from_cid = class_id(conn, from_class).map_err(|_| {
                DbError::Api(format!("move_parameter: class {from_class:?} not found"))
            })?;
            let to_cid = class_id(conn, to_class).map_err(|_| {
                DbError::Api(format!("move_parameter: class {to_class:?} not found"))
            })?;
            let pd_id = conn
                .query_row(
                    "SELECT id FROM parameter_definition WHERE cur_cls = ? \
                     AND cur_name = lower(?)",
                    params![from_cid, name],
                    |r| r.get::<_, i64>(0),
                )
                .map_err(|e| step_lookup_err(e, "move_parameter", name))?;
            let taken: i64 = conn.query_row(
                "SELECT count(*) FROM parameter_definition WHERE cur_cls = ? \
                 AND cur_name = lower(?)",
                params![to_cid, name],
                |r| r.get(0),
            )?;
            if taken > 0 {
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
            let (description, lid, uid, required, display_order, retain, version) = conn
                .query_row(
                    "SELECT description, parameter_value_list_id, unit_id, required, \
                            display_order, retain_content, version \
                     FROM parameter_definition WHERE cur_id = ?",
                    params![pd_id],
                    |r| {
                        Ok((
                            r.get::<_, Option<String>>(0)?,
                            r.get::<_, Option<i64>>(1)?,
                            r.get::<_, Option<i64>>(2)?,
                            r.get::<_, bool>(3)?,
                            r.get::<_, Option<i64>>(4)?,
                            r.get::<_, bool>(5)?,
                            r.get::<_, i64>(6)?,
                        ))
                    },
                )?;
            conn.execute(
                "UPDATE parameter_definition SET valid_to_commit = ? WHERE id = ? \
                 AND valid_to_commit IS NULL",
                params![c, pd_id],
            )?;
            conn.execute(
                "INSERT INTO parameter_definition \
                   (id, entity_class_id, name, description, parameter_value_list_id, \
                    unit_id, required, display_order, retain_content, valid_from_commit, \
                    version) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![pd_id, to_cid, name, description, lid, uid, required, display_order,
                        retain, c, version + 1],
            )?;
        }
        MigrationStep::Run(_) => unreachable!("run steps execute through the registry"),
    }
    Ok(())
}

fn step_lookup_err(e: duckdb::Error, step: &str, name: &str) -> DbError {
    match e {
        duckdb::Error::QueryReturnedNoRows => {
            DbError::Api(format!("{step}: {name:?} not found (is the chain ordered right?)"))
        }
        e => e.into(),
    }
}
