//! The sanctioned structural (non-value) write path — the fourth
//! `commit_changes` channel (structural_write_api_plan §2/§4/§5).
//!
//! Every table these appliers touch (`alternative`, `scenario`,
//! `scenario_alternative`, `entity_alternative`, `membership`, and — via the
//! `Create*` ops — `entity_class`, `entity`, `parameter_definition`) has ONE
//! writer site (invariant 2): the appliers here are the conn-taking bodies both
//! the standalone G8 verbs (their own-commit wrappers with an idempotent read
//! pre-check) AND this composable channel call. No table gets a second writer;
//! the history/typing rules live in exactly one place.
//!
//! History discipline (VMP Part 5, invariant 3): append-only close-out — create
//! is a fresh row, rename is close + same-id successor (version+1), ranking is
//! A-min contiguous `1..=n` minimal-diff, activation/membership is the per-key
//! close-out protocol. The `valid_from == commit_id` **same-commit guard** edits
//! a row created THIS commit in place (rename/flag) or physically DELETEs it (a
//! close), never closing it (would trip the span CHECK). An uncommitted insert
//! was never visible, so editing/deleting it is not a history rewrite (D-S5).
//!
//! Referent resolution is **strict** (lookup-only, hard error naming a miss,
//! §4.4) — the only auto-creates are the `Create*` ops and the scenario
//! auto-create inside `SetScenarioAlternatives`.

use std::collections::{HashMap, HashSet};

use duckdb::{params, Connection};

use crate::api::{
    find_alternative, find_entity, find_entity_class, find_parameter, find_scenario,
    find_value_list, insert_alternative_row, lookup_alternative, lookup_entity_class,
    lookup_parameter, lookup_scenario,
};
use crate::error::{DbError, TemplateStampedRenameDetails, TemplateStampedRenameItem};
use crate::object_delete::find_element;
use crate::template::dbvocab::read_metadata_key;
use crate::template::ir::{TypeDecl, DIM_KINDS, SCALAR_KINDS};
use crate::template::reconcile::{
    insert_class_dim_row, insert_class_row, insert_definition_row, insert_entity_element_row,
    insert_entity_row, insert_membership_row, insert_scenario_row, insert_type_member, read_ec_row,
    read_pd_row, revise_alternative_row, revise_ec_row, revise_pd_row, revise_scenario_row,
};
use crate::text::NameFolds;
use crate::types::{
    charset_ok, fold, EntityAddr, ObjectDeleteKey, ObjectDeleteOpts, ObjectKind, ObjectRef,
    StructuralChange, StructuralEffect, StructuralKind, StructuralOutcome, WriteReport,
};

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// Apply one [`StructuralChange`] under the open transaction's `commit_id`,
/// appending its [`StructuralOutcome`] to `report.structural`. Items apply
/// sequentially against the one open txn (§4.1), so item N's inserts are visible
/// to item N+1's strict lookups (create → activate resolves in one save).
///
/// Every reference to an existing object (an idempotent create's own name
/// included, a rename's new name excluded) is noted in `folds` for the
/// call's `NameFolded` warnings; every rename is recorded in `calls`.
pub(crate) fn apply_structural(
    conn: &Connection,
    c: i64,
    change_index: usize,
    change: &StructuralChange,
    report: &mut WriteReport,
    folds: &mut NameFolds,
    calls: &mut CallRenames,
) -> Result<(), DbError> {
    let (kind, object_id, effect) = match change {
        StructuralChange::CreateAlternative { name, description } => {
            let (id, e) = apply_create_alternative(conn, c, name, description.as_deref(), folds)?;
            (StructuralKind::Alternative, id, e)
        }
        StructuralChange::RenameAlternative {
            current,
            new_name,
            description,
        } => {
            let (id, e) = apply_rename_alternative(
                conn,
                c,
                current,
                new_name,
                description.as_deref(),
                folds,
                calls,
            )?;
            (StructuralKind::Alternative, id, e)
        }
        StructuralChange::CreateScenario { name, description } => {
            let (id, e) = apply_create_scenario(conn, c, name, description.as_deref(), folds)?;
            (StructuralKind::Scenario, id, e)
        }
        StructuralChange::RenameScenario {
            current,
            new_name,
            description,
        } => {
            let (id, e) = apply_rename_scenario(
                conn,
                c,
                current,
                new_name,
                description.as_deref(),
                folds,
                calls,
            )?;
            (StructuralKind::Scenario, id, e)
        }
        StructuralChange::SetScenarioAlternatives {
            scenario,
            alternatives,
        } => {
            let (id, e) =
                apply_set_scenario_alternatives(conn, c, scenario, alternatives, folds, calls)?;
            (StructuralKind::ScenarioRanking, id, e)
        }
        StructuralChange::SetEntityActivation {
            class,
            entity,
            alternative,
            active,
        } => {
            let (id, e) =
                apply_set_entity_activation(conn, c, class, entity, alternative, *active, folds)?;
            (StructuralKind::Activation, id, e)
        }
        StructuralChange::SetMembership {
            group,
            member,
            alternative,
            included,
        } => {
            let (id, e) = apply_set_membership(
                conn,
                c,
                group,
                member,
                alternative.as_deref(),
                *included,
                folds,
            )?;
            (StructuralKind::Membership, id, e)
        }
        StructuralChange::CreateEntity {
            class,
            name,
            elements,
        } => {
            let (id, e) = apply_create_entity(conn, c, class, name.as_deref(), elements, folds)?;
            (StructuralKind::Entity, id, e)
        }
        StructuralChange::CreateEntityClass {
            name,
            dims,
            description,
        } => {
            let (id, e) =
                apply_create_entity_class(conn, c, name, dims, description.as_deref(), folds)?;
            (StructuralKind::EntityClass, id, e)
        }
        StructuralChange::RenameEntityClass {
            current,
            new_name,
            description,
            allow_on_template,
        } => {
            let r = VocabRename {
                change_index,
                current,
                new_name,
                description: description.as_deref(),
                allow_on_template: *allow_on_template,
            };
            let (id, e) = apply_rename_entity_class(conn, c, &r, folds, calls)?;
            (StructuralKind::EntityClass, id, e)
        }
        StructuralChange::RenameParameter {
            class,
            current,
            new_name,
            description,
            allow_on_template,
        } => {
            let r = VocabRename {
                change_index,
                current,
                new_name,
                description: description.as_deref(),
                allow_on_template: *allow_on_template,
            };
            let (id, e) = apply_rename_parameter(conn, c, class, &r, folds, calls)?;
            (StructuralKind::Parameter, id, e)
        }
        StructuralChange::CreateParameter {
            class,
            name,
            types,
            value_list,
            description,
        } => {
            let (id, e) = apply_create_parameter(
                conn,
                c,
                class,
                name,
                types,
                value_list.as_deref(),
                description.as_deref(),
                folds,
            )?;
            (StructuralKind::Parameter, id, e)
        }
    };
    report.structural.push(StructuralOutcome {
        change_index,
        kind,
        object_id,
        effect,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Alternatives + scenarios (identity rows)
// ---------------------------------------------------------------------------

pub(crate) fn apply_create_alternative(
    conn: &Connection,
    c: i64,
    name: &str,
    description: Option<&str>,
    folds: &mut NameFolds,
) -> Result<(i64, StructuralEffect), DbError> {
    if let Some(f) = find_alternative(conn, name)? {
        // idempotent
        return Ok((f.noted(folds, "alternative", name), StructuralEffect::NoOp));
    }
    let id = insert_alternative_row(conn, c, name, description)?;
    Ok((id, StructuralEffect::Created))
}

pub(crate) fn apply_create_scenario(
    conn: &Connection,
    c: i64,
    name: &str,
    description: Option<&str>,
    folds: &mut NameFolds,
) -> Result<(i64, StructuralEffect), DbError> {
    if let Some(f) = find_scenario(conn, name)? {
        return Ok((f.noted(folds, "scenario", name), StructuralEffect::NoOp));
    }
    let id = insert_scenario_row(conn, c, name, description)?;
    Ok((id, StructuralEffect::Created))
}

struct IdentityRow {
    name: String,
    description: Option<String>,
    version: i64,
    valid_from: i64,
}

fn read_identity(conn: &Connection, table: &str, id: i64) -> Result<IdentityRow, DbError> {
    Ok(conn.query_row(
        &format!(
            "SELECT name, description, version, valid_from_commit FROM {table} WHERE cur_id = ?"
        ),
        params![id],
        |r| {
            Ok(IdentityRow {
                name: r.get(0)?,
                description: r.get(1)?,
                version: r.get(2)?,
                valid_from: r.get(3)?,
            })
        },
    )?)
}

// ---------------------------------------------------------------------------
// Per-call rename book-keeping (class_param_rename_PLAN §4.5 b/f, §5.2, §6.1)
// ---------------------------------------------------------------------------

/// What the renames of one `commit_changes` attempt did, for the checks and
/// writes that follow them in the same call. A closure local of the write
/// transaction (rebuilt on a conflict retry, like the write buffers), so no
/// id from a rolled-back attempt is reused.
#[derive(Default)]
pub(crate) struct CallRenames {
    /// Renamed-away names, `fold(old)` -> (id, current name), for renames
    /// whose fold changed (a case-only rename frees no name).
    pub(crate) alternatives: HashMap<String, (i64, String)>,
    pub(crate) scenarios: HashMap<String, (i64, String)>,
    pub(crate) classes: HashMap<String, (i64, String)>,
    /// `(class id, fold(old))` -> (definition id, current name).
    pub(crate) parameters: HashMap<(i64, String), (i64, String)>,
    /// Class id -> its name before the call's first rename of it, for every
    /// class whose name bytes a rename of this call changed (the metadata
    /// document rewrite, §5.2).
    pub(crate) class_original: HashMap<i64, String>,
    /// Definition id -> (class id, its name before the call's first rename
    /// of it).
    pub(crate) parameter_original: HashMap<i64, (i64, String)>,
    /// Class and parameter renames that changed a folded name without
    /// `allow_on_template` (the template-stamp check, §6.1).
    pub(crate) template_candidates: Vec<TemplateCandidate>,
}

/// One rename the template-stamp check may refuse.
pub(crate) struct TemplateCandidate {
    pub(crate) change_index: usize,
    pub(crate) kind: StructuralKind,
    pub(crate) object_id: i64,
    pub(crate) class: Option<String>,
    pub(crate) current: String,
    pub(crate) new_name: String,
}

impl CallRenames {
    /// Nothing renamed away: the reference checks have nothing to do.
    pub(crate) fn frees_no_name(&self) -> bool {
        self.alternatives.is_empty()
            && self.scenarios.is_empty()
            && self.classes.is_empty()
            && self.parameters.is_empty()
    }

    /// The current name of the class `name` was renamed away from in this
    /// call, when `name` no longer resolves (a same-call create of the old
    /// name makes it resolve again, to the new object).
    pub(crate) fn renamed_class(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<Option<String>, DbError> {
        match self.classes.get(&fold(name)) {
            Some((_, to)) if lookup_entity_class(conn, name)?.is_none() => Ok(Some(to.clone())),
            _ => Ok(None),
        }
    }

    /// As [`CallRenames::renamed_class`], for parameter `name` of the class
    /// `class` resolves to now.
    pub(crate) fn renamed_parameter(
        &self,
        conn: &Connection,
        class: &str,
        name: &str,
    ) -> Result<Option<String>, DbError> {
        if self.parameters.is_empty() {
            return Ok(None);
        }
        let Some(cid) = lookup_entity_class(conn, class)? else {
            return Ok(None);
        };
        match self.parameters.get(&(cid, fold(name))) {
            Some((_, to)) if lookup_parameter(conn, cid, name)?.is_none() => Ok(Some(to.clone())),
            _ => Ok(None),
        }
    }

    pub(crate) fn renamed_alternative(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<Option<String>, DbError> {
        match self.alternatives.get(&fold(name)) {
            Some((_, to)) if lookup_alternative(conn, name)?.is_none() => Ok(Some(to.clone())),
            _ => Ok(None),
        }
    }

    pub(crate) fn renamed_scenario(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<Option<String>, DbError> {
        match self.scenarios.get(&fold(name)) {
            Some((_, to)) if lookup_scenario(conn, name)?.is_none() => Ok(Some(to.clone())),
            _ => Ok(None),
        }
    }
}

/// The template-stamp check (class_param_rename_PLAN OD7, §6.1), after the
/// structural changes ran: refuse every class or parameter rename of the
/// call that changed a folded name without `allow_on_template`, of an
/// object that existed before the call (one this call created cannot be
/// template-declared), when the database is stamped with a template. Reads
/// `template_name` only when there is something to refuse.
pub(crate) fn refuse_template_stamped_renames(
    conn: &Connection,
    calls: &CallRenames,
    report: &WriteReport,
) -> Result<(), DbError> {
    if calls.template_candidates.is_empty() {
        return Ok(());
    }
    let created: HashSet<(StructuralKind, i64)> = report
        .structural
        .iter()
        .filter(|o| o.effect == StructuralEffect::Created)
        .map(|o| (o.kind, o.object_id))
        .collect();
    let renames: Vec<TemplateStampedRenameItem> = calls
        .template_candidates
        .iter()
        .filter(|t| !created.contains(&(t.kind, t.object_id)))
        .map(|t| TemplateStampedRenameItem {
            change_index: t.change_index,
            kind: t.kind,
            class: t.class.clone(),
            current: t.current.clone(),
            new_name: t.new_name.clone(),
        })
        .collect();
    if renames.is_empty() {
        return Ok(());
    }
    let Some(template_name) = read_metadata_key(conn, "template_name")? else {
        return Ok(());
    };
    let template_version = read_metadata_key(conn, "template_version")?;
    let items: Vec<String> = renames
        .iter()
        .map(|r| match &r.class {
            None => format!(
                "change {}: class {:?} -> {:?}",
                r.change_index, r.current, r.new_name
            ),
            Some(class) => format!(
                "change {}: parameter {class}.{} -> {:?}",
                r.change_index, r.current, r.new_name
            ),
        })
        .collect();
    let message = format!(
        "the database is stamped with template {template_name:?}{}: renaming its classes \
         or parameters needs allow_on_template on each rename. The core cannot tell which \
         ones the template declares, and a renamed declared class or parameter blocks the \
         next apply of a changed template until it is renamed back or the template ships a \
         rename hint. Refused (nothing was written): {}",
        template_version
            .as_deref()
            .map(|v| format!(" (version {v:?})"))
            .unwrap_or_default(),
        items.join("; ")
    );
    Err(DbError::TemplateStampedRename(Box::new(
        TemplateStampedRenameDetails {
            template_name,
            template_version,
            renames,
            message,
        },
    )))
}

/// Record that object `id` was renamed from `old` to `new` in `away`
/// (keyed by `key(fold(old))`), keeping every earlier entry of the same id
/// pointing at its newest name (a chain `a -> b -> c` leaves `a` and `b`
/// both renamed to `c`). A case-only rename frees no name.
fn note_renamed_away<K: std::hash::Hash + Eq>(
    away: &mut HashMap<K, (i64, String)>,
    key: K,
    id: i64,
    old: &str,
    new: &str,
) {
    for (oid, to) in away.values_mut() {
        if *oid == id {
            *to = new.to_string();
        }
    }
    if fold(old) != fold(new) {
        away.insert(key, (id, new.to_string()));
    }
}

/// The in-call swap guard (B2): a rename may not take a name an earlier
/// rename of the same call freed. Otherwise `a -> t`, `b -> a`, `t -> b`
/// would make both old names resolve again, to each other's objects, and a
/// later reference by an old name would silently address the other object.
fn refuse_freed_name(
    what: &str,
    current: &str,
    new_name: &str,
    stored: &str,
    freed: Option<&(i64, String)>,
) -> Result<(), DbError> {
    if fold(stored) == fold(new_name) {
        return Ok(()); // case-only: takes no other name
    }
    match freed {
        Some((_, to)) => Err(DbError::Api(format!(
            "rename_{what}: {current:?} -> {new_name:?}: the name {new_name:?} was renamed \
             away (to {to:?}) earlier in this call; a name freed in a call cannot be taken \
             in the same call (in-call swaps and round trips are refused): rename in two \
             commits"
        ))),
        None => Ok(()),
    }
}

/// An identity table's one revision site (`revise_alternative_row` /
/// `revise_scenario_row`: `(conn, c, id, name, description, version,
/// valid_from)`), which holds the same-commit in-place branch (D-S5).
type Revise = fn(&Connection, i64, i64, &str, Option<&str>, i64, i64) -> Result<(), DbError>;
type Lookup = fn(&Connection, &str) -> Result<Option<i64>, DbError>;

/// Versioned identity rename (close + same-id successor, version+1), with the
/// same-commit in-place edit (D-S5), the in-call swap guard (B2) and the
/// self-excluding uniqueness check (m5). Records the rename in `away`.
#[allow(clippy::too_many_arguments)]
fn rename_identity(
    conn: &Connection,
    c: i64,
    table: &str,
    id: i64,
    current: &str,
    new_name: &str,
    description: Option<&str>,
    lookup: Lookup,
    revise: Revise,
    away: &mut HashMap<String, (i64, String)>,
) -> Result<StructuralEffect, DbError> {
    let row = read_identity(conn, table, id)?;
    refuse_freed_name(
        table,
        current,
        new_name,
        &row.name,
        away.get(&fold(new_name)),
    )?;
    // Uniqueness: `new_name` must not collide with a DIFFERENT live object; the
    // self row (by id) is excluded, so a pure case-restyle is not a false
    // self-collision (critique m5). A clean error, never a raw uq_* violation.
    if let Some(other) = lookup(conn, new_name)? {
        if other != id {
            return Err(DbError::Api(format!(
                "rename {table}: {new_name:?} already names a different {table}"
            )));
        }
    }
    let Some(rev) = decide_revision(&row.name, row.description.as_deref(), new_name, description)
    else {
        return Ok(StructuralEffect::NoOp);
    };
    revise(
        conn,
        c,
        id,
        new_name,
        rev.description.as_deref(),
        row.version,
        row.valid_from,
    )?;
    if rev.effect == StructuralEffect::Renamed {
        note_renamed_away(away, fold(&row.name), id, &row.name, new_name);
    }
    Ok(rev.effect)
}

/// What one rename or description revision does to its row
/// (class_param_rename_PLAN §4.4): the row's description after it, and the
/// effect to report. `None` from [`decide_revision`] is a `NoOp`.
pub(crate) struct Revision {
    pub(crate) description: Option<String>,
    pub(crate) effect: StructuralEffect,
}

/// The one decision step of the four rename ops (alternative, scenario,
/// entity class, parameter definition).
///
/// A rename that differs only in case or Unicode form IS a real rename
/// (editor request R4, owner decision 2026-10-27: the client decides
/// whether case matters, not the core). `new_name` arrives already NFC
/// (`nfc_structural_names`, the text rule's API-entry-point normalization)
/// and the stored `row_name` is NFC too, so a plain byte comparison is
/// exactly "byte-identical post-NFC" — no Rust-side fold here: the fold
/// belongs only in the uniqueness lookup (self-excluded by id).
///
/// The description (OD3, S6): `None` keeps the stored one, `Some("")`
/// clears it (a stored description is never the empty string), `Some(d)`
/// sets it. The clear is mapped BEFORE the comparison, so clearing an
/// absent description is no change. Effect (OD4): `Renamed` when the name
/// bytes change, `Updated` for a description-only revision.
pub(crate) fn decide_revision(
    row_name: &str,
    row_desc: Option<&str>,
    new_name: &str,
    description: Option<&str>,
) -> Option<Revision> {
    let desired = match description {
        None => row_desc,
        Some(d) => crate::text::stored_description(Some(d)),
    };
    let name_same = row_name == new_name;
    let desc_change = desired != row_desc;
    if name_same && !desc_change {
        return None;
    }
    Some(Revision {
        description: desired.map(str::to_string),
        effect: if name_same {
            StructuralEffect::Updated
        } else {
            StructuralEffect::Renamed
        },
    })
}

pub(crate) fn apply_rename_alternative(
    conn: &Connection,
    c: i64,
    current: &str,
    new_name: &str,
    description: Option<&str>,
    folds: &mut NameFolds,
    calls: &mut CallRenames,
) -> Result<(i64, StructuralEffect), DbError> {
    let id = find_alternative(conn, current)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "rename_alternative: alternative {current:?} does not exist"
            ))
        })?
        .noted(folds, "alternative", current);
    let e = rename_identity(
        conn,
        c,
        "alternative",
        id,
        current,
        new_name,
        description,
        lookup_alternative,
        revise_alternative_row,
        &mut calls.alternatives,
    )?;
    Ok((id, e))
}

pub(crate) fn apply_rename_scenario(
    conn: &Connection,
    c: i64,
    current: &str,
    new_name: &str,
    description: Option<&str>,
    folds: &mut NameFolds,
    calls: &mut CallRenames,
) -> Result<(i64, StructuralEffect), DbError> {
    let id = find_scenario(conn, current)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "rename_scenario: scenario {current:?} does not exist"
            ))
        })?
        .noted(folds, "scenario", current);
    let e = rename_identity(
        conn,
        c,
        "scenario",
        id,
        current,
        new_name,
        description,
        lookup_scenario,
        revise_scenario_row,
        &mut calls.scenarios,
    )?;
    Ok((id, e))
}

/// One `RenameEntityClass` / `RenameParameter` item, as written.
pub(crate) struct VocabRename<'a> {
    pub(crate) change_index: usize,
    pub(crate) current: &'a str,
    pub(crate) new_name: &'a str,
    pub(crate) description: Option<&'a str>,
    pub(crate) allow_on_template: bool,
}

/// `RenameEntityClass` (class_param_rename_PLAN §4.2): strict resolution,
/// the swap guard, whole-database uniqueness (self excluded by id), the
/// shared decision step, then the one `entity_class` revision site, which
/// carries every other column and holds the D-S5 in-place branch. Nothing
/// else is written: every reference to a class is by id.
pub(crate) fn apply_rename_entity_class(
    conn: &Connection,
    c: i64,
    r: &VocabRename<'_>,
    folds: &mut NameFolds,
    calls: &mut CallRenames,
) -> Result<(i64, StructuralEffect), DbError> {
    let (current, new_name) = (r.current, r.new_name);
    let id = find_entity_class(conn, current)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "rename_entity_class: class {current:?} does not exist"
            ))
        })?
        .noted(folds, "entity_class", current);
    let mut row = read_ec_row(conn, id)?;
    refuse_freed_name(
        "entity_class",
        current,
        new_name,
        &row.name,
        calls.classes.get(&fold(new_name)),
    )?;
    if let Some(other) = lookup_entity_class(conn, new_name)? {
        if other != id {
            return Err(DbError::Api(format!(
                "rename_entity_class: {new_name:?} already names a different class"
            )));
        }
    }
    let Some(rev) = decide_revision(
        &row.name,
        row.description.as_deref(),
        new_name,
        r.description,
    ) else {
        return Ok((id, StructuralEffect::NoOp));
    };
    let old = std::mem::replace(&mut row.name, new_name.to_string());
    row.description = rev.description;
    revise_ec_row(conn, c, &row)?;
    if rev.effect == StructuralEffect::Renamed {
        note_renamed_away(&mut calls.classes, fold(&old), id, &old, new_name);
        calls
            .class_original
            .entry(id)
            .or_insert_with(|| old.clone());
        if fold(&old) != fold(new_name) && !r.allow_on_template {
            calls.template_candidates.push(TemplateCandidate {
                change_index: r.change_index,
                kind: StructuralKind::EntityClass,
                object_id: id,
                class: None,
                current: current.to_string(),
                new_name: new_name.to_string(),
            });
        }
    }
    Ok((id, rev.effect))
}

/// `RenameParameter` (class_param_rename_PLAN §4.3): as
/// [`apply_rename_entity_class`], for one definition of `class` (resolved
/// strictly by the name it has at this point of the call); names are unique
/// per class. The one `parameter_definition` revision site carries the
/// value list, unit, flags and order; values, defaults and type members
/// key on the id and are untouched.
pub(crate) fn apply_rename_parameter(
    conn: &Connection,
    c: i64,
    class: &str,
    r: &VocabRename<'_>,
    folds: &mut NameFolds,
    calls: &mut CallRenames,
) -> Result<(i64, StructuralEffect), DbError> {
    let (current, new_name) = (r.current, r.new_name);
    let cid = find_entity_class(conn, class)?
        .ok_or_else(|| DbError::Api(format!("rename_parameter: class {class:?} not found")))?
        .noted(folds, "entity_class", class);
    let id = find_parameter(conn, cid, current)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "rename_parameter: parameter {class}.{current} does not exist"
            ))
        })?
        .noted(folds, "parameter_definition", current);
    let mut row = read_pd_row(conn, id)?;
    refuse_freed_name(
        "parameter",
        current,
        new_name,
        &row.name,
        calls.parameters.get(&(cid, fold(new_name))),
    )?;
    if let Some(other) = lookup_parameter(conn, cid, new_name)? {
        if other != id {
            return Err(DbError::Api(format!(
                "rename_parameter: {new_name:?} already names a different parameter of \
                 class {class:?}"
            )));
        }
    }
    let Some(rev) = decide_revision(
        &row.name,
        row.description.as_deref(),
        new_name,
        r.description,
    ) else {
        return Ok((id, StructuralEffect::NoOp));
    };
    let old = std::mem::replace(&mut row.name, new_name.to_string());
    row.description = rev.description;
    revise_pd_row(conn, c, &row)?;
    if rev.effect == StructuralEffect::Renamed {
        note_renamed_away(&mut calls.parameters, (cid, fold(&old)), id, &old, new_name);
        calls
            .parameter_original
            .entry(id)
            .or_insert_with(|| (cid, old.clone()));
        if fold(&old) != fold(new_name) && !r.allow_on_template {
            calls.template_candidates.push(TemplateCandidate {
                change_index: r.change_index,
                kind: StructuralKind::Parameter,
                object_id: id,
                class: Some(class.to_string()),
                current: current.to_string(),
                new_name: new_name.to_string(),
            });
        }
    }
    Ok((id, rev.effect))
}

// ---------------------------------------------------------------------------
// Scenario ranking — A-min (§4.3, critique F1: normatively two-phase)
// ---------------------------------------------------------------------------

pub(crate) fn apply_set_scenario_alternatives(
    conn: &Connection,
    c: i64,
    scenario: &str,
    alternatives: &[String],
    folds: &mut NameFolds,
    calls: &CallRenames,
) -> Result<(i64, StructuralEffect), DbError> {
    // Strict resolution of every alternative BEFORE touching anything.
    let mut alt_ids: Vec<i64> = Vec::with_capacity(alternatives.len());
    for a in alternatives {
        let id = find_alternative(conn, a)?
            .ok_or_else(|| {
                DbError::Api(format!(
                    "set_scenario_alternatives: alternative {a:?} does not exist (create it first)"
                ))
            })?
            .noted(folds, "alternative", a);
        alt_ids.push(id);
    }
    // Resolve or auto-create the scenario (the one sanctioned auto-create, §4.4).
    let (sid, scenario_created) = match find_scenario(conn, scenario)? {
        Some(f) => (f.noted(folds, "scenario", scenario), false),
        None => {
            // Never auto-create a name an earlier rename of this call freed:
            // the caller means the renamed scenario, and a new empty one
            // would fork it (class_param_rename_PLAN S4).
            if let Some(to) = calls.renamed_scenario(conn, scenario)? {
                return Err(DbError::Api(format!(
                    "set_scenario_alternatives: scenario {scenario:?} was renamed to {to:?} \
                     earlier in this call; address it by its new name"
                )));
            }
            (insert_scenario_row(conn, c, scenario, None)?, true)
        }
    };

    // Current ranking: alternative_id -> (rank, version).
    let mut stmt = conn.prepare(
        "SELECT alternative_id, rank, version FROM scenario_alternative \
         WHERE cur_s = ? ORDER BY cur_r",
    )?;
    let cur: Vec<(i64, i64, i64)> = stmt
        .query_map(params![sid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<_, _>>()?;
    drop(stmt);
    let cur_map: HashMap<i64, (i64, i64)> = cur.iter().map(|(a, r, v)| (*a, (*r, *v))).collect();
    let desired: HashMap<i64, i64> = alt_ids
        .iter()
        .enumerate()
        .map(|(i, a)| (*a, (i + 1) as i64))
        .collect();

    // Classify: changed ∪ removed (close), changed ∪ added (insert).
    let mut to_close: Vec<i64> = Vec::new();
    for (aid, (rank, _)) in &cur_map {
        match desired.get(aid) {
            Some(d) if *d == *rank => {} // unchanged — never touched
            _ => to_close.push(*aid),    // changed or removed
        }
    }
    let mut to_insert: Vec<(i64, i64, i64)> = Vec::new(); // (aid, rank, version)
    for (i, aid) in alt_ids.iter().enumerate() {
        let rank = (i + 1) as i64;
        match cur_map.get(aid) {
            Some((cur_rank, ver)) => {
                if *cur_rank != rank {
                    to_insert.push((*aid, rank, ver + 1)); // changed: version carries +1
                }
            }
            None => to_insert.push((*aid, rank, 1)), // added: starts at 1
        }
    }

    if to_close.is_empty() && to_insert.is_empty() {
        // Nothing to write. If we just created the scenario (empty list on an
        // absent scenario), that create IS the effect; otherwise a true no-op.
        let effect = if scenario_created {
            StructuralEffect::Created
        } else {
            StructuralEffect::NoOp
        };
        return Ok((sid, effect));
    }

    // Phase 1 — close ALL of (changed ∪ removed) in ONE guarded UPDATE, BEFORE
    // any insert (F1). This vacates exactly the complementary rank-set of the
    // untouched rows, so every phase-2 insert hits a free rank — collision-free.
    if !to_close.is_empty() {
        let placeholders = vec!["?"; to_close.len()].join(", ");
        let sql = format!(
            "UPDATE scenario_alternative SET valid_to_commit = ? \
             WHERE cur_s = ? AND cur_a IN ({placeholders}) AND valid_from_commit < ?"
        );
        let mut binds: Vec<i64> = Vec::with_capacity(to_close.len() + 3);
        binds.push(c);
        binds.push(sid);
        binds.extend_from_slice(&to_close);
        binds.push(c);
        conn.execute(&sql, duckdb::params_from_iter(binds.iter()))?;
    }
    // Phase 2 — insert (changed ∪ added) at their desired ranks.
    for (aid, rank, version) in &to_insert {
        conn.execute(
            "INSERT INTO scenario_alternative \
               (scenario_id, alternative_id, rank, valid_from_commit, version) \
             VALUES (?, ?, ?, ?, ?)",
            params![sid, aid, rank, c, version],
        )?;
    }

    let effect = if to_insert.is_empty() {
        StructuralEffect::Cleared // desired empty / everything removed
    } else {
        StructuralEffect::Updated
    };
    Ok((sid, effect))
}

// ---------------------------------------------------------------------------
// Entity activation — per-key close-out protocol + same-commit guard (m4)
// ---------------------------------------------------------------------------

pub(crate) fn apply_set_entity_activation(
    conn: &Connection,
    c: i64,
    class: &str,
    entity: &str,
    alternative: &str,
    active: Option<bool>,
    folds: &mut NameFolds,
) -> Result<(i64, StructuralEffect), DbError> {
    let cid = find_entity_class(conn, class)?
        .ok_or_else(|| DbError::Api(format!("set_entity_activation: class {class:?} not found")))?
        .noted(folds, "entity_class", class);
    let entity_id = find_entity(conn, cid, entity)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "set_entity_activation: entity {class}:{entity} not found"
            ))
        })?
        .noted(folds, "entity", entity);
    let alt_id = find_alternative(conn, alternative)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "set_entity_activation: alternative {alternative:?} not found"
            ))
        })?
        .noted(folds, "alternative", alternative);
    // (active, version, valid_from) of the current row, if any.
    let current = opt_row3(
        conn,
        "SELECT active, version, valid_from_commit FROM entity_alternative \
         WHERE cur_e = ? AND cur_a = ?",
        entity_id,
        alt_id,
    )?;
    let effect = match (active, current) {
        (None, None) => StructuralEffect::NoOp,
        (Some(b), Some((cur, _, _))) if cur == b => StructuralEffect::NoOp,
        (None, Some((_, _, vf))) => {
            if vf == c {
                // Created this commit: DELETE the uncommitted insert (D-S5).
                conn.execute(
                    "DELETE FROM entity_alternative \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit = ?",
                    params![entity_id, alt_id, c],
                )?;
            } else {
                conn.execute(
                    "UPDATE entity_alternative SET valid_to_commit = ? \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit < ?",
                    params![c, entity_id, alt_id, c],
                )?;
            }
            StructuralEffect::Cleared
        }
        (Some(b), Some((_, v, vf))) => {
            if vf == c {
                // Created this commit: edit `active` in place, no version bump.
                conn.execute(
                    "UPDATE entity_alternative SET active = ? \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit = ?",
                    params![b, entity_id, alt_id, c],
                )?;
            } else {
                conn.execute(
                    "UPDATE entity_alternative SET valid_to_commit = ? \
                     WHERE cur_e = ? AND cur_a = ? AND valid_from_commit < ?",
                    params![c, entity_id, alt_id, c],
                )?;
                insert_activation_row(conn, c, entity_id, alt_id, b, v + 1)?;
            }
            StructuralEffect::Updated
        }
        (Some(b), None) => {
            insert_activation_row(conn, c, entity_id, alt_id, b, 1)?;
            StructuralEffect::Updated
        }
    };
    Ok((entity_id, effect))
}

fn insert_activation_row(
    conn: &Connection,
    c: i64,
    entity_id: i64,
    alt_id: i64,
    active: bool,
    version: i64,
) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO entity_alternative \
           (entity_id, alternative_id, active, valid_from_commit, version) \
         VALUES (?, ?, ?, ?, ?)",
        params![entity_id, alt_id, active, c, version],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Membership — activation mirror with the base-row sentinel spelled out (M4)
// ---------------------------------------------------------------------------

pub(crate) fn apply_set_membership(
    conn: &Connection,
    c: i64,
    group: &EntityAddr,
    member: &EntityAddr,
    alternative: Option<&str>,
    included: Option<bool>,
    folds: &mut NameFolds,
) -> Result<(i64, StructuralEffect), DbError> {
    let gid = resolve_entity_addr(conn, group, "group", folds)?;
    let mid = resolve_entity_addr(conn, member, "member", folds)?;
    let alt_id: Option<i64> = match alternative {
        Some(a) => Some(
            find_alternative(conn, a)?
                .ok_or_else(|| {
                    DbError::Api(format!("set_membership: alternative {a:?} not found"))
                })?
                .noted(folds, "alternative", a),
        ),
        None => None,
    };
    // Base rows join `one_current_membership` on cur_a = coalesce(alt, 0), so a
    // base lookup/close keys on cur_a = 0 (NEVER `alternative_id = ?` bound to
    // NULL, which matches nothing); an insert stores NULL, never 0 (M4).
    let cur_a: i64 = alt_id.unwrap_or(0);
    let current = membership_current(conn, gid, mid, cur_a)?;
    let effect = match (included, current) {
        (None, None) => StructuralEffect::NoOp,
        (Some(b), Some((cur, _, _))) if cur == b => StructuralEffect::NoOp,
        (None, Some((_, _, vf))) => {
            if vf == c {
                conn.execute(
                    "DELETE FROM membership \
                     WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit = ?",
                    params![gid, mid, cur_a, c],
                )?;
            } else {
                conn.execute(
                    "UPDATE membership SET valid_to_commit = ? \
                     WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit < ?",
                    params![c, gid, mid, cur_a, c],
                )?;
            }
            StructuralEffect::Cleared
        }
        (Some(b), prev) => {
            match prev {
                Some((_, _, vf)) if vf == c => {
                    // Created this commit: edit `included` in place (D-S5).
                    conn.execute(
                        "UPDATE membership SET included = ? \
                         WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit = ?",
                        params![b, gid, mid, cur_a, c],
                    )?;
                }
                Some((_, v, _)) => {
                    conn.execute(
                        "UPDATE membership SET valid_to_commit = ? \
                         WHERE cur_g = ? AND cur_m = ? AND cur_a = ? AND valid_from_commit < ?",
                        params![c, gid, mid, cur_a, c],
                    )?;
                    insert_membership_row(conn, c, gid, mid, alt_id, b, v + 1)?;
                }
                None => insert_membership_row(conn, c, gid, mid, alt_id, b, 1)?,
            }
            StructuralEffect::Updated
        }
    };
    let object_id = if matches!(effect, StructuralEffect::Cleared) {
        0
    } else {
        gid
    };
    Ok((object_id, effect))
}

fn membership_current(
    conn: &Connection,
    gid: i64,
    mid: i64,
    cur_a: i64,
) -> Result<Option<(bool, i64, i64)>, DbError> {
    match conn.query_row(
        "SELECT included, version, valid_from_commit FROM membership \
         WHERE cur_g = ? AND cur_m = ? AND cur_a = ?",
        params![gid, mid, cur_a],
        |r| {
            Ok((
                r.get::<_, bool>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        },
    ) {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn resolve_entity_addr(
    conn: &Connection,
    addr: &EntityAddr,
    role: &str,
    folds: &mut NameFolds,
) -> Result<i64, DbError> {
    let cid = find_entity_class(conn, &addr.class)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "set_membership: {role} class {:?} not found",
                addr.class
            ))
        })?
        .noted(folds, "entity_class", &addr.class);
    Ok(find_entity(conn, cid, &addr.name)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "set_membership: {role} entity {}:{} not found",
                addr.class, addr.name
            ))
        })?
        .noted(folds, "entity", &addr.name))
}

/// A `(bool, i64, i64)` current-row read keyed on two ids (activation), or
/// `None` for no current row.
fn opt_row3(
    conn: &Connection,
    sql: &str,
    a: i64,
    b: i64,
) -> Result<Option<(bool, i64, i64)>, DbError> {
    match conn.query_row(sql, params![a, b], |r| {
        Ok((
            r.get::<_, bool>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    }) {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

// ---------------------------------------------------------------------------
// D-S9 creates — the ensure_* verb txn bodies, shared (critique F4)
// ---------------------------------------------------------------------------

/// Declared dimension class names of `cid`, in position order (`<class_set>` for
/// a class-set dimension). Shared by [`apply_create_entity_class`] and the verb's
/// read pre-check.
pub(crate) fn class_dims_of(conn: &Connection, cid: i64) -> Result<Vec<String>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT COALESCE((SELECT lower(ec.name) FROM entity_class ec \
                          WHERE ec.cur_id = d.dimension_class_id), '<class_set>') \
         FROM entity_class_dimension d WHERE d.cur_c = ? ORDER BY d.cur_p",
    )?;
    let rows = stmt
        .query_map(params![cid], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// An EXISTING class matches iff its declared dims equal `dims` (folded, in
/// order); otherwise a hard error — the deferred reshape door (the v25/v58 shape).
pub(crate) fn check_class_dims(
    conn: &Connection,
    name: &str,
    cid: i64,
    dims: &[String],
) -> Result<i64, DbError> {
    let have = class_dims_of(conn, cid)?;
    let want: Vec<String> = dims.iter().map(|d| fold(d)).collect();
    if have == want {
        Ok(cid)
    } else {
        Err(DbError::Api(format!(
            "ensure_entity_class: {name:?} already exists with dimensions [{}], not [{}] — \
             in-place re-dimensioning (reshape) is not supported; create a new class and \
             migrate the data",
            have.join(", "),
            want.join(", ")
        )))
    }
}

pub(crate) fn apply_create_entity_class(
    conn: &Connection,
    c: i64,
    name: &str,
    dims: &[String],
    description: Option<&str>,
    folds: &mut NameFolds,
) -> Result<(i64, StructuralEffect), DbError> {
    if let Some(f) = find_entity_class(conn, name)? {
        let cid = f.noted(folds, "entity_class", name);
        return Ok((
            check_class_dims(conn, name, cid, dims)?,
            StructuralEffect::NoOp,
        ));
    }
    // Resolve every dimension class BEFORE inserting anything.
    let mut dim_ids: Vec<i64> = Vec::with_capacity(dims.len());
    for d in dims {
        let id = find_entity_class(conn, d)?
            .ok_or_else(|| {
                DbError::Api(format!(
                    "ensure_entity_class: dimension class {d:?} does not exist (create it first)"
                ))
            })?
            .noted(folds, "entity_class", d);
        dim_ids.push(id);
    }
    let cid = insert_class_row(conn, c, name, description, true, false, None)?;
    for (i, did) in dim_ids.iter().enumerate() {
        insert_class_dim_row(conn, c, cid, (i + 1) as i64, Some(*did), None)?;
    }
    Ok((cid, StructuralEffect::Created))
}

/// The class shape (`cid` + ordered `(position, dim_class, dim_set)`), shared by
/// [`apply_create_entity`] and the verb's read pre-check.
pub(crate) struct ClassShape {
    pub(crate) cid: i64,
    pub(crate) dims: Vec<(i64, Option<i64>, Option<i64>)>,
}

pub(crate) fn entity_class_shape(conn: &Connection, class: &str) -> Result<ClassShape, DbError> {
    entity_class_shape_noted(conn, class, &mut NameFolds::default())
}

/// [`entity_class_shape`], noting the class reference in `folds`.
fn entity_class_shape_noted(
    conn: &Connection,
    class: &str,
    folds: &mut NameFolds,
) -> Result<ClassShape, DbError> {
    let cid = find_entity_class(conn, class)?
        .ok_or_else(|| DbError::Api(format!("ensure_entity: class {class:?} not found")))?
        .noted(folds, "entity_class", class);
    let mut stmt = conn.prepare(
        "SELECT cur_p, dimension_class_id, dimension_class_set_id \
         FROM entity_class_dimension WHERE cur_c = ? ORDER BY cur_p",
    )?;
    let dims = stmt
        .query_map(params![cid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<_, _>>()?;
    Ok(ClassShape { cid, dims })
}

/// Verify an EXISTING entity's live element tuple equals `elements` (folded, in
/// order) when a tuple is given; a mismatch is a hard error (never a silent
/// alias). Shared by [`apply_create_entity`] and the verb's read pre-check.
pub(crate) fn verify_entity_elements(
    conn: &Connection,
    class: &str,
    name: &str,
    eid: i64,
    elements: &[String],
) -> Result<(), DbError> {
    if elements.is_empty() {
        return Ok(());
    }
    let mut stmt = conn.prepare(
        "SELECT lower(e.name) FROM entity_element el \
         JOIN entity e ON e.cur_id = el.element_entity_id \
         WHERE el.cur_e = ? ORDER BY el.cur_p",
    )?;
    let have: Vec<String> = stmt
        .query_map(params![eid], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    let want: Vec<String> = elements.iter().map(|e| fold(e)).collect();
    if have == want {
        Ok(())
    } else {
        Err(DbError::Api(format!(
            "ensure_entity: {class}:{name} already exists with elements [{}], not [{}]",
            have.join(", "),
            want.join(", ")
        )))
    }
}

/// The effective entity name: the given `name`, else the elements joined with
/// `__` (N-D), else an error (a 0-D entity needs a name).
pub(crate) fn effective_entity_name(
    name: Option<&str>,
    elements: &[String],
) -> Result<String, DbError> {
    match name {
        Some(n) => Ok(n.to_string()),
        None if !elements.is_empty() => Ok(elements.join("__")),
        None => Err(DbError::Api(
            "ensure_entity: a 0-D entity needs a name (N-D may derive it from elements)".into(),
        )),
    }
}

pub(crate) fn apply_create_entity(
    conn: &Connection,
    c: i64,
    class: &str,
    name: Option<&str>,
    elements: &[String],
    folds: &mut NameFolds,
) -> Result<(i64, StructuralEffect), DbError> {
    let effective_name = effective_entity_name(name, elements)?;
    let s = entity_class_shape_noted(conn, class, folds)?;
    if let Some(f) = find_entity(conn, s.cid, &effective_name)? {
        let eid = f.noted(folds, "entity", &effective_name);
        verify_entity_elements(conn, class, &effective_name, eid, elements)?;
        return Ok((eid, StructuralEffect::NoOp));
    }
    if s.dims.is_empty() {
        if !elements.is_empty() {
            return Err(DbError::Api(format!(
                "ensure_entity: class {class:?} is 0-D but elements were given"
            )));
        }
    } else if elements.len() != s.dims.len() {
        return Err(DbError::Api(format!(
            "ensure_entity: class {class:?} is {}-D; got {} element(s)",
            s.dims.len(),
            elements.len()
        )));
    }
    // Resolve every element BEFORE inserting anything (strict, §4.4).
    let mut el_ids: Vec<(i64, i64)> = Vec::with_capacity(elements.len());
    for ((pos, dc, ds), el) in s.dims.iter().zip(elements.iter()) {
        let id = find_element(conn, *dc, *ds, el)?
            .ok_or_else(|| {
                DbError::Api(format!(
                    "ensure_entity: element {el:?} at position {pos} does not name an entity of \
                     the dimension's class"
                ))
            })?
            .noted(folds, "entity", el);
        el_ids.push((*pos, id));
    }
    let eid = insert_entity_row(conn, c, s.cid, &effective_name, None)?;
    for (pos, el_id) in &el_ids {
        insert_entity_element_row(conn, c, eid, *pos, *el_id)?;
    }
    Ok((eid, StructuralEffect::Created))
}

/// Pure member-declaration validation (the load-time subset of `ir::validate`):
/// legal kinds, D3 dim rules, rank cap, pairwise distinguishability (D2a).
/// Shared by the G8 verb's pre-section and [`prepare_structural`] (`what` names
/// the calling op in messages).
pub(crate) fn validate_type_decls(
    what: &str,
    class: &str,
    name: &str,
    types: &[TypeDecl],
) -> Result<(), DbError> {
    for (ti, ty) in types.iter().enumerate() {
        let loc = format!("{what} {class}.{name} types[{ti}]");
        match ty.kind.as_str() {
            "indexed" => match ty.dims.as_deref() {
                None | Some([]) => {
                    return Err(DbError::Api(format!(
                        "{loc}: kind \"indexed\" requires a non-empty dims list"
                    )))
                }
                Some(dims) => {
                    if dims.len() > 4 {
                        return Err(DbError::Api(format!(
                            "{loc}: {} dims exceeds the rank cap of 4",
                            dims.len()
                        )));
                    }
                    for d in dims {
                        if d.name.trim().is_empty() {
                            return Err(DbError::Api(format!("{loc}: empty dimension name")));
                        }
                        if !DIM_KINDS.contains(&d.kind.as_str()) {
                            return Err(DbError::Api(format!(
                                "{loc}: unknown dimension kind {:?} (expected one of {DIM_KINDS:?})",
                                d.kind
                            )));
                        }
                    }
                }
            },
            k if SCALAR_KINDS.contains(&k) => {
                if ty.dims.as_deref().map(|d| !d.is_empty()).unwrap_or(false) {
                    return Err(DbError::Api(format!(
                        "{loc}: scalar kind {k:?} must not declare dims (use kind: indexed)"
                    )));
                }
            }
            other => {
                return Err(DbError::Api(format!(
                    "{loc}: unknown kind {other:?} (expected {SCALAR_KINDS:?} or \"indexed\")"
                )))
            }
        }
    }
    let keys: Vec<_> = types.iter().map(|t| t.match_key()).collect();
    for i in 0..keys.len() {
        for j in (i + 1)..keys.len() {
            if keys[i] == keys[j] {
                return Err(DbError::Api(format!(
                    "{what} {class}.{name}: declared members {i} and {j} are indistinguishable \
                     ({}) — exactly-one-member matching would be impossible",
                    types[i].describe()
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_create_parameter(
    conn: &Connection,
    c: i64,
    class: &str,
    name: &str,
    types: &[TypeDecl],
    value_list: Option<&str>,
    description: Option<&str>,
    folds: &mut NameFolds,
) -> Result<(i64, StructuralEffect), DbError> {
    let cid = find_entity_class(conn, class)?
        .ok_or_else(|| {
            DbError::Api(format!(
                "ensure_parameter_definition: class {class:?} not found (create it first)"
            ))
        })?
        .noted(folds, "entity_class", class);
    if let Some(f) = find_parameter(conn, cid, name)? {
        // exists: as-is, never re-declared (D-F4)
        return Ok((
            f.noted(folds, "parameter_definition", name),
            StructuralEffect::NoOp,
        ));
    }
    let lid = match value_list {
        None => None,
        Some(l) => Some(
            find_value_list(conn, l)?
                .ok_or_else(|| {
                    DbError::Api(format!(
                        "ensure_parameter_definition: value_list {l:?} not found"
                    ))
                })?
                .noted(folds, "parameter_value_list", l),
        ),
    };
    // §3.5 inference: no explicit types + a bound list ⇒ the single rank-0 member
    // of the list's uniform member kind.
    let mut effective = types.to_vec();
    if effective.is_empty() {
        if let Some(lid) = lid {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT value_kind FROM list_value \
                 WHERE cur_l = ? AND valid_to_commit IS NULL ORDER BY value_kind",
            )?;
            let kinds: Vec<String> = stmt
                .query_map(params![lid], |r| r.get(0))?
                .collect::<Result<_, _>>()?;
            match kinds.as_slice() {
                [k] => effective.push(TypeDecl {
                    kind: k.clone(),
                    dims: None,
                }),
                [] => {
                    return Err(DbError::Api(format!(
                        "ensure_parameter_definition: cannot infer a kind from empty value_list \
                         {:?} (declare types explicitly)",
                        value_list.unwrap_or("")
                    )))
                }
                many => {
                    return Err(DbError::Api(format!(
                        "ensure_parameter_definition: value_list {:?} members mix kinds {many:?} \
                         — declare types explicitly",
                        value_list.unwrap_or("")
                    )))
                }
            }
        }
    }
    let pd_id = insert_definition_row(conn, c, cid, name, description, lid, None, false, None)?;
    for m in &effective {
        insert_type_member(conn, c, pd_id, m)?;
    }
    Ok((pd_id, StructuralEffect::Created))
}

// ---------------------------------------------------------------------------
// Pure prepare-time guards (§4.5) — name-level, no DB. A caller-contract
// problem is a clean DbError::Api, never a raw CHECK/index error mid-txn.
// ---------------------------------------------------------------------------

pub(crate) fn prepare_structural(
    changes: &[StructuralChange],
    object_deletes: &[(ObjectDeleteKey, ObjectDeleteOpts)],
) -> Result<(), DbError> {
    // Per-change name/charset/type validation.
    for ch in changes {
        validate_change(ch)?;
    }

    // At most one SetScenarioAlternatives per folded scenario (critique M3).
    let mut set_scenarios: HashSet<String> = HashSet::new();
    for ch in changes {
        if let StructuralChange::SetScenarioAlternatives { scenario, .. } = ch {
            if !set_scenarios.insert(fold(scenario)) {
                return Err(DbError::Api(format!(
                    "structural: two SetScenarioAlternatives target scenario {scenario:?} in one \
                     commit — one declarative set per scenario per commit"
                )));
            }
        }
    }

    // Contradictory renames: two different new_names for one object (§4.5).
    check_rename_coherence(changes, |ch| match ch {
        StructuralChange::RenameAlternative {
            current, new_name, ..
        } => Some((fold(current), current, new_name)),
        _ => None,
    })?;
    check_rename_coherence(changes, |ch| match ch {
        StructuralChange::RenameScenario {
            current, new_name, ..
        } => Some((fold(current), current, new_name)),
        _ => None,
    })?;
    check_rename_coherence(changes, |ch| match ch {
        StructuralChange::RenameEntityClass {
            current, new_name, ..
        } => Some((fold(current), current, new_name)),
        _ => None,
    })?;
    // Parameters: keyed by (class as written, parameter). A class renamed
    // earlier in the call is addressed by its new name, so two spellings of
    // one class can slip past this pure check; the in-transaction swap guard
    // and uniqueness check still refuse anything that would collide.
    check_rename_coherence(changes, |ch| match ch {
        StructuralChange::RenameParameter {
            class,
            current,
            new_name,
            ..
        } => Some((
            format!("{}\u{0}{}", fold(class), fold(current)),
            current,
            new_name,
        )),
        _ => None,
    })?;

    // Cross-channel object-delete guard (critique M2): a commit that both
    // structurally references an object AND object-deletes it by folded name
    // would leave a live orphan under the same-commit guard. That includes
    // creating it (Create*), or creating an entity, parameter or class under
    // a deleted class (the class cascade skips rows this commit created).
    // `commit_changes` repeats the create check on the resolved ids.
    let mut del_alts: HashSet<String> = HashSet::new();
    let mut del_classes: HashSet<String> = HashSet::new();
    let mut del_params: HashSet<(String, String)> = HashSet::new();
    let mut del_scens: HashSet<String> = HashSet::new();
    let mut del_entities: HashSet<(String, String)> = HashSet::new();
    for (key, _) in object_deletes {
        if let ObjectRef::Name(n) = &key.target {
            match key.kind {
                ObjectKind::Alternative => {
                    del_alts.insert(fold(n));
                }
                ObjectKind::Scenario => {
                    del_scens.insert(fold(n));
                }
                ObjectKind::Class => {
                    del_classes.insert(fold(n));
                }
                ObjectKind::Parameter => {
                    if let Some(cls) = &key.class {
                        del_params.insert((fold(cls), fold(n)));
                    }
                }
                ObjectKind::Entity => {
                    // Element-tuple-addressed (`ObjectRef::Elements`) entity
                    // deletes are intentionally NOT matched by this name-level
                    // guard — that residual self-heals in resolution (a D5-class
                    // edge, like the value-write/delete edges).
                    if let Some(cls) = &key.class {
                        del_entities.insert((fold(cls), fold(n)));
                    }
                }
            }
        }
    }
    let clash_alt = |name: &str| del_alts.contains(&fold(name));
    let clash_scen = |name: &str| del_scens.contains(&fold(name));
    let clash_entity = |class: &str, name: &str| del_entities.contains(&(fold(class), fold(name)));
    let clash_class = |name: &str| del_classes.contains(&fold(name));
    let orphan = |kind: &str, name: &str| {
        DbError::Api(format!(
            "structural: {kind} {name:?} is both structurally referenced and object-deleted in \
             one commit — the same-commit guard would leave a live orphan"
        ))
    };
    for ch in changes {
        match ch {
            StructuralChange::SetScenarioAlternatives {
                scenario,
                alternatives,
            } => {
                for a in alternatives {
                    if clash_alt(a) {
                        return Err(orphan("alternative", a));
                    }
                }
                if !alternatives.is_empty() && clash_scen(scenario) {
                    return Err(orphan("scenario", scenario));
                }
            }
            StructuralChange::SetEntityActivation {
                class,
                entity,
                alternative,
                ..
            } => {
                if clash_entity(class, entity) {
                    return Err(orphan("entity", entity));
                }
                if clash_alt(alternative) {
                    return Err(orphan("alternative", alternative));
                }
            }
            StructuralChange::SetMembership {
                group,
                member,
                alternative,
                ..
            } => {
                if clash_entity(&group.class, &group.name) {
                    return Err(orphan("entity", &group.name));
                }
                if clash_entity(&member.class, &member.name) {
                    return Err(orphan("entity", &member.name));
                }
                if let Some(a) = alternative {
                    if clash_alt(a) {
                        return Err(orphan("alternative", a));
                    }
                }
            }
            StructuralChange::RenameAlternative {
                current, new_name, ..
            } => {
                if clash_alt(current) || clash_alt(new_name) {
                    return Err(orphan("alternative", current));
                }
            }
            StructuralChange::RenameScenario {
                current, new_name, ..
            } if (clash_scen(current) || clash_scen(new_name)) => {
                return Err(orphan("scenario", current));
            }
            StructuralChange::RenameEntityClass {
                current, new_name, ..
            } => {
                if clash_class(current) || clash_class(new_name) {
                    return Err(orphan("class", current));
                }
            }
            StructuralChange::RenameParameter {
                class,
                current,
                new_name,
                ..
            } => {
                if clash_class(class) {
                    return Err(orphan("class", class));
                }
                if del_params.contains(&(fold(class), fold(current)))
                    || del_params.contains(&(fold(class), fold(new_name)))
                {
                    return Err(orphan("parameter", current));
                }
            }
            StructuralChange::CreateAlternative { name, .. } if clash_alt(name) => {
                return Err(orphan("alternative", name));
            }
            StructuralChange::CreateScenario { name, .. } if clash_scen(name) => {
                return Err(orphan("scenario", name));
            }
            StructuralChange::CreateEntityClass { name, dims, .. } => {
                if clash_class(name) {
                    return Err(orphan("class", name));
                }
                if let Some(d) = dims.iter().find(|d| clash_class(d)) {
                    return Err(orphan("class", d));
                }
            }
            StructuralChange::CreateEntity { class, name, .. } => {
                if clash_class(class) {
                    return Err(orphan("class", class));
                }
                if let Some(n) = name {
                    if clash_entity(class, n) {
                        return Err(orphan("entity", n));
                    }
                }
            }
            StructuralChange::CreateParameter { class, name, .. } => {
                if clash_class(class) {
                    return Err(orphan("class", class));
                }
                if del_params.contains(&(fold(class), fold(name))) {
                    return Err(orphan("parameter", name));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn require_nonempty(what: &str, name: &str) -> Result<(), DbError> {
    if name.trim().is_empty() {
        return Err(DbError::Api(format!(
            "structural: {what} name must be non-empty"
        )));
    }
    Ok(())
}

fn require_charset(what: &str, name: &str) -> Result<(), DbError> {
    if !charset_ok(name) {
        return Err(DbError::Api(format!(
            "structural: {what} name {name:?} must be non-empty, trimmed, and free of control characters"
        )));
    }
    Ok(())
}

fn validate_change(ch: &StructuralChange) -> Result<(), DbError> {
    match ch {
        StructuralChange::CreateAlternative { name, .. } => require_nonempty("alternative", name),
        StructuralChange::RenameAlternative {
            current, new_name, ..
        } => {
            require_nonempty("alternative", current)?;
            require_nonempty("alternative", new_name)
        }
        StructuralChange::CreateScenario { name, .. } => require_nonempty("scenario", name),
        StructuralChange::RenameScenario {
            current, new_name, ..
        } => {
            require_nonempty("scenario", current)?;
            require_nonempty("scenario", new_name)
        }
        StructuralChange::SetScenarioAlternatives {
            scenario,
            alternatives,
        } => {
            require_nonempty("scenario", scenario)?;
            let mut seen: HashSet<String> = HashSet::new();
            for a in alternatives {
                require_nonempty("alternative", a)?;
                if !seen.insert(fold(a)) {
                    return Err(DbError::Api(format!(
                        "set_scenario_alternatives: alternative {a:?} appears twice \
                         (name resolution is case-insensitive)"
                    )));
                }
            }
            Ok(())
        }
        StructuralChange::SetEntityActivation {
            class,
            entity,
            alternative,
            ..
        } => {
            require_nonempty("class", class)?;
            require_nonempty("entity", entity)?;
            require_nonempty("alternative", alternative)
        }
        StructuralChange::SetMembership {
            group,
            member,
            alternative,
            ..
        } => {
            require_nonempty("group class", &group.class)?;
            require_nonempty("group", &group.name)?;
            require_nonempty("member class", &member.class)?;
            require_nonempty("member", &member.name)?;
            if let Some(a) = alternative {
                require_nonempty("alternative", a)?;
            }
            Ok(())
        }
        StructuralChange::CreateEntity {
            class,
            name,
            elements,
        } => {
            require_nonempty("class", class)?;
            let effective = effective_entity_name(name.as_deref(), elements)?;
            require_charset("entity", &effective)?;
            for el in elements {
                require_nonempty("element", el)?;
            }
            Ok(())
        }
        StructuralChange::CreateEntityClass { name, dims, .. } => {
            require_charset("class", name)?;
            if dims.len() > 4 {
                return Err(DbError::Api(format!(
                    "create_entity_class: {} dimensions exceeds the rank cap of 4",
                    dims.len()
                )));
            }
            for d in dims {
                require_nonempty("dimension class", d)?;
            }
            Ok(())
        }
        StructuralChange::CreateParameter {
            class, name, types, ..
        } => {
            require_nonempty("class", class)?;
            require_charset("parameter", name)?;
            validate_type_decls("create_parameter", class, name, types)
        }
        // A new name carries the same hygiene floor as a created one.
        StructuralChange::RenameEntityClass {
            current, new_name, ..
        } => {
            require_nonempty("class", current)?;
            require_charset("class", new_name)
        }
        StructuralChange::RenameParameter {
            class,
            current,
            new_name,
            ..
        } => {
            require_nonempty("class", class)?;
            require_nonempty("parameter", current)?;
            require_charset("parameter", new_name)
        }
    }
}

/// Refuse two renames of one source (`pick`'s key: the folded source, for a
/// parameter with its class) to different target bytes in one call.
fn check_rename_coherence(
    changes: &[StructuralChange],
    pick: impl Fn(&StructuralChange) -> Option<(String, &String, &String)>,
) -> Result<(), DbError> {
    let mut seen: HashMap<String, String> = HashMap::new();
    for ch in changes {
        if let Some((key, current, new_name)) = pick(ch) {
            // The SOURCE key still folds: `current` only has to resolve to the
            // same object (DB lookup is case-insensitive), so "a" and "A" as
            // `current` name one rename target. The TARGET must compare by
            // exact post-NFC bytes, never fold (R4, owner decision
            // 2026-10-27, name_fold_identity_PLAN §10 K6): now that a
            // case-only or Unicode-form-only rename is itself a real rename,
            // `a -> Foo` and `a -> FOO` in one commit are two DIFFERENT
            // target spellings, not one folded value — folding here would
            // let the later one silently win (last-wins) instead of being
            // refused as contradictory. `new_name` arrives already NFC
            // (`api::nfc_structural_names`, run before `prepare_structural`),
            // so a plain byte comparison is exactly "post-NFC equality".
            let val = new_name.clone();
            match seen.get(&key) {
                Some(prev) if *prev != val => {
                    return Err(DbError::Api(format!(
                        "structural: contradictory renames of {current:?} (to {prev:?} and \
                         {new_name:?}) in one commit"
                    )));
                }
                _ => {
                    seen.insert(key, val);
                }
            }
        }
    }
    Ok(())
}
