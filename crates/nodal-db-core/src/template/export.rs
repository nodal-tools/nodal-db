//! `template export` (§8.1.7): re-emit the IR from a loaded database. The
//! exported document and the source template must be **semantically equal
//! after normalization** — parse both, compare [`super::ir::normalized_model`]
//! outputs (the same normalization that feeds `template_content_hash`).
//! Textual equality is NOT the contract (anchors are parse-time, ordering is
//! normalized).
//!
//! Scope notes (v1, flagged in the phase report):
//! - **Seed block = every current entity + base membership.** The store does
//!   not mark which entities were vocabulary seed vs user data, so export of
//!   a *populated* DB pulls user entities into the `seed:` block; on a
//!   freshly loaded DB (the round-trip acceptance case) the two coincide
//!   exactly. A dedicated seed marker is a possible later door.
//! - `migrations:` is not persisted (chain transport, not vocabulary state) —
//!   it is absent from exports and excluded from normalization.
//! - Non-scalar defaults and `date_time`/`duration` literals have no v1 IR
//!   form; they surface as export warnings, never silent drops.

use std::path::Path;

use serde_json::Value;

use crate::conn::with_read;
use crate::error::DbError;
use crate::types::ScalarValue;

use super::dbvocab::{read_vocab, DbVocab};
use super::ir::{
    AuditRules, Class, ClassSet, DefaultValue, DimRef, DimSpec, EntityRef, Identity, ListMember,
    Lit, Parameter, ParameterGroup, Seed, SeedEntity, SeedMembership, Template, TypeDecl, Unit,
    ValueList,
};

fn fold(s: &str) -> String {
    s.to_lowercase()
}

fn lit_of(v: &ScalarValue) -> Option<(Lit, &'static str)> {
    match v {
        ScalarValue::Float(f) => Some((Lit::Float(*f), "float")),
        ScalarValue::Int(i) => Some((Lit::Int(*i), "int")),
        ScalarValue::Bool(b) => Some((Lit::Bool(*b), "bool")),
        ScalarValue::Str(s) => Some((Lit::Str(s.clone()), "string")),
        _ => None,
    }
}

/// Export the database's current vocabulary as a [`Template`], plus warnings
/// for anything the v1 IR cannot represent. One per-operation RO connection.
pub fn export(db: &Path) -> Result<(Template, Vec<String>), DbError> {
    let mut warnings = Vec::new();
    let vocab = with_read(db, |conn| read_vocab(conn, &mut warnings))?;
    let t = template_of(&vocab, &mut warnings)?;
    Ok((t, warnings))
}

fn template_of(v: &DbVocab, warnings: &mut Vec<String>) -> Result<Template, DbError> {
    let name = v.template_name.clone().unwrap_or_else(|| {
        warnings.push(
            "database has no template_name stamped; exporting as \"unnamed\"".to_string(),
        );
        "unnamed".to_string()
    });
    let version = v.template_version.clone().unwrap_or_else(|| {
        warnings.push(
            "database has no template_version stamped; exporting as \"0\"".to_string(),
        );
        "0".to_string()
    });

    // Presentation document -> groups + per-definition group refs (D8).
    let mut parameter_groups: Vec<ParameterGroup> = Vec::new();
    let mut group_of: std::collections::HashMap<String, String> = Default::default();
    if let Some(Value::Object(doc)) = &v.presentation {
        if let Some(Value::Array(gs)) = doc.get("parameter_groups") {
            for g in gs {
                let (Some(name), color, order) = (
                    g.get("name").and_then(|x| x.as_str()),
                    g.get("color").and_then(|x| x.as_str()),
                    g.get("order").and_then(|x| x.as_i64()),
                ) else {
                    warnings.push(format!("presentation: malformed parameter_group entry {g}"));
                    continue;
                };
                parameter_groups.push(ParameterGroup {
                    name: name.to_string(),
                    color: color.map(|s| s.to_string()),
                    order,
                });
            }
        }
        if let Some(Value::Object(m)) = doc.get("parameter_group_membership") {
            for (k, val) in m {
                if let Some(g) = val.as_str() {
                    group_of.insert(fold(k), g.to_string());
                }
            }
        }
    }

    // Units.
    let mut units: Vec<Unit> = v
        .units
        .values()
        .map(|u| Unit {
            name: u.name.clone(),
            description: u.description.clone(),
            quantity_kind: u.quantity_kind.clone(),
            registry_extras: Default::default(),
        })
        .collect();
    units.sort_by_key(|u| fold(&u.name));

    // Value lists (members in ordinal order; DbVocab already warned about
    // unrepresentable members).
    let mut value_lists: Vec<ValueList> = v
        .value_lists
        .values()
        .map(|l| ValueList {
            name: l.name.clone(),
            description: l.description.clone(),
            members: l
                .members
                .iter()
                .filter_map(|m| {
                    let (lit, kind) = lit_of(&m.value)?;
                    Some(ListMember {
                        value: lit,
                        // Emit the kind only where inference would differ
                        // (int members would infer int; float stays implicit
                        // only when the literal is a float — normalization
                        // reconciles either spelling, so keep it minimal
                        // and always explicit when kinds could ambiguate).
                        kind: if m.value_kind != kind {
                            Some(m.value_kind.clone())
                        } else {
                            None
                        },
                        description: m.description.clone(),
                    })
                })
                .collect(),
        })
        .collect();
    value_lists.sort_by_key(|l| fold(&l.name));

    // Class sets.
    let mut class_sets: Vec<ClassSet> = v
        .class_sets
        .values()
        .map(|s| {
            let mut members = s.members.clone();
            members.sort_by_key(|m| fold(m));
            ClassSet {
                name: s.name.clone(),
                description: s.description.clone(),
                members,
            }
        })
        .collect();
    class_sets.sort_by_key(|s| fold(&s.name));

    // Classes + parameters.
    let mut classes: Vec<Class> = Vec::new();
    for c in v.classes.values() {
        let mut parameters: Vec<Parameter> = Vec::new();
        for p in c.params.values() {
            // Type set: emit explicit `types:` unless the declaration is
            // exactly what value-list inference would reproduce (a single
            // rank-0 member of the list's uniform kind) — keeps round-trips
            // of list-typed parameters free of redundant `types:` blocks.
            let mut types: Option<Vec<TypeDecl>> = if p.types.is_empty() {
                None
            } else {
                Some(
                    p.types
                        .iter()
                        .map(|m| TypeDecl {
                            kind: m.value_kind.clone(),
                            dims: if m.dims.is_empty() {
                                None
                            } else {
                                Some(
                                    m.dims
                                        .iter()
                                        .map(|(n, k)| DimSpec {
                                            name: n.clone(),
                                            kind: k.clone(),
                                        })
                                        .collect(),
                                )
                            },
                        })
                        .collect(),
                )
            };
            if let (Some(list_name), Some(ts)) = (&p.value_list, &types) {
                if ts.len() == 1 && ts[0].dims.is_none() {
                    if let Some(l) = v.value_lists.get(&fold(list_name)) {
                        let kinds: std::collections::HashSet<&str> =
                            l.members.iter().map(|m| m.value_kind.as_str()).collect();
                        if l.unrepresentable_members == 0
                            && kinds.len() == 1
                            && kinds.contains(ts[0].kind.as_str())
                        {
                            types = None; // inference reproduces it (§3.5)
                        }
                    }
                }
            }
            let default = match &p.default {
                None => None,
                Some(d) => match lit_of(&d.value) {
                    Some((lit, _)) => Some(DefaultValue { kind: d.value_kind.clone(), value: lit }),
                    None => {
                        warnings.push(format!(
                            "parameter {}.{}: default kind {:?} has no v1 IR literal form — \
                             omitted from the export",
                            c.name, p.name, d.value_kind
                        ));
                        None
                    }
                },
            };
            parameters.push(Parameter {
                name: p.name.clone(),
                description: p.description.clone(),
                types,
                value_list: p.value_list.clone(),
                default,
                unit: p.unit.clone(),
                group: group_of.get(&fold(&format!("{}.{}", c.name, p.name))).cloned(),
                required: p.required,
                display_order: p.display_order,
            });
        }
        parameters.sort_by_key(|p| fold(&p.name));
        classes.push(Class {
            name: c.name.clone(),
            description: c.description.clone(),
            active_by_default: c.active_by_default,
            required: c.required,
            display_order: c.display_order,
            dimensions: c
                .dims
                .iter()
                .map(|d| {
                    if d.is_set() {
                        DimRef::Set { class_set: d.referent_name().to_string() }
                    } else {
                        DimRef::Class(d.referent_name().to_string())
                    }
                })
                .collect(),
            parameters,
        });
    }
    classes.sort_by_key(|c| fold(&c.name));

    // Audit rules document.
    let audit_rules: Option<AuditRules> = match &v.audit_rules {
        None => None,
        Some(doc) => match serde_json::from_str::<AuditRules>(doc) {
            Ok(r) => Some(r),
            Err(e) => {
                warnings.push(format!(
                    "audit_rules metadata document does not parse as the IR block ({e}); \
                     omitted from the export"
                ));
                None
            }
        },
    };

    // Seed: every current entity + base membership (module docs).
    let mut entities: Vec<SeedEntity> = v
        .entities
        .values()
        .map(|e| SeedEntity {
            class: e.class.clone(),
            name: Some(e.name.clone()),
            elements: if e.elements.is_empty() {
                None
            } else {
                Some(e.elements.clone())
            },
            description: e.description.clone(),
        })
        .collect();
    entities.sort_by_key(|e| (fold(&e.class), fold(&e.effective_name())));
    let mut memberships: Vec<SeedMembership> = v
        .memberships
        .iter()
        .map(|m| SeedMembership {
            group: EntityRef { class: m.group_class.clone(), entity: m.group.clone() },
            member: EntityRef { class: m.member_class.clone(), entity: m.member.clone() },
        })
        .collect();
    memberships.sort_by_key(|m| {
        (
            fold(&m.group.class),
            fold(&m.group.entity),
            fold(&m.member.class),
            fold(&m.member.entity),
        )
    });
    let seed = if entities.is_empty() && memberships.is_empty() {
        None
    } else {
        Some(Seed { entities, memberships })
    };

    Ok(Template {
        template: Identity {
            name,
            version,
            description: None,
            engine_schema: None,
            role: None,
        },
        units,
        value_lists,
        parameter_groups,
        class_sets,
        classes,
        audit_rules,
        seed,
        migrations: Vec::new(),
    })
}
