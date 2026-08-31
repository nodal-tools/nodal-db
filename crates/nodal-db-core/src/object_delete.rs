//! Object deletion — the structural close-out path (object_delete plan §2-§4).
//!
//! An *object* delete closes out a whole object row (an alternative, and in
//! later phases an entity / parameter / class) plus its dependents, inside the
//! SAME atomic commit as any value writes/deletes ([`crate::api::commit_changes`]).
//! Everything here is driven by ONE **leg registry** ([`object_legs`]) so the
//! forward COUNTs the preview runs and the forward UPDATEs the commit runs can
//! never drift (CLAUDE.md invariant 2 — one canonicalizer, one dependency
//! finder). P1 implements the alternative leg set (§4a) and P2 the parameter
//! set (§4d), including indirect-key and filtered legs; the registry is shaped
//! so P3/P4 add entity/class legs without touching this code.
//!
//! ## The same-commit guard (§2, critique C1/C9)
//! Every close-out here — the object row, each dependent, and the array
//! move-to-hist arm — is guarded by `valid_from_commit < commit_id AND
//! valid_to_commit IS NULL`. A row CREATED in this same commit
//! (`valid_from = commit_id`) is therefore never closed: closing it would set
//! `valid_to = valid_from` and trip the span CHECK, rolling the whole txn back
//! with a raw DuckDB error. A target that the guard matches nothing for is
//! reported [`ObjectDeleteResult::NotFound`], never an error.
//!
//! ## `run` provenance is intentionally out of scope (change-model ruling 2)
//! `run.result_alternative_id` (and, for entities, `run.tool_entity_id`) is an
//! immutable append-only leg. A `run` pointing at a *closed-out* alternative is
//! coherent as-of, so it is NOT a dependent here: cascade never touches it, it
//! never blocks a `cascade:false` delete, and it is not a residual on the
//! report. The 2-line `audit_dangling_ref` fix (schema/120) keeps such a `run`
//! off the dangling-ref list.

use duckdb::{params, Connection};

use crate::error::DbError;
use crate::types::{
    Blocker, CascadeCounts, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteOutcome,
    ObjectDeletePreview, ObjectDeleteResult, ObjectKind, ObjectRef, TableCount,
};

// ---------------------------------------------------------------------------
// Leg registry (§4) — the single source of truth for every consumer.
// ---------------------------------------------------------------------------

/// One dependent/identity leg: a table + the match-predicate that selects the
/// rows referencing the target object's id. The predicate is a SQL fragment
/// with EXACTLY ONE `?` bound to the target id, so the SAME leg drives a forward
/// COUNT (preview / blocker), a guarded close-out UPDATE (cascade), and a
/// blocker `sample` SELECT — one predicate string, so those three can never
/// drift (CLAUDE.md invariant 2).
///
/// The predicate generalizes past a bare `column = ?` to cover two shapes the
/// parameter leg set (§4d) needs:
///   - **indirect key** — the target is referenced via a join, e.g.
///     `parameter_type_dimension` keys on `parameter_type_id`, whose predicate
///     is `"parameter_type_id IN (SELECT id FROM parameter_type WHERE
///     parameter_definition_id = ?)"`;
///   - **filtered leg** — a polymorphic table where only some rows count, e.g.
///     `semantic_annotation` with predicate `"object_id = ? AND
///     object_type = 'parameter_definition'"`.
///
/// A direct leg is just `"alternative_id = ?"` etc. `column` is kept purely as
/// the human-facing label for [`Blocker::column`]; it no longer drives SQL.
#[derive(Clone, Copy)]
struct DepLeg {
    /// Owning table, e.g. `"value_scalar"`.
    table: &'static str,
    /// FK-intent column label for [`Blocker::column`] (reporting only — the SQL
    /// is driven by `predicate`), e.g. `"alternative_id"`.
    column: &'static str,
    /// Match predicate with exactly one `?` bound to the target id, e.g.
    /// `"alternative_id = ?"`. Drives COUNT, sample, and close-out identically.
    predicate: &'static str,
    /// The array tier (`value_array`) has no `valid_to_commit` column — its
    /// current table holds only live rows and it closes by move-to-`_hist` +
    /// DELETE (mirroring `history::close_out_tier`). Everything else closes with
    /// `SET valid_to_commit = ?`.
    array: bool,
    /// A SQL expression producing a human-readable identifier per referencing
    /// row, for the bounded blocker `sample`.
    sample_expr: &'static str,
}

impl DepLeg {
    /// The live-row predicate for this leg (array has no validity span; its mere
    /// presence is "live").
    fn live_predicate(&self) -> &'static str {
        if self.array {
            ""
        } else {
            " AND valid_to_commit IS NULL"
        }
    }
}

/// The full close-out plan for one object kind: the target's own table (whose
/// `id` row is the object row) plus its identity and dependent legs.
///   - `identity` legs close out WITH the object, count in `object_rows`, and
///     are NEVER blockers (the object's own definition rows — e.g. an N-D
///     entity's `entity_element` tuple, a parameter's `parameter_type`). Empty
///     for an alternative.
///   - `dependent` legs are external references: cascade closes them, and a
///     `cascade:false` delete blocks on any that are live.
struct ObjectLegSet {
    object_table: &'static str,
    identity: Vec<DepLeg>,
    dependents: Vec<DepLeg>,
}

/// Build the leg set for `kind`. VERIFIED against the live schema on 2026-08-31
/// (see the module docs and the return of the P1 handoff). P1 returns the
/// alternative + parameter sets; entity/class are not wired yet (P3/P4) and surface a
/// clear "not implemented" error at [`apply_object_delete`] rather than here, so
/// the registry stays a pure data description.
fn object_legs(kind: ObjectKind) -> Option<ObjectLegSet> {
    match kind {
        // §4a — alternative, target `alternative` row by id.
        ObjectKind::Alternative => Some(ObjectLegSet {
            object_table: "alternative",
            identity: Vec::new(), // an alternative owns no identity rows
            dependents: vec![
                DepLeg {
                    table: "value_scalar",
                    column: "alternative_id",
                    predicate: "alternative_id = ?",
                    array: false,
                    sample_expr: "'entity=' || entity_id || ',pd=' || parameter_definition_id",
                },
                DepLeg {
                    table: "value_long",
                    column: "alternative_id",
                    predicate: "alternative_id = ?",
                    array: false,
                    sample_expr: "'entity=' || entity_id || ',pd=' || parameter_definition_id",
                },
                DepLeg {
                    table: "value_array",
                    column: "alternative_id",
                    predicate: "alternative_id = ?",
                    array: true,
                    sample_expr: "'entity=' || entity_id || ',pd=' || parameter_definition_id",
                },
                DepLeg {
                    table: "entity_alternative",
                    column: "alternative_id",
                    predicate: "alternative_id = ?",
                    array: false,
                    sample_expr: "'entity=' || entity_id",
                },
                // Only alternative-scoped membership rows: base rows carry
                // alternative_id IS NULL, which `alternative_id = ?` never
                // matches, so they are left untouched.
                DepLeg {
                    table: "membership",
                    column: "alternative_id",
                    predicate: "alternative_id = ?",
                    array: false,
                    sample_expr: "'group=' || group_entity_id || ',member=' || member_entity_id",
                },
                DepLeg {
                    table: "scenario_alternative",
                    column: "alternative_id",
                    predicate: "alternative_id = ?",
                    array: false,
                    sample_expr: "'scenario=' || scenario_id || ',rank=' || rank",
                },
                DepLeg {
                    table: "parameter_value_metadata",
                    column: "alternative_id",
                    predicate: "alternative_id = ?",
                    array: false,
                    sample_expr:
                        "'entity=' || entity_id || ',pd=' || parameter_definition_id",
                },
            ],
        }),
        // §4d — parameter, target `parameter_definition` row by id.
        ObjectKind::Parameter => Some(ObjectLegSet {
            object_table: "parameter_definition",
            // Identity: the definition's OWN structural rows — closed out WITH
            // the definition, counted in `object_rows`, never blockers.
            identity: vec![
                // The type declaration(s) for this definition.
                DepLeg {
                    table: "parameter_type",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false,
                    sample_expr: "'type=' || id",
                },
                // Named index dimensions — keyed INDIRECTLY via the definition's
                // parameter_type rows (§4d amended: this is an identity leg with
                // an indirect key; there is no direct parameter_definition_id).
                DepLeg {
                    table: "parameter_type_dimension",
                    column: "parameter_type_id",
                    predicate: "parameter_type_id IN \
                                (SELECT id FROM parameter_type WHERE parameter_definition_id = ?)",
                    array: false,
                    sample_expr: "'dim=' || dimension_name",
                },
                // The definition's OWN default/prefill content (mirrors). §4d
                // amended: these are IDENTITY, not dependents — a param whose
                // only content is its own default must not RESTRICT-block on it.
                // definition_value_array has NO `_hist` twin (050:198) ⇒ it
                // closes with a plain SET valid_to_commit (`array: false`).
                DepLeg {
                    table: "definition_value_scalar",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false,
                    sample_expr: "'slot=' || slot",
                },
                DepLeg {
                    table: "definition_value_long",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false,
                    sample_expr: "'slot=' || slot",
                },
                DepLeg {
                    table: "definition_value_array",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false, // no `_hist` twin ⇒ plain UPDATE, NOT move-to-hist
                    sample_expr: "'slot=' || slot",
                },
            ],
            dependents: vec![
                DepLeg {
                    table: "value_scalar",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false,
                    sample_expr: "'entity=' || entity_id || ',alt=' || alternative_id",
                },
                DepLeg {
                    table: "value_long",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false,
                    sample_expr: "'entity=' || entity_id || ',alt=' || alternative_id",
                },
                DepLeg {
                    table: "value_array",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: true,
                    sample_expr: "'entity=' || entity_id || ',alt=' || alternative_id",
                },
                DepLeg {
                    table: "entity_parameter_unit",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false,
                    sample_expr: "'entity=' || entity_id || ',unit=' || unit_id",
                },
                DepLeg {
                    table: "parameter_value_metadata",
                    column: "parameter_definition_id",
                    predicate: "parameter_definition_id = ?",
                    array: false,
                    sample_expr: "'entity=' || entity_id || ',alt=' || alternative_id",
                },
                // Polymorphic annotation table — FILTERED to the
                // parameter_definition rows (object_type discriminator).
                DepLeg {
                    table: "semantic_annotation",
                    column: "object_id",
                    predicate: "object_id = ? AND object_type = 'parameter_definition'",
                    array: false,
                    sample_expr: "'term=' || term_iri",
                },
            ],
        }),
        // §4b/§4c — entity, target `entity` row by id. Identity: the entity's
        // OWN element tuple (`entity_element WHERE entity_id = ?`) — empty for
        // 0-D, the defining tuple for N-D; closed WITH the entity, counted in
        // `object_rows`, never a blocker. The reverse edge (`element_entity_id`,
        // E-as-element-of-a-container) is NOT a plain dependent — it is the B1
        // recursive cascade edge (see [`cascade_closure`]).
        ObjectKind::Entity => Some(ObjectLegSet {
            object_table: "entity",
            identity: vec![DepLeg {
                table: "entity_element",
                column: "entity_id",
                predicate: "entity_id = ?",
                array: false,
                sample_expr: "'pos=' || position || ',elem=' || element_entity_id",
            }],
            dependents: vec![
                DepLeg {
                    table: "value_scalar",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: false,
                    sample_expr: "'pd=' || parameter_definition_id || ',alt=' || alternative_id",
                },
                DepLeg {
                    table: "value_long",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: false,
                    sample_expr: "'pd=' || parameter_definition_id || ',alt=' || alternative_id",
                },
                DepLeg {
                    table: "value_array",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: true,
                    sample_expr: "'pd=' || parameter_definition_id || ',alt=' || alternative_id",
                },
                DepLeg {
                    table: "entity_location",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: false,
                    sample_expr: "'loc=' || entity_id",
                },
                DepLeg {
                    table: "entity_alternative",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: false,
                    sample_expr: "'alt=' || alternative_id",
                },
                // membership has TWO entity legs (§4b): the entity as group and
                // as member. Both are dependents.
                DepLeg {
                    table: "membership",
                    column: "group_entity_id",
                    predicate: "group_entity_id = ?",
                    array: false,
                    sample_expr: "'member=' || member_entity_id || ',alt=' || coalesce(alternative_id, 0)",
                },
                DepLeg {
                    table: "membership",
                    column: "member_entity_id",
                    predicate: "member_entity_id = ?",
                    array: false,
                    sample_expr: "'group=' || group_entity_id || ',alt=' || coalesce(alternative_id, 0)",
                },
                DepLeg {
                    table: "entity_parameter_unit",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: false,
                    sample_expr: "'pd=' || parameter_definition_id || ',unit=' || unit_id",
                },
                DepLeg {
                    table: "entity_metadata",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: false,
                    sample_expr: "'metadata=' || metadata_id",
                },
                DepLeg {
                    table: "parameter_value_metadata",
                    column: "entity_id",
                    predicate: "entity_id = ?",
                    array: false,
                    sample_expr: "'pd=' || parameter_definition_id || ',alt=' || alternative_id",
                },
                // Polymorphic annotation table — FILTERED to entity rows.
                DepLeg {
                    table: "semantic_annotation",
                    column: "object_id",
                    predicate: "object_id = ? AND object_type = 'entity'",
                    array: false,
                    sample_expr: "'term=' || term_iri",
                },
                // EXCLUDED: run.tool_entity_id — immutable append-only leg (§10
                // ruling 2), like run.result_alternative_id for alternatives.
            ],
        }),
        // §4e/C7 — class, target `entity_class` row by id. This is the highest
        // blast radius: besides the flat legs below it has three CROSS-KIND
        // cascade edges (child entities, child parameters, and the B2 dependent
        // classes that type a dimension on it) handled by [`cascade_edges`], NOT
        // as flat DepLegs. Identity: the class's OWN dimension declarations.
        ObjectKind::Class => Some(ObjectLegSet {
            object_table: "entity_class",
            identity: vec![DepLeg {
                table: "entity_class_dimension",
                column: "entity_class_id",
                predicate: "entity_class_id = ?",
                array: false,
                sample_expr: "'pos=' || position",
            }],
            dependents: vec![
                // The class as a member of a named class-set.
                DepLeg {
                    table: "class_set_member",
                    column: "member_class_id",
                    predicate: "member_class_id = ?",
                    array: false,
                    sample_expr: "'set=' || class_set_id",
                },
                DepLeg {
                    table: "entity_class_display_mode",
                    column: "entity_class_id",
                    predicate: "entity_class_id = ?",
                    array: false,
                    sample_expr: "'mode=' || display_mode_id",
                },
                // Polymorphic annotation table — FILTERED to entity_class rows.
                DepLeg {
                    table: "semantic_annotation",
                    column: "object_id",
                    predicate: "object_id = ? AND object_type = 'entity_class'",
                    array: false,
                    sample_expr: "'term=' || term_iri",
                },
                // C7 — class-dimension annotations (object_id = the class id,
                // object_subkey = position). Deliberately EXCLUDED from
                // `audit_dangling_semantic_ref` (`120:203-204`), so a class
                // delete must close them here or they orphan silently.
                DepLeg {
                    table: "semantic_annotation",
                    column: "object_id",
                    predicate: "object_id = ? AND object_type = 'class_dimension'",
                    array: false,
                    sample_expr: "'term=' || term_iri",
                },
            ],
        }),
    }
}

// ---------------------------------------------------------------------------
// Cross-kind cascade edges (§10 ruling 1, §4e). A structural delete that would
// punch a hole in a LIVE N-D container recurses into that container; a class
// delete additionally cascades its CHILD objects (entities + parameters), each
// of a DIFFERENT kind and each running its OWN full cascade. Every edge is
// expressed as data so the closure walk (cascade) and the blocker report
// (cascade:false) share one definition (invariant 2 — no drift).
//
// The edges by source kind:
//   - Entity — B1: reverse `entity_element.element_entity_id` into container
//     entities (deleting element E cascades every live container C listing E).
//   - Class  — child `entity WHERE entity_class_id=?` (each → an Entity cascade),
//     child `parameter_definition WHERE entity_class_id=?` (each → a Parameter
//     cascade), and B2 `entity_class_dimension.dimension_class_id=?` into every
//     class that types a dimension on this one (each → its OWN Class cascade).
//   - Alternative / Parameter — none (closure is just `[self]`).
// ---------------------------------------------------------------------------

/// One cross-kind cascade edge: from an object of the source kind, the SQL that
/// finds the neighboring objects (possibly of a DIFFERENT kind) a `cascade:true`
/// delete also closes, plus the reporting metadata a `cascade:false` blocker
/// needs.
struct CascadeEdge {
    /// The kind of the neighbor objects this edge reaches.
    neighbor_kind: ObjectKind,
    /// Blocker table label — the real dependent table — for [`Blocker::table`].
    table: &'static str,
    /// Blocker column label for [`Blocker::column`].
    column: &'static str,
    /// SQL selecting the neighbor ids: EXACTLY ONE `?` bound to the source id,
    /// returning a single id column, live-guarded. Drives the closure walk AND
    /// (wrapped) the blocker COUNT + sample, so they can never drift.
    select_ids: &'static str,
    /// The table carrying `(id, name)` for the neighbor kind, for the bounded
    /// blocker `sample`.
    name_table: &'static str,
}

/// The cross-kind cascade edges out of `kind` (§10 ruling 1, §4e). Entity has the
/// single B1 edge; a class has three (child entities, child parameters, and the
/// B2 dependent classes). Alternative and Parameter have none, so their closure
/// reduces to `[self]` and the P1/P2 behavior is unchanged.
fn cascade_edges(kind: ObjectKind) -> Vec<CascadeEdge> {
    match kind {
        ObjectKind::Entity => vec![CascadeEdge {
            neighbor_kind: ObjectKind::Entity,
            table: "entity",
            column: "element_entity_id",
            select_ids: "SELECT DISTINCT ee.entity_id \
                         FROM entity_element ee \
                         JOIN entity e ON e.id = ee.entity_id AND e.valid_to_commit IS NULL \
                         WHERE ee.element_entity_id = ? AND ee.valid_to_commit IS NULL",
            name_table: "entity",
        }],
        ObjectKind::Class => vec![
            // Child entities — each runs its own §4b/§4c entity cascade.
            CascadeEdge {
                neighbor_kind: ObjectKind::Entity,
                table: "entity",
                column: "entity_class_id",
                select_ids: "SELECT id FROM entity \
                             WHERE entity_class_id = ? AND valid_to_commit IS NULL",
                name_table: "entity",
            },
            // Child parameter definitions — each runs its own §4d cascade.
            CascadeEdge {
                neighbor_kind: ObjectKind::Parameter,
                table: "parameter_definition",
                column: "entity_class_id",
                select_ids: "SELECT id FROM parameter_definition \
                             WHERE entity_class_id = ? AND valid_to_commit IS NULL",
                name_table: "parameter_definition",
            },
            // B2 — classes that declare THIS class as a dimension type; each such
            // class then runs its own §4e cascade (recurses, cycle-guarded).
            CascadeEdge {
                neighbor_kind: ObjectKind::Class,
                table: "entity_class",
                column: "dimension_class_id",
                select_ids: "SELECT DISTINCT ecd.entity_class_id \
                             FROM entity_class_dimension ecd \
                             JOIN entity_class ec ON ec.id = ecd.entity_class_id \
                                                 AND ec.valid_to_commit IS NULL \
                             WHERE ecd.dimension_class_id = ? AND ecd.valid_to_commit IS NULL",
                name_table: "entity_class",
            },
        ],
        _ => Vec::new(),
    }
}

/// The neighbor ids one edge reaches from `id`.
fn edge_neighbors(conn: &Connection, edge: &CascadeEdge, id: i64) -> Result<Vec<i64>, DbError> {
    let mut stmt = conn.prepare(edge.select_ids)?;
    let ids = stmt
        .query_map(params![id], |r| r.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ids)
}

/// The full cross-kind work-list a `cascade:true` delete closes: the target plus
/// every object reachable through the kind's cascade edges, each carried as a
/// `(kind, id)` item and cycle-guarded by a visited-set on `(kind, id)`. Each
/// item is later closed using ITS OWN kind's leg set, so a child entity closes
/// its values/tuple and a child parameter its type+values. For kinds with no edge
/// this is `[(kind, root)]`, so alternative/parameter/entity closures reduce to
/// exactly what P1–P3 produced. Computed once, up front, over the pre-close live
/// state.
fn cascade_closure(
    conn: &Connection,
    kind: ObjectKind,
    root: i64,
) -> Result<Vec<(ObjectKind, i64)>, DbError> {
    use std::collections::HashSet;
    let mut visited: HashSet<(ObjectKind, i64)> = HashSet::new();
    let mut order: Vec<(ObjectKind, i64)> = Vec::new();
    let mut stack = vec![(kind, root)];
    while let Some((k, id)) = stack.pop() {
        if !visited.insert((k, id)) {
            continue; // cycle / already-scheduled guard
        }
        order.push((k, id));
        for edge in cascade_edges(k) {
            for nid in edge_neighbors(conn, &edge, id)? {
                if !visited.contains(&(edge.neighbor_kind, nid)) {
                    stack.push((edge.neighbor_kind, nid));
                }
            }
        }
    }
    Ok(order)
}

/// The cross-kind cascade edges out of the target, each rendered as ONE blocker
/// on its real dependent table (`entity` / `parameter_definition` /
/// `entity_class`) with a bounded name sample — what a `cascade:false` delete
/// reports instead of recursing. Empty for kinds with no edge, so the
/// alternative/parameter RESTRICT reports are unchanged; for an entity it is the
/// single B1 container blocker (behavior-identical to P3).
fn collect_container_blockers(
    conn: &Connection,
    kind: ObjectKind,
    target_id: i64,
) -> Result<Vec<Blocker>, DbError> {
    let mut blockers = Vec::new();
    for edge in cascade_edges(kind) {
        let count: i64 = conn.query_row(
            &format!("SELECT count(*) FROM ({}) t", edge.select_ids),
            params![target_id],
            |r| r.get(0),
        )?;
        if count == 0 {
            continue;
        }
        let mut stmt = conn.prepare(&format!(
            "SELECT name FROM {} WHERE id IN ({}) AND valid_to_commit IS NULL LIMIT {}",
            edge.name_table, edge.select_ids, SAMPLE_LIMIT
        ))?;
        let sample = stmt
            .query_map(params![target_id], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        blockers.push(Blocker {
            table: edge.table.to_string(),
            column: edge.column.to_string(),
            count: count as usize,
            sample,
        });
    }
    Ok(blockers)
}

/// Max blocker-sample rows rendered per leg (bounded — §1).
const SAMPLE_LIMIT: usize = 5;

// ---------------------------------------------------------------------------
// Preparation (pure over the request; §2).
// ---------------------------------------------------------------------------

/// One prepared object-delete request, carrying its original caller index.
pub(crate) struct PreparedObjectDelete {
    pub key_index: usize,
    pub key: ObjectDeleteKey,
    pub opts: ObjectDeleteOpts,
}

/// Validate + dedupe the staged object deletes purely (no DB), mirroring
/// `api::prepare_deletes`: `class` present iff kind ∈ {Entity, Parameter};
/// `Elements` only for `Entity`; names non-blank; fold-colliding distinct
/// spellings rejected, exact duplicates deduped, original index preserved.
pub(crate) fn prepare_object_deletes(
    reqs: &[(ObjectDeleteKey, ObjectDeleteOpts)],
) -> Result<Vec<PreparedObjectDelete>, DbError> {
    use std::collections::HashMap;
    let mut seen: HashMap<(ObjectKind, Option<String>, String), (usize, ObjectDeleteKey)> =
        HashMap::new();
    let mut out: Vec<PreparedObjectDelete> = Vec::new();
    for (i, (key, opts)) in reqs.iter().enumerate() {
        validate_object_key(i, key)?;
        let fold = (key.kind, class_fold(&key.class), target_fold(&key.target));
        match seen.get(&fold) {
            Some((j, prev)) => {
                if prev != key {
                    return Err(DbError::Api(format!(
                        "object-delete keys {j} and {i} resolve to the same target \
                         (name resolution is case-insensitive); use one spelling"
                    )));
                }
                // identical spelling ⇒ harmless duplicate, dropped.
            }
            None => {
                seen.insert(fold, (i, key.clone()));
                out.push(PreparedObjectDelete {
                    key_index: i,
                    key: key.clone(),
                    opts: *opts,
                });
            }
        }
    }
    Ok(out)
}

fn class_fold(class: &Option<String>) -> Option<String> {
    class.as_ref().map(|c| c.to_lowercase())
}

fn target_fold(target: &ObjectRef) -> String {
    match target {
        ObjectRef::Name(n) => format!("name:{}", n.to_lowercase()),
        ObjectRef::Elements(es) => {
            let parts: Vec<String> = es.iter().map(|e| e.to_lowercase()).collect();
            format!("elems:{}", parts.join("\u{1f}"))
        }
    }
}

fn validate_object_key(i: usize, key: &ObjectDeleteKey) -> Result<(), DbError> {
    let per_class = matches!(key.kind, ObjectKind::Entity | ObjectKind::Parameter);
    match (&key.class, per_class) {
        (Some(c), true) => {
            if c.trim().is_empty() {
                return Err(DbError::Api(format!(
                    "object-delete key {i}: class name must be non-empty"
                )));
            }
        }
        (None, false) => {}
        (Some(_), false) => {
            return Err(DbError::Api(format!(
                "object-delete key {i}: {} is addressed by its own name; class must be None",
                key.kind.tag()
            )))
        }
        (None, true) => {
            return Err(DbError::Api(format!(
                "object-delete key {i}: {} is per-class; class is required",
                key.kind.tag()
            )))
        }
    }
    match &key.target {
        ObjectRef::Name(n) => {
            if n.trim().is_empty() {
                return Err(DbError::Api(format!(
                    "object-delete key {i}: target name must be non-empty"
                )));
            }
        }
        ObjectRef::Elements(es) => {
            if key.kind != ObjectKind::Entity {
                return Err(DbError::Api(format!(
                    "object-delete key {i}: an element tuple only addresses an entity"
                )));
            }
            if es.is_empty() || es.iter().any(|e| e.trim().is_empty()) {
                return Err(DbError::Api(format!(
                    "object-delete key {i}: element names must all be non-empty"
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Target resolution (lookup-only, never auto-create).
// ---------------------------------------------------------------------------

/// A resolved target: its object id and a rendered name for reporting.
struct ResolvedTarget {
    id: i64,
}

/// Resolve one object key to its live object id. `Ok(None)` ⇒ the target has no
/// current row (already closed or never existed) ⇒ `NotFound`. P1 resolves only
/// `Alternative`; other kinds error (the leg set is `None`).
fn resolve_target(
    conn: &Connection,
    key: &ObjectDeleteKey,
) -> Result<Option<ResolvedTarget>, DbError> {
    match key.kind {
        ObjectKind::Alternative => {
            let name = match &key.target {
                ObjectRef::Name(n) => n,
                ObjectRef::Elements(_) => {
                    return Err(DbError::Api(
                        "an alternative is addressed by name, not an element tuple".into(),
                    ))
                }
            };
            Ok(crate::api::lookup_alternative(conn, name)?.map(|id| ResolvedTarget { id }))
        }
        ObjectKind::Parameter => {
            // A parameter is per-class and name-addressed: resolve its class,
            // then the definition within it. An element tuple never addresses a
            // parameter (validated in prepare, defended here).
            let name = match &key.target {
                ObjectRef::Name(n) => n,
                ObjectRef::Elements(_) => {
                    return Err(DbError::Api(
                        "a parameter is addressed by name, not an element tuple".into(),
                    ))
                }
            };
            let class = key.class.as_deref().ok_or_else(|| {
                DbError::Api("a parameter delete requires its class".into())
            })?;
            // Miss at EITHER step ⇒ None ⇒ NotFound (never auto-create).
            let class_id = match crate::api::lookup_entity_class(conn, class)? {
                Some(id) => id,
                None => return Ok(None),
            };
            Ok(crate::api::lookup_parameter(conn, class_id, name)?
                .map(|id| ResolvedTarget { id }))
        }
        ObjectKind::Entity => {
            // Entity is per-class; resolve the class first. Miss ⇒ None.
            let class = key.class.as_deref().ok_or_else(|| {
                DbError::Api("an entity delete requires its class".into())
            })?;
            let class_id = match crate::api::lookup_entity_class(conn, class)? {
                Some(id) => id,
                None => return Ok(None),
            };
            match &key.target {
                // 0-D or the (per-class-unique) N-D name.
                ObjectRef::Name(n) => Ok(crate::api::lookup_entity(conn, class_id, n)?
                    .map(|id| ResolvedTarget { id })),
                // N-D element tuple (§4c, critique C6).
                ObjectRef::Elements(es) => {
                    resolve_entity_tuple(conn, class_id, class, es).map(|o| o.map(|id| ResolvedTarget { id }))
                }
            }
        }
        ObjectKind::Class => {
            // A class is globally name-addressed (class is None, validated in
            // prepare); an element tuple never addresses a class.
            let name = match &key.target {
                ObjectRef::Name(n) => n,
                ObjectRef::Elements(_) => {
                    return Err(DbError::Api(
                        "a class is addressed by name, not an element tuple".into(),
                    ))
                }
            };
            Ok(crate::api::lookup_entity_class(conn, name)?.map(|id| ResolvedTarget { id }))
        }
    }
}

/// Resolve an N-D entity by its element tuple within `class_id` (§4c, C6). Reads
/// the class's declared dimensions (ordered by position), resolves each provided
/// element name within its position's dimension type (a class, or any member
/// class of a class-set dimension), then finds the single entity in `class_id`
/// whose live tuple matches ALL positions with EXACTLY the declared arity.
///
///   - a class with no declared dimensions, or an arity mismatch, is a caller
///     error (clean [`DbError`]);
///   - a missing element name, or no matching entity, ⇒ `Ok(None)` ⇒ NotFound;
///   - `> 1` matching entity (no schema constraint guarantees tuple-uniqueness —
///     `one_current_entity_element` is on `(entity_id, position)`) ⇒ a clean
///     ambiguity error, never a silent pick.
fn resolve_entity_tuple(
    conn: &Connection,
    class_id: i64,
    class: &str,
    es: &[String],
) -> Result<Option<i64>, DbError> {
    // Declared dimensions of the class, in position order. The REAL `position`
    // is carried through: the schema only guarantees `position >= 1` with
    // per-class uniqueness, NOT contiguity 1..n, so a gapped tuple (e.g. [1,3])
    // must bind its actual positions into the HAVING, never `i+1`.
    let mut stmt = conn.prepare(
        "SELECT dimension_class_id, dimension_class_set_id, position \
         FROM entity_class_dimension \
         WHERE entity_class_id = ? AND valid_to_commit IS NULL \
         ORDER BY position",
    )?;
    let dims = stmt
        .query_map(params![class_id], |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?,
                r.get::<_, Option<i64>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if dims.is_empty() {
        return Err(DbError::Api(format!(
            "class '{class}' is 0-D; address its entity by name, not an element tuple"
        )));
    }
    if dims.len() != es.len() {
        return Err(DbError::Api(format!(
            "class '{class}' has arity {} but {} element name(s) were given",
            dims.len(),
            es.len()
        )));
    }

    // Resolve each element name within its position's dimension type. Any miss
    // ⇒ NotFound (the tuple cannot exist).
    let mut element_ids: Vec<i64> = Vec::with_capacity(es.len());
    for ((dim_class, dim_set, _pos), name) in dims.iter().zip(es.iter()) {
        match resolve_element(conn, *dim_class, *dim_set, name)? {
            Some(id) => element_ids.push(id),
            None => return Ok(None),
        }
    }

    // Find the entity in `class_id` whose live tuple matches ALL positions and
    // has EXACTLY the declared arity (so a longer tuple sharing this prefix is
    // not matched). One GROUP BY … HAVING over the live element rows.
    let n = element_ids.len();
    let mut having = String::new();
    // Bind each element to its REAL declared position (dims kept in the same
    // order as element_ids), so a non-contiguous position set resolves correctly.
    for (dim, _id) in dims.iter().zip(element_ids.iter()) {
        having.push_str(&format!(
            " AND sum(CASE WHEN ee.position = {} AND ee.element_entity_id = ? THEN 1 ELSE 0 END) = 1",
            dim.2
        ));
    }
    let sql = format!(
        "SELECT ee.entity_id \
         FROM entity_element ee \
         JOIN entity e ON e.id = ee.entity_id AND e.valid_to_commit IS NULL \
                      AND e.entity_class_id = ? \
         WHERE ee.valid_to_commit IS NULL \
         GROUP BY ee.entity_id \
         HAVING count(*) = {n}{having}"
    );
    let mut stmt = conn.prepare(&sql)?;
    // Bind class_id, then the element id per position.
    let mut binds: Vec<i64> = Vec::with_capacity(n + 1);
    binds.push(class_id);
    binds.extend_from_slice(&element_ids);
    let params_iter = duckdb::params_from_iter(binds.iter());
    let matches = stmt
        .query_map(params_iter, |r| r.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    match matches.as_slice() {
        [] => Ok(None),
        [id] => Ok(Some(*id)),
        many => Err(DbError::Api(format!(
            "element tuple in class '{class}' is ambiguous: it matches {} entities \
             (ids {:?}); no schema constraint guarantees tuple-uniqueness, so this \
             must be addressed by name",
            many.len(),
            many
        ))),
    }
}

/// Resolve one element NAME to an entity id within a dimension type: a single
/// class (`dim_class`), or ANY member class of a class-set (`dim_set`). Within a
/// single class the per-class name uniqueness gives ≤1 hit; across a set's
/// members a name collision (only audit-guarded, #2364) ⇒ a clean ambiguity
/// error. Miss ⇒ `Ok(None)`.
fn resolve_element(
    conn: &Connection,
    dim_class: Option<i64>,
    dim_set: Option<i64>,
    name: &str,
) -> Result<Option<i64>, DbError> {
    // Candidate dimension classes for this position.
    let candidates: Vec<i64> = match (dim_class, dim_set) {
        (Some(c), None) => vec![c],
        (None, Some(set_id)) => {
            let mut stmt = conn.prepare(
                "SELECT member_class_id FROM class_set_member \
                 WHERE class_set_id = ? AND valid_to_commit IS NULL",
            )?;
            stmt.query_map(params![set_id], |r| r.get::<_, i64>(0))?
                .collect::<Result<Vec<_>, _>>()?
        }
        // The dimension typing CHECK guarantees exactly one is set; defend anyway.
        _ => {
            return Err(DbError::Api(
                "malformed entity_class_dimension: neither a class nor a class-set type".into(),
            ))
        }
    };
    let mut hits: Vec<i64> = Vec::new();
    for c in candidates {
        if let Some(id) = crate::api::lookup_entity(conn, c, name)? {
            hits.push(id);
        }
    }
    match hits.as_slice() {
        [] => Ok(None),
        [id] => Ok(Some(*id)),
        many => Err(DbError::Api(format!(
            "element name '{name}' is ambiguous across the class-set dimension: \
             it matches {} entities {:?}",
            many.len(),
            many
        ))),
    }
}

// ---------------------------------------------------------------------------
// Forward COUNTs (shared by preview + the cascade:false blocker decision).
// ---------------------------------------------------------------------------

/// Count live rows a single leg contributes for `target_id`.
fn count_leg(conn: &Connection, leg: &DepLeg, target_id: i64) -> Result<usize, DbError> {
    let sql = format!(
        "SELECT count(*) FROM {} WHERE {}{}",
        leg.table,
        leg.predicate,
        leg.live_predicate()
    );
    let n: i64 = conn.query_row(&sql, params![target_id], |r| r.get(0))?;
    Ok(n as usize)
}

/// A bounded sample of rendered identifiers for one leg's referencing rows.
fn sample_leg(conn: &Connection, leg: &DepLeg, target_id: i64) -> Result<Vec<String>, DbError> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {}{} LIMIT {}",
        leg.sample_expr,
        leg.table,
        leg.predicate,
        leg.live_predicate(),
        SAMPLE_LIMIT
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params![target_id], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Count the DISTINCT live dependent rows a whole cross-kind closure would close,
/// per table (§3 preview). A single physical row can be reached from more than
/// one work item — e.g. a `value_scalar` row is keyed by BOTH its child entity
/// (`entity_id`) and its child parameter (`parameter_definition_id`), and a
/// `membership` row by two legs of the same entity — so a naive per-leg SUM would
/// over-count relative to the commit path, whose live-guard closes each row once.
/// Grouping all of a table's leg predicates across the closure into ONE
/// `count(*) WHERE (p1) OR (p2) OR …` counts each row once, so preview matches
/// commit even across kinds. For a single-item, non-overlapping closure this
/// reduces to the per-leg counts P1–P3 produced.
fn preview_dependent_counts(
    conn: &Connection,
    closure: &[(ObjectKind, i64)],
) -> Result<Vec<TableCount>, DbError> {
    // table -> (array flag, [(predicate, bound id)]). One entry per physical
    // table; every leg on a given table shares its tier (array vs versioned).
    type TableGroup = (&'static str, bool, Vec<(&'static str, i64)>);
    let mut groups: Vec<TableGroup> = Vec::new();
    for (k, id) in closure {
        let legs = object_legs(*k).expect("cascade-closure item kind has a leg set");
        for leg in &legs.dependents {
            if let Some(g) = groups.iter_mut().find(|g| g.0 == leg.table) {
                g.2.push((leg.predicate, *id));
            } else {
                groups.push((leg.table, leg.array, vec![(leg.predicate, *id)]));
            }
        }
    }
    let mut out = Vec::new();
    for (table, array, preds) in groups {
        let live = if array { "" } else { " AND valid_to_commit IS NULL" };
        let clause = preds
            .iter()
            .map(|(p, _)| format!("({p})"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let sql = format!("SELECT count(*) FROM {table} WHERE ({clause}){live}");
        let binds: Vec<i64> = preds.iter().map(|(_, id)| *id).collect();
        let n: i64 = conn.query_row(&sql, duckdb::params_from_iter(binds.iter()), |r| r.get(0))?;
        if n > 0 {
            out.push(TableCount {
                table: table.to_string(),
                rows: n as usize,
            });
        }
    }
    Ok(out)
}

/// Assemble the blocker list for the dependent legs with ≥1 live row.
fn collect_blockers(
    conn: &Connection,
    legs: &ObjectLegSet,
    target_id: i64,
) -> Result<Vec<Blocker>, DbError> {
    let mut blockers = Vec::new();
    for leg in &legs.dependents {
        let count = count_leg(conn, leg, target_id)?;
        if count > 0 {
            blockers.push(Blocker {
                table: leg.table.to_string(),
                column: leg.column.to_string(),
                count,
                sample: sample_leg(conn, leg, target_id)?,
            });
        }
    }
    Ok(blockers)
}

// ---------------------------------------------------------------------------
// Guarded close-out (cascade; §2 same-commit guard on every arm).
// ---------------------------------------------------------------------------

/// Close one dependent/identity leg, guarded by `valid_from_commit < commit_id`.
/// Returns the number of rows actually closed (the `execute()` rowcount — a
/// same-commit-created row is skipped and not counted, D5).
fn close_leg(
    conn: &Connection,
    leg: &DepLeg,
    commit_id: i64,
    target_id: i64,
) -> Result<usize, DbError> {
    if leg.array {
        // Array tier: move the guarded pre-image to `_hist` (always retained —
        // a structural object delete keeps history; single path per C8), then
        // DELETE the same guarded rows. The guard also keeps a same-commit
        // array row out of `_hist`, whose CHECK has no NULL branch (C9).
        conn.execute(
            &format!(
                "INSERT INTO value_array_hist SELECT *, ?, TRUE FROM {} \
                 WHERE {} AND valid_from_commit < ?",
                leg.table, leg.predicate
            ),
            params![commit_id, target_id, commit_id],
        )?;
        let deleted = conn.execute(
            &format!(
                "DELETE FROM {} WHERE {} AND valid_from_commit < ?",
                leg.table, leg.predicate
            ),
            params![target_id, commit_id],
        )?;
        Ok(deleted)
    } else {
        let closed = conn.execute(
            &format!(
                "UPDATE {} SET valid_to_commit = ? \
                 WHERE {} AND valid_from_commit < ? AND valid_to_commit IS NULL",
                leg.table, leg.predicate
            ),
            params![commit_id, target_id, commit_id],
        )?;
        Ok(closed)
    }
}

/// Close the target object row itself (and, for future kinds, its identity
/// legs), guarded by `valid_from_commit < commit_id`. Returns the object-row
/// count (0 ⇒ same-commit-created or already closed ⇒ NotFound).
fn close_object_and_identity(
    conn: &Connection,
    legs: &ObjectLegSet,
    commit_id: i64,
    target_id: i64,
) -> Result<usize, DbError> {
    let mut object_rows = conn.execute(
        &format!(
            "UPDATE {} SET valid_to_commit = ? \
             WHERE id = ? AND valid_from_commit < ? AND valid_to_commit IS NULL",
            legs.object_table
        ),
        params![commit_id, target_id, commit_id],
    )?;
    for leg in &legs.identity {
        object_rows += close_leg(conn, leg, commit_id, target_id)?;
    }
    Ok(object_rows)
}

/// Accumulates a cascade's counts across the target AND every recursively-
/// cascaded object (§10 ruling 1). `per_table` is merged by table name so a leg
/// that fires on several cascaded objects (e.g. `value_scalar` on the target and
/// each container entity) reports one summed row, matching the flat single-object
/// framing for kinds with no recursive edge.
#[derive(Default)]
struct CascadeAccum {
    object_rows: usize,
    per_table: Vec<TableCount>,
}

impl CascadeAccum {
    fn add_table(&mut self, table: &str, rows: usize) {
        if rows == 0 {
            return;
        }
        if let Some(tc) = self.per_table.iter_mut().find(|t| t.table == table) {
            tc.rows += rows;
        } else {
            self.per_table.push(TableCount {
                table: table.to_string(),
                rows,
            });
        }
    }

    fn dep_total(&self) -> usize {
        self.per_table.iter().map(|t| t.rows).sum()
    }

    fn into_counts(self) -> CascadeCounts {
        let dep = self.dep_total();
        CascadeCounts {
            object_rows: self.object_rows,
            total_rows: self.object_rows + dep,
            per_table: self.per_table,
        }
    }
}

// ---------------------------------------------------------------------------
// Commit-time apply (§2). One call per prepared request, after value writes +
// value deletes, under the same commit_id.
// ---------------------------------------------------------------------------

/// Resolve + close out one object-delete request, appending its
/// [`ObjectDeleteOutcome`] to `report.object_deletes`.
pub(crate) fn apply_object_delete(
    conn: &Connection,
    commit_id: i64,
    prepared: &PreparedObjectDelete,
    report: &mut crate::types::WriteReport,
) -> Result<(), DbError> {
    let key_index = prepared.key_index;
    let kind = prepared.key.kind;
    let result = apply_one(conn, commit_id, &prepared.key, prepared.opts)?;
    report.object_deletes.push(ObjectDeleteOutcome {
        key_index,
        kind,
        result,
    });
    Ok(())
}

fn apply_one(
    conn: &Connection,
    commit_id: i64,
    key: &ObjectDeleteKey,
    opts: ObjectDeleteOpts,
) -> Result<ObjectDeleteResult, DbError> {
    let legs = object_legs(key.kind).ok_or_else(|| {
        DbError::Api(format!(
            "object delete for {} is not implemented yet (P3/P4)",
            key.kind.tag()
        ))
    })?;
    let target = match resolve_target(conn, key)? {
        Some(t) => t,
        None => return Ok(ObjectDeleteResult::NotFound), // absent / already closed (D4)
    };

    if !opts.cascade {
        // cascade:false — RESTRICT contract. Any live dependent OR live
        // recursive container (B1, §10 ruling 1) ⇒ close NOTHING.
        let mut blockers = collect_blockers(conn, &legs, target.id)?;
        blockers.extend(collect_container_blockers(conn, key.kind, target.id)?);
        if !blockers.is_empty() {
            return Ok(ObjectDeleteResult::Restricted { blockers });
        }
        // No dependents / containers ⇒ bare delete of the object row + its own
        // identity rows (guarded). RESTRICT never recurses.
        let object_rows = close_object_and_identity(conn, &legs, commit_id, target.id)?;
        if object_rows == 0 {
            // Same-commit-created target (guard matched nothing) ⇒ NotFound (§2).
            return Ok(ObjectDeleteResult::NotFound);
        }
        return Ok(ObjectDeleteResult::Deleted {
            closed_out: CascadeCounts {
                object_rows,
                per_table: Vec::new(),
                total_rows: object_rows,
            },
        });
    }

    // cascade:true — close the target AND every recursively-reachable container
    // (B1: entity-as-element of a live N-D container, §10 ruling 1), all guarded.
    // For kinds with no recursive edge the closure is just `[target]`, so the
    // alternative/parameter behavior is unchanged. Counts come from the execute()
    // rowcounts (D5: a row already closed by a staged value delete, or created
    // this same commit, is not touched and not counted), merged per table.
    let closure = cascade_closure(conn, key.kind, target.id)?;
    let mut acc = CascadeAccum::default();
    for (k, id) in &closure {
        // Each work item closes with ITS OWN kind's leg set (cross-kind: a class
        // closure holds Class/Entity/Parameter items). The live-guard on every
        // close-out means a row reachable from two items — e.g. a value keyed by
        // both a child entity and a child parameter — is closed (and counted)
        // exactly once by whichever item reaches it first (D5, structurally).
        let item_legs = object_legs(*k).expect("cascade-closure item kind has a leg set");
        for leg in &item_legs.dependents {
            let rows = close_leg(conn, leg, commit_id, *id)?;
            acc.add_table(leg.table, rows);
        }
        acc.object_rows += close_object_and_identity(conn, &item_legs, commit_id, *id)?;
    }
    if acc.object_rows == 0 && acc.per_table.is_empty() {
        // Nothing closable (same-commit-created target + only same-commit deps).
        return Ok(ObjectDeleteResult::NotFound);
    }
    Ok(ObjectDeleteResult::Deleted {
        closed_out: acc.into_counts(),
    })
}

// ---------------------------------------------------------------------------
// Preview (§3, decision D6). Read-only; drives the SAME leg registry as forward
// COUNTs. Best-effort snapshot — the commit-time outcome is authoritative
// (critique C5); a concurrent writer between preview and commit can still move
// the counts. No same-commit guard exists here (there is no commit).
// ---------------------------------------------------------------------------

pub(crate) fn preview_one(
    conn: &Connection,
    key_index: usize,
    key: &ObjectDeleteKey,
) -> Result<ObjectDeletePreview, DbError> {
    // Validate the key exactly as the commit path does (via
    // `prepare_object_deletes`), so a malformed key commit rejects (e.g. an
    // Alternative/Class key carrying `class: Some(..)`) is rejected by preview
    // too, rather than silently resolving.
    validate_object_key(key_index, key)?;
    let kind = key.kind;
    let legs = match object_legs(kind) {
        Some(l) => l,
        None => {
            return Err(DbError::Api(format!(
                "object-delete preview for {} is not implemented yet (P3/P4)",
                kind.tag()
            )))
        }
    };
    let target = match resolve_target(conn, key)? {
        Some(t) => t,
        None => {
            return Ok(ObjectDeletePreview {
                key_index,
                kind,
                found: false,
                cascade: CascadeCounts::default(),
                blockers: Vec::new(),
            })
        }
    };

    // Cascade framing spans the FULL cross-kind blast radius (§10 ruling 1: the
    // preview must surface every object a cascade would also close): the target
    // plus every reachable work item. For kinds with no cascade edge the closure
    // is `[target]`, so alt/parameter previews are unchanged. Each item's own
    // row + its identity legs → object_rows (identity legs never overlap across
    // items — they key on the item's own id); dependents → per_table, counted
    // DISTINCT across the closure so a value keyed by both a child entity and a
    // child parameter is counted ONCE, matching the commit path's live-guarded
    // rowcount (D5) exactly.
    let closure = cascade_closure(conn, kind, target.id)?;
    let mut object_rows = 0usize;
    for (k, id) in &closure {
        let item_legs = object_legs(*k).expect("cascade-closure item kind has a leg set");
        object_rows += 1; // this object's own live row
        for leg in &item_legs.identity {
            object_rows += count_leg(conn, leg, *id)?;
        }
    }
    let per_table = preview_dependent_counts(conn, &closure)?;
    let dep_total: usize = per_table.iter().map(|t| t.rows).sum();
    let cascade = CascadeCounts {
        object_rows,
        total_rows: object_rows + dep_total,
        per_table,
    };

    // Blocker framing: the IMMEDIATE dependents of the target plus the immediate
    // containers (one level — what a cascade:false RESTRICT would report).
    let mut blockers = collect_blockers(conn, &legs, target.id)?;
    blockers.extend(collect_container_blockers(conn, kind, target.id)?);

    Ok(ObjectDeletePreview {
        key_index,
        kind,
        found: true,
        cascade,
        blockers,
    })
}
