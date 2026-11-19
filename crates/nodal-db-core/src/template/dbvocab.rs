//! Read the database's CURRENT vocabulary into an in-memory snapshot — the
//! "DB side" of the reconcile diff (§4.2 Layer 1) and the source `template
//! export` re-emits from (§8.1.7). Reads current rows only (`valid_to_commit
//! IS NULL`); history never participates in a diff.
//!
//! Runs on an already-open connection so the reconcile apply can re-read
//! inside its own write transaction (fresh state per conflict retry), and
//! `plan`/`export` can read under one per-operation RO connection.

use std::collections::BTreeMap;

use duckdb::Connection;

use crate::api::{index_kind_tag, read_all_declared_types};
use crate::error::DbError;
use crate::property;
use crate::types::{fold, ScalarValue};

/// One declared type member as stored (`parameter_type` +
/// `parameter_type_dimension` current rows).
#[derive(Clone, Debug)]
pub struct DbTypeMember {
    pub pt_id: i64,
    pub value_kind: String,
    /// (dimension_name as stored, kind tag), position order.
    pub dims: Vec<(String, String)>,
}

impl DbTypeMember {
    /// The same match key the write path and the IR use (folded names).
    pub fn match_key(&self) -> (String, Vec<(String, String)>) {
        (
            self.value_kind.clone(),
            self.dims
                .iter()
                .map(|(n, k)| (fold(n), k.clone()))
                .collect(),
        )
    }

    pub fn describe(&self) -> String {
        if self.dims.is_empty() {
            format!("{} rank 0", self.value_kind)
        } else {
            let d: Vec<String> = self.dims.iter().map(|(n, k)| format!("{n}: {k}")).collect();
            format!("indexed rank {} ({})", self.dims.len(), d.join(", "))
        }
    }
}

/// The current scalar default of a definition (`definition_value_scalar`,
/// slot `default_value`).
#[derive(Clone, Debug)]
pub struct DbDefault {
    pub value_kind: String,
    pub value: ScalarValue,
}

#[derive(Clone, Debug)]
pub struct DbParam {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub value_list: Option<String>,
    pub unit: Option<String>,
    pub required: bool,
    pub display_order: Option<i64>,
    pub types: Vec<DbTypeMember>,
    pub default: Option<DbDefault>,
    /// A non-scalar (long/array) default/prefill row exists — v1 export/diff
    /// cannot represent it; surfaced as a warning, never silently dropped.
    pub has_nonscalar_default: bool,
}

#[derive(Clone, Debug)]
pub enum DbDimRef {
    Class(String),
    Set(String),
}

impl DbDimRef {
    pub fn referent_name(&self) -> &str {
        match self {
            DbDimRef::Class(n) | DbDimRef::Set(n) => n,
        }
    }
    pub fn is_set(&self) -> bool {
        matches!(self, DbDimRef::Set(_))
    }
}

#[derive(Clone, Debug)]
pub struct DbClass {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub active_by_default: bool,
    pub required: bool,
    pub display_order: Option<i64>,
    /// Position order (1..rank).
    pub dims: Vec<DbDimRef>,
    /// folded name -> parameter.
    pub params: BTreeMap<String, DbParam>,
}

#[derive(Clone, Debug)]
pub struct DbClassSet {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    /// Member class names as stored (case preserved).
    pub members: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DbListMember {
    pub id: i64,
    pub ordinal: i64,
    pub value_kind: String,
    pub value: ScalarValue,
    pub description: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DbValueList {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    /// Ordinal order.
    pub members: Vec<DbListMember>,
    /// Members whose kind has no v1 literal form (date_time/duration) — kept
    /// out of `members`, surfaced as warnings.
    pub unrepresentable_members: usize,
}

#[derive(Clone, Debug)]
pub struct DbUnit {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub quantity_kind: Option<String>,
}

/// A current entity (for seed diffing and export).
#[derive(Clone, Debug)]
pub struct DbEntity {
    pub id: i64,
    pub class: String,
    pub name: String,
    pub description: Option<String>,
    /// Element entity names, position order (empty for 0-D).
    pub elements: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DbMembership {
    pub group_class: String,
    pub group: String,
    pub member_class: String,
    pub member: String,
    /// The row's `included` flag. Excluded rows (`included = FALSE`) are read
    /// too (review item 10): the loader must SEE a user's exclusion to
    /// respect it — and to avoid inserting a second current row into the
    /// `(group, member, alt)` one-current index.
    pub included: bool,
}

/// The whole current vocabulary + scaffolding snapshot.
#[derive(Clone, Debug, Default)]
pub struct DbVocab {
    /// folded name -> class.
    pub classes: BTreeMap<String, DbClass>,
    pub class_sets: BTreeMap<String, DbClassSet>,
    pub value_lists: BTreeMap<String, DbValueList>,
    pub units: BTreeMap<String, DbUnit>,
    /// (folded class, folded entity name) -> entity.
    pub entities: BTreeMap<(String, String), DbEntity>,
    /// Base (alternative-NULL, included) membership rows.
    pub memberships: Vec<DbMembership>,
    /// The full Decision-7 `presentation` document (whole JSON value — the
    /// template owns only its two keys; foreign keys are preserved on write).
    pub presentation: Option<serde_json::Value>,
    /// The raw `audit_rules` document (JSON string) if stamped.
    pub audit_rules: Option<String>,
    pub template_name: Option<String>,
    pub template_version: Option<String>,
    pub template_content_hash: Option<String>,
    pub schema_version: Option<String>,
    /// Closed-out spellings of LIVE classes whose fold differs from the
    /// class's current name (the renames in the file's history), keyed by
    /// the folded old name. The reconcile's renamed-away blocker reads it.
    pub renamed_classes: BTreeMap<String, Vec<RenamedAway>>,
    /// The same for live parameter definitions, keyed by (the class id the
    /// closed row had, folded old name).
    pub renamed_parameters: BTreeMap<(i64, String), Vec<RenamedAway>>,
    /// Classes removed locally (purge plan §3.8, V-OD1): the folded name
    /// of every class whose id has no live row any more, closed by a
    /// commit that is not template-owned ([`template_owned`]). The
    /// reconcile's removed-locally blocker reads it.
    pub removed_classes: BTreeMap<String, RemovedLocally>,
    /// The same for parameter definitions, keyed by (the class id the
    /// closed row had, folded name).
    pub removed_parameters: BTreeMap<(i64, String), RemovedLocally>,
    /// The same for value lists, keyed by folded name.
    pub removed_value_lists: BTreeMap<String, RemovedLocally>,
}

/// A vocabulary object whose last row a local (not template-owned) commit
/// closed: its name as stored and the closing commit.
#[derive(Clone, Debug)]
pub struct RemovedLocally {
    pub name: String,
    pub at_commit: i64,
}

/// The template-owned commits (purge plan §3.8, V-S1): every commit that
/// wrote a version of the core property `template_applied_commit`
/// (each template-apply and migration-step commit writes one), plus the
/// ranges a `run:` program's progress commit records (`"a..=c"`: the
/// program's own commits through its progress commit). Inclusive ranges.
pub fn template_owned(conn: &Connection) -> Result<Vec<(i64, i64)>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT valid_from_commit, value FROM database_property \
         WHERE lower(key) = 'template_applied_commit'",
    )?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
    let mut out = Vec::new();
    for row in rows {
        let (from, value) = row?;
        out.push((from, from));
        if let Some((a, b)) = value.split_once("..=") {
            if let (Ok(a), Ok(b)) = (a.trim().parse::<i64>(), b.trim().parse::<i64>()) {
                out.push((a.min(b), a.max(b)));
            }
        }
    }
    Ok(out)
}

/// A removed object's key: (the scoping id or 0, folded name).
type RemovedKey = (i64, String);

/// The vocabulary objects of `table` whose id has no live row, by folded
/// name (every name the id ever had), with the commit that closed the id's
/// last row. Per name the LATEST such closure decides, and the name is
/// removed locally only when that commit is not template-owned: an older
/// local removal must not hide a newer template-owned one (a list a user
/// dropped, re-created, then dropped and re-declared by a migration step).
/// `key` adds the row's column that scopes the name (`t.entity_class_id`
/// for a parameter) or `NULL::BIGINT`.
fn removed_locally(
    conn: &Connection,
    table: &str,
    key: &str,
    owned: &[(i64, i64)],
) -> Result<BTreeMap<RemovedKey, RemovedLocally>, DbError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {key}, t.name, x.closed FROM {table} t \
         JOIN (SELECT id, max(valid_to_commit) AS closed FROM {table} GROUP BY id \
               HAVING count(*) FILTER (WHERE valid_to_commit IS NULL) = 0) x ON x.id = t.id \
         ORDER BY x.closed, t.id"
    ))?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, Option<i64>>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;
    // Latest closure per name first (rows come ordered by closing commit),
    // with its owner; then the template-owned ones drop out.
    let mut latest: BTreeMap<RemovedKey, (RemovedLocally, bool)> = BTreeMap::new();
    for row in rows {
        let (scope, name, at_commit) = row?;
        let is_owned = owned.iter().any(|&(a, b)| a <= at_commit && at_commit <= b);
        latest.insert(
            (scope.unwrap_or(0), fold(&name)),
            (RemovedLocally { name, at_commit }, is_owned),
        );
    }
    Ok(latest
        .into_iter()
        .filter(|(_, (_, is_owned))| !is_owned)
        .map(|(k, (r, _))| (k, r))
        .collect())
}

/// One earlier spelling of a live object (a class or a parameter
/// definition): its id, the old name as stored, and the commit that closed
/// the last row carrying it.
#[derive(Clone, Debug)]
pub struct RenamedAway {
    pub id: i64,
    pub old_name: String,
    pub at_commit: i64,
}

fn scalar_from_row(
    value_kind: &str,
    f: Option<f64>,
    i: Option<i64>,
    b: Option<bool>,
    s: Option<String>,
) -> Option<ScalarValue> {
    match value_kind {
        "float" => f.map(ScalarValue::Float),
        "int" => i.map(ScalarValue::Int),
        "bool" => b.map(ScalarValue::Bool),
        "string" => s.map(ScalarValue::Str),
        _ => None, // date_time/duration: no v1 literal form
    }
}

/// Add `r` to one folded old name's list, keeping one entry per id (the
/// latest commit; two case spellings of one old name are one name).
fn push_renamed(list: &mut Vec<RenamedAway>, r: RenamedAway) {
    match list.iter_mut().find(|x| x.id == r.id) {
        Some(x) if x.at_commit < r.at_commit => *x = r,
        Some(_) => {}
        None => list.push(r),
    }
}

/// Snapshot the current vocabulary. `warnings` collects representability
/// gaps (non-scalar defaults, non-literal list members) — reported, never
/// silently dropped.
pub fn read_vocab(conn: &Connection, warnings: &mut Vec<String>) -> Result<DbVocab, DbError> {
    let mut v = DbVocab::default();

    // --- classes ---
    {
        let mut stmt = conn.prepare(
            "SELECT id, name, description, active_by_default, required, display_order \
             FROM entity_class WHERE valid_to_commit IS NULL ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(DbClass {
                id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                active_by_default: r.get(3)?,
                required: r.get(4)?,
                display_order: r.get(5)?,
                dims: Vec::new(),
                params: BTreeMap::new(),
            })
        })?;
        for c in rows {
            let c = c?;
            v.classes.insert(fold(&c.name), c);
        }
    }
    // id -> folded name for joins below.
    let class_by_id: BTreeMap<i64, String> =
        v.classes.values().map(|c| (c.id, fold(&c.name))).collect();

    // --- class sets + members ---
    {
        let mut stmt = conn.prepare(
            "SELECT id, name, description FROM class_set \
             WHERE valid_to_commit IS NULL ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(DbClassSet {
                id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                members: Vec::new(),
            })
        })?;
        for s in rows {
            let s = s?;
            v.class_sets.insert(fold(&s.name), s);
        }
        let set_by_id: BTreeMap<i64, String> = v
            .class_sets
            .values()
            .map(|s| (s.id, fold(&s.name)))
            .collect();
        let mut mstmt = conn.prepare(
            "SELECT class_set_id, member_class_id FROM class_set_member \
             WHERE valid_to_commit IS NULL ORDER BY class_set_id, member_class_id",
        )?;
        let mrows = mstmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        for row in mrows {
            let (sid, cid) = row?;
            let (Some(sname), Some(cname)) = (set_by_id.get(&sid), class_by_id.get(&cid)) else {
                continue; // dangling — audit_dangling_ref territory
            };
            let member = v.classes[cname].name.clone();
            v.class_sets.get_mut(sname).unwrap().members.push(member);
        }
    }

    // --- dimensions ---
    {
        let set_by_id: BTreeMap<i64, String> = v
            .class_sets
            .values()
            .map(|s| (s.id, s.name.clone()))
            .collect();
        let cname_by_id: BTreeMap<i64, String> =
            v.classes.values().map(|c| (c.id, c.name.clone())).collect();
        let mut stmt = conn.prepare(
            "SELECT entity_class_id, position, dimension_class_id, dimension_class_set_id \
             FROM entity_class_dimension WHERE valid_to_commit IS NULL \
             ORDER BY entity_class_id, position",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<i64>>(3)?,
            ))
        })?;
        for row in rows {
            let (cid, _pos, dc, ds) = row?;
            let Some(cf) = class_by_id.get(&cid) else {
                continue;
            };
            let referent = match (dc, ds) {
                (Some(id), None) => cname_by_id.get(&id).cloned().map(DbDimRef::Class),
                (None, Some(id)) => set_by_id.get(&id).cloned().map(DbDimRef::Set),
                _ => None,
            };
            if let Some(rf) = referent {
                v.classes.get_mut(cf).unwrap().dims.push(rf);
            } else {
                warnings.push(format!(
                    "class id {cid}: dimension with dangling referent (see audit_dangling_ref)"
                ));
            }
        }
    }

    // --- units ---
    {
        let mut stmt = conn.prepare(
            "SELECT id, name, description, quantity_kind_link FROM units_of_measure \
             WHERE valid_to_commit IS NULL ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(DbUnit {
                id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                quantity_kind: r.get(3)?,
            })
        })?;
        for u in rows {
            let u = u?;
            v.units.insert(fold(&u.name), u);
        }
    }

    // --- value lists + members ---
    {
        let mut stmt = conn.prepare(
            "SELECT id, name, description FROM parameter_value_list \
             WHERE valid_to_commit IS NULL ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(DbValueList {
                id: r.get(0)?,
                name: r.get(1)?,
                description: r.get(2)?,
                members: Vec::new(),
                unrepresentable_members: 0,
            })
        })?;
        for l in rows {
            let l = l?;
            v.value_lists.insert(fold(&l.name), l);
        }
        let list_by_id: BTreeMap<i64, String> = v
            .value_lists
            .values()
            .map(|l| (l.id, fold(&l.name)))
            .collect();
        let mut mstmt = conn.prepare(
            "SELECT parameter_value_list_id, id, ordinal, description, value_kind, \
                    v_float, v_int, v_bool, v_str \
             FROM list_value WHERE valid_to_commit IS NULL \
             ORDER BY parameter_value_list_id, ordinal",
        )?;
        let mrows = mstmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<f64>>(5)?,
                r.get::<_, Option<i64>>(6)?,
                r.get::<_, Option<bool>>(7)?,
                r.get::<_, Option<String>>(8)?,
            ))
        })?;
        for row in mrows {
            let (lid, id, ordinal, description, kind, f, i, b, s) = row?;
            let Some(lf) = list_by_id.get(&lid) else {
                continue;
            };
            let list = v.value_lists.get_mut(lf).unwrap();
            match scalar_from_row(&kind, f, i, b, s) {
                Some(value) => list.members.push(DbListMember {
                    id,
                    ordinal,
                    value_kind: kind,
                    value,
                    description,
                }),
                None => {
                    list.unrepresentable_members += 1;
                    warnings.push(format!(
                        "value_list {:?}: member with kind {kind:?} has no v1 IR literal \
                         form — not diffable/exportable",
                        list.name
                    ));
                }
            }
        }
    }

    // --- parameter definitions + types + defaults ---
    {
        let list_name_by_id: BTreeMap<i64, String> = v
            .value_lists
            .values()
            .map(|l| (l.id, l.name.clone()))
            .collect();
        let unit_name_by_id: BTreeMap<i64, String> =
            v.units.values().map(|u| (u.id, u.name.clone())).collect();
        let mut stmt = conn.prepare(
            "SELECT id, entity_class_id, name, description, parameter_value_list_id, \
                    unit_id, required, display_order \
             FROM parameter_definition WHERE valid_to_commit IS NULL ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, Option<i64>>(5)?,
                r.get::<_, bool>(6)?,
                r.get::<_, Option<i64>>(7)?,
            ))
        })?;
        let mut pd_class: BTreeMap<i64, String> = BTreeMap::new();
        for row in rows {
            let (id, cid, name, description, list_id, unit_id, required, display_order) = row?;
            let Some(cf) = class_by_id.get(&cid) else {
                warnings.push(format!(
                    "parameter_definition {id} ({name:?}) references a non-current class \
                     (see audit_dangling_ref)"
                ));
                continue;
            };
            pd_class.insert(id, cf.clone());
            v.classes.get_mut(cf).unwrap().params.insert(
                fold(&name),
                DbParam {
                    id,
                    name,
                    description,
                    value_list: list_id.and_then(|i| list_name_by_id.get(&i).cloned()),
                    unit: unit_id.and_then(|i| unit_name_by_id.get(&i).cloned()),
                    required,
                    display_order,
                    types: Vec::new(),
                    default: None,
                    has_nonscalar_default: false,
                },
            );
        }

        // Type members — through the write path's OWN batch reader
        // (api::read_all_declared_types, review item 18): typed IndexKind
        // parse, unknown-tag detection, and position-contiguity validation
        // are the same code the entry check runs, so the snapshot cannot
        // interpret a stored declaration differently than a write would.
        for (pd_id, declared) in read_all_declared_types(conn)? {
            let Some(cf) = pd_class.get(&pd_id) else {
                continue;
            };
            let class = v.classes.get_mut(cf).unwrap();
            if let Some(p) = class.params.values_mut().find(|p| p.id == pd_id) {
                for d in declared {
                    p.types.push(DbTypeMember {
                        pt_id: d.pt_id,
                        value_kind: d.value_kind,
                        dims: d
                            .dim_names
                            .iter()
                            .zip(&d.shape.positions)
                            .map(|(n, (_, k))| (n.clone(), index_kind_tag(*k).to_string()))
                            .collect(),
                    });
                }
            }
        }

        // Scalar defaults (slot default_value) + non-scalar default detection.
        let mut dvstmt = conn.prepare(
            "SELECT parameter_definition_id, value_kind, v_float, v_int, v_bool, v_str \
             FROM definition_value_scalar \
             WHERE valid_to_commit IS NULL AND slot = 'default_value'",
        )?;
        let dvrows = dvstmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<f64>>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, Option<bool>>(4)?,
                r.get::<_, Option<String>>(5)?,
            ))
        })?;
        for row in dvrows {
            let (pd_id, kind, f, i, b, s) = row?;
            let Some(cf) = pd_class.get(&pd_id) else {
                continue;
            };
            let class = v.classes.get_mut(cf).unwrap();
            if let Some(p) = class.params.values_mut().find(|p| p.id == pd_id) {
                match scalar_from_row(&kind, f, i, b, s) {
                    Some(value) => {
                        p.default = Some(DbDefault {
                            value_kind: kind,
                            value,
                        })
                    }
                    None => {
                        p.has_nonscalar_default = true;
                        warnings.push(format!(
                            "parameter {:?}: default of kind {kind:?} has no v1 IR literal \
                             form — not diffable/exportable",
                            p.name
                        ));
                    }
                }
            }
        }
        for table in ["definition_value_long", "definition_value_array"] {
            let sql = format!(
                "SELECT DISTINCT parameter_definition_id FROM {table} \
                 WHERE valid_to_commit IS NULL AND slot = 'default_value'"
            );
            let mut nstmt = conn.prepare(&sql)?;
            let nrows = nstmt.query_map([], |r| r.get::<_, i64>(0))?;
            for row in nrows {
                let pd_id = row?;
                let Some(cf) = pd_class.get(&pd_id) else {
                    continue;
                };
                let class = v.classes.get_mut(cf).unwrap();
                if let Some(p) = class.params.values_mut().find(|p| p.id == pd_id) {
                    p.has_nonscalar_default = true;
                    warnings.push(format!(
                        "parameter {:?}: non-scalar default ({table}) — v1 IR cannot \
                         represent it; left untouched",
                        p.name
                    ));
                }
            }
        }
    }

    // --- entities + elements + base memberships ---
    {
        let cname_by_id: BTreeMap<i64, (String, String)> = v
            .classes
            .values()
            .map(|c| (c.id, (fold(&c.name), c.name.clone())))
            .collect();
        let mut stmt = conn.prepare(
            "SELECT id, entity_class_id, name, description FROM entity \
             WHERE valid_to_commit IS NULL ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })?;
        let mut ent_by_id: BTreeMap<i64, (String, String)> = BTreeMap::new();
        for row in rows {
            let (id, cid, name, description) = row?;
            let Some((cf, cname)) = cname_by_id.get(&cid) else {
                continue;
            };
            ent_by_id.insert(id, (cf.clone(), fold(&name)));
            v.entities.insert(
                (cf.clone(), fold(&name)),
                DbEntity {
                    id,
                    class: cname.clone(),
                    name,
                    description,
                    elements: Vec::new(),
                },
            );
        }
        let mut estmt = conn.prepare(
            "SELECT entity_id, position, element_entity_id FROM entity_element \
             WHERE valid_to_commit IS NULL ORDER BY entity_id, position",
        )?;
        let erows = estmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(2)?)))?;
        for row in erows {
            let (eid, el_id) = row?;
            let (Some(ek), Some(elk)) = (ent_by_id.get(&eid), ent_by_id.get(&el_id)) else {
                continue;
            };
            let el_name = v.entities[elk].name.clone();
            v.entities.get_mut(ek).unwrap().elements.push(el_name);
        }
        let mut mstmt = conn.prepare(
            "SELECT group_entity_id, member_entity_id, included FROM membership \
             WHERE valid_to_commit IS NULL AND alternative_id IS NULL \
             ORDER BY group_entity_id, member_entity_id",
        )?;
        let mrows = mstmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, bool>(2)?,
            ))
        })?;
        for row in mrows {
            let (gid, mid, included) = row?;
            let (Some(gk), Some(mk)) = (ent_by_id.get(&gid), ent_by_id.get(&mid)) else {
                continue;
            };
            let g = &v.entities[gk];
            let m = &v.entities[mk];
            v.memberships.push(DbMembership {
                group_class: g.class.clone(),
                group: g.name.clone(),
                member_class: m.class.clone(),
                member: m.name.clone(),
                included,
            });
        }
    }

    // --- renamed-away spellings (the reconcile's renamed-away blocker) ---
    // The vocabulary tables are small, so the closed rows are read whole.
    // Only spellings of live ids whose fold differs from the live name are
    // kept: a case-only rename frees no name.
    {
        let live_class: BTreeMap<i64, String> =
            v.classes.values().map(|c| (c.id, fold(&c.name))).collect();
        let mut stmt = conn.prepare(
            "SELECT id, name, max(valid_to_commit) FROM entity_class \
             WHERE valid_to_commit IS NOT NULL GROUP BY id, name ORDER BY id, name",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        for row in rows {
            let (id, old_name, at_commit) = row?;
            if live_class
                .get(&id)
                .is_some_and(|live| *live != fold(&old_name))
            {
                push_renamed(
                    v.renamed_classes.entry(fold(&old_name)).or_default(),
                    RenamedAway {
                        id,
                        old_name,
                        at_commit,
                    },
                );
            }
        }
        let live_param: BTreeMap<i64, String> = v
            .classes
            .values()
            .flat_map(|c| c.params.values().map(|p| (p.id, fold(&p.name))))
            .collect();
        let mut stmt = conn.prepare(
            "SELECT id, entity_class_id, name, max(valid_to_commit) FROM parameter_definition \
             WHERE valid_to_commit IS NOT NULL GROUP BY id, entity_class_id, name \
             ORDER BY id, name",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?;
        for row in rows {
            let (id, cid, old_name, at_commit) = row?;
            if live_param
                .get(&id)
                .is_some_and(|live| *live != fold(&old_name))
            {
                push_renamed(
                    v.renamed_parameters
                        .entry((cid, fold(&old_name)))
                        .or_default(),
                    RenamedAway {
                        id,
                        old_name,
                        at_commit,
                    },
                );
            }
        }
    }

    // --- removed-locally objects (the reconcile's removed-locally blocker,
    // V-OD1): read whole like the renamed-away spellings; the latest
    // closure of a fold decides (removed_locally). ---
    {
        let owned = template_owned(conn)?;
        for ((_, f), r) in removed_locally(conn, "entity_class", "NULL::BIGINT", &owned)? {
            v.removed_classes.insert(f, r);
        }
        for (k, r) in removed_locally(conn, "parameter_definition", "t.entity_class_id", &owned)? {
            v.removed_parameters.insert(k, r);
        }
        for ((_, f), r) in removed_locally(conn, "parameter_value_list", "NULL::BIGINT", &owned)? {
            v.removed_value_lists.insert(f, r);
        }
    }

    // --- metadata documents + identity ---
    v.template_name = property::read(conn, "template_name")?;
    v.template_version = property::read(conn, "template_version")?;
    v.template_content_hash = property::read(conn, "template_content_hash")?;
    v.schema_version = property::read(conn, "schema_version")?;
    v.audit_rules = property::read(conn, "audit_rules")?;
    if let Some(doc) = property::read(conn, "presentation")? {
        match serde_json::from_str::<serde_json::Value>(&doc) {
            Ok(val) => v.presentation = Some(val),
            Err(e) => warnings.push(format!(
                "presentation metadata document is not valid JSON ({e}); the template's \
                 group keys will replace it"
            )),
        }
    }

    Ok(v)
}
