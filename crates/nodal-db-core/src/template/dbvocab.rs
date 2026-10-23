//! Read the database's CURRENT vocabulary into an in-memory snapshot — the
//! "DB side" of the reconcile diff (§4.2 Layer 1) and the source `template
//! export` re-emits from (§8.1.7). Reads current rows only (`valid_to_commit
//! IS NULL`); history never participates in a diff.
//!
//! Runs on an already-open connection so the reconcile apply can re-read
//! inside its own write transaction (fresh state per conflict retry), and
//! `plan`/`export` can read under one per-operation RO connection.

use std::collections::BTreeMap;

use duckdb::{params, Connection};

use crate::api::{index_kind_tag, read_all_declared_types};
use crate::error::DbError;
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
                .map(|(n, k)| (n.to_ascii_lowercase(), k.clone()))
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

/// Read one reserved single-valued metadata key's current value.
pub fn read_metadata_key(conn: &Connection, key: &str) -> Result<Option<String>, DbError> {
    let r = conn.query_row(
        "SELECT value FROM metadata WHERE cur_name = lower(?)",
        params![key],
        |r| r.get::<_, String>(0),
    );
    match r {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
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

    // --- metadata documents + identity ---
    v.template_name = read_metadata_key(conn, "template_name")?;
    v.template_version = read_metadata_key(conn, "template_version")?;
    v.template_content_hash = read_metadata_key(conn, "template_content_hash")?;
    v.schema_version = read_metadata_key(conn, "schema_version")?;
    v.audit_rules = read_metadata_key(conn, "audit_rules")?;
    if let Some(doc) = read_metadata_key(conn, "presentation")? {
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
