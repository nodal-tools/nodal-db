//! Marshaling only (engine_portability_FINDINGS §7): Python objects <->
//! the core's plain-data types, and `DbError` -> the exception hierarchy.
//! Nothing in this file makes a decision the core does not already make —
//! every function is a mechanical re-shaping.

use std::collections::HashMap;

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::RecordBatch;
use arrow_pyarrow::FromPyArrow;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use nodal_db_core::api::{DeleteKey, Segment};
use nodal_db_core::canon::LabelDomain;
use nodal_db_core::compaction::CompactionReport;
use nodal_db_core::erasure::{ErasePreview, EraseReport};
use nodal_db_core::error::DbError;
use nodal_db_core::extract::ExtractReport;
use nodal_db_core::template::{ApplyOutcome, ReconcilePlan};
use nodal_db_core::types::{
    Blocker, CascadeCounts, EntityAddr, ObjectDeleteKey, ObjectDeleteOpts, ObjectDeleteOutcome,
    ObjectDeletePreview, ObjectDeleteResult, ObjectKind, ObjectRef, StructuralChange,
    StructuralEffect, StructuralKind, StructuralOutcome, TableCount, Tier, Warning, WarningKind,
    WriteReport,
};

use crate::{
    ApiError, ArrowError, Busy, CanonError, Conflict, DatabaseError, InternalError, SchemaError,
    VersionTooOld,
};

/// `DbError` -> the one exception hierarchy. Message text is the core's own
/// `Display` — the skin adds nothing and drops nothing.
pub(crate) fn db_err(e: DbError) -> PyErr {
    let msg = e.to_string();
    match e {
        DbError::Busy { .. } => Busy::new_err(msg),
        DbError::Conflict { .. } => Conflict::new_err(msg),
        DbError::Api(_) => ApiError::new_err(msg),
        DbError::VersionTooOld { .. } => VersionTooOld::new_err(msg),
        DbError::SchemaDir { .. } => SchemaError::new_err(msg),
        DbError::Duckdb(_) => DatabaseError::new_err(msg),
        DbError::Encode(_) | DbError::Canon(_) => CanonError::new_err(msg),
        DbError::Arrow(_) => ArrowError::new_err(msg),
        DbError::UnknownAxisKind { .. } | DbError::MalformedAxis { .. } => {
            InternalError::new_err(msg)
        }
    }
}

// ---------------------------------------------------------------------------
// Arrow in
// ---------------------------------------------------------------------------

/// Import any Arrow-bearing Python object as record batches, zero-copy via
/// the Arrow C data interface: a stream producer (`__arrow_c_stream__` —
/// `pyarrow.Table`, `pyarrow.RecordBatchReader`, polars frames, …) yields its
/// batches; anything else must be a single batch (`__arrow_c_array__` /
/// `pyarrow.RecordBatch`). The imported batches ARE the `duckdb::arrow`
/// `RecordBatch` type the core API takes (one arrow in the tree — the R2
/// pin), so they cross into the core with no conversion.
pub(crate) fn batches_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    if obj.hasattr("__arrow_c_stream__")? {
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(obj)?;
        let mut out = Vec::new();
        for b in reader {
            out.push(b.map_err(|e| ArrowError::new_err(e.to_string()))?);
        }
        Ok(out)
    } else {
        Ok(vec![RecordBatch::from_pyarrow_bound(obj)?])
    }
}

/// `{position: [label, ...]}` -> the core's ordinal label domains
/// (decision D's in-process channel; the Arrow field-metadata channel
/// `spinal:label_domain` passes through untouched on the batch itself).
pub(crate) fn domains_from_py(domains: Option<HashMap<u8, Vec<String>>>) -> Vec<LabelDomain> {
    domains
        .unwrap_or_default()
        .into_iter()
        .map(|(position, labels)| LabelDomain { position, labels })
        .collect()
}

/// One segment dict: `{"class": str, "parameter": str, "alternative": str,
/// "batch": <Arrow object>, "domains": {pos: [labels]}?}`.
pub(crate) fn segment_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Segment> {
    let get_str = |key: &str| -> PyResult<String> {
        obj.get_item(key)
            .map_err(|_| ApiError::new_err(format!("segment is missing key {key:?}")))?
            .extract::<String>()
    };
    let batch_obj = obj
        .get_item("batch")
        .map_err(|_| ApiError::new_err("segment is missing key \"batch\""))?;
    let domains: Option<HashMap<u8, Vec<String>>> = match obj.get_item("domains") {
        Ok(d) if !d.is_none() => Some(d.extract()?),
        _ => None,
    };
    Ok(Segment {
        class: get_str("class")?,
        parameter: get_str("parameter")?,
        alternative: get_str("alternative")?,
        batches: batches_from_py(&batch_obj)?,
        domains: domains_from_py(domains),
    })
}

/// G8 verb type declarations (`ensure_parameter_definition`): a list of
/// member dicts `{"kind": str, "dims"?: [{"name": str, "kind": str}]}` —
/// the IR's `types:` shape 1:1. Validation (legal kinds, D3 dim rules,
/// distinguishability) lives in the core verb, not here.
pub(crate) fn type_decls_from_py(
    types: Option<Vec<Bound<'_, PyAny>>>,
) -> PyResult<Vec<nodal_db_core::template::ir::TypeDecl>> {
    use nodal_db_core::template::ir::{DimSpec, TypeDecl};
    let mut out = Vec::new();
    for (i, obj) in types.unwrap_or_default().iter().enumerate() {
        let kind: String = obj
            .get_item("kind")
            .map_err(|_| ApiError::new_err(format!("types[{i}] is missing key \"kind\"")))?
            .extract()?;
        let dims = match obj.get_item("dims") {
            Ok(d) if !d.is_none() => {
                let mut ds = Vec::new();
                for (j, dim) in d.try_iter()?.enumerate() {
                    let dim = dim?;
                    let name: String = dim
                        .get_item("name")
                        .map_err(|_| {
                            ApiError::new_err(format!(
                                "types[{i}].dims[{j}] is missing key \"name\""
                            ))
                        })?
                        .extract()?;
                    let dkind: String = dim
                        .get_item("kind")
                        .map_err(|_| {
                            ApiError::new_err(format!(
                                "types[{i}].dims[{j}] is missing key \"kind\""
                            ))
                        })?
                        .extract()?;
                    ds.push(DimSpec { name, kind: dkind });
                }
                Some(ds)
            }
            _ => None,
        };
        out.push(TypeDecl { kind, dims });
    }
    Ok(out)
}

/// A value-delete key: a `(class, parameter, alternative, entity)` 4-tuple
/// (or any sequence of 4 strings).
pub(crate) fn delete_key_from_py(obj: &Bound<'_, PyAny>) -> PyResult<DeleteKey> {
    let (class, parameter, alternative, entity): (String, String, String, String) =
        obj.extract().map_err(|_| {
            ApiError::new_err(
                "delete key must be a (class, parameter, alternative, entity) tuple of strings",
            )
        })?;
    Ok(DeleteKey {
        class,
        parameter,
        alternative,
        entity,
    })
}

fn object_kind_from_str(s: &str) -> PyResult<ObjectKind> {
    Ok(match s {
        "entity" => ObjectKind::Entity,
        "parameter" => ObjectKind::Parameter,
        "alternative" => ObjectKind::Alternative,
        "class" => ObjectKind::Class,
        other => {
            return Err(ApiError::new_err(format!(
                "unknown object kind {other:?} (expected entity/parameter/alternative/class)"
            )))
        }
    })
}

/// An object-delete key dict: `{"kind": "entity"|"parameter"|"alternative"|
/// "class", "class": str?, "name": str}` or, for an N-D entity,
/// `"elements": [str, ...]` instead of `"name"`. The exactly-one-of
/// name/elements rule mirrors the core's `ObjectRef` enum.
pub(crate) fn object_key_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ObjectDeleteKey> {
    let kind = object_kind_from_str(
        &obj.get_item("kind")
            .map_err(|_| ApiError::new_err("object key is missing key \"kind\""))?
            .extract::<String>()?,
    )?;
    let class: Option<String> = match obj.get_item("class") {
        Ok(c) if !c.is_none() => Some(c.extract()?),
        _ => None,
    };
    let name: Option<String> = match obj.get_item("name") {
        Ok(n) if !n.is_none() => Some(n.extract()?),
        _ => None,
    };
    let elements: Option<Vec<String>> = match obj.get_item("elements") {
        Ok(e) if !e.is_none() => Some(e.extract()?),
        _ => None,
    };
    let target = match (name, elements) {
        (Some(n), None) => ObjectRef::Name(n),
        (None, Some(e)) => ObjectRef::Elements(e),
        _ => {
            return Err(ApiError::new_err(
                "object key must carry exactly one of \"name\" or \"elements\"",
            ))
        }
    };
    Ok(ObjectDeleteKey {
        kind,
        class,
        target,
    })
}

/// The commit-side variant: same dict plus `"cascade": bool` (default false).
pub(crate) fn object_delete_from_py(
    obj: &Bound<'_, PyAny>,
) -> PyResult<(ObjectDeleteKey, ObjectDeleteOpts)> {
    let key = object_key_from_py(obj)?;
    let cascade: bool = match obj.get_item("cascade") {
        Ok(c) if !c.is_none() => c.extract()?,
        _ => false,
    };
    Ok((key, ObjectDeleteOpts { cascade }))
}

// ---------------------------------------------------------------------------
// Structural changes in (structural_write_api_plan §6). Each dict's `"op"`
// discriminator maps 1:1 to a `StructuralChange` variant. No decision here —
// every rule (strict resolution, A-min, same-commit guard) lives in the core.
// ---------------------------------------------------------------------------

fn req_str(obj: &Bound<'_, PyAny>, op: &str, key: &str) -> PyResult<String> {
    obj.get_item(key)
        .map_err(|_| ApiError::new_err(format!("structural {op:?} is missing key {key:?}")))?
        .extract::<String>()
}

fn opt_str(obj: &Bound<'_, PyAny>, key: &str) -> PyResult<Option<String>> {
    match obj.get_item(key) {
        Ok(v) if !v.is_none() => Ok(Some(v.extract()?)),
        _ => Ok(None),
    }
}

fn opt_bool(obj: &Bound<'_, PyAny>, key: &str) -> PyResult<Option<bool>> {
    match obj.get_item(key) {
        Ok(v) if !v.is_none() => Ok(Some(v.extract()?)),
        _ => Ok(None),
    }
}

fn str_list(obj: &Bound<'_, PyAny>, key: &str) -> PyResult<Vec<String>> {
    match obj.get_item(key) {
        Ok(v) if !v.is_none() => Ok(v.extract()?),
        _ => Ok(Vec::new()),
    }
}

/// An entity address: a dict `{"class": str, "name": str}` or a `(class, name)`
/// 2-tuple.
fn entity_addr_from_py(obj: &Bound<'_, PyAny>) -> PyResult<EntityAddr> {
    if let Ok((class, name)) = obj.extract::<(String, String)>() {
        return Ok(EntityAddr { class, name });
    }
    Ok(EntityAddr {
        class: req_str(obj, "entity_addr", "class")?,
        name: req_str(obj, "entity_addr", "name")?,
    })
}

/// Collect an optional iterable of `"op"`-keyed dicts into `StructuralChange`s.
pub(crate) fn structural_changes_from_py(
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<StructuralChange>> {
    let mut out = Vec::new();
    if let Some(items) = obj {
        for item in items.try_iter()? {
            out.push(structural_change_from_py(&item?)?);
        }
    }
    Ok(out)
}

/// One structural change dict, keyed on `"op"`.
pub(crate) fn structural_change_from_py(obj: &Bound<'_, PyAny>) -> PyResult<StructuralChange> {
    let op: String = obj
        .get_item("op")
        .map_err(|_| ApiError::new_err("structural change is missing key \"op\""))?
        .extract()?;
    Ok(match op.as_str() {
        "create_alternative" => StructuralChange::CreateAlternative {
            name: req_str(obj, &op, "name")?,
            description: opt_str(obj, "description")?,
        },
        "rename_alternative" => StructuralChange::RenameAlternative {
            current: req_str(obj, &op, "current")?,
            new_name: req_str(obj, &op, "new_name")?,
            description: opt_str(obj, "description")?,
        },
        "create_scenario" => StructuralChange::CreateScenario {
            name: req_str(obj, &op, "name")?,
            description: opt_str(obj, "description")?,
        },
        "rename_scenario" => StructuralChange::RenameScenario {
            current: req_str(obj, &op, "current")?,
            new_name: req_str(obj, &op, "new_name")?,
            description: opt_str(obj, "description")?,
        },
        "set_scenario_alternatives" => StructuralChange::SetScenarioAlternatives {
            scenario: req_str(obj, &op, "scenario")?,
            alternatives: str_list(obj, "alternatives")?,
        },
        "set_entity_activation" => StructuralChange::SetEntityActivation {
            class: req_str(obj, &op, "class")?,
            entity: req_str(obj, &op, "entity")?,
            alternative: req_str(obj, &op, "alternative")?,
            active: opt_bool(obj, "active")?,
        },
        "create_entity" => StructuralChange::CreateEntity {
            class: req_str(obj, &op, "class")?,
            name: opt_str(obj, "name")?,
            elements: str_list(obj, "elements")?,
        },
        "create_entity_class" => StructuralChange::CreateEntityClass {
            name: req_str(obj, &op, "name")?,
            dims: str_list(obj, "dims")?,
        },
        "create_parameter" => {
            let types = match obj.get_item("types") {
                Ok(t) if !t.is_none() => Some(t.try_iter()?.collect::<PyResult<Vec<_>>>()?),
                _ => None,
            };
            StructuralChange::CreateParameter {
                class: req_str(obj, &op, "class")?,
                name: req_str(obj, &op, "name")?,
                types: type_decls_from_py(types)?,
                value_list: opt_str(obj, "value_list")?,
                description: opt_str(obj, "description")?,
            }
        }
        "set_membership" => {
            let group = entity_addr_from_py(
                &obj.get_item("group")
                    .map_err(|_| ApiError::new_err("set_membership is missing key \"group\""))?,
            )?;
            let member = entity_addr_from_py(
                &obj.get_item("member")
                    .map_err(|_| ApiError::new_err("set_membership is missing key \"member\""))?,
            )?;
            StructuralChange::SetMembership {
                group,
                member,
                alternative: opt_str(obj, "alternative")?,
                included: opt_bool(obj, "included")?,
            }
        }
        other => {
            return Err(ApiError::new_err(format!(
                "unknown structural op {other:?}"
            )))
        }
    })
}

// ---------------------------------------------------------------------------
// Reports out (plain dicts/lists — no wrapper classes to hold on to)
// ---------------------------------------------------------------------------

fn tier_tag(t: Tier) -> &'static str {
    match t {
        Tier::Scalar => "scalar",
        Tier::Array => "array",
        Tier::Long => "long",
    }
}

fn warning_to_py<'py>(py: Python<'py>, w: &Warning) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    let kind = match &w.kind {
        WarningKind::AutoCreated => "auto_created",
        WarningKind::TimezoneNormalized { column, tz } => {
            d.set_item("column", column)?;
            d.set_item("tz", tz)?;
            "timezone_normalized"
        }
        WarningKind::UnknownReferent { key_index, level } => {
            d.set_item("key_index", key_index)?;
            d.set_item("level", level)?;
            "unknown_referent"
        }
        WarningKind::NothingToDelete { key_index } => {
            d.set_item("key_index", key_index)?;
            "nothing_to_delete"
        }
    };
    d.set_item("kind", kind)?;
    d.set_item("object_type", &w.object_type)?;
    d.set_item("name", &w.name)?;
    d.set_item("id", w.id)?;
    Ok(d)
}

fn table_counts_to_py<'py>(py: Python<'py>, tc: &[TableCount]) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for t in tc {
        let d = PyDict::new(py);
        d.set_item("table", &t.table)?;
        d.set_item("rows", t.rows)?;
        out.append(d)?;
    }
    Ok(out)
}

fn cascade_to_py<'py>(py: Python<'py>, c: &CascadeCounts) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("object_rows", c.object_rows)?;
    d.set_item("per_table", table_counts_to_py(py, &c.per_table)?)?;
    d.set_item("total_rows", c.total_rows)?;
    Ok(d)
}

fn blockers_to_py<'py>(py: Python<'py>, bs: &[Blocker]) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for b in bs {
        let d = PyDict::new(py);
        d.set_item("table", &b.table)?;
        d.set_item("column", &b.column)?;
        d.set_item("count", b.count)?;
        d.set_item("sample", &b.sample)?;
        out.append(d)?;
    }
    Ok(out)
}

fn object_delete_outcome_to_py<'py>(
    py: Python<'py>,
    o: &ObjectDeleteOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("key_index", o.key_index)?;
    d.set_item("kind", o.kind.tag())?;
    match &o.result {
        ObjectDeleteResult::Deleted { closed_out } => {
            d.set_item("status", "deleted")?;
            d.set_item("closed_out", cascade_to_py(py, closed_out)?)?;
        }
        ObjectDeleteResult::Restricted { blockers } => {
            d.set_item("status", "restricted")?;
            d.set_item("blockers", blockers_to_py(py, blockers)?)?;
        }
        ObjectDeleteResult::NotFound => {
            d.set_item("status", "not_found")?;
        }
    }
    Ok(d)
}

fn structural_kind_tag(k: StructuralKind) -> &'static str {
    match k {
        StructuralKind::Alternative => "alternative",
        StructuralKind::Scenario => "scenario",
        StructuralKind::ScenarioRanking => "scenario_ranking",
        StructuralKind::Activation => "activation",
        StructuralKind::Membership => "membership",
        StructuralKind::Entity => "entity",
        StructuralKind::EntityClass => "entity_class",
        StructuralKind::Parameter => "parameter",
    }
}

fn structural_effect_tag(e: StructuralEffect) -> &'static str {
    match e {
        StructuralEffect::Created => "created",
        StructuralEffect::Renamed => "renamed",
        StructuralEffect::Updated => "updated",
        StructuralEffect::NoOp => "no_op",
        StructuralEffect::Cleared => "cleared",
    }
}

fn structural_outcome_to_py<'py>(
    py: Python<'py>,
    o: &StructuralOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("change_index", o.change_index)?;
    d.set_item("kind", structural_kind_tag(o.kind))?;
    d.set_item("object_id", o.object_id)?;
    d.set_item("effect", structural_effect_tag(o.effect))?;
    Ok(d)
}

pub(crate) fn report_to_py<'py>(py: Python<'py>, r: &WriteReport) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("commit_id", r.commit_id)?;
    d.set_item("values_written", r.values_written)?;
    d.set_item("rows_written", r.rows_written)?;
    d.set_item("rows_closed", r.rows_closed)?;
    d.set_item("axes_created", r.axes_created)?;
    d.set_item("axes_reused", r.axes_reused)?;
    d.set_item(
        "tiers",
        r.tiers.iter().map(|t| tier_tag(*t)).collect::<Vec<_>>(),
    )?;
    let warnings = PyList::empty(py);
    for w in &r.warnings {
        warnings.append(warning_to_py(py, w)?)?;
    }
    d.set_item("warnings", warnings)?;
    let odos = PyList::empty(py);
    for o in &r.object_deletes {
        odos.append(object_delete_outcome_to_py(py, o)?)?;
    }
    d.set_item("object_deletes", odos)?;
    let structs = PyList::empty(py);
    for o in &r.structural {
        structs.append(structural_outcome_to_py(py, o)?)?;
    }
    d.set_item("structural", structs)?;
    Ok(d)
}

pub(crate) fn object_previews_to_py<'py>(
    py: Python<'py>,
    ps: &[ObjectDeletePreview],
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for p in ps {
        let d = PyDict::new(py);
        d.set_item("key_index", p.key_index)?;
        d.set_item("kind", p.kind.tag())?;
        d.set_item("found", p.found)?;
        d.set_item("cascade", cascade_to_py(py, &p.cascade)?)?;
        d.set_item("blockers", blockers_to_py(py, &p.blockers)?)?;
        out.append(d)?;
    }
    Ok(out)
}

pub(crate) fn erase_preview_to_py<'py>(
    py: Python<'py>,
    p: &ErasePreview,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("found", p.found)?;
    d.set_item("alternative_id", p.alternative_id)?;
    d.set_item("alternative_name", &p.alternative_name)?;
    d.set_item("per_table", table_counts_to_py(py, &p.per_table)?)?;
    d.set_item("rows_total", p.rows_total)?;
    d.set_item("bytes_estimate", p.bytes_estimate)?;
    d.set_item("still_current", p.still_current)?;
    let scen = PyList::empty(py);
    for s in &p.referencing_scenarios {
        let sd = PyDict::new(py);
        sd.set_item("id", s.id)?;
        sd.set_item("name", &s.name)?;
        scen.append(sd)?;
    }
    d.set_item("referencing_scenarios", scen)?;
    d.set_item("referencing_runs", &p.referencing_runs)?;
    d.set_item("expect_present_problems", &p.expect_present_problems)?;
    Ok(d)
}

pub(crate) fn erase_report_to_py<'py>(
    py: Python<'py>,
    r: &EraseReport,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("erased", r.erased)?;
    d.set_item("marker_id", r.marker_id)?;
    d.set_item("commit_id", r.commit_id)?;
    d.set_item("alternative_id", r.alternative_id)?;
    d.set_item("identity_closed", r.identity_closed)?;
    d.set_item("per_table", table_counts_to_py(py, &r.per_table)?)?;
    d.set_item("rows_total", r.rows_total)?;
    d.set_item("bytes_estimate", r.bytes_estimate)?;
    Ok(d)
}

pub(crate) fn extract_report_to_py<'py>(
    py: Python<'py>,
    r: &ExtractReport,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("source_identity", &r.source_identity)?;
    d.set_item("extract_identity", &r.extract_identity)?;
    d.set_item("horizon_commit", r.horizon_commit)?;
    d.set_item("extract_commit", r.extract_commit)?;
    d.set_item("marker_id", r.marker_id)?;
    let rows = PyDict::new(py);
    for (t, n) in &r.rows_per_table {
        rows.set_item(t, n)?;
    }
    d.set_item("rows_per_table", rows)?;
    d.set_item("rows_removed", r.rows_removed)?;
    d.set_item("axes_dropped", r.axes_dropped)?;
    d.set_item("bytes_source", r.bytes_source)?;
    d.set_item("bytes_extract", r.bytes_extract)?;
    Ok(d)
}

pub(crate) fn compaction_report_to_py<'py>(
    py: Python<'py>,
    r: &CompactionReport,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("swapped", r.swapped)?;
    d.set_item("bytes_before", r.bytes_before)?;
    d.set_item("bytes_after", r.bytes_after)?;
    d.set_item("blocks_before", r.blocks_before)?;
    d.set_item("blocks_after", r.blocks_after)?;
    let rows = PyDict::new(py);
    for (t, n) in &r.rows_per_table {
        rows.set_item(t, n)?;
    }
    d.set_item("rows_per_table", rows)?;
    Ok(d)
}

pub(crate) fn plan_to_py<'py>(py: Python<'py>, p: &ReconcilePlan) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("template_name", &p.template_name)?;
    d.set_item("template_version", &p.template_version)?;
    d.set_item("content_hash", &p.content_hash)?;
    d.set_item("db_template_name", &p.db_template_name)?;
    d.set_item("db_template_version", &p.db_template_version)?;
    // Quote this to `template_apply(expect_commit=...)` to pin plan -> apply.
    d.set_item("db_commit", p.db_commit)?;
    d.set_item("no_op", p.no_op)?;
    let migs = PyList::empty(py);
    for m in &p.pending_migrations {
        let md = PyDict::new(py);
        md.set_item("from", &m.from)?;
        md.set_item("to", &m.to)?;
        md.set_item("steps", &m.steps)?;
        migs.append(md)?;
    }
    d.set_item("pending_migrations", migs)?;
    d.set_item(
        "actions",
        p.actions.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
    )?;
    d.set_item("blocked", &p.blocked)?;
    d.set_item("skipped", &p.skipped)?;
    d.set_item("warnings", &p.warnings)?;
    d.set_item("notes", &p.notes)?;
    // The core's own human rendering, verbatim.
    d.set_item("text", p.to_string())?;
    Ok(d)
}

pub(crate) fn apply_outcome_to_py<'py>(
    py: Python<'py>,
    o: &ApplyOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("no_op", o.no_op)?;
    d.set_item("commits", &o.commits)?;
    let runs = PyList::empty(py);
    for r in &o.run_programs {
        let rd = PyDict::new(py);
        rd.set_item("program", &r.program)?;
        rd.set_item("commits", &r.commits)?;
        runs.append(rd)?;
    }
    d.set_item("run_programs", runs)?;
    d.set_item("converged", o.converged)?;
    d.set_item("warnings", &o.warnings)?;
    d.set_item("plan", plan_to_py(py, &o.plan)?)?;
    Ok(d)
}
