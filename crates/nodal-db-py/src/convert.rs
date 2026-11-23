//! Marshaling only: Python objects <->
//! the core's plain-data types, and `DbError` -> the exception hierarchy.
//! Nothing in this file makes a decision the core does not already make —
//! every function is a mechanical re-shaping.

use std::collections::HashMap;

use arrow::ffi_stream::RecordBatchStreamReader;
use arrow::record_batch::RecordBatch;
use arrow_pyarrow::FromPyArrow;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use nodal_db_core::api::{DeleteKey, Segment};
use nodal_db_core::canon::LabelDomain;
use nodal_db_core::compaction::{CompactionReport, CompactionTrigger};
use nodal_db_core::erasure::{ErasePreview, EraseReport};
use nodal_db_core::error::DbError;
use nodal_db_core::extract::ExtractReport;
use nodal_db_core::read_surface::{SurfaceReport, SurfaceVersion};
use nodal_db_core::template::{ApplyOutcome, ReconcilePlan};
use nodal_db_core::types::{
    Action, Blocker, CascadeCounts, CoveredWrites, CoveringDelete, EntityAddr, LicenseFields,
    LinkSpec, MetadataChange, MetadataEffect, MetadataKind, MetadataOutcome, ObjectDeleteKey,
    ObjectDeleteOpts, ObjectDeleteOutcome, ObjectDeletePreview, ObjectDeleteResult, ObjectKind,
    ObjectRef, PairRef, PurgeOutcome, PurgePreview, PurgeRequest, PurgeScope, Ref, RoleSel, Scope,
    SourceFields, StructuralChange, StructuralEffect, StructuralKind, StructuralOutcome, Subject,
    TableCount, Target, Tier, ValueAddr, ValueLinkCounts, Warning, WarningKind, WriteReport,
};

use crate::{
    ApiError, ArrowError, Busy, CanonError, Conflict, CoveredWrite, DatabaseError, InternalError,
    ReadSurfaceStale, SchemaError, SchemaMismatch, ShadowedBuiltin, Stale, TemplateStampedPurge,
    TemplateStampedRename, UndeclaredValueType, UnreplayableWal, ValueMetadataUnstated,
    VersionTooOld, WriteAndDeleteSameKey,
};

/// `DbError` -> the one exception hierarchy. Message text is the core's own
/// `Display` — the skin adds nothing and drops nothing.
pub(crate) fn db_err(e: DbError) -> PyErr {
    let msg = e.to_string();
    match e {
        DbError::Busy { .. } => Busy::new_err(msg),
        DbError::Conflict { .. } => Conflict::new_err(msg),
        DbError::Api(_) => ApiError::new_err(msg),
        DbError::ValueMetadataUnstated { count, sample, .. } => {
            value_metadata_unstated_err(msg, count, sample)
        }
        DbError::Stale { expected, found } => stale_err(msg, expected, found),
        DbError::CoveredWrite(d) => {
            covered_write_err(msg, d.key_index, d.kind, d.target, d.class, d.resolved)
        }
        DbError::WriteAndDeleteSameKey(d) => write_and_delete_same_key_err(
            msg,
            d.key_index,
            d.class,
            d.parameter,
            d.alternative,
            d.entity,
            d.resolved,
        ),
        DbError::SchemaMismatch {
            found, expected, ..
        } => schema_mismatch_err(msg, found, expected),
        DbError::ReadSurfaceStale {
            found, expected, ..
        } => read_surface_stale_err(msg, found, expected),
        DbError::VersionTooOld { .. } => VersionTooOld::new_err(msg),
        DbError::SchemaDir { .. } => SchemaError::new_err(msg),
        DbError::UnreplayableWal { wal, cause } => unreplayable_wal_err(msg, wal, cause),
        DbError::TemplateStampedRename(d) => template_stamped_rename_err(msg, *d),
        DbError::TemplateStampedPurge(d) => template_stamped_purge_err(msg, *d),
        DbError::UndeclaredValueType(d) => undeclared_value_type_err(msg, *d),
        DbError::ShadowedBuiltin { name, schema, kind } => {
            shadowed_builtin_err(msg, name, schema, kind)
        }
        DbError::Duckdb(_) => DatabaseError::new_err(msg),
        DbError::Encode(_) | DbError::Canon(_) => CanonError::new_err(msg),
        DbError::Arrow(_) => ArrowError::new_err(msg),
        DbError::UnknownAxisKind { .. } | DbError::MalformedAxis { .. } | DbError::Internal(_) => {
            InternalError::new_err(msg)
        }
    }
}

/// A `Stale` exception carrying `expected` / `found` as attributes.
fn stale_err(msg: String, expected: i64, found: i64) -> PyErr {
    let err = Stale::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message carries both ids in any case.
        let _ = v.setattr("expected", expected);
        let _ = v.setattr("found", found);
    });
    err
}

/// A `CoveredWrite` exception carrying the covering object-delete key
/// and the covered write's resolved ids (when known) as attributes.
fn covered_write_err(
    msg: String,
    key_index: usize,
    kind: ObjectKind,
    target: String,
    class: Option<String>,
    resolved: Option<(i64, i64, i64)>,
) -> PyErr {
    let err = CoveredWrite::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message carries the same information in any case.
        let _ = v.setattr("key_index", key_index);
        let _ = v.setattr("kind", kind.tag());
        let _ = v.setattr("target", target);
        // "class_name", not "class": the latter is a Python keyword, so
        // `err.class` would be a syntax error (`getattr(err, "class")`
        // still works, but every other class-bearing attribute in this
        // skin is already named "class_name" for the same reason).
        let _ = v.setattr("class_name", class);
        let _ = v.setattr("resolved", resolved);
    });
    err
}

/// A `WriteAndDeleteSameKey` exception carrying the colliding delete
/// key and its resolved ids (when known) as attributes.
fn write_and_delete_same_key_err(
    msg: String,
    key_index: usize,
    class: String,
    parameter: String,
    alternative: String,
    entity: String,
    resolved: Option<(i64, i64, i64)>,
) -> PyErr {
    let err = WriteAndDeleteSameKey::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        let _ = v.setattr("key_index", key_index);
        let _ = v.setattr("class_name", class);
        let _ = v.setattr("parameter", parameter);
        let _ = v.setattr("alternative", alternative);
        let _ = v.setattr("entity", entity);
        let _ = v.setattr("resolved", resolved);
    });
    err
}

/// An `UndeclaredValueType` exception carrying the refused group's target,
/// first entity, entity count and type, and the declared member types.
fn undeclared_value_type_err(
    msg: String,
    d: nodal_db_core::error::UndeclaredValueTypeDetails,
) -> PyErr {
    let err = UndeclaredValueType::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message carries the same information in any case.
        let _ = v.setattr("segment_index", d.segment_index);
        let _ = v.setattr("class_name", d.class);
        let _ = v.setattr("parameter", d.parameter);
        let _ = v.setattr("alternative", d.alternative);
        let _ = v.setattr("entity", d.entity);
        let _ = v.setattr("entity_count", d.entity_count);
        let _ = v.setattr("value_kind", d.value_kind);
        let _ = v.setattr("dimensions", d.dimensions);
        let _ = v.setattr("declared", d.declared);
    });
    err
}

/// A `SchemaMismatch` exception carrying `found` / `expected` (the file's
/// and this build's `schema_version`) as attributes.
fn schema_mismatch_err(msg: String, found: String, expected: String) -> PyErr {
    let err = SchemaMismatch::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message carries both versions in any case.
        let _ = v.setattr("found", found);
        let _ = v.setattr("expected", expected);
    });
    err
}

/// An `UnreplayableWal` exception carrying `wal` (the WAL's path, as a str)
/// and `cause` as attributes.
fn unreplayable_wal_err(msg: String, wal: std::path::PathBuf, cause: String) -> PyErr {
    let err = UnreplayableWal::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message carries both in any case.
        let _ = v.setattr("wal", wal.display().to_string());
        let _ = v.setattr("cause", cause);
    });
    err
}

/// A `ShadowedBuiltin` exception carrying `name`, `schema` and `kind` as
/// attributes.
fn shadowed_builtin_err(msg: String, name: String, schema: String, kind: String) -> PyErr {
    let err = ShadowedBuiltin::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message carries all three in any case.
        let _ = v.setattr("name", name);
        let _ = v.setattr("schema", schema);
        let _ = v.setattr("kind", kind);
    });
    err
}

/// A `TemplateStampedRename` exception carrying the stamped template and
/// every refused rename (`renames`: dicts) as attributes.
fn template_stamped_rename_err(
    msg: String,
    d: nodal_db_core::error::TemplateStampedRenameDetails,
) -> PyErr {
    let err = TemplateStampedRename::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message lists the same items in any case.
        let _ = v.setattr("template_name", d.template_name);
        let _ = v.setattr("template_version", d.template_version);
        let renames = PyList::empty(py);
        for r in d.renames {
            let item = PyDict::new(py);
            let _ = item.set_item("change_index", r.change_index);
            let _ = item.set_item("kind", structural_kind_tag(r.kind));
            // A dict key, not an attribute: "class" (as in the structural
            // item it echoes) is fine here, unlike `err.class`.
            let _ = item.set_item("class", r.class);
            let _ = item.set_item("current", r.current);
            let _ = item.set_item("new_name", r.new_name);
            let _ = renames.append(item);
        }
        let _ = v.setattr("renames", renames);
    });
    err
}

/// A `TemplateStampedPurge` exception carrying the stamped template and
/// every refused request (`requests`: dicts) as attributes.
fn template_stamped_purge_err(
    msg: String,
    d: nodal_db_core::error::TemplateStampedPurgeDetails,
) -> PyErr {
    let err = TemplateStampedPurge::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        let _ = v.setattr("template_name", d.template_name);
        let _ = v.setattr("template_version", d.template_version);
        let requests = PyList::empty(py);
        for r in d.requests {
            let item = PyDict::new(py);
            let _ = item.set_item("request_index", r.request_index);
            let _ = item.set_item("classes", r.classes);
            let _ = item.set_item("parameters", r.parameters);
            let _ = item.set_item("value_lists", r.value_lists);
            let _ = item.set_item("sample", r.sample);
            let _ = requests.append(item);
        }
        let _ = v.setattr("requests", requests);
    });
    err
}

/// A `ReadSurfaceStale` exception carrying `found` (the file's read-surface
/// version, None when it predates versioning) / `expected` (this build's).
fn read_surface_stale_err(msg: String, found: Option<String>, expected: String) -> PyErr {
    let err = ReadSurfaceStale::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        let _ = v.setattr("found", found);
        let _ = v.setattr("expected", expected);
    });
    err
}

/// A read-surface version as `{"contract_major", "revision", "ddl_hash"}`.
fn surface_version_to_py<'py>(py: Python<'py>, v: &SurfaceVersion) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("contract_major", v.contract_major)?;
    d.set_item("revision", v.revision)?;
    d.set_item("ddl_hash", &v.ddl_hash)?;
    Ok(d)
}

/// `read_surface_status()` / `refresh_read_surface()` result dict.
pub(crate) fn surface_report_to_py<'py>(
    py: Python<'py>,
    r: &SurfaceReport,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("schema_version", r.schema_version.as_deref())?;
    d.set_item("status", r.check.status.as_str())?;
    let found = match &r.check.found {
        Some(v) => Some(surface_version_to_py(py, v)?),
        None => None,
    };
    d.set_item("found", found)?;
    d.set_item("expected", surface_version_to_py(py, &r.check.expected)?)?;
    d.set_item("refreshed", r.refreshed)?;
    Ok(d)
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
///
/// A `list` or `tuple` of such objects yields all their batches in order:
/// the way to hand one segment batches of different schemas (one parameter,
/// values of several declared types in one call — the core groups the
/// batches by column types and binds each group to its declared member).
pub(crate) fn batches_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        let mut out = Vec::new();
        for item in obj.try_iter()? {
            let item = item?;
            if item.is_instance_of::<PyList>() || item.is_instance_of::<PyTuple>() {
                return Err(ApiError::new_err(
                    "a list of batches must hold Arrow objects, not nested lists",
                ));
            }
            out.extend(batches_from_py(&item)?);
        }
        return Ok(out);
    }
    if obj.hasattr("__arrow_c_stream__")? {
        let reader = RecordBatchStreamReader::from_pyarrow_bound(obj)?;
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
/// (the in-process channel; the Arrow field-metadata channel
/// `nodal:label_domain` passes through untouched on the batch itself).
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

/// Verb type declarations (`ensure_parameter`): a list of
/// member dicts `{"kind": str, "dims"?: [{"name": str, "kind": str}]}` —
/// the IR's `types:` shape 1:1. Validation (legal kinds, dimension rules,
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
        "scenario" => ObjectKind::Scenario,
        other => {
            return Err(ApiError::new_err(format!(
            "unknown object kind {other:?} (expected entity/parameter/alternative/class/scenario)"
        )))
        }
    })
}

/// An object-delete key dict: `{"kind": "entity"|"parameter"|"alternative"|
/// "class"|"scenario", "class": str?, "name": str}` or, for an N-D entity,
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

// ---------------------------------------------------------------------------
// Purge requests in (purge plan §3.10): `{"scope": <name>, <filters>,
// "allow_outside_scope"?, "allow_on_template"?}`.
// ---------------------------------------------------------------------------

pub(crate) fn purge_request_from_py(obj: &Bound<'_, PyAny>) -> PyResult<PurgeRequest> {
    let scope_name = req_str(obj, "purge", "scope")?;
    // The scope name first (an unknown scope is named as such), then: a key
    // the scope does not take is refused, since ignoring it would silently
    // widen the purge (e.g. memberships with "class" purges every group).
    let filters: &[&str] = match scope_name.as_str() {
        "values" => &["class", "parameter", "alternative"],
        "entities" | "parameters" => &["class"],
        "activations" => &["class", "alternative"],
        "memberships" => &["group_class"],
        "alternatives" | "classes" | "value_lists" => &["except"],
        "scenarios" => &[],
        other => {
            return Err(ApiError::new_err(format!(
                "unknown purge scope {other:?} (expected values, entities, activations, \
                 memberships, alternatives, scenarios, classes, parameters or value_lists)"
            )))
        }
    };
    let dict = obj
        .cast::<PyDict>()
        .map_err(|_| ApiError::new_err("a purge request is a dict"))?;
    for k in dict.keys() {
        let k: String = k.extract()?;
        let common = ["scope", "allow_outside_scope", "allow_on_template"];
        if !common.contains(&k.as_str()) && !filters.contains(&k.as_str()) {
            return Err(ApiError::new_err(format!(
                "purge scope {scope_name:?} does not take {k:?} (it takes {filters:?}, \
                 allow_outside_scope, allow_on_template)"
            )));
        }
    }
    let except = || str_list(obj, "except");
    let scope = match scope_name.as_str() {
        "values" => PurgeScope::Values {
            class: opt_str(obj, "class")?,
            parameter: opt_str(obj, "parameter")?,
            alternative: opt_str(obj, "alternative")?,
        },
        "entities" => PurgeScope::Entities {
            class: opt_str(obj, "class")?,
        },
        "activations" => PurgeScope::Activations {
            class: opt_str(obj, "class")?,
            alternative: opt_str(obj, "alternative")?,
        },
        "memberships" => PurgeScope::Memberships {
            group_class: opt_str(obj, "group_class")?,
        },
        "alternatives" => PurgeScope::Alternatives { except: except()? },
        "scenarios" => PurgeScope::Scenarios,
        "classes" => PurgeScope::Classes { except: except()? },
        "parameters" => PurgeScope::Parameters {
            class: opt_str(obj, "class")?,
        },
        "value_lists" => PurgeScope::ValueLists { except: except()? },
        other => {
            return Err(ApiError::new_err(format!(
                "unknown purge scope {other:?} (expected values, entities, activations, \
                 memberships, alternatives, scenarios, classes, parameters or value_lists)"
            )))
        }
    };
    Ok(PurgeRequest {
        scope,
        allow_outside_scope: opt_bool(obj, "allow_outside_scope")?.unwrap_or(false),
        allow_on_template: opt_bool(obj, "allow_on_template")?.unwrap_or(false),
    })
}

pub(crate) fn purge_requests_from_py(
    reqs: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<PurgeRequest>> {
    let mut out = Vec::new();
    if let Some(r) = reqs {
        for item in r.try_iter()? {
            out.push(purge_request_from_py(&item?)?);
        }
    }
    Ok(out)
}

fn purge_outcome_to_py<'py>(py: Python<'py>, o: &PurgeOutcome) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("request_index", o.request_index)?;
    d.set_item("closed_out", cascade_to_py(py, &o.closed_out)?)?;
    d.set_item("objects", o.objects)?;
    d.set_item("spared", cascade_to_py(py, &o.spared)?)?;
    d.set_item("unbound", o.unbound)?;
    Ok(d)
}

pub(crate) fn purge_previews_to_py<'py>(
    py: Python<'py>,
    ps: &[PurgePreview],
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for p in ps {
        let d = PyDict::new(py);
        d.set_item("request_index", p.request_index)?;
        d.set_item("objects", p.objects)?;
        d.set_item("cascade", cascade_to_py(py, &p.cascade)?)?;
        d.set_item("outside_scope", blockers_to_py(py, &p.outside_scope)?)?;
        d.set_item("template_stamped", p.template_stamped)?;
        d.set_item("cells", p.cells)?;
        let warnings = PyList::empty(py);
        for w in &p.warnings {
            warnings.append(warning_to_py(py, w)?)?;
        }
        d.set_item("warnings", warnings)?;
        out.append(d)?;
    }
    Ok(out)
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
            description: opt_str(obj, "description")?,
        },
        "rename_entity" => StructuralChange::RenameEntity {
            class: req_str(obj, &op, "class")?,
            current: req_str(obj, &op, "current")?,
            new_name: req_str(obj, &op, "new_name")?,
            description: opt_str(obj, "description")?,
        },
        "create_class" => StructuralChange::CreateClass {
            name: req_str(obj, &op, "name")?,
            dims: str_list(obj, "dims")?,
            description: opt_str(obj, "description")?,
        },
        "rename_class" => StructuralChange::RenameClass {
            current: req_str(obj, &op, "current")?,
            new_name: req_str(obj, &op, "new_name")?,
            description: opt_str(obj, "description")?,
            allow_on_template: opt_bool(obj, "allow_on_template")?.unwrap_or(false),
        },
        "rename_parameter" => StructuralChange::RenameParameter {
            class: req_str(obj, &op, "class")?,
            current: req_str(obj, &op, "current")?,
            new_name: req_str(obj, &op, "new_name")?,
            description: opt_str(obj, "description")?,
            allow_on_template: opt_bool(obj, "allow_on_template")?.unwrap_or(false),
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

/// The `covered_writes=` keyword: `"refuse"` (the default) or
/// `"delete_wins"`.
pub(crate) fn covered_writes_from_str(s: &str) -> PyResult<CoveredWrites> {
    match s {
        "refuse" => Ok(CoveredWrites::Refuse),
        "delete_wins" => Ok(CoveredWrites::DeleteWins),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "covered_writes must be \"refuse\" or \"delete_wins\", got {other:?}"
        ))),
    }
}

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
        WarningKind::DroppedByDelete { segment_index, by } => {
            d.set_item("segment_index", segment_index)?;
            let (channel, key_index) = match by {
                CoveringDelete::Value { key_index } => ("deletes", key_index),
                CoveringDelete::Object { key_index } => ("object_deletes", key_index),
            };
            d.set_item("by", channel)?;
            d.set_item("key_index", key_index)?;
            "dropped_by_delete"
        }
        WarningKind::TextNormalized {
            what,
            count,
            written,
            stored,
        } => {
            d.set_item("what", what)?;
            d.set_item("count", count)?;
            d.set_item("written", written)?;
            d.set_item("stored", stored)?;
            "text_normalized"
        }
        WarningKind::NameFolded {
            what,
            count,
            written,
            stored,
            spellings,
        } => {
            d.set_item("what", what)?;
            d.set_item("count", count)?;
            d.set_item("written", written)?;
            d.set_item("stored", stored)?;
            let list = PyList::empty(py);
            for s in spellings {
                let e = PyDict::new(py);
                e.set_item("written", &s.written)?;
                e.set_item("stored", &s.stored)?;
                e.set_item("id", s.id)?;
                list.append(e)?;
            }
            d.set_item("spellings", list)?;
            "name_folded"
        }
        WarningKind::PurgeLarge {
            request_index,
            cells,
            threshold,
        } => {
            d.set_item("request_index", request_index)?;
            d.set_item("cells", cells)?;
            d.set_item("threshold", threshold)?;
            "purge_large"
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
    k.tag()
}

fn structural_effect_tag(e: StructuralEffect) -> &'static str {
    e.tag()
}

fn structural_outcome_to_py<'py>(
    py: Python<'py>,
    o: &StructuralOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("change_index", o.change_index)?;
    d.set_item("kind", structural_kind_tag(o.kind))?;
    d.set_item("id", o.id)?;
    d.set_item("effect", structural_effect_tag(o.effect))?;
    Ok(d)
}

// ---------------------------------------------------------------------------
// The metadata channel in (metadata ops as `"op"`-keyed dicts) and its
// outcomes out. No decision here: every rule lives in the core.
// ---------------------------------------------------------------------------

fn md_err(msg: String) -> PyErr {
    ApiError::new_err(format!("metadata: {msg}"))
}

/// Refuse a key `what` does not take: ignoring a misspelled key would
/// silently change the meaning (a misspelled "fields" revises to empty, a
/// misspelled "role" links without one).
fn md_keys(obj: &Bound<'_, PyAny>, what: &str, allowed: &[&str]) -> PyResult<()> {
    let dict = obj
        .cast::<PyDict>()
        .map_err(|_| md_err(format!("{what} is a dict, got {obj}")))?;
    for k in dict.keys() {
        let k: String = k.extract()?;
        if !allowed.contains(&k.as_str()) {
            return Err(md_err(format!(
                "{what} does not take {k:?} (it takes {allowed:?})"
            )));
        }
    }
    Ok(())
}

/// A source / license / role: a name (str) or an id (int).
fn md_ref(obj: &Bound<'_, PyAny>) -> PyResult<Ref> {
    if let Ok(i) = obj.extract::<i64>() {
        if !obj.is_instance_of::<pyo3::types::PyBool>() {
            return Ok(Ref::Id(i));
        }
    }
    obj.extract::<String>()
        .map(Ref::Name)
        .map_err(|_| md_err(format!("expected a name or an id, got {obj}")))
}

fn md_req<'py>(obj: &Bound<'py, PyAny>, op: &str, key: &str) -> PyResult<Bound<'py, PyAny>> {
    match obj.get_item(key) {
        Ok(v) if !v.is_none() => Ok(v),
        _ => Err(md_err(format!("{op:?} is missing key {key:?}"))),
    }
}

fn md_opt<'py>(obj: &Bound<'py, PyAny>, key: &str) -> Option<Bound<'py, PyAny>> {
    match obj.get_item(key) {
        Ok(v) if !v.is_none() => Some(v),
        _ => None,
    }
}

/// Two strings from a list or a tuple.
fn md_two(obj: &Bound<'_, PyAny>, what: &str) -> PyResult<(String, String)> {
    match obj.extract::<Vec<String>>() {
        Ok(v) if v.len() == 2 => Ok((v[0].clone(), v[1].clone())),
        _ => Err(md_err(format!("{what} is a [str, str] pair, got {obj}"))),
    }
}

/// A pair: `[name, value]` / `(name, value)` or an id.
fn md_pair(obj: &Bound<'_, PyAny>) -> PyResult<PairRef> {
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(PairRef::Id(i));
    }
    let (name, value) = md_two(obj, "a pair ([name, value] or an id)")?;
    Ok(PairRef::Pair { name, value })
}

/// A target dict: `{"source": ref}`, `{"license": ref}`, `{"pair": [name,
/// value]}` or `{"id": n, "kind": "source"|"license"|"pair"}`.
fn md_target(obj: &Bound<'_, PyAny>) -> PyResult<Target> {
    md_target_in(obj, false)
}

/// A target dict; `with_role`: it may also carry `"role"` (a set list's
/// entries).
fn md_target_in(obj: &Bound<'_, PyAny>, with_role: bool) -> PyResult<Target> {
    let base: &[&str] = &["source", "license", "pair", "id", "kind", "role"];
    md_keys(obj, "a target", if with_role { base } else { &base[..5] })?;
    let kinds = ["source", "license", "pair", "id"]
        .iter()
        .filter(|k| md_opt(obj, k).is_some())
        .count();
    if kinds != 1 {
        return Err(md_err(format!(
            "a target names exactly one of \"source\", \"license\", \"pair\" or \"id\" (with \
             \"kind\"), got {obj}"
        )));
    }
    if let Some(v) = md_opt(obj, "source") {
        return Ok(Target::Source(md_ref(&v)?));
    }
    if let Some(v) = md_opt(obj, "license") {
        return Ok(Target::License(md_ref(&v)?));
    }
    if let Some(v) = md_opt(obj, "pair") {
        return Ok(Target::Pair(md_pair(&v)?));
    }
    if let (Some(id), Some(kind)) = (md_opt(obj, "id"), md_opt(obj, "kind")) {
        let id: i64 = id.extract()?;
        let kind: String = kind.extract()?;
        return match kind.as_str() {
            "source" => Ok(Target::Source(Ref::Id(id))),
            "license" => Ok(Target::License(Ref::Id(id))),
            "pair" => Ok(Target::Pair(PairRef::Id(id))),
            other => Err(md_err(format!("unknown target kind {other:?}"))),
        };
    }
    Err(md_err(format!(
        "a target is {{\"source\": ..}}, {{\"license\": ..}}, {{\"pair\": [name, value]}} or \
         {{\"id\": n, \"kind\": ..}}, got {obj}"
    )))
}

fn md_role(obj: &Bound<'_, PyAny>) -> PyResult<RoleSel> {
    Ok(match md_opt(obj, "role") {
        None => RoleSel::NoRole,
        Some(r) => RoleSel::Named(r.extract()?),
    })
}

/// A link: the target keys plus an optional `"role"` (in a `link` op, the
/// target is under `"target"`; in a set list, the dict is the target).
fn md_link(target: &Bound<'_, PyAny>, role_from: &Bound<'_, PyAny>) -> PyResult<LinkSpec> {
    Ok(LinkSpec {
        target: md_target(target)?,
        role: md_role(role_from)?,
    })
}

fn md_value(obj: &Bound<'_, PyAny>, op: &str) -> PyResult<ValueAddr> {
    md_keys(
        obj,
        "a value address",
        &["class", "parameter", "alternative", "entity"],
    )?;
    Ok(ValueAddr {
        class: md_req(obj, op, "class")?.extract()?,
        parameter: md_req(obj, op, "parameter")?.extract()?,
        alternative: md_req(obj, op, "alternative")?.extract()?,
        entity: md_req(obj, op, "entity")?.extract()?,
    })
}

/// A subject dict: `{"dataset": True}`, `{"entity": [class, name]}`,
/// `{"class": name}`, `{"parameter": [class, name]}`, `{"alternative": ..}`,
/// `{"scenario": ..}`, `{"run": id}`, `{"source": ref}`, `{"license": ref}`,
/// `{"value_list": ..}`, `{"unit": ..}`.
fn md_subject(obj: &Bound<'_, PyAny>) -> PyResult<Subject> {
    md_keys(
        obj,
        "a subject",
        &[
            "dataset",
            "entity",
            "class",
            "parameter",
            "alternative",
            "scenario",
            "run",
            "source",
            "license",
            "value_list",
            "unit",
        ],
    )?;
    if obj.cast::<PyDict>().map(|d| d.len()).unwrap_or(0) != 1 {
        return Err(md_err(format!("a subject has exactly one key, got {obj}")));
    }
    if md_opt(obj, "dataset").is_some() {
        return Ok(Subject::Dataset);
    }
    if let Some(v) = md_opt(obj, "entity") {
        let (class, name) = md_two(&v, "a subject [class, name]")?;
        return Ok(Subject::Entity { class, name });
    }
    if let Some(v) = md_opt(obj, "parameter") {
        let (class, name) = md_two(&v, "a subject [class, name]")?;
        return Ok(Subject::Parameter { class, name });
    }
    for (key, f) in [
        ("class", Subject::Class as fn(String) -> Subject),
        ("alternative", Subject::Alternative),
        ("scenario", Subject::Scenario),
        ("value_list", Subject::ValueList),
        ("unit", Subject::Unit),
    ] {
        if let Some(v) = md_opt(obj, key) {
            return Ok(f(v.extract()?));
        }
    }
    if let Some(v) = md_opt(obj, "run") {
        return Ok(Subject::Run(v.extract()?));
    }
    if let Some(v) = md_opt(obj, "source") {
        return Ok(Subject::Source(md_ref(&v)?));
    }
    if let Some(v) = md_opt(obj, "license") {
        return Ok(Subject::License(md_ref(&v)?));
    }
    Err(md_err(format!("unknown subject {obj}")))
}

/// `"keep"`, `"clear"` or `{"set": [target, ...]}`.
pub(crate) fn md_action(obj: &Bound<'_, PyAny>) -> PyResult<Action> {
    if let Ok(s) = obj.extract::<String>() {
        return match s.as_str() {
            "keep" => Ok(Action::Keep),
            "clear" => Ok(Action::Clear),
            other => Err(md_err(format!(
                "a value-metadata action is \"keep\", \"clear\" or {{\"set\": [...]}}, got \
                 {other:?}"
            ))),
        };
    }
    md_keys(obj, "a value-metadata action", &["set"])?;
    let list = md_opt(obj, "set").ok_or_else(|| {
        md_err(format!(
            "a value-metadata action is \"keep\", \"clear\" or {{\"set\": [...]}}, got {obj}"
        ))
    })?;
    let mut links = Vec::new();
    for t in list.try_iter()? {
        let t = t?;
        links.push(LinkSpec {
            target: md_target_in(&t, true)?,
            role: md_role(&t)?,
        });
    }
    Ok(Action::Set(links))
}

/// `"save"`, `{"class", "parameter", "alternative"}` (a target) or the same
/// plus `"entity"` (a value).
fn md_scope(obj: &Bound<'_, PyAny>) -> PyResult<Scope> {
    if let Ok(s) = obj.extract::<String>() {
        if s == "save" {
            return Ok(Scope::Save);
        }
        return Err(md_err(format!("unknown scope {s:?}")));
    }
    if md_opt(obj, "entity").is_some() {
        return Ok(Scope::Value(md_value(obj, "value_metadata")?));
    }
    md_keys(
        obj,
        "a target scope",
        &["class", "parameter", "alternative"],
    )?;
    Ok(Scope::Target {
        class: md_req(obj, "value_metadata", "class")?.extract()?,
        parameter: md_req(obj, "value_metadata", "parameter")?.extract()?,
        alternative: md_req(obj, "value_metadata", "alternative")?.extract()?,
    })
}

fn md_fields_source(obj: &Bound<'_, PyAny>) -> PyResult<SourceFields> {
    let Some(f) = md_opt(obj, "fields") else {
        return Ok(SourceFields::default());
    };
    md_keys(
        &f,
        "source fields",
        &[
            "title",
            "path",
            "edition",
            "publisher",
            "issued",
            "retrieved",
            "citation",
            "description",
        ],
    )?;
    Ok(SourceFields {
        title: opt_str(&f, "title")?,
        path: opt_str(&f, "path")?,
        edition: opt_str(&f, "edition")?,
        publisher: opt_str(&f, "publisher")?,
        issued: opt_str(&f, "issued")?,
        retrieved: opt_str(&f, "retrieved")?,
        citation: opt_str(&f, "citation")?,
        description: opt_str(&f, "description")?,
    })
}

fn md_fields_license(obj: &Bound<'_, PyAny>) -> PyResult<LicenseFields> {
    let Some(f) = md_opt(obj, "fields") else {
        return Ok(LicenseFields::default());
    };
    md_keys(&f, "license fields", &["title", "path", "spdx", "policy"])?;
    Ok(LicenseFields {
        title: opt_str(&f, "title")?,
        path: opt_str(&f, "path")?,
        spdx: opt_bool(&f, "spdx")?.unwrap_or(false),
        policy: opt_str(&f, "policy")?,
    })
}

fn md_cascade(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(opt_bool(obj, "cascade")?.unwrap_or(false))
}

pub(crate) fn metadata_change_from_py(obj: &Bound<'_, PyAny>) -> PyResult<MetadataChange> {
    let op: String = md_req(obj, "metadata item", "op")?.extract()?;
    let o = op.as_str();
    let keys: &[&str] = match o {
        "put_source" => &["source", "new_name", "fields"],
        "put_license" => &["license", "new_name", "fields"],
        "set_source_licenses" => &["source", "licenses"],
        "create_pair" => &["name", "value"],
        "revise_pair" => &["pair", "value"],
        "rename_pair_key" => &["current", "new_name"],
        "put_role" => &["role", "new_name", "description", "iri"],
        "set_property" => &["key", "value"],
        "delete_source" => &["source", "cascade"],
        "delete_license" => &["license", "cascade"],
        "delete_pair" => &["pair", "cascade"],
        "delete_role" => &["role"],
        "link" | "unlink" => &["subject", "target", "role"],
        "link_value" | "unlink_value" => &["value", "target", "role"],
        "value_metadata" => &["scope", "action"],
        other => return Err(md_err(format!("unknown op {other:?}"))),
    };
    let mut all = vec!["op"];
    all.extend_from_slice(keys);
    md_keys(obj, &format!("{o:?}"), &all)?;
    Ok(match o {
        "put_source" => MetadataChange::PutSource {
            source: md_ref(&md_req(obj, o, "source")?)?,
            new_name: opt_str(obj, "new_name")?,
            fields: md_fields_source(obj)?,
        },
        "put_license" => MetadataChange::PutLicense {
            license: md_ref(&md_req(obj, o, "license")?)?,
            new_name: opt_str(obj, "new_name")?,
            fields: md_fields_license(obj)?,
        },
        "set_source_licenses" => {
            let mut licenses = Vec::new();
            for l in md_req(obj, o, "licenses")?.try_iter()? {
                licenses.push(md_ref(&l?)?);
            }
            MetadataChange::SetSourceLicenses {
                source: md_ref(&md_req(obj, o, "source")?)?,
                licenses,
            }
        }
        "create_pair" => MetadataChange::CreatePair {
            name: md_req(obj, o, "name")?.extract()?,
            value: md_req(obj, o, "value")?.extract()?,
        },
        "revise_pair" => MetadataChange::RevisePair {
            pair: md_pair(&md_req(obj, o, "pair")?)?,
            value: md_req(obj, o, "value")?.extract()?,
        },
        "rename_pair_key" => MetadataChange::RenamePairKey {
            current: md_req(obj, o, "current")?.extract()?,
            new_name: md_req(obj, o, "new_name")?.extract()?,
        },
        "put_role" => MetadataChange::PutRole {
            role: md_ref(&md_req(obj, o, "role")?)?,
            new_name: opt_str(obj, "new_name")?,
            description: opt_str(obj, "description")?,
            iri: opt_str(obj, "iri")?,
        },
        "set_property" => MetadataChange::SetProperty {
            key: md_req(obj, o, "key")?.extract()?,
            // Required: an explicit None clears (a missing key must not).
            value: match obj.get_item("value") {
                Ok(v) if v.is_none() => None,
                Ok(v) => Some(v.extract()?),
                Err(_) => {
                    return Err(md_err(
                        "\"set_property\" needs \"value\" (None clears the key)".into(),
                    ))
                }
            },
        },
        "delete_source" => MetadataChange::DeleteSource {
            source: md_ref(&md_req(obj, o, "source")?)?,
            cascade: md_cascade(obj)?,
        },
        "delete_license" => MetadataChange::DeleteLicense {
            license: md_ref(&md_req(obj, o, "license")?)?,
            cascade: md_cascade(obj)?,
        },
        "delete_pair" => MetadataChange::DeletePair {
            pair: md_pair(&md_req(obj, o, "pair")?)?,
            cascade: md_cascade(obj)?,
        },
        "delete_role" => MetadataChange::DeleteRole {
            role: md_ref(&md_req(obj, o, "role")?)?,
        },
        "link" => MetadataChange::Link {
            subject: md_subject(&md_req(obj, o, "subject")?)?,
            link: md_link(&md_req(obj, o, "target")?, obj)?,
        },
        "unlink" => MetadataChange::Unlink {
            subject: md_subject(&md_req(obj, o, "subject")?)?,
            target: md_target(&md_req(obj, o, "target")?)?,
            role: md_role(obj)?,
        },
        "link_value" => MetadataChange::LinkValue {
            value: md_value(&md_req(obj, o, "value")?, o)?,
            link: md_link(&md_req(obj, o, "target")?, obj)?,
        },
        "unlink_value" => MetadataChange::UnlinkValue {
            value: md_value(&md_req(obj, o, "value")?, o)?,
            target: md_target(&md_req(obj, o, "target")?)?,
            role: md_role(obj)?,
        },
        "value_metadata" => MetadataChange::ValueMetadata {
            scope: md_scope(&md_req(obj, o, "scope")?)?,
            action: md_action(&md_req(obj, o, "action")?)?,
        },
        other => return Err(md_err(format!("unknown op {other:?}"))),
    })
}

/// The `metadata=` list plus the `value_metadata=` shorthand (one
/// save-scope statement).
pub(crate) fn metadata_from_py(
    items: Option<&Bound<'_, PyAny>>,
    shorthand: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<MetadataChange>> {
    let mut out = Vec::new();
    if let Some(s) = shorthand {
        out.push(MetadataChange::ValueMetadata {
            scope: Scope::Save,
            action: md_action(s)?,
        });
    }
    if let Some(items) = items {
        for it in items.try_iter()? {
            out.push(metadata_change_from_py(&it?)?);
        }
    }
    Ok(out)
}

fn md_kind_tag(k: MetadataKind) -> &'static str {
    k.tag()
}

fn md_effect_tag(e: MetadataEffect) -> &'static str {
    e.tag()
}

fn metadata_outcome_to_py<'py>(
    py: Python<'py>,
    o: &MetadataOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("item_index", o.item_index)?;
    d.set_item("kind", md_kind_tag(o.kind))?;
    d.set_item("id", o.id)?;
    d.set_item("effect", md_effect_tag(o.effect))?;
    d.set_item("links_closed", o.links_closed)?;
    d.set_item("blockers", blockers_to_py(py, &o.blockers)?)?;
    Ok(d)
}

fn value_links_to_py<'py>(py: Python<'py>, c: &ValueLinkCounts) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("kept", c.kept)?;
    d.set_item("set", c.set)?;
    d.set_item("cleared", c.cleared)?;
    d.set_item("closed_by_delete", c.closed_by_delete)?;
    d.set_item("carried", c.carried)?;
    Ok(d)
}

/// A `ValueMetadataUnstated` exception carrying `count` and `sample`.
fn value_metadata_unstated_err(msg: String, count: usize, sample: Vec<String>) -> PyErr {
    let err = ValueMetadataUnstated::new_err(msg);
    Python::attach(|py| {
        let v = err.value(py);
        // Best-effort: the message carries the same information in any case.
        let _ = v.setattr("count", count);
        let _ = v.setattr("sample", sample);
    });
    err
}

pub(crate) fn report_to_py<'py>(py: Python<'py>, r: &WriteReport) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("commit_id", r.commit_id)?;
    d.set_item("values_written", r.values_written)?;
    d.set_item("values_unchanged", r.values_unchanged)?;
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
    let purges = PyList::empty(py);
    for o in &r.purges {
        purges.append(purge_outcome_to_py(py, o)?)?;
    }
    d.set_item("purges", purges)?;
    d.set_item("read_surface_warning", r.read_surface_warning.as_deref())?;
    let metadata = PyList::empty(py);
    for o in &r.metadata {
        metadata.append(metadata_outcome_to_py(py, o)?)?;
    }
    d.set_item("metadata", metadata)?;
    d.set_item("value_links", value_links_to_py(py, &r.value_links)?)?;
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
    d.set_item("total_rows", p.total_rows)?;
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
    d.set_item("total_rows", r.total_rows)?;
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
    d.set_item("warning", r.warning.as_deref())?;
    Ok(d)
}

pub(crate) fn compaction_report_to_py<'py>(
    py: Python<'py>,
    r: &CompactionReport,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("swapped", r.swapped)?;
    d.set_item(
        "trigger",
        r.trigger.map(|t| match t {
            CompactionTrigger::Requested => "requested",
            CompactionTrigger::NoBaseline => "no_baseline",
            CompactionTrigger::Growth => "growth",
            CompactionTrigger::Commits => "commits",
        }),
    )?;
    d.set_item("bytes_before", r.bytes_before)?;
    d.set_item("bytes_after", r.bytes_after)?;
    d.set_item("blocks_before", r.blocks_before)?;
    d.set_item("blocks_after", r.blocks_after)?;
    d.set_item("kept_original", r.kept_original)?;
    d.set_item("bytes_rebuilt", r.bytes_rebuilt)?;
    d.set_item("warning", r.warning.as_deref())?;
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
