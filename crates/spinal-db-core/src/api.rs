//! The public Arrow-in / plain-data-out write path (plan Part 3 `api`; VMP
//! Parts 3-6). `write_parameter` is the one function that ties the whole write
//! path together end-to-end: name resolution (auto-create, decision B), the
//! declared-type lookup from the DB (critique S2), `canon` → `tier` → axis
//! dedup → the `history` protocol, all inside ONE atomic, conflict-retried
//! commit. `read_parameter` reads a written parameter back through the SQL
//! views for round-trip verification.
//!
//! ## Batch column contract (logical-long)
//! `batch` has, in order:
//!   - column 0: `entity` — the entity **names** as `Utf8` (decision B is
//!     resolve-by-name; ids would defeat auto-create). One value is written per
//!     distinct entity name.
//!   - columns `1..rank+1`: the `rank` dimension columns, in declared position
//!     order (position = column index). Arrow type fixes the index kind:
//!     `Timestamp(µs)` → datetime, `Int64` → integer, `Utf8` → label. The field
//!     **name** becomes the dimension name when a definition is auto-created.
//!   - last column: `value` — the measure. Arrow type fixes the measure kind:
//!     `Float64`/`Int64`/`Boolean`/`Utf8`/`Timestamp(µs)`.
//! Rank 0 (scalar) is a two-column batch `(entity, value)`.
//!
//! The **library decides the tier** (VMP Part 6): callers never pick scalar /
//! array / long. Ordinal domains for label dimensions arrive by the in-process
//! `domains` arg and/or Arrow field metadata `spinal:label_domain` (a JSON array
//! of the ordered labels) — decision D, both channels.

use std::collections::HashMap;
use std::path::Path;

use duckdb::arrow::array::{Array, BooleanArray, StringArray};
use duckdb::arrow::compute::{concat_batches, filter_record_batch};
use duckdb::arrow::datatypes::{DataType, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::{params, Connection};

use crate::axis::AxisCache;
use crate::canon::{canonicalize, LabelDomain};
use crate::conn::{with_read, with_write_txn_retrying};
use crate::error::DbError;
use crate::history::write_value;
use crate::types::{
    DeclaredType, IndexKind, ValueKey, ValueKind, Warning, WarningKind, WriteOptions, WriteReport,
};

/// Write one parameter (all its per-entity values for one alternative) from a
/// logical-long Arrow batch. See the module docs for the batch column contract.
///
/// Missing referents (entity class, the parameter definition within that class,
/// the alternative, and every entity named in the batch) are **auto-created**
/// with a machine-readable [`Warning`] appended to the returned report
/// (decision B — never a hard error, never a mere log line). Name resolution,
/// auto-creation, and every value write happen in one transaction, retried as a
/// whole on a write-write conflict (VMP Part 5).
///
/// `user` / `message` stamp the `commit` row this call creates.
#[allow(clippy::too_many_arguments)]
pub fn write_parameter(
    db: &Path,
    class: &str,
    parameter: &str,
    alternative: &str,
    batch: RecordBatch,
    domains: &[LabelDomain],
    opts: WriteOptions,
    user: &str,
    message: &str,
) -> Result<WriteReport, DbError> {
    let ncols = batch.num_columns();
    if ncols < 2 {
        return Err(DbError::Api(format!(
            "batch needs at least an entity column and a value column, got {ncols}"
        )));
    }
    let rank = ncols - 2;
    if rank > 4 {
        return Err(DbError::Api(format!("declared rank {rank} exceeds the cap of 4")));
    }

    // --- Pure, DB-independent preparation (safe to compute once; the retried
    // closure below re-does only the DB-touching work). ---

    // Measure kind from the value column's Arrow type.
    let value_kind = measure_kind(batch.schema().field(ncols - 1).data_type())?;

    // Inferred (dimension_name, index kind) per dimension, from the dim columns'
    // Arrow types + field names. Used when a definition is auto-created; the
    // declared type of an EXISTING definition is read from the DB instead.
    let mut inferred: Vec<(String, IndexKind)> = Vec::with_capacity(rank);
    for i in 0..rank {
        let field = batch.schema().field(i + 1).clone();
        inferred.push((field.name().clone(), index_kind(field.data_type())?));
    }

    // Effective label domains: the in-process arg, plus any Arrow field-metadata
    // domain for a label dimension the arg did not cover (decision D).
    let domains = effective_domains(&batch, &inferred, domains)?;

    // The per-entity row masks and the dims+value sub-batch (entity col dropped).
    let entity_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DbError::Api("entity column must be Utf8 (entity names)".into()))?;
    let nrows = batch.num_rows();
    let mut slot: HashMap<String, usize> = HashMap::new();
    let mut names: Vec<String> = Vec::new();
    let mut masks: Vec<Vec<bool>> = Vec::new();
    for row in 0..nrows {
        if entity_arr.is_null(row) {
            return Err(DbError::Api(format!("null entity name in row {row}")));
        }
        let name = entity_arr.value(row).to_string();
        let idx = *slot.entry(name.clone()).or_insert_with(|| {
            names.push(name.clone());
            masks.push(vec![false; nrows]);
            names.len() - 1
        });
        masks[idx][row] = true;
    }
    let project: Vec<usize> = (1..ncols).collect();
    let sub_all = batch.project(&project)?;

    // --- The atomic, retried commit. Everything below re-reads/re-creates DB
    // state inside a fresh transaction on each retry (VMP Part 5), so the axis
    // cache is built HERE, not outside (a rollback must not leave stale ids). ---
    with_write_txn_retrying(db, user, message, |conn, commit_id| {
        let mut cache = AxisCache::new();
        let mut report = WriteReport {
            commit_id,
            ..Default::default()
        };

        let class_id = resolve_entity_class(conn, commit_id, class, &mut report.warnings)?;
        let (pd_id, declared) = resolve_parameter(
            conn,
            commit_id,
            class_id,
            parameter,
            &inferred,
            value_kind,
            &mut report.warnings,
        )?;
        let retain_content = read_retain_content(conn, pd_id)?;
        let alt_id = resolve_alternative(conn, commit_id, alternative, &mut report.warnings)?;

        // Fold the definition's retain_content into retain_override unless the
        // caller already overrode it (VMP Part 5 primary retention lever): else a
        // write to a retain_content=FALSE definition would silently keep payloads.
        let effective_retain = opts.retain_override.unwrap_or(retain_content);
        let resolved_opts = WriteOptions {
            force_replace: opts.force_replace,
            retain_override: Some(effective_retain),
        };

        for (name, mask) in names.iter().zip(masks.iter()) {
            let entity_id =
                resolve_entity(conn, commit_id, class_id, name, &mut report.warnings)?;
            let sub = filter_record_batch(&sub_all, &BooleanArray::from(mask.clone()))?;
            let value = canonicalize(&sub, &declared, value_kind, &domains)?;
            let key = ValueKey {
                entity_id,
                parameter_definition_id: pd_id,
                alternative_id: alt_id,
            };
            let outcome = write_value(conn, commit_id, key, &value, &mut cache, &resolved_opts)?;
            report.absorb(&outcome);
        }
        Ok(report)
    })
}

/// Read a written parameter back for one class/parameter/alternative into a
/// `RecordBatch`, through the `read_parameter` SQL view/macro (VMP Part 6; plan
/// Part 3 — reads are SQL, the library never gatekeeps them). One fresh RO
/// connection, opened and closed per call (CLAUDE.md invariant 1). Enough to
/// round-trip-verify a write; the columns are the macro's canonical long shape
/// (`entity, alternative, tier, value_kind, idx*_*, v_*`).
pub fn read_parameter(
    db: &Path,
    class: &str,
    parameter: &str,
    alternative: &str,
) -> Result<RecordBatch, DbError> {
    with_read(db, |conn| {
        let mut stmt = conn.prepare(
            "SELECT * FROM read_parameter(?, ?) WHERE lower(alternative) = lower(?)",
        )?;
        let arrow = stmt.query_arrow(params![class, parameter, alternative])?;
        let schema = arrow.get_schema();
        let batches: Vec<RecordBatch> = arrow.collect();
        Ok(concat_batches(&schema, &batches)?)
    })
}

// ---------------------------------------------------------------------------
// Arrow-type → kind mapping
// ---------------------------------------------------------------------------

fn measure_kind(dt: &DataType) -> Result<ValueKind, DbError> {
    Ok(match dt {
        DataType::Float64 => ValueKind::Float,
        DataType::Int64 => ValueKind::Int,
        DataType::Boolean => ValueKind::Bool,
        DataType::Utf8 => ValueKind::String,
        DataType::Timestamp(TimeUnit::Microsecond, _) => ValueKind::DateTime,
        other => {
            return Err(DbError::Api(format!(
                "unsupported value column Arrow type {other:?}"
            )))
        }
    })
}

fn index_kind(dt: &DataType) -> Result<IndexKind, DbError> {
    Ok(match dt {
        DataType::Timestamp(TimeUnit::Microsecond, _) => IndexKind::Datetime,
        DataType::Int64 => IndexKind::Integer,
        DataType::Utf8 => IndexKind::Label,
        other => {
            return Err(DbError::Api(format!(
                "unsupported dimension column Arrow type {other:?}"
            )))
        }
    })
}

fn index_kind_tag(k: IndexKind) -> &'static str {
    match k {
        IndexKind::Datetime => "datetime",
        IndexKind::Integer => "integer",
        IndexKind::Label => "label",
        IndexKind::Period => "period",
        IndexKind::TimePattern => "time_pattern",
    }
}

fn parse_index_kind(s: &str) -> Result<IndexKind, DbError> {
    Ok(match s {
        "datetime" => IndexKind::Datetime,
        "integer" => IndexKind::Integer,
        "label" => IndexKind::Label,
        "period" => IndexKind::Period,
        "time_pattern" => IndexKind::TimePattern,
        other => {
            return Err(DbError::Api(format!(
                "unknown dimension kind {other:?} in parameter_type_dimension"
            )))
        }
    })
}

/// Merge the caller's label domains with any carried in Arrow field metadata
/// (`spinal:label_domain` = a JSON array of the ordered labels) for a label
/// dimension the arg did not cover (decision D — both channels).
fn effective_domains(
    batch: &RecordBatch,
    inferred: &[(String, IndexKind)],
    provided: &[LabelDomain],
) -> Result<Vec<LabelDomain>, DbError> {
    let mut out: Vec<LabelDomain> = provided.to_vec();
    for (i, (_, kind)) in inferred.iter().enumerate() {
        if *kind != IndexKind::Label {
            continue;
        }
        let position = (i + 1) as u8;
        if out.iter().any(|d| d.position == position) {
            continue;
        }
        let field = batch.schema().field(i + 1).clone();
        if let Some(js) = field.metadata().get("spinal:label_domain") {
            let labels: Vec<String> = serde_json::from_str(js).map_err(|e| {
                DbError::Api(format!("bad spinal:label_domain metadata on {:?}: {e}", field.name()))
            })?;
            out.push(LabelDomain { position, labels });
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Name resolution + auto-create (decision B)
// ---------------------------------------------------------------------------

fn opt_id(conn: &Connection, sql: &str, id_param: i64, name: &str) -> Result<Option<i64>, DbError> {
    // Two shapes share this: class/alternative (name only) and entity (class +
    // name). `id_param` < 0 means "name only"; the SQL then ignores it.
    let r = if id_param < 0 {
        conn.query_row(sql, params![name], |r| r.get::<_, i64>(0))
    } else {
        conn.query_row(sql, params![id_param, name], |r| r.get::<_, i64>(0))
    };
    match r {
        Ok(id) => Ok(Some(id)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn resolve_entity_class(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<i64, DbError> {
    if let Some(id) = opt_id(conn, "SELECT id FROM entity_class WHERE cur_name = lower(?)", -1, name)? {
        return Ok(id);
    }
    let id: i64 = conn.query_row(
        "INSERT INTO entity_class (name, valid_from_commit) VALUES (?, ?) RETURNING id",
        params![name, commit_id],
        |r| r.get(0),
    )?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "entity_class".into(),
        name: name.into(),
        id,
    });
    Ok(id)
}

fn resolve_alternative(
    conn: &Connection,
    commit_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<i64, DbError> {
    if let Some(id) = opt_id(conn, "SELECT id FROM alternative WHERE cur_name = lower(?)", -1, name)? {
        return Ok(id);
    }
    let id: i64 = conn.query_row(
        "INSERT INTO alternative (name, valid_from_commit) VALUES (?, ?) RETURNING id",
        params![name, commit_id],
        |r| r.get(0),
    )?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "alternative".into(),
        name: name.into(),
        id,
    });
    Ok(id)
}

fn resolve_entity(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    name: &str,
    warnings: &mut Vec<Warning>,
) -> Result<i64, DbError> {
    if let Some(id) = opt_id(
        conn,
        "SELECT id FROM entity WHERE cur_cls = ? AND cur_name = lower(?)",
        class_id,
        name,
    )? {
        return Ok(id);
    }
    let id: i64 = conn.query_row(
        "INSERT INTO entity (entity_class_id, name, valid_from_commit) VALUES (?, ?, ?) RETURNING id",
        params![class_id, name, commit_id],
        |r| r.get(0),
    )?;
    warnings.push(Warning {
        kind: WarningKind::AutoCreated,
        object_type: "entity".into(),
        name: name.into(),
        id,
    });
    Ok(id)
}

/// Resolve the parameter definition within `class_id`, auto-creating it (and its
/// `parameter_type` + `parameter_type_dimension` declaration) when absent. The
/// declared type is READ FROM THE DB for an existing definition (critique S2);
/// only a freshly created definition takes its declared type from the batch's
/// inferred dimensions.
fn resolve_parameter(
    conn: &Connection,
    commit_id: i64,
    class_id: i64,
    name: &str,
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
    warnings: &mut Vec<Warning>,
) -> Result<(i64, DeclaredType), DbError> {
    if let Some(pd_id) = opt_id(
        conn,
        "SELECT id FROM parameter_definition WHERE cur_cls = ? AND cur_name = lower(?)",
        class_id,
        name,
    )? {
        // Existing definition: read the declared type from the DB. If it somehow
        // has no parameter_type row yet, materialize one from the batch.
        match read_declared_type(conn, pd_id)? {
            Some(declared) => Ok((pd_id, declared)),
            None => {
                create_parameter_type(conn, commit_id, pd_id, inferred, value_kind)?;
                Ok((pd_id, inferred_declared(inferred)))
            }
        }
    } else {
        let pd_id: i64 = conn.query_row(
            "INSERT INTO parameter_definition (entity_class_id, name, valid_from_commit) \
             VALUES (?, ?, ?) RETURNING id",
            params![class_id, name, commit_id],
            |r| r.get(0),
        )?;
        create_parameter_type(conn, commit_id, pd_id, inferred, value_kind)?;
        warnings.push(Warning {
            kind: WarningKind::AutoCreated,
            object_type: "parameter_definition".into(),
            name: name.into(),
            id: pd_id,
        });
        Ok((pd_id, inferred_declared(inferred)))
    }
}

fn inferred_declared(inferred: &[(String, IndexKind)]) -> DeclaredType {
    DeclaredType {
        positions: inferred
            .iter()
            .enumerate()
            .map(|(i, (_, k))| ((i + 1) as u8, *k))
            .collect(),
    }
}

/// Read a definition's declared dimensions from `parameter_type` /
/// `parameter_type_dimension`. `None` means no current `parameter_type` row.
fn read_declared_type(conn: &Connection, pd_id: i64) -> Result<Option<DeclaredType>, DbError> {
    let pt_id: Option<i64> = match conn.query_row(
        "SELECT id FROM parameter_type \
         WHERE parameter_definition_id = ? AND valid_to_commit IS NULL",
        params![pd_id],
        |r| r.get::<_, i64>(0),
    ) {
        Ok(id) => Some(id),
        Err(duckdb::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };
    let Some(pt_id) = pt_id else { return Ok(None) };

    let mut stmt = conn.prepare(
        "SELECT position, kind FROM parameter_type_dimension \
         WHERE parameter_type_id = ? AND valid_to_commit IS NULL ORDER BY position",
    )?;
    let mut positions: Vec<(u8, IndexKind)> = Vec::new();
    let rows = stmt.query_map(params![pt_id], |r| {
        Ok((r.get::<_, i8>(0)?, r.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (pos, kind) = row?;
        positions.push((pos as u8, parse_index_kind(&kind)?));
    }
    Ok(Some(DeclaredType { positions }))
}

/// Insert the `parameter_type` (+ its dimension rows) declaring the batch's
/// shape: `value_kind = 'indexed'` for rank ≥ 1 else the measure kind tag;
/// dimension names/kinds from the inferred columns (VMP Part 2/7).
fn create_parameter_type(
    conn: &Connection,
    commit_id: i64,
    pd_id: i64,
    inferred: &[(String, IndexKind)],
    value_kind: ValueKind,
) -> Result<(), DbError> {
    let type_kind = if inferred.is_empty() {
        value_kind.tag()
    } else {
        "indexed"
    };
    let rank = inferred.len() as i8;
    let pt_id: i64 = conn.query_row(
        "INSERT INTO parameter_type (parameter_definition_id, value_kind, rank, valid_from_commit) \
         VALUES (?, ?, ?, ?) RETURNING id",
        params![pd_id, type_kind, rank, commit_id],
        |r| r.get(0),
    )?;
    for (i, (dname, kind)) in inferred.iter().enumerate() {
        conn.execute(
            "INSERT INTO parameter_type_dimension \
               (parameter_type_id, position, dimension_name, kind, valid_from_commit) \
             VALUES (?, ?, ?, ?, ?)",
            params![pt_id, (i + 1) as i8, dname, index_kind_tag(*kind), commit_id],
        )?;
    }
    Ok(())
}

/// The definition's `retain_content` — VMP Part 5's primary retention lever
/// (default TRUE; FALSE on result classes) that `history::write_value` must see
/// via `retain_override`, else a FALSE definition silently keeps full payloads.
fn read_retain_content(conn: &Connection, pd_id: i64) -> Result<bool, DbError> {
    Ok(conn.query_row(
        "SELECT retain_content FROM parameter_definition WHERE cur_id = ?",
        params![pd_id],
        |r| r.get(0),
    )?)
}
