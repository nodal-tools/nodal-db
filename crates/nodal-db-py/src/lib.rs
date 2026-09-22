//! `nodal_db` — the thin Python skin over `nodal-db-core`.
//!
//! **This is the write-path SDK and nothing more**
//! (`specs/engine_portability_FINDINGS.md` §7): marshal Arrow tables and
//! scalars in, call the one core, translate `DbError` into the exception
//! hierarchy out. No canonicalization, tier, history, or reconcile rule is
//! implemented here (CLAUDE.md invariant 2) — a rule you cannot find in
//! `nodal-db-core` does not exist in this crate either.
//!
//! ## Connection discipline (CLAUDE.md invariant 1)
//! [`Database`] holds the PATH only — never a connection. Every method opens,
//! works, and closes inside the core (per-operation connections), so a
//! long-lived `Database` object in a notebook can never starve a writer.
//!
//! ## GIL / reentrancy (template_ir_plan D13 — the Layer-3 contract)
//! Every entry point releases the GIL (`py.detach`) around the core call, so
//! other Python threads run during DB work. `template_apply` registers plain
//! Python callables as `run:` migration programs: the core invokes them at a
//! point where it holds **no connection and no in-process path lock** (the
//! `run:` step in `reconcile::apply` sits between per-operation commits), and
//! the wrapper re-acquires the GIL (`Python::attach`) only for the call
//! itself. A program that calls back into `nodal_db` therefore re-enters
//! cleanly: its own entry point releases the GIL again and the core opens a
//! fresh per-operation connection. Deadlock would require the core to hold a
//! lock across the callback — it does not, by construction. A Python
//! exception raised by a program aborts the apply and is re-raised to the
//! caller as the ORIGINAL Python exception, not a wrapped message.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{RecordBatchIterator, RecordBatchReader};
use arrow::record_batch::RecordBatch;
use arrow_pyarrow::{IntoPyArrow, ToPyArrow};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use nodal_db_core::api;
use nodal_db_core::conn::with_read;
use nodal_db_core::erasure;
use nodal_db_core::error::DbError;
use nodal_db_core::schema_init;
use nodal_db_core::template as tpl;
use nodal_db_core::types::{ObjectDeleteKey, WriteOptions};

mod convert;
use convert::*;

// ---------------------------------------------------------------------------
// The exception hierarchy (error taxonomy: one Python class per DbError
// variant family; `building_against_nodal-db.md` — busy/conflict are defined
// outcomes, never hangs or stack traces).
// ---------------------------------------------------------------------------

create_exception!(
    nodal_db,
    NodalDbError,
    PyException,
    "Base class for every nodal-db error."
);
create_exception!(
    nodal_db,
    Busy,
    NodalDbError,
    "The database file is locked by another connection (DbError::Busy). \
     Retry shortly; hold no long-lived handles yourself."
);
create_exception!(
    nodal_db,
    Conflict,
    NodalDbError,
    "A write-write conflict persisted past the bounded retry budget \
     (DbError::Conflict)."
);
create_exception!(
    nodal_db,
    ApiError,
    NodalDbError,
    "Caller-contract error: a batch, name, or argument the core cannot \
     interpret (DbError::Api)."
);
create_exception!(
    nodal_db,
    VersionTooOld,
    NodalDbError,
    "The bundled DuckDB is older than the verified line (DbError::VersionTooOld)."
);
create_exception!(
    nodal_db,
    SchemaError,
    NodalDbError,
    "The schema directory could not be read (DbError::SchemaDir)."
);
create_exception!(
    nodal_db,
    DatabaseError,
    NodalDbError,
    "A DuckDB error surfaced unchanged (DbError::Duckdb)."
);
create_exception!(
    nodal_db,
    CanonError,
    NodalDbError,
    "A canonicalization-pipeline error (DbError::Canon / DbError::Encode)."
);
create_exception!(
    nodal_db,
    ArrowError,
    NodalDbError,
    "An Arrow compute/import error (DbError::Arrow or a batch import failure)."
);
create_exception!(
    nodal_db,
    InternalError,
    NodalDbError,
    "Stored-data damage the core refuses to interpret \
     (DbError::UnknownAxisKind / DbError::MalformedAxis)."
);

// ---------------------------------------------------------------------------
// Embedded schema: the ONE compile-time list lives in the core
// (`schema_init::EMBEDDED_SCHEMA`, shared with the CLI `init` verb and
// census-tested against `schema/*.sql`), so `nodal_db.init` works from an
// installed wheel with no repo checkout and cannot drift from the CLI's.
// ---------------------------------------------------------------------------

use nodal_db_core::schema_init::EMBEDDED_SCHEMA;

// ---------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------

/// A lightweight handle: **it holds the database PATH only, never a
/// connection.** Every method opens a fresh per-operation connection inside
/// the core and closes it before returning (CLAUDE.md invariant 1) — keeping
/// a `Database` object alive is free and never blocks anyone. There is no
/// `close()` because there is nothing to close.
#[pyclass(module = "nodal_db", frozen)]
struct Database {
    path: PathBuf,
}

#[pymethods]
impl Database {
    /// The database file path this handle points at.
    #[getter]
    fn path(&self) -> String {
        self.path.display().to_string()
    }

    fn __repr__(&self) -> String {
        format!("Database({:?})", self.path.display().to_string())
    }

    /// The database's stable identity UUID (minted once at init; survives
    /// compaction). One per-operation read.
    fn db_identity(&self, py: Python<'_>) -> PyResult<String> {
        let path = self.path.clone();
        py.detach(move || api::db_identity(&path)).map_err(db_err)
    }

    /// Run one read-only SQL query and return the result as a
    /// `pyarrow.RecordBatchReader` (zero-copy across the Arrow C data
    /// interface; call `.read_all()` for a `pyarrow.Table`).
    ///
    /// The connection is opened READ-ONLY, used for this one query, and
    /// closed before this returns — the reader you get is backed by
    /// already-materialized batches, never by a live connection, so holding
    /// it does not hold the file. Read through the views/macros
    /// (`read_parameter(...)`, `audit_*`, ...) — "SQL is the API"; base-table
    /// layout is not a contract.
    fn query<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyAny>> {
        let path = self.path.clone();
        let (schema, batches) = py
            .detach(move || {
                with_read(&path, |conn| {
                    let mut stmt = conn.prepare(&sql)?;
                    let arrow = stmt.query_arrow([])?;
                    let schema = arrow.get_schema();
                    let batches: Vec<RecordBatch> = arrow.collect();
                    Ok((schema, batches))
                })
            })
            .map_err(db_err)?;
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        ));
        reader.into_pyarrow(py)
    }

    /// Read one parameter (all entities of one class/parameter/alternative)
    /// back through the `read_parameter` SQL macro, as a `pyarrow.RecordBatch`
    /// in the macro's canonical long shape.
    fn read_parameter<'py>(
        &self,
        py: Python<'py>,
        class_name: String,
        parameter: String,
        alternative: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let path = self.path.clone();
        let batch = py
            .detach(move || api::read_parameter(&path, &class_name, &parameter, &alternative))
            .map_err(db_err)?;
        batch.to_pyarrow(py)
    }

    /// Write one parameter (one class/parameter/alternative target) from a
    /// logical-long Arrow batch (`pyarrow.RecordBatch`, `pyarrow.Table`, or
    /// any Arrow PyCapsule producer): column 0 = entity names (utf8), then
    /// the dimension columns in declared order, last column = the value.
    /// The CORE decides the storage tier and auto-creates missing referents
    /// with machine-readable warnings in the returned report dict.
    ///
    /// `domains`: optional `{position: [label, ...]}` ordinal domains for
    /// label dimensions (position is 1-based).
    #[pyo3(signature = (class_name, parameter, alternative, batch, *, user, message = String::new(),
                        domains = None, force_replace = false, retain_override = None))]
    #[allow(clippy::too_many_arguments)]
    fn write_parameter<'py>(
        &self,
        py: Python<'py>,
        class_name: String,
        parameter: String,
        alternative: String,
        batch: &Bound<'py, PyAny>,
        user: String,
        message: String,
        domains: Option<HashMap<u8, Vec<String>>>,
        force_replace: bool,
        retain_override: Option<bool>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let batches = batches_from_py(batch)?;
        let path = self.path.clone();
        let opts = WriteOptions {
            force_replace,
            retain_override,
        };
        let seg = api::Segment {
            class: class_name,
            parameter,
            alternative,
            batches,
            domains: domains_from_py(domains),
        };
        let report = py
            .detach(move || api::ingest_segments(&path, vec![seg], &user, &message, opts))
            .map_err(db_err)?;
        report_to_py(py, &report)
    }

    /// Commit writes AND deletes atomically under ONE commit row — the
    /// editor-save shape, mirroring `api::commit_changes`.
    ///
    /// * `segments`: list of dicts `{"class", "parameter", "alternative",
    ///   "batch", "domains"?}` (see `write_parameter` for the batch shape).
    /// * `deletes`: list of `(class, parameter, alternative, entity)` tuples
    ///   (resolved case-insensitively, never auto-created; a missing referent
    ///   is a warning + skip, never an error).
    /// * `object_deletes`: list of dicts `{"kind": "entity"|"parameter"|
    ///   "alternative"|"class", "class"?: str, "name": str | "elements":
    ///   [str], "cascade": bool}`.
    #[pyo3(signature = (*, segments = None, deletes = None, object_deletes = None, user,
                        message = String::new(), force_replace = false, retain_override = None))]
    #[allow(clippy::too_many_arguments)]
    fn commit_changes<'py>(
        &self,
        py: Python<'py>,
        segments: Option<&Bound<'py, PyAny>>,
        deletes: Option<&Bound<'py, PyAny>>,
        object_deletes: Option<&Bound<'py, PyAny>>,
        user: String,
        message: String,
        force_replace: bool,
        retain_override: Option<bool>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let mut segs = Vec::new();
        if let Some(s) = segments {
            for item in s.try_iter()? {
                segs.push(segment_from_py(&item?)?);
            }
        }
        let mut dels = Vec::new();
        if let Some(d) = deletes {
            for item in d.try_iter()? {
                dels.push(delete_key_from_py(&item?)?);
            }
        }
        let mut odels = Vec::new();
        if let Some(o) = object_deletes {
            for item in o.try_iter()? {
                odels.push(object_delete_from_py(&item?)?);
            }
        }
        let path = self.path.clone();
        let opts = WriteOptions {
            force_replace,
            retain_override,
        };
        let report = py
            .detach(move || api::commit_changes(&path, segs, dels, odels, &user, &message, opts))
            .map_err(db_err)?;
        report_to_py(py, &report)
    }

    /// Delete (append-only close-out) named values in one atomic commit —
    /// the deletes-only convenience over `commit_changes`. `keys` is a list
    /// of `(class, parameter, alternative, entity)` tuples.
    #[pyo3(signature = (keys, *, user, message = String::new()))]
    fn delete_values<'py>(
        &self,
        py: Python<'py>,
        keys: &Bound<'py, PyAny>,
        user: String,
        message: String,
    ) -> PyResult<Bound<'py, PyDict>> {
        let mut dels = Vec::new();
        for item in keys.try_iter()? {
            dels.push(delete_key_from_py(&item?)?);
        }
        let path = self.path.clone();
        let report = py
            .detach(move || {
                api::delete_values(&path, dels, &user, &message, WriteOptions::default())
            })
            .map_err(db_err)?;
        report_to_py(py, &report)
    }

    /// Read-only preview of object deletes: what `cascade=True` would close
    /// out and what `cascade=False` would block on, per key (same dict shape
    /// as `commit_changes`' `object_deletes`, minus `cascade`). Best-effort
    /// snapshot — the commit-time outcome is authoritative.
    fn preview_object_deletes<'py>(
        &self,
        py: Python<'py>,
        keys: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut ks: Vec<ObjectDeleteKey> = Vec::new();
        for item in keys.try_iter()? {
            ks.push(object_key_from_py(&item?)?);
        }
        let path = self.path.clone();
        let previews = py
            .detach(move || api::preview_object_deletes(&path, ks))
            .map_err(db_err)?;
        object_previews_to_py(py, &previews)
    }

    // -- template ----------------------------------------------------------

    /// Read-only dry run of a template reconcile: parse + validate the
    /// template file, diff it against the database, and return the plan as a
    /// dict (`"text"` is the core's human rendering; quote `"db_commit"` to
    /// `template_apply(expect_commit=...)` to pin plan -> apply).
    fn template_plan<'py>(
        &self,
        py: Python<'py>,
        template_path: PathBuf,
    ) -> PyResult<Bound<'py, PyDict>> {
        let path = self.path.clone();
        let plan = py
            .detach(move || {
                let t = tpl::parse_file(&template_path)?;
                tpl::plan(&path, &t)
            })
            .map_err(db_err)?;
        plan_to_py(py, &plan)
    }

    /// Apply a template: run the pending migration chain (declarative steps
    /// AND `run:` programs), then the additive reconcile diff in one commit.
    /// On a fresh database this IS the load.
    ///
    /// `programs` is the D13 registration surface: a `{name: callable}` dict
    /// of plain Python callables, registered as `run:` migration programs
    /// for the duration of THIS call only. Each is invoked as
    /// `program(db_path, user)` and must do all its writes through the
    /// ordinary API (e.g. `nodal_db.open(db_path).write_parameter(...)`) —
    /// calling back into `nodal_db` from inside a program is the intended
    /// use and is reentrancy-safe (module docs). Programs MUST be idempotent:
    /// a crash between a program's own commit and the progress substamp
    /// re-runs it (the core's Layer-3 contract). A program that raises
    /// aborts the apply; the original exception propagates.
    #[pyo3(signature = (template_path, *, user, message = String::new(), programs = None,
                        expect_commit = None, allow_retarget = false, allow_downgrade = false))]
    #[allow(clippy::too_many_arguments)]
    fn template_apply<'py>(
        &self,
        py: Python<'py>,
        template_path: PathBuf,
        user: String,
        message: String,
        programs: Option<HashMap<String, Py<PyAny>>>,
        expect_commit: Option<i64>,
        allow_retarget: bool,
        allow_downgrade: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        // Build the registry for the duration of this call. Each entry wraps
        // the Python callable: re-acquire the GIL only for the call
        // (`Python::attach`), translate a raised exception into DbError so
        // the core aborts the chain, and stash the ORIGINAL PyErr so the
        // caller sees their exception, not a rendering of it. No core-side
        // bound relaxation was needed: `Py<PyAny>` is already Send + Sync,
        // so the closure satisfies MigrationRegistry's existing
        // `Fn + Send + Sync` bound as-is (D13 anticipated a relaxation;
        // pyo3's GIL-independent handles make it unnecessary).
        let mut registry = tpl::MigrationRegistry::new();
        let pending: Arc<Mutex<Option<PyErr>>> = Arc::new(Mutex::new(None));
        for (name, callable) in programs.unwrap_or_default() {
            if !callable.bind(py).is_callable() {
                return Err(ApiError::new_err(format!(
                    "program {name:?} is not callable"
                )));
            }
            let pend = pending.clone();
            let pname = name.clone();
            registry.register(&name, move |db: &Path, user: &str| {
                Python::attach(|py| {
                    let db_str = db.display().to_string();
                    match callable.call1(py, (db_str, user)) {
                        Ok(_) => Ok(()),
                        Err(err) => {
                            // INVARIANT this stash depends on: the core's
                            // runner propagates a program's Err IMMEDIATELY
                            // (`program(db, user)?` in reconcile.rs `apply`,
                            // the MigrationStep::Run arm) — the chain aborts,
                            // no later step runs, and the Err we return here
                            // is the Err `apply` returns, so the stashed
                            // PyErr below is exactly the exception the apply
                            // failed on. If the core ever starts swallowing
                            // or deferring program errors, this stash could
                            // surface a STALE exception for an unrelated
                            // failure — revisit this pairing then.
                            let msg = format!("run program {pname:?} raised: {err}");
                            *pend.lock().unwrap() = Some(err);
                            Err(DbError::Api(msg))
                        }
                    }
                })
            });
        }
        let path = self.path.clone();
        let opts = tpl::ApplyOptions {
            allow_retarget,
            allow_downgrade,
            expect_commit,
        };
        let result = py.detach(move || {
            let t = tpl::parse_file(&template_path)?;
            tpl::apply(&path, &t, &registry, &user, &message, &opts)
        });
        match result {
            Ok(outcome) => apply_outcome_to_py(py, &outcome),
            Err(e) => {
                // A program's own Python exception outranks the wrapper text.
                if let Some(orig) = pending.lock().unwrap().take() {
                    return Err(orig);
                }
                Err(db_err(e))
            }
        }
    }

    // -- erasure -----------------------------------------------------------

    /// Read-only preview of `erase_alternative`: per-table row counts, an
    /// advisory bytes estimate, referencing scenarios/runs, and the verdict
    /// the same `expect_present` names would produce.
    #[pyo3(signature = (alternative, *, expect_present = Vec::new()))]
    fn preview_erase_alternative<'py>(
        &self,
        py: Python<'py>,
        alternative: String,
        expect_present: Vec<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let path = self.path.clone();
        let preview = py
            .detach(move || {
                let key = erase_key(&alternative);
                let expect: Vec<&str> = expect_present.iter().map(String::as_str).collect();
                let opts = erasure::EraseOpts {
                    note: None,
                    expect_present: &expect,
                };
                erasure::preview_erase_alternative(&path, &key, &opts)
            })
            .map_err(db_err)?;
        erase_preview_to_py(py, &preview)
    }

    /// Hard erasure: physically remove every value row of one alternative
    /// and write the `erasure_marker`, in one atomic transaction — the
    /// sanctioned exception to append-only history. `expect_present` is the
    /// rerun-overwrite belt: names that MUST have a current alternative row
    /// or the erase refuses, touching nothing.
    #[pyo3(signature = (alternative, *, user, note = None, expect_present = Vec::new()))]
    fn erase_alternative<'py>(
        &self,
        py: Python<'py>,
        alternative: String,
        user: String,
        note: Option<String>,
        expect_present: Vec<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let path = self.path.clone();
        let report = py
            .detach(move || {
                let key = erase_key(&alternative);
                let expect: Vec<&str> = expect_present.iter().map(String::as_str).collect();
                let opts = erasure::EraseOpts {
                    note: note.as_deref(),
                    expect_present: &expect,
                };
                erasure::erase_alternative(&path, &key, &opts, &user)
            })
            .map_err(db_err)?;
        erase_report_to_py(py, &report)
    }
}

/// The typed erase target for an alternative name (the only erase scope in
/// v1), mirroring the CLI's construction.
fn erase_key(name: &str) -> ObjectDeleteKey {
    ObjectDeleteKey {
        kind: nodal_db_core::types::ObjectKind::Alternative,
        class: None,
        target: nodal_db_core::types::ObjectRef::Name(name.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Module functions
// ---------------------------------------------------------------------------

/// Open a database by path. Cheap: this only records the path (and checks the
/// file exists) — connections are per-operation inside each method call.
#[pyfunction]
fn open(path: PathBuf) -> PyResult<Database> {
    if !path.is_file() {
        return Err(ApiError::new_err(format!(
            "no database file at {:?} (create one with nodal_db.init)",
            path.display().to_string()
        )));
    }
    Ok(Database { path })
}

/// Create a fresh database at `path` and apply the full schema (embedded in
/// this wheel at build time — same files as `nodal-db init`). Seeds NO
/// alternative unless `default_alternative` is given. `schema_dir` is the
/// dev override to apply the schema from a directory instead.
#[pyfunction]
#[pyo3(signature = (path, *, default_alternative = None, schema_dir = None))]
fn init(
    py: Python<'_>,
    path: PathBuf,
    default_alternative: Option<String>,
    schema_dir: Option<PathBuf>,
) -> PyResult<Database> {
    py.detach(move || {
        match schema_dir {
            Some(dir) => schema_init::init_schema_from_dir_with_default(
                &path,
                &dir,
                default_alternative.as_deref(),
            ),
            None => schema_init::init_schema_from_sources_with_default(
                &path,
                EMBEDDED_SCHEMA,
                default_alternative.as_deref(),
            ),
        }
        .map(|()| path)
    })
    .map(|path| Database { path })
    .map_err(db_err)
}

/// The thin Python skin over nodal-db-core. See the crate docs at the top of
/// `lib.rs` for the connection-discipline and GIL/reentrancy contracts.
#[pymodule]
fn nodal_db(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Database>()?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(init, m)?)?;
    let py = m.py();
    m.add("NodalDbError", py.get_type::<NodalDbError>())?;
    m.add("Busy", py.get_type::<Busy>())?;
    m.add("Conflict", py.get_type::<Conflict>())?;
    m.add("ApiError", py.get_type::<ApiError>())?;
    m.add("VersionTooOld", py.get_type::<VersionTooOld>())?;
    m.add("SchemaError", py.get_type::<SchemaError>())?;
    m.add("DatabaseError", py.get_type::<DatabaseError>())?;
    m.add("CanonError", py.get_type::<CanonError>())?;
    m.add("ArrowError", py.get_type::<ArrowError>())?;
    m.add("InternalError", py.get_type::<InternalError>())?;
    Ok(())
}
