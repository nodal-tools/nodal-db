//! The core's write entry point across revisions (see `build.rs`): the
//! harness calls `commit_changes` (and, with the metadata channel,
//! `commit_change_set`) in the shape older cores export; on a core that has
//! the single `api::commit(db, ChangeSet, ..)` these forward to it.

#[cfg(core_commit)]
use nodal_db_core::{
    api::{ChangeSet, DeleteKey, Segment},
    error::DbError,
    types::{ObjectDeleteKey, ObjectDeleteOpts, StructuralChange, WriteOptions, WriteReport},
};

#[cfg(core_commit)]
#[allow(clippy::too_many_arguments)]
pub fn commit_changes(
    db: &std::path::Path,
    segments: Vec<Segment>,
    structural: Vec<StructuralChange>,
    deletes: Vec<DeleteKey>,
    object_deletes: Vec<(ObjectDeleteKey, ObjectDeleteOpts)>,
    user: &str,
    message: &str,
    opts: WriteOptions,
) -> Result<WriteReport, DbError> {
    nodal_db_core::api::commit(
        db,
        ChangeSet {
            segments,
            structural,
            deletes,
            object_deletes,
            ..ChangeSet::default()
        },
        user,
        message,
        opts,
    )
}

#[cfg(not(core_commit))]
pub use nodal_db_core::api::commit_changes;

#[cfg(all(feature = "metadata_channel", core_commit))]
pub use nodal_db_core::api::commit as commit_change_set;

#[cfg(all(feature = "metadata_channel", not(core_commit)))]
pub use nodal_db_core::api::commit_change_set;

/// `write_parameter` in the older argument order (`opts, user, message`).
#[cfg(core_wp_user_first)]
#[allow(clippy::too_many_arguments)]
pub fn write_parameter(
    db: &std::path::Path,
    class: &str,
    parameter: &str,
    alternative: &str,
    batch: nodal_db_core::duckdb::arrow::record_batch::RecordBatch,
    domains: &[nodal_db_core::canon::LabelDomain],
    opts: nodal_db_core::types::WriteOptions,
    user: &str,
    message: &str,
) -> Result<nodal_db_core::types::WriteReport, nodal_db_core::error::DbError> {
    nodal_db_core::api::write_parameter(
        db,
        class,
        parameter,
        alternative,
        batch,
        domains,
        user,
        message,
        opts,
    )
}

#[cfg(not(core_wp_user_first))]
pub use nodal_db_core::api::write_parameter;
