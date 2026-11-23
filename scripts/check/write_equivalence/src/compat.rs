//! The core's write API across revisions (see `build.rs`): check.py builds
//! this workload against an older and a newer core, so the calls whose
//! names or option fields changed go through here. Only HOW the workload
//! calls the core differs; what it writes is the same.

use nodal_db_core::types::WriteOptions;

#[cfg(core_commit)]
use nodal_db_core::{
    api::{ChangeSet, DeleteKey, Segment},
    error::DbError,
    types::{ObjectDeleteKey, ObjectDeleteOpts, StructuralChange, WriteReport},
};

/// `commit_changes` in its older positional shape, forwarding to
/// `api::commit(db, ChangeSet, ..)` on a core that has it.
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

/// Options that keep no content for replaced versions (older cores:
/// `force_replace`).
pub fn opts_no_retain() -> WriteOptions {
    #[cfg(core_retain_content)]
    return WriteOptions {
        retain_content: Some(false),
        ..WriteOptions::default()
    };
    #[cfg(not(core_retain_content))]
    return WriteOptions {
        force_replace: true,
        ..WriteOptions::default()
    };
}

/// Options with an explicit retention (older cores: `retain_override`).
pub fn opts_retain(retain: Option<bool>) -> WriteOptions {
    #[cfg(core_retain_content)]
    return WriteOptions {
        retain_content: retain,
        ..WriteOptions::default()
    };
    #[cfg(not(core_retain_content))]
    return WriteOptions {
        retain_override: retain,
        ..WriteOptions::default()
    };
}
