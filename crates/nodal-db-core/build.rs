//! Passes the release build's git commit to the `created_by` database
//! property (`schema_init::created_by`), so a file from a 0.x test user can
//! be traced to the build that made it.
//!
//! Only a build that sets `NODAL_DB_GIT_COMMIT` records a commit: release
//! and CI builds export it (e.g. `NODAL_DB_GIT_COMMIT=$(git rev-parse
//! --short=12 HEAD) cargo build --release`, or `maturin build` with the same
//! variable). Every other build records `(dev)`. Nothing here runs git or
//! watches files, so a commit in a dev checkout does not rebuild the core.

fn main() {
    println!("cargo:rerun-if-env-changed=NODAL_DB_GIT_COMMIT");
    let commit = std::env::var("NODAL_DB_GIT_COMMIT")
        .map(|c| c.trim().to_string())
        .unwrap_or_default();
    println!("cargo:rustc-env=NODAL_DB_GIT_COMMIT={commit}");
}
