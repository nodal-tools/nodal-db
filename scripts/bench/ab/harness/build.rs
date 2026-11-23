//! Detects which write entry point the core this harness builds against
//! exports, so one harness source builds against every revision the perf
//! gate compares. A core with `api::commit(db, ChangeSet, ..)` gets the
//! `core_commit` cfg (and `core_wp_user_first` when `write_parameter` takes
//! `user, message` before `opts`); an older core (with `commit_changes` /
//! `commit_change_set`) does not. The probe reads the core's source at the
//! path dependency's location (the same relative path in every arm's
//! extracted tree). It only switches HOW the harness calls the core
//! (`src/compat.rs`), never what a metric measures.
use std::path::Path;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(core_commit)");
    println!("cargo::rustc-check-cfg=cfg(core_wp_user_first)");
    let api = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../../crates/nodal-db-core/src/api.rs");
    println!("cargo::rerun-if-changed={}", api.display());
    let src = std::fs::read_to_string(&api).unwrap_or_default();
    if src.contains("\npub fn commit(") {
        println!("cargo::rustc-cfg=core_commit");
    }
    // `write_parameter(.., domains, user, message, opts)` (newer) vs
    // `(.., domains, opts, user, message)`.
    if src.contains("domains: &[LabelDomain],\n    user: &str,") {
        println!("cargo::rustc-cfg=core_wp_user_first");
    }
}
