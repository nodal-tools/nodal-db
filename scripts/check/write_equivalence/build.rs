//! Detects the write API of the core this workload builds against (the
//! path dependency points at each revision's core in check.py's extracted
//! trees): `core_commit` when it has `api::commit(db, ChangeSet, ..)`,
//! `core_retain_content` when `WriteOptions` has `retain_content`. Only
//! `src/compat.rs` reads them.
use std::path::Path;

fn main() {
    println!("cargo::rustc-check-cfg=cfg(core_commit)");
    println!("cargo::rustc-check-cfg=cfg(core_retain_content)");
    let core = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../crates/nodal-db-core/src");
    for f in ["api.rs", "types.rs"] {
        println!("cargo::rerun-if-changed={}", core.join(f).display());
    }
    let read = |f: &str| std::fs::read_to_string(core.join(f)).unwrap_or_default();
    if read("api.rs").contains("\npub fn commit(") {
        println!("cargo::rustc-cfg=core_commit");
    }
    if read("types.rs").contains("pub retain_content: Option<bool>") {
        println!("cargo::rustc-cfg=core_retain_content");
    }
}
