//! Records the git commit of the build for the `created_by` database
//! property (`schema_init::created_by`), so a file from a 0.x test user can
//! be traced to the build that made it.
//!
//! `NODAL_DB_GIT_COMMIT` in the build environment wins (a release pipeline
//! building from an archive without `.git`); otherwise `git rev-parse` in the
//! source tree; otherwise empty (the property then names the package version
//! only). The script reruns when HEAD or the branch it points to moves.

use std::path::{Path, PathBuf};
use std::process::Command;

fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// Ask cargo to rerun when `path` changes; only for existing files (a
/// missing path would rerun the script on every build).
fn watch(path: &Path) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn main() {
    println!("cargo:rerun-if-env-changed=NODAL_DB_GIT_COMMIT");
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default());
    let commit = match std::env::var("NODAL_DB_GIT_COMMIT") {
        Ok(c) => c.trim().to_string(),
        Err(_) => {
            if let Some(git_dir) = git(&manifest, &["rev-parse", "--absolute-git-dir"]) {
                let git_dir = PathBuf::from(git_dir);
                let head = git_dir.join("HEAD");
                watch(&head);
                let common = git(
                    &manifest,
                    &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                )
                .map(PathBuf::from)
                .unwrap_or_else(|| git_dir.clone());
                if let Ok(text) = std::fs::read_to_string(&head) {
                    if let Some(r) = text.trim().strip_prefix("ref: ") {
                        watch(&common.join(r));
                    }
                }
                watch(&common.join("packed-refs"));
            }
            git(&manifest, &["rev-parse", "--short=12", "HEAD"]).unwrap_or_default()
        }
    };
    println!("cargo:rustc-env=NODAL_DB_GIT_COMMIT={commit}");
}
