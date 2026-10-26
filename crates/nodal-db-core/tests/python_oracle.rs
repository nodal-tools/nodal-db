//! Runs the independent Python oracle inside `cargo test`.
//!
//! `conformance/oracle/encode_axis.py` re-derives every encoding vector's
//! canonical bytes and XXH3-64 hash from the spec, sharing no code with the
//! Rust encoder (the "second canonicalizer for validation", CLAUDE.md
//! invariant 2). This test shells out to it and asserts it exits 0 — so a
//! Rust-side spec divergence that also regenerated the Rust goldens still gets
//! caught by an implementation that never touched Rust.
//!
//! The oracle needs the project venv (`.venv/bin/python`, with `xxhash`
//! installed); if that interpreter is absent the test fails loudly rather than
//! silently skipping — the venv is a documented conformance dependency
//! (`conformance/oracle/encode_axis.py` docstring;
//! `specs/rust_impl_session_decisions.md`).

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    // crates/nodal-db-core -> repo root
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// The oracle must stay a second implementation: it may never call the
/// core it checks. The Python skin (`nodal_db`, which exposes the core) is
/// installed in the same `.venv`, so the shortcut is one import away; this
/// fails if any oracle file imports it (plan T5,
/// `specs/unicode_text_rule_PLAN.md` §3: the oracle normalizes text with
/// its own code, never the core's `text::nfc`).
#[test]
fn oracle_never_imports_the_core() {
    fn py_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read oracle dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                py_files(&path, out);
            } else if path.extension().is_some_and(|e| e == "py") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    py_files(&repo_root().join("conformance/oracle"), &mut files);
    assert!(!files.is_empty(), "no oracle files found");
    let mut offenders = Vec::new();
    for f in &files {
        let text = std::fs::read_to_string(f).expect("read oracle file");
        for (n, line) in text.lines().enumerate() {
            let l = line.trim_start();
            let is_import = l.starts_with("import ")
                || l.starts_with("from ")
                || l.contains("import_module")
                || l.contains("__import__");
            if is_import && l.contains("nodal_db") {
                offenders.push(format!("{}:{}: {line}", f.display(), n + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "the conformance oracle must not import the core it checks (nodal_db):\n{}",
        offenders.join("\n")
    );
}

#[test]
fn independent_python_oracle_reproduces_every_vector() {
    let root = repo_root();
    let python = root.join(".venv/bin/python");
    assert!(
        python.exists(),
        "missing {} — create it with `python3 -m venv .venv && .venv/bin/pip install duckdb xxhash` \
         (see conformance/oracle/encode_axis.py)",
        python.display()
    );
    // The axis encoder (spec §4-§7) and the value encoder (spec §9, OPEN-7)
    // each have an independent Python re-encoder; both must agree. The
    // oracle's own NFC (`nfc16.py`, Unicode 16.0.0 from the vendored
    // table) first checks itself against the official NormalizationTest, in
    // its own run so a failure names it.
    for rel in [
        "conformance/oracle/nfc16.py",
        "conformance/oracle/encode_axis.py",
        "conformance/oracle/encode_value.py",
    ] {
        let oracle = root.join(rel);
        let output = Command::new(&python)
            .arg(&oracle)
            .output()
            .expect("spawn python oracle");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        println!("{rel}:\n{stdout}");
        if !stderr.is_empty() {
            eprintln!("{stderr}");
        }
        assert!(
            output.status.success(),
            "independent Python oracle {rel} DISAGREED with the Rust encoder (or failed):\n{stdout}\n{stderr}"
        );
    }
}
