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
    // each have an independent Python re-encoder; both must agree.
    for rel in [
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
