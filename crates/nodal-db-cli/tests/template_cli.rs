//! End-to-end CLI test for the un-frozen `template` verb surface
//! (template_ir_plan phases 2b/3/6): `init --template`, `template plan`,
//! `template apply` (idempotent), `template load` (fresh-init guard), and
//! `template export` feeding a second `init --template` — the CLI-level
//! round trip.

use std::path::Path;
use std::process::Command;

use nodal_db_core::template::ir::normalized_model;
use nodal_db_core::template::{export, parse_file};

const SAMPLE: &str =
    include_str!("../../nodal-db-core/tests/fixtures/template_sample.yaml");

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_nodal-db"));
    c.env("USER", "cli_tester");
    c
}

fn run_ok(c: &mut Command) -> String {
    let out = c.output().expect("spawn nodal-db");
    assert!(
        out.status.success(),
        "command failed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn run_err(c: &mut Command) -> String {
    let out = c.output().expect("spawn nodal-db");
    assert!(!out.status.success(), "expected failure");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn template_verbs_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let tpl = dir.path().join("sample.yaml");
    std::fs::write(&tpl, SAMPLE).unwrap();

    // init --template: schema + load in one invocation.
    let db1 = dir.path().join("a.duckdb");
    let out = run_ok(bin().args(["init"]).arg(&db1).arg("--template").arg(&tpl));
    assert!(out.contains("applied template sample version 3"), "{out}");

    // plan against an up-to-date DB: no actions.
    let out = run_ok(
        bin().args(["template", "plan"]).arg(&tpl).arg("--db").arg(&db1),
    );
    assert!(out.contains("NO-OP"), "{out}");

    // apply is idempotent.
    let out = run_ok(
        bin().args(["template", "apply"]).arg(&tpl).arg("--db").arg(&db1),
    );
    assert!(out.contains("no-op"), "{out}");

    // export to a file…
    let exported = dir.path().join("exported.yaml");
    run_ok(
        bin()
            .args(["template", "export"])
            .arg("--db")
            .arg(&db1)
            .arg("--out")
            .arg(&exported),
    );

    // …and the export loads into a second DB, semantically identical.
    let db2 = dir.path().join("b.duckdb");
    run_ok(bin().args(["init"]).arg(&db2).arg("--template").arg(&exported));
    let (e1, _) = export(Path::new(&db1)).unwrap();
    let (e2, _) = export(Path::new(&db2)).unwrap();
    assert_eq!(normalized_model(&e1), normalized_model(&e2));
    let source = parse_file(&tpl).unwrap();
    assert_eq!(normalized_model(&source), normalized_model(&e1));

    // The retarget guard lives in the CORE (one decision, review item 1): a
    // DB stamped with a DIFFERENT template refuses via load AND apply, and
    // the CLI exposes no override.
    let other = dir.path().join("other.yaml");
    std::fs::write(
        &other,
        "template: {name: other, version: \"1\"}\nclasses: [{name: c}]\n",
    )
    .unwrap();
    let err = run_err(
        bin().args(["template", "load"]).arg(&other).arg("--db").arg(&db1),
    );
    assert!(err.contains("retarget"), "{err}");
    let err = run_err(
        bin().args(["template", "apply"]).arg(&other).arg("--db").arg(&db1),
    );
    assert!(err.contains("retarget"), "{err}");

    // --expect-commit: a stale pin refuses (plan→apply staleness guard).
    let err = run_err(
        bin()
            .args(["template", "apply"])
            .arg(&tpl)
            .arg("--db")
            .arg(&db1)
            .args(["--expect-commit", "1"]),
    );
    assert!(err.contains("stale plan"), "{err}");

    // JSON input parses through the same loader (D1).
    let json_tpl = dir.path().join("mini.json");
    std::fs::write(
        &json_tpl,
        r#"{"template": {"name": "mini", "version": "1"}, "classes": [{"name": "c"}]}"#,
    )
    .unwrap();
    let db3 = dir.path().join("c.duckdb");
    run_ok(bin().args(["init"]).arg(&db3).arg("--template").arg(&json_tpl));
}
