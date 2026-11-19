//! Metadata gate modes (metadata_write plan §4 cost, §13 perf): a fresh
//! file with 10^5 scalar values of `p`, each linked to one source (seeded
//! with raw SQL, untimed), then the timed write:
//!
//! - `wr_reimport_keep_100k_links`: a re-import of all 10^5 values, changed,
//!   with a save-scope keep: one commit, every link closed and reopened at
//!   version 2 (the keep's close / reopen rounds);
//! - `wr_results_next_to_links_100k`: a results write of 10^5 link-free
//!   values of another parameter into the annotated file, with no
//!   statement: one commit, no link touched (the write path records no
//!   changed values of a target that carries no links).
//!
//! Needs the metadata channel (capability `metadata_channel`).

use std::path::Path;
use std::time::Instant;

use nodal_db_core::api::{commit_change_set, ChangeSet, Segment};
use nodal_db_core::conn::with_write_txn;
use nodal_db_core::schema_init::init_schema_with_default;
use nodal_db_core::types::{Action, MetadataChange, Scope, WriteOptions};

use crate::check::{check, check_commits, check_rows, elapsed_ms, head, read_hash, scalar_i64, Sample};
use crate::{api_scalars, names, scalar_batch};

const N: usize = 100_000;

fn seg(param: &str, vals: &[f64], ns: &[String]) -> Segment {
    Segment {
        class: "node".into(),
        parameter: param.into(),
        alternative: "Base".into(),
        batches: vec![scalar_batch(ns, vals)],
        domains: vec![],
    }
}

/// The annotated fixture: 10^5 scalars of `p` (10 untimed commits), one
/// source, a link from every value to it.
fn annotated(dir: &Path) -> (std::path::PathBuf, Vec<String>) {
    let db = dir.join("links.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    let ns = names(0..N);
    for chunk in ns.chunks(10_000) {
        let vals: Vec<f64> = (0..chunk.len()).map(|i| i as f64 * 0.5).collect();
        api_scalars(&db, "p", chunk, &vals);
    }
    with_write_txn(&db, "ab", "links", |conn, c| {
        conn.execute_batch(&format!(
            "INSERT INTO source (name, valid_from_commit) VALUES ('IEA', {c});
             INSERT INTO value_metadata_link (entity_id, parameter_definition_id,
                 alternative_id, target_kind, target_id, valid_from_commit)
               SELECT v.entity_id, v.parameter_definition_id, v.alternative_id, 'source',
                      (SELECT id FROM source), {c}
               FROM value_scalar v WHERE v.valid_to_commit IS NULL;"
        ))?;
        Ok(())
    })
    .unwrap();
    check_rows(
        "seeded value links",
        scalar_i64(&db, "SELECT count(*) FROM value_metadata_link") as usize,
        N,
    );
    (db, ns)
}

fn links_digest(s: Sample, db: &Path) -> Sample {
    let h = read_hash(
        db,
        "SELECT entity, parameter, target_name FROM value_metadata_named",
    );
    check_rows("live value links", h.rows as usize, N);
    s.hash("links", &h)
}

/// Dispatch a metadata gate mode; `None` if `mode` is not one.
pub fn run(mode: &str, dir: &Path) -> Option<Sample> {
    Some(match mode {
        "wr_reimport_keep_100k_links" => {
            let (db, ns) = annotated(dir);
            let vals: Vec<f64> = (0..N).map(|i| i as f64 * 0.5 + 0.25).collect();
            let before = head(&db);
            let t = Instant::now();
            let r = commit_change_set(
                &db,
                ChangeSet {
                    segments: vec![seg("p", &vals, &ns)],
                    metadata: vec![MetadataChange::ValueMetadata {
                        scope: Scope::Save,
                        action: Action::Keep,
                    }],
                    ..Default::default()
                },
                "ab",
                "ab",
                WriteOptions::default(),
            )
            .unwrap();
            let ms = elapsed_ms(t);
            check_commits(&db, before, 1);
            check(r.value_links.kept == N, "every link kept");
            check_rows(
                "links at version 2",
                scalar_i64(
                    &db,
                    "SELECT count(*) FROM value_metadata_link \
                     WHERE valid_to_commit IS NULL AND version = 2",
                ) as usize,
                N,
            );
            links_digest(Sample::new(ms), &db)
        }
        "wr_results_next_to_links_100k" => {
            let (db, ns) = annotated(dir);
            let vals: Vec<f64> = (0..N).map(|i| i as f64 * 0.125).collect();
            let rows = scalar_i64(&db, "SELECT count(*) FROM value_metadata_link");
            let before = head(&db);
            let t = Instant::now();
            commit_change_set(
                &db,
                ChangeSet {
                    segments: vec![seg("r", &vals, &ns)],
                    ..Default::default()
                },
                "ab",
                "ab",
                WriteOptions::default(),
            )
            .unwrap();
            let ms = elapsed_ms(t);
            check_commits(&db, before, 1);
            check_rows(
                "link rows untouched",
                scalar_i64(&db, "SELECT count(*) FROM value_metadata_link") as usize,
                rows as usize,
            );
            let s = links_digest(Sample::new(ms), &db);
            let h = read_hash(
                &db,
                "SELECT entity, v_float FROM read_parameter_scalar('node', 'r')",
            );
            check_rows("results read back", h.rows as usize, N);
            s.hash("results", &h)
        }
        _ => return None,
    })
}
