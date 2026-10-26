//! The gate modes (ab_ratio_gates_PLAN §2 metric set) outside the synthetic
//! fixture: whole-commit writes through `api::*` on copies of the
//! `NODAL_AB_BASE` / `NODAL_AB_BIG` / `NODAL_AB_LONGBIG` files or on a
//! fresh file, single-name lookups, and the editor save / reload.
//!
//! Every mode times only the operation (setup, copies and checks are
//! untimed) and then checks its result (critique M4): writes check the
//! number of new commits and read the written keys back through the SQL
//! read macros (count + digest), deletes check the keys are gone and digest
//! what remains, skip-unchanged modes check that no commit and no history
//! row was made, lookups map the ids back to names.

use std::path::Path;
use std::time::Instant;

use nodal_db_core::schema_init::init_schema_with_default;

use crate::check::{
    check, check_commits, check_rows, elapsed_ms, head, in_list, read_hash, scalar_i64, Sample,
};
use crate::{
    api_commit, api_scalars, copy_base, copy_big, del_keys, names, q_segment, series_seg, sname,
};

fn scalar_readback(param: &str, entities: &[String]) -> String {
    format!(
        "SELECT entity, alternative, value_kind, v_float, v_int, v_bool, v_str \
         FROM read_parameter_scalar('node', '{param}') WHERE entity IN ({})",
        in_list(entities)
    )
}

fn param_readback(class: &str, param: &str, entities: Option<&[String]>) -> String {
    let filter = entities.map_or(String::new(), |e| {
        format!(" WHERE entity IN ({})", in_list(e))
    });
    format!("SELECT * FROM read_parameter('{class}', '{param}'){filter}")
}

fn fresh(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("fresh.duckdb");
    init_schema_with_default(&db, Some("Base")).unwrap();
    db
}

/// Read back `sql`, check its row count, and attach its digest.
fn readback(s: Sample, db: &Path, sql: &str, want: usize) -> Sample {
    let h = read_hash(db, sql);
    check_rows("read-back", h.rows as usize, want);
    s.hash("readback", &h)
}

fn scalar_writes(dir: &Path, mode: &str) -> Sample {
    let db = copy_base(dir);
    let before = head(&db);
    let mut tot = 0.0;
    let mut written = vec![];
    let (commits, units) = match mode {
        "wr_scalar_100_new_100k" => {
            for run in 0..3 {
                let ns: Vec<String> = (0..100).map(|i| format!("w{run}_{i:03}")).collect();
                let vals: Vec<f64> = (0..100).map(|i| i as f64).collect();
                tot += api_scalars(&db, "p", &ns, &vals);
                written.extend(ns);
            }
            (3, 300.0)
        }
        _ => {
            // wr_scalar_1_replace_100k: the editor's single-value save.
            for run in 0..3 {
                let ns = vec![sname(50_000 + run)];
                tot += api_scalars(&db, "p", &ns, &[run as f64 + 0.25]);
                written.extend(ns);
            }
            (3, 3.0)
        }
    };
    check_commits(&db, before, commits);
    let n = written.len();
    readback(
        Sample::new(tot / units),
        &db,
        &scalar_readback("p", &written),
        n,
    )
}

fn q_readback() -> String {
    (0..20)
        .map(|i| {
            format!(
                "SELECT 'q{i:02}' AS param, entity, alternative, value_kind, v_float \
                 FROM read_parameter_scalar('node', 'q{i:02}') WHERE entity = 's000000'"
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn deletes(dir: &Path, mode: &str) -> Sample {
    let db = copy_big(dir);
    let (param, gone, remain, per_commit_runs): (&str, Vec<String>, usize, usize) = match mode {
        "wr_delete_1000_scalar_big" => ("p", names(10_000..11_000), 99_000, 1),
        "wr_delete_1000_long_big" => ("bg24", names(0..1_000), 9_000 * 24, 1),
        "wr_delete_500_array_big" => ("arr24", names(0..500), 1_500 * 24, 1),
        _ => ("p", (0..3).map(|r| sname(50_000 + r)).collect(), 99_997, 3),
    };
    let before = head(&db);
    let ms = if per_commit_runs == 1 {
        api_commit(&db, vec![], del_keys(param, gone.iter().cloned()))
    } else {
        let mut tot = 0.0;
        for e in &gone {
            tot += api_commit(&db, vec![], del_keys(param, std::iter::once(e.clone())));
        }
        tot / per_commit_runs as f64
    };
    check_commits(&db, before, per_commit_runs as i64);
    let (count_sql, all_sql) = if param == "p" {
        (
            format!(
                "SELECT count(*) FROM read_parameter_scalar('node', 'p') WHERE entity IN ({})",
                in_list(&gone)
            ),
            "SELECT entity, v_float FROM read_parameter_scalar('node', 'p')".to_string(),
        )
    } else {
        (
            format!(
                "SELECT count(*) FROM read_parameter('node', '{param}') WHERE entity IN ({})",
                in_list(&gone)
            ),
            format!(
                "SELECT entity, idx1_num, v_float, v_int FROM read_parameter('node', '{param}')"
            ),
        )
    };
    check_rows(
        "deleted keys still current",
        scalar_i64(&db, &count_sql) as usize,
        0,
    );
    readback(Sample::new(ms), &db, &all_sql, remain)
}

/// Dispatch a gate mode of this module; `None` if `mode` is not one.
pub fn run(mode: &str, dir: &Path) -> Option<Sample> {
    Some(match mode {
        "wr_scalar_100_new_100k" | "wr_scalar_1_replace_100k" => scalar_writes(dir, mode),
        "wr_20x1_scalars_100k" => {
            let db = copy_base(dir);
            let before = head(&db);
            let mut tot = 0.0;
            for run in 0..3 {
                let segs = (0..20)
                    .map(|i| q_segment(i, "s000000", (run * 100 + i) as f64 + 0.5))
                    .collect();
                tot += api_commit(&db, segs, vec![]);
            }
            check_commits(&db, before, 3);
            readback(Sample::new(tot / 3.0), &db, &q_readback(), 20)
        }
        "wr_long_1000x24_new" | "wr_array_1000x24_new" => {
            let db = fresh(dir);
            let (param, int) = if mode == "wr_long_1000x24_new" {
                ("l24", true)
            } else {
                ("a24", false)
            };
            let before = head(&db);
            let ms = api_commit(
                &db,
                vec![series_seg(param, &names(0..1_000), 24, int, 0)],
                vec![],
            );
            check_commits(&db, before, 1);
            readback(
                Sample::new(ms),
                &db,
                &param_readback("node", param, None),
                24_000,
            )
        }
        "wr_long_10x8760_replace" => {
            let db = fresh(dir);
            let ns = names(0..10);
            api_commit(&db, vec![series_seg("l8760", &ns, 8760, true, 0)], vec![]);
            let before = head(&db);
            let ms = api_commit(&db, vec![series_seg("l8760", &ns, 8760, true, 5)], vec![]);
            check_commits(&db, before, 1);
            readback(
                Sample::new(ms),
                &db,
                &param_readback("node", "l8760", None),
                87_600,
            )
        }
        "wr_array_1000x24_replace_big" => {
            let db = copy_big(dir);
            let ns = names(0..1_000);
            let before = head(&db);
            let ms = api_commit(&db, vec![series_seg("arr24", &ns, 24, false, 7)], vec![]);
            check_commits(&db, before, 1);
            readback(
                Sample::new(ms),
                &db,
                &param_readback("node", "arr24", Some(&ns)),
                24_000,
            )
        }
        "wr_delete_1000_scalar_big"
        | "wr_delete_1000_long_big"
        | "wr_delete_500_array_big"
        | "wr_1_delete_big" => deletes(dir, mode),
        "wr_unchanged_scalar_10k" => {
            // Needs a core that skips unchanged writes (capability
            // skip_unchanged): the rewrite makes no commit and no row.
            let db = fresh(dir);
            let ns = names(0..10_000);
            let vals: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.5).collect();
            api_scalars(&db, "p", &ns, &vals);
            let before = head(&db);
            let hist = scalar_i64(&db, "SELECT count(*) FROM value_scalar");
            let ms = api_scalars(&db, "p", &ns, &vals);
            check_commits(&db, before, 0);
            check_rows(
                "history rows after an unchanged rewrite",
                scalar_i64(&db, "SELECT count(*) FROM value_scalar") as usize,
                hist as usize,
            );
            readback(Sample::new(ms), &db, &scalar_readback("p", &ns), 10_000)
        }
        "wr_longbig_1cell" => crate::longbig::gate_1cell(dir),
        "wr_editor_save" => crate::save_path::gate_editor_save(dir),
        "rd_editor_reload" => crate::save_path::gate_editor_reload(dir),
        "lk_ensure_entity_100k" => {
            let db = copy_base(dir);
            let before = head(&db);
            let entities = scalar_i64(&db, "SELECT count(*) FROM entity");
            let want: Vec<String> = (0..20).map(|i| sname(40_000 + i * 7)).collect();
            let t = Instant::now();
            let ids: Vec<i64> = want
                .iter()
                .map(|n| {
                    nodal_db_core::verbs::ensure_entity(&db, "node", Some(n), &[], "ab").unwrap()
                })
                .collect();
            let ms = elapsed_ms(t) / 20.0;
            check_commits(&db, before, 0);
            check_rows(
                "entities after ensuring existing ones",
                scalar_i64(&db, "SELECT count(*) FROM entity") as usize,
                entities as usize,
            );
            let ids_sql = ids
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let h = read_hash(
                &db,
                &format!("SELECT name FROM entity WHERE id IN ({ids_sql})"),
            );
            let expect = read_hash(
                &db,
                &format!("SELECT name FROM entity WHERE name IN ({})", in_list(&want)),
            );
            check_rows("ensured ids", h.rows as usize, 20);
            check(h == expect, "ensure_entity returned ids of other entities");
            Sample::new(ms).hash("names", &h)
        }
        "lk_core_sql_100k" => lookup_core_sql(dir),
        "imp_ipc_dense" => crate::import::import_dense(dir),
        _ => {
            return crate::synth::run(mode, dir)
                .or_else(|| crate::axes::run(mode))
                .or_else(|| crate::maint::run(mode, dir))
        }
    })
}

#[cfg(feature = "lookup_const")]
fn lookup_core_sql(dir: &Path) -> Sample {
    use nodal_db_core::conn::with_read;
    use nodal_db_core::duckdb;
    let db = copy_base(dir);
    let (ms, ids) = with_read(&db, |conn| {
        let cid: i64 = conn.query_row(
            "SELECT id FROM entity_class WHERE cur_name = 'node'",
            [],
            |r| r.get(0),
        )?;
        let mut stmt = conn.prepare(nodal_db_core::api::ENTITY_NAME_LOOKUP_SQL)?;
        for i in 0..20 {
            let name = format!("S{i:06}");
            let _: i64 = stmt.query_row(duckdb::params![cid, name], |r| r.get(0))?;
        }
        let mut ids = Vec::with_capacity(200);
        let t = Instant::now();
        for i in 0..200 {
            let name = format!("S{:06}", 30_000 + i * 13);
            let id: i64 = stmt.query_row(duckdb::params![cid, name], |r| r.get(0))?;
            ids.push(id);
        }
        Ok((elapsed_ms(t) / 200.0, ids))
    })
    .unwrap_or_else(|e| crate::check::query_failed(&format!("lookup: {e}")));
    let ids_sql = ids
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let want: Vec<String> = (0..200).map(|i| sname(30_000 + i * 13)).collect();
    let h = read_hash(
        &db,
        &format!("SELECT name FROM entity WHERE id IN ({ids_sql})"),
    );
    let expect = read_hash(
        &db,
        &format!("SELECT name FROM entity WHERE name IN ({})", in_list(&want)),
    );
    check_rows("looked-up ids", h.rows as usize, 200);
    check(h == expect, "the lookup returned ids of other entities");
    Sample::new(ms).hash("names", &h)
}

#[cfg(not(feature = "lookup_const"))]
fn lookup_core_sql(_dir: &Path) -> Sample {
    panic!("lk_core_sql_100k needs the lookup_const capability (feature)");
}
