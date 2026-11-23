//! Purge: a bulk, versioned close-out of every item in a named scope, in
//! one commit. It never erases history
//! (as-of reads see everything, no erasure marker) and never shrinks the
//! file.
//!
//! ## Execution: set-based, one dependency definition
//! A purge runs the object delete's leg registry
//! (`object_delete::object_legs` and its cascade edges) in **set
//! form**: every leg's key (`object_delete::LegKey`) is rendered against a
//! txn-local id table instead of one id, so the close-out of a whole scope
//! is O(legs + depth) statements, independent of the number of objects. The
//! per-id and set forms come from one template, and share one SET list and
//! one same-commit guard (`valid_from_commit < c AND valid_to_commit IS
//! NULL`): a row this call wrote is never closed.
//!
//! ## Two sets per kind
//! For each object kind a request reaches, two txn-local sets:
//!   - the **dependent set**: every live object in the request's scope,
//!     spared ones included, expanded by the cascade edges (containers of
//!     entities, a class's children and its dependant classes) by an
//!     iterative frontier query;
//!   - the **close set**: the dependent set minus the call's survivors.
//!
//! The close set drives object rows and identity legs; the dependent set
//! drives dependent legs, each with its own survivor anti-join where its
//! table has a survivor grain (`object_delete::Grain`). Survivors
//! are what the call itself re-states, spared upward to every object a
//! survivor's identity references (an entity's class and elements,
//! transitively; a parameter's class and bound list; a class's dimension
//! classes). The frontier of the close set must never reach a survivor:
//! that is a core bug, reported as [`DbError::Internal`].
//!
//! Every set is a `TEMP` table of the write connection, created inside the
//! write transaction (so a conflict retry rebuilds it) and dropped when the
//! purge phase ends; the connection is per-operation anyway. Temp tables
//! rather than bound `BIGINT[]` lists (measured in
//! `scripts/bench/RESULTS_purge_spike.md`): the sets are computed in SQL from
//! the scope, so no id list ever passes through Rust.

use std::collections::HashSet;

use duckdb::{params, Connection};

use crate::api::{find_alternative, find_entity_class, find_parameter, find_value_list};
use crate::error::{DbError, TemplateStampedPurgeDetails, TemplateStampedPurgeItem};
use crate::history::{ARRAY_PREIMAGE, ARRAY_PREIMAGE_STUB, CLOSE_SET, CLOSE_SET_STUB};
use crate::object_delete::{
    cascade_edges, close_current_rows_set, close_leg_set, count_leg_set, object_legs, DepLeg,
    Grain, IdSet, SAMPLE_LIMIT,
};
use crate::property;
use crate::structural::{CallRenames, CallTouches};
use crate::template::reconcile::{close_value_lists_set, read_pd_row, revise_pd_row};
use crate::text::{NameFolds, Normalized};
use crate::types::{
    Blocker, CascadeCounts, ObjectKind, PurgeOutcome, PurgePreview, PurgeRequest, PurgeScope,
    TableCount, Warning, WarningKind, WriteReport,
};

// ---------------------------------------------------------------------------
// Requests: names (pure checks, NFC) and resolution
// ---------------------------------------------------------------------------

/// The names of one request in NFC (the text rule, engine rule 10); a
/// purge only looks names up, so they are normalized silently.
pub(crate) fn nfc_request(r: &mut PurgeRequest, n: &mut Normalized) {
    let opt = |o: &mut Option<String>, n: &mut Normalized| {
        if let Some(s) = o {
            n.nfc_in_place(s);
        }
    };
    match &mut r.scope {
        PurgeScope::Values {
            class,
            parameter,
            alternative,
        } => {
            opt(class, n);
            opt(parameter, n);
            opt(alternative, n);
        }
        PurgeScope::Entities { class } | PurgeScope::Parameters { class } => opt(class, n),
        PurgeScope::Activations { class, alternative } => {
            opt(class, n);
            opt(alternative, n);
        }
        PurgeScope::Memberships { group_class } => opt(group_class, n),
        PurgeScope::Alternatives { except }
        | PurgeScope::Classes { except }
        | PurgeScope::ValueLists { except } => except.iter_mut().for_each(|s| n.nfc_in_place(s)),
        PurgeScope::Scenarios => {}
    }
}

/// Pure checks of the requests (no DB): no blank name, and
/// `Values.parameter` only with its `class`.
pub(crate) fn validate_requests(reqs: &[PurgeRequest]) -> Result<(), DbError> {
    for (i, r) in reqs.iter().enumerate() {
        let blank = |what: &str, s: &str| {
            if s.trim().is_empty() {
                Err(DbError::Api(format!(
                    "purge request {i}: {what} name must be non-empty"
                )))
            } else {
                Ok(())
            }
        };
        let opt = |what: &str, o: &Option<String>| match o {
            Some(s) => blank(what, s),
            None => Ok(()),
        };
        match &r.scope {
            PurgeScope::Values {
                class,
                parameter,
                alternative,
            } => {
                opt("class", class)?;
                opt("parameter", parameter)?;
                opt("alternative", alternative)?;
                if parameter.is_some() && class.is_none() {
                    return Err(DbError::Api(format!(
                        "purge request {i}: a values scope names a parameter only with its class"
                    )));
                }
            }
            PurgeScope::Entities { class } | PurgeScope::Parameters { class } => {
                opt("class", class)?
            }
            PurgeScope::Activations { class, alternative } => {
                opt("class", class)?;
                opt("alternative", alternative)?;
            }
            PurgeScope::Memberships { group_class } => opt("group class", group_class)?,
            PurgeScope::Alternatives { except } => {
                for s in except {
                    blank("alternative", s)?;
                }
            }
            PurgeScope::Classes { except } => {
                for s in except {
                    blank("class", s)?;
                }
            }
            PurgeScope::ValueLists { except } => {
                for s in except {
                    blank("value list", s)?;
                }
            }
            PurgeScope::Scenarios => {}
        }
    }
    Ok(())
}

/// One request's scope with its names resolved to ids.
#[derive(Clone, Debug)]
pub(crate) enum Scope {
    Values {
        class: Option<i64>,
        parameter: Option<i64>,
        alternative: Option<i64>,
    },
    Entities {
        class: Option<i64>,
    },
    Activations {
        class: Option<i64>,
        alternative: Option<i64>,
    },
    Memberships {
        group_class: Option<i64>,
    },
    Alternatives {
        except: Vec<i64>,
    },
    Scenarios,
    Classes {
        except: Vec<i64>,
    },
    Parameters {
        class: Option<i64>,
    },
    ValueLists {
        except: Vec<i64>,
    },
}

impl Scope {
    /// Vocabulary scopes close classes, parameters or lists (template
    /// protection, §3.7).
    pub(crate) fn is_vocabulary(&self) -> bool {
        matches!(
            self,
            Scope::Classes { .. } | Scope::Parameters { .. } | Scope::ValueLists { .. }
        )
    }
}

/// Resolve every request's names, lookup-only and by the name fold, after
/// the call's structural changes: an unknown name is refused, and
/// so is a name a rename of this call freed (OD5 extended to purge names:
/// a replace would silently become an append). Every resolved spelling is
/// noted in `folds` (`NameFolded`).
pub(crate) fn resolve_requests(
    conn: &Connection,
    reqs: &[PurgeRequest],
    calls: &CallRenames,
    folds: &mut NameFolds,
) -> Result<Vec<Scope>, DbError> {
    reqs.iter()
        .enumerate()
        .map(|(i, r)| resolve_one(conn, i, &r.scope, calls, folds))
        .collect()
}

fn missing(i: usize, kind: &str, name: &str, renamed: Option<String>) -> DbError {
    match renamed {
        Some(to) => DbError::Api(format!(
            "purge request {i}: {kind} {name:?} was renamed to {to:?} earlier in this call; \
             address it by its new name"
        )),
        None => DbError::Api(format!("purge request {i}: unknown {kind} {name:?}")),
    }
}

fn resolve_one(
    conn: &Connection,
    i: usize,
    scope: &PurgeScope,
    calls: &CallRenames,
    folds: &mut NameFolds,
) -> Result<Scope, DbError> {
    let mut class = |name: &str, folds: &mut NameFolds| -> Result<i64, DbError> {
        match find_entity_class(conn, name)? {
            Some(f) => Ok(f.noted(folds, "entity_class", name)),
            None => Err(missing(i, "class", name, calls.renamed_class(conn, name)?)),
        }
    };
    let alternative = |name: &str, folds: &mut NameFolds| -> Result<i64, DbError> {
        match find_alternative(conn, name)? {
            Some(f) => Ok(f.noted(folds, "alternative", name)),
            None => Err(missing(
                i,
                "alternative",
                name,
                calls.renamed_alternative(conn, name)?,
            )),
        }
    };
    let opt_class = |o: &Option<String>,
                     folds: &mut NameFolds,
                     class: &mut dyn FnMut(&str, &mut NameFolds) -> Result<i64, DbError>|
     -> Result<Option<i64>, DbError> {
        o.as_deref().map(|n| class(n, folds)).transpose()
    };
    Ok(match scope {
        PurgeScope::Values {
            class: c,
            parameter,
            alternative: a,
        } => {
            let cid = opt_class(c, folds, &mut class)?;
            let pid = match (parameter, cid, c) {
                (Some(p), Some(cid), Some(cn)) => Some(match find_parameter(conn, cid, p)? {
                    Some(f) => f.noted(folds, "parameter_definition", p),
                    None => {
                        return Err(missing(
                            i,
                            "parameter",
                            &format!("{cn}.{p}"),
                            calls.renamed_parameter(conn, cn, p)?,
                        ))
                    }
                }),
                _ => None,
            };
            let aid = a.as_deref().map(|n| alternative(n, folds)).transpose()?;
            Scope::Values {
                class: cid,
                parameter: pid,
                alternative: aid,
            }
        }
        PurgeScope::Entities { class: c } => Scope::Entities {
            class: opt_class(c, folds, &mut class)?,
        },
        PurgeScope::Activations {
            class: c,
            alternative: a,
        } => Scope::Activations {
            class: opt_class(c, folds, &mut class)?,
            alternative: a.as_deref().map(|n| alternative(n, folds)).transpose()?,
        },
        PurgeScope::Memberships { group_class } => Scope::Memberships {
            group_class: opt_class(group_class, folds, &mut class)?,
        },
        PurgeScope::Alternatives { except } => Scope::Alternatives {
            except: except
                .iter()
                .map(|n| alternative(n, folds))
                .collect::<Result<_, _>>()?,
        },
        PurgeScope::Scenarios => Scope::Scenarios,
        PurgeScope::Classes { except } => Scope::Classes {
            except: except
                .iter()
                .map(|n| class(n, folds))
                .collect::<Result<_, _>>()?,
        },
        PurgeScope::Parameters { class: c } => Scope::Parameters {
            class: opt_class(c, folds, &mut class)?,
        },
        PurgeScope::ValueLists { except } => Scope::ValueLists {
            except: except
                .iter()
                .map(|n| match find_value_list(conn, n)? {
                    Some(f) => Ok(f.noted(folds, "parameter_value_list", n)),
                    None => Err(missing(i, "value list", n, None)),
                })
                .collect::<Result<_, DbError>>()?,
        },
    })
}

/// `id NOT IN (…)` for an except list (empty: `TRUE`). Ids are integers
/// the core resolved, so they are rendered inline.
fn not_in(column: &str, ids: &[i64]) -> String {
    if ids.is_empty() {
        "TRUE".into()
    } else {
        let list: Vec<String> = ids.iter().map(i64::to_string).collect();
        format!("{column} NOT IN ({})", list.join(", "))
    }
}

// ---------------------------------------------------------------------------
// The purge phase
// ---------------------------------------------------------------------------

/// What the purge phase reads of the rest of the call.
pub(crate) struct PurgeCall<'a> {
    pub(crate) requests: &'a [PurgeRequest],
    pub(crate) scopes: &'a [Scope],
    pub(crate) touches: &'a CallTouches,
    /// The value keys the call buffered (written or unchanged).
    pub(crate) written: &'a HashSet<(i64, i64, i64)>,
    pub(crate) retain_override: Option<bool>,
}

/// Run the call's purges, last in the call, under its commit `c`,
/// appending one [`PurgeOutcome`] per request to `report.purges`. One
/// refusal rolls the whole call back; the order of refusals is
/// name errors (already raised by [`resolve_requests`]), then the
/// template stamp, then reach outside a scope.
pub(crate) fn run(
    conn: &Connection,
    c: i64,
    call: &PurgeCall<'_>,
    report: &mut WriteReport,
) -> Result<bool, DbError> {
    if call.requests.is_empty() {
        return Ok(false);
    }
    fill_survivors(conn, call)?;
    fill_retention(conn, call.retain_override)?;
    for (r, scope) in call.scopes.iter().enumerate() {
        build_request_sets(conn, r, scope)?;
    }
    build_all_close_parameters(conn, call.requests.len())?;
    for (r, scope) in call.scopes.iter().enumerate() {
        report
            .warnings
            .extend(large_warning(r, scope_cells(conn, r, scope)?));
    }
    refuse_template_stamped(conn, call)?;
    refuse_outside_scope(conn, call)?;
    let mut vocabulary = false;
    for (r, scope) in call.scopes.iter().enumerate() {
        let (outcome, closed_vocabulary) = execute(conn, c, r, scope)?;
        vocabulary |= closed_vocabulary || outcome.unbound > 0;
        report.purges.push(outcome);
        if test_hooks::take_conflict() {
            // A DuckDB error carrying the `Conflict` substring the retry
            // loop keys on, after the first request closed its rows.
            conn.execute_batch("SELECT error('Conflict (test-injected, mid-purge)')")?;
        }
    }
    drop_tables(conn)?;
    Ok(vocabulary)
}

/// The memory guard's threshold: a purge request
/// whose scope holds more long-tier cells plus array elements than this
/// gets a [`WarningKind::PurgeLarge`] warning, in the preview and the
/// commit; it is not refused. Half the largest point measured
/// (105 M long cells: about ten minutes and 3.4 GB at the peak, within a
/// 6.4 GB DuckDB limit; `scripts/bench/RESULTS_purge_spike.md`).
pub const PURGE_LARGE_CELLS: u64 = 50_000_000;

/// Long-tier cells plus array elements request `r`'s value steps cover
/// (before survivors: an upper bound), counted once per table.
fn scope_cells(conn: &Connection, r: usize, scope: &Scope) -> Result<u64, DbError> {
    let mut long: Vec<String> = Vec::new();
    let mut array: Vec<String> = Vec::new();
    for step in steps(conn, r, scope)? {
        if let Step::Values { table, pred, .. } = step {
            match table {
                "value_long" => long.push(format!("({pred})")),
                "value_array" => array.push(format!("({pred})")),
                _ => {}
            }
        }
    }
    let mut cells = 0i64;
    if !long.is_empty() {
        cells += query_count(
            conn,
            &format!(
                "SELECT count(*) FROM value_long WHERE ({}) AND valid_to_commit IS NULL",
                long.join(" OR ")
            ),
        )?;
    }
    if !array.is_empty() {
        // Axis lengths on narrow columns, never the payload (engine rule 2).
        cells += query_count(
            conn,
            &format!(
                "SELECT coalesce(sum(a1.length::BIGINT * coalesce(a2.length, 1) \
                   * coalesce(a3.length, 1) * coalesce(a4.length, 1)), 0)::BIGINT \
                 FROM (SELECT axis1_id, axis2_id, axis3_id, axis4_id FROM value_array \
                       WHERE {}) v \
                 JOIN axis a1 ON a1.id = v.axis1_id \
                 LEFT JOIN axis a2 ON a2.id = v.axis2_id \
                 LEFT JOIN axis a3 ON a3.id = v.axis3_id \
                 LEFT JOIN axis a4 ON a4.id = v.axis4_id",
                array.join(" OR ")
            ),
        )?;
    }
    Ok(cells.max(0) as u64)
}

/// The [`WarningKind::PurgeLarge`] warning of request `r`, if its scope
/// passes the threshold.
fn large_warning(r: usize, cells: u64) -> Option<Warning> {
    let threshold = test_hooks::large_threshold();
    (cells > threshold).then(|| Warning {
        kind: WarningKind::PurgeLarge {
            request_index: r,
            cells,
            threshold,
        },
        object_type: "purge".into(),
        name: String::new(),
        id: 0,
    })
}

/// Did commit `c` close a class, parameter definition or value list that
/// existed before it (its id has no live row left)? Rows the commit itself
/// created are never closed (the same-commit guard), and a revision keeps
/// its id live, so this is exactly "pre-call vocabulary removed".
pub(crate) fn closed_vocabulary(conn: &Connection, c: i64) -> Result<bool, DbError> {
    let n = query_count(
        conn,
        &format!(
            "SELECT count(*) FROM ( \
               SELECT id FROM entity_class WHERE valid_to_commit = {c} \
                 AND id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL) \
               UNION ALL SELECT id FROM parameter_definition WHERE valid_to_commit = {c} \
                 AND id NOT IN (SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL) \
               UNION ALL SELECT id FROM parameter_value_list WHERE valid_to_commit = {c} \
                 AND id NOT IN (SELECT id FROM parameter_value_list WHERE valid_to_commit IS NULL))"
        ),
    )?;
    Ok(n > 0)
}

/// Every request's parameter close set, together: a value list's binding
/// by a definition another request closes is not reach outside the call's
/// scopes, and is not unbound (the definition closes).
const ALL_CLOSE_PARAMETER: &str = "pg_all_close_parameter";

fn build_all_close_parameters(conn: &Connection, n: usize) -> Result<(), DbError> {
    let union = (0..n)
        .map(|r| format!("SELECT id FROM {}", close_table(r, SetKind::Parameter)))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    exec_batch(
        conn,
        &format!("CREATE OR REPLACE TEMP TABLE {ALL_CLOSE_PARAMETER} AS {union}"),
    )?;
    Ok(())
}

/// The live definitions outside every request's close set that bind a list
/// of request `r`'s close set: reach outside the scope, removed with
/// `allow_outside_scope`.
fn bindings_sql(r: usize) -> String {
    format!(
        "SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL \
           AND parameter_value_list_id IN (SELECT id FROM {}) \
           AND id NOT IN (SELECT id FROM {ALL_CLOSE_PARAMETER})",
        close_table(r, SetKind::ValueList)
    )
}

/// The template-stamp check: on a database stamped with a
/// template, a vocabulary request that would close a class, parameter or
/// value list that existed before the call needs `allow_on_template`; every
/// refused request is listed in one typed error. Objects the call created
/// are survivors, never in a close set, so a request that closes only those
/// passes.
fn refuse_template_stamped(conn: &Connection, call: &PurgeCall<'_>) -> Result<(), DbError> {
    let mut items = Vec::new();
    for (r, (req, scope)) in call.requests.iter().zip(call.scopes).enumerate() {
        if req.allow_on_template || !scope.is_vocabulary() {
            continue;
        }
        let item = stamped_item(conn, r)?;
        if item.classes + item.parameters + item.value_lists > 0 {
            items.push(item);
        }
    }
    if items.is_empty() {
        return Ok(());
    }
    let Some(template_name) = property::read(conn, "template_name")? else {
        return Ok(());
    };
    let template_version = property::read(conn, "template_version")?;
    let lines: Vec<String> = items
        .iter()
        .map(|i| {
            format!(
                "request {}: {} class(es), {} parameter(s), {} value list(s) ({})",
                i.request_index,
                i.classes,
                i.parameters,
                i.value_lists,
                i.sample.join(", ")
            )
        })
        .collect();
    let message = format!(
        "the database is stamped with template {template_name:?}{}: purging its classes, \
         parameters or value lists needs allow_on_template on each request. The core cannot \
         tell which ones the template declares, and a purged declared item blocks the next \
         apply of a changed template until it is re-created with recreate_removed. Refused \
         (nothing was written): {}",
        template_version
            .as_deref()
            .map(|v| format!(" (version {v:?})"))
            .unwrap_or_default(),
        lines.join("; ")
    );
    Err(DbError::TemplateStampedPurge(Box::new(
        TemplateStampedPurgeDetails {
            template_name,
            template_version,
            requests: items,
            message,
        },
    )))
}

/// What request `r` would close of the pre-call vocabulary (its close
/// sets), with a bounded sample of names.
pub(crate) fn stamped_item(
    conn: &Connection,
    r: usize,
) -> Result<TemplateStampedPurgeItem, DbError> {
    let count = |k: SetKind| -> Result<usize, DbError> {
        Ok(query_count(
            conn,
            &format!(
                "SELECT count(*) FROM {} WHERE valid_to_commit IS NULL \
                 AND id IN (SELECT id FROM {})",
                k.object_table(),
                close_table(r, k)
            ),
        )? as usize)
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT n FROM ( \
           SELECT 0 AS o, name AS n FROM entity_class WHERE valid_to_commit IS NULL \
             AND id IN (SELECT id FROM {cls}) \
           UNION ALL SELECT 1, ec.name || '.' || p.name FROM parameter_definition p \
             JOIN entity_class ec ON ec.id = p.entity_class_id AND ec.valid_to_commit IS NULL \
             WHERE p.valid_to_commit IS NULL AND p.id IN (SELECT id FROM {par}) \
           UNION ALL SELECT 2, name FROM parameter_value_list WHERE valid_to_commit IS NULL \
             AND id IN (SELECT id FROM {lst})) \
         ORDER BY o, n LIMIT {SAMPLE_LIMIT}",
        cls = close_table(r, SetKind::Class),
        par = close_table(r, SetKind::Parameter),
        lst = close_table(r, SetKind::ValueList),
    ))?;
    let sample = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<_, _>>()?;
    Ok(TemplateStampedPurgeItem {
        request_index: r,
        classes: count(SetKind::Class)?,
        parameters: count(SetKind::Parameter)?,
        value_lists: count(SetKind::ValueList)?,
        sample,
    })
}

/// The survivor sets: what the call names or creates
/// ([`CallTouches`]), the value keys it buffered, spared upward.
fn fill_survivors(conn: &Connection, call: &PurgeCall<'_>) -> Result<(), DbError> {
    create_survivor_tables(conn)?;
    if test_hooks::survivors_disabled() {
        return Ok(());
    }
    let t = call.touches;
    let ids = |s: &HashSet<i64>| -> Vec<i64> { s.iter().copied().collect() };
    insert_ids(conn, &surv_table(SetKind::Entity), &ids(&t.entities))?;
    insert_ids(conn, &surv_table(SetKind::Class), &ids(&t.classes))?;
    insert_ids(conn, &surv_table(SetKind::Parameter), &ids(&t.parameters))?;
    insert_ids(
        conn,
        &surv_table(SetKind::Alternative),
        &ids(&t.alternatives),
    )?;
    insert_ids(conn, &surv_table(SetKind::Scenario), &ids(&t.scenarios))?;
    insert_ids(conn, SURV_RANKING, &ids(&t.rankings))?;
    let list = |v: Vec<i64>| duckdb::types::Value::List(v.into_iter().map(Into::into).collect());
    if !call.written.is_empty() {
        let (mut e, mut p, mut a) = (Vec::new(), Vec::new(), Vec::new());
        for &(x, y, z) in call.written {
            e.push(x);
            p.push(y);
            a.push(z);
        }
        exec(
            conn,
            &format!(
                "INSERT INTO {SURV_VALUE} SELECT unnest(?::BIGINT[]), unnest(?::BIGINT[]), \
                 unnest(?::BIGINT[])"
            ),
            [list(e), list(p), list(a)],
        )?;
    }
    if !t.activations.is_empty() {
        let (e, a): (Vec<i64>, Vec<i64>) = t.activations.iter().copied().unzip();
        exec(
            conn,
            &format!(
                "INSERT INTO {SURV_ACTIVATION} SELECT unnest(?::BIGINT[]), unnest(?::BIGINT[])"
            ),
            [list(e), list(a)],
        )?;
    }
    if !t.memberships.is_empty() {
        let (mut g, mut m, mut a) = (Vec::new(), Vec::new(), Vec::new());
        for &(x, y, z) in &t.memberships {
            g.push(x);
            m.push(y);
            a.push(z);
        }
        exec(
            conn,
            &format!(
                "INSERT INTO {SURV_MEMBERSHIP} SELECT unnest(?::BIGINT[]), unnest(?::BIGINT[]), \
                 unnest(?::BIGINT[])"
            ),
            [list(g), list(m), list(a)],
        )?;
    }
    spare_upward(conn)
}

/// The retention partition `pg_retention(pd_id, retained)`:
/// every definition's `retain_override.unwrap_or(retain_content)`, read
/// before any close (from each definition's latest row, so a definition
/// the same purge closes keeps its retention).
fn fill_retention(conn: &Connection, retain_override: Option<bool>) -> Result<(), DbError> {
    let over = match retain_override {
        Some(true) => "TRUE",
        Some(false) => "FALSE",
        None => "NULL",
    };
    exec_batch(
        conn,
        &format!(
            "CREATE OR REPLACE TEMP TABLE pg_retention AS \
             SELECT id AS pd_id, coalesce({over}, arg_max(retain_content, version)) AS retained \
             FROM parameter_definition GROUP BY id"
        ),
    )?;
    Ok(())
}

/// Request `r`'s scope table of kind `k`: what it has in scope before its
/// cascade grows it.
fn scope_table(r: usize, k: SetKind) -> String {
    format!("pg{r}_scope_{}", k.tag())
}

/// The live rows of `table` (`id` column) matching `filter`.
fn live(table: &str, filter: &str) -> String {
    format!("SELECT id FROM {table} WHERE valid_to_commit IS NULL AND {filter}")
}

/// Build request `r`'s scope, dependent and close sets, and run the
/// frontier hard check. Data scopes that close rows, never objects
/// (`Values`, `Activations`, `Memberships`), get filter sets only.
fn build_request_sets(conn: &Connection, r: usize, scope: &Scope) -> Result<(), DbError> {
    let (base, expand): (Vec<(SetKind, String)>, bool) = match scope {
        Scope::Values {
            class,
            parameter,
            alternative,
        } => {
            let mut b = Vec::new();
            match (parameter, class) {
                (Some(p), _) => b.push((SetKind::Parameter, format!("SELECT {p}"))),
                (None, Some(c)) => b.push((
                    SetKind::Parameter,
                    live("parameter_definition", &format!("entity_class_id = {c}")),
                )),
                (None, None) => {}
            }
            if let Some(a) = alternative {
                b.push((SetKind::Alternative, format!("SELECT {a}")));
            }
            (b, false)
        }
        Scope::Activations { class, alternative } => {
            let mut b = Vec::new();
            if let Some(c) = class {
                b.push((
                    SetKind::Entity,
                    live("entity", &format!("entity_class_id = {c}")),
                ));
            }
            if let Some(a) = alternative {
                b.push((SetKind::Alternative, format!("SELECT {a}")));
            }
            (b, false)
        }
        Scope::Memberships { group_class } => (
            group_class
                .iter()
                .map(|c| {
                    (
                        SetKind::Entity,
                        live("entity", &format!("entity_class_id = {c}")),
                    )
                })
                .collect(),
            false,
        ),
        Scope::Entities { class } => (
            vec![(
                SetKind::Entity,
                live(
                    "entity",
                    &class
                        .map(|c| format!("entity_class_id = {c}"))
                        .unwrap_or_else(|| "TRUE".into()),
                ),
            )],
            true,
        ),
        Scope::Alternatives { except } => (
            vec![(
                SetKind::Alternative,
                live("alternative", &not_in("id", except)),
            )],
            false,
        ),
        Scope::Scenarios => (vec![(SetKind::Scenario, live("scenario", "TRUE"))], false),
        Scope::Classes { except } => (
            vec![(SetKind::Class, live("entity_class", &not_in("id", except)))],
            true,
        ),
        Scope::Parameters { class } => (
            vec![(
                SetKind::Parameter,
                live(
                    "parameter_definition",
                    &class
                        .map(|c| format!("entity_class_id = {c}"))
                        .unwrap_or_else(|| "TRUE".into()),
                ),
            )],
            false,
        ),
        Scope::ValueLists { except } => (
            vec![(
                SetKind::ValueList,
                live("parameter_value_list", &not_in("id", except)),
            )],
            false,
        ),
    };
    // The scope sets: the base, plus a class's own entities and parameters
    // (the class cascade is the scope, §3.7).
    for k in SetKind::ALL {
        create_id_table(conn, &scope_table(r, k))?;
    }
    for (k, select) in &base {
        exec(
            conn,
            &format!("INSERT INTO {} {select}", scope_table(r, *k)),
            [],
        )?;
    }
    if matches!(scope, Scope::Classes { .. }) {
        let classes = scope_table(r, SetKind::Class);
        exec_batch(
            conn,
            &format!(
                "INSERT INTO {} {}; INSERT INTO {} {}",
                scope_table(r, SetKind::Entity),
                live(
                    "entity",
                    &format!("entity_class_id IN (SELECT id FROM {classes})")
                ),
                scope_table(r, SetKind::Parameter),
                live(
                    "parameter_definition",
                    &format!("entity_class_id IN (SELECT id FROM {classes})")
                ),
            ),
        )?;
    }
    build_dep_sets(conn, r, &base, expand)?;
    build_close_sets(conn, r)?;
    check_frontier(conn, r)
}

/// Refuse the call when a request's close set holds an object no request
/// of the call has in scope, unless that request sets
/// `allow_outside_scope`: every reaching request is listed, per kind and
/// class, with a bounded sample.
fn refuse_outside_scope(conn: &Connection, call: &PurgeCall<'_>) -> Result<(), DbError> {
    let mut lines: Vec<String> = Vec::new();
    for (r, req) in call.requests.iter().enumerate() {
        if req.allow_outside_scope {
            continue;
        }
        let reach = outside_scope(conn, r, call.requests.len())?;
        if !reach.is_empty() {
            let items: Vec<String> = reach
                .iter()
                .map(|b| {
                    format!(
                        "{} {} of {} ({}{})",
                        b.count,
                        b.table,
                        b.column,
                        b.sample.join(", "),
                        if b.count > b.sample.len() {
                            ", …"
                        } else {
                            ""
                        }
                    )
                })
                .collect();
            lines.push(format!("request {r}: {}", items.join("; ")));
        }
    }
    if lines.is_empty() {
        return Ok(());
    }
    Err(DbError::Api(format!(
        "purge reaches outside its scope (nothing was written): {}. Its cascade would close \
         these objects, which no request of the call has in scope; purge them too, or set \
         allow_outside_scope on the request",
        lines.join(" | ")
    )))
}

/// Request `r`'s reach outside every request's scope (of `n` requests):
/// one [`Blocker`] per reached kind and class (`table` the object table,
/// `column` its class, or `"dimension"` for classes), with a count and a
/// bounded sample of names.
pub(crate) fn outside_scope(
    conn: &Connection,
    r: usize,
    n: usize,
) -> Result<Vec<Blocker>, DbError> {
    let union = |k: SetKind| -> String {
        (0..n)
            .map(|q| format!("SELECT id FROM {}", scope_table(q, k)))
            .collect::<Vec<_>>()
            .join(" UNION ALL ")
    };
    let mut out = Vec::new();
    for (k, group) in [
        (SetKind::Entity, Some("entity_class_id")),
        (SetKind::Parameter, Some("entity_class_id")),
        (SetKind::Class, None),
    ] {
        let table = k.object_table();
        let outside = format!(
            "SELECT id FROM {} WHERE id NOT IN ({})",
            close_table(r, k),
            union(k)
        );
        let sql = match group {
            Some(col) => format!(
                "SELECT ec.name, count(*), array_to_string(list(o.name ORDER BY o.name)[1:{SAMPLE_LIMIT}], chr(31)) \
                 FROM {table} o JOIN entity_class ec ON ec.id = o.{col} \
                   AND ec.valid_to_commit IS NULL \
                 WHERE o.valid_to_commit IS NULL AND o.id IN ({outside}) \
                 GROUP BY ec.name ORDER BY ec.name"
            ),
            None => format!(
                "SELECT 'dimension', count(*), array_to_string(list(o.name ORDER BY o.name)[1:{SAMPLE_LIMIT}], chr(31)) \
                 FROM {table} o WHERE o.valid_to_commit IS NULL AND o.id IN ({outside}) \
                 HAVING count(*) > 0"
            ),
        };
        test_hooks::count();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(Blocker {
                table: table.to_string(),
                column: row.get(0)?,
                count: row.get::<_, i64>(1)? as usize,
                sample: row
                    .get::<_, String>(2)?
                    .split('\u{1f}')
                    .map(str::to_string)
                    .collect(),
            })
        })?;
        for b in rows {
            out.push(b?);
        }
    }
    // A purged list bound by a definition no request closes (V-OD2).
    let sql = format!(
        "SELECT count(*), array_to_string(list(ec.name || '.' || p.name \
             ORDER BY ec.name, p.name)[1:{SAMPLE_LIMIT}], chr(31)) \
         FROM parameter_definition p JOIN entity_class ec ON ec.id = p.entity_class_id \
           AND ec.valid_to_commit IS NULL \
         WHERE p.valid_to_commit IS NULL AND p.id IN ({}) HAVING count(*) > 0",
        bindings_sql(r)
    );
    test_hooks::count();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(Blocker {
            table: "parameter_definition".into(),
            column: "parameter_value_list_id".into(),
            count: row.get::<_, i64>(0)? as usize,
            sample: row
                .get::<_, String>(1)?
                .split('\u{1f}')
                .map(str::to_string)
                .collect(),
        })
    })?;
    for b in rows {
        out.push(b?);
    }
    Ok(out)
}

/// The survivor anti-join of a leg's table at its grain (`None`: none).
fn survivor_antijoin(leg: &DepLeg) -> Option<String> {
    let t = leg.table;
    match leg.grain {
        Grain::Value | Grain::ValueMeta => Some(value_antijoin(t)),
        Grain::Activation => Some(format!(
            "NOT EXISTS (SELECT 1 FROM {SURV_ACTIVATION} s WHERE s.e = {t}.entity_id \
             AND s.a = {t}.alternative_id)"
        )),
        Grain::Membership => Some(format!(
            "NOT EXISTS (SELECT 1 FROM {SURV_MEMBERSHIP} s WHERE s.g = {t}.group_entity_id \
             AND s.m = {t}.member_entity_id AND s.a = coalesce({t}.alternative_id, 0))"
        )),
        Grain::Ranking => Some(format!(
            "{t}.scenario_id NOT IN (SELECT id FROM {SURV_RANKING})"
        )),
        Grain::Attached | Grain::Plain => None,
    }
}

/// The value-key survivor anti-join of a value-keyed table.
fn value_antijoin(t: &str) -> String {
    format!(
        "NOT EXISTS (SELECT 1 FROM {SURV_VALUE} s WHERE s.e = {t}.entity_id \
         AND s.p = {t}.parameter_definition_id AND s.a = {t}.alternative_id)"
    )
}

/// The registry's dependent leg of `kind` on `table` (keyed by `column`).
fn leg(kind: ObjectKind, table: &str, column: &str) -> DepLeg {
    let legs = object_legs(kind).expect("purge kinds have leg sets");
    *legs
        .dependents
        .iter()
        .find(|l| l.table == table && l.key_column() == column)
        .unwrap_or_else(|| panic!("registry leg {table}.{column} of {kind:?}"))
}

/// Counts of one request, merged per table.
#[derive(Default)]
struct Accum {
    object_rows: usize,
    objects: usize,
    per_table: Vec<TableCount>,
    spared_objects: usize,
    spared: Vec<TableCount>,
    unbound: usize,
    /// A class, parameter definition or value list closed.
    vocabulary: bool,
}

fn add(v: &mut Vec<TableCount>, table: &str, rows: usize) {
    if rows == 0 {
        return;
    }
    match v.iter_mut().find(|t| t.table == table) {
        Some(t) => t.rows += rows,
        None => v.push(TableCount {
            table: table.to_string(),
            rows,
        }),
    }
}

fn counts(object_rows: usize, per_table: Vec<TableCount>) -> CascadeCounts {
    let dep: usize = per_table.iter().map(|t| t.rows).sum();
    CascadeCounts {
        object_rows,
        total_rows: object_rows + dep,
        per_table,
    }
}

/// One close-out statement (group) of a request, in execution order. The
/// commit runs them ([`run_step`]); the preview counts the rows they match
/// ([`preview_one`]): one list of predicates, so the two cannot drift.
enum Step {
    /// A value tier (or `value_metadata_link`, `meta`) over `pred`,
    /// spared by the value-key anti-join, retention per definition.
    Values {
        table: &'static str,
        pred: String,
        meta: bool,
    },
    /// The rows of a data scope (`Activations`, `Memberships`) over `pred`,
    /// spared by `anti`.
    Rows {
        table: &'static str,
        pred: String,
        anti: Option<String>,
    },
    /// A registry leg over a set; `identity` counts in the object rows.
    Leg {
        leg: DepLeg,
        set: String,
        anti: Option<String>,
        identity: bool,
    },
    /// Remove the bindings of the request's lists by definitions no
    /// request closes.
    Unbind,
    /// The request's value lists, members and annotations (the
    /// `close_value_list` door).
    Lists { set: String },
    /// The object rows of a close set.
    Objects { kind: SetKind, set: String },
}

/// Request `r`'s steps: values, then dependents, then identity legs,
/// then object rows (V-S3).
fn steps(conn: &Connection, r: usize, scope: &Scope) -> Result<Vec<Step>, DbError> {
    let mut out = Vec::new();
    let ent = dep_table(r, SetKind::Entity);
    let par = dep_table(r, SetKind::Parameter);
    let alt = dep_table(r, SetKind::Alternative);
    // The AND of registry legs over filter sets (`TRUE` for none).
    let filter = |legs: &[(Option<DepLeg>, &str)]| -> String {
        let parts: Vec<String> = legs
            .iter()
            .filter_map(|(l, set)| l.map(|l| l.key.render(IdSet::Table(set))))
            .collect();
        if parts.is_empty() {
            "TRUE".into()
        } else {
            parts.join(" AND ")
        }
    };
    match scope {
        Scope::Values {
            class,
            parameter,
            alternative,
        } => {
            let by_pd = class.is_some() || parameter.is_some();
            for table in [
                "value_scalar",
                "value_long",
                "value_array",
                "value_metadata_link",
            ] {
                let pred = filter(&[
                    (
                        by_pd.then(|| leg(ObjectKind::Parameter, table, "parameter_definition_id")),
                        &par,
                    ),
                    (
                        alternative.map(|_| leg(ObjectKind::Alternative, table, "alternative_id")),
                        &alt,
                    ),
                ]);
                out.push(Step::Values {
                    table,
                    pred,
                    meta: table == "value_metadata_link",
                });
            }
            return Ok(out);
        }
        Scope::Activations { class, alternative } => {
            let t = "entity_alternative";
            let l = leg(ObjectKind::Entity, t, "entity_id");
            out.push(Step::Rows {
                table: t,
                pred: filter(&[
                    (class.map(|_| l), &ent),
                    (
                        alternative.map(|_| leg(ObjectKind::Alternative, t, "alternative_id")),
                        &alt,
                    ),
                ]),
                anti: survivor_antijoin(&l),
            });
            return Ok(out);
        }
        Scope::Memberships { group_class } => {
            let t = "membership";
            let l = leg(ObjectKind::Entity, t, "group_entity_id");
            out.push(Step::Rows {
                table: t,
                pred: filter(&[(group_class.map(|_| l), &ent)]),
                anti: survivor_antijoin(&l),
            });
            return Ok(out);
        }
        _ => {}
    }
    // Object scopes: every kind the request reaches.
    let mut kinds: Vec<(SetKind, ObjectKind)> = Vec::new();
    for k in SetKind::ALL {
        let Some(kind) = k.object_kind() else {
            continue;
        };
        if has(conn, &dep_table(r, k))? {
            kinds.push((k, kind));
        }
    }
    let has_lists = has(conn, &dep_table(r, SetKind::ValueList))?;
    // Values, over the dependent sets.
    for &(k, kind) in &kinds {
        let legs = object_legs(kind).expect("purge kinds have leg sets");
        let dep = dep_table(r, k);
        for l in legs.dependents.iter().filter(|l| l.grain == Grain::Value) {
            out.push(Step::Values {
                table: l.table,
                pred: l.key.render(IdSet::Table(&dep)),
                meta: false,
            });
        }
    }
    // Dependents, at their grain: descriptors over the close set, the rest
    // over the dependent set with their survivor anti-join.
    for &(k, kind) in &kinds {
        let legs = object_legs(kind).expect("purge kinds have leg sets");
        for l in legs.dependents.iter().filter(|l| l.grain != Grain::Value) {
            let set = if l.grain == Grain::Attached {
                close_table(r, k)
            } else {
                dep_table(r, k)
            };
            out.push(Step::Leg {
                leg: *l,
                set,
                anti: survivor_antijoin(l),
                identity: false,
            });
        }
    }
    if has_lists {
        out.push(Step::Unbind);
    }
    // Identity legs, then object rows, over the close sets.
    for &(k, kind) in &kinds {
        let legs = object_legs(kind).expect("purge kinds have leg sets");
        for l in &legs.identity {
            out.push(Step::Leg {
                leg: *l,
                set: close_table(r, k),
                anti: None,
                identity: true,
            });
        }
    }
    if has_lists {
        out.push(Step::Lists {
            set: close_table(r, SetKind::ValueList),
        });
    }
    for &(k, _) in &kinds {
        out.push(Step::Objects {
            kind: k,
            set: close_table(r, k),
        });
    }
    Ok(out)
}

/// Execute request `r`: its [`steps`], each a set-form statement
/// with the same-commit guard. Returns its outcome and whether it closed
/// a class, parameter definition or value list.
fn execute(
    conn: &Connection,
    c: i64,
    r: usize,
    scope: &Scope,
) -> Result<(PurgeOutcome, bool), DbError> {
    let mut acc = Accum::default();
    for step in steps(conn, r, scope)? {
        run_step(conn, c, r, &step, &mut acc)?;
    }
    Ok((
        PurgeOutcome {
            request_index: r,
            closed_out: counts(acc.object_rows, acc.per_table),
            objects: acc.objects,
            spared: counts(acc.spared_objects, acc.spared),
            unbound: acc.unbound,
        },
        acc.vocabulary,
    ))
}

fn run_step(
    conn: &Connection,
    c: i64,
    r: usize,
    step: &Step,
    acc: &mut Accum,
) -> Result<(), DbError> {
    match step {
        Step::Values { table, pred, meta } => {
            if *meta {
                close_rows(conn, c, table, pred, Some(&value_antijoin(table)), acc)
            } else {
                close_values(conn, c, table, pred, acc)
            }
        }
        Step::Rows { table, pred, anti } => close_rows(conn, c, table, pred, anti.as_deref(), acc),
        Step::Leg {
            leg,
            set,
            anti,
            identity,
        } => {
            if let Some(a) = anti {
                let spared = count_leg_set(
                    conn,
                    leg,
                    set,
                    Some(&format!("valid_from_commit < {c} AND NOT ({a})")),
                )?;
                add(&mut acc.spared, leg.table, spared);
            }
            let n = close_leg_set(conn, leg, c, set, anti.as_deref())?;
            if *identity {
                acc.object_rows += n;
            } else {
                add(&mut acc.per_table, leg.table, n);
            }
            Ok(())
        }
        Step::Unbind => {
            test_hooks::count();
            let mut stmt = conn.prepare(&bindings_sql(r))?;
            let ids: Vec<i64> = stmt
                .query_map([], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            for pd in ids {
                let mut row = read_pd_row(conn, pd)?;
                row.parameter_value_list_id = None;
                revise_pd_row(conn, c, &row)?;
                acc.unbound += 1;
            }
            Ok(())
        }
        Step::Lists { set } => {
            let closed = close_value_lists_set(conn, c, set)?;
            add(
                &mut acc.per_table,
                "semantic_annotation",
                closed.annotations,
            );
            acc.object_rows += closed.members + closed.lists;
            acc.objects += closed.lists;
            acc.vocabulary |= closed.lists > 0;
            acc.spared_objects += spared_objects(conn, c, r, SetKind::ValueList)?;
            Ok(())
        }
        Step::Objects { kind, set } => {
            let n = close_current_rows_set(conn, kind.object_table(), c, set)?;
            acc.objects += n;
            acc.object_rows += n;
            acc.vocabulary |= n > 0 && matches!(kind, SetKind::Class | SetKind::Parameter);
            acc.spared_objects += spared_objects(conn, c, r, *kind)?;
            Ok(())
        }
    }
}

/// Is the id table non-empty?
fn has(conn: &Connection, table: &str) -> Result<bool, DbError> {
    Ok(query_count(
        conn,
        &format!("SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT 1)"),
    )? > 0)
}

/// Read-only preview of `reqs` on `conn` (one per-operation RO
/// connection): the same sets and the same [`steps`], counted instead of
/// run, with no survivors, so the counts, `outside_scope` and
/// `template_stamped` are upper bounds of what a call that also writes
/// would close. Rows are counted once per table across a request's steps
/// (the `preview_dependent_counts` OR grouping), and a row an earlier
/// request of the call already counts is not counted again, as the commit
/// closes it once, in the earlier request.
pub(crate) fn preview(
    conn: &Connection,
    reqs: &[PurgeRequest],
) -> Result<Vec<PurgePreview>, DbError> {
    let scopes = resolve_requests(
        conn,
        reqs,
        &CallRenames::default(),
        &mut NameFolds::default(),
    )?;
    create_survivor_tables(conn)?;
    for (r, scope) in scopes.iter().enumerate() {
        build_request_sets(conn, r, scope)?;
    }
    build_all_close_parameters(conn, reqs.len())?;
    let stamped = property::read(conn, "template_name")?.is_some();
    let mut out = Vec::with_capacity(reqs.len());
    // (table, predicate) of every earlier request's steps.
    let mut earlier: Vec<(&'static str, String)> = Vec::new();
    let excl = |earlier: &[(&'static str, String)], table: &str| -> String {
        let preds: Vec<String> = earlier
            .iter()
            .filter(|(t, _)| *t == table)
            .map(|(_, p)| format!("({p})"))
            .collect();
        if preds.is_empty() {
            String::new()
        } else {
            format!(" AND NOT ({})", preds.join(" OR "))
        }
    };
    for (r, scope) in scopes.iter().enumerate() {
        // (table, counts as object rows, live filter, predicates)
        type Group = (&'static str, bool, &'static str, Vec<String>);
        let mut groups: Vec<Group> = Vec::new();
        let mut push = |table: &'static str, object: bool, pred: String| {
            let live = if table == "value_array" {
                ""
            } else {
                " AND valid_to_commit IS NULL"
            };
            match groups.iter_mut().find(|g| g.0 == table && g.1 == object) {
                Some(g) => g.3.push(pred),
                None => groups.push((table, object, live, vec![pred])),
            }
        };
        let mut objects = 0usize;
        for step in steps(conn, r, scope)? {
            match step {
                Step::Values { table, pred, .. } | Step::Rows { table, pred, .. } => {
                    push(table, false, pred)
                }
                Step::Leg {
                    leg, set, identity, ..
                } => push(leg.table, identity, leg.key.render(IdSet::Table(&set))),
                Step::Unbind => {}
                Step::Lists { set } => {
                    let ids = format!("IN (SELECT id FROM {set})");
                    push(
                        "semantic_annotation",
                        false,
                        format!("object_type = 'parameter_value_list' AND object_id {ids}"),
                    );
                    push(
                        "semantic_annotation",
                        false,
                        format!(
                            "object_type = 'list_value' AND object_id IN \
                             (SELECT id FROM list_value WHERE parameter_value_list_id {ids})"
                        ),
                    );
                    push("list_value", true, format!("parameter_value_list_id {ids}"));
                    push("parameter_value_list", true, format!("id {ids}"));
                    objects += query_count(
                        conn,
                        &format!(
                            "SELECT count(*) FROM parameter_value_list \
                             WHERE valid_to_commit IS NULL AND id {ids}{}",
                            excl(&earlier, "parameter_value_list")
                        ),
                    )? as usize;
                }
                Step::Objects { kind, set } => {
                    let pred = format!("id IN (SELECT id FROM {set})");
                    objects += query_count(
                        conn,
                        &format!(
                            "SELECT count(*) FROM {} WHERE valid_to_commit IS NULL AND {pred}{}",
                            kind.object_table(),
                            excl(&earlier, kind.object_table())
                        ),
                    )? as usize;
                    push(kind.object_table(), true, pred);
                }
            }
        }
        let mut object_rows = 0usize;
        let mut per_table: Vec<TableCount> = Vec::new();
        let mut mine: Vec<(&'static str, String)> = Vec::new();
        for (table, object, live, preds) in groups {
            let clause = preds
                .iter()
                .map(|p| format!("({p})"))
                .collect::<Vec<_>>()
                .join(" OR ");
            let n = query_count(
                conn,
                &format!(
                    "SELECT count(*) FROM {table} WHERE ({clause}){live}{}",
                    excl(&earlier, table)
                ),
            )? as usize;
            if object {
                object_rows += n;
            } else {
                add(&mut per_table, table, n);
            }
            mine.extend(preds.into_iter().map(|p| (table, p)));
        }
        earlier.extend(mine);
        let template_stamped = stamped && scope.is_vocabulary() && {
            let i = stamped_item(conn, r)?;
            i.classes + i.parameters + i.value_lists > 0
        };
        let cells = scope_cells(conn, r, scope)?;
        out.push(PurgePreview {
            request_index: r,
            objects,
            cascade: counts(object_rows, per_table),
            outside_scope: outside_scope(conn, r, reqs.len())?,
            cells,
            warnings: large_warning(r, cells).into_iter().collect(),
            template_stamped,
        });
    }
    drop_tables(conn)?;
    Ok(out)
}

/// Close the live, pre-call rows of `table` matching `pred` and not spared
/// by `anti`, counting closed and spared rows.
fn close_rows(
    conn: &Connection,
    c: i64,
    table: &str,
    pred: &str,
    anti: Option<&str>,
    acc: &mut Accum,
) -> Result<(), DbError> {
    if let Some(anti) = anti {
        let spared = query_count(
            conn,
            &format!(
                "SELECT count(*) FROM {table} WHERE {pred} AND valid_from_commit < {c} \
                 AND valid_to_commit IS NULL AND NOT ({anti})"
            ),
        )?;
        add(&mut acc.spared, table, spared as usize);
    }
    let anti = anti.map(|a| format!(" AND {a}")).unwrap_or_default();
    let n = exec(
        conn,
        &format!(
            "UPDATE {table} SET valid_to_commit = ? WHERE {pred} \
             AND valid_from_commit < ? AND valid_to_commit IS NULL{anti}"
        ),
        params![c, c],
    )?;
    add(&mut acc.per_table, table, n);
    Ok(())
}

/// The value close-out of one tier over `pred`, on the SET
/// lists and pre-image copies `history` closes every value with (one
/// close-out definition, CR2-9): per
/// retention mode one statement, the definition's effective retention from
/// `pg_retention`; survivors spared by the value-key anti-join. Arrays
/// resolve their `array_id`s on narrow columns first, then copy the
/// pre-image to `value_array_hist` and delete, keyed by `array_id`
/// (engine rule 2).
fn close_values(
    conn: &Connection,
    c: i64,
    table: &str,
    pred: &str,
    acc: &mut Accum,
) -> Result<(), DbError> {
    let anti = value_antijoin(table);
    if table == "value_array" {
        let spared = query_count(
            conn,
            &format!(
                "SELECT count(*) FROM value_array WHERE {pred} AND valid_from_commit < {c} \
                 AND NOT ({anti})"
            ),
        )?;
        add(&mut acc.spared, table, spared as usize);
        exec_batch(
            conn,
            &format!(
                "CREATE OR REPLACE TEMP TABLE pg_arrays AS \
                 SELECT v.array_id, r.retained FROM \
                   (SELECT array_id, parameter_definition_id FROM value_array \
                    WHERE {pred} AND valid_from_commit < {c} AND {anti}) v \
                 JOIN pg_retention r ON r.pd_id = v.parameter_definition_id"
            ),
        )?;
        let copied_kept = exec(
            conn,
            &format!("{ARRAY_PREIMAGE}array_id IN (SELECT array_id FROM pg_arrays WHERE retained)"),
            params![c],
        )?;
        let copied_stub = exec(
            conn,
            &format!(
                "{ARRAY_PREIMAGE_STUB}array_id IN \
                 (SELECT array_id FROM pg_arrays WHERE NOT retained)"
            ),
            params![c],
        )?;
        let deleted = exec(
            conn,
            "DELETE FROM value_array WHERE array_id IN (SELECT array_id FROM pg_arrays)",
            [],
        )?;
        if deleted != copied_kept + copied_stub {
            return Err(DbError::Internal(format!(
                "purge array close-out: copied {} pre-images, deleted {deleted}",
                copied_kept + copied_stub
            )));
        }
        add(&mut acc.per_table, table, deleted);
        return Ok(());
    }
    let spared = query_count(
        conn,
        &format!(
            "SELECT count(*) FROM {table} WHERE {pred} AND valid_from_commit < {c} \
             AND valid_to_commit IS NULL AND NOT ({anti})"
        ),
    )?;
    add(&mut acc.spared, table, spared as usize);
    for retained in [true, false] {
        let set = if retained { CLOSE_SET } else { CLOSE_SET_STUB };
        let n = exec(
            conn,
            &format!(
                "UPDATE {table} SET {set} WHERE {pred} \
                 AND parameter_definition_id IN \
                   (SELECT pd_id FROM pg_retention WHERE retained = {retained}) \
                 AND valid_from_commit < ? AND valid_to_commit IS NULL AND {anti}"
            ),
            params![c, c],
        )?;
        add(&mut acc.per_table, table, n);
    }
    Ok(())
}

/// The objects of request `r`'s dependent set of kind `k` that survived
/// (pre-call rows only).
fn spared_objects(conn: &Connection, c: i64, r: usize, k: SetKind) -> Result<usize, DbError> {
    Ok(query_count(
        conn,
        &format!(
            "SELECT count(*) FROM {} WHERE valid_to_commit IS NULL \
             AND valid_from_commit < {c} AND id IN (SELECT id FROM {}) \
             AND id IN (SELECT id FROM {})",
            k.object_table(),
            dep_table(r, k),
            surv_table(k)
        ),
    )? as usize)
}

// ---------------------------------------------------------------------------
// Statement helpers (counted, for the statement-count scale guard)
// ---------------------------------------------------------------------------

/// `conn.execute`, counted by [`test_hooks::statements`].
pub(crate) fn exec<P: duckdb::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> Result<usize, duckdb::Error> {
    test_hooks::count();
    conn.execute(sql, params)
}

/// A batch of statements, counted as one each.
pub(crate) fn exec_batch(conn: &Connection, sql: &str) -> Result<(), duckdb::Error> {
    test_hooks::count();
    conn.execute_batch(sql)
}

/// A `SELECT count(*) …` (no binds), counted.
pub(crate) fn query_count(conn: &Connection, sql: &str) -> Result<i64, DbError> {
    test_hooks::count();
    Ok(conn.query_row(sql, [], |r| r.get(0))?)
}

// ---------------------------------------------------------------------------
// The object kinds a purge holds id sets of
// ---------------------------------------------------------------------------

/// An object kind with purge id sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SetKind {
    Entity,
    Class,
    Parameter,
    Alternative,
    Scenario,
    ValueList,
}

impl SetKind {
    pub(crate) const ALL: [SetKind; 6] = [
        SetKind::Entity,
        SetKind::Class,
        SetKind::Parameter,
        SetKind::Alternative,
        SetKind::Scenario,
        SetKind::ValueList,
    ];

    pub(crate) fn tag(self) -> &'static str {
        match self {
            SetKind::Entity => "entity",
            SetKind::Class => "class",
            SetKind::Parameter => "parameter",
            SetKind::Alternative => "alternative",
            SetKind::Scenario => "scenario",
            SetKind::ValueList => "value_list",
        }
    }

    /// The table whose `id` rows are the objects.
    pub(crate) fn object_table(self) -> &'static str {
        match self {
            SetKind::Entity => "entity",
            SetKind::Class => "entity_class",
            SetKind::Parameter => "parameter_definition",
            SetKind::Alternative => "alternative",
            SetKind::Scenario => "scenario",
            SetKind::ValueList => "parameter_value_list",
        }
    }

    /// The object delete's kind (its leg registry), if it has one.
    pub(crate) fn object_kind(self) -> Option<ObjectKind> {
        match self {
            SetKind::Entity => Some(ObjectKind::Entity),
            SetKind::Class => Some(ObjectKind::Class),
            SetKind::Parameter => Some(ObjectKind::Parameter),
            SetKind::Alternative => Some(ObjectKind::Alternative),
            SetKind::Scenario => Some(ObjectKind::Scenario),
            SetKind::ValueList => None,
        }
    }

    pub(crate) fn of(kind: ObjectKind) -> SetKind {
        match kind {
            ObjectKind::Entity => SetKind::Entity,
            ObjectKind::Class => SetKind::Class,
            ObjectKind::Parameter => SetKind::Parameter,
            ObjectKind::Alternative => SetKind::Alternative,
            ObjectKind::Scenario => SetKind::Scenario,
        }
    }
}

/// Request `r`'s dependent set of kind `k`.
pub(crate) fn dep_table(r: usize, k: SetKind) -> String {
    format!("pg{r}_dep_{}", k.tag())
}

/// Request `r`'s close set of kind `k`.
pub(crate) fn close_table(r: usize, k: SetKind) -> String {
    format!("pg{r}_close_{}", k.tag())
}

/// The call's survivors of kind `k` (spared upward).
pub(crate) fn surv_table(k: SetKind) -> String {
    format!("pg_surv_{}", k.tag())
}

/// The call's survivor value keys `(e, p, a)`.
pub(crate) const SURV_VALUE: &str = "pg_surv_value";
/// The call's survivor activations `(e, a)`.
pub(crate) const SURV_ACTIVATION: &str = "pg_surv_activation";
/// The call's survivor memberships `(g, m, a)`, `a = 0` for a base row.
pub(crate) const SURV_MEMBERSHIP: &str = "pg_surv_membership";
/// The scenarios whose ranking the call sets (`id`).
pub(crate) const SURV_RANKING: &str = "pg_surv_ranking";

/// Create (or empty) a one-column `id` temp table.
pub(crate) fn create_id_table(conn: &Connection, name: &str) -> Result<(), DbError> {
    exec_batch(
        conn,
        &format!("CREATE OR REPLACE TEMP TABLE {name} (id BIGINT)"),
    )?;
    Ok(())
}

/// Append `ids` to an `id` table (one statement, one bind).
pub(crate) fn insert_ids(conn: &Connection, name: &str, ids: &[i64]) -> Result<(), DbError> {
    if ids.is_empty() {
        return Ok(());
    }
    let list = duckdb::types::Value::List(ids.iter().map(|&i| i.into()).collect());
    exec(
        conn,
        &format!("INSERT INTO {name} SELECT unnest(?::BIGINT[])"),
        [list],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The two sets (§3.6, V-B1)
// ---------------------------------------------------------------------------

/// Create the survivor tables, empty.
pub(crate) fn create_survivor_tables(conn: &Connection) -> Result<(), DbError> {
    for k in SetKind::ALL {
        create_id_table(conn, &surv_table(k))?;
    }
    create_id_table(conn, SURV_RANKING)?;
    exec_batch(
        conn,
        &format!(
            "CREATE OR REPLACE TEMP TABLE {SURV_VALUE} (e BIGINT, p BIGINT, a BIGINT); \
             CREATE OR REPLACE TEMP TABLE {SURV_ACTIVATION} (e BIGINT, a BIGINT); \
             CREATE OR REPLACE TEMP TABLE {SURV_MEMBERSHIP} (g BIGINT, m BIGINT, a BIGINT)"
        ),
    )?;
    Ok(())
}

/// Spare upward: every object a survivor's identity references
/// survives too. Value keys, activations and memberships first lend their
/// entities, parameters and alternatives; then an entity's elements
/// (transitively, N-D chains), the classes of surviving entities and
/// parameters, a class's dimension classes (transitively) and the value list
/// a surviving definition actually binds (V-S6). Iterative SQL, no id
/// passes through Rust.
pub(crate) fn spare_upward(conn: &Connection) -> Result<(), DbError> {
    if test_hooks::upward_disabled() {
        return Ok(());
    }
    let ent = surv_table(SetKind::Entity);
    let class = surv_table(SetKind::Class);
    let param = surv_table(SetKind::Parameter);
    let alt = surv_table(SetKind::Alternative);
    let scen = surv_table(SetKind::Scenario);
    let list = surv_table(SetKind::ValueList);
    exec_batch(
        conn,
        &format!(
            "INSERT INTO {ent} SELECT e FROM {SURV_VALUE}; \
             INSERT INTO {param} SELECT p FROM {SURV_VALUE}; \
             INSERT INTO {alt} SELECT a FROM {SURV_VALUE}; \
             INSERT INTO {ent} SELECT e FROM {SURV_ACTIVATION}; \
             INSERT INTO {alt} SELECT a FROM {SURV_ACTIVATION}; \
             INSERT INTO {ent} SELECT g FROM {SURV_MEMBERSHIP}; \
             INSERT INTO {ent} SELECT m FROM {SURV_MEMBERSHIP}; \
             INSERT INTO {alt} SELECT a FROM {SURV_MEMBERSHIP} WHERE a <> 0; \
             INSERT INTO {scen} SELECT id FROM {SURV_RANKING}"
        ),
    )?;
    // An N-D entity's elements, transitively.
    fixpoint(
        conn,
        &format!(
            "INSERT INTO {ent} SELECT DISTINCT ee.element_entity_id FROM entity_element ee \
             WHERE ee.valid_to_commit IS NULL AND ee.entity_id IN (SELECT id FROM {ent}) \
               AND ee.element_entity_id NOT IN (SELECT id FROM {ent})"
        ),
    )?;
    exec_batch(
        conn,
        &format!(
            "INSERT INTO {class} SELECT DISTINCT entity_class_id FROM entity \
             WHERE valid_to_commit IS NULL AND id IN (SELECT id FROM {ent}); \
             INSERT INTO {class} SELECT DISTINCT entity_class_id FROM parameter_definition \
             WHERE valid_to_commit IS NULL AND id IN (SELECT id FROM {param}); \
             INSERT INTO {list} SELECT DISTINCT parameter_value_list_id FROM parameter_definition \
             WHERE valid_to_commit IS NULL AND parameter_value_list_id IS NOT NULL \
               AND id IN (SELECT id FROM {param})"
        ),
    )?;
    // A class's dimension classes, transitively.
    fixpoint(
        conn,
        &format!(
            "INSERT INTO {class} SELECT DISTINCT dimension_class_id FROM entity_class_dimension \
             WHERE valid_to_commit IS NULL AND dimension_class_id IS NOT NULL \
               AND entity_class_id IN (SELECT id FROM {class}) \
               AND dimension_class_id NOT IN (SELECT id FROM {class})"
        ),
    )?;
    Ok(())
}

/// Run an `INSERT … WHERE … NOT IN (target)` until it inserts nothing.
/// Returns the number of rounds that inserted rows.
fn fixpoint(conn: &Connection, insert: &str) -> Result<usize, DbError> {
    let mut rounds = 0;
    while exec(conn, insert, [])? > 0 {
        rounds += 1;
    }
    Ok(rounds)
}

/// Build request `r`'s dependent sets: `base` gives, per kind, a `SELECT`
/// of the scope's object ids (live rows only); then, when `expand`, the
/// cascade edges grow them by an iterative frontier until nothing is added
/// (a class adds its entities, parameters and B2 dependant classes; an
/// entity its B1 containers). Every kind's table exists afterwards (empty
/// when unreached).
///
/// A class's own entities and parameters are reached from the whole
/// dependent set (a spared class's unstated children still close), but the
/// cross-object edges (B1: an entity's N-D containers; B2: the classes
/// that use a class as a dimension) only from the dependent set minus the
/// call's survivors, recomputed every round: a spared object closes
/// nothing, so it pushes nothing outside the scope. The survivor tables
/// must exist (empty for a preview).
pub(crate) fn build_dep_sets(
    conn: &Connection,
    r: usize,
    base: &[(SetKind, String)],
    expand: bool,
) -> Result<(), DbError> {
    for k in SetKind::ALL {
        create_id_table(conn, &dep_table(r, k))?;
    }
    for (k, select) in base {
        exec(
            conn,
            &format!("INSERT INTO {} {select}", dep_table(r, *k)),
            [],
        )?;
    }
    if !expand {
        return Ok(());
    }
    loop {
        let mut added = 0;
        for src in [SetKind::Class, SetKind::Entity] {
            let kind = src.object_kind().expect("class and entity have leg sets");
            for edge in cascade_edges(kind) {
                let dst = dep_table(r, SetKind::of(edge.neighbor_kind));
                let from = dep_table(r, src);
                let closing = format!(
                    "SELECT id FROM {from} WHERE id NOT IN (SELECT id FROM {})",
                    surv_table(src)
                );
                let ids = if SetKind::of(edge.neighbor_kind) == src {
                    IdSet::Query(&closing)
                } else {
                    IdSet::Table(&from)
                };
                added += exec(
                    conn,
                    &format!(
                        "INSERT INTO {dst} SELECT DISTINCT x.id FROM ({}) x(id) \
                         WHERE x.id NOT IN (SELECT id FROM {dst})",
                        edge.select_ids(ids)
                    ),
                    [],
                )?;
            }
        }
        if added == 0 {
            return Ok(());
        }
    }
}

/// Request `r`'s close sets: its dependent sets minus the call's survivors.
pub(crate) fn build_close_sets(conn: &Connection, r: usize) -> Result<(), DbError> {
    for k in SetKind::ALL {
        exec_batch(
            conn,
            &format!(
                "CREATE OR REPLACE TEMP TABLE {} AS SELECT DISTINCT id FROM {} \
                 WHERE id NOT IN (SELECT id FROM {})",
                close_table(r, k),
                dep_table(r, k),
                surv_table(k)
            ),
        )?;
    }
    Ok(())
}

/// The frontier hard check: no cascade edge out of request `r`'s
/// close set may reach a survivor. Upward sparing makes that impossible (a
/// survivor's elements, class and dimension classes survive), so a hit is a
/// core bug: [`DbError::Internal`], and the call rolls back.
pub(crate) fn check_frontier(conn: &Connection, r: usize) -> Result<(), DbError> {
    for src in [SetKind::Class, SetKind::Entity] {
        let kind = src.object_kind().expect("class and entity have leg sets");
        for edge in cascade_edges(kind) {
            let dst = SetKind::of(edge.neighbor_kind);
            let n = query_count(
                conn,
                &format!(
                    "SELECT count(*) FROM ({}) x(id) WHERE x.id IN (SELECT id FROM {})",
                    edge.select_ids(IdSet::Table(&close_table(r, src))),
                    surv_table(dst)
                ),
            )?;
            if n > 0 {
                return Err(DbError::Internal(format!(
                    "purge request {r}: the {} -> {} frontier ({}.{}) of the close set \
                     reaches {n} surviving object(s)",
                    src.tag(),
                    dst.tag(),
                    edge.table,
                    edge.column
                )));
            }
        }
    }
    Ok(())
}

/// Drop every purge temp table of the connection (end of the purge phase).
pub(crate) fn drop_tables(conn: &Connection) -> Result<(), DbError> {
    let mut stmt = conn.prepare(
        "SELECT table_name FROM duckdb_tables() \
         WHERE temporary AND regexp_matches(table_name, '^pg[0-9]*_')",
    )?;
    let names: Vec<String> = stmt
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    for n in names {
        exec_batch(conn, &format!("DROP TABLE IF EXISTS temp.main.{n}"))?;
    }
    Ok(())
}

/// Test-only switches (debug builds). Thread-local, like
/// [`crate::api::test_hooks`].
#[doc(hidden)]
pub mod test_hooks {
    #[cfg(debug_assertions)]
    thread_local! {
        static STATEMENTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        static NO_UPWARD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        static NO_SURVIVORS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        static CONFLICTS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        static LARGE: std::cell::Cell<u64> = const { std::cell::Cell::new(super::PURGE_LARGE_CELLS) };
    }

    /// Run `f` with the memory guard's threshold set to `n` cells.
    #[cfg(debug_assertions)]
    pub fn with_large_threshold<R>(n: u64, f: impl FnOnce() -> R) -> R {
        LARGE.with(|c| c.set(n));
        let out = f();
        LARGE.with(|c| c.set(super::PURGE_LARGE_CELLS));
        out
    }

    pub(crate) fn large_threshold() -> u64 {
        #[cfg(debug_assertions)]
        {
            LARGE.with(|c| c.get())
        }
        #[cfg(not(debug_assertions))]
        {
            super::PURGE_LARGE_CELLS
        }
    }

    /// Make the next `n` purge phases on this thread fail with a synthetic
    /// write-write conflict after their first request closed its rows, so
    /// the retry loop re-runs the whole call.
    #[cfg(debug_assertions)]
    pub fn inject_conflicts_mid_purge(n: u32) {
        CONFLICTS.with(|c| c.set(n));
    }

    pub(crate) fn take_conflict() -> bool {
        #[cfg(debug_assertions)]
        {
            CONFLICTS.with(|c| {
                let n = c.get();
                if n > 0 {
                    c.set(n - 1);
                }
                n > 0
            })
        }
        #[cfg(not(debug_assertions))]
        {
            false
        }
    }

    pub(crate) fn count() {
        #[cfg(debug_assertions)]
        STATEMENTS.with(|c| c.set(c.get() + 1));
    }

    /// The purge statements run on this thread so far.
    #[cfg(debug_assertions)]
    pub fn statements() -> u64 {
        STATEMENTS.with(|c| c.get())
    }

    /// Run `f` with upward sparing off: a survivor no longer spares the
    /// objects its identity references, so the frontier hard check fires.
    #[cfg(debug_assertions)]
    pub fn without_upward_sparing<R>(f: impl FnOnce() -> R) -> R {
        NO_UPWARD.with(|c| c.set(true));
        let out = f();
        NO_UPWARD.with(|c| c.set(false));
        out
    }

    /// Run `f` with the call's survivor sets forced empty (the same-commit
    /// guard alone must keep the rows the call wrote live).
    #[cfg(debug_assertions)]
    pub fn without_survivors<R>(f: impl FnOnce() -> R) -> R {
        NO_SURVIVORS.with(|c| c.set(true));
        let out = f();
        NO_SURVIVORS.with(|c| c.set(false));
        out
    }

    pub(crate) fn upward_disabled() -> bool {
        #[cfg(debug_assertions)]
        {
            NO_UPWARD.with(|c| c.get())
        }
        #[cfg(not(debug_assertions))]
        {
            false
        }
    }

    pub(crate) fn survivors_disabled() -> bool {
        #[cfg(debug_assertions)]
        {
            NO_SURVIVORS.with(|c| c.get())
        }
        #[cfg(not(debug_assertions))]
        {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::api::commit_structural;
    use crate::object_delete::{close_leg_set_sql, object_legs};
    use crate::types::{StructuralChange as SC, WriteOptions};

    fn class(name: &str, dims: &[&str]) -> SC {
        SC::CreateEntityClass {
            name: name.into(),
            dims: dims.iter().map(|d| d.to_string()).collect(),
            description: None,
        }
    }

    fn entity(class: &str, name: &str, elements: &[&str]) -> SC {
        SC::CreateEntity {
            class: class.into(),
            name: (!name.is_empty()).then(|| name.to_string()),
            elements: elements.iter().map(|e| e.to_string()).collect(),
            description: None,
        }
    }

    /// node, unit; link(node, unit); flow(link, node): a three-level N-D
    /// chain. Entities n1, n2, u1, l1 = (n1, u1), l2 = (n2, u1),
    /// f1 = (l1, n2).
    fn chain_db(dir: &tempfile::TempDir) -> std::path::PathBuf {
        let db = dir.path().join("t.duckdb");
        crate::schema_init::init_schema(&db).unwrap();
        commit_structural(
            &db,
            vec![
                class("node", &[]),
                class("unit", &[]),
                class("link", &["node", "unit"]),
                class("flow", &["link", "node"]),
                entity("node", "n1", &[]),
                entity("node", "n2", &[]),
                entity("unit", "u1", &[]),
                entity("link", "l1", &["n1", "u1"]),
                entity("link", "l2", &["n2", "u1"]),
                entity("flow", "f1", &["l1", "n2"]),
            ],
            "t",
            "m",
            WriteOptions::default(),
        )
        .unwrap();
        db
    }

    fn id(conn: &Connection, table: &str, name: &str) -> i64 {
        conn.query_row(
            &format!("SELECT id FROM {table} WHERE cur_name = lower(?)"),
            [name],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn names(conn: &Connection, set: &str, table: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!(
                "SELECT name FROM {table} WHERE valid_to_commit IS NULL \
                 AND id IN (SELECT id FROM {set}) ORDER BY name"
            ))
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    fn open(db: &Path) -> Connection {
        Connection::open(db).unwrap()
    }

    /// The dependent set grows by the B1 and class frontiers; the close
    /// set is it minus the survivors spared upward; the frontier of the
    /// close set never reaches a survivor.
    #[test]
    fn two_sets_spare_upward_through_an_nd_chain() {
        let dir = tempfile::tempdir().unwrap();
        let db = chain_db(&dir);
        let conn = open(&db);
        create_survivor_tables(&conn).unwrap();

        // Entities{class: node}: the B1 frontier reaches l1, l2, then f1.
        let node = id(&conn, "entity_class", "node");
        build_dep_sets(
            &conn,
            0,
            &[(
                SetKind::Entity,
                format!(
                    "SELECT id FROM entity WHERE valid_to_commit IS NULL \
                     AND entity_class_id = {node}"
                ),
            )],
            true,
        )
        .unwrap();
        assert_eq!(
            names(&conn, &dep_table(0, SetKind::Entity), "entity"),
            ["f1", "l1", "l2", "n1", "n2"]
        );

        // Classes{unit}: B2 reaches link, then flow; the classes' entities
        // and their B1 containers follow.
        let unit = id(&conn, "entity_class", "unit");
        build_dep_sets(
            &conn,
            1,
            &[(SetKind::Class, format!("SELECT {unit}"))],
            true,
        )
        .unwrap();
        assert_eq!(
            names(&conn, &dep_table(1, SetKind::Class), "entity_class"),
            ["flow", "link", "unit"]
        );
        assert_eq!(
            names(&conn, &dep_table(1, SetKind::Entity), "entity"),
            ["f1", "l1", "l2", "u1"]
        );

        // Entities{} with f1 re-stated: f1 spares l1 and n2, l1 spares n1
        // and u1, and their classes; only l2 closes.
        build_dep_sets(
            &conn,
            2,
            &[(
                SetKind::Entity,
                "SELECT id FROM entity WHERE valid_to_commit IS NULL".into(),
            )],
            true,
        )
        .unwrap();
        let f1 = id(&conn, "entity", "f1");
        insert_ids(&conn, &surv_table(SetKind::Entity), &[f1]).unwrap();
        spare_upward(&conn).unwrap();
        assert_eq!(
            names(&conn, &surv_table(SetKind::Entity), "entity"),
            ["f1", "l1", "n1", "n2", "u1"]
        );
        assert_eq!(
            names(&conn, &surv_table(SetKind::Class), "entity_class"),
            ["flow", "link", "node", "unit"]
        );
        build_close_sets(&conn, 2).unwrap();
        assert_eq!(
            names(&conn, &close_table(2, SetKind::Entity), "entity"),
            ["l2"]
        );
        check_frontier(&conn, 2).unwrap();
        drop_tables(&conn).unwrap();
        let left: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_tables() WHERE temporary",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
    }

    /// With upward sparing off, a re-stated f1 leaves its elements in the
    /// close set, and the B1 frontier of the closing l1 reaches f1: the hard
    /// check refuses.
    #[test]
    fn frontier_reaching_a_survivor_is_internal() {
        let dir = tempfile::tempdir().unwrap();
        let db = chain_db(&dir);
        let conn = open(&db);
        create_survivor_tables(&conn).unwrap();
        build_dep_sets(
            &conn,
            0,
            &[(
                SetKind::Entity,
                "SELECT id FROM entity WHERE valid_to_commit IS NULL".into(),
            )],
            true,
        )
        .unwrap();
        let f1 = id(&conn, "entity", "f1");
        insert_ids(&conn, &surv_table(SetKind::Entity), &[f1]).unwrap();
        test_hooks::without_upward_sparing(|| spare_upward(&conn)).unwrap();
        build_close_sets(&conn, 0).unwrap();
        let err = check_frontier(&conn, 0).unwrap_err();
        assert!(matches!(err, DbError::Internal(_)), "{err}");
        assert!(err.to_string().contains("entity -> entity"), "{err}");
    }

    /// A set-form leg is a hash join of the table against the id set, never
    /// a nested loop.
    #[test]
    fn set_form_leg_plans_as_a_hash_join() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("t.duckdb");
        crate::schema_init::init_schema(&db).unwrap();
        let conn = open(&db);
        conn.execute_batch(
            "INSERT INTO commit (user, message) VALUES ('t', 'fixture'); \
             INSERT INTO alternative (name, valid_from_commit) VALUES ('base', 2); \
             INSERT INTO entity_class (name, valid_from_commit) VALUES ('node', 2); \
             INSERT INTO entity (entity_class_id, name, valid_from_commit) \
               SELECT 1, 'e' || i, 2 FROM range(20000) t(i); \
             INSERT INTO entity_alternative (entity_id, alternative_id, active, valid_from_commit) \
               SELECT id, 1, TRUE, 2 FROM entity; \
             CREATE TEMP TABLE ids AS SELECT id FROM entity WHERE id % 2 = 0;",
        )
        .unwrap();
        let legs = object_legs(ObjectKind::Entity).unwrap();
        for leg in legs.dependents.iter().filter(|l| !l.array) {
            let sql = close_leg_set_sql(leg, "ids", None);
            let mut stmt = conn.prepare(&format!("EXPLAIN {sql}")).unwrap();
            let plan: Vec<String> = stmt
                .query_map([99i64, 99], |r| r.get::<_, String>(1))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let plan = plan.join("\n");
            // DuckDB 2.0 labels operators "Hash Join"; older releases
            // "HASH_JOIN": accept either spelling.
            assert!(
                plan.contains("Hash Join") || plan.contains("HASH_JOIN"),
                "{}:\n{plan}",
                leg.table
            );
            for bad in [
                "Nested Loop",
                "NESTED_LOOP",
                "Blockwise NL",
                "BLOCKWISE_NL",
                "Piecewise Merge",
                "PIECEWISE_MERGE",
                "Cross Product",
                "CROSS_PRODUCT",
            ] {
                assert!(!plan.contains(bad), "{}: {bad}\n{plan}", leg.table);
            }
        }
    }
}
