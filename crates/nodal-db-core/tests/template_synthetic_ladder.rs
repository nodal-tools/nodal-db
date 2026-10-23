//! Synthetic production-scale template ladder: parse, load, chain, reconcile.
//!
//! A made-up "synth" tenant generated in code (no fixture file): ~15 classes
//! (0-D up to 4-D), ~170 parameter definitions of every declared-type flavor
//! (float, float-or-series, list-bound string, typeless), value lists,
//! parameter groups, and a linear 20-span migration ladder (versions 1..=21)
//! that uses EVERY step kind of the hint vocabulary (rename_class /
//! _parameter / _value_list / _list_member, move_parameter, merge_parameter,
//! merge_list_member, drop_parameter / _class / _value_list / _list_member,
//! `run:` programs) interleaved with reconcile-only EMPTY spans whose content
//! is absorbed by the head template's additive diff.
//!
//! The generator keeps a vocabulary model per version: span `k → k+1` is a
//! list of authored steps plus the matching edit of the model, so the
//! template AT any version `k` (its vocabulary + the ladder up to `k`) is
//! derivable. Properties covered:
//!
//! 1. Every per-version template parses and validates; the head ladder is
//!    complete and linear, each span's step-kind signature matches an
//!    independently written table, and intra-span ordering discipline holds
//!    (drops follow the program that reads what they destroy; drop_parameter
//!    precedes the orphaned list's drop_value_list).
//! 2. A fresh head load is ONE commit, audits clean, counts exact against the
//!    generator model, export round-trips (normalized model + content hash),
//!    and a re-apply no-ops.
//! 3. A floor (v1) DB holding data climbs the whole ladder with the programs
//!    registered: converged, stamped, the data carried through every hint,
//!    and the vocabulary ends identical to a fresh head load.
//! 4. A stamped mid-version entrant refuses on an unregistered program
//!    before any step commits; one past the last program walks to head with
//!    no programs registered.
//! 5. (slow tier) Every version 1..=20 is a legal entry point: a fresh
//!    version-k DB chains + reconciles to exactly the head vocabulary.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use duckdb::arrow::array::{ArrayRef, Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use serde_json::Value as Json;

use nodal_db_core::api::write_parameter;
use nodal_db_core::schema_init::init_schema;
use nodal_db_core::template::ir::{normalized_model, MigrationStep};
use nodal_db_core::template::{
    apply, export, parse_str, plan, template_content_hash, validate, ApplyOptions,
    MigrationRegistry, Template,
};
use nodal_db_core::types::WriteOptions;
use nodal_db_core::verbs::stamp_template_identity;

// ---------------------------------------------------------------------------
// The vocabulary model
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Kind {
    /// Rank-0 float, optional declared default.
    Float(Option<f64>),
    /// Two declared members: float + a label-indexed `t` series.
    FloatOrSeries,
    /// String bound to a value list (kind inferred from the list).
    Str(String),
    /// No declared type: materializes on first write.
    Typeless,
}

#[derive(Clone, Debug)]
struct Param {
    name: String,
    kind: Kind,
    group: &'static str,
}

#[derive(Clone, Debug)]
struct Class {
    name: String,
    dims: Vec<String>,
    params: Vec<Param>,
}

#[derive(Clone, Debug)]
struct List {
    name: String,
    members: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct Vocab {
    lists: Vec<List>,
    classes: Vec<Class>,
}

const STEMS: &[&str] = &[
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "theta", "iota", "kappa", "lambda",
    "sigma", "omega",
];
const SUFFIXES: &[&str] = &["coef", "limit", "weight", "offset", "share"];
const GROUPS: &[&str] = &["core", "economics", "operation", "misc"];

/// `n` made-up filler definitions cycling through every declared-type flavor.
fn fillers(n: usize, seed: usize) -> Vec<Param> {
    assert!(n <= STEMS.len() * SUFFIXES.len());
    (0..n)
        .map(|i| {
            let name = format!(
                "{}_{}",
                STEMS[i % STEMS.len()],
                SUFFIXES[(i / STEMS.len() + seed) % SUFFIXES.len()]
            );
            let kind = match (i + seed) % 6 {
                0 => Kind::FloatOrSeries,
                1 => Kind::Float(Some(i as f64 * 0.25)),
                2 => Kind::Float(None),
                3 => Kind::Str("levels".into()),
                4 => Kind::Typeless,
                _ => Kind::Str("yes_no".into()),
            };
            Param {
                name,
                kind,
                group: GROUPS[(i + seed) % GROUPS.len()],
            }
        })
        .collect()
}

fn p(name: &str, kind: Kind) -> Param {
    Param {
        name: name.into(),
        kind,
        group: "core",
    }
}

fn class(name: &str, dims: &[&str], n_fill: usize, seed: usize, special: Vec<Param>) -> Class {
    let mut params = special;
    params.extend(fillers(n_fill, seed));
    Class {
        name: name.into(),
        dims: dims.iter().map(|s| s.to_string()).collect(),
        params,
    }
}

fn list(name: &str, members: &[&str]) -> List {
    List {
        name: name.into(),
        members: members.iter().map(|s| s.to_string()).collect(),
    }
}

/// The floor vocabulary (version 1).
fn base_vocab() -> Vocab {
    use Kind::*;
    Vocab {
        lists: vec![
            list("yes_no", &["yes", "no"]),
            list("levels", &["low", "mid", "high"]),
            list(
                "modes_old",
                &[
                    "on_off",
                    "continuous_old",
                    "continuous",
                    "deprecated_x",
                    "stepped",
                ],
            ),
            list("flag_modes", &["f_a", "f_b"]),
            list("shapes", &["flat", "ramp", "peak"]),
        ],
        classes: vec![
            class("area", &[], 10, 0, vec![p("hub_count", Float(None))]),
            class(
                "hub",
                &[],
                24,
                1,
                vec![
                    p("demand_old", Float(None)),
                    p("demand", Float(None)),
                    p("flag_mode", Str("flag_modes".into())),
                ],
            ),
            class(
                "widget",
                &[],
                28,
                2,
                vec![
                    p("power_max", Float(None)),
                    p("legacy_scale", Float(None)),
                    p("scale_factor", Float(Some(1.0))),
                    p("mode", Str("modes_old".into())),
                    p("spare_coef", Float(None)),
                    p("enabled", Str("yes_no".into())),
                ],
            ),
            class(
                "gizmo",
                &[],
                14,
                3,
                vec![p("eff_a", Float(None)), p("efficiency", Float(Some(1.0)))],
            ),
            class(
                "store",
                &[],
                12,
                4,
                vec![p("profile_shape", Str("shapes".into()))],
            ),
            class("route", &[], 10, 5, vec![p("len_km", Float(None))]),
            class("fleet", &[], 8, 0, vec![]),
            class("legacy_thing", &[], 4, 1, vec![]),
            class("hub__widget", &["hub", "widget"], 10, 2, vec![]),
            class("widget__hub", &["widget", "hub"], 10, 3, vec![]),
            class("area__hub", &["area", "hub"], 8, 4, vec![]),
            class(
                "area__route__fleet",
                &["area", "route", "fleet"],
                6,
                5,
                vec![],
            ),
            class(
                "area__hub__widget__route",
                &["area", "hub", "widget", "route"],
                4,
                0,
                vec![],
            ),
            class("legacy_thing__hub", &["legacy_thing", "hub"], 4, 1, vec![]),
        ],
    }
}

impl Vocab {
    fn class_mut(&mut self, name: &str) -> &mut Class {
        self.classes
            .iter_mut()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("{name}"))
    }
    fn list_mut(&mut self, name: &str) -> &mut List {
        self.lists
            .iter_mut()
            .find(|l| l.name == name)
            .unwrap_or_else(|| panic!("{name}"))
    }
    fn param_mut(&mut self, class: &str, name: &str) -> &mut Param {
        self.class_mut(class)
            .params
            .iter_mut()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("{class}.{name}"))
    }
    fn take_param(&mut self, class: &str, name: &str) -> Param {
        let c = self.class_mut(class);
        let i = c.params.iter().position(|p| p.name == name).unwrap();
        c.params.remove(i)
    }
    fn rename_param(&mut self, class: &str, from: &str, to: &str) {
        self.param_mut(class, from).name = to.into();
    }
    fn remove_member(&mut self, list: &str, value: &str) {
        self.list_mut(list).members.retain(|m| m != value);
    }
    fn n_defs(&self) -> usize {
        self.classes.iter().map(|c| c.params.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// The ladder: span k → k+1 = authored steps + the matching model edit
// ---------------------------------------------------------------------------

struct Span {
    steps: &'static [&'static str],
    edit: fn(&mut Vocab),
}

const PROG_BACKFILL: &str = "synth_scale_backfill";
const PROG_HUB_COUNT: &str = "synth_hub_count";

const HEAD: i64 = 21;

fn ladder_spans() -> Vec<Span> {
    vec![
        // 1→2
        Span {
            steps: &[
                "rename_parameter: {class: widget, from: power_max, to: capacity}",
                "rename_parameter: {class: route, from: len_km, to: length}",
            ],
            edit: |v| {
                v.rename_param("widget", "power_max", "capacity");
                v.rename_param("route", "len_km", "length");
            },
        },
        // 2→3 reconcile-only: a new series parameter.
        Span {
            steps: &[],
            edit: |v| v.class_mut("widget").params.push(p("ramp_rate", Kind::FloatOrSeries)),
        },
        // 3→4
        Span {
            steps: &["rename_class: {from: gizmo, to: gadget}"],
            edit: |v| v.class_mut("gizmo").name = "gadget".into(),
        },
        // 4→5 reconcile-only: a list gains a member.
        Span { steps: &[], edit: |v| v.list_mut("modes_old").members.push("stepped_fast".into()) },
        // 5→6
        Span {
            steps: &["rename_value_list: {from: modes_old, to: operating_mode}"],
            edit: |v| {
                v.list_mut("modes_old").name = "operating_mode".into();
                v.param_mut("widget", "mode").kind = Kind::Str("operating_mode".into());
            },
        },
        // 6→7 (rewrite of stored values is the default)
        Span {
            steps: &["rename_list_member: {list: operating_mode, from: on_off, to: binary}"],
            edit: |v| {
                for m in &mut v.list_mut("operating_mode").members {
                    if m == "on_off" {
                        *m = "binary".into();
                    }
                }
            },
        },
        // 7→8: program reads legacy_scale, THEN the drop.
        Span {
            steps: &[
                "run: synth_scale_backfill",
                "drop_parameter: {class: widget, parameter: legacy_scale}",
            ],
            edit: |v| {
                v.take_param("widget", "legacy_scale");
            },
        },
        // 8→9 reconcile-only: a new class.
        Span { steps: &[], edit: |v| v.classes.push(class("sensor", &[], 5, 3, vec![])) },
        // 9→10 (target present: fill-if-absent)
        Span {
            steps: &["merge_parameter: {class: hub, from: demand_old, to: demand}"],
            edit: |v| {
                v.take_param("hub", "demand_old");
            },
        },
        // 10→11 (target member present: rewrite + close)
        Span {
            steps: &[
                "merge_list_member: {list: operating_mode, from: continuous_old, to: continuous}",
            ],
            edit: |v| v.remove_member("operating_mode", "continuous_old"),
        },
        // 11→12 reconcile-only: a new 2-D class over a reconcile-added class.
        Span {
            steps: &[],
            edit: |v| v.classes.push(class("sensor__hub", &["sensor", "hub"], 3, 1, vec![])),
        },
        // 12→13: drop the only binding, then the orphaned list.
        Span {
            steps: &[
                "drop_parameter: {class: hub, parameter: flag_mode}",
                "drop_value_list: {list: flag_modes}",
            ],
            edit: |v| {
                v.take_param("hub", "flag_mode");
                v.lists.retain(|l| l.name != "flag_modes");
            },
        },
        // 13→14 (definition holds no values)
        Span {
            steps: &["move_parameter: {name: spare_coef, from_class: widget, to_class: gadget}"],
            edit: |v| {
                let moved = v.take_param("widget", "spare_coef");
                v.class_mut("gadget").params.push(moved);
            },
        },
        // 14→15: class cascade takes the dependent 2-D class with it.
        Span {
            steps: &["drop_class: {class: legacy_thing}"],
            edit: |v| v.classes.retain(|c| c.name != "legacy_thing" && c.name != "legacy_thing__hub"),
        },
        // 15→16: target exists ⇒ the stale source is dropped.
        Span {
            steps: &[
                "rename_parameter: {class: gadget, from: eff_a, to: efficiency, on_collision: drop_source}",
            ],
            edit: |v| {
                v.take_param("gadget", "eff_a");
            },
        },
        // 16→17
        Span {
            steps: &["drop_list_member: {list: operating_mode, value: deprecated_x}"],
            edit: |v| v.remove_member("operating_mode", "deprecated_x"),
        },
        // 17→18: a values-only program.
        Span { steps: &["run: synth_hub_count"], edit: |_| {} },
        // 18→19 reconcile-only: a new list + its binding.
        Span {
            steps: &[],
            edit: |v| {
                v.lists.push(list("tiers", &["bronze", "silver", "gold"]));
                v.class_mut("store").params.push(p("tier", Kind::Str("tiers".into())));
            },
        },
        // 19→20: rename with a translation table; reconcile rebinds the list.
        Span {
            steps: &[
                "rename_parameter: {class: widget, from: enabled, to: commit_mode, \
                 map_values: {\"yes\": forced}, unmapped_values: drop}",
            ],
            edit: |v| {
                v.lists.push(list("commit_modes", &["none", "forced"]));
                let prm = v.param_mut("widget", "enabled");
                prm.name = "commit_mode".into();
                prm.kind = Kind::Str("commit_modes".into());
            },
        },
        // 20→21 reconcile-only head absorption: content declared at head.
        Span {
            steps: &[],
            edit: |v| v.class_mut("hub").params.push(p("reserve_margin", Kind::Float(Some(0.1)))),
        },
    ]
}

/// Independent transcription of the ladder's shape (kind:count, sorted) per
/// span `from` — written by hand, NOT derived from `ladder_spans`, so a slip
/// in either side fails the cross-check.
#[rustfmt::skip]
const EXPECTED_SIGNATURES: &[(i64, &str)] = &[
    (1, "rename_parameter:2"),
    (2, ""),
    (3, "rename_class:1"),
    (4, ""),
    (5, "rename_value_list:1"),
    (6, "rename_list_member:1"),
    (7, "drop_parameter:1,run:1"),
    (8, ""),
    (9, "merge_parameter:1"),
    (10, "merge_list_member:1"),
    (11, ""),
    (12, "drop_parameter:1,drop_value_list:1"),
    (13, "move_parameter:1"),
    (14, "drop_class:1"),
    (15, "rename_parameter:1"),
    (16, "drop_list_member:1"),
    (17, "run:1"),
    (18, ""),
    (19, "rename_parameter:1"),
    (20, ""),
];

fn vocab_at(version: i64) -> Vocab {
    let mut v = base_vocab();
    for span in ladder_spans().iter().take((version - 1) as usize) {
        (span.edit)(&mut v);
    }
    v
}

fn q(s: &str) -> String {
    format!("\"{s}\"")
}

/// Render the template AT `version`: its vocabulary + the ladder up to it.
fn template_text(version: i64) -> String {
    let v = vocab_at(version);
    let mut y = String::new();
    y.push_str(&format!(
        "# Generated synthetic template (template_synthetic_ladder.rs).\n\
         template: {{name: synth, version: {}, role: input}}\n",
        q(&version.to_string())
    ));
    y.push_str("value_lists:\n");
    for l in &v.lists {
        let members: Vec<String> = l
            .members
            .iter()
            .map(|m| format!("{{value: {}}}", q(m)))
            .collect();
        y.push_str(&format!(
            "  - name: {}\n    members: [{}]\n",
            q(&l.name),
            members.join(", ")
        ));
    }
    y.push_str("parameter_groups:\n");
    for (i, g) in GROUPS.iter().enumerate() {
        y.push_str(&format!(
            "  - {{name: {}, order: {}}}\n",
            q(g),
            (i + 1) * 10
        ));
    }
    y.push_str("classes:\n");
    for c in &v.classes {
        y.push_str(&format!("  - name: {}\n", q(&c.name)));
        if !c.dims.is_empty() {
            let d: Vec<String> = c.dims.iter().map(|d| q(d)).collect();
            y.push_str(&format!("    dimensions: [{}]\n", d.join(", ")));
        }
        if c.params.is_empty() {
            continue;
        }
        y.push_str("    parameters:\n");
        for prm in &c.params {
            let body = match &prm.kind {
                Kind::Float(None) => "types: [{kind: float}]".to_string(),
                Kind::Float(Some(d)) => {
                    format!("types: [{{kind: float}}], default: {{kind: float, value: {d:?}}}")
                }
                Kind::FloatOrSeries => "types: [{kind: float}, \
                     {kind: indexed, dims: [{name: t, kind: label}]}]"
                    .to_string(),
                Kind::Str(l) => format!("value_list: {}", q(l)),
                Kind::Typeless => String::new(),
            };
            let sep = if body.is_empty() { "" } else { ", " };
            y.push_str(&format!(
                "      - {{name: {}{sep}{body}, group: {}}}\n",
                q(&prm.name),
                q(prm.group)
            ));
        }
    }
    let spans = ladder_spans();
    if version > 1 {
        y.push_str("migrations:\n");
        for (i, s) in spans.iter().take((version - 1) as usize).enumerate() {
            let from = i + 1;
            y.push_str(&format!(
                "  - from: {}\n    to: {}\n",
                q(&from.to_string()),
                q(&(from + 1).to_string())
            ));
            if s.steps.is_empty() {
                y.push_str("    steps: []\n");
            } else {
                y.push_str("    steps:\n");
                for st in s.steps {
                    y.push_str(&format!("      - {st}\n"));
                }
            }
        }
    }
    y
}

fn head_text() -> String {
    template_text(HEAD)
}

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

fn opts() -> ApplyOptions {
    ApplyOptions::default()
}

fn q1(db: &Path, sql: &str) -> i64 {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn qs(db: &Path, sql: &str) -> Option<String> {
    let conn = Connection::open(db).unwrap();
    conn.query_row(sql, [], |r| r.get::<_, String>(0)).ok()
}

fn meta(db: &Path, key: &str) -> Option<String> {
    qs(
        db,
        &format!("SELECT value FROM metadata WHERE cur_name = '{key}'"),
    )
}

fn live(db: &Path, table: &str, pred: &str) -> i64 {
    q1(
        db,
        &format!("SELECT count(*) FROM {table} WHERE valid_to_commit IS NULL AND {pred}"),
    )
}

const MUST_BE_EMPTY: &[&str] = &[
    "audit_dangling_ref",
    "audit_dangling_semantic_ref",
    "audit_class_set_entity_name_dup",
    "audit_element_class_mismatch",
    "audit_element_arity",
    "audit_value_class_mismatch",
    "audit_axis_integrity",
    "audit_position_range",
    "audit_type_conformance",
    "audit_array_integrity",
    "audit_array_payload_xor",
    "audit_index_accelerator",
    "audit_cross_tier_duplicate",
    "audit_tier_intent",
    "audit_value_list_membership",
    "audit_metadata_reserved_dup",
    "audit_membership_cycle",
    "audit_realizes_cycle",
    "audit_history_tiling",
];

fn assert_audits_empty(db: &Path, which: &str) {
    for view in MUST_BE_EMPTY {
        assert_eq!(
            q1(db, &format!("SELECT count(*) FROM {view}")),
            0,
            "{which}: {view}"
        );
    }
}

fn fresh_db(dir: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let db = dir.path().join(name);
    init_schema(&db).unwrap();
    db
}

/// Normalized model minus the seed block (entities written as DATA show up
/// in an export's seed; the vocabulary comparison ignores them).
fn vocab_model(t: &Template) -> Json {
    let mut m = normalized_model(t);
    if let Some(o) = m.as_object_mut() {
        o.remove("seed");
    }
    m
}

fn assert_db_vocab_is_head(db: &Path, head: &Template, which: &str) {
    let (exported, _warnings) = export(db).unwrap();
    assert_eq!(
        vocab_model(head),
        vocab_model(&exported),
        "{which}: vocabulary != head"
    );
}

fn float_batch(entity: &str, v: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(Float64Array::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn string_batch(entity: &str, v: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![entity])) as ArrayRef,
            Arc::new(StringArray::from(vec![v])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn write_f(db: &Path, class: &str, param: &str, entity: &str, v: f64) {
    write_parameter(
        db,
        class,
        param,
        "alt",
        float_batch(entity, v),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
}

fn write_s(db: &Path, class: &str, param: &str, entity: &str, v: &str) {
    write_parameter(
        db,
        class,
        param,
        "alt",
        string_batch(entity, v),
        &[],
        WriteOptions::default(),
        "tester",
        "",
    )
    .unwrap();
}

fn cur_scalar(db: &Path, class: &str, param: &str, entity: &str, col: &str) -> Option<String> {
    qs(
        db,
        &format!(
            "SELECT CAST(v.{col} AS VARCHAR) FROM value_scalar v \
         JOIN parameter_definition pd ON pd.id = v.parameter_definition_id \
         JOIN entity_class c ON c.id = pd.cur_cls \
         JOIN entity e ON e.id = v.entity_id \
         WHERE v.valid_to_commit IS NULL AND c.cur_name = '{class}' \
           AND pd.cur_name = '{param}' AND e.cur_name = '{entity}'"
        ),
    )
}

fn cur_f(db: &Path, class: &str, param: &str, entity: &str) -> Option<f64> {
    cur_scalar(db, class, param, entity, "v_float").map(|s| s.parse().unwrap())
}

fn cur_s(db: &Path, class: &str, param: &str, entity: &str) -> Option<String> {
    cur_scalar(db, class, param, entity, "v_str")
}

/// The two ladder programs, written against the PUBLIC API only (Layer 3):
/// read per-operation, close, then write through `write_parameter`.
fn registry(calls: &Arc<AtomicUsize>) -> MigrationRegistry {
    let mut r = MigrationRegistry::new();
    let c1 = calls.clone();
    r.register(PROG_BACKFILL, move |db: &Path, user: &str| {
        c1.fetch_add(1, Ordering::SeqCst);
        // widget.scale_factor := 100 × widget.legacy_scale, per entity/alternative.
        let rows: Vec<(String, String, f64)> = {
            let conn = Connection::open(db)?;
            let mut st = conn.prepare(
                "SELECT e.name, a.name, v.v_float FROM value_scalar v \
                 JOIN parameter_definition pd ON pd.id = v.parameter_definition_id \
                 JOIN entity_class c ON c.id = pd.cur_cls \
                 JOIN entity e ON e.id = v.entity_id AND e.valid_to_commit IS NULL \
                 JOIN alternative a ON a.id = v.alternative_id AND a.valid_to_commit IS NULL \
                 WHERE v.valid_to_commit IS NULL AND c.cur_name = 'widget' \
                   AND pd.cur_name = 'legacy_scale' ORDER BY e.name",
            )?;
            let it = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            it.collect::<Result<_, _>>()?
        };
        for (ent, alt, v) in rows {
            write_parameter(
                db,
                "widget",
                "scale_factor",
                &alt,
                float_batch(&ent, v * 100.0),
                &[],
                WriteOptions::default(),
                user,
                "synth v8: scale backfill",
            )?;
        }
        Ok(())
    });
    let c2 = calls.clone();
    r.register(PROG_HUB_COUNT, move |db: &Path, user: &str| {
        c2.fetch_add(1, Ordering::SeqCst);
        let (areas, hubs): (Vec<String>, i64) = {
            let conn = Connection::open(db)?;
            let hubs: i64 = conn.query_row(
                "SELECT count(*) FROM entity e JOIN entity_class c ON c.id = e.cur_cls \
                 WHERE c.cur_name = 'hub'",
                [],
                |r| r.get(0),
            )?;
            let mut st = conn.prepare(
                "SELECT e.name FROM entity e JOIN entity_class c ON c.id = e.cur_cls \
                 WHERE c.cur_name = 'area' ORDER BY e.name",
            )?;
            let areas = st.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
            (areas, hubs)
        };
        for a in areas {
            write_parameter(
                db,
                "area",
                "hub_count",
                "alt",
                float_batch(&a, hubs as f64),
                &[],
                WriteOptions::default(),
                user,
                "synth v18: hub count",
            )?;
        }
        Ok(())
    });
    r
}

fn step_kind(s: &MigrationStep) -> &'static str {
    match s {
        MigrationStep::RenameClass { .. } => "rename_class",
        MigrationStep::RenameParameter { .. } => "rename_parameter",
        MigrationStep::RenameValueList { .. } => "rename_value_list",
        MigrationStep::RenameListMember { .. } => "rename_list_member",
        MigrationStep::MoveParameter { .. } => "move_parameter",
        MigrationStep::MergeParameter { .. } => "merge_parameter",
        MigrationStep::MergeListMember { .. } => "merge_list_member",
        MigrationStep::DropParameter { .. } => "drop_parameter",
        MigrationStep::DropClass { .. } => "drop_class",
        MigrationStep::DropValueList { .. } => "drop_value_list",
        MigrationStep::DropListMember { .. } => "drop_list_member",
        MigrationStep::Run(_) => "run",
    }
}

fn signature(steps: &[MigrationStep]) -> String {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for s in steps {
        *counts.entry(step_kind(s)).or_default() += 1;
    }
    counts
        .iter()
        .map(|(k, n)| format!("{k}:{n}"))
        .collect::<Vec<_>>()
        .join(",")
}

// ---------------------------------------------------------------------------
// 1. Parse / validate / ladder shape
// ---------------------------------------------------------------------------

#[test]
fn every_version_template_parses_and_validates() {
    for k in 1..=HEAD {
        let text = template_text(k);
        let t = parse_str(&text).unwrap_or_else(|e| panic!("v{k} parse: {e}"));
        validate(&t).unwrap_or_else(|e| panic!("v{k} validate: {e}"));
        assert_eq!(t.template.version, k.to_string());
        assert_eq!(t.migrations.len() as i64, k - 1);
    }
    // Medium-scale sanity: the head is a real-sized vocabulary.
    let head = vocab_at(HEAD);
    assert!(
        head.n_defs() >= 150,
        "head carries {} definitions",
        head.n_defs()
    );
    assert!(head_text().lines().count() >= 200);
}

#[test]
fn ladder_is_complete_linear_and_matches_the_independent_table() {
    let t = parse_str(&head_text()).unwrap();
    validate(&t).unwrap();
    let spans: BTreeMap<i64, _> = t
        .migrations
        .iter()
        .map(|m| (m.from.parse::<i64>().unwrap(), m))
        .collect();
    assert_eq!(spans.len() as i64, HEAD - 1);
    for v in 1..HEAD {
        let m = spans
            .get(&v)
            .unwrap_or_else(|| panic!("missing span from {v}"));
        assert_eq!(
            m.to.parse::<i64>().unwrap(),
            v + 1,
            "span {v} steps one version"
        );
    }
    // Every version is a legal starting point reaching the ceiling.
    for start in 1..=HEAD {
        let (mut cur, mut n) = (start, 0);
        while cur != HEAD {
            cur = spans[&cur].to.parse().unwrap();
            n += 1;
        }
        assert_eq!(n, HEAD - start);
    }

    assert_eq!(EXPECTED_SIGNATURES.len() as i64, HEAD - 1);
    for &(from, sig) in EXPECTED_SIGNATURES {
        assert_eq!(signature(&spans[&from].steps), sig, "span {from} signature");
    }
    // Every step kind of the vocabulary is exercised somewhere.
    let mut kinds: Vec<&str> = t
        .migrations
        .iter()
        .flat_map(|m| m.steps.iter().map(step_kind))
        .collect();
    kinds.sort();
    kinds.dedup();
    assert_eq!(kinds.len(), 12, "all twelve step kinds used: {kinds:?}");

    // Run refs: the two-program roster, each exactly once.
    let mut refs: Vec<&str> = t
        .migrations
        .iter()
        .flat_map(|m| m.steps.iter())
        .filter_map(|s| match s {
            MigrationStep::Run(p) => Some(p.as_str()),
            _ => None,
        })
        .collect();
    refs.sort();
    assert_eq!(refs, [PROG_HUB_COUNT, PROG_BACKFILL]); // sorted

    // Intra-span ordering discipline.
    for span in &t.migrations {
        if let Some(run_at) = span
            .steps
            .iter()
            .position(|s| matches!(s, MigrationStep::Run(_)))
        {
            for (i, s) in span.steps.iter().enumerate() {
                if matches!(
                    s,
                    MigrationStep::DropParameter { .. }
                        | MigrationStep::DropClass { .. }
                        | MigrationStep::DropValueList { .. }
                ) {
                    assert!(
                        i > run_at,
                        "span {}: drop must follow the program",
                        span.from
                    );
                }
            }
        }
        let last_dp = span
            .steps
            .iter()
            .rposition(|s| matches!(s, MigrationStep::DropParameter { .. }));
        let first_dvl = span
            .steps
            .iter()
            .position(|s| matches!(s, MigrationStep::DropValueList { .. }));
        if let (Some(dp), Some(dvl)) = (last_dp, first_dvl) {
            assert!(
                dp < dvl,
                "span {}: drop_parameter before drop_value_list",
                span.from
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Fresh head load
// ---------------------------------------------------------------------------

#[test]
fn head_fresh_load_counts_audits_and_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir, "head.duckdb");
    let text = head_text();
    let t = parse_str(&text).unwrap();
    let out = apply(&db, &t, &MigrationRegistry::new(), "tester", "", &opts()).unwrap();
    assert!(out.converged && !out.no_op);
    assert_eq!(
        out.commits.len(),
        1,
        "fresh load is ONE commit (no chain on an unstamped DB)"
    );
    assert_audits_empty(&db, "head");

    assert_eq!(meta(&db, "template_name").as_deref(), Some("synth"));
    assert_eq!(
        meta(&db, "template_version").as_deref(),
        Some(HEAD.to_string().as_str())
    );
    assert_eq!(
        meta(&db, "template_content_hash").as_deref(),
        Some(template_content_hash(&t).as_str())
    );

    // Counts exact against the generator model.
    let v = vocab_at(HEAD);
    let n_dims: usize = v.classes.iter().map(|c| c.dims.len()).sum();
    let n_members: usize = v.lists.iter().map(|l| l.members.len()).sum();
    let n_series = v
        .classes
        .iter()
        .flat_map(|c| &c.params)
        .filter(|p| matches!(p.kind, Kind::FloatOrSeries))
        .count();
    let n_typeless = v
        .classes
        .iter()
        .flat_map(|c| &c.params)
        .filter(|p| matches!(p.kind, Kind::Typeless))
        .count();
    let n_defaults = v
        .classes
        .iter()
        .flat_map(|c| &c.params)
        .filter(|p| matches!(p.kind, Kind::Float(Some(_))))
        .count();
    assert_eq!(live(&db, "entity_class", "true"), v.classes.len() as i64);
    assert_eq!(live(&db, "entity_class_dimension", "true"), n_dims as i64);
    assert_eq!(live(&db, "parameter_definition", "true"), v.n_defs() as i64);
    assert_eq!(
        live(&db, "parameter_value_list", "true"),
        v.lists.len() as i64
    );
    assert_eq!(live(&db, "list_value", "true"), n_members as i64);
    // One member per typed definition, two for float-or-series, none typeless.
    assert_eq!(
        live(&db, "parameter_type", "true"),
        (v.n_defs() - n_typeless + n_series) as i64
    );
    assert_eq!(
        live(&db, "definition_value_scalar", "slot = 'default_value'"),
        n_defaults as i64
    );

    // Groups: every definition grouped.
    let pres: Json = serde_json::from_str(&meta(&db, "presentation").unwrap()).unwrap();
    assert_eq!(
        pres["parameter_groups"].as_array().unwrap().len(),
        GROUPS.len()
    );

    // Semantic round-trip + idempotency.
    let (exported, _w) = export(&db).unwrap();
    assert_eq!(normalized_model(&t), normalized_model(&exported));
    assert_eq!(template_content_hash(&t), template_content_hash(&exported));
    let again = apply(
        &db,
        &parse_str(&text).unwrap(),
        &MigrationRegistry::new(),
        "tester",
        "",
        &opts(),
    )
    .unwrap();
    assert!(
        again.no_op,
        "unchanged head re-apply is the hash short-circuit"
    );
}

// ---------------------------------------------------------------------------
// 3. Floor DB with data climbs the whole ladder
// ---------------------------------------------------------------------------

#[test]
fn floor_db_with_data_climbs_the_whole_ladder() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir, "floor.duckdb");
    let floor = parse_str(&template_text(1)).unwrap();
    assert!(
        apply(
            &db,
            &floor,
            &MigrationRegistry::new(),
            "tester",
            "",
            &opts()
        )
        .unwrap()
        .converged
    );

    // Data touched by the hints and programs.
    write_f(&db, "widget", "power_max", "w1", 10.0);
    write_f(&db, "widget", "power_max", "w2", 20.0);
    write_f(&db, "widget", "legacy_scale", "w1", 0.5);
    write_s(&db, "widget", "mode", "w1", "on_off");
    write_s(&db, "widget", "mode", "w2", "continuous_old");
    write_s(&db, "widget", "mode", "w3", "stepped");
    write_s(&db, "widget", "enabled", "w1", "yes");
    write_s(&db, "widget", "enabled", "w2", "no");
    write_f(&db, "hub", "demand_old", "h1", 5.0);
    write_f(&db, "hub", "demand_old", "h2", 6.0);
    write_f(&db, "hub", "demand", "h2", 7.0);
    write_s(&db, "hub", "flag_mode", "h1", "f_a");
    write_f(&db, "gizmo", "eff_a", "g1", 0.3);
    write_f(&db, "gizmo", "efficiency", "g1", 0.9);
    write_f(&db, "route", "len_km", "r1", 42.0);
    write_f(&db, "area", "alpha_coef", "a1", 1.0);
    write_f(&db, "legacy_thing", "alpha_limit", "lt1", 3.0);

    let head = parse_str(&head_text()).unwrap();
    let p = plan(&db, &head).unwrap();
    assert_eq!(
        p.pending_migrations.len() as i64,
        HEAD - 1,
        "the whole ladder is pending"
    );
    assert!(p.blocked.is_empty(), "blockers: {:?}", p.blocked);

    let calls = Arc::new(AtomicUsize::new(0));
    let out = apply(&db, &head, &registry(&calls), "tester", "", &opts()).unwrap();
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "each program ran exactly once"
    );
    assert_eq!(out.run_programs.len(), 2);
    assert_eq!(
        meta(&db, "template_version").as_deref(),
        Some(HEAD.to_string().as_str())
    );
    assert_eq!(
        meta(&db, "template_content_hash").as_deref(),
        Some(template_content_hash(&head).as_str())
    );
    assert_eq!(meta(&db, "template_migration_step"), None);
    assert_audits_empty(&db, "floor walk");

    // Data carried through the hints.
    assert_eq!(cur_f(&db, "widget", "capacity", "w1"), Some(10.0));
    assert_eq!(cur_f(&db, "widget", "capacity", "w2"), Some(20.0));
    assert_eq!(cur_f(&db, "route", "length", "r1"), Some(42.0));
    assert_eq!(
        cur_f(&db, "widget", "scale_factor", "w1"),
        Some(50.0),
        "program 1 backfill"
    );
    assert_eq!(
        live(&db, "parameter_definition", "cur_name = 'legacy_scale'"),
        0
    );
    assert_eq!(
        cur_s(&db, "widget", "mode", "w1").as_deref(),
        Some("binary")
    );
    assert_eq!(
        cur_s(&db, "widget", "mode", "w2").as_deref(),
        Some("continuous")
    );
    assert_eq!(
        cur_s(&db, "widget", "mode", "w3").as_deref(),
        Some("stepped")
    );
    assert_eq!(
        cur_s(&db, "widget", "commit_mode", "w1").as_deref(),
        Some("forced")
    );
    assert_eq!(
        cur_s(&db, "widget", "commit_mode", "w2"),
        None,
        "unmapped value dropped"
    );
    assert_eq!(
        cur_f(&db, "hub", "demand", "h1"),
        Some(5.0),
        "merge fills the absent"
    );
    assert_eq!(
        cur_f(&db, "hub", "demand", "h2"),
        Some(7.0),
        "merge keeps the present"
    );
    assert_eq!(cur_s(&db, "hub", "flag_mode", "h1"), None);
    assert_eq!(cur_f(&db, "gadget", "efficiency", "g1"), Some(0.9));
    assert_eq!(
        live(&db, "entity", "cur_name = 'g1'"),
        1,
        "class rename keeps entities"
    );
    assert_eq!(
        cur_f(&db, "area", "hub_count", "a1"),
        Some(2.0),
        "program 2 ran on hubs"
    );
    assert_eq!(
        live(&db, "entity", "cur_name = 'lt1'"),
        0,
        "drop_class cascade"
    );
    // Versioned close-out, never erasure.
    assert!(q1(&db, "SELECT count(*) FROM entity WHERE name = 'lt1'") > 0);

    // The climbed vocabulary is exactly the head's.
    assert_db_vocab_is_head(&db, &head, "floor walk");
    let again = apply(&db, &head, &registry(&calls), "tester", "", &opts()).unwrap();
    assert!(again.no_op);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "no program re-ran");
}

// ---------------------------------------------------------------------------
// 4. Stamped mid-version entrants (the importer's path)
// ---------------------------------------------------------------------------

#[test]
fn stamped_entrant_refuses_on_unregistered_program_before_any_step() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir, "stamped.duckdb");
    stamp_template_identity(&db, "synth", "5", false, "tester").unwrap();
    let before = q1(&db, "SELECT max(id) FROM commit");
    let head = parse_str(&head_text()).unwrap();
    let err = apply(&db, &head, &MigrationRegistry::new(), "tester", "", &opts())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(PROG_BACKFILL) && err.contains("not registered"),
        "refusal names the first missing program: {err}"
    );
    assert_eq!(
        q1(&db, "SELECT max(id) FROM commit"),
        before,
        "no step committed"
    );
    assert_eq!(meta(&db, "template_version").as_deref(), Some("5"));
}

#[test]
fn stamped_entrant_past_the_last_program_walks_to_head_without_programs() {
    let dir = tempfile::tempdir().unwrap();
    let db = fresh_db(&dir, "v18.duckdb");
    // A v18 DB (the load stamps its identity) holding data the remaining
    // hint span (19→20 rename with translation table) must carry.
    let t18 = parse_str(&template_text(18)).unwrap();
    assert!(
        apply(&db, &t18, &MigrationRegistry::new(), "tester", "", &opts())
            .unwrap()
            .converged
    );
    assert_eq!(meta(&db, "template_version").as_deref(), Some("18"));
    write_s(&db, "widget", "enabled", "w1", "yes");
    let head = parse_str(&head_text()).unwrap();
    let out = apply(&db, &head, &MigrationRegistry::new(), "tester", "", &opts()).unwrap();
    assert!(out.converged, "skips: {:?}", out.plan.skipped);
    assert_eq!(
        meta(&db, "template_version").as_deref(),
        Some(HEAD.to_string().as_str())
    );
    assert!(meta(&db, "template_content_hash").is_some());
    assert!(out.run_programs.is_empty());
    assert_eq!(
        cur_s(&db, "widget", "commit_mode", "w1").as_deref(),
        Some("forced")
    );
    assert_db_vocab_is_head(&db, &head, "v18 walk");
}

// ---------------------------------------------------------------------------
// 5. Every version is a legal entry point (slow tier)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "slow: pre-release tier; run via `just full` / --include-ignored"]
fn every_version_chains_and_reconciles_to_the_head_vocabulary() {
    let dir = tempfile::tempdir().unwrap();
    let head = parse_str(&head_text()).unwrap();
    for k in 1..HEAD {
        let db = fresh_db(&dir, &format!("v{k}.duckdb"));
        let tk = parse_str(&template_text(k)).unwrap();
        assert!(
            apply(&db, &tk, &MigrationRegistry::new(), "tester", "", &opts())
                .unwrap()
                .converged
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let out = apply(&db, &head, &registry(&calls), "tester", "", &opts())
            .unwrap_or_else(|e| panic!("v{k} → head: {e}"));
        assert!(out.converged, "v{k}: skips {:?}", out.plan.skipped);
        let expected_runs = [8i64, 18].iter().filter(|&&r| k < r).count();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            expected_runs,
            "v{k} program runs"
        );
        assert_eq!(
            meta(&db, "template_version").as_deref(),
            Some(HEAD.to_string().as_str())
        );
        assert_audits_empty(&db, &format!("v{k}"));
        assert_db_vocab_is_head(&db, &head, &format!("v{k}"));
    }
}
