//! The template IR object model (`specs/template_ir_plan.md` §2, decision D1):
//! YAML 1.2 authoring surface over a JSON-compatible object model, parsed here
//! into plain serde structs, plus the pure load-time validation (§2.2, §3.4,
//! §3.5) and the normalized-object-model content hash (§4.1).
//!
//! ## Serialization choices (D1), stated for the review record
//! - Parser/emitter: **`serde_norway`** (the maintained `serde_yaml` fork).
//!   Chosen because its plain-scalar resolution follows the **YAML 1.2 core
//!   schema in every context**: only `true`/`false` resolve as booleans, so
//!   `yes`/`no`/`on`/`off` stay strings even under `deserialize_any` (untagged
//!   enums like [`Lit`]) — the Norway problem (§2.1 mitigation (a)) is closed
//!   at the parser. The alternative `serde-saphyr` 1.3.0 was spike-tested and
//!   **rejected**: its `deserialize_any` resolves `yes`/`no`/`on`/`off`/`y`/`n`
//!   as YAML 1.1 booleans, which reopens the exact footgun the plan names.
//! - Strict typing (§2.1 mitigation (b)): every struct below uses
//!   `deny_unknown_fields`, so a typo'd key fails at parse, before any write.
//!   One deliberate deviation: `serde_norway` **coerces** a YAML integer into
//!   a `String` field (`template_version: 69` loads as `"69"` rather than
//!   erroring). This *unifies* the `69` vs `"69"` type-flap instead of
//!   rejecting it — both spellings load identically — so the plan's concern
//!   (silent divergence) cannot occur; flagged in the phase report.
//! - JSON input (D1 "one loader reads both surfaces"): JSON is a YAML 1.2
//!   subset, so [`parse_str`] handles both through the single YAML parser.
//! - Emission: `serde_norway::to_string` quotes any scalar its own parser
//!   would not read back as a string (`'69'`, `'true'`, `'1.5'`); it leaves
//!   `yes` unquoted because under YAML 1.2 that IS a string. Round-trips
//!   through this loader are exact; a third-party YAML **1.1** consumer would
//!   misread an unquoted `yes` — a documented caveat, not a loader bug.
//!
//! ## Normalization (§4.1, §8.1.7)
//! [`normalized_model`] produces the canonical JSON object the content hash is
//! minted over and the round-trip comparator compares. It is *semantic*:
//! anchors are already gone (parse-time), map keys are emitted through
//! `serde_json`'s sorted map, sibling lists whose order carries no meaning
//! (classes, parameters, units, sets, type members…) are sorted by folded
//! name/key, value-list members keep their order (ordinal is semantic), and
//! literals are unified under their declared kind (`1` and `1.0` normalize
//! identically under `kind: float`). Fields with no persisted store carrier —
//! the identity block's `description`/`engine_schema`/`role`, unit registry
//! extras (D7 "dropped at load"), and the whole `migrations:` block (chain
//! transport, not vocabulary state) — are **excluded**, so the hash is stable
//! across chain-history edits and `template export` can reproduce it from the
//! database alone. Exclusions are named in the phase report.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::marker::PhantomData;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::encoding::content_hash as xxh3_hash;
use crate::error::DbError;
use crate::types::{fold, ScalarValue};

/// A duplicate-rejecting string-keyed map (review item: YAML mappings
/// deserialized into a plain `BTreeMap` silently last-win on duplicate keys —
/// an authoring footgun for `when:` branches, migration-step maps, and unit
/// registry extras). The visitor errors, NAMING the duplicated key. Order is
/// canonical (BTreeMap) as everywhere else in the normalized model.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UniqueMap<V>(pub BTreeMap<String, V>);

impl<V> UniqueMap<V> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<V> std::ops::Deref for UniqueMap<V> {
    type Target = BTreeMap<String, V>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V> std::ops::DerefMut for UniqueMap<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<V: Serialize> Serialize for UniqueMap<V> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for UniqueMap<V> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Vis<V>(PhantomData<V>);
        impl<'de, V: Deserialize<'de>> serde::de::Visitor<'de> for Vis<V> {
            type Value = UniqueMap<V>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a mapping with unique keys")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut m: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error;
                let mut out = BTreeMap::new();
                while let Some(k) = m.next_key::<String>()? {
                    let v = m.next_value::<V>()?;
                    if out.insert(k.clone(), v).is_some() {
                        return Err(A::Error::custom(format!(
                            "duplicate key {k:?} (each key may appear once)"
                        )));
                    }
                }
                Ok(UniqueMap(out))
            }
        }
        d.deserialize_map(Vis(PhantomData))
    }
}

/// Scalar `value_kind` tags legal in a `types:` member (the store's
/// `parameter_type.value_kind` vocabulary minus `indexed`, `api.rs`
/// `KNOWN_TAGS`).
pub const SCALAR_KINDS: [&str; 6] = ["float", "int", "bool", "string", "date_time", "duration"];

/// Scalar kinds a v1 template may use for *literals* (defaults, value-list
/// members). `date_time`/`duration` literals have no IR literal syntax yet
/// (would need an ISO-8601 grammar) — a loud load-time error, flagged in the
/// phase report; no acceptance source needs them (FlexTool/INES literals are
/// floats and strings).
pub const LITERAL_KINDS: [&str; 4] = ["float", "int", "bool", "string"];

/// Index-dimension kinds (`parameter_type_dimension.kind`, VMP Part 1).
pub const DIM_KINDS: [&str; 5] = ["datetime", "period", "label", "integer", "time_pattern"];

/// The store's name charset CHECK (`#2373`): classes, entities, parameters.
fn name_charset_ok(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Object model (template_ir_plan §2.2)
// ---------------------------------------------------------------------------

/// One template document. Top-level keys per §2.2 — all optional except
/// `template` and `classes`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub template: Identity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub units: Vec<Unit>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub value_lists: Vec<ValueList>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parameter_groups: Vec<ParameterGroup>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub class_sets: Vec<ClassSet>,
    pub classes: Vec<Class>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_rules: Option<AuditRules>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<Seed>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub migrations: Vec<Migration>,
}

/// The identity block (§1.1). `version` is a string by design (§4.1: stored as
/// a string; `69` coerces to `"69"` — see the module docs).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Engine-schema compatibility note (§1.1). Carried, compared against the
    /// DB's `schema_version` at apply (warning on mismatch, never a block).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_schema: Option<String>,
    /// `input` | `results` (§1.1, §7). No store carrier — excluded from the
    /// normalized model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

/// A unit declaration (D7): name + quantity-kind IRI + description are what
/// the store persists (`units_of_measure`). Any *other* key is registry
/// authoring/audit metadata — tolerated, surfaced as a load warning, and
/// dropped (never persisted; D7 "express-never-assert").
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Unit {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_kind: Option<String>,
    #[serde(flatten, skip_serializing_if = "UniqueMap::is_empty", default)]
    pub registry_extras: UniqueMap<Value>,
}

/// A value list (§1.1): named, ordered, typed members. Member order IS the
/// ordinal (1-based) — semantic, kept by normalization.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueList {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub members: Vec<ListMember>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListMember {
    pub value: Lit,
    /// Optional explicit kind; inferred from the literal when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A presentation group (D8): persisted inside the Decision-7 `presentation`
/// metadata document, zero schema change.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterGroup {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<i64>,
}

/// A class-set (Decision 5): a named closed union of classes, usable as a
/// dimension type. Members may be N-D classes of any shape (finding 1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClassSet {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub members: Vec<String>,
}

/// An entity class, 0-D (no `dimensions`) or N-D (§2.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Class {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Resolution semantics (`entity_class.active_by_default`); FlexTool
    /// fidelity: false on node/unit/connection only (§8.1.3).
    #[serde(default = "default_true")]
    pub active_by_default: bool,
    /// Completeness marker (`audit_required_class_empty`).
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_order: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dimensions: Vec<DimRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parameters: Vec<Parameter>,
}

/// A dimension referent (§2.2): a bare name ALWAYS resolves as a class; a
/// class-set is always explicit `{class_set: name}` (shadowing rule).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DimRef {
    Class(String),
    Set {
        class_set: String,
    },
}

impl DimRef {
    pub fn referent_name(&self) -> &str {
        match self {
            DimRef::Class(n) => n,
            DimRef::Set { class_set } => class_set,
        }
    }
    pub fn is_set(&self) -> bool {
        matches!(self, DimRef::Set { .. })
    }
}

/// A parameter definition (§2.2). `types` MAY be omitted with `value_list`
/// (kind inferred, §3.5) or with neither (loads typeless, materializes on
/// first write — §2.2, the INES `test`-class case).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<TypeDecl>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_list: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<DefaultValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_order: Option<i64>,
}

/// One member of the declared allowed-type set (D2/D3): a scalar kind, or
/// `indexed` with explicitly named-and-kinded dims. `rank` is never written —
/// derived as `dims.len()` (§3.1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeDecl {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dims: Option<Vec<DimSpec>>,
}

impl TypeDecl {
    /// The (value_kind tag, folded (name, kind) vector) the write path matches
    /// and distinguishes on (`api.rs` `MatchKey`, VMP Part 7 as amended).
    /// Folding is the crate-wide Unicode [`fold`] (DuckDB `lower()` parity);
    /// the write path's own matcher folds ASCII-only, so this load-time check
    /// is equal-or-STRICTER — a set it refuses would only ever differ on
    /// non-ASCII case aliases the SQL layer would collapse anyway.
    pub fn match_key(&self) -> (String, Vec<(String, String)>) {
        (
            self.kind.clone(),
            self.dims
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|d| (fold(&d.name), d.kind.clone()))
                .collect(),
        )
    }

    /// Human-readable one-liner for plans/errors.
    pub fn describe(&self) -> String {
        match self.dims.as_deref() {
            None | Some([]) => format!("{} rank 0", self.kind),
            Some(dims) => {
                let d: Vec<String> =
                    dims.iter().map(|d| format!("{}: {}", d.name, d.kind)).collect();
                format!("indexed rank {} ({})", dims.len(), d.join(", "))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DimSpec {
    pub name: String,
    pub kind: String,
}

/// A typed default literal, e.g. `{kind: float, value: 1.0}` (§2.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefaultValue {
    pub kind: String,
    pub value: Lit,
}

/// A scalar literal as parsed. Variant order matters for serde's untagged
/// resolution: bool, then int, then float, then string — under the YAML 1.2
/// core schema `yes` reaches `Str` (verified; see module docs).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Lit {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl Lit {
    /// The kind this literal infers when none is declared.
    pub fn inferred_kind(&self) -> &'static str {
        match self {
            Lit::Bool(_) => "bool",
            Lit::Int(_) => "int",
            Lit::Float(_) => "float",
            Lit::Str(_) => "string",
        }
    }

    /// Resolve against a declared kind (int coerces to float; everything else
    /// must match exactly). Returns the typed [`ScalarValue`] the write path
    /// consumes, or an error string.
    pub fn resolve(&self, kind: &str) -> Result<ScalarValue, String> {
        match (kind, self) {
            ("float", Lit::Float(f)) => Ok(ScalarValue::Float(*f)),
            ("float", Lit::Int(i)) => Ok(ScalarValue::Float(*i as f64)),
            ("int", Lit::Int(i)) => Ok(ScalarValue::Int(*i)),
            ("bool", Lit::Bool(b)) => Ok(ScalarValue::Bool(*b)),
            ("string", Lit::Str(s)) => Ok(ScalarValue::Str(s.clone())),
            (k, l) => Err(format!(
                "literal {l:?} does not instantiate declared kind {k:?} \
                 (quote string literals; YAML 1.2 resolves unquoted yes/no as strings already)"
            )),
        }
    }
}

/// Explicit conditional-requirement rules (§5.2, D4): carried as data,
/// client-enforced in v1, persisted as a JSON document under the reserved
/// `audit_rules` metadata key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditRules {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditional_requirements: Vec<ConditionalRequirement>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConditionalRequirement {
    pub class: String,
    /// A value-list parameter on the class whose value selects the branch.
    pub selector: String,
    /// method value -> required parameters (key order canonical; duplicate
    /// branch keys are a parse error, [`UniqueMap`]).
    pub when: UniqueMap<WhenClause>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhenClause {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub require: Vec<String>,
}

/// Vocabulary-completing seed scaffolding (§1.2, D6): entities and base
/// (structural, alternative-NULL) memberships ONLY. Bounded by intent — model
/// content never belongs here (a review rule, not a mechanism).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<SeedEntity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memberships: Vec<SeedMembership>,
}

/// A seed entity. 0-D: `name` required. N-D: `elements` (entity names in
/// declared-dimension order) required; `name` optional (defaults to the
/// elements joined with `__`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedEntity {
    pub class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elements: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl SeedEntity {
    /// The effective entity name: explicit, else the joined element tuple.
    pub fn effective_name(&self) -> String {
        match (&self.name, &self.elements) {
            (Some(n), _) => n.clone(),
            (None, Some(els)) => els.join("__"),
            (None, None) => String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedMembership {
    pub group: EntityRef,
    pub member: EntityRef,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityRef {
    pub class: String,
    pub entity: String,
}

/// One migration span (§4.3): identity-changing steps between two template
/// versions, applied in `steps` order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Migration {
    pub from: String,
    pub to: String,
    pub steps: Vec<MigrationStep>,
}

/// The closed Layer-2 hint vocabulary plus the Layer-3 `run:` escape hatch
/// (§4.2, D5). Serialized as a ONE-KEY MAP per step (`- rename_class: {from,
/// to}` / `- run: prog`) — the plan's §4.3 authoring syntax. Hand-written
/// serde impls below: the serde_yaml-0.9 family represents derived enums as
/// YAML `!tags`, which is not the decided authoring surface.
#[derive(Clone, Debug, PartialEq)]
pub enum MigrationStep {
    RenameClass {
        from: String,
        to: String,
    },
    RenameParameter {
        class: String,
        from: String,
        to: String,
    },
    RenameValueList {
        from: String,
        to: String,
    },
    /// The one data-touching hint: renames a (string) list member and, with
    /// `rewrite_values`, rewrites matching current scalar values and defaults
    /// of definitions bound to the list (mechanical, value-list-scoped).
    RenameListMember {
        list: String,
        from: String,
        to: String,
        rewrite_values: bool,
    },
    /// Re-home a definition between classes. v1 guard: legal only while the
    /// definition holds no current values (the "carry 1:1" case is deferred
    /// with the reshape door).
    MoveParameter {
        name: String,
        from_class: String,
        to_class: String,
    },
    /// Layer 3: a named tool-side program executed through the ordinary write
    /// API (`MigrationRegistry`). The IR records identity/ordering only.
    Run(String),
}

/// The serde mirror of [`MigrationStep`] (remote derive). The derived
/// externally-tagged representation IS the one-key map — under `serde_json`;
/// the serde_yaml-0.9 family would render it as a YAML `!tag`, so both impls
/// below detour through `serde_json::Value` (the object model is
/// JSON-compatible by design, D1). Deserialization goes through [`UniqueMap`]
/// first for the duplicate-key rejection and the one-key contract.
#[derive(Serialize, Deserialize)]
#[serde(remote = "MigrationStep", rename_all = "snake_case", deny_unknown_fields)]
enum StepDef {
    RenameClass {
        from: String,
        to: String,
    },
    RenameParameter {
        class: String,
        from: String,
        to: String,
    },
    RenameValueList {
        from: String,
        to: String,
    },
    RenameListMember {
        list: String,
        from: String,
        to: String,
        #[serde(default)]
        rewrite_values: bool,
    },
    MoveParameter {
        name: String,
        from_class: String,
        to_class: String,
    },
    Run(String),
}

impl Serialize for MigrationStep {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "StepDef")] &'a MigrationStep);
        let v = serde_json::to_value(Helper(self)).map_err(serde::ser::Error::custom)?;
        v.serialize(s)
    }
}

impl<'de> Deserialize<'de> for MigrationStep {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let m = UniqueMap::<Value>::deserialize(d)?;
        if m.0.len() != 1 {
            return Err(D::Error::custom(format!(
                "a migration step is a ONE-key map (rename_class / rename_parameter / \
                 rename_value_list / rename_list_member / move_parameter / run), got {} keys",
                m.0.len()
            )));
        }
        let obj: Map<String, Value> = m.0.into_iter().collect();
        StepDef::deserialize(Value::Object(obj)).map_err(D::Error::custom)
    }
}

impl MigrationStep {
    /// One-line rendering for plans and commit messages.
    pub fn describe(&self) -> String {
        match self {
            MigrationStep::RenameClass { from, to } => format!("rename_class {from} -> {to}"),
            MigrationStep::RenameParameter { class, from, to } => {
                format!("rename_parameter {class}.{from} -> {to}")
            }
            MigrationStep::RenameValueList { from, to } => {
                format!("rename_value_list {from} -> {to}")
            }
            MigrationStep::RenameListMember { list, from, to, rewrite_values } => format!(
                "rename_list_member {list}: {from:?} -> {to:?}{}",
                if *rewrite_values { " (rewriting stored values)" } else { "" }
            ),
            MigrationStep::MoveParameter { name, from_class, to_class } => {
                format!("move_parameter {from_class}.{name} -> {to_class}.{name}")
            }
            MigrationStep::Run(p) => format!("run {p} (imperative escape hatch)"),
        }
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse a template from YAML 1.2 text. JSON is a YAML subset, so JSON input
/// parses through the same call (D1: one loader, both surfaces).
pub fn parse_str(text: &str) -> Result<Template, DbError> {
    serde_norway::from_str(text)
        .map_err(|e| DbError::Api(format!("template parse error: {e}")))
}

/// Parse a template file (YAML or JSON — one parser, D1).
pub fn parse_file(path: &Path) -> Result<Template, DbError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| DbError::Api(format!("cannot read {}: {e}", path.display())))?;
    parse_str(&text)
}

/// Emit YAML (authoring surface). See the module docs for quoting semantics.
pub fn to_yaml(t: &Template) -> Result<String, DbError> {
    serde_norway::to_string(t).map_err(|e| DbError::Api(format!("template emit error: {e}")))
}

/// Emit pretty JSON (the machine serialization / compiler target).
pub fn to_json(t: &Template) -> Result<String, DbError> {
    serde_json::to_string_pretty(t).map_err(|e| DbError::Api(format!("template emit error: {e}")))
}

// ---------------------------------------------------------------------------
// Load-time validation (pure; §2.2 "load-time error, caught before any write")
// ---------------------------------------------------------------------------

/// The validated template plus derived data the loader/reconciler reuse.
pub struct Checked<'a> {
    pub template: &'a Template,
    /// Load-time warnings (shadowing, dropped unit registry fields, …).
    pub warnings: Vec<String>,
    /// folded class name -> index into `template.classes`.
    pub class_ix: HashMap<String, usize>,
    /// folded set name -> index into `template.class_sets`.
    pub set_ix: HashMap<String, usize>,
    /// Per parameter (class_ix, param_ix): the effective declared type set —
    /// explicit `types`, else the value-list-inferred rank-0 member, else
    /// empty (typeless).
    pub effective_types: HashMap<(usize, usize), Vec<TypeDecl>>,
    /// The `template_content_hash` (§4.1), computed ONCE here and carried so
    /// plan/apply never recompute it.
    pub hash: String,
}

/// Validate a parsed template: name rules and per-kind uniqueness (§2.2),
/// dangling in-template references, type-set distinguishability (§3.3), the
/// D3 dim rules, literal kinds, default-vs-list membership (§3.3.4), seed
/// shape/arity/element checks and the cross-member name-dup rule (D6), and
/// audit-rule references (§5.2). All errors are collected and reported
/// together; warnings ride the returned [`Checked`].
pub fn validate(t: &Template) -> Result<Checked<'_>, DbError> {
    let mut errs: Vec<String> = Vec::new();
    let mut warns: Vec<String> = Vec::new();

    if t.template.name.trim().is_empty() {
        errs.push("template.name must be non-empty".into());
    }
    if t.template.version.trim().is_empty() {
        errs.push("template.version must be non-empty".into());
    }
    if let Some(role) = &t.template.role {
        if role != "input" && role != "results" {
            errs.push(format!("template.role must be \"input\" or \"results\", got {role:?}"));
        }
    }

    // --- per-kind name indexes + uniqueness (store scoping, §2.2) ---
    let mut class_ix: HashMap<String, usize> = HashMap::new();
    for (i, c) in t.classes.iter().enumerate() {
        if !name_charset_ok(&c.name) {
            errs.push(format!("class {:?}: name must match [A-Za-z0-9_]+", c.name));
        }
        if class_ix.insert(fold(&c.name), i).is_some() {
            errs.push(format!("duplicate class name {:?} (case-insensitive)", c.name));
        }
    }
    let mut set_ix: HashMap<String, usize> = HashMap::new();
    for (i, s) in t.class_sets.iter().enumerate() {
        if !name_charset_ok(&s.name) {
            errs.push(format!("class_set {:?}: name must match [A-Za-z0-9_]+", s.name));
        }
        if set_ix.insert(fold(&s.name), i).is_some() {
            errs.push(format!("duplicate class_set name {:?} (case-insensitive)", s.name));
        }
        if s.members.is_empty() {
            errs.push(format!("class_set {:?}: members must be non-empty", s.name));
        }
        let mut seen = HashSet::new();
        for m in &s.members {
            if !class_ix.contains_key(&fold(m)) {
                errs.push(format!("class_set {:?}: member {m:?} is not a declared class", s.name));
            }
            if !seen.insert(fold(m)) {
                errs.push(format!("class_set {:?}: duplicate member {m:?}", s.name));
            }
        }
        // A set MAY shadow a class name (separate tables) — loader warns (§2.2).
        if class_ix.contains_key(&fold(&s.name)) {
            warns.push(format!(
                "class_set {:?} shadows a class of the same name; in dimensions a bare \
                 name resolves as the CLASS — write {{class_set: {}}} to mean the set",
                s.name, s.name
            ));
        }
    }
    let mut list_ix: HashMap<String, usize> = HashMap::new();
    for (i, l) in t.value_lists.iter().enumerate() {
        if l.name.trim().is_empty() {
            errs.push("value_list with empty name".into());
        }
        if list_ix.insert(fold(&l.name), i).is_some() {
            errs.push(format!("duplicate value_list name {:?} (case-insensitive)", l.name));
        }
        let mut seen: HashSet<(String, String)> = HashSet::new();
        for (mi, m) in l.members.iter().enumerate() {
            let kind = match effective_member_kind(m) {
                Ok(k) => k,
                Err(e) => {
                    errs.push(format!("value_list {:?} member {}: {e}", l.name, mi + 1));
                    continue;
                }
            };
            if let Err(e) = m.value.resolve(&kind) {
                errs.push(format!("value_list {:?} member {}: {e}", l.name, mi + 1));
            }
            if let Lit::Float(f) = &m.value {
                if !f.is_finite() {
                    errs.push(format!(
                        "value_list {:?} member {}: non-finite float literal",
                        l.name,
                        mi + 1
                    ));
                }
            }
            let key = (kind.clone(), canonical_literal_string(&m.value, &kind));
            if !seen.insert(key) {
                errs.push(format!(
                    "value_list {:?}: duplicate member value {:?}",
                    l.name, m.value
                ));
            }
        }
    }
    let mut unit_ix: HashMap<String, usize> = HashMap::new();
    for (i, u) in t.units.iter().enumerate() {
        if u.name.trim().is_empty() {
            errs.push("unit with empty name".into());
        }
        if unit_ix.insert(fold(&u.name), i).is_some() {
            errs.push(format!("duplicate unit name {:?} (case-insensitive)", u.name));
        }
        if !u.registry_extras.is_empty() {
            let keys: Vec<&str> = u.registry_extras.keys().map(|k| k.as_str()).collect();
            warns.push(format!(
                "unit {:?}: registry fields [{}] are authoring/audit metadata — \
                 not persisted, dropped at load (D7)",
                u.name,
                keys.join(", ")
            ));
        }
    }
    let mut group_names: HashSet<String> = HashSet::new();
    for g in &t.parameter_groups {
        if g.name.trim().is_empty() {
            errs.push("parameter_group with empty name".into());
        }
        if !group_names.insert(fold(&g.name)) {
            errs.push(format!("duplicate parameter_group name {:?} (case-insensitive)", g.name));
        }
    }

    // --- classes: dimensions, parameters, types, defaults ---
    let mut effective_types: HashMap<(usize, usize), Vec<TypeDecl>> = HashMap::new();
    for (ci, c) in t.classes.iter().enumerate() {
        if c.dimensions.len() > 4 {
            errs.push(format!(
                "class {:?}: {} dimensions exceeds the rank cap of 4",
                c.name,
                c.dimensions.len()
            ));
        }
        for (di, d) in c.dimensions.iter().enumerate() {
            match d {
                DimRef::Class(n) => {
                    if !class_ix.contains_key(&fold(n)) {
                        errs.push(format!(
                            "class {:?} dimension {}: {n:?} is not a declared class \
                             (a bare name resolves as a class; write {{class_set: {n}}} for a set)",
                            c.name,
                            di + 1
                        ));
                    } else if set_ix.contains_key(&fold(n)) {
                        warns.push(format!(
                            "class {:?} dimension {}: bare name {n:?} also matches a class_set; \
                             it resolves as the CLASS (§2.2 shadowing rule)",
                            c.name,
                            di + 1
                        ));
                    }
                }
                DimRef::Set { class_set } => {
                    if !set_ix.contains_key(&fold(class_set)) {
                        errs.push(format!(
                            "class {:?} dimension {}: {class_set:?} is not a declared class_set",
                            c.name,
                            di + 1
                        ));
                    }
                }
            }
        }

        let mut pnames: HashSet<String> = HashSet::new();
        for (pi, p) in c.parameters.iter().enumerate() {
            let loc = format!("class {:?} parameter {:?}", c.name, p.name);
            if !name_charset_ok(&p.name) {
                errs.push(format!("{loc}: name must match [A-Za-z0-9_]+"));
            }
            if !pnames.insert(fold(&p.name)) {
                errs.push(format!("{loc}: duplicate parameter name (case-insensitive)"));
            }
            if let Some(g) = &p.group {
                if !group_names.contains(&fold(g)) {
                    errs.push(format!("{loc}: group {g:?} is not a declared parameter_group"));
                }
            }
            if let Some(u) = &p.unit {
                if !unit_ix.contains_key(&fold(u)) {
                    errs.push(format!("{loc}: unit {u:?} is not a declared unit"));
                }
            }
            let list = match &p.value_list {
                Some(l) => match list_ix.get(&fold(l)) {
                    Some(li) => Some(&t.value_lists[*li]),
                    None => {
                        errs.push(format!("{loc}: value_list {l:?} is not declared"));
                        None
                    }
                },
                None => None,
            };

            // Effective type set: explicit types, else inferred from the list.
            let mut eff: Vec<TypeDecl> = Vec::new();
            match &p.types {
                Some(types) => {
                    for (ti, ty) in types.iter().enumerate() {
                        let tloc = format!("{loc} types[{ti}]");
                        match ty.kind.as_str() {
                            "indexed" => match ty.dims.as_deref() {
                                None | Some([]) => errs.push(format!(
                                    "{tloc}: kind \"indexed\" requires a non-empty dims list (D3)"
                                )),
                                Some(dims) => {
                                    if dims.len() > 4 {
                                        errs.push(format!(
                                            "{tloc}: {} dims exceeds the rank cap of 4",
                                            dims.len()
                                        ));
                                    }
                                    let mut dn: HashSet<String> = HashSet::new();
                                    for d in dims {
                                        if d.name.trim().is_empty() {
                                            errs.push(format!("{tloc}: empty dimension name"));
                                        }
                                        if !DIM_KINDS.contains(&d.kind.as_str()) {
                                            errs.push(format!(
                                                "{tloc}: unknown dimension kind {:?} \
                                                 (expected one of {DIM_KINDS:?})",
                                                d.kind
                                            ));
                                        }
                                        if !dn.insert(d.name.to_ascii_lowercase()) {
                                            errs.push(format!(
                                                "{tloc}: duplicate dimension name {:?}",
                                                d.name
                                            ));
                                        }
                                    }
                                }
                            },
                            k if SCALAR_KINDS.contains(&k) => {
                                if ty.dims.as_deref().map(|d| !d.is_empty()).unwrap_or(false) {
                                    errs.push(format!(
                                        "{tloc}: scalar kind {k:?} must not declare dims \
                                         (use kind: indexed)"
                                    ));
                                }
                            }
                            other => errs.push(format!(
                                "{tloc}: unknown kind {other:?} (expected {SCALAR_KINDS:?} \
                                 or \"indexed\")"
                            )),
                        }
                        eff.push(ty.clone());
                    }
                }
                None => {
                    if let Some(l) = list {
                        // §3.5: infer the scalar kind from the list members.
                        match uniform_list_kind(l) {
                            Ok(k) => eff.push(TypeDecl { kind: k, dims: None }),
                            Err(e) => errs.push(format!("{loc}: {e}")),
                        }
                    }
                    // Neither types nor list: legal, loads typeless (§2.2).
                }
            }

            // Explicit types + value_list must be mutually consistent (review
            // item 8 — the check the inference branch gets by construction):
            // every rank-0 member's kind must be a kind the list's members can
            // instantiate, else list-bound scalar values could never satisfy
            // both constraints at once.
            if p.types.is_some() {
                if let Some(l) = list {
                    let list_kinds: HashSet<String> = l
                        .members
                        .iter()
                        .filter_map(|m| effective_member_kind(m).ok())
                        .collect();
                    for m in &eff {
                        let rank0 =
                            m.dims.as_deref().map(|d| d.is_empty()).unwrap_or(true);
                        if rank0 && !list_kinds.contains(&m.kind) {
                            errs.push(format!(
                                "{loc}: declared rank-0 type kind {:?} has no member of \
                                 that kind in value_list {:?} (kinds present: {:?})",
                                m.kind,
                                l.name,
                                {
                                    let mut ks: Vec<&String> = list_kinds.iter().collect();
                                    ks.sort();
                                    ks
                                }
                            ));
                        }
                    }
                }
            }

            // Pairwise distinguishability (§3.3: refused at load; core re-checks).
            let keys: Vec<_> = eff.iter().map(|d| d.match_key()).collect();
            for a in 0..keys.len() {
                for b in (a + 1)..keys.len() {
                    if keys[a] == keys[b] {
                        errs.push(format!(
                            "{loc}: declared type members {} and {} are indistinguishable \
                             ({}) — exactly-one-member matching would be impossible (D2a)",
                            a, b, eff[a].describe()
                        ));
                    }
                }
            }

            // Default literal checks (kind, member instantiation, list membership).
            if let Some(dv) = &p.default {
                if !LITERAL_KINDS.contains(&dv.kind.as_str()) {
                    errs.push(format!(
                        "{loc}: default kind {:?} is not supported as a v1 literal \
                         (supported: {LITERAL_KINDS:?})",
                        dv.kind
                    ));
                } else {
                    if let Lit::Float(f) = &dv.value {
                        if !f.is_finite() {
                            errs.push(format!("{loc}: non-finite float default"));
                        }
                    }
                    match dv.value.resolve(&dv.kind) {
                        Err(e) => errs.push(format!("{loc}: default {e}")),
                        Ok(_) => {
                            // The default must instantiate a declared rank-0 member
                            // (a value the write path itself would accept).
                            if !eff.is_empty()
                                && !eff.iter().any(|m| {
                                    m.kind == dv.kind
                                        && m.dims.as_deref().map(|d| d.is_empty()).unwrap_or(true)
                                })
                            {
                                errs.push(format!(
                                    "{loc}: default kind {:?} instantiates no declared rank-0 \
                                     type member",
                                    dv.kind
                                ));
                            }
                            if eff.is_empty() {
                                errs.push(format!(
                                    "{loc}: a default on a typeless parameter is not \
                                     representable (declare types or a value_list first)"
                                ));
                            }
                            // Default-vs-list (§3.3.4: fail fast at load).
                            if let Some(l) = list {
                                let dvs = canonical_literal_string(&dv.value, &dv.kind);
                                let found = l.members.iter().any(|m| {
                                    effective_member_kind(m)
                                        .map(|k| {
                                            k == dv.kind
                                                && canonical_literal_string(&m.value, &k) == dvs
                                        })
                                        .unwrap_or(false)
                                });
                                if !found {
                                    errs.push(format!(
                                        "{loc}: default {:?} is not a member of value_list {:?}",
                                        dv.value, l.name
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            effective_types.insert((ci, pi), eff);
        }
    }

    // --- audit rules (§5.2) ---
    if let Some(rules) = &t.audit_rules {
        for (ri, r) in rules.conditional_requirements.iter().enumerate() {
            let loc = format!("audit_rules.conditional_requirements[{ri}]");
            match class_ix.get(&fold(&r.class)) {
                None => errs.push(format!("{loc}: class {:?} is not declared", r.class)),
                Some(&ci) => {
                    let c = &t.classes[ci];
                    let sel =
                        c.parameters.iter().position(|p| fold(&p.name) == fold(&r.selector));
                    match sel {
                        None => errs.push(format!(
                            "{loc}: selector {:?} is not a parameter of class {:?}",
                            r.selector, r.class
                        )),
                        Some(pi) => {
                            let p = &c.parameters[pi];
                            match &p.value_list {
                                None => errs.push(format!(
                                    "{loc}: selector {:?} must be a value-list parameter",
                                    r.selector
                                )),
                                Some(ln) => {
                                    if let Some(&li) = list_ix.get(&fold(ln)) {
                                        let l = &t.value_lists[li];
                                        for k in r.when.keys() {
                                            let hit = l.members.iter().any(|m| {
                                                matches!(&m.value, Lit::Str(s) if s == k)
                                            });
                                            if !hit {
                                                errs.push(format!(
                                                    "{loc}: when-branch {k:?} is not a member \
                                                     of value_list {ln:?}"
                                                ));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (k, w) in r.when.iter() {
                        for req in &w.require {
                            if !c.parameters.iter().any(|p| fold(&p.name) == fold(req)) {
                                errs.push(format!(
                                    "{loc}: when {k:?} requires {req:?}, which is not a \
                                     parameter of class {:?}",
                                    r.class
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    // --- seed scaffolding (D6) ---
    if let Some(seed) = &t.seed {
        // (folded class, folded name) -> declared, for element/membership refs.
        let mut seeded: HashSet<(String, String)> = HashSet::new();
        for (si, e) in seed.entities.iter().enumerate() {
            let loc = format!("seed.entities[{si}] (class {:?})", e.class);
            let Some(&ci) = class_ix.get(&fold(&e.class)) else {
                errs.push(format!("{loc}: class is not declared"));
                continue;
            };
            let c = &t.classes[ci];
            let name = e.effective_name();
            if name.is_empty() {
                errs.push(format!("{loc}: a 0-D seed entity needs a name"));
                continue;
            }
            if !name_charset_ok(&name) {
                errs.push(format!("{loc}: entity name {name:?} must match [A-Za-z0-9_]+"));
            }
            if !seeded.insert((fold(&e.class), fold(&name))) {
                errs.push(format!("{loc}: duplicate seed entity {name:?} (case-insensitive)"));
            }
            match (&e.elements, c.dimensions.is_empty()) {
                (Some(_), true) => {
                    errs.push(format!("{loc}: elements given but class is 0-D"))
                }
                (None, false) => errs.push(format!(
                    "{loc}: class is {}-D; seed entity needs an elements tuple",
                    c.dimensions.len()
                )),
                (Some(els), false) => {
                    if els.len() != c.dimensions.len() {
                        errs.push(format!(
                            "{loc}: {} elements for a {}-D class",
                            els.len(),
                            c.dimensions.len()
                        ));
                    }
                }
                (None, true) => {}
            }
        }
        // Element references: each must name a declared seed entity of a class
        // acceptable at its dimension position (D6 closure: templates cannot
        // reference user data).
        for (si, e) in seed.entities.iter().enumerate() {
            let (Some(els), Some(&ci)) = (&e.elements, class_ix.get(&fold(&e.class))) else {
                continue;
            };
            let c = &t.classes[ci];
            for (di, el) in els.iter().enumerate() {
                let Some(dim) = c.dimensions.get(di) else { continue };
                let ok = match dim {
                    DimRef::Class(cls) => seeded.contains(&(fold(cls), fold(el))),
                    DimRef::Set { class_set } => set_ix
                        .get(&fold(class_set))
                        .map(|&sx| {
                            t.class_sets[sx]
                                .members
                                .iter()
                                .any(|m| seeded.contains(&(fold(m), fold(el))))
                        })
                        .unwrap_or(false),
                };
                if !ok {
                    errs.push(format!(
                        "seed.entities[{si}] (class {:?}): element {el:?} at position {} does \
                         not name a declared seed entity of the dimension's class{}",
                        e.class,
                        di + 1,
                        if dim.is_set() { "-set members" } else { "" }
                    ));
                }
            }
        }
        // Cross-member entity-name uniqueness within each set
        // (audit_class_set_entity_name_dup, checked at load — §8.1.2).
        for s in &t.class_sets {
            let mut names: HashMap<String, String> = HashMap::new();
            for e in seed.entities.iter() {
                if s.members.iter().any(|m| fold(m) == fold(&e.class)) {
                    let n = fold(&e.effective_name());
                    if let Some(prev) = names.insert(n.clone(), e.class.clone()) {
                        errs.push(format!(
                            "class_set {:?}: seed entity name {n:?} appears in members \
                             {prev:?} and {:?} (set-scoped names must be unique)",
                            s.name, e.class
                        ));
                    }
                }
            }
        }
        for (mi, m) in seed.memberships.iter().enumerate() {
            for (what, r) in [("group", &m.group), ("member", &m.member)] {
                if !seeded.contains(&(fold(&r.class), fold(&r.entity))) {
                    errs.push(format!(
                        "seed.memberships[{mi}]: {what} {}:{} does not name a declared \
                         seed entity",
                        r.class, r.entity
                    ));
                }
            }
        }
    }

    // --- migrations: shape checks only (targets are names in OLDER template
    // versions, unresolvable against this document) ---
    for (mi, m) in t.migrations.iter().enumerate() {
        if m.from == m.to {
            errs.push(format!("migrations[{mi}]: from == to ({:?})", m.from));
        }
        if m.steps.is_empty() {
            errs.push(format!("migrations[{mi}]: empty steps"));
        }
        for s in &m.steps {
            if let MigrationStep::Run(p) = s {
                if p.trim().is_empty() {
                    errs.push(format!("migrations[{mi}]: run step with empty program name"));
                }
            }
        }
    }
    let mut froms: HashSet<&str> = HashSet::new();
    for m in &t.migrations {
        if !froms.insert(m.from.as_str()) {
            errs.push(format!(
                "migrations: two spans start from version {:?} (the chain must be linear)",
                m.from
            ));
        }
    }

    if !errs.is_empty() {
        return Err(DbError::Api(format!(
            "template {:?} failed load-time validation:\n  - {}",
            t.template.name,
            errs.join("\n  - ")
        )));
    }
    Ok(Checked {
        template: t,
        warnings: warns,
        class_ix,
        set_ix,
        effective_types,
        hash: template_content_hash(t),
    })
}

/// The effective kind of a list member: explicit `kind` (v1 literal kinds
/// only; must match the literal) else inferred.
pub fn effective_member_kind(m: &ListMember) -> Result<String, String> {
    match &m.kind {
        Some(k) => {
            if !LITERAL_KINDS.contains(&k.as_str()) {
                return Err(format!(
                    "kind {k:?} is not supported as a v1 literal (supported: {LITERAL_KINDS:?})"
                ));
            }
            Ok(k.clone())
        }
        None => Ok(m.value.inferred_kind().to_string()),
    }
}

/// §3.5 inference: a value-list-typed parameter with no explicit `types` takes
/// the single scalar kind shared by every list member.
fn uniform_list_kind(l: &ValueList) -> Result<String, String> {
    let mut kind: Option<String> = None;
    for m in &l.members {
        let k = effective_member_kind(m)?;
        match &kind {
            None => kind = Some(k),
            Some(prev) if *prev != k => {
                return Err(format!(
                    "cannot infer a kind from value_list {:?}: members mix kinds \
                     {prev:?} and {k:?} (declare types: explicitly)",
                    l.name
                ))
            }
            _ => {}
        }
    }
    kind.ok_or_else(|| {
        format!("cannot infer a kind from empty value_list {:?} (declare types:)", l.name)
    })
}

/// A canonical string for literal equality (kind-aware: `1` == `1.0` under
/// float; used for duplicate detection and default-vs-list membership).
/// Delegates to the crate-wide [`ScalarValue::identity_string`] — ONE scalar
/// identity encoding shared with the reconcile diff and list matching.
pub fn canonical_literal_string(l: &Lit, kind: &str) -> String {
    match l.resolve(kind) {
        Ok(v) => v.identity_string(),
        Err(_) => format!("?:{l:?}"),
    }
}

// ---------------------------------------------------------------------------
// Normalized object model + content hash (§4.1)
// ---------------------------------------------------------------------------

fn lit_value_json(l: &Lit, kind: &str) -> Value {
    match l.resolve(kind) {
        Ok(ScalarValue::Float(f)) => json!(f),
        Ok(ScalarValue::Int(i)) => json!(i),
        Ok(ScalarValue::Bool(b)) => json!(b),
        Ok(ScalarValue::Str(s)) => json!(s),
        _ => Value::Null, // unreachable post-validation
    }
}

fn opt(map: &mut Map<String, Value>, key: &str, v: &Option<String>) {
    if let Some(s) = v {
        map.insert(key.into(), json!(s));
    }
}

/// THE type-elision rule, shared by normalization and `template export`
/// (review item 11 — one rule, two users, so a §3.5-style explicit override
/// that inference would reproduce hashes identically whether written or
/// elided): a `types:` block is elidable iff the parameter binds a value
/// list, the block is a single rank-0 member, and its kind equals the list's
/// uniform member kind.
pub fn types_elidable(types: &[TypeDecl], list: Option<&ValueList>) -> bool {
    let Some(l) = list else { return false };
    types.len() == 1
        && types[0].dims.as_deref().map(|d| d.is_empty()).unwrap_or(true)
        && uniform_list_kind(l).map(|k| k == types[0].kind).unwrap_or(false)
}

fn type_member_json(ty: &TypeDecl) -> Value {
    let mut m = Map::new();
    m.insert("kind".into(), json!(ty.kind));
    if let Some(dims) = ty.dims.as_deref() {
        if !dims.is_empty() {
            let dv: Vec<Value> = dims
                .iter()
                .map(|d| json!({"name": d.name, "kind": d.kind}))
                .collect();
            m.insert("dims".into(), Value::Array(dv));
        }
    }
    Value::Object(m)
}

/// The canonical, JSON-valued normalized object model (module docs). This is
/// the hash input AND the round-trip comparator (§8.1.7): parse both sides,
/// compare `normalized_model` outputs.
pub fn normalized_model(t: &Template) -> Value {
    let mut root = Map::new();
    let lists_by_name: HashMap<String, &ValueList> =
        t.value_lists.iter().map(|l| (fold(&l.name), l)).collect();
    root.insert(
        "template".into(),
        json!({"name": t.template.name, "version": t.template.version}),
    );

    // units, sorted by folded name; registry extras excluded (D7).
    let mut units: Vec<&Unit> = t.units.iter().collect();
    units.sort_by_key(|u| fold(&u.name));
    if !units.is_empty() {
        let arr: Vec<Value> = units
            .iter()
            .map(|u| {
                let mut m = Map::new();
                m.insert("name".into(), json!(u.name));
                opt(&mut m, "description", &u.description);
                opt(&mut m, "quantity_kind", &u.quantity_kind);
                Value::Object(m)
            })
            .collect();
        root.insert("units".into(), Value::Array(arr));
    }

    // value lists sorted by name; members in ordinal order.
    let mut lists: Vec<&ValueList> = t.value_lists.iter().collect();
    lists.sort_by_key(|l| fold(&l.name));
    if !lists.is_empty() {
        let arr: Vec<Value> = lists
            .iter()
            .map(|l| {
                let members: Vec<Value> = l
                    .members
                    .iter()
                    .map(|m| {
                        let kind = effective_member_kind(m).unwrap_or_else(|_| "?".into());
                        let mut mm = Map::new();
                        mm.insert("kind".into(), json!(kind));
                        mm.insert("value".into(), lit_value_json(&m.value, &kind));
                        opt(&mut mm, "description", &m.description);
                        Value::Object(mm)
                    })
                    .collect();
                let mut m = Map::new();
                m.insert("name".into(), json!(l.name));
                opt(&mut m, "description", &l.description);
                m.insert("members".into(), Value::Array(members));
                Value::Object(m)
            })
            .collect();
        root.insert("value_lists".into(), Value::Array(arr));
    }

    // parameter groups sorted by folded name.
    let mut groups: Vec<&ParameterGroup> = t.parameter_groups.iter().collect();
    groups.sort_by_key(|g| fold(&g.name));
    if !groups.is_empty() {
        let arr: Vec<Value> = groups
            .iter()
            .map(|g| {
                let mut m = Map::new();
                m.insert("name".into(), json!(g.name));
                opt(&mut m, "color", &g.color);
                if let Some(o) = g.order {
                    m.insert("order".into(), json!(o));
                }
                Value::Object(m)
            })
            .collect();
        root.insert("parameter_groups".into(), Value::Array(arr));
    }

    // class sets sorted by name, members sorted.
    let mut sets: Vec<&ClassSet> = t.class_sets.iter().collect();
    sets.sort_by_key(|s| fold(&s.name));
    if !sets.is_empty() {
        let arr: Vec<Value> = sets
            .iter()
            .map(|s| {
                let mut members = s.members.clone();
                members.sort_by_key(|m| fold(m));
                let mut m = Map::new();
                m.insert("name".into(), json!(s.name));
                opt(&mut m, "description", &s.description);
                m.insert("members".into(), json!(members));
                Value::Object(m)
            })
            .collect();
        root.insert("class_sets".into(), Value::Array(arr));
    }

    // classes sorted by name; parameters sorted by name; type members sorted
    // by match key (the types: list order carries no semantics, §3.1).
    let mut classes: Vec<&Class> = t.classes.iter().collect();
    classes.sort_by_key(|c| fold(&c.name));
    let carr: Vec<Value> = classes
        .iter()
        .map(|c| {
            let mut m = Map::new();
            m.insert("name".into(), json!(c.name));
            opt(&mut m, "description", &c.description);
            m.insert("active_by_default".into(), json!(c.active_by_default));
            m.insert("required".into(), json!(c.required));
            if let Some(o) = c.display_order {
                m.insert("display_order".into(), json!(o));
            }
            if !c.dimensions.is_empty() {
                let dims: Vec<Value> = c
                    .dimensions
                    .iter()
                    .map(|d| match d {
                        DimRef::Class(n) => json!(n),
                        DimRef::Set { class_set } => json!({ "class_set": class_set }),
                    })
                    .collect();
                m.insert("dimensions".into(), Value::Array(dims));
            }
            let mut params: Vec<&Parameter> = c.parameters.iter().collect();
            params.sort_by_key(|p| fold(&p.name));
            if !params.is_empty() {
                let parr: Vec<Value> = params
                    .iter()
                    .map(|p| {
                        let mut pm = Map::new();
                        pm.insert("name".into(), json!(p.name));
                        opt(&mut pm, "description", &p.description);
                        // Explicit types normalize through the SAME elision
                        // rule export applies (types_elidable): an explicit
                        // block inference would reproduce is omitted, so
                        // source and export hash identically either way.
                        let bound_list = p
                            .value_list
                            .as_deref()
                            .and_then(|n| lists_by_name.get(&fold(n)).copied());
                        if let Some(types) = &p.types {
                            if !types_elidable(types, bound_list) {
                                let mut sorted: Vec<&TypeDecl> = types.iter().collect();
                                sorted.sort_by_key(|ty| ty.match_key());
                                pm.insert(
                                    "types".into(),
                                    Value::Array(
                                        sorted.iter().map(|ty| type_member_json(ty)).collect(),
                                    ),
                                );
                            }
                        }
                        opt(&mut pm, "value_list", &p.value_list);
                        if let Some(dv) = &p.default {
                            pm.insert(
                                "default".into(),
                                json!({"kind": dv.kind,
                                       "value": lit_value_json(&dv.value, &dv.kind)}),
                            );
                        }
                        opt(&mut pm, "unit", &p.unit);
                        opt(&mut pm, "group", &p.group);
                        pm.insert("required".into(), json!(p.required));
                        if let Some(o) = p.display_order {
                            pm.insert("display_order".into(), json!(o));
                        }
                        Value::Object(pm)
                    })
                    .collect();
                m.insert("parameters".into(), Value::Array(parr));
            }
            Value::Object(m)
        })
        .collect();
    root.insert("classes".into(), Value::Array(carr));

    // audit rules (shared with the reconcile diff and the persisted D4 doc).
    if let Some(rules) = normalized_audit_rules(t) {
        root.insert("audit_rules".into(), rules);
    }

    // seed: entities sorted by (class, effective name); memberships sorted.
    if let Some(seed) = &t.seed {
        let mut sm = Map::new();
        if !seed.entities.is_empty() {
            let mut es: Vec<&SeedEntity> = seed.entities.iter().collect();
            es.sort_by_key(|e| (fold(&e.class), fold(&e.effective_name())));
            let arr: Vec<Value> = es
                .iter()
                .map(|e| {
                    let mut m = Map::new();
                    m.insert("class".into(), json!(e.class));
                    m.insert("name".into(), json!(e.effective_name()));
                    if let Some(els) = &e.elements {
                        m.insert("elements".into(), json!(els));
                    }
                    opt(&mut m, "description", &e.description);
                    Value::Object(m)
                })
                .collect();
            sm.insert("entities".into(), Value::Array(arr));
        }
        if !seed.memberships.is_empty() {
            let mut ms: Vec<&SeedMembership> = seed.memberships.iter().collect();
            ms.sort_by_key(|m| {
                (fold(&m.group.class), fold(&m.group.entity), fold(&m.member.class),
                 fold(&m.member.entity))
            });
            let arr: Vec<Value> = ms
                .iter()
                .map(|m| {
                    json!({"group": {"class": m.group.class, "entity": m.group.entity},
                           "member": {"class": m.member.class, "entity": m.member.entity}})
                })
                .collect();
            sm.insert("memberships".into(), Value::Array(arr));
        }
        if !sm.is_empty() {
            root.insert("seed".into(), Value::Object(sm));
        }
    }
    // migrations: EXCLUDED (module docs).
    Value::Object(root)
}

/// The canonical JSON form of the `audit_rules:` block (D4): rules sorted by
/// (class, selector), `when` keys canonical (BTreeMap), `require` lists
/// sorted. This exact value is persisted under the reserved `audit_rules`
/// metadata key and compared by the reconcile diff — one normalization, three
/// uses (hash, storage, diff), so they cannot drift.
pub fn normalized_audit_rules(t: &Template) -> Option<Value> {
    let rules = t.audit_rules.as_ref()?;
    if rules.conditional_requirements.is_empty() {
        return None;
    }
    let mut rs: Vec<&ConditionalRequirement> = rules.conditional_requirements.iter().collect();
    rs.sort_by_key(|r| (fold(&r.class), fold(&r.selector)));
    let arr: Vec<Value> = rs
        .iter()
        .map(|r| {
            let when: Map<String, Value> = r
                .when
                .iter()
                .map(|(k, w)| {
                    let mut req = w.require.clone();
                    req.sort_by_key(|s| fold(s));
                    (k.clone(), json!({ "require": req }))
                })
                .collect();
            json!({"class": r.class, "selector": r.selector, "when": Value::Object(when)})
        })
        .collect();
    Some(json!({ "conditional_requirements": Value::Array(arr) }))
}

/// The `template_content_hash` value (§4.1): XXH3-64 (the project's one
/// content-hash function, `encoding::content_hash`) over the serialized
/// normalized object model, as 16 lowercase hex digits.
pub fn template_content_hash(t: &Template) -> String {
    let json = serde_json::to_string(&normalized_model(t))
        .expect("normalized model is always serializable");
    format!("{:016x}", xxh3_hash(json.as_bytes()))
}
