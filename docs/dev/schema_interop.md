# Schema interop — external domain schemas and nodal-db

> **Role of this file:** how nodal-db maps, imports from, and exports to
> the external domain schemas and tools around it — CESM, SiennaSchemas,
> INES, and IRENA FlexTool — and the decided build order. Distilled from
> the local design record after review (2026-09-18); the working record
> (deep-dive findings, pointers) is `specs/schema_interop_FINDINGS.md`,
> and changes here flow through that record's review gate. Note the
> split: **`specs/`** is local working memory (gitignored) — pointers to
> it resolve only on machines carrying the local record — while `spec/`
> and `docs/` are committed. "Decision N" references are to the decision
> log in `specs/schema_draft_plan.md` Part 3.
> **Last substantial update: 2026-09-18.**

## 1. Background: where domain schemas sit in nodal-db

nodal-db is domain-agnostic: "node", "unit", "bus", "generator" are not
DDL — they are entity-class and parameter-definition *rows*, loaded from a
template at DB init. Adopting an external domain schema therefore means
**compiling it into an init template** (classes, dimensions, parameter
definitions, defaults, units, validation rules as audit views), not
changing the store. Three candidate schemas were assessed:

- **CESM** (`G-PST/CommonEnergySystemModel`): a LinkML schema
  (`model/cesm_v0.1.0.yaml`, 13 concrete classes) for zonal multi-energy
  *planning* models (capacity expansion + dispatch), with per-tool
  transformers (FlexTool, GridDB, Spine DB) around a DuckDB hub.
- **SiennaSchemas** (`Sienna-Platform/SiennaSchemas`): hand-written JSON
  Schema, 183 component types, the declared PowerSystems.jl v6 data model
  for power-system *operations* (plus investments, market, nascent
  dynamics), with generated Pydantic/Julia/SQL artifacts and a
  machine-readable unit registry (`Core/units.json`).
- **INES** (`ines-tools/ines-spec`): a Spine-Toolbox-native
  interoperability spec for energy system models — CESM's predecessor/
  sibling (CESM's README records its roots in ines-spec). Surveyed more
  lightly; see §7.

The scopes are complementary (multi-energy planning breadth vs
power-system operations depth); the mapping approach below is the same for
both.

A fourth artifact class sits alongside the standards: **tool-native
vocabularies**. A modelling tool (IRENA FlexTool, §8) carries its own
class/parameter vocabulary designed from the user's perspective; it does
not conform exactly to any of the standards, though it is close to CESM
and INES in many aspects. For nodal-db a tool template is authored and
versioned directly rather than compiled from a standard, and the
standards become *import/export paths* into it.

**Role taxonomy (decided 2026-09-18).** The four external artifacts play
three different roles, which set their build order and depth:

- **Tenants** — the native store becomes nodal-db: **FlexTool**
  (§8); **INES** is a possible future host (§7). Both
  should be used to guide design as test templates (§5 ①).
- **Interchange endpoints** — import from / export to, permanently:
  **SiennaSchemas**. Import serves users arriving with Sienna datasets
  who want curation/scenario/history; export is the
  resolve-scenario-at-commit → `SystemDocument` pattern (§4) for
  running Sienna-side simulations from nodal-db-curated data. Build
  order follows whichever demand appears first (trigger-based, §5 ⑤).
- **Contingent** — **CESM**: its compiler is scheduled contingent on
  CESM's continued development; the paper-level IR requirements check
  (§5 ①) keeps IR v1 CESM-compatible either way.

## 2. Construct mapping (both schemas → nodal-db meta-model)

| External construct | nodal-db mapping |
|---|---|
| Concrete class / component type | 0-D entity class (one template row each). |
| CESM `is_dimension`-annotated refs (ports, links); Sienna one-to-many refs (`bus`, `area`, `arc`) and association rows (Service/Plant/SupplementalAttribute/Requirement) | N-D entity class; the endpoints are dimensions, edge-scoped fields become parameters on the relationship entity. Sienna's `Arc` stays a 2-D class with branches 1-D over arc. N-D structure adds referential integrity these formats leave to convention (an N-D entity cannot exist without its elements). |
| Abstract-class ranges (CESM `Port.sink: Node`) and heterogeneous endpoints (Sienna `ServiceAssociation.entity_id` over Device/Branch/Service) | **Class-set-typed dimension** — already in the schema (Decision 5; `entity_class_dimension.dimension_class_set_id`, `schema/010_entities.sql:64-79`). Compiler rule for LinkML: every abstract class used as a range becomes a class-set of its concrete descendants. Zero schema change. |
| CESM `Group_entity` | The native `membership` relation (Decision 3) — cross-class, nestable, alternative-scoped. Not an N-D class. |
| Sienna supplemental attributes (GeographicInfo, outage models, EmissionsData, plant groupings, …) | Attribute type → entity class holding the payload as parameters; the attachment (`SupplementalAttributeAssociation(component_id, component_type, attribute_id, attribute_type)`) → 2-D class with a class-set-typed component dimension. Structurally the entity-plus-relationship pattern nodal-db uses natively — with the added capability that in nodal-db the attachment relationship can itself carry parameters (Sienna's association rows carry none beyond discriminators; exceptions: `PlantAssociation.group_index`, time-series association metadata). Import under a closed allowlist — §3.6. |
| Inheritance / mixins (CESM; Sienna is already flat) | Flatten at compile time: copy inherited + mixin attributes onto concrete classes (the sanctioned "template expansion with provenance" mechanism from Decision 5). Optionally record `from_mixin`/`from_parent` provenance in the template for round-tripping. |
| Scalar/enum/string fields, defaults | Parameter definitions; enums → string parameters + allowed-value audit views. |
| Value pairs (Sienna `MinMax`, `UpDown`, `InOut`, …) | Either exploded parameter pairs or one 1-D label-axis value (`{min,max}`); pick one convention repo-wide per compiler. |
| Nested cost objects (Sienna ValueCurve/FunctionData tower; CESM `ConversionRatesFloatFloat`) | Explode into dotted parameter definitions with a discriminator parameter + audit view for conditional field sets; piecewise data = N×2 float-array values. Study SiennaGridDB's SQL projection first — it already solved this flattening for the same schemas. |
| Units (CESM QUDT/UCUM annotations; Sienna `units.json` + `x-unit`) | Parameter-definition unit metadata; Sienna's quantity-kind/dimension/conversion registry carries over as template metadata for audit views. Sienna per-component unit bases (`power_units` vs own `base_power`): normalize to natural units at import; drop the basis parameters (pu-impedance conversion needs the arc→bus `base_voltage` join). |
| Time series | CESM positional arrays + global timeline → typed datetime-axis values (parametric when the timeline is regular). Sienna `SingleTimeSeries` (initial timestamp + resolution + length) → parametric datetime axis directly; `NonSequentialTimeSeries` → materialized axis; forecasts (Deterministic/Probabilistic/Scenarios) → additional value axes (window-start × offset [× percentile | × scenario-member]). |
| Validation (`required`, bounds, patterns, referential integrity) | Audit views (validation-as-reporting), emitted from machine-readable constraint rows in the template. Dimension references become structural. |
| Identifiers | Entity names. Derived names for N-D entities (element tuple is the identity); source formats' joined-name conventions are display-only. |

### Sienna relationship mechanisms and the import/export asymmetry

Sienna has no general multi-dimensional component concept. Three
mechanisms express relationships, with different capabilities:

| Mechanism | Identity | Carries data | Participates in further structure |
|---|---|---|---|
| Promoted component (`Arc`, branches, `HybridSystem`) | yes | yes | yes (e.g. a Branch can be a service member) |
| Typed ref field (`generator → bus`) | no — edge is implicit | via the owning component's fields | only via the owner |
| Association row (service membership, attachments) | no — identity *is* the pair | only pre-declared fields | no — cannot be an attachment target |

The last cell is their own design document's statement (§13.2.2): an
association's identity is a `(component_id, attribute_id)` pair, so "a
series cannot be named as the subject of a provenance record"; the
association-model change scheduled for their second release candidate
(RC2) addresses this. **Watch item: the RC2 association-model change is
the piece of the Sienna roadmap most likely to ease nodal-db's export
path.**

Consequence — the bridge is asymmetric:

- **Import (Sienna → nodal-db) is lossless**: promoted components map to
  classes; bare associations map to 2-D classes that simply carry no
  parameters yet.
- **Export (nodal-db → Sienna) is lossy** wherever relationship entities
  carry parameters with no promoted-component counterpart (e.g.
  FlexTool-style 4-D reserve classes). The exporter design must
  enumerate the affected relationship classes and choose per class:
  drop, fold onto an endpoint component by convention, or wait for the
  RC2 association model.

**Multi-energy sharpens the export edge (2026-09-18).** Sienna's
extension route to multi-energy is parallel domain stacks:
`InfrastructureCore` is kept domain-neutral (CI layering gate) precisely
so new carrier domains (heat, gas, hydrogen) arrive as new component
families — bus/branch analogues per carrier plus a type per coupling
device. Two structural reasons make this the intended route: references
are typed per class, binding a device to carrier-specific bus types
(contrast CESM, where `Port` ranges over any `Node` and carrier identity
is implicit in topology), and component types are the dispatch interface
for formulation selection in PowerSimulations. Consequence for the
lossiness list above: **carrier coverage is a second dimension** — a
multi-energy entity (e.g. a heat pump coupling electricity and heat) has
no Sienna landing type at all until the corresponding domain stack
exists, and the fold-onto-endpoint convention cannot apply when the
endpoint carrier is absent; multi-energy datasets do not round-trip to
Sienna until then. One level down, nodal-db is agnostic: the meta-model
hosts a 183-class Sienna-shaped template and a 30-class FlexTool-shaped
one indifferently — the nominal↔structural typing choice is made per
template, by each community.

## 3. Cautions (bake into compiler/importer design)

1. **Sienna "Scenarios" time series ≠ nodal-db scenarios.** It is an
   ensemble axis of one forecast value → map to a value axis, never to
   alternatives/scenarios. The true counterpart of a nodal-db scenario on
   the Sienna side is a separately exported dataset.
2. **Sienna `features` tags** (e.g. weather year) ↔ nodal-db
   **alternatives** is the promising mapping (a feature-tagged series is
   "the same parameter under a different alternative"); needs a naming
   scheme.
3. **Name uniqueness**: Sienna guarantees per-type uniqueness only;
   class-sets expect cross-member unambiguity (audit view
   `audit_class_set_entity_name_dup`). Import policy: prefix on collision
   or accept audit flags — decide per importer, it is not a schema issue.
4. **Calendar-step axes**: verify nodal-db parametric axes cover calendar
   resolutions (`P1M`) before relying on parametric encoding for them.
5. **Function-data-per-timestep** (Sienna `element_type` allowing e.g.
   piecewise-linear per hour): no direct typed-axis analogue — reject or
   opaque-encode; do not stretch the value model for it.
6. **Supplemental-attribute openness, precisely scoped.** Payloads of
   *published* attribute types are fully schematized closed objects;
   what is open is the **type space** (`attribute_type` is a free-form
   string by design — "new attribute types are added continuously";
   domains define their own schemas) and the **attachment** (nothing
   constrains which types attach to which components). Their consumer
   contract is filter-by-discriminator, skip the rest. Importer rule,
   final form: **closed allowlist + loud residue** — convert only the
   supported (component-type, attribute-type) pairs, enumerated in the
   template as concrete 2-D classes / narrow class-sets, and report
   everything skipped, per type and component, via audit ("N
   supplemental attributes of unknown types present, not imported").
   Never silent, never crashing. The same rule generalizes to
   heterogeneous association members (`ServiceAssociation`).

## 4. Compiler / importer / exporter architecture

Per schema, **two deliverables, budgeted separately**:

1. **Template compiler** (schema → init template): mechanical given the
   mapping table; pinned to an upstream tag; re-runnable per schema
   version (mirror CESM's own per-version transformer directory layout).
   Sienna scope for a first compiler: Operations + Core + TimeSeries
   semantics (Dynamics is a 7-schema sample; Investments diverges from its
   Julia counterpart by its own changelog).
2. **Data importer/exporter** (instance data ↔ nodal-db):
   - CESM: YAML/CSV datasets; the global timeline resolves into value
     axes on import and must be regenerated (+ alignment-audited) on
     export.
   - Sienna: `SystemDocument` + HDF5 time-series sidecar. **Import reads
     the sidecar via Sienna's own Python package, version-pinned** — do
     not reverse-engineer the HDF5 layout unless/until it is publicly
     specified. On import the metadata/payload split dissolves into
     ordinary typed-axis values committed atomically. Supplemental
     attributes and heterogeneous association members import under the
     closed-allowlist rule (§3.6). Export emits document + sidecar as
     a consistent pair (URI generation included) and carries the
     per-relationship-class lossiness decisions from the asymmetry note
     in §2.
   - Export in general = **resolve a scenario at a commit → emit one
     fully-resolved snapshot dataset**. nodal-db acts as the system of
     record upstream; the external format is the immutable publication
     artifact. Both formats' single-dataset versioning model is then a
     natural consequence of exporting, not something users edit under.
   - **Activation → presence.** The external formats encode run
     configuration as *presence* (an attached series overrides the
     static field it names; an outage attribute present implies outage
     modelling). The exporter therefore **computes presence**: scenario
     resolution decides which parameters, series and supplemental
     attributes are active, and the exporter materializes those
     decisions as inclusion or omission in the emitted dataset
     (activation flags in, presence out). The importer applies the
     mirror rule: absence in a case means "not configured in this
     case" — never "unknown" and never "false"; the store keeps those
     three states distinct.

## 5. Conforming with all — options (compose, don't choose)

1. **Two pinned template compilers**, coexisting in separate DB instances
   — the interim state; fully adequate for "a CESM/Sienna user can use
   nodal-db".
2. **A common template IR** — nodal-db's init-template format (classes,
   dimensions incl. class-sets and membership, parameter defs with units
   and allowed values, audit-rule declarations) designed as the explicit
   compilation target both compilers emit. **The foundation; do first.**
   INES makes it three sources for one target (§7), strengthening the
   case — and the FlexTool tenant makes it four; the IR's v1 gate is the
   dual acceptance test decided in the sequencing below.
3. **Upstream vocabulary sharing** — where the schemas overlap, share
   vocabulary rather than translate: the standout candidate is Sienna's
   unit registry approach (`units.json`: quantity kinds, dimension
   exponents, default units, exact conversion factors, UCUM codes), which
   CESM could adopt in place of per-slot annotations; second-tier:
   ValueCurve/FunctionData for piecewise curves, forecast time-series
   semantics for future stochastics work. (Retargeted 2026-09-18: these
   alignment moves point at INES first — §7 "possible future host";
   CESM remains a beneficiary if it advances.)
4. **nodal-db as hub** — CESM↔Sienna conversion through one nodal-db
   instance holding both templates. Let this emerge from 2+3.

**Sequencing — DECIDED (jkiviluo, 2026-09-18): dual acceptance from the
start.** Order, not calendar:

① **Template IR design**, against the union of FlexTool's and INES's
requirements (the polymorphic-dimension question is already answered by
Decision 5's class-sets — carry class-set and membership as first-class
IR constructs). The IR's first real instance is a **hand-authored
production template** (FlexTool, §8), so it must be human-authorable
and reviewable (diffable text), not merely a compiler emission format.
The IR is not v1 until **both acceptance tests pass**: the hand-authored
FlexTool template (core meta-model at production depth: value lists,
per-parameter types, N-D classes up to 4-D, temporal-structure classes,
a results vocabulary, the migration story) and the compiled INES
template (class-sets incl. N-D members; the compiler path end-to-end).
The two tests stress deliberately disjoint IR features. Hedge: the
design phase includes a paper-level requirements check against CESM and
SiennaSchemas — two schemas validate v1, but four schemas must not be
able to invalidate it.

② **FlexTool template port** — the first implementation effort
(production pull; forces the template-migration story).

③ **INES compiler** — the second acceptance test; the cheapest compiler
(near format-conversion, §7).

④ **CESM compiler** — **contingent on CESM's continued development**
(role taxonomy, §1), not scheduled: once the IR exists, CESM's needs
are a subset of what dual acceptance already forces
(abstract-range→class-set derivation, enum/units/timeline mapping), so
the compiler is mostly mechanical *when warranted* — but it should not
be built against v0.1.0 with its untyped slots (`range: Any`) and open
upstream items (§6). The paper-level requirements check in ① keeps IR
v1 CESM-compatible for free either way.

⑤ **Sienna** — a permanent interchange endpoint (§1), trigger-based,
and *not* trivial: the template compiler
is the smaller half; the **data importer dominates** (SystemDocument +
HDF5 sidecar read via the pinned Sienna package, integer-id→name
resolution, per-component unit-basis normalization) and needs designed
conventions before code (cost-tower flattening after studying
SiennaGridDB, name-collision policy, features→alternatives mapping).
That is a standing maintenance commitment against a pre-1.0 upstream
with no demand signal yet — build when a concrete Sienna-side user
appears.

⑥ Unit-registry conversation upstream (option 3).

## 6. CESM upstream action list (fix in CESM, before/while compiling)

*(Contingent along with the compiler — §1 role taxonomy. The list
stays valid whenever CESM development resumes; the stochastics-alignment
item now applies to INES first, §7.)*

- Declare the `efficiency` slot the sample data and methods doc already
  use (absent from `model/cesm_v0.1.0.yaml`; only `conversion_rates`
  exists — schema/sample drift).
- Type the `range: Any` slots (`flow_profile`, Storage/Link cost slots) —
  a schema-driven compiler cannot derive parameter types for them today.
- Make the method→required-parameters tables machine-readable (currently
  prose in `methods.adoc`) so both CESM validators and nodal-db audit
  views consume one source.
- Align any future CESM stochastics/scenario design with the
  alternatives/scenarios model from the start, so scenario semantics are
  shared rather than bridged.
- Keep the whole-number-percent and dataset-currency conventions explicit
  in exported unit metadata (silent-magnitude-error trap across tools).

## 7. INES (ines-spec) — preparedness note

**What it is.** The INES (Interoperable Energy System) specification is a
standardized exchange format for energy system models, authored *as a
Spine Toolbox database* (`ines-spec.sqlite`, Spine Toolbox ≥ 0.8) and
exported to CSV/JSON/YAML/TOML/TTL. The schema content is Spine-native:
24 entity classes (0-D `node`, `unit`, `link`, `set`, `constraint`,
`period`, `reserve`, `solve_pattern`, `system`; 2-D port/membership
classes; a 3-D `node__link__node`), 264 parameter definitions with
value lists (109 members) and per-parameter valid-type declarations, and
one Spine **superclass** `unit_flow` over `node__to_unit`/`unit__to_node`
(used as a dimension in `unit_flow__unit_flow`). Scope is close to
CESM's (zonal multi-energy planning + operations control structures) and
additionally covers reserves and stochastics (`*_forecasts` parameters
with forecast/analysis-time map dimensions; stochastic scope/method/
weights on solve patterns and sets). Versioning is branch-per-major
(`v1.x` current, changelog at 1.1.5). The surrounding `ines-tools` org
maintains converters to/from SpineOpt, FlexTool, Backbone, OSeMOSYS,
EMPIRE, EnergyModelsX, PyPSA, and Tulipa, plus data pipelines and an
`ines-certify` model-capability test suite.

**How a template compiler treats it.** As a near-direct translation —
INES is already expressed in the Spine data model that nodal-db's
meta-model descends from. Entity classes → classes (including the 3-D
class as-is); the `unit_flow` superclass → a **class-set** with the two
port classes as members, used as a dimension type (Decision 5's intended
replacement for Spine's `superclass_subclass`, exercised here on N-D
member classes); value lists → allowed-value audit views; per-parameter
valid-type lists (e.g. `float | map | time_series`) → the same
union-type-per-definition policy decision already flagged for CESM;
Spine map/array/time-series values → typed-axis values. The
`ines-spec.json` export is a sufficient, machine-readable compiler input.

**Relation to CESM in the conform-with-both picture.** CESM is INES
redesigned (ontology conformance, spec/implementation separation, LinkML
instead of a Spine DB as the normative form); the projects currently
coexist, with INES carrying the larger live converter ecosystem and CESM
the cleaner spec layer. For nodal-db they are one family: an INES
template is a third compiler target (the cheapest — effectively a Spine
template import), not a second strategy. Where INES and CESM diverge
(reserves and stochastics exist in INES but not CESM v0.1.0), the INES
constructs preview what CESM will eventually need — the stochastics
representation in particular should inform the upstream alignment item
in §6. Data-level bridges to the eight INES-converter model formats
come with the template, via the existing INES tooling.

**Possible future host (noted 2026-09-18).** INES's normative artifact
is already a database (a Spine DB), so a future in which the INES spec
itself is hosted on nodal-db is plausible. The INES compiler in the
dual-acceptance plan (§5 ③) then doubles as the first half of that
migration path; an INES-on-nodal-db spec would gain commit history and
diffable evolution, and the converter ecosystem would gain a SQL read
surface. Consequently the upstream-alignment items (union-type policy
settlement, the unit-registry idea of §5 option 3, stochastics
alignment) point at INES first. This is a held option requiring
coordination with INES's current maintainers, not a commitment.

## 8. Tool tenant: IRENA FlexTool — preparedness note

**What it is.** IRENA FlexTool (v4) is an energy and power systems
model for capacity-expansion and operational planning — a modelling
tool, not an interoperability standard. Technically it is a Python
layer over a Rust optimisation core: inputs are read from a Spine
database (`spinedb_api`), materialised as polars frames, assembled into
an LP/MIP via `polar-high`, solved with HiGHS in-process; results are
written to parquet as the canonical output, with CSV/Excel/plot
renderings and a desktop GUI on top. Its data vocabulary is defined as a
versioned Spine template (`flextool/schemas/spinedb_schema.json`): 30
entity classes (14 of them 0-D — `node`, `unit`, `connection`,
`commodity`, `reserve`, `constraint`, `group`, `profile`, `model`,
`solve`, `timeline`, `timeset`, …— plus 16 N-D classes up to 4-D, e.g.
`unit__inputNode`, `connection__node__node`,
`reserve__upDown__unit__node`), 229 parameter definitions, and 35
parameter value lists (140 members); a separate results vocabulary (12 classes, 58
parameter definitions) describes outputs. The input schema carries a
migration chain (`FLEXTOOL_DB_VERSION = 69`) with per-version canonical
databases, and an **axis contract** (`flextool_axis_contract.json`)
declaring the categorical axis vocabularies the engine casts against.
One scenario is resolved per solve run via the Spine scenario filter.

**Why the tool changes the interop picture.**

1. **The template is authored, not compiled.** FlexTool's vocabulary is
   designed from the user's perspective and versioned with the tool. Its
   nodal-db template is therefore the template IR's first *hand-written*
   instance — the IR must serve authors, not only compilers (§5
   sequencing note).
2. **Standards become import paths, not endpoints.** CESM and INES data
   arrive *into* the FlexTool template (CESM already ships a
   CESM↔FlexTool transformer; INES has a FlexTool converter). Once both
   ends are nodal-db instances, those transformations can run
   template-to-template inside the store — the first concrete instance
   of the hub option (§5.4). Exact conformance with the standards is
   explicitly not a goal; the tool vocabulary stays user-shaped.
3. **Results enter the story.** Standards carry inputs; a tool's store
   also describes outputs. Decided architecture: results live in
   separate output databases in full nodal-db format — §9.
4. **The engine is a read-surface consumer.** FlexTool already funnels
   all input access through a single backend (`SpineDBBackend`, the
   only input door) behind a provider interface (`FlexDataProvider`)
   that deliberately decouples the engine from the data source. A
   nodal-db-backed source slots into exactly that seam — the engine and
   LP builder are unaffected by design.
5. **FlexTool's own roadmap reserves this slot.** Its architecture
   record (`specs/architecture_provider_and_future_duckdb.md` in the
   FlexTool repo) plans a "Tier-1" replacement of the Spine input layer
   with a relational DuckDB store plus a rewritten DB editor —
   which is, in substance, nodal-db plus the nodal editor. The
   migration path (template conversion + data migration from the v69
   Spine chain, scenario/alternative carry-over) is the concrete work
   package.

**Decided (jkiviluo, 2026-09-18):**

- **Fidelity principle.** The FlexTool template mirrors FlexTool's
  decisions class by class: nodal-db's native per-alternative activation
  where FlexTool uses `entity_alternative.active` (still the case for
  the main physical classes `node`, `unit`, `connection`), and the plain
  `is_enabled` parameter where FlexTool uses that (the v56 change did
  not replace `active` everywhere). No redesign at the mapping layer.
- **Editor mandate.** nodal-editor will eventually replace/augment the
  Spine DB editor — it is the "rewritten DB editor" of FlexTool's
  Tier-1 plan.

## 9. Results architecture — input and output databases (decided)

Decided with jkiviluo, 2026-09-18 (settles the question left open in
§8.3).

**Shape.** Inputs and results live in **separate files, both full
nodal-db databases** — same schema, same canonicalization pipeline, same
read surface. There is no special "output mode": a second write mode
would be a cousin of the two-canonicalizers mistake. What makes an
output DB an output DB is its *content* (a results template) and a
**provenance stamp**, not a different format.

**Why split.** The unit of *sharing* is the driver: an input DB is a
curated shared asset with a long lifecycle; exploratory outputs are
personal and disposable until proven otherwise. Mixing them forces every
share of the inputs to either drag results along or strip them out.

**Granularity.** One output DB holds **many runs**, each written under
its own alternative — cross-run/scenario comparison (the typical mode of
result analysis) then uses the native resolution machinery inside one
file. Per-run output files are rejected: they push comparison back out
of the data model. Multiple output DBs per input DB are fine (analysis
stages, old versions) — file-level management; each file is
self-contained.

**Overwrite-on-rerun** (user choice per run): write the new run's
alternative → hard-erase the old run's alternative → compact. This
**un-defers the hard-erasure door** from the object-delete record, with
a concrete grain: **erase-by-alternative**. The compaction primitive
(built 2026-09) is its space-reclaim substrate. The "delete only after
the new result is fully in" ordering is naturally safe — the new run
lands in its own alternative before the old one is touched.

**Cross-file references are by name, resolved as-of.** The output DB
stamps the **input DB identity + commit + scenario** that produced it;
joint views join through the input DB's `*_asof` time-travel macros, so
an old output DB stays joinable against an input DB that has moved on.
Join views are as-of by construction, never current-state views.
Requires a **stable DB identity** (UUID minted at init) — a
pre-encoding-freeze schema touch; flag on the freeze checklist.

**The input DB stays clean of run bookkeeping.** Provenance points
output→input only; discovering "which outputs exist for these inputs"
is a project/editor-level concern, not a schema feature.

**Connection discipline extends to attached pairs.** Joint views use
per-operation `ATTACH ... (READ_ONLY)` of the second file (machinery the
compaction module already exercises); lock symmetry applies per file —
an editor reading input+output while the engine writes a new run dances
through `Busy` on one file, not both. Needs a paragraph in the
connection-discipline spec, not a redesign.

**Prerequisites before committing FlexTool to this path:**

1. **Bulk-ingest benchmark** — array writes currently bind via generated
   `list_value` constructors (`history.rs:704-716`), unproven at
   millions of result rows; the Arrow-IPC import boundary is the likely
   fast lane. Steel-man benchmark, not argument.
2. **Windowed-read check** — verify the read surface slices a `DOUBLE[]`
   year efficiently for a one-week window (list slicing vs
   unnest-then-filter); directly shapes nodal-editor's result viewer.

**Parquet demotes to an export/interchange format.** It remains
FlexTool's canonical output today; its parquet consumers migrate at
their own pace behind the Provider seam.

Distinction from metadata-plus-sidecar two-artifact patterns: here both
files are complete, individually atomic, queryable databases split on a
semantic boundary (curated inputs vs derived results) with an explicit
as-of provenance link — not a metadata document describing payloads it
cannot see.
