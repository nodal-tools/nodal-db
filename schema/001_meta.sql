-- 001_meta.sql — sequences, commit log, metadata.
-- Plan citations: SDP = specs/schema_draft_plan.md, VMP = specs/value_model_plan.md.
-- SDP Part 1 rows `commit`, `metadata`; SDP Part 2 "History, commits, and
-- change detection"; VMP Part 5 (commit ids as validity edges).

-- Stable-id contract (SDP Part 2, #3218): ids are durable across sessions.
-- One sequence per identity-bearing table; versioned rows share the id across
-- versions, so id is NOT a primary key on versioned tables (see ddl_sketch.md
-- deviation D1: FK intent to versioned tables falls back to validation views).
CREATE SEQUENCE seq_commit;
CREATE SEQUENCE seq_metadata;
CREATE SEQUENCE seq_entity_class;
CREATE SEQUENCE seq_class_set;
CREATE SEQUENCE seq_entity;
CREATE SEQUENCE seq_alternative;
CREATE SEQUENCE seq_scenario;
CREATE SEQUENCE seq_units_of_measure;
CREATE SEQUENCE seq_parameter_value_list;
CREATE SEQUENCE seq_list_value;
CREATE SEQUENCE seq_parameter_definition;
CREATE SEQUENCE seq_parameter_type;
CREATE SEQUENCE seq_axis;
CREATE SEQUENCE seq_axis_label;
CREATE SEQUENCE seq_display_mode;
-- (no seq for entity_class_display_mode: composite-keyed link table)
CREATE SEQUENCE seq_run;                 -- provenance keystone (Decision 13, run table)
CREATE SEQUENCE seq_semantic_annotation; -- polymorphic ontology crosswalk (fitness 2b)

-- The commit table (SDP Part 1 `commit`): append-only and immutable — the one
-- exception to the mutable-table from/to pattern. max(id) doubles as the
-- change-detection polling cookie (#2252); `user` defaults client-side to the
-- OS username (#1723). Commit ids, never wall-clock, are the validity edges
-- everywhere (VMP Part 5).
CREATE TABLE commit (
  id        BIGINT PRIMARY KEY DEFAULT nextval('seq_commit'),
  timestamp TIMESTAMP NOT NULL DEFAULT now()::TIMESTAMP,
  user      VARCHAR NOT NULL,
  message   VARCHAR
);

-- One key/value metadata table (SDP Part 1 `metadata`): dataset-level facts
-- (#645: author, sources, licenses), template identity keys (Decision 6:
-- template name/version/content hash), the shared authored presentation slot
-- (Decision 7: one JSON document under one reserved key), and schema_version
-- (replaces alembic_version) are reserved keys with no link row.
-- Fitness round 2b adds semantic/interop reserved keys (convention only, no
-- new table): `semantic_context` (JSON-LD @context / CURIE-prefix map that
-- expands the IRIs stored in semantic_link / quantity_kind_link / term_iri),
-- `conforms_to` (DCTERMS profile IRI the dataset claims), plus the agreed
-- Frictionless/oemetadata `licenses` / per-source `sourceLicenses`,
-- role-typed `contributors`, and dataset `version` keys. JSON-valued keys
-- ride the single `value` VARCHAR as a JSON document, like `presentation`.
-- Full catalogue: ddl_sketch.md §4.1.
-- Mutable table -> from/to-commit pattern (VMP Part 5 "Scope of
-- record-per-commit"); one-current per id via generated-column unique index
-- (specs/versioning_from_to_date_research.md trick — partial indexes are
-- unsupported in DuckDB, verified 1.4.4).
CREATE TABLE metadata (
  id                BIGINT NOT NULL DEFAULT nextval('seq_metadata'),
  name              VARCHAR NOT NULL,          -- key; reserved keys documented in ddl_sketch.md
  value             VARCHAR NOT NULL,          -- scalar or JSON document (presentation slot)
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),  -- NULL = current
  version           SMALLINT NOT NULL DEFAULT 1,   -- 1..n per id (VMP Part 5, critique #1)
  -- span coherence (critique F2): a closed span must close after it opened;
  -- the same CHECK appears on every from/to table
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  -- case-insensitive uniqueness on (name, value) among current rows (SDP
  -- Decision 11 applied to metadata keys; same (name, value) uniqueness as
  -- the carried Spine table):
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL,
  cur_val  VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN value END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_metadata ON metadata (cur_id);
CREATE UNIQUE INDEX uq_metadata_name_value ON metadata (cur_name, cur_val);

-- The two cheap FK-clean link tables (SDP Part 1 `entity_metadata`,
-- `parameter_value_metadata`). Values are keyed by the key triple across
-- tiers (VMP Part 2), so the value link carries the triple, not a value id.
-- FK intent to entity/metadata is view-validated (deviation D1).
CREATE TABLE entity_metadata (
  entity_id         BIGINT NOT NULL,
  metadata_id       BIGINT NOT NULL,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_e BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL,
  cur_m BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN metadata_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_entity_metadata ON entity_metadata (cur_e, cur_m);

CREATE TABLE parameter_value_metadata (
  entity_id               BIGINT NOT NULL,
  parameter_definition_id BIGINT NOT NULL,
  alternative_id          BIGINT NOT NULL,
  metadata_id             BIGINT NOT NULL,
  valid_from_commit       BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit         BIGINT REFERENCES commit(id),
  version                 SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_e BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL,
  cur_p BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL,
  cur_a BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN alternative_id END) VIRTUAL,
  cur_m BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN metadata_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_pv_metadata
  ON parameter_value_metadata (cur_e, cur_p, cur_a, cur_m);
