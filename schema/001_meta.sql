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
CREATE SEQUENCE seq_erasure_marker;      -- hard-erasure trace (erasure_compaction_plan §1.1, D-E1)
-- value_array.array_id (surrogate row handle). Stride 2: every id shares one
-- parity, so no two ids are adjacent and a set of ids is never contiguous;
-- DuckDB rewrites an exactly contiguous IN set into a two-sided range filter,
-- which never uses the index. Any re-creation of this sequence (compaction,
-- extract, hand migration) must keep INCREMENT BY and the parity.
CREATE SEQUENCE seq_value_array INCREMENT BY 2;

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

-- The one sanctioned trace of physical removal (erasure_compaction_plan §1.1,
-- decision D-E1 2026-09-19). Append-only and immutable, like `commit`: markers
-- are the permanent, truthful record that removal happened — never versioned,
-- updated, or themselves erased. A marker row is written in the SAME
-- transaction as the removal it records (the removed rows carry no close-out
-- edge — the marker IS their trace). Shaped for the whole anticipated grain
-- family (HANDOFF §8.4: alternative / horizon / entity, with detach-to-archive
-- riding kind='alternative' + continuation_db); the v1 core writes only
-- kind='alternative' and refuses the rest. The as-of guard in
-- 110_views_history.sql consults this table relationally on every as-of read —
-- which is why it is a base table, not a `metadata` reserved key.
-- Scope columns are ids, not names; the identity rows they point to are
-- RETAINED (closed, never removed — D-E2), so a marker always resolves to a
-- name. They carry no engine FKs (versioned targets — deviation D1);
-- `at_commit` is engine-enforced against the append-only commit log.
CREATE TABLE erasure_marker (
  id               BIGINT PRIMARY KEY DEFAULT nextval('seq_erasure_marker'),
  kind             VARCHAR NOT NULL,   -- 'alternative' | 'horizon' | 'entity'
                                       -- (extensible tag; v1 core writes only
                                       --  'alternative' and refuses the rest)
  -- exactly one scope column per kind (CHECKed below):
  alternative_id   BIGINT,             -- kind='alternative': all data under it
  entity_id        BIGINT,             -- kind='entity' (future): poison removal
  horizon_commit   BIGINT REFERENCES commit(id),
                                       -- kind='horizon' (future): superseded
                                       -- content with valid_to_commit <= this
                                       -- was removed
  at_commit        BIGINT NOT NULL REFERENCES commit(id),
                                       -- the erasure commit (when it happened)
  continuation_db  VARCHAR,            -- NULL = truncated (bytes gone).
                                       -- Else: the DB-identity UUID of the
                                       -- archive file now holding the removed
                                       -- rows (detach-to-archive, future).
  rows_removed     BIGINT NOT NULL,    -- total across tables (report detail in
                                       -- the API report, not the marker)
  bytes_estimate   BIGINT,             -- advisory payload estimate at erase time
  note             VARCHAR,            -- free-form operator context
  CHECK ((kind = 'alternative') = (alternative_id IS NOT NULL)),
  CHECK ((kind = 'entity')      = (entity_id      IS NOT NULL)),
  CHECK ((kind = 'horizon')     = (horizon_commit IS NOT NULL)),
  -- The tag is extensible BY DDL EDIT, not by writing unknown strings: a new
  -- grain ships by widening this list (a schema change, like the grain's
  -- operation itself), so a marker whose kind nothing can interpret is
  -- unrepresentable rather than silently inert. The scope-column CHECK trio
  -- above stays per-kind.
  CHECK (kind IN ('alternative', 'entity', 'horizon'))
);

-- One key/value metadata table (SDP Part 1 `metadata`): dataset-level facts
-- (#645: author, sources, licenses), template identity keys (Decision 6:
-- template name/version/content hash), the shared authored presentation slot
-- (Decision 7: one JSON document under one reserved key), schema_version
-- (replaces alembic_version), and db_identity (the stable DB-identity UUID,
-- minted once by 900_bootstrap.sql and never changed — the anchor for
-- output→input provenance stamps, schema_interop.md §9; read with
--   SELECT value FROM metadata
--   WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL)
-- are reserved keys with no link row.
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
