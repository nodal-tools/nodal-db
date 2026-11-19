-- 001_meta.sql — sequences, commit log, database properties, metadata.
-- Plan citations: SDP = specs/schema_draft_plan.md, VMP = specs/value_model_plan.md.
-- SDP Part 1 row `commit`; SDP Part 2 "History, commits, and
-- change detection"; VMP Part 5 (commit ids as validity edges).

-- Stable-id contract (SDP Part 2, #3218): ids are durable across sessions.
-- One sequence per identity-bearing table; versioned rows share the id across
-- versions, so id is NOT a primary key on versioned tables (see ddl_sketch.md
-- deviation D1: FK intent to versioned tables falls back to validation views).
CREATE SEQUENCE seq_commit;
CREATE SEQUENCE seq_metadata_pair;
CREATE SEQUENCE seq_source;
CREATE SEQUENCE seq_license;
CREATE SEQUENCE seq_metadata_role;
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
-- family (HANDOFF §8.4: alternative / horizon / entity). The core writes
-- kind='alternative' (erase-by-alternative) and kind='horizon' (the
-- history-free extract, which writes it into the NEW file it builds, with
-- continuation_db = the source's identity: the untouched source is the
-- archive); kind='entity' is not written yet. The as-of guard in
-- 110_views_history.sql consults this table relationally on every as-of read —
-- which is why it is a base table, not a `database_property` key.
-- Scope columns are ids, not names; the identity rows they point to are
-- RETAINED (closed, never removed — D-E2), so a marker always resolves to a
-- name. They carry no engine FKs (versioned targets — deviation D1);
-- `at_commit` is engine-enforced against the append-only commit log.
CREATE TABLE erasure_marker (
  id               BIGINT PRIMARY KEY DEFAULT nextval('seq_erasure_marker'),
  kind             VARCHAR NOT NULL,   -- 'alternative' | 'horizon' | 'entity'
                                       -- (extensible tag; the core writes
                                       --  'alternative' and refuses the rest)
  -- exactly one scope column per kind (CHECKed below):
  alternative_id   BIGINT,             -- kind='alternative': all data under it
  entity_id        BIGINT,             -- kind='entity' (future): poison removal
  horizon_commit   BIGINT REFERENCES commit(id),
                                       -- kind='horizon' (history-free extract): superseded
                                       -- content with valid_to_commit <= this
                                       -- was removed
  at_commit        BIGINT NOT NULL REFERENCES commit(id),
                                       -- the erasure commit (when it happened)
  continuation_db  VARCHAR,            -- NULL = truncated (bytes gone).
                                       -- Else: the DB-identity UUID of the
                                       -- archive file now holding the removed
                                       -- rows (the extract's source file).
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

-- ---------------------------------------------------------------------------
-- Database properties and metadata.
-- Every table: from/to-commit pattern, `version` 1..n per key, the span CHECK,
-- `cur_*` generated columns and current-scoped unique indexes (partial
-- indexes are unsupported in DuckDB). No engine FKs to versioned tables
-- (deviation D1); FK intent is checked by `audit_dangling_ref` (120).
-- Unique-index key order follows engine rule 4 (few-distinct columns first,
-- the most-varying last); no index here is an access path (engine rule 3).
-- The core's `property` module owns `database_property`; its `metadata`
-- module owns the other seven tables (one writer per table).
-- ---------------------------------------------------------------------------

-- Single-valued database properties: one current row per key (case-folded).
-- `owner` classifies the key and is CHECKed against its namespace, so the DDL
-- is the one classifier:
--   'core' — written only by the core: schema_version (replaces
--            alembic_version), db_identity (the stable DB-identity UUID
--            minted by 900_bootstrap.sql and never changed), the template
--            stamp keys, the shared `presentation` document and
--            `audit_rules`. Adding a core key edits the CHECK list below, i.e.
--            it is a schema change with a SCHEMA_VERSION bump (core keys come
--            only with core changes, like erasure_marker.kind);
--   'tool' — `<tool>:<name>` keys a tool owns (e.g. `flextool:frozen`);
--   'user' — every other key (documented well-known keys: identifier,
--            base_iri, title, description, version, created, homepage,
--            language, semantic_context, conforms_to).
-- JSON-valued keys ride the single `value` VARCHAR as a JSON document.
-- Read the identity with plain SQL:
--   SELECT value FROM database_property
--   WHERE lower(key) = 'db_identity' AND valid_to_commit IS NULL
CREATE TABLE database_property (
  key               VARCHAR NOT NULL,
  value             VARCHAR NOT NULL,
  owner             VARCHAR NOT NULL CHECK (owner IN ('core', 'user', 'tool')),
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),  -- NULL = current
  version           SMALLINT NOT NULL DEFAULT 1,   -- 1..n per key
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  -- tool keys: '<tool>:<name>' (the writer checks both parts; this is the
  -- coarse backstop)
  CHECK ((owner = 'tool') = (strpos(key, ':') > 1)),
  CHECK ((owner = 'core') = (lower(key) IN ('schema_version', 'db_identity',
     'template_name', 'template_version', 'template_content_hash',
     'template_migration_step', 'template_applied_commit',
     'presentation', 'audit_rules'))),
  cur_key VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(key) END) VIRTUAL
);
CREATE UNIQUE INDEX uq_database_property ON database_property (cur_key);

-- The pair pool: name/value annotations (Spine's `metadata` rows land here).
-- A pair lives once; entities, values, schema objects and the dataset link to
-- it. Case-insensitive uniqueness on (name, value) among current rows.
CREATE TABLE metadata_pair (
  id                BIGINT NOT NULL DEFAULT nextval('seq_metadata_pair'),
  name              VARCHAR NOT NULL,          -- the key: an identifier (NFC)
  value             VARCHAR NOT NULL,          -- data: verbatim
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL,
  cur_val  VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN value END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_metadata_pair ON metadata_pair (cur_id);
CREATE UNIQUE INDEX uq_metadata_pair_name_value ON metadata_pair (cur_name, cur_val);

-- A typed source record (a publication, a dataset, a measurement campaign).
-- `name` is the short, unique label; the rest is descriptive data.
CREATE TABLE source (
  id                BIGINT NOT NULL DEFAULT nextval('seq_source'),
  name              VARCHAR NOT NULL,
  title             VARCHAR,
  path              VARCHAR,                   -- URL, DOI link or file path
  edition           VARCHAR,
  publisher         VARCHAR,
  issued            DATE,
  retrieved         DATE,
  citation          VARCHAR,
  description       VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_source ON source (cur_id);
CREATE UNIQUE INDEX uq_source_name ON source (cur_name);

-- A license: free, unique name; `spdx` marks a name that is an SPDX license
-- identifier; `policy` is an optional ODRL policy (JSON-LD: stored, never
-- interpreted).
CREATE TABLE license (
  id                BIGINT NOT NULL DEFAULT nextval('seq_license'),
  name              VARCHAR NOT NULL,
  title             VARCHAR,
  path              VARCHAR,
  spdx              BOOLEAN NOT NULL DEFAULT FALSE,
  policy            VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_license ON license (cur_id);
CREATE UNIQUE INDEX uq_license_name ON license (cur_name);

-- The licenses a source is published under (many-to-many).
CREATE TABLE source_license (
  source_id         BIGINT NOT NULL,
  license_id        BIGINT NOT NULL,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_s BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN source_id END) VIRTUAL,
  cur_l BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN license_id END) VIRTUAL
);
CREATE UNIQUE INDEX uq_source_license ON source_license (cur_l, cur_s);

-- The role vocabulary of links, created on purpose (a fresh file has none).
-- `iri`: the PROV / DataCite / Frictionless term the role stands for.
CREATE TABLE metadata_role (
  id                BIGINT NOT NULL DEFAULT nextval('seq_metadata_role'),
  name              VARCHAR NOT NULL,
  description       VARCHAR,
  iri               VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_metadata_role ON metadata_role (cur_id);
CREATE UNIQUE INDEX uq_metadata_role_name ON metadata_role (cur_name);

-- Links from a subject (the dataset, an entity, a schema object, a source or
-- a license) to a target (a source, a license or a pair), with an optional
-- role. Target = kind tag + id, not three nullable id columns: a unique index
-- does not enforce rows with a NULL key column. For the same reason the role
-- enters the key as coalesce(role_id, 0): "no role" is one key value. Object
-- types are a CHECK list (each type has a close-out leg when its object
-- dies), not an open tag.
CREATE TABLE object_metadata_link (
  object_type       VARCHAR NOT NULL CHECK (object_type IN ('dataset', 'entity',
     'entity_class', 'parameter_definition', 'alternative', 'scenario', 'run',
     'source', 'license', 'parameter_value_list', 'units_of_measure')),
  object_id         BIGINT NOT NULL,           -- 0 for 'dataset'
  target_kind       VARCHAR NOT NULL CHECK (target_kind IN ('source', 'license', 'pair')),
  target_id         BIGINT NOT NULL,
  role_id           BIGINT,                    -- NULL = no role
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  CHECK ((object_type = 'dataset') = (object_id = 0)),
  CHECK (NOT (object_type = target_kind AND object_id = target_id)),  -- no self-link
  cur_ot VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN object_type END) VIRTUAL,
  cur_o  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN object_id END) VIRTUAL,
  cur_tk VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN target_kind END) VIRTUAL,
  cur_t  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN target_id END) VIRTUAL,
  cur_r  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN coalesce(role_id, 0) END) VIRTUAL
);
CREATE UNIQUE INDEX uq_object_metadata_link
  ON object_metadata_link (cur_ot, cur_tk, cur_r, cur_t, cur_o);

-- Links from a value (its key triple, across tiers) to a source or a pair.
-- A link row belongs to the value version current at its valid_from_commit:
-- every new value version closes the row and (keep) reopens it at the same
-- commit, so link spans tile with value versions (a link may start later
-- than its version, never earlier: audit_value_link_version). Values reach
-- licenses through their sources.
CREATE TABLE value_metadata_link (
  entity_id               BIGINT NOT NULL,
  parameter_definition_id BIGINT NOT NULL,
  alternative_id          BIGINT NOT NULL,
  target_kind             VARCHAR NOT NULL CHECK (target_kind IN ('source', 'pair')),
  target_id               BIGINT NOT NULL,
  role_id                 BIGINT,              -- NULL = no role
  valid_from_commit       BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit         BIGINT REFERENCES commit(id),
  version                 SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),
  cur_tk VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN target_kind END) VIRTUAL,
  cur_t  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN target_id END) VIRTUAL,
  cur_r  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN coalesce(role_id, 0) END) VIRTUAL,
  cur_a  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN alternative_id END) VIRTUAL,
  cur_p  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL,
  cur_e  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL
);
CREATE UNIQUE INDEX uq_value_metadata_link
  ON value_metadata_link (cur_tk, cur_t, cur_r, cur_a, cur_p, cur_e);
