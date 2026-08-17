-- 030_parameters.sql — units registry, value lists, parameter definitions,
-- per-entity unit override, and structural type declarations.
-- SDP Part 1 rows `parameter_definition`, `units_of_measure` (Decision 9),
-- `parameter_type`, `parameter_value_list`/`list_value`; SDP Part 2 "Units —
-- express, never assert"; VMP Part 7 "Validation and typing".

-- Units registry (Decision 9, D#2180): no conversion or dimension machinery.
-- A unit is an expressed default, never an assertion.
CREATE TABLE units_of_measure (
  id                BIGINT NOT NULL DEFAULT nextval('seq_units_of_measure'),
  name              VARCHAR NOT NULL,
  description       VARCHAR,
  semantic_link     VARCHAR,   -- optional semantic/ontology link — the only structured door held open
  quantity_kind_link VARCHAR,  -- optional QUDT quantitykind: IRI (e.g. .../Power) — expresses
                               -- "capacity is a power, not an energy"; cold, inert (fitness 2b/T1.2)
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_units ON units_of_measure (cur_id);
CREATE UNIQUE INDEX uq_units_name ON units_of_measure (cur_name);

-- Value lists, de-blobbed (SDP Part 1 `parameter_value_list`/`list_value`):
-- members are typed native values, each with a description (#2286) surfaced
-- as tooltips, and an ordinal (ordering is semantic, never row order —
-- Decision 1 rider).
CREATE TABLE parameter_value_list (
  id                BIGINT NOT NULL DEFAULT nextval('seq_parameter_value_list'),
  name              VARCHAR NOT NULL,
  description       VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_pvl ON parameter_value_list (cur_id);
CREATE UNIQUE INDEX uq_pvl_name ON parameter_value_list (cur_name);

CREATE TABLE list_value (
  id                      BIGINT NOT NULL DEFAULT nextval('seq_list_value'),
  parameter_value_list_id BIGINT NOT NULL,
  ordinal                 INTEGER NOT NULL,
  description             VARCHAR,                 -- #2286
  semantic_link           VARCHAR,                 -- optional SKOS concept IRI for this member — turns a
                                                   -- value-list into a controlled codelist; cold, inert (fitness 2b/T1.2)
  -- typed native member value — same scalar kind set as value_scalar
  -- (VMP Part 2); value_kind is the extensible tag (Decision 4: no CHECK
  -- enum so future kinds are additive without migration):
  value_kind    VARCHAR NOT NULL,
  v_float DOUBLE, v_int BIGINT, v_bool BOOLEAN, v_str VARCHAR,
  v_datetime TIMESTAMP,
  v_dur_months INTEGER, v_dur_micros BIGINT,       -- duration pair (VMP Part 1, critique #9)
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_l  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_value_list_id END) VIRTUAL,
  cur_o  INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN ordinal END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_list_value ON list_value (cur_id);
CREATE UNIQUE INDEX uq_list_value_ordinal ON list_value (cur_l, cur_o);

-- Parameter definition (SDP Part 1, "carried, deliberately enriched").
-- default_value / prefill_value are NOT blobs — they live in the mirrored
-- definition-scoped value tables (050_values.sql), same tiered
-- representation as values (#1598). retain_content is the first-class
-- retention policy (VMP Part 5, critique #3).
CREATE TABLE parameter_definition (
  id                      BIGINT NOT NULL DEFAULT nextval('seq_parameter_definition'),
  entity_class_id         BIGINT NOT NULL,
  name                    VARCHAR NOT NULL CHECK (regexp_matches(name, '^[A-Za-z0-9_]+$')),  -- #2373
  description             VARCHAR,
  parameter_value_list_id BIGINT,                    -- optional value-list constraint (writer-checked, view-audited)
  unit_id                 BIGINT,                    -- expressed default unit (Decision 9)
  required                BOOLEAN NOT NULL DEFAULT FALSE,  -- completeness flag (#2710)
  display_order           INTEGER,                   -- authored order (#691)
  retain_content          BOOLEAN NOT NULL DEFAULT TRUE,   -- FALSE = superseded rows become stubs (VMP Part 5)
  valid_from_commit       BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit         BIGINT REFERENCES commit(id),
  version                 SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_cls  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_class_id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_parameter_definition ON parameter_definition (cur_id);
CREATE UNIQUE INDEX uq_parameter_definition_name ON parameter_definition (cur_cls, cur_name);

-- Sparse per-entity unit override (Decision 9): rows only where the default
-- unit is wrong for that entity. Effective unit = override else default —
-- view parameter_effective_unit (080).
CREATE TABLE entity_parameter_unit (
  entity_id               BIGINT NOT NULL,
  parameter_definition_id BIGINT NOT NULL,
  unit_id                 BIGINT NOT NULL,
  valid_from_commit       BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit         BIGINT REFERENCES commit(id),
  version                 SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_e BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL,
  cur_p BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_entity_parameter_unit ON entity_parameter_unit (cur_e, cur_p);

-- Structural typing (VMP Part 7): declares what a definition's values may
-- be. Dimensionality is declared per definition, never per value (VMP
-- Part 2). value_kind: scalar kinds or 'indexed'; extensible tag, no CHECK
-- enum (Decision 4 door: symbol_ref additive without migration).
CREATE TABLE parameter_type (
  id                      BIGINT NOT NULL DEFAULT nextval('seq_parameter_type'),
  parameter_definition_id BIGINT NOT NULL,
  value_kind              VARCHAR NOT NULL,
  rank                    TINYINT NOT NULL DEFAULT 0 CHECK (rank BETWEEN 0 AND 4),  -- rank cap = 4 (VMP binding decision)
  aggregation             VARCHAR CHECK (aggregation IN ('point', 'mean', 'sum')),
                          -- reserved cell_methods-style semantics slot; NULL = 'point' (VMP Part 7)
  expected_tier           VARCHAR CHECK (expected_tier IN ('array', 'long')),
                          -- audit-only, never enforcement (critique #8): surfaces raggedness at write time
  valid_from_commit       BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit         BIGINT REFERENCES commit(id),
  version                 SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_parameter_type ON parameter_type (cur_id);

-- Named, kind-typed index dimensions of an indexed declaration
-- (VMP Part 2 "Where dimensionality is declared"): (position,
-- dimension_name, kind). Dimension names live here; axis objects are
-- anonymous and content-addressed (VMP Part 4). kind: Part 1 enum
-- (datetime|period|label|integer|time_pattern), additive — no CHECK.
CREATE TABLE parameter_type_dimension (
  parameter_type_id BIGINT NOT NULL,
  position          TINYINT NOT NULL CHECK (position BETWEEN 1 AND 4),
  dimension_name    VARCHAR NOT NULL,
  kind              VARCHAR NOT NULL,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_t BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_type_id END) VIRTUAL,
  cur_p TINYINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN position END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_pt_dimension ON parameter_type_dimension (cur_t, cur_p);
