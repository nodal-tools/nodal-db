-- 010_entities.sql — entity classes, class-sets, dimensions, entities,
-- elements, locations.
-- SDP Part 1 rows `entity_class`, `entity_class_dimension`,
-- `superclass_subclass` (replaced by class-set, Decision 5), `entity`,
-- `entity_element`, `entity_location`; SDP Part 2 "Validation philosophy"
-- (name rules #2373/#1302); VMP Part 5 (from/to on all narrow mutable tables).

-- Entity class (SDP Part 1 `entity_class`): carried, pruned. Keeps
-- active_by_default (resolution semantics), display_order (authored template
-- presentation, #691), required (feeds completeness views, #3133).
-- display_icon / hidden are NOT carried (editor layer).
CREATE TABLE entity_class (
  id                BIGINT NOT NULL DEFAULT nextval('seq_entity_class'),
  name              VARCHAR NOT NULL CHECK (regexp_matches(name, '^[A-Za-z0-9_]+$')),  -- #2373 charset
  description       VARCHAR,
  active_by_default BOOLEAN NOT NULL DEFAULT TRUE,   -- resolution semantics (SDP Part 1)
  required          BOOLEAN NOT NULL DEFAULT FALSE,  -- completeness marker (#3133)
  display_order     INTEGER,                         -- authored template order (#691)
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_entity_class ON entity_class (cur_id);
-- case-preserving storage, case-insensitive uniqueness (#1302, Decision 11):
CREATE UNIQUE INDEX uq_entity_class_name ON entity_class (cur_name);

-- Named class-set (Decision 5, replaces superclass_subclass): union-type
-- semantics with inheritance connotations stripped. Usable as a dimension
-- type wherever a class can be; not a class, cannot have entities, no
-- chains/nesting. Split into a header + member relation so a set has a
-- stable id referenceable from entity_class_dimension (deviation D2; the
-- member relation is the plan's (set, member_class) table).
CREATE TABLE class_set (
  id                BIGINT NOT NULL DEFAULT nextval('seq_class_set'),
  name              VARCHAR NOT NULL CHECK (regexp_matches(name, '^[A-Za-z0-9_]+$')),  -- class-like name
  description       VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_class_set ON class_set (cur_id);
CREATE UNIQUE INDEX uq_class_set_name ON class_set (cur_name);

CREATE TABLE class_set_member (
  class_set_id      BIGINT NOT NULL,
  member_class_id   BIGINT NOT NULL,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_s BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN class_set_id END) VIRTUAL,
  cur_c BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN member_class_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_class_set_member ON class_set_member (cur_s, cur_c);
-- Entity-name uniqueness across a set's members (#2364, composes with
-- Decision 11): cross-table -> audit_class_set_entity_name_dup view.

-- N-dimensional classes (SDP Part 1 `entity_class_dimension`, carried as-is;
-- Decision 5 rider: a dimension may be typed by a class OR by a class-set).
CREATE TABLE entity_class_dimension (
  entity_class_id        BIGINT NOT NULL,
  position               TINYINT NOT NULL CHECK (position >= 1),
  dimension_class_id     BIGINT,   -- exactly one of the two typing columns:
  dimension_class_set_id BIGINT,
  valid_from_commit      BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit        BIGINT REFERENCES commit(id),
  version                SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  CHECK ((dimension_class_id IS NULL) <> (dimension_class_set_id IS NULL)),
  cur_c BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_class_id END) VIRTUAL,
  cur_p TINYINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN position END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_ec_dimension ON entity_class_dimension (cur_c, cur_p);

-- Entity (SDP Part 1 `entity`): day-one name rules; ids durable (#3218).
CREATE TABLE entity (
  id                BIGINT NOT NULL DEFAULT nextval('seq_entity'),
  entity_class_id   BIGINT NOT NULL,
  name              VARCHAR NOT NULL CHECK (regexp_matches(name, '^[A-Za-z0-9_]+$')),  -- #2373
  description       VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_cls  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_class_id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_entity ON entity (cur_id);
-- Base/base collisions impossible within a class (#1302):
CREATE UNIQUE INDEX uq_entity_name ON entity (cur_cls, cur_name);

-- N-D entity membership (SDP Part 1 `entity_element`, carried as-is).
-- Plan intent: FK-enforced so importing over missing members fails (#1015).
-- DuckDB cannot FK a versioned target (no unique id) -> writer-enforced +
-- audit_dangling_ref view (deviation D1).
CREATE TABLE entity_element (
  entity_id         BIGINT NOT NULL,
  position          TINYINT NOT NULL CHECK (position >= 1),
  element_entity_id BIGINT NOT NULL,   -- may itself be an N-D entity (Decision 5 resolution of #2364)
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_e BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL,
  cur_p TINYINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN position END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_entity_element ON entity_element (cur_e, cur_p);

-- Geographic position is model data, not presentation (SDP Part 1
-- `entity_location`); graph-layout pseudo-coordinates do NOT live here
-- (Decision 7).
CREATE TABLE entity_location (
  entity_id         BIGINT NOT NULL,
  lat               DOUBLE,
  lon               DOUBLE,
  alt               DOUBLE,
  shape             VARCHAR,   -- geometry document (e.g. GeoJSON); interpretation is client-layer
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_e BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_entity_location ON entity_location (cur_e);
