-- 050_values.sql — the three physical value tiers (VMP Part 3), the array
-- tier's `_hist` twin (VMP Part 5 hybrid history), and the definition-scoped
-- default/prefill mirrors (#1598).
-- One-hop rule (VMP binding decision): every tier's table carries the key
-- triple (entity_id, parameter_definition_id, alternative_id) on the row
-- that holds the data. FK intent to entity/definition/alternative is
-- view-validated (deviation D1); commit and axis FKs are engine-enforced.

-- Tier L, rank 0: scalars. Current AND superseded rows in one table;
-- valid_to_commit IS NULL marks current (hybrid, VMP Part 5).
CREATE TABLE value_scalar (
  entity_id                BIGINT NOT NULL,
  parameter_definition_id  BIGINT NOT NULL,
  alternative_id           BIGINT NOT NULL,
  valid_from_commit        BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit          BIGINT REFERENCES commit(id),   -- NULL = current
  version                  SMALLINT NOT NULL,              -- 1..n per key (critique #1)
  content_retained         BOOLEAN NOT NULL DEFAULT TRUE,  -- FALSE = retention stub (VMP Part 5)
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  value_kind               VARCHAR NOT NULL,  -- 'float'|'int'|'bool'|'string'|'date_time'|'duration'|… (Decision 4: additive tag, no CHECK)
  v_float DOUBLE, v_int BIGINT, v_bool BOOLEAN,
  v_str VARCHAR, v_datetime TIMESTAMP,
  v_dur_months INTEGER, v_dur_micros BIGINT,   -- duration pair, never INTERVAL (critique #9)
  -- one-current-per-key guard: any-NULL composite index entries are exempt,
  -- so superseded rows are unconstrained and a second current row is a
  -- constraint error (versioning research feature check, DuckDB 1.4.4):
  ce BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL,
  cp BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL,
  ca BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN alternative_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_scalar ON value_scalar (ce, cp, ca);
-- ART index on the key triple — required for the point-read budget (VMP Part 6):
CREATE INDEX ix_value_scalar_triple
  ON value_scalar (entity_id, parameter_definition_id, alternative_id);

-- Tier L, rank 1–4: long indexed. Many rows per value; axis-grounded
-- (critique #6): every dimension binds an axis, index identity is the
-- 0-based position into that axis, so all five index kinds round-trip.
CREATE TABLE value_long (
  entity_id                BIGINT NOT NULL,
  parameter_definition_id  BIGINT NOT NULL,
  alternative_id           BIGINT NOT NULL,
  valid_from_commit        BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit          BIGINT REFERENCES commit(id),   -- NULL = current
  version                  SMALLINT NOT NULL,              -- per (triple, index tuple)
  content_retained         BOOLEAN NOT NULL DEFAULT TRUE,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  axis1_id BIGINT NOT NULL REFERENCES axis(id),
  axis2_id BIGINT REFERENCES axis(id),
  axis3_id BIGINT REFERENCES axis(id),
  axis4_id BIGINT REFERENCES axis(id),
  -- canonical index identity: 0-based position into the bound axis; NOT
  -- NULL with 0 beyond the declared rank — what makes a real engine-
  -- enforced key possible (VMP Part 8 Q4, closed):
  i1_pos INTEGER NOT NULL,
  i2_pos INTEGER NOT NULL DEFAULT 0,
  i3_pos INTEGER NOT NULL DEFAULT 0,
  i4_pos INTEGER NOT NULL DEFAULT 0,
  -- denormalized scan accelerators for sortable kinds (derived from the
  -- axis, audited by audit_index_accelerator, never authoritative):
  i1_dt TIMESTAMP, i1_num BIGINT,
  i2_dt TIMESTAMP, i2_num BIGINT,
  i3_dt TIMESTAMP, i3_num BIGINT,
  i4_dt TIMESTAMP, i4_num BIGINT,
  value_kind VARCHAR NOT NULL,
  -- full VMP Part 2 scalar-kind set including date_time and duration
  -- (critique F9: the Part 3 sketch omitted them — a Part 2 ↔ Part 3
  -- contradiction inside the frozen plan; additive columns close it so
  -- timestamp-/duration-valued indexed parameters are representable):
  v_float DOUBLE, v_int BIGINT, v_bool BOOLEAN, v_str VARCHAR,
  v_datetime TIMESTAMP, v_dur_months INTEGER, v_dur_micros BIGINT,
  -- one-current guard over (triple, positions) — 7-column generated-column
  -- unique index (width verified on DuckDB 1.4.4):
  ce BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL,
  cp BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL,
  ca BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN alternative_id END) VIRTUAL,
  c1 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i1_pos END) VIRTUAL,
  c2 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i2_pos END) VIRTUAL,
  c3 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i3_pos END) VIRTUAL,
  c4 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i4_pos END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_long ON value_long (ce, cp, ca, c1, c2, c3, c4);
CREATE INDEX ix_value_long_triple
  ON value_long (entity_id, parameter_definition_id, alternative_id);

-- Tier A: dense float arrays. Current table holds ONLY live rows (the
-- hottest scan surface physically cannot degrade — VMP Part 5); history in
-- the `_hist` twin. One row per value; rank = number of non-NULL axes;
-- `values` is flattened row-major in declared dimension order, length =
-- product of axis lengths (audited), contains no NULLs (canonicalization
-- step 1). Float OR int payload: dense-numeric is the entire performance case
-- (VMP Part 3); the `values_int` sibling column (VMP Part 3 reserved door) now
-- carries dense INTEGER arrays without long-tier row explosion.
CREATE TABLE value_array (
  entity_id                BIGINT NOT NULL,
  parameter_definition_id  BIGINT NOT NULL,
  alternative_id           BIGINT NOT NULL,
  valid_from_commit        BIGINT NOT NULL REFERENCES commit(id),
  version                  SMALLINT NOT NULL,
  axis1_id BIGINT NOT NULL REFERENCES axis(id),
  axis2_id BIGINT REFERENCES axis(id),
  axis3_id BIGINT REFERENCES axis(id),
  axis4_id BIGINT REFERENCES axis(id),
  values     DOUBLE[],                -- dense float payload; NULL iff values_int populated
  values_int BIGINT[],                -- dense INTEGER payload (VMP Part 3 door): categorical
                                      -- rasters (land-use/soil/Köppen), integer masks/series
  CHECK ((values IS NULL) != (values_int IS NULL)),  -- exactly one populated (current table only)
  v_min DOUBLE, v_max DOUBLE,        -- per-row pruning metadata (Timescale lesson, VMP Part 3);
                                      -- DOUBLE for both kinds (int min/max cast when exact, else NULL)
  PRIMARY KEY (entity_id, parameter_definition_id, alternative_id)   -- uniqueness of the triple, engine-enforced (VMP Part 2)
);

-- Array-tier history twin (VMP Part 5): same columns in the same order
-- (so the pre-image copy is `INSERT INTO value_array_hist SELECT *, c,
-- retained FROM value_array WHERE key`), plus the two history columns.
-- NO foreign-key constraints: hist rows must never block dimension/axis
-- deletion (critique #2); referential integrity of history is the
-- offload/GC protocol's job.
CREATE TABLE value_array_hist (
  entity_id                BIGINT NOT NULL,
  parameter_definition_id  BIGINT NOT NULL,
  alternative_id           BIGINT NOT NULL,
  valid_from_commit        BIGINT NOT NULL,
  version                  SMALLINT NOT NULL,
  axis1_id BIGINT NOT NULL,
  axis2_id BIGINT,
  axis3_id BIGINT,
  axis4_id BIGINT,
  values     DOUBLE[],                -- NULL when content_retained = FALSE (stub)
  values_int BIGINT[],                -- sibling; NULL for float rows or stubs (NO XOR: stubs null both)
  v_min DOUBLE, v_max DOUBLE,
  valid_to_commit          BIGINT NOT NULL,
  content_retained         BOOLEAN NOT NULL DEFAULT TRUE,
  CHECK (valid_to_commit > valid_from_commit)   -- span coherence (critique F2)
);
CREATE INDEX ix_value_array_hist_triple
  ON value_array_hist (entity_id, parameter_definition_id, alternative_id);

-- ---------------------------------------------------------------------------
-- default_value / prefill_value mirrors (#1598; VMP Part 3 end): same three
-- shapes, definition-scoped (columns minus entity_id/alternative_id, plus a
-- slot discriminator — deviation D5: one table per shape with a `slot`
-- column instead of six tables). Mutable authored content: uniformly the
-- from/to-commit pattern, all three shapes; the array-shaped mirror gets NO
-- `_hist` twin (defaults are small and low-churn; dead payload rows wait
-- for compaction).

CREATE TABLE definition_value_scalar (
  parameter_definition_id  BIGINT NOT NULL,
  slot                     VARCHAR NOT NULL CHECK (slot IN ('default_value', 'prefill_value')),
  valid_from_commit        BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit          BIGINT REFERENCES commit(id),
  version                  SMALLINT NOT NULL,
  content_retained         BOOLEAN NOT NULL DEFAULT TRUE,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  value_kind               VARCHAR NOT NULL,
  v_float DOUBLE, v_int BIGINT, v_bool BOOLEAN,
  v_str VARCHAR, v_datetime TIMESTAMP,
  v_dur_months INTEGER, v_dur_micros BIGINT,
  cp BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL,
  cs VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN slot END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_def_scalar ON definition_value_scalar (cp, cs);

CREATE TABLE definition_value_long (
  parameter_definition_id  BIGINT NOT NULL,
  slot                     VARCHAR NOT NULL CHECK (slot IN ('default_value', 'prefill_value')),
  valid_from_commit        BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit          BIGINT REFERENCES commit(id),
  version                  SMALLINT NOT NULL,
  content_retained         BOOLEAN NOT NULL DEFAULT TRUE,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  axis1_id BIGINT NOT NULL REFERENCES axis(id),
  axis2_id BIGINT REFERENCES axis(id),
  axis3_id BIGINT REFERENCES axis(id),
  axis4_id BIGINT REFERENCES axis(id),
  i1_pos INTEGER NOT NULL,
  i2_pos INTEGER NOT NULL DEFAULT 0,
  i3_pos INTEGER NOT NULL DEFAULT 0,
  i4_pos INTEGER NOT NULL DEFAULT 0,
  i1_dt TIMESTAMP, i1_num BIGINT,
  i2_dt TIMESTAMP, i2_num BIGINT,
  i3_dt TIMESTAMP, i3_num BIGINT,
  i4_dt TIMESTAMP, i4_num BIGINT,
  value_kind VARCHAR NOT NULL,
  -- full scalar-kind set incl. date_time/duration (critique F9, as value_long):
  v_float DOUBLE, v_int BIGINT, v_bool BOOLEAN, v_str VARCHAR,
  v_datetime TIMESTAMP, v_dur_months INTEGER, v_dur_micros BIGINT,
  cp BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL,
  cs VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN slot END) VIRTUAL,
  c1 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i1_pos END) VIRTUAL,
  c2 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i2_pos END) VIRTUAL,
  c3 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i3_pos END) VIRTUAL,
  c4 INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN i4_pos END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_def_long ON definition_value_long (cp, cs, c1, c2, c3, c4);

CREATE TABLE definition_value_array (
  parameter_definition_id  BIGINT NOT NULL,
  slot                     VARCHAR NOT NULL CHECK (slot IN ('default_value', 'prefill_value')),
  valid_from_commit        BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit          BIGINT REFERENCES commit(id),   -- from/to even on the array shape (no twin here)
  version                  SMALLINT NOT NULL,
  content_retained         BOOLEAN NOT NULL DEFAULT TRUE,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  axis1_id BIGINT NOT NULL REFERENCES axis(id),
  axis2_id BIGINT REFERENCES axis(id),
  axis3_id BIGINT REFERENCES axis(id),
  axis4_id BIGINT REFERENCES axis(id),
  values     DOUBLE[],
  values_int BIGINT[],                -- default/prefill int-array payload; XOR is writer-enforced (no twin)
  v_min DOUBLE, v_max DOUBLE,
  cp BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN parameter_definition_id END) VIRTUAL,
  cs VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN slot END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_def_array ON definition_value_array (cp, cs);
