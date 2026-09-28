-- 060_display.sql — authored display modes (Spine-Toolbox #2492).
-- REOPENS the SDP Part 1 / Decision 7 drop (amended by Decision 12): display
-- modes are the tool developer's AUTHORED, shareable presentation model
-- (creator / user / result-checking), not one user's transient view state, so
-- they travel with the data. Per-user filters/zoom/selection stay editor-side
-- (sidecar), unchanged. See specs/display_mode_schema_proposal.md.
-- Category = authored schema-level config (same as entity_class): versioned
-- identically (from/to-commit), so the companion surface mirrors entity_class
-- (current view, as-of macro, dangling-ref audit).

-- A named display mode (Spine `display_mode`): carried as authored intent
-- (Decision 12).
CREATE TABLE display_mode (
  id                BIGINT NOT NULL DEFAULT nextval('seq_display_mode'),
  name              VARCHAR NOT NULL,  -- name is free-text (was #2373 [A-Za-z0-9_]+; relaxed for Spine/FlexTool interop — writer enforces a non-empty/no-control-char/trimmed hygiene floor)
  description       VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_display_mode ON display_mode (cur_id);
-- case-preserving storage, case-insensitive uniqueness (#1302, Decision 11):
CREATE UNIQUE INDEX uq_display_mode_name ON display_mode (cur_name);

-- Per (mode, entity_class) presentation (Spine `entity_class_display_mode`,
-- #2492): visibility (greyed-not-hidden per #2492), per-mode order, colours.
-- FK intent to display_mode / entity_class is view-validated (deviation D1:
-- DuckDB cannot FK a versioned target) -> audit_dangling_ref. display_status
-- CHECK matches Spine exactly (visible|hidden|greyed_out); font/background
-- colours are hex strings, interpretation is client-layer (cf.
-- entity_location.shape).
CREATE TABLE entity_class_display_mode (
  display_mode_id   BIGINT NOT NULL,
  entity_class_id   BIGINT NOT NULL,
  display_status    VARCHAR NOT NULL DEFAULT 'visible' CHECK (display_status IN ('visible','hidden','greyed_out')),  -- #2492
  display_order     INTEGER,                             -- per-mode order (distinct from entity_class.display_order, #691)
  font_color        VARCHAR,
  background_color  VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_dm BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN display_mode_id END) VIRTUAL,
  cur_ec BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_class_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_ecdm ON entity_class_display_mode (cur_dm, cur_ec);
