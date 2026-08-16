-- 040_axes.sql — shared axis objects (VMP Part 4) and their materialized
-- child tables (VMP Part 1 index kinds).
-- Axes are immutable once written (a changed grid is a new axis), created by
-- the writer library via content-verified dedup (hash is an accelerator,
-- content is the truth — critique #7), garbage-collected only at compaction.
-- Immutable -> no from/to columns, and axis.id can be a real PK: the one
-- family of FK targets the engine can enforce (with commit).

CREATE TABLE axis (
  id           BIGINT PRIMARY KEY DEFAULT nextval('seq_axis'),
  kind         VARCHAR NOT NULL,      -- VMP Part 1 enum: datetime|period|label|integer|time_pattern; additive, no CHECK
  length       INTEGER NOT NULL CHECK (length >= 1),
  content_hash UBIGINT NOT NULL,      -- dedup ACCELERATOR, deliberately NOT unique (critique #7)
  -- parametric forms — regular datetime/integer axes MUST be stored
  -- parametrically (parametric-first canonical form, VMP Part 4); steps are
  -- the (months, micros) duration pair, never INTERVAL (critique #9):
  p_start_ts    TIMESTAMP,
  p_step_months INTEGER,
  p_step_micros BIGINT,
  p_start_int   BIGINT,
  p_step_int    BIGINT,
  calendar     VARCHAR,               -- expressed, never asserted (VMP Part 1); hash input
  timezone     VARCHAR                -- expressed, never asserted; hash input
);
CREATE INDEX axis_hash ON axis (content_hash);

-- Materialized child tables for irregular/labelled kinds. pos is the
-- physical slot in flattened arrays (0-based); ordinal is semantic order
-- (Arrow `permutation` in relational form — VMP Part 4).

-- Irregular datetime axes; must be strictly increasing (writer-checked —
-- the only datetime-axis validation; representative-year note, VMP Part 1).
CREATE TABLE axis_datetime (
  axis_id BIGINT NOT NULL REFERENCES axis(id),
  pos     INTEGER NOT NULL CHECK (pos >= 0),
  ts      TIMESTAMP NOT NULL,
  PRIMARY KEY (axis_id, pos)
);

-- Period axes: CF bounds pair [start, end), both stored — the end is never
-- derived (critique #10); years_presented is the authored/display form.
CREATE TABLE axis_period (
  axis_id         BIGINT NOT NULL REFERENCES axis(id),
  pos             INTEGER NOT NULL CHECK (pos >= 0),
  start_ts        TIMESTAMP NOT NULL,
  end_ts          TIMESTAMP NOT NULL CHECK (end_ts > start_ts),
  years_presented DOUBLE,
  PRIMARY KEY (axis_id, pos)
);

-- Label axes: surrogate id is the long tier's conceptual FK target
-- (critique #6); explicit ordinal scoped to the axis (GDX global-registry
-- anti-lesson).
CREATE TABLE axis_label (
  id      BIGINT PRIMARY KEY DEFAULT nextval('seq_axis_label'),
  axis_id BIGINT NOT NULL REFERENCES axis(id),
  pos     INTEGER NOT NULL CHECK (pos >= 0),
  label   VARCHAR NOT NULL,
  ordinal INTEGER NOT NULL,
  UNIQUE (axis_id, pos),
  UNIQUE (axis_id, ordinal)
);

-- Irregular integer axes (deviation D4: not in the plan's child-table list,
-- but required — canonicalization derives axes from data, and a non-regular
-- distinct-integer set must materialize somewhere to bind long-tier
-- positions).
CREATE TABLE axis_integer (
  axis_id BIGINT NOT NULL REFERENCES axis(id),
  pos     INTEGER NOT NULL CHECK (pos >= 0),
  num     BIGINT NOT NULL,
  PRIMARY KEY (axis_id, pos)
);

-- Time-pattern axes: structured terms, not strings, so `h17-26` is
-- unrepresentable (#1569). Real per-unit range CHECKs (the v1 placeholder
-- TRUE was flagged must-fix — VMP Part 4). Units: Y year(>=1), M month 1-12,
-- D day 1-31, WD weekday 1-7, h hour 1-24, m minute 1-60, s second 1-60.
CREATE TABLE axis_time_pattern (
  axis_id     BIGINT NOT NULL REFERENCES axis(id),
  pos         INTEGER NOT NULL CHECK (pos >= 0),
  ordinal     INTEGER NOT NULL,   -- explicit per-position ordinal (patterns are set-like)
  term_no     INTEGER NOT NULL CHECK (term_no >= 1),  -- terms of one position union together
  unit        VARCHAR NOT NULL,
  range_start SMALLINT NOT NULL,
  range_end   SMALLINT NOT NULL,
  PRIMARY KEY (axis_id, pos, term_no),
  CHECK (range_start <= range_end AND
         CASE unit
           WHEN 'Y'  THEN range_start >= 1
           WHEN 'M'  THEN range_start >= 1 AND range_end <= 12
           WHEN 'D'  THEN range_start >= 1 AND range_end <= 31
           WHEN 'WD' THEN range_start >= 1 AND range_end <= 7
           WHEN 'h'  THEN range_start >= 1 AND range_end <= 24
           WHEN 'm'  THEN range_start >= 1 AND range_end <= 60
           WHEN 's'  THEN range_start >= 1 AND range_end <= 60
           ELSE FALSE
         END)
);
