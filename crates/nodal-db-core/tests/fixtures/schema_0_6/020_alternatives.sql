-- 020_alternatives.sql — alternatives, scenarios, entity activation, and the
-- generalized group-membership relation.
-- SDP Part 1 rows `alternative`, `scenario`, `scenario_alternative`,
-- `entity_alternative`, `entity_group` (replaced by membership, Decision 3);
-- SDP Part 2 "Scenario and alternative resolution".

-- Alternative (carried; SDP Part 1). Default `Base` auto-created in
-- 900_bootstrap.sql (#2331). Deletion is RESTRICT-by-default (#2606) — with
-- FK intent to versioned tables view-validated (deviation D1), the explicit
-- cascade report is writer-library behavior over audit_dangling_ref.
CREATE TABLE alternative (
  -- id > 0 guards the D3 sentinel: membership's one-current index maps base
  -- rows (alternative_id NULL) to 0, so a real alternative with id 0 would
  -- collide with — or masquerade as — base membership (critique F7):
  id                BIGINT NOT NULL DEFAULT nextval('seq_alternative') CHECK (id > 0),
  name              VARCHAR NOT NULL,
  description       VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_alternative ON alternative (cur_id);
CREATE UNIQUE INDEX uq_alternative_name ON alternative (cur_name);

-- Scenario (carried, slimmed; the `active` execution flag is NOT carried —
-- workflow layer; SDP Part 1). Scenarios are a scenario-building layer only
-- (D#1506), never model scoping.
CREATE TABLE scenario (
  id                BIGINT NOT NULL DEFAULT nextval('seq_scenario'),
  name              VARCHAR NOT NULL,
  description       VARCHAR,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_id   BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN id END) VIRTUAL,
  cur_name VARCHAR GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN lower(name) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_scenario ON scenario (cur_id);
CREATE UNIQUE INDEX uq_scenario_name ON scenario (cur_name);

-- Rank-ordered scenario contents (carried; SDP Part 1). Rank drives
-- deterministic last-wins resolution in the views (#2409): higher rank wins.
CREATE TABLE scenario_alternative (
  scenario_id       BIGINT NOT NULL,
  alternative_id    BIGINT NOT NULL,
  rank              INTEGER NOT NULL CHECK (rank >= 1),
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_s  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN scenario_id END) VIRTUAL,
  cur_a  BIGINT  GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN alternative_id END) VIRTUAL,
  cur_r  INTEGER GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN rank END) VIRTUAL
);
-- unique on (scenario, rank) and (scenario, alternative), current-scoped:
CREATE UNIQUE INDEX uq_scenario_alt_rank ON scenario_alternative (cur_s, cur_r);
CREATE UNIQUE INDEX uq_scenario_alt_alt  ON scenario_alternative (cur_s, cur_a);

-- Entity activation (SDP Part 1 `entity_alternative`, carried). Resolved by
-- the same rank-ordered views as values; clients never write rows that
-- restate the class default (#3010 — audited, never blocked).
CREATE TABLE entity_alternative (
  entity_id         BIGINT NOT NULL,
  alternative_id    BIGINT NOT NULL,
  active            BOOLEAN NOT NULL,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_e BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN entity_id END) VIRTUAL,
  cur_a BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN alternative_id END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_entity_alternative ON entity_alternative (cur_e, cur_a);

-- Generalized membership relation (Decision 3, replaces entity_group and
-- generalizes #1553 bags). A group is an ordinary entity of any class;
-- cross-class by construction, nestable (a member may be a group).
-- alternative_id NULL = structural/base membership; alternative-scoped rows
-- add or remove (`included`) members, overriding base per (group, member)
-- under the scenario's alternative ranking — same override pattern as values.
-- Resolution lives in views (090). entity_group rows migrate 1:1 as the
-- same-class base-membership special case.
CREATE TABLE membership (
  group_entity_id   BIGINT NOT NULL,
  member_entity_id  BIGINT NOT NULL,
  alternative_id    BIGINT,            -- NULL = base membership
  included          BOOLEAN NOT NULL DEFAULT TRUE,
  valid_from_commit BIGINT NOT NULL REFERENCES commit(id),
  valid_to_commit   BIGINT REFERENCES commit(id),
  version           SMALLINT NOT NULL DEFAULT 1,
  CHECK (valid_to_commit IS NULL OR valid_to_commit > valid_from_commit),  -- span coherence (critique F2)
  cur_g BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN group_entity_id END) VIRTUAL,
  cur_m BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN member_entity_id END) VIRTUAL,
  -- COALESCE sentinel 0 (ids start at 1): NULL alternative_id is a *key
  -- value* (base row), but NULLs are distinct in unique indexes, so base
  -- rows must map to a real sentinel to be one-current-constrained
  -- (deviation D3):
  cur_a BIGINT GENERATED ALWAYS AS (CASE WHEN valid_to_commit IS NULL THEN coalesce(alternative_id, 0) END) VIRTUAL
);
CREATE UNIQUE INDEX one_current_membership ON membership (cur_g, cur_m, cur_a);
