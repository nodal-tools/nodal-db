-- 055_provenance.sql — the `run` table (Decision 13, toolchain_role_design
-- T2.1 "load-bearing minimum").
-- A run is a production event: the immutable fact that a tool consumed an
-- input commit and produced outputs into a result-alternative. It makes
-- results first-class and provenance-carrying (PROV-O prov:Activity) without
-- putting producing-tool identity on the generic `alternative`.
-- Append-only / immutable, like `commit` (001_meta.sql): no from/to span,
-- no version, id is a real PRIMARY KEY. A correction is a new run row, never
-- an in-place edit — so no _current view and no _asof macro are needed.

CREATE TABLE run (
  id                    BIGINT PRIMARY KEY DEFAULT nextval('seq_run'),
  tool_entity_id        BIGINT NOT NULL,   -- the tool that ran (an entity; tools dogfood as entities, Decision 13). FK-intent -> entity (D1 audit)
  result_alternative_id BIGINT NOT NULL,   -- the alternative holding this run's outputs. FK-intent -> alternative (D1 audit)
  -- commit is append-only with a real PK, so these two are enforceable FKs
  -- (unlike the versioned targets above — deviation D1):
  input_commit_id       BIGINT NOT NULL REFERENCES commit(id),  -- exact input set (as-of reconstructs it)
  produced_at_commit_id BIGINT REFERENCES commit(id),           -- commit the outputs landed in (NULL until recorded)
  solver                VARCHAR,           -- e.g. the LP/MILP solver used
  tool_version          VARCHAR,           -- released tool version
  git_ref               VARCHAR,           -- exact source revision
  status                VARCHAR,           -- open tag (Decision 4): e.g. optimal|infeasible|error|...
  solved_at             TIMESTAMP,         -- solve wall-clock (informational, never a validity edge)
  external_artifact_uri VARCHAR,           -- sidecar for dense EMT/RMS output traces (T3 offload)
  -- temporal coherence: outputs cannot land before the input set they consumed
  -- (commit ids are monotonic). Mirrors the span-coherence CHECKs on versioned
  -- tables (valid_to > valid_from).
  CHECK (produced_at_commit_id IS NULL OR produced_at_commit_id >= input_commit_id)
);
-- Readers reach a run by the alternative it produced ("what made these
-- values?"); the deferred alternative.produced_by_run_id pointer would serve
-- the same path (toolchain_role_design D-T6).
CREATE INDEX ix_run_result_alt ON run (result_alternative_id);
