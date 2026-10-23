-- 090_views_resolution.sql — scenario resolution in SQL (SDP Part 2
-- "Scenario and alternative resolution": rank-ordered deterministic
-- last-wins #2409, pre-collapse conflict exposure, two-pass membership
-- resolution with the binding scenario-contents and scenario-diff surfaces).

-- ---------------------------------------------------------------------------
-- Value resolution (whole-value granularity: uniqueness is the key triple,
-- so an alternative's value wins or loses as one unit).

-- every current key triple, with its tier (also feeds the cross-tier audit):
CREATE VIEW value_keys AS
SELECT entity_id, parameter_definition_id, alternative_id, 'scalar' AS tier
FROM value_scalar WHERE valid_to_commit IS NULL
UNION ALL
SELECT DISTINCT entity_id, parameter_definition_id, alternative_id, 'long'
FROM value_long WHERE valid_to_commit IS NULL
UNION ALL
SELECT entity_id, parameter_definition_id, alternative_id, 'array'
FROM value_array;

-- pre-collapse state: every (scenario, entity, parameter) candidate row
-- with its alternative and rank — editors render conflicts from this
-- (#2409 "expose per-alternative pre-collapse state AND the collapsed
-- result").
-- Candidacy requires a CURRENT alternative and scenario row (critique F1):
-- without the currency joins, a half-deleted alternative's values stayed
-- candidates, won by rank, then vanished at the name join — silently hiding
-- the Base value they outranked. With them, a closed-out alternative's
-- values drop out of candidacy and the next rank wins — self-healing; the
-- leftovers still surface in audit_dangling_ref.
CREATE VIEW scenario_value_candidate AS
SELECT sa.scenario_id, vk.entity_id, vk.parameter_definition_id,
       vk.alternative_id, sa.rank, vk.tier
FROM value_keys vk
JOIN scenario_alternative sa
  ON sa.alternative_id = vk.alternative_id AND sa.valid_to_commit IS NULL
JOIN alternative al ON al.id = vk.alternative_id AND al.valid_to_commit IS NULL
JOIN scenario s ON s.id = sa.scenario_id AND s.valid_to_commit IS NULL;

-- collapsed result: highest rank wins, deterministically (#2409).
CREATE VIEW scenario_value_resolved AS
SELECT scenario_id, entity_id, parameter_definition_id, alternative_id, rank, tier
FROM scenario_value_candidate
QUALIFY row_number() OVER (
  PARTITION BY scenario_id, entity_id, parameter_definition_id
  ORDER BY rank DESC) = 1;

-- conflicts: the same (entity, parameter) under >1 selected alternative,
-- before last-wins collapses it — color-coded by editors, never blocked.
CREATE VIEW scenario_value_conflict AS
SELECT *, count(*) OVER (
         PARTITION BY scenario_id, entity_id, parameter_definition_id) AS n_candidates
FROM scenario_value_candidate
QUALIFY n_candidates > 1;

-- resolved long-shape read for one scenario (macro because views take no
-- parameters; joins the collapsed winner back to the canonical rows).
CREATE MACRO resolved_value_rows(scenario_name) AS TABLE
SELECT s.name AS scenario, e.name AS entity, ec.name AS entity_class,
       pd.name AS parameter, al.name AS alternative, r.rank, vr.tier, vr.value_kind,
       vr.idx1_text, vr.idx2_text, vr.idx3_text, vr.idx4_text,
       vr.idx1_ts, vr.idx2_ts, vr.idx3_ts, vr.idx4_ts,
       vr.idx1_num, vr.idx2_num, vr.idx3_num, vr.idx4_num,
       vr.v_float, vr.v_int, vr.v_bool, vr.v_str, vr.v_datetime,
       vr.v_dur_months, vr.v_dur_micros
FROM scenario_value_resolved r
JOIN scenario s ON s.id = r.scenario_id AND s.valid_to_commit IS NULL
JOIN value_rows vr ON vr.entity_id = r.entity_id
                  AND vr.parameter_definition_id = r.parameter_definition_id
                  AND vr.alternative_id = r.alternative_id
JOIN entity e ON e.id = r.entity_id AND e.valid_to_commit IS NULL
JOIN entity_class ec ON ec.id = e.entity_class_id AND ec.valid_to_commit IS NULL
JOIN parameter_definition pd ON pd.id = r.parameter_definition_id AND pd.valid_to_commit IS NULL
JOIN alternative al ON al.id = r.alternative_id AND al.valid_to_commit IS NULL
WHERE lower(s.name) = lower(scenario_name);

-- ---------------------------------------------------------------------------
-- Membership resolution, pass 1: base membership
-- (alternative_id NULL, rank 0) overridden by the highest-ranked
-- alternative-scoped row per (scenario, group, member).
CREATE VIEW membership_resolved AS
WITH cand AS (
  SELECT s.id AS scenario_id, m.group_entity_id, m.member_entity_id, m.included, 0 AS rank
  FROM membership m
  CROSS JOIN scenario s
  WHERE m.alternative_id IS NULL AND m.valid_to_commit IS NULL AND s.valid_to_commit IS NULL
  UNION ALL
  -- alternative-scoped rows require a current alternative and scenario
  -- (critique F1, same currency rule as value candidacy):
  SELECT sa.scenario_id, m.group_entity_id, m.member_entity_id, m.included, sa.rank
  FROM membership m
  JOIN scenario_alternative sa
    ON sa.alternative_id = m.alternative_id AND sa.valid_to_commit IS NULL
  JOIN alternative al ON al.id = m.alternative_id AND al.valid_to_commit IS NULL
  JOIN scenario s ON s.id = sa.scenario_id AND s.valid_to_commit IS NULL
  WHERE m.valid_to_commit IS NULL
)
SELECT scenario_id, group_entity_id, member_entity_id, included, rank
FROM cand
QUALIFY row_number() OVER (
  PARTITION BY scenario_id, group_entity_id, member_entity_id
  ORDER BY rank DESC) = 1;

-- transitive flattening of resolved membership (included rows only).
-- Cycle guard: recursive UNION (distinct) computes a fixpoint over the
-- finite (scenario, group, member) pair set, so cycles terminate instead of
-- recursing forever (the accepted-residual-risk guard, SDP Part 2); cycles
-- themselves are surfaced by audit_membership_cycle (120).
CREATE VIEW membership_flat AS
WITH RECURSIVE flat(scenario_id, group_entity_id, member_entity_id) AS (
  SELECT scenario_id, group_entity_id, member_entity_id
  FROM membership_resolved WHERE included
  UNION
  SELECT f.scenario_id, f.group_entity_id, m.member_entity_id
  FROM flat f
  JOIN membership_resolved m
    ON m.scenario_id = f.scenario_id
   AND m.group_entity_id = f.member_entity_id
   AND m.included
)
SELECT * FROM flat;

-- Base-only membership flattening, NOT gated on scenarios (a model must
-- be definable "with everything on Base", and with
-- zero scenarios the scenario-scoped views above are empty — base
-- membership and its cycles would be invisible). Rank-0 rows only.
CREATE VIEW membership_base_flat AS
WITH RECURSIVE flat(group_entity_id, member_entity_id) AS (
  SELECT group_entity_id, member_entity_id
  FROM membership
  WHERE alternative_id IS NULL AND included AND valid_to_commit IS NULL
  UNION
  SELECT f.group_entity_id, m.member_entity_id
  FROM flat f
  JOIN membership m
    ON m.group_entity_id = f.member_entity_id
   AND m.alternative_id IS NULL AND m.included AND m.valid_to_commit IS NULL
)
SELECT * FROM flat;

-- Membership resolution, pass 2: propagate activation over
-- resolved membership. An entity's own entity_alternative row always wins
-- (rank-resolved, last-wins); absent it, deactivation propagates
-- recursively from any containing group — deactivation-wins on overlap; a
-- member's own row is a barrier to propagation through it. Absent both,
-- the class's active_by_default applies (which is why clients must not
-- write rows restating the default, #3010).
-- Refinement edges (Decision 13, multifidelity_entity_linking §7): the same
-- real thing at two resolutions (ESM plant -> its power-system components).
-- A `realizes` link is an ordinary 2-D relationship entity (position 1 =
-- coarse, position 2 = fine); its class is self-marking by the reserved name
-- prefix `realizes` (marker convention, zero schema change). Structural and
-- scenario-independent (current rows only) — it does NOT read
-- entity_activation_resolved, so there is no view cycle. This is a structural
-- resolution, never a value transform: capacity->P_max stays tool-side
-- (invariant 2). Bags are left free (not guarded) — the dedicated typed
-- class is the marker.
CREATE VIEW realizes_edge AS
SELECT ee1.element_entity_id AS coarse_id,
       ee2.element_entity_id AS fine_id
FROM entity r
JOIN entity_class ec ON ec.id = r.entity_class_id AND ec.valid_to_commit IS NULL
                    AND lower(ec.name) LIKE 'realizes%'
JOIN entity_element ee1 ON ee1.entity_id = r.id AND ee1.position = 1 AND ee1.valid_to_commit IS NULL
JOIN entity_element ee2 ON ee2.entity_id = r.id AND ee2.position = 2 AND ee2.valid_to_commit IS NULL
WHERE r.valid_to_commit IS NULL;

CREATE VIEW entity_activation_own AS
SELECT sa.scenario_id, ea.entity_id, ea.active, sa.rank
FROM entity_alternative ea
JOIN scenario_alternative sa
  ON sa.alternative_id = ea.alternative_id AND sa.valid_to_commit IS NULL
JOIN alternative al ON al.id = ea.alternative_id AND al.valid_to_commit IS NULL  -- currency (critique F1)
WHERE ea.valid_to_commit IS NULL
QUALIFY row_number() OVER (
  PARTITION BY sa.scenario_id, ea.entity_id ORDER BY sa.rank DESC) = 1;

-- Deactivation edges for the pass-2 fixpoint: the
-- ONE directed "deactivates" graph, unifying two edge kinds so the recursive
-- CTE needs a single recursive term (DuckDB allows only one). Membership
-- edges are scenario-scoped; realizes edges are scenario-independent
-- (structural), so they apply in every current scenario — inactive coarse
-- deactivates fine identically in each. Both are the same edge type in the
-- same graph (multifidelity_entity_linking §7.2).
CREATE VIEW deact_edge AS
SELECT scenario_id, group_entity_id AS from_id, member_entity_id AS to_id
FROM membership_resolved WHERE included
UNION ALL
SELECT sc.id, re.coarse_id, re.fine_id
FROM realizes_edge re
CROSS JOIN scenario sc WHERE sc.valid_to_commit IS NULL;

CREATE VIEW entity_activation_resolved AS
WITH RECURSIVE deact(scenario_id, entity_id) AS (
  -- seeds: entities whose own resolved row deactivates them. Note: a
  -- default-inactive entity (active_by_default=FALSE, no own row) is NOT a
  -- seed, so it does not propagate deactivation to members/fine children —
  -- intentional, matching membership and #3010 (don't restate the default);
  -- only an explicit deactivation propagates.
  SELECT scenario_id, entity_id FROM entity_activation_own WHERE NOT active
  UNION   -- distinct fixpoint = cycle guard (terminates membership AND realizes cycles)
  -- propagate deactivation along either edge kind, to targets lacking their
  -- own resolved row (own wins — the propagation barrier).
  SELECT de.scenario_id, de.to_id
  FROM deact d
  JOIN deact_edge de
    ON de.scenario_id = d.scenario_id
   AND de.from_id = d.entity_id
  WHERE NOT EXISTS (SELECT 1 FROM entity_activation_own o
                    WHERE o.scenario_id = de.scenario_id
                      AND o.entity_id = de.to_id)
)
SELECT s.id AS scenario_id, e.id AS entity_id,
       CASE WHEN own.entity_id IS NOT NULL THEN own.active
            WHEN dx.entity_id IS NOT NULL THEN FALSE
            ELSE ec.active_by_default END AS active,
       CASE WHEN own.entity_id IS NOT NULL THEN 'own'
            WHEN dx.entity_id IS NOT NULL THEN 'propagated'
            ELSE 'default' END AS active_source
FROM entity e
JOIN entity_class ec ON ec.id = e.entity_class_id AND ec.valid_to_commit IS NULL
CROSS JOIN scenario s
LEFT JOIN entity_activation_own own
       ON own.scenario_id = s.id AND own.entity_id = e.id
LEFT JOIN (SELECT DISTINCT scenario_id, entity_id FROM deact) dx
       ON dx.scenario_id = s.id AND dx.entity_id = e.id
WHERE e.valid_to_commit IS NULL AND s.valid_to_commit IS NULL;

-- ---------------------------------------------------------------------------
-- The two binding views of Decision 3 ("they are what makes
-- alternative-scoped membership acceptable"), as parameterized macros.

-- Resolved-scenario-contents: full flattened membership plus activation for
-- one scenario. One row per (entity, containing-group); ungrouped entities
-- appear with NULL group.
CREATE MACRO scenario_contents(scenario_name) AS TABLE
SELECT e.name AS entity, ec.name AS entity_class,
       act.active, act.active_source,
       ge.name AS in_group
FROM scenario s
JOIN entity_activation_resolved act ON act.scenario_id = s.id
JOIN entity e ON e.id = act.entity_id AND e.valid_to_commit IS NULL
JOIN entity_class ec ON ec.id = e.entity_class_id AND ec.valid_to_commit IS NULL
LEFT JOIN membership_flat mf
       ON mf.scenario_id = s.id AND mf.member_entity_id = e.id
LEFT JOIN entity ge ON ge.id = mf.group_entity_id AND ge.valid_to_commit IS NULL
WHERE s.valid_to_commit IS NULL AND lower(s.name) = lower(scenario_name);

-- Scenario-diff: membership/activation delta between two scenarios
-- (Decision 3 rider). Both names must resolve to a current scenario
-- (critique F8): a NULL sid previously made one side empty and rendered a
-- typo as a plausible "everything removed" diff; now a nonexistent name
-- yields the empty result, matching scenario_contents.
CREATE MACRO scenario_diff(scenario_a, scenario_b) AS TABLE
WITH sid AS (
  SELECT
    (SELECT id FROM scenario WHERE valid_to_commit IS NULL AND lower(name) = lower(scenario_a)) AS a,
    (SELECT id FROM scenario WHERE valid_to_commit IS NULL AND lower(name) = lower(scenario_b)) AS b
),
ok AS (SELECT 1 AS ok FROM sid WHERE sid.a IS NOT NULL AND sid.b IS NOT NULL),
ma AS (SELECT group_entity_id, member_entity_id FROM membership_flat, sid, ok WHERE scenario_id = sid.a),
mb AS (SELECT group_entity_id, member_entity_id FROM membership_flat, sid, ok WHERE scenario_id = sid.b),
aa AS (SELECT entity_id, active FROM entity_activation_resolved, sid, ok WHERE scenario_id = sid.a),
ab AS (SELECT entity_id, active FROM entity_activation_resolved, sid, ok WHERE scenario_id = sid.b)
SELECT 'membership' AS kind,
       ge.name AS group_name, me.name AS entity_name,
       (ma.member_entity_id IS NOT NULL) AS in_a,
       (mb.member_entity_id IS NOT NULL) AS in_b,
       NULL::BOOLEAN AS active_a, NULL::BOOLEAN AS active_b
FROM ma
FULL OUTER JOIN mb ON ma.group_entity_id = mb.group_entity_id
                  AND ma.member_entity_id = mb.member_entity_id
JOIN entity ge ON ge.id = coalesce(ma.group_entity_id, mb.group_entity_id) AND ge.valid_to_commit IS NULL
JOIN entity me ON me.id = coalesce(ma.member_entity_id, mb.member_entity_id) AND me.valid_to_commit IS NULL
WHERE ma.member_entity_id IS NULL OR mb.member_entity_id IS NULL
UNION ALL
SELECT 'activation', NULL, e.name, NULL, NULL, aa.active, ab.active
FROM aa
JOIN ab ON aa.entity_id = ab.entity_id
JOIN entity e ON e.id = aa.entity_id AND e.valid_to_commit IS NULL
WHERE aa.active <> ab.active;
