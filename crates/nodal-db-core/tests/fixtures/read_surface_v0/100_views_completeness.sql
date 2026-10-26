-- 100_views_completeness.sql — completeness is a property of the RESOLVED
-- view, checked at scenario level: an alternative may be incomplete, a
-- scenario should not be (SDP Part 2 "Validation philosophy"; #2710, #1900,
-- #3036, #3133). Views color-code; nothing blocks edits.

-- A scenario resolving to zero alternatives is a red-flag view result, not
-- a crash downstream (#3036).
CREATE VIEW audit_scenario_empty AS
SELECT s.id AS scenario_id, s.name AS scenario
FROM scenario s
WHERE s.valid_to_commit IS NULL
  AND NOT EXISTS (SELECT 1 FROM scenario_alternative sa
                  WHERE sa.scenario_id = s.id AND sa.valid_to_commit IS NULL);

-- Required parameters (#2710) on active entities with neither a resolved
-- value nor a definition default (default_value is the resolution fallback,
-- #1598) — per scenario. Zero-alternative scenarios are excluded (critique
-- M3): they cannot resolve anything and are already the red flag
-- audit_scenario_empty raises — flooding this view with every required
-- parameter would drown the real signal.
CREATE VIEW audit_missing_required_value AS
SELECT act.scenario_id, s.name AS scenario, e.id AS entity_id, e.name AS entity,
       pd.id AS parameter_definition_id, pd.name AS parameter
FROM entity e
JOIN parameter_definition pd
  ON pd.entity_class_id = e.entity_class_id AND pd.required AND pd.valid_to_commit IS NULL
JOIN entity_activation_resolved act ON act.entity_id = e.id AND act.active
JOIN scenario s ON s.id = act.scenario_id AND s.valid_to_commit IS NULL
WHERE e.valid_to_commit IS NULL
  AND EXISTS (SELECT 1 FROM scenario_alternative sa
              WHERE sa.scenario_id = act.scenario_id AND sa.valid_to_commit IS NULL)
  AND NOT EXISTS (SELECT 1 FROM scenario_value_resolved r
                  WHERE r.scenario_id = act.scenario_id
                    AND r.entity_id = e.id
                    AND r.parameter_definition_id = pd.id)
  AND NOT EXISTS (SELECT 1 FROM definition_value_scalar d
                  WHERE d.parameter_definition_id = pd.id
                    AND d.slot = 'default_value' AND d.valid_to_commit IS NULL)
  AND NOT EXISTS (SELECT 1 FROM definition_value_long d
                  WHERE d.parameter_definition_id = pd.id
                    AND d.slot = 'default_value' AND d.valid_to_commit IS NULL)
  AND NOT EXISTS (SELECT 1 FROM definition_value_array d
                  WHERE d.parameter_definition_id = pd.id
                    AND d.slot = 'default_value' AND d.valid_to_commit IS NULL);

-- Required classes (#3133) with no active entity in the resolved scenario.
CREATE VIEW audit_required_class_empty AS
SELECT s.id AS scenario_id, s.name AS scenario,
       ec.id AS entity_class_id, ec.name AS entity_class
FROM entity_class ec
CROSS JOIN scenario s
WHERE ec.required AND ec.valid_to_commit IS NULL AND s.valid_to_commit IS NULL
  AND NOT EXISTS (SELECT 1
                  FROM entity e
                  JOIN entity_activation_resolved act
                    ON act.entity_id = e.id AND act.scenario_id = s.id AND act.active
                  WHERE e.entity_class_id = ec.id AND e.valid_to_commit IS NULL);
