-- 110_views_history.sql — history reads (VMP Part 5 "Reading history"):
-- as-of-commit macros (one span predicate on from/to tiers; current UNION
-- `_hist` on the array tier), the cross-tier value timeline for the history
-- viewer, and the untrusted-span surface for the recovery protocol.

-- The span predicate: valid_from_commit <= c AND (valid_to_commit IS NULL
-- OR valid_to_commit > c). Macros because views take no parameters.
-- Coverage is COMPLETE over the mutable tables (critique M4): every from/to
-- table has an _asof macro, so any past state — effective units, defaults,
-- metadata, type declarations — reconstructs without hand-rolled predicates.

CREATE MACRO metadata_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name, cur_val) FROM metadata
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_metadata_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_m) FROM entity_metadata
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_value_metadata_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_p, cur_a, cur_m) FROM parameter_value_metadata
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_class_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM entity_class
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO class_set_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM class_set
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO class_set_member_asof(c) AS TABLE
SELECT * EXCLUDE (cur_s, cur_c) FROM class_set_member
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_class_dimension_asof(c) AS TABLE
SELECT * EXCLUDE (cur_c, cur_p) FROM entity_class_dimension
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM entity
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_location_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e) FROM entity_location
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_element_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_p) FROM entity_element
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO alternative_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM alternative
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO scenario_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM scenario
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO scenario_alternative_asof(c) AS TABLE
SELECT * EXCLUDE (cur_s, cur_a, cur_r) FROM scenario_alternative
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_alternative_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_a) FROM entity_alternative
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO membership_asof(c) AS TABLE
SELECT * EXCLUDE (cur_g, cur_m, cur_a) FROM membership
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO units_of_measure_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM units_of_measure
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_value_list_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM parameter_value_list
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO list_value_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_l, cur_o) FROM list_value
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_definition_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM parameter_definition
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_parameter_unit_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_p) FROM entity_parameter_unit
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_type_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id) FROM parameter_type
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_type_dimension_asof(c) AS TABLE
SELECT * EXCLUDE (cur_t, cur_p) FROM parameter_type_dimension
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO display_mode_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM display_mode
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_class_display_mode_asof(c) AS TABLE
SELECT * EXCLUDE (cur_dm, cur_ec) FROM entity_class_display_mode
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO semantic_annotation_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id) FROM semantic_annotation
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);
-- (no _asof macro for `run`: append-only, reconstructed by input_commit_id)

-- value tiers as of commit c
CREATE MACRO value_scalar_asof(c) AS TABLE
SELECT * EXCLUDE (ce, cp, ca) FROM value_scalar
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO value_long_asof(c) AS TABLE
SELECT * EXCLUDE (ce, cp, ca, c1, c2, c3, c4) FROM value_long
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

-- array tier: current UNION ALL `_hist` under the same span predicate
-- (VMP Part 5). Column shapes normalized to the hist layout.
CREATE MACRO value_array_asof(c) AS TABLE
SELECT entity_id, parameter_definition_id, alternative_id, valid_from_commit,
       version, axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max,
       NULL::BIGINT AS valid_to_commit, TRUE AS content_retained
FROM value_array WHERE valid_from_commit <= c
UNION ALL
SELECT entity_id, parameter_definition_id, alternative_id, valid_from_commit,
       version, axis1_id, axis2_id, axis3_id, axis4_id, values, values_int, v_min, v_max,
       valid_to_commit, content_retained
FROM value_array_hist
WHERE valid_from_commit <= c AND valid_to_commit > c;

-- definition-value mirrors (uniformly from/to, no twin — VMP Part 3)
CREATE MACRO definition_value_scalar_asof(c) AS TABLE
SELECT * EXCLUDE (cp, cs) FROM definition_value_scalar
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO definition_value_long_asof(c) AS TABLE
SELECT * EXCLUDE (cp, cs, c1, c2, c3, c4) FROM definition_value_long
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO definition_value_array_asof(c) AS TABLE
SELECT * EXCLUDE (cp, cs) FROM definition_value_array
WHERE valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

-- Cross-tier value timeline for the history viewer: every version of every
-- key triple across all three tiers, one logical timeline (the key triple
-- is the history link across tier transitions — VMP Part 3).
CREATE VIEW value_timeline AS
SELECT entity_id, parameter_definition_id, alternative_id, 'scalar' AS tier,
       version, valid_from_commit, valid_to_commit, content_retained
FROM value_scalar
UNION ALL
SELECT DISTINCT entity_id, parameter_definition_id, alternative_id, 'long',
       version, valid_from_commit, valid_to_commit, content_retained
FROM value_long
UNION ALL
SELECT entity_id, parameter_definition_id, alternative_id, 'array',
       version, valid_from_commit, NULL, TRUE
FROM value_array
UNION ALL
SELECT entity_id, parameter_definition_id, alternative_id, 'array',
       version, valid_from_commit, valid_to_commit, content_retained
FROM value_array_hist;

-- Untrusted/stubbed history spans (VMP Part 5 recovery protocol +
-- retention): the viewer renders these as "changed, content not retained"
-- or "unknown before commit c" instead of silently wrong answers.
CREATE VIEW history_untrusted_span AS
SELECT * FROM value_timeline WHERE NOT content_retained;
