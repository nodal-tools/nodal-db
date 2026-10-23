-- 110_views_history.sql — history reads (VMP Part 5 "Reading history"):
-- as-of-commit macros (one span predicate on from/to tiers; current UNION
-- `_hist` on the array tier), the cross-tier value timeline for the history
-- viewer, and the untrusted-span surface for the recovery protocol.

-- The span predicate: valid_from_commit <= c AND (valid_to_commit IS NULL
-- OR valid_to_commit > c). Macros because views take no parameters.
-- Coverage is COMPLETE over the mutable tables (critique M4): every from/to
-- table has an _asof macro, so any past state — effective units, defaults,
-- metadata, type declarations — reconstructs without hand-rolled predicates,
-- EXCEPT behind an erasure marker, where reconstruction fails loudly (below).

-- ---------------------------------------------------------------------------
-- The as-of erasure guard (erasure_compaction_plan §1.3, decision D-E3
-- 2026-09-19; critique B1/M1 — both verified empirically on DuckDB 1.5.5;
-- re-verified on 2.0.0 by the erasure tests).
--
-- Every macro body below carries, as the FIRST conjunct of its WHERE clause:
--
--   (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
--    OR error('as-of ' || c || ' is unreliable: ...'))
--
-- Contract: any as-of read at a commit c older than the newest erasure marker
-- FAILS LOUDLY — content that existed at c was later physically removed, so a
-- whole-table as-of result at c is genuinely incomplete. The guard is FULLY
-- CONSERVATIVE in v1 (D-E3, "ok let's be blunt for now"): the macros take no
-- alternative argument, so even reads that would only touch unaffected
-- alternatives error. Alternative-precise two-arg variants are a deferred
-- read-surface addition, pulled forward when a customer needs pre-erase
-- time-travel inside output DBs. For c at or after the newest marker, and on
-- any marker-free database, every macro is exact and silent.
--
-- WHY the WHERE-clause form, and ONLY that form (critique B1 — do not
-- "simplify" this; the alternatives were tried and are silently broken):
-- DuckDB's projection pushdown prunes unreferenced projections, deleting an
-- error() expression that rides a projection. Verified failures with a
-- matching marker present, on DuckDB 1.5.5 (the guard re-verified on 2.0.0):
--   (a) a CROSS JOIN guard whose column the body never references — rows
--       returned, NO error;
--   (b) a NULL-padded UNION-ALL guard leg — body rows returned PLUS a phantom
--       all-NULL row, NO error;
--   (c) even with a correct macro, a consumer's count(*) or column-pruned
--       projection strips a projection-riding guard.
-- These forms are FORBIDDEN. Filter expressions are not prunable: the
-- WHERE-clause form raises under bare SELECT *, count(*), pruned projections,
-- joins, and — because the uncorrelated EXISTS subplan executes regardless of
-- outer rows — even over an empty table (verified, all shapes, both
-- directions). The message is fixed text (a WHERE guard cannot cheaply
-- interpolate per-marker detail); `audit_erasure` (120) carries the detail.
-- The guard's soundness rests on optimizer behavior, not a documented engine
-- contract: the consumer-shape sweep (smoke_test.py + tests/erasure.rs) is
-- the PERMANENT regression tripwire for engine upgrades — never remove it.
-- Cost: one probe of a tiny append-only table per as-of call.
-- ---------------------------------------------------------------------------

CREATE MACRO metadata_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name, cur_val) FROM metadata
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_metadata_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_m) FROM entity_metadata
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_value_metadata_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_p, cur_a, cur_m) FROM parameter_value_metadata
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_class_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM entity_class
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO class_set_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM class_set
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO class_set_member_asof(c) AS TABLE
SELECT * EXCLUDE (cur_s, cur_c) FROM class_set_member
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_class_dimension_asof(c) AS TABLE
SELECT * EXCLUDE (cur_c, cur_p) FROM entity_class_dimension
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM entity
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_location_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e) FROM entity_location
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_element_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_p) FROM entity_element
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO alternative_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM alternative
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO scenario_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM scenario
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO scenario_alternative_asof(c) AS TABLE
SELECT * EXCLUDE (cur_s, cur_a, cur_r) FROM scenario_alternative
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_alternative_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_a) FROM entity_alternative
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO membership_asof(c) AS TABLE
SELECT * EXCLUDE (cur_g, cur_m, cur_a) FROM membership
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO units_of_measure_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM units_of_measure
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_value_list_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM parameter_value_list
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO list_value_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_l, cur_o) FROM list_value
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_definition_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM parameter_definition
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_parameter_unit_asof(c) AS TABLE
SELECT * EXCLUDE (cur_e, cur_p) FROM entity_parameter_unit
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_type_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id) FROM parameter_type
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO parameter_type_dimension_asof(c) AS TABLE
SELECT * EXCLUDE (cur_t, cur_p) FROM parameter_type_dimension
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO display_mode_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id, cur_name) FROM display_mode
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO entity_class_display_mode_asof(c) AS TABLE
SELECT * EXCLUDE (cur_dm, cur_ec) FROM entity_class_display_mode
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO semantic_annotation_asof(c) AS TABLE
SELECT * EXCLUDE (cur_id) FROM semantic_annotation
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);
-- (no _asof macro for `run`: append-only, reconstructed by input_commit_id)

-- value tiers as of commit c
CREATE MACRO value_scalar_asof(c) AS TABLE
SELECT * EXCLUDE (ce, cp, ca) FROM value_scalar
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO value_long_asof(c) AS TABLE
SELECT * EXCLUDE (ce, cp, ca, c1, c2, c3, c4) FROM value_long
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

-- array tier: current UNION ALL `_hist` under the same span predicate
-- (VMP Part 5). Column shapes normalized to the hist layout: every real
-- column of value_array_hist, in its order (array_id included — the physical
-- row handle; table-mirror surfaces show it). The erasure guard sits in BOTH
-- legs' WHERE clauses — either leg alone may carry rows. Kept as a plain scan:
-- a narrow-first self-join per leg measured slower (one leg is always empty
-- for a point read, and an empty key side still scans the payload).
CREATE MACRO value_array_asof(c) AS TABLE
SELECT array_id, entity_id, parameter_definition_id, alternative_id, parameter_type_id,
       valid_from_commit, version, axis1_id, axis2_id, axis3_id, axis4_id,
       values, values_int, v_min, v_max,
       NULL::BIGINT AS valid_to_commit, TRUE AS content_retained
FROM value_array
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c
UNION ALL
SELECT array_id, entity_id, parameter_definition_id, alternative_id, parameter_type_id,
       valid_from_commit, version, axis1_id, axis2_id, axis3_id, axis4_id,
       values, values_int, v_min, v_max,
       valid_to_commit, content_retained
FROM value_array_hist
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND valid_to_commit > c;

-- definition-value mirrors (uniformly from/to, no twin — VMP Part 3)
CREATE MACRO definition_value_scalar_asof(c) AS TABLE
SELECT * EXCLUDE (cp, cs) FROM definition_value_scalar
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO definition_value_long_asof(c) AS TABLE
SELECT * EXCLUDE (cp, cs, c1, c2, c3, c4) FROM definition_value_long
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

CREATE MACRO definition_value_array_asof(c) AS TABLE
SELECT * EXCLUDE (cp, cs) FROM definition_value_array
WHERE (NOT EXISTS (SELECT 1 FROM erasure_marker em WHERE em.at_commit > c)
       OR error('as-of ' || c || ' is unreliable: content erased at a later commit — see erasure_marker / audit_erasure'))
  AND valid_from_commit <= c AND (valid_to_commit IS NULL OR valid_to_commit > c);

-- Cross-tier value timeline for the history viewer: every version of every
-- key triple across all three tiers, one logical timeline (the key triple
-- is the history link across tier transitions — VMP Part 3). NOT guarded:
-- it takes no as-of commit and reports what physically exists — an erased
-- alternative simply contributes no rows (the marker + audit_erasure carry
-- the "content erased" annotation for the viewer).
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

-- Untrusted/stubbed history spans (left by the recovery protocol or the
-- retention policy): the viewer renders these as "changed, content not retained"
-- or "unknown before commit c" instead of silently wrong answers.
CREATE VIEW history_untrusted_span AS
SELECT * FROM value_timeline WHERE NOT content_retained;
