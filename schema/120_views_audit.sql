-- 120_views_audit.sql — validation and health views (VMP Part 7 "Views, not
-- blocks"; SDP Part 2 "Validation philosophy": enforce structure at entry,
-- color-code everything else, never hard-block editing). Also the
-- referential-integrity fallback for FK intent DuckDB cannot enforce on
-- versioned tables (deviation D1).

-- ---------------------------------------------------------------------------
-- Referential integrity (FK-intent fallback): current rows referencing ids
-- with no current row in the target table.
CREATE VIEW audit_dangling_ref AS
SELECT 'entity' AS src_table, 'id=' || id AS src_key,
       'entity_class' AS ref_table, entity_class_id AS ref_id
FROM entity WHERE valid_to_commit IS NULL
  AND entity_class_id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_class_dimension', 'class=' || entity_class_id || ',pos=' || position, 'entity_class', entity_class_id
FROM entity_class_dimension WHERE valid_to_commit IS NULL
  AND entity_class_id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_class_dimension', 'class=' || entity_class_id || ',pos=' || position, 'entity_class', dimension_class_id
FROM entity_class_dimension WHERE valid_to_commit IS NULL AND dimension_class_id IS NOT NULL
  AND dimension_class_id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_class_dimension', 'class=' || entity_class_id || ',pos=' || position, 'class_set', dimension_class_set_id
FROM entity_class_dimension WHERE valid_to_commit IS NULL AND dimension_class_set_id IS NOT NULL
  AND dimension_class_set_id NOT IN (SELECT id FROM class_set WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'class_set_member', 'set=' || class_set_id, 'class_set', class_set_id
FROM class_set_member WHERE valid_to_commit IS NULL
  AND class_set_id NOT IN (SELECT id FROM class_set WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'class_set_member', 'set=' || class_set_id, 'entity_class', member_class_id
FROM class_set_member WHERE valid_to_commit IS NULL
  AND member_class_id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL)
UNION ALL
-- the FK-enforced import intent of #1015 lands here (deviation D1)
SELECT 'entity_element', 'entity=' || entity_id || ',pos=' || position, 'entity', entity_id
FROM entity_element WHERE valid_to_commit IS NULL
  AND entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_element', 'entity=' || entity_id || ',pos=' || position, 'entity', element_entity_id
FROM entity_element WHERE valid_to_commit IS NULL
  AND element_entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_location', 'entity=' || entity_id, 'entity', entity_id
FROM entity_location WHERE valid_to_commit IS NULL
  AND entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'membership', 'group=' || group_entity_id || ',member=' || member_entity_id, 'entity', group_entity_id
FROM membership WHERE valid_to_commit IS NULL
  AND group_entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'membership', 'group=' || group_entity_id || ',member=' || member_entity_id, 'entity', member_entity_id
FROM membership WHERE valid_to_commit IS NULL
  AND member_entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'membership', 'group=' || group_entity_id || ',member=' || member_entity_id, 'alternative', alternative_id
FROM membership WHERE valid_to_commit IS NULL AND alternative_id IS NOT NULL
  AND alternative_id NOT IN (SELECT id FROM alternative WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_alternative', 'entity=' || entity_id, 'entity', entity_id
FROM entity_alternative WHERE valid_to_commit IS NULL
  AND entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_alternative', 'entity=' || entity_id, 'alternative', alternative_id
FROM entity_alternative WHERE valid_to_commit IS NULL
  AND alternative_id NOT IN (SELECT id FROM alternative WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'scenario_alternative', 'scenario=' || scenario_id, 'scenario', scenario_id
FROM scenario_alternative WHERE valid_to_commit IS NULL
  AND scenario_id NOT IN (SELECT id FROM scenario WHERE valid_to_commit IS NULL)
UNION ALL
-- alternative -> RESTRICT intent (#2606): a deleted alternative still
-- referenced surfaces here; the explicit-cascade report is writer-library
SELECT 'scenario_alternative', 'scenario=' || scenario_id, 'alternative', alternative_id
FROM scenario_alternative WHERE valid_to_commit IS NULL
  AND alternative_id NOT IN (SELECT id FROM alternative WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_definition', 'id=' || id, 'entity_class', entity_class_id
FROM parameter_definition WHERE valid_to_commit IS NULL
  AND entity_class_id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_definition', 'id=' || id, 'parameter_value_list', parameter_value_list_id
FROM parameter_definition WHERE valid_to_commit IS NULL AND parameter_value_list_id IS NOT NULL
  AND parameter_value_list_id NOT IN (SELECT id FROM parameter_value_list WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_definition', 'id=' || id, 'units_of_measure', unit_id
FROM parameter_definition WHERE valid_to_commit IS NULL AND unit_id IS NOT NULL
  AND unit_id NOT IN (SELECT id FROM units_of_measure WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'list_value', 'id=' || id, 'parameter_value_list', parameter_value_list_id
FROM list_value WHERE valid_to_commit IS NULL
  AND parameter_value_list_id NOT IN (SELECT id FROM parameter_value_list WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_type', 'id=' || id, 'parameter_definition', parameter_definition_id
FROM parameter_type WHERE valid_to_commit IS NULL
  AND parameter_definition_id NOT IN (SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_type_dimension', 'type=' || parameter_type_id || ',pos=' || position, 'parameter_type', parameter_type_id
FROM parameter_type_dimension WHERE valid_to_commit IS NULL
  AND parameter_type_id NOT IN (SELECT id FROM parameter_type WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_parameter_unit', 'entity=' || entity_id, 'entity', entity_id
FROM entity_parameter_unit WHERE valid_to_commit IS NULL
  AND entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_parameter_unit', 'entity=' || entity_id, 'parameter_definition', parameter_definition_id
FROM entity_parameter_unit WHERE valid_to_commit IS NULL
  AND parameter_definition_id NOT IN (SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_parameter_unit', 'entity=' || entity_id, 'units_of_measure', unit_id
FROM entity_parameter_unit WHERE valid_to_commit IS NULL
  AND unit_id NOT IN (SELECT id FROM units_of_measure WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_metadata', 'entity=' || entity_id, 'entity', entity_id
FROM entity_metadata WHERE valid_to_commit IS NULL
  AND entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_metadata', 'entity=' || entity_id, 'metadata', metadata_id
FROM entity_metadata WHERE valid_to_commit IS NULL
  AND metadata_id NOT IN (SELECT id FROM metadata WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_value_metadata', 'entity=' || entity_id, 'metadata', metadata_id
FROM parameter_value_metadata WHERE valid_to_commit IS NULL
  AND metadata_id NOT IN (SELECT id FROM metadata WHERE valid_to_commit IS NULL)
UNION ALL
-- the value link's other legs (critique M1): entity, definition,
-- alternative, and the key triple itself (a link whose triple matches no
-- current value in any tier is dangling — the tiered model has no value id)
SELECT 'parameter_value_metadata', 'entity=' || entity_id, 'entity', entity_id
FROM parameter_value_metadata WHERE valid_to_commit IS NULL
  AND entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_value_metadata', 'entity=' || entity_id, 'parameter_definition', parameter_definition_id
FROM parameter_value_metadata WHERE valid_to_commit IS NULL
  AND parameter_definition_id NOT IN (SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_value_metadata', 'entity=' || entity_id, 'alternative', alternative_id
FROM parameter_value_metadata WHERE valid_to_commit IS NULL
  AND alternative_id NOT IN (SELECT id FROM alternative WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_value_metadata',
       'entity=' || pvm.entity_id || ',pd=' || pvm.parameter_definition_id
         || ',alt=' || pvm.alternative_id,
       'value_keys (triple)', pvm.entity_id
FROM parameter_value_metadata pvm
WHERE pvm.valid_to_commit IS NULL
  AND NOT EXISTS (SELECT 1 FROM value_keys vk
                  WHERE vk.entity_id = pvm.entity_id
                    AND vk.parameter_definition_id = pvm.parameter_definition_id
                    AND vk.alternative_id = pvm.alternative_id)
UNION ALL
-- value tiers via value_keys (covers all three current surfaces)
SELECT 'value_' || tier, 'entity=' || entity_id || ',pd=' || parameter_definition_id, 'entity', entity_id
FROM value_keys WHERE entity_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'value_' || tier, 'entity=' || entity_id || ',pd=' || parameter_definition_id, 'parameter_definition', parameter_definition_id
FROM value_keys WHERE parameter_definition_id NOT IN (SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'value_' || tier, 'entity=' || entity_id || ',pd=' || parameter_definition_id, 'alternative', alternative_id
FROM value_keys WHERE alternative_id NOT IN (SELECT id FROM alternative WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'definition_value', 'pd=' || parameter_definition_id || ',' || slot, 'parameter_definition', parameter_definition_id
FROM (SELECT parameter_definition_id, slot FROM definition_value_scalar WHERE valid_to_commit IS NULL
      UNION ALL SELECT parameter_definition_id, slot FROM definition_value_long WHERE valid_to_commit IS NULL
      UNION ALL SELECT parameter_definition_id, slot FROM definition_value_array WHERE valid_to_commit IS NULL)
WHERE parameter_definition_id NOT IN (SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL)
UNION ALL
-- entity_class_display_mode FK intent to versioned display_mode / entity_class
-- (deviation D1; Decision 12): lifecycle is writer-owned — closing out a class
-- closes out its ecdm rows; a stale row lands here, never hard-blocks.
SELECT 'entity_class_display_mode', 'mode=' || display_mode_id || ',class=' || entity_class_id,
       'display_mode', display_mode_id
FROM entity_class_display_mode WHERE valid_to_commit IS NULL
  AND display_mode_id NOT IN (SELECT id FROM display_mode WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity_class_display_mode', 'mode=' || display_mode_id || ',class=' || entity_class_id,
       'entity_class', entity_class_id
FROM entity_class_display_mode WHERE valid_to_commit IS NULL
  AND entity_class_id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL)
UNION ALL
-- run FK intent to versioned entity / alternative (deviation D1; Decision 13).
-- run is append-only so all its rows are "current"; the commit columns are
-- real FKs enforced at the table, only these two versioned targets fall here.
-- Change-model ruling 2 (object_delete plan §10): a run leg to a CLOSED-OUT
-- (not absent) entity/alternative is coherent as-of — provenance intentionally
-- outlives the object it references, and object-delete cascade never touches
-- run. So these two legs test membership against ALL versions of the target
-- (no `valid_to_commit IS NULL` filter): they fire only on a TRULY-ABSENT id
-- (one that never existed / corruption), not on a deliberately deleted one.
SELECT 'run', 'id=' || id, 'entity', tool_entity_id
FROM run
  WHERE tool_entity_id NOT IN (SELECT id FROM entity)
UNION ALL
SELECT 'run', 'id=' || id, 'alternative', result_alternative_id
FROM run
  WHERE result_alternative_id NOT IN (SELECT id FROM alternative);

-- semantic_annotation FK intent (deviation D1; fitness 2b): the polymorphic
-- (object_type, object_id) reference cannot be an engine FK, so each object
-- type with a clean stable id is checked against its table's current rows. A
-- dangling term mapping is surfaced, never blocked (express-never-assert).
-- object_type values with no clean id (e.g. 'class_dimension', keyed via
-- object_subkey) are validated by writer convention, not here.
CREATE VIEW audit_dangling_semantic_ref AS
SELECT 'entity_class' AS object_type, object_id, term_iri
FROM semantic_annotation WHERE valid_to_commit IS NULL AND object_type = 'entity_class'
  AND object_id NOT IN (SELECT id FROM entity_class WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_definition', object_id, term_iri
FROM semantic_annotation WHERE valid_to_commit IS NULL AND object_type = 'parameter_definition'
  AND object_id NOT IN (SELECT id FROM parameter_definition WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'entity', object_id, term_iri
FROM semantic_annotation WHERE valid_to_commit IS NULL AND object_type = 'entity'
  AND object_id NOT IN (SELECT id FROM entity WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'unit', object_id, term_iri
FROM semantic_annotation WHERE valid_to_commit IS NULL AND object_type = 'unit'
  AND object_id NOT IN (SELECT id FROM units_of_measure WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'parameter_value_list', object_id, term_iri
FROM semantic_annotation WHERE valid_to_commit IS NULL AND object_type = 'parameter_value_list'
  AND object_id NOT IN (SELECT id FROM parameter_value_list WHERE valid_to_commit IS NULL)
UNION ALL
SELECT 'list_value', object_id, term_iri
FROM semantic_annotation WHERE valid_to_commit IS NULL AND object_type = 'list_value'
  AND object_id NOT IN (SELECT id FROM list_value WHERE valid_to_commit IS NULL);

-- Entity-name uniqueness across a class-set's members (#2364, Decision 5 —
-- cross-table, so a view rather than an index).
CREATE VIEW audit_class_set_entity_name_dup AS
SELECT cs.id AS class_set_id, cs.name AS class_set, lower(e.name) AS entity_name,
       count(*) AS n, list(e.id) AS entity_ids
FROM class_set cs
JOIN class_set_member m ON m.class_set_id = cs.id AND m.valid_to_commit IS NULL
JOIN entity e ON e.entity_class_id = m.member_class_id AND e.valid_to_commit IS NULL
WHERE cs.valid_to_commit IS NULL
GROUP BY cs.id, cs.name, lower(e.name)
HAVING count(*) > 1;

-- Element class conformance (Decision 5: set-typed dimensions accept
-- entities of any member class; class-typed accept that class).
-- LEFT JOIN on the dimension (critique F6): an element at an UNDECLARED
-- position previously dropped out of this audit — the exact malformed shape
-- it exists to catch — and now fires instead.
CREATE VIEW audit_element_class_mismatch AS
SELECT ee.entity_id, ee.position, ee.element_entity_id,
       el.entity_class_id AS element_class_id,
       (d.entity_class_id IS NULL) AS undeclared_position
FROM entity_element ee
JOIN entity pe ON pe.id = ee.entity_id AND pe.valid_to_commit IS NULL
JOIN entity el ON el.id = ee.element_entity_id AND el.valid_to_commit IS NULL
LEFT JOIN entity_class_dimension d
  ON d.entity_class_id = pe.entity_class_id AND d.position = ee.position
 AND d.valid_to_commit IS NULL
WHERE ee.valid_to_commit IS NULL
  AND CASE
        WHEN d.entity_class_id IS NULL THEN TRUE   -- undeclared position
        WHEN d.dimension_class_id IS NOT NULL THEN el.entity_class_id <> d.dimension_class_id
        ELSE NOT EXISTS (SELECT 1 FROM class_set_member m
                         WHERE m.class_set_id = d.dimension_class_set_id
                           AND m.member_class_id = el.entity_class_id
                           AND m.valid_to_commit IS NULL)
      END;

-- Element arity (critique F6, the #1015 import-typo class): per current
-- entity of a dimensional class, the set of current element positions must
-- equal the declared dimension positions. Reports both directions —
-- declared-but-missing (incl. entities with zero elements) and
-- present-but-undeclared.
CREATE VIEW audit_element_arity AS
WITH expected AS (
  SELECT e.id AS entity_id, d.position
  FROM entity e
  JOIN entity_class_dimension d
    ON d.entity_class_id = e.entity_class_id AND d.valid_to_commit IS NULL
  WHERE e.valid_to_commit IS NULL
),
actual AS (
  SELECT ee.entity_id, ee.position
  FROM entity_element ee
  JOIN entity pe ON pe.id = ee.entity_id AND pe.valid_to_commit IS NULL
  WHERE ee.valid_to_commit IS NULL
)
SELECT coalesce(x.entity_id, a.entity_id) AS entity_id,
       coalesce(x.position, a.position) AS position,
       CASE WHEN a.entity_id IS NULL THEN 'missing element at declared position'
            ELSE 'element at undeclared position' END AS problem
FROM expected x
FULL OUTER JOIN actual a
  ON a.entity_id = x.entity_id AND a.position = x.position
WHERE x.entity_id IS NULL OR a.entity_id IS NULL;

-- Values whose definition belongs to a different class than the entity.
CREATE VIEW audit_value_class_mismatch AS
SELECT vk.*
FROM value_keys vk
JOIN entity e ON e.id = vk.entity_id AND e.valid_to_commit IS NULL
JOIN parameter_definition pd ON pd.id = vk.parameter_definition_id AND pd.valid_to_commit IS NULL
WHERE e.entity_class_id <> pd.entity_class_id;

-- ---------------------------------------------------------------------------
-- Axis-content integrity (critique F3). Axes are writer-created and
-- immutable with no engine checks tying children to the header; a corrupt
-- axis renders values with silently-NULL index labels — the round-trip
-- guarantee (VMP Part 1) defeated. One row per (axis, problem).

CREATE VIEW audit_axis_integrity AS
WITH child_stats AS (
  SELECT axis_id, count(*) AS n, min(pos) AS pmin, max(pos) AS pmax,
         count(DISTINCT pos) AS ndist
  FROM (
    SELECT axis_id, pos FROM axis_datetime
    UNION ALL SELECT axis_id, pos FROM axis_period
    UNION ALL SELECT axis_id, pos FROM axis_label
    UNION ALL SELECT axis_id, pos FROM axis_integer
    UNION ALL SELECT axis_id, pos FROM axis_float
    UNION ALL SELECT DISTINCT axis_id, pos FROM axis_time_pattern
  )
  GROUP BY axis_id
),
nonmono AS (   -- sortable kinds must be strictly increasing (CF rule)
  SELECT DISTINCT axis_id FROM (
    SELECT axis_id, ts >= lead(ts) OVER (PARTITION BY axis_id ORDER BY pos) AS bad
    FROM axis_datetime
    UNION ALL
    SELECT axis_id, num >= lead(num) OVER (PARTITION BY axis_id ORDER BY pos)
    FROM axis_integer
    UNION ALL
    SELECT axis_id, start_ts >= lead(start_ts) OVER (PARTITION BY axis_id ORDER BY pos)
    FROM axis_period
    UNION ALL
    SELECT axis_id, coord >= lead(coord) OVER (PARTITION BY axis_id ORDER BY pos)
    FROM axis_float
  ) WHERE bad
)
-- materialized kinds: child rows must exist, count = length, pos tile 0..length-1
SELECT a.id AS axis_id, a.kind, a.length,
       'child rows do not tile 0..length-1 (n=' || coalesce(cs.n, 0) || ')' AS problem
FROM axis a
LEFT JOIN child_stats cs ON cs.axis_id = a.id
WHERE ((a.kind = 'datetime' AND a.p_start_ts IS NULL)
    OR (a.kind = 'integer' AND a.p_start_int IS NULL)
    OR (a.kind = 'float' AND a.p_start_dbl IS NULL)
    OR a.kind IN ('period', 'label', 'time_pattern'))
  AND (coalesce(cs.n, 0) <> a.length OR cs.pmin <> 0
       OR cs.pmax <> a.length - 1 OR cs.ndist <> cs.n)
UNION ALL
SELECT a.id, a.kind, a.length, 'sortable axis children not strictly increasing'
FROM axis a JOIN nonmono nm ON nm.axis_id = a.id
UNION ALL
-- parametric completeness (critique M7: a NULL step renders ts = NULL)
SELECT a.id, a.kind, a.length, 'parametric datetime axis missing step field(s)'
FROM axis a
WHERE a.kind = 'datetime' AND a.p_start_ts IS NOT NULL
  AND (a.p_step_months IS NULL OR a.p_step_micros IS NULL)
UNION ALL
SELECT a.id, a.kind, a.length, 'parametric integer axis missing p_step_int'
FROM axis a
WHERE a.kind = 'integer' AND a.p_start_int IS NOT NULL AND a.p_step_int IS NULL
UNION ALL
SELECT a.id, a.kind, a.length, 'parametric float axis missing/invalid p_step_dbl'
FROM axis a
WHERE a.kind = 'float' AND a.p_start_dbl IS NOT NULL
  AND (a.p_step_dbl IS NULL OR a.p_step_dbl <= 0)   -- <=0 breaks strict-increasing
UNION ALL
-- parametric XOR materialized
SELECT a.id, a.kind, a.length, 'parametric fields and child rows both populated'
FROM axis a
JOIN child_stats cs ON cs.axis_id = a.id
WHERE (a.kind = 'datetime' AND a.p_start_ts IS NOT NULL)
   OR (a.kind = 'integer' AND a.p_start_int IS NOT NULL)
   OR (a.kind = 'float' AND a.p_start_dbl IS NOT NULL);

-- Long-tier positions must fall inside the bound axis (critique F3: an
-- out-of-range iN_pos previously vanished from every audit and rendered a
-- NULL index). Covers value_long and its definition mirror, all dims.
CREATE VIEW audit_position_range AS
SELECT d.src_table, d.entity_id, d.parameter_definition_id, d.alternative_id,
       d.dim, d.axis_id, d.pos, a.length
FROM (
  SELECT 'value_long' AS src_table, entity_id, parameter_definition_id, alternative_id,
         1 AS dim, axis1_id AS axis_id, i1_pos AS pos
  FROM value_long WHERE valid_to_commit IS NULL
  UNION ALL
  SELECT 'value_long', entity_id, parameter_definition_id, alternative_id, 2, axis2_id, i2_pos
  FROM value_long WHERE valid_to_commit IS NULL AND axis2_id IS NOT NULL
  UNION ALL
  SELECT 'value_long', entity_id, parameter_definition_id, alternative_id, 3, axis3_id, i3_pos
  FROM value_long WHERE valid_to_commit IS NULL AND axis3_id IS NOT NULL
  UNION ALL
  SELECT 'value_long', entity_id, parameter_definition_id, alternative_id, 4, axis4_id, i4_pos
  FROM value_long WHERE valid_to_commit IS NULL AND axis4_id IS NOT NULL
  UNION ALL
  SELECT 'definition_value_long', NULL, parameter_definition_id, NULL, 1, axis1_id, i1_pos
  FROM definition_value_long WHERE valid_to_commit IS NULL
  UNION ALL
  SELECT 'definition_value_long', NULL, parameter_definition_id, NULL, 2, axis2_id, i2_pos
  FROM definition_value_long WHERE valid_to_commit IS NULL AND axis2_id IS NOT NULL
  UNION ALL
  SELECT 'definition_value_long', NULL, parameter_definition_id, NULL, 3, axis3_id, i3_pos
  FROM definition_value_long WHERE valid_to_commit IS NULL AND axis3_id IS NOT NULL
  UNION ALL
  SELECT 'definition_value_long', NULL, parameter_definition_id, NULL, 4, axis4_id, i4_pos
  FROM definition_value_long WHERE valid_to_commit IS NULL AND axis4_id IS NOT NULL
) d
JOIN axis a ON a.id = d.axis_id
WHERE d.pos < 0 OR d.pos >= a.length;

-- ---------------------------------------------------------------------------
-- Type/structure audits (VMP Part 7).

-- Type-conformance: populated value columns must agree with value_kind
-- (possible only for out-of-contract writers). Unknown future kinds are
-- skipped (Decision 4 additive door).
CREATE VIEW audit_type_conformance AS
SELECT 'value_scalar' AS src_table, entity_id, parameter_definition_id, alternative_id, value_kind
FROM value_scalar
WHERE valid_to_commit IS NULL AND content_retained
  AND value_kind IN ('float', 'int', 'bool', 'string', 'date_time', 'duration')
  AND NOT (
    ((v_float IS NOT NULL)::INT + (v_int IS NOT NULL)::INT + (v_bool IS NOT NULL)::INT
     + (v_str IS NOT NULL)::INT + (v_datetime IS NOT NULL)::INT
     + (v_dur_months IS NOT NULL OR v_dur_micros IS NOT NULL)::INT) = 1
    AND CASE value_kind
          WHEN 'float' THEN v_float IS NOT NULL
          WHEN 'int' THEN v_int IS NOT NULL
          WHEN 'bool' THEN v_bool IS NOT NULL
          WHEN 'string' THEN v_str IS NOT NULL
          WHEN 'date_time' THEN v_datetime IS NOT NULL
          WHEN 'duration' THEN v_dur_months IS NOT NULL AND v_dur_micros IS NOT NULL
        END)
UNION ALL
SELECT 'value_long', entity_id, parameter_definition_id, alternative_id, value_kind
FROM value_long
WHERE valid_to_commit IS NULL AND content_retained
  AND value_kind IN ('float', 'int', 'bool', 'string', 'date_time', 'duration')  -- full kind set (critique F9)
  AND NOT (
    ((v_float IS NOT NULL)::INT + (v_int IS NOT NULL)::INT
     + (v_bool IS NOT NULL)::INT + (v_str IS NOT NULL)::INT
     + (v_datetime IS NOT NULL)::INT
     + (v_dur_months IS NOT NULL OR v_dur_micros IS NOT NULL)::INT) = 1
    AND CASE value_kind
          WHEN 'float' THEN v_float IS NOT NULL
          WHEN 'int' THEN v_int IS NOT NULL
          WHEN 'bool' THEN v_bool IS NOT NULL
          WHEN 'string' THEN v_str IS NOT NULL
          WHEN 'date_time' THEN v_datetime IS NOT NULL
          WHEN 'duration' THEN v_dur_months IS NOT NULL AND v_dur_micros IS NOT NULL
        END)
UNION ALL
-- array tier (owner decision H): the populated payload column must agree with
-- the declared value_kind (values => float, values_int => int). value_array
-- carries no value_kind of its own — the XOR CHECK guarantees exactly one
-- payload but cannot check it against the declaration; this branch does.
-- Set-aware (D2(a), template_ir_plan §3.3): a definition may declare an
-- allowed-type SET (multi-row parameter_type); the payload kind need only
-- match SOME current declared member, so an int payload beside a coexisting
-- float member no longer false-positives. Fires once per offending value,
-- reporting the actual payload kind.
SELECT 'value_array', va.entity_id, va.parameter_definition_id, va.alternative_id,
       CASE WHEN va.values IS NOT NULL THEN 'float' ELSE 'int' END AS value_kind
FROM value_array va
WHERE EXISTS (
        SELECT 1 FROM parameter_type pt
        WHERE pt.parameter_definition_id = va.parameter_definition_id
          AND pt.valid_to_commit IS NULL AND pt.value_kind IN ('float', 'int'))
  AND NOT EXISTS (
        SELECT 1 FROM parameter_type pt
        WHERE pt.parameter_definition_id = va.parameter_definition_id
          AND pt.valid_to_commit IS NULL
          AND ((pt.value_kind = 'float' AND va.values     IS NOT NULL)
            OR (pt.value_kind = 'int'   AND va.values_int IS NOT NULL)));

-- Array-integrity: len(values) = product of axis lengths; no NULL cells
-- (canonicalization step 1); pruning metadata honest.
CREATE VIEW audit_array_integrity AS
SELECT va.entity_id, va.parameter_definition_id, va.alternative_id,
       coalesce(len(va.values), len(va.values_int)) AS actual_len,
       (a1.length * coalesce(a2.length, 1) * coalesce(a3.length, 1) * coalesce(a4.length, 1)) AS expected_len
FROM value_array va
JOIN axis a1 ON a1.id = va.axis1_id
LEFT JOIN axis a2 ON a2.id = va.axis2_id
LEFT JOIN axis a3 ON a3.id = va.axis3_id
LEFT JOIN axis a4 ON a4.id = va.axis4_id
WHERE coalesce(len(va.values), len(va.values_int)) <> (a1.length * coalesce(a2.length, 1) * coalesce(a3.length, 1) * coalesce(a4.length, 1))
   OR len(list_filter(coalesce(va.values, []) , x -> x IS NULL)) > 0
   OR len(list_filter(coalesce(va.values_int, []), x -> x IS NULL)) > 0
   OR (va.v_min IS NOT NULL AND va.values     IS NOT NULL AND va.v_min <> list_aggregate(va.values, 'min'))
   OR (va.v_max IS NOT NULL AND va.values     IS NOT NULL AND va.v_max <> list_aggregate(va.values, 'max'))
   OR (va.v_min IS NOT NULL AND va.values_int IS NOT NULL AND va.v_min <> list_aggregate(va.values_int, 'min'))
   OR (va.v_max IS NOT NULL AND va.values_int IS NOT NULL AND va.v_max <> list_aggregate(va.values_int, 'max'));

-- Retained-row payload XOR (critique F5): value_array_hist / definition_value_array
-- drop the live-table XOR CHECK (stubs legitimately null BOTH payloads), so a
-- content-retained / non-stub row carrying both-or-neither payload is otherwise
-- unguarded at rest. The live value_array XOR catches such a row the moment it
-- would re-enter live state; this closes the at-rest hole. One row per offender.
CREATE VIEW audit_array_payload_xor AS
SELECT 'value_array_hist' AS src_table,
       'entity=' || entity_id || ',pd=' || parameter_definition_id
         || ',alt=' || alternative_id || ',v=' || version AS src_key
FROM value_array_hist
WHERE content_retained AND ((values IS NULL) = (values_int IS NULL))
UNION ALL
SELECT 'definition_value_array',
       'pd=' || parameter_definition_id || ',' || slot
FROM definition_value_array
WHERE content_retained AND ((values IS NULL) = (values_int IS NULL));

-- Index-accelerator audit: long-tier denormalized iN_dt/iN_num columns must
-- agree with the bound axis at iN_pos (derived, never authoritative).
-- LEFT JOIN on axis_position (critique F3): a position with no rendering
-- row — out of range, or a short/corrupt child table — fires here instead
-- of vanishing from the audit.
CREATE VIEW audit_index_accelerator AS
SELECT d.entity_id, d.parameter_definition_id, d.alternative_id, d.dim,
       d.axis_id, d.pos, d.acc_dt, d.acc_num, p.kind, p.ts, p.num
FROM (
  SELECT entity_id, parameter_definition_id, alternative_id,
         1 AS dim, axis1_id AS axis_id, i1_pos AS pos, i1_dt AS acc_dt, i1_num AS acc_num
  FROM value_long WHERE valid_to_commit IS NULL
  UNION ALL
  SELECT entity_id, parameter_definition_id, alternative_id, 2, axis2_id, i2_pos, i2_dt, i2_num
  FROM value_long WHERE valid_to_commit IS NULL AND axis2_id IS NOT NULL
  UNION ALL
  SELECT entity_id, parameter_definition_id, alternative_id, 3, axis3_id, i3_pos, i3_dt, i3_num
  FROM value_long WHERE valid_to_commit IS NULL AND axis3_id IS NOT NULL
  UNION ALL
  SELECT entity_id, parameter_definition_id, alternative_id, 4, axis4_id, i4_pos, i4_dt, i4_num
  FROM value_long WHERE valid_to_commit IS NULL AND axis4_id IS NOT NULL
) d
LEFT JOIN axis_position p ON p.axis_id = d.axis_id AND p.pos = d.pos
WHERE p.axis_id IS NULL   -- unrenderable position (critique F3)
   OR (p.kind IN ('datetime', 'period') AND (d.acc_dt IS DISTINCT FROM p.ts OR d.acc_num IS NOT NULL))
   OR (p.kind = 'integer' AND (d.acc_num IS DISTINCT FROM p.num OR d.acc_dt IS NOT NULL))
   OR (p.kind IN ('label', 'time_pattern', 'float') AND (d.acc_dt IS NOT NULL OR d.acc_num IS NOT NULL));

-- Cross-tier duplicate audit (critique #5): no key triple current in more
-- than one tier — guards buggy tier transitions.
CREATE VIEW audit_cross_tier_duplicate AS
SELECT entity_id, parameter_definition_id, alternative_id,
       list(DISTINCT tier) AS tiers
FROM value_keys
GROUP BY entity_id, parameter_definition_id, alternative_id
HAVING count(DISTINCT tier) > 1;

-- Tier-intent audit (critique #8): actual tier differs from the type's
-- expected_tier — surfaces raggedness at write time, never enforces.
-- Set-aware (D2(a), template_ir_plan §3.3): fires only when the actual tier
-- matches NONE of the current declared members' expected tiers — a value on
-- any one declared member's expected tier is in-intent, so a multi-member
-- declaration no longer row-multiplies into false positives. When none
-- matches, one row per expectation-carrying member (the expected set, listed).
CREATE VIEW audit_tier_intent AS
SELECT vk.entity_id, vk.parameter_definition_id, vk.alternative_id,
       vk.tier AS actual_tier, pt.expected_tier
FROM value_keys vk
JOIN parameter_type pt
  ON pt.parameter_definition_id = vk.parameter_definition_id
 AND pt.valid_to_commit IS NULL AND pt.expected_tier IS NOT NULL
WHERE vk.tier <> 'scalar'
  AND NOT EXISTS (
        SELECT 1 FROM parameter_type pt2
        WHERE pt2.parameter_definition_id = vk.parameter_definition_id
          AND pt2.valid_to_commit IS NULL AND pt2.expected_tier = vk.tier);

-- Value-list membership (writer-checked at entry, audited here). Two legs:
-- entity-scoped scalar values, and definition-scoped scalar defaults/prefills
-- (D2(a) bundle, template_ir_plan §3.3.4 / critique m5: a template-declared
-- DEFAULT violating its own value list — the exact authoring typo a loader
-- exists to catch — was previously invisible). Definition-scoped rows carry
-- no entity/alternative; src_table names the slot instead.
CREATE VIEW audit_value_list_membership AS
SELECT 'value_scalar' AS src_table,
       vs.entity_id, vs.parameter_definition_id, vs.alternative_id, vs.value_kind
FROM value_scalar vs
JOIN parameter_definition pd
  ON pd.id = vs.parameter_definition_id AND pd.valid_to_commit IS NULL
 AND pd.parameter_value_list_id IS NOT NULL
WHERE vs.valid_to_commit IS NULL AND vs.content_retained
  AND NOT EXISTS (
    SELECT 1 FROM list_value lv
    WHERE lv.parameter_value_list_id = pd.parameter_value_list_id
      AND lv.valid_to_commit IS NULL
      AND lv.value_kind = vs.value_kind
      AND ((vs.value_kind = 'float' AND lv.v_float = vs.v_float)
        OR (vs.value_kind = 'int' AND lv.v_int = vs.v_int)
        OR (vs.value_kind = 'bool' AND lv.v_bool = vs.v_bool)
        OR (vs.value_kind = 'string' AND lv.v_str = vs.v_str)
        OR (vs.value_kind = 'date_time' AND lv.v_datetime = vs.v_datetime)
        OR (vs.value_kind = 'duration' AND lv.v_dur_months = vs.v_dur_months
                                       AND lv.v_dur_micros = vs.v_dur_micros)))
UNION ALL
SELECT 'definition_value_scalar:' || dv.slot AS src_table,
       NULL::BIGINT AS entity_id, dv.parameter_definition_id,
       NULL::BIGINT AS alternative_id, dv.value_kind
FROM definition_value_scalar dv
JOIN parameter_definition pd
  ON pd.id = dv.parameter_definition_id AND pd.valid_to_commit IS NULL
 AND pd.parameter_value_list_id IS NOT NULL
WHERE dv.valid_to_commit IS NULL AND dv.content_retained
  AND NOT EXISTS (
    SELECT 1 FROM list_value lv
    WHERE lv.parameter_value_list_id = pd.parameter_value_list_id
      AND lv.valid_to_commit IS NULL
      AND lv.value_kind = dv.value_kind
      AND ((dv.value_kind = 'float' AND lv.v_float = dv.v_float)
        OR (dv.value_kind = 'int' AND lv.v_int = dv.v_int)
        OR (dv.value_kind = 'bool' AND lv.v_bool = dv.v_bool)
        OR (dv.value_kind = 'string' AND lv.v_str = dv.v_str)
        OR (dv.value_kind = 'date_time' AND lv.v_datetime = dv.v_datetime)
        OR (dv.value_kind = 'duration' AND lv.v_dur_months = dv.v_dur_months
                                       AND lv.v_dur_micros = dv.v_dur_micros)));

-- Reserved single-valued metadata keys must not fork (critique F5): the
-- carried Spine (lower(name), value) uniqueness constrains duplicates of a
-- pair, never per-key multiplicity — right for tag-like keys, wrong for the
-- reserved keys the plans read singly (schema_version drives migration
-- dispatch; template identity drives Decision 6 diff/apply; the Decision 7
-- presentation document is one slot). Writer rule: close-out-then-insert on
-- reserved keys, never a second current row.
CREATE VIEW audit_metadata_reserved_dup AS
SELECT lower(name) AS name, count(*) AS n, list(value) AS conflicting_values
FROM metadata
WHERE valid_to_commit IS NULL
  AND lower(name) IN ('schema_version', 'template_name', 'template_version',
                      'template_content_hash', 'presentation',
                      'semantic_context', 'conforms_to')  -- single-valued reserved keys (fitness 2b)
GROUP BY lower(name)
HAVING count(*) > 1;

-- Redundant activation rows (#3010): rows restating the class default.
-- Advisory only — under propagation semantics an own row equal to the
-- default can still act as a propagation barrier.
CREATE VIEW audit_entity_alternative_redundant AS
SELECT ea.entity_id, ea.alternative_id, ea.active
FROM entity_alternative ea
JOIN entity e ON e.id = ea.entity_id AND e.valid_to_commit IS NULL
JOIN entity_class ec ON ec.id = e.entity_class_id AND ec.valid_to_commit IS NULL
WHERE ea.valid_to_commit IS NULL AND ea.active = ec.active_by_default;

-- Membership cycles (SDP Part 2 accepted residual risk, surfaced).
-- The base branch (scenario_id NULL) covers rank-0 membership so cycles are
-- visible even before any scenario exists (critique M2).
CREATE VIEW audit_membership_cycle AS
SELECT scenario_id, group_entity_id AS entity_id
FROM membership_flat
WHERE group_entity_id = member_entity_id
UNION ALL
SELECT NULL::BIGINT, group_entity_id
FROM membership_base_flat
WHERE group_entity_id = member_entity_id;

-- Refinement (realizes) cycles (Decision 13, option (c) sibling of the
-- membership-cycle audit): the coarse->fine fixpoint terminates on a cycle
-- via the distinct UNION, but a cycle is a modelling error worth surfacing.
-- Flattens the refinement DAG and reports any entity reachable from itself.
CREATE VIEW audit_realizes_cycle AS
WITH RECURSIVE reach(coarse_id, fine_id) AS (
  SELECT coarse_id, fine_id FROM realizes_edge
  UNION
  SELECT r.coarse_id, e.fine_id
  FROM reach r JOIN realizes_edge e ON e.coarse_id = r.fine_id
)
SELECT coarse_id AS entity_id FROM reach WHERE coarse_id = fine_id;

-- ---------------------------------------------------------------------------
-- History audits (these two read superseded/_hist rows by definition —
-- VMP Part 7 retraction note).

-- Version-tiling + span coherence (critique #1, critique F2): superseded
-- rows per key must tile 1..m contiguously, with current version = m + 1
-- (or no current row after a delete); AND the validity spans must cohere in
-- version order — valid_from non-decreasing, each span closed before the
-- next opens, no reversed spans. Reversed spans are also blocked at entry
-- by the table CHECKs; the audit still watches for them (attached legacy
-- files, raw writes predating the CHECK). Any hit triggers the recovery
-- protocol (mark spans untrusted).
CREATE VIEW audit_history_tiling AS
WITH keyed AS (
  SELECT 'scalar' AS tier, entity_id, parameter_definition_id, alternative_id,
         0 AS i1_pos, 0 AS i2_pos, 0 AS i3_pos, 0 AS i4_pos,
         version, valid_from_commit, valid_to_commit
  FROM value_scalar
  UNION ALL
  SELECT 'long', entity_id, parameter_definition_id, alternative_id,
         i1_pos, i2_pos, i3_pos, i4_pos, version, valid_from_commit, valid_to_commit
  FROM value_long
  UNION ALL
  SELECT 'array', entity_id, parameter_definition_id, alternative_id,
         0, 0, 0, 0, version, valid_from_commit, NULL
  FROM value_array
  UNION ALL
  SELECT 'array', entity_id, parameter_definition_id, alternative_id,
         0, 0, 0, 0, version, valid_from_commit, valid_to_commit
  FROM value_array_hist
),
agg AS (
  SELECT tier, entity_id, parameter_definition_id, alternative_id,
         i1_pos, i2_pos, i3_pos, i4_pos,
         max(CASE WHEN valid_to_commit IS NULL THEN version END) AS current_version,
         count(*) FILTER (WHERE valid_to_commit IS NOT NULL) AS n_dead,
         min(version) FILTER (WHERE valid_to_commit IS NOT NULL) AS dead_min,
         max(version) FILTER (WHERE valid_to_commit IS NOT NULL) AS dead_max,
         count(DISTINCT version) FILTER (WHERE valid_to_commit IS NOT NULL) AS dead_distinct
  FROM keyed
  GROUP BY ALL
),
spans AS (   -- span coherence in version order, per key (critique F2)
  SELECT tier, entity_id, parameter_definition_id, alternative_id,
         i1_pos, i2_pos, i3_pos, i4_pos,
         version, valid_from_commit, valid_to_commit,
         lag(valid_from_commit) OVER w AS prev_from,
         lag(valid_to_commit)   OVER w AS prev_to
  FROM keyed
  WINDOW w AS (PARTITION BY tier, entity_id, parameter_definition_id,
                            alternative_id, i1_pos, i2_pos, i3_pos, i4_pos
               ORDER BY version)
)
SELECT *,
       CASE
         WHEN n_dead = 0 AND current_version <> 1
           THEN 'current version > 1 with no superseded rows'
         WHEN n_dead > 0 AND (dead_distinct <> n_dead OR dead_min <> 1 OR dead_max <> n_dead)
           THEN 'superseded versions do not tile 1..m'
         WHEN current_version IS NOT NULL AND n_dead > 0 AND current_version <> dead_max + 1
           THEN 'current version <> superseded max + 1'
       END AS problem
FROM agg
WHERE problem IS NOT NULL
UNION ALL
SELECT tier, entity_id, parameter_definition_id, alternative_id,
       i1_pos, i2_pos, i3_pos, i4_pos,
       NULL AS current_version, NULL AS n_dead, NULL AS dead_min,
       NULL AS dead_max, NULL AS dead_distinct,
       CASE
         WHEN valid_to_commit IS NOT NULL AND valid_to_commit <= valid_from_commit
           THEN 'reversed validity span (to <= from)'
         WHEN prev_from IS NOT NULL AND valid_from_commit < prev_from
           THEN 'valid_from decreases with version'
         WHEN prev_to IS NOT NULL AND valid_from_commit < prev_to
           THEN 'validity spans overlap within key'
         WHEN prev_from IS NOT NULL AND prev_to IS NULL
           THEN 'non-final version has open span'
       END AS problem
FROM spans
WHERE problem IS NOT NULL;

-- Maintenance-health audit (VMP Part 5): per-tier dead fraction, _hist
-- volume, file size; advisory compaction thresholds (~30% dead — measured
-- 2.5x scan cost at 56% dead). Trigger is manual: compaction is a
-- first-class client-library operation, not a daemon.
CREATE VIEW audit_health AS
SELECT 'value_scalar' AS surface,
       count(*) FILTER (WHERE valid_to_commit IS NULL) AS live_rows,
       count(*) FILTER (WHERE valid_to_commit IS NOT NULL) AS dead_rows,
       round(count(*) FILTER (WHERE valid_to_commit IS NOT NULL)::DOUBLE / greatest(count(*), 1), 3) AS dead_fraction,
       count(*) FILTER (WHERE valid_to_commit IS NOT NULL)::DOUBLE / greatest(count(*), 1) > 0.30 AS compaction_advised,
       NULL::VARCHAR AS note
FROM value_scalar
UNION ALL
SELECT 'value_long',
       count(*) FILTER (WHERE valid_to_commit IS NULL),
       count(*) FILTER (WHERE valid_to_commit IS NOT NULL),
       round(count(*) FILTER (WHERE valid_to_commit IS NOT NULL)::DOUBLE / greatest(count(*), 1), 3),
       count(*) FILTER (WHERE valid_to_commit IS NOT NULL)::DOUBLE / greatest(count(*), 1) > 0.30,
       NULL
FROM value_long
UNION ALL
SELECT 'value_array (+_hist)',
       (SELECT count(*) FROM value_array),
       count(*),
       round(count(*)::DOUBLE / greatest((SELECT count(*) FROM value_array) + count(*), 1), 3),
       count(*)::DOUBLE / greatest((SELECT count(*) FROM value_array) + count(*), 1) > 0.30,
       'hist payload cells: ' || coalesce(sum(coalesce(len(values), len(values_int))), 0)
FROM value_array_hist
UNION ALL
-- one pass over the narrow tables (critique M8: the union used to run twice)
SELECT 'narrow tables (all)', live_rows, dead_rows, NULL, NULL, NULL
FROM (
  SELECT count(*) FILTER (WHERE valid_to_commit IS NULL) AS live_rows,
         count(*) FILTER (WHERE valid_to_commit IS NOT NULL) AS dead_rows
  FROM (
    SELECT valid_to_commit FROM metadata UNION ALL SELECT valid_to_commit FROM entity_metadata
    UNION ALL SELECT valid_to_commit FROM parameter_value_metadata
    UNION ALL SELECT valid_to_commit FROM entity_class UNION ALL SELECT valid_to_commit FROM class_set
    UNION ALL SELECT valid_to_commit FROM class_set_member UNION ALL SELECT valid_to_commit FROM entity_class_dimension
    UNION ALL SELECT valid_to_commit FROM entity UNION ALL SELECT valid_to_commit FROM entity_element
    UNION ALL SELECT valid_to_commit FROM entity_location UNION ALL SELECT valid_to_commit FROM alternative
    UNION ALL SELECT valid_to_commit FROM scenario UNION ALL SELECT valid_to_commit FROM scenario_alternative
    UNION ALL SELECT valid_to_commit FROM entity_alternative UNION ALL SELECT valid_to_commit FROM membership
    UNION ALL SELECT valid_to_commit FROM units_of_measure UNION ALL SELECT valid_to_commit FROM parameter_value_list
    UNION ALL SELECT valid_to_commit FROM list_value UNION ALL SELECT valid_to_commit FROM parameter_definition
    UNION ALL SELECT valid_to_commit FROM entity_parameter_unit UNION ALL SELECT valid_to_commit FROM parameter_type
    UNION ALL SELECT valid_to_commit FROM parameter_type_dimension
    UNION ALL SELECT valid_to_commit FROM definition_value_scalar
    UNION ALL SELECT valid_to_commit FROM definition_value_long
    UNION ALL SELECT valid_to_commit FROM definition_value_array
    UNION ALL SELECT valid_to_commit FROM display_mode
    UNION ALL SELECT valid_to_commit FROM entity_class_display_mode
    UNION ALL SELECT valid_to_commit FROM semantic_annotation
  )
)
UNION ALL
SELECT 'database file', NULL, NULL, NULL, NULL, 'size: ' || database_size
FROM pragma_database_size();
