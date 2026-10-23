-- The pre-array_id (schema 0.3) two-branch value_array_cells view, kept as the
-- parity oracle for the single-branch self-join form (tests/array_id_semantics.rs).
-- Reference only: never applied to a real database.
CREATE TEMP VIEW value_array_cells_two_branch AS
-- float payloads
SELECT entity_id, parameter_definition_id, alternative_id,
       axis1_id, axis2_id, axis3_id, axis4_id,
       (flat_pos // (n2 * n3 * n4))::INTEGER        AS i1_pos,
       ((flat_pos // (n3 * n4)) % n2)::INTEGER      AS i2_pos,
       ((flat_pos // n4) % n3)::INTEGER             AS i3_pos,
       (flat_pos % n4)::INTEGER                     AS i4_pos,
       'float'                                      AS value_kind,
       v                                            AS v_float,
       NULL::BIGINT                                 AS v_int
FROM (
  SELECT va.entity_id, va.parameter_definition_id, va.alternative_id,
         va.axis1_id, va.axis2_id, va.axis3_id, va.axis4_id,
         coalesce(a2.length, 1) AS n2,
         coalesce(a3.length, 1) AS n3,
         coalesce(a4.length, 1) AS n4,
         unnest(va.values) AS v,
         unnest(range(len(va.values))) AS flat_pos
  FROM value_array va
  LEFT JOIN axis a2 ON a2.id = va.axis2_id
  LEFT JOIN axis a3 ON a3.id = va.axis3_id
  LEFT JOIN axis a4 ON a4.id = va.axis4_id
  WHERE va.values IS NOT NULL
)
UNION ALL
-- int payloads (same zip-unnest decode over values_int)
SELECT entity_id, parameter_definition_id, alternative_id,
       axis1_id, axis2_id, axis3_id, axis4_id,
       (flat_pos // (n2 * n3 * n4))::INTEGER        AS i1_pos,
       ((flat_pos // (n3 * n4)) % n2)::INTEGER      AS i2_pos,
       ((flat_pos // n4) % n3)::INTEGER             AS i3_pos,
       (flat_pos % n4)::INTEGER                     AS i4_pos,
       'int'                                        AS value_kind,
       NULL::DOUBLE                                 AS v_float,
       v                                            AS v_int
FROM (
  SELECT va.entity_id, va.parameter_definition_id, va.alternative_id,
         va.axis1_id, va.axis2_id, va.axis3_id, va.axis4_id,
         coalesce(a2.length, 1) AS n2,
         coalesce(a3.length, 1) AS n3,
         coalesce(a4.length, 1) AS n4,
         unnest(va.values_int) AS v,
         unnest(range(len(va.values_int))) AS flat_pos
  FROM value_array va
  LEFT JOIN axis a2 ON a2.id = va.axis2_id
  LEFT JOIN axis a3 ON a3.id = va.axis3_id
  LEFT JOIN axis a4 ON a4.id = va.axis4_id
  WHERE va.values_int IS NOT NULL
);
