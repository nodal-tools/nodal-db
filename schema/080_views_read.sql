-- 080_views_read.sql — the unified read surface: axis rendering, the
-- canonical logical-long shape over all three tiers (VMP Part 3 "The read
-- surface unifies the tiers"; VMP Part 6 canonical bulk read), and the
-- effective-unit view (Decision 9).
--
-- Reworked after the DDL critique (F4): the array decode is zip-unnest
-- (one pass, no per-cell list_extract), and value_rows is split into
-- rank-specialized branches so scalar reads never touch axis_position and
-- rank-1 reads probe it once instead of four times. Constant filters on the
-- branch discriminators (tier, axis2_id) let the optimizer prune whole
-- branches of the UNION ALL.

-- One row per (axis, position): parametric axes materialized on the fly,
-- child tables unioned. pos = physical slot (0-based); ordinal = semantic
-- order (equals pos for sortable kinds — axes are stored monotonic, CF rule).
CREATE VIEW axis_position AS
-- parametric datetime: ts = start + pos*step, step is the (months, micros)
-- pair. unnest(range(...)) in the SELECT list, not a lateral subquery — the
-- lateral form measured ~5x slower per evaluation (critique F4 rework).
SELECT x.id AS axis_id, x.kind, x.pos, x.pos AS ordinal,
       x.p_start_ts + to_months(x.p_step_months * x.pos) + to_microseconds(x.p_step_micros * x.pos) AS ts,
       NULL::TIMESTAMP AS end_ts, NULL::BIGINT AS num, NULL::VARCHAR AS label,
       NULL::DOUBLE AS years_presented
FROM (SELECT a.id, a.kind, a.p_start_ts, a.p_step_months, a.p_step_micros,
             unnest(range(a.length))::INTEGER AS pos
      FROM axis a
      WHERE a.kind = 'datetime' AND a.p_start_ts IS NOT NULL) x
UNION ALL
-- materialized (irregular) datetime
SELECT a.id, a.kind, d.pos, d.pos, d.ts, NULL, NULL, NULL, NULL
FROM axis a JOIN axis_datetime d ON d.axis_id = a.id
WHERE a.kind = 'datetime' AND a.p_start_ts IS NULL
UNION ALL
-- parametric integer
SELECT x.id, x.kind, x.pos, x.pos, NULL, NULL,
       x.p_start_int + x.p_step_int * x.pos, NULL, NULL
FROM (SELECT a.id, a.kind, a.p_start_int, a.p_step_int,
             unnest(range(a.length))::INTEGER AS pos
      FROM axis a
      WHERE a.kind = 'integer' AND a.p_start_int IS NOT NULL) x
UNION ALL
-- materialized (irregular) integer (deviation D4)
SELECT a.id, a.kind, i.pos, i.pos, NULL, NULL, i.num, NULL, NULL
FROM axis a JOIN axis_integer i ON i.axis_id = a.id
WHERE a.kind = 'integer' AND a.p_start_int IS NULL
UNION ALL
-- periods: explicit stored [start, end) bounds (critique #10)
SELECT a.id, a.kind, p.pos, p.pos, p.start_ts, p.end_ts, NULL, NULL, p.years_presented
FROM axis a JOIN axis_period p ON p.axis_id = a.id
UNION ALL
-- labels: stored ordinal, scoped to the axis (GDX anti-lesson)
SELECT a.id, a.kind, l.pos, l.ordinal, NULL, NULL, NULL, l.label, NULL
FROM axis a JOIN axis_label l ON l.axis_id = a.id
UNION ALL
-- time patterns: terms of one position render as one unioned pattern text
SELECT a.id, a.kind, t.pos, any_value(t.ordinal), NULL, NULL, NULL,
       string_agg(t.unit || t.range_start || '-' || t.range_end, ',' ORDER BY t.term_no), NULL
FROM axis a JOIN axis_time_pattern t ON t.axis_id = a.id
GROUP BY a.id, a.kind, t.pos;

-- Array tier decoded to cells: row-major flat index -> per-dimension
-- positions (rank is metadata over flattened storage — VMP Part 3).
-- Zip-unnest form (critique F4a): values and range(len) unnest side by side
-- in one SELECT, so the cell value arrives with its flat position and no
-- per-cell list_extract runs (measured ~4x faster than vals[flat_pos + 1]).
CREATE VIEW value_array_cells AS
SELECT entity_id, parameter_definition_id, alternative_id,
       axis1_id, axis2_id, axis3_id, axis4_id,
       (flat_pos // (n2 * n3 * n4))::INTEGER        AS i1_pos,
       ((flat_pos // (n3 * n4)) % n2)::INTEGER      AS i2_pos,
       ((flat_pos // n4) % n3)::INTEGER             AS i3_pos,
       (flat_pos % n4)::INTEGER                     AS i4_pos,
       v                                            AS v_float
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
);

-- The tier-unifying cell view: current rows of all three tiers in one
-- logical long shape keyed by (triple, axis ids, positions). Tier choice is
-- invisible to readers (VMP Part 3); the straddling fix (critique #8) —
-- a parameter split across array and long tiers reads as one relation.
-- The long branch carries the full VMP Part 2 scalar-kind set including
-- date_time and duration (critique F9).
CREATE VIEW value_cells AS
SELECT entity_id, parameter_definition_id, alternative_id, 'scalar' AS tier,
       value_kind,
       NULL::BIGINT AS axis1_id, NULL::BIGINT AS axis2_id,
       NULL::BIGINT AS axis3_id, NULL::BIGINT AS axis4_id,
       0 AS i1_pos, 0 AS i2_pos, 0 AS i3_pos, 0 AS i4_pos,
       v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros
FROM value_scalar WHERE valid_to_commit IS NULL
UNION ALL
SELECT entity_id, parameter_definition_id, alternative_id, 'long',
       value_kind, axis1_id, axis2_id, axis3_id, axis4_id,
       i1_pos, i2_pos, i3_pos, i4_pos,
       v_float, v_int, v_bool, v_str, v_datetime, v_dur_months, v_dur_micros
FROM value_long WHERE valid_to_commit IS NULL
UNION ALL
SELECT entity_id, parameter_definition_id, alternative_id, 'array',
       'float', axis1_id, axis2_id, axis3_id, axis4_id,
       i1_pos, i2_pos, i3_pos, i4_pos,
       v_float, NULL, NULL, NULL, NULL, NULL, NULL
FROM value_array_cells;

-- Fully rendered long rows: cells joined to axis_position per dimension.
-- idxN_text carries the native text of label/time_pattern positions (NULL
-- for sortable kinds — clients render timestamps/integers from the typed
-- idxN_ts/idxN_num columns; the previous uniform ts::VARCHAR rendering cost
-- ~4.8 s alone on an 8.76M-cell read). Typed columns carry the native
-- index values; idxN_ordinal the semantic sort key.
-- Rank-specialized branches (critique F4b): the scalar branch performs no
-- axis joins at all, the rank-1 branch probes axis_position once, and only
-- rank >= 2 rows pay the four-way join. The branch predicates (tier,
-- axis2_id) are pushed into the tier scans, so a query that only touches
-- one shape scans only that shape. (A MATERIALIZED CTE over axis_position
-- was tried and rejected: it blocks outer-filter pushdown into the tier
-- scans, which costs far more than the repeated small evaluations.)
-- Stated non-goal (VMP Part 3): this view cannot push predicates into LIST
-- cells — engines wanting raw speed use the client library's bulk reader
-- (or the read_parameter_dense macro below).
CREATE VIEW value_rows AS
-- rank 0: scalars, no axis machinery
SELECT c.entity_id, c.parameter_definition_id, c.alternative_id, c.tier, c.value_kind,
       c.i1_pos, c.i2_pos, c.i3_pos, c.i4_pos,
       NULL::VARCHAR AS idx1_text, NULL::TIMESTAMP AS idx1_ts, NULL::TIMESTAMP AS idx1_end_ts,
       NULL::BIGINT AS idx1_num, NULL::INTEGER AS idx1_ordinal,
       NULL::VARCHAR AS idx2_text, NULL::TIMESTAMP AS idx2_ts, NULL::TIMESTAMP AS idx2_end_ts,
       NULL::BIGINT AS idx2_num, NULL::INTEGER AS idx2_ordinal,
       NULL::VARCHAR AS idx3_text, NULL::TIMESTAMP AS idx3_ts, NULL::TIMESTAMP AS idx3_end_ts,
       NULL::BIGINT AS idx3_num, NULL::INTEGER AS idx3_ordinal,
       NULL::VARCHAR AS idx4_text, NULL::TIMESTAMP AS idx4_ts, NULL::TIMESTAMP AS idx4_end_ts,
       NULL::BIGINT AS idx4_num, NULL::INTEGER AS idx4_ordinal,
       c.v_float, c.v_int, c.v_bool, c.v_str, c.v_datetime, c.v_dur_months, c.v_dur_micros
FROM value_cells c
WHERE c.tier = 'scalar'
UNION ALL
-- rank 1: one axis_position probe (LEFT JOIN so a corrupt axis renders NULL
-- and is caught by audit_axis_integrity / audit_position_range, never dropped)
SELECT c.entity_id, c.parameter_definition_id, c.alternative_id, c.tier, c.value_kind,
       c.i1_pos, c.i2_pos, c.i3_pos, c.i4_pos,
       p1.label, p1.ts, p1.end_ts, p1.num, p1.ordinal,
       NULL::VARCHAR, NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::BIGINT, NULL::INTEGER,
       NULL::VARCHAR, NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::BIGINT, NULL::INTEGER,
       NULL::VARCHAR, NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::BIGINT, NULL::INTEGER,
       c.v_float, c.v_int, c.v_bool, c.v_str, c.v_datetime, c.v_dur_months, c.v_dur_micros
FROM value_cells c
LEFT JOIN axis_position p1 ON p1.axis_id = c.axis1_id AND p1.pos = c.i1_pos
WHERE c.tier <> 'scalar' AND c.axis2_id IS NULL
UNION ALL
-- rank 2-4: full four-way rendering
SELECT c.entity_id, c.parameter_definition_id, c.alternative_id, c.tier, c.value_kind,
       c.i1_pos, c.i2_pos, c.i3_pos, c.i4_pos,
       p1.label, p1.ts, p1.end_ts, p1.num, p1.ordinal,
       p2.label, p2.ts, p2.end_ts, p2.num, p2.ordinal,
       p3.label, p3.ts, p3.end_ts, p3.num, p3.ordinal,
       p4.label, p4.ts, p4.end_ts, p4.num, p4.ordinal,
       c.v_float, c.v_int, c.v_bool, c.v_str, c.v_datetime, c.v_dur_months, c.v_dur_micros
FROM value_cells c
LEFT JOIN axis_position p1 ON p1.axis_id = c.axis1_id AND p1.pos = c.i1_pos
LEFT JOIN axis_position p2 ON p2.axis_id = c.axis2_id AND p2.pos = c.i2_pos
LEFT JOIN axis_position p3 ON p3.axis_id = c.axis3_id AND p3.pos = c.i3_pos
LEFT JOIN axis_position p4 ON p4.axis_id = c.axis4_id AND p4.pos = c.i4_pos
WHERE c.axis2_id IS NOT NULL;

-- Canonical bulk read (VMP Part 6): one logical long shape regardless of
-- tier mix. SQL macros cannot emit per-parameter dimension-name columns —
-- the client library renames idxN to the declared dimension names from
-- parameter_type_dimension (canonical result shape note in ddl_sketch.md).
-- The definition is resolved first so its id reaches the tier scans as a
-- selective join filter.
CREATE MACRO read_parameter(class_name, parameter_name) AS TABLE
WITH pd_pick AS (
  SELECT pd.id
  FROM parameter_definition pd
  JOIN entity_class ec ON ec.id = pd.entity_class_id AND ec.valid_to_commit IS NULL
  WHERE pd.valid_to_commit IS NULL
    AND lower(ec.name) = lower(class_name) AND lower(pd.name) = lower(parameter_name)
)
SELECT e.name AS entity, al.name AS alternative, vr.tier, vr.value_kind,
       vr.idx1_text, vr.idx2_text, vr.idx3_text, vr.idx4_text,
       vr.idx1_ts, vr.idx2_ts, vr.idx3_ts, vr.idx4_ts,
       vr.idx1_num, vr.idx2_num, vr.idx3_num, vr.idx4_num,
       vr.idx1_ordinal, vr.idx2_ordinal, vr.idx3_ordinal, vr.idx4_ordinal,
       vr.v_float, vr.v_int, vr.v_bool, vr.v_str, vr.v_datetime,
       vr.v_dur_months, vr.v_dur_micros
FROM value_rows vr
JOIN pd_pick ON pd_pick.id = vr.parameter_definition_id
JOIN entity e ON e.id = vr.entity_id AND e.valid_to_commit IS NULL
JOIN alternative al ON al.id = vr.alternative_id AND al.valid_to_commit IS NULL;

-- Hot-path bulk read, dense form (VMP Part 6 budget line "canonical or
-- dense form"): array-tier rows as list<double> payloads with axis ids —
-- the client library renders indices and entity/alternative names from its
-- cached dictionaries (ids only on purpose: routing the ~70 MB list payload
-- through name joins measured +45% wall time). This is the engine-side
-- budget path for large series; read_parameter is the convenience/display
-- path (critique F4c).
CREATE MACRO read_parameter_dense(class_name, parameter_name) AS TABLE
WITH pd_pick AS (
  SELECT pd.id
  FROM parameter_definition pd
  JOIN entity_class ec ON ec.id = pd.entity_class_id AND ec.valid_to_commit IS NULL
  WHERE pd.valid_to_commit IS NULL
    AND lower(ec.name) = lower(class_name) AND lower(pd.name) = lower(parameter_name)
)
SELECT va.entity_id, va.alternative_id,
       va.axis1_id, va.axis2_id, va.axis3_id, va.axis4_id,
       va.values, va.v_min, va.v_max
FROM value_array va
JOIN pd_pick ON pd_pick.id = va.parameter_definition_id;

-- Hot-path bulk read, scalar form: value_scalar only — no tier union, no
-- axis_position hash builds (a tier-generic read pays ~25 ms of fixed
-- axis-machinery overhead even when the parameter is scalar; this macro
-- reads a 10^5-row scalar parameter at raw-scan parity, VMP Part 6 second
-- budget line).
CREATE MACRO read_parameter_scalar(class_name, parameter_name) AS TABLE
WITH pd_pick AS (
  SELECT pd.id
  FROM parameter_definition pd
  JOIN entity_class ec ON ec.id = pd.entity_class_id AND ec.valid_to_commit IS NULL
  WHERE pd.valid_to_commit IS NULL
    AND lower(ec.name) = lower(class_name) AND lower(pd.name) = lower(parameter_name)
)
SELECT e.name AS entity, al.name AS alternative, vs.value_kind,
       vs.v_float, vs.v_int, vs.v_bool, vs.v_str, vs.v_datetime,
       vs.v_dur_months, vs.v_dur_micros
FROM value_scalar vs
JOIN pd_pick ON pd_pick.id = vs.parameter_definition_id
JOIN entity e ON e.id = vs.entity_id AND e.valid_to_commit IS NULL
JOIN alternative al ON al.id = vs.alternative_id AND al.valid_to_commit IS NULL
WHERE vs.valid_to_commit IS NULL;

-- Effective unit = override if present, else definition default; no
-- per-value replication anywhere (Decision 9).
CREATE VIEW parameter_effective_unit AS
SELECT e.id AS entity_id, pd.id AS parameter_definition_id,
       coalesce(o.unit_id, pd.unit_id) AS unit_id,
       (o.unit_id IS NOT NULL) AS overridden
FROM parameter_definition pd
JOIN entity e ON e.entity_class_id = pd.entity_class_id AND e.valid_to_commit IS NULL
LEFT JOIN entity_parameter_unit o
       ON o.entity_id = e.id AND o.parameter_definition_id = pd.id AND o.valid_to_commit IS NULL
WHERE pd.valid_to_commit IS NULL;
