-- FROZEN pre-change read rendering (main 244ce29, before
-- axis_position_cost_PLAN): axis_position, value_rows, read_parameter,
-- resolved_value_rows and audit_index_accelerator, verbatim except that each
-- is a TEMP object named *_old that reads the other *_old objects. The
-- equality oracle of tests/axis_render_equality.rs (invariant 2: the new
-- rendering must be bit-identical). Never edit it to make a test pass: a
-- difference is drift in the new text.

CREATE OR REPLACE TEMP VIEW axis_position_old AS
-- parametric datetime: ts = start + pos*step, step is the (months, micros)
-- pair. unnest(range(...)) in the SELECT list, not a lateral subquery — the
-- lateral form measured ~5x slower per evaluation (critique F4 rework).
SELECT x.id AS axis_id, x.kind, x.pos, x.pos AS ordinal,
       x.p_start_ts + to_months(x.p_step_months * x.pos) + to_microseconds(x.p_step_micros * x.pos) AS ts,
       NULL::TIMESTAMP AS end_ts, NULL::BIGINT AS num, NULL::VARCHAR AS label,
       NULL::DOUBLE AS years_presented, NULL::DOUBLE AS coord
FROM (SELECT a.id, a.kind, a.p_start_ts, a.p_step_months, a.p_step_micros,
             unnest(range(a.length))::INTEGER AS pos
      FROM axis a
      WHERE a.kind = 'datetime' AND a.p_start_ts IS NOT NULL) x
UNION ALL
-- materialized (irregular) datetime
SELECT a.id, a.kind, d.pos, d.pos, d.ts, NULL, NULL, NULL, NULL, NULL
FROM axis a JOIN axis_datetime d ON d.axis_id = a.id
WHERE a.kind = 'datetime' AND a.p_start_ts IS NULL
UNION ALL
-- parametric integer
SELECT x.id, x.kind, x.pos, x.pos, NULL, NULL,
       x.p_start_int + x.p_step_int * x.pos, NULL, NULL, NULL
FROM (SELECT a.id, a.kind, a.p_start_int, a.p_step_int,
             unnest(range(a.length))::INTEGER AS pos
      FROM axis a
      WHERE a.kind = 'integer' AND a.p_start_int IS NOT NULL) x
UNION ALL
-- materialized (irregular) integer (deviation D4)
SELECT a.id, a.kind, i.pos, i.pos, NULL, NULL, i.num, NULL, NULL, NULL
FROM axis a JOIN axis_integer i ON i.axis_id = a.id
WHERE a.kind = 'integer' AND a.p_start_int IS NULL
UNION ALL
-- periods: explicit stored [start, end) bounds (critique #10)
SELECT a.id, a.kind, p.pos, p.pos, p.start_ts, p.end_ts, NULL, NULL, p.years_presented, NULL
FROM axis a JOIN axis_period p ON p.axis_id = a.id
UNION ALL
-- labels: stored ordinal, scoped to the axis (GDX anti-lesson)
SELECT a.id, a.kind, l.pos, l.ordinal, NULL, NULL, NULL, l.label, NULL, NULL
FROM axis a JOIN axis_label l ON l.axis_id = a.id
UNION ALL
-- time patterns: terms of one position render as one unioned pattern text
SELECT a.id, a.kind, t.pos, any_value(t.ordinal), NULL, NULL, NULL,
       string_agg(t.unit || t.range_start || '-' || t.range_end, ',' ORDER BY t.term_no), NULL, NULL
FROM axis a JOIN axis_time_pattern t ON t.axis_id = a.id
GROUP BY a.id, a.kind, t.pos
UNION ALL
-- parametric float: coord = p_start_dbl + p_step_dbl*pos (THE expression the
-- writer uses to test bit-exact regularity — parametric-first, VMP Part 4 §3.3)
SELECT x.id, x.kind, x.pos, x.pos AS ordinal,
       NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::BIGINT, NULL::VARCHAR, NULL::DOUBLE,
       x.p_start_dbl + x.p_step_dbl * x.pos AS coord
FROM (SELECT a.id, a.kind, a.p_start_dbl, a.p_step_dbl,
             unnest(range(a.length))::INTEGER AS pos
      FROM axis a
      WHERE a.kind = 'float' AND a.p_start_dbl IS NOT NULL) x
UNION ALL
-- materialized (irregular) float
SELECT a.id, a.kind, f.pos, f.pos, NULL, NULL, NULL, NULL, NULL, f.coord
FROM axis a JOIN axis_float f ON f.axis_id = a.id
WHERE a.kind = 'float' AND a.p_start_dbl IS NULL;

CREATE OR REPLACE TEMP VIEW value_rows_old AS
-- rank 0: scalars, no axis machinery
SELECT c.entity_id, c.parameter_definition_id, c.alternative_id, c.tier, c.value_kind,
       c.i1_pos, c.i2_pos, c.i3_pos, c.i4_pos,
       NULL::VARCHAR AS idx1_text, NULL::TIMESTAMP AS idx1_ts, NULL::TIMESTAMP AS idx1_end_ts,
       NULL::BIGINT AS idx1_num, NULL::INTEGER AS idx1_ordinal, NULL::DOUBLE AS idx1_coord,
       NULL::VARCHAR AS idx2_text, NULL::TIMESTAMP AS idx2_ts, NULL::TIMESTAMP AS idx2_end_ts,
       NULL::BIGINT AS idx2_num, NULL::INTEGER AS idx2_ordinal, NULL::DOUBLE AS idx2_coord,
       NULL::VARCHAR AS idx3_text, NULL::TIMESTAMP AS idx3_ts, NULL::TIMESTAMP AS idx3_end_ts,
       NULL::BIGINT AS idx3_num, NULL::INTEGER AS idx3_ordinal, NULL::DOUBLE AS idx3_coord,
       NULL::VARCHAR AS idx4_text, NULL::TIMESTAMP AS idx4_ts, NULL::TIMESTAMP AS idx4_end_ts,
       NULL::BIGINT AS idx4_num, NULL::INTEGER AS idx4_ordinal, NULL::DOUBLE AS idx4_coord,
       c.v_float, c.v_int, c.v_bool, c.v_str, c.v_datetime, c.v_dur_months, c.v_dur_micros
FROM value_cells c
WHERE c.tier = 'scalar'
UNION ALL
-- rank 1: one axis_position probe (LEFT JOIN so a corrupt axis renders NULL
-- and is caught by audit_axis_integrity / audit_position_range, never dropped)
SELECT c.entity_id, c.parameter_definition_id, c.alternative_id, c.tier, c.value_kind,
       c.i1_pos, c.i2_pos, c.i3_pos, c.i4_pos,
       p1.label, p1.ts, p1.end_ts, p1.num, p1.ordinal, p1.coord,
       NULL::VARCHAR, NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::BIGINT, NULL::INTEGER, NULL::DOUBLE,
       NULL::VARCHAR, NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::BIGINT, NULL::INTEGER, NULL::DOUBLE,
       NULL::VARCHAR, NULL::TIMESTAMP, NULL::TIMESTAMP, NULL::BIGINT, NULL::INTEGER, NULL::DOUBLE,
       c.v_float, c.v_int, c.v_bool, c.v_str, c.v_datetime, c.v_dur_months, c.v_dur_micros
FROM value_cells c
LEFT JOIN axis_position_old p1 ON p1.axis_id = c.axis1_id AND p1.pos = c.i1_pos
WHERE c.tier <> 'scalar' AND c.axis2_id IS NULL
UNION ALL
-- rank 2-4: full four-way rendering
SELECT c.entity_id, c.parameter_definition_id, c.alternative_id, c.tier, c.value_kind,
       c.i1_pos, c.i2_pos, c.i3_pos, c.i4_pos,
       p1.label, p1.ts, p1.end_ts, p1.num, p1.ordinal, p1.coord,
       p2.label, p2.ts, p2.end_ts, p2.num, p2.ordinal, p2.coord,
       p3.label, p3.ts, p3.end_ts, p3.num, p3.ordinal, p3.coord,
       p4.label, p4.ts, p4.end_ts, p4.num, p4.ordinal, p4.coord,
       c.v_float, c.v_int, c.v_bool, c.v_str, c.v_datetime, c.v_dur_months, c.v_dur_micros
FROM value_cells c
LEFT JOIN axis_position_old p1 ON p1.axis_id = c.axis1_id AND p1.pos = c.i1_pos
LEFT JOIN axis_position_old p2 ON p2.axis_id = c.axis2_id AND p2.pos = c.i2_pos
LEFT JOIN axis_position_old p3 ON p3.axis_id = c.axis3_id AND p3.pos = c.i3_pos
LEFT JOIN axis_position_old p4 ON p4.axis_id = c.axis4_id AND p4.pos = c.i4_pos
WHERE c.axis2_id IS NOT NULL;

CREATE OR REPLACE TEMP MACRO read_parameter_old(class_name, parameter_name) AS TABLE
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
       vr.idx1_coord, vr.idx2_coord, vr.idx3_coord, vr.idx4_coord,
       vr.v_float, vr.v_int, vr.v_bool, vr.v_str, vr.v_datetime,
       vr.v_dur_months, vr.v_dur_micros
FROM value_rows_old vr
JOIN pd_pick ON pd_pick.id = vr.parameter_definition_id
JOIN entity e ON e.id = vr.entity_id AND e.valid_to_commit IS NULL
JOIN alternative al ON al.id = vr.alternative_id AND al.valid_to_commit IS NULL;

CREATE OR REPLACE TEMP MACRO resolved_value_rows_old(scenario_name) AS TABLE
SELECT s.name AS scenario, e.name AS entity, ec.name AS entity_class,
       pd.name AS parameter, al.name AS alternative, r.rank, vr.tier, vr.value_kind,
       vr.idx1_text, vr.idx2_text, vr.idx3_text, vr.idx4_text,
       vr.idx1_ts, vr.idx2_ts, vr.idx3_ts, vr.idx4_ts,
       vr.idx1_num, vr.idx2_num, vr.idx3_num, vr.idx4_num,
       vr.v_float, vr.v_int, vr.v_bool, vr.v_str, vr.v_datetime,
       vr.v_dur_months, vr.v_dur_micros
FROM scenario_value_resolved r
JOIN scenario s ON s.id = r.scenario_id AND s.valid_to_commit IS NULL
JOIN value_rows_old vr ON vr.entity_id = r.entity_id
                  AND vr.parameter_definition_id = r.parameter_definition_id
                  AND vr.alternative_id = r.alternative_id
JOIN entity e ON e.id = r.entity_id AND e.valid_to_commit IS NULL
JOIN entity_class ec ON ec.id = e.entity_class_id AND ec.valid_to_commit IS NULL
JOIN parameter_definition pd ON pd.id = r.parameter_definition_id AND pd.valid_to_commit IS NULL
JOIN alternative al ON al.id = r.alternative_id AND al.valid_to_commit IS NULL
WHERE lower(s.name) = lower(scenario_name);

CREATE OR REPLACE TEMP VIEW audit_index_accelerator_old AS
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
LEFT JOIN axis_position_old p ON p.axis_id = d.axis_id AND p.pos = d.pos
WHERE p.axis_id IS NULL   -- unrenderable position (critique F3)
   OR (p.kind IN ('datetime', 'period') AND (d.acc_dt IS DISTINCT FROM p.ts OR d.acc_num IS NOT NULL))
   OR (p.kind = 'integer' AND (d.acc_num IS DISTINCT FROM p.num OR d.acc_dt IS NOT NULL))
   OR (p.kind IN ('label', 'time_pattern', 'float') AND (d.acc_dt IS NOT NULL OR d.acc_num IS NOT NULL));
