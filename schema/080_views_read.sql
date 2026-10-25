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

-- Array tier decoded to cells: row-major flat index -> per-dimension
-- positions (rank is metadata over flattened storage — VMP Part 3).
-- Zip-unnest form (critique F4a): the payload and range(len) unnest side by
-- side in one SELECT, so the cell value arrives with its flat position and no
-- per-cell list_extract runs (measured ~4x faster than vals[flat_pos + 1]).
-- One branch over BOTH payload columns: exactly one of values/values_int is
-- populated (the XOR CHECK), and the NULL one unnests as NULL padding beside
-- the other, so float and int rows decode in the same pass.
-- Narrow-first self-join: `k` resolves the keys without touching the list
-- columns; `p` fetches the payload by the single-column key array_id. A
-- selective outer filter (one value's triple) then reaches the payload scan
-- as an IN filter on array_id, which DuckDB serves by an index scan instead
-- of scanning the list data of the whole table.
CREATE VIEW value_array_cells AS
SELECT entity_id, parameter_definition_id, alternative_id,
       axis1_id, axis2_id, axis3_id, axis4_id,
       (flat_pos // (n2 * n3 * n4))::INTEGER        AS i1_pos,
       ((flat_pos // (n3 * n4)) % n2)::INTEGER      AS i2_pos,
       ((flat_pos // n4) % n3)::INTEGER             AS i3_pos,
       (flat_pos % n4)::INTEGER                     AS i4_pos,
       CASE WHEN is_float THEN 'float' ELSE 'int' END AS value_kind,
       vf                                           AS v_float,
       vi                                           AS v_int
FROM (
  SELECT k.entity_id, k.parameter_definition_id, k.alternative_id,
         k.axis1_id, k.axis2_id, k.axis3_id, k.axis4_id,
         coalesce(a2.length, 1) AS n2,
         coalesce(a3.length, 1) AS n3,
         coalesce(a4.length, 1) AS n4,
         p.values IS NOT NULL AS is_float,
         unnest(p.values) AS vf,
         unnest(p.values_int) AS vi,
         unnest(range(coalesce(len(p.values), len(p.values_int)))) AS flat_pos
  FROM value_array k
  JOIN value_array p ON p.array_id = k.array_id
  LEFT JOIN axis a2 ON a2.id = k.axis2_id
  LEFT JOIN axis a3 ON a3.id = k.axis3_id
  LEFT JOIN axis a4 ON a4.id = k.axis4_id
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
       value_kind, axis1_id, axis2_id, axis3_id, axis4_id,
       i1_pos, i2_pos, i3_pos, i4_pos,
       v_float, v_int, NULL, NULL, NULL, NULL, NULL
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
-- (or the read_parameter_dense macro below). Point reads of array values
-- go through read_value / read_values / value_rows_for (rendered, below),
-- read_parameter_dense or value_cells, which fetch the payload by
-- array_id; this view's rank branches keep a full scan of the array payload
-- even for a point read.
CREATE VIEW value_rows AS
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
LEFT JOIN axis_position p1 ON p1.axis_id = c.axis1_id AND p1.pos = c.i1_pos
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
LEFT JOIN axis_position p1 ON p1.axis_id = c.axis1_id AND p1.pos = c.i1_pos
LEFT JOIN axis_position p2 ON p2.axis_id = c.axis2_id AND p2.pos = c.i2_pos
LEFT JOIN axis_position p3 ON p3.axis_id = c.axis3_id AND p3.pos = c.i3_pos
LEFT JOIN axis_position p4 ON p4.axis_id = c.axis4_id AND p4.pos = c.i4_pos
WHERE c.axis2_id IS NOT NULL;

-- Canonical bulk read: one logical long shape regardless of
-- tier mix. SQL macros cannot emit per-parameter dimension-name columns —
-- the client library renames idxN to the declared dimension names from
-- parameter_type_dimension (canonical result shape note in ddl_sketch.md).
-- D2(a)/D11 (template_ir_plan §3.3, D11 2026-09-18): under a declared
-- allowed-type SET, every writer-produced value row persists the member it
-- bound to (`parameter_type_id` on the payload tables), so bound rows are
-- EXACTLY labelable — the client resolves the row's member and renames idxN
-- to that member's declared dimension names. The blanket rename by
-- definition alone remains well-defined only when all current members agree
-- on the name vector per (rank, index-kind signature). For NULL-binding rows
-- only (pre-D11 data, out-of-contract writers) member attribution is
-- unrecoverable — axes carry kinds, not names; clients may disambiguate such
-- rows by rank and the populated index-kind columns (idxN_ts/idxN_num/
-- idxN_text). These read macros do not yet expose the binding column; a
-- later change MAY add it.
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
       vr.idx1_coord, vr.idx2_coord, vr.idx3_coord, vr.idx4_coord,
       vr.v_float, vr.v_int, vr.v_bool, vr.v_str, vr.v_datetime,
       vr.v_dur_months, vr.v_dur_micros
FROM value_rows vr
JOIN pd_pick ON pd_pick.id = vr.parameter_definition_id
JOIN entity e ON e.id = vr.entity_id AND e.valid_to_commit IS NULL
JOIN alternative al ON al.id = vr.alternative_id AND al.valid_to_commit IS NULL;

-- Hot-path bulk read, dense form: array-tier rows as list<double> payloads
-- with axis ids —
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
), k AS (
  -- narrow key resolution: never touches the list columns
  SELECT va.array_id, va.entity_id, va.alternative_id,
         va.axis1_id, va.axis2_id, va.axis3_id, va.axis4_id, va.v_min, va.v_max
  FROM value_array va
  JOIN pd_pick ON pd_pick.id = va.parameter_definition_id
)
-- payload fetch by the single-column key: an outer filter on entity or
-- alternative reaches this scan as an IN filter on array_id (index scan, up
-- to 50 keys; larger key sets and whole-parameter reads scan, as before)
SELECT k.entity_id, k.alternative_id,
       k.axis1_id, k.axis2_id, k.axis3_id, k.axis4_id,
       p.values, p.values_int, k.v_min, k.v_max
FROM k
JOIN value_array p ON p.array_id = k.array_id;

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

-- ---------------------------------------------------------------------------
-- Point and few-key reads (fast_point_reads_PLAN): the current rows of ONE
-- parameter for a small key set, rendered exactly like value_rows /
-- read_parameter, without scanning the array payload of the whole table.
-- read_parameter / value_rows cannot do this: a filter written outside a
-- macro or view never reaches its tier scans. One body (value_rows_for),
-- two name-level entry points (read_values, read_value).
--
-- How the payload is fetched (docs/dev/engine_rules.md §2 and §6; both are
-- optimizer behaviour, guarded by tests/point_read_index_scan.rs):
--   * keys are resolved on narrow columns first, then folded into ONE
--     scalar list: `array_id = ANY(coalesce((SELECT list(DISTINCT …)), [-1]))`.
--     That is an index scan of the payload for 0 to 50 keys. [-1] stands in
--     only for an EMPTY list (a scalar/long parameter, a miss): a sentinel
--     appended to a non-empty list takes one of the 50 dynamic-filter slots.
--     Never a `UNION ALL SELECT -1` sentinel: inside this macro it falls
--     back to a full payload scan. Above 50 value hits (existing arrays,
--     i.e. entities x alternatives, not names) the scan is sequential.
--   * no column produced by a join's build side passes through the UNNEST
--     (the DuckDB 2.0 dict-cache bug, engine_rules §6): the payload scan
--     meets its key list in a SEMI join, which emits probe-side columns
--     only, and the axis-length lookups run after the unnest.
--   * axis_position is evaluated only for the value's own axes (`ap`): the
--     unfiltered view costs in proportion to every axis in the file.
-- Input keys are deduplicated (kk is DISTINCT): a repeated id must neither
-- duplicate scalar/long rows nor take extra dynamic-filter slots.
-- Row order is unspecified, as for read_parameter.

-- Point read by ids, in value_rows' columns: the current rows of one
-- parameter for lists of entity and alternative ids (every pair).
CREATE MACRO value_rows_for(pd_id_arg, entity_ids, alternative_ids) AS TABLE
WITH kk AS (
  SELECT DISTINCT pd_id_arg::BIGINT AS pd_id, e.id AS e_id, a.id AS al_id
  FROM (SELECT unnest(entity_ids) AS id) e CROSS JOIN (SELECT unnest(alternative_ids) AS id) a
), cells AS MATERIALIZED (
  SELECT vs.entity_id, vs.alternative_id, 'scalar' AS tier, vs.value_kind,
         NULL::BIGINT AS axis1_id, NULL::BIGINT AS axis2_id, NULL::BIGINT AS axis3_id, NULL::BIGINT AS axis4_id,
         0 AS i1_pos, 0 AS i2_pos, 0 AS i3_pos, 0 AS i4_pos,
         vs.v_float, vs.v_int, vs.v_bool, vs.v_str, vs.v_datetime, vs.v_dur_months, vs.v_dur_micros
  FROM value_scalar vs
  JOIN kk ON vs.entity_id = kk.e_id AND vs.parameter_definition_id = kk.pd_id AND vs.alternative_id = kk.al_id
  WHERE vs.valid_to_commit IS NULL
  UNION ALL
  SELECT vl.entity_id, vl.alternative_id, 'long', vl.value_kind, vl.axis1_id, vl.axis2_id, vl.axis3_id, vl.axis4_id,
         vl.i1_pos, vl.i2_pos, vl.i3_pos, vl.i4_pos,
         vl.v_float, vl.v_int, vl.v_bool, vl.v_str, vl.v_datetime, vl.v_dur_months, vl.v_dur_micros
  FROM value_long vl
  JOIN kk ON vl.entity_id = kk.e_id AND vl.parameter_definition_id = kk.pd_id AND vl.alternative_id = kk.al_id
  WHERE vl.valid_to_commit IS NULL
  UNION ALL
  -- array leg: the payload is fetched by array_id through the scalar key
  -- list (index scan), and every column that is unnested or passed through
  -- comes from the payload scan itself (engine_rules §6).
  SELECT x.entity_id, x.alternative_id, 'array', CASE WHEN x.is_float THEN 'float' ELSE 'int' END,
         x.axis1_id, x.axis2_id, x.axis3_id, x.axis4_id,
         (x.flat_pos // (n2 * n3 * n4))::INTEGER, ((x.flat_pos // (n3 * n4)) % n2)::INTEGER,
         ((x.flat_pos // n4) % n3)::INTEGER, (x.flat_pos % n4)::INTEGER,
         x.vf, x.vi, NULL, NULL, NULL, NULL, NULL
  FROM (
    SELECT p.entity_id, p.alternative_id, p.axis1_id, p.axis2_id, p.axis3_id, p.axis4_id,
           p.values IS NOT NULL AS is_float,
           unnest(p.values) AS vf, unnest(p.values_int) AS vi,
           unnest(range(coalesce(len(p.values), len(p.values_int)))) AS flat_pos
    FROM value_array p
    WHERE p.array_id = ANY(coalesce(
            (SELECT list(DISTINCT va.array_id) FROM value_array va
             JOIN kk ON va.entity_id = kk.e_id AND va.parameter_definition_id = kk.pd_id
                    AND va.alternative_id = kk.al_id),
            [-1]))
  ) x,
  LATERAL (SELECT coalesce((SELECT length FROM axis WHERE id = x.axis2_id), 1) AS n2,
                  coalesce((SELECT length FROM axis WHERE id = x.axis3_id), 1) AS n3,
                  coalesce((SELECT length FROM axis WHERE id = x.axis4_id), 1) AS n4)
), ax AS (
  SELECT DISTINCT a FROM (SELECT unnest([axis1_id, axis2_id, axis3_id, axis4_id]) AS a
                          FROM (SELECT DISTINCT axis1_id, axis2_id, axis3_id, axis4_id FROM cells))
  WHERE a IS NOT NULL
), ap AS MATERIALIZED (
  -- the value's own axes only (a runtime join filter reaches the axis scans)
  SELECT * FROM axis_position WHERE axis_id IN (SELECT a FROM ax)
)
SELECT c.entity_id, pd_id_arg::BIGINT AS parameter_definition_id, c.alternative_id, c.tier, c.value_kind,
       c.i1_pos, c.i2_pos, c.i3_pos, c.i4_pos,
       p1.label AS idx1_text, p1.ts AS idx1_ts, p1.end_ts AS idx1_end_ts, p1.num AS idx1_num, p1.ordinal AS idx1_ordinal, p1.coord AS idx1_coord,
       p2.label AS idx2_text, p2.ts AS idx2_ts, p2.end_ts AS idx2_end_ts, p2.num AS idx2_num, p2.ordinal AS idx2_ordinal, p2.coord AS idx2_coord,
       p3.label AS idx3_text, p3.ts AS idx3_ts, p3.end_ts AS idx3_end_ts, p3.num AS idx3_num, p3.ordinal AS idx3_ordinal, p3.coord AS idx3_coord,
       p4.label AS idx4_text, p4.ts AS idx4_ts, p4.end_ts AS idx4_end_ts, p4.num AS idx4_num, p4.ordinal AS idx4_ordinal, p4.coord AS idx4_coord,
       c.v_float, c.v_int, c.v_bool, c.v_str, c.v_datetime, c.v_dur_months, c.v_dur_micros
FROM cells c
LEFT JOIN ap p1 ON p1.axis_id = c.axis1_id AND p1.pos = c.i1_pos
LEFT JOIN ap p2 ON p2.axis_id = c.axis2_id AND p2.pos = c.i2_pos
LEFT JOIN ap p3 ON p3.axis_id = c.axis3_id AND p3.pos = c.i3_pos
LEFT JOIN ap p4 ON p4.axis_id = c.axis4_id AND p4.pos = c.i4_pos;

-- Point read of a few values by name, in read_parameter's columns: lists
-- of entity and alternative names (every pair). Names match
-- case-insensitively (lower()), like the class and parameter arguments.
-- alternative_names NULL (the default) = every current alternative;
-- [] = none.
CREATE MACRO read_values(class_name, parameter_name, entity_names, alternative_names := NULL) AS TABLE
WITH pd_pick AS (
  SELECT pd.id, pd.entity_class_id FROM parameter_definition pd
  JOIN entity_class ec ON ec.id = pd.entity_class_id AND ec.valid_to_commit IS NULL
  WHERE pd.valid_to_commit IS NULL
    AND lower(ec.name) = lower(class_name) AND lower(pd.name) = lower(parameter_name)
), e_pick AS (
  SELECT e.id, e.name FROM entity e JOIN pd_pick ON e.entity_class_id = pd_pick.entity_class_id
  WHERE e.valid_to_commit IS NULL AND lower(e.name) IN (SELECT lower(unnest(entity_names)))
), al_pick AS (
  SELECT al.id, al.name FROM alternative al
  WHERE al.valid_to_commit IS NULL
    AND (alternative_names IS NULL
         OR lower(al.name) IN (SELECT lower(unnest(alternative_names))))
)
SELECT e.name AS entity, al.name AS alternative, vr.tier, vr.value_kind,
       vr.idx1_text, vr.idx2_text, vr.idx3_text, vr.idx4_text,
       vr.idx1_ts, vr.idx2_ts, vr.idx3_ts, vr.idx4_ts,
       vr.idx1_num, vr.idx2_num, vr.idx3_num, vr.idx4_num,
       vr.idx1_ordinal, vr.idx2_ordinal, vr.idx3_ordinal, vr.idx4_ordinal,
       vr.idx1_coord, vr.idx2_coord, vr.idx3_coord, vr.idx4_coord,
       vr.v_float, vr.v_int, vr.v_bool, vr.v_str, vr.v_datetime, vr.v_dur_months, vr.v_dur_micros
FROM value_rows_for(coalesce((SELECT id FROM pd_pick), -1),
                    coalesce((SELECT list(id) FROM e_pick), []),
                    coalesce((SELECT list(id) FROM al_pick), [])) vr
JOIN e_pick e ON e.id = vr.entity_id
JOIN al_pick al ON al.id = vr.alternative_id;

-- Point read of one value by name, in read_parameter's columns (one entity
-- in every current alternative when the alternative is omitted). [NULL] is
-- not NULL, so an omitted alternative is passed through as NULL rather
-- than wrapped, or it would silently match nothing.
CREATE MACRO read_value(class_name, parameter_name, entity_name, alternative_name := NULL) AS TABLE
SELECT * FROM read_values(class_name, parameter_name, [entity_name],
                          CASE WHEN alternative_name IS NULL THEN NULL ELSE [alternative_name] END);

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

-- Resolved per-mode class presentation (Spine-Toolbox #2492): every current
-- class gets a row; a class with no entity_class_display_mode row defaults to
-- visible + its own display_order (greyed-not-hidden means a class is never
-- dropped from the list). Any interface renders this without tool-specific
-- knowledge. Views take no parameters, so this is a MACRO (cf. the *_asof
-- macros / read_parameter).
CREATE MACRO entity_class_display(mode_name) AS TABLE
SELECT dm.name                                        AS display_mode,
       ec.id                                          AS entity_class_id,
       ec.name                                        AS entity_class,
       coalesce(ecdm.display_status, 'visible')       AS display_status,
       coalesce(ecdm.display_order, ec.display_order) AS display_order,
       ecdm.font_color,
       ecdm.background_color
FROM display_mode dm
CROSS JOIN entity_class ec
LEFT JOIN entity_class_display_mode ecdm
  ON ecdm.display_mode_id = dm.id AND ecdm.entity_class_id = ec.id
  AND ecdm.valid_to_commit IS NULL
WHERE dm.valid_to_commit IS NULL AND ec.valid_to_commit IS NULL
  AND dm.cur_name = lower(mode_name);
