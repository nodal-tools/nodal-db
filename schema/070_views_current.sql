-- 070_views_current.sql — current-state views: the default read surface
-- (Decision 2 binding rider: current-state views are the default read
-- surface and must stay cheap; NULL-statistics zonemaps prune fully-dead
-- row groups, research-verified). One `<table>_current` view per versioned
-- table, generated columns excluded; value_array is already current-only
-- and gets an alias view for tier symmetry.

-- change-detection cookie (#2252): clients poll one integer.
-- @surface: stable
CREATE VIEW dataset_version AS SELECT max(id) AS version FROM commit;

-- The commit log, one row per commit: its id (the validity edge every
-- versioned row cites, increasing), the time it was written (naive UTC),
-- its author and message. Append-only: rows are never changed or removed.
-- The base table `commit` is private like every base table; this is its
-- stable name. `timestamp` and `user` are SQL keywords in some engines:
-- quote them ("timestamp", "user").
-- @surface: stable
CREATE VIEW commit_log AS SELECT id, "timestamp", "user", message FROM "commit";

-- @surface: mirror
CREATE VIEW database_property_current AS
  SELECT * EXCLUDE (cur_key) FROM database_property WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW metadata_pair_current AS
  SELECT * EXCLUDE (cur_id, cur_name, cur_val) FROM metadata_pair WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW source_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM source WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW license_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM license WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW source_license_current AS
  SELECT * EXCLUDE (cur_s, cur_l) FROM source_license WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW metadata_role_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM metadata_role WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW object_metadata_link_current AS
  SELECT * EXCLUDE (cur_ot, cur_o, cur_tk, cur_t, cur_r) FROM object_metadata_link WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW value_metadata_link_current AS
  SELECT * EXCLUDE (cur_tk, cur_t, cur_r, cur_a, cur_p, cur_e) FROM value_metadata_link WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_class_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM entity_class WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW class_set_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM class_set WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW class_set_member_current AS
  SELECT * EXCLUDE (cur_s, cur_c) FROM class_set_member WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_class_dimension_current AS
  SELECT * EXCLUDE (cur_c, cur_p) FROM entity_class_dimension WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_current AS
  SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM entity WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_element_current AS
  SELECT * EXCLUDE (cur_e, cur_p) FROM entity_element WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_location_current AS
  SELECT * EXCLUDE (cur_e) FROM entity_location WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW alternative_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM alternative WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW scenario_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM scenario WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW scenario_alternative_current AS
  SELECT * EXCLUDE (cur_s, cur_a, cur_r) FROM scenario_alternative WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_alternative_current AS
  SELECT * EXCLUDE (cur_e, cur_a) FROM entity_alternative WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW membership_current AS
  SELECT * EXCLUDE (cur_g, cur_m, cur_a) FROM membership WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW units_of_measure_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM units_of_measure WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW parameter_value_list_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM parameter_value_list WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW list_value_current AS
  SELECT * EXCLUDE (cur_id, cur_l, cur_o) FROM list_value WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW parameter_definition_current AS
  SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM parameter_definition WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_parameter_unit_current AS
  SELECT * EXCLUDE (cur_e, cur_p) FROM entity_parameter_unit WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW parameter_type_current AS
  SELECT * EXCLUDE (cur_id) FROM parameter_type WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW parameter_type_dimension_current AS
  SELECT * EXCLUDE (cur_t, cur_p) FROM parameter_type_dimension WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW display_mode_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM display_mode WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW entity_class_display_mode_current AS
  SELECT * EXCLUDE (cur_dm, cur_ec) FROM entity_class_display_mode WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW semantic_annotation_current AS
  SELECT * EXCLUDE (cur_id) FROM semantic_annotation WHERE valid_to_commit IS NULL;
-- (no _current view for `run`: append-only, every row is current — 055_provenance.sql)

-- value tiers (VMP Part 5 "Reading history": current = one predicate)
-- @surface: mirror
CREATE VIEW value_scalar_current AS
  SELECT * EXCLUDE (ce, cp, ca) FROM value_scalar WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW value_long_current AS
  SELECT * EXCLUDE (ce, cp, ca, c1, c2, c3, c4) FROM value_long WHERE valid_to_commit IS NULL;
-- table-mirror view: shows every real column, including the physical row
-- handle array_id (the logical read surface in 080 does not expose it)
-- @surface: mirror
CREATE VIEW value_array_current AS
  SELECT * FROM value_array;   -- twin layout: the current table IS current state
-- @surface: mirror
CREATE VIEW definition_value_scalar_current AS
  SELECT * EXCLUDE (cp, cs) FROM definition_value_scalar WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW definition_value_long_current AS
  SELECT * EXCLUDE (cp, cs, c1, c2, c3, c4) FROM definition_value_long WHERE valid_to_commit IS NULL;
-- @surface: mirror
CREATE VIEW definition_value_array_current AS
  SELECT * EXCLUDE (cp, cs) FROM definition_value_array WHERE valid_to_commit IS NULL;
