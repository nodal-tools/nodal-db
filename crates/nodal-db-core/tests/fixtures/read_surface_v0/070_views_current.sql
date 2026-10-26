-- 070_views_current.sql — current-state views: the default read surface
-- (Decision 2 binding rider: current-state views are the default read
-- surface and must stay cheap; NULL-statistics zonemaps prune fully-dead
-- row groups, research-verified). One `<table>_current` view per versioned
-- table, generated columns excluded; value_array is already current-only
-- and gets an alias view for tier symmetry.

-- change-detection cookie (#2252): clients poll one integer.
CREATE VIEW dataset_version AS SELECT max(id) AS version FROM commit;

CREATE VIEW metadata_current AS
  SELECT * EXCLUDE (cur_id, cur_name, cur_val) FROM metadata WHERE valid_to_commit IS NULL;
CREATE VIEW entity_metadata_current AS
  SELECT * EXCLUDE (cur_e, cur_m) FROM entity_metadata WHERE valid_to_commit IS NULL;
CREATE VIEW parameter_value_metadata_current AS
  SELECT * EXCLUDE (cur_e, cur_p, cur_a, cur_m) FROM parameter_value_metadata WHERE valid_to_commit IS NULL;
CREATE VIEW entity_class_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM entity_class WHERE valid_to_commit IS NULL;
CREATE VIEW class_set_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM class_set WHERE valid_to_commit IS NULL;
CREATE VIEW class_set_member_current AS
  SELECT * EXCLUDE (cur_s, cur_c) FROM class_set_member WHERE valid_to_commit IS NULL;
CREATE VIEW entity_class_dimension_current AS
  SELECT * EXCLUDE (cur_c, cur_p) FROM entity_class_dimension WHERE valid_to_commit IS NULL;
CREATE VIEW entity_current AS
  SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM entity WHERE valid_to_commit IS NULL;
CREATE VIEW entity_element_current AS
  SELECT * EXCLUDE (cur_e, cur_p) FROM entity_element WHERE valid_to_commit IS NULL;
CREATE VIEW entity_location_current AS
  SELECT * EXCLUDE (cur_e) FROM entity_location WHERE valid_to_commit IS NULL;
CREATE VIEW alternative_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM alternative WHERE valid_to_commit IS NULL;
CREATE VIEW scenario_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM scenario WHERE valid_to_commit IS NULL;
CREATE VIEW scenario_alternative_current AS
  SELECT * EXCLUDE (cur_s, cur_a, cur_r) FROM scenario_alternative WHERE valid_to_commit IS NULL;
CREATE VIEW entity_alternative_current AS
  SELECT * EXCLUDE (cur_e, cur_a) FROM entity_alternative WHERE valid_to_commit IS NULL;
CREATE VIEW membership_current AS
  SELECT * EXCLUDE (cur_g, cur_m, cur_a) FROM membership WHERE valid_to_commit IS NULL;
CREATE VIEW units_of_measure_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM units_of_measure WHERE valid_to_commit IS NULL;
CREATE VIEW parameter_value_list_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM parameter_value_list WHERE valid_to_commit IS NULL;
CREATE VIEW list_value_current AS
  SELECT * EXCLUDE (cur_id, cur_l, cur_o) FROM list_value WHERE valid_to_commit IS NULL;
CREATE VIEW parameter_definition_current AS
  SELECT * EXCLUDE (cur_id, cur_cls, cur_name) FROM parameter_definition WHERE valid_to_commit IS NULL;
CREATE VIEW entity_parameter_unit_current AS
  SELECT * EXCLUDE (cur_e, cur_p) FROM entity_parameter_unit WHERE valid_to_commit IS NULL;
CREATE VIEW parameter_type_current AS
  SELECT * EXCLUDE (cur_id) FROM parameter_type WHERE valid_to_commit IS NULL;
CREATE VIEW parameter_type_dimension_current AS
  SELECT * EXCLUDE (cur_t, cur_p) FROM parameter_type_dimension WHERE valid_to_commit IS NULL;
CREATE VIEW display_mode_current AS
  SELECT * EXCLUDE (cur_id, cur_name) FROM display_mode WHERE valid_to_commit IS NULL;
CREATE VIEW entity_class_display_mode_current AS
  SELECT * EXCLUDE (cur_dm, cur_ec) FROM entity_class_display_mode WHERE valid_to_commit IS NULL;
CREATE VIEW semantic_annotation_current AS
  SELECT * EXCLUDE (cur_id) FROM semantic_annotation WHERE valid_to_commit IS NULL;
-- (no _current view for `run`: append-only, every row is current — 055_provenance.sql)

-- value tiers (VMP Part 5 "Reading history": current = one predicate)
CREATE VIEW value_scalar_current AS
  SELECT * EXCLUDE (ce, cp, ca) FROM value_scalar WHERE valid_to_commit IS NULL;
CREATE VIEW value_long_current AS
  SELECT * EXCLUDE (ce, cp, ca, c1, c2, c3, c4) FROM value_long WHERE valid_to_commit IS NULL;
-- table-mirror view: shows every real column, including the physical row
-- handle array_id (the logical read surface in 080 does not expose it)
CREATE VIEW value_array_current AS
  SELECT * FROM value_array;   -- twin layout: the current table IS current state
CREATE VIEW definition_value_scalar_current AS
  SELECT * EXCLUDE (cp, cs) FROM definition_value_scalar WHERE valid_to_commit IS NULL;
CREATE VIEW definition_value_long_current AS
  SELECT * EXCLUDE (cp, cs, c1, c2, c3, c4) FROM definition_value_long WHERE valid_to_commit IS NULL;
CREATE VIEW definition_value_array_current AS
  SELECT * EXCLUDE (cp, cs) FROM definition_value_array WHERE valid_to_commit IS NULL;
