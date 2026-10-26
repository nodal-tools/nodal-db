-- 130_read_surface.sql — the version of the read surface this file carries.
--
-- The read surface is every view and macro in 070_views_current.sql through
-- 120_views_audit.sql plus this file (docs/dev/read_surface.md). Those
-- objects live inside each database file, so a file keeps the surface of the
-- build that created (or last refreshed) it. This macro states which one:
--
--   SELECT * FROM nodal_read_surface();
--
-- contract_major: 0 until the encoding freeze, then 1; bumped only for a
--   breaking change to a stable object (a removed or renamed object, column
--   or parameter, a changed column type or position, changed semantics) or
--   a removed or renamed table mirror.
-- revision: bumped on every change to the normalized text of 070-120
--   (comments other than the `@surface:` tags and whitespace do not count).
-- ddl_hash: the xxh3-64 hash (16 hex digits) of that normalized text. A
--   test (crates/nodal-db-core/tests/read_surface.rs) recomputes it
--   and fails, printing the new value, when the DDL changed without a bump.
--
-- The core reads this macro before a write and replaces the views and
-- macros of an older file with its own (the refresh), and reads an older
-- file through temporary copies of its own (the overlay). A missing macro
-- means the file predates versioning. Keep this file's layout: the core
-- parses the three CAST literals below. Applied last of the surface files,
-- so the marker exists only if every object before it was created.

-- The read-surface version of this file: contract major, revision and the
-- hash of the surface DDL, one row.
-- @surface: stable
CREATE MACRO nodal_read_surface() AS TABLE
SELECT CAST(0 AS INTEGER) AS contract_major,
       CAST(1 AS INTEGER) AS revision,
       CAST('fd12361ce439752c' AS VARCHAR) AS ddl_hash;
