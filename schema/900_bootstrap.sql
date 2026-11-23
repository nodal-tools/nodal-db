-- 900_bootstrap.sql — day-one contents of a fresh database.
-- Decision D3: a fresh DB seeds NO alternative. The default alternative (if
-- any) is chosen explicitly by the creator — the Rust core's
-- `init_schema*_with_default(.., Some(name))` variants and the CLI
-- `init --alternative <NAME>` flag seed one after the schema is applied.
-- Omitted ⇒ zero alternatives; the value write path auto-creates on first use.
-- SDP Part 1 `alembic_version` row: replaced by the `schema_version` core
-- database property. A fresh file seeds no metadata roles, pairs, sources or
-- licenses.

INSERT INTO commit (user, message)
VALUES ('nodal_db', 'bootstrap: schema creation');

-- Must match SCHEMA_VERSION in build.py (asserted after every build — F10).
INSERT INTO database_property (key, value, owner, valid_from_commit)
VALUES ('schema_version', '7', 'core', currval('seq_commit'));

-- DB-identity UUID (specs/schema_interop.md §9): minted ONCE here — the
-- only writer — and stable for the lifetime of the database. Output DBs stamp
-- "produced from input DB <identity> at commit C under scenario S" against it,
-- so it must never be re-minted. It is DATA, not a DDL side effect: the
-- compaction repack deletes the fresh temp's own bootstrap rows and copies the
-- source's database_property rows verbatim, so the identity survives physical
-- rebuilds. A history-free extract is a NEW database: it keeps the identity
-- its own fresh bootstrap minted and never copies the source's (whose
-- identity its horizon marker records as continuation_db).
-- Read it with plain SQL:
--   SELECT value FROM database_property
--   WHERE lower(key) = 'db_identity' AND valid_to_commit IS NULL;
INSERT INTO database_property (key, value, owner, valid_from_commit)
VALUES ('db_identity', uuid()::VARCHAR, 'core', currval('seq_commit'));
