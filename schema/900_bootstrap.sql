-- 900_bootstrap.sql — day-one contents of a fresh database.
-- Decision D3: a fresh DB seeds NO alternative. The default alternative (if
-- any) is chosen explicitly by the creator — the Rust core's
-- `init_schema*_with_default(.., Some(name))` variants and the CLI
-- `init --alternative <NAME>` flag seed one after the schema is applied.
-- Omitted ⇒ zero alternatives; the value write path auto-creates on first use.
-- SDP Part 1 `alembic_version` row: replaced by a `schema_version` reserved
-- metadata key.

INSERT INTO commit (user, message)
VALUES ('nodal_db', 'bootstrap: schema creation');

-- Must match SCHEMA_VERSION in build.py (asserted after every build — F10).
INSERT INTO metadata (id, name, value, valid_from_commit)
VALUES (nextval('seq_metadata'), 'schema_version', '0.3-erasure-marker',
        currval('seq_commit'));

-- DB-identity UUID (specs/schema_interop.md §9): minted ONCE here — the
-- only writer — and stable for the lifetime of the database. Output DBs stamp
-- "produced from input DB <identity> at commit C under scenario S" against it,
-- so it must never be re-minted. It is DATA, not a DDL side effect: the
-- compaction repack deletes the fresh temp's own bootstrap rows and copies the
-- source's metadata rows verbatim, so the identity survives physical rebuilds.
-- Read it with plain SQL:
--   SELECT value FROM metadata
--   WHERE lower(name) = 'db_identity' AND valid_to_commit IS NULL;
INSERT INTO metadata (id, name, value, valid_from_commit)
VALUES (nextval('seq_metadata'), 'db_identity', uuid()::VARCHAR,
        currval('seq_commit'));
