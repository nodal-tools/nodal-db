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
VALUES (nextval('seq_metadata'), 'schema_version', '0.2-ddl-critique-fixes',
        currval('seq_commit'));
