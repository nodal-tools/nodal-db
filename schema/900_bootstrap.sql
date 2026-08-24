-- 900_bootstrap.sql — day-one contents of a fresh database.
-- SDP #2331: a default `Base` alternative is auto-created on DB creation
-- (deletable by advanced users; purge can recreate a named default).
-- SDP Part 1 `alembic_version` row: replaced by a `schema_version` reserved
-- metadata key.

INSERT INTO commit (user, message)
VALUES ('nodal_db', 'bootstrap: schema creation');

INSERT INTO alternative (id, name, description, valid_from_commit)
VALUES (nextval('seq_alternative'), 'Base', 'Default alternative',
        currval('seq_commit'));

-- Must match SCHEMA_VERSION in build.py (asserted after every build — F10).
INSERT INTO metadata (id, name, value, valid_from_commit)
VALUES (nextval('seq_metadata'), 'schema_version', '0.2-ddl-critique-fixes',
        currval('seq_commit'));
