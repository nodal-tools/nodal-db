#!/usr/bin/env python3
"""Build the nodal_db schema into a DuckDB file.

Usage: python build.py TARGET.duckdb [--force]

Executes the numbered .sql files in this directory in lexical order inside
one transaction per file. Stdlib + duckdb only.

Build hygiene (critique F10): the DuckDB version is gated (everything in
ddl_sketch.md section 1 is verified on 2.0.x — a silent behavior drift in
e.g. generated-column indexes would otherwise go unnoticed; and an older
engine would write the 1.5 storage format, whose WAL logs the whole list
column on every array write), the build runs
against a temp file that is atomically renamed into place (a failure in
file N cannot leave a half-built target), stale WAL files next to a
replaced target are removed, and the bootstrap schema_version is asserted
after the build.
"""
import argparse
import pathlib
import sys

import duckdb

SCHEMA_DIR = pathlib.Path(__file__).resolve().parent

# The engine facts the DDL rests on are verified on this DuckDB line
# (ddl_sketch.md section 1); refuse to build on anything older. This gate is
# load-bearing: DuckDB >= 2.0 creates files in the native 2.0 storage format
# (tag v2.0.0+), which is what keeps array-write commits cheap. A stale venv
# with duckdb 1.x would silently build a 1.5-format file instead.
MINIMUM_DUCKDB = (2, 0)

# Must match the bootstrap row in 900_bootstrap.sql.
SCHEMA_VERSION = "0.3-erasure-marker"


def check_duckdb_version() -> None:
    ver = tuple(int(p) for p in duckdb.__version__.split(".")[:2])
    if ver < MINIMUM_DUCKDB:
        sys.exit(
            f"error: duckdb {duckdb.__version__} < required "
            f"{'.'.join(map(str, MINIMUM_DUCKDB))} (schema verified on 2.0.x)")


def sql_files():
    return sorted(p for p in SCHEMA_DIR.glob("[0-9]*.sql"))


def build(target: str, force: bool = False) -> None:
    check_duckdb_version()
    path = pathlib.Path(target)
    wal = path.with_name(path.name + ".wal")
    if path.exists():
        if not force:
            sys.exit(f"error: {target} exists (use --force to overwrite)")
        path.unlink()
    if wal.exists():  # stale WAL next to a deleted/replaced target (F10)
        wal.unlink()
    # build against a temp file; rename into place only on success (F10)
    tmp = path.with_name(path.name + ".building")
    tmp_wal = tmp.with_name(tmp.name + ".wal")
    for stale in (tmp, tmp_wal):
        if stale.exists():
            stale.unlink()
    con = duckdb.connect(str(tmp))
    try:
        for sql_file in sql_files():
            statements = sql_file.read_text()
            try:
                con.execute("BEGIN")
                con.execute(statements)
                con.execute("COMMIT")
            except Exception:
                print(f"FAILED in {sql_file.name}", file=sys.stderr)
                raise
            print(f"applied {sql_file.name}")
        row = con.execute(
            "SELECT value FROM metadata WHERE lower(name) = 'schema_version'"
            " AND valid_to_commit IS NULL").fetchone()
        if row is None or row[0] != SCHEMA_VERSION:
            raise RuntimeError(
                f"bootstrap schema_version {row and row[0]!r} != expected {SCHEMA_VERSION!r}")
    except Exception:
        con.close()
        for stale in (tmp, tmp_wal):
            if stale.exists():
                stale.unlink()
        raise
    con.close()
    if tmp_wal.exists():
        tmp_wal.unlink()
    tmp.rename(path)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("target", help="path of the DuckDB file to create")
    ap.add_argument("--force", action="store_true", help="overwrite an existing file")
    args = ap.parse_args()
    build(args.target, args.force)
    print(f"built {args.target}")


if __name__ == "__main__":
    main()
