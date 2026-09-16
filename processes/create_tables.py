"""Create the dirspec / storms schema from scripts/schema_live.sql.

The previous version of this module carried a hand-written CREATE TABLE block
that had drifted away from the database and could not run at all: `id,` had no
type, dirspec.buoys was declared with lat/lon/depth columns the live table does
not have (it has `project`), dirspec.buoy_deployments was missing entirely, and
the foreign key referenced dirspec.buoys(buoy_id) instead of (id).

The authoritative definition is now scripts/schema_live.sql, produced by:

    pg_dump --schema-only --no-owner --no-privileges -n dirspec -n storms \\
            -h localhost -U <user> -d <db> -f scripts/schema_live.sql

Regenerate that file whenever the schema changes, and commit it.

Usage:
    python processes/create_tables.py                 # create if absent
    python processes/create_tables.py --drop-first    # DESTRUCTIVE, prompts

Honours the PGDIRSPEC_* environment variables, so it can target a scratch
database instead of production.
"""
import argparse
import io
import os
import sys

import psycopg2

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
import config.db as db  # noqa: E402

SCHEMA_SQL = os.path.join(REPO, "scripts", "schema_live.sql")


def create_tables(conn, drop_first=False):
    """Apply the schema dump to an open connection."""
    if not os.path.exists(SCHEMA_SQL):
        raise FileNotFoundError(
            f"{SCHEMA_SQL} not found -- regenerate it with pg_dump (see module docstring)")
    with io.open(SCHEMA_SQL, encoding="utf-8") as fh:
        sql = fh.read()
    with conn.cursor() as cur:
        if drop_first:
            cur.execute("drop schema if exists dirspec cascade")
            cur.execute("drop schema if exists storms cascade")
        cur.execute(sql)
    conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--drop-first", action="store_true",
                    help="DROP SCHEMA dirspec/storms CASCADE before creating (destroys all data)")
    args = ap.parse_args()

    params = db.conn_params()
    target = f"{params['host']}:{params['port']}/{params['dbname']}"

    if args.drop_first:
        print(f"About to DROP SCHEMA dirspec CASCADE and storms CASCADE on {target}.")
        print("Every buoy, timestep and spectrum in that database will be destroyed.")
        if input(f"Type the database name ({params['dbname']}) to confirm: ").strip() != params["dbname"]:
            sys.exit("aborted")

    conn = psycopg2.connect(**params, connect_timeout=10)
    create_tables(conn, drop_first=args.drop_first)
    conn.close()
    print(f"schema applied to {target}")


if __name__ == "__main__":
    main()
