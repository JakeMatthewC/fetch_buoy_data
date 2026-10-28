"""End-to-end pipeline smoke test against a throwaway database.

Builds a scratch database from the live schema, copies in the storm tables the
matcher needs, runs the REAL ingest (fetch -> bulk params -> ENSO -> tide ->
storm match -> calc_D) for a small slice of one station, then runs
scripts/validate_spectra.py against the result.

    python scripts/smoke_test.py                      # NOAA station 42055, 2024, 120 timesteps
    python scripts/smoke_test.py --station 41010 --limit 300
    python scripts/smoke_test.py --keep                # leave the scratch DB for inspection

Nothing touches the production database except two read-only COPY statements
that export the storms tables. The scratch database is dropped and recreated on
every run; the script refuses to target the production database by name.

Requires network access for the NOAA tide API (the rest is served from the local
raw-file cache).
"""
import argparse
import io
import os
import subprocess
import sys
import time

import psycopg2

REPO = __file__.rsplit("scripts", 1)[0].rstrip("\\/")
sys.path.insert(0, REPO)
import config.db as db  # noqa: E402

SCRATCH_DB = "dirspec_smoke"
SCHEMA_SQL = os.path.join(REPO, "scripts", "schema_live.sql")


def log(msg):
    print(f"[smoke] {msg}", flush=True)


def build_scratch_db(source_params, year):
    if SCRATCH_DB == source_params["dbname"]:
        sys.exit(f"refusing to use the source database '{SCRATCH_DB}' as scratch")
    if not os.path.exists(SCHEMA_SQL):
        sys.exit(f"missing {SCHEMA_SQL}\n"
                 "  regenerate with:\n"
                 '  pg_dump --schema-only --no-owner --no-privileges -n dirspec -n storms '
                 '-h localhost -U <user> -d <db> -f scripts/schema_live.sql')

    admin = psycopg2.connect(**source_params, connect_timeout=10)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("select pg_terminate_backend(pid) from pg_stat_activity "
                    "where datname = %s and pid <> pg_backend_pid()", (SCRATCH_DB,))
        cur.execute(f'drop database if exists "{SCRATCH_DB}"')
        cur.execute(f'create database "{SCRATCH_DB}"')
    log(f"created scratch database '{SCRATCH_DB}'")

    scratch_params = dict(source_params, dbname=SCRATCH_DB)
    scratch = psycopg2.connect(**scratch_params, connect_timeout=10)
    scratch.autocommit = True
    with scratch.cursor() as cur, io.open(SCHEMA_SQL, encoding="utf-8") as fh:
        cur.execute(fh.read())
    log("applied schema_live.sql")

    # storm_buoy_match needs the storms tables populated; copy just the year in play
    copied = {}
    with admin.cursor() as src, scratch.cursor() as dst:
        for table, where in (("storms.storms", ""),
                             ("storms.storm_tracks",
                              f"where extract(year from timestamp) = {int(year)}")):
            buf = io.StringIO()
            src.copy_expert(f"copy (select * from {table} {where}) to stdout", buf)
            buf.seek(0)
            dst.copy_expert(f"copy {table} from stdin", buf)
            dst.execute(f"select count(*) from {table}")
            copied[table] = dst.fetchone()[0]
    log(f"copied storm data: {copied}")

    # keep the sequences ahead of the copied rows
    with scratch.cursor() as cur:
        for tbl, seq in (("storms.storms", "storms.storms_id_seq"),
                         ("storms.storm_tracks", "storms.storm_tracks_id_seq")):
            cur.execute(f"select setval(%s, coalesce((select max(id) from {tbl}), 1))", (seq,))
    admin.close()
    scratch.close()
    return scratch_params


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default="42055")
    ap.add_argument("--year", default="2024")
    ap.add_argument("--type", default="noaa-year", choices=["noaa-year", "cdip"],
                    help="which fetcher to exercise")
    ap.add_argument("--deployment", type=int, default=None,
                    help="CDIP deployment number (required for --type cdip)")
    ap.add_argument("--limit", type=int, default=120, help="timesteps to ingest")
    ap.add_argument("--keep", action="store_true", help="do not drop the scratch DB at the end")
    args = ap.parse_args()

    source_params = db.conn_params()
    log(f"source database: {source_params['host']}/{source_params['dbname']}")
    scratch_params = build_scratch_db(source_params, args.year)

    # point the whole pipeline at the scratch DB *before* importing anything that
    # opens a connection at import time (config.config, data.query, storm_buoy_match)
    os.environ["PGDIRSPEC_DB"] = SCRATCH_DB
    for key, env in (("user", "PGDIRSPEC_USER"), ("password", "PGDIRSPEC_PASSWORD"),
                     ("host", "PGDIRSPEC_HOST"), ("port", "PGDIRSPEC_PORT")):
        os.environ[env] = str(source_params[key])

    import fetch_buoy_data as pipeline

    if args.type == "cdip" and args.deployment is None:
        sys.exit("--type cdip requires --deployment")
    station = int(args.station) if args.type == "cdip" else args.station

    log(f"ingesting {args.type} station {station} ({args.year}), limit={args.limit} ...")
    t0 = time.time()
    pipeline.run(file_station_id=[station],
                 file_type=[args.type],
                 file_date=[args.year],
                 cdip_deployments=[args.deployment],
                 limit_timesteps=args.limit)
    log(f"ingest finished in {time.time() - t0:.1f}s")

    with psycopg2.connect(**scratch_params, connect_timeout=10) as conn, conn.cursor() as cur:
        for t in ("dirspec.buoys", "dirspec.buoy_deployments", "dirspec.time_steps",
                  "dirspec.spectra_parameters", "storms.storm_matches"):
            cur.execute(f"select count(*) from {t}")
            log(f"  {t:32s} {cur.fetchone()[0]:>10,}")

    log("running validate_spectra.py against the scratch database ...")
    rc = subprocess.call([sys.executable, os.path.join(REPO, "scripts", "validate_spectra.py"),
                          "--sample", str(args.limit)], env=os.environ, cwd=REPO)

    if args.keep:
        log(f"scratch database '{SCRATCH_DB}' left in place "
            f"(PGDIRSPEC_DB={SCRATCH_DB} to query it)")
    else:
        admin = psycopg2.connect(**source_params, connect_timeout=10)
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute("select pg_terminate_backend(pid) from pg_stat_activity "
                        "where datname = %s and pid <> pg_backend_pid()", (SCRATCH_DB,))
            cur.execute(f'drop database if exists "{SCRATCH_DB}"')
        admin.close()
        log(f"dropped scratch database '{SCRATCH_DB}'")

    log("SMOKE TEST PASSED" if rc == 0 else "SMOKE TEST FAILED (validator reported failures)")
    return rc


if __name__ == "__main__":
    sys.exit(main())
