"""Single source of truth for the Postgres connection.

Defaults reproduce the values that used to be hard-coded in config/config.py,
data/query.py and processes/storm_buoy_match.py, so normal runs are unchanged.
Set the PGDIRSPEC_* environment variables to point the whole pipeline at a
different database -- that is what scripts/smoke_test.py uses to run against a
throwaway database instead of the production one.
"""
import os

DEFAULTS = {
    "dbname": "postgres",
    "user": "Jacob",
    "password": "",
    "host": "localhost",
    "port": "5432",
}


def conn_params():
    """psycopg2.connect(**kwargs) for the configured target database."""
    return {
        "dbname": os.environ.get("PGDIRSPEC_DB", DEFAULTS["dbname"]),
        "user": os.environ.get("PGDIRSPEC_USER", DEFAULTS["user"]),
        "password": os.environ.get("PGDIRSPEC_PASSWORD", DEFAULTS["password"]),
        "host": os.environ.get("PGDIRSPEC_HOST", DEFAULTS["host"]),
        "port": os.environ.get("PGDIRSPEC_PORT", DEFAULTS["port"]),
    }


def conn_str():
    """SQLAlchemy URL for the same target."""
    p = conn_params()
    return (f"postgresql+psycopg2://{p['user']}:{p['password']}"
            f"@{p['host']}:{p['port']}/{p['dbname']}")
