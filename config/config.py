"""Pipeline configuration: paths, the NDBC frequency grid, and database handles.

Paths come from config.paths (environment-driven). The frequency grid is read
from WPM_spectra.xlsx once at import.

The `conn` and `cur` database handles are LAZY: they are created on first
attribute access, not at import. Importing this module used to open a Postgres
connection as a side effect, which meant no module that imported it -- which is
most of the pipeline -- could be unit tested without a live database. Call sites
are unchanged; `c.conn` and `c.cur` still work exactly as before.
"""
import numpy as np
import pandas as pd
import psycopg2

import config.db as db
from config.paths import *          # noqa: F401,F403  (paths stay importable as c.<name>)
from config.paths import wpm_path

# --- frequency grid ----------------------------------------------------------
# WPM_spectra.xlsx is the NDBC 47-bin layout: bin index, centre frequency (Hz),
# bandwidth (Hz). skiprows=1 drops bin 1 at 0.0201 Hz, which the fetchers also
# drop from the raw files, leaving 46 bins spanning 0.0325-0.4850 Hz. The bins
# are contiguous and tile 0.030-0.495 Hz; bandwidths are 0.005, 0.01 or 0.02.
wpm_data = pd.read_excel(wpm_path, header=None, skiprows=1)
noaa_freqs = np.array(pd.Series(wpm_data.iloc[:, 1]))
noaa_bandwidths = np.array(pd.Series(wpm_data.iloc[:, 2]))

# 5-degree directional grid for D(theta). These are METEOROLOGICAL bearings
# (direction FROM, clockwise from true north) -- calc_D builds D directly on this
# grid so stored directions match alpha1/alpha2 and time_steps.mwd.
directional_pnts_deg = np.arange(0, 360, 5)
directional_pnts = np.deg2rad(directional_pnts_deg)
theta_grid = directional_pnts[None, :]
delta_theta_deg = 5
delta_theta_rad = np.deg2rad(delta_theta_deg)

# --- lazy database handles ---------------------------------------------------
_conn = None
_cur = None


def _connect():
    global _conn, _cur
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(**db.conn_params())
        _cur = _conn.cursor()
    return _conn, _cur


def __getattr__(name):
    """Create the connection on first use of c.conn or c.cur.

    Python calls this only for names not already defined in the module, so it
    costs nothing for the paths and grid above.
    """
    if name == "conn":
        return _connect()[0]
    if name == "cur":
        return _connect()[1]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
