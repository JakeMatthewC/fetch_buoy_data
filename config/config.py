"""Pipeline configuration: paths, the NDBC frequency grid, and database handles.

Paths come from config.paths (environment-driven). The frequency grid is read
from WPM_spectra.xlsx once at import.

`conn` and `cur` are LAZY -- created on first attribute access, not at import,
so the pipeline can be imported without a live database. Call sites are unchanged.
"""
import numpy as np
import pandas as pd
import psycopg2

import config.db as db
from config.paths import *          # noqa: F401,F403  (paths stay importable as c.<name>)
from config.paths import wpm_path

# WPM_spectra.xlsx is the NDBC 47-bin layout: index, centre frequency, bandwidth.
# skiprows=1 drops bin 1 at 0.0201 Hz, which the fetchers drop from the raw files
# too, leaving 46 contiguous bins spanning 0.0325-0.4850 Hz.
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
    """Connect on first use of c.conn / c.cur.

    Python calls this only for names not already defined in the module.
    """
    if name == "conn":
        return _connect()[0]
    if name == "cur":
        return _connect()[1]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
