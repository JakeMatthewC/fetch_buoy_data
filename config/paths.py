"""Filesystem locations, resolved from the environment.

Deliberately dependency-free apart from python-dotenv: no pandas, no psycopg2,
no database connection. That lets scripts and tests import paths without
standing up Postgres or reading the frequency spreadsheet.

Set these in .env at the repo root (see .env.example):

    BUOY_DATA_ROOT   where the raw downloads live, e.g. D:\\Buoy_work
    FETCH_DATA_ROOT  this repository, e.g. D:\\fetch_buoy_data

Missing values raise at import with a message naming the variable, rather than
silently falling back to someone else's directory layout.
"""
import os

from dotenv import load_dotenv

load_dotenv()


def _required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. Copy .env.example to .env and fill it in, or "
            f"export {name} before running. See config/paths.py.")
    return value.rstrip("\\/")


buoy_data_root = _required("BUOY_DATA_ROOT")
fetch_data_root = _required("FETCH_DATA_ROOT")

# --- reference data shipped alongside the raws -------------------------------
config_data_root = f"{buoy_data_root}/Config_Data"
noaa_stations_path = f"{config_data_root}/activestations.xml"
stations_path = f"{config_data_root}/stations.json"
wpm_path = f"{config_data_root}/WPM_spectra.xlsx"

# --- raw download cache ------------------------------------------------------
raws_root = f"{buoy_data_root}/Raws Storage"
noaa_rt_path = f"{raws_root}/NOAA_Raws/rt"
noaa_year_path = f"{raws_root}/NOAA_Raws/year"
cdip_path = f"{raws_root}/CDIP Raws"
era5_path = f"{raws_root}/ERA5 Raws"
tidal_path = f"{raws_root}/Tidal_Raws"

# --- checked into this repository --------------------------------------------
meiv2_path = f"{fetch_data_root}/resources/meiv2.data"

# --- remote endpoints --------------------------------------------------------
url = r"https://www.ndbc.noaa.gov/data/realtime2/"
