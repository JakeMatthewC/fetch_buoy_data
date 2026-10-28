"""Configuration package.

Re-exports the filesystem paths so `import config as c; c.buoy_data_root` works.
Only config.paths is pulled in here -- it needs nothing but python-dotenv, so
importing this package never opens a database connection or reads a spreadsheet.

  import config as c               -> paths only, cheap
  import config.config as c        -> paths + frequency grid + lazy db handles
  import config.db as db           -> connection parameters only
"""
from config.paths import *          # noqa: F401,F403
from config.paths import (           # noqa: F401  (explicit, for editors)
    buoy_data_root,
    fetch_data_root,
    config_data_root,
    noaa_stations_path,
    stations_path,
    wpm_path,
    raws_root,
    noaa_rt_path,
    noaa_year_path,
    cdip_path,
    era5_path,
    tidal_path,
    meiv2_path,
    url,
)
