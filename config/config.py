import numpy as np
import pandas as pd
import psycopg2

## config and file locations
# db conns
conn = psycopg2.connect(
    dbname="postgres",
    user="Jacob",
    password="",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# NOAA active stations xml
noaa_stations_path = r"D:\\Buoy_work\\Config_Data\\activestations.xml"

# NOAA api
url = r'https://www.ndbc.noaa.gov/data/realtime2/'

# wave frequency bin path
wpm_path = r'D:\\Buoy_work\\Config_Data\\WPM_spectra.xlsx'

# local save folders
noaa_rt_path = r"D:\\Buoy_work\\Raws Storage\\NOAA_Raws\\rt"
noaa_year_path = r"D:\\Buoy_work\\Raws Storage\\NOAA_Raws\\year"
tidal_path = r"D:\\Buoy_work\\Raws Storage\\Tidal_Raws"
stations_path = r'D:\\Buoy_work\\Config_Data\\stations.json'
cdip_path = r"D:\\Buoy_work\\Raws Storage\\CDIP Raws"

# pull in the wpm freqs and bin sizes
wpm_data = pd.read_excel(wpm_path,header=None,skiprows=1)
noaa_freqs = np.array(pd.Series(wpm_data.iloc[:,1]))
noaa_bandwidths = np.array(pd.Series(wpm_data.iloc[:,2]))

# create 360 directional points to iterate over for NOAA buoys
directional_pnts_deg = np.arange(0,360,5)
directional_pnts = np.deg2rad(directional_pnts_deg)
theta_grid = directional_pnts[None, :]
delta_theta_deg = 5
delta_theta_rad = np.deg2rad(delta_theta_deg)