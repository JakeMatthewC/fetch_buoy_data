import cdsapi
import netCDF4
import numpy as np
import os
import pandas as pd
import shutil
import xarray as xr
import zipfile

from contextlib import ExitStack

def fetch_from_era5(station_id, era5_start_time, lat, lon, era5_output_file, era5_zip_output_file):
    # see if the file is already saved local
    if not os.path.exists(era5_zip_output_file):

        # make sure local path exists
        os.makedirs(os.path.dirname(era5_zip_output_file), exist_ok=True)
        
        # list of hours, 24 hour reporting from era5
        year = [f"{era5_start_time.year}"]
        month = [f"{era5_start_time.month:02d}"]
        day = [f"{d:02d}" for d in range(1, 32)]
        hours = [f"{h:02d}:00" for h in range(24)]  # ERA5 gives full hourly data  

        # ERA5 API request
        c = cdsapi.Client()
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': [
                    '10m_u_component_of_wind',
                    '10m_v_component_of_wind',
                    '10m_wind_gust_since_previous_post_processing',
                    '2m_temperature',
                    'mean_sea_level_pressure',
                ],
                'year': year,
                'month': month,
                'day': day,
                'time': hours,
                'area': [lat, lon, lat, lon],  # [N, W, S, E] = point location
            },
            era5_zip_output_file # save to local path
        )

        print(f"Fetched ERA5 data for {era5_start_time:%Y-%m} from api.")
    else:
        print(f"Using locally saved ERA5 data for {era5_start_time:%Y-%m}.")

    # unzip the nc file
    # create a temporary extract folder
    extract_dir = f"{os.path.dirname(era5_output_file)}/extracted"
    with zipfile.ZipFile(era5_zip_output_file, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    # Find all .nc files for the month in extract folder
    nc_files = [
        os.path.join(extract_dir, f)
        for f in os.listdir(extract_dir)
        if f.endswith(".nc")
    ]

    if not nc_files:
        raise FileNotFoundError(f"No .nc files found for ERA5 {station_id} {era5_start_time:%Y%m}")

    # load and close files
    datasets = []
    for f in nc_files:
        with xr.open_dataset(f, engine='netcdf4') as ds_tmp:
            datasets.append(ds_tmp.load())

    # merge the files
    ds = xr.merge(datasets)

    # save the combined nc file to the station folder
    ds.to_netcdf(era5_output_file)
    print(f"Saved merged .nc to local drive: {era5_output_file}")

    # Clean up
    shutil.rmtree(extract_dir)

    if ds is not None:
        # Extract raw data
        met_timestamps = pd.to_datetime(ds['valid_time'].values)
        u10 = ds['u10'].values.squeeze()
        v10 = ds['v10'].values.squeeze()
        gst = ds['fg10'].values.squeeze()
        atmp = ds['t2m'].values.squeeze() - 273.15  # K to °C
        pres = ds['msl'].values.squeeze() / 100     # Pa to hPa

        # Combine into DataFrame
        df = pd.DataFrame({
            'datetime': met_timestamps,
            'u10': u10,
            'v10': v10,
            'GST': gst,
            'ATMP': atmp,
            'PRES': pres,
        })

        # Interpolate to 30-minute timesteps
        df.set_index('datetime', inplace=True)
        df_30min = df.resample('30min').interpolate(method='linear')

        # Calculate wind speed and direction
        u = df_30min['u10']
        v = df_30min['v10']
        wspd = np.sqrt(u**2 + v**2)
        wdir = (180 + np.degrees(np.arctan2(u, v))) % 360  # Compass direction

        df_30min['WSPD'] = wspd
        df_30min['WDIR'] = wdir

        # Optional: drop raw components
        df_30min = df_30min.drop(columns=['u10', 'v10']).reset_index()

        # ensure datetime correct format
        df_30min['datetime'] = pd.to_datetime(df_30min['datetime']).dt.tz_localize('UTC')

        return df_30min