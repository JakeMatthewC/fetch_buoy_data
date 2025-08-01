import numpy as np
import os
import pandas as pd
import xarray as xr
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY
from scipy.interpolate import interp1d

# modules
import data.query as q
import config.config as c
from fetch_data.fetch_from_era5 import fetch_from_era5

def fetch_cdip_file(station_id: int, deployment: int, cdip_output_file: str = None) -> xr.Dataset | None:
    """
    Fetch CDIP NetCDF dataset via THREDDS OPeNDAP.

    Args:
        station_id (int): CDIP station ID (e.g. 244).
        deployment (int): Deployment number (e.g. 5 for d05.nc).

    Returns:
        xarray.Dataset or None
    """

    # make sure the path exists
    os.makedirs(os.path.dirname(cdip_output_file), exist_ok=True)

    if os.path.exists(cdip_output_file):
        print(f'Loading CDIP from local drive: {cdip_output_file}')
        return xr.open_dataset(cdip_output_file)
    else:
        url = f"http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/archive/{station_id}p1/{station_id}p1_d{deployment:02d}.nc"
        print(f'Fetching CDIP from THREDDS: {url}')

        try:
            ds = xr.open_dataset(url)
            ds.to_netcdf(cdip_output_file)
            print(f'Saved locally to {cdip_output_file}')
            return ds
        except Exception as e:
            print(f"[CDIP FETCH ERROR] Station {station_id}, deployment {deployment}: {e}")
            return None

def fetch_from_cdip(station_id: int, deployment: int) -> xr.Dataset | None:   
    # set local location for the raw data
    cdip_output_file= rf"{c.cdip_path}\{station_id}\cdip_{station_id}_d{deployment:02d}.nc"

    # attempt to fetch from local and pull from THREDDS if not found
    ds = fetch_cdip_file(station_id, deployment, cdip_output_file)
    
    if ds is not None:
        # --- Buoy Metadata ---
        name = ds['metaStationName'].values.item()  
        name_str = name.decode('utf-8').strip()
        lat = ds['metaDeployLatitude'].values.item()
        lon = ds['metaDeployLongitude'].values.item()
        depth = ds['metaWaterDepth'].values.item()
        project = ds.attrs.get('project')

        # --- Frequency Configuration ---
        freq_cdip = ds['waveFrequency'].values                # shape: (nfreq,)
        
        # --- Timestep Data (1D time-series arrays) ---
        wave_time = ds['waveTime'].values                      # shape: (ntime,)

        # Convert to datetime:  
        timestamp = pd.to_datetime(wave_time, unit='s')

        # fetch met data from era5 in month-long chunks
        era5_start_time = timestamp.min().replace(day=1)
        era5_end_time = timestamp.max().replace(day=1) + relativedelta(months=1)
        date_ranges = list(rrule(freq=MONTHLY, dtstart=era5_start_time, until=era5_end_time))

        df_met_list = []
        for start_month in date_ranges:
            start = datetime(year=start_month.year, month = start_month.month, day = 1)

            era5_output_file = f"D:/Buoy_work/Raws Storage/ERA5 Raws/{station_id}/era5_{station_id}_{start:%Y%m}.nc"
            era5_zip_output_file = f"D:/Buoy_work/Raws Storage/ERA5 Raws/{station_id}/zips/era5_{station_id}_{start:%Y%m}.zip"
            met_df_month = fetch_from_era5(station_id, start, lat, lon, era5_output_file, era5_zip_output_file)
            df_met_list.append(met_df_month)

        # concatenate all the monthly api pulls
        met_df = pd.concat(df_met_list).sort_values('datetime').reset_index(drop=True)

        # CDIP typically does not provide wind, gust, pressure, or tide. getting that from era5 instead
        wvht = ds['waveHs'].values
        dpd = ds['waveTp'].values
        apd = ds['waveTa'].values
        mwd = ds['waveDp'].values
        
        # Not typically included, but placeholder fields:
        wtmp = np.full(len(timestamp), np.nan)
        dewp = np.full(len(timestamp), np.nan)
        vis = np.full(len(timestamp), np.nan)
        ptdy = np.full(len(timestamp), np.nan)
        if 'seaSurfaceElevation' in ds.variables:
            tide = ds['seaSurfaceElevation'].values
        else:
            tide = np.full(len(timestamp), np.nan)

        # --- Directional Spectra Components ---
        bandwidth_cdip = ds['waveBandwidth'].values
        Ef_cdip = ds['waveEnergyDensity'].values               # shape: (ntime, nfreq)
        wave_a1 = ds['waveA1Value'].values                     # shape: (ntime, nfreq)
        wave_a2 = ds['waveA2Value'].values
        wave_b1 = ds['waveB1Value'].values
        wave_b2 = ds['waveB2Value'].values
        
        # use energy-weighted interp for Ef
        E_cdip = Ef_cdip * bandwidth_cdip
        interp_E = interp1d(freq_cdip, E_cdip, axis=1, bounds_error=False, fill_value=0)
        E_noaa_bins = interp_E(c.noaa_freqs)
        Ef_interp = E_noaa_bins / c.noaa_bandwidths

        # non-energy weighted for fourier
        a1_interp = interp1d(freq_cdip, wave_a1, axis=1, bounds_error=False, fill_value=0)
        a2_interp = interp1d(freq_cdip, wave_a2, axis=1, bounds_error=False, fill_value=0)
        b1_interp = interp1d(freq_cdip, wave_b1, axis=1, bounds_error=False, fill_value=0)
        b2_interp = interp1d(freq_cdip, wave_b2, axis=1, bounds_error=False, fill_value=0)

        a1_noaa_interp = a1_interp(c.noaa_freqs)
        a2_noaa_interp = a2_interp(c.noaa_freqs)
        b1_noaa_interp = b1_interp(c.noaa_freqs)
        b2_noaa_interp = b2_interp(c.noaa_freqs)

        # compute r and alpha
        r1 = np.sqrt(a1_noaa_interp**2 + b1_noaa_interp**2)
        r2 = np.sqrt(a2_noaa_interp**2 + b2_noaa_interp**2)

        alpha1_rad = np.arctan2(b1_noaa_interp, a1_noaa_interp)
        alpha2_rad = np.arctan2(b2_noaa_interp, a2_noaa_interp)

        # convert to meteorological & degrees
        alpha1_met = (270 - np.degrees(alpha1_rad)) % 360
        alpha2_met = (270 - np.degrees(alpha2_rad)) % 360

        # not using enery-weighted fourier currently, but commented out in case needed in future
        '''
        # create energy-weighted values for interpolation
        E_cdip = Ef_cdip * bandwidth_cdip
        Ea1 = E_cdip * wave_a1
        Eb1 = E_cdip * wave_b1
        Ea2 = E_cdip * wave_a2
        Eb2 = E_cdip * wave_b2

        # perform interpolation to NOAA bins
        interp_E = interp1d(freq_cdip, E_cdip, axis=1, bounds_error=False, fill_value=0)
        E_noaa = interp_E(c.noaa_freqs)

        interp_Ea1 = interp1d(freq_cdip, Ea1, axis=1, bounds_error=False, fill_value=0)
        Ea1_noaa = interp_Ea1(c.noaa_freqs)

        interp_Ea2 = interp1d(freq_cdip, Ea2, axis=1, bounds_error=False, fill_value=0)
        Ea2_noaa = interp_Ea2(c.noaa_freqs)

        interp_Eb1 = interp1d(freq_cdip, Eb1, axis=1, bounds_error=False, fill_value=0)
        Eb1_noaa = interp_Eb1(c.noaa_freqs)

        interp_Eb2 = interp1d(freq_cdip, Eb2, axis=1, bounds_error=False, fill_value=0)
        Eb2_noaa = interp_Eb2(c.noaa_freqs)

        # normalize the interpolated energy-weighted values
        Ef_interp = E_noaa / c.noaa_bandwidths

        a1_noaa = Ea1_noaa / E_noaa
        a1_noaa[E_noaa == 0] = 0

        b1_noaa = Eb1_noaa / E_noaa
        b1_noaa[E_noaa == 0] = 0

        a2_noaa = Ea2_noaa / E_noaa
        a2_noaa[E_noaa == 0] = 0

        b2_noaa = Eb2_noaa / E_noaa
        b2_noaa[E_noaa == 0] = 0
        
        # calculate alpha and r values
        # r1, r2 = sqrt(a1² + b1²), sqrt(a2² + b2²)
        # α1, α2 = atan2(b1, a1), atan2(b2, a2)
        r1 = np.sqrt(a1_noaa**2 + b1_noaa**2)
        r2 = np.sqrt(a2_noaa**2 + b2_noaa**2)

        alpha1 = np.arctan2(b1_noaa, a1_noaa)
        alpha2 = np.arctan2(b2_noaa, a2_noaa)

        # convert to met degrees for calc_D function
        alpha1 = np.where(~np.isnan(alpha1), u.math_to_met_dir(alpha1), np.nan)
        alpha2 = np.where(~np.isnan(alpha2), u.math_to_met_dir(alpha2), np.nan)'''

        meta_buoy = {
            'station_id': station_id,
            'name': name_str,
            'project': project
        }
        
        df_txt_buoy = pd.DataFrame({
            'station_id': station_id,
            'datetime': timestamp,
            'WVHT': wvht,
            'DPD': dpd,
            'APD': apd,
            'MWD': mwd,
            'WTMP': wtmp,
            'DEWP': dewp,
            'VIS': vis,
            'PTDY': ptdy,
            'TIDE': tide       
        })

        # align met data to cdip buoy data timestamps in the dataframes
        met_df['datetime'] = pd.to_datetime(met_df['datetime'])
        df_txt_buoy['datetime'] = pd.to_datetime(df_txt_buoy['datetime']).dt.tz_localize('UTC')

        df_txt = pd.merge_asof(
            df_txt_buoy.sort_values('datetime'),
            met_df.sort_values('datetime'),
            on='datetime',
            direction='nearest',
            tolerance=pd.Timedelta('15min')
        )

        # build the remaining dataframes to conform to existing workflow
        df_data_spec = pd.DataFrame(Ef_interp, columns=c.noaa_freqs)
        df_data_spec.insert(0, 'datetime', timestamp)
        df_data_spec.insert(0, 'station_id', station_id)
        df_data_spec['datetime'] = pd.to_datetime(df_data_spec['datetime']).dt.tz_localize('UTC')

        df_swdir = pd.DataFrame(alpha1_met, columns=c.noaa_freqs)
        df_swdir.insert(0, 'datetime', timestamp)
        df_swdir.insert(0, 'station_id', station_id)
        df_swdir['datetime'] = pd.to_datetime(df_swdir['datetime']).dt.tz_localize('UTC')

        df_swdir2 = pd.DataFrame(alpha2_met, columns=c.noaa_freqs)
        df_swdir2.insert(0, 'datetime', timestamp)
        df_swdir2.insert(0, 'station_id', station_id)
        df_swdir2['datetime'] = pd.to_datetime(df_swdir2['datetime']).dt.tz_localize('UTC')

        df_swr1 = pd.DataFrame(r1, columns=c.noaa_freqs)
        df_swr1.insert(0, 'datetime', timestamp)
        df_swr1.insert(0, 'station_id', station_id)
        df_swr1['datetime'] = pd.to_datetime(df_swr1['datetime']).dt.tz_localize('UTC')

        df_swr2 = pd.DataFrame(r2, columns=c.noaa_freqs)
        df_swr2.insert(0, 'datetime', timestamp)
        df_swr2.insert(0, 'station_id', station_id)
        df_swr2['datetime'] = pd.to_datetime(df_swr2['datetime']).dt.tz_localize('UTC')

        df_txt = df_txt.where(pd.notnull(df_txt), None)

        # save buoy metadata to buoys table
        cur = c.conn.cursor()
        q.insert_buoy(cur, meta_buoy)
        c.conn.commit()

        # save to buoy deployments table
        buoy_id = q.get_buoy_id(str(station_id))
        buoy_id = buoy_id.loc[0,'id']
        deployment_id = deployment
        buoy_deploy = {
            'buoy_id': buoy_id,
            'deployment_id': deployment_id,
            'start_time': df_txt['datetime'].min(),
            'end_time': df_txt['datetime'].max(),
            'latitude': lat,
            'longitude': lon,
            'deployment_type': 'CDIP',
            'depth': float(depth)
        }

        q.insert_deployment(cur, buoy_deploy)
        c.conn.commit()

        return df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, deployment_id
