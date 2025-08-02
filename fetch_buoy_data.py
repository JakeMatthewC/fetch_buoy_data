import pandas as pd

# modules
import config.config as c
import processes.utils as u
import processes.calc_D as calc_D
import processes.storm_buoy_match as sbm
from fetch_data.fetch_from_rt import fetch_from_rt
from fetch_data.fetch_from_year import fetch_from_year
from fetch_data.fetch_from_api import fetch_from_api
from fetch_data.fetch_from_cdip import fetch_from_cdip

# file name parameters
file_station_id = [144]

file_type = ["cdip"]
file_date = ["2024"]
cdip_deployments = [15]

# directional distributions to save to database
start_date = pd.to_datetime('2021-01-01').tz_localize("UTC")
end_date = pd.to_datetime('2021-12-31').tz_localize("UTC")
save_is_storm = True

for station_id, f_date, f_type, cdip_deployment in zip(file_station_id, file_date, file_type, cdip_deployments):
    match f_type:
        case 'noaa-rt':
            df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = fetch_from_rt(station_id, f_date)
        case 'noaa-year':
            df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, deployment_id = fetch_from_year(station_id, f_date)
        case 'noaa-api':
            df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = fetch_from_api(station_id, f_date)
        case 'cdip':
            df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, deployment_id = fetch_from_cdip(station_id, cdip_deployment)
        case _:
            raise ValueError(f"Unsupported file_type entry: {f_type}")

    # perform calcs for bulk parameters at each timestep
    df_txt = u.df_txt_calcs(df_txt, df_data_spec)
    print("Bulk wave parameters calculated.")

    # get ENSO index values for the timesteps
    df_txt = u.get_enso_index(df_txt)
    print("ENSO index values assigned to timesteps.")

    # get tidal values for the timesteps
    df_txt = u.get_tidal_data(df_txt, deployment_id)
    print("Tidal values assigned to timesteps.")

    # move processed timesteps to database
    cur = c.conn.cursor()
    u.insert_time_steps(cur, df_txt, f_type)
    c.conn.commit()
    print("Timesteps uploaded to database if no conflict.")

    # perform storm analysis at each timestep
    df_txt, storm_dict = sbm.storm_buoy_match(cur, None, df_txt, 400, deployment_id )
    print("Completed storm-buoy matches.")

    # filter the timesteps to only unprocessed ones
    # check for spectrum ingested flag across timesteps
    unprocessed_timesteps = u.get_unprocessed_timesteps(cur, str(station_id))
    if not unprocessed_timesteps:
        continue

    flat = [row[0] for row in unprocessed_timesteps if row and row[0] is not None]
    dt_index = pd.to_datetime(flat, utc=True)

    # select timesteps from the current data where ingested flag isn't set to true
    df_data_spec = df_data_spec[df_data_spec['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swr1 = df_swr1[df_swr1['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swr2 = df_swr2[df_swr2['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swdir = df_swdir[df_swdir['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swdir2 = df_swdir2[df_swdir2['datetime'].isin(dt_index)].reset_index(drop=True)

    # process for calculating D and determining modality
    calc_D.calc_D(storm_dict, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, station_id, f_type, start_date, end_date, save_is_storm)
    print(f"Completed processing for buoy {station_id}.")