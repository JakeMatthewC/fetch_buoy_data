import pandas as pd

# modules
import config.config as c
import processes.utils as u
import processes as p
from fetch_data.fetch_from_rt import fetch_from_rt
from fetch_data.fetch_from_year import fetch_from_year
from fetch_data.fetch_from_api import fetch_from_api

# file name parameters
file_station_id = [41010]
file_date = ["2024"]
file_type = ["year"]

for station_id,f_date,f_type in zip(file_station_id,file_date,file_type):
    if f_type == 'rt':
        df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = fetch_from_rt(station_id,f_date)

    elif f_type == 'year':
        df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = fetch_from_year(station_id,f_date)

    elif f_type == 'api':
        df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = fetch_from_api(station_id,f_date)

    # perform calcs for bulk parameters at each timestep
    df_txt = u.df_txt_calcs(df_txt, df_data_spec)


    # perform storm analysis at each timestep


    # move processed timesteps to database
    cur = c.conn.cursor()
    u.insert_time_steps(cur, df_txt, f_type)
    c.conn.commit()

    # filter the timesteps to only unprocessed ones
    # check for spectrum ingested flag across timesteps
    unprocessed_timesteps = u.get_unprocessed_timesteps(cur, str(station_id))
    if not unprocessed_timesteps:
        # move on to next station_id if the list is empty
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
    p.calc_D(df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, station_id, f_type)