import config as c
import utils as u
import pandas as pd
import sys
from process_rt import process_rt
from process_year import process_year
from process_api import process_api

# file name parameters
file_station_id = [42055]
file_date = ["2024"]
file_type = ["year"]

for station_id,f_date,f_type in zip(file_station_id,file_date,file_type):
    if f_type == 'rt':
        df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = process_rt(station_id,f_date)

    elif f_type == 'year':
        df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = process_year(station_id,f_date)

    elif f_type == 'api':
        df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2 = process_api(station_id,f_date)

    # perform calcs for bulk parameters at each timestep
    df_txt = u.df_txt_calcs(df_txt, df_data_spec)

    # move processed timesteps to database
    cur = c.conn.cursor()
    u.insert_time_steps(cur, df_txt)
    c.conn.commit()

    # filter the timesteps to only unprocessed ones
    # check for spectrum ingested flag across timesteps
    unprocessed_timesteps = u.get_unprocessed_timesteps(cur, str(station_id))
    if not unprocessed_timesteps:
        # move on to next station_id if the list is empty
        continue

    flat = [row[0] for row in unprocessed_timesteps if row and row[0] is not None]
    dt_index = pd.to_datetime(flat, utc=True)

    # select timesteps from the current data where flag isn't set to true
    df_data_spec = df_data_spec[df_data_spec['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swr1 = df_swr1[df_swr1['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swr2 = df_swr2[df_swr2['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swdir = df_swdir[df_swdir['datetime'].isin(dt_index)].reset_index(drop=True)
    df_swdir2 = df_swdir2[df_swdir2['datetime'].isin(dt_index)].reset_index(drop=True)

    for i,spec_row in df_data_spec.iterrows():
        # get the timestep rows for all tables needed
        swdir_row = df_swdir.iloc[i,:]
        swdir2_row = df_swdir2.iloc[i,:]
        swr1_row = df_swr1.iloc[i,:]
        swr2_row = df_swr2.iloc[i,:]

        # check that all files have the timestep
        if spec_row['datetime'] == swdir_row['datetime'] and spec_row['datetime'] == swdir2_row['datetime'] and spec_row['datetime'] == swr1_row['datetime'] and spec_row['datetime'] == swr2_row['datetime']:
            pass
        else:
            print('Datetime mismatch')
            sys.exit()
            break

        # save the datetime object for reference later
        datetime_obj = spec_row['datetime']
        
        if f_type == 'rt' or f_type == 'api':
            # drop the unneeded rows for calculations
            spec_row = spec_row.iloc[3:]
        else:
            spec_row = spec_row.iloc[2:]
        swdir_row = swdir_row.iloc[2:]
        swdir2_row = swdir2_row.iloc[2:]
        swr1_row = swr1_row.iloc[2:]
        swr2_row = swr2_row.iloc[2:]

        # perform vector calcs and upload
        u.vec_calcs(station_id, spec_row, swdir_row, swdir2_row, swr1_row, swr2_row, datetime_obj)