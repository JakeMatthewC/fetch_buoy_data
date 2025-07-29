import pandas as pd
import config.config as c
import data.query as q
import processes.utils as u

def fetch_from_year(station_id, date):
    from fetch_data.fetch_save_year_files import download_noaa_year_txt
    download_noaa_year_txt(station_id, date)

    import processes.utils as u
    # look for local annual bulk file for given year
    df_txt = pd.read_csv(f"{c.noaa_year_path}\\{station_id}\\{date}_year_{station_id}.txt", sep=r'\s+', skiprows=[1], na_values=["MM",'999.0','99'])
    df_data_spec = pd.read_csv(f"{c.noaa_year_path}\\{station_id}\\{date}_year_{station_id}.data_spec", sep=r'\s+', skiprows=[1], na_values=["MM",'999.0','99'])
    df_swdir = pd.read_csv(f"{c.noaa_year_path}\\{station_id}\\{date}_year_{station_id}.swdir", sep=r'\s+', skiprows=[1], na_values=["MM",'999.0','99'])
    df_swdir2 = pd.read_csv(f"{c.noaa_year_path}\\{station_id}\\{date}_year_{station_id}.swdir2", sep=r'\s+', skiprows=[1], na_values=["MM",'999.0','99'])
    df_swr1 = pd.read_csv(f"{c.noaa_year_path}\\{station_id}\\{date}_year_{station_id}.swr1", sep=r'\s+', skiprows=[1], na_values=["MM",'999.0','99'])
    df_swr2 = pd.read_csv(f"{c.noaa_year_path}\\{station_id}\\{date}_year_{station_id}.swr2", sep=r'\s+', skiprows=[1], na_values=["MM",'999.0','99'])

    df_list = [df_txt,df_data_spec,df_swdir,df_swr1,df_swdir2,df_swr2]
    for df in df_list:
        df = u.datetime_dfs(df,station_id)

    # drop the .02 bin for consistency with other data
    df_data_spec.drop(".0200",axis='columns',inplace=True)
    df_swdir.drop(".0200",axis='columns',inplace=True)
    df_swdir2.drop(".0200",axis='columns',inplace=True)
    df_swr1.drop(".0200",axis='columns',inplace=True)
    df_swr2.drop(".0200",axis='columns',inplace=True)

    # rename frequency columns with integers for simplicity
    column_list = ['station_id','datetime'] + list(range(1,47))
    df_data_spec = df_data_spec.set_axis(column_list,axis=1)
    df_swdir = df_swdir.set_axis(column_list,axis=1)
    df_swdir2 = df_swdir2.set_axis(column_list,axis=1)
    df_swr1 = df_swr1.set_axis(column_list,axis=1)
    df_swr2 = df_swr2.set_axis(column_list,axis=1)

    # remove unneeded timesteps from df_txt
    df_txt = df_txt[df_txt['datetime'].isin(df_data_spec['datetime'])]
    df_txt = df_txt.reset_index(drop=True)

    # remove unmatching timesteps from spec dataframes (no df_txt match)
    df_data_spec = df_data_spec[df_data_spec['datetime'].isin(df_txt['datetime'])]
    df_swdir = df_swdir[df_swdir['datetime'].isin(df_txt['datetime'])]
    df_swdir2 = df_swdir2[df_swdir2['datetime'].isin(df_txt['datetime'])]
    df_swr1 = df_swr1[df_swr1['datetime'].isin(df_txt['datetime'])]
    df_swr2 = df_swr2[df_swr2['datetime'].isin(df_txt['datetime'])]

    # reset the index
    df_data_spec = df_data_spec.reset_index(drop=True)
    df_swdir = df_swdir.reset_index(drop=True)
    df_swdir2 = df_swdir2.reset_index(drop=True)
    df_swr1 = df_swr1.reset_index(drop=True)
    df_swr2 = df_swr2.reset_index(drop=True)

    row = u.get_noaa_station_row(station_id)

    meta_buoy = {
        'station_id': station_id,
        'name': str(row['name']),
        'project': 'NOAA'
    }

    cur = c.conn.cursor()
    q.insert_buoy(cur, meta_buoy)
    c.conn.commit()

    # save to buoy deployments table
    buoy_id = q.get_buoy_id(str(station_id))
    buoy_id = buoy_id.loc[0,'id']

    timestamps = df_txt['datetime']
    deployment_id = f"NOAA-{station_id}-{date}"
    buoy_deploy = {
        'buoy_id': buoy_id,
        'deployment_id': deployment_id,
        'start_time': timestamps.min(),
        'end_time': timestamps.max(),
        'latitude': row['lat'],
        'longitude': row['lon'],
        'deployment_type': 'NOAA',
        'depth': None
    }

    q.insert_deployment(cur, buoy_deploy)
    c.conn.commit()

    return df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, deployment_id