import pandas as pd
import config.config as c
from datetime import datetime

def process_api(station_id, date):
    import processes.utils as u
    ## if no local file is used, read from api 
    # create the buoy filepath to request from
    txt_buoy_file = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt"
    data_spec_buoy_file = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.data_spec"
    swdir_buoy_file = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.swdir"
    swr1_buoy_file = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.swr1"
    swdir2_buoy_file = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.swdir2"
    swr2_buoy_file = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.swr2"

    # load to dataframes
    df_txt = pd.read_csv(txt_buoy_file, sep='\s+', skiprows=[1], na_values=["MM",'999.0'])
    df_data_spec = pd.read_csv(data_spec_buoy_file, sep='\s+', skiprows=[0], na_values=["MM",'999.0'], header=None)
    df_swdir = pd.read_csv(swdir_buoy_file, sep='\s+', skiprows=[0], na_values=["MM",'999.0'], header=None)
    df_swr1 = pd.read_csv(swr1_buoy_file, sep='\s+', skiprows=[0], na_values=["MM",'999.0'], header=None)
    df_swdir2 = pd.read_csv(swdir2_buoy_file, sep='\s+', skiprows=[0], na_values=["MM",'999.0'], header=None)
    df_swr2 = pd.read_csv(swr2_buoy_file, sep='\s+', skiprows=[0], na_values=["MM",'999.0'], header=None)

    # build paths to save out the raw files in case they are needed again
    txt_path = f"{c.noaa_rt_path}\\{datetime.today().strftime('%Y-%m-%d')}_rt_{station_id}.txt"
    data_spec_path = f"{c.noaa_rt_path}\\{datetime.today().strftime('%Y-%m-%d')}_rt_{station_id}.data_spec"
    swdir_path = f"{c.noaa_rt_path}\\{datetime.today().strftime('%Y-%m-%d')}_rt_{station_id}.swdir"
    swdir2_path = f"{c.noaa_rt_patht}\\{datetime.today().strftime('%Y-%m-%d')}_rt_{station_id}.swdir2"
    swr1_path = f"{c.noaa_rt_path}\\{datetime.today().strftime('%Y-%m-%d')}_rt_{station_id}.swr1"
    swr2_path = f"{c.noaa_rt_path}\\{datetime.today().strftime('%Y-%m-%d')}_rt_{station_id}.swr2"

    # save the "rt" files to csvs
    df_txt.to_csv(txt_path)
    df_data_spec.to_csv(data_spec_path)
    df_swdir.to_csv(swdir_path)
    df_swdir2.to_csv(swdir2_path)
    df_swr1.to_csv(swr1_path)
    df_swr2.to_csv(swr2_path)

    df_list = [df_txt,df_data_spec,df_swdir,df_swr1,df_swdir2,df_swr2]
    for df in df_list:
            df = u.datetime_dfs(df,station_id)

    # remove frequency identifiers
    df_data_spec.drop(range(7,98,2),axis='columns',inplace=True)        
    df_swdir.drop(range(6,97,2),axis='columns',inplace=True)
    df_swr1.drop(range(6,97,2),axis='columns',inplace=True)
    df_swdir2.drop(range(6,97,2),axis='columns',inplace=True)
    df_swr2.drop(range(6,97,2),axis='columns',inplace=True)

    # rename frequency columns with integers for simplicity
    column_list = ['station_id','datetime','sep_freq'] + list(range(1,47))
    df_data_spec = df_data_spec.set_axis(column_list,axis=1)
    # repeat for the rest without sep_freq column
    column_list = ['station_id','datetime'] + list(range(1,47))
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

    return df_txt, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2