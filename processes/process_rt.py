import pandas as pd
import config.config as c
import processes.utils as u

def process_rt(station_id, date):
    # look for local "realtime" files
    df_txt = pd.read_csv(f"{c.noaa_rt_path}\\{date}_rt_{station_id}.txt",index_col=0)
    df_data_spec = pd.read_csv(f"{c.noaa_rt_path}\\{date}_rt_{station_id}.data_spec",index_col=0)
    df_swdir = pd.read_csv(f"{c.noaa_rt_path}\\{date}_rt_{station_id}.swdir",index_col=0)
    df_swdir2 = pd.read_csv(f"{c.noaa_rt_path}\\{date}_rt_{station_id}.swdir2",index_col=0)
    df_swr1 = pd.read_csv(f"{c.noaa_rt_path}\\{date}_rt_{station_id}.swr1",index_col=0)
    df_swr2 = pd.read_csv(f"{c.noaa_rt_path}\\{date}_rt_{station_id}.swr2",index_col=0)

    df_data_spec.columns = df_data_spec.columns[:2].tolist() + df_data_spec.columns[2:].astype(int).tolist()
    df_swdir.columns = df_swdir.columns[:2].tolist() + df_swdir.columns[2:].astype(int).tolist()
    df_swdir2.columns = df_swdir2.columns[:2].tolist() + df_swdir2.columns[2:].astype(int).tolist()
    df_swr1.columns = df_swr1.columns[:2].tolist() + df_swr1.columns[2:].astype(int).tolist()
    df_swr2.columns = df_swr2.columns[:2].tolist() + df_swr2.columns[2:].astype(int).tolist()

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