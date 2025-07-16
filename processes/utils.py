import numpy as np
import pandas as pd

# modules
import data.query as q
import config.config as c

def insert_time_steps(cur, df_time_steps, f_type):
    for _, row in df_time_steps.iterrows():
        buoy_id = q.get_buoy_id(row['station_id'])
        buoy_id = [int(buoy_id.loc[0,"id"])]
        if f_type == 'noaa-year':
            source = 'noaa-year'
            met_source = 'buoy'
        elif f_type == 'noaa-api' or f_type == 'noaa-rt':
            source = 'noaa-rt'
            met_source = 'buoy'
        elif f_type == 'cdip':
            source = 'cdip'
            met_source = 'ERA5'

        if buoy_id:
            cur.execute("""
                INSERT INTO dirspec.time_steps (
                    buoy_id, timestamp, WDIR, WSPD, GST, WVHT, DPD, APD, MWD, PRES,
                    ATMP, WTMP, DEWP, VIS, PTDY, TIDE, m0, hm0, m_1, Te, P, source, met_source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (buoy_id, timestamp) DO NOTHING;
            """, (
                buoy_id[0], row['datetime'],
                safe_val(row.get('WDIR')), safe_val(row.get('WSPD')), safe_val(row.get('GST')), safe_val(row.get('WVHT')),
                safe_val(row.get('DPD')), safe_val(row.get('APD')), safe_val(row.get('MWD')), safe_val(row.get('PRES')),
                safe_val(row.get('ATMP')), safe_val(row.get('WTMP')), safe_val(row.get('DEWP')), safe_val(row.get('VIS')),
                safe_val(row.get('PTDY')), safe_val(row.get('TIDE')), safe_val(row.get('m0')), safe_val(row.get('hm0')),
                safe_val(row.get('m_1')), safe_val(row.get('Te')), safe_val(row.get('P')), source, met_source
            ))

def get_unprocessed_timesteps(cur,station_id):
    # Step 1: get buoy_id from station_id
    buoy = q.get_buoy_id(station_id)
    buoy = [int(buoy.loc[0,"id"])]
    if not buoy:
        return []
    buoy_id = buoy[0]

    # Step 2: get time steps where spectra_ingested is false
    cur.execute("""
        SELECT timestamp
        FROM dirspec.time_steps
        WHERE buoy_id = %s AND (spectra_ingested = FALSE OR spectra_ingested IS NULL)
        ORDER BY timestamp
    """, (buoy_id,))

    return cur.fetchall() 

def get_time_step_id(cur, station_id, dt_utc):
    cur.execute("""
        SELECT ts.id
        FROM dirspec.time_steps ts
        JOIN dirspec.buoys b ON ts.buoy_id = b.id
        WHERE b.station_id = %s AND ts.timestamp = %s
    """, (station_id, dt_utc))
    
    result = cur.fetchone()
    return result[0] if result else None

def datetime_dfs(x,buoy_id):
    new_columns = ['year','month','day','hour','minute']
    x.rename(columns=dict(zip(x.columns[0:5], new_columns)),inplace=True)
    x.insert(0,'datetime',pd.to_datetime(x[['year', 'month', 'day', 'hour', 'minute']],utc=True))
    x.insert(0,'station_id',buoy_id)
    x.drop(['year', 'month', 'day', 'hour', 'minute'], axis='columns',inplace=True)
    return x

def safe_val(val):
    return None if pd.isna(val) else val

def met_to_math_dir(angle_deg):
    return np.deg2rad((270 - angle_deg) % 360)

def math_to_met_dir(angle_rad):
    return (270 - np.degrees(angle_rad)) % 360

def df_txt_calcs(df_txt, df_data_spec):
    bandwidths = c.noaa_bandwidths

    # do calculations for specific timestep -> df_txt is timestep output table
    calc = df_data_spec.iloc[:, -46:].values * bandwidths
    
    # zeroth moment and Hm0
    df_txt['m0'] = calc.sum(axis=1)
    df_txt['hm0'] = np.sqrt(df_txt['m0'])*4
    calc2 = calc / c.noaa_freqs
    
    # 1st moment, energy period, and wave power
    df_txt['m_1'] = calc2.sum(axis=1)
    df_txt['Te'] = df_txt['m_1'] / df_txt['m0']
    df_txt['P'] = (1025 * 9.81**2 * df_txt['hm0']**2 * df_txt['Te']) / (64 * np.pi * 1000)

    # convert the station ids to strings
    df_txt['station_id'] = df_txt['station_id'].astype(str)
    return df_txt

