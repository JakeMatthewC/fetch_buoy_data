import numpy as np
import pandas as pd
import psycopg2
import data.query as q
import config.config as c

def insert_time_steps(cur,df_time_steps):
    for _, row in df_time_steps.iterrows():
        buoy_id = q.get_buoy_id(row['station_id'])
        buoy_id = [int(buoy_id.loc[0,"id"])]
        if buoy_id:
            cur.execute("""
                INSERT INTO dirspec.time_steps (
                    buoy_id, timestamp, WDIR, WSPD, GST, WVHT, DPD, APD, MWD, PRES,
                    ATMP, WTMP, DEWP, VIS, PTDY, TIDE, m0, hm0, m_1, Te, P
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (buoy_id, timestamp) DO NOTHING;
            """, (
                buoy_id[0], row['datetime'],
                safe_val(row.get('WDIR')), safe_val(row.get('WSPD')), safe_val(row.get('GST')), safe_val(row.get('WVHT')),
                safe_val(row.get('DPD')), safe_val(row.get('APD')), safe_val(row.get('MWD')), safe_val(row.get('PRES')),
                safe_val(row.get('ATMP')), safe_val(row.get('WTMP')), safe_val(row.get('DEWP')), safe_val(row.get('VIS')),
                safe_val(row.get('PTDY')), safe_val(row.get('TIDE')), safe_val(row.get('m0')), safe_val(row.get('hm0')),
                safe_val(row.get('m_1')), safe_val(row.get('Te')), safe_val(row.get('P'))
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

def df_txt_calcs(df_txt, df_data_spec):
    # do calculations for specific timestep -> df_txt is timestep output table
    calc = df_data_spec.iloc[:,3:50] * c.bandwidths
    # zeroth moment and Hm0
    df_txt['m0'] = calc.sum(axis=1)
    df_txt['hm0'] = np.sqrt(df_txt['m0'])*4
    calc2 = calc / c.center_freqs
    # 1st moment, energy period, and wave power
    df_txt['m_1'] = calc2.sum(axis=1)
    df_txt['Te'] = df_txt['m_1'] / df_txt['m0']
    df_txt['P'] = (1025 * 9.81**2 * df_txt['hm0']**2 * df_txt['Te']) / (64 * np.pi * 1000)

    # convert the station ids to strings
    df_txt['station_id'] = df_txt['station_id'].astype(str)
    return df_txt

def vec_calcs(station_id, spec_row, swdir_row, swdir2_row, swr1_row, swr2_row, datetime_obj):    
    # prepare for vectorized calculation
    alpha1 = pd.to_numeric(swdir_row, errors='coerce')
    alpha1 = np.where(~np.isnan(alpha1), met_to_math_dir(alpha1), np.nan)
    alpha1 = alpha1.astype(float)
    alpha2 = pd.to_numeric(swdir2_row, errors='coerce')
    alpha2 = np.where(~np.isnan(alpha2), met_to_math_dir(alpha2), np.nan)
    alpha2 = alpha2.astype(float)

    r1 = pd.to_numeric(swr1_row,errors='coerce')
    r1 = np.array(r1)
    r2 = pd.to_numeric(swr2_row, errors='coerce')
    r2 = np.array(r2)

    Ef = pd.to_numeric(spec_row, errors='coerce')
    Ef = np.array(Ef)

    alpha1_grid = alpha1[:,None]
    alpha2_grid = alpha2[:, None]
    E = Ef[:, None]

    D = (1 / (2 * np.pi)) * (
        (1 + 2 * r1[:, None] * np.cos(c.theta_grid - alpha1_grid))
        + (2 * r2[:, None] * np.cos(2 * (c.theta_grid - alpha2_grid))
        ))
    
    # remove negatives from D output
    D = np.maximum(D, 0)
    
    row_sums = np.sum(D, axis=1, keepdims=True) * c.delta_theta_rad
    # prevent division by 0
    row_sums[row_sums == 0] = 1
    # normalize
    D_normalized = D / row_sums
    #check = np.sum(D_normalized, axis=1, keepdims=True) * delta_theta_rad
    #print(check)

    S = D_normalized * E

    # get the timestep id from the timesteps table
    timestep_id = get_time_step_id(c.cur, str(station_id), datetime_obj)

    # organize for exporting to postgres table
    records_param = []
    records_dir = []
    for m, f in enumerate(c.freqs):
        # these values are recorded to every row for this frequency bin
        a_1 = float(alpha1[m]) if not np.isnan(alpha1[m]) else None
        a_2 = float(alpha2[m]) if not np.isnan(alpha2[m]) else None
        r_1 = float(r1[m]) if not np.isnan(r1[m]) else None
        r_2 = float(r2[m]) if not np.isnan(r2[m]) else None
        energy_density = float(E[m, 0])
        records_param.append((int(timestep_id), float(f), a_1, a_2, r_1, r_2, float(energy_density)))

    # write the spectral data to the spec table
    spectra_parameters_insert_query = """
        INSERT INTO dirspec.spectra_parameters (time_step_id, frequency, alpha1, alpha2, r1, r2, energy_density)
        VALUES %s
        ON CONFLICT (time_step_id, frequency) DO NOTHING
    """
    psycopg2.extras.execute_values(c.cur, spectra_parameters_insert_query, records_param, page_size=100)

    for m, f in enumerate(c.freqs):
        for n, theta in enumerate(c.directional_pnts_deg):
            spreading = float(D_normalized[m, n])
            records_dir.append((timestep_id, f, theta, spreading))

    records_dir = [(int(timestep_id), float(f), int(theta), float(spreading)) for (timestep_id, f, theta, spreading) in records_dir]

    direction_insert_query = """
        INSERT INTO dirspec.spectra_directional (
            time_step_id, frequency, direction, spreading
        ) VALUES %s
        ON CONFLICT (time_step_id, frequency, direction) DO NOTHING
    """
    psycopg2.extras.execute_values(c.cur, direction_insert_query, records_dir, page_size=500)

    c.conn.commit()
    # update flag
    c.cur.execute("""
        UPDATE dirspec.time_steps
        SET spectra_ingested = TRUE
        WHERE id = %s
    """, (timestep_id,))
    c.conn.commit()
        
