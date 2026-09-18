import json
import numpy as np
import os
import pandas as pd
import requests
from math import radians, cos, sin, sqrt, atan2
from datetime import timedelta

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
            # 'tide' (lower case) is the NOAA Tides & Currents value that
            # get_tidal_data() interpolates onto the buoy timestamps: metres
            # relative to MLLW. The upper-case 'TIDE' that used to be written here
            # is the raw NDBC column, which is in FEET and is a 99.00 fill value in
            # essentially every record -- the result was that time_steps.tide,
            # tide_source and tide_method were 100% NULL in every row ingested so
            # far, and the ENSO index was computed and then dropped on the floor.
            cur.execute("""
                INSERT INTO dirspec.time_steps (
                    buoy_id, timestamp, WDIR, WSPD, GST, WVHT, DPD, APD, MWD, PRES,
                    ATMP, WTMP, DEWP, VIS, PTDY, TIDE, m0, hm0, m_1, Te, P, source, met_source,
                    tide_source, tide_method, enso_mei
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                ON CONFLICT (buoy_id, timestamp) DO NOTHING;
            """, (
                buoy_id[0], row['datetime'],
                safe_val(row.get('WDIR')), safe_val(row.get('WSPD')), safe_val(row.get('GST')), safe_val(row.get('WVHT')),
                safe_val(row.get('DPD')), safe_val(row.get('APD')), safe_val(row.get('MWD')), safe_val(row.get('PRES')),
                safe_val(row.get('ATMP')), safe_val(row.get('WTMP')), safe_val(row.get('DEWP')), safe_val(row.get('VIS')),
                safe_val(row.get('PTDY')), safe_val(row.get('tide')), safe_val(row.get('m0')), safe_val(row.get('hm0')),
                safe_val(row.get('m_1')), safe_val(row.get('Te')), safe_val(row.get('P')), source, met_source,
                safe_val(row.get('tide_source')), safe_val(row.get('tide_method')), safe_val(row.get('MEI'))
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

# NDBC stdmet fill values, per column (ndbc.noaa.gov/measdes.shtml).
# These cannot go in a single read_csv(na_values=[...]) list: pandas normalises
# numerically, so a token of '99.0' also matches a bare '99' -- which would blank
# a legitimate WDIR/MWD of 99 degrees (328 such cells in 42055/2024 alone).
STDMET_SENTINELS = {
    'WDIR': 999, 'WSPD': 99.0, 'GST': 99.0,
    'WVHT': 99.0, 'DPD': 99.0, 'APD': 99.0, 'MWD': 999,
    'PRES': 9999.0, 'ATMP': 999.0, 'WTMP': 999.0, 'DEWP': 999.0,
    'VIS': 99.0, 'PTDY': 99.0, 'TIDE': 99.0,
}

def mask_stdmet_sentinels(df):
    """Blank NDBC stdmet fill values column by column, leaving real data intact."""
    for col, fill in STDMET_SENTINELS.items():
        if col in df.columns:
            v = pd.to_numeric(df[col], errors='coerce')
            df[col] = v.mask(v == fill)
    return df

def met_to_math_dir(angle_deg):
    return np.deg2rad((270 - angle_deg) % 360)

def math_to_met_dir(angle_rad):
    return (270 - np.degrees(angle_rad)) % 360

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))

def get_noaa_station_row(station_id):
    station_list = pd.read_xml(c.noaa_stations_path)
    station_list['id'] = station_list['id'].astype(str)
    row = station_list.loc[station_list['id'] == str(station_id)]
    return row

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

def get_enso_index(df_txt):
    mei_seasons = ["DJ", "JF", "FM", "MA", "AM", "MJ", "JJ", "JA", "AS", "SO", "ON", "ND"]
    mei_season_match = {
        "DJ": 12, "JF": 1, "FM": 2, "MA": 3, "AM": 4, "MJ": 5, 
        "JJ": 6, "JA": 7, "AS": 8, "SO": 9, "ON": 10, "ND": 11
    }
    
    # load in mei data and melt
    mei_df = pd.read_csv(c.meiv2_path, header=None, sep=r'\s+', names=mei_seasons)
    mei_df = mei_df.reset_index()
    mei_df.rename(columns={'index': 'year'},inplace=True)
    mei_long = mei_df.melt(id_vars='year', var_name='month', value_name='MEI')
    mei_long['month'] = mei_long['month'].map(mei_season_match)
    mei_long['datetime'] = pd.to_datetime(dict(year=mei_long['year'], month=mei_long['month'], day=1))
    mei_long = mei_long[['datetime', 'MEI']]

    # get year and month from buoy timesteps
    df_txt['year'] = df_txt['datetime'].dt.year
    df_txt['month'] = df_txt['datetime'].dt.month
    
    # get year and month from mei data
    mei_long['year'] = mei_long['datetime'].dt.year
    mei_long['month'] = mei_long['datetime'].dt.month

    # merge on year and month
    df_txt = df_txt.merge(
        mei_long[['year', 'month', 'MEI']],
        on=['year', 'month'],
        how='left'
    )
    df_txt.drop(columns=['year', 'month'], inplace=True)

    return df_txt

def date_chunks(start_date, end_date, days=30):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    chunks = []
    # `<=`, not `<`: a deployment whose timesteps all fall on one calendar day has
    # start == end and previously produced zero chunks, so no tide data was ever
    # requested and get_tidal_data() blew up on an unassigned tide_df.
    while start <= end:
        chunk_end = min(start + timedelta(days=days), end)
        chunks.append((start.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        start = chunk_end + timedelta(days=1)
    return chunks

def get_tidal_data(df_txt, deployment):
    with open(c.stations_path, 'r') as f:
        stations = json.load(f)['stations']

    # Convert to DataFrame
    df_stations = pd.DataFrame([{
        'id': s['id'],
        'name': s['name'],
        'lat': s['lat'],
        'lon': s['lng'],
        'state': s.get('state', None)
    } for s in stations])

    # get station lat and lon
    buoy_id = q.get_buoy_id(df_txt.loc[0,'station_id'])
    df_lat_lon = q.get_station_lat_lon(int(buoy_id.loc[0,'id']), str(deployment))

    # calculate haversine distances
    df_stations['distance_km'] = df_stations.apply(
        lambda row: haversine(df_lat_lon.loc[0,'lat'], df_lat_lon.loc[0,'lon'], row['lat'], row['lon']),
        axis=1
    )

    # pull the closest tidal station from the list
    closest = df_stations.sort_values('distance_km').iloc[0]
    tide_station_id = closest['id']
    print(f"Closest tide station: {closest['id']} - {closest['name']} ({closest['distance_km']:.1f} km)")

    # get min and max date to pull from api
    timestamps = pd.to_datetime(df_txt['datetime'])
    begin_date = timestamps.min().strftime("%Y%m%d")
    end_date = timestamps.max().strftime("%Y%m%d")

    # check if there's already a cached file for this data
    cache_file = f"{c.tidal_path}/{tide_station_id}/tide_{tide_station_id}_{begin_date}_{end_date}.csv"
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)

    if os.path.exists(cache_file):
        print(f"Loading cached tide data: {cache_file}")
        tide_df = pd.read_csv(cache_file, parse_dates=['t'])

    else:
        # make 1 month date chunks
        chunks = date_chunks(begin_date, end_date, days=30)

        all_data = []
        for begin, end in chunks:
            # make api call for tidal data
            params = {
                "station": f"{tide_station_id}",
                "begin_date": begin,
                "end_date": end,
                "product": "hourly_height",
                "datum": "MLLW",
                "units": "metric",
                "format": "json",
                "time_zone": "gmt"
            }

            response = requests.get("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter", params=params)
            chunk_data = response.json().get("data", [])
            if chunk_data:
                chunk_df = pd.DataFrame(chunk_data)
                all_data.append(chunk_df)

        if not all_data:
            raise RuntimeError(
                f"No tide data returned for station {tide_station_id} between "
                f"{begin_date} and {end_date}; cannot fill time_steps.tide.")

        if all_data:
            tide_df = pd.concat(all_data, ignore_index=True)
            tide_df['t'] = pd.to_datetime(tide_df['t'])
            tide_df['v'] = pd.to_numeric(tide_df['v'], errors='coerce')
            tide_df = tide_df.dropna(subset=['v'])
            tide_df[['t', 'v']].to_csv(cache_file, index=False)

    # NOAA tide data (hourly)
    tide_df['t'] = pd.to_datetime(tide_df['t'])
    tide_df['v'] = pd.to_numeric(tide_df['v'], errors='coerce')
    tide_df = tide_df.sort_values('t') # just in case the data times comes in mixed up, though unlikely
    tide_df = tide_df.dropna()

    # make a series for the tidal times for joining, set tz at UTC
    tide_series = pd.Series(tide_df['v'].values, index=tide_df['t'])
    tide_series.index = tide_series.index.tz_localize('UTC')

    # pull out the buoy times so that they can be merged on
    buoy_times = pd.to_datetime(df_txt['datetime'], utc=True)

    # create a union on the two
    full_index = tide_series.index.union(buoy_times).sort_values()

    interpolated_series = tide_series.reindex(full_index).interpolate(method='time')
    tide_at_buoy_times = interpolated_series.loc[buoy_times]

    df_txt['tide'] = tide_at_buoy_times.values
    df_txt['tide_source'] = tide_station_id
    df_txt['tide_method'] = "auto"

    return df_txt