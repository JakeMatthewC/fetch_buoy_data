from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values, Json
import pandas as pd
import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="Jacob",
    password="",
    host="localhost",
    port="5432"
)

CONN_STR = "postgresql+psycopg2://Jacob:@localhost:5432/postgres"
engine = create_engine(CONN_STR)
conn_eng = engine.connect() 

def get_buoy_id(station_id):
    # Get buoy ID (assumes station_id already inserted in buoys)
    buoy_id = pd.read_sql(text("""SELECT id FROM dirspec.buoys WHERE station_id = :station_id
        """), conn_eng, params={"station_id": station_id})
    return buoy_id

def insert_ts_row(cur, buoy_id, row):
    from processes.utils import safe_val
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

def get_spec_ing_false(buoy_id):
    df = pd.read_sql(text("""
        SELECT timestamp
        FROM dirspec.time_steps
        WHERE buoy_id = :buoy_id AND (spectra_ingested = FALSE OR spectra_ingested IS NULL)
        ORDER BY timestamp
    """), conn_eng, params={"buoy_id": buoy_id})
    return df

def find_buoys_with_timestamp(storm_timestamp):   
    df = pd.read_sql(text("""
        SELECT ts.id, ts.timestamp, b.station_id, b.lat, b.lon  
        FROM dirspec.time_steps ts
        JOIN dirspec.buoys b on ts.buoy_id = b.id
        WHERE ts.timestamp BETWEEN (:storm_timestamp - INTERVAL '15 minutes')
                               AND (:storm_timestamp + INTERVAL '15 minutes')    
    """), conn, params={"storm_timestamp": storm_timestamp})
    return df

def find_stormtracks_with_timestamp(buoy_timestamp):
    df = pd.read_sql(text("""
        SELECT st.id, st.storm_id, st.timestamp, st.lat, st.lon, st.wind_speed, st.pressure, st.heading, st.speed, st.storm_type, s.storm_name
        FROM storms.storm_tracks st
        JOIN storms.storms s on st.storm_id = s.storm_id
        WHERE :buoy_timestamp BETWEEN (st.timestamp - INTERVAL '15 minutes')
                                  AND (st.timestamp + INTERVAL '15 minutes')    
    """), conn, params={"buoy_timestamp": buoy_timestamp})   
    return df

def get_station_lat_lon(buoy_id):   
    df = pd.read_sql(text("""
        SELECT b.lat, b.lon
        FROM dirspec.buoys b
        WHERE b.id = :buoy_id      
    """), conn, params={"buoy": buoy_id})  
    return df    

def get_storm_name(hurdat_storm_id):
    df = pd.read_sql(text("""
    SELECT s.storm_name
    FROM storms.storms s
    WHERE s.hurdat_storm_id = :hurdat_storm_id                      
    """), conn, params={"hurdat_storm_id": hurdat_storm_id})
    return df['storm_name']       

def update_storm_match(cur, time_step_id, hurdat_storm_id, dist, storm_timestamp):
    execute_values(cur, """
        INSERT INTO storms.storm_matches
        (time_step_id, hurdat_storm_id, storm_distance_km, storm_track_time)
        VALUES %s
        ON CONFLICT (time_step_id, storm_id) DO NOTHING;
    """, [(time_step_id, hurdat_storm_id, dist, storm_timestamp)
    ])