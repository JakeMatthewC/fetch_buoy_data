from math import radians, sin, cos, sqrt, atan2
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extras import execute_values

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

def storm_buoy_match(cur, storm_df, buoy_df, max_distance_km, deployment_id):
    # if updating storms tables or adding a buoy
    # for each stormtrack timestep -> filter to buoy timesteps within 15 min of the timestep
    # check haversine distance and windspeed at buoy at the timesteps
    # mark is_storm and update matches table if needed

    # storm_df: a df containing all storm timesteps
    # buoy_df if updating storms table: a df containing all buoy timesteps within 15 min of a storm timestep
    # buoy_df if adding a buoy: a df containing all timesteps for the buoy witin 15 min of a storm timestep

    # updating storms condition, storm_df is all new storm data
    if storm_df is not None:
        # check each storm timestep for nearby buoys that meet conditions
        for _, storm_row in storm_df.iterrows():
            storm_timestamp = storm_row['timestamp']
            storm_lat = storm_row['lat']
            storm_lon = storm_row['lon']
            hurdat_storm_id = storm_row['hurdat_storm_id']
            
            # query all buoys with this timestamp
            buoys_with_ts = find_buoys_with_timestamp(storm_timestamp)

            # check for conditions to set is_storm=True
            for idx, buoy_row in buoys_with_ts.iterrows():
                time_step_id = buoy_row['id']
                dist = haversine_km(storm_lat, storm_lon, buoy_row['lat'], buoy_row['lon'])

                if dist <= max_distance_km:
                    # update database tables
                    update_storm_match(cur, time_step_id, hurdat_storm_id, dist, storm_timestamp)
                    buoys_with_ts, storm_name = update_buoy_time_steps(buoys_with_ts, storm_row, dist, idx)
                    row_to_update = buoys_with_ts.iloc[idx]
                    update_time_steps_table(cur, row_to_update, storm_name, time_step_id)
        return None

    # adding new buoy condition
    elif buoy_df is not None:
        storm_dict = {}

        # find the start and end timestamps for the buoy data being pulled in
        buoy_timestamps = buoy_df['datetime']
        start_datetime = min(buoy_timestamps)
        end_datetime = max(buoy_timestamps)
        station_id = buoy_df.loc[0,'station_id']

        # get stormtrack timesteps within this timeframe
        storms_within_timestamps = find_storms_within_timestamps(start_datetime, end_datetime)

        for _, storm_row in storms_within_timestamps.iterrows():
            storm_timestamp = storm_row['timestamp']
            
            # query the buoy timestep closest to the stormtrack timestep
            buoy_id = get_buoy_id(station_id)
            buoy_ts = find_buoy_timestamp(storm_timestamp, buoy_id, deployment_id)
            if buoy_ts.empty == False:
                timestamp = buoy_ts['timestamp'].iloc[0]
                buoy_df_timestep = buoy_df[buoy_df['datetime'] == timestamp]

                # check for conditions to set is_storm=True
                for _, buoy_row in buoy_df_timestep.iterrows():
                    storm_lat = storm_row['lat']
                    storm_lon = storm_row['lon']
                    dist = haversine_km(storm_lat, storm_lon, buoy_ts['lat'].iloc[0], buoy_ts['lon'].iloc[0])

                    if dist <= max_distance_km:
                        hurdat_storm_id = storm_row['hurdat_storm_id']
                        time_step_id = int(buoy_ts['id'].iloc[0])

                        # update database tables
                        update_storm_match(cur, time_step_id, hurdat_storm_id, dist, storm_timestamp)
                        buoy_df_timestep, storm_name = update_buoy_time_steps(buoy_row, storm_row, dist)
                        update_time_steps_table(cur, buoy_row, storm_name, time_step_id)

                        storm_dict[time_step_id] = True

        return buoy_df, storm_dict
    
def get_buoy_id(station_id):
    # Get buoy ID (assumes station_id already inserted in buoys)
    buoy_id = pd.read_sql(text("""SELECT id FROM dirspec.buoys WHERE station_id = :station_id
        """), conn_eng, params={"station_id": station_id})
    return buoy_id.loc[0, 'id']

def find_buoys_with_timestamp(storm_timestamp):   
    df = pd.read_sql(text("""
        SELECT ts.id, ts.timestamp, b.lat, b.lon  
        FROM dirspec.time_steps ts
        JOIN dirspec.buoys b on ts.buoy_id = b.id
        WHERE ts.timestamp BETWEEN (:storm_timestamp - INTERVAL '15 minutes')
                               AND (:storm_timestamp + INTERVAL '15 minutes')    
    """), conn_eng, params={"storm_timestamp": storm_timestamp})
    return df

def find_buoy_timestamp(storm_timestamp, buoy_id, deployment_id):
    df = pd.read_sql(text("""
        SELECT ts.id, ts.timestamp, b.lat, b.lon  
        FROM dirspec.time_steps ts
        JOIN dirspec.buoy_deployments b on ts.buoy_id = b.buoy_id
        WHERE ts.timestamp BETWEEN (:storm_timestamp - INTERVAL '15 minutes')
                               AND (:storm_timestamp + INTERVAL '15 minutes')  
        AND b.buoy_id = :buoy_id  
        AND b.deployment_id = :deployment_id
    """), conn_eng, params={"storm_timestamp": storm_timestamp, "buoy_id": int(buoy_id), "deployment_id": str(deployment_id)})
    return df

def find_stormtracks_with_timestamp(buoy_timestamp):
    df = pd.read_sql(text("""
        SELECT st.id, st.hurdat_storm_id, st.timestamp, st.lat, st.lon, st.wind_speed, st.pressure, st.heading, st.speed, st.storm_type, s.storm_name
        FROM storms.storm_tracks st
        JOIN storms.storms s on st.hurdat_storm_id = s.hurdat_storm_id
        WHERE :buoy_timestamp BETWEEN (st.timestamp - INTERVAL '15 minutes')
                                  AND (st.timestamp + INTERVAL '15 minutes')    
    """), conn_eng, params={"buoy_timestamp": buoy_timestamp})   
    return df

def get_storm_name(hurdat_storm_id):
    df = pd.read_sql(text("""
    SELECT s.storm_name
    FROM storms.storms s
    WHERE s.hurdat_storm_id = :hurdat_storm_id                      
    """), conn_eng, params={"hurdat_storm_id": hurdat_storm_id})
    return df['storm_name']    

def find_storms_within_timestamps(start_datetime,end_datetime):
    df = pd.read_sql(text("""
        SELECT st.*, s.storm_name
        FROM storms.storm_tracks st
        JOIN storms.storms s ON st.hurdat_storm_id = s.hurdat_storm_id
        WHERE st.timestamp BETWEEN (:start_datetime - INTERVAL '15 minutes')
                              AND (:end_datetime + INTERVAL '15 minutes')
    """), conn_eng, params={"start_datetime": start_datetime, "end_datetime": end_datetime})
    return df

def update_storm_match(cur, time_step_id, hurdat_storm_id, dist, storm_timestamp):
    execute_values(cur, """
        INSERT INTO storms.storm_matches
        (time_step_id, hurdat_storm_id, storm_distance_km, storm_track_time)
        VALUES %s
        ON CONFLICT (time_step_id, hurdat_storm_id) DO NOTHING;
    """, [(time_step_id, hurdat_storm_id, dist, storm_timestamp)
    ])

def update_buoy_time_steps(buoy_df, storm_row, dist):
    '''for col in ['storm_name', 'storm_type', 'storm_heading_deg', 'storm_speed_kts',
            'storm_distance_km', 'storm_section_9', 'hurdat_storm_id', 'is_storm']:
        if col not in buoy_df.columns:
            buoy_df[col] = None'''

    storm_name = get_storm_name(storm_row['hurdat_storm_id']).iloc[0]
    buoy_df['storm_name'] = storm_name
    buoy_df['storm_type'] = storm_row['storm_type']
    buoy_df['storm_heading_deg'] = storm_row['heading']
    buoy_df['storm_speed_kts'] = storm_row['speed']
    buoy_df['storm_distance_km'] = dist
    buoy_df['storm_section_9'] = None
    buoy_df['hurdat_storm_id'] = storm_row['hurdat_storm_id']
    buoy_df['is_storm'] = True
    return buoy_df, storm_name

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0  # Earth radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))  

def update_time_steps_table(cur, row_to_update, storm_name, time_step_id):
    cur.execute("""
        UPDATE dirspec.time_steps ts
        SET storm_name = %s,
            storm_type = %s,
            storm_heading_deg = %s,
            storm_speed_kts = %s,
            storm_distance_km = %s,
            storm_section_9 = %s,
            hurdat_storm_id = %s,
            is_storm = %s
        WHERE ts.id = %s;
    """, (
        storm_name,
        row_to_update['storm_type'], 
        row_to_update['storm_heading_deg'], 
        row_to_update['storm_speed_kts'],
        row_to_update['storm_distance_km'], 
        row_to_update['storm_section_9'], 
        row_to_update['hurdat_storm_id'], 
        row_to_update['is_storm'],
        time_step_id
    ))