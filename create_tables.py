import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="Jacob",
    password="",
    host="localhost",
    port="5432"
)

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dirspec.buoys (
                id SERIAL PRIMARY KEY,
                station_id TEXT UNIQUE NOT NULL,
                name TEXT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                depth DOUBLE PRECISION
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dirspec.time_steps (
                id SERIAL PRIMARY KEY,
                buoy_id INTEGER REFERENCES dirspec.buoys(id),
                timestamp TIMESTAMPTZ NOT NULL,

                -- Observational metadata
                wdir INTEGER,                     -- Wind direction (degrees)
                wspd DOUBLE PRECISION,            -- Wind speed (m/s or knots)
                gst  DOUBLE PRECISION,            -- Wind gust (m/s or knots)
                wvht DOUBLE PRECISION,            -- Significant wave height [m]
                dpd  DOUBLE PRECISION,            -- Dominant period [s]
                apd  DOUBLE PRECISION,            -- Average period [s]
                mwd  DOUBLE PRECISION,            -- Mean wave direction (from) [deg]
                pres DOUBLE PRECISION,            -- Atmospheric pressure [hPa]
                atmp DOUBLE PRECISION,            -- Air temp [°C]
                wtmp DOUBLE PRECISION,            -- Water temp [°C]
                dewp DOUBLE PRECISION,            -- Dew point [°C]
                vis  DOUBLE PRECISION,            -- Visibility [nmi]
                ptdy DOUBLE PRECISION,            -- Pressure tendency [hPa]
                tide DOUBLE PRECISION,            -- Tide level [ft or m]

                -- Derived spectral parameters
                m0   DOUBLE PRECISION,            -- Spectral moment 0
                hm0  DOUBLE PRECISION,            -- Significant wave height from spectrum
                m_1  DOUBLE PRECISION,            -- Spectral moment 1
                Te   DOUBLE PRECISION,            -- Energy period
                P    DOUBLE PRECISION,            -- Wave power [kW/m]
                    
                spectra_ingested BOOLEAN DEFAULT FALSE, -- Marker to record that spectral data was ingested for the timestep

            UNIQUE (buoy_id, timestamp)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dirspec.spectra_parameters (
                time_step_id INTEGER REFERENCES dirspec.time_steps(id),
                frequency DOUBLE PRECISION,
                alpha1 DOUBLE PRECISION,
                alpha2 DOUBLE PRECISION,
                r1 DOUBLE PRECISION,
                r2 DOUBLE PRECISION,
                energy_density DOUBLE PRECISION,
                UNIQUE (time_step_id, frequency, direction)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dirspec.spectra_directional (
                time_step_id INTEGER REFERENCES dirspec.time_steps(id),
                frequency DOUBLE PRECISION,
                direction INTEGER,
                spreading DOUBLE PRECISION,
                UNIQUE (time_step_id, frequency, direction)
            );
        """)

        conn.commit()