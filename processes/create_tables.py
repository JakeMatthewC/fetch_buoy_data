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
            CREATE TABLE dirspec.buoys (
                id,
                station_id TEXT,
                name TEXT,
                lat FLOAT,
                lon FLOAT,
                depth FLOAT,

                PRIMARY KEY (id)
            );
        """)

        cur.execute("""
            CREATE TABLE dirspec.time_steps (
                id,
                buoy_id TEXT NOT NULL,
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,

                -- Wind and meteorological data
                wdir FLOAT,              -- Wind direction
                wspd FLOAT,              -- Wind speed
                gst FLOAT,               -- Gust speed

                -- Wave parameters
                wvht FLOAT,              -- Significant wave height
                dpd FLOAT,               -- Dominant period
                apd FLOAT,               -- Average period
                mwd FLOAT,               -- Mean wave direction

                -- Atmospheric & temperature data
                pres FLOAT,              -- Pressure
                atmp FLOAT,              -- Air temperature
                wtmp FLOAT,              -- Water temperature
                dewp FLOAT,              -- Dew point
                vis FLOAT,               -- Visibility
                ptdy FLOAT,              -- Pressure tendency

                -- Tide & spectral moments (these are all calculated during ingestion)
                tide FLOAT,
                m0 FLOAT,
                hm0 FLOAT,
                m_1 FLOAT,
                te FLOAT,
                p FLOAT,

                -- Ingestion and source tracking
                spectra_ingested BOOLEAN DEFAULT FALSE,
                source TEXT,

                -- Storm-related metadata
                storm_name TEXT,
                storm_type TEXT,
                storm_heading_deg FLOAT,
                storm_speed_kts FLOAT,
                storm_distance_km FLOAT,
                storm_section_9 TEXT,
                hurdat_storm_id TEXT,
                is_storm BOOLEAN DEFAULT FALSE,

                -- Modality classification
                modality_boot TEXT,          -- e.g., 'unimodal', 'bimodal'
                modality_model TEXT,         -- model used
                modality_conf FLOAT,         -- confidence
                modality_ver TEXT,           -- model version

                -- Source attribution
                met_source TEXT,
                tide_source TEXT,
                tide_method TEXT,

                PRIMARY KEY (buoy_id, timestamp),
                FOREIGN KEY (buoy_id) REFERENCES dirspec.buoys (buoy_id)
            );

        """)

        cur.execute("""
            CREATE TABLE dirspec.spectra_parameters (
                time_step_id TEXT,
                frequency FLOAT,          -- Frequency (Hz)

                energy_density FLOAT,            -- Energy density
                r1 FLOAT,
                r2 FLOAT,
                alpha1 FLOAT,
                alpha2 FLOAT,

                PRIMARY KEY (time_step_id, frequency),
                FOREIGN KEY (time_step_id) REFERENCES dirspec.time_steps (id)
            );

        """)

        cur.execute("""
            CREATE TABLE dirspec.spectra_directional (
                time_step_id TEXT,
                frequency FLOAT,        -- Frequency (Hz)
                direction INTEGER,       -- Direction bin (degrees: 0–359)
                spreading FLOAT,           -- Normalized directional energy (unitless)

                PRIMARY KEY (time_step_id, frequency, direction),
                FOREIGN KEY (time_step_id) REFERENCES dirspec.time_steps (id)
            );

        """)

        conn.commit()