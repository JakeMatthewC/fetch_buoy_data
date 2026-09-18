--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'WIN1252';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: dirspec; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA dirspec;


--
-- Name: SCHEMA dirspec; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA dirspec IS 'Buoy deployments, per-timestep bulk wave/met parameters, and directional wave spectra.';


--
-- Name: storms; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA storms;


--
-- Name: SCHEMA storms; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA storms IS 'HURDAT2 tropical cyclone tracks and their matches to buoy timesteps.';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: buoy_deployments; Type: TABLE; Schema: dirspec; Owner: -
--

CREATE TABLE dirspec.buoy_deployments (
    id integer NOT NULL,
    buoy_id integer NOT NULL,
    deployment_id text,
    start_time timestamp with time zone NOT NULL,
    end_time timestamp with time zone NOT NULL,
    lat double precision NOT NULL,
    lon double precision NOT NULL,
    deployment_type text,
    notes text,
    depth double precision
);


--
-- Name: TABLE buoy_deployments; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON TABLE dirspec.buoy_deployments IS 'One row per deployment (a continuous period at a fixed position). Position lives here, not on buoys, because a station can be redeployed.';


--
-- Name: COLUMN buoy_deployments.id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.id IS 'Surrogate key.';


--
-- Name: COLUMN buoy_deployments.buoy_id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.buoy_id IS 'FK to dirspec.buoys.id.';


--
-- Name: COLUMN buoy_deployments.deployment_id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.deployment_id IS 'Natural key. "NOAA-<station>-<year>" for NDBC year files; the CDIP deployment number for CDIP.';


--
-- Name: COLUMN buoy_deployments.start_time; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.start_time IS 'First timestamp of the deployment, UTC.';


--
-- Name: COLUMN buoy_deployments.end_time; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.end_time IS 'Last timestamp of the deployment, UTC. NULL means open-ended.';


--
-- Name: COLUMN buoy_deployments.lat; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.lat IS 'Deployment latitude, decimal degrees, WGS84, positive north.';


--
-- Name: COLUMN buoy_deployments.lon; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.lon IS 'Deployment longitude, decimal degrees, WGS84, positive east.';


--
-- Name: COLUMN buoy_deployments.deployment_type; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.deployment_type IS 'Provider: "NOAA" or "CDIP".';


--
-- Name: COLUMN buoy_deployments.notes; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.notes IS 'Free text; not populated by the ingest pipeline.';


--
-- Name: COLUMN buoy_deployments.depth; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoy_deployments.depth IS 'Water depth at the deployment site, metres. NULL for NDBC (not ingested); CDIP metaWaterDepth otherwise.';


--
-- Name: buoy_deployments_id_seq; Type: SEQUENCE; Schema: dirspec; Owner: -
--

CREATE SEQUENCE dirspec.buoy_deployments_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: buoy_deployments_id_seq; Type: SEQUENCE OWNED BY; Schema: dirspec; Owner: -
--

ALTER SEQUENCE dirspec.buoy_deployments_id_seq OWNED BY dirspec.buoy_deployments.id;


--
-- Name: buoys; Type: TABLE; Schema: dirspec; Owner: -
--

CREATE TABLE dirspec.buoys (
    id integer NOT NULL,
    station_id text NOT NULL,
    name text,
    project text
);


--
-- Name: TABLE buoys; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON TABLE dirspec.buoys IS 'One row per physical buoy station.';


--
-- Name: COLUMN buoys.id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoys.id IS 'Surrogate key. Referenced by time_steps.buoy_id and buoy_deployments.buoy_id.';


--
-- Name: COLUMN buoys.station_id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoys.station_id IS 'Provider station identifier: NDBC 5-character id (e.g. "42055") or CDIP numeric station (e.g. "134"). Unique.';


--
-- Name: COLUMN buoys.name; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoys.name IS 'Human-readable station name as published by the provider.';


--
-- Name: COLUMN buoys.project; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.buoys.project IS 'Operating programme: "NOAA" for NDBC stations, or the CDIP netCDF global "project" attribute.';


--
-- Name: buoys_id_seq; Type: SEQUENCE; Schema: dirspec; Owner: -
--

CREATE SEQUENCE dirspec.buoys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: buoys_id_seq; Type: SEQUENCE OWNED BY; Schema: dirspec; Owner: -
--

ALTER SEQUENCE dirspec.buoys_id_seq OWNED BY dirspec.buoys.id;


--
-- Name: spectra_parameters; Type: TABLE; Schema: dirspec; Owner: -
--

CREATE TABLE dirspec.spectra_parameters (
    time_step_id integer NOT NULL,
    frequency double precision NOT NULL,
    alpha1 double precision,
    alpha2 double precision,
    r1 double precision,
    r2 double precision,
    energy_density double precision,
    bandwidth double precision
);


--
-- Name: TABLE spectra_parameters; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON TABLE dirspec.spectra_parameters IS 'Frequency-binned wave spectrum: energy density plus the first two directional Fourier pairs. This is the authoritative directional spectrum - D(theta) for any timestep, frequency and direction grid can be reconstructed from these columns alone, exactly reproducing dirspec.spectra_directional. 46 rows per timestep on the NDBC bin layout (CDIP data is resampled onto the same bins).';


--
-- Name: COLUMN spectra_parameters.time_step_id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.time_step_id IS 'FK to dirspec.time_steps.id (ON DELETE CASCADE).';


--
-- Name: COLUMN spectra_parameters.frequency; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.frequency IS 'Bin centre frequency, Hz. 46 NDBC bins from 0.0325 to 0.4850 Hz.';


--
-- Name: COLUMN spectra_parameters.alpha1; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.alpha1 IS 'First-harmonic mean wave direction for this frequency bin, degrees true, direction the waves are coming FROM (meteorological). Same convention as time_steps.mwd. NDBC swdir; for CDIP, degrees(atan2(b1, a1)).';


--
-- Name: COLUMN spectra_parameters.alpha2; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.alpha2 IS 'Second-harmonic (principal) wave direction, degrees true, coming FROM. NDBC swdir2; for CDIP, 0.5*degrees(atan2(b2, a2)) - the half angle, because a2 = r2*cos(2*alpha2).';


--
-- Name: COLUMN spectra_parameters.r1; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.r1 IS 'First-harmonic normalised Fourier coefficient, dimensionless, 0-1. Larger means a narrower directional distribution. NDBC publishes this scaled by 100 in the historical files; the ingest divides it back down.';


--
-- Name: COLUMN spectra_parameters.r2; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.r2 IS 'Second-harmonic normalised Fourier coefficient, dimensionless, 0-1. Also divided by 100 on ingest for NDBC historical files.';


--
-- Name: COLUMN spectra_parameters.energy_density; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.energy_density IS 'Non-directional spectral energy density E(f), m^2/Hz. For CDIP, interpolated onto the NDBC bins as energy (E*df) and then divided back out by bandwidth.';


--
-- Name: COLUMN spectra_parameters.bandwidth; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.spectra_parameters.bandwidth IS 'Width of this frequency bin, Hz (0.005, 0.01 or 0.02 depending on the band). Bins are contiguous and tile 0.030-0.495 Hz. Multiply by energy_density to integrate the spectrum.';


--
-- Name: time_steps; Type: TABLE; Schema: dirspec; Owner: -
--

CREATE TABLE dirspec.time_steps (
    id integer NOT NULL,
    buoy_id integer,
    "timestamp" timestamp with time zone NOT NULL,
    wdir integer,
    wspd double precision,
    gst double precision,
    wvht double precision,
    dpd double precision,
    apd double precision,
    mwd double precision,
    pres double precision,
    atmp double precision,
    wtmp double precision,
    dewp double precision,
    vis double precision,
    ptdy double precision,
    tide double precision,
    m0 double precision,
    hm0 double precision,
    m_1 double precision,
    te double precision,
    p double precision,
    spectra_ingested boolean DEFAULT false,
    source text,
    storm_name text,
    storm_type text,
    storm_heading_deg real,
    storm_speed_kts real,
    storm_distance_km real,
    storm_section_9 text,
    hurdat_storm_id text,
    is_storm boolean DEFAULT false,
    modality_boot text,
    modality_model text,
    modality_conf double precision,
    modality_ver text,
    met_source text,
    tide_source text,
    tide_method text,
    enso_mei double precision,
    dir_concentration_r double precision,
    sea_swell_sep_deg double precision
);


--
-- Name: TABLE time_steps; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON TABLE dirspec.time_steps IS 'One row per buoy observation time. Bulk wave/met parameters plus derived spectral moments, storm match metadata and climate indices. Unique on (buoy_id, timestamp).';


--
-- Name: COLUMN time_steps.id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.id IS 'Surrogate key. Referenced by spectra_parameters, spectra_directional and storms.storm_matches.';


--
-- Name: COLUMN time_steps.buoy_id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.buoy_id IS 'FK to dirspec.buoys.id.';


--
-- Name: COLUMN time_steps."timestamp"; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps."timestamp" IS 'Observation time, UTC.';


--
-- Name: COLUMN time_steps.wdir; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.wdir IS 'Wind direction, degrees true, direction the wind is coming FROM. Buoy anemometer when met_source=''buoy''; derived from ERA5 u10/v10 as (180 + atan2(u,v)) when met_source=''ERA5''.';


--
-- Name: COLUMN time_steps.wspd; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.wspd IS 'Wind speed, m/s. Buoy anemometer, or ERA5 10 m wind magnitude.';


--
-- Name: COLUMN time_steps.gst; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.gst IS 'Wind gust speed, m/s.';


--
-- Name: COLUMN time_steps.wvht; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.wvht IS 'Significant wave height as published by the provider, metres (NDBC WVHT / CDIP waveHs). Compare with hm0, which this pipeline computes from the spectrum.';


--
-- Name: COLUMN time_steps.dpd; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.dpd IS 'Dominant (peak) wave period, seconds (NDBC DPD / CDIP waveTp).';


--
-- Name: COLUMN time_steps.apd; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.apd IS 'Average wave period, seconds (NDBC APD / CDIP waveTa).';


--
-- Name: COLUMN time_steps.mwd; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.mwd IS 'Mean wave direction, degrees true, direction the waves are coming FROM (NDBC MWD / CDIP waveDp). Same convention as spectra_parameters.alpha1.';


--
-- Name: COLUMN time_steps.pres; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.pres IS 'Sea level pressure, hPa (ERA5 msl converted from Pa when met_source=''ERA5'').';


--
-- Name: COLUMN time_steps.atmp; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.atmp IS 'Air temperature, degrees Celsius (ERA5 t2m converted from Kelvin when met_source=''ERA5'').';


--
-- Name: COLUMN time_steps.wtmp; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.wtmp IS 'Sea surface temperature, degrees Celsius. NULL for CDIP-sourced rows (not ingested).';


--
-- Name: COLUMN time_steps.dewp; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.dewp IS 'Dew point temperature, degrees Celsius. NULL for CDIP-sourced rows.';


--
-- Name: COLUMN time_steps.vis; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.vis IS 'Station visibility as reported by NDBC. The historical file header labels this "mi" while NDBC measdes documents nautical miles - treat the unit as uncertain. NULL for CDIP-sourced rows.';


--
-- Name: COLUMN time_steps.ptdy; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.ptdy IS 'Pressure tendency over the preceding 3 hours, hPa. NULL for CDIP-sourced rows.';


--
-- Name: COLUMN time_steps.tide; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.tide IS 'Water level in METRES relative to MLLW, from the nearest NOAA Tides & Currents station (product=hourly_height, datum=MLLW, units=metric), linearly interpolated in time onto this timestamp. NOT the raw NDBC TIDE column, which is in feet.';


--
-- Name: COLUMN time_steps.m0; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.m0 IS 'Zeroth spectral moment, sum(energy_density * bandwidth) over all frequency bins. Units m^2.';


--
-- Name: COLUMN time_steps.hm0; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.hm0 IS 'Significant wave height from the spectrum, 4*sqrt(m0), metres. Independent of the provider-published wvht.';


--
-- Name: COLUMN time_steps.m_1; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.m_1 IS 'Minus-first spectral moment, sum(energy_density * bandwidth / frequency). Units m^2*s.';


--
-- Name: COLUMN time_steps.te; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.te IS 'Energy period, m_1 / m0, seconds.';


--
-- Name: COLUMN time_steps.p; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.p IS 'Deep-water wave power per metre of crest, kW/m: (rho * g^2 * hm0^2 * Te) / (64 * pi), with rho=1025 kg/m^3 and g=9.81 m/s^2, divided by 1000.';


--
-- Name: COLUMN time_steps.spectra_ingested; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.spectra_ingested IS 'TRUE once spectra_parameters rows have been written for this timestep. Previously meant "per-direction rows were written to spectra_directional", which no longer exists.';


--
-- Name: COLUMN time_steps.source; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.source IS 'Ingest path: "noaa-year" (NDBC historical), "noaa-rt" (NDBC realtime), or "cdip".';


--
-- Name: COLUMN time_steps.storm_name; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.storm_name IS 'Name of the matched storm.';


--
-- Name: COLUMN time_steps.storm_type; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.storm_type IS 'HURDAT2 system status at the matched track point (HU, TS, TD, EX, ...).';


--
-- Name: COLUMN time_steps.storm_heading_deg; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.storm_heading_deg IS 'Direction the storm is moving TOWARD, degrees true. NOTE: this is the opposite convention from every wave and wind direction in this schema, which are "from" directions. Derived as the great-circle bearing between consecutive HURDAT2 track points.';


--
-- Name: COLUMN time_steps.storm_speed_kts; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.storm_speed_kts IS 'Storm translation speed, knots, from consecutive HURDAT2 track points.';


--
-- Name: COLUMN time_steps.storm_distance_km; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.storm_distance_km IS 'Great-circle distance from the buoy to the storm centre at the matched track time, kilometres.';


--
-- Name: COLUMN time_steps.storm_section_9; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.storm_section_9 IS 'Intended 9-sector position of the buoy relative to the storm. NOT POPULATED - the ingest code writes NULL.';


--
-- Name: COLUMN time_steps.hurdat_storm_id; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.hurdat_storm_id IS 'HURDAT2 storm identifier (e.g. "AL092021") of the matched storm.';


--
-- Name: COLUMN time_steps.is_storm; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.is_storm IS 'TRUE when a HURDAT2 track point fell within the match radius of this timestep.';


--
-- Name: COLUMN time_steps.modality_boot; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.modality_boot IS 'Rule-based sea-state label derived from dir_concentration_r and sea_swell_sep_deg: "bimodal" when r < 0.85 AND sep > 45 deg, otherwise "unimodal"; "undetermined" when either metric is unavailable. Thresholds were fitted on NDBC 42055/2024 and validated against the anemometer - the selected 10.3% of timesteps have their swell band a median 104.8 deg off the measured wind, against 34.0 deg for the rest. Deliberately conservative. SUPERSEDES an earlier definition that counted directional peaks within each single frequency bin; that quantity was anti-correlated (r = -0.24) with genuine two-system seas, because two Fourier harmonics cannot resolve two modes inside one bin and it was reading the alpha2 mirror lobe as a second system. Prefer the two continuous columns over this label.';


--
-- Name: COLUMN time_steps.modality_model; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.modality_model IS 'Name of the ML model that produced modality_conf, if any.';


--
-- Name: COLUMN time_steps.modality_conf; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.modality_conf IS 'Model confidence for the modality classification, 0-1.';


--
-- Name: COLUMN time_steps.modality_ver; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.modality_ver IS 'Version tag of the modality model.';


--
-- Name: COLUMN time_steps.met_source; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.met_source IS 'Origin of the wind/pressure/temperature fields: "buoy" for NDBC, "ERA5" for CDIP stations.';


--
-- Name: COLUMN time_steps.tide_source; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.tide_source IS 'NOAA CO-OPS station id supplying tide, chosen as the nearest station by great-circle distance to the deployment.';


--
-- Name: COLUMN time_steps.tide_method; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.tide_method IS '"auto" = nearest-station lookup with linear time interpolation.';


--
-- Name: COLUMN time_steps.enso_mei; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.enso_mei IS 'Multivariate ENSO Index v2 for the bimonthly season containing this timestamp. Dimensionless standardised anomaly; positive = El Nino.';


--
-- Name: COLUMN time_steps.dir_concentration_r; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.dir_concentration_r IS 'Directional concentration across frequency, 0 to 1. Energy-weighted resultant length of the per-frequency mean directions: each alpha1(f) becomes a unit vector, weighted by E(f)*bandwidth, and this is the length of their sum. 1 = every frequency arrives from the same bearing; 0 = they cancel. A CONCENTRATION, NOT A SEPARATION: two systems 109 deg apart, two systems 118 deg apart split 35/65, and one broad unimodal sea all give ~0.58. The equivalent wrapped-normal spread is sqrt(-2*ln(r)) radians, but that saturates at r=0 and under-reads lopsided pairs, so r is what is stored.';


--
-- Name: COLUMN time_steps.sea_swell_sep_deg; Type: COMMENT; Schema: dirspec; Owner: -
--

COMMENT ON COLUMN dirspec.time_steps.sea_swell_sep_deg IS 'Angle in degrees, 0 to 180, between the energy-weighted mean wave directions below and at/above the spectral peak - i.e. how far apart the swell and the wind sea are pointing. Split at the peak of E(f)*df; that rule beat a fixed 0.125 Hz boundary, a wind-based wave-age split and a maximum-contrast search on 7,031 real timesteps, and needs no wind speed (which CDIP rows lack). HAS A FLOOR OF ABOUT 12 DEG from a single system turning with frequency, so a small value means one system, not two close together. Cannot say WHETHER two systems exist - that is dir_concentration_r.';


--
-- Name: time_steps_id_seq; Type: SEQUENCE; Schema: dirspec; Owner: -
--

CREATE SEQUENCE dirspec.time_steps_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: time_steps_id_seq; Type: SEQUENCE OWNED BY; Schema: dirspec; Owner: -
--

ALTER SEQUENCE dirspec.time_steps_id_seq OWNED BY dirspec.time_steps.id;


--
-- Name: storm_matches; Type: TABLE; Schema: storms; Owner: -
--

CREATE TABLE storms.storm_matches (
    time_step_id integer NOT NULL,
    hurdat_storm_id text NOT NULL,
    storm_distance_km double precision,
    storm_section text,
    storm_track_time timestamp without time zone
);


--
-- Name: TABLE storm_matches; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON TABLE storms.storm_matches IS 'Join table: buoy timesteps that fell within the configured radius of a storm track point.';


--
-- Name: COLUMN storm_matches.time_step_id; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_matches.time_step_id IS 'FK to dirspec.time_steps.id (ON DELETE CASCADE).';


--
-- Name: COLUMN storm_matches.hurdat_storm_id; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_matches.hurdat_storm_id IS 'FK to storms.storms.hurdat_storm_id (ON DELETE CASCADE).';


--
-- Name: COLUMN storm_matches.storm_distance_km; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_matches.storm_distance_km IS 'Great-circle distance from buoy to storm centre, kilometres.';


--
-- Name: COLUMN storm_matches.storm_section; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_matches.storm_section IS 'Intended sector of the buoy relative to the storm. Not populated by the current ingest.';


--
-- Name: COLUMN storm_matches.storm_track_time; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_matches.storm_track_time IS 'Timestamp of the matched storm_tracks point, UTC (within 15 minutes of the buoy timestamp).';


--
-- Name: storm_tracks; Type: TABLE; Schema: storms; Owner: -
--

CREATE TABLE storms.storm_tracks (
    id integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    lat double precision,
    lon double precision,
    wind_speed double precision,
    pressure double precision,
    heading double precision,
    speed double precision,
    radii_34kt jsonb,
    storm_type text,
    rad_max_wind integer,
    hurdat_storm_id text
);


--
-- Name: TABLE storm_tracks; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON TABLE storms.storm_tracks IS 'HURDAT2 track points, interpolated to a 30-minute cadence.';


--
-- Name: COLUMN storm_tracks.id; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.id IS 'Surrogate key.';


--
-- Name: COLUMN storm_tracks."timestamp"; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks."timestamp" IS 'Track point time, UTC.';


--
-- Name: COLUMN storm_tracks.lat; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.lat IS 'Storm centre latitude, decimal degrees, positive north.';


--
-- Name: COLUMN storm_tracks.lon; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.lon IS 'Storm centre longitude, decimal degrees, positive east.';


--
-- Name: COLUMN storm_tracks.wind_speed; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.wind_speed IS 'Maximum sustained wind, KNOTS (HURDAT2 column 7).';


--
-- Name: COLUMN storm_tracks.pressure; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.pressure IS 'Minimum central pressure, millibars / hPa (HURDAT2 column 8). NULL where HURDAT2 reports -999.';


--
-- Name: COLUMN storm_tracks.heading; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.heading IS 'Direction the storm is moving TOWARD, degrees true - the great-circle bearing from the previous track point. Opposite convention from the wave/wind "from" directions elsewhere in this database. NULL at the first point of a track.';


--
-- Name: COLUMN storm_tracks.speed; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.speed IS 'Storm translation speed, KNOTS, from the distance and time between consecutive track points. NULL at the first point.';


--
-- Name: COLUMN storm_tracks.radii_34kt; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.radii_34kt IS 'JSON object of 34-knot wind radii by quadrant (NE, SE, SW, NW), NAUTICAL MILES.';


--
-- Name: COLUMN storm_tracks.storm_type; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.storm_type IS 'HURDAT2 system status: HU hurricane, TS tropical storm, TD tropical depression, EX extratropical, SS/SD subtropical, LO low, DB disturbance.';


--
-- Name: COLUMN storm_tracks.rad_max_wind; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.rad_max_wind IS 'Radius of maximum wind, NAUTICAL MILES. NULL where HURDAT2 reports -999.';


--
-- Name: COLUMN storm_tracks.hurdat_storm_id; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storm_tracks.hurdat_storm_id IS 'FK to storms.storms.hurdat_storm_id.';


--
-- Name: storm_tracks_id_seq; Type: SEQUENCE; Schema: storms; Owner: -
--

CREATE SEQUENCE storms.storm_tracks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: storm_tracks_id_seq; Type: SEQUENCE OWNED BY; Schema: storms; Owner: -
--

ALTER SEQUENCE storms.storm_tracks_id_seq OWNED BY storms.storm_tracks.id;


--
-- Name: storms; Type: TABLE; Schema: storms; Owner: -
--

CREATE TABLE storms.storms (
    id integer NOT NULL,
    storm_name text,
    basin text,
    season_year integer,
    storm_number integer,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    hurdat_storm_id text
);


--
-- Name: TABLE storms; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON TABLE storms.storms IS 'One row per HURDAT2 tropical cyclone.';


--
-- Name: COLUMN storms.id; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.id IS 'Surrogate key.';


--
-- Name: COLUMN storms.storm_name; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.storm_name IS 'Storm name, or "UNNAMED".';


--
-- Name: COLUMN storms.basin; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.basin IS 'HURDAT2 basin code: "AL" North Atlantic, "EP" eastern North Pacific.';


--
-- Name: COLUMN storms.season_year; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.season_year IS 'Season year.';


--
-- Name: COLUMN storms.storm_number; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.storm_number IS 'Sequential storm number within the season.';


--
-- Name: COLUMN storms.start_time; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.start_time IS 'First track point, UTC.';


--
-- Name: COLUMN storms.end_time; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.end_time IS 'Last track point, UTC.';


--
-- Name: COLUMN storms.hurdat_storm_id; Type: COMMENT; Schema: storms; Owner: -
--

COMMENT ON COLUMN storms.storms.hurdat_storm_id IS 'HURDAT2 identifier, e.g. "AL092021" (basin, storm number, season). Unique; the natural join key.';


--
-- Name: storms_id_seq; Type: SEQUENCE; Schema: storms; Owner: -
--

CREATE SEQUENCE storms.storms_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: storms_id_seq; Type: SEQUENCE OWNED BY; Schema: storms; Owner: -
--

ALTER SEQUENCE storms.storms_id_seq OWNED BY storms.storms.id;


--
-- Name: buoy_deployments id; Type: DEFAULT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.buoy_deployments ALTER COLUMN id SET DEFAULT nextval('dirspec.buoy_deployments_id_seq'::regclass);


--
-- Name: buoys id; Type: DEFAULT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.buoys ALTER COLUMN id SET DEFAULT nextval('dirspec.buoys_id_seq'::regclass);


--
-- Name: time_steps id; Type: DEFAULT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.time_steps ALTER COLUMN id SET DEFAULT nextval('dirspec.time_steps_id_seq'::regclass);


--
-- Name: storm_tracks id; Type: DEFAULT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storm_tracks ALTER COLUMN id SET DEFAULT nextval('storms.storm_tracks_id_seq'::regclass);


--
-- Name: storms id; Type: DEFAULT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storms ALTER COLUMN id SET DEFAULT nextval('storms.storms_id_seq'::regclass);


--
-- Name: buoy_deployments buoy_deployments_pkey; Type: CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.buoy_deployments
    ADD CONSTRAINT buoy_deployments_pkey PRIMARY KEY (id);


--
-- Name: buoys buoys_buoy_id_key; Type: CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.buoys
    ADD CONSTRAINT buoys_buoy_id_key UNIQUE (station_id);


--
-- Name: buoys buoys_pkey; Type: CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.buoys
    ADD CONSTRAINT buoys_pkey PRIMARY KEY (id);


--
-- Name: spectra_parameters spectra_parameters_pkey; Type: CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.spectra_parameters
    ADD CONSTRAINT spectra_parameters_pkey PRIMARY KEY (time_step_id, frequency);


--
-- Name: time_steps time_steps_buoy_id_timestamp_key; Type: CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.time_steps
    ADD CONSTRAINT time_steps_buoy_id_timestamp_key UNIQUE (buoy_id, "timestamp");


--
-- Name: time_steps time_steps_pkey; Type: CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.time_steps
    ADD CONSTRAINT time_steps_pkey PRIMARY KEY (id);


--
-- Name: buoy_deployments unique_deployment_period; Type: CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.buoy_deployments
    ADD CONSTRAINT unique_deployment_period UNIQUE (deployment_id, start_time, end_time);


--
-- Name: storm_matches storm_matches_pkey; Type: CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storm_matches
    ADD CONSTRAINT storm_matches_pkey PRIMARY KEY (time_step_id, hurdat_storm_id);


--
-- Name: storm_tracks storm_tracks_pkey; Type: CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storm_tracks
    ADD CONSTRAINT storm_tracks_pkey PRIMARY KEY (id);


--
-- Name: storms storms_pkey; Type: CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storms
    ADD CONSTRAINT storms_pkey PRIMARY KEY (id);


--
-- Name: storms unique_hurdat_storm_id; Type: CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storms
    ADD CONSTRAINT unique_hurdat_storm_id UNIQUE (hurdat_storm_id);


--
-- Name: storms unique_storm_id; Type: CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storms
    ADD CONSTRAINT unique_storm_id UNIQUE (hurdat_storm_id);


--
-- Name: idx_buoy_deployments_buoy_id; Type: INDEX; Schema: dirspec; Owner: -
--

CREATE INDEX idx_buoy_deployments_buoy_id ON dirspec.buoy_deployments USING btree (buoy_id);


--
-- Name: idx_buoy_deployments_time; Type: INDEX; Schema: dirspec; Owner: -
--

CREATE INDEX idx_buoy_deployments_time ON dirspec.buoy_deployments USING btree (start_time, end_time);


--
-- Name: spectra_parameters_timestep_idx; Type: INDEX; Schema: dirspec; Owner: -
--

CREATE INDEX spectra_parameters_timestep_idx ON dirspec.spectra_parameters USING btree (time_step_id);


--
-- Name: buoy_deployments buoy_deployments_buoy_id_fkey; Type: FK CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.buoy_deployments
    ADD CONSTRAINT buoy_deployments_buoy_id_fkey FOREIGN KEY (buoy_id) REFERENCES dirspec.buoys(id);


--
-- Name: spectra_parameters spectra_parameters_timestep_id_fkey; Type: FK CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.spectra_parameters
    ADD CONSTRAINT spectra_parameters_timestep_id_fkey FOREIGN KEY (time_step_id) REFERENCES dirspec.time_steps(id) ON DELETE CASCADE;


--
-- Name: time_steps time_steps_buoy_id_fkey; Type: FK CONSTRAINT; Schema: dirspec; Owner: -
--

ALTER TABLE ONLY dirspec.time_steps
    ADD CONSTRAINT time_steps_buoy_id_fkey FOREIGN KEY (buoy_id) REFERENCES dirspec.buoys(id);


--
-- Name: storm_tracks fk_hurdat_storm_id; Type: FK CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storm_tracks
    ADD CONSTRAINT fk_hurdat_storm_id FOREIGN KEY (hurdat_storm_id) REFERENCES storms.storms(hurdat_storm_id) ON DELETE CASCADE;


--
-- Name: storm_matches storm_matches_hurdat_storm_id_fkey; Type: FK CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storm_matches
    ADD CONSTRAINT storm_matches_hurdat_storm_id_fkey FOREIGN KEY (hurdat_storm_id) REFERENCES storms.storms(hurdat_storm_id) ON DELETE CASCADE;


--
-- Name: storm_matches storm_matches_time_step_id_fkey; Type: FK CONSTRAINT; Schema: storms; Owner: -
--

ALTER TABLE ONLY storms.storm_matches
    ADD CONSTRAINT storm_matches_time_step_id_fkey FOREIGN KEY (time_step_id) REFERENCES dirspec.time_steps(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

