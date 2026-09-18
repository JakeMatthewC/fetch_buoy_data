-- Adds the two missing columns and documents every table and column with its
-- units and direction convention.
--
--   psql -h localhost -U Jacob -d postgres -f scripts/migrate_metadata.sql
--
-- Idempotent: safe to re-run. Regenerate scripts/schema_live.sql afterwards so
-- the dump carries the comments.
--
-- DIRECTION CONVENTIONS USED IN THIS DATABASE
--   Every wave and wind direction is METEOROLOGICAL: degrees true, clockwise
--   from north, naming the direction the waves/wind are coming FROM.
--   The single exception is storms.storm_tracks.heading (and the
--   time_steps.storm_heading_deg copy of it), which is the direction the storm
--   is moving TOWARD. That inconsistency is deliberate (it matches HURDAT2) and
--   is called out again on those columns.

BEGIN;

-- ---------------------------------------------------------------- new columns
ALTER TABLE dirspec.spectra_parameters
    ADD COLUMN IF NOT EXISTS bandwidth double precision;

ALTER TABLE dirspec.time_steps
    ADD COLUMN IF NOT EXISTS enso_mei double precision;

-- ------------------------------------------------------------------- schemas
COMMENT ON SCHEMA dirspec IS
  'Buoy deployments, per-timestep bulk wave/met parameters, and directional wave spectra.';
COMMENT ON SCHEMA storms IS
  'HURDAT2 tropical cyclone tracks and their matches to buoy timesteps.';

-- ------------------------------------------------------------ dirspec.buoys
COMMENT ON TABLE  dirspec.buoys IS 'One row per physical buoy station.';
COMMENT ON COLUMN dirspec.buoys.id IS 'Surrogate key. Referenced by time_steps.buoy_id and buoy_deployments.buoy_id.';
COMMENT ON COLUMN dirspec.buoys.station_id IS 'Provider station identifier: NDBC 5-character id (e.g. "42055") or CDIP numeric station (e.g. "134"). Unique.';
COMMENT ON COLUMN dirspec.buoys.name IS 'Human-readable station name as published by the provider.';
COMMENT ON COLUMN dirspec.buoys.project IS 'Operating programme: "NOAA" for NDBC stations, or the CDIP netCDF global "project" attribute.';

-- ------------------------------------------------- dirspec.buoy_deployments
COMMENT ON TABLE  dirspec.buoy_deployments IS
  'One row per deployment (a continuous period at a fixed position). Position lives here, not on buoys, because a station can be redeployed.';
COMMENT ON COLUMN dirspec.buoy_deployments.id IS 'Surrogate key.';
COMMENT ON COLUMN dirspec.buoy_deployments.buoy_id IS 'FK to dirspec.buoys.id.';
COMMENT ON COLUMN dirspec.buoy_deployments.deployment_id IS 'Natural key. "NOAA-<station>-<year>" for NDBC year files; the CDIP deployment number for CDIP.';
COMMENT ON COLUMN dirspec.buoy_deployments.start_time IS 'First timestamp of the deployment, UTC.';
COMMENT ON COLUMN dirspec.buoy_deployments.end_time IS 'Last timestamp of the deployment, UTC. NULL means open-ended.';
COMMENT ON COLUMN dirspec.buoy_deployments.lat IS 'Deployment latitude, decimal degrees, WGS84, positive north.';
COMMENT ON COLUMN dirspec.buoy_deployments.lon IS 'Deployment longitude, decimal degrees, WGS84, positive east.';
COMMENT ON COLUMN dirspec.buoy_deployments.deployment_type IS 'Provider: "NOAA" or "CDIP".';
COMMENT ON COLUMN dirspec.buoy_deployments.notes IS 'Free text; not populated by the ingest pipeline.';
COMMENT ON COLUMN dirspec.buoy_deployments.depth IS 'Water depth at the deployment site, metres. NULL for NDBC (not ingested); CDIP metaWaterDepth otherwise.';

-- ------------------------------------------------------- dirspec.time_steps
COMMENT ON TABLE dirspec.time_steps IS
  'One row per buoy observation time. Bulk wave/met parameters plus derived spectral moments, storm match metadata and climate indices. Unique on (buoy_id, timestamp).';
COMMENT ON COLUMN dirspec.time_steps.id IS 'Surrogate key. Referenced by spectra_parameters and storms.storm_matches.';
COMMENT ON COLUMN dirspec.time_steps.buoy_id IS 'FK to dirspec.buoys.id.';
COMMENT ON COLUMN dirspec.time_steps."timestamp" IS 'Observation time, UTC.';

COMMENT ON COLUMN dirspec.time_steps.wdir IS 'Wind direction, degrees true, direction the wind is coming FROM. Buoy anemometer when met_source=''buoy''; derived from ERA5 u10/v10 as (180 + atan2(u,v)) when met_source=''ERA5''.';
COMMENT ON COLUMN dirspec.time_steps.wspd IS 'Wind speed, m/s. Buoy anemometer, or ERA5 10 m wind magnitude.';
COMMENT ON COLUMN dirspec.time_steps.gst IS 'Wind gust speed, m/s.';

COMMENT ON COLUMN dirspec.time_steps.wvht IS 'Significant wave height as published by the provider, metres (NDBC WVHT / CDIP waveHs). Compare with hm0, which this pipeline computes from the spectrum.';
COMMENT ON COLUMN dirspec.time_steps.dpd IS 'Dominant (peak) wave period, seconds (NDBC DPD / CDIP waveTp).';
COMMENT ON COLUMN dirspec.time_steps.apd IS 'Average wave period, seconds (NDBC APD / CDIP waveTa).';
COMMENT ON COLUMN dirspec.time_steps.mwd IS 'Mean wave direction, degrees true, direction the waves are coming FROM (NDBC MWD / CDIP waveDp). Same convention as spectra_parameters.alpha1.';

COMMENT ON COLUMN dirspec.time_steps.pres IS 'Sea level pressure, hPa (ERA5 msl converted from Pa when met_source=''ERA5'').';
COMMENT ON COLUMN dirspec.time_steps.atmp IS 'Air temperature, degrees Celsius (ERA5 t2m converted from Kelvin when met_source=''ERA5'').';
COMMENT ON COLUMN dirspec.time_steps.wtmp IS 'Sea surface temperature, degrees Celsius. NULL for CDIP-sourced rows (not ingested).';
COMMENT ON COLUMN dirspec.time_steps.dewp IS 'Dew point temperature, degrees Celsius. NULL for CDIP-sourced rows.';
COMMENT ON COLUMN dirspec.time_steps.vis IS 'Station visibility as reported by NDBC. The historical file header labels this "mi" while NDBC measdes documents nautical miles - treat the unit as uncertain. NULL for CDIP-sourced rows.';
COMMENT ON COLUMN dirspec.time_steps.ptdy IS 'Pressure tendency over the preceding 3 hours, hPa. NULL for CDIP-sourced rows.';

COMMENT ON COLUMN dirspec.time_steps.tide IS
  'Water level in METRES relative to MLLW, from the nearest NOAA Tides & Currents station (product=hourly_height, datum=MLLW, units=metric), linearly interpolated in time onto this timestamp. NOT the raw NDBC TIDE column, which is in feet.';
COMMENT ON COLUMN dirspec.time_steps.tide_source IS 'NOAA CO-OPS station id supplying tide, chosen as the nearest station by great-circle distance to the deployment.';
COMMENT ON COLUMN dirspec.time_steps.tide_method IS '"auto" = nearest-station lookup with linear time interpolation.';

COMMENT ON COLUMN dirspec.time_steps.m0 IS 'Zeroth spectral moment, sum(energy_density * bandwidth) over all frequency bins. Units m^2.';
COMMENT ON COLUMN dirspec.time_steps.hm0 IS 'Significant wave height from the spectrum, 4*sqrt(m0), metres. Independent of the provider-published wvht.';
COMMENT ON COLUMN dirspec.time_steps.m_1 IS 'Minus-first spectral moment, sum(energy_density * bandwidth / frequency). Units m^2*s.';
COMMENT ON COLUMN dirspec.time_steps.te IS 'Energy period, m_1 / m0, seconds.';
COMMENT ON COLUMN dirspec.time_steps.p IS 'Deep-water wave power per metre of crest, kW/m: (rho * g^2 * hm0^2 * Te) / (64 * pi), with rho=1025 kg/m^3 and g=9.81 m/s^2, divided by 1000.';

COMMENT ON COLUMN dirspec.time_steps.enso_mei IS 'Multivariate ENSO Index v2 for the bimonthly season containing this timestamp. Dimensionless standardised anomaly; positive = El Nino.';

COMMENT ON COLUMN dirspec.time_steps.spectra_ingested IS 'TRUE once spectra_parameters rows have been written for this timestep.';
COMMENT ON COLUMN dirspec.time_steps.source IS 'Ingest path: "noaa-year" (NDBC historical), "noaa-rt" (NDBC realtime), or "cdip".';
COMMENT ON COLUMN dirspec.time_steps.met_source IS 'Origin of the wind/pressure/temperature fields: "buoy" for NDBC, "ERA5" for CDIP stations.';

COMMENT ON COLUMN dirspec.time_steps.is_storm IS 'TRUE when a HURDAT2 track point fell within the match radius of this timestep.';
COMMENT ON COLUMN dirspec.time_steps.storm_name IS 'Name of the matched storm.';
COMMENT ON COLUMN dirspec.time_steps.storm_type IS 'HURDAT2 system status at the matched track point (HU, TS, TD, EX, ...).';
COMMENT ON COLUMN dirspec.time_steps.storm_heading_deg IS
  'Direction the storm is moving TOWARD, degrees true. NOTE: this is the opposite convention from every wave and wind direction in this schema, which are "from" directions. Derived as the great-circle bearing between consecutive HURDAT2 track points.';
COMMENT ON COLUMN dirspec.time_steps.storm_speed_kts IS 'Storm translation speed, knots, from consecutive HURDAT2 track points.';
COMMENT ON COLUMN dirspec.time_steps.storm_distance_km IS 'Great-circle distance from the buoy to the storm centre at the matched track time, kilometres.';
COMMENT ON COLUMN dirspec.time_steps.storm_section_9 IS 'Intended 9-sector position of the buoy relative to the storm. NOT POPULATED - the ingest code writes NULL.';
COMMENT ON COLUMN dirspec.time_steps.hurdat_storm_id IS 'HURDAT2 storm identifier (e.g. "AL092021") of the matched storm.';

COMMENT ON COLUMN dirspec.time_steps.modality_boot IS 'Rule-based modality label from the reconstructed directional spectrum: "unimodal", "bimodal" or "undetermined".';
COMMENT ON COLUMN dirspec.time_steps.modality_model IS 'Name of the ML model that produced modality_conf, if any.';
COMMENT ON COLUMN dirspec.time_steps.modality_conf IS 'Model confidence for the modality classification, 0-1.';
COMMENT ON COLUMN dirspec.time_steps.modality_ver IS 'Version tag of the modality model.';

-- ----------------------------------------------- dirspec.spectra_parameters
COMMENT ON TABLE dirspec.spectra_parameters IS
  'Frequency-binned wave spectrum: energy density plus the first two directional Fourier pairs. This is the authoritative directional spectrum - D(theta) for any timestep, frequency and direction grid can be reconstructed from these columns alone, with no second copy stored. 46 rows per timestep on the NDBC bin layout (CDIP data is resampled onto the same bins).';
COMMENT ON COLUMN dirspec.spectra_parameters.time_step_id IS 'FK to dirspec.time_steps.id (ON DELETE CASCADE).';
COMMENT ON COLUMN dirspec.spectra_parameters.frequency IS 'Bin centre frequency, Hz. 46 NDBC bins from 0.0325 to 0.4850 Hz.';
COMMENT ON COLUMN dirspec.spectra_parameters.bandwidth IS
  'Width of this frequency bin, Hz (0.005, 0.01 or 0.02 depending on the band). Bins are contiguous and tile 0.030-0.495 Hz. Multiply by energy_density to integrate the spectrum.';
COMMENT ON COLUMN dirspec.spectra_parameters.energy_density IS
  'Non-directional spectral energy density E(f), m^2/Hz. For CDIP, interpolated onto the NDBC bins as energy (E*df) and then divided back out by bandwidth.';
COMMENT ON COLUMN dirspec.spectra_parameters.alpha1 IS
  'First-harmonic mean wave direction for this frequency bin, degrees true, direction the waves are coming FROM (meteorological). Same convention as time_steps.mwd. NDBC swdir; for CDIP, degrees(atan2(b1, a1)).';
COMMENT ON COLUMN dirspec.spectra_parameters.alpha2 IS
  'Second-harmonic (principal) wave direction, degrees true, coming FROM. NDBC swdir2; for CDIP, 0.5*degrees(atan2(b2, a2)) - the half angle, because a2 = r2*cos(2*alpha2).';
COMMENT ON COLUMN dirspec.spectra_parameters.r1 IS
  'First-harmonic normalised Fourier coefficient, dimensionless, 0-1. Larger means a narrower directional distribution. NDBC publishes this scaled by 100 in the historical files; the ingest divides it back down.';
COMMENT ON COLUMN dirspec.spectra_parameters.r2 IS
  'Second-harmonic normalised Fourier coefficient, dimensionless, 0-1. Also divided by 100 on ingest for NDBC historical files.';

-- dirspec.spectra_directional was dropped by migrate_drop_directional.sql;
-- its COMMENT statements lived here and have been removed so this file
-- still runs. D(theta) is reconstructed from spectra_parameters on demand.

-- --------------------------------------------------------- storms.storms
COMMENT ON TABLE  storms.storms IS 'One row per HURDAT2 tropical cyclone.';
COMMENT ON COLUMN storms.storms.id IS 'Surrogate key.';
COMMENT ON COLUMN storms.storms.hurdat_storm_id IS 'HURDAT2 identifier, e.g. "AL092021" (basin, storm number, season). Unique; the natural join key.';
COMMENT ON COLUMN storms.storms.storm_name IS 'Storm name, or "UNNAMED".';
COMMENT ON COLUMN storms.storms.basin IS 'HURDAT2 basin code: "AL" North Atlantic, "EP" eastern North Pacific.';
COMMENT ON COLUMN storms.storms.season_year IS 'Season year.';
COMMENT ON COLUMN storms.storms.storm_number IS 'Sequential storm number within the season.';
COMMENT ON COLUMN storms.storms.start_time IS 'First track point, UTC.';
COMMENT ON COLUMN storms.storms.end_time IS 'Last track point, UTC.';

-- ---------------------------------------------------- storms.storm_tracks
COMMENT ON TABLE storms.storm_tracks IS 'HURDAT2 track points, interpolated to a 30-minute cadence.';
COMMENT ON COLUMN storms.storm_tracks.id IS 'Surrogate key.';
COMMENT ON COLUMN storms.storm_tracks.hurdat_storm_id IS 'FK to storms.storms.hurdat_storm_id.';
COMMENT ON COLUMN storms.storm_tracks."timestamp" IS 'Track point time, UTC.';
COMMENT ON COLUMN storms.storm_tracks.lat IS 'Storm centre latitude, decimal degrees, positive north.';
COMMENT ON COLUMN storms.storm_tracks.lon IS 'Storm centre longitude, decimal degrees, positive east.';
COMMENT ON COLUMN storms.storm_tracks.wind_speed IS 'Maximum sustained wind, KNOTS (HURDAT2 column 7).';
COMMENT ON COLUMN storms.storm_tracks.pressure IS 'Minimum central pressure, millibars / hPa (HURDAT2 column 8). NULL where HURDAT2 reports -999.';
COMMENT ON COLUMN storms.storm_tracks.heading IS
  'Direction the storm is moving TOWARD, degrees true - the great-circle bearing from the previous track point. Opposite convention from the wave/wind "from" directions elsewhere in this database. NULL at the first point of a track.';
COMMENT ON COLUMN storms.storm_tracks.speed IS 'Storm translation speed, KNOTS, from the distance and time between consecutive track points. NULL at the first point.';
COMMENT ON COLUMN storms.storm_tracks.storm_type IS 'HURDAT2 system status: HU hurricane, TS tropical storm, TD tropical depression, EX extratropical, SS/SD subtropical, LO low, DB disturbance.';
COMMENT ON COLUMN storms.storm_tracks.radii_34kt IS 'JSON object of 34-knot wind radii by quadrant (NE, SE, SW, NW), NAUTICAL MILES.';
COMMENT ON COLUMN storms.storm_tracks.rad_max_wind IS 'Radius of maximum wind, NAUTICAL MILES. NULL where HURDAT2 reports -999.';

-- --------------------------------------------------- storms.storm_matches
COMMENT ON TABLE storms.storm_matches IS 'Join table: buoy timesteps that fell within the configured radius of a storm track point.';
COMMENT ON COLUMN storms.storm_matches.time_step_id IS 'FK to dirspec.time_steps.id (ON DELETE CASCADE).';
COMMENT ON COLUMN storms.storm_matches.hurdat_storm_id IS 'FK to storms.storms.hurdat_storm_id (ON DELETE CASCADE).';
COMMENT ON COLUMN storms.storm_matches.storm_distance_km IS 'Great-circle distance from buoy to storm centre, kilometres.';
COMMENT ON COLUMN storms.storm_matches.storm_section IS 'Intended sector of the buoy relative to the storm. Not populated by the current ingest.';
COMMENT ON COLUMN storms.storm_matches.storm_track_time IS 'Timestamp of the matched storm_tracks point, UTC (within 15 minutes of the buoy timestamp).';

COMMIT;
