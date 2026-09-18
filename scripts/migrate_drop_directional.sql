-- Drops dirspec.spectra_directional and adds the two sea-state metrics that
-- replace the old modality detector.
--
--   psql -h localhost -U Jacob -d postgres -f scripts/migrate_drop_directional.sql
--
-- Idempotent. Regenerate scripts/schema_live.sql afterwards.
--
-- DESTRUCTIVE: the DROP removes ~165.7M rows / 17 GB. That is intended. Every
-- row was exactly reproducible from spectra_parameters via MEM (verified
-- max|diff| = 0.000e+00), so the table was a cache, and keeping a second copy of
-- a derived quantity is what let the viewer and the pipeline drift into
-- disagreeing by a reflection. D(theta) is now rebuilt on demand.

BEGIN;

-- ------------------------------------------------------- the cache table goes
DROP TABLE IF EXISTS dirspec.spectra_directional;

-- ------------------------------------------------------------- new metrics
ALTER TABLE dirspec.time_steps
    ADD COLUMN IF NOT EXISTS dir_concentration_r double precision;
ALTER TABLE dirspec.time_steps
    ADD COLUMN IF NOT EXISTS sea_swell_sep_deg   double precision;

COMMENT ON COLUMN dirspec.time_steps.dir_concentration_r IS
  'Directional concentration across frequency, 0 to 1. Energy-weighted resultant '
  'length of the per-frequency mean directions: each alpha1(f) becomes a unit '
  'vector, weighted by E(f)*bandwidth, and this is the length of their sum. '
  '1 = every frequency arrives from the same bearing; 0 = they cancel. '
  'A CONCENTRATION, NOT A SEPARATION: two systems 109 deg apart, two systems '
  '118 deg apart split 35/65, and one broad unimodal sea all give ~0.58. '
  'The equivalent wrapped-normal spread is sqrt(-2*ln(r)) radians, but that '
  'saturates at r=0 and under-reads lopsided pairs, so r is what is stored.';

COMMENT ON COLUMN dirspec.time_steps.sea_swell_sep_deg IS
  'Angle in degrees, 0 to 180, between the energy-weighted mean wave directions '
  'below and at/above the spectral peak - i.e. how far apart the swell and the '
  'wind sea are pointing. Split at the peak of E(f)*df; that rule beat a fixed '
  '0.125 Hz boundary, a wind-based wave-age split and a maximum-contrast search '
  'on 7,031 real timesteps, and needs no wind speed (which CDIP rows lack). '
  'HAS A FLOOR OF ABOUT 12 DEG from a single system turning with frequency, so a '
  'small value means one system, not two close together. Cannot say WHETHER two '
  'systems exist - that is dir_concentration_r.';

COMMENT ON COLUMN dirspec.time_steps.modality_boot IS
  'Rule-based sea-state label derived from dir_concentration_r and '
  'sea_swell_sep_deg: "bimodal" when r < 0.85 AND sep > 45 deg, otherwise '
  '"unimodal"; "undetermined" when either metric is unavailable. Thresholds were '
  'fitted on NDBC 42055/2024 and validated against the anemometer - the selected '
  '10.3% of timesteps have their swell band a median 104.8 deg off the measured '
  'wind, against 34.0 deg for the rest. Deliberately conservative. '
  'SUPERSEDES an earlier definition that counted directional peaks within each '
  'single frequency bin; that quantity was anti-correlated (r = -0.24) with '
  'genuine two-system seas, because two Fourier harmonics cannot resolve two '
  'modes inside one bin and it was reading the alpha2 mirror lobe as a second '
  'system. Prefer the two continuous columns over this label.';

COMMENT ON COLUMN dirspec.time_steps.spectra_ingested IS
  'TRUE once spectra_parameters rows have been written for this timestep. '
  'Previously meant "per-direction rows were written to spectra_directional", '
  'which no longer exists.';

COMMIT;
