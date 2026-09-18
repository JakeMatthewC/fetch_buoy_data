def calc_D(df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, station_id, f_type):
    # storm_dict / start_date / end_date / save_is_storm used to decide which
    # timesteps got spectra_directional rows. That table is gone, so every
    # timestep now gets the same treatment and those arguments were removed
    # rather than left as no-ops.
    import sys
    import pandas as pd
    import psycopg2
    import numpy as np

    # modules
    import processes.utils as u
    import config.config as c
    import processes.directional as dirn

    # loops through each timestep where spec_ingested = False
    for i,spec_row in df_data_spec.iterrows():
        # get the timestep row for all tables needed
        swdir_row = df_swdir.iloc[i,:]
        swdir2_row = df_swdir2.iloc[i,:]
        swr1_row = df_swr1.iloc[i,:]
        swr2_row = df_swr2.iloc[i,:]

        # check that all rows have the timestep
        datetime_obj = spec_row['datetime']
        if all(datetime_obj == row['datetime'] for row in [swdir_row, swdir2_row, swr1_row, swr2_row]):
            pass
        else:
            print('Datetime mismatch')
            sys.exit()
        
        if f_type == 'noaa-rt' or f_type == 'noaa-api':
            # drop the unneeded rows for calculations
            spec_row = spec_row.iloc[3:]
        else:
            spec_row = spec_row.iloc[2:]
        swdir_row = swdir_row.iloc[2:]
        swdir2_row = swdir2_row.iloc[2:]
        swr1_row = swr1_row.iloc[2:]
        swr2_row = swr2_row.iloc[2:]

        # prepare for vectorized calculation.
        # alpha1/alpha2 arrive as meteorological degrees (direction FROM, clockwise
        # from true north) -- the same convention as time_steps.mwd and as the
        # direction grid, so no rotation happens anywhere. See processes/directional.py.
        alpha1_deg = pd.to_numeric(swdir_row, errors='coerce').to_numpy(dtype=float) % 360
        alpha2_deg = pd.to_numeric(swdir2_row, errors='coerce').to_numpy(dtype=float) % 360

        r1 = pd.to_numeric(swr1_row,errors='coerce')
        r1 = np.array(r1)
        r2 = pd.to_numeric(swr2_row, errors='coerce')
        r2 = np.array(r2)

        Ef = pd.to_numeric(spec_row, errors='coerce')
        Ef = np.array(Ef)

        E = Ef[:, None]

        # Maximum Entropy Method rather than the two-term cosine series. MEM
        # reproduces the moments the buoy measured; the truncated series does not,
        # because clipping its negative lobes and renormalising lowers r1 -- which
        # overstated the directional spread by ~21 deg on real NDBC data. It also
        # removes the spurious mirror lobe near alpha2+180 that the 180-deg period
        # of cos(2(theta-alpha2)) always produces when r2 is large.
        #
        # processes/directional.py clips, normalises, and returns NaN rows where the
        # inputs are missing or 1-|c1|^2 collapses (r1 -> 1, ~0.05% of real bins).
        D_normalized = dirn.mem(alpha1_deg, alpha2_deg, r1, r2, c.directional_pnts_deg)
        valid_row = np.isfinite(D_normalized).all(axis=1, keepdims=True)

        # get the timestep id from the timesteps table
        timestep_id = u.get_time_step_id(c.cur, str(station_id), datetime_obj)

        # organize for exporting to postgres table
        records_param = []
        for m, f in enumerate(c.noaa_freqs):
            # already meteorological degrees - no conversion needed on the way out
            a_1 = float(alpha1_deg[m]) if np.isfinite(alpha1_deg[m]) else None
            a_2 = float(alpha2_deg[m]) if np.isfinite(alpha2_deg[m]) else None
            r_1 = float(r1[m]) if np.isfinite(r1[m]) else None
            r_2 = float(r2[m]) if np.isfinite(r2[m]) else None
            energy_density = float(E[m, 0]) if np.isfinite(E[m, 0]) else None
            # store the bin width alongside the density so the spectrum can be
            # integrated (m0, Te, Hm0) from the database alone, without needing
            # WPM_spectra.xlsx. Matters because CDIP data is resampled onto these
            # NOAA bins and nothing else in the schema records that.
            bandwidth = float(c.noaa_bandwidths[m])
            records_param.append((int(timestep_id), float(f), bandwidth, a_1, a_2, r_1, r_2, energy_density))

        # write the spectral data to the spec table
        spectra_parameters_insert_query = """
            INSERT INTO dirspec.spectra_parameters (time_step_id, frequency, bandwidth, alpha1, alpha2, r1, r2, energy_density)
            VALUES %s
            ON CONFLICT (time_step_id, frequency) DO NOTHING
        """
        psycopg2.extras.execute_values(c.cur, spectra_parameters_insert_query, records_param, page_size=100)

        # ---- per-timestep directional metrics -------------------------------
        # spectra_directional is gone: D(theta) is reconstructed on demand from
        # the parameters above, exactly (verified max|diff| = 0), so caching it
        # only created a second copy that could drift out of step.
        #
        # Instead store two numbers that summarise the sea state and that a model
        # can use directly. They answer different questions and neither replaces
        # the other -- see processes/directional.py.
        energy_per_bin = Ef * c.noaa_bandwidths            # E(f)*df, not density
        conc_r, _mean_dir = dirn.frequency_concentration(alpha1_deg, energy_per_bin)
        sep_deg = dirn.sea_swell_separation(alpha1_deg, energy_per_bin, c.noaa_freqs)
        modality_res = dirn.modality_label(conc_r, sep_deg)

        conc_out = float(conc_r) if np.isfinite(conc_r) else None
        sep_out = float(sep_deg) if np.isfinite(sep_deg) else None

        c.cur.execute("""
            UPDATE dirspec.time_steps
            SET dir_concentration_r = %s,
                sea_swell_sep_deg   = %s,
                modality_boot       = %s,
                spectra_ingested    = TRUE
            WHERE id = %s
        """, (conc_out, sep_out, modality_res, timestep_id))
        c.conn.commit()
