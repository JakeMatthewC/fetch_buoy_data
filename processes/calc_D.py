def calc_D(storm_dict, df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, station_id, f_type, start_date, end_date, save_is_storm):
    import sys
    import pandas as pd
    import psycopg2
    import numpy as np

    # modules
    import processes.utils as u
    import config.config as c
    import processes.detect_modality as dm

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

        # prepare for vectorized calculation
        # alpha1/alpha2 arrive as meteorological degrees (direction FROM, clockwise
        # from true north) -- the same convention as time_steps.mwd.
        #
        # D is now built directly on the meteorological grid. cos() is even and the
        # met<->math map is a reflection (theta_math = 270 - theta_met), so
        #     cos(theta_math - alpha_math) == cos(theta_met - alpha_met)
        # and likewise for the 2*(...) harmonic. Staying in met therefore yields a
        # numerically identical D while putting spectra_directional.direction,
        # spectra_parameters.alpha1/alpha2 and time_steps.mwd all on ONE convention.
        # Previously D was built in math space but its bins were stored with math
        # labels while alpha1/alpha2 were stored as met -- a hidden reflection.
        alpha1_deg = pd.to_numeric(swdir_row, errors='coerce').to_numpy(dtype=float) % 360
        alpha2_deg = pd.to_numeric(swdir2_row, errors='coerce').to_numpy(dtype=float) % 360
        alpha1 = np.deg2rad(alpha1_deg)
        alpha2 = np.deg2rad(alpha2_deg)

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
        # a missing alpha or r makes the whole row NaN, and `NaN == 0` is False, so
        # the previous guard let NaN rows through and wrote NaN spreading to postgres
        valid_row = np.isfinite(row_sums) & (row_sums > 0)
        # normalize so that the integral of D over theta is 1
        D_normalized = np.divide(D, row_sums, out=np.full_like(D, np.nan),
                                 where=valid_row)

        S = D_normalized * E

        # get the timestep id from the timesteps table
        timestep_id = u.get_time_step_id(c.cur, str(station_id), datetime_obj)

        # organize for exporting to postgres table
        records_param = []
        records_dir = []
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

        for m, f in enumerate(c.noaa_freqs):
            if not valid_row[m, 0]:
                # D(theta) is undefined for this bin - record nothing rather than NaN
                continue
            for n, theta in enumerate(c.directional_pnts_deg):
                # theta is a METEOROLOGICAL direction bin, matching alpha1/alpha2
                records_dir.append((int(timestep_id), float(f), int(theta),
                                    float(D_normalized[m, n])))

        # determine modality and record for each timestep
        modality_res = dm.detect_modality_from_dmatrix(S)

        in_date_range = (start_date <= datetime_obj <= end_date)
        is_storm_case = save_is_storm and timestep_id in storm_dict

        if in_date_range or is_storm_case:
            direction_insert_query = """
                INSERT INTO dirspec.spectra_directional (
                    time_step_id, frequency, direction, spreading
                ) VALUES %s
                ON CONFLICT (time_step_id, frequency, direction) DO NOTHING
            """
            if records_dir:
                psycopg2.extras.execute_values(c.cur, direction_insert_query, records_dir, page_size=500)

            c.conn.commit()

            c.cur.execute("""
                UPDATE dirspec.time_steps
                SET modality_boot = %s,     
                    spectra_ingested = TRUE
                WHERE id = %s
            """, (modality_res, timestep_id,))
            c.conn.commit()

        else:
            c.cur.execute("""
                UPDATE dirspec.time_steps
                SET modality_boot = %s,     
                    spectra_ingested = FALSE
                WHERE id = %s
            """, (modality_res, timestep_id,))
            c.conn.commit()
            
