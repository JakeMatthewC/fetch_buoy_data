def calc_D(df_data_spec, df_swdir, df_swdir2, df_swr1, df_swr2, station_id, f_type):
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

        # check that all files have the timestep
        if spec_row['datetime'] == swdir_row['datetime'] and spec_row['datetime'] == swdir2_row['datetime'] and spec_row['datetime'] == swr1_row['datetime'] and spec_row['datetime'] == swr2_row['datetime']:
            pass
        else:
            print('Datetime mismatch')
            sys.exit()
            break

        # save the datetime object for reference later
        datetime_obj = spec_row['datetime']
        
        if f_type == 'rt' or f_type == 'api':
            # drop the unneeded rows for calculations
            spec_row = spec_row.iloc[3:]
        else:
            spec_row = spec_row.iloc[2:]
        swdir_row = swdir_row.iloc[2:]
        swdir2_row = swdir2_row.iloc[2:]
        swr1_row = swr1_row.iloc[2:]
        swr2_row = swr2_row.iloc[2:]

        # prepare for vectorized calculation
        alpha1 = pd.to_numeric(swdir_row, errors='coerce')
        alpha1 = np.where(~np.isnan(alpha1), u.met_to_math_dir(alpha1), np.nan)
        alpha1 = alpha1.astype(float)
        alpha2 = pd.to_numeric(swdir2_row, errors='coerce')
        alpha2 = np.where(~np.isnan(alpha2), u.met_to_math_dir(alpha2), np.nan)
        alpha2 = alpha2.astype(float)

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
        # prevent division by 0
        row_sums[row_sums == 0] = 1
        # normalize
        D_normalized = D / row_sums
        #check = np.sum(D_normalized, axis=1, keepdims=True) * delta_theta_rad
        #print(check)

        S = D_normalized * E

        # get the timestep id from the timesteps table
        timestep_id = u.get_time_step_id(c.cur, str(station_id), datetime_obj)

        # organize for exporting to postgres table
        records_param = []
        records_dir = []
        for m, f in enumerate(c.freqs):
            # these values are recorded to every row for this frequency bin
            a_1 = float(alpha1[m]) if not np.isnan(alpha1[m]) else None
            a_2 = float(alpha2[m]) if not np.isnan(alpha2[m]) else None
            r_1 = float(r1[m]) if not np.isnan(r1[m]) else None
            r_2 = float(r2[m]) if not np.isnan(r2[m]) else None
            energy_density = float(E[m, 0])
            records_param.append((int(timestep_id), float(f), a_1, a_2, r_1, r_2, float(energy_density)))

        # write the spectral data to the spec table
        spectra_parameters_insert_query = """
            INSERT INTO dirspec.spectra_parameters (time_step_id, frequency, alpha1, alpha2, r1, r2, energy_density)
            VALUES %s
            ON CONFLICT (time_step_id, frequency) DO NOTHING
        """
        psycopg2.extras.execute_values(c.cur, spectra_parameters_insert_query, records_param, page_size=100)

        for m, f in enumerate(c.freqs):
            for n, theta in enumerate(c.directional_pnts_deg):
                spreading = float(D_normalized[m, n])
                records_dir.append((timestep_id, f, theta, spreading))

        records_dir = [(int(timestep_id), float(f), int(theta), float(spreading)) for (timestep_id, f, theta, spreading) in records_dir]

        direction_insert_query = """
            INSERT INTO dirspec.spectra_directional (
                time_step_id, frequency, direction, spreading
            ) VALUES %s
            ON CONFLICT (time_step_id, frequency, direction) DO NOTHING
        """
        psycopg2.extras.execute_values(c.cur, direction_insert_query, records_dir, page_size=500)

        # Now determine modality and record for each timestep
        modality_res = dm.detect_modality_from_dmatrix(S)

        c.conn.commit()
        # update flag
        c.cur.execute("""
            UPDATE dirspec.time_steps
            SET modality_boot = %s,     
                spectra_ingested = TRUE
            WHERE id = %s
        """, (modality_res, timestep_id,))
        c.conn.commit()
            
