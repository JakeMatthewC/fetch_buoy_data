"""Trace one timestep and one frequency bin from raw file through to the plot.

Prints every intermediate number so each step can be checked by hand:

    raw NDBC token -> parsed value -> unit conversion -> stored column
    -> reconstructed D(theta) -> the bearing the viewer draws

    python scripts/trace_spectrum.py                          # first record, peak bin
    python scripts/trace_spectrum.py --row 500 --freq 0.11
    python scripts/trace_spectrum.py --station 42055 --year 2024 --row 81
    python scripts/trace_spectrum.py --no-db                  # raw files only

With a database available it also pulls the stored row for the same timestep and
diffs it against the values derived here, so a mismatch between what the pipeline
wrote and what the maths says is impossible to miss.

Read-only. Never writes anything.
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

DIRS = np.arange(0, 360, 5)
DTH = np.deg2rad(5.0)
NA_SPEC = ["MM", "999.0", "9999.0"]


def rule(title):
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)


def compass(deg):
    pts = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
           "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return pts[int((deg % 360) / 22.5 + 0.5) % 16]


def reconstruct(alpha1_deg, alpha2_deg, r1, r2, dirs=DIRS, method="mem"):
    """Call the pipeline's own reconstruction -- never a private copy of it."""
    import processes.directional as dirn
    return dirn.METHODS[method](alpha1_deg, alpha2_deg, r1, r2, dirs)[0]


def rose(D, alpha1_deg, size=15):
    """ASCII compass rose: north up, east right. Lets you see the direction."""
    grid = [[" "] * (size * 2 + 3) for _ in range(size + 1)]
    peak = D.max() or 1.0
    for deg, val in zip(DIRS, D):
        rad = (val / peak) * size
        th = np.deg2rad(float(deg))
        x = int(round(rad * np.sin(th)))          # east is +x
        y = int(round(rad * np.cos(th)))          # north is +y
        col, row = size + x, size // 2 - y // 2
        if 0 <= row <= size and 0 <= col < size * 2 + 3:
            grid[row][col] = "#" if val > 0.6 * peak else ("+" if val > 0.25 * peak else ".")
    th = np.deg2rad(float(alpha1_deg))
    ax, ay = int(round(size * np.sin(th))), int(round(size * np.cos(th)))
    arow, acol = size // 2 - ay // 2, size + ax
    if 0 <= arow <= size and 0 <= acol < size * 2 + 3 and grid[arow][acol] == " ":
        grid[arow][acol] = "*"
    grid[size // 2][size] = "o"
    print("\n              N (0deg)")
    for r_ in grid:
        print("      " + "".join(r_).rstrip())
    print("              S (180deg)      W=left(270)  E=right(90)")
    print("      # peak energy   + mid   . low   * alpha1 bearing   o buoy")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default="42055")
    ap.add_argument("--year", default="2024")
    ap.add_argument("--row", type=int, default=0, help="record index in the year file")
    ap.add_argument("--freq", type=float, default=None,
                    help="frequency bin in Hz; default is the peak-energy bin")
    ap.add_argument("--no-db", action="store_true", help="skip the database comparison")
    args = ap.parse_args()

    import config.config as cfg_freqs  # noqa: E402  (reads WPM_spectra.xlsx)
    base = os.path.join(r"D:\Buoy_work\Raws Storage\NOAA_Raws\year",
                        args.station, f"{args.year}_year_{args.station}")

    # ---------------------------------------------------------------- 1. raw
    rule(f"1. RAW NDBC FILES   station {args.station}  {args.year}  record #{args.row}")
    load = lambda ext: pd.read_csv(f"{base}.{ext}", sep=r"\s+", skiprows=[1], na_values=NA_SPEC)
    sp, d1, d2, r1f, r2f = (load(e) for e in
                            ("data_spec", "swdir", "swdir2", "swr1", "swr2"))
    txt = pd.read_csv(f"{base}.txt", sep=r"\s+", skiprows=[1], na_values=["MM"])
    import processes.utils as u
    txt = u.mask_stdmet_sentinels(txt)

    stamp = "-".join(str(int(v)) for v in sp.iloc[args.row, 0:3]) + " " + \
            ":".join(f"{int(v):02d}" for v in sp.iloc[args.row, 3:5]) + " UTC"
    print(f"   timestamp                 {stamp}")

    # column 5 is the .0200 bin the pipeline drops; bins start at column 6
    freqs = np.array([float(c) for c in sp.columns[6:]])
    if args.freq is None:
        E_all = sp.iloc[args.row, 6:].to_numpy(float)
        k = int(np.nanargmax(E_all))
        print(f"   frequency bin             peak-energy bin (auto)")
    else:
        k = int(np.argmin(np.abs(freqs - args.freq)))
    f = freqs[k]
    col = 6 + k

    raw_e = sp.iloc[args.row, col]
    raw_a1, raw_a2 = d1.iloc[args.row, col], d2.iloc[args.row, col]
    raw_r1, raw_r2 = r1f.iloc[args.row, col], r2f.iloc[args.row, col]
    print(f"   bin index / frequency     #{k + 2} of 47   f = {f:.4f} Hz   T = {1 / f:.2f} s")
    print(f"   .data_spec  (m^2/Hz)      {raw_e}")
    print(f"   .swdir      (deg)         {raw_a1}")
    print(f"   .swdir2     (deg)         {raw_a2}")
    print(f"   .swr1       (percent)     {raw_r1}")
    print(f"   .swr2       (percent)     {raw_r2}")

    # -------------------------------------------------------- 2. conversions
    rule("2. UNIT CONVERSIONS APPLIED ON INGEST")
    r1 = float(raw_r1) / 100.0
    r2 = float(raw_r2) / 100.0
    a1, a2 = float(raw_a1), float(raw_a2)
    ef = float(raw_e)
    bw = float(cfg_freqs.noaa_bandwidths[k])
    print(f"   r1   {raw_r1:>8} / 100  = {r1:.4f}      NDBC scales r by 100 in year files")
    print(f"   r2   {raw_r2:>8} / 100  = {r2:.4f}")
    print(f"   alpha1  {a1:>8.1f} deg   unchanged -- already meteorological (FROM)")
    print(f"   alpha2  {a2:>8.1f} deg   unchanged")
    print(f"   E(f)    {ef:>8.4f} m^2/Hz  unchanged")
    print(f"   bandwidth {bw:>6.4f} Hz    from WPM_spectra.xlsx column 3, now stored too")

    # --------------------------------------------------------- 3. rebuild D
    rule("3. RECONSTRUCT D(theta)   (Maximum Entropy Method)")
    print("   MEM (Lygre & Krogstad 1986) from c1 = r1*exp(i*a1), c2 = r2*exp(2i*a2)")
    D = reconstruct(a1, a2, r1, r2, method="mem")
    D_old = reconstruct(a1, a2, r1, r2, method="truncated_fourier")

    import processes.directional as _dirn
    for nm, dd in (("MEM", D), ("old 2-term series", D_old)):
        if not np.all(np.isfinite(dd)):
            print(f"   {nm:18s} not defined for this bin (r1 -> 1)")
            continue
        g1, ga1, _, _ = _dirn.directional_moments(dd[None, :], DIRS)
        print(f"   {nm:18s} recovers r1 = {float(g1[0]):.4f} (measured {r1:.4f}), "
              f"alpha1 = {float(ga1[0]):6.1f} (measured {a1:.1f})")
    if not np.all(np.isfinite(D)):
        print("\n   MEM is degenerate here; the pipeline stores NULL for this bin.")
        return 0
    integral = D.sum() * DTH
    peak = int(DIRS[int(np.argmax(D))])
    mx = float((D * np.cos(np.deg2rad(DIRS.astype(float)))).sum() * DTH)
    my = float((D * np.sin(np.deg2rad(DIRS.astype(float)))).sum() * DTH)
    circ = float(np.degrees(np.arctan2(my, mx)) % 360)
    spread = float(np.degrees(np.sqrt(max(0.0, -2 * np.log(min(1.0, np.hypot(mx, my)))))))

    print(f"\n   integral of D dtheta      {integral:.10f}   (must be 1)")
    print(f"   peak bearing              {peak:3d} deg  ({compass(peak)})")
    print(f"   circular mean bearing     {circ:6.1f} deg  ({compass(circ)})")
    print(f"   stored alpha1             {a1:6.1f} deg  ({compass(a1)})")
    print(f"   directional spread        {spread:6.1f} deg")

    published_mwd = txt["MWD"].iloc[args.row] if "MWD" in txt.columns else np.nan
    if np.isfinite(published_mwd):
        err = abs((circ - published_mwd + 180) % 360 - 180)
        print(f"   NDBC published MWD        {published_mwd:6.1f} deg  ({compass(published_mwd)})")
        print(f"   |circular mean - MWD|     {err:6.2f} deg   <-- independent check")

    # The 2-term Longuet-Higgins expansion cannot suppress a mirror lobe when r2 is
    # large: cos(2(theta-alpha2)) has period 180 deg, so it peaks at alpha2 AND at
    # alpha2+180. Worth naming, because in the viewer it looks like a second sea.
    rolled = np.roll(D, -int(np.argmax(D)))
    secondary = None
    for i in range(2, len(rolled) - 2):
        if rolled[i] > rolled[i - 1] and rolled[i] >= rolled[i + 1] and rolled[i] > 0:
            off = i * 5
            if 90 <= off <= 270:
                if secondary is None or rolled[i] > secondary[1]:
                    secondary = (off, rolled[i])
    if secondary is not None and D.max() > 0:
        off, val = secondary
        back = (peak + off) % 360
        print(f"\n   secondary lobe            {back:3d} deg ({compass(back)}), "
              f"{100 * val / D.max():.0f}% of peak")
        print("     an artefact of the expansion, not a second sea: cos(2(t-a2)) has a")
        print(f"     180 deg period, so a large r2 ({r2:.2f}) always raises a mirror lobe near")
        print("     alpha2+180. MEM (Lygre & Krogstad 1986) is the usual way to suppress it.")

    rose(D, a1)

    # ------------------------------------------------- 4. a few sample bins
    rule("4. D(theta) AT SELECTED BEARINGS")
    print("   bearing   D (1/rad)   share of peak")
    for deg in range(0, 360, 30):
        v = D[int(deg // 5)]
        bar = "#" * int(round(40 * v / (D.max() or 1)))
        print(f"   {deg:3d} {compass(deg):>4}  {v:8.5f}   {bar}")

    # ----------------------------------------------------- 5. what's stored
    if args.no_db:
        return 0
    rule("5. COMPARE AGAINST THE DATABASE")
    try:
        import psycopg2
        import config.db as db
        conn = psycopg2.connect(**db.conn_params(), connect_timeout=8)
        conn.set_session(readonly=True)
        cur = conn.cursor()
    except Exception as e:
        print(f"   no database connection ({type(e).__name__}); skipping")
        return 0

    ts = pd.to_datetime(dict(year=[sp.iloc[args.row, 0]], month=[sp.iloc[args.row, 1]],
                             day=[sp.iloc[args.row, 2]], hour=[sp.iloc[args.row, 3]],
                             minute=[sp.iloc[args.row, 4]]), utc=True)[0]
    cur.execute("""
        select ts.id, ts.mwd, ts.hm0, ts.wvht, ts.tide, ts.enso_mei
        from dirspec.time_steps ts join dirspec.buoys b on b.id = ts.buoy_id
        where b.station_id = %s and ts.timestamp = %s""", (str(args.station), ts))
    got = cur.fetchone()
    if not got:
        print(f"   no time_steps row for {ts} -- station not ingested into this database")
        conn.close()
        return 0
    tsid, mwd_db, hm0_db, wvht_db, tide_db, mei_db = got
    print(f"   time_step_id {tsid}   mwd={mwd_db}  hm0={hm0_db}  wvht={wvht_db}"
          f"  tide={tide_db}  enso_mei={mei_db}")

    cur.execute("""select frequency, bandwidth, energy_density, alpha1, alpha2, r1, r2
                   from dirspec.spectra_parameters
                   where time_step_id = %s and abs(frequency - %s) < 1e-9""", (tsid, float(f)))
    row = cur.fetchone()
    if row:
        _, bw_db, ef_db, a1_db, a2_db, r1_db, r2_db = row
        print("\n   spectra_parameters vs the values derived above:")
        for name, here, there in (("bandwidth", bw, bw_db), ("energy_density", ef, ef_db),
                                  ("alpha1", a1, a1_db), ("alpha2", a2, a2_db),
                                  ("r1", r1, r1_db), ("r2", r2, r2_db)):
            if there is None:
                print(f"     {name:16s} derived {here!s:>10}   stored NULL")
                continue
            ok = "ok" if abs(float(there) - here) < 1e-9 else "MISMATCH"
            print(f"     {name:16s} derived {here:10.5f}   stored {float(there):10.5f}   {ok}")
    else:
        print("   no spectra_parameters row for this frequency")

    cur.execute("""select direction, spreading from dirspec.spectra_directional
                   where time_step_id = %s and abs(frequency - %s) < 1e-9
                   order by direction""", (tsid, float(f)))
    stored = cur.fetchall()
    if stored and len(stored) == len(DIRS):
        sv = np.array([s[1] for s in stored], dtype=float)
        print(f"\n   spectra_directional: {len(stored)} bins stored")
        print(f"     max |stored - reconstructed|   {np.abs(sv - D).max():.3e}"
              "   (0 means the cache and the formula agree)")
    else:
        print("\n   spectra_directional: no rows for this timestep/frequency"
              " (reconstructed on the fly by the viewer)")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
