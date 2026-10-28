"""Re-ingest every buoy deployment present in local raw storage.

Discovers what is on disk, optionally clears the existing dirspec tables, then
runs the real pipeline over each unit and validates the result.

    python scripts/reingest.py --dry-run          # show the plan, touch nothing
    python scripts/reingest.py --clear            # wipe and rebuild (prompts)
    python scripts/reingest.py                    # fill in what is missing only
    python scripts/reingest.py --only noaa
    python scripts/reingest.py --only cdip --skip-missing-era5

Resumable. The pipeline skips timesteps whose spectra_ingested flag is already
set and inserts time_steps ON CONFLICT DO NOTHING, so re-running after an
interruption picks up where it stopped without duplicating anything.

--clear truncates dirspec.buoys, buoy_deployments, time_steps, spectra_parameters
and storms.storm_matches. It does NOT touch storms.storms or storms.storm_tracks,
which are ingested separately by the hurdat2 project and take much longer to
rebuild.
"""
import argparse
import glob
import os
import re
import sys
import time
import traceback

import psycopg2

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
import config.db as db  # noqa: E402
import config as c  # noqa: E402  (paths only -- no database, no pandas)

NOAA_ROOT = c.noaa_year_path
CDIP_ROOT = c.cdip_path
ERA5_ROOT = c.era5_path
NOAA_EXTS = ("txt", "data_spec", "swdir", "swdir2", "swr1", "swr2")


def log(msg):
    print(f"[reingest {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def discover_noaa():
    """Station-years with all six NDBC files present."""
    out = []
    for d in sorted(glob.glob(os.path.join(NOAA_ROOT, "*"))):
        station = os.path.basename(d)
        years = {m.group(1) for f in glob.glob(os.path.join(d, "*"))
                 if (m := re.match(r"(\d{4})_year_", os.path.basename(f)))}
        for year in sorted(years):
            have = [e for e in NOAA_EXTS
                    if os.path.exists(os.path.join(d, f"{year}_year_{station}.{e}"))]
            if len(have) == len(NOAA_EXTS):
                out.append(("noaa-year", station, year, None))
            else:
                log(f"  skipping NOAA {station} {year}: only {len(have)}/6 files")
    return out


def discover_cdip():
    """CDIP deployments, newest station first for a stable order."""
    out = []
    for f in sorted(glob.glob(os.path.join(CDIP_ROOT, "*", "*.nc"))):
        station = os.path.basename(os.path.dirname(f))
        m = re.search(r"_d(\d+)\.nc$", f)
        if m:
            out.append(("cdip", station, None, int(m.group(1))))
    return out


def era5_gaps(station, deployment):
    """Months a CDIP deployment needs that are not already cached locally.

    Any gap means a Copernicus API request, which can queue for a long time --
    worth knowing before starting an unattended run.
    """
    try:
        import pandas as pd
        import xarray as xr
        path = os.path.join(CDIP_ROOT, station, f"cdip_{station}_d{deployment:02d}.nc")
        ds = xr.open_dataset(path)
        t = pd.to_datetime(ds["waveTime"].values, unit="s")
        ds.close()
        # fetch_from_cdip asks for one month PAST the end of the deployment, so the
        # range has to match or this reports "OK" for one that still hits the API.
        last = t.max().replace(day=1) + pd.offsets.MonthBegin(1)
        need, cur = set(), t.min().replace(day=1)
        while cur <= last:
            need.add(cur.strftime("%Y%m"))
            cur = cur + pd.offsets.MonthBegin(1)
        have = {os.path.basename(x)[-9:-3]
                for x in glob.glob(os.path.join(ERA5_ROOT, station, "*.nc"))}
        return sorted(need - have)
    except Exception:
        return []


def clear_tables(params, assume_yes):
    target = f"{params['host']}:{params['port']}/{params['dbname']}"
    conn = psycopg2.connect(**params, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("select count(*) from dirspec.time_steps")
        n = cur.fetchone()[0]
    print(f"\nAbout to TRUNCATE dirspec.buoys, buoy_deployments, time_steps,")
    print(f"spectra_parameters and storms.storm_matches on {target}.")
    print(f"That is {n:,} timesteps. storms.storms and storms.storm_tracks are kept.")
    if not assume_yes:
        if input(f"Type the database name ({params['dbname']}) to confirm: ").strip() != params["dbname"]:
            sys.exit("aborted")
    with conn.cursor() as cur:
        cur.execute("truncate dirspec.time_steps, dirspec.buoy_deployments, "
                    "dirspec.buoys restart identity cascade")
    conn.commit()
    conn.close()
    log(f"cleared {n:,} timesteps")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clear", action="store_true", help="truncate before ingesting")
    ap.add_argument("--yes", action="store_true", help="skip the --clear confirmation prompt")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    ap.add_argument("--only", choices=["noaa", "cdip"], default=None)
    ap.add_argument("--limit", type=int, default=None,
                    help="cap timesteps per unit (for a quick trial)")
    ap.add_argument("--skip-missing-era5", action="store_true",
                    help="skip CDIP deployments that would need a Copernicus request")
    args = ap.parse_args()

    units = []
    if args.only != "cdip":
        units += discover_noaa()
    if args.only != "noaa":
        units += discover_cdip()

    log(f"found {len(units)} unit(s) in raw storage")
    gaps = {}
    for kind, station, year, dep in units:
        if kind == "cdip":
            g = era5_gaps(station, dep)
            if g:
                gaps[(station, dep)] = g
    for (station, dep), g in gaps.items():
        log(f"  CDIP {station} d{dep:02d} needs {len(g)} ERA5 month(s) from the API: {g}")
    if gaps and args.skip_missing_era5:
        units = [u for u in units if (u[1], u[3]) not in gaps]
        log(f"  --skip-missing-era5: {len(units)} unit(s) remain")

    for kind, station, year, dep in units:
        label = f"{station} {year}" if kind == "noaa-year" else f"{station} d{dep:02d}"
        print(f"    {kind:10s} {label}")

    if args.dry_run:
        log("dry run, nothing touched")
        return 0

    params = db.conn_params()
    if args.clear:
        clear_tables(params, args.yes)

    import fetch_buoy_data as pipeline

    started = time.time()
    done, failed = [], []
    for i, (kind, station, year, dep) in enumerate(units, 1):
        label = f"{kind} {station} {year or ''}{'' if dep is None else f'd{dep:02d}'}".strip()
        log(f"[{i}/{len(units)}] {label} ...")
        t0 = time.time()
        try:
            pipeline.run(file_station_id=[int(station) if kind == "cdip" else station],
                         file_type=[kind],
                         file_date=[year or "n/a"],
                         cdip_deployments=[dep],
                         limit_timesteps=args.limit)
            done.append(label)
            log(f"[{i}/{len(units)}] {label} done in {time.time() - t0:.0f}s")
        except Exception as e:
            failed.append((label, f"{type(e).__name__}: {e}"))
            log(f"[{i}/{len(units)}] {label} FAILED after {time.time() - t0:.0f}s -- "
                f"{type(e).__name__}: {e}")
            traceback.print_exc()
            log("continuing with the next unit; re-run to retry this one")

    mins = (time.time() - started) / 60
    log(f"finished in {mins:.1f} min -- {len(done)} ok, {len(failed)} failed")
    for label, err in failed:
        log(f"  FAILED {label}: {err}")

    conn = psycopg2.connect(**params, connect_timeout=10)
    with conn.cursor() as cur:
        for t in ("dirspec.buoys", "dirspec.buoy_deployments", "dirspec.time_steps",
                  "dirspec.spectra_parameters", "storms.storm_matches"):
            cur.execute(f"select count(*) from {t}")
            log(f"  {t:32s} {cur.fetchone()[0]:>12,}")
    conn.close()

    log("run scripts/validate_spectra.py --sample 5000 to check the result")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
