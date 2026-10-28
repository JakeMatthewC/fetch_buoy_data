"""Physics / consistency checks on ingested directional spectra.

Run after an ingest to confirm the stored spectra are self-consistent and agree
with the buoy's own published bulk parameters:

    python scripts/validate_spectra.py                 # default sample
    python scripts/validate_spectra.py --sample 2000
    python scripts/validate_spectra.py --source cdip

Honours the PGDIRSPEC_* environment variables (see config/db.py), so it can be
pointed at a smoke-test database as easily as at production.

Every check is read-only.
"""
import argparse
import sys

import numpy as np
import psycopg2

sys.path.insert(0, __file__.rsplit("scripts", 1)[0])
import config.db as db  # noqa: E402
import processes.directional as dirn  # noqa: E402

DIRS = np.arange(0, 360, 5)
N_DIR_BINS = 72          # 5-degree bins
DELTA_THETA_RAD = 0.0872664625997165   # radians(5)

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"
results = []


def angdiff(a, b):
    """SQL for the smallest absolute angular difference between two bearings.

    Postgres mod() keeps the sign of the dividend -- mod(-180, 360) is -180, not
    180 -- so the usual `mod(a - b + 180, 360) - 180` silently returns garbage
    whenever a - b is negative (e.g. degrees(atan2(...)) < 0). Adding 540 before
    the second mod puts the value back in range first.
    """
    return f"abs(((((({a}) - ({b}))::numeric %% 360) + 540) %% 360) - 180)"


def check(name, status, detail):
    results.append((status, name, detail))
    colour = {PASS: "  ", FAIL: ">>", WARN: " ?"}[status]
    print(f"{colour} [{status}] {name}\n        {detail}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=500,
                    help="number of ingested timesteps to inspect for the heavy checks")
    ap.add_argument("--source", default=None, help="restrict to one time_steps.source")
    args = ap.parse_args()

    conn = psycopg2.connect(**db.conn_params(), connect_timeout=10)
    conn.set_session(readonly=True)
    cur = conn.cursor()

    src_filter = "and ts.source = %(source)s" if args.source else ""
    p = {"source": args.source, "n": args.sample}

    # ---------- 1. Fourier coefficient ranges, per source ----------
    cur.execute(f"""
        select ts.source, count(*),
               min(sp.r1), max(sp.r1), min(sp.r2), max(sp.r2)
        from dirspec.spectra_parameters sp
        join dirspec.time_steps ts on ts.id = sp.time_step_id
        where sp.r1 is not null {src_filter}
        group by 1 order by 1""", p)
    rows = cur.fetchall()
    if not rows:
        check("r1/r2 in [0,1]", WARN, "no spectra_parameters rows found")
    for source, n, r1lo, r1hi, r2lo, r2hi in rows:
        ok = 0 <= r1lo and r1hi <= 1.0001 and 0 <= r2lo and r2hi <= 1.0001
        check(f"r1/r2 in [0,1]  [{source}]", PASS if ok else FAIL,
              f"n={n:,}  r1 {r1lo:.3f}..{r1hi:.3f}   r2 {r2lo:.3f}..{r2hi:.3f}"
              + ("" if ok else "   <- NDBC scales r by 100 in historical files"))

    # ---------- 2. alpha ranges ----------
    cur.execute(f"""
        select ts.source, min(sp.alpha1), max(sp.alpha1), min(sp.alpha2), max(sp.alpha2)
        from dirspec.spectra_parameters sp
        join dirspec.time_steps ts on ts.id = sp.time_step_id
        where sp.alpha1 is not null {src_filter}
        group by 1 order by 1""", p)
    for source, a1lo, a1hi, a2lo, a2hi in cur.fetchall():
        ok = 0 <= a1lo and a1hi < 360 and 0 <= a2lo and a2hi < 360
        check(f"alpha1/alpha2 in [0,360)  [{source}]", PASS if ok else FAIL,
              f"alpha1 {a1lo:.1f}..{a1hi:.1f}   alpha2 {a2lo:.1f}..{a2hi:.1f}")

    # ---------- 3. D(theta) rebuilds cleanly from the stored parameters ----
    #     spectra_directional is gone, so this reconstructs what the viewer will
    #     draw rather than reading a cached copy of it
    cur.execute(f"""
        with s as (select ts.id from dirspec.time_steps ts
                   where ts.spectra_ingested {src_filter} order by ts.id limit %(n)s)
        select sp.alpha1, sp.alpha2, sp.r1, sp.r2, sp.energy_density
        from s join dirspec.spectra_parameters sp on sp.time_step_id = s.id
        where sp.alpha1 is not null and sp.r1 is not null""", p)
    rows = cur.fetchall()
    if not rows:
        check("D(theta) reconstructs from spectra_parameters", WARN, "no parameter rows found")
    else:
        a1 = np.array([r[0] for r in rows], float); a2 = np.array([r[1] for r in rows], float)
        r1 = np.array([r[2] for r in rows], float); r2 = np.array([r[3] for r in rows], float)
        en = np.array([r[4] if r[4] is not None else 0.0 for r in rows], float)
        D = dirn.mem(a1, a2, r1, r2, DIRS)
        finite = np.isfinite(D).all(axis=1)
        integrals = D[finite].sum(axis=1) * DELTA_THETA_RAD
        neg = int((D[finite] < 0).sum())
        worst = float(np.abs(integrals - 1).max()) if finite.any() else float("nan")
        ok = worst < 1e-9 and neg == 0
        check("D(theta) reconstructs: integral 1, no negatives", PASS if ok else FAIL,
              f"{finite.sum():,}/{len(rows):,} bins usable  max|integral-1| = {worst:.2e}  "
              f"negatives = {neg}  (undefined {(~finite).sum():,}, r1 -> 1)")

        # the property the truncated Fourier series lacked
        g1, _, _, _ = dirn.directional_moments(D[finite], DIRS)
        w = en[finite] / en[finite].sum() if en[finite].sum() > 0 else None
        if w is not None:
            err = float(np.sum(w * np.abs(g1 - r1[finite])))
            check("MEM preserves the measured r1", PASS if err < 5e-3 else FAIL,
                  f"energy-weighted |dr1| = {err:.2e}")

    # ---------- 6. stored alpha1 vs the buoy's own published MWD ----------
    #     both are meteorological "direction from"; compared at the peak-energy bin
    cur.execute(f"""
        with s as (select ts.id, ts.mwd, ts.source from dirspec.time_steps ts
                   where ts.spectra_ingested and ts.mwd is not null {src_filter}
                   order by ts.id limit %(n)s),
        pk as (select s.source, s.mwd, sp.alpha1,
                      row_number() over (partition by s.id order by sp.energy_density desc) rn
               from s join dirspec.spectra_parameters sp on sp.time_step_id = s.id
               where sp.alpha1 is not null and sp.energy_density is not null)
        select source, count(*), avg({angdiff('alpha1', 'mwd')})
        from pk where rn = 1 group by 1 order by 1""", p)
    for source, n, err in cur.fetchall():
        err = float(err)
        check(f"alpha1(peak bin) vs time_steps.mwd  [{source}]",
              PASS if err < 15 else FAIL,
              f"n={n:,}  mean|error| = {err:.2f} deg  (met convention; >90 suggests a reflection)")

    # ---------- 7. the two sea-state metrics ----------
    cur.execute(f"""
        select ts.source, count(*),
               count(ts.dir_concentration_r), min(ts.dir_concentration_r), max(ts.dir_concentration_r),
               count(ts.sea_swell_sep_deg), min(ts.sea_swell_sep_deg), max(ts.sea_swell_sep_deg)
        from dirspec.time_steps ts where ts.spectra_ingested {src_filter}
        group by 1 order by 1""", p)
    for source, n, nr, rlo, rhi, ns, slo, shi in cur.fetchall():
        ok = (nr > 0 and ns > 0 and 0 <= (rlo or 0) and (rhi or 0) <= 1.0001
              and 0 <= (slo or 0) and (shi or 0) <= 180.0001)
        check(f"dir_concentration_r / sea_swell_sep_deg  [{source}]", PASS if ok else FAIL,
              f"of {n:,} ingested: r {nr:,} populated, range {rlo}..{rhi}; "
              f"sep {ns:,} populated, range {slo}..{shi}")

    cur.execute(f"""
        select ts.source, ts.modality_boot, count(*)
        from dirspec.time_steps ts where ts.spectra_ingested {src_filter}
        group by 1,2 order by 1,3 desc""", p)
    dist = {}
    for source, label, n in cur.fetchall():
        dist.setdefault(source, []).append((label, n))
    for source, items in dist.items():
        tot = sum(n for _, n in items)
        txt = "  ".join(f"{lab}={100*n/tot:.1f}%" for lab, n in items)
        bim = next((n for lab, n in items if lab == "bimodal"), 0) / max(tot, 1)
        # A label that fires on almost everything carries no information -- the
        # previous single-bin detector sat at 99.8% bimodal. Only judge that on a
        # sample big enough to span changing weather: a few dozen consecutive
        # timesteps are legitimately all one sea state, and flagging those as
        # suspicious would train you to ignore this check.
        if tot < 500:
            check(f"modality_boot distribution  [{source}]", PASS,
                  f"n={tot:,}  {txt}  (sample too short to judge informativeness)")
        else:
            ok = 0.01 < bim < 0.60
            check(f"modality_boot is informative  [{source}]", PASS if ok else WARN,
                  f"n={tot:,}  {txt}"
                  + ("" if ok else "   <- fires on nearly everything or nothing"))

    # ---------- 8. hm0 from the spectrum vs the buoy's published wvht ----------
    cur.execute(f"""
        select ts.source, count(*),
               avg(abs(ts.hm0 - ts.wvht)), corr(ts.hm0, ts.wvht)
        from dirspec.time_steps ts
        where ts.hm0 is not null and ts.wvht is not null {src_filter}
        group by 1 order by 1""", p)
    for source, n, mae, r in cur.fetchall():
        ok = mae is not None and float(mae) < 0.30 and (r or 0) > 0.95
        check(f"hm0 (from spectrum) vs wvht (published)  [{source}]", PASS if ok else WARN,
              f"n={n:,}  mean|diff| = {float(mae):.3f} m  corr = {float(r or 0):.4f}")

    conn.close()

    nf = sum(1 for s, _, _ in results if s == FAIL)
    nw = sum(1 for s, _, _ in results if s == WARN)
    print(f"\n{'=' * 68}\n{len(results)} checks: "
          f"{sum(1 for s, _, _ in results if s == PASS)} pass, {nf} fail, {nw} warn")
    return 1 if nf else 0


if __name__ == "__main__":
    sys.exit(main())
