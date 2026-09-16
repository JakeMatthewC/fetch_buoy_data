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

import psycopg2

sys.path.insert(0, __file__.rsplit("scripts", 1)[0])
import config.db as db  # noqa: E402

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

    # ---------- 3. spreading sanity ----------
    cur.execute(f"""
        with s as (select ts.id from dirspec.time_steps ts
                   where ts.spectra_ingested {src_filter} order by ts.id limit %(n)s)
        select count(*) filter (where sd.spreading is null),
               -- Postgres does NOT follow IEEE here: NaN = NaN is TRUE and NaN sorts
               -- above every other value, so `x != x` never detects it. Compare to
               -- the NaN literal instead.
               count(*) filter (where sd.spreading = double precision 'NaN'),
               count(*) filter (where sd.spreading < 0),
               count(*)
        from s join dirspec.spectra_directional sd on sd.time_step_id = s.id""", p)
    nulls, nans, negs, tot = cur.fetchone()
    check("spreading has no NULL / NaN / negative", PASS if (nulls or 0) + (nans or 0) + (negs or 0) == 0 else FAIL,
          f"of {tot:,} rows: {nulls} null, {nans} NaN, {negs} negative")

    # ---------- 4. every (timestep, frequency) has a full 72-bin circle ----------
    cur.execute(f"""
        with s as (select ts.id from dirspec.time_steps ts
                   where ts.spectra_ingested {src_filter} order by ts.id limit %(n)s)
        select count(*) from (
            select sd.time_step_id, sd.frequency, count(*) nb,
                   min(sd.direction) lo, max(sd.direction) hi
            from s join dirspec.spectra_directional sd on sd.time_step_id = s.id
            group by 1,2
        ) g where g.nb != {N_DIR_BINS} or g.lo != 0 or g.hi != 355""", p)
    bad = cur.fetchone()[0]
    check(f"each (timestep, frequency) has {N_DIR_BINS} bins spanning 0..355",
          PASS if bad == 0 else FAIL, f"{bad} malformed (frequency, direction) groups")

    # ---------- 5. D integrates to 1 over theta ----------
    cur.execute(f"""
        with s as (select ts.id from dirspec.time_steps ts
                   where ts.spectra_ingested {src_filter} order by ts.id limit %(n)s),
        i as (select sd.time_step_id, sd.frequency,
                     sum(sd.spreading) * {DELTA_THETA_RAD} as integral
              from s join dirspec.spectra_directional sd on sd.time_step_id = s.id
              group by 1,2)
        select count(*), avg(integral), min(integral), max(integral),
               count(*) filter (where abs(integral - 1) > 0.01)
        from i""", p)
    n, avg, lo, hi, off = cur.fetchone()
    check("integral of D dtheta == 1", PASS if off == 0 else FAIL,
          f"n={n:,} groups  mean={avg:.5f}  range {lo:.5f}..{hi:.5f}  {off} off by >1%")

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

    # ---------- 7. circular mean of stored D vs published MWD ----------
    #     catches a direction column stored in the wrong convention
    cur.execute(f"""
        with s as (select ts.id, ts.mwd, ts.source from dirspec.time_steps ts
                   where ts.spectra_ingested and ts.mwd is not null {src_filter}
                   order by ts.id limit %(n)s),
        pk as (select s.id, s.source, s.mwd, sp.frequency,
                      row_number() over (partition by s.id order by sp.energy_density desc) rn
               from s join dirspec.spectra_parameters sp on sp.time_step_id = s.id
               where sp.energy_density is not null),
        cm as (select pk.source, pk.mwd,
                      degrees(atan2(sum(sd.spreading * sin(radians(sd.direction))),
                                    sum(sd.spreading * cos(radians(sd.direction))))) as mean_dir
               from pk join dirspec.spectra_directional sd
                 on sd.time_step_id = pk.id and sd.frequency = pk.frequency
               where pk.rn = 1 group by pk.id, pk.source, pk.mwd)
        select source, count(*), avg({angdiff('mean_dir', 'mwd')})
        from cm group by 1 order by 1""", p)
    for source, n, err in cur.fetchall():
        err = float(err)
        check(f"circular mean of D vs time_steps.mwd  [{source}]",
              PASS if err < 20 else FAIL,
              f"n={n:,}  mean|error| = {err:.2f} deg  (direction column convention + D shape)")

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

    # ---------- 9. orphaned directional rows ----------
    cur.execute(f"""
        with s as (select ts.id from dirspec.time_steps ts order by ts.id limit %(n)s)
        select count(*) from dirspec.spectra_directional sd
        where sd.time_step_id not in (select id from dirspec.time_steps)
          and sd.time_step_id <= (select max(id) from s)""", p)
    orphans = cur.fetchone()[0]
    check("no orphaned spectra_directional rows", PASS if orphans == 0 else WARN,
          f"{orphans:,} rows reference a missing time_step "
          "(spectra_directional has no FK / ON DELETE CASCADE)")

    # ---------- 10. bandwidth is populated ----------
    cur.execute(f"""
        select ts.source, count(*), count(sp.bandwidth), min(sp.bandwidth), max(sp.bandwidth)
        from dirspec.spectra_parameters sp
        join dirspec.time_steps ts on ts.id = sp.time_step_id
        where 1=1 {src_filter}
        group by 1 order by 1""", p)
    for source, n, nbw, lo, hi in cur.fetchall():
        check(f"bandwidth populated  [{source}]", PASS if nbw == n else FAIL,
              f"{nbw:,}/{n:,} rows  range {lo}..{hi} Hz"
              + ("" if nbw == n else "   <- run scripts/migrate_metadata.sql and re-ingest"))

    # ---------- 11. m0 recomputed from the stored spectrum ----------
    #     only possible because bandwidth is stored; proves spectra_parameters is
    #     self-sufficient for integrating the spectrum
    cur.execute(f"""
        with s as (select ts.id, ts.m0, ts.source from dirspec.time_steps ts
                   where ts.m0 is not null {src_filter} order by ts.id limit %(n)s),
        recomputed as (select s.source, s.m0,
                              sum(sp.energy_density * sp.bandwidth) as m0_from_spectrum
                       from s join dirspec.spectra_parameters sp on sp.time_step_id = s.id
                       where sp.bandwidth is not null and sp.energy_density is not null
                       group by s.id, s.source, s.m0)
        select source, count(*), avg(abs(m0 - m0_from_spectrum)),
               max(abs(m0 - m0_from_spectrum))
        from recomputed group by 1 order by 1""", p)
    rows = cur.fetchall()
    if not rows:
        check("m0 recomputable from spectra_parameters", WARN, "no rows with bandwidth stored yet")
    for source, n, mae, worst in rows:
        ok = worst is not None and float(worst) < 1e-6
        check(f"m0 recomputable from spectra_parameters  [{source}]", PASS if ok else FAIL,
              f"n={n:,}  mean|diff| = {float(mae):.2e}  max|diff| = {float(worst):.2e} m^2")

    # ---------- 12. tide and ENSO actually landed ----------
    cur.execute(f"""
        select ts.source, count(*), count(ts.tide), count(ts.tide_source),
               count(ts.tide_method), count(ts.enso_mei)
        from dirspec.time_steps ts where 1=1 {src_filter}
        group by 1 order by 1""", p)
    for source, n, ntide, nsrc, nmeth, nenso in cur.fetchall():
        ok = ntide > 0 and nsrc > 0 and nmeth > 0 and nenso > 0
        check(f"tide / ENSO populated  [{source}]", PASS if ok else FAIL,
              f"of {n:,} rows: tide {ntide:,}  tide_source {nsrc:,}  "
              f"tide_method {nmeth:,}  enso_mei {nenso:,}")

    conn.close()

    nf = sum(1 for s, _, _ in results if s == FAIL)
    nw = sum(1 for s, _, _ in results if s == WARN)
    print(f"\n{'=' * 68}\n{len(results)} checks: "
          f"{sum(1 for s, _, _ in results if s == PASS)} pass, {nf} fail, {nw} warn")
    return 1 if nf else 0


if __name__ == "__main__":
    sys.exit(main())
