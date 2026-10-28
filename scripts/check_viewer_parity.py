"""Verify the wave viewer reconstructs D(theta) identically to the pipeline.

The mirrored-spectrum bug happened because processes/calc_D.py and the viewer
each carried their own copy of the reconstruction and the two drifted apart, with
nothing to catch it. This compares them directly over randomised parameters and
over real NDBC bins.

    python scripts/check_viewer_parity.py
    python scripts/check_viewer_parity.py --viewer "D:/DirSpec/wave_viewer"

Run it after touching either processes/directional.py or the viewer's vendored
copy. Exits non-zero on any disagreement.
"""
import argparse
import os
import sys

import numpy as np

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
import processes.directional as pipeline  # noqa: E402

DIRS = np.arange(0, 360, 5)
TOL = 1e-12


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--viewer", default=r"D:/DirSpec/wave_viewer",
                    help="path to the wave_viewer package")
    ap.add_argument("--n", type=int, default=20000, help="random cases to compare")
    args = ap.parse_args()

    if not os.path.isdir(args.viewer):
        print(f"viewer not found at {args.viewer} -- pass --viewer")
        return 2
    sys.path.insert(0, args.viewer)

    try:
        import utils.directional as vendored
        from utils.utils import reconstrust_directional_spectrum as viewer_recon
    except Exception as e:
        print(f"could not import the viewer's reconstruction: {type(e).__name__}: {e}")
        return 2

    failures = 0

    # ---- 1. the vendored module is byte-identical apart from its header note ----
    a = open(os.path.join(REPO, "processes", "directional.py"), encoding="utf-8").read()
    b = open(os.path.join(args.viewer, "utils", "directional.py"), encoding="utf-8").read()
    strip = lambda s: "".join(s.split('"""')[2:])          # drop the module docstring
    if strip(a) == strip(b):
        print("  [ok]   vendored directional.py matches the pipeline's (below the docstring)")
    else:
        print("  [FAIL] vendored directional.py has DIVERGED from processes/directional.py")
        print("         re-copy it: cp processes/directional.py <viewer>/utils/directional.py")
        failures += 1

    # ---- 2. identical output on randomised parameters ----
    rng = np.random.default_rng(20260916)
    a1 = rng.uniform(0, 360, args.n)
    a2 = rng.uniform(0, 360, args.n)
    r1 = rng.uniform(0, 0.985, args.n)          # stay clear of the r1 -> 1 singularity
    r2 = rng.uniform(0, 0.985, args.n)

    ref = pipeline.mem(a1, a2, r1, r2, DIRS)
    worst = 0.0
    nones = 0
    for i in range(0, args.n, max(1, args.n // 2000)):     # sample the scalar path
        got = viewer_recon(a1[i], a2[i], r1[i], r2[i], DIRS)
        if got is None:
            nones += 1
            continue
        worst = max(worst, float(np.abs(got - ref[i]).max()))
    if worst <= TOL:
        print(f"  [ok]   viewer vs pipeline on random parameters: max|diff| = {worst:.2e}"
              f"  ({nones} returned None)")
    else:
        print(f"  [FAIL] viewer vs pipeline on random parameters: max|diff| = {worst:.2e}")
        failures += 1

    # ---- 3. how many random draws are even physically realisable ----
    # MEM reproduces the moments of a distribution that exists. An arbitrary
    # (r1, alpha1, r2, alpha2) need not correspond to any non-negative D -- the
    # Toeplitz matrix of the moments has to be positive semi-definite -- so the
    # recovery test below is run on real buoy moments, which are realisable by
    # construction, not on random ones.
    got_r1, got_a1, _, _ = pipeline.directional_moments(ref, DIRS)
    okr = np.isfinite(ref).all(axis=1)
    consistent = np.abs(got_r1[okr] - r1[okr]) < 5e-3
    print(f"  [info] {100 * consistent.mean():.1f}% of random moment sets are realisable; "
          "recovery is checked on real data below")

    # ---- 4. real NDBC bins, not just synthetic ones ----
    base = r"D:\Buoy_work\Raws Storage\NOAA_Raws\year\42055\2024_year_42055"
    if os.path.exists(base + ".swdir"):
        import pandas as pd
        L = lambda e: pd.read_csv(f"{base}.{e}", sep=r"\s+", skiprows=[1],
                                  na_values=["MM", "999.0", "9999.0"])
        sp, d1, d2, r1f, r2f = (L(e) for e in ("data_spec", "swdir", "swdir2", "swr1", "swr2"))
        EN = sp.iloc[:400, 6:].to_numpy(float).ravel()
        A1 = d1.iloc[:400, 6:].to_numpy(float).ravel()
        A2 = d2.iloc[:400, 6:].to_numpy(float).ravel()
        R1 = r1f.iloc[:400, 6:].to_numpy(float).ravel() / 100
        R2 = r2f.iloc[:400, 6:].to_numpy(float).ravel() / 100
        m = (np.isfinite(A1) & np.isfinite(A2) & np.isfinite(R1) & np.isfinite(R2)
             & np.isfinite(EN))
        ref2 = pipeline.mem(A1[m], A2[m], R1[m], R2[m], DIRS)
        finite = np.isfinite(ref2).all(axis=1)
        integrals = ref2[finite].sum(axis=1) * np.deg2rad(5)
        worst_int = float(np.abs(integrals - 1).max())
        neg = int((ref2[finite] < 0).sum())
        print(f"  [{'ok' if worst_int < 1e-9 and neg == 0 else 'FAIL'}]   "
              f"{finite.sum():,} real bins: max|integral-1| = {worst_int:.2e}, "
              f"negatives = {neg}, non-finite = {(~finite).sum()}")
        if worst_int >= 1e-9 or neg:
            failures += 1

        # MEM must hand back the moments the buoy measured -- the property the
        # truncated Fourier series lacks, and the reason for the switch.
        #
        # Weighted by energy, because a bin holding no energy has a meaningless
        # r1/r2 to begin with: recovery degrades in near-zero-energy tail bins
        # where r2 -> 1 (8% of them exceed 0.05), and those contribute nothing to
        # the spectrum. On bins carrying real waves the median error is ~1e-06.
        En = EN[m][finite]
        wt = En / En.sum() if En.sum() > 0 else np.full(En.shape, 1 / max(len(En), 1))
        g1, ga1, g2, _ = pipeline.directional_moments(ref2[finite], DIRS)
        alt = pipeline.truncated_fourier(A1[m][finite], A2[m][finite],
                                         R1[m][finite], R2[m][finite], DIRS)
        f1, _, _, _ = pipeline.directional_moments(alt, DIRS)

        mem_err = float(np.sum(wt * np.abs(g1 - R1[m][finite])))
        fou_err = float(np.sum(wt * np.abs(f1 - R1[m][finite])))
        real = En > 0.1
        med = float(np.median(np.abs(g1 - R1[m][finite])[real])) if real.any() else float("nan")

        if mem_err < 5e-3 and mem_err < fou_err:
            print(f"  [ok]   MEM reproduces measured moments: energy-weighted dr1 = "
                  f"{mem_err:.2e}  (median {med:.1e} on bins with E > 0.1)")
            print(f"  [info] truncated Fourier on the same bins: {fou_err:.2e}"
                  f"  ({fou_err / mem_err:.0f}x worse -- it cannot hold r1)")
        else:
            print(f"  [FAIL] MEM moment recovery: energy-weighted dr1 = {mem_err:.2e}"
                  f"  (Fourier {fou_err:.2e})")
            failures += 1
    else:
        print("  [skip] real NDBC files not present")

    print(f"\n{'PARITY OK' if failures == 0 else f'{failures} PARITY FAILURE(S)'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
