"""Directional distribution D(theta) from the first two Fourier pairs.

Deliberately dependency-free (numpy only, no config, no database) so the wave
viewer can vendor this file verbatim. The mirrored-spectrum bug happened because
the pipeline and the viewer each carried their own reconstruction and the two
drifted; scripts/check_viewer_parity.py now compares them.

CONVENTION: alpha1/alpha2 and dirs_deg are all meteorological bearings -- degrees
true, clockwise from north, naming the direction the waves come FROM. D is built
on that grid directly and is returned aligned to it, so nothing downstream has to
rotate anything.
"""
import numpy as np

__all__ = ["mem", "truncated_fourier", "directional_moments", "METHODS"]


def _prepare(alpha1_deg, alpha2_deg, r1, r2, dirs_deg):
    a1 = np.deg2rad(np.atleast_1d(np.asarray(alpha1_deg, dtype=float)))
    a2 = np.deg2rad(np.atleast_1d(np.asarray(alpha2_deg, dtype=float)))
    r1 = np.atleast_1d(np.asarray(r1, dtype=float))
    r2 = np.atleast_1d(np.asarray(r2, dtype=float))
    th = np.deg2rad(np.asarray(dirs_deg, dtype=float))
    dth = float(np.deg2rad(float(dirs_deg[1]) - float(dirs_deg[0])))
    return a1, a2, r1, r2, th, dth


def _normalise(D, dth):
    """Clip negatives, then scale so the integral over direction is 1."""
    D = np.where(np.isfinite(D), D, np.nan)
    D = np.maximum(D, 0.0)
    total = D.sum(axis=1, keepdims=True) * dth
    total = np.where(np.isfinite(total) & (total > 0), total, np.nan)
    return D / total


def mem(alpha1_deg, alpha2_deg, r1, r2, dirs_deg, eps=1e-8):
    """Maximum Entropy Method (Lygre & Krogstad 1986).

    The least-informative distribution consistent with the measured moments, and
    unlike truncated_fourier it actually reproduces them: feed its output back
    through directional_moments() and r1/alpha1/r2/alpha2 come back unchanged.
    The truncated form cannot, because clipping its negative lobes and
    renormalising lowers r1 -- on real NDBC data by enough to overstate the
    directional spread by about 21 degrees.

    Closed form, so it costs the same as the truncated series. Returns NaN for
    bins where 1 - |c1|^2 collapses (r1 -> 1); callers store those as NULL.

    KNOWN LIMITATION -- MEM over-sharpens, and can split a genuinely unimodal sea
    into two lobes while still reproducing its moments exactly. Measured on 29,940
    energy-bearing NDBC bins: MEM renders 28.4% as a single lobe against 14.7% for
    the truncated series, so it is substantially better overall, but it splits
    14.4% of the bins the series shows as unimodal. Do not read lobe COUNT off a
    MEM reconstruction and call it sea-state modality -- that is what
    frequency_concentration() and sea_swell_separation() are for, and neither
    depends on the shape of D.
    """
    a1, a2, r1, r2, th, dth = _prepare(alpha1_deg, alpha2_deg, r1, r2, dirs_deg)

    # alpha2 is the half angle: a2 = r2*cos(2*alpha2), b2 = r2*sin(2*alpha2)
    c1 = r1 * np.exp(1j * a1)
    c2 = r2 * np.exp(2j * a2)

    denom = 1.0 - np.abs(c1) ** 2
    denom = np.where(np.abs(denom) < eps, np.nan, denom)
    # NaN here is the contract, not an anomaly: r1 -> 1 is genuinely degenerate
    # and the caller stores NULL, so do not let numpy warn about it
    with np.errstate(invalid="ignore", divide="ignore"):
        phi1 = (c1 - c2 * np.conj(c1)) / denom
        phi2 = c2 - c1 * phi1

    e1 = np.exp(-1j * th)[None, :]
    e2 = np.exp(-2j * th)[None, :]

    numer = np.real(1.0 - phi1 * np.conj(c1) - phi2 * np.conj(c2))[:, None]
    denom2 = np.abs(1.0 - phi1[:, None] * e1 - phi2[:, None] * e2) ** 2
    denom2 = np.where(denom2 < eps, eps, denom2)

    return _normalise((1.0 / (2 * np.pi)) * numer / denom2, dth)


def truncated_fourier(alpha1_deg, alpha2_deg, r1, r2, dirs_deg):
    """Two-term cosine series -- the method used before MEM.

    Kept for comparison. Note that cos(2(theta - alpha2)) has a 180 degree
    period, so a large r2 always raises a mirror lobe near alpha2+180 that is an
    artefact of the truncation, not a second wave system.
    """
    a1, a2, r1, r2, th, dth = _prepare(alpha1_deg, alpha2_deg, r1, r2, dirs_deg)
    D = (1 / (2 * np.pi)) * (
        1
        + 2 * r1[:, None] * np.cos(th[None, :] - a1[:, None])
        + 2 * r2[:, None] * np.cos(2 * (th[None, :] - a2[:, None]))
    )
    return _normalise(D, dth)


def directional_moments(D, dirs_deg):
    """Recover (r1, alpha1_deg, r2, alpha2_deg) from a distribution.

    The inverse of the reconstruction, used to check that a method preserves the
    measurements it was given.
    """
    th = np.deg2rad(np.asarray(dirs_deg, dtype=float))[None, :]
    dth = float(np.deg2rad(float(dirs_deg[1]) - float(dirs_deg[0])))
    a1 = (D * np.cos(th)).sum(axis=1) * dth
    b1 = (D * np.sin(th)).sum(axis=1) * dth
    a2 = (D * np.cos(2 * th)).sum(axis=1) * dth
    b2 = (D * np.sin(2 * th)).sum(axis=1) * dth
    return (np.hypot(a1, b1),
            np.degrees(np.arctan2(b1, a1)) % 360,
            np.hypot(a2, b2),
            np.degrees(0.5 * np.arctan2(b2, a2)) % 360)


def frequency_concentration(alpha1_deg, energy):
    """Energy-weighted directional concentration of alpha1 ACROSS frequency.

    Returns (R, mean_direction_deg).

    R is the resultant length of the energy-weighted unit vectors, 0 to 1. It
    answers "is this a single-direction wave environment?" -- 1 means every
    frequency arrives from the same bearing, 0 means they cancel out.

    R is NOT a separation: two systems 109 degrees apart, two systems 118 degrees
    apart with a 35/65 energy split, and one broad unimodal sea all give R ~ 0.58.
    Use sea_swell_separation() for the angle between systems.

    The equivalent wrapped-normal spread is sigma = sqrt(-2 ln R) radians, but
    that saturates (two opposed equal systems give R = 0, sigma = infinity) and
    under-reads lopsided pairs, so R is the value worth storing.

    energy: E(f) * bandwidth per bin, NOT energy density.
    """
    a = np.asarray(alpha1_deg, dtype=float)
    e = np.asarray(energy, dtype=float)
    good = np.isfinite(a) & np.isfinite(e) & (e > 0)
    if good.sum() < 3 or e[good].sum() <= 0:
        return np.nan, np.nan
    w = e[good] / e[good].sum()
    th = np.deg2rad(a[good])
    C = float((w * np.cos(th)).sum())
    S = float((w * np.sin(th)).sum())
    return float(np.hypot(C, S)), float(np.degrees(np.arctan2(S, C)) % 360)


def sea_swell_separation(alpha1_deg, energy, freqs):
    """Angle between the mean directions either side of the spectral peak.

    Splits at the peak of E(f)*df, takes the energy-weighted circular mean
    direction of the bins below it and of the bins at or above it, and returns
    the unsigned angle between them, 0 to 180 degrees.

    Splitting at the peak beat the alternatives on 7,031 real timesteps. Against
    directionally focused seas (R > 0.95, almost certainly one system) it reported
    a median 12 degrees and never exceeded 60; a fixed 0.125 Hz boundary
    false-positived 29.8% of the time, a wind-based wave-age split 18.2%, and
    searching for the split that maximises contrast 59.5%. The peak also needs no
    wind speed, which CDIP rows do not reliably have.

    There is a floor of roughly 12 degrees from a single system turning with
    frequency, so a small value does not mean "two systems close together" -- it
    means one system. This cannot tell you WHETHER two systems exist; that is what
    frequency_concentration() is for.
    """
    a = np.asarray(alpha1_deg, dtype=float)
    e = np.asarray(energy, dtype=float)
    f = np.asarray(freqs, dtype=float)
    good = np.isfinite(a) & np.isfinite(e) & np.isfinite(f) & (e > 0)
    if good.sum() < 6:
        return np.nan
    a, e, f = a[good], e[good], f[good]
    f_peak = f[int(np.argmax(e))]
    lo, hi = f < f_peak, f >= f_peak
    if lo.sum() < 2 or hi.sum() < 2:
        return np.nan
    def _mean(mask):
        w = e[mask] / e[mask].sum()
        th = np.deg2rad(a[mask])
        return np.degrees(np.arctan2((w * np.sin(th)).sum(),
                                     (w * np.cos(th)).sum())) % 360
    d_lo, d_hi = _mean(lo), _mean(hi)
    return float(abs(((d_lo - d_hi + 180) % 360) - 180))


# modality_boot thresholds. Chosen on 7,030 real timesteps from NDBC 42055/2024
# and validated against the anemometer: this rule selects 10.3% of timesteps, and
# their swell band sits a median 104.8 degrees off the measured wind against 34.0
# degrees for the rest -- a 70.8 degree gap. Deliberately conservative: when it
# says bimodal, there really are two systems. Both columns are stored, so any
# other threshold can be applied later without recomputing.
MODALITY_R_MAX = 0.85
MODALITY_SEP_MIN_DEG = 45.0


def modality_label(R, sep_deg):
    """'bimodal', 'unimodal', or 'undetermined' from the two stored metrics."""
    if R is None or sep_deg is None or not np.isfinite(R) or not np.isfinite(sep_deg):
        return "undetermined"
    if R < MODALITY_R_MAX and sep_deg > MODALITY_SEP_MIN_DEG:
        return "bimodal"
    return "unimodal"


METHODS = {"mem": mem, "truncated_fourier": truncated_fourier}
