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
    """
    a1, a2, r1, r2, th, dth = _prepare(alpha1_deg, alpha2_deg, r1, r2, dirs_deg)

    # alpha2 is the half angle: a2 = r2*cos(2*alpha2), b2 = r2*sin(2*alpha2)
    c1 = r1 * np.exp(1j * a1)
    c2 = r2 * np.exp(2j * a2)

    denom = 1.0 - np.abs(c1) ** 2
    denom = np.where(np.abs(denom) < eps, np.nan, denom)
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


METHODS = {"mem": mem, "truncated_fourier": truncated_fourier}
