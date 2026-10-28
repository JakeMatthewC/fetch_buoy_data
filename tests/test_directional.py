"""Tests for the directional spectrum maths.

processes/directional.py is where every correctness-critical decision in this
project lives, and it imports only numpy -- no database, no config -- so it can
be tested directly.

Several tests below are REGRESSION PINS for bugs that were shipped: each one
fails if the specific mistake comes back. They are labelled where that is the
case, because a test that encodes a real defect is worth more than one that
restates the implementation.

    pytest tests/ -v
    pytest tests/test_directional.py -k mirror -v
"""
import numpy as np
import pytest

import processes.directional as dirn

DIRS = np.arange(0, 360, 5)
DTH = np.deg2rad(5.0)


def sea(mean_deg, spread_s=8.0, weight=1.0):
    """A cos^2s directional spreading lobe, the standard unimodal form.

    Used instead of inventing (r1, alpha1, r2, alpha2) by hand. An arbitrary set
    of four numbers is usually NOT a valid set of moments -- no non-negative
    D(theta) produces them -- and MEM correctly returns NaN for those. Building a
    real distribution and taking ITS moments guarantees the inputs are physical,
    which is what a buoy actually reports.
    """
    # the angle difference must be wrapped to [-180, 180] first, or cos() of the
    # half angle goes negative on the far side and the lobe grows a twin
    delta = np.deg2rad(((DIRS - mean_deg + 180) % 360) - 180)
    d = np.cos(delta / 2.0) ** (2 * spread_s)
    return weight * d / (d.sum() * DTH)


def moments_of(D):
    """(alpha1, alpha2, r1, r2) of a distribution, in the pipeline's argument order."""
    D = D / (D.sum() * DTH)
    r1, a1, r2, a2 = dirn.directional_moments(D[None, :], DIRS)
    return float(a1[0]), float(a2[0]), float(r1[0]), float(r2[0])


# --------------------------------------------------------------------------
# The argument for using MEM at all: it returns the moments it was given.
# --------------------------------------------------------------------------
@pytest.mark.parametrize("mean_deg,spread_s", [
    (261.0, 20.0),     # narrow westerly swell
    (98.0, 3.0),       # broad sea
    (2.0, 10.0),       # peaks near due north, straddling the 0/360 wrap
    (169.0, 15.0),     # southerly swell
])
def test_mem_reproduces_its_input_moments(mean_deg, spread_s):
    a1, a2, r1, r2 = moments_of(sea(mean_deg, spread_s))
    D = dirn.mem(a1, a2, r1, r2, DIRS)
    assert np.isfinite(D).all(), "these moments came from a real distribution"
    got_r1, got_a1, got_r2, _ = dirn.directional_moments(D, DIRS)

    assert got_r1[0] == pytest.approx(r1, abs=5e-3)
    assert got_r2[0] == pytest.approx(r2, abs=5e-3)
    # circular difference, so 359 and 1 are 2 degrees apart rather than 358
    assert abs((got_a1[0] - a1 + 180) % 360 - 180) < 1.0


def test_truncated_fourier_cannot_hold_r1():
    """REGRESSION PIN: the reason MEM replaced the two-term series.

    Clipping the negative lobes and renormalising necessarily lowers the first
    moment, so the old method reported a broader sea than the buoy measured --
    about 21 degrees broader on real data. If someone switches the pipeline back,
    this documents what it costs.
    """
    D = dirn.truncated_fourier(261.0, 262.0, 0.95, 0.87, DIRS)
    got_r1, _, _, _ = dirn.directional_moments(D, DIRS)

    assert got_r1[0] < 0.75, "expected the truncated series to lose r1"
    assert got_r1[0] == pytest.approx(0.703, abs=0.01)


# --------------------------------------------------------------------------
# Shape invariants that must hold for anything we call a distribution.
# --------------------------------------------------------------------------
def test_mem_is_a_normalised_distribution():
    rng = np.random.default_rng(0)
    params = [moments_of(sea(rng.uniform(0, 360), rng.uniform(1.5, 25.0)))
              for _ in range(200)]
    a1, a2, r1, r2 = (np.array([p[i] for p in params]) for i in range(4))

    D = dirn.mem(a1, a2, r1, r2, DIRS)
    finite = np.isfinite(D).all(axis=1)

    assert finite.all(), "moments taken from real distributions must all be usable"
    assert (D[finite] >= 0).all(), "a distribution cannot be negative"
    np.testing.assert_allclose(D[finite].sum(axis=1) * DTH, 1.0, atol=1e-9)


def test_mem_is_undefined_as_r1_approaches_one():
    """MEM divides by 1 - |c1|^2. At r1 = 1 that collapses.

    The pipeline stores NULL for these bins rather than a fabricated number, so
    NaN is the contract, not a bug.
    """
    D = dirn.mem(1.0, 0.0, 1.0, 0.5, DIRS)
    assert not np.isfinite(D).all()


# --------------------------------------------------------------------------
# Direction convention. This is the bug that shipped for the longest.
# --------------------------------------------------------------------------
@pytest.mark.parametrize("alpha1", [0.0, 45.0, 90.0, 180.0, 270.0, 315.0])
def test_reconstruction_points_at_alpha1_not_its_mirror(alpha1):
    """REGRESSION PIN: the mirrored spectrum.

    alpha and the theta grid are both meteorological, so a sea from alpha1 must
    come back pointing at alpha1. Converting alpha to the mathematical convention
    while leaving theta as raw bearings reflects D about the 135/315 axis, which
    sent waves-from-north to waves-from-west. Under that bug the reconstruction
    would point at (270 - alpha1) % 360, asserted against explicitly below.

    Tested on the circular MEAN rather than the peak, because MEM can split a
    unimodal sea into two symmetric lobes -- see the note in directional.mem --
    and then the peak is not at the centre even though the answer is right.
    """
    a1, a2, r1, r2 = moments_of(sea(alpha1, spread_s=12.0))
    D = dirn.mem(a1, a2, r1, r2, DIRS)
    assert np.isfinite(D).all(), "moments from a real distribution must be usable"

    _, got_a1, _, _ = dirn.directional_moments(D, DIRS)
    got = float(got_a1[0])

    assert abs((got - alpha1 + 180) % 360 - 180) <= 2
    mirrored = (270 - alpha1) % 360
    if abs((mirrored - alpha1 + 180) % 360 - 180) > 10:      # skip the fixed points
        assert abs((got - mirrored + 180) % 360 - 180) > 10


def test_alpha2_is_a_half_angle_so_the_series_raises_a_mirror_lobe():
    """cos(2(theta - alpha2)) has a 180 degree period.

    The two-term series therefore always produces a second lobe near
    alpha2 + 180 when r2 is large. It is an artefact of the truncation, not a
    second wave system, and MEM does not produce it -- which is what made the
    old modality detector count mathematics as weather.
    """
    def lobes(D):
        n = len(D)
        return sum(1 for k in range(n)
                   if D[k] >= D[(k - 1) % n] and D[k] >= D[(k + 1) % n] and D[k] > 0)

    series = dirn.truncated_fourier(261.0, 262.0, 0.95, 0.87, DIRS)[0]
    mem = dirn.mem(261.0, 262.0, 0.95, 0.87, DIRS)[0]

    # the back lobe sits near alpha2 + 180
    back = int(((262.0 + 180) % 360) // 5)
    assert lobes(series) == 2, "the truncated series should show the mirror lobe"
    assert series[back] > 0.10 * series.max(), "and it should be substantial"
    # MEM suppresses it HERE. It is not a general guarantee: MEM has its own
    # splitting behaviour on other parameter ranges, documented in directional.mem
    assert mem[back] < 0.01 * mem.max(), "MEM should not raise a lobe opposite the swell"


# --------------------------------------------------------------------------
# frequency_concentration: a concentration, emphatically not a separation.
# --------------------------------------------------------------------------
def test_concentration_is_one_when_every_frequency_agrees():
    alpha = np.full(10, 137.0)
    energy = np.ones(10)
    R, mean_dir = dirn.frequency_concentration(alpha, energy)

    assert R == pytest.approx(1.0, abs=1e-9)
    assert mean_dir == pytest.approx(137.0, abs=1e-6)


def test_concentration_collapses_when_directions_oppose():
    alpha = np.array([0.0] * 5 + [180.0] * 5)
    energy = np.ones(10)
    R, _ = dirn.frequency_concentration(alpha, energy)

    assert R == pytest.approx(0.0, abs=1e-9)


def test_concentration_is_energy_weighted_not_a_plain_mean():
    """A bin holding no energy must not drag the mean direction around."""
    alpha = np.array([10.0, 10.0, 200.0, 200.0])
    R_equal, dir_equal = dirn.frequency_concentration(alpha, np.array([1.0, 1.0, 1.0, 1.0]))
    R_lopsided, dir_lopsided = dirn.frequency_concentration(alpha, np.array([1.0, 1.0, 0.001, 0.001]))

    assert R_lopsided > R_equal
    assert dir_lopsided == pytest.approx(10.0, abs=1.0)


def test_the_same_concentration_comes_from_very_different_seas():
    """Documents the degeneracy, so nobody reads R as an angle.

    One broad unimodal sea and two systems roughly 109 degrees apart both give
    R about 0.58. That is why sea_swell_sep_deg is stored alongside it.
    """
    broad = float(np.exp(-np.deg2rad(60.0) ** 2 / 2))          # wrapped normal, sd 60
    two_lobes, _ = dirn.frequency_concentration(
        np.array([0.0, 0.0, 109.0, 109.0]), np.ones(4))

    assert broad == pytest.approx(two_lobes, abs=0.02)


def test_concentration_needs_enough_bins():
    R, mean_dir = dirn.frequency_concentration(np.array([10.0, 20.0]), np.array([1.0, 1.0]))
    assert np.isnan(R) and np.isnan(mean_dir)


# --------------------------------------------------------------------------
# sea_swell_separation
# --------------------------------------------------------------------------
def test_separation_is_small_for_one_system():
    freqs = np.linspace(0.05, 0.30, 12)
    alpha = np.full(12, 200.0)
    energy = np.exp(-((freqs - 0.12) ** 2) / 0.002)            # single peak
    assert dirn.sea_swell_separation(alpha, energy, freqs) == pytest.approx(0.0, abs=1.0)


def test_separation_finds_two_opposed_systems():
    """Swell below the peak from the south, wind sea above it from the north."""
    freqs = np.linspace(0.05, 0.30, 12)
    alpha = np.where(freqs < 0.15, 170.0, 350.0)
    # the peak must fall inside the band, not on the first bin, or there are no
    # frequencies below it to average
    energy = np.exp(-((freqs - 0.11) ** 2) / 0.0008) + 0.6 * np.exp(-((freqs - 0.22) ** 2) / 0.0015)
    assert dirn.sea_swell_separation(alpha, energy, freqs) == pytest.approx(180.0, abs=5.0)


def test_separation_needs_bins_on_both_sides_of_the_peak():
    freqs = np.linspace(0.05, 0.30, 12)
    energy = np.zeros(12)
    energy[0] = 1.0                                             # peak at the lowest bin
    assert np.isnan(dirn.sea_swell_separation(np.full(12, 90.0), energy, freqs))


# --------------------------------------------------------------------------
# modality_label
# --------------------------------------------------------------------------
@pytest.mark.parametrize("R,sep,expected", [
    (0.60, 120.0, "bimodal"),      # unfocused and widely separated
    (0.95, 5.0, "unimodal"),       # focused, one system
    (0.60, 20.0, "unimodal"),      # unfocused but not separated -> scattered, not two systems
    (0.95, 120.0, "unimodal"),     # separated but still focused -> one dominant system
    (0.84, 46.0, "bimodal"),       # just inside both thresholds
    (0.86, 46.0, "unimodal"),      # just outside r
    (0.84, 44.0, "unimodal"),      # just outside sep
])
def test_modality_thresholds(R, sep, expected):
    assert dirn.modality_label(R, sep) == expected


@pytest.mark.parametrize("R,sep", [(None, 40.0), (0.5, None), (np.nan, 40.0), (0.5, np.nan)])
def test_modality_is_undetermined_without_both_metrics(R, sep):
    assert dirn.modality_label(R, sep) == "undetermined"
