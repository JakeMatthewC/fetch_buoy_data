"""Tests for the parsing and geometry helpers in processes/utils.py.

These only became testable once config stopped opening a database connection at
import time -- utils imports config, so importing utils used to require Postgres.
See config/config.py.

Every test here is a REGRESSION PIN for a defect that reached production data.
"""
import numpy as np
import pandas as pd
import pytest

import processes.utils as u


# --------------------------------------------------------------------------
# NDBC fill values. The bug: '99' was passed to read_csv(na_values=...), which
# blanked 2,502 legitimate swdir bearings of 99 degrees and 57 legitimate r
# values of 0.99 in a single station-year.
# --------------------------------------------------------------------------
def test_stdmet_sentinels_blank_only_the_right_columns():
    df = pd.DataFrame({
        "WDIR": [99, 999, 180],       # 99 deg is a real bearing; 999 is the fill
        "MWD": [99, 999, 45],         # same
        "WVHT": [99.0, 1.25, 0.4],    # here 99.00 IS the fill
        "DPD": [99.0, 8.3, 7.1],
        "PRES": [9999.0, 1013.2, 1009.0],
        "WTMP": [999.0, 21.4, 22.0],
    })
    out = u.mask_stdmet_sentinels(df.copy())

    # bearings of 99 degrees survive -- this is the whole point
    assert out["WDIR"].iloc[0] == 99
    assert out["MWD"].iloc[0] == 99

    # the documented fill values are blanked
    assert pd.isna(out["WDIR"].iloc[1])
    assert pd.isna(out["MWD"].iloc[1])
    assert pd.isna(out["WVHT"].iloc[0])
    assert pd.isna(out["DPD"].iloc[0])
    assert pd.isna(out["PRES"].iloc[0])
    assert pd.isna(out["WTMP"].iloc[0])

    # real measurements are untouched
    assert out["WVHT"].iloc[1] == pytest.approx(1.25)
    assert out["PRES"].iloc[1] == pytest.approx(1013.2)


def test_stdmet_sentinels_ignores_absent_columns():
    df = pd.DataFrame({"WVHT": [99.0, 2.0]})
    out = u.mask_stdmet_sentinels(df.copy())
    assert pd.isna(out["WVHT"].iloc[0]) and out["WVHT"].iloc[1] == 2.0


# --------------------------------------------------------------------------
# Tide request chunking. The bug: `while start < end` produced ZERO chunks when
# a deployment's timesteps all fell on one calendar day, so no tide data was
# requested and get_tidal_data raised UnboundLocalError on an unassigned frame.
# --------------------------------------------------------------------------
def test_single_day_span_still_produces_a_chunk():
    chunks = u.date_chunks("20240101", "20240101")
    assert chunks == [("20240101", "20240101")]


def test_short_span_is_one_chunk():
    assert u.date_chunks("20240101", "20240103") == [("20240101", "20240103")]


def test_long_span_is_split_into_month_sized_chunks():
    chunks = u.date_chunks("20240101", "20240331", days=30)
    assert len(chunks) == 3
    assert chunks[0][0] == "20240101"
    assert chunks[-1][1] == "20240331"
    # chunks must not overlap: each starts after the previous one ends
    for (_, end), (start, _) in zip(chunks, chunks[1:]):
        assert pd.to_datetime(start) > pd.to_datetime(end)


# --------------------------------------------------------------------------
# Direction conversion. Not used when building D any more -- that is done on the
# meteorological grid directly -- but still exported, so its contract is pinned.
# --------------------------------------------------------------------------
@pytest.mark.parametrize("bearing", [0.0, 45.0, 90.0, 135.0, 180.0, 270.0, 359.0])
def test_met_math_conversion_round_trips(bearing):
    assert u.math_to_met_dir(u.met_to_math_dir(bearing)) == pytest.approx(bearing, abs=1e-9)


def test_met_to_math_is_a_reflection_not_a_rotation():
    """Why converting alpha without converting theta mirrors the spectrum.

    A rotation preserves the gap between two bearings. This map does not: it
    reverses the sign, which is what reflected D about the 135/315 axis.
    """
    gap_before = 40.0
    a = np.degrees(u.met_to_math_dir(10.0))
    b = np.degrees(u.met_to_math_dir(10.0 + gap_before))
    assert (b - a) % 360 == pytest.approx(360 - gap_before)


# --------------------------------------------------------------------------
# Geometry used for nearest-tide-station and storm distance.
# --------------------------------------------------------------------------
def test_haversine_matches_a_known_distance():
    # one degree of latitude at the equator is about 111.19 km
    assert u.haversine(0.0, 0.0, 1.0, 0.0) == pytest.approx(111.19, abs=0.5)


def test_haversine_is_zero_for_the_same_point():
    assert u.haversine(29.1, -85.0, 29.1, -85.0) == pytest.approx(0.0, abs=1e-9)


def test_haversine_is_symmetric():
    a = u.haversine(29.1, -85.0, 25.9, -89.7)
    b = u.haversine(25.9, -89.7, 29.1, -85.0)
    assert a == pytest.approx(b, abs=1e-9)


def test_safe_val_maps_missing_to_none():
    assert u.safe_val(np.nan) is None
    assert u.safe_val(None) is None
    assert u.safe_val(3.5) == 3.5
    assert u.safe_val(0.0) == 0.0          # zero is a value, not missing
