"""Tests for configuration loading and, importantly, what it does NOT do.

The pipeline used to open a Postgres connection as a side effect of importing
config, which meant none of it could be unit tested without a live database.
These pin the fix in place.
"""
import importlib

import pytest


def test_paths_load_without_a_database_or_pandas():
    import config.paths as paths
    for name in ("buoy_data_root", "fetch_data_root", "noaa_year_path",
                 "cdip_path", "era5_path", "tidal_path", "meiv2_path", "wpm_path"):
        value = getattr(paths, name)
        assert isinstance(value, str) and value, f"{name} is empty"


def test_paths_are_derived_from_the_root_not_hardcoded():
    import config.paths as paths
    root = paths.buoy_data_root
    for name in ("noaa_year_path", "cdip_path", "era5_path", "tidal_path", "wpm_path"):
        assert getattr(paths, name).startswith(root), f"{name} escapes BUOY_DATA_ROOT"


def test_missing_environment_variable_fails_loudly(monkeypatch):
    """A missing path should name itself, not fall back to someone else's disk."""
    import config.paths as paths
    monkeypatch.delenv("BUOY_DATA_ROOT", raising=False)
    monkeypatch.setattr(paths, "load_dotenv", lambda *a, **k: None, raising=False)
    with pytest.raises(RuntimeError, match="BUOY_DATA_ROOT"):
        paths._required("BUOY_DATA_ROOT")


def test_importing_config_does_not_open_a_connection():
    """REGRESSION PIN: the import-time side effect that blocked unit testing."""
    import config.config as c
    importlib.reload(c)
    assert c._conn is None, "config opened a database connection at import"


def test_importing_the_pipeline_does_not_open_a_connection():
    import config.config as c
    import processes.utils            # noqa: F401  imports config.config
    import processes.directional      # noqa: F401
    assert c._conn is None


def test_frequency_grid_matches_the_ndbc_layout():
    import config.config as c
    assert len(c.noaa_freqs) == 46
    assert len(c.noaa_bandwidths) == 46
    assert c.noaa_freqs[0] == pytest.approx(0.0325)
    assert c.noaa_freqs[-1] == pytest.approx(0.4850)
    # bins are contiguous: the bandwidths tile the band with no gaps
    span = (c.noaa_freqs[-1] + c.noaa_bandwidths[-1] / 2) - (c.noaa_freqs[0] - c.noaa_bandwidths[0] / 2)
    assert float(c.noaa_bandwidths.sum()) == pytest.approx(span, abs=1e-9)


def test_direction_grid_is_a_full_five_degree_circle():
    import config.config as c
    assert len(c.directional_pnts_deg) == 72
    assert c.directional_pnts_deg[0] == 0
    assert c.directional_pnts_deg[-1] == 355
