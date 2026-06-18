"""
test_asosawos_clean_append.py

Pure-function unit tests for the append-mode helpers added to ASOSAWOS_clean.py.
No AWS / moto required: all tests operate on in-memory data.

To run: pytest scripts/tests/test_asosawos_clean_append.py
"""

import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "2_clean_data"))
)

from ASOSAWOS_clean import (
    _baseline_last_time,
    _filter_rows_after,
    _station_name_to_isd_id,
    _year_files_after,
    _year_from_filename,
)

# ---------------------------------------------------------------------------
# _year_from_filename
# ---------------------------------------------------------------------------


def test_year_from_filename_standard():
    """Extracts year from a typical ISD S3 key."""
    key = "1_raw_wx/ASOSAWOS/726300-14733-2020.gz"
    assert _year_from_filename(key) == 2020


def test_year_from_filename_different_year():
    assert _year_from_filename("1_raw_wx/ASOSAWOS/726300-14733-1999.gz") == 1999


def test_year_from_filename_invalid_returns_none():
    assert _year_from_filename("1_raw_wx/ASOSAWOS/stationlist.csv") is None


def test_year_from_filename_non_numeric_year():
    assert _year_from_filename("1_raw_wx/ASOSAWOS/726300-14733-abcd.gz") is None


# ---------------------------------------------------------------------------
# _year_files_after
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_subfiles():
    return [
        "1_raw_wx/ASOSAWOS/726300-14733-2018.gz",
        "1_raw_wx/ASOSAWOS/726300-14733-2019.gz",
        "1_raw_wx/ASOSAWOS/726300-14733-2020.gz",
        "1_raw_wx/ASOSAWOS/726300-14733-2021.gz",
        "1_raw_wx/ASOSAWOS/726300-14733-2022.gz",
    ]


def test_year_files_after_mid_range(sample_subfiles):
    """Keeps files for year(T) and later."""
    T = datetime(2020, 6, 15)
    result = _year_files_after(sample_subfiles, T)
    years = [_year_from_filename(f) for f in result]
    assert years == [2020, 2021, 2022]


def test_year_files_after_includes_boundary_year(sample_subfiles):
    """year(T) itself is included even if T is in the middle of the year."""
    T = datetime(2021, 12, 31, 23, 59)
    result = _year_files_after(sample_subfiles, T)
    years = [_year_from_filename(f) for f in result]
    assert 2021 in years


def test_year_files_after_all_old_returns_empty(sample_subfiles):
    """Returns empty list when T is after all available years."""
    T = datetime(2030, 1, 1)
    assert _year_files_after(sample_subfiles, T) == []


def test_year_files_after_all_new_returns_all(sample_subfiles):
    """Returns all files when T is before the earliest year."""
    T = datetime(2000, 1, 1)
    assert len(_year_files_after(sample_subfiles, T)) == len(sample_subfiles)


# ---------------------------------------------------------------------------
# _filter_rows_after
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df():
    times = [
        datetime(2020, 1, 1),
        datetime(2020, 6, 1),
        datetime(2021, 1, 1),
        datetime(2021, 6, 1),
        datetime(2022, 1, 1),
    ]
    return pd.DataFrame({"time": times, "tas": [270.0, 271.0, 272.0, 273.0, 274.0]})


def test_filter_rows_after_basic(sample_df):
    """Keeps only rows strictly after T."""
    T = datetime(2021, 1, 1)
    result = _filter_rows_after(sample_df, T)
    assert all(result["time"] > T)
    assert len(result) == 2  # 2021-06-01 and 2022-01-01


def test_filter_rows_after_excludes_exact_boundary(sample_df):
    """The boundary timestamp itself is excluded (strictly after)."""
    T = datetime(2021, 6, 1)
    result = _filter_rows_after(sample_df, T)
    assert datetime(2021, 6, 1) not in result["time"].values


def test_filter_rows_after_all_old_returns_empty(sample_df):
    """Returns empty DataFrame when all rows are before T."""
    T = datetime(2030, 1, 1)
    result = _filter_rows_after(sample_df, T)
    assert result.empty


def test_filter_rows_after_all_new_returns_all(sample_df):
    """Returns all rows when T is before every record."""
    T = datetime(2000, 1, 1)
    result = _filter_rows_after(sample_df, T)
    assert len(result) == len(sample_df)


# ---------------------------------------------------------------------------
# _station_name_to_isd_id
# ---------------------------------------------------------------------------


def test_station_name_to_isd_id_standard():
    assert _station_name_to_isd_id("ASOSAWOS_72630014733") == "726300-14733"


def test_station_name_to_isd_id_different_station():
    assert _station_name_to_isd_id("ASOSAWOS_72251099999") == "722510-99999"


# ---------------------------------------------------------------------------
# _baseline_last_time — monkeypatched (no real S3 call)
# ---------------------------------------------------------------------------


def _make_baseline_zarr(tmp_path, times):
    """Write a minimal zarr with a time dimension to tmp_path."""
    ds = xr.Dataset(
        {"tas": ("time", np.ones(len(times)))},
        coords={"time": times},
    )
    zarr_path = str(tmp_path / "station.zarr")
    ds.to_zarr(zarr_path)
    return zarr_path


def test_baseline_last_time_returns_last_timestamp(tmp_path, monkeypatch):
    """Returns the final time coordinate from a valid zarr."""
    times = pd.date_range("2020-01-01", periods=5, freq="h")
    zarr_path = _make_baseline_zarr(tmp_path, times)

    # Patch the xr module object directly — same reference used by ASOSAWOS_clean.
    _real_open_zarr = xr.open_zarr

    def fake_open_zarr(url, *args, **kwargs):
        return _real_open_zarr(zarr_path)

    monkeypatch.setattr(xr, "open_zarr", fake_open_zarr)

    result = _baseline_last_time("ASOSAWOS_72630014733")
    assert result == times[-1].to_pydatetime()


def test_baseline_last_time_missing_zarr_returns_none(monkeypatch):
    """Returns None when the zarr does not exist (triggers full-record clean)."""

    def fake_open_zarr(url, *args, **kwargs):
        raise FileNotFoundError("no zarr here")

    monkeypatch.setattr(xr, "open_zarr", fake_open_zarr)

    result = _baseline_last_time("ASOSAWOS_72630014733")
    assert result is None


def test_baseline_last_time_empty_zarr_returns_none(tmp_path, monkeypatch):
    """Returns None when the zarr has an empty time dimension."""
    ds = xr.Dataset(
        {"tas": ("time", np.array([]))},
        coords={"time": pd.DatetimeIndex([])},
    )
    zarr_path = str(tmp_path / "empty.zarr")
    ds.to_zarr(zarr_path)

    _real_open_zarr = xr.open_zarr

    def fake_open_zarr(url, *args, **kwargs):
        return _real_open_zarr(zarr_path)

    monkeypatch.setattr(xr, "open_zarr", fake_open_zarr)

    result = _baseline_last_time("ASOSAWOS_72630014733")
    assert result is None
