"""
Unit tests for merge append helpers in MERGE_pipeline.py.
"""

import os
import sys

import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "4_merge_data"))

from MERGE_pipeline import merge_existing_and_new_slice


def _make_ds(times, tas_values):
    return xr.Dataset(
        {"tas": ("time", np.array(tas_values, dtype=float))},
        coords={"time": pd.to_datetime(times)},
    )


def test_merge_existing_and_new_slice_dedups_overlap_keep_last():
    existing = _make_ds(
        ["2024-01-01T00:00:00", "2024-01-01T01:00:00", "2024-01-01T02:00:00"],
        [10.0, 11.0, 12.0],
    )
    new_slice = _make_ds(
        ["2024-01-01T02:00:00", "2024-01-01T03:00:00"],
        [99.0, 13.0],
    )

    merged = merge_existing_and_new_slice(existing, new_slice)

    assert merged.sizes["time"] == 4
    assert list(merged["time"].values) == list(
        pd.to_datetime(
            [
                "2024-01-01T00:00:00",
                "2024-01-01T01:00:00",
                "2024-01-01T02:00:00",
                "2024-01-01T03:00:00",
            ]
        ).values
    )
    # Overlapping timestamp takes value from new_slice (keep='last')
    assert merged["tas"].sel(time=pd.Timestamp("2024-01-01T02:00:00")).item() == 99.0


def test_merge_existing_and_new_slice_sorts_time_after_concat():
    existing = _make_ds(["2024-01-01T01:00:00", "2024-01-01T00:00:00"], [11.0, 10.0])
    new_slice = _make_ds(["2024-01-01T03:00:00", "2024-01-01T02:00:00"], [13.0, 12.0])

    merged = merge_existing_and_new_slice(existing, new_slice)

    assert list(merged["time"].values) == list(
        pd.to_datetime(
            [
                "2024-01-01T00:00:00",
                "2024-01-01T01:00:00",
                "2024-01-01T02:00:00",
                "2024-01-01T03:00:00",
            ]
        ).values
    )


def test_merge_existing_and_new_slice_preserves_non_overlapping_existing_data():
    existing = _make_ds(["2024-01-01T00:00:00", "2024-01-01T01:00:00"], [5.0, 6.0])
    new_slice = _make_ds(["2024-01-01T02:00:00"], [7.0])

    merged = merge_existing_and_new_slice(existing, new_slice)

    assert merged.sizes["time"] == 3
    assert merged["tas"].sel(time=pd.Timestamp("2024-01-01T00:00:00")).item() == 5.0
    assert merged["tas"].sel(time=pd.Timestamp("2024-01-01T01:00:00")).item() == 6.0
    assert merged["tas"].sel(time=pd.Timestamp("2024-01-01T02:00:00")).item() == 7.0
