"""
test_qaqc_append.py

Pure-function unit tests for hdp-b1d.3: QAQC --append mode helpers.
No moto / AWS calls required.
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "3_qaqc_data"))

from QAQC_pipeline import _build_fit_context, _transfer_climatological_flags

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_df(times, station="TEST_STN", tas_vals=None, tas_eraqc=None):
    """Build a minimal pipeline-format DataFrame with time / station / tas / tas_eraqc."""
    n = len(times)
    df = pd.DataFrame(
        {
            "time": pd.to_datetime(times),
            "station": station,
            "tas": tas_vals if tas_vals is not None else np.ones(n),
            "tas_eraqc": tas_eraqc if tas_eraqc is not None else [np.nan] * n,
            "hour": [t.hour for t in pd.to_datetime(times)],
            "month": [t.month for t in pd.to_datetime(times)],
            "day": [t.day for t in pd.to_datetime(times)],
            "year": [t.year for t in pd.to_datetime(times)],
        }
    )
    return df


# ---------------------------------------------------------------------------
# _build_fit_context tests
# ---------------------------------------------------------------------------


class TestBuildFitContext:
    def test_no_overlap_concat_and_sort(self):
        """History and slice that don't overlap are concatenated and sorted by time."""
        hist_times = ["2020-01-01", "2020-06-01", "2021-01-01"]
        slice_times = ["2022-01-01", "2022-06-01"]
        hist = _make_df(hist_times)
        slc = _make_df(slice_times)

        combined = _build_fit_context(hist, slc)

        assert len(combined) == 5
        assert list(combined["time"]) == sorted(combined["time"].tolist())

    def test_overlapping_timestamps_use_slice_row(self):
        """History rows at timestamps present in the slice are dropped; slice rows kept."""
        overlap_time = "2022-01-01"
        hist = _make_df(["2020-01-01", overlap_time], tas_vals=[1.0, 99.0])
        slc = _make_df([overlap_time, "2022-06-01"], tas_vals=[2.0, 3.0])

        combined = _build_fit_context(hist, slc)

        # Only one row for overlap_time — must be from slice (tas=2.0)
        overlap_rows = combined[combined["time"] == pd.Timestamp(overlap_time)]
        assert len(overlap_rows) == 1
        assert overlap_rows["tas"].values[0] == 2.0

    def test_full_overlap_gives_only_slice(self):
        """When all history timestamps are in the slice, result equals the slice."""
        times = ["2022-01-01", "2022-06-01"]
        hist = _make_df(times, tas_vals=[99.0, 99.0])
        slc = _make_df(times, tas_vals=[1.0, 2.0])

        combined = _build_fit_context(hist, slc)

        assert len(combined) == 2
        assert list(combined["tas"]) == [1.0, 2.0]

    def test_result_has_no_duplicate_times(self):
        """Result must never contain duplicate time values."""
        times = ["2020-01-01", "2021-01-01", "2022-01-01"]
        hist = _make_df(times)
        slc = _make_df(["2021-01-01", "2022-01-01", "2023-01-01"])

        combined = _build_fit_context(hist, slc)

        assert combined["time"].nunique() == len(combined)

    def test_result_sorted_by_time(self):
        """Result is always sorted ascending by time regardless of input order."""
        hist = _make_df(["2021-01-01", "2019-01-01"])
        slc = _make_df(["2020-01-01", "2022-01-01"])

        combined = _build_fit_context(hist, slc)

        times = combined["time"].tolist()
        assert times == sorted(times)


# ---------------------------------------------------------------------------
# _transfer_climatological_flags tests
# ---------------------------------------------------------------------------


class TestTransferClimatologicalFlags:
    def test_flag_transferred_for_slice_timestamps(self):
        """Flags set in the combined df for slice timestamps appear in the slice result."""
        slice_times = ["2022-01-01", "2022-06-01"]
        slc = _make_df(slice_times)

        # combined has the same rows but with flags set on the first slice ts
        combined_times = ["2020-01-01", "2022-01-01", "2022-06-01"]
        combined_eraqc = [np.nan, 26.0, np.nan]
        combined = _make_df(combined_times, tas_eraqc=combined_eraqc)

        result = _transfer_climatological_flags(slc, combined)

        assert (
            result.loc[
                result["time"] == pd.Timestamp("2022-01-01"), "tas_eraqc"
            ].values[0]
            == 26.0
        )
        assert pd.isna(
            result.loc[
                result["time"] == pd.Timestamp("2022-06-01"), "tas_eraqc"
            ].values[0]
        )

    def test_history_flags_not_transferred(self):
        """Flags on history-only timestamps are not transferred to the slice."""
        slice_times = ["2022-01-01"]
        slc = _make_df(slice_times)

        combined_times = ["2020-01-01", "2022-01-01"]
        combined_eraqc = [26.0, np.nan]  # flag only on history row
        combined = _make_df(combined_times, tas_eraqc=combined_eraqc)

        result = _transfer_climatological_flags(slc, combined)

        # slice row should still be NaN (no flag in combined for that ts)
        assert pd.isna(
            result.loc[
                result["time"] == pd.Timestamp("2022-01-01"), "tas_eraqc"
            ].values[0]
        )

    def test_existing_slice_flags_preserved_when_combined_nan(self):
        """Pre-existing flags on the slice are not overwritten by NaN from combined."""
        slice_times = ["2022-01-01", "2022-06-01"]
        # slice already has a flag on the second row from an earlier per-row check
        slc = _make_df(slice_times, tas_eraqc=[np.nan, 10.0])

        # combined has NaN for that timestamp (clim check didn't flag it)
        combined_times = ["2020-01-01", "2022-01-01", "2022-06-01"]
        combined_eraqc = [np.nan, np.nan, np.nan]
        combined = _make_df(combined_times, tas_eraqc=combined_eraqc)

        result = _transfer_climatological_flags(slc, combined)

        # pre-existing flag 10.0 must survive
        assert (
            result.loc[
                result["time"] == pd.Timestamp("2022-06-01"), "tas_eraqc"
            ].values[0]
            == 10.0
        )

    def test_missing_eraqc_column_in_combined_handled(self):
        """Columns in combined but absent from slice (or vice-versa) don't cause errors."""
        slice_times = ["2022-01-01"]
        slc = _make_df(slice_times)  # has tas_eraqc

        # combined has an extra column not in slice
        combined = _make_df(["2020-01-01", "2022-01-01"])
        combined["pr_eraqc"] = [np.nan, 22.0]  # extra column absent from slice

        # should not raise; tas_eraqc handled normally, pr_eraqc silently skipped
        result = _transfer_climatological_flags(slc, combined)
        assert "pr_eraqc" not in result.columns

    def test_append_false_no_flag_change(self):
        """_transfer_climatological_flags with identical combined=slice leaves slice unchanged."""
        times = ["2022-01-01", "2022-06-01"]
        slc = _make_df(times, tas_eraqc=[10.0, np.nan])
        combined = _make_df(times, tas_eraqc=[10.0, np.nan])

        result = _transfer_climatological_flags(slc, combined)

        assert result["tas_eraqc"].tolist()[0] == 10.0
        assert pd.isna(result["tas_eraqc"].tolist()[1])
