"""Unit tests for ASOSAWOS last-timestamp discovery utility."""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "misc"))

from discover_last_timestamps_asosawos import (  # noqa: E402
    build_inventory_rows,
    list_baseline_station_ids,
    load_station_ids,
    read_baseline_last_timestamp,
)


def test_load_station_ids_filters_network(monkeypatch):
    stations_df = pd.DataFrame(
        {
            "network": ["ASOSAWOS", "OTHER", "ASOSAWOS"],
            "era-id": ["ASOSAWOS_1", "OTHER_1", "ASOSAWOS_2"],
        }
    )

    monkeypatch.setattr(pd, "read_csv", lambda *args, **kwargs: stations_df)

    assert load_station_ids("unused.csv") == ["ASOSAWOS_1", "ASOSAWOS_2"]


class _FakePaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kwargs):
        return self.pages


class _FakeS3Client:
    def __init__(self, pages):
        self.pages = pages

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self.pages)


def test_list_baseline_station_ids_extracts_station_names():
    pages = [
        {
            "Contents": [
                {"Key": "hdp/ASOSAWOS/ASOSAWOS_111.zarr/.zattrs"},
                {"Key": "hdp/ASOSAWOS/ASOSAWOS_222.zarr/0.0"},
                {"Key": "hdp/ASOSAWOS/README.txt"},
            ]
        }
    ]

    station_ids = list_baseline_station_ids(
        bucket="auto-hdp",
        publish_prefix="hdp",
        s3_client=_FakeS3Client(pages),
    )

    assert station_ids == {"ASOSAWOS_111", "ASOSAWOS_222"}


def _make_baseline_zarr(tmp_path, times):
    ds = xr.Dataset(
        {"tas": ("time", np.ones(len(times)))},
        coords={"time": times},
    )
    zarr_path = str(tmp_path / "station.zarr")
    ds.to_zarr(zarr_path)
    return zarr_path


def test_read_baseline_last_timestamp_returns_last_timestamp(tmp_path, monkeypatch):
    times = pd.date_range("2020-01-01", periods=5, freq="h")
    zarr_path = _make_baseline_zarr(tmp_path, times)

    real_open_zarr = xr.open_zarr

    def fake_open_zarr(url, *args, **kwargs):
        return real_open_zarr(zarr_path)

    monkeypatch.setattr(xr, "open_zarr", fake_open_zarr)

    result, status, detail = read_baseline_last_timestamp("ASOSAWOS_72630014733")

    assert result == times[-1].to_pydatetime()
    assert status == "ok"
    assert detail == ""


def test_read_baseline_last_timestamp_missing_zarr(monkeypatch):
    def fake_open_zarr(url, *args, **kwargs):
        raise FileNotFoundError("no zarr here")

    monkeypatch.setattr(xr, "open_zarr", fake_open_zarr)

    result, status, detail = read_baseline_last_timestamp("ASOSAWOS_72630014733")

    assert result is None
    assert status == "missing"
    assert detail == "zarr not found"


def test_read_baseline_last_timestamp_empty_zarr(tmp_path, monkeypatch):
    ds = xr.Dataset(
        {"tas": ("time", np.array([]))},
        coords={"time": pd.DatetimeIndex([])},
    )
    zarr_path = str(tmp_path / "empty.zarr")
    ds.to_zarr(zarr_path)

    real_open_zarr = xr.open_zarr

    def fake_open_zarr(url, *args, **kwargs):
        return real_open_zarr(zarr_path)

    monkeypatch.setattr(xr, "open_zarr", fake_open_zarr)

    result, status, detail = read_baseline_last_timestamp("ASOSAWOS_72630014733")

    assert result is None
    assert status == "empty"
    assert detail == "missing or empty time coordinate"


def test_build_inventory_rows_splits_success_and_missing(monkeypatch):
    def fake_reader(station_id, bucket, publish_prefix, network):
        if station_id == "ASOSAWOS_111":
            return datetime(2020, 1, 1, 12, 0, 0), "ok", ""
        if station_id == "ASOSAWOS_222":
            return None, "empty", "missing or empty time coordinate"
        raise AssertionError(f"unexpected station {station_id}")

    monkeypatch.setattr(
        "discover_last_timestamps_asosawos.read_baseline_last_timestamp", fake_reader
    )

    success_rows, problem_rows = build_inventory_rows(
        station_ids=["ASOSAWOS_111", "ASOSAWOS_222", "ASOSAWOS_333"],
        baseline_station_ids={"ASOSAWOS_111", "ASOSAWOS_222"},
        bucket="auto-hdp",
        publish_prefix="hdp",
    )

    assert success_rows == [
        {
            "station_id": "ASOSAWOS_111",
            "last_timestamp": "2020-01-01T12:00:00",
        }
    ]
    assert problem_rows == [
        {
            "station_id": "ASOSAWOS_222",
            "status": "empty",
            "detail": "missing or empty time coordinate",
        },
        {
            "station_id": "ASOSAWOS_333",
            "status": "missing",
            "detail": "baseline zarr absent from publish bucket",
        },
    ]
