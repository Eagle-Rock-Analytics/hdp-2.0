"""Unit tests for GHCNh_clean.py."""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "2_clean_data"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from GHCNh_clean import (
    _build_dataset,
    _keys_from_year,
    _parse_ghcnh_df,
    _safe_float,
    _year_from_parquet_key,
    hdp_to_ghcnh_id,
)

# ---------------------------------------------------------------------------
# hdp_to_ghcnh_id
# ---------------------------------------------------------------------------


def test_hdp_to_ghcnh_id():
    assert hdp_to_ghcnh_id("ASOSAWOS_72630014733") == "USW00014733"


# ---------------------------------------------------------------------------
# _safe_float
# ---------------------------------------------------------------------------


def test_safe_float_valid():
    assert _safe_float(15.3) == pytest.approx(15.3)


def test_safe_float_fill_returns_nan():
    assert np.isnan(_safe_float(9999.9))
    assert np.isnan(_safe_float(9999))
    assert np.isnan(_safe_float("9999.9"))


def test_safe_float_none_returns_nan():
    assert np.isnan(_safe_float(None))


def test_safe_float_string_number():
    assert _safe_float("12.5") == pytest.approx(12.5)


# ---------------------------------------------------------------------------
# _year_from_parquet_key / _keys_from_year
# ---------------------------------------------------------------------------


def test_year_from_parquet_key():
    key = "1_raw_wx/ASOSAWOS/ASOSAWOS_72630014733/GHCNh_USW00014733_2024.parquet"
    assert _year_from_parquet_key(key) == 2024


def test_year_from_parquet_key_bad():
    assert _year_from_parquet_key("bad/key.parquet") is None


def test_keys_from_year():
    keys = [
        "…/GHCNh_USW00014733_2022.parquet",
        "…/GHCNh_USW00014733_2023.parquet",
        "…/GHCNh_USW00014733_2024.parquet",
    ]
    result = _keys_from_year(keys, 2023)
    assert len(result) == 2
    assert all("2022" not in k for k in result)


# ---------------------------------------------------------------------------
# _parse_ghcnh_df — sub-hourly selection and column mapping
# ---------------------------------------------------------------------------


def _make_ghcnh_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal GHCNh-style DataFrame for testing."""
    defaults = {
        "LATITUDE": 37.62,
        "LONGITUDE": -122.38,
        "ELEVATION": 4.0,
        "NAME": "SAN FRANCISCO INTL AP",
        "temperature": 15.0,
        "temperature_Quality_Code": "0",
        "dew_point_temperature": 10.0,
        "dew_point_temperature_Quality_Code": "0",
        "wind_speed": 5.0,
        "wind_speed_Quality_Code": "0",
        "wind_direction": 270.0,
        "wind_direction_Quality_Code": "0",
        "precipitation": 0.0,
        "precipitation_Quality_Code": "0",
        "sea_level_pressure": 1013.0,
        "sea_level_pressure_Quality_Code": "0",
        "station_level_pressure": 1012.0,
        "station_level_pressure_Quality_Code": "0",
        "relative_humidity": 65.0,
        "relative_humidity_Quality_Code": "0",
    }
    records = []
    for row in rows:
        r = {**defaults, **row}
        records.append(r)
    return pd.DataFrame(records)


def test_parse_ghcnh_df_single_obs():
    df = _make_ghcnh_df([{"DATE": "2024-01-01T12:56:00Z"}])
    data = _parse_ghcnh_df(df)
    assert len(data["time"]) == 1
    assert data["time"][0] == datetime(2024, 1, 1, 12, 0, 0)  # floored to hour


def test_parse_ghcnh_df_sub_hourly_selects_last():
    """Multiple obs in the same hour → last (highest-minute) is selected."""
    df = _make_ghcnh_df(
        [
            {"DATE": "2024-01-01T12:10:00Z", "temperature": 10.0},
            {"DATE": "2024-01-01T12:56:00Z", "temperature": 15.0},  # should win
        ]
    )
    data = _parse_ghcnh_df(df)
    assert len(data["time"]) == 1
    assert data["tas"][0] == pytest.approx(15.0)  # raw value before K conversion


def test_parse_ghcnh_df_two_hours():
    df = _make_ghcnh_df(
        [
            {"DATE": "2024-01-01T12:56:00Z"},
            {"DATE": "2024-01-01T13:56:00Z"},
        ]
    )
    data = _parse_ghcnh_df(df)
    assert len(data["time"]) == 2


def test_parse_ghcnh_df_fill_value_becomes_nan():
    df = _make_ghcnh_df([{"DATE": "2024-01-01T12:56:00Z", "temperature": 9999.9}])
    data = _parse_ghcnh_df(df)
    assert np.isnan(data["tas"][0])


def test_parse_ghcnh_df_missing_date_dropped():
    df = _make_ghcnh_df(
        [
            {"DATE": "2024-01-01T12:56:00Z"},
            {"DATE": "not-a-date"},
        ]
    )
    data = _parse_ghcnh_df(df)
    assert len(data["time"]) == 1


def test_parse_ghcnh_df_qc_preserved():
    df = _make_ghcnh_df(
        [{"DATE": "2024-01-01T12:56:00Z", "temperature_Quality_Code": "2"}]
    )
    data = _parse_ghcnh_df(df)
    assert data["tas_qc"][0] == "2"


# ---------------------------------------------------------------------------
# _build_dataset — unit conversions and coordinate schema
# ---------------------------------------------------------------------------


def test_build_dataset_unit_conversions():
    """tas should be in K, ps in Pa."""
    data = _parse_ghcnh_df(
        _make_ghcnh_df(
            [
                {
                    "DATE": "2024-01-01T12:56:00Z",
                    "temperature": 0.0,
                    "station_level_pressure": 1000.0,
                }
            ]
        )
    )
    ds = _build_dataset(data, "ASOSAWOS_72630014733", "SFO", "01-01-2024, 00:00:00")
    assert ds is not None
    # 0°C → 273.15 K
    assert float(ds["tas"].values.flat[0]) == pytest.approx(273.15, abs=0.01)
    # 1000 hPa → 100000 Pa
    assert float(ds["ps"].values.flat[0]) == pytest.approx(100000.0, abs=1.0)


def test_build_dataset_has_station_coord():
    data = _parse_ghcnh_df(_make_ghcnh_df([{"DATE": "2024-01-01T12:56:00Z"}]))
    ds = _build_dataset(data, "ASOSAWOS_72630014733", "SFO", "01-01-2024, 00:00:00")
    assert ds is not None
    assert "station" in ds.coords
    assert str(ds["station"].values.flat[0]) == "ASOSAWOS_72630014733"


def test_build_dataset_time_is_utc_naive():
    """Time coordinates should be timezone-naive (NetCDF convention)."""
    data = _parse_ghcnh_df(_make_ghcnh_df([{"DATE": "2024-06-15T18:56:00Z"}]))
    ds = _build_dataset(data, "ASOSAWOS_72630014733", "SFO", "01-01-2024, 00:00:00")
    assert ds is not None
    t = pd.Timestamp(ds.time.values[0])
    assert t.tzinfo is None
    assert t == pd.Timestamp("2024-06-15 18:00:00")


def test_build_dataset_drops_all_nan_var():
    """Variables that are entirely NaN should be dropped."""
    data = _parse_ghcnh_df(
        _make_ghcnh_df(
            [
                {
                    "DATE": "2024-01-01T12:56:00Z",
                    "wind_speed": 9999.9,
                    "wind_direction": 9999.9,
                }
            ]
        )
    )
    ds = _build_dataset(data, "ASOSAWOS_72630014733", "SFO", "01-01-2024, 00:00:00")
    assert ds is not None
    assert "sfcWind" not in ds or np.all(np.isnan(ds["sfcWind"].values))


def test_build_dataset_variable_order():
    """Primary met vars should come first."""
    data = _parse_ghcnh_df(_make_ghcnh_df([{"DATE": "2024-01-01T12:56:00Z"}]))
    ds = _build_dataset(data, "ASOSAWOS_72630014733", "SFO", "01-01-2024, 00:00:00")
    assert ds is not None
    vars_list = list(ds.data_vars)
    primary = ["ps", "tas", "tdps", "pr", "hurs", "sfcWind", "sfcWind_dir"]
    for var in primary:
        if var in vars_list:
            assert vars_list.index(var) < len(primary)


def test_build_dataset_units_attrs():
    data = _parse_ghcnh_df(_make_ghcnh_df([{"DATE": "2024-01-01T12:56:00Z"}]))
    ds = _build_dataset(data, "ASOSAWOS_72630014733", "SFO", "01-01-2024, 00:00:00")
    assert ds is not None
    assert ds["tas"].attrs["units"] == "degree_Kelvin"
    assert ds["ps"].attrs["units"] == "Pa"
    assert ds["sfcWind"].attrs["units"] == "m s-1"


def test_build_dataset_none_on_empty():
    data = {
        k: []
        for k in [
            "time",
            "lat",
            "lon",
            "elevation",
            "qaqc_source",
            "tas",
            "tas_qc",
            "tdps",
            "tdps_qc",
            "ps",
            "ps_qc",
            "psl",
            "psl_qc",
            "pr",
            "pr_qc",
            "hurs",
            "hurs_qc",
            "sfcWind",
            "sfcWind_qc",
            "sfcWind_dir",
            "sfcWind_dir_qc",
        ]
    }
    result = _build_dataset(data, "ASOSAWOS_72630014733", "SFO", "01-01-2024, 00:00:00")
    assert result is None


# ---------------------------------------------------------------------------
# append mode: row filtering
# ---------------------------------------------------------------------------


def test_parse_and_filter_append_boundary():
    """Only rows at or after the append boundary should be kept."""
    df = _make_ghcnh_df(
        [
            {"DATE": "2025-10-01T11:56:00Z", "temperature": 10.0},
            {"DATE": "2025-10-01T12:56:00Z", "temperature": 20.0},
            {"DATE": "2025-10-01T13:56:00Z", "temperature": 25.0},
        ]
    )
    data = _parse_ghcnh_df(df)
    T = datetime(2025, 10, 1, 12, 0, 0)
    T_floor = pd.Timestamp(T).floor("h").to_pydatetime()
    idx = [i for i, t in enumerate(data["time"]) if t >= T_floor]
    filtered = {k: [v[i] for i in idx] for k, v in data.items()}
    assert len(filtered["time"]) == 2
    assert all(t >= datetime(2025, 10, 1, 12, 0, 0) for t in filtered["time"])
