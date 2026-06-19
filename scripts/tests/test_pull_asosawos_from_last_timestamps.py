"""Unit tests for per-station ASOSAWOS pull orchestration."""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "1_pull_data"))

from pull_asosawos_from_last_timestamps import (  # noqa: E402
    build_missing_year_subgroups,
    build_pull_groups,
    filter_existing_station_year_files,
    isd_id_to_station_id,
    load_timestamp_inventory,
    resolve_s3_target,
    run_pull_groups,
    station_id_to_isd_id,
)


def test_station_id_to_isd_id():
    assert station_id_to_isd_id("ASOSAWOS_72630014733") == "726300-14733"


def test_load_timestamp_inventory_parses_year_and_isd(tmp_path):
    path = tmp_path / "timestamps.csv"
    pd.DataFrame(
        [
            {
                "station_id": "ASOSAWOS_72630014733",
                "last_timestamp": "2020-05-01T00:00:00",
            },
            {"station_id": "ASOSAWOS_72251099999", "last_timestamp": "not-a-date"},
        ]
    ).to_csv(path, index=False)

    out = load_timestamp_inventory(str(path))

    assert out["station_id"].tolist() == ["ASOSAWOS_72630014733"]
    assert out["isd_id"].tolist() == ["726300-14733"]
    assert out["start_year"].tolist() == [2020]


def test_build_pull_groups_matches_and_buckets():
    inventory_df = pd.DataFrame(
        {
            "station_id": ["ASOSAWOS_A", "ASOSAWOS_B", "ASOSAWOS_C"],
            "last_timestamp": pd.to_datetime(
                ["2021-06-01", "2019-01-01", "2022-01-01"]
            ),
            "isd_id": ["111111-11111", "222222-22222", "333333-33333"],
            "start_year": [2021, 2019, 2022],
        }
    )
    station_df = pd.DataFrame(
        {
            "ISD-ID": ["111111-11111", "333333-33333"],
            "end_time": ["2099-12-31", "2099-12-31"],
        }
    )

    groups, stats, offline_df, unknown_end_df = build_pull_groups(
        inventory_df,
        station_df,
        pull_cutoff=date(2026, 5, 4),
        end_year=2022,
    )

    assert stats == {
        "inventory_rows": 3,
        "matched_rows": 2,
        "unmatched_rows": 1,
        "offline_skipped_rows": 0,
        "unknown_end_time_rows": 0,
        "year_groups": 2,
    }
    assert len(offline_df) == 0
    assert len(unknown_end_df) == 0
    assert sorted(groups.keys()) == [2021, 2022]
    assert groups[2021]["ISD-ID"].tolist() == ["111111-11111"]
    assert groups[2022]["ISD-ID"].tolist() == ["333333-33333"]


def test_build_pull_groups_applies_year_floor():
    inventory_df = pd.DataFrame(
        {
            "station_id": ["ASOSAWOS_A"],
            "last_timestamp": pd.to_datetime(["1970-01-01"]),
            "isd_id": ["111111-11111"],
            "start_year": [1970],
        }
    )
    station_df = pd.DataFrame(
        {
            "ISD-ID": ["111111-11111"],
            "end_time": ["2099-12-31"],
        }
    )

    groups, _, _, _ = build_pull_groups(
        inventory_df,
        station_df,
        pull_cutoff=date(2026, 5, 4),
        start_year_floor=1980,
    )
    assert sorted(groups.keys()) == [1980]


def test_build_pull_groups_skips_offline_by_default():
    inventory_df = pd.DataFrame(
        {
            "station_id": ["ASOSAWOS_A", "ASOSAWOS_B"],
            "last_timestamp": pd.to_datetime(["2020-01-01", "2022-08-31"]),
            "isd_id": ["111111-11111", "222222-22222"],
            "start_year": [2020, 2022],
        }
    )
    station_df = pd.DataFrame(
        {
            "ISD-ID": ["111111-11111", "222222-22222"],
            "end_time": ["2020-12-31", "2099-12-31"],
        }
    )

    groups, stats, offline_df, _ = build_pull_groups(
        inventory_df,
        station_df,
        pull_cutoff=date(2026, 5, 4),
    )

    assert stats["offline_skipped_rows"] == 1
    assert len(offline_df) == 1
    assert groups[2022]["ISD-ID"].tolist() == ["222222-22222"]


def test_build_pull_groups_include_offline_override():
    inventory_df = pd.DataFrame(
        {
            "station_id": ["ASOSAWOS_A", "ASOSAWOS_B"],
            "last_timestamp": pd.to_datetime(["2020-01-01", "2022-08-31"]),
            "isd_id": ["111111-11111", "222222-22222"],
            "start_year": [2020, 2022],
        }
    )
    station_df = pd.DataFrame(
        {
            "ISD-ID": ["111111-11111", "222222-22222"],
            "end_time": ["2020-12-31", "2099-12-31"],
        }
    )

    groups, stats, offline_df, _ = build_pull_groups(
        inventory_df,
        station_df,
        pull_cutoff=date(2026, 5, 4),
        include_offline=True,
    )

    assert stats["offline_skipped_rows"] == 0
    assert len(offline_df) == 1
    assert groups[2020]["ISD-ID"].tolist() == ["111111-11111"]
    assert groups[2022]["ISD-ID"].tolist() == ["222222-22222"]


def test_run_pull_groups_invokes_pull_with_year_bounds(monkeypatch):
    calls = []

    def fake_get_asosawos_data_ftp(stations, directory, start_date, end_date, get_all):
        calls.append(
            {
                "n": len(stations),
                "directory": directory,
                "start_date": start_date,
                "end_date": end_date,
                "get_all": get_all,
            }
        )

    monkeypatch.setattr(
        "pull_asosawos_from_last_timestamps.get_asosawos_data_ftp",
        fake_get_asosawos_data_ftp,
    )

    groups = {
        2020: pd.DataFrame({"ISD-ID": ["111111-11111"]}),
        2022: pd.DataFrame({"ISD-ID": ["333333-33333", "444444-44444"]}),
    }

    run_pull_groups(
        groups,
        directory="1_raw_wx/ASOSAWOS/",
        end_date="2026-05-04",
        skip_existing=False,
        backend="isd",
    )

    assert calls == [
        {
            "n": 1,
            "directory": "1_raw_wx/ASOSAWOS/",
            "start_date": "2020-01-01",
            "end_date": "2026-05-04",
            "get_all": True,
        },
        {
            "n": 2,
            "directory": "1_raw_wx/ASOSAWOS/",
            "start_date": "2022-01-01",
            "end_date": "2026-05-04",
            "get_all": True,
        },
    ]


def test_run_pull_groups_dry_run_does_not_invoke_pull(monkeypatch):
    def fake_get_asosawos_data_ftp(*args, **kwargs):
        raise AssertionError("pull function should not be called in dry run")

    monkeypatch.setattr(
        "pull_asosawos_from_last_timestamps.get_asosawos_data_ftp",
        fake_get_asosawos_data_ftp,
    )

    run_pull_groups(
        groups={2020: pd.DataFrame({"ISD-ID": ["111111-11111"]})},
        directory="1_raw_wx/ASOSAWOS/",
        end_date="2026-05-04",
        dry_run=True,
        skip_existing=False,
    )


def test_filter_existing_station_year_files_skips_present_files():
    stations = pd.DataFrame(
        {
            "ISD-ID": ["111111-11111", "222222-22222", "333333-33333"],
        }
    )

    filtered, skipped = filter_existing_station_year_files(
        stations=stations,
        start_year=2022,
        end_year=2023,
        existing_filenames={
            "111111-11111-2022.gz",
            "111111-11111-2023.gz",
            "222222-22222-2022.gz",
            # Missing 222222-22222-2023.gz means this station is still incomplete.
            "333333-33333-2022.gz",
            "333333-33333-2023.gz",
        },
    )

    assert skipped == 2
    assert filtered["ISD-ID"].tolist() == ["222222-22222"]


def test_build_missing_year_subgroups_only_returns_missing_years():
    stations = pd.DataFrame(
        {
            "ISD-ID": ["111111-11111", "222222-22222"],
        }
    )

    groups, fully_present = build_missing_year_subgroups(
        stations=stations,
        start_year=2022,
        end_year=2024,
        existing_filenames={
            "111111-11111-2022.gz",
            "111111-11111-2023.gz",
            "111111-11111-2024.gz",
            "222222-22222-2022.gz",
            # 2023 missing for second station
            "222222-22222-2024.gz",
        },
    )

    assert fully_present == 1
    assert sorted(groups.keys()) == [2023]
    assert groups[2023]["ISD-ID"].tolist() == ["222222-22222"]


def test_build_missing_year_subgroups_force_years_always_included():
    stations = pd.DataFrame(
        {
            "ISD-ID": ["111111-11111", "222222-22222"],
        }
    )

    groups, fully_present = build_missing_year_subgroups(
        stations=stations,
        start_year=2025,
        end_year=2026,
        existing_filenames={
            "111111-11111-2025.gz",
            "111111-11111-2026.gz",
            "222222-22222-2025.gz",
            "222222-22222-2026.gz",
        },
        force_years={2026},
    )

    assert fully_present == 0
    assert sorted(groups.keys()) == [2026]
    assert sorted(groups[2026]["ISD-ID"].tolist()) == [
        "111111-11111",
        "222222-22222",
    ]


def test_resolve_s3_target_supports_prefix_and_full_uri(monkeypatch):
    monkeypatch.setattr("pull_asosawos_from_last_timestamps.BUCKET_NAME", "my-bucket")

    bucket, prefix = resolve_s3_target("1_raw_wx/ASOSAWOS/")
    assert bucket == "my-bucket"
    assert prefix == "1_raw_wx/ASOSAWOS"

    bucket, prefix = resolve_s3_target("s3://other-bucket/custom/prefix/")
    assert bucket == "other-bucket"
    assert prefix == "custom/prefix"


# ---------------------------------------------------------------------------
# New: isd_id_to_station_id + GHCNh routing
# ---------------------------------------------------------------------------


def test_isd_id_to_station_id():
    assert isd_id_to_station_id("726300-14733") == "ASOSAWOS_72630014733"


def test_isd_id_to_station_id_roundtrip():
    hdp_id = "ASOSAWOS_72630014733"
    assert isd_id_to_station_id(station_id_to_isd_id(hdp_id)) == hdp_id


def test_run_pull_groups_ghcnh_calls_pull_ghcnh_station_years(monkeypatch):
    """GHCNh backend should call pull_ghcnh_station_years, not get_asosawos_data_ftp."""
    from unittest.mock import MagicMock, patch

    station_df = pd.DataFrame(
        {
            "ISD-ID": ["726300-14733", "722500-12345"],
            "end_time": ["2099-12-31", "2099-12-31"],
        }
    )
    groups = {2024: station_df}

    ghcnh_mock = MagicMock(
        return_value={"fetched": 2, "skipped_existing": 0, "not_found": 0, "errors": []}
    )
    ftp_mock = MagicMock()

    with (
        patch(
            "pull_asosawos_from_last_timestamps.pull_ghcnh_station_years", ghcnh_mock
        ),
        patch("pull_asosawos_from_last_timestamps.get_asosawos_data_ftp", ftp_mock),
    ):
        run_pull_groups(
            groups=groups,
            directory="1_raw_wx/ASOSAWOS/",
            end_date="2026-06-01",
            dry_run=False,
            skip_existing=True,
            backend="ghcnh",
        )

    assert ghcnh_mock.called
    assert not ftp_mock.called
    call_kwargs = ghcnh_mock.call_args
    station_ids_called = call_kwargs.kwargs.get("station_ids") or call_kwargs.args[0]
    assert "ASOSAWOS_72630014733" in station_ids_called
    assert "ASOSAWOS_72250012345" in station_ids_called


def test_run_pull_groups_isd_calls_ftp(monkeypatch):
    """ISD backend should call get_asosawos_data_ftp, not pull_ghcnh_station_years."""
    from unittest.mock import MagicMock, patch

    station_df = pd.DataFrame({"ISD-ID": ["726300-14733"], "end_time": ["2099-12-31"]})
    groups = {2024: station_df}

    ftp_mock = MagicMock()
    ghcnh_mock = MagicMock()

    with (
        patch("pull_asosawos_from_last_timestamps.get_asosawos_data_ftp", ftp_mock),
        patch(
            "pull_asosawos_from_last_timestamps.pull_ghcnh_station_years", ghcnh_mock
        ),
        patch(
            "pull_asosawos_from_last_timestamps.list_existing_raw_filenames",
            return_value=set(),
        ),
        patch(
            "pull_asosawos_from_last_timestamps.build_missing_year_subgroups",
            return_value=({2024: station_df}, 0),
        ),
    ):
        run_pull_groups(
            groups=groups,
            directory="1_raw_wx/ASOSAWOS/",
            end_date="2026-06-01",
            dry_run=False,
            skip_existing=True,
            backend="isd",
        )

    assert ftp_mock.called
    assert not ghcnh_mock.called
