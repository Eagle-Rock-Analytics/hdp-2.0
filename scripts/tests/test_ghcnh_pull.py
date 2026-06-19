"""Unit tests for GHCNh_pull.py."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "1_pull_data"))

from GHCNh_pull import (
    _fetch_parquet_bytes,
    _object_exists,
    ghcnh_parquet_url,
    hdp_to_ghcnh_id,
    pull_ghcnh_station_years,
    s3_key,
)

# ---------------------------------------------------------------------------
# hdp_to_ghcnh_id
# ---------------------------------------------------------------------------


def test_hdp_to_ghcnh_id_sfo():
    assert hdp_to_ghcnh_id("ASOSAWOS_72630014733") == "USW00014733"


def test_hdp_to_ghcnh_id_small_wban():
    # WBAN 00005 -> USW00000005
    assert hdp_to_ghcnh_id("ASOSAWOS_72630000005") == "USW00000005"


def test_hdp_to_ghcnh_id_missing_prefix():
    with pytest.raises(ValueError, match="ASOSAWOS_"):
        hdp_to_ghcnh_id("72630014733")


def test_hdp_to_ghcnh_id_wrong_length():
    with pytest.raises(ValueError, match="11-digit"):
        hdp_to_ghcnh_id("ASOSAWOS_123")


# ---------------------------------------------------------------------------
# ghcnh_parquet_url / s3_key
# ---------------------------------------------------------------------------


def test_ghcnh_parquet_url():
    url = ghcnh_parquet_url("USW00014733", 2024)
    assert "by-year/2024/parquet/GHCNh_USW00014733_2024.parquet" in url
    assert url.startswith("https://")


def test_s3_key_structure():
    key = s3_key("ASOSAWOS_72630014733", "USW00014733", 2024, "1_raw_wx")
    assert (
        key == "1_raw_wx/ASOSAWOS/ASOSAWOS_72630014733/GHCNh_USW00014733_2024.parquet"
    )


# ---------------------------------------------------------------------------
# _fetch_parquet_bytes
# ---------------------------------------------------------------------------


def _mock_session(status_code: int, content: bytes = b"") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        resp.raise_for_status.return_value = None
    session = MagicMock()
    session.get.return_value = resp
    return session


def test_fetch_parquet_bytes_success():
    session = _mock_session(200, b"PAR1fakeparquetbytes")
    result = _fetch_parquet_bytes("http://example.com/test.parquet", session)
    assert result == b"PAR1fakeparquetbytes"


def test_fetch_parquet_bytes_404_returns_none():
    session = _mock_session(404)
    result = _fetch_parquet_bytes("http://example.com/missing.parquet", session)
    assert result is None


# ---------------------------------------------------------------------------
# _object_exists
# ---------------------------------------------------------------------------


def test_object_exists_true():
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 100}
    assert _object_exists(s3, "my-bucket", "some/key.parquet") is True


def test_object_exists_false():
    from botocore.exceptions import ClientError

    s3 = MagicMock()
    s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )
    assert _object_exists(s3, "my-bucket", "missing/key.parquet") is False


# ---------------------------------------------------------------------------
# pull_ghcnh_station_years — dry run
# ---------------------------------------------------------------------------


def test_pull_dry_run_no_s3_calls(capsys):
    with patch("GHCNh_pull.boto3") as mock_boto3:
        summary = pull_ghcnh_station_years(
            station_ids=["ASOSAWOS_72630014733"],
            start_year=2024,
            end_year=2024,
            bucket="test-bucket",
            dry_run=True,
        )
    # No S3 client methods called in dry run
    mock_boto3.client.return_value.put_object.assert_not_called()
    assert summary["fetched"] == 0
    captured = capsys.readouterr()
    assert "DRY RUN" in captured.out


def test_pull_invalid_station_id_logged():
    with patch("GHCNh_pull.boto3"):
        summary = pull_ghcnh_station_years(
            station_ids=["BAD_STATION_ID"],
            start_year=2024,
            end_year=2024,
            bucket="test-bucket",
        )
    assert len(summary["errors"]) == 1
    assert "BAD_STATION_ID" in summary["errors"][0]["station_id"]
    assert summary["fetched"] == 0


def test_pull_404_increments_not_found():
    with (
        patch("GHCNh_pull.boto3") as mock_boto3,
        patch("GHCNh_pull._fetch_parquet_bytes", return_value=None),
        patch("GHCNh_pull._object_exists", return_value=False),
    ):
        summary = pull_ghcnh_station_years(
            station_ids=["ASOSAWOS_72630014733"],
            start_year=2024,
            end_year=2024,
            bucket="test-bucket",
        )
    assert summary["not_found"] == 1
    assert summary["fetched"] == 0
    mock_boto3.client.return_value.put_object.assert_not_called()


def test_pull_success_fetches_and_uploads():
    fake_parquet = b"PAR1" + b"\x00" * 100
    with (
        patch("GHCNh_pull.boto3") as mock_boto3,
        patch("GHCNh_pull._fetch_parquet_bytes", return_value=fake_parquet),
        patch("GHCNh_pull._object_exists", return_value=False),
    ):
        summary = pull_ghcnh_station_years(
            station_ids=["ASOSAWOS_72630014733"],
            start_year=2024,
            end_year=2025,
            bucket="test-bucket",
        )
    assert summary["fetched"] == 2
    assert summary["errors"] == []
    assert mock_boto3.client.return_value.put_object.call_count == 2


def test_pull_skips_existing():
    with (
        patch("GHCNh_pull.boto3"),
        patch("GHCNh_pull._object_exists", return_value=True),
        patch("GHCNh_pull._fetch_parquet_bytes") as mock_fetch,
    ):
        summary = pull_ghcnh_station_years(
            station_ids=["ASOSAWOS_72630014733"],
            start_year=2024,
            end_year=2024,
            bucket="test-bucket",
            skip_existing=True,
        )
    mock_fetch.assert_not_called()
    assert summary["skipped_existing"] == 1
    assert summary["fetched"] == 0


# ---------------------------------------------------------------------------
# CSV station loading (via main argparse path)
# ---------------------------------------------------------------------------


def test_load_stations_from_csv(tmp_path):
    from GHCNh_pull import _load_stations_from_csv

    csv_path = tmp_path / "stations.csv"
    pd.DataFrame(
        {"station_id": ["ASOSAWOS_72630014733", "ASOSAWOS_72251099999"]}
    ).to_csv(csv_path, index=False)
    result = _load_stations_from_csv(str(csv_path))
    assert result == ["ASOSAWOS_72630014733", "ASOSAWOS_72251099999"]


def test_load_stations_from_csv_missing_column(tmp_path):
    from GHCNh_pull import _load_stations_from_csv

    csv_path = tmp_path / "bad.csv"
    pd.DataFrame({"id": ["A", "B"]}).to_csv(csv_path, index=False)
    with pytest.raises(ValueError, match="station_id"):
        _load_stations_from_csv(str(csv_path))
