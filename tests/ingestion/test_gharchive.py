import gzip
import json
import urllib.error
from datetime import date

import pytest
from pyspark.sql import SparkSession

from dev_trends.ingestion.gharchive import (
    build_url,
    download_hour,
    download_range,
    read_bronze,
)


def test_build_url_format() -> None:
    assert build_url(date(2024, 1, 15), 10) == "https://data.gharchive.org/2024-01-15-10.json.gz"


def test_build_url_hour_no_zero_padding() -> None:
    assert build_url(date(2024, 1, 15), 0) == "https://data.gharchive.org/2024-01-15-0.json.gz"


def test_download_hour_success(mocker, tmp_path) -> None:
    mock_resp = mocker.MagicMock()
    mock_resp.__enter__.return_value = mock_resp  # with ... as response → response es mock_resp
    mock_resp.read.return_value = b"fake gz content"
    mocker.patch("urllib.request.urlopen", return_value=mock_resp)

    dest = tmp_path / "test.json.gz"
    result = download_hour("https://data.gharchive.org/2024-01-15-10.json.gz", dest)

    assert result is True
    assert dest.read_bytes() == b"fake gz content"


def test_download_hour_sets_user_agent(mocker, tmp_path) -> None:
    mock_resp = mocker.MagicMock()
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.read.return_value = b"gz"
    mock_urlopen = mocker.patch("urllib.request.urlopen", return_value=mock_resp)

    download_hour("https://data.gharchive.org/2024-01-15-10.json.gz", tmp_path / "f.json.gz")

    request = mock_urlopen.call_args.args[0]
    # GH Archive (Fastly) rechaza el UA por defecto de urllib con 403.
    assert "dev-trends" in request.get_header("User-agent")


def test_download_hour_404_returns_false(mocker, tmp_path) -> None:
    mocker.patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.HTTPError(url="", code=404, msg="Not Found", hdrs=None, fp=None),
    )
    dest = tmp_path / "missing.json.gz"
    result = download_hour("https://data.gharchive.org/2024-01-15-10.json.gz", dest)

    assert result is False
    assert not dest.exists()


def test_download_hour_reraises_non_404(mocker, tmp_path) -> None:
    mocker.patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.HTTPError(
            url="", code=500, msg="Server Error", hdrs=None, fp=None
        ),
    )
    with pytest.raises(urllib.error.HTTPError):
        download_hour("https://data.gharchive.org/2024-01-15-10.json.gz", tmp_path / "f.json.gz")


def test_download_range_returns_only_available_hours(mocker, tmp_path) -> None:
    mocker.patch(
        "dev_trends.ingestion.gharchive.download_hour",
        side_effect=[True, False, True],
    )
    paths = download_range(date(2024, 1, 15), tmp_path, hours=range(3))
    assert len(paths) == 2


def test_download_range_creates_dest_dir(mocker, tmp_path) -> None:
    mocker.patch("dev_trends.ingestion.gharchive.download_hour", return_value=False)
    new_dir = tmp_path / "bronze" / "2024-01-15"
    download_range(date(2024, 1, 15), new_dir, hours=range(1))
    assert new_dir.exists()


def test_read_bronze_counts_events(spark: SparkSession, tmp_path) -> None:
    events = [
        {
            "id": "1",
            "type": "PushEvent",
            "repo": {"name": "apache/airflow"},
            "created_at": "2024-01-15T10:00:00Z",
        },
        {
            "id": "2",
            "type": "WatchEvent",
            "repo": {"name": "dbt-labs/dbt-core"},
            "created_at": "2024-01-15T10:00:01Z",
        },
    ]
    gz_path = tmp_path / "2024-01-15-10.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    result = read_bronze(spark, [gz_path])
    assert result.count() == 2
