from datetime import date

import pytest

from dev_trends.ingestion.pypi_downloads import all_pypi_packages, build_download_query
from dev_trends.transform.technologies import TECHNOLOGY_PYPI_PACKAGES

# --- Parte pura: no toca google-cloud-bigquery, corre siempre (incluida la CI) ---


def test_build_download_query_targets_public_table() -> None:
    assert "bigquery-public-data.pypi.file_downloads" in build_download_query()


def test_query_filters_raw_timestamp_for_partition_pruning() -> None:
    """El WHERE filtra timestamp SIN envolver (poda de particiones); DATE() solo en el SELECT."""
    sql = build_download_query()
    assert "WHERE timestamp >= TIMESTAMP(@start_date)" in sql
    assert "DATE(timestamp) AS download_date" in sql


def test_query_uses_parameters_not_interpolation() -> None:
    sql = build_download_query()
    for param in ("@start_date", "@end_date", "@packages"):
        assert param in sql


def test_all_pypi_packages_flattens_dedups_and_sorts() -> None:
    expected = sorted({pkg for pkgs in TECHNOLOGY_PYPI_PACKAGES.values() for pkg in pkgs})
    assert all_pypi_packages() == expected


# --- Parte que toca la API: se salta si el extra `pypi` no está instalado (p. ej. CI) ---


def test_estimate_bytes_is_dry_run_and_returns_estimate(mocker) -> None:
    pytest.importorskip("google.cloud.bigquery")
    from dev_trends.ingestion import pypi_downloads

    client = mocker.Mock()
    client.query.return_value = mocker.Mock(total_bytes_processed=123456)

    result = pypi_downloads.estimate_bytes(client, date(2026, 4, 1), date(2026, 7, 1), ["pyspark"])

    assert result == 123456
    assert client.query.call_args.kwargs["job_config"].dry_run is True


def test_run_download_query_enforces_max_bytes_and_maps_rows(mocker) -> None:
    pytest.importorskip("google.cloud.bigquery")
    from dev_trends.ingestion import pypi_downloads

    row = mocker.Mock(download_date=date(2026, 4, 1), pypi_package="pyspark", download_count=7)
    job = mocker.Mock()
    job.result.return_value = [row]
    client = mocker.Mock()
    client.query.return_value = job

    rows = pypi_downloads.run_download_query(
        client, date(2026, 4, 1), date(2026, 7, 1), ["pyspark"], max_bytes=10**9
    )

    assert rows == [(date(2026, 4, 1), "pyspark", 7)]
    config = client.query.call_args.kwargs["job_config"]
    assert config.dry_run is False
    assert config.maximum_bytes_billed == 10**9
