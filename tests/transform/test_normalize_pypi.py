from datetime import date

from pyspark.sql import SparkSession

from dev_trends.transform.normalize_pypi import PYPI_SILVER_COLUMNS, normalize_downloads
from dev_trends.transform.technologies import build_pypi_package_mapping


def _raw_downloads(spark: SparkSession):
    return spark.createDataFrame(
        [
            (date(2026, 4, 1), "pyspark", 100),
            (date(2026, 4, 2), "dbt-core", 40),
            (date(2026, 4, 1), "paquete-desconocido", 5),
        ],
        schema=["download_date", "pypi_package", "download_count"],
    )


def test_normalize_downloads_has_silver_schema(spark: SparkSession) -> None:
    out = normalize_downloads(_raw_downloads(spark), build_pypi_package_mapping(spark))
    assert out.columns == PYPI_SILVER_COLUMNS


def test_normalize_downloads_filters_unknown_packages(spark: SparkSession) -> None:
    """El inner join descarta paquetes que no están en el catálogo."""
    out = normalize_downloads(_raw_downloads(spark), build_pypi_package_mapping(spark))
    assert out.count() == 2
    packages = {row.pypi_package for row in out.collect()}
    assert "paquete-desconocido" not in packages


def test_normalize_downloads_labels_technology_and_partitions(spark: SparkSession) -> None:
    out = normalize_downloads(_raw_downloads(spark), build_pypi_package_mapping(spark))
    row = out.filter(out.pypi_package == "pyspark").collect()[0]
    assert row.technology == "spark"
    assert (row.year, row.month, row.day) == (2026, 4, 1)
    assert row.download_count == 100
