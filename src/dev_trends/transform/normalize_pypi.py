from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PYPI_SILVER_COLUMNS: list[str] = [
    "download_date",
    "technology",
    "pypi_package",
    "download_count",
    "year",
    "month",
    "day",
]


def attach_pypi_technology(df: DataFrame, mapping: DataFrame) -> DataFrame:
    """Etiqueta cada paquete con su tecnología (el inner join descarta paquetes desconocidos)."""
    return df.join(F.broadcast(mapping), on="pypi_package", how="inner")


def add_download_date_partitions(df: DataFrame) -> DataFrame:
    """Añade columnas de partición year/month/day a partir de download_date."""
    return (
        df.withColumn("year", F.year(F.col("download_date")))
        .withColumn("month", F.month(F.col("download_date")))
        .withColumn("day", F.dayofmonth(F.col("download_date")))
    )


def normalize_downloads(df: DataFrame, mapping: DataFrame) -> DataFrame:
    """Agregado crudo de BigQuery → esquema Silver de descargas PyPI.

    Args:
        df: DataFrame [download_date, pypi_package, download_count].
        mapping: DataFrame [pypi_package, technology] de build_pypi_package_mapping().

    Returns:
        DataFrame con el esquema PYPI_SILVER_COLUMNS.
    """
    return (
        df.transform(attach_pypi_technology, mapping)
        .transform(add_download_date_partitions)
        .select(PYPI_SILVER_COLUMNS)
    )
