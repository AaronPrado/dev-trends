from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def _configure_delta(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Configura Delta en el builder.

    En local usa los JARs del caché ivy2 (sin red).
    En CI (caché vacío) descarga desde Maven; ivy2 se guarda entre runs via actions/cache.
    """
    ivy_delta = Path.home() / ".ivy2" / "cache" / "io.delta"
    jars = list(ivy_delta.glob("*/jars/*.jar"))
    if jars:
        return (
            builder.config("spark.jars", ",".join(str(j) for j in jars))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(builder)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local con Delta Lake para tests. scope=session: una sola instancia."""
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("dev-trends-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
    )
    return _configure_delta(builder).getOrCreate()
