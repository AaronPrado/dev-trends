from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def _delta_jars() -> str:
    """Localiza los JARs de Delta en la caché ivy2 (~/.ivy2/cache/io.delta).

    Los JARs se descargan la primera vez al ejecutar configure_spark_with_delta_pip.
    Versiones futuras se añaden automáticamente al glob.
    """
    ivy_delta = Path.home() / ".ivy2" / "cache" / "io.delta"
    jars = list(ivy_delta.glob("*/jars/*.jar"))
    if not jars:
        raise FileNotFoundError(
            f"No se encontraron JARs de Delta en {ivy_delta}. "
            'Ejecuta una vez: python -c "from delta import configure_spark_with_delta_pip; '
            "from pyspark.sql import SparkSession; "
            'configure_spark_with_delta_pip(SparkSession.builder).getOrCreate()"'
        )
    return ",".join(str(j) for j in jars)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local con Delta Lake para tests. scope=session: una sola instancia."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("dev-trends-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars", _delta_jars())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
