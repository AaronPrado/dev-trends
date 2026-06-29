import urllib.request
from importlib.metadata import version as pkg_version
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def _ensure_delta_jars() -> str:
    """Localiza los JARs de Delta, descargándolos de Maven si no están en caché.

    - Local: usa ~/.ivy2/cache/io.delta (ya existente tras la primera ejecución).
    - CI primera ejecución: descarga desde Maven Central y los guarda en ivy2.
    - CI ejecuciones siguientes: usa la caché de ivy2 guardada por actions/cache.
    """
    version = pkg_version("delta-spark")
    ivy_delta = Path.home() / ".ivy2" / "cache" / "io.delta"
    artifacts = [
        ("delta-spark_2.12", f"delta-spark_2.12-{version}.jar"),
        ("delta-storage", f"delta-storage-{version}.jar"),
    ]
    jars = []
    for group, jar_name in artifacts:
        jar_path = ivy_delta / group / "jars" / jar_name
        if not jar_path.exists():
            jar_path.parent.mkdir(parents=True, exist_ok=True)
            url = f"https://repo1.maven.org/maven2/io/delta/{group}/{version}/{jar_name}"
            urllib.request.urlretrieve(url, jar_path)
        jars.append(str(jar_path))
    return ",".join(jars)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local con Delta Lake para tests. scope=session: una sola instancia."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("dev-trends-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.jars", _ensure_delta_jars())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
