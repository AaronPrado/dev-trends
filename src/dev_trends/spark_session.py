from importlib.metadata import version

from pyspark.sql import SparkSession

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{version('pyspark')}"


def build_spark(app_name: str, extra_packages: list[str] | None = None) -> SparkSession:
    """Construye una SparkSession con Delta Lake y paquetes Maven extra opcionales.

    Args:
        app_name: Nombre de la aplicación Spark.
        extra_packages: Coordenadas Maven adicionales (e.g. KAFKA_PACKAGE).

    Returns:
        SparkSession configurada con las extensiones de Delta.
    """
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(
        builder, extra_packages=list(extra_packages or [])
    ).getOrCreate()
