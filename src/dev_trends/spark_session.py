import os
from importlib.metadata import version

from pyspark.sql import SparkSession

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{version('pyspark')}"

# hadoop-aws debe emparejar con el Hadoop que empaqueta pyspark (3.3.4 en pyspark 3.5.x,
# pin <3.5.6). Ivy arrastra su transitiva aws-java-sdk-bundle: no se declara a mano.
S3A_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"

# Región del endpoint s3a. Parametrizable por entorno; por defecto la del proyecto.
_AWS_REGION = os.environ.get("AWS_REGION", "eu-west-1")


def build_spark(
    app_name: str,
    extra_packages: list[str] | None = None,
    *,
    enable_s3a: bool = False,
) -> SparkSession:
    """Construye una SparkSession con Delta Lake y, opcionalmente, acceso a S3 (s3a).

    Args:
        app_name: Nombre de la aplicación Spark.
        extra_packages: Coordenadas Maven adicionales (e.g. KAFKA_PACKAGE).
        enable_s3a: Si True, añade hadoop-aws y configura el sistema de ficheros
            s3a leyendo las credenciales de la cadena por defecto del SDK (perfil
            AWS_PROFILE / variables de entorno), sin claves en código.

    Returns:
        SparkSession configurada con Delta (y s3a si se solicita).
    """
    from delta import configure_spark_with_delta_pip

    packages = list(extra_packages or [])
    if enable_s3a:
        packages.append(S3A_PACKAGE)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if enable_s3a:
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            # Endpoint regional explícito: evita el redirect 301 al escribir en
            # un bucket que no está en la región adecuada.
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{_AWS_REGION}.amazonaws.com")
        )

    return configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
