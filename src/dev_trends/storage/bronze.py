from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from dev_trends.ingestion.gharchive import RAW_SCHEMA

BRONZE_COLUMNS: list[str] = [
    "key",
    "value",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "ingested_at",
]


def to_bronze(df: DataFrame) -> DataFrame:
    """Refactoriza de Kafka para la capa Bronze.

    Conserva el valor crudo y añade metadatos de Kafka y de
    ingesta.

    Args:
        df: DataFrame con el esquema fuente de Kafka (key, value en binario,
            topic, partition, offset, timestamp).

    Returns:
        DataFrame con las columnas de BRONZE_COLUMNS (key/value como string).
    """
    return df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
    )


def read_bronze_stream(spark: SparkSession, bronze_path: str) -> DataFrame:
    """Abre un stream de lectura sobre la tabla Bronze (Delta como fuente)."""
    return spark.readStream.format("delta").load(bronze_path)


def parse_bronze_value(df: DataFrame) -> DataFrame:
    """Decodifica el value crudo de Bronze al esquema anidado de GH Archive.

    El value de Bronze es la línea JSON original (string). Aplica RAW_SCHEMA con
    from_json y eleva los campos al nivel superior, dejando el DataFrame en la
    forma que espera normalize_events (id, type, repo, created_at).
    """
    return df.select(F.from_json(F.col("value"), RAW_SCHEMA).alias("data")).select("data.*")


def write_bronze_stream(df: DataFrame, output_path: str, checkpoint_path: str) -> StreamingQuery:
    """Escribe el stream Bronze a Delta con trigger availableNow.

    Args:
        df: DataFrame de streaming con el esquema Bronze.
        output_path: Ruta raíz de la tabla Bronze (Delta).
        checkpoint_path: Ruta del checkpoint (offsets del stream).

    Returns:
        StreamingQuery en ejecución; el llamante decide cuándo esperar.
    """
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(output_path)
    )
