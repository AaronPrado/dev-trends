from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

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
        DataFrame con las columnas de BRONZE_COLUMNS.
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
