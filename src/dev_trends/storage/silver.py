from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_silver(df: DataFrame, output_path: str, mode: str = "append") -> None:
    """Escribe el DataFrame normalizado a Silver en formato Delta, particionado por fecha.

    Args:
        df: DataFrame con el esquema Silver.
        output_path: Ruta raíz de la capa Silver.
        mode: Modo de escritura Delta ('append' -> carga incremental).
    """
    (df.write.format("delta").mode(mode).partitionBy("year", "month", "day").save(output_path))


def write_silver_stream(df: DataFrame, output_path: str, checkpoint_path: str) -> StreamingQuery:
    """Escribe el stream Silver a Delta, particionado por fecha, con availableNow.

    Args:
        df: DataFrame de streaming con el esquema Silver.
        output_path: Ruta raíz de la capa Silver (Delta).
        checkpoint_path: Ruta del checkpoint del stream.

    Returns:
        StreamingQuery en ejecución.
    """
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("year", "month", "day")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(output_path)
    )
