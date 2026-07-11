from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

_AVAILABLE_NOW: dict[str, Any] = {"availableNow": True}


def write_silver(
    df: DataFrame,
    output_path: str,
    mode: str = "append",
    replace_where: str | None = None,
) -> None:
    """Escribe el DataFrame normalizado a Silver en Delta, particionado por fecha.

    Args:
        df: DataFrame con el esquema Silver.
        output_path: Ruta raíz de la capa Silver.
        mode: Modo de escritura Delta ('append' -> carga incremental).
        replace_where: Predicado de partición (p. ej. "year = 2026 AND month = 4
            AND day = 1"). Con mode='overwrite', reemplaza SOLO esa partición
            (idempotente por día): el backfill se puede reiniciar sin duplicar.
    """
    writer = df.write.format("delta").mode(mode).partitionBy("year", "month", "day")
    if replace_where is not None:
        writer = writer.option("replaceWhere", replace_where)
    writer.save(output_path)


def write_silver_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    query_name: str = "bronze-to-silver",
    trigger: dict[str, Any] | None = None,
) -> StreamingQuery:
    """Escribe el stream Silver a Delta, particionado por fecha.

    Args:
        df: DataFrame de streaming con el esquema Silver.
        output_path: Ruta raíz de la capa Silver (Delta).
        checkpoint_path: Ruta del checkpoint del stream.
        query_name: Nombre estable de la query; etiqueta las métricas de
            observabilidad (una serie por nombre, no un UUID por arranque).
        trigger: Opciones de trigger de Spark (p. ej. {"availableNow": True} o
            {"processingTime": "5 seconds"}). Por defecto availableNow.

    Returns:
        StreamingQuery en ejecución.
    """
    return (
        df.writeStream.format("delta")
        .queryName(query_name)
        .outputMode("append")
        .partitionBy("year", "month", "day")
        .option("checkpointLocation", checkpoint_path)
        .trigger(**(trigger or _AVAILABLE_NOW))
        .start(output_path)
    )
