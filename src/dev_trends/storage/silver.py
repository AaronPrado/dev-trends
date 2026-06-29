from pyspark.sql import DataFrame


def write_silver(df: DataFrame, output_path: str, mode: str = "append") -> None:
    """Escribe el DataFrame normalizado a Silver en formato Delta, particionado por fecha.

    Args:
        df: DataFrame con el esquema Silver.
        output_path: Ruta raíz de la capa Silver.
        mode: Modo de escritura Delta ('append' -> carga incremental).
    """
    (df.write.format("delta").mode(mode).partitionBy("year", "month", "day").save(output_path))
