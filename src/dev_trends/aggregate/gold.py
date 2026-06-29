"""Agregación provisional de Silver a Gold.

(se reescribirá con watermarks en el paso de streaming
y se sustituirá por modelos dbt)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_to_gold(df: DataFrame) -> DataFrame:
    """Agrega eventos Silver a Gold: conteo mensual por tecnología y tipo de evento."""
    return df.groupBy("technology", "event_type", "year", "month").agg(
        F.count("event_id").alias("event_count")
    )
