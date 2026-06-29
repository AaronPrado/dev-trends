from datetime import datetime

from pyspark.sql import SparkSession

from dev_trends.storage.bronze import BRONZE_COLUMNS, to_bronze


def test_to_bronze_keeps_raw_value_and_adds_metadata(spark: SparkSession) -> None:
    data = [
        (
            bytearray(b"apache/spark"),
            bytearray(b'{"type":"PushEvent"}'),
            "github.push.raw",
            0,
            5,
            datetime(2024, 1, 15, 0, 0, 0),
        )
    ]
    schema = (
        "key binary, value binary, topic string, partition int, offset long, timestamp timestamp"
    )
    df = spark.createDataFrame(data, schema)

    result = to_bronze(df)
    row = result.collect()[0]

    # El orden y el conjunto de columnas deben coincidir con el esquema Bronze.
    assert result.columns == BRONZE_COLUMNS
    # El value crudo se conserva intacto (solo cast binary -> string).
    assert row["value"] == '{"type":"PushEvent"}'
    assert row["key"] == "apache/spark"
    assert row["offset"] == 5
    assert row["ingested_at"] is not None
