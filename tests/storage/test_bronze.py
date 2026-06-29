from datetime import datetime

from pyspark.sql import SparkSession

from dev_trends.storage.bronze import BRONZE_COLUMNS, parse_bronze_value, to_bronze
from dev_trends.transform.normalize import SILVER_COLUMNS, normalize_events
from dev_trends.transform.technologies import build_technology_mapping

_RAW_PUSH_EVENT = (
    '{"id":"42","type":"PushEvent","repo":{"name":"apache/spark"},'
    '"created_at":"2024-01-15T10:00:00Z"}'
)


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


def test_parse_bronze_value_extracts_event_fields(spark: SparkSession) -> None:
    df = spark.createDataFrame([(_RAW_PUSH_EVENT,)], "value string")

    result = parse_bronze_value(df)
    row = result.collect()[0]

    assert row["id"] == "42"
    assert row["type"] == "PushEvent"
    assert row["repo"]["name"] == "apache/spark"
    assert row["created_at"] == "2024-01-15T10:00:00Z"


def test_parse_then_normalize_yields_silver_schema(spark: SparkSession) -> None:
    # Prueba la Regla 1: normalize_events se reutiliza intacto sobre el value de Bronze.
    df = spark.createDataFrame([(_RAW_PUSH_EVENT,)], "value string")
    mapping = build_technology_mapping(spark)

    result = normalize_events(parse_bronze_value(df), mapping)
    row = result.collect()[0]

    assert result.columns == SILVER_COLUMNS
    assert row["technology"] == "spark"
