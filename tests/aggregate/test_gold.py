"""Tests de aggregate/gold.py."""

from pyspark.sql import SparkSession

from dev_trends.aggregate.gold import aggregate_to_gold


def test_aggregate_to_gold_output_columns(spark: SparkSession) -> None:
    data = [("airflow", "push", 2024, 1, "1"), ("airflow", "push", 2024, 1, "2")]
    df = spark.createDataFrame(
        data, schema=["technology", "event_type", "year", "month", "event_id"]
    )
    result = aggregate_to_gold(df)
    assert set(result.columns) == {"technology", "event_type", "year", "month", "event_count"}


def test_aggregate_to_gold_counts_correctly(spark: SparkSession) -> None:
    data = [
        ("airflow", "push", 2024, 1, "1"),
        ("airflow", "push", 2024, 1, "2"),
        ("dbt", "watch", 2024, 1, "3"),
    ]
    df = spark.createDataFrame(
        data, schema=["technology", "event_type", "year", "month", "event_id"]
    )
    result = aggregate_to_gold(df)
    rows = {(r.technology, r.event_type): r.event_count for r in result.collect()}
    assert rows[("airflow", "push")] == 2
    assert rows[("dbt", "watch")] == 1


def test_aggregate_to_gold_groups_by_month(spark: SparkSession) -> None:
    """Eventos en meses distintos generan filas separadas."""
    data = [
        ("airflow", "push", 2024, 1, "1"),
        ("airflow", "push", 2024, 2, "2"),
    ]
    df = spark.createDataFrame(
        data, schema=["technology", "event_type", "year", "month", "event_id"]
    )
    result = aggregate_to_gold(df)
    assert result.count() == 2
