import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dev_trends.storage.silver import write_silver


@pytest.fixture
def silver_df(spark: SparkSession):
    data = [
        ("1", "airflow", "push", "apache/airflow", "apache", "2024-01-15T10:00:00Z"),
        ("2", "dbt", "watch", "dbt-labs/dbt-core", "dbt-labs", "2024-01-15T11:00:00Z"),
    ]
    return (
        spark.createDataFrame(
            data,
            schema=[
                "event_id",
                "technology",
                "event_type",
                "repository",
                "organization",
                "created_at",
            ],
        )
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("year", F.lit(2024))
        .withColumn("month", F.lit(1))
        .withColumn("day", F.lit(15))
    )


def test_write_silver_creates_delta_table(spark: SparkSession, silver_df, tmp_path) -> None:
    path = str(tmp_path / "silver")
    write_silver(silver_df, path)
    assert spark.read.format("delta").load(path).count() == 2


def test_write_silver_partitions_by_date(silver_df, tmp_path) -> None:
    path = str(tmp_path / "silver")
    write_silver(silver_df, path)
    assert (tmp_path / "silver" / "year=2024" / "month=1" / "day=15").exists()


def test_write_silver_append_mode(spark: SparkSession, silver_df, tmp_path) -> None:
    path = str(tmp_path / "silver")
    write_silver(silver_df, path)
    write_silver(silver_df, path)
    assert spark.read.format("delta").load(path).count() == 4
