"""Tests de transform/normalize.py."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from dev_trends.transform.normalize import (
    EVENT_TYPE_MAP,
    SILVER_COLUMNS,
    add_date_partitions,
    derive_organization,
    filter_event_types,
    normalize_event_type,
    normalize_events,
    select_raw_fields,
)
from dev_trends.transform.technologies import build_technology_mapping


@pytest.fixture
def raw_df(spark: SparkSession):
    """DataFrame con el esquema anidado del JSON crudo de GH Archive."""
    schema = StructType([
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("repo", StructType([StructField("name", StringType())])),
        StructField("created_at", StringType()),
        StructField("public", StringType()),
    ])
    data = [
        ("1", "PushEvent", ("apache/airflow",), "2024-01-15T10:00:00Z", "true"),
        ("2", "WatchEvent", ("dbt-labs/dbt-core",), "2024-01-15T11:30:00Z", "true"),
        ("3", "CreateEvent", ("some/other-repo",), "2024-01-15T12:00:00Z", "true"),
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def flat_df(spark: SparkSession):
    """DataFrame plano (post select_raw_fields) con varios tipos de evento."""
    data = [
        ("1", "PushEvent", "apache/airflow", "2024-01-15T10:00:00Z"),
        ("2", "WatchEvent", "dbt-labs/dbt-core", "2024-01-15T11:00:00Z"),
        ("3", "CreateEvent", "some/other-repo", "2024-01-15T12:00:00Z"),
        ("4", "PullRequestEvent", "dagster-io/dagster", "2024-01-15T13:00:00Z"),
    ]
    return spark.createDataFrame(data, schema=["event_id", "event_type", "repository", "created_at"])


# --- select_raw_fields ---


def test_select_raw_fields_output_columns(raw_df):
    result = select_raw_fields(raw_df)
    assert set(result.columns) == {"event_id", "event_type", "repository", "created_at"}


def test_select_raw_fields_flattens_repo_name(raw_df):
    result = select_raw_fields(raw_df)
    repos = {r.repository for r in result.collect()}
    assert "apache/airflow" in repos


# --- filter_event_types ---


def test_filter_event_types_keeps_monitored(flat_df):
    result = filter_event_types(flat_df)
    types = {r.event_type for r in result.collect()}
    assert types <= set(EVENT_TYPE_MAP.keys())


def test_filter_event_types_drops_unmonitored(flat_df):
    result = filter_event_types(flat_df)
    types = {r.event_type for r in result.collect()}
    assert "CreateEvent" not in types


# --- normalize_event_type ---


def test_normalize_event_type_all_normalized(flat_df):
    result = normalize_event_type(filter_event_types(flat_df))
    types = {r.event_type for r in result.collect()}
    assert types <= set(EVENT_TYPE_MAP.values())


def test_normalize_event_type_push_mapping(spark: SparkSession):
    df = spark.createDataFrame(
        [("1", "PushEvent", "r/r", "2024-01-01T00:00:00Z")],
        schema=["event_id", "event_type", "repository", "created_at"],
    )
    assert normalize_event_type(df).first().event_type == "push"


# --- derive_organization ---


def test_derive_organization_extracts_correctly(spark: SparkSession):
    df = spark.createDataFrame([("apache/airflow",)], schema=["repository"])
    assert derive_organization(df).first().organization == "apache"


# --- add_date_partitions ---


def test_add_date_partitions_adds_columns(flat_df):
    result = add_date_partitions(flat_df)
    assert {"year", "month", "day"}.issubset(set(result.columns))


def test_add_date_partitions_correct_values(spark: SparkSession):
    df = spark.createDataFrame(
        [("1", "push", "r/r", "2024-03-07T00:00:00Z")],
        schema=["event_id", "event_type", "repository", "created_at"],
    )
    row = add_date_partitions(df).first()
    assert (row.year, row.month, row.day) == (2024, 3, 7)


# --- normalize_events (composición completa) ---


def test_normalize_events_silver_schema(spark: SparkSession, raw_df):
    mapping = build_technology_mapping(spark)
    result = normalize_events(raw_df, mapping)
    assert set(result.columns) == set(SILVER_COLUMNS)


def test_normalize_events_filters_correctly(spark: SparkSession, raw_df):
    """raw_df: airflow(Push)✓, dbt(Watch)✓, other(Create)✗ → 2 filas."""
    mapping = build_technology_mapping(spark)
    result = normalize_events(raw_df, mapping)
    assert result.count() == 2


def test_normalize_events_event_types_normalized(spark: SparkSession, raw_df):
    mapping = build_technology_mapping(spark)
    result = normalize_events(raw_df, mapping)
    types = {r.event_type for r in result.select("event_type").collect()}
    assert types <= set(EVENT_TYPE_MAP.values())
