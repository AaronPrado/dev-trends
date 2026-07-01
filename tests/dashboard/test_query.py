import pandas as pd

from dev_trends.dashboard.query import (
    DAILY_ACTIVITY_QUERY,
    shape_daily_activity,
    total_by_technology,
)


def test_daily_activity_query_targets_fact_table() -> None:
    assert "fact_github_activity" in DAILY_ACTIVITY_QUERY
    assert "activity_date" in DAILY_ACTIVITY_QUERY


def test_shape_daily_activity_pivots_to_wide_format() -> None:
    rows = [
        ("2026-06-17", "airflow", 10),
        ("2026-06-17", "dbt", 5),
        ("2026-06-18", "airflow", 20),
    ]
    wide = shape_daily_activity(rows)

    assert list(wide.columns) == ["airflow", "dbt"]
    assert wide.loc[pd.Timestamp("2026-06-17"), "airflow"] == 10
    assert wide.loc[pd.Timestamp("2026-06-17"), "dbt"] == 5
    assert wide.loc[pd.Timestamp("2026-06-18"), "airflow"] == 20


def test_shape_daily_activity_fills_missing_combinations_with_zero() -> None:
    rows = [
        ("2026-06-17", "airflow", 10),
        ("2026-06-18", "airflow", 20),
    ]
    wide = shape_daily_activity(rows)
    assert wide.loc[pd.Timestamp("2026-06-18"), "airflow"] == 20
    # "dbt" no aparece en las filas de origen para el 17: sin datos = 0, no NaN.
    rows_with_gap = [
        ("2026-06-17", "airflow", 10),
        ("2026-06-17", "dbt", 5),
        ("2026-06-18", "airflow", 20),
    ]
    wide_with_gap = shape_daily_activity(rows_with_gap)
    assert wide_with_gap.loc[pd.Timestamp("2026-06-18"), "dbt"] == 0


def test_shape_daily_activity_sorts_by_date() -> None:
    rows = [("2026-06-18", "spark", 1), ("2026-06-17", "spark", 2)]
    wide = shape_daily_activity(rows)
    assert list(wide.index) == [pd.Timestamp("2026-06-17"), pd.Timestamp("2026-06-18")]


def test_total_by_technology_sums_and_sorts_descending() -> None:
    wide = pd.DataFrame(
        {"spark": [10, 20], "dbt": [5, 5]},
        index=pd.to_datetime(["2026-06-17", "2026-06-18"]),
    )
    totals = total_by_technology(wide)
    assert list(totals.index) == ["spark", "dbt"]
    assert totals["spark"] == 30
    assert totals["dbt"] == 10
