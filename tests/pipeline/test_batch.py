from datetime import date

from dev_trends.pipeline.batch import date_range


def test_date_range_excludes_end() -> None:
    assert date_range(date(2026, 4, 1), date(2026, 4, 4)) == [
        date(2026, 4, 1),
        date(2026, 4, 2),
        date(2026, 4, 3),
    ]


def test_date_range_empty_when_start_equals_end() -> None:
    assert date_range(date(2026, 4, 1), date(2026, 4, 1)) == []


def test_date_range_spans_month_boundary() -> None:
    assert date_range(date(2026, 4, 29), date(2026, 5, 2)) == [
        date(2026, 4, 29),
        date(2026, 4, 30),
        date(2026, 5, 1),
    ]
