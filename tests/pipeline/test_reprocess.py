from datetime import date

from dev_trends.pipeline.reprocess import partition_predicate


def test_partition_predicate_formatea_year_month_day() -> None:
    assert partition_predicate(date(2026, 4, 1)) == "year = 2026 AND month = 4 AND day = 1"


def test_partition_predicate_sin_zero_pad() -> None:
    """El predicado usa los enteros de year/month/day tal como los genera
    add_date_partitions (F.year/F.month/F.dayofmonth, sin ceros a la izquierda);
    con zero-pad el replaceWhere no casaría con la partición Delta."""
    assert partition_predicate(date(2026, 12, 25)) == "year = 2026 AND month = 12 AND day = 25"
