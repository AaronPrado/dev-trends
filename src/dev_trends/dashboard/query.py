from collections.abc import Sequence
from datetime import date

import pandas as pd

DAILY_ACTIVITY_QUERY = """
SELECT activity_date, technology, SUM(event_count) AS events
FROM fact_github_activity
GROUP BY activity_date, technology
ORDER BY 1, 2
""".strip()


def shape_daily_activity(rows: Sequence[tuple[date, str, int]]) -> pd.DataFrame:
    """Da forma ancha (día x tecnología) a las filas de Athena, lista para `st.line_chart`.

    Espera filas `(activity_date, technology, events)`, el orden de columnas de
    `DAILY_ACTIVITY_QUERY`. Los días sin eventos para una tecnología quedan a 0 (no
    hay fila en el origen para esa combinación), para que la línea no se corte.
    """
    df = pd.DataFrame(rows, columns=["activity_date", "technology", "events"])
    df["activity_date"] = pd.to_datetime(df["activity_date"])
    wide = df.pivot(index="activity_date", columns="technology", values="events")
    return wide.sort_index().fillna(0).astype(int)


def total_by_technology(wide: pd.DataFrame) -> pd.Series:
    """Total de eventos por tecnología en todo el rango cargado, de mayor a menor."""
    return wide.sum().sort_values(ascending=False)
