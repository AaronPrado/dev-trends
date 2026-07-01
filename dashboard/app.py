"""Dashboard Streamlit: evolución diaria de actividad de desarrollo."""

import os
from datetime import date

import pandas as pd
import streamlit as st
from pyathena import connect

from dev_trends.dashboard.query import (
    DAILY_ACTIVITY_QUERY,
    shape_daily_activity,
    total_by_technology,
)

REGION = os.environ.get("AWS_REGION", "eu-west-1")
WORKGROUP = os.environ.get("DEV_TRENDS_ATHENA_WORKGROUP", "dev-trends-v1")
DATABASE = os.environ.get("DEV_TRENDS_GLUE_DB", "dev_trends")
PROFILE = os.environ.get("AWS_PROFILE", "dev-trends-pipeline")


@st.cache_data(ttl=600)
def load_daily_activity() -> list[tuple[date, str, int]]:
    """Ejecuta la query en Athena. Cacheada 10 min para no volver a escanear en cada rerun."""
    conn = connect(
        region_name=REGION,
        work_group=WORKGROUP,
        schema_name=DATABASE,
        profile_name=PROFILE,
    )
    with conn.cursor() as cursor:
        cursor.execute(DAILY_ACTIVITY_QUERY)
        return cursor.fetchall()


st.set_page_config(page_title="Tech Ecosystem Observatory", layout="wide")
st.title("Evolución diaria de actividad de desarrollo")
st.caption(
    "PushEvents de GH Archive para 5 herramientas de Data Engineering "
    "(Airflow, Spark, dbt, Dagster, Prefect)."
)

rows = load_daily_activity()
if not rows:
    st.warning("No hay datos en el Gold todavía. Ejecuta el pipeline y `dbt build` primero.")
else:
    wide = shape_daily_activity(rows)

    min_date, max_date = wide.index.min().date(), wide.index.max().date()
    date_range = st.date_input(
        "Rango de fechas",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if len(date_range) != 2:
        st.stop()  # el usuario todavía no ha elegido las dos fechas del rango

    start_date, end_date = date_range
    selected = wide.loc[pd.Timestamp(start_date) : pd.Timestamp(end_date)]

    st.line_chart(selected)

    st.subheader("Total de PushEvents por tecnología (rango seleccionado)")
    totals = total_by_technology(selected)
    cols = st.columns(len(totals))
    for col, (technology, total) in zip(cols, totals.items(), strict=True):
        col.metric(technology, total)
