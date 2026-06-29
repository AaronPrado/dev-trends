from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EVENT_TYPE_MAP: dict[str, str] = {
    "PushEvent": "push",
    "PullRequestEvent": "pull_request",
    "ReleaseEvent": "release",
    "WatchEvent": "watch",
}

SILVER_COLUMNS: list[str] = [
    "event_id",
    "technology",
    "event_type",
    "repository",
    "organization",
    "created_at",
    "year",
    "month",
    "day",
]


def select_raw_fields(df: DataFrame) -> DataFrame:
    """Selecciona y aplana los campos del JSON de GH Archive.

    Entrada: esquema anidado del JSON (id, type, repo struct, created_at, ...).
    Salida: DataFrame plano con event_id, event_type, repository, created_at (string).
    """
    return df.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("repo.name").alias("repository"),
        F.col("created_at"),
    )


def filter_event_types(df: DataFrame) -> DataFrame:
    """Filtra al subconjunto de tipos de eventos."""
    return df.filter(F.col("event_type").isin(list(EVENT_TYPE_MAP.keys())))


def normalize_event_type(df: DataFrame) -> DataFrame:
    """Normaliza los nombres de tipo de evento."""
    literals = [lit for k, v in EVENT_TYPE_MAP.items() for lit in (F.lit(k), F.lit(v))]
    return df.withColumn("event_type", F.create_map(*literals)[F.col("event_type")])


def derive_organization(df: DataFrame) -> DataFrame:
    """Deriva organization del campo repository: 'org/repo' → 'org'."""
    return df.withColumn("organization", F.split(F.col("repository"), "/")[0])


def add_date_partitions(df: DataFrame) -> DataFrame:
    """Convierte created_at a timestamp y añade columnas de partición year/month/day."""
    return (
        df.withColumn(
            "created_at",
            F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        )
        .withColumn("year", F.year(F.col("created_at")))
        .withColumn("month", F.month(F.col("created_at")))
        .withColumn("day", F.dayofmonth(F.col("created_at")))
    )


def attach_technology(df: DataFrame, mapping: DataFrame) -> DataFrame:
    """Filtra y etiqueta eventos por tecnología."""
    return df.join(F.broadcast(mapping), on="repository", how="inner")


def normalize_events(df: DataFrame, mapping: DataFrame) -> DataFrame:
    """Pipeline completo: JSON anidado de GH Archive → esquema Silver.

    Args:
        df: DataFrame con el esquema anidado del JSON de GH Archive.
        mapping: DataFrame [repository, technology] de build_technology_mapping().

    Returns:
        DataFrame con el esquema Silver (columnas en SILVER_COLUMNS).
    """
    return (
        df.transform(select_raw_fields)
        .transform(filter_event_types)
        .transform(normalize_event_type)
        .transform(derive_organization)
        .transform(attach_technology, mapping)
        .transform(add_date_partitions)
        .select(SILVER_COLUMNS)
    )
