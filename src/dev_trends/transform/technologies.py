from pyspark.sql import DataFrame, SparkSession

TECHNOLOGY_REPOS: dict[str, list[str]] = {
    "airflow": ["apache/airflow"],
    "spark": ["apache/spark"],
    "dbt": ["dbt-labs/dbt-core"],
    "dagster": ["dagster-io/dagster"],
    "prefect": ["PrefectHQ/prefect"],
}


def build_technology_mapping(spark: SparkSession) -> DataFrame:
    """TECHNOLOGY_REPOS como DataFrame con columnas.

    Se usa en un inner join contra los eventos de GH Archive para filtrar
    y etiquetar la tecnología en una sola operación.
    """
    rows = [(repo, tech) for tech, repos in TECHNOLOGY_REPOS.items() for repo in repos]
    return spark.createDataFrame(rows, schema=["repository", "technology"])
