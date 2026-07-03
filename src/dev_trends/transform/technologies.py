from pyspark.sql import DataFrame, SparkSession

TECHNOLOGY_REPOS: dict[str, list[str]] = {
    "airflow": ["apache/airflow"],
    "spark": ["apache/spark"],
    "dbt": ["dbt-labs/dbt-core"],
    "dagster": ["dagster-io/dagster"],
    "prefect": ["PrefectHQ/prefect"],
}

TECHNOLOGY_PYPI_PACKAGES: dict[str, list[str]] = {
    "airflow": ["apache-airflow"],
    "spark": ["pyspark"],
    "dbt": ["dbt-core"],
    "dagster": ["dagster"],
    "prefect": ["prefect"],
}


def build_technology_mapping(spark: SparkSession) -> DataFrame:
    """TECHNOLOGY_REPOS como DataFrame con columnas.

    Se usa en un inner join contra los eventos de GH Archive para filtrar
    y etiquetar la tecnología en una sola operación.
    """
    rows = [(repo, tech) for tech, repos in TECHNOLOGY_REPOS.items() for repo in repos]
    return spark.createDataFrame(rows, schema=["repository", "technology"])


def build_pypi_package_mapping(spark: SparkSession) -> DataFrame:
    """TECHNOLOGY_PYPI_PACKAGES como DataFrame [pypi_package, technology].

    Se usa en un inner join contra el agregado de descargas para filtrar y
    etiquetar la tecnología en una sola operación (análogo a build_technology_mapping).
    """
    rows = [(pkg, tech) for tech, pkgs in TECHNOLOGY_PYPI_PACKAGES.items() for pkg in pkgs]
    return spark.createDataFrame(rows, schema=["pypi_package", "technology"])
