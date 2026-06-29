from pyspark.sql import SparkSession

from dev_trends.transform.technologies import TECHNOLOGY_REPOS, build_technology_mapping

V1_TECHNOLOGIES = {"airflow", "spark", "dbt", "dagster", "prefect"}


def test_technology_repos_has_all_v1_techs() -> None:
    assert set(TECHNOLOGY_REPOS.keys()) == V1_TECHNOLOGIES


def test_technology_repos_no_empty_lists() -> None:
    for tech, repos in TECHNOLOGY_REPOS.items():
        assert repos, f"{tech} no tiene repos definidos"


def test_technology_repos_no_duplicates() -> None:
    all_repos = [repo for repos in TECHNOLOGY_REPOS.values() for repo in repos]
    assert len(all_repos) == len(set(all_repos)), "hay repos duplicados en el mapping"


def test_build_technology_mapping_schema(spark: SparkSession) -> None:
    df = build_technology_mapping(spark)
    assert df.columns == ["repository", "technology"]


def test_build_technology_mapping_row_count(spark: SparkSession) -> None:
    expected = sum(len(repos) for repos in TECHNOLOGY_REPOS.values())
    assert build_technology_mapping(spark).count() == expected


def test_build_technology_mapping_all_techs_present(spark: SparkSession) -> None:
    df = build_technology_mapping(spark)
    techs = {row.technology for row in df.select("technology").collect()}
    assert techs == V1_TECHNOLOGIES
