"""Garantiza que el seed dim_technology no se desincronice de TECHNOLOGY_REPOS.

El catálogo de tecnologías vive en dos sitios por necesidad: el código
(`TECHNOLOGY_REPOS`, que filtra/etiqueta en Silver) y el seed de dbt
(`dim_technology.csv`, dimensión de Gold). Estos tests fallan si divergen.
"""

import csv
from pathlib import Path

from dev_trends.transform.technologies import TECHNOLOGY_REPOS

_SEED = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "dim_technology.csv"


def _read_seed() -> list[dict[str, str]]:
    with _SEED.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_seed_technologies_match_source() -> None:
    """El conjunto de tecnologías del seed coincide con el de TECHNOLOGY_REPOS."""
    seed_techs = {row["technology"] for row in _read_seed()}
    assert seed_techs == set(TECHNOLOGY_REPOS)


def test_seed_github_repo_matches_source() -> None:
    """Cada github_repo del seed pertenece a su tecnología en TECHNOLOGY_REPOS."""
    for row in _read_seed():
        assert row["github_repo"] in TECHNOLOGY_REPOS[row["technology"]]
