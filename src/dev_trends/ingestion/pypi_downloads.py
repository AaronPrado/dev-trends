from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dev_trends.transform.technologies import TECHNOLOGY_PYPI_PACKAGES

if TYPE_CHECKING:
    from datetime import date

    from google.cloud.bigquery import Client, QueryJobConfig

logger = logging.getLogger(__name__)

_TABLE = "bigquery-public-data.pypi.file_downloads"

# WHERE filtra `timestamp` SIN envolver (columna de partición → poda de
# particiones); DATE() solo en SELECT/GROUP BY. Se leen solo 2 columnas.
_DOWNLOAD_QUERY = f"""
SELECT
    DATE(timestamp) AS download_date,
    file.project AS pypi_package,
    COUNT(*) AS download_count
FROM `{_TABLE}`
WHERE timestamp >= TIMESTAMP(@start_date)
  AND timestamp < TIMESTAMP(@end_date)
  AND file.project IN UNNEST(@packages)
GROUP BY download_date, pypi_package
""".strip()


def build_download_query() -> str:
    """SQL de agregación de descargas, parametrizado (@start_date, @end_date,
    @packages). Los valores van como parámetros al ejecutar, nunca interpolados."""
    return _DOWNLOAD_QUERY


def all_pypi_packages() -> list[str]:
    """Lista plana, deduplicada y ordenada de todos los paquetes PyPI monitorizados."""
    return sorted({pkg for pkgs in TECHNOLOGY_PYPI_PACKAGES.values() for pkg in pkgs})


def build_client(project: str | None = None) -> Client:
    """Crea el cliente de BigQuery (auth por ADC).

    Args:
        project: proyecto de facturación; con Sandbox, tu proyecto Sandbox. Si es
            None, usa el proyecto por defecto de las credenciales.
    """
    from google.cloud import bigquery

    return bigquery.Client(project=project)


def _query_config(
    start: date, end: date, packages: list[str], *, dry_run: bool, max_bytes: int | None
) -> QueryJobConfig:
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(
        dry_run=dry_run,
        maximum_bytes_billed=max_bytes,
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end),
            bigquery.ArrayQueryParameter("packages", "STRING", packages),
        ],
    )


def estimate_bytes(client: Client, start: date, end: date, packages: list[str]) -> int:
    """Dry-run: bytes que escanearía la query, sin ejecutarla ni facturar.

    El rango es [start, end) — end exclusivo (día siguiente al último a incluir).
    """
    config = _query_config(start, end, packages, dry_run=True, max_bytes=None)
    job = client.query(build_download_query(), job_config=config)
    return job.total_bytes_processed


def run_download_query(
    client: Client,
    start: date,
    end: date,
    packages: list[str],
    *,
    max_bytes: int,
) -> list[tuple[date, str, int]]:
    """Ejecuta la agregación con tope de escaneo y devuelve el agregado.

    Args:
        max_bytes: tope duro de bytes facturables (maximum_bytes_billed). Sin
            valor por defecto a propósito: la guarda de coste es obligatoria.

    Returns:
        Filas (download_date, pypi_package, download_count).

    Raises:
        google.api_core.exceptions.GoogleAPICallError: error de la API (permisos,
            tope de bytes superado, etc.). No se silencia.
    """
    from google.api_core.exceptions import GoogleAPICallError

    config = _query_config(start, end, packages, dry_run=False, max_bytes=max_bytes)
    try:
        job = client.query(build_download_query(), job_config=config)
        return [(row.download_date, row.pypi_package, row.download_count) for row in job.result()]
    except GoogleAPICallError:
        logger.exception("Falló la consulta de descargas PyPI en BigQuery")
        raise
