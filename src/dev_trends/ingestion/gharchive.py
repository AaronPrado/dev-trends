import logging
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_BASE_URL = "https://data.gharchive.org"


def build_url(event_date: date, hour: int) -> str:
    """Construye la URL de un fichero horario de GH Archive."""
    return f"{_BASE_URL}/{event_date.strftime('%Y-%m-%d')}-{hour}.json.gz"


def download_hour(url: str, dest_path: Path) -> bool:
    """Descarga un fichero horario de GH Archive.

    Args:
        url: URL del fichero.
        dest_path: Ruta local donde guardar el fichero.

    Returns:
        True si la descarga fue exitosa, False si la hora no existe (404).

    Raises:
        urllib.error.HTTPError: Para errores HTTP distintos de 404.
        urllib.error.URLError: Para errores de red.
    """
    try:
        with urllib.request.urlopen(url) as response:
            dest_path.write_bytes(response.read())
        return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            logger.warning("Hora no disponible en GH Archive (404): %s", url)
            return False
        raise


def download_range(
    event_date: date,
    dest_dir: Path,
    hours: range = range(24),
) -> list[Path]:
    """Descarga los ficheros horarios disponibles de GH Archive para una fecha.

    Args:
        event_date: Fecha a descargar.
        dest_dir: Directorio donde guardar los ficheros.
        hours: Rango de horas a intentar (por defecto las 24 del día).

    Returns:
        Lista de rutas descargadas; excluye las horas no disponibles en GH Archive.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded: list[Path] = []
    for hour in hours:
        url = build_url(event_date, hour)
        dest_path = dest_dir / f"{event_date.strftime('%Y-%m-%d')}-{hour}.json.gz"
        if download_hour(url, dest_path):
            downloaded.append(dest_path)
    return downloaded


def read_bronze(spark: SparkSession, paths: list[Path]) -> DataFrame:
    """Lee ficheros .json.gz de Bronze en un DataFrame de Spark.

    Spark infiere el esquema y descomprime gzip automáticamente.
    Cada línea del fichero es un evento JSON (JSONL).
    """
    return spark.read.json([str(p) for p in paths])
