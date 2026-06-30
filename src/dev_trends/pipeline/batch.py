import logging
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from dev_trends.ingestion.gharchive import download_range, read_bronze
from dev_trends.spark_session import build_spark
from dev_trends.storage.silver import write_silver
from dev_trends.transform.normalize import normalize_events
from dev_trends.transform.technologies import build_technology_mapping

logger = logging.getLogger(__name__)


def _build_spark() -> SparkSession:
    return build_spark("dev-trends-batch")


def run_batch(
    event_date: date,
    bronze_dir: Path,
    silver_path: str,
    hours: range = range(24),
) -> None:
    """Ejecuta el pipeline batch para una fecha: descarga → Silver.

    La agregación a Gold ya no vive aquí: la construye dbt sobre Silver (Paso 3).

    Args:
        event_date: Fecha a procesar.
        bronze_dir: Directorio donde aterrizar los .json.gz (Bronze local).
        silver_path: Ruta raíz de la capa Silver (local o s3://).
        hours: Rango de horas a intentar (por defecto las 24 del día).
    """
    spark = _build_spark()

    paths = download_range(event_date, bronze_dir, hours)
    if not paths:
        logger.warning("Sin ficheros disponibles para %s — se omite la fecha.", event_date)
        return

    raw_df = read_bronze(spark, paths)
    mapping = build_technology_mapping(spark)
    silver_df = normalize_events(raw_df, mapping)
    write_silver(silver_df, silver_path)
    logger.info("Silver escrito para %s", event_date)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Pipeline batch GH Archive → Silver")
    parser.add_argument("--date", required=True, help="Fecha a procesar (YYYY-MM-DD)")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-path", default="data/silver")
    parser.add_argument("--hours", default="0-23", help="Rango de horas, e.g. '0-0' para 1 hora")
    args = parser.parse_args()

    start, end = (int(h) for h in args.hours.split("-"))
    run_batch(
        event_date=date.fromisoformat(args.date),
        bronze_dir=Path(args.bronze_dir),
        silver_path=args.silver_path,
        hours=range(start, end + 1),
    )
