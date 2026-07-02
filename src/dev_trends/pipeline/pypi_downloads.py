import logging
from datetime import date

from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from dev_trends.ingestion.pypi_downloads import (
    all_pypi_packages,
    build_client,
    estimate_bytes,
    run_download_query,
)
from dev_trends.spark_session import build_spark
from dev_trends.storage.silver import write_silver
from dev_trends.transform.normalize_pypi import normalize_downloads
from dev_trends.transform.technologies import build_pypi_package_mapping

logger = logging.getLogger(__name__)

_GIB = 1024**3

# Forma del agregado que devuelve run_download_query (list[tuple]); esquema
# explícito para no depender de la inferencia de tipos de Spark.
_RAW_DOWNLOADS_SCHEMA = StructType(
    [
        StructField("download_date", DateType()),
        StructField("pypi_package", StringType()),
        StructField("download_count", LongType()),
    ]
)


def run_pypi_ingest(
    start: date,
    end: date,
    silver_path: str,
    max_bytes: int,
    *,
    project: str | None = None,
    dry_run: bool = False,
) -> None:
    """Ingesta de descargas PyPI: BigQuery (agregado) → Silver.

    Rango [start, end) — end exclusivo. Siempre estima el escaneo antes de ejecutar;
    dry_run=True estima y termina.

    Args:
        start: Primer día a incluir (inclusive).
        end: Día de corte (exclusivo).
        silver_path: Ruta raíz de la capa Silver PyPI (local o s3a://).
        max_bytes: Tope duro de bytes facturables (maximum_bytes_billed).
        project: Proyecto GCP de facturación (Sandbox); None = el de las credenciales.
        dry_run: Si True, solo estima el escaneo y no ejecuta la query ni escribe.
    """
    packages = all_pypi_packages()
    client = build_client(project)

    estimated = estimate_bytes(client, start, end, packages)
    logger.info("Escaneo estimado: %.2f GiB (tope: %.2f GiB)", estimated / _GIB, max_bytes / _GIB)
    if dry_run:
        logger.info("dry-run: no se ejecuta la query ni se escribe.")
        return

    rows = run_download_query(client, start, end, packages, max_bytes=max_bytes)
    if not rows:
        logger.warning("Query sin filas para [%s, %s) — nada que escribir.", start, end)
        return

    spark = build_spark("dev-trends-pypi", enable_s3a=silver_path.startswith("s3a://"))
    raw_df = spark.createDataFrame(rows, schema=_RAW_DOWNLOADS_SCHEMA)
    mapping = build_pypi_package_mapping(spark)
    silver_df = normalize_downloads(raw_df, mapping)
    # Backfill de una ventana fija: overwrite hace el re-run idempotente.
    write_silver(silver_df, silver_path, mode="overwrite")
    logger.info("Silver PyPI escrito (%d filas) en %s", len(rows), silver_path)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ingesta descargas PyPI (BigQuery) → Silver")
    parser.add_argument("--start", required=True, help="Primer día, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="Día de corte, exclusivo (YYYY-MM-DD)")
    parser.add_argument("--silver-path", default="data/silver/pypi_downloads")
    parser.add_argument("--max-gib", type=float, default=100.0, help="Tope de escaneo facturable")
    parser.add_argument("--project", default=None, help="Proyecto GCP de facturación (Sandbox)")
    parser.add_argument("--dry-run", action="store_true", help="Solo estima el escaneo, no ejecuta")
    args = parser.parse_args()

    run_pypi_ingest(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        silver_path=args.silver_path,
        max_bytes=int(args.max_gib * _GIB),
        project=args.project,
        dry_run=args.dry_run,
    )
