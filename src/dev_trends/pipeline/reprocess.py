import logging
from datetime import date

from dev_trends.ingestion.kafka_source import read_kafka_batch
from dev_trends.pipeline.batch import date_range
from dev_trends.spark_session import KAFKA_PACKAGE, build_spark
from dev_trends.storage.bronze import parse_bronze_value, to_bronze
from dev_trends.storage.silver import write_silver
from dev_trends.transform.normalize import normalize_events
from dev_trends.transform.technologies import build_technology_mapping

logger = logging.getLogger(__name__)


def partition_predicate(day: date) -> str:
    """Predicado replaceWhere de la partición de un día (year/month/day).

    Coincide con las columnas de partición que produce add_date_partitions
    para que el replaceWhere encuentre la partición.
    """
    return f"year = {day.year} AND month = {day.month} AND day = {day.day}"


def run_reprocess_window(
    start: date,
    end: date,
    topic: str,
    bootstrap_servers: str,
    silver_path: str,
) -> None:
    """Reprocesa [start, end) releyendo de Kafka en batch y reescribe Silver.

    Reproceso dirigido de una ventana: lee el topic como query BATCH,
    lo pasa por la misma normalización que el stream
    (to_bronze -> parse_bronze_value -> normalize_events)
    y reescribe cada día con replaceWhere. No duplica.

    La ventana se ancla en created_at (el día real del evento), no en el offset
    de Kafka: el timestamp de Kafka es cuándo se publicó el mensaje, no cuándo
    ocurrió el evento en GitHub. Lee el topic entero y se filtra por partición.

    Requiere que los eventos sigan en el topic (retención ~7 días). Si la ventana
    ya expiró de Kafka, reprocesa desde el origen con run_backfill.

    Args:
        start: Primer día a reprocesar (inclusive).
        end: Día de corte (exclusivo).
        topic: Topic de Kafka a releer.
        bootstrap_servers: Brokers de Kafka (e.g. 'localhost:9092').
        silver_path: Ruta raíz de la capa Silver (local o s3a://).
    """
    spark = build_spark(
        "dev-trends-reprocess",
        extra_packages=[KAFKA_PACKAGE],
        enable_s3a=silver_path.startswith("s3a://"),
    )
    mapping = build_technology_mapping(spark)

    source = read_kafka_batch(spark, topic, bootstrap_servers)
    silver = normalize_events(parse_bronze_value(to_bronze(source)), mapping)

    for day in date_range(start, end):
        where = partition_predicate(day)
        day_df = silver.filter(where)
        if day_df.isEmpty():
            logger.warning(
                "Sin eventos en Kafka para %s — se omite (¿retención expirada? "
                "reprocesa desde el origen con run_backfill).",
                day,
            )
            continue
        write_silver(day_df, silver_path, mode="overwrite", replace_where=where)
        logger.info("Silver reprocesado desde Kafka para %s", day)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Reprocesa una ventana releyendo de Kafka (batch) -> Silver"
    )
    parser.add_argument("--start", required=True, help="Primer día inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="Día de corte exclusivo (YYYY-MM-DD)")
    parser.add_argument("--topic", default="github.push.raw")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--silver-path", default="data/silver")
    args = parser.parse_args()

    run_reprocess_window(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end),
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        silver_path=args.silver_path,
    )
