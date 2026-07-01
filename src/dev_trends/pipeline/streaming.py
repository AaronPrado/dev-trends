import logging

from dev_trends.ingestion.kafka_source import read_kafka_stream
from dev_trends.spark_session import KAFKA_PACKAGE, build_spark
from dev_trends.storage.bronze import (
    parse_bronze_value,
    read_bronze_stream,
    to_bronze,
    write_bronze_stream,
)
from dev_trends.storage.silver import write_silver_stream
from dev_trends.transform.normalize import normalize_events
from dev_trends.transform.technologies import build_technology_mapping

logger = logging.getLogger(__name__)


def run_kafka_to_bronze(
    topic: str,
    bootstrap_servers: str,
    bronze_path: str,
    checkpoint_path: str,
) -> None:
    """Pipeline streaming Kafka -> Bronze (Delta), crudo sin transformar.

    Lee todo lo disponible en el topic (trigger availableNow) y termina.
    """
    spark = build_spark("dev-trends-stream-bronze", extra_packages=[KAFKA_PACKAGE])

    source = read_kafka_stream(spark, topic, bootstrap_servers)
    bronze = to_bronze(source)
    query = write_bronze_stream(bronze, bronze_path, checkpoint_path)
    query.awaitTermination()
    logger.info("Bronze streaming completado: topic '%s' -> %s", topic, bronze_path)


def run_bronze_to_silver(
    bronze_path: str,
    silver_path: str,
    checkpoint_path: str,
) -> None:
    """Pipeline streaming Bronze -> Silver (Delta).

    Lee Bronze como fuente Delta, parsea el value crudo y reutiliza
    normalize_events (intacto). No necesita el conector de Kafka.
    Activa s3a solo si Silver apunta a S3.
    """
    spark = build_spark(
        "dev-trends-stream-silver",
        enable_s3a=silver_path.startswith("s3a://"),
    )

    bronze = read_bronze_stream(spark, bronze_path)
    parsed = parse_bronze_value(bronze)
    mapping = build_technology_mapping(spark)
    silver = normalize_events(parsed, mapping)
    query = write_silver_stream(silver, silver_path, checkpoint_path)
    query.awaitTermination()
    logger.info("Silver streaming completado: %s -> %s", bronze_path, silver_path)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Pipelines de streaming (Bronze/Silver)")
    parser.add_argument("--stage", choices=["bronze", "silver"], required=True)
    parser.add_argument("--topic", default="github.push.raw")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--bronze-path", default="data/bronze_delta")
    parser.add_argument("--silver-path", default="data/silver")
    parser.add_argument("--bronze-checkpoint", default="data/checkpoints/bronze")
    parser.add_argument("--silver-checkpoint", default="data/checkpoints/silver")
    args = parser.parse_args()

    if args.stage == "bronze":
        run_kafka_to_bronze(
            topic=args.topic,
            bootstrap_servers=args.bootstrap_servers,
            bronze_path=args.bronze_path,
            checkpoint_path=args.bronze_checkpoint,
        )
    else:
        run_bronze_to_silver(
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
            checkpoint_path=args.silver_checkpoint,
        )
