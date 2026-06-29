import logging

from dev_trends.ingestion.kafka_source import read_kafka_stream
from dev_trends.spark_session import KAFKA_PACKAGE, build_spark
from dev_trends.storage.bronze import to_bronze, write_bronze_stream

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


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Streaming Kafka -> Bronze (Delta)")
    parser.add_argument("--topic", default="github.push.raw")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--bronze-path", default="data/bronze_delta")
    parser.add_argument("--checkpoint-path", default="data/checkpoints/bronze")
    args = parser.parse_args()

    run_kafka_to_bronze(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        bronze_path=args.bronze_path,
        checkpoint_path=args.checkpoint_path,
    )
