import logging
from typing import Any

from pyspark.sql import SparkSession

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


def _build_trigger(mode: str, interval: str) -> dict[str, Any]:
    """Traduce el modo de trigger de la CLI a las opciones de writeStream.trigger().

    Args:
        mode: 'available-now' (procesa lo disponible y termina -> backfill) o
            'processing-time' (deja la query viva, disparando cada `interval`,
            para que Prometheus pueda scrapear un proceso que no se muere).
        interval: Intervalo del modo processing-time (p. ej. '5 seconds').
            Ignorado en available-now.

    Returns:
        Kwargs para df.writeStream.trigger(**...): {"availableNow": True} o
        {"processingTime": interval}.
    """
    if mode == "processing-time":
        return {"processingTime": interval}
    return {"availableNow": True}


def _enable_observability(spark: SparkSession, port: int) -> None:
    """Registra el listener de Prometheus y abre el endpoint de métricas.

    Import perezoso: si el extra `observability` no está instalado
    (prometheus-client falta), avisa y sigue SIN métricas en vez de abortar el
    stream. La observabilidad es un accesorio opt-in, nunca una dependencia que
    pueda tumbar un backfill que por lo demás funcionaría.

    Args:
        spark: SparkSession viva a la que engancharle el listener.
        port: Puerto donde exponer /metrics para que Prometheus lo scrapee.
    """
    try:
        from dev_trends.observability.prometheus import (
            PrometheusStreamingListener,
            start_metrics_server,
        )
    except ImportError:
        logger.warning(
            "Extra 'observability' no instalado "
            "(pip install -e '.[observability]'); el stream corre sin métricas."
        )
        return

    spark.streams.addListener(PrometheusStreamingListener())
    start_metrics_server(port)


def run_kafka_to_bronze(
    topic: str,
    bootstrap_servers: str,
    bronze_path: str,
    checkpoint_path: str,
    trigger: dict[str, Any] | None = None,
    metrics_port: int | None = None,
) -> None:
    """Pipeline streaming Kafka -> Bronze (Delta), crudo sin transformar.

    Con trigger availableNow (defecto) procesa lo disponible y termina; con
    processingTime la query queda viva. Si metrics_port se indica, expone las
    métricas del stream a Prometheus en ese puerto.
    """
    spark = build_spark("dev-trends-stream-bronze", extra_packages=[KAFKA_PACKAGE])

    if metrics_port is not None:
        _enable_observability(spark, metrics_port)

    source = read_kafka_stream(spark, topic, bootstrap_servers)
    bronze = to_bronze(source)
    query = write_bronze_stream(bronze, bronze_path, checkpoint_path, trigger=trigger)
    query.awaitTermination()
    logger.info("Bronze streaming completado: topic '%s' -> %s", topic, bronze_path)


def run_bronze_to_silver(
    bronze_path: str,
    silver_path: str,
    checkpoint_path: str,
    trigger: dict[str, Any] | None = None,
    metrics_port: int | None = None,
) -> None:
    """Pipeline streaming Bronze -> Silver (Delta).

    Lee Bronze como fuente Delta, parsea el value crudo y reutiliza
    normalize_events (intacto). No necesita el conector de Kafka.
    Activa s3a solo si Silver apunta a S3. Si metrics_port se indica, expone las
    métricas del stream a Prometheus en ese puerto.
    """
    spark = build_spark(
        "dev-trends-stream-silver",
        enable_s3a=silver_path.startswith("s3a://"),
    )

    if metrics_port is not None:
        _enable_observability(spark, metrics_port)

    bronze = read_bronze_stream(spark, bronze_path)
    parsed = parse_bronze_value(bronze)
    mapping = build_technology_mapping(spark)
    silver = normalize_events(parsed, mapping)
    query = write_silver_stream(silver, silver_path, checkpoint_path, trigger=trigger)
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
    parser.add_argument(
        "--trigger",
        choices=["available-now", "processing-time"],
        default="available-now",
        help="available-now: procesa lo disponible y termina (backfill). "
        "processing-time: deja la query viva para observabilidad.",
    )
    parser.add_argument(
        "--trigger-interval",
        default="5 seconds",
        help="Intervalo del trigger processing-time (ignorado en available-now).",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="Puerto del endpoint Prometheus (sugerido: 9101 bronze, 9102 silver). "
        "Sin él, no se exponen métricas.",
    )
    args = parser.parse_args()

    trigger = _build_trigger(args.trigger, args.trigger_interval)

    if args.stage == "bronze":
        run_kafka_to_bronze(
            topic=args.topic,
            bootstrap_servers=args.bootstrap_servers,
            bronze_path=args.bronze_path,
            checkpoint_path=args.bronze_checkpoint,
            trigger=trigger,
            metrics_port=args.metrics_port,
        )
    else:
        run_bronze_to_silver(
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
            checkpoint_path=args.silver_checkpoint,
            trigger=trigger,
            metrics_port=args.metrics_port,
        )
