import gzip
import json
import logging
from datetime import date
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer

from dev_trends.ingestion.gharchive import download_range

logger = logging.getLogger(__name__)

PUSH_EVENT_TYPE = "PushEvent"


def extract_push_event(raw_line: bytes) -> tuple[bytes, bytes] | None:
    """Convierte una línea cruda de GH Archive en un mensaje de Kafka.

    Args:
        raw_line: Una línea del .json.gz (un evento JSON en bytes).

    Returns:
        (key, value) si la línea es un PushEvent, donde key = nombre del repo
        en bytes (para particionar por repositorio) y value = la línea cruda
        intacta. None si el evento no es un PushEvent.

    Raises:
        json.JSONDecodeError: si la línea no es JSON válido. Lo maneja el
            llamante (publish_file) para contar y omitir líneas malformadas.
    """
    event = json.loads(raw_line)
    if event.get("type") != PUSH_EVENT_TYPE:
        return None
    repo_name = (event.get("repo") or {}).get("name", "")
    return repo_name.encode(), raw_line


def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Callback de entrega de Kafka: registra los fallos de envío."""
    if err is not None:
        logger.error("Fallo al entregar mensaje a Kafka: %s", err)


def _produce(producer: Producer, topic: str, key: bytes, value: bytes) -> None:
    """Encola un mensaje gestionando la contrapresión de la cola local."""
    while True:
        try:
            producer.produce(topic, value=value, key=key, on_delivery=_delivery_report)
            producer.poll(0)
            return
        except BufferError:
            # Cola local de librdkafka llena: sirve callbacks pendientes y reintenta.
            logger.debug("Cola del productor llena; esperando a que se vacíe...")
            producer.poll(0.5)


def publish_file(producer: Producer, topic: str, path: Path) -> tuple[int, int]:
    """Publica los PushEvent de un .json.gz al topic.

    Lee el fichero línea a línea (no lo carga entero en memoria). Las líneas con
    JSON malformado se cuentan y se omiten, sin abortar la publicación.

    Returns:
        (publicados, malformados): nº de PushEvent enviados y nº de líneas con
        JSON inválido descartadas.
    """
    published = 0
    malformed = 0
    with gzip.open(path, "rb") as fh:
        for raw_line in fh:
            try:
                message = extract_push_event(raw_line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if message is None:
                continue
            key, value = message
            _produce(producer, topic, key, value)
            published += 1
    return published, malformed


def run_producer(
    event_date: date,
    bronze_dir: Path,
    topic: str,
    bootstrap_servers: str,
    hours: range = range(24),
) -> None:
    """Descarga los .json.gz de una fecha y publica sus PushEvent a Kafka.

    Args:
        event_date: Fecha a publicar.
        bronze_dir: Directorio donde aterrizar los .json.gz (landing del crudo).
        topic: Topic de Kafka destino.
        bootstrap_servers: Brokers de Kafka (e.g. 'localhost:9092').
        hours: Rango de horas a procesar (por defecto las 24 del día).
    """
    paths = download_range(event_date, bronze_dir, hours)
    if not paths:
        logger.warning("Sin ficheros disponibles para %s — nada que publicar.", event_date)
        return

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    total_published = 0
    total_malformed = 0
    for path in paths:
        published, malformed = publish_file(producer, topic, path)
        total_published += published
        total_malformed += malformed
        logger.info("%s: %d PushEvent publicados (%d malformados)", path.name, published, malformed)

    producer.flush()
    logger.info(
        "Total: %d PushEvent publicados en '%s' (%d líneas malformadas).",
        total_published,
        topic,
        total_malformed,
    )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Publica PushEvent de GH Archive a Kafka")
    parser.add_argument("--date", required=True, help="Fecha a publicar (YYYY-MM-DD)")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--topic", default="github.push.raw")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--hours", default="0-23", help="Rango de horas, e.g. '0-0' para 1 hora")
    args = parser.parse_args()

    start, end = (int(h) for h in args.hours.split("-"))
    run_producer(
        event_date=date.fromisoformat(args.date),
        bronze_dir=Path(args.bronze_dir),
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        hours=range(start, end + 1),
    )
