"""Puente entre el progress de Structured Streaming y Prometheus."""

import logging
from typing import TYPE_CHECKING

from pyspark.sql.streaming.listener import (
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

from dev_trends.observability.metrics import extract_metrics

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

# Prefijo común de todas las series.
_NS = "dev_trends_streaming"


class PrometheusStreamingListener(StreamingQueryListener):
    """Listener que publica las métricas de cada micro-batch como gauges.

    Un gauge por métrica, etiquetado por `query`, de modo que un solo listener
    sirve a las dos etapas (kafka-to-bronze y bronze-to-silver) sin colisión.
    """

    def __init__(self, registry: "CollectorRegistry | None" = None) -> None:
        from prometheus_client import REGISTRY, Gauge

        # En producción se registra en el REGISTRY global (lo que expone
        # start_http_server); los tests inyectan uno aislado para no colisionar
        # en el registro global entre casos.
        reg = registry if registry is not None else REGISTRY

        self._batch_id = Gauge(
            f"{_NS}_last_batch_id",
            "Id del último micro-batch procesado.",
            ["query"],
            registry=reg,
        )
        self._batch_duration = Gauge(
            f"{_NS}_batch_duration_seconds",
            "Latencia de proceso del último micro-batch.",
            ["query"],
            registry=reg,
        )
        self._input_rows = Gauge(
            f"{_NS}_input_rows",
            "Filas leídas en el último micro-batch.",
            ["query"],
            registry=reg,
        )
        self._input_rate = Gauge(
            f"{_NS}_input_rows_per_second",
            "Ritmo de llegada de datos a la fuente.",
            ["query"],
            registry=reg,
        )
        self._process_rate = Gauge(
            f"{_NS}_processed_rows_per_second",
            "Ritmo de proceso. Por debajo del de entrada de forma sostenida, el lag crece.",
            ["query"],
            registry=reg,
        )
        self._phase_duration = Gauge(
            f"{_NS}_phase_duration_seconds",
            "Duración de cada fase interna del micro-batch.",
            ["query", "phase"],
            registry=reg,
        )
        self._kafka_lag = Gauge(
            f"{_NS}_kafka_max_offsets_behind_latest",
            "Offsets que le faltan al stream para alcanzar el final del topic.",
            ["query"],
            registry=reg,
        )

    # Los tres onQuery* son overrides de la interfaz StreamingQueryListener de
    # PySpark: Spark los invoca por estos nombres camelCase vía Py4J, así que no
    # pueden pasar a snake_case. De ahí el noqa: N802 (naming) en cada uno.
    def onQueryStarted(self, event: QueryStartedEvent) -> None:  # noqa: N802
        logger.info("Streaming query iniciada: %s", event.name or event.id)

    def onQueryProgress(self, event: QueryProgressEvent) -> None:  # noqa: N802
        m = extract_metrics(event.progress)
        self._batch_id.labels(m.query).set(m.batch_id)
        self._batch_duration.labels(m.query).set(m.batch_duration_seconds)
        self._input_rows.labels(m.query).set(m.num_input_rows)
        self._input_rate.labels(m.query).set(m.input_rows_per_second)
        self._process_rate.labels(m.query).set(m.processed_rows_per_second)
        for phase, seconds in m.phase_durations_seconds.items():
            self._phase_duration.labels(m.query, phase).set(seconds)
        if m.kafka_max_offsets_behind_latest is not None:
            self._kafka_lag.labels(m.query).set(m.kafka_max_offsets_behind_latest)

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:  # noqa: N802
        if event.exception:
            logger.error("Streaming query terminada con error: %s", event.exception)
        else:
            logger.info("Streaming query terminada: %s", event.id)


def start_metrics_server(port: int) -> None:
    """Arranca el endpoint HTTP que Prometheus scrapea (/metrics en `port`)."""
    from prometheus_client import start_http_server

    start_http_server(port)
    logger.info("Servidor de métricas Prometheus escuchando en :%d", port)
