from typing import Any

import pytest

prometheus_client = pytest.importorskip("prometheus_client")

from pyspark.sql.streaming.listener import (  # noqa: E402
    QueryProgressEvent,
    StreamingQueryProgress,
)

from dev_trends.observability.prometheus import PrometheusStreamingListener  # noqa: E402

_METRIC = "dev_trends_streaming"


def _progress(*, name: str, sources: list[dict[str, Any]]) -> StreamingQueryProgress:
    return StreamingQueryProgress.fromJson(
        {
            "id": "8c1e1b0e-0000-4000-8000-000000000001",
            "runId": "8c1e1b0e-0000-4000-8000-000000000002",
            "name": name,
            "timestamp": "2026-07-10T10:00:00.000Z",
            "batchId": 3,
            "batchDuration": 2000,
            "durationMs": {"addBatch": 1500},
            "stateOperators": [],
            "sources": sources,
            "sink": {"description": "DeltaSink", "numOutputRows": 50},
            "numInputRows": 50,
            "inputRowsPerSecond": 25.0,
            "processedRowsPerSecond": 25.0,
        }
    )


def _kafka_source(lag: str) -> dict[str, Any]:
    return {
        "description": "KafkaV2[Subscribe[github.push.raw]]",
        "startOffset": "0",
        "endOffset": "50",
        "latestOffset": "50",
        "numInputRows": 50,
        "inputRowsPerSecond": 25.0,
        "processedRowsPerSecond": 25.0,
        "metrics": {"maxOffsetsBehindLatest": lag},
    }


def _delta_source() -> dict[str, Any]:
    return {
        "description": "DeltaSource[data/bronze_delta]",
        "startOffset": "0",
        "endOffset": "50",
        "latestOffset": "50",
        "numInputRows": 50,
        "inputRowsPerSecond": 25.0,
        "processedRowsPerSecond": 25.0,
        "metrics": {},
    }


@pytest.fixture
def registry():
    """Registro aislado por test: evita colisiones en el REGISTRY global."""
    return prometheus_client.CollectorRegistry()


def test_publica_los_gauges_del_progress(registry):
    listener = PrometheusStreamingListener(registry=registry)

    listener.onQueryProgress(
        QueryProgressEvent(_progress(name="kafka-to-bronze", sources=[_kafka_source("7")]))
    )

    labels = {"query": "kafka-to-bronze"}
    assert registry.get_sample_value(f"{_METRIC}_last_batch_id", labels) == 3
    assert registry.get_sample_value(f"{_METRIC}_batch_duration_seconds", labels) == 2.0
    assert registry.get_sample_value(f"{_METRIC}_input_rows", labels) == 50
    assert registry.get_sample_value(f"{_METRIC}_kafka_max_offsets_behind_latest", labels) == 7.0


def test_publica_la_duracion_por_fase(registry):
    listener = PrometheusStreamingListener(registry=registry)

    listener.onQueryProgress(
        QueryProgressEvent(_progress(name="kafka-to-bronze", sources=[_kafka_source("0")]))
    )

    value = registry.get_sample_value(
        f"{_METRIC}_phase_duration_seconds", {"query": "kafka-to-bronze", "phase": "addBatch"}
    )
    assert value == 1.5


def test_sin_fuente_kafka_no_emite_serie_de_lag(registry):
    """Bronze->Silver lee Delta: la serie de lag no debe existir (no vale 0)."""
    listener = PrometheusStreamingListener(registry=registry)

    listener.onQueryProgress(
        QueryProgressEvent(_progress(name="bronze-to-silver", sources=[_delta_source()]))
    )

    labels = {"query": "bronze-to-silver"}
    assert registry.get_sample_value(f"{_METRIC}_input_rows", labels) == 50
    assert registry.get_sample_value(f"{_METRIC}_kafka_max_offsets_behind_latest", labels) is None


def test_un_listener_sirve_a_las_dos_queries(registry):
    """El mismo listener etiqueta por query: una serie por etapa, sin colisión."""
    listener = PrometheusStreamingListener(registry=registry)

    listener.onQueryProgress(
        QueryProgressEvent(_progress(name="kafka-to-bronze", sources=[_kafka_source("7")]))
    )
    listener.onQueryProgress(
        QueryProgressEvent(_progress(name="bronze-to-silver", sources=[_delta_source()]))
    )

    assert registry.get_sample_value(f"{_METRIC}_input_rows", {"query": "kafka-to-bronze"}) == 50
    assert registry.get_sample_value(f"{_METRIC}_input_rows", {"query": "bronze-to-silver"}) == 50
