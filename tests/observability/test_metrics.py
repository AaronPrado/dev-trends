import math
from typing import Any

import pytest
from pyspark.sql.streaming.listener import StreamingQueryProgress

from dev_trends.observability.metrics import extract_metrics

_QUERY_ID = "8c1e1b0e-0000-4000-8000-000000000001"
_RUN_ID = "8c1e1b0e-0000-4000-8000-000000000002"


def _source(description: str, metrics: dict[str, str] | None = None) -> dict[str, Any]:
    """Construye el dict de una fuente tal y como lo serializa Spark."""
    return {
        "description": description,
        "startOffset": "0",
        "endOffset": "10",
        "latestOffset": "10",
        "numInputRows": 10,
        "inputRowsPerSecond": 5.0,
        "processedRowsPerSecond": 5.0,
        "metrics": metrics or {},
    }


def _progress(
    *,
    name: str | None = "silver",
    batch_duration: int | None = 2000,
    num_input_rows: int = 120,
    input_rows_per_second: float = 60.0,
    processed_rows_per_second: float = 60.0,
    duration_ms: dict[str, int] | None = None,
    sources: list[dict[str, Any]] | None = None,
) -> StreamingQueryProgress:
    """Progress real de PySpark construido desde su forma JSON.

    Se usa la API pública `fromJson` en lugar de un doble de test: si la forma
    del progress cambia entre versiones de Spark, estos tests lo detectan.
    """
    payload: dict[str, Any] = {
        "id": _QUERY_ID,
        "runId": _RUN_ID,
        "name": name,
        "timestamp": "2026-07-10T10:00:00.000Z",
        "batchId": 7,
        "batchDuration": batch_duration,
        "durationMs": duration_ms if duration_ms is not None else {"addBatch": 1500},
        "stateOperators": [],
        "sources": sources if sources is not None else [_source("KafkaV2[Subscribe[t]]")],
        "sink": {"description": "DeltaSink[data/silver]", "numOutputRows": num_input_rows},
        "numInputRows": num_input_rows,
        "inputRowsPerSecond": input_rows_per_second,
        "processedRowsPerSecond": processed_rows_per_second,
    }
    return StreamingQueryProgress.fromJson(payload)


def test_extract_metrics_convierte_milisegundos_a_segundos():
    metrics = extract_metrics(_progress(batch_duration=2500, duration_ms={"addBatch": 1500}))

    assert metrics.batch_duration_seconds == 2.5
    assert metrics.phase_durations_seconds == {"addBatch": 1.5}


def test_extract_metrics_copia_las_filas_y_los_ritmos():
    metrics = extract_metrics(
        _progress(num_input_rows=120, input_rows_per_second=60.0, processed_rows_per_second=40.0)
    )

    assert metrics.query == "silver"
    assert metrics.batch_id == 7
    assert metrics.num_input_rows == 120
    assert metrics.input_rows_per_second == 60.0
    assert metrics.processed_rows_per_second == 40.0


def test_extract_metrics_toma_el_lag_de_la_fuente_kafka():
    sources = [_source("KafkaV2[Subscribe[github.push.raw]]", {"maxOffsetsBehindLatest": "42"})]

    metrics = extract_metrics(_progress(sources=sources))

    assert metrics.kafka_max_offsets_behind_latest == 42.0


def test_extract_metrics_toma_el_lag_de_la_fuente_mas_rezagada():
    sources = [
        _source("KafkaV2[Subscribe[a]]", {"maxOffsetsBehindLatest": "42"}),
        _source("KafkaV2[Subscribe[b]]", {"maxOffsetsBehindLatest": "1300"}),
    ]

    metrics = extract_metrics(_progress(sources=sources))

    assert metrics.kafka_max_offsets_behind_latest == 1300.0


def test_extract_metrics_sin_fuente_kafka_no_reporta_lag():
    """Bronze->Silver lee Delta: no tiene lag de topic. None != 0 (0 mentiría)."""
    sources = [_source("DeltaSource[data/bronze_delta]")]

    metrics = extract_metrics(_progress(sources=sources))

    assert metrics.kafka_max_offsets_behind_latest is None


@pytest.mark.parametrize("valor", [math.nan, math.inf, -math.inf])
def test_extract_metrics_sanea_los_ritmos_no_finitos(valor: float):
    """Un micro-batch vacío divide 0/0: publicar NaN dibujaría un hueco en Grafana."""
    metrics = extract_metrics(
        _progress(input_rows_per_second=valor, processed_rows_per_second=valor)
    )

    assert metrics.input_rows_per_second == 0.0
    assert metrics.processed_rows_per_second == 0.0


def test_extract_metrics_sanea_la_duracion_ausente():
    metrics = extract_metrics(_progress(batch_duration=None))

    assert metrics.batch_duration_seconds == 0.0


def test_extract_metrics_usa_el_id_cuando_la_query_no_tiene_nombre():
    metrics = extract_metrics(_progress(name=None))

    assert metrics.query == _QUERY_ID
