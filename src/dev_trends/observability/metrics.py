import math
from dataclasses import dataclass

from pyspark.sql.streaming.listener import StreamingQueryProgress

# Clave que publica el conector spark-sql-kafka en SourceProgress.metrics:
# KafkaMicroBatchStream implementa ReportsSourceMetrics y reporta
# min/max/avgOffsetsBehindLatest. Las fuentes que no son Kafka -- p. ej. la
# fuente Delta del stream Bronze->Silver -- no traen esta clave.
KAFKA_LAG_METRIC = "maxOffsetsBehindLatest"


@dataclass(frozen=True)
class StreamingMetrics:
    """Instantánea de un micro-batch, lista para publicarse como gauges.

    Attributes:
        query: Nombre de la query (o su id si no se le puso nombre).
        batch_id: Identificador incremental del micro-batch.
        batch_duration_seconds: Latencia de proceso del micro-batch completo.
        num_input_rows: Filas leídas en este micro-batch.
        input_rows_per_second: Ritmo de llegada de datos a la fuente.
        processed_rows_per_second: Ritmo de proceso. Si cae por debajo del
            ritmo de entrada de forma sostenida, el lag crece sin techo.
        phase_durations_seconds: Desglose de la latencia por fase interna
            (addBatch, latestOffset, queryPlanning, walCommit...).
        kafka_max_offsets_behind_latest: Offsets que le faltan al stream para
            alcanzar el final del topic. None si ninguna fuente es Kafka.
    """

    query: str
    batch_id: int
    batch_duration_seconds: float
    num_input_rows: int
    input_rows_per_second: float
    processed_rows_per_second: float
    phase_durations_seconds: dict[str, float]
    kafka_max_offsets_behind_latest: float | None


def _finite(value: float | None) -> float:
    """Sanea un valor de Spark a un float publicable.

    Spark reporta NaN o Infinity en los ritmos por segundo cuando el
    micro-batch no tiene filas o dura 0 ms (división por cero). Publicar NaN
    en Prometheus produce huecos en las gráficas que se confunden con caídas
    del job, así que se normalizan a 0.
    """
    if value is None or not math.isfinite(value):
        return 0.0
    return float(value)


def _kafka_lag(progress: StreamingQueryProgress) -> float | None:
    """Extrae el lag de la(s) fuente(s) Kafka del progress, si las hay.

    Se queda con el máximo: con varias particiones o fuentes, lo que importa
    es la más rezagada, no la media.
    """
    lags = [
        float(source.metrics[KAFKA_LAG_METRIC])
        for source in progress.sources
        if KAFKA_LAG_METRIC in source.metrics
    ]
    return max(lags) if lags else None


def extract_metrics(progress: StreamingQueryProgress) -> StreamingMetrics:
    """Traduce el progress de un micro-batch de Spark a métricas publicables.

    Args:
        progress: Progress que Spark entrega en onQueryProgress.

    Returns:
        Las métricas del micro-batch, con las unidades ya en segundos.
    """
    return StreamingMetrics(
        query=progress.name or str(progress.id),
        batch_id=progress.batchId,
        batch_duration_seconds=_finite(progress.batchDuration) / 1000.0,
        num_input_rows=progress.numInputRows,
        input_rows_per_second=_finite(progress.inputRowsPerSecond),
        processed_rows_per_second=_finite(progress.processedRowsPerSecond),
        phase_durations_seconds={
            phase: millis / 1000.0 for phase, millis in progress.durationMs.items()
        },
        kafka_max_offsets_behind_latest=_kafka_lag(progress),
    )
