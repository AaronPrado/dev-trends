from dev_trends.pipeline.streaming import _build_trigger


def test_build_trigger_available_now_es_el_defecto():
    assert _build_trigger("available-now", "5 seconds") == {"availableNow": True}


def test_build_trigger_processing_time_usa_el_intervalo():
    assert _build_trigger("processing-time", "10 seconds") == {"processingTime": "10 seconds"}


def test_build_trigger_modo_desconocido_cae_en_available_now():
    """Ante un modo inesperado, el comportamiento seguro (terminar), no un job colgado."""
    assert _build_trigger("otra-cosa", "5 seconds") == {"availableNow": True}
