.DEFAULT_GOAL := help

.PHONY: help install lint format test check hooks up down pipeline clean

help:  ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Instala el paquete en editable + tooling dev y activa pre-commit
	pip install -e ".[dev]"
	pre-commit install

lint:  ## Comprueba estilo y formato SIN modificar (lo que corre la CI)
	ruff check .
	ruff format --check .

format:  ## Formatea y autocorrige con ruff
	ruff format .
	ruff check --fix .

test:  ## Ejecuta los tests
	pytest

check: lint test  ## lint + test: puerta local equivalente a la CI

hooks:  ## Pasa todos los hooks de pre-commit sobre el repo
	pre-commit run --all-files

up:  ## [Fase 2] Levanta el entorno local (Kafka, Spark) con Docker Compose
	@echo "Pendiente: docker compose up -d (llega en la fase de Kafka)"

down:  ## [Fase 2] Detiene el entorno local
	@echo "Pendiente: docker compose down (llega en la fase de Kafka)"

pipeline:  ## [Fase 1] Lanza el pipeline batch de Spark
	@echo "Pendiente: ejecutar src/dev_trends/pipeline/batch.py (llega en la fase de Spark)"

clean:  ## Borra cachés de herramientas y artefactos de Python
	rm -rf .ruff_cache .pytest_cache .mypy_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +

pipeline:  ## Lanza el pipeline batch. Uso: make pipeline DATE=2024-01-15 HOURS=0-0
	python -m dev_trends.pipeline.batch --date $(DATE) --hours $(HOURS)
