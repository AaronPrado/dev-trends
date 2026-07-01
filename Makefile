.DEFAULT_GOAL := help

COMPOSE := docker compose -f docker/docker-compose.yml

TOPIC := github.push.raw

DATA_ROOT := $(CURDIR)/data

S3_ROOT   := s3a://$(DEV_TRENDS_S3_BUCKET)
DBT_S3    := DEV_TRENDS_DATA_ROOT=$(S3_ROOT) AWS_PROFILE=dev-trends-pipeline dbt
DBT_PARSE := DEV_TRENDS_DATA_ROOT=$(DATA_ROOT) dbt

TF := terraform -chdir=infra

.PHONY: help install lint format test check hooks up down pipeline clean topic produce stream-bronze dbt-build dbt-test dbt-parse

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

up:  ## Levanta Kafka en local con Docker Compose
	$(COMPOSE) up -d

down:  ## Detiene Kafka y limpia los contenedores
	$(COMPOSE) down

pipeline:  ## Lanza el pipeline batch. Uso: make pipeline DATE=2024-01-15 HOURS=0-0
	python -m dev_trends.pipeline.batch --date $(DATE) --hours $(HOURS)

clean:  ## Borra cachés de herramientas y artefactos de Python
	rm -rf .ruff_cache .pytest_cache .mypy_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +

topic:  ## Crea el topic de PushEvents
	docker exec dev-trends-kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions 1 --replication-factor 1

produce:  ## Publica PushEvent a Kafka. Uso: make produce DATE=2024-01-15 HOURS=0-0
	python -m dev_trends.ingestion.producer --date $(DATE) --hours $(HOURS) --topic $(TOPIC)

stream-bronze:  ## Streaming Kafka -> Bronze (Delta)
	python -m dev_trends.pipeline.streaming --stage bronze --topic $(TOPIC)

stream-silver:  ## Streaming Bronze -> Silver (Delta)
	python -m dev_trends.pipeline.streaming --stage silver

dbt-build: guard-DEV_TRENDS_S3_BUCKET  ## Construye el Gold en S3 (seeds + modelos + tests)
	cd dbt && $(DBT_S3) build --profiles-dir .

dbt-test: guard-DEV_TRENDS_S3_BUCKET  ## Corre los tests dbt sobre el Gold en S3
	cd dbt && $(DBT_S3) test --profiles-dir .

dbt-parse:  ## Valida el proyecto dbt sin conexión (refs, Jinja, YAML)
	cd dbt && $(DBT_PARSE) parse --profiles-dir .

guard-%:
	@if [ -z "$($*)" ]; then echo "ERROR: define $* (p.ej. export DEV_TRENDS_S3_BUCKET=dev-trends-medallion-<account_id>)"; exit 1; fi

tf-validate:  ## Valida la infra Terraform sin credenciales (lo que corre la CI)
	$(TF) fmt -check
	$(TF) init -backend=false
	$(TF) validate

tf-plan:  ## Calcula el plan de Terraform (requiere credenciales AWS)
	$(TF) plan

tf-apply:  ## Aplica la infra Terraform (requiere credenciales AWS)
	$(TF) apply

athena-register: guard-DEV_TRENDS_S3_BUCKET  ## Registra el Gold (Delta) en Glue para Athena
	AWS_PROFILE=dev-trends-pipeline scripts/athena_register_gold.sh