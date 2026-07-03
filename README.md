# dev-trends

> Plataforma de datos que mide la **actividad de desarrollo** y la **adopción** de
> tecnologías de software a partir de fuentes públicas (eventos de GitHub vía GH
> Archive, y descargas de PyPI vía BigQuery). Pipeline de Data Engineering de
> extremo a extremo: ingesta en streaming y batch, procesamiento distribuido,
> arquitectura medallion sobre un data lake en AWS y modelado analítico.

> **Estado: V1 completa (etiquetada en `v1.0.0`); ampliándose.** El pipeline base
> funciona de punta a punta con datos reales: GH Archive → Kafka → Spark Structured
> Streaming → Silver/Gold en S3 (Delta) → dbt → Athena → Streamlit. Ya integrada una
> **segunda fuente (PyPI)** para medir adopción; el resto de ampliaciones (dashboard
> en Power BI, calidad de datos, observabilidad) están en el roadmap más abajo.

---

## Qué es

`dev-trends` combina dos señales públicas por tecnología:

- **Actividad de desarrollo** — eventos de GitHub (pushes, pull requests, releases…)
  vía GH Archive: cuánto se *construye* una herramienta.
- **Adopción** — descargas de paquetes de PyPI vía BigQuery: cuánto se *usa*.

Cruzando ambas responde preguntas que ninguna fuente contesta sola: qué
herramientas se construyen mucho pero se usan poco (emergentes), cuáles se usan
mucho con menos desarrollo (maduras), y hacia dónde va cada una en el tiempo.

El proyecto está diseñado como demostración de un pipeline de Data Engineering
moderno de principio a fin, con las prácticas que se esperan en producción:
arquitectura por capas, transición de batch a streaming, infraestructura como
código y modelado analítico desacoplado.

> **Nota sobre las métricas.** GitHub mide *actividad de desarrollo*
> (commits, PRs, releases), no adopción en producción. Las descargas de PyPI son un
> proxy de *uso* inflado por CI/CD, mirrors y bots: se leen como **tendencia
> relativa por paquete**, no como recuento de usuarios. Por eso se reportan **por
> fuente** y se comparan por *ranking*, no por magnitud absoluta.

---

## Stack

| Capa | Tecnología |
|---|---|
| Fuentes | GH Archive (eventos de GitHub) · BigQuery (descargas de PyPI) |
| Ingesta | Apache Kafka (streaming) · cliente de BigQuery (agregación en origen) |
| Procesamiento | Apache Spark (Structured Streaming) |
| Almacenamiento | AWS S3 + Delta Lake (arquitectura medallion) |
| Catálogo | AWS Glue Data Catalog |
| Modelado | dbt |
| Consulta | AWS Athena |
| Visualización | Streamlit (Power BI, en el roadmap) |
| Infraestructura | Terraform |
| Orquestación local | Docker Compose |

**Entorno híbrido:** el cómputo (Kafka, Spark, dbt) corre en local sobre Docker;
el almacenamiento y la consulta (S3, Glue, Athena) viven en AWS.

---

## Arquitectura

```
GH Archive ──────▶ Kafka ──▶ Spark ─┐
                                     ├─▶ S3 / Delta (medallion) ──▶ Athena ──▶ Dashboard
BigQuery (PyPI) ──────────▶ Spark ──┘   Bronze → Silver → Gold
                                                      (dbt)

           Terraform aprovisiona la infraestructura AWS (S3, Glue, Athena, IAM)
```

- **Bronze:** eventos crudos de GH Archive, tal cual (solo la fuente de GitHub).
- **Silver:** datos normalizados y particionados por fecha — un evento de GitHub por
  fila, y las descargas de PyPI agregadas por día y paquete.
- **Gold:** hechos analíticos a grano diario modelados con dbt — `fact_github_activity`
  (actividad) y `fact_pypi_downloads` (adopción), sobre dimensiones conformadas.

### Esquema Silver

Un evento por fila, particionado por fecha (`year`/`month`/`day`). Congelado desde
la Fase 1 para que dbt pudiera construirse encima sin romper capas anteriores:

| Columna | Tipo | Descripción |
|---|---|---|
| `event_id` | string | Id del evento en GitHub, único. |
| `technology` | string | Tecnología asociada al repositorio (`airflow`, `spark`, `dbt`, `dagster`, `prefect`). |
| `event_type` | string | Tipo de evento normalizado (`push`, `pull_request`, `release`, `watch`). |
| `repository` | string | Repositorio de origen (`org/repo`). |
| `organization` | string | Organización, derivada de `repository`. |
| `created_at` | timestamp | Momento del evento en GitHub. |
| `year` / `month` / `day` | int | Partición derivada de `created_at`. |

No incluye el actor del evento (PII, y no aporta a la pregunta de actividad por
tecnología).

### Esquema Silver de descargas PyPI

Descargas agregadas por día y paquete (la agregación fina se hace en BigQuery; la
suma por tecnología la construye dbt en Gold), particionado por fecha:

| Columna | Tipo | Descripción |
|---|---|---|
| `download_date` | date | Día de las descargas. |
| `technology` | string | Tecnología asociada al paquete. |
| `pypi_package` | string | Paquete de PyPI (`dbt-core`, `pyspark`, `apache-airflow`…). |
| `download_count` | long | Descargas del paquete ese día. |
| `year` / `month` / `day` | int | Partición derivada de `download_date`. |

---

## Estado del proyecto

Construcción por fases. El orden prioriza las tecnologías núcleo y deja un
pipeline funcional de extremo a extremo lo antes posible.

**V1 — recorrido vertical completo (etiquetada en `v1.0.0`):**

- [x] **Fase 1 — Spark (batch):** ingesta de ficheros de GH Archive, parseo y
      normalización a Silver. (La agregación a Gold inicial era provisional; la
      asume dbt en la Fase 3.)
- [x] **Fase 2 — Kafka + streaming:** ingesta vía Kafka y migración a Spark
      Structured Streaming (Kafka → Bronze → Silver, con trigger `availableNow`).
      La agregación a Gold se reserva para dbt (Fase 3).
- [x] **Fase 3 — dbt:** modelado de la capa Gold como *star schema* (dimensiones
      `dim_technology`, `dim_date`, `dim_event_type`, `dim_source` y hecho
      `fact_github_activity`) sobre Silver, con tests de dbt.
- [x] **Fase 4 — Terraform:** infraestructura AWS como código (S3 del medallion,
      Glue Data Catalog, workgroup de Athena con tope de escaneo, IAM de mínimo
      privilegio y alerta de presupuesto).
- [x] **Cutover a AWS:** el pipeline escribe Silver y Gold en S3 (Delta) vía `s3a`;
      el Gold se registra en el Glue Data Catalog y se consulta desde Athena, que
      lee Delta de forma nativa (sin generar manifiestos).
- [x] **Dashboard:** visualización en Streamlit, con la evolución diaria de actividad
      por tecnología y el total por tecnología para el rango seleccionado, leyendo de
      Athena.

### V_final — Amplificación

Misma arquitectura, creciendo en amplitud (más fuentes y piezas de soporte):

- [x] **PyPI como 2ª fuente (adopción):** ingesta de descargas de PyPI desde BigQuery
      (agregadas en origen, con guardas de coste), normalizadas a Silver y modeladas
      con dbt como `fact_pypi_downloads`. `dim_source` pasa a ser real y `dim_date`
      cubre la unión de rangos de ambas fuentes; GitHub se backfillea a la misma
      ventana para comparar actividad vs adopción sobre el mismo periodo.
- [ ] **Dashboard analítico en Power BI** (lee de Athena) — siguiente.
- [ ] Validación de calidad de datos (Great Expectations).
- [ ] Métricas compuestas (momentum / salud de comunidad), donde las señales sean homogéneas.
- [ ] Observabilidad (Prometheus / Grafana).
- [ ] Más categorías de tecnologías.

---

## Puesta en marcha

> Las instrucciones detalladas se añadirán conforme avance la implementación.

**Requisitos previos:**

- Docker y Docker Compose
- Python 3.11+
- Terraform 1.6+ (para aprovisionar la infraestructura AWS)
- Una cuenta de AWS (las capas de almacenamiento usan el free tier)
- Credenciales de AWS configuradas (variables de entorno o `~/.aws/credentials`)
- Para la ingesta de PyPI: un proyecto de GCP (BigQuery Sandbox, sin tarjeta) con
  credenciales ADC (`gcloud auth application-default login`) y el extra `pypi`
  instalado (`pip install -e ".[pypi]"`)

> Las credenciales de AWS **nunca** se versionan. Consulta `.gitignore` y usa un
> fichero `.env` local (excluido del control de versiones).

### Flujo local (V1, streaming)

El pipeline de streaming corre en local sobre Kafka (Docker) y Spark. El esquema
medallion se construye en dos *queries* de streaming encadenadas (Kafka → Bronze,
Bronze → Silver):

```bash
make up                              # levanta Kafka (KRaft) en Docker
make topic                           # crea el topic github.push.raw

# Ingesta: publica los PushEvent de una hora de GH Archive en Kafka
make produce DATE=2024-01-15 HOURS=0-0

# Streaming Kafka → Bronze → Silver (Delta)
make stream-bronze
make stream-silver                   # Silver en local (data/silver)

make down                            # detiene Kafka
```

> Se desarrolla con **1 hora** de datos; el mismo flujo escala a 1 día o más
> cambiando solo `DATE`/`HOURS`, sin tocar la lógica de transformación.

> **Entorno híbrido (Silver en S3):** `make stream-silver-s3` escribe el Silver en
> `s3a://<bucket>/silver` con el perfil de mínimo privilegio (requiere
> `DEV_TRENDS_S3_BUCKET`); Bronze y los checkpoints permanecen en local.

El pipeline **batch** original (Fase 1) sigue disponible como alternativa:

```bash
make pipeline DATE=2024-01-15 HOURS=0-0
```

> El pipeline batch produce **Silver**; la agregación a Gold la construye dbt.

### Segunda fuente: descargas de PyPI (BigQuery)

La ingesta de PyPI consulta el dataset público de BigQuery, **agrega en origen**
(día × paquete) y trae solo el agregado a Silver, con guardas de coste: un dry-run
mide los bytes antes de gastar y `maximum_bytes_billed` es un tope duro.

```bash
# Estimación de escaneo, sin ejecutar ni escribir (mide bytes en BigQuery):
python -m dev_trends.pipeline.pypi_downloads --start 2026-04-01 --end 2026-07-01 \
  --dry-run --project <tu-proyecto-gcp>

# Backfill real a Silver en S3 (requiere DEV_TRENDS_S3_BUCKET y DEV_TRENDS_GCP_PROJECT):
make pypi-ingest START=2026-04-01 END=2026-07-01
```

> Las credenciales de GCP (ADC) viven en `~/.config/gcloud` y **nunca** se versionan.

### Backfill de GitHub a una ventana amplia

Para comparar actividad y adopción sobre el mismo periodo, GitHub se carga a la
misma ventana con el pipeline batch, escribiendo cada día de forma idempotente
(`replaceWhere` por partición: re-lanzar el rango no duplica eventos):

```bash
make backfill-github START=2026-04-01 END=2026-07-01
```

### Modelado analítico con dbt

dbt construye la capa **Gold** como *star schema* sobre el Silver ya escrito, con el
adapter `dbt-spark` (método `session`). Las dimensiones conformadas y los hechos
`fact_github_activity` (actividad) y `fact_pypi_downloads` (adopción) se materializan
como tablas Delta en **S3**, a grano diario y sobre las mismas dimensiones.

```bash
export DEV_TRENDS_S3_BUCKET=<bucket-medallion>   # p. ej. dev-trends-medallion-<account_id>
make dbt-build    # seeds + modelos + tests, escribiendo el Gold en s3a://<bucket>/gold
make dbt-parse    # valida el proyecto sin conexión (igual que la CI)
```

> `make dbt-build` usa el perfil AWS `dev-trends-pipeline` (mínimo privilegio). El
> nombre del bucket se pasa por `DEV_TRENDS_S3_BUCKET` (no se versiona: lleva el
> identificador de cuenta). Los reruns son idempotentes.

### Consulta con Athena

El Gold en S3 se registra en el Glue Data Catalog para consultarlo desde Athena, que
lee Delta de forma nativa (sin generar manifiestos):

```bash
make athena-register    # registra las tablas Gold en Glue (solo la primera vez)
```

> El registro es de **una sola vez**: tras cada `dbt build` posterior, Athena ya lee
> la versión nueva de cada tabla directamente del log de transacciones de Delta, sin
> necesidad de volver a registrarla.

A partir de ahí Athena responde la pregunta de V1 agregando el hecho por día y
tecnología, dentro del tope de datos escaneados del workgroup.

### Dashboard

Un dashboard en Streamlit, en local, lee la capa Gold desde Athena y muestra la
evolución diaria de actividad por tecnología para un rango de fechas seleccionable,
con el total de eventos por tecnología en ese rango:

```bash
make dashboard
```

> Usa el perfil AWS `dev-trends-pipeline` y el mismo workgroup/base de datos de
> Athena que el resto del pipeline. La consulta se cachea en la sesión de Streamlit
> para no volver a escanear datos en cada interacción.

![Dashboard: evolución diaria de actividad de desarrollo](docs/dashboard-streamlit.png)

### Infraestructura AWS con Terraform (Fase 4)

La infraestructura de almacenamiento y consulta se declara como código en `infra/`:
los buckets S3 del medallion y de resultados de Athena, la base de datos del Glue
Data Catalog, el workgroup de Athena (con tope de datos escaneados como guarda de
coste), un usuario y una política IAM de mínimo privilegio para el pipeline, y una
alerta de presupuesto mensual.

```bash
cd infra
cp example.tfvars terraform.tfvars   # y pon tu email para la alerta de presupuesto
terraform init
terraform plan
terraform apply
```

> Requiere credenciales de AWS con permisos para crear estos recursos. El estado de
> Terraform (`terraform.tfstate`), el `terraform.tfvars` y cualquier `*.tfvars` con
> valores propios **no se versionan**; sí se versiona `example.tfvars` como plantilla.

Para revisar el proyecto **sin credenciales** (igual que la CI):

```bash
cd infra
terraform fmt -check
terraform init -backend=false
terraform validate
```

---

## Calidad de código

El proyecto sigue prácticas estándar de la industria:

- Formateo y linting con `ruff`
- Tests con `pytest`
- Hooks de `pre-commit`
- Tareas comunes automatizadas con `Makefile`
- Integración continua con GitHub Actions (lint, tests y validación de Terraform en cada push/PR)

---

## Licencia

MIT — ver [`LICENSE`](LICENSE).
