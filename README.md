# dev-trends

> Plataforma de datos que mide la **actividad de desarrollo** y la **adopción** de
> tecnologías de software a partir de fuentes públicas (eventos de GitHub vía GH
> Archive, y descargas de PyPI vía BigQuery). Pipeline de Data Engineering de
> extremo a extremo: ingesta en streaming y batch, procesamiento distribuido,
> arquitectura medallion sobre un data lake en AWS y modelado analítico.

> **Estado: V1 completa (etiquetada en `v1.0.0`); ampliándose.** El pipeline base
> funciona de punta a punta con datos reales: GH Archive → Kafka → Spark Structured
> Streaming → Silver/Gold en S3 (Delta) → dbt → Athena → Streamlit. Ya integrada una
> **segunda fuente (PyPI)** para medir adopción, y añadido el **dashboard analítico
> en Power BI**; el resto de ampliaciones (calidad de datos, observabilidad) están
> en el roadmap más abajo.

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
| Visualización | Streamlit · Power BI (dashboard analítico) |
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
- [x] **Dashboard analítico en Power BI** (lee de Athena): comparación de actividad
      vs adopción por tecnología y fecha, con detalle por tecnología (*drillthrough*).
- [x] **Validación de calidad de datos:** gate sobre la capa Silver (validez
      estructural + detección de anomalías de negocio) con los tests de dbt y
      `dbt-expectations`.
- ~~Métricas compuestas (momentum / salud de comunidad)~~ — **descartadas** tras medir
      las señales; el razonamiento está en «Métricas compuestas: por qué no existen».
- [ ] Observabilidad del streaming (Prometheus / Grafana).
- [ ] Reprocesamiento en Kafka (retención, *offsets*, idempotencia frente al *checkpoint*).

**Mejoras futuras** (evaluadas y pospuestas, no olvidadas):

- **Catálogo completo de tecnologías** (Backend, IA). El pipeline ya es agnóstico al
  número de repositorios: ampliarlo son filas en un *seed*, sin código ni conceptos
  nuevos. A cambio obliga a reingerir la ventana completa de GH Archive, porque el
  filtrado por tecnología precede a la capa Silver.
- **Más *topics* en Kafka** (`pypi-releases`, `technology-metadata`). Las *releases* de
  PyPI se obtienen de una API REST paginada, no de un flujo continuo: publicarlas en un
  *topic* repetiría el patrón del productor existente sin ejercitar nada nuevo.
- **Más fuentes de adopción**: Docker Hub, npm, Maven Central.

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

### Calidad de datos

El pipeline valida la capa **Silver** antes de consolidarla en los hechos
analíticos de **Gold**, con un gate de calidad de datos de dos familias:

- **Validez estructural** (el contrato de la capa Silver): cada evento tiene
  `event_id` único y no nulo, `technology`/`event_type`/`created_at` presentes,
  `event_type` dentro del dominio esperado (`push`, `pull_request`, `release`,
  `watch`) y `repository` con formato `org/repo`. Si algo falla, el *build* **se
  detiene**: es corrupción y no debe llegar a los modelos.
- **Anomalías de negocio**: patrones que sesgan la interpretación aunque el dato
  sea correcto. Se emiten como **aviso** (no detienen el *build*): el dato es real,
  solo hay que no leerlo de forma ingenua.

Se implementa con los **tests nativos de dbt** más el paquete **`dbt-expectations`**. 
El pipeline ya usa dbt para el modelado, los agregados que la detección de anomalías 
necesita ya existen en la capa Gold, y así la validación vive junto a los datos que valida, 
sin añadir un motor aparte ni una dependencia pesada.

Un ejemplo concreto de anomalía: un test marca los días en que una tecnología
acumula mucha actividad de `push` con *pull requests* casi ausentes — la firma de la
**automatización** (bots o CI que empujan *commits* sin revisión), que no es
desarrollo humano pero infla la actividad. Sobre los datos reales el test señala
**dbt el 2026-06-01: 931 pushes y 0 PRs**, un día que vale **22 veces la mediana
diaria de ese mes**. Es el mismo desbalance `push ≫ PR` que el desglose por tipo de
evento del dashboard hace visible: sin este aviso, ese pico se leería como desarrollo
genuino.

El test opera a grano **diario** por diseño: sobre el agregado mensual, ese día pasa
inadvertido. Junio suma 2260 eventos, casi los mismos que abril (2319) — pero 990 de
ellos son del día 1. Descontándolo, junio va a 44 eventos diarios frente a los 77 de
abril. Un agregado suficientemente grueso siempre acaba dando el visto bueno.

```bash
make dbt-deps    # instala dbt-expectations (packages.yml), una sola vez
make dbt-build   # construye el Gold y ejecuta TODOS los tests (estructurales + anomalías)
make dbt-test    # solo los tests, sobre el Gold ya construido
```

> Los tests que dependen de datos corren en local (`dbt build`/`dbt test`); la CI
> valida el proyecto dbt sin conexión (`dbt parse`), como el resto del modelado.

### Hallazgo: la actividad de estos repositorios se está automatizando

El desglose por tipo de evento a lo largo del trimestre analizado muestra un
desplazamiento sostenido del trabajo humano al automatizado:

| mes | pull_request | push | release | watch | % humano |
|---|---|---|---|---|---|
| abril | 1014 | 1056 | 7 | 242 | 44 % |
| mayo | 671 | 1430 | 4 | 137 | 30 % |
| junio | 184 | 2017 | 2 | 57 | 8 % |

`pull_request` cae en las cinco tecnologías (entre −66 % y −100 %) y `watch` un 76 %,
mientras `push` se dispara. **No es que GitHub estuviera más tranquilo:** los ficheros
horarios del origen traen 158 392 eventos en abril y 157 497 en junio. Son estos cinco
repositorios los que se han vuelto *push-only*.

El dato está verificado contra la fuente: se contrastó el crudo de GH Archive con la
capa Silver para horas concretas, y los conteos coinciden evento a evento. La caída
está en el origen, no en el pipeline.

Conviene además desglosar por tecnología antes de leer el agregado: el crecimiento de
`push` es casi todo de un solo repositorio (dbt, ×12 entre abril y junio). Descontándolo,
junio suma 944 *pushes* contra 968 en abril — plano.

### Métricas compuestas: por qué no existen

Se evaluó un *Momentum Score* que combinara actividad (GitHub) y adopción (PyPI) por
tecnología. Se descartó tras medir las señales, y el motivo merece contarse.

Las dos fuentes miden magnitudes incomparables: descargas en cientos de millones frente
a eventos en decenas por día. Lo único que se puede componer entre ellas son **tasas de
variación** adimensionales, nunca los niveles. Y toda tasa necesita un denominador con
volumen suficiente — que la señal de desarrollo humano no tiene: los cinco repositorios
suman **13 releases en todo el trimestre**, y `dagster-io/dagster` registra entre 0 y 8
*pull requests* al mes. Un cociente sobre contadores de una cifra no mide impulso, mide
ruido de muestreo. No lo arregla ampliar el histórico: tres meses es la ventana completa.

La divergencia entre *cuánto se construye* una herramienta y *cuánto se usa* —el hallazgo
que el score pretendía resumir en un número— ya está disponible sin él: los rankings de
actividad y de adopción se comparan directamente en el dashboard, cada uno en su escala
y sin promediarlos. Un índice que promedia dos señales incomparables esconde más de lo
que explica.

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

### Dashboard analítico en Power BI

El dashboard definitivo de la ampliación (`dashboard/powerbi/`) lee la capa Gold
desde Athena: los agregados se cargan una vez y toda la
interacción es local, sin re-escanear Athena. Se conecta con un usuario IAM de
**solo lectura** dedicado (mínimo privilegio).
Cruza `fact_github_activity` (actividad) y `fact_pypi_downloads` (adopción) sobre
las dimensiones conformadas, en tres páginas:

1. **Comparativa** — rankings de actividad (GitHub) vs adopción (PyPI) por
   tecnología para el periodo seleccionado. Al elegir una tecnología se resalta en
   ambos rankings y las tarjetas y el logo se adaptan a ella.
2. **Tendencia en el tiempo** — *small multiples* de las cinco tecnologías, cada
   serie normalizada a su propio máximo (media móvil de 7 días, % del pico): compara
   la **dirección** de actividad y adopción en el tiempo, no su magnitud.
3. **Detalle por tecnología** (*drillthrough*) — al profundizar en una tecnología:
   su posición en cada ranking, el **desglose por tipo de evento** (push, pull
   request, release, watch) y su tendencia. El desglose expone la *calidad* de la
   actividad: pushes ≈ PRs refleja desarrollo humano; pushes ≫ PRs delata
   automatización, no desarrollo real.

![Comparativa: actividad vs adopción por tecnología](dashboard/powerbi/screenshots/pagina-1-comparativa.png)
![Tendencia normalizada por tecnología](dashboard/powerbi/screenshots/pagina-2-tendencia.png)
![Detalle por tecnología con desglose por tipo de evento](dashboard/powerbi/screenshots/pagina-3-detalle.png)

> La conexión se configura con un DSN ODBC de Athena

### Infraestructura AWS con Terraform (Fase 4)

La infraestructura de almacenamiento y consulta se declara como código en `infra/`:
los buckets S3 del medallion y de resultados de Athena, la base de datos del Glue
Data Catalog, el workgroup de Athena (con tope de datos escaneados como guarda de
coste), dos usuarios IAM de mínimo privilegio —uno de lectura y escritura para el
pipeline, y otro de **solo lectura** para el dashboard de Power BI, limitado a
consultar Athena y leer la capa Gold— y una alerta de presupuesto mensual.

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
