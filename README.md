# dev-trends

> Plataforma de datos que mide la **actividad de desarrollo** de tecnologías de
> software a partir de fuentes públicas (GitHub, vía GH Archive). Pipeline de
> Data Engineering de extremo a extremo: ingesta en streaming, procesamiento
> distribuido, arquitectura medallion sobre un data lake en AWS y modelado
> analítico.

> **Estado: en construcción (V1).** Este repositorio se desarrolla por fases.
> Más abajo se detalla qué está implementado y qué está planificado.

---

## Qué es

`dev-trends` ingiere el flujo de eventos públicos de GitHub (pushes, pull
requests, releases, etc.) y lo transforma en métricas de actividad por
tecnología, para responder preguntas como *qué herramientas de su categoría
crecen en actividad de desarrollo y cuáles se estancan*.

El proyecto está diseñado como demostración de un pipeline de Data Engineering
moderno de principio a fin, con las prácticas que se esperan en producción:
arquitectura por capas, transición de batch a streaming, infraestructura como
código y modelado analítico desacoplado.

> **Nota sobre qué se mide.** GitHub refleja *actividad de desarrollo* (commits,
> PRs, releases), no *adopción* en producción. `dev-trends` mide lo primero. La
> medición de adopción (vía descargas de paquetes) está contemplada como
> ampliación futura.

---

## Stack

| Capa | Tecnología |
|---|---|
| Fuente | GH Archive (eventos públicos de GitHub) |
| Ingesta | Apache Kafka (batch → streaming) |
| Procesamiento | Apache Spark (Structured Streaming) |
| Almacenamiento | AWS S3 + Delta Lake (arquitectura medallion) |
| Catálogo | AWS Glue Data Catalog |
| Modelado | dbt |
| Consulta | AWS Athena |
| Visualización | Streamlit |
| Infraestructura | Terraform |
| Orquestación local | Docker Compose |

**Entorno híbrido:** el cómputo (Kafka, Spark, dbt) corre en local sobre Docker;
el almacenamiento y la consulta (S3, Glue, Athena) viven en AWS.

---

## Arquitectura

```
GH Archive ──▶ Kafka ──▶ Spark ──▶ S3 / Delta (medallion) ──▶ Athena ──▶ Dashboard
                                   Bronze → Silver → Gold
                                                 (dbt)

           Terraform aprovisiona la infraestructura AWS (S3, Glue, Athena, IAM)
```

- **Bronze:** eventos crudos de GH Archive, tal cual.
- **Silver:** eventos normalizados y filtrados (un evento por fila, tipado).
- **Gold:** agregados analíticos por tecnología y periodo, modelados con dbt.

---

## Estado del proyecto

Construcción por fases. El orden prioriza las tecnologías núcleo y deja un
pipeline funcional de extremo a extremo lo antes posible.

- [ ] **Fase 1 — Spark (batch):** ingesta de ficheros de GH Archive, parseo,
      normalización a Silver y agregación a Gold.
- [ ] **Fase 2 — Kafka + streaming:** ingesta desacoplada y migración a Spark
      Structured Streaming.
- [ ] **Fase 3 — dbt:** modelado de la capa Gold (dimensiones y hechos).
- [ ] **Fase 4 — Terraform:** infraestructura AWS como código.
- [ ] **Dashboard:** visualización en Streamlit.

### Ampliaciones futuras

Descargas de PyPI como segunda fuente (para medir adopción, no solo actividad),
más categorías de tecnologías, validación de calidad de datos, observabilidad y
un dashboard analítico en Power BI.

---

## Puesta en marcha

> Las instrucciones detalladas se añadirán conforme avance la implementación.

**Requisitos previos:**

- Docker y Docker Compose
- Python 3.11+
- Una cuenta de AWS (las capas de almacenamiento usan el free tier)
- Credenciales de AWS configuradas (variables de entorno o `~/.aws/credentials`)

> Las credenciales de AWS **nunca** se versionan. Consulta `.gitignore` y usa un
> fichero `.env` local (excluido del control de versiones).

---

## Calidad de código

El proyecto sigue prácticas estándar de la industria:

- Formateo y linting con `ruff`
- Tests con `pytest`
- Hooks de `pre-commit`
- Tareas comunes automatizadas con `Makefile`
- Integración continua con GitHub Actions (lint + tests en cada push/PR)

---

## Licencia

MIT — ver [`LICENSE`](LICENSE).
