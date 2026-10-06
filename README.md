# PIDS Parte 2 — Infraestructura de datos

**Escenario E8: Retención y ciclo de vida de los datos**, sobre el esquema del
dataset NYC Yellow Taxi.

Los viajes entran por **Kafka**, viven 30 días en **PostgreSQL** (tier
caliente), donde se consultan rápido, y después **Airflow** los archiva en
**Iceberg sobre MinIO** (tier frío), donde ocupan mucho menos. Una **API
FastAPI** decide en qué tier está cada consulta y **Grafana** muestra las
métricas del ciclo de vida.

| Documento | Contenido |
|---|---|
| [`docs/GUIA.md`](docs/GUIA.md) | Guía de funcionamiento: comandos en orden, comprobaciones y demo del ciclo de vida |
| [`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md) | Diseño, decisiones técnicas, esquema de datos, contrato de la API y métricas |

---

## Arquitectura

```
 datos/ + generador sintético ──► BRONZE (MinIO, copia cruda)
                                        │ carga inicial (PyIceberg)
                                        ▼
 SIMULADOR ──Kafka──► consumidor ──► HOT · PostgreSQL ──Airflow──► COLD · Iceberg + MinIO
                          │          partición diaria   archivado   partición mensual, ZSTD
                          ▼                │                               │
                    trips_cuarentena       └───────► API FastAPI ◄─────────┘
                                                 (router de tiers)
                                                        │
                                          Grafana · frontend web · chatbot
```

| Componente | Tecnología |
|---|---|
| Tier caliente | PostgreSQL (particionado por día) |
| Tier frío y bronze | MinIO + Apache Iceberg (PyIceberg, catálogo SQL en PostgreSQL) |
| Ingesta | Kafka + consumidor Python, con contrato de datos y cuarentena |
| Orquestación | Airflow (4 DAGs: particiones, archivado, estadísticas, purga) |
| Consulta | FastAPI + frontend web |
| Visualización | Grafana |
| Parte 3 | Chatbot (Streamlit + FastAPI + OpenRouter) |

### Ciclo de vida (E8)

- **La política de retención es un dato, no código:** vive en la tabla
  `retention_policy` y se cambia con un `UPDATE` o desde la API, sin redesplegar.
- **Archivado seguro con máquina de estados:**
  `PENDIENTE → ESCRIBIENDO → ESCRITO → VERIFICADO → DESALOJADO`. Nunca se borra
  una partición del caliente sin haber verificado antes que el conteo de filas
  en Iceberg coincide. El job es idempotente y se puede relanzar tras un fallo.
- **Purga del frío:** cierra el ciclo borrando los meses que superan la
  retención final y expirando snapshots.
- **Métricas:** filas y bytes por fila en cada tier, latencia p50/p95 frente a
  los SLAs (PostgreSQL < 500 ms, Iceberg < 10 s), cumplimiento de la política y
  calidad de los datos.

### Datos

Datos **de 2026, del 1 de enero hasta hoy**: `datos/muestra_1000.csv` (mil
viajes reales del portal desplazados a 2026) es la semilla;
`scripts/generar_datos_sinteticos.py` genera el histórico en volumen y el
simulador produce el flujo en vivo. Todo pasa por el contrato de datos de
`common/`, que rechaza a cuarentena los registros inválidos y marca con avisos
las anomalías reales del dataset.

---

## Puesta en marcha

Requisitos: Docker (con Compose v2) y Python 3.11 o superior.

```bash
cp .env.example .env
docker compose --profile core --profile orch --profile viz up -d
docker compose ps                         # esperar a que todo esté healthy

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

python scripts/prueba_humo.py             # → TODO CORRECTO
python scripts/prueba_archivado.py        # → TODO CORRECTO

python scripts/generar_datos_sinteticos.py 1000000 --seed 42
python ingesta/subir_bronze.py --origen datos/sinteticos
python ingesta/carga_inicial.py --recrear
```

El flujo en vivo, la demo de migración caliente → frío y el chatbot se
explican paso a paso en la [guía](docs/GUIA.md).

### Perfiles de Docker Compose

| Perfil | Servicios |
|---|---|
| `core` | PostgreSQL, MinIO, API |
| `stream` | Kafka, consumidor, simulador |
| `orch` | Airflow |
| `viz` | Grafana |
| `chat` | Chatbot de la Parte 3 |

### Servicios

| Servicio | URL | Credenciales |
|---|---|---|
| API (Swagger) | http://localhost:8000/docs | — |
| Frontend web | http://localhost:8000/app/ | — |
| Airflow | http://localhost:8080 | sin login |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| MinIO consola | http://localhost:9001 | `minioadmin` / `minioadmin_dev_2026` |
| PostgreSQL | `localhost:5432` | `pids` / `pids_dev_2026` |
| Chatbot | http://localhost:8501 | — |

---

## Estructura del repositorio

```
├── docs/            Guía de funcionamiento y documento de arquitectura
├── common/          Contrato de datos: esquema, validación, Iceberg, mensajes Kafka
├── postgres/init/   Esquema, funciones de particionado/archivado, políticas y vistas
├── datos/           Muestra de 1.000 viajes reales
├── scripts/         Pruebas, generador de datos sintéticos y utilidades
├── ingesta/         Subida a bronze, carga inicial a Iceberg y consumidor Kafka
├── simulador/       Productor de viajes en vivo con jitter
├── archivado/       Job de archivado hot → cold y purga del frío
├── airflow/         DAGs de orquestación
├── api/             API FastAPI y frontend web
├── grafana/         Datasource y dashboard «E8 · Ciclo de vida»
├── chatbot/         Asistente de la Parte 3
└── docker-compose.yml
```
