# Arquitectura — PIDS Parte 2

Documento de diseño: qué se ha construido, cómo encajan las piezas y por qué
se tomó cada decisión.

**Escenario:** E8 — Retención y ciclo de vida de los datos.
**Dataset:** esquema de NYC Yellow Taxi, con datos de 2026 (del 1 de enero
hasta hoy).

---

## 1. Alcance y decisiones

Los datos se guardan solo en **PostgreSQL** (tier caliente) y en **MinIO +
Iceberg** (bronze y tier frío). Se mueven con **Kafka**, que es la única
entrada de datos en vivo, y con **Airflow**, que hace todas las
transformaciones y traspasos entre tiers.

| Tema | Decisión | Por qué |
|---|---|---|
| Almacenamiento | Solo PostgreSQL y MinIO/Iceberg | Con dos tiers bien resueltos se cubre E8 |
| Modelo de tiers | **Mudanza** (sin solape): PostgreSQL 0-30 días, Iceberg 31+ | La migración entre tiers se ve de verdad |
| Frontera hot/cold | 30 días, **parametrizable** | En la demo se baja a minutos y el archivado se dispara en directo |
| Tier caliente | PostgreSQL particionado **por día** | Desalojar es un `DROP` de la partición: instantáneo y sin bloat |
| Tier frío | Iceberg sobre MinIO, particionado **por mes**, **ZSTD** | ZSTD ocupa ~30-40 % menos que Snappy y aquí los datos se leen poco |
| Catálogo Iceberg | Catálogo SQL sobre el propio PostgreSQL | Un contenedor menos, sin Hive Metastore |
| Acceso a Iceberg | **PyIceberg** (no Spark) | Sin JVM ni JARs (§3.4) |
| Ingesta | Simulador → Kafka → consumidor Python → PostgreSQL | Kafka es la única entrada en vivo |
| Orquestación | Airflow standalone (1 contenedor) | Todos los movimientos entre tiers son DAGs |
| Fuera de alcance | Redis, Spark, Trino, sub-tiers, alertas, Kubernetes, datos que llegan tarde | No aportan a E8 |

---

## 2. Visión general

```
  datos/muestra_1000.csv + generador sintético
              │
              ▼
     ┌──────────────────┐
     │  BRONZE · MinIO  │  copia cruda intacta, nunca se modifica
     └────────┬─────────┘
              │ carga inicial (PyIceberg): reparte por edad
              │ lo anterior al corte → frío · lo reciente → caliente
              ▼
                          ┌──────────────────────────┐
                          │  COLD · Iceberg + MinIO  │  histórico, ZSTD, part. mensual
                          └──────────▲───────────────┘
                                     │ DAG de archivado (Airflow + PyIceberg)
                                     │ desaloja lo que supera el umbral
                          ┌──────────┴───────────────┐
  ┌────────────┐  Kafka   │  HOT · PostgreSQL        │  últimos 30 días, part. diaria
  │ SIMULADOR  │ ───────▶ │  + trips_cuarentena      │
  │ con jitter │ consumi- └──────────┬───────────────┘
  └────────────┘ dor Python          │
                    ┌────────────────▼─────────────────┐
                    │  FastAPI · router de tiers       │  decide tier, fusiona y anota
                    └────────────────┬─────────────────┘  data_source y latencia
                                     │
                    Grafana · frontend web · chatbot (Parte 3)
```

**En una frase:** los datos entran por Kafka, viven 30 días en PostgreSQL,
donde se consultan rápido, y después Airflow los traslada a Iceberg, donde
ocupan mucho menos. La API sabe en qué tier está cada dato y lo indica en cada
respuesta.

### Origen de los datos

| Fuente | Qué aporta | Fechas |
|---|---|---|
| `datos/muestra_1000.csv` | Mil viajes reales del portal: semilla y fixture de las pruebas | Enero de 2026 (el original era de enero de 2020, desplazado 6 años) |
| `scripts/generar_datos_sinteticos.py` | Histórico en volumen para las métricas | Del 2026-01-01 hasta ahora |
| `simulador/` | Flujo en vivo por Kafka | Viajes que acaban de terminar |

El generador parte de la muestra y altera cada fila (distancia, duración,
importes, zonas, pasajeros), conserva las anomalías reales y reparte las horas
con el perfil diario típico de los taxis de Nueva York. El simulador añade
**jitter** por la misma razón: repetir las mismas filas daría un ratio de
compresión irreal (~35x en lugar de ~7,5x), porque Parquet comprime casi gratis
los valores idénticos.

---

## 3. Diseños clave

### 3.1 Dos columnas de tiempo

- **`tpep_pickup_datetime`**: cuándo ocurrió el viaje. Es el dato de negocio y
  se usa en las consultas analíticas.
- **`event_time`**: cuándo el sistema considera que entró el evento. **Es la
  única columna que gobierna el ciclo de vida**: sobre ella se particiona, se
  calcula la edad y se decide el desalojo.

En el flujo en vivo las dos coinciden casi al segundo. Se mantienen separadas
para que el ciclo de vida no dependa de la fecha de negocio: si el sistema
recibe datos con otra fecha de negocio (por ejemplo, el histórico real de 2020
del portal), siguen envejeciendo según su entrada y no caducan nada más llegar.

### 3.2 Máquina de estados del archivado

El modelo de mudanza tiene un riesgo: si el job muere entre «he escrito en
Iceberg» y «he borrado de PostgreSQL», se duplican o se pierden datos. Se
resuelve con una máquina de estados por partición, guardada en `archival_jobs`:

```
PENDIENTE ──▶ ESCRIBIENDO ──▶ ESCRITO ──▶ VERIFICADO ──▶ DESALOJADO
```

```sql
CREATE TABLE archival_jobs (
    particion        DATE PRIMARY KEY,
    estado           TEXT NOT NULL,   -- PENDIENTE|ESCRIBIENDO|ESCRITO|VERIFICADO|DESALOJADO|ERROR
    filas_origen     BIGINT,
    filas_escritas   BIGINT,
    snapshot_id      BIGINT,          -- snapshot de Iceberg resultante
    intentos         INT DEFAULT 0,
    error            TEXT,
    actualizado_en   TIMESTAMPTZ DEFAULT NOW()
);
```

**Regla de oro: nunca se borra una partición sin pasar por VERIFICADO.** La
verificación compara las filas que hay en Iceberg para esa partición con las
que había en PostgreSQL; `desalojar_particion()` falla si no se cumple. Si el
job se cae en cualquier punto, al relanzarlo retoma desde el estado guardado,
y si se lanza dos veces seguidas la segunda no hace nada. Si una partición
acaba en `ERROR`, el job sale con código 1 para que Airflow la marque y la
reintente.

### 3.3 La política de retención es un dato, no código

Los umbrales están en una tabla que Airflow lee en cada ejecución:

```sql
SELECT dataset, tier_origen, tier_destino, umbral_valor, umbral_unidad, accion
  FROM retention_policy;
--  trips | hot  | cold | 30 | days  | ARCHIVE
--  trips | cold |      |  7 | years | DELETE
```

Cambiar la política es un `UPDATE` (o `PUT /lifecycle/policy`), sin
redesplegar nada, igual que las *lifecycle rules* de S3. En la demo se baja el
archivado a 5 minutos y la migración ocurre en directo.

Cada motor aplica la retención con su mecanismo nativo: PostgreSQL con `DROP`
de particiones e Iceberg con `DELETE` + `expire_snapshots`. Airflow solo los
coordina.

### 3.4 PyIceberg en lugar de Spark

Hacer que Spark escriba en Iceberg obliga a encajar versiones de Spark, Scala,
`iceberg-spark-runtime`, `hadoop-aws` y el SDK de AWS. PyIceberg es Iceberg en
Python puro: el mismo formato, los mismos snapshots y metadatos, sin JVM. Así
todo el proyecto queda en Python.

PyIceberg apenas permite compactar (`rewrite_data_files`), así que se evita
por diseño: el DAG archiva una partición completa por ejecución y escribe
ficheros grandes desde el principio.

Detalles de implementación resueltos en `common/lakehouse.py`:

- **`pyiceberg-core` es obligatorio**: sin él, cualquier `append` a una tabla
  particionada falla con `NotInstalledError`.
- **PyArrow no convierte `float64` a `decimal128`**: hay que pasar por objetos
  `Decimal` (`df_a_arrow`).
- **El conteo de verificación no usa `plan_files()`**: su `record_count`
  cuenta todas las filas de cada fichero candidato, no las que cumplen el
  filtro. Como sobrecuenta, bloquearía el archivado. `contar_particion` lee de
  verdad proyectando una sola columna.

---

## 4. Contrato de datos y calidad

Todo lo que lee el dataset pasa por `common/` (`esquema.py`,
`validacion.py`). Si cambia el formato de origen, se corrige ahí una sola vez.

- **Nombres de columna**: el export CSV del portal usa `VendorID` y la API
  SODA, `vendorid`. El mapeo no distingue mayúsculas.
- **Fechas**: el export usa `01/01/2020 12:28:15 AM` y la API,
  `2020-01-01T00:28:15.000`. `esquema.parsear_fechas` detecta el formato y
  nunca lo infiere, para no confundir día y mes sin dar error.

Las reglas salen de perfilar el dataset real y tienen dos severidades:

- **Rechazo** (va a `trips_cuarentena` con el motivo): recogida anterior a
  2025-12 o en el futuro, cronología invertida, importes por encima de 10.000,
  distancias o duraciones imposibles, zonas fuera de rango.
- **Aviso** (se acepta, marcado en `avisos`): importe negativo (devoluciones),
  `passenger_count` a cero, distancia cero, duración sospechosa, velocidad
  imposible, zonas 264/265.

---

## 5. Componentes

| Servicio | Función | Perfil |
|---|---|---|
| `postgres` | Tier caliente, catálogo Iceberg, políticas y vistas de métricas | core |
| `minio` + `minio-init` | Bronze y tier frío (S3) | core |
| `api` | FastAPI: router de tiers, endpoints y frontend web | core |
| `kafka` | Broker en modo KRaft, sin Zookeeper | stream |
| `consumidor` | Kafka → PostgreSQL + cuarentena, por lotes | stream |
| `simulador` | Productor de viajes en vivo con jitter | stream |
| `airflow` | DAGs de ciclo de vida | orch |
| `grafana` | Dashboard «E8 · Ciclo de vida» | viz |
| `chatbot` + `chat-ui` | Asistente de la Parte 3 | chat |

### DAGs de Airflow

| DAG | Frecuencia | Qué hace |
|---|---|---|
| `mantener_particiones` | Diario | Crea las particiones diarias con antelación |
| `archivar` | Cada 5 min | Recorre la máquina de estados: caliente → Iceberg → `DROP` |
| `estadisticas_frio` | Cada 5 min | Escribe `cold_stats` y `hot_stats` para Grafana |
| `purga_final` | Diario | Borra del frío lo que supera la retención final y expira snapshots |

---

## 6. Esquema de datos

### Tier caliente: `taxi_trips`

Particionada por rango diario de `event_time`, con las columnas del dataset
NYC Yellow Taxi más metadatos de procedencia:

```sql
CREATE TABLE taxi_trips (
    trip_id                UUID NOT NULL DEFAULT gen_random_uuid(),
    event_time             TIMESTAMPTZ NOT NULL,   -- gobierna el ciclo de vida
    tpep_pickup_datetime   TIMESTAMPTZ NOT NULL,   -- dato de negocio
    tpep_dropoff_datetime  TIMESTAMPTZ,
    -- ... columnas del dataset (vendor_id, passenger_count, trip_distance,
    --     pu/do_location_id, payment_type, importes y recargos)
    origen                 TEXT NOT NULL,          -- 'carga_inicial' | 'stream'
    avisos                 TEXT[],                 -- avisos de calidad
    PRIMARY KEY (trip_id, event_time)
) PARTITION BY RANGE (event_time);
```

Una partición `taxi_trips_default` recoge cualquier fila sin partición propia,
para que una inserción nunca falle.

### Otras tablas

| Tabla | Contenido |
|---|---|
| `trips_cuarentena` | Registros rechazados por la validación, con el motivo |
| `retention_policy` | Política de retención (§3.3) |
| `archival_jobs` | Estado del archivado por partición (§3.2) |
| `cold_stats` / `hot_stats` | Filas, bytes y ficheros por partición. Grafana no lee Iceberg, así que lee estas tablas |
| `query_log` | Cada consulta de la API con su tier y su latencia |

### Tier frío: tabla Iceberg `lakehouse.trips`

Mismo esquema, particionada por `month(event_time)` y comprimida con ZSTD.
Iceberg guarda el mínimo y el máximo por fichero, así que la partición mensual
sigue permitiendo podar por día.

---

## 7. API

Cada respuesta indica de dónde viene el dato:

```json
{
  "data": [ ... ],
  "meta": {
    "data_source": "mixto",
    "coverage": [
      {"desde": "2026-01-01", "hasta": "2026-09-06", "tier": "cold"},
      {"desde": "2026-09-06", "hasta": "2026-10-06", "tier": "hot"}
    ],
    "as_of": "2026-10-06T14:32:10Z",
    "latency_ms": 847,
    "rows": 1520
  }
}
```

**El router de tiers** (`api/router_tiers.py`) recibe un rango de fechas, lo
divide entre los tiers, consulta cada uno, fusiona los resultados y los anota.
La frontera no se calcula como `NOW() - umbral`, porque el archivado mueve días
completos y solo cuando pasa el DAG. Se decide **día a día, consultando
PostgreSQL**:

- un día está en **caliente** si su partición existe y `archival_jobs` no lo
  marca como `DESALOJADO` (hasta entonces sus filas siguen en PostgreSQL);
- está en **frío** si es anterior al día más antiguo del caliente o ya está
  `DESALOJADO`.

Los días consecutivos del mismo tier se agrupan en un tramo de `coverage`. Si
un día se queda en `ERROR`, la consulta sigue saliendo completa.

| Endpoint | Descripción |
|---|---|
| `GET /trips` | Viajes en un rango de `event_time`, enrutados por tier |
| `GET /trips/resumen` | Agregados por zona y fechas (lo usa el chatbot) |
| `GET /metrics/{nombre}` | Vistas de métricas: `coste_por_tier`, `latencia_por_tier`, `calidad`, `metricas_caliente` |
| `GET /stats` | Resumen general del sistema |
| `GET /lifecycle/status` | Estado del archivado por partición |
| `GET` / `PUT /lifecycle/policy` | Consultar o cambiar la política de retención |
| `GET /ingesta` | Actividad reciente de ingesta y cuarentena |
| `GET` / `PUT /simulador`, `POST /ingesta/muestras` | Controles del flujo en vivo |
| `GET /health` | Comprobación de salud |

---

## 8. Métricas

Todas se ven en Grafana y salen de vistas de PostgreSQL:

1. **Filas por tier en el tiempo**: hace visible la migración.
2. **Bytes por fila, PostgreSQL frente a Iceberg** (`v_coste_por_tier`): el
   «barato» de E8.
3. **Latencia p50/p95 por tier** (`v_latencia_por_tier`, de `query_log`): el
   «rápido» de E8.
4. **Ratio de compresión** del frío frente al tamaño equivalente en PostgreSQL.
5. **Edad del registro más antiguo en caliente** (`v_cumplimiento_politica`):
   debe quedar por debajo del umbral.
6. **Porcentaje de registros en cuarentena** (`v_calidad`).

**SLAs:** PostgreSQL < 500 ms e Iceberg < 10 s (p95).

---

## 9. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Memoria en equipos de 8 GB (Kafka pide ~1 GB) | Sin Spark ni Redis el total ronda 3,6 GB; perfiles de Compose para levantar solo lo necesario |
| El consumidor Python no aguanta el ritmo | Inserción por lotes (`execute_values`) y confirmación del offset solo tras el commit en PostgreSQL |
| PyIceberg tiene menos funciones de mantenimiento que Spark | Se escriben particiones completas para no generar ficheros pequeños |
| MinIO dejó de publicar imágenes en Docker Hub | Imagen de quay.io fijada en `RELEASE.2025-04-22T22-12-26Z`, la última con consola web. Alternativas S3: SeaweedFS o Garage |
| Pérdida o duplicación de datos al archivar | Máquina de estados con verificación previa al borrado (§3.2) |
