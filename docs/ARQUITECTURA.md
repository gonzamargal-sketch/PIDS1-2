# Arquitectura — PIDS Parte 2

El documento de diseño: qué se construye, cómo encajan las piezas y por qué
cada decisión es la que es.

**Escenario:** E8 — Retención y ciclo de vida de los datos.
**Dataset:** esquema de NYC Yellow Taxi.

> **Sobre las fechas: los datos de trabajo son de 2026.** El diseño se pensó
> para el año 2020 del portal, pero la muestra de mil viajes se ha movido de
> enero de 2020 a enero de 2026, el histórico en volumen
> lo genera `scripts/generar_datos_sinteticos.py` (del 1 de enero hasta ahora,
> nunca en el futuro) y el simulador emite viajes que acaban de terminar. La
> validación acepta recogidas desde 2025-12 hasta «ahora». La carga inicial ya
> no va toda al frío: reparte por edad con la política (lo anterior al corte a
> Iceberg, los últimos días a PostgreSQL). La separación `event_time` /
> `tpep_pickup_datetime` de §3.1 se mantiene: el ciclo de vida sigue
> gobernándolo solo `event_time`. Donde este documento dice «2020», léase la
> fecha de negocio del viaje.

---

## 0. Alcance

Los datos viven solo en dos sitios:

- **PostgreSQL** — tier caliente.
- **MinIO + Iceberg** — bronze y tier frío. Las consultas al histórico se hacen sobre Iceberg.

Y se mueven con dos piezas:

- **Kafka** — única puerta de entrada de los datos en vivo.
- **Airflow** — todas las transformaciones y traspasos entre tiers.

**No hay Redis ni Spark.** Cualquier referencia a ellos que quede en el código es un resto de la
versión anterior del diseño.

---

## 1. Decisiones

| Tema | Decisión | Por qué |
|---|---|---|
| Almacenamiento | **Solo PostgreSQL y MinIO/Iceberg** | Dos tiers bien hechos cubren E8; un tercero no añade nada a la historia |
| Modelo de tiers | **Mudanza** (disjunto): Postgres 0-30 días, Iceberg 31+ | Se ve la migración de verdad en la demo |
| Frontera hot/cold | 30 días, **parametrizable** | En demo la bajamos a minutos y se dispara en directo |
| Tier caliente | PostgreSQL particionado **por día** | Desalojar = `DROP PARTITION`, instantáneo |
| Tier frío | **Iceberg** sobre MinIO, particionado por mes, **ZSTD** | ZSTD ~30-40% menor que Snappy; ideal para datos que se leen poco |
| Catálogo Iceberg | **SQL catalog** sobre el propio Postgres | Un contenedor menos, sin Hive Metastore |
| Cómo hablamos con Iceberg | **PyIceberg** (no Spark+Iceberg) | Sin JVM ni JARs. Ver §3.4 |
| Ingesta | Simulador → **Kafka** → consumidor en Python → Postgres | Kafka es la única entrada en vivo |
| Transformaciones y traspasos | **Airflow**: todos los movimientos entre tiers son DAGs | Un solo sitio donde mirar qué se mueve, cuándo y por qué |
| Orquestación | **Airflow** standalone, 1 contenedor | Los DAGs lucen; el despliegue completo no cabe |
| Sin Redis, sin Spark, sin Timescale, sin Trino, sin sub-tiers, sin alertas, sin K8s | | Fuera de alcance |
| Late-arriving data | Se documenta, no se implementa | Fuera de alcance |

---

## 2. La idea en una página

```
  12 ficheros Parquet 2020 (TLC)
              │
              ▼
     ┌──────────────────┐
     │  BRONZE · MinIO  │  copia cruda intacta, nunca se modifica
     └────────┬─────────┘
              │ carga inicial (una vez, PyIceberg)
              │ todo 2020 ya es "antiguo" → va directo al frío
              ▼
                          ┌──────────────────────────┐
                          │  COLD · Iceberg + MinIO  │  histórico, ZSTD, part. mensual
                          └──────────▲───────────────┘
                                     │ DAG de archivado (Airflow + PyIceberg)
                                     │ desaloja lo que supera el umbral
                          ┌──────────┴───────────────┐
  ┌────────────┐  Kafka   │  HOT · PostgreSQL        │  últimos 30 días, part. diaria
  │ SIMULADOR  │ ───────▶ │  particionado por día    │
  │ replay2020 │ consumi- └──────────┬───────────────┘
  │ con reloj  │ dor Python          │
  │ desplazado │                     │
  └────────────┘                     │
                    ┌────────────────▼─────────────────┐
                    │  FastAPI · router de consultas   │  decide tier (Postgres o
                    │  anota data_source y latencia    │  Iceberg), fusiona, anota
                    └────────────────┬─────────────────┘
                                     │
                              ┌──────▼──────┐
                              │   GRAFANA   │
                              └─────────────┘
```

**En una frase:** los datos entran por Kafka, viven 30 días en PostgreSQL donde se consultan
rápido, y después Airflow los desaloja a Iceberg donde ocupan mucho menos. La API sabe en qué
tier está cada cosa y lo dice en cada respuesta.

---

## 3. Los cuatro diseños que hay que entender

Si alguien solo lee una sección del documento, que sea esta.

### 3.1 Dos columnas de tiempo (resuelve el problema de 2020)

Nuestros datos son de 2020 pero estamos en 2026. Si envejeciéramos por la fecha de recogida
del viaje, **todo** el dataset estaría caducado desde el primer segundo y nunca veríamos nada
moverse. Por eso separamos:

- **`tpep_pickup_datetime`** — cuándo ocurrió el viaje de verdad. Enero de 2020. Es el dato de
  negocio, el que se usa para las consultas analíticas ("viajes de marzo").
- **`event_time`** — cuándo el sistema considera que el evento entró. **Es la única columna que
  gobierna el ciclo de vida.** Sobre ella se particiona, se calcula la edad y se decide el
  desalojo.

Con esa separación todo encaja:

| | `tpep_pickup_datetime` | `event_time` | Dónde acaba |
|---|---|---|---|
| Carga inicial (12 meses de 2020) | 2020 real | 2020 real | Directo a **Iceberg** |
| Flujo simulado | 2020 real | desplazado a "ahora" | **Postgres** → envejece → Iceberg |

La carga inicial nos da un tier frío poblado desde el minuto uno (~24M filas), y el simulador
nos da el movimiento en vivo. No hay que inventar nada para la demo.

### 3.2 Máquina de estados del archivado (resuelve la idempotencia)

Elegimos el modelo mudanza, que tiene un riesgo real: si el job muere entre "he escrito en
Iceberg" y "he borrado de Postgres", o duplicamos o perdemos datos. Se resuelve así:

```
PENDIENTE ──▶ ESCRIBIENDO ──▶ ESCRITO ──▶ VERIFICADO ──▶ DESALOJADO
```

Tabla `archival_jobs` con una fila por partición:

```sql
CREATE TABLE archival_jobs (
    particion        DATE PRIMARY KEY,      -- p.ej. 2026-08-15
    estado           TEXT NOT NULL,         -- PENDIENTE|ESCRIBIENDO|ESCRITO|VERIFICADO|DESALOJADO
    filas_origen     BIGINT,
    filas_escritas   BIGINT,
    snapshot_id      BIGINT,                -- snapshot de Iceberg resultante
    intentos         INT DEFAULT 0,
    error            TEXT,
    actualizado_en   TIMESTAMPTZ DEFAULT NOW()
);
```

**Regla de oro: nunca se ejecuta `DROP PARTITION` sin haber pasado por VERIFICADO.** La
verificación es un conteo: filas en Iceberg para esa partición == filas que había en Postgres.
Si el DAG se cae en cualquier punto, al reejecutarse retoma desde el estado guardado. Si se
lanza dos veces seguidas, la segunda no hace nada.

Esto convierte un diseño frágil en uno seguro.

### 3.3 La política de retención es un dato, no código

Nada de umbrales escritos en un `.py`. Una tabla que Airflow lee en cada ejecución:

```sql
CREATE TABLE retention_policy (
    id              SERIAL PRIMARY KEY,
    dataset         TEXT NOT NULL,      -- 'trips'
    tier_origen     TEXT NOT NULL,      -- 'hot' | 'cold'
    tier_destino    TEXT,               -- 'cold' | NULL si es borrado
    umbral_valor    INT NOT NULL,
    umbral_unidad   TEXT NOT NULL,      -- 'minutes' | 'days' | 'years'
    accion          TEXT NOT NULL,      -- 'ARCHIVE' | 'DELETE'
    activa          BOOLEAN DEFAULT TRUE
);

INSERT INTO retention_policy VALUES
  (DEFAULT, 'trips', 'hot',  'cold', 30, 'days',  'ARCHIVE', TRUE),
  (DEFAULT, 'trips', 'cold', NULL,    7, 'years', 'DELETE',  TRUE);
```

Para la demo: `UPDATE retention_policy SET umbral_valor=5, umbral_unidad='minutes' WHERE ...`
y en cinco minutos empieza a archivarse en directo. Cambiar la política es un UPDATE, no un
redeploy — que es exactamente como funcionan las lifecycle rules de S3.

**Cada motor usa su mecanismo nativo:** cada capa expresa su retención con su mecanismo nativo. PostgreSQL con
`DROP PARTITION` e Iceberg con `expire_snapshots`. No hemos inventado un sistema de retención por
encima: usamos el que cada motor ya trae y Airflow solo los coordina.

### 3.4 Por qué PyIceberg y no Spark+Iceberg

Hacer que Spark escriba en Iceberg exige encajar versiones de Spark, Scala,
`iceberg-spark-runtime`, `hadoop-aws` y el SDK de AWS. Es el punto donde más proyectos se atascan.

PyIceberg es Iceberg en Python puro: mismo formato, mismos snapshots, mismos metadatos, sin
JVM. Habla con el catálogo SQL sobre nuestro Postgres y con MinIO por S3. A 24M de filas va
sobrado.

Sin Spark en el stack, todo el proyecto queda en Python y cada pieza hace lo que mejor sabe:

- **Consumidor de Kafka en Python** → valida con `common.validacion` y escribe en PostgreSQL y en
  cuarentena.
- **Airflow + PyIceberg** → todo lo que mueve datos entre tiers y todo lo que toca el tier frío.

> **Nota sobre compactación:** PyIceberg tiene soporte limitado de `rewrite_data_files`. Lo
> evitamos por diseño: el DAG archiva una partición completa por ejecución, así que escribe
> ficheros grandes desde el principio en vez de muchos pequeños. Prevenir en lugar de compactar.

---

## 4. Componentes

| Servicio | Qué hace | RAM aprox. | Perfil |
|---|---|---|---|
| `postgres` | Tier caliente, catálogo Iceberg, políticas, marts | 512 MB | core |
| `minio` + `minio-init` | Bronze y tier frío (S3) | 512 MB | core |
| `api` | FastAPI: router de consultas y endpoints | 256 MB | core |
| `kafka` | Broker en modo **KRaft**, sin Zookeeper | 1 GB | stream |
| `consumidor` | Consumidor de Kafka en Python → PostgreSQL + cuarentena | 128 MB | stream |
| `simulator` | Replay de 2020 con reloj desplazado | 128 MB | stream |
| `airflow` | `airflow standalone`, DAGs de transformación y ciclo de vida | 800 MB | orch |
| `grafana` | Dashboards | 256 MB | viz |

**Total con todo levantado: ~3.6 GB.** Quitar Redis y Spark nos ahorra unos 1,7 GB, que en las
máquinas de 8 GB es la diferencia entre poder levantarlo todo o no.

### Perfiles de Compose (pensados para máquinas de 8 GB)

No hace falta levantarlo todo para cada cosa:

```bash
docker compose --profile core up -d                    # ~1.3 GB, siempre
docker compose --profile core --profile stream up -d   # trabajar en ingesta
docker compose --profile core --profile orch up -d     # trabajar en DAGs
docker compose --profile "*" up -d                     # todo
```

En WSL2 hay que configurar `%UserProfile%\.wslconfig` o Docker se queda sin memoria:

```ini
[wsl2]
memory=6GB     # con 8 GB de RAM física
processors=4
swap=4GB
```

---

## 5. Esquema de datos (lo esencial)

### Tier caliente — `taxi_trips`

```sql
CREATE TABLE taxi_trips (
    trip_id                UUID DEFAULT gen_random_uuid(),
    event_time             TIMESTAMPTZ NOT NULL,   -- gobierna el ciclo de vida
    tpep_pickup_datetime   TIMESTAMPTZ NOT NULL,   -- dato de negocio (2020)
    tpep_dropoff_datetime  TIMESTAMPTZ,
    vendor_id              SMALLINT,
    passenger_count        SMALLINT,
    trip_distance          NUMERIC(8,2),
    pu_location_id         INTEGER,
    do_location_id         INTEGER,
    payment_type           SMALLINT,
    fare_amount            NUMERIC(10,2),
    tip_amount             NUMERIC(10,2),
    total_amount           NUMERIC(10,2),
    -- ... resto de columnas del dataset
    origen                 TEXT NOT NULL,          -- 'carga_inicial' | 'stream'
    PRIMARY KEY (trip_id, event_time)
) PARTITION BY RANGE (event_time);
```

Las particiones diarias las crea un DAG con antelación. Desalojar una = `DROP TABLE
taxi_trips_2026_08_15`, instantáneo y sin bloat.

### Otras tablas

- **`trips_cuarentena`** — registros que no pasan validación, con el motivo. Alimenta la métrica
  de calidad.
- **`retention_policy`** — §3.3
- **`archival_jobs`** — §3.2
- **`cold_stats`** — estadísticas del tier frío (filas, bytes, ficheros por partición) que
  escribe un DAG. **Grafana no sabe leer Iceberg**, así que lee esta tabla.
- **`query_log`** — cada consulta de la API con su tier y latencia. De aquí salen los percentiles.

### Tier frío — tabla Iceberg `lakehouse.trips`

Mismo esquema, particionada por `month(event_time)`, compresión ZSTD.

---

## 6. Contrato de la API

Toda respuesta lleva de dónde viene el dato. Esto es requisito de E8 y además es lo que la
Parte 3 necesitará para decirle al usuario si está viendo algo fresco o histórico.

```json
{
  "data": [ ... ],
  "meta": {
    "data_source": "mixto",              // hot | cold | mixto
    "coverage": [
      {"desde": "2026-09-01", "hasta": "2026-09-21", "tier": "hot"},
      {"desde": "2020-01-01", "hasta": "2026-08-31", "tier": "cold"}
    ],
    "as_of": "2026-09-21T14:32:10Z",     // frescura
    "latency_ms": 847,
    "rows": 1520
  }
}
```

**El router** es el componente con más chicha del proyecto. Recibe un rango de fechas, lo corta
por la frontera de retención, lanza las consultas que hagan falta a cada tier, fusiona y anota.
Orden de decisión: Postgres si el rango cae en caliente → Iceberg si cae en frío → ambos y fusión
si cruza la frontera.

**Dónde está la frontera.** No es `NOW() - umbral`: la política dice *cuándo* se mueve un día, no
dónde está cada fila ahora. El archivado mueve **días completos** y solo cuando pasa el DAG, así que
cortar en el umbral perdía filas: las del día de la frontera anteriores a esa hora (siguen en
Postgres, pero se pedían a Iceberg) y, tras bajar el umbral a 5 min en la demo, todo lo que el DAG
aún no había movido. El router decide **día a día, leyendo Postgres en cada consulta**:

- un día está en **caliente** si su partición existe y `archival_jobs` no lo marca `DESALOJADO`
  (en `ESCRIBIENDO`, `ESCRITO` o `VERIFICADO` sus filas siguen en Postgres, y se leen de ahí);
- está en **frío** si es anterior al día más antiguo del caliente o está `DESALOJADO`.

Los días seguidos del mismo tier se juntan en un tramo del `coverage`, cortados a las 00:00 UTC. Si
un día se queda en `ERROR` y el job sigue con los siguientes, el plan tiene varios tramos de cada
tier y la consulta sigue saliendo completa. Código en `api/router_tiers.py`.

Endpoints mínimos: `/trips`, `/metrics/{nombre}`, `/stats`, `/lifecycle/status`,
`/lifecycle/policy` (GET y PUT, para bajar el umbral en la demo), `/health`.

---

## 7. Qué medimos

Sin números medidos no hemos demostrado E8. Estas son las seis, y todas acaban en Grafana:

1. **Filas por tier en el tiempo** — área apilada. Hace visible la migración.
2. **Bytes por fila: Postgres vs Iceberg** — *nuestra métrica estrella.* Mismo dato, el caliente
   cuesta varias veces más. Esto es literalmente el "barato" de E8.
3. **Latencia p50/p95 por tier** — de `query_log`. Esto es el "rápido" de E8.
4. **Ratio de compresión** del tier frío frente al tamaño equivalente en Postgres.
5. **Edad del registro más antiguo en caliente** — debe mantenerse bajo el umbral. Es una métrica
   de *cumplimiento de la política*, y queda muy bien.
6. **Porcentaje de registros en cuarentena** — calidad de los datos.

**SLAs que nos ponemos:** PostgreSQL < 500 ms, Iceberg < 10 s (p95).

---

## 8. Riesgos y mitigaciones

**Kafka en 8 GB.** Es lo más pesado del stack. Sin Spark ni Redis el total baja a ~3,6 GB, pero
Kafka sigue pidiendo 1 GB. Mitigación: perfiles de Compose y `.wslconfig` bien puesto.

**El consumidor en Python tiene que aguantar el ritmo.** Sin Spark, el paso de Kafka a Postgres
depende de un proceso Python. Mitigación: insertar por lotes (`execute_values`, `page_size` de
cientos de filas) y confirmar el offset solo después del commit en Postgres, para no perder
mensajes si el proceso se cae.

**PyIceberg tiene menos funciones que Spark+Iceberg.** Sobre todo en mantenimiento. Mitigación:
escribimos particiones completas para no generar ficheros pequeños (§3.4). Si algo falta de
verdad, el plan B es Parquet plano particionado, que cubre E8 igual.

**MinIO ya no publica imágenes mantenidas.** Su edición community quedó
archivada en 2026 y las imágenes salieron de Docker Hub. Tiramos de quay.io con
la versión fijada en `RELEASE.2025-04-22T22-12-26Z`, que es la última que
conserva la consola web; las posteriores la quitaron. Mitigación si quay también
cayera: cambiar a SeaweedFS o Garage, que hablan S3 igual. Afectaría al paso 5 y
a la carga a Iceberg, no al resto de la arquitectura.

**El simulador y el reloj.** Es donde más fácil es liarse. Mitigación: la separación de
`event_time` y `tpep_pickup_datetime` (§3.1) tiene que estar clara antes de escribir una línea,
y las dos columnas se llevan a Kafka desde el primer mensaje.
