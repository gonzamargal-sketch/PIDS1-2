# Guía de funcionamiento

Cómo levantar el proyecto completo, cargar los datos, comprobar que todo
funciona y ver el ciclo de vida de E8 en marcha. **Es la guía viva del
grupo**: se actualiza cada vez que entra una parte nueva o cambia cómo se hace
algo.

> **Última actualización:** 2026-09-28 · Los cuatro bloques en `main`. Nueva
> lista de comprobación completa en el [paso 5](#5-comprobar-que-todo-funciona):
> qué ejecutar, qué tocar y qué tiene que salir en cada bloque.

---

## Estado actual

| Bloque | Qué hace | Estado |
|---|---|---|
| **P1** · Almacenamiento y ciclo de vida | Job de archivado caliente→frío, purga del frío, carga inicial | ✅ en `main` |
| **P2** · Ingesta | Simulador con jitter, Kafka, consumidor → caliente + cuarentena | ✅ en `main` |
| **P3** · Acceso | API: `/trips` con router de tiers, métricas, `/lifecycle/*` | ✅ en `main` |
| **P4** · Orquestación y observabilidad | 4 DAGs de Airflow, dashboard de Grafana | ✅ en `main` |

**Datos de trabajo:** de 2026, del 1 de enero hasta *ahora*, nunca en el
futuro. La muestra real (`datos/muestra_1000.csv`) es la semilla; el volumen lo
da `scripts/generar_datos_sinteticos.py` y el flujo en vivo, el simulador.

---

## 0. Preparación (una sola vez por máquina)

```bash
git clone https://github.com/gonzamargal-sketch/PIDS1-2.git pids-parte2 && cd pids-parte2
cp .env.example .env
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

Si ya tenéis el repo: `git checkout main && git pull`, y en cada terminal
nueva `source .venv/bin/activate`.

Requisitos: Docker funcionando (ver *Problemas frecuentes* en el
[README](../README.md)) y Python 3.11 o superior.

---

## 1. Base nueva y servicios

```bash
docker compose --profile "*" down -v          # BORRA todos los datos anteriores
docker compose --profile core --profile orch --profile viz up -d
docker compose ps                             # esperar a que todo esté "healthy" (~1 min)
```

- `down -v` borra los volúmenes: base de datos, MinIO, Airflow y Grafana. Es
  lo que garantiza que se ejecutan **todos** los SQL de `postgres/init/`,
  incluidos los de P2 y P4. Si no queréis perder datos, ver
  [Poner al día una base existente](#poner-al-día-una-base-existente).
- Perfiles: `core` = PostgreSQL + MinIO + API · `orch` = Airflow ·
  `viz` = Grafana · `stream` = Kafka + consumidor + simulador (paso 4).
- `minio-init` sale como `Exited (0)`: es normal, crea los buckets y termina.

---

## 2. Pruebas (antes de cargar datos)

```bash
python scripts/prueba_humo.py                 # → TODO CORRECTO
python scripts/prueba_archivado.py            # → TODO CORRECTO
```

**Hacedlas antes del paso 3**: las dos vacían el tier caliente para partir de
cero.

| Prueba | Qué comprueba |
|---|---|
| `prueba_humo.py` | Contrato de datos, esquema, inserción, vistas de métricas y que no se puede desalojar sin verificar |
| `prueba_archivado.py` | El ciclo de vida de punta a punta con el job real: política a 5 min, caídas a mitad y relanzado sin duplicar, conteos cuadrando, idempotencia y que si no cuadra no se borra nada |

---

## 3. Datos de 2026 (1M de filas)

```bash
python scripts/generar_datos_sinteticos.py 1000000 --seed 42    # ~25 s → datos/sinteticos/
python ingesta/subir_bronze.py --origen datos/sinteticos        # copia cruda a MinIO (bronze)
python ingesta/carga_inicial.py --recrear                       # ~1 min
```

- El generador reparte los viajes **de 2026-01-01 hasta ahora**, con el perfil
  horario de los taxis de Nueva York y las anomalías reales de la muestra. Los
  CSV van a `datos/sinteticos/`, que está fuera de git. Relanzarlo otro día da
  datos hasta ese día.
- La carga inicial pasa todo por el contrato de datos (los rechazos van a
  `trips_cuarentena`) y **reparte por edad**: lo anterior a hoy menos 30 días
  va a **Iceberg** (frío) y lo demás a **PostgreSQL** (caliente), sin solape.
- `--recrear` borra la tabla Iceberg y la carga anterior del caliente antes de
  volver a cargar. Se puede repetir sin duplicar.

---

## 4. Flujo en vivo

```bash
docker compose --profile core --profile stream --profile orch --profile viz up -d
```

Añade Kafka, el consumidor y el simulador, que emite **200 viajes por segundo**
a Kafka. El consumidor los valida y los escribe en el caliente o en
cuarentena. Cada viaje "acaba de terminar": `event_time` y la fecha del viaje
son de ahora.

Para parar solo el simulador: `docker compose stop simulador`.

---

## 5. Comprobar que todo funciona

Lista completa, bloque a bloque: **qué se ejecuta, qué se toca y qué tiene que
salir**. Hacedla entera después de los pasos 1-4. Si algo no sale como dice la
columna «Tiene que salir», ese bloque no está bien.

Para no repetir el comando largo de psql en cada línea (hay que definirlo en
cada terminal nueva, junto con `source .venv/bin/activate`):

```bash
alias pg='docker compose exec -T postgres psql -U pids -d pids -c'
```

### Dónde está cada cosa

| Dónde | Acceso | Qué es |
|---|---|---|
| **Airflow** · http://localhost:8080 | sin login | Los 4 DAGs de P4 |
| **Grafana** · http://localhost:3000 | `admin` / `admin` | Dashboard «E8 · Ciclo de vida de los datos» (se refresca cada 30 s) |
| **MinIO** · http://localhost:9001 | `minioadmin` / `minioadmin_dev_2026` | Buckets `bronze` (CSV crudos) y `lakehouse` (Parquet de Iceberg) |
| **API** · http://localhost:8000/docs | — | Swagger: todos los endpoints, con botón *Try it out* |

### 5.1 Servicios (todos)

| Ejecutar / tocar | Tiene que salir |
|---|---|
| `docker compose ps -a` | Todo `Up (healthy)`, salvo `pids_minio_init` en `Exited (0)` (crea los buckets y termina) |
| `curl -s localhost:8000/health` | `{"estado":"ok"}` |
| `pg "SELECT count(*) FROM taxi_trips_default;"` | `0`. Si hay filas, llegaron datos de días sin partición: ver *Problemas frecuentes* del README |

### 5.2 P1 · Almacenamiento y carga

| Ejecutar / tocar | Tiene que salir |
|---|---|
| `python scripts/prueba_humo.py` *(solo antes del paso 3: vacía el caliente)* | `TODO CORRECTO` |
| `python scripts/prueba_archivado.py` *(ídem)* | `TODO CORRECTO`, con cada paso en `[OK]` |
| `python ingesta/subir_bronze.py --listar` | Los CSV de `datos/sinteticos/` bajo `nyc-taxi/2026/` |
| MinIO → bucket `lakehouse` | Carpetas `lakehouse/trips/data/` (Parquet por mes) y `metadata/` |
| `curl -s localhost:8000/stats` | `hot.filas` ≈ los últimos 30 días y `cold.filas` con el resto; `cold.bytes_por_fila` en torno a 55 |
| `pg "SELECT min(event_time), max(event_time) FROM taxi_trips;"` | El mínimo, de hace ~30 días; el máximo, de hoy. **Nunca en el futuro** |
| `pg "SELECT count(*) FROM taxi_trips WHERE event_time > NOW();"` | `0` |
| `pg "SELECT * FROM v_coste_por_tier;"` | PostgreSQL ocupa **varias veces más** por fila que Iceberg (~320-550 B frente a ~55 B) |
| `pg "SELECT * FROM v_cumplimiento_politica;"` | `estado = OK`: nada en el caliente más viejo que el umbral |

### 5.3 P2 · Ingesta en vivo (perfil `stream`, paso 4)

| Ejecutar / tocar | Tiene que salir |
|---|---|
| `docker compose logs --tail 3 simulador` | `N emitidos (200 ev/s de media)`, con N subiendo. Al arrancar sale `Emitiendo a 'kafka' · 200.0 ev/s · … · jitter=True` |
| `docker compose logs --tail 3 consumidor` | `N recibidos → X en caliente, Y en cuarentena, 0 duplicados`, con N subiendo |
| `pg "SELECT origen, count(*) FROM taxi_trips GROUP BY 1;"` (dos veces, con unos segundos entre medias) | `stream` sube ~200 por segundo; `carga_inicial` no cambia |
| `pg "SELECT max(event_time), NOW() FROM taxi_trips WHERE origen='stream';"` | El último evento, de hace unos segundos |
| `pg "SELECT * FROM v_calidad;"` | Motivos de rechazo (`fecha_fuera_de_rango`, `tipo_invalido`…) con registros: la suciedad del simulador acaba en cuarentena, no en el caliente |
| `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group pids-consumidor` | Columna `LAG` cerca de 0: el consumidor va al día |

**Prueba de resiliencia (no se pierde ni se duplica nada):**

```bash
docker compose stop consumidor        # el simulador sigue emitiendo a Kafka
# esperad ~30 s: el LAG del comando de arriba crece
docker compose start consumidor       # se pone al día
docker compose logs --tail 3 consumidor
```

Tiene que salir: el `LAG` vuelve a ~0 y el consumidor sigue con `0 duplicados`.
Kafka guardó los mensajes mientras estaba parado.

Para parar solo el simulador: `docker compose stop simulador`. El contador de
`stream` deja de subir.

### 5.4 P4 · Orquestación (Airflow)

| Ejecutar / tocar | Tiene que salir |
|---|---|
| Airflow → *Dags* | Los 4 activos (no en pausa) y sin errores de importación |
| `docker compose exec airflow airflow dags list-import-errors` | `No data found` |
| DAG `archivar` → *Runs* | Una ejecución cada 5 min, en verde. Al terminar dispara `estadisticas_frio` |
| DAG `estadisticas_frio` | En verde, cada 5 min |
| DAG `mantener_particiones` → ▶ *Trigger* | En verde. Luego `pg "SELECT particion FROM v_particiones ORDER BY dia DESC LIMIT 3;"` muestra particiones hasta 7 días por delante |
| DAG `purga_final` | En verde (diario). Con la política de 7 años no borra nada |
| `pg "SELECT medido_en, filas FROM hot_stats ORDER BY id DESC LIMIT 1;"` | `medido_en` de hace menos de 5 min: las estadísticas se están refrescando |

### 5.5 P4 · Grafana

Dashboard «E8 · Ciclo de vida de los datos». Qué tiene que verse en cada fila:

| Fila del dashboard | Tiene que salir |
|---|---|
| **0 · La política** | Umbral de 30 días, custodia en frío de 7 años, registro más antiguo en caliente ≤ 30 días, cumplimiento **OK**, 0 particiones pendientes |
| **1 · Migración** | «Filas por tier en el tiempo» con las dos áreas (caliente subiendo si hay flujo en vivo). «Máquina de estados» con las particiones en `DESALOJADO` |
| **2 · Coste** | «Bytes por fila»: la barra de PostgreSQL varias veces más alta. Ratio de compresión > 1. «Frío por partición mensual» con un mes por barra desde enero |
| **3 · Latencia** | Datos en cuanto se hayan hecho consultas a la API (5.6). p95 caliente < 500 ms y p95 frío < 10 s, en verde |
| **4 · Calidad** | Registros en cuarentena > 0 y los motivos de rechazo |

Si la fila 3 está vacía, haced unas cuantas llamadas a `/trips` (5.6) y
esperad al siguiente refresco.

### 5.6 P3 · API

| Ejecutar / tocar | Tiene que salir |
|---|---|
| `curl -s localhost:8000/stats` | `data.hot` y `data.cold`, con `meta.data_source = mixto` |
| `curl -s localhost:8000/lifecycle/policy` | Dos políticas: `ARCHIVE` a 30 días y `DELETE` a 7 años |
| `curl -s localhost:8000/lifecycle/status` | `resumen_por_estado`, `candidatas_a_archivar` vacía y `cumplimiento.estado = OK` |
| `curl -s localhost:8000/metrics/coste` (y `latencia`, `calidad`, `caliente`) | Lo mismo que las vistas `v_*` de PostgreSQL |
| `curl -s localhost:8000/metrics/noexiste` | Error `404` con la lista de métricas disponibles |
| `/trips` solo caliente, solo frío y cruzando la frontera (ver abajo) | `data_source` `hot`, `cold` y `mixto`; en el mixto, `coverage` con un tramo `cold` y otro `hot` que se tocan a las 00:00 del primer día del caliente |
| `curl -s "localhost:8000/trips?desde=2026-09-22&hasta=2026-09-20"` | `400`: `desde` tiene que ser anterior a `hasta` |
| `curl -s -X PUT localhost:8000/lifecycle/policy -H 'content-type: application/json' -d '{"umbral_valor":0,"umbral_unidad":"weeks"}'` | `422`: la política se valida antes de tocar la base |
| `pg "SELECT endpoint, data_source, count(*) FROM query_log GROUP BY 1,2;"` | Una fila por cada llamada hecha, con su tier |

**Prueba de que el router no pierde nada.** Una consulta que cruza la
frontera tiene que devolver exactamente las filas del caliente más las del frío
en ese rango (poned un rango que cruce la frontera y que no pase de 100.000
filas):

```bash
curl -s "localhost:8000/trips?desde=2026-08-10&hasta=2026-09-29&limite=100000" \
  | python -c "import json,sys; m=json.load(sys.stdin)['meta']; print(m['data_source'], m['rows'])"
pg "SELECT count(*) FROM taxi_trips WHERE event_time >= '2026-08-10' AND event_time < '2026-09-29';"
python -c "from datetime import datetime as D, timezone as Z; from common.lakehouse import obtener_tabla, contar_particion; print(contar_particion(obtener_tabla(crear=False), D(2026,8,10,tzinfo=Z.utc), D(2026,9,29,tzinfo=Z.utc)))"
```

Tiene que salir: las filas de la API = caliente + frío. Con el flujo en vivo
encendido, parad antes el simulador, que si no el caliente cambia entre una
consulta y otra.

### 5.7 Referencia rápida de la API (P3)

Toda respuesta que toca datos lleva `data` + `meta` (`data_source`,
`coverage`, `as_of`, `latency_ms`, `rows`), y cada llamada deja una fila en
`query_log`, que es de donde salen las latencias por tier de Grafana.

```bash
curl -s localhost:8000/stats                  # filas en caliente + estadísticas del frío
curl -s localhost:8000/lifecycle/policy       # la política vigente
curl -s localhost:8000/lifecycle/status       # archival_jobs, candidatas y cumplimiento
curl -s localhost:8000/metrics/coste          # también: latencia, calidad, caliente

# El router de tiers: /trips filtra por event_time en [desde, hasta)
# (sustituid las fechas: el caliente son los últimos 30 días)
curl -s "localhost:8000/trips?desde=2026-09-20&hasta=2026-09-22"   # solo caliente → data_source=hot
curl -s "localhost:8000/trips?desde=2026-08-01&hasta=2026-08-03"   # solo frío     → data_source=cold
curl -s "localhost:8000/trips?desde=2026-08-28&hasta=2026-08-31"   # cruza frontera → mixto, coverage con 2 tramos
```

`limite` (por defecto 1000, máximo 100.000) es global a la consulta. Sin zona
horaria, las fechas se interpretan en UTC.

**La frontera sigue a los datos, no al umbral.** El router mira en cada
consulta qué días siguen en PostgreSQL: un día está en caliente si su
partición existe y no está `DESALOJADO` en `archival_jobs`. Por eso los tramos
del `coverage` empiezan y acaban a las 00:00 UTC, y la frontera es el primer
día del caliente, no «hace 30 días a esta hora». Explicación completa en
[ARQUITECTURA §6](ARQUITECTURA.md).

Referencia de la prueba en instalación limpia (3M de filas; con 1M, todo en
proporción): Kafka 35.800 emitidos = 35.800 recibidos; una fila ocupa
**~320 B en PostgreSQL y ~54 B en Iceberg** (unas 6 veces menos).

---

## 6. La demo: ver la migración caliente → frío

```bash
# 1. Bajar la política desde la API, sin redesplegar nada. La respuesta dice
#    cuántas particiones pasan a ser candidatas en ese mismo momento
curl -s -X PUT localhost:8000/lifecycle/policy \
  -H 'content-type: application/json' -d '{"umbral_valor":5,"umbral_unidad":"minutes"}'

# 2. En la siguiente pasada de "archivar" (cada 5 min) los días anteriores a
#    hoy pasan al frío. Seguirlo: todo en DESALOJADO y origen = escritas
docker compose exec postgres psql -U pids -d pids -c \
  "SELECT estado, count(*), sum(filas_origen) origen, sum(filas_escritas) escritas FROM archival_jobs GROUP BY 1;"

#    (o por la API: curl -s localhost:8000/lifecycle/status)

# 3. Al acabar, volver a la política real
curl -s -X PUT localhost:8000/lifecycle/policy \
  -H 'content-type: application/json' -d '{"umbral_valor":30,"umbral_unidad":"days"}'
```

- Mientras tanto, `/trips` sigue saliendo completo: un día se pide al frío
  en cuanto el DAG lo desaloja, no antes. Repetir la consulta que cruza la
  frontera durante la demo muestra cómo avanza el `coverage` al frío.
- Para no esperar a la siguiente pasada, se puede lanzar a mano desde Airflow
  (botón ▶ del DAG `archivar`) o con
  `docker compose exec airflow airflow dags trigger archivar`.
- **Lo de hoy no se archiva hasta mañana**: las particiones son diarias y solo
  se mueven los días anteriores al corte. Por eso la carga inicial deja los
  últimos 30 días en el caliente: son los que se ven migrar.
- En Grafana se ve bajar el caliente y subir el frío. Estado de cada partición:
  `SELECT * FROM archival_jobs ORDER BY particion;`

> ⚠️ **La demo no se deshace.** Volver a poner 30 días no devuelve los datos
> al caliente: se quedan en Iceberg. Para repetirla desde cero, volved a hacer
> el paso 3 (`carga_inicial.py --recrear`). Si vais a grabar el vídeo, haced
> antes toda la lista del paso 5.

**Qué tiene que verse en cada momento:**

| Momento | Dónde | Tiene que salir |
|---|---|---|
| Antes | `curl -s "localhost:8000/trips?desde=<hace 32 días>&hasta=<hace 28 días>"` | `mixto`, con la frontera hace ~30 días |
| Tras el `PUT` a 5 min | La respuesta del `PUT` | `umbral_valor: 5`, `umbral_unidad: minutes` y `particiones_candidatas_ahora` > 0 (todos los días anteriores a hoy) |
| | Grafana, fila 0 | Umbral de 5 min, cumplimiento en **INCUMPLE** y particiones pendientes > 0 |
| | `/trips` otra vez | Igual que antes: aún no se ha movido nada, así que el caliente se sigue leyendo de PostgreSQL |
| Durante el DAG `archivar` | `curl -s localhost:8000/lifecycle/status` o Grafana «Máquina de estados» | Las particiones pasan por `ESCRIBIENDO → ESCRITO → VERIFICADO → DESALOJADO`, de la más antigua a la más reciente |
| Después | Consulta del paso 2 | Todo en `DESALOJADO` y `origen = escritas` (no se ha perdido ninguna fila) |
| | Grafana, fila 0 | Cumplimiento de vuelta en **OK** (en el caliente solo queda lo de hoy) y 0 pendientes |
| | Grafana, fila 1 | El área del caliente baja y la del frío sube en la misma cantidad |
| | `curl -s localhost:8000/stats` | `hot.filas` = solo lo de hoy; `cold.filas` ha subido lo que ha bajado el caliente |
| | `/trips` del rango anterior | Ahora `cold`; y un rango de ayer a mañana sale `mixto`, con la frontera a las 00:00 de hoy |
| | Grafana, fila 3 | Las consultas al frío con más latencia que las del caliente, las dos dentro del SLA |
| Al volver a 30 días | La respuesta del `PUT` | `particiones_candidatas_ahora: 0`, y el cumplimiento vuelve a **OK** |

### Purga del frío (cierre del ciclo de vida)

La política de borrado es de 7 años, así que con datos de 2026 no borra nada.
Para ver qué haría, sin borrar:

```bash
docker compose exec airflow /opt/pids-venv/bin/python -m archivado.purga --simulacro
```

---

## Parar y retomar

```bash
docker compose --profile "*" stop             # para todo y CONSERVA los datos
docker compose --profile core --profile stream --profile orch --profile viz up -d   # retomar
```

---

## Poner al día una base existente

Si no queréis hacer `down -v` después de un `git pull`, aplicad los SQL
nuevos. Son idempotentes y no borran nada:

```bash
docker compose exec -T postgres psql -U pids -d pids < postgres/init/05_p2.sql
docker compose exec -T postgres psql -U pids -d pids < postgres/init/07_p4.sql
docker restart pids_grafana     # para que cargue el datasource y el dashboard
```

Y para tener los datos de 2026, los pasos 3 y 4.

---

## Pendiente

Lo que falta para tener todo E8, en el orden en que se irá añadiendo a esta
guía:

- [ ] **Consultas de ejemplo del vídeo** con las fechas definitivas (solo
  caliente, solo frío y una que cruce la frontera).
- [ ] **Mediciones (T5.1)**: las seis métricas de §7 con capturas, en
  `docs/MEDICIONES.md`.
- [ ] **Memoria (T5.2)** y **vídeo (T5.3)**.
