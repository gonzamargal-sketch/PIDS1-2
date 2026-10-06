# Guía de funcionamiento

Cómo levantar el proyecto completo, cargar los datos, comprobar que todo
funciona y ver el ciclo de vida de E8 en marcha. **Es la guía viva del
grupo**: se actualiza cada vez que entra una parte nueva o cambia cómo se hace
algo.

> **Última actualización:** 2026-10-05 · Botones en la pestaña **En vivo**:
> encender y apagar el simulador y meter viajes de ejemplo
> ([5.8](#58-frontend-web-p3)). Guion del vídeo de 60-90 s y cómo montarlo en
> Recordly ([paso 9](#9-guion-del-vídeo-60-90-s)). Antes: chatbot de la Parte 3 (perfil
> `chat`, [paso 8](#8-chatbot-parte-3)) y frontend web de la API en
> `localhost:8000/app/` ([5.8](#58-frontend-web-p3)). El
> [paso 5](#5-comprobar-que-todo-funciona) es para **comprobar** que todo
> funciona; el [paso 7](#7-qué-podéis-hacer-vosotros-tocar-el-sistema), para
> **tocarlo**: meter viajes, cambiar la política, mover datos y provocar fallos.

---

## Estado actual

| Bloque | Qué hace | Estado |
|---|---|---|
| **P1** · Almacenamiento y ciclo de vida | Job de archivado caliente→frío, purga del frío, carga inicial | ✅ en `main` |
| **P2** · Ingesta | Simulador con jitter, Kafka, consumidor → caliente + cuarentena | ✅ en `main` |
| **P3** · Acceso | API: `/trips` con router de tiers, métricas, `/lifecycle/*` | ✅ en `main` |
| **P4** · Orquestación y observabilidad | 4 DAGs de Airflow, dashboard de Grafana | ✅ en `main` |
| **Parte 3** · Chatbot | Streamlit → FastAPI → OpenRouter con herramientas sobre la API; ruta `/trips/resumen` | 🔶 rama `parte3/chatbot` |

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

Para parar solo el simulador: botón **Apagar** en la pestaña **En vivo** del
frontend ([5.8](#58-frontend-web-p3)), o `docker compose stop simulador`.

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
| **Frontend** · http://localhost:8000/app/ | — | La API con interfaz: recorrido de un dato, ingesta en vivo, viajes por rango, política y archivado en directo, métricas y explorador de rutas ([5.8](#58-frontend-web-p3)) |
| **API** · http://localhost:8000/docs | — | Swagger: todos los endpoints, con botón *Try it out* |
| **Chatbot** · http://localhost:8501 | — | Asistente de la Parte 3 (perfil `chat`, [paso 8](#8-chatbot-parte-3)) |

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
| Frontend → **En vivo** (o `curl -s localhost:8000/ingesta`) | Ritmo en torno a 200/s, último viaje llegado hace segundos y retraso ~0 |
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
Kafka guardó los mensajes mientras estaba parado. En la pestaña **En vivo** del
frontend se ve sin comandos: el hueco, el pico al volver y el retraso subiendo y
bajando.

Para parar solo el simulador: botón **Apagar** de la pestaña En vivo, o
`docker compose stop simulador`. El contador de
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

**Para verlo sin curl:** el frontend ([5.8](#58-frontend-web-p3)).

**Las únicas rutas que escriben viajes** son las de los botones de En vivo:
`POST /ingesta/muestras` (8 viajes de ejemplo, por el mismo contrato de
datos) y `PUT /simulador` con `{"activo": false}` o `true`. Apagar el
simulador no para el contenedor: cambia una fila (`simulador_control`) que el
simulador mira cada segundo.

Toda respuesta que toca datos lleva `data` + `meta` (`data_source`,
`coverage`, `as_of`, `latency_ms`, `rows`), y cada llamada deja una fila en
`query_log`, que es de donde salen las latencias por tier de Grafana.

```bash
curl -s localhost:8000/stats                  # filas en caliente + estadísticas del frío
curl -s localhost:8000/lifecycle/policy       # la política vigente
curl -s localhost:8000/lifecycle/status       # archival_jobs, candidatas y cumplimiento
curl -s localhost:8000/metrics/coste          # también: latencia, calidad, caliente
curl -s localhost:8000/ingesta                # lo que llega ahora del flujo en vivo (no va a query_log)

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

### 5.8 Frontend web (P3)

http://localhost:8000/app/ (también `localhost:8000/`, que redirige). Es
HTML + JS sin compilar en `api/web/`, servido por la propia API: no hay que
levantar nada más, y como la carpeta está montada, un cambio se ve recargando
la página. Solo usa las rutas de siempre; no tiene lógica propia.

| Pestaña | Rutas que usa | Qué probar | Tiene que salir |
|---|---|---|---|
| **En vivo** | `/ingesta`, `/simulador` (GET y PUT), `/ingesta/muestras` | Abrirla con el perfil `stream` levantado; **Apagar** y **Encender** el simulador; **Meter muestras**; parar y arrancar el consumidor | Ritmo en torno a 200/s, último viaje llegado hace < 2 s y retraso **al día**. Al apagar, en un segundo el ritmo cae a 0 y sale **parado**; al encender, vuelve. «Meter muestras» dice 4 al caliente y 4 a cuarentena, y las 8 salen en «Metido a mano» con su motivo. Con el consumidor parado: barras a cero; al arrancarlo, un pico de llegadas y el retraso sube y vuelve a ~0 |
| **Inicio** | `/stats`, `/lifecycle/*`, `/metrics/coste`, `/metrics/calidad` | Abrirla | El recorrido entrada → caliente → frío → borrado con las filas de cada tier, cumplimiento **OK** y el frío ocupando varias veces menos por fila |
| **Viajes** | `/trips` | Rango rápido «Cruzando la frontera» | Origen **mixto**, la barra de tramos con frío y caliente tocándose a las 00:00, el histograma en dos colores y cada viaje con su etiqueta de tier |
| **Ciclo de vida** | `/lifecycle/policy` (GET y PUT), `/lifecycle/status` | «Demo: 5 minutos» (pide confirmación) | «N particiones pasan a estar pendientes»; en ≤ 5 min la máquina de estados las mueve a `DESALOJADO` sola (se refresca cada 5 s). **Al acabar, «Volver a 30 días»** |
| **Métricas** | `/metrics/{coste,latencia,calidad,caliente}` | Abrirla tras hacer unas consultas | Bytes por fila por tier, p95 por tier con su objetivo en verde y los motivos de cuarentena |
| **Explorador** | Todas | «Errores a propósito» y *Enviar* en cada ruta | `400`, `404` y `422` respectivamente; cada ruta con su `curl` para copiar y el JSON de respuesta. Abajo, el registro de todas las llamadas del frontend |

Cada bloque lleva debajo el `meta` de su respuesta (ruta, `data_source`,
latencia, `as_of`), desplegable para ver el JSON. Las horas, siempre en UTC.
`localhost:8000/visor` (el visor de antes) redirige a la pestaña Viajes.

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

## 7. Qué podéis hacer vosotros (tocar el sistema)

El paso 5 es para **mirar**; esto es para **actuar**: meter viajes, cambiar la
política, mover datos a mano y ver cómo reacciona todo. Cada apartado dice
qué hacer y dónde se ve el efecto. Si no dice lo contrario, se puede repetir y
no rompe nada.

### 7.1 Añadir un viaje a mano

**Sin terminal:** pestaña **En vivo** → **Meter muestras**. Mete 8 viajes, uno
por cada salida del contrato (los de la tabla de abajo), y dice dónde ha
acabado cada uno.

```bash
python scripts/anadir_viaje.py                                    # un viaje normal
python scripts/anadir_viaje.py --distancia 12 --minutos 35 --importe 48.5 --propina 9
```

Opciones: `--distancia` (millas), `--minutos` (duración), `--importe` y
`--propina` (en $), `--pasajeros`, `--pago` (1 tarjeta, 2 efectivo),
`--zona-origen` y `--zona-destino` (1-265). El viaje "acaba de terminar"
(`event_time` = ahora), así que siempre cae en el caliente.

Pasa por el **mismo contrato de datos** que el flujo en vivo. El script dice
dónde ha acabado y por qué:

| Probad con | Resultado |
|---|---|
| *(nada)* | **Caliente** |
| `--importe -20` | **Caliente con aviso** `importe_negativo` (sospechoso pero posible: una devolución) |
| `--distancia 30 --minutos 5` | **Caliente con aviso** `velocidad_imposible` |
| `--pasajeros 0` | **Caliente con aviso** `sin_pasajeros` |
| `--distancia 600` | **Cuarentena**: `distancia_excesiva` |
| `--minutos 400` | **Cuarentena**: `duracion_excesiva` |
| `--minutos 0` | **Cuarentena**: `cronologia_invalida` (baja a la vez que sube) |
| `--importe 20000` | **Cuarentena**: `importe_excesivo` |
| `--zona-origen 999` | **Cuarentena**: `zona_fuera_de_rango` |

Dónde se ve: en el panel «Metido a mano» de la pestaña **En vivo** del
frontend (si ha ido al caliente o a cuarentena, y por qué); el propio script
imprime la consulta para verlo; en
`curl -s "localhost:8000/trips?desde=<hace 5 min>&hasta=<dentro de 1 min>"`
(las fechas en UTC, p. ej. `2026-09-28T12:25:00`), con `origen = manual`; y los
rechazados, en `/metrics/calidad` y en la fila 4 de Grafana. Las reglas están
en `common/validacion.py`.

### 7.2 Mandar viajes por Kafka a mano

Necesita el perfil `stream` levantado (paso 4). Con `--json` el script no
escribe nada: saca el mensaje, y se lo pasamos a Kafka como lo haría el
simulador:

```bash
python scripts/anadir_viaje.py --json --distancia 7 --importe 31 | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic trips.raw

# Un mensaje que ni siquiera es JSON
echo 'esto no es json' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic trips.raw
```

Dónde se ve: panel «Metido a mano» de la pestaña **En vivo**, o
`docker compose logs --tail 2 consumidor`, que cuenta uno más en
caliente y uno más en cuarentena (tarda hasta 10 s en escribir el log). El
segundo acaba en `trips_cuarentena` con el motivo `mensaje_ilegible` y el texto
tal cual en `payload`.

### 7.3 Meter muchos viajes de golpe

```bash
# N viajes directos a PostgreSQL, sin Kafka (no hace falta el perfil stream)
docker compose run --rm --no-deps simulador \
  python -m simulador.simulador --sumidero postgres --total 5000 --eps 0

# Lo mismo con mucha más suciedad, para llenar la cuarentena
docker compose run --rm --no-deps simulador \
  python -m simulador.simulador --sumidero postgres --total 5000 --eps 0 --pct-suciedad 20
```

`--eps 0` es "tan rápido como se pueda"; `--eps 50` los emite a 50 por
segundo. Quitando `--sumidero postgres` van por Kafka (entonces sí hace falta
el perfil `stream`).

Para cambiar el **flujo en vivo** que va siempre, editad en `.env`
`SIMULADOR_EVENTOS_POR_SEGUNDO` (por defecto 200) o
`SIMULADOR_PCT_SUCIEDAD_EXTRA` (por defecto 1.0) y recread el simulador:
`docker compose up -d simulador`.

### 7.4 Más datos históricos

La forma segura es regenerar y **recargar todo** (paso 3), con otro tamaño o
rango:

```bash
python scripts/generar_datos_sinteticos.py 3000000 --seed 7        # 3M en vez de 1M
python scripts/generar_datos_sinteticos.py 500000 --desde 2026-06-01
python ingesta/subir_bronze.py --origen datos/sinteticos
python ingesta/carga_inicial.py --recrear
```

Borrad antes los CSV viejos de `datos/sinteticos/`, que si no se cargan
también. **No carguéis un CSV encima sin `--recrear`** si ya habéis hecho la
demo: podría volver a crear en el caliente particiones de días que ya están en
el frío, y el router dejaría de ver esas filas.

### 7.5 Cambiar la política de archivado (lo de los 5 minutos)

Es un dato, no código: se cambia en caliente, sin reiniciar nada. Unidades:
`minutes`, `hours`, `days` o `years`.

```bash
curl -s -X PUT localhost:8000/lifecycle/policy -H 'content-type: application/json' \
  -d '{"umbral_valor":5,"umbral_unidad":"minutes"}'     # o 2 hours, 7 days, 30 days…
```

- La respuesta dice cuántas particiones pasan a ser candidatas **en ese
  momento** (`particiones_candidatas_ahora`).
- **Los datos no se mueven al hacer el PUT**, sino en la siguiente pasada del
  DAG `archivar` (cada 5 min, o lanzándolo a mano, 7.7).
- Se mueven **días completos**: con un umbral de minutos u horas sale
  prácticamente lo mismo (todo lo anterior a hoy). Para ver una diferencia real entre
  umbrales, probad `7 days` frente a `20 days`.
- **Bajar es irreversible**: al volver a subir el umbral no vuelve nada al
  caliente. Para empezar de cero, el paso 3.
- También se puede tocar desde Swagger (http://localhost:8000/docs →
  `PUT /lifecycle/policy` → *Try it out*), o con SQL directo:
  `pg "UPDATE retention_policy SET umbral_valor=5, umbral_unidad='minutes' WHERE accion='ARCHIVE';"`

Dónde se ve: `/lifecycle/status`, fila 0 de Grafana (umbral, cumplimiento y
pendientes) y, tras el DAG, filas 1 y 2.

### 7.6 Cambiar la política de borrado del frío (purga)

La de borrado es otra fila de la misma tabla (`accion=DELETE`, 7 años). **Lo
que se purga se borra de verdad**: mirad siempre antes el simulacro.

No existe la unidad `months` (daría `422`): los meses se ponen en días. La
purga borra **meses completos** del frío.

```bash
# 1. Bajarla, p. ej. a 180 días: con datos desde enero, borraría los meses más viejos
curl -s -X PUT "localhost:8000/lifecycle/policy?accion=DELETE" \
  -H 'content-type: application/json' -d '{"umbral_valor":180,"umbral_unidad":"days"}'

# 2. Ver qué borraría, sin borrar nada
docker compose exec airflow /opt/pids-venv/bin/python -m archivado.purga --simulacro

# 3. Si de verdad se quiere ver borrar: DAG purga_final → ▶ en Airflow

# 4. Volver a la real
curl -s -X PUT "localhost:8000/lifecycle/policy?accion=DELETE" \
  -H 'content-type: application/json' -d '{"umbral_valor":7,"umbral_unidad":"years"}'
```

Dónde se ve: fila 2 de Grafana, «Frío por partición mensual», pierde los meses
purgados; `curl -s localhost:8000/stats` baja `cold.filas`.

### 7.7 Mover datos a mano (sin esperar al DAG)

```bash
# Qué se llevaría ahora el archivado, sin mover nada
docker compose exec airflow /opt/pids-venv/bin/python -m archivado.job_archivado --simulacro

# Archivar un solo día concreto (tiene que ser candidato según la política)
docker compose exec airflow /opt/pids-venv/bin/python -m archivado.job_archivado --dia 2026-08-30

# Lanzar un DAG ya (también con el botón ▶ en Airflow)
docker compose exec airflow airflow dags trigger archivar
docker compose exec airflow airflow dags trigger mantener_particiones
```

**Congelar el archivado** para enseñar algo con calma:

```bash
docker compose exec airflow airflow dags pause archivar      # deja de pasar
docker compose exec airflow airflow dags unpause archivar    # vuelve
```

Truco para el vídeo: pausar `archivar`, hacer el `PUT` a 5 minutos y enseñar
que el cumplimiento sale **INCUMPLE** con particiones pendientes, pero `/trips`
sigue devolviéndolo todo desde el caliente (el router sigue a los datos, no a
la política). Después, `unpause` y ver cómo se mueven. **Acordaos del
`unpause`.**

### 7.8 Consultar datos

| Qué | Cómo |
|---|---|
| Viajes de un rango, del tier que sea | `curl -s "localhost:8000/trips?desde=…&hasta=…&limite=…"` (fechas ISO; sin zona = UTC; `limite` 1-100.000) |
| Lo mismo, con botones | http://localhost:8000/docs → `GET /trips` → *Try it out* |
| Cualquier métrica | `/metrics/coste`, `/metrics/latencia`, `/metrics/calidad`, `/metrics/caliente` |
| SQL libre sobre el caliente | `pg "SELECT …"` (alias del paso 5) |
| Los Parquet del frío | MinIO → `lakehouse/lakehouse/trips/data/` |

Cada consulta a la API queda en `query_log` y alimenta la fila 3 de Grafana:
hacer varias al frío y al caliente es la forma de llenar la gráfica de
latencias para el vídeo.

### 7.9 Provocar fallos

| Qué | Cómo | Tiene que pasar |
|---|---|---|
| Se cae el consumidor | `docker compose stop consumidor`, esperar, `start` | Kafka guarda los mensajes; al volver se pone al día sin perder ni duplicar (5.3) |
| Se cae el archivado a mitad | `python scripts/prueba_archivado.py` *(vacía el caliente: solo antes del paso 3)* | Relanzado, termina sin duplicar y sin borrar nada que no esté verificado |
| Se cae la API | `docker compose restart api` | En ~15 s vuelve `healthy`; no se pierde nada, la API no guarda estado |

### 7.10 Volver al estado normal

| Si habéis… | Para deshacerlo |
|---|---|
| Bajado la política de archivado | `PUT` a `{"umbral_valor":30,"umbral_unidad":"days"}` (los datos movidos se quedan en el frío) |
| Bajado la de borrado | `PUT ?accion=DELETE` a `{"umbral_valor":7,"umbral_unidad":"years"}` |
| Pausado un DAG | `airflow dags unpause <dag>` o el interruptor en Airflow |
| Metido viajes a mano | `pg "DELETE FROM taxi_trips WHERE origen='manual';"` |
| Movido datos y queréis empezar de cero | Paso 3 (`carga_inicial.py --recrear`) |
| Roto algo y no sabéis qué | Pasos 1-3 enteros (`down -v` y a empezar) |

---

## 8. Chatbot (Parte 3)

```bash
docker compose --profile core --profile chat up -d --build   # la primera vez con --build
```

Abrid **http://localhost:8501** (o «Asistente ↗» en el menú del frontend).

```
Streamlit (chat-ui, :8501) → FastAPI (chatbot, :8001) → SDK openai → OpenRouter → modelo
                                   └→ herramientas → API de la Parte 2 (/trips/resumen, /lifecycle…)
```

- **Sin clave funciona igual**, en **modo simulado**: un analizador por reglas
  hace de modelo (intención, zona y fechas) y propone las mismas llamadas a
  herramientas. Sirve para probar todo el circuito sin coste. No entiende el
  contexto de la conversación ni frases complicadas: eso lo hace el modelo real.
- **Con el modelo real (gratis):** por defecto usa modelos **gratuitos** de
  OpenRouter (sufijo `:free`): `qwen/qwen3.8-27b:free` y, si está saturado o
  limitado, OpenRouter salta solo a `google/gemma-4-31b-it:free` y luego a
  `nvidia/nemotron-3-super-120b-a12b:free`. Hace falta igualmente una clave
  (cuenta gratuita en openrouter.ai, sin saldo). Los gratuitos tienen cupo de
  peticiones por minuto y por día: si se agota, el chat lo dice y basta con
  esperar. La clave solo llega al contenedor `chatbot`; la interfaz nunca la ve.

  ```bash
  # en .env (sale de https://openrouter.ai/keys)
  OPENROUTER_API_KEY=sk-or-v1-...
  # opcionales: cualquier modelo con tools; vacíos = los gratuitos de arriba
  CHATBOT_MODELO=
  CHATBOT_MODELOS_RESPALDO=
  ```
  ```bash
  docker compose --profile core --profile chat up -d --force-recreate chatbot
  ```
  La barra lateral del chat dice en qué modo está.

**Herramientas** (todas de solo lectura; el contrato que ve el modelo está en
`curl -s localhost:8001/herramientas`):

| Herramienta | Qué hace | Llama a |
|---|---|---|
| `consultar_ingresos(inicio, fin, zona?)` | Viajes, ingresos, propinas, medias de un periodo | `/trips/resumen` |
| `desglose_viajes(inicio, fin, por=dia\|zona)` | Evolución por día o ranking de zonas | `/trips/resumen?agrupar=` |
| `buscar_zona(texto)` | «JFK» → zona 132 (tabla oficial de la TLC, `chatbot/datos/zonas_taxi.csv`) | — |
| `ver_viajes(inicio, fin, limite≤20)` | Unos viajes de ejemplo | `/trips` |
| `estado_ciclo_vida()` | Política, cumplimiento, particiones archivadas | `/lifecycle/*` |
| `metricas(nombre)` | Coste, latencia, calidad o estado del caliente | `/metrics/*` |

Cada llamada se valida con Pydantic y con las reglas del escenario antes de
tocar la API (solo 2026, nada del futuro, `inicio < fin`, zonas 1-265, como
mucho 20 viajes), y devuelve valor, periodo y origen (tier y `as_of`). Límites:
5 pasos de herramientas, 90 s por pregunta, 30 s por llamada y 1.500 tokens por
respuesta (`CHATBOT_*` en `chatbot/config.py`).

**Comprobar que funciona:**

| Ejecutar / tocar | Tiene que salir |
|---|---|
| `curl -s localhost:8001/health` | `estado: ok` y `modo: simulado` u `openrouter` |
| Botón «¿Cuánto se facturó en JFK en agosto?» | Viajes e ingresos de JFK en agosto, con el origen (frío). En «Cómo lo he resuelto»: `buscar_zona` → `consultar_ingresos(zona=132)` |
| «¿Qué zonas generaron más ingresos la última semana?» | Ranking con nombres de zona, origen caliente |
| «¿Cuánto se facturó en noviembre?» | Que no hay datos del futuro (la herramienta lo rechaza; en modo real, el modelo lo explica) |
| «Baja la política a 5 minutos» (modo real) | Que no puede: es de solo lectura y se hace desde el frontend |
| Explorador del frontend → `GET /trips/resumen` | Totales con `meta`; los de un rango mixto = caliente + frío |

Cada respuesta enseña en «Cómo lo he resuelto» las herramientas, los argumentos
ya validados, el resultado de la API, los tokens, el coste (0 $ con los
gratuitos) y qué modelo respondió de verdad (el principal o uno de respaldo).

---

## 9. Guion del vídeo (60-90 s)

El vídeo dura **entre 1 y 1,5 minutos**, así que no se enseña todo: solo lo
que demuestra E8 de un vistazo. La forma de conseguirlo es **grabar una toma
larga** (unos 10 minutos, con las esperas incluidas) y **recortarla en
Recordly** ([9.4](#94-montaje-en-recordly)): se quitan las esperas y se
aceleran los tramos lentos.

Se graba **solo la ventana del navegador** (pestañas del frontend y de
Airflow). Lo que se hace fuera, como parar el consumidor, no sale: en el vídeo
se ve su efecto.

Se usan **los dos generadores**: el script (`generar_datos_sinteticos.py`)
monta el **histórico** antes de grabar, y el **simulador** va encendido para
que se vea la ingesta en vivo. El simulador solo crea viajes de *ahora*.

### 9.1 Escaleta

Lo que queda en el vídeo final. Los tiempos son orientativos; para dejarlo en
**60 s**, quitad los planos marcados con *(cortable)*.

| Tiempo | Plano | Qué se ve | Voz / texto en pantalla |
|---|---|---|---|
| 0:00-0:08 | Frontend · **Inicio** | El recorrido entrada → caliente → frío → borrado, con las filas de cada tier | «Los viajes de taxi entran calientes en PostgreSQL y a los 30 días pasan al frío, en Iceberg» |
| 0:08-0:20 | **En vivo** | Ritmo, «llegando», la gráfica avanzando; clic en **Meter muestras** y el panel «Metido a mano» con 4 en caliente y 4 en cuarentena, cada uno con su motivo | «Llegan en tiempo real por Kafka. Lo que no cumple el contrato de datos va a cuarentena» |
| 0:20-0:35 | **En vivo**, ventana «1 min» | Las barras a cero y **parado**; después el pico y el retraso subiendo y bajando | «Si se cae el consumidor, Kafka guarda los mensajes. Al volver se pone al día sin perder ni duplicar» |
| 0:35-0:47 | **Viajes** · «Cruzando la frontera» *(cortable)* | Origen **mixto**, la barra de tramos y el histograma en dos colores | «Una consulta que cruza la frontera lee de los dos tiers y junta el resultado» |
| 0:47-0:55 | **Ciclo de vida** · «Demo: 5 minutos» | «N particiones pasan a estar pendientes» | «Bajamos la política a 5 minutos, sin tocar código» |
| 0:55-1:00 | **Airflow** · `archivar` → ▶ *(cortable)* | La ejecución arrancando | «Airflow ejecuta el archivado» |
| 1:00-1:15 | **Ciclo de vida** (acelerado) | La máquina de estados llevando las particiones a `DESALOJADO`; el historial con «¿Cuadran?» en **sí** | «Cada partición se copia, se cuenta en los dos lados y solo si cuadra se borra del caliente» |
| 1:15-1:25 | **Inicio** | El caliente ha bajado y el frío ha subido lo mismo | «Lo reciente, rápido; lo antiguo, barato. Y sin perder una fila» |
| 1:25-1:30 | **Ciclo de vida** · «Volver a 30 días» *(cortable)* | Cumplimiento **OK** | — |

Unas 2-2,5 palabras por segundo: si la voz no cabe, se recorta el texto, no se
acelera la imagen.

### 9.2 Antes de grabar

```bash
# 1. Base limpia con el histórico (pasos 1 y 3)
docker compose --profile "*" down -v
docker compose --profile core --profile orch --profile viz up -d
python scripts/generar_datos_sinteticos.py 1000000 --seed 42
python ingesta/subir_bronze.py --origen datos/sinteticos
python ingesta/carga_inicial.py --recrear

# 2. Simulador más lento: en .env, SIMULADOR_EVENTOS_POR_SEGUNDO=50
# 3. Flujo en vivo
docker compose --profile core --profile stream --profile orch --profile viz up -d
```

Y justo antes de grabar:

- [ ] La lista del [paso 5](#5-comprobar-que-todo-funciona) en verde y
  `pg "SELECT count(*) FROM taxi_trips_default;"` a `0` (si no, DAG
  `mantener_particiones` → ▶).
- [ ] **No grabéis cerca de las 02:00 hora española** (medianoche UTC): cambia
  el día y se mueven las particiones a mitad de toma.
- [ ] En **En vivo**, el simulador **encendido**. «Metido a mano» se llena en
  la toma con el botón **Meter muestras**.

- [ ] **Pausar `archivar`** en Airflow, para que no se adelante a la toma.
- [ ] Navegador: ventana a **1920×1080**, zoom **125 %** (que se lean los
  números en un vídeo pequeño), sin barra de marcadores y con dos pestañas:
  el frontend en **Inicio** y Airflow en el DAG `archivar`. Cerrad lo demás.
- [ ] Docker Desktop abierto en *Containers*, fuera de la ventana grabada.

### 9.3 La toma (unos 10 min, todo seguido)

No pasa nada por ir despacio o repetir un clic: en el montaje se corta. Dejad
**2-3 segundos quietos** en cada plano, que es lo que luego se usa.

1. **Inicio**: pasad el ratón por los tres tiers.
2. **En vivo**: dejadla correr unos segundos. Pulsad **Meter muestras** y
   bajad hasta «Metido a mano».
3. **En vivo** → ventana **«1 min»**. En Docker Desktop, **Stop** en
   `pids_consumidor`. Esperad a que salga **parado** (~30 s). **Start**.
   Esperad a que pase el pico y el retraso vuelva a ~0.
4. **Viajes** → «Cruzando la frontera». Pasad el ratón por la barra de tramos.
5. **Ciclo de vida** → «Demo: 5 minutos» → aceptar.
6. Pestaña de **Airflow** → `archivar`: quitar la pausa y **▶ Trigger**.
7. Vuelta al frontend → **Ciclo de vida**. Esperad sin tocar a que todo esté
   en `DESALOJADO` (unos minutos). Bajad a «Historial de archivado».
8. **Inicio**.
9. **Ciclo de vida** → «Volver a 30 días».

Si algo sale mal en los pasos 1-4, se repite ahí mismo. Si sale mal del 5 en
adelante, hay que recargar el histórico ([9.5](#95-después-de-grabar)).

### 9.4 Montaje en Recordly

[Recordly](https://github.com/webadderallorg/Recordly) es gratuito y de código
abierto: graba la pantalla y abre la grabación en un editor con línea de
tiempo, zoom automático y exportación a MP4. En WSL se usa la **versión de
Windows**: el navegador que graba es el de Windows, y `localhost` llega a los
contenedores de WSL igualmente.

**Instalar:** en [Releases](https://github.com/webadderallorg/Recordly/releases)
bajad `Recordly-windows-x64.exe` (pide Windows 10 build 19041 o posterior) e
instaladlo. Si Windows avisa de que es una aplicación desconocida: *Más
información* → *Ejecutar de todas formas*.

**Grabar:**
1. Abrir Recordly → elegir la **ventana** del navegador (no la pantalla
   entera: así no sale Docker Desktop ni las notificaciones).
2. Audio: **sin micrófono y sin audio del sistema**. La voz se graba después,
   sobre el montaje, porque la toma dura 10 minutos y el vídeo 90 s.
3. Grabar → hacer la toma de [9.3](#93-la-toma-unos-10-min-todo-seguido) →
   parar. Al parar se abre el editor solo.
4. Guardad el proyecto (`.recordly`) antes de editar: se puede reabrir con
   todo lo hecho.

**Editar** (siguiendo la [escaleta](#91-escaleta)):
- **Recortar** las esperas: lo que hay entre parar y arrancar el consumidor
  (se queda el «parado» y el pico), los clics de más, y los minutos de la
  máquina de estados.
- **Acelerar** (región de velocidad) lo que no se puede cortar porque se ve
  avanzar: la máquina de estados, a x8-x16 hasta que quepa en unos 10 s.
- **Zoom**: aceptad las sugerencias automáticas (siguen al ratón) y añadid a
  mano las de las tarjetas de **En vivo**, el panel «Metido a mano» y
  «¿Cuadran? sí».
- **Textos** (anotaciones de texto) con las frases de la escaleta, si no hay
  voz, o como apoyo si la hay. Recordly también genera subtítulos
  automáticos.
- **Cursor**: tamaño algo mayor y suavizado, para que se siga bien.
- **Fondo y marco**: un fondo liso, algo de margen y esquinas redondeadas.
  Formato **16:9**.
- **Voz**: grabadla aparte con la escaleta delante (cualquier grabadora del
  móvil o del PC vale) y añadidla como región de audio en la línea de tiempo.

**Exportar:** MP4, calidad alta, 1920×1080. Mirad la duración antes: entre
1:00 y 1:30.

### 9.5 Después de grabar

```bash
docker compose stop simulador                 # que el caliente no siga creciendo (o «Apagar» en En vivo)
```

- Devolved `SIMULADOR_EVENTOS_POR_SEGUNDO=200` en `.env` si queréis el valor
  por defecto.
- Comprobad en Airflow que `archivar` no se ha quedado en pausa.
- **Repetir la toma desde el paso 5** obliga a volver a cargar el histórico:
  `python ingesta/carga_inicial.py --recrear` (~1 min), la política a 30 días
  y `archivar` en pausa otra vez. Los pasos 1-4 se pueden repetir sin esto.

---

## Parar y retomar

```bash
docker compose --profile "*" stop             # para todo (chat incluido) y CONSERVA los datos
docker compose --profile core --profile stream --profile orch --profile viz up -d   # retomar
```

---

## Poner al día una base existente

Si no queréis hacer `down -v` después de un `git pull`, aplicad los SQL
nuevos. Son idempotentes y no borran nada:

```bash
docker compose exec -T postgres psql -U pids -d pids < postgres/init/05_p2.sql
docker compose exec -T postgres psql -U pids -d pids < postgres/init/07_p4.sql
docker compose exec -T postgres psql -U pids -d pids < postgres/init/08_p3.sql   # pestaña En vivo: índices e interruptor
docker compose restart simulador   # si estaba levantado: carga el interruptor (perfil stream)
docker restart pids_grafana     # para que cargue el datasource y el dashboard
```

Y para tener los datos de 2026, los pasos 3 y 4.

---

## Pendiente

Lo que falta para tener todo E8, en el orden en que se irá añadiendo a esta
guía:

- [x] **Guion del vídeo**: [paso 9](#9-guion-del-vídeo-60-90-s). Las consultas usan
  los rangos rápidos del frontend, así que valen el día que se grabe.
- [ ] **Mediciones (T5.1)**: las seis métricas de §7 con capturas, en
  `docs/MEDICIONES.md`.
- [ ] **Memoria (T5.2)** y **vídeo (T5.3)**.

**Chatbot (Parte 3)**, arreglos pendientes:

- [ ] **Timeout que supera al de la interfaz.** El presupuesto de 90 s solo se
  comprueba entre vueltas; con 30 s por llamada al modelo y un reintento
  (`max_retries=1`), una pregunta de varias vueltas pasa de los 150 s que
  espera Streamlit y sale «el backend no responde» aunque siga trabajando.
  Arreglo: sin reintentos y cada llamada con el tiempo que quede del
  presupuesto (`chatbot/agente.py`, `chatbot/config.py`).
- [ ] **Respuesta vacía con modelos que razonan.** Si el razonamiento se come
  los 1.500 tokens, `content` llega vacío y sale «No he podido generar una
  respuesta». Arreglo: limitar el razonamiento (`reasoning` de OpenRouter) o
  subir `CHATBOT_MAX_TOKENS`, y reintentar una vez si llega vacío.
- [ ] **Ajuste de privacidad de OpenRouter.** Si en
  https://openrouter.ai/settings/privacy no se permite a los proveedores
  gratuitos usar los prompts, los `:free` dan 404 y el chat dice «el modelo
  no existe». Arreglo: mensaje que lo explique y añadirlo al
  [paso 8](#8-chatbot-parte-3).
- [ ] **El modelo principal casi nunca responde.** `qwen3.8-27b:free` y
  `gemma-4-31b-it:free` devuelven 429 *«temporarily rate-limited upstream»*
  (saturación del proveedor, no de nuestra clave) y contesta siempre el
  último respaldo, `nemotron-3-super-120b-a12b:free`. Arreglo: poner nemotron
  primero y qwen y gemma de respaldo (`chatbot/config.py`, `.env.example`).
