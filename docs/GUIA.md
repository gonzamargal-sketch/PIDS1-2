# Guía de funcionamiento

Cómo levantar el proyecto completo, cargar los datos, comprobar que todo
funciona y ver el ciclo de vida de E8 en marcha. **Es la guía viva del
grupo**: se actualiza cada vez que entra una parte nueva o cambia cómo se hace
algo.

> **Última actualización:** 2026-10-05 · Pestaña **En vivo** del frontend
> (ingesta en directo, [5.8](#58-frontend-web-p3)) y el guion del vídeo hecho
> desde el navegador ([paso 9](#9-guion-del-vídeo)). Antes: chatbot de la Parte 3 (perfil
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

**Para verlo sin curl:** el frontend ([5.8](#58-frontend-web-p3)).

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
| **En vivo** | `/ingesta` | Abrirla con el perfil `stream` levantado; meter un viaje con `anadir_viaje.py` (7.1, 7.2); parar y arrancar el consumidor | Ritmo en torno a 200/s, último viaje llegado hace < 2 s y retraso **al día**. El viaje metido, en «Metido a mano» con su destino. Con el consumidor parado: barras a cero y **parado**; al arrancarlo, un pico de llegadas y el retraso sube y vuelve a ~0 |
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

## 9. Guion del vídeo

El vídeo se graba **en el navegador**: frontend, Grafana, Airflow y MinIO. El
terminal solo aparece en la escena 3, para mandar dos mensajes a Kafka.

Se usan **los dos generadores**, cada uno para una cosa: el script
(`generar_datos_sinteticos.py`) monta el **histórico** antes de grabar (frío,
caliente y la frontera de 30 días), y el **simulador** va encendido durante la
grabación para que se vea la ingesta en vivo. El simulador no sustituye al
script: solo crea viajes de *ahora*, nunca del pasado.

| # | Escena | Dónde | Qué demuestra | Duración |
|---|---|---|---|---|
| 1 | El sistema y la política | Frontend · Inicio, Grafana | El recorrido de un dato y la política como dato | ~1 min |
| 2 | Ingesta en tiempo real | Frontend · En vivo | Los viajes llegan en segundos | ~1 min |
| 3 | Calidad de datos | Terminal + En vivo, Métricas | El contrato separa lo bueno de lo sucio | ~1,5 min |
| 4 | Resiliencia | Docker Desktop + En vivo | Se cae el consumidor y no se pierde nada | ~1,5 min |
| 5 | Router de tiers | Frontend · Viajes, Métricas | Una consulta lee del tier que toca, o de los dos | ~2 min |
| 6 | Chatbot *(opcional)* | Chat | Preguntas en lenguaje natural sobre los dos tiers | ~1,5 min |
| 7 | Migración caliente → frío | Airflow, Ciclo de vida, Grafana, MinIO | El ciclo de vida en marcha, sin perder filas | ~3-4 min |
| 8 | Cierre | Ciclo de vida | Volver a la política real | ~0,5 min |

**La escena 7 no se puede deshacer**: por eso va al final. Si una toma de las
escenas 1-6 sale mal, se repite sin más.

### 9.1 Antes de grabar (no sale en el vídeo)

```bash
# 1. Base limpia con el histórico (pasos 1 y 3)
docker compose --profile "*" down -v
docker compose --profile core --profile orch --profile viz up -d
python scripts/generar_datos_sinteticos.py 1000000 --seed 42
python ingesta/subir_bronze.py --origen datos/sinteticos
python ingesta/carga_inicial.py --recrear

# 2. Simulador más lento para grabar: en .env
#    SIMULADOR_EVENTOS_POR_SEGUNDO=50
#    (se ve entrar datos y el caliente no se dispara si hay que repetir tomas)

# 3. Todo levantado, chat incluido si sale en el vídeo
docker compose --profile core --profile stream --profile orch --profile viz --profile chat up -d
```

Lista antes de darle a grabar:

- [ ] La lista del [paso 5](#5-comprobar-que-todo-funciona) entera, en verde.
- [ ] `pg "SELECT count(*) FROM taxi_trips_default;"` da `0`. Si no, los
  viajes en vivo no tienen partición y el router no los ve: DAG
  `mantener_particiones` → ▶.
- [ ] **Hora**: todo va en UTC (en octubre, 2 horas menos que en España). No
  grabéis cerca de las **02:00 hora española**, que es la medianoche UTC: cambia
  el día y se mueven las particiones y la frontera a mitad de vídeo.
- [ ] Unas cuantas consultas en la pestaña Viajes de los tres tipos, para que
  las latencias (Métricas y fila 3 de Grafana) tengan datos.
- [ ] **En vivo** con ritmo en torno a 50/s y **llegando**.
- [ ] Abierto: el frontend en **En vivo** (`localhost:8000/app/#/envivo`),
  Grafana (dashboard «E8 · Ciclo de vida de los datos»), Airflow, MinIO
  (bucket `lakehouse`), el chat si sale, Docker Desktop en *Containers* y un
  terminal con `source .venv/bin/activate` y los comandos de la escena 3 ya
  escritos.
- [ ] Si la base es de antes de la pestaña **En vivo**, aplicad
  `postgres/init/08_p3.sql` (ver
  [Poner al día una base existente](#poner-al-día-una-base-existente)). Con el
  `down -v` del punto 1 ya entra solo.

### 9.2 Durante la grabación

Cada escena dice qué hacer, qué tiene que verse y la idea que hay que contar.

#### Escena 1 · El sistema y la política

| Hacer | Tiene que verse |
|---|---|
| Frontend → **Inicio** | El recorrido entrada → caliente → frío → borrado, con las filas de cada tier, el cumplimiento en **OK** y el frío ocupando varias veces menos por fila |
| Grafana → fila 0 | Umbral de 30 días, custodia de 7 años, cumplimiento **OK** y 0 pendientes |

> **Contar:** cada viaje entra al caliente (PostgreSQL, rápido y caro), pasa al
> frío (Iceberg en MinIO, lento y barato) a los 30 días y se borra a los 7
> años. La política es una fila de una tabla, no código.

#### Escena 2 · Ingesta en tiempo real

| Hacer | Tiene que verse |
|---|---|
| Frontend → **En vivo** | «Ritmo ahora» en torno a 50/s, «Último viaje llegó hace» de 1-2 s y **llegando**, retraso **al día** |
| Mirar unos segundos | La gráfica «Llegadas» avanzando y «Últimos viajes llegados» cambiando: hora de llegada y de evento casi iguales |
| Frontend → **Viajes** → «Última hora» | Origen `hot`, con viajes de hace segundos |

> **Contar:** el simulador emite viajes que acaban de terminar, Kafka los
> recibe y el consumidor los valida y los escribe en el caliente. Del evento a
> la base pasan uno o dos segundos.

#### Escena 3 · Calidad de datos

Con **En vivo** a la vista, en el terminal:

```bash
python scripts/anadir_viaje.py --json --distancia 7 --importe 31 | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic trips.raw
echo 'esto no es json' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic trips.raw
python scripts/anadir_viaje.py --distancia 600
```

| Hacer | Tiene que verse |
|---|---|
| Volver a **En vivo** → panel «Metido a mano» | El viaje de 7 millas en **caliente**; el mensaje roto en **cuarentena** con `mensaje_ilegible` y el texto tal cual; el de 600 millas en **cuarentena** con `distancia_excesiva` |
| Frontend → **Métricas** → «Calidad de los datos» (o Grafana, fila 4) | Los motivos de rechazo, con lo vuestro junto a la suciedad que mete el simulador |

> **Contar:** todo lo que entra, por Kafka o directo, pasa por el mismo
> contrato de datos. Lo que no lo cumple va a cuarentena con el motivo y el
> mensaje original: nunca llega al caliente y no se pierde.

#### Escena 4 · Resiliencia

En **En vivo**, ventana **«1 min»**.

| Hacer | Tiene que verse |
|---|---|
| Docker Desktop → `pids_consumidor` → **Stop** (o `docker compose stop consumidor`) | — |
| Esperar ~30 s mirando **En vivo** | Las barras a cero; «Último viaje llegó hace» subiendo hasta **parado** |
| Docker Desktop → `pids_consumidor` → **Start** (o `docker compose start consumidor`) | Un **pico** de llegadas muy por encima del ritmo normal y la línea de «Retraso» subiendo a ~30 s y bajando a ~0 en segundos. Luego, el ritmo de siempre |

> **Contar:** mientras el consumidor está caído, el simulador sigue emitiendo y
> Kafka guarda los mensajes. Al volver se pone al día: el pico es justo lo que
> faltaba en el hueco. Como el offset se confirma después de escribir en
> PostgreSQL y la escritura ignora duplicados, ni se pierde ni se repite nada.
> El retraso es el LAG de Kafka medido en segundos.

#### Escena 5 · El router de tiers

| Hacer | Tiene que verse |
|---|---|
| Frontend → **Viajes** → «Últimos 7 días» | Origen **caliente** |
| Un rango de agosto a mano (p. ej. del 1 al 3) | Origen **frío** |
| «Cruzando la frontera» | Origen **mixto**: la barra de tramos con frío y caliente tocándose a las 00:00, el histograma en dos colores y cada viaje con su tier |
| Frontend → **Métricas** → «Latencia por tier» | El frío más lento que el caliente, los dos dentro de su objetivo (500 ms y 10 s) |

> **Contar:** quien consulta no elige tier. La API mira qué días siguen en
> PostgreSQL, lee cada tramo de donde está y junta el resultado. Lo reciente
> sale rápido; lo antiguo, más lento pero mucho más barato.

#### Escena 6 · Chatbot *(opcional)*

| Hacer (en `localhost:8501`) | Tiene que verse |
|---|---|
| «¿Cuánto se facturó en JFK en agosto?» | Viajes e ingresos desde el **frío**. En «Cómo lo he resuelto»: `buscar_zona` → `consultar_ingresos(zona=132)` |
| «¿Qué zonas generaron más ingresos hoy?» | Ranking desde el **caliente**. Repetida, la cifra cambia: siguen entrando viajes |
| «¿Cuánto se facturó en noviembre?» | Que no hay datos del futuro |

> **Contar:** el modelo no toca la base: llama a la misma API con herramientas
> de solo lectura validadas, y cada respuesta dice de qué tier sale.

#### Escena 7 · Migración caliente → frío

| Hacer | Tiene que verse |
|---|---|
| Airflow → *Dags* → `archivar` → interruptor en **pausa** | — |
| Frontend → **Ciclo de vida** → **«Demo: 5 minutos»** (pide confirmación) | «N particiones pasan a estar pendientes» y la tabla «Pendientes de archivar» llena |
| Grafana → fila 0 | Cumplimiento en **INCUMPLE** y particiones pendientes > 0 |
| Frontend → **Viajes** → «Cruzando la frontera» | **Igual que antes**: la política ha cambiado, pero los datos siguen en PostgreSQL y el router los lee de ahí |
| Airflow → `archivar` → quitar la pausa y **▶ Trigger** | Una ejecución en marcha |
| Frontend → **Ciclo de vida** (se refresca cada 5 s) | La máquina de estados mueve las particiones por `ESCRIBIENDO → ESCRITO → VERIFICADO → DESALOJADO`, de la más antigua a la más reciente (unos minutos) |
| Al terminar: «Historial de archivado» | Todo en `DESALOJADO` y «¿Cuadran?» en **sí** en cada partición |
| MinIO → `lakehouse` → `lakehouse/trips/data/` | Los Parquet nuevos del mes actual |
| Frontend → **Inicio** | El caliente ha bajado y el frío ha subido lo mismo; en **En vivo** sigue llegando todo, porque lo de hoy no se archiva hasta mañana |
| Grafana → filas 0 y 1 | Cumplimiento de vuelta en **OK**; el área del caliente baja y la del frío sube |
| Frontend → **Viajes** → «Cruzando la frontera» | Ahora **frío**; un rango de ayer a hoy sale **mixto**, con la frontera a las 00:00 de hoy |

> **Contar:** bajar la política no mueve nada por sí solo: el sistema detecta
> que la incumple y el DAG lo corrige. Cada partición se copia a Iceberg, se
> cuenta en los dos lados y solo si cuadra se borra de PostgreSQL. Las
> consultas no se enteran: un día se lee del frío en cuanto ya solo está allí.

#### Escena 8 · Cierre

| Hacer | Tiene que verse |
|---|---|
| Frontend → **Ciclo de vida** → **«Volver a 30 días»** | 0 particiones pendientes y cumplimiento **OK** |

> **Contar:** los datos movidos se quedan en el frío; el ciclo sigue solo,
> cada 5 minutos, con la política real.

### 9.3 Después de grabar

```bash
docker compose stop simulador                 # que el caliente no siga creciendo
```

- Devolved `SIMULADOR_EVENTOS_POR_SEGUNDO=200` en `.env` si queréis el valor
  por defecto.
- Comprobad en Airflow que `archivar` no se ha quedado en pausa.
- **Repetir la escena 7** (o el vídeo entero) obliga a volver a cargar el
  histórico: `python ingesta/carga_inicial.py --recrear` (~1 min) y otra vez
  la política a 30 días. Las escenas 1-6 se pueden repetir sin esto.

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
docker compose exec -T postgres psql -U pids -d pids < postgres/init/08_p3.sql   # índice de la pestaña En vivo
docker restart pids_grafana     # para que cargue el datasource y el dashboard
```

Y para tener los datos de 2026, los pasos 3 y 4.

---

## Pendiente

Lo que falta para tener todo E8, en el orden en que se irá añadiendo a esta
guía:

- [x] **Guion del vídeo**: [paso 9](#9-guion-del-vídeo). Las consultas usan
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
