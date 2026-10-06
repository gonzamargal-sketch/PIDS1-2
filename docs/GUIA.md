# Guía de funcionamiento

Cómo levantar el proyecto, cargar los datos, comprobar que todo funciona y ver
en marcha el ciclo de vida de E8.

**Datos de trabajo:** de 2026, del 1 de enero hasta ahora, nunca en el futuro.
La muestra real (`datos/muestra_1000.csv`) es la semilla, el volumen lo genera
`scripts/generar_datos_sinteticos.py` y el flujo en vivo, el simulador.

---

## 1. Preparación

Requisitos: Docker con Compose v2 y Python 3.11 o superior.

```bash
cp .env.example .env
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
```

## 2. Servicios

```bash
docker compose --profile "*" down -v          # parte de cero: BORRA los datos anteriores
docker compose --profile core --profile orch --profile viz up -d
docker compose ps                             # esperar a que todo esté "healthy" (~1 min)
```

| Perfil | Servicios |
|---|---|
| `core` | PostgreSQL, MinIO, API |
| `stream` | Kafka, consumidor, simulador |
| `orch` | Airflow |
| `viz` | Grafana |
| `chat` | Chatbot de la Parte 3 |

`minio-init` termina en `Exited (0)`: es normal, crea los buckets y acaba. Los
scripts de `postgres/init/` solo se ejecutan con el volumen vacío; por eso se
parte de `down -v`.

## 3. Pruebas

Antes de cargar datos, porque las dos vacían el tier caliente:

```bash
python scripts/prueba_humo.py                 # → TODO CORRECTO
python scripts/prueba_archivado.py            # → TODO CORRECTO
```

| Prueba | Qué comprueba |
|---|---|
| `prueba_humo.py` | Contrato de datos, esquema, inserción, vistas de métricas y que no se puede desalojar una partición sin verificarla |
| `prueba_archivado.py` | Ciclo de vida completo con el job real: política a 5 min, caídas a mitad y relanzado sin duplicar, conteos que cuadran, idempotencia y que si el conteo no cuadra no se borra nada |

## 4. Carga de datos (1M de filas)

```bash
python scripts/generar_datos_sinteticos.py 1000000 --seed 42    # ~25 s → datos/sinteticos/
python ingesta/subir_bronze.py --origen datos/sinteticos        # copia cruda a MinIO (bronze)
python ingesta/carga_inicial.py --recrear                       # ~1 min
```

- El generador reparte los viajes **del 2026-01-01 hasta ahora**, con el perfil
  horario de los taxis de Nueva York y las anomalías reales de la muestra.
- La carga inicial pasa todo por el contrato de datos (los rechazos van a
  `trips_cuarentena`) y **reparte por edad**: lo anterior a hoy menos 30 días
  va a **Iceberg** y lo demás a **PostgreSQL**, sin solape.
- `--recrear` borra la tabla Iceberg y la carga anterior del caliente, así que
  se puede repetir sin duplicar.

## 5. Flujo en vivo

```bash
docker compose --profile core --profile stream --profile orch --profile viz up -d
```

Añade Kafka, el consumidor y el simulador, que emite **200 viajes por segundo**.
El consumidor los valida y los escribe en el caliente o en cuarentena. Cada
viaje «acaba de terminar»: su `event_time` es el momento actual.

El simulador se apaga y se enciende desde la pestaña **En vivo** del frontend
o con `docker compose stop simulador`.

---

## 6. Comprobar que todo funciona

| Servicio | URL | Credenciales |
|---|---|---|
| Frontend web | http://localhost:8000/app/ | — |
| API (Swagger) | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | sin login |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| MinIO | http://localhost:9001 | `minioadmin` / `minioadmin_dev_2026` |
| Chatbot | http://localhost:8501 | — |

Alias para las consultas a PostgreSQL:

```bash
alias pg='docker compose exec -T postgres psql -U pids -d pids -c'
```

### 6.1 Servicios y almacenamiento

| Ejecutar | Resultado esperado |
|---|---|
| `docker compose ps -a` | Todo `Up (healthy)`, salvo `minio-init` en `Exited (0)` |
| `curl -s localhost:8000/health` | `{"estado":"ok"}` |
| `curl -s localhost:8000/stats` | `hot.filas` con los últimos 30 días y `cold.filas` con el resto |
| `pg "SELECT min(event_time), max(event_time) FROM taxi_trips;"` | Mínimo de hace ~30 días y máximo de hoy, nunca en el futuro |
| `pg "SELECT * FROM v_coste_por_tier;"` | PostgreSQL ocupa varias veces más por fila que Iceberg (~320-550 B frente a ~55 B) |
| `pg "SELECT * FROM v_cumplimiento_politica;"` | `estado = OK` |
| `pg "SELECT count(*) FROM taxi_trips_default;"` | `0` (si no, faltan particiones: `pg "SELECT crear_particiones_adelanto(7);"`) |
| MinIO → bucket `lakehouse` | `lakehouse/trips/data/` (Parquet por mes) y `metadata/` |

### 6.2 Ingesta en vivo

| Ejecutar | Resultado esperado |
|---|---|
| `docker compose logs --tail 3 simulador` | `N emitidos (200 ev/s de media)`, con N subiendo |
| `docker compose logs --tail 3 consumidor` | `N recibidos → X en caliente, Y en cuarentena, 0 duplicados` |
| Frontend → **En vivo** | Ritmo de ~200/s y retraso ~0 |
| `pg "SELECT * FROM v_calidad;"` | Motivos de rechazo con registros: la suciedad del simulador acaba en cuarentena |
| `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group pids-consumidor` | `LAG` cerca de 0 |

**Resiliencia (no se pierde ni se duplica nada):**

```bash
docker compose stop consumidor        # el simulador sigue emitiendo a Kafka
# ~30 s después, el LAG ha crecido
docker compose start consumidor       # se pone al día
docker compose logs --tail 3 consumidor
```

El `LAG` vuelve a ~0 y el consumidor sigue con `0 duplicados`: Kafka guardó
los mensajes mientras estaba parado.

### 6.3 Airflow

| Comprobación | Resultado esperado |
|---|---|
| Airflow → *Dags* | Los 4 DAGs activos y sin errores de importación |
| DAG `archivar` | Una ejecución cada 5 min, en verde; al terminar dispara `estadisticas_frio` |
| DAG `mantener_particiones` → ▶ | En verde; hay particiones creadas hasta 7 días por delante |
| DAG `purga_final` | En verde (diario); con la política de 7 años no borra nada |

### 6.4 Grafana

Dashboard «E8 · Ciclo de vida de los datos» (se refresca cada 30 s):

| Fila | Resultado esperado |
|---|---|
| **0 · La política** | Umbral de 30 días, retención en frío de 7 años, cumplimiento **OK** y 0 particiones pendientes |
| **1 · Migración** | Filas por tier en el tiempo y estado de cada partición en la máquina de estados |
| **2 · Coste** | Bytes por fila (PostgreSQL varias veces por encima), ratio de compresión y tamaño del frío por mes |
| **3 · Latencia** | p95 caliente < 500 ms y p95 frío < 10 s (aparece tras hacer consultas a `/trips`) |
| **4 · Calidad** | Registros en cuarentena y sus motivos |

### 6.5 API

Las respuestas llevan `data` y `meta` (`data_source`, `coverage`, `as_of`,
`latency_ms`, `rows`), y cada llamada queda en `query_log`. `/trips` filtra por
`event_time` en `[desde, hasta)`; las fechas sin zona horaria se interpretan
en UTC. Las fechas de los ejemplos hay que ajustarlas al día en curso: el
caliente son los últimos 30 días.

```bash
curl -s localhost:8000/lifecycle/policy       # política vigente
curl -s localhost:8000/lifecycle/status       # archival_jobs, candidatas y cumplimiento
curl -s localhost:8000/metrics/coste          # también: latencia, calidad, caliente

curl -s "localhost:8000/trips?desde=2026-09-20&hasta=2026-09-22"   # solo caliente → hot
curl -s "localhost:8000/trips?desde=2026-08-01&hasta=2026-08-03"   # solo frío     → cold
curl -s "localhost:8000/trips?desde=2026-08-28&hasta=2026-09-10"   # cruza la frontera → mixto
```

| Caso | Resultado esperado |
|---|---|
| Rango que cruza la frontera | `data_source = mixto` y `coverage` con un tramo `cold` y otro `hot` que se tocan a las 00:00 del primer día del caliente |
| `desde` posterior a `hasta` | `400` |
| `/metrics/noexiste` | `404` con la lista de métricas disponibles |
| `PUT /lifecycle/policy` con `{"umbral_valor":0,"umbral_unidad":"weeks"}` | `422`: la política se valida antes de tocar la base |

**El router no pierde filas.** Una consulta que cruza la frontera devuelve
exactamente las filas del caliente más las del frío en ese rango (con el
simulador parado, para que el caliente no cambie entre consultas):

```bash
curl -s "localhost:8000/trips?desde=2026-08-10&hasta=2026-09-29&limite=100000" \
  | python -c "import json,sys; m=json.load(sys.stdin)['meta']; print(m['data_source'], m['rows'])"
pg "SELECT count(*) FROM taxi_trips WHERE event_time >= '2026-08-10' AND event_time < '2026-09-29';"
python -c "from datetime import datetime as D, timezone as Z; from common.lakehouse import obtener_tabla, contar_particion; print(contar_particion(obtener_tabla(crear=False), D(2026,8,10,tzinfo=Z.utc), D(2026,9,29,tzinfo=Z.utc)))"
```

### 6.6 Frontend web

http://localhost:8000/app/ es HTML + JS servido por la propia API, sin lógica
propia: solo usa los endpoints anteriores.

| Pestaña | Qué muestra |
|---|---|
| **En vivo** | Ritmo de ingesta, retraso y cuarentena en tiempo real; botones para apagar o encender el simulador y meter viajes de muestra |
| **Inicio** | Recorrido entrada → caliente → frío → borrado con las filas de cada tier |
| **Viajes** | Consulta por rango, con los tramos de cada tier y la etiqueta de tier en cada viaje |
| **Ciclo de vida** | Política editable, botón «Demo: 5 minutos» y máquina de estados refrescándose cada 5 s |
| **Métricas** | Bytes por fila, latencia p95 frente a su SLA y motivos de cuarentena |
| **Explorador** | Cada endpoint con su `curl`, su respuesta JSON y ejemplos de errores |

---

## 7. Demo: migración caliente → frío

```bash
# 1. Bajar la política desde la API, sin redesplegar. La respuesta dice
#    cuántas particiones pasan a ser candidatas
curl -s -X PUT localhost:8000/lifecycle/policy \
  -H 'content-type: application/json' -d '{"umbral_valor":5,"umbral_unidad":"minutes"}'

# 2. En la siguiente pasada de "archivar" (cada 5 min, o ▶ en Airflow)
#    los días anteriores a hoy pasan al frío
docker compose exec airflow airflow dags trigger archivar
pg "SELECT estado, count(*), sum(filas_origen) origen, sum(filas_escritas) escritas FROM archival_jobs GROUP BY 1;"

# 3. Al acabar, volver a la política real
curl -s -X PUT localhost:8000/lifecycle/policy \
  -H 'content-type: application/json' -d '{"umbral_valor":30,"umbral_unidad":"days"}'
```

| Momento | Dónde | Resultado esperado |
|---|---|---|
| Tras el `PUT` | Respuesta | `particiones_candidatas_ahora` > 0 |
| | Grafana, fila 0 | Umbral de 5 min, cumplimiento **INCUMPLE** y particiones pendientes |
| | `/trips` | Igual que antes: aún no se ha movido nada y el router sigue leyendo esos días de PostgreSQL |
| Durante `archivar` | `/lifecycle/status` o Grafana | Particiones pasando por `ESCRIBIENDO → ESCRITO → VERIFICADO → DESALOJADO` |
| Después | `archival_jobs` | Todo en `DESALOJADO` y `origen = escritas`: no se ha perdido ninguna fila |
| | Grafana, filas 0 y 1 | Cumplimiento **OK**; el caliente baja y el frío sube en la misma cantidad |
| | `/trips` | El rango anterior sale ahora `cold`; la frontera queda a las 00:00 de hoy |

- Se mueven **días completos**: lo de hoy no se archiva hasta mañana.
- **La demo no se deshace**: volver a 30 días no devuelve los datos al
  caliente. Para repetirla desde cero, se vuelve a ejecutar
  `carga_inicial.py --recrear` (paso 4).
- Para enseñarlo paso a paso, se puede pausar el DAG
  (`airflow dags pause archivar`), bajar la política, comprobar que `/trips`
  sigue completo aunque el cumplimiento salga **INCUMPLE**, y reanudarlo con
  `unpause`.

### Purga del frío

La política de borrado es de 7 años, así que con datos de 2026 no borra nada.
Para ver qué borraría con otra política, sin borrar nada:

```bash
curl -s -X PUT "localhost:8000/lifecycle/policy?accion=DELETE" \
  -H 'content-type: application/json' -d '{"umbral_valor":180,"umbral_unidad":"days"}'
docker compose exec airflow /opt/pids-venv/bin/python -m archivado.purga --simulacro
curl -s -X PUT "localhost:8000/lifecycle/policy?accion=DELETE" \
  -H 'content-type: application/json' -d '{"umbral_valor":7,"umbral_unidad":"years"}'
```

La purga borra **meses completos** del frío y expira los snapshots de Iceberg.
Se ejecuta de verdad con el DAG `purga_final`.

---

## 8. Interactuar con el sistema

### 8.1 Añadir un viaje a mano

Desde el frontend: pestaña **En vivo** → **Meter muestras** (8 viajes, uno por
cada resultado posible del contrato de datos). Desde la terminal:

```bash
python scripts/anadir_viaje.py                                    # un viaje normal
python scripts/anadir_viaje.py --distancia 12 --minutos 35 --importe 48.5 --propina 9
```

El viaje pasa por el mismo contrato de datos que el flujo en vivo, y el script
dice dónde ha acabado:

| Opción | Resultado |
|---|---|
| *(ninguna)* | Caliente |
| `--importe -20` | Caliente con aviso `importe_negativo` |
| `--distancia 30 --minutos 5` | Caliente con aviso `velocidad_imposible` |
| `--pasajeros 0` | Caliente con aviso `sin_pasajeros` |
| `--distancia 600` | Cuarentena: `distancia_excesiva` |
| `--minutos 0` | Cuarentena: `cronologia_invalida` |
| `--importe 20000` | Cuarentena: `importe_excesivo` |
| `--zona-origen 999` | Cuarentena: `zona_fuera_de_rango` |

Con `--json` el script solo imprime el mensaje, que se puede mandar por Kafka:

```bash
python scripts/anadir_viaje.py --json --distancia 7 --importe 31 | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic trips.raw
```

### 8.2 Meter muchos viajes de golpe

```bash
docker compose run --rm --no-deps simulador \
  python -m simulador.simulador --sumidero postgres --total 5000 --eps 0 --pct-suciedad 20
```

Escribe directamente en PostgreSQL, sin Kafka. Sin `--sumidero postgres` van
por Kafka (requiere el perfil `stream`).

### 8.3 Mover datos a mano

```bash
docker compose exec airflow /opt/pids-venv/bin/python -m archivado.job_archivado --simulacro
docker compose exec airflow /opt/pids-venv/bin/python -m archivado.job_archivado --dia 2026-08-30
```

### 8.4 Provocar fallos

| Fallo | Cómo | Qué ocurre |
|---|---|---|
| Se cae el consumidor | `docker compose stop consumidor` y luego `start` | Kafka guarda los mensajes; al volver se pone al día sin perder ni duplicar |
| Se cae el archivado a mitad | `python scripts/prueba_archivado.py` (vacía el caliente) | Al relanzarlo termina sin duplicar y sin borrar nada sin verificar |
| Se cae la API | `docker compose restart api` | Vuelve en ~15 s; no guarda estado, así que no se pierde nada |

---

## 9. Chatbot (Parte 3)

```bash
docker compose --profile core --profile chat up -d --build
```

Se abre en http://localhost:8501 (o «Asistente ↗» en el frontend).

```
Streamlit (chat-ui, :8501) → FastAPI (chatbot, :8001) → OpenRouter → modelo
                                   └→ herramientas → API de la Parte 2
```

- **Sin clave** funciona en **modo simulado**: un analizador por reglas hace
  de modelo y propone las mismas llamadas a herramientas.
- **Con modelo real**: se pone una clave gratuita de OpenRouter en `.env`
  (`OPENROUTER_API_KEY=...`) y se recrea el servicio con
  `docker compose --profile core --profile chat up -d --force-recreate chatbot`.
  Por defecto usa modelos gratuitos (`:free`) con respaldo automático. La clave
  solo llega al contenedor `chatbot`.

**Herramientas** (todas de solo lectura):

| Herramienta | Qué hace | Llama a |
|---|---|---|
| `consultar_ingresos(inicio, fin, zona?)` | Viajes, ingresos, propinas y medias de un periodo | `/trips/resumen` |
| `desglose_viajes(inicio, fin, por=dia\|zona)` | Evolución por día o ranking de zonas | `/trips/resumen?agrupar=` |
| `buscar_zona(texto)` | Traduce nombres a zonas («JFK» → 132) con la tabla oficial de la TLC | — |
| `ver_viajes(inicio, fin, limite≤20)` | Viajes de ejemplo | `/trips` |
| `estado_ciclo_vida()` | Política, cumplimiento y particiones archivadas | `/lifecycle/*` |
| `metricas(nombre)` | Coste, latencia, calidad o estado del caliente | `/metrics/*` |

Cada llamada se valida antes de tocar la API (solo 2026, nada del futuro,
`inicio < fin`, zonas 1-265, como mucho 20 viajes) y devuelve el valor, el
periodo y el origen (tier y `as_of`). Cada respuesta muestra en «Cómo lo he
resuelto» las herramientas usadas, sus argumentos y qué modelo contestó.

| Pregunta | Resultado esperado |
|---|---|
| «¿Cuánto se facturó en JFK en agosto?» | Viajes e ingresos de JFK en agosto, origen frío (`buscar_zona` → `consultar_ingresos`) |
| «¿Qué zonas generaron más ingresos la última semana?» | Ranking de zonas, origen caliente |
| «¿Cuánto se facturó en noviembre?» | Indica que no hay datos del futuro |
| «Baja la política a 5 minutos» | Indica que es de solo lectura |

**Limitaciones conocidas:**

- Los modelos gratuitos de OpenRouter suelen estar limitados (429) y a menudo
  responde el último modelo de respaldo.
- Si en la configuración de privacidad de OpenRouter no se permite a los
  proveedores gratuitos usar los prompts, los modelos `:free` devuelven 404.
- Una pregunta con varias llamadas lentas puede superar el tiempo de espera de
  la interfaz aunque el backend siga trabajando.

---

## Parar y retomar

```bash
docker compose --profile "*" stop             # para todo y conserva los datos
docker compose --profile core --profile stream --profile orch --profile viz up -d
```
