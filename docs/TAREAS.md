# Tareas pendientes

Los pasos 1 a 5 están cerrados: esqueleto, Compose, esquema de PostgreSQL,
contrato de datos, bronze en MinIO y tabla Iceberg cargada. Lo que queda son
los pasos 6 a 12, troceados en **17 tareas**.

Antes de coger una, leed las secciones **3.1, 3.2 y 3.3** de
[`ARQUITECTURA.md`](ARQUITECTURA.md). Son las tres ideas que condicionan lo que
escribe cada uno.

---

## Cómo está troceado esto

**Regla de oro: cada fichero tiene un único dueño.** Las tareas no se han
partido por «temas» sino por **ficheros**, de forma que dos personas que
trabajan a la vez nunca editan el mismo. Si dos tareas necesitaran tocar el
mismo fichero, o se han fusionado en una, o se ha movido el punto de contacto a
un fichero nuevo.

Cada tarea declara:

- **Dueño** — el bloque de [`REPARTO.md`](REPARTO.md) al que pertenece.
- **Depende de** — qué tiene que estar mergeado antes. Casi todas ponen «nada».
- **Ficheros propios** — los crea y los edita solo esta tarea.
- **Solo lee** — los usa pero no los modifica.
- **Hecho cuando** — el criterio de aceptación. Sin esto no está cerrada.

Dentro de un mismo bloque las tareas van en orden, porque las hace la misma
persona. **Entre bloques distintos todo es paralelo**, con las dos únicas
excepciones que marca la tabla resumen.

Una rama por bloque: `p1/almacenamiento`, `p2/ingesta`, `p3/api`,
`p4/orquestacion`. Antes de cada commit, `python scripts/prueba_humo.py` tiene
que decir `TODO CORRECTO`.

---

## Paso 0 · Andamiaje (se hace primero y a solas)

Hay una única tarea que **bloquea la paralelización de verdad**, y es la de los
contenedores: `docker-compose.yml` es el fichero que, si no se hace esto, los
cuatro bloques tendrían que editar a la vez para declarar su servicio. Se hace
una vez, al principio, y después ya nadie más lo toca.

### A1 · Declarar en Compose los cinco servicios que faltan

| | |
|---|---|
| **Dueño** | P4 |
| **Depende de** | nada |
| **Dura** | una tarde |

**Ficheros propios:** `docker-compose.yml`, `api/Dockerfile`,
`api/requirements.txt`, `api/app.py`, `simulador/Dockerfile`,
`simulador/requirements.txt`, `ingesta/Dockerfile`, `ingesta/requirements.txt`,
`airflow/Dockerfile`, `airflow/requirements.txt`, `.env.example`.

Declara en Compose los servicios `api` (perfil `core`), `simulador` y
`consumidor` (`stream`), `airflow` (`orch`) y `grafana` (`viz`), con las RAM
que fija §4 de la arquitectura, su `env_file: .env`, sus `depends_on` y su
`build:` apuntando a la carpeta del bloque. Cada carpeta se lleva un
`Dockerfile` y un `requirements.txt` propios: **las dependencias de cada
bloque no van a `requirements-dev.txt`**, que es solo para el entorno local.

`api/app.py` se crea aquí con **únicamente** `/health`, para que el perfil
`core` siga levantando verde desde el primer día. A partir del commit de A1 ese
fichero es de P3.

**Hecho cuando:** `docker compose --profile "*" build` termina sin errores y
`docker compose --profile core up -d` deja `postgres`, `minio` y `api` en
`healthy`, con `/health` respondiendo. A partir de aquí **nadie más edita
`docker-compose.yml`**: quien necesite un cambio, se lo pide a P4.

---

## Tabla resumen

| ID | Tarea | Dueño | Paso | Depende de |
|---|---|---|---|---|
| **A1** | Declarar los servicios en Compose | P4 | — | nada |
| **T1.1** | Job de archivado con máquina de estados | P1 | 8 | nada |
| **T1.2** | Purga del tier frío | P1 | 8 | nada |
| **T1.3** | Prueba end-to-end del ciclo de vida | P1 | 8 | T1.1 |
| **T2.1** | Simulador con jitter → PostgreSQL | P2 | 6 | nada |
| **T2.2** | Consumidor de Kafka | P2 | 7 | nada |
| **T2.3** | Sumidero Kafka del simulador | P2 | 7 | T2.1 |
| **T3.1** | Esqueleto de la API e instrumentación | P3 | 10 | A1 |
| **T3.2** | Router de tiers y `/trips` | P3 | 10 | T3.1 |
| **T3.3** | Endpoints de ciclo de vida y métricas | P3 | 10 | T3.1 |
| **T4.1** | Airflow + DAG `mantener_particiones` | P4 | 9 | A1 |
| **T4.2** | DAG `estadisticas_frio` | P4 | 9 | A1 |
| **T4.3** | DAGs `archivar` y `purga_final` | P4 | 9 | T1.1, T1.2 |
| **T4.4** | Grafana: datasource y dashboards | P4 | 11 | A1 |
| **T5.1** | Mediciones de §7 | P2 | 12 | T2.1, T4.3 |
| **T5.2** | Memoria (un fichero por bloque) | todos | 12 | su bloque |
| **T5.3** | Guion y grabación del vídeo | P4 | 12 | todo |

**Quitando A1, la única dependencia entre bloques distintos es T4.3 → T1.1 y
T1.2**, porque esos DAGs envuelven los scripts de P1. Todas las demás esperan
como mucho a la tarea anterior *de su propio bloque*, que hace la misma
persona, así que no son espera real.

En cuanto A1 esté mergeada hay **ocho tareas que se pueden empezar el mismo
día** — dos por bloque: T1.1 y T1.2, T2.1 y T2.2, T3.1, y T4.1, T4.2 y T4.4.
Los cuatro tenéis trabajo desde el minuto uno sin esperar a nadie.

---

## P1 · Almacenamiento y ciclo de vida

### T1.1 · Job de archivado con máquina de estados

| | |
|---|---|
| **Depende de** | nada |
| **Ficheros propios** | `archivado/job_archivado.py`, `archivado/estado.py` |
| **Solo lee** | `common/lakehouse.py`, `common/config.py` |

Es **la pieza que hace visible E8**: la que mueve datos del caliente al frío
recorriendo la máquina de estados de §3.2. El orden no se puede alterar ni
saltar ningún paso:

1. Lee la política vigente y pide candidatas con `particiones_a_archivar()`.
2. Por cada partición, marca `ESCRIBIENDO` en `archival_jobs` e incrementa
   `intentos`.
3. Lee esas filas de PostgreSQL y las escribe en Iceberg con
   `lakehouse.df_a_arrow(df)` + `tabla.append(...)`.
4. Marca `ESCRITO` guardando `snapshot_id` y `filas_escritas`.
5. **VERIFICA**: `lakehouse.contar_particion(tabla, desde, hasta)` tiene que dar
   exactamente lo mismo que `contar_particion(dia)` daba en PostgreSQL. Si no
   cuadra, estado `ERROR` y **no se borra nada**.
6. Solo entonces, `desalojar_particion(dia)`.

`archivado/estado.py` encapsula las transiciones de `archival_jobs` para que el
job no tenga SQL suelto por en medio y para que T1.2 no necesite duplicarlo.

Las piezas ya están hechas y probadas: las funciones SQL, el conteo exacto de
Iceberg y la tabla de estados. Esta tarea es pegarlas en orden.

**Hecho cuando:** se baja la retención a 5 minutos, se lanza el job, y se ven
filas desaparecer del caliente y aparecer en el frío con los conteos cuadrando.
Y además: lanzarlo dos veces seguidas no duplica nada, y matarlo entre el paso 3
y el 6 y relanzarlo retoma desde el estado guardado.

### T1.2 · Purga del tier frío

| | |
|---|---|
| **Depende de** | nada — no comparte fichero con T1.1 |
| **Ficheros propios** | `archivado/purga.py` |
| **Solo lee** | `common/lakehouse.py`, `archivado/estado.py` |

Cierra el ciclo de vida: la política `('trips','cold',NULL,7,'years','DELETE')`.
Borra de Iceberg las particiones mensuales que superan la custodia y ejecuta
después `expire_snapshots`, que es el mecanismo **nativo** de Iceberg para que
los ficheros dejen de ocupar de verdad. Ese detalle es media página de memoria:
no hemos inventado un sistema de retención por encima, usamos el que cada motor
ya trae.

**Hecho cuando:** bajando el umbral a `1 years`, las particiones de 2020
desaparecen de la tabla, `expire_snapshots` libera los ficheros en MinIO, y el
tamaño del bucket baja de verdad.

### T1.3 · Prueba end-to-end del ciclo de vida

| | |
|---|---|
| **Depende de** | T1.1 |
| **Ficheros propios** | `scripts/prueba_archivado.py` |

`scripts/prueba_humo.py` está **congelado** y no se toca: esta prueba va en un
fichero nuevo. Siembra filas viejas, baja la política a minutos, lanza el job y
comprueba los conteos a ambos lados, que el desalojo sin `VERIFICADO` se
rechaza, y que relanzar es idempotente. Es lo que convierte el «hecho cuando»
de T1.1 en algo que se puede repetir sin acordarse de los pasos.

**Hecho cuando:** termina en `TODO CORRECTO` sobre una base recién creada.

---

## P2 · Ingesta

**De este bloque dependen todas las métricas del proyecto.** Es el más crítico
de los cuatro.

### T2.1 · Simulador con jitter → PostgreSQL

| | |
|---|---|
| **Depende de** | nada |
| **Ficheros propios** | `simulador/simulador.py`, `simulador/jitter.py`, `simulador/sumidero_postgres.py` |
| **Solo lee** | `common/esquema.py`, `common/validacion.py`, `datos/muestra_1000.csv` |

Lee las mil filas de la muestra, **re-estampa `event_time` a «ahora»**
conservando `tpep_pickup_datetime` en 2020 (§3.1, y esto es lo más fácil de
liar del proyecto), y escribe en `taxi_trips` con `origen='stream'` insertando
por lotes con `execute_values`.

**El jitter no es un adorno, es lo que hace válidas las métricas.** Si el
simulador repite las mismas mil filas sin variarlas, la codificación por
diccionario de Parquet comprime los valores idénticos casi a coste cero y el
ratio sale **35x en vez de ~7,5x**. Los benchmarks publicados para datos de
taxi están en 4-8x: un 35x es pedir que os pregunten por qué vuestro resultado
es cinco veces mejor que el estado del arte, y la respuesta sería «porque
duplicamos las mismas mil filas». Perturbad importes, distancias, zonas y
pasajeros.

Inyecta además un porcentaje configurable de suciedad **que no aparezca de
forma natural**: nulos, fechas imposibles, esquemas rotos. Lo demás ya lo trae
el dataset (importes negativos, distancia cero, pasajeros a cero).

**Empezad por la escritura directa a PostgreSQL, sin Kafka**: en cuanto genere
un millón de filas realistas desbloquea las métricas de compresión y latencia
de todo el grupo. Kafka es fontanería y viene en T2.3.

Para que T2.3 no tenga que volver a abrir `simulador.py`, el sumidero se elige
con `--sumidero {postgres,kafka}` cargando el módulo
`simulador/sumidero_<nombre>.py`. Deja la interfaz cerrada en esta tarea.

**Hecho cuando:** el sistema se llena solo a un ritmo configurable y los
registros generados son **realmente distintos entre sí** — comprobadlo con un
`count(DISTINCT total_amount)`, no a ojo.

### T2.2 · Consumidor de Kafka

| | |
|---|---|
| **Depende de** | nada — se prueba con `kafka-console-producer` |
| **Ficheros propios** | `ingesta/consumidor_kafka.py` |
| **Solo lee** | `common/validacion.py`, `common/esquema.py` |

Consume de `trips.raw`, valida con `common.validacion.validar()`, y escribe los
válidos en el tier caliente y los rechazados en `trips_cuarentena` con su
`motivos` y el `payload` original en JSONB. Dos cosas que no se pueden saltar:

- **Insertar por lotes** con `execute_values`, igual que la prueba de humo. Fila
  a fila no aguanta el ritmo del simulador.
- **Confirmar el offset de Kafka después del commit en PostgreSQL**, nunca
  antes. Si el proceso se cae entre medias, se reprocesa el lote en vez de
  perderlo.

**Hecho cuando:** matando el proceso a mitad de lote y relanzándolo, no se
pierde ningún mensaje, y las filas rechazadas aparecen en `v_calidad`.

### T2.3 · Sumidero Kafka del simulador

| | |
|---|---|
| **Depende de** | T2.1 (la interfaz de sumidero) |
| **Ficheros propios** | `simulador/sumidero_kafka.py` |

Publica en `trips.raw` las dos columnas de tiempo desde el primer mensaje
(§3.1). No toca `simulador.py`: solo implementa la interfaz que T2.1 dejó.

**Hecho cuando:** con `--sumidero kafka` y el consumidor de T2.2 levantado, las
filas llegan a PostgreSQL habiendo pasado por Kafka, y el conteo cuadra con lo
emitido.

---

## P3 · Acceso

### T3.1 · Esqueleto de la API e instrumentación de `query_log`

| | |
|---|---|
| **Depende de** | A1 |
| **Ficheros propios** | `api/app.py`, `api/dependencias.py`, `api/instrumentacion.py`, `api/modelos.py`, y los tres módulos de rutas **vacíos**: `api/rutas_trips.py`, `api/rutas_metricas.py`, `api/rutas_ciclo_vida.py` |

Monta la aplicación, las conexiones a PostgreSQL y a Iceberg como dependencias,
y el modelo de respuesta de §6 (`data` + `meta` con `data_source`, `coverage`,
`as_of`, `latency_ms`, `rows`).

Lo importante de esta tarea: **`api/instrumentacion.py` escribe en `query_log`
desde el primer endpoint**. De esa tabla salen los percentiles de latencia por
tier, que son la prueba del «los datos recientes son rápidos». Si la
instrumentación existe desde el principio, las métricas de la semana 3 salen
solas; si no, no hay forma de inventarlas al final.

Crea los tres módulos de rutas con su `APIRouter` vacío y los registra en
`app.py`. **A partir de aquí nadie vuelve a tocar `app.py`**, y por eso T3.2 y
T3.3 pueden ir en paralelo, incluso repartidas entre dos personas.

**Hecho cuando:** `/health` y `/stats` responden con el `meta` completo y cada
llamada deja su fila en `query_log`, de forma que `v_latencia_por_tier` empieza
a devolver datos reales.

### T3.2 · Router de tiers y `/trips`

| | |
|---|---|
| **Depende de** | T3.1 |
| **Ficheros propios** | `api/router_tiers.py`, `api/lectura_hot.py`, `api/lectura_cold.py`, `api/rutas_trips.py` |

**El router es lo más interesante del proyecto técnicamente.** Recibe un rango
de fechas, lo corta por la frontera de retención — que lee de
`retention_policy`, no de una constante —, decide qué tiers tocar, consulta,
fusiona y anota de dónde viene cada dato. Orden de decisión: PostgreSQL si el
rango cae en caliente → Iceberg si cae en frío → los dos y fusión si cruza la
frontera.

`lectura_cold.py` va contra Iceberg con `lakehouse.obtener_tabla()` y
`tabla.scan(row_filter=..., selected_fields=...)`, proyectando solo las
columnas pedidas.

**Hecho cuando:** una consulta que cruza la frontera devuelve datos fusionados
de ambos tiers, con `data_source: "mixto"` y un `coverage` de dos tramos
correcto.

### T3.3 · Endpoints de ciclo de vida y métricas

| | |
|---|---|
| **Depende de** | T3.1 — **no** de T3.2, son ficheros distintos |
| **Ficheros propios** | `api/rutas_ciclo_vida.py`, `api/rutas_metricas.py` |

`/lifecycle/status` (estado de `archival_jobs` más `v_cumplimiento_politica`),
`/lifecycle/policy` en GET y **PUT**, y `/metrics/{nombre}` sirviendo las
vistas `v_coste_por_tier`, `v_latencia_por_tier`, `v_calidad` y
`v_metricas_caliente`.

El PUT es **el momento clave del vídeo** (§10, minuto 2:30): baja el umbral a 5
minutos en directo y dispara el archivado sin redesplegar nada. Que valide la
unidad contra el `CHECK` de la tabla y devuelva la política resultante.

**Hecho cuando:** un `PUT /lifecycle/policy` con 5 minutes cambia la tabla y
`particiones_a_archivar()` empieza a devolver candidatas acto seguido.

---

## P4 · Orquestación y observabilidad

**Todas las transformaciones y traspasos entre tiers pasan por Airflow**: si un
dato cambia de sitio, lo mueve un DAG. Un solo sitio donde mirar qué se mueve,
cuándo y por qué.

### T4.1 · Airflow + DAG `mantener_particiones`

| | |
|---|---|
| **Depende de** | A1 |
| **Ficheros propios** | `airflow/dags/mantener_particiones.py` |

Airflow en modo `standalone`, un contenedor. El DAG es diario y llama a
`crear_particiones_adelanto(7)`. Es el más sencillo de los cuatro y sirve para
dejar el contenedor probado antes de meter los que tienen chicha.

**Hecho cuando:** el DAG corre verde en Airflow y `v_particiones` muestra las
particiones de los próximos siete días.

### T4.2 · DAG `estadisticas_frio`

| | |
|---|---|
| **Depende de** | A1 — no depende de P1 |
| **Ficheros propios** | `airflow/dags/estadisticas_frio.py` |
| **Solo lee** | `common/lakehouse.py` |

Vuelca filas, bytes y ficheros de Iceberg a `cold_stats`, **porque Grafana no
sabe leer Iceberg**. Una fila con `particion_mes = NULL` para el total y una por
partición mensual. Sale entero de `lakehouse.estadisticas(tabla)`, que lee los
manifiestos sin tocar los datos.

Sin este DAG, `v_coste_por_tier` solo devuelve la mitad caliente, y esa vista es
**la métrica estrella de E8**.

**Hecho cuando:** `SELECT * FROM v_coste_por_tier` devuelve las dos filas, hot y
cold, con su `bytes_por_fila`.

### T4.3 · DAGs `archivar` y `purga_final`

| | |
|---|---|
| **Depende de** | T1.1 y T1.2 |
| **Ficheros propios** | `airflow/dags/archivar.py`, `airflow/dags/purga_final.py` |

Envuelven los scripts de P1. Son finos a propósito: la lógica vive en
`archivado/`, el DAG solo la programa, le pone reintentos y la hace visible.
Mientras P1 termina, T4.1, T4.2 y T4.4 dan trabajo de sobra.

**Hecho cuando:** se baja el umbral desde la API y el DAG mueve una partición
entera de caliente a frío sin intervención manual, con la máquina de estados
visible en `/lifecycle/status`.

### T4.4 · Grafana: datasource y dashboards

| | |
|---|---|
| **Depende de** | A1 — las vistas ya existen |
| **Ficheros propios** | `grafana/provisioning/datasources/postgres.yml`, `grafana/provisioning/dashboards/*` |

Un solo datasource, PostgreSQL. Las seis métricas de §7:

1. Filas por tier en el tiempo (área apilada) — hace visible la migración.
2. **Bytes por fila: PostgreSQL vs Iceberg** — la métrica estrella de E8.
3. Latencia p50/p95/p99 por tier frente a los SLAs (500 ms y 10 s).
4. Ratio de compresión del frío.
5. Edad del registro más antiguo en caliente — cumplimiento de la política.
6. Porcentaje de registros en cuarentena.

Casi todas salen de vistas que **ya existen y están probadas**:
`v_coste_por_tier`, `v_latencia_por_tier`, `v_cumplimiento_politica`,
`v_calidad`, `v_metricas_caliente`. Se puede hacer con los datos que deja la
prueba de humo y afinar después con los de P2.

**Hecho cuando:** el dashboard cuenta la historia de E8 sin que nadie tenga que
explicarla.

---

## Cierre (semana 3 — no se programa nada nuevo)

### T5.1 · Mediciones de §7

| | |
|---|---|
| **Dueño** | P2 |
| **Depende de** | T2.1 (volumen real) y T4.3 (ciclo cerrado) |
| **Ficheros propios** | `docs/MEDICIONES.md`, `docs/img/` |

Las seis métricas medidas de verdad, con capturas, sobre un volumen que no sea
de juguete. **Ojo con los bytes por fila del caliente**: con mil filas
repartidas en decenas de particiones el número está dominado por el coste fijo
de página y de índice, y sale absurdo. No lo llevéis a la memoria hasta tener
millones de filas.

### T5.2 · Memoria

| | |
|---|---|
| **Dueño** | cada uno el suyo |
| **Ficheros propios** | `docs/memoria/01-almacenamiento.md` (P1), `02-ingesta.md` (P2), `03-acceso.md` (P3), `04-orquestacion.md` (P4) |

Un fichero por bloque, **para que los cuatro podáis escribir a la vez sin
conflictos**. El ensamblado final lo hace P4 en `docs/memoria/00-memoria.md`,
que es el único fichero que toca.

### T5.3 · Guion y grabación del vídeo

| | |
|---|---|
| **Dueño** | P4 coordina |
| **Ficheros propios** | `docs/GUION_VIDEO.md` |

El guion de §10 de la arquitectura, con los tiempos ajustados a lo que de
verdad haya. Al ser grabado y no en directo, se puede preparar el estado del
sistema antes y repetir tomas.

---

## Mapa de propiedad de ficheros

Quién puede editar qué. Si vuestro nombre no está, no lo tocáis: lo pedís.

| Ruta | Dueño | Tarea |
|---|---|---|
| `docker-compose.yml` | P4 | A1, y después nadie |
| `archivado/job_archivado.py`, `archivado/estado.py` | P1 | T1.1 |
| `archivado/purga.py` | P1 | T1.2 |
| `scripts/prueba_archivado.py` | P1 | T1.3 |
| `simulador/simulador.py`, `jitter.py`, `sumidero_postgres.py` | P2 | T2.1 |
| `simulador/sumidero_kafka.py` | P2 | T2.3 |
| `ingesta/consumidor_kafka.py` | P2 | T2.2 |
| `api/app.py`, `dependencias.py`, `instrumentacion.py`, `modelos.py` | P3 | T3.1 |
| `api/router_tiers.py`, `lectura_hot.py`, `lectura_cold.py`, `rutas_trips.py` | P3 | T3.2 |
| `api/rutas_ciclo_vida.py`, `api/rutas_metricas.py` | P3 | T3.3 |
| `airflow/dags/mantener_particiones.py` | P4 | T4.1 |
| `airflow/dags/estadisticas_frio.py` | P4 | T4.2 |
| `airflow/dags/archivar.py`, `airflow/dags/purga_final.py` | P4 | T4.3 |
| `grafana/provisioning/**` | P4 | T4.4 |
| `<carpeta>/Dockerfile`, `<carpeta>/requirements.txt` | el dueño de la carpeta | tras A1 |

---

## Ficheros compartidos: el protocolo

Estos cuatro no tienen dueño y son donde de verdad se puede liar:

**`common/`** — el contrato de datos. Se puede **ampliar** (una función nueva,
una constante nueva) avisando al grupo; **cambiar** una firma o una regla de
validación se acuerda antes, porque lo importan los cuatro bloques.

**`postgres/init/*.sql`** — congelados. El esquema está completo para todo lo
que pide la arquitectura. Si de verdad falta algo, va en un fichero nuevo y hay
**un número reservado por bloque** para que no choquen: `04_p1.sql`,
`05_p2.sql`, `06_p3.sql`, `07_p4.sql`. Aviso al grupo obligatorio: tocar esto
obliga a todos a `docker compose down -v`, que borra los datos, y enterarse por
un conflicto de git es perder la tarde.

**`.env.example`** — cada bloque añade sus variables **en su sección**, que ya
existe. Al ser añadidos en sitios distintos del fichero, git los mezcla solo.

**`requirements-dev.txt`** — solo entorno local, y solo lo sube P4. Las
dependencias de cada servicio van en el `requirements.txt` de su carpeta. Si
alguien sube un pin, los demás tienen que rehacer su venv, así que quien lo
suba pasa la prueba de humo antes de commitear.

**`scripts/prueba_humo.py`** — congelado. Pruebas nuevas, ficheros nuevos.

**`README.md`** — la tabla de «Estado y siguientes pasos» la actualiza cada uno
al cerrar su tarea. Es una línea: si hay conflicto, se resuelve en diez
segundos.

---

## Orden sugerido

**Día 1** — A1, a solas. Después, los cuatro bloques arrancan a la vez.

**Semana 2** — P1 cierra T1.1 y T1.2, que son los que desbloquean T4.3. P2
cierra T2.1, que es el que desbloquea las métricas de todos. P3 y P4 avanzan en
paralelo sin esperar a nadie.

**Semana 3** — T5.1, T5.2 y T5.3. **No se programa nada nuevo.** Ese margen es
lo que separa un proyecto que se entrega bien de uno que se entrega a medias.
