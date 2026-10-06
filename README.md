# PIDS Parte 2 — Infraestructura de datos

Escenario **E8: Retención y ciclo de vida de los datos**, sobre el esquema del
dataset NYC Yellow Taxi con **datos de 2026**: del 1 de enero hasta hoy,
generados a partir de mil viajes reales del portal (ver
[Datos de trabajo](#datos-de-trabajo-2026)).

**Para levantarlo todo y verlo funcionar, seguid la
[guía de funcionamiento](docs/GUIA.md)**: comandos en orden, qué comprobar y la
demo del ciclo de vida. El diseño y el porqué de cada decisión están en
[`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md).

---

## Alcance

Los datos se reparten solo entre **PostgreSQL** (tier caliente) y **MinIO**
(bronze y tier frío, consultado con **Iceberg**). La ingesta entra por
**Kafka** y todas las transformaciones y traspasos entre tiers los hace
**Airflow**. No hay Redis ni Spark.

---

## Arranque en 3 minutos

```bash
git clone https://github.com/gonzamargal-sketch/PIDS1-2.git pids-parte2 && cd pids-parte2
cp .env.example .env

docker compose --profile core up -d
docker compose ps                      # esperar a que postgres esté healthy
```

Comprobad que el esqueleto está sano:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

python scripts/prueba_humo.py
```

> Requiere **Python 3.11 o superior**. Probado en 3.11 y 3.14.

Tiene que terminar en `TODO CORRECTO`. Esa prueba lee las 1.000 filas de
muestra, las pasa por el contrato de datos, las inserta, y ejercita la máquina
de estados del archivado incluida la comprobación de que **no se puede
desalojar una partición sin haberla verificado antes**.

Servicios levantados:

| Servicio | URL | Credenciales |
|---|---|---|
| PostgreSQL | `localhost:5432` | `pids` / `pids_dev_2026` |
| MinIO consola | http://localhost:9001 | `minioadmin` / `minioadmin_dev_2026` |
| API | http://localhost:8000/docs | — |
| Airflow (perfil `orch`) | http://localhost:8080 | sin login en desarrollo |
| Grafana (perfil `viz`) | http://localhost:3000 | `admin` / `admin` |

Tras cambiar un `requirements.txt` de un servicio:
`docker compose build <servicio>`. El código se monta como volumen, así que
tocar un `.py` no pide rebuild.

---

## Perfiles de Docker Compose

No hace falta levantarlo todo para cada cosa:

```bash
docker compose --profile core up -d                     # ~1,3 GB, siempre
docker compose --profile core --profile stream up -d    # + Kafka, consumidor y simulador
docker compose --profile core --profile orch up -d      # + Airflow
docker compose --profile core --profile chat up -d      # + chatbot de la Parte 3 (localhost:8501)
docker compose --profile "*" up -d                      # todo
```

**Si tenéis 8 GB de RAM**, configurad `%UserProfile%\.wslconfig` o Docker se
quedará sin memoria:

```ini
[wsl2]
memory=6GB
processors=4
swap=4GB
```

Luego `wsl --shutdown` desde PowerShell para que tome efecto.

---

## Estructura

```
pids-parte2/
├── docs/
│   ├── GUIA.md              ·  guía de funcionamiento: comandos, comprobaciones, demo
│   └── ARQUITECTURA.md      ·  el documento de diseño y el porqué de cada decisión
├── docker-compose.yml       ← con perfiles core/stream/orch/viz/chat
├── .env.example             ← copiar a .env
├── common/                  ← CONTRATO DE DATOS (compartido por todos)
│   ├── config.py            ·  variables de entorno en un solo sitio
│   ├── esquema.py           ·  lectura y normalización del dataset
│   ├── validacion.py        ·  reglas de calidad (rechazo / aviso)
│   ├── lakehouse.py         ·  tabla Iceberg: esquema, partición, ZSTD
│   └── mensajes.py          ·  formato de los mensajes de Kafka
├── postgres/init/           ← se ejecuta solo la PRIMERA vez que arranca
│   ├── 01_esquema.sql       ·  tablas
│   ├── 02_funciones.sql     ·  particiones, archivado, vistas de métricas
│   ├── 03_politicas.sql     ·  políticas de retención iniciales
│   ├── 05_p2.sql            ·  índice de idempotencia de la cuarentena
│   ├── 07_p4.sql            ·  hot_stats y v_historico_por_tier
│   └── 08_p3.sql            ·  índices de la pestaña En vivo e interruptor del simulador
├── datos/
│   └── muestra_1000.csv     ← semilla del simulador y fixture de tests
├── scripts/
│   ├── prueba_humo.py              ·  verifica que el esqueleto está sano
│   ├── prueba_archivado.py         ·  prueba end-to-end del ciclo de vida
│   ├── generar_datos_sinteticos.py ·  CSV sintético con el esquema del portal
│   ├── anadir_viaje.py             ·  mete un viaje a mano por el contrato de datos (GUIA §7)
│   └── descargar_bronze.py         ·  descarga los 24,6M del portal (opcional)
├── ingesta/
│   ├── subir_bronze.py      ·  paso 4: dataset crudo → MinIO
│   ├── carga_inicial.py     ·  paso 5: bronze → Iceberg
│   └── consumidor_kafka.py  ·  Kafka → PostgreSQL + cuarentena
├── simulador/               ← replay con jitter; sumideros postgres y kafka
├── archivado/               ← job hot→cold (máquina de estados) y purga del frío
├── airflow/                 ← 4 DAGs: particiones, archivar, estadísticas, purga
├── grafana/                 ← datasource PostgreSQL y dashboard «E8 · Ciclo de vida»
├── api/                     ← /trips (router de tiers), /stats, /metrics, /lifecycle
│   └── web/                 ·  frontend en localhost:8000/app/
└── chatbot/                 ← asistente de la Parte 3 (Streamlit + FastAPI)
```

> **Si cambiáis el esquema**, los ficheros de `postgres/init/` solo se ejecutan
> cuando el volumen está vacío. Para recargarlos:
> `docker compose down -v && docker compose --profile core up -d`
> (esto borra todos los datos).

---

## El contrato de datos

Todo lo que lee el dataset pasa por `common/`. Si el formato de origen cambia,
se arregla ahí una vez y no en cinco sitios.

```python
from common import esquema, validacion

df = esquema.leer_csv("datos/muestra_1000.csv")   # normaliza y tipa
res = validacion.validar(df)

print(validacion.informe(res))
res.validos       # DataFrame con columna `avisos`
res.rechazados    # DataFrame con columna `motivos` → van a cuarentena
```

Resuelve dos trampas del portal que si no os costarían una tarde:

**Los nombres de columna cambian según cómo bajéis los datos.** El export CSV
da `VendorID` y `PULocationID`; la API SODA da `vendorid` y `pulocationid`. El
mapeo es insensible a mayúsculas, así que funcionan las dos.

**Y los formatos de fecha también.** El export da `01/01/2020 12:28:15 AM` y la
API da `2020-01-01T00:28:15.000`. Esto es lo peligroso: si se deja que pandas
infiera el formato americano, confundirá día y mes **sin dar ningún error**.
`esquema.parsear_fechas` detecta el formato y nunca infiere.

### Reglas de calidad

Salen de perfilar el dataset real, no de imaginarlo. Hay dos severidades:

**Rechazo** (va a `trips_cuarentena`): recogida anterior a 2025-12 **o en el
futuro** (el límite es «ahora», así que avanza solo con los días; el dataset
real de 2020 traía viajes de 2002), cronología invertida, importes por encima de
10.000 — hay uno de 998.310 $ —, distancias o duraciones imposibles, zonas
fuera de rango.

**Aviso** (entra marcado en `avisos`): importe negativo (son devoluciones
reales), `passenger_count` a cero (2,1% del dataset), distancia cero (1,2%,
carreras canceladas), duración sospechosa, velocidad imposible, zona 264/265.

No se inyecta suciedad falsa para lo que ya existe: el dataset trae un 3,7% de
anomalías reales.

---

## Ciclo de vida (E8)

### La política es un dato, no código

```sql
SELECT * FROM retention_policy;
```

Cambiarla es un `UPDATE`, no un redeploy. Para que el archivado se dispare
en la demo:

```sql
UPDATE retention_policy SET umbral_valor = 5, umbral_unidad = 'minutes'
 WHERE dataset = 'trips' AND accion = 'ARCHIVE';
```

### Máquina de estados

```
PENDIENTE → ESCRIBIENDO → ESCRITO → VERIFICADO → DESALOJADO
```

**Regla de oro: nunca se hace `DROP` de una partición sin pasar por
VERIFICADO.** La verificación compara el conteo de filas en Iceberg con el que
había en Postgres. Esto es lo que hace segura la mudanza: si el job muere a
mitad, al reejecutarse retoma desde el estado guardado, y si se lanza dos veces
la segunda no hace nada.

```sql
SELECT * FROM particiones_a_archivar();   -- candidatas según la política
SELECT desalojar_particion('2026-08-12'); -- falla si no está VERIFICADO
```

El job que recorre la máquina de estados y la purga del frío viven en
`archivado/`. Los dos se pueden relanzar cuando se quiera: retoman desde el
estado guardado y no duplican nada.

```bash
python archivado/job_archivado.py --simulacro   # qué particiones archivaría
python archivado/job_archivado.py               # caliente -> Iceberg -> DROP
python archivado/purga.py --simulacro           # qué meses borraría del frío
python archivado/purga.py                       # DELETE + expire_snapshots

python scripts/prueba_archivado.py              # prueba end-to-end: TODO CORRECTO
```

Desde Airflow: `/opt/pids-venv/bin/python -m archivado.job_archivado` y
`-m archivado.purga`. El job sale con código 1 si alguna partición acaba en
`ERROR`, para que el DAG la marque en rojo y la reintente.

### Consultas útiles

```sql
SELECT * FROM v_particiones;            -- inventario con tamaños y edades
SELECT * FROM v_metricas_caliente;      -- ocupación del tier caliente
SELECT * FROM v_coste_por_tier;         -- LA métrica de E8: bytes/fila hot vs cold
SELECT * FROM v_cumplimiento_politica;  -- ¿el archivado va al día?
SELECT * FROM v_latencia_por_tier;      -- percentiles frente a los SLAs
SELECT * FROM v_calidad;                -- qué se rechaza y por qué
```

---

## El tier frío (Iceberg)

La tabla vive en `lakehouse.trips`, sobre MinIO, con **catálogo SQL en el
propio PostgreSQL** (un contenedor menos que un Hive Metastore).

### Datos de trabajo (2026)

Los datos con los que se prueba y se mide son **de 2026, del 1 de enero hasta
ahora**, nunca del futuro. Salen de tres sitios:

| Fuente | Qué da | Fechas |
|---|---|---|
| `datos/muestra_1000.csv` | mil viajes reales del portal: semilla y fixture de las pruebas | enero de 2026 (el original era enero de 2020, movido 6 años) |
| `scripts/generar_datos_sinteticos.py` | el histórico en volumen, para las métricas | de 2026-01-01 hasta ahora; relanzarlo mañana da un día más |
| `simulador/` | el flujo en vivo por Kafka | viajes que acaban de terminar |

El generador parte de la muestra y hace cada fila distinta (distancia, duración,
importes, zonas, pasajeros), conserva las anomalías reales (importes negativos,
distancia cero, pasajeros a cero) y reparte las horas del día con el perfil
típico de los taxis de Nueva York. Trabaja en UTC, como PostgreSQL.

```bash
# 1. Generar el histórico: 1M de filas (~25 s; va a datos/sinteticos/, fuera de git)
python scripts/generar_datos_sinteticos.py 1000000 --seed 42

# 2. Subirlo a bronze y cargarlo: cada fila va al tier que le toca por edad (~1 min)
python ingesta/subir_bronze.py --origen datos/sinteticos
python ingesta/carga_inicial.py --recrear
```

**La carga inicial reparte por edad**, con la misma regla que
`particiones_a_archivar()`: lo anterior al día de corte de la política
(hoy menos 30 días) va a Iceberg y lo demás a PostgreSQL, sin solape. El
sistema arranca como si llevara funcionando desde enero, y el caliente ya
tiene días anteriores a hoy que el archivado puede mover en cuanto se baja la
política. `--recrear` borra la tabla Iceberg y la carga anterior del caliente
(`origen = 'carga_inicial'`) antes de volver a cargar.

> **El jitter del simulador no es un adorno.** Si amplifica repitiendo
> las mismas 1.000 filas, el ratio de compresión sale 35x en vez de ~7,5x,
> porque la codificación por diccionario de Parquet comprime valores idénticos
> casi a coste cero. Ese 35x es indefendible frente a los benchmarks
> publicados. Con jitter los números son honestos.

### Decisiones y por qué

**Partición por mes, no por día.** Con años de datos, por día serían 365
particiones al año y un problema de ficheros pequeños. Iceberg guarda min/max
por fichero en los manifiestos, así que con partición mensual seguimos teniendo
poda a nivel de día.

**ZSTD, no Snappy.** Snappy está pensado para datos que se leen constantemente.
El frío se escribe una vez y se lee poco, que es justo donde ZSTD gana un 30-40%
de tamaño. Va directo al «barato» de E8.

**PyIceberg, no Spark+Iceberg.** Hacer que Spark escriba en Iceberg exige
encajar versiones de Spark, Scala, `iceberg-spark-runtime`, `hadoop-aws` y el
SDK de AWS. Es donde se atascan estos proyectos. PyIceberg es Iceberg en Python
puro: mismo formato, mismos snapshots, sin JVM.

### Tres trampas que ya están resueltas en `common/lakehouse.py`

**`pyiceberg-core` es obligatorio.** PyIceberg lo deja como extra opcional, pero
sin él cualquier `append` a una tabla particionada revienta con
`NotInstalledError`. No se ve venir hasta el primer intento de escritura.

**PyArrow no convierte float64 a decimal128.** Falla con `Got bytestring of
length 8 (expected 16)`. Hay que pasar por objetos `Decimal` de Python, que es
lo que hace `df_a_arrow`.

**No contéis filas sumando `record_count` de `plan_files()`.** Eso devuelve los
*ficheros* que podrían contener filas coincidentes, y `record_count` cuenta
todas las filas de cada fichero, no las que casan con el filtro. Sobrecuenta, y
como ese conteo es la verificación previa al borrado, dejaría el archivado
bloqueado para siempre. `contar_particion` lee de verdad proyectando una sola
columna.

---

## Problemas frecuentes

### `Command 'docker' not found` dentro de WSL

**No instaléis nada de lo que sugiere la terminal.** Ni `docker.io` (trae una
versión antigua sin el plugin de Compose v2, así que `docker compose` con
espacio seguiría sin funcionar), ni `podman-docker` (es otro runtime distinto),
ni el snap. Y con Docker Desktop instalado en Windows, meter Docker también
dentro de WSL deja dos demonios peleándose.

Diagnóstico:

```bash
echo "distro: $WSL_DISTRO_NAME"
ls /mnt/wsl/docker-desktop/cli-tools/usr/bin/ 2>/dev/null
ls -la /var/run/docker.sock 2>/dev/null || echo "sin socket -> el demonio NO corre"
echo $PATH | tr ':' '\n' | grep -i docker || echo "docker NO está en el PATH"
```

**Si aparece el binario en `cli-tools` pero no hay socket ni PATH**, Docker
Desktop está instalado e integrado pero **no arrancado**. Ese montaje sobrevive
al cierre de la aplicación, así que su existencia no prueba nada. Abrid Docker
Desktop, esperad a *Engine running*, y desde PowerShell `wsl --shutdown` para
que WSL recoja el PATH limpio. Cerrar la ventana de la terminal no basta: la
distro sigue viva en segundo plano con el PATH viejo.

**Si no aparece el binario**, falta activar la integración: Docker Desktop →
*Settings* → *Resources* → *WSL Integration* → activar vuestra distro →
*Apply & Restart*. Con varias distros instaladas, comprobad con `wsl -l -v`
que la marcada es la que usáis; es fácil activarla en una y trabajar en otra.

### `pull access denied for minio/mc`

**MinIO archivó su edición community.** El repositorio de GitHub está archivado
desde abril de 2026 ("THIS REPOSITORY IS NO LONGER MAINTAINED") y las imágenes
`minio/minio` y `minio/mc` ya no existen en Docker Hub. La community edition se
distribuye ahora solo como código fuente.

El compose ya está arreglado: el servidor viene de **quay.io**, que es el
registro propio de MinIO, y el `mc` se ha sustituido por el cliente de AWS, que
hace lo mismo contra la API S3 y sí está mantenido.

**La versión está fijada a propósito.** En `RELEASE.2025-05-24` ("Breaking
Release") MinIO quitó la consola web embebida. Fijamos
`RELEASE.2025-04-22T22-12-26Z`, la última con el navegador de objetos completo,
porque se usa para enseñar el bronze y los Parquet del frío. **No la subáis a
`latest`**: os quedaríais sin consola.

Si `docker pull quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z` también
fallase, habría que cambiar de almacén S3 (SeaweedFS o Garage son los
candidatos). Afectaría a la subida a bronze y a la carga a Iceberg.

### `pg_config executable not found` al hacer pip install

Pip está intentando compilar `psycopg2-binary` desde el código fuente porque
no encuentra un wheel precompilado para vuestra versión de Python. Casi siempre
es que las versiones de `requirements-dev.txt` son anteriores a esa versión de
Python, no que Python sea "demasiado nuevo".

Comprobad si existe wheel para vuestra versión antes de dar por hecho que hay
que bajar de Python:

```bash
pip download psycopg2-binary --only-binary=:all: --no-deps -d /tmp/x
```

Si descarga algo, la solución es subir el pin en `requirements-dev.txt`, no
cambiar de intérprete. Si de verdad no existe wheel, entonces sí hace falta un
Python anterior. Tras subir un pin hay que rehacer el venv, y `pandas` 3.x
tiene cambios de ruptura frente al 2.x: pasad la prueba de humo después.

### La API sale `unhealthy` tras reiniciar el PC (`Could not import module "api.app"`)

Docker Desktop arranca los contenedores solo, **antes** de que el disco de WSL
esté montado, y los bind-mounts se quedan vacíos: la API sin código, Airflow
sin DAGs y Grafana sin dashboard. Todo sale `Up`, pero nada funciona.

Se comprueba con `docker compose exec api ls /app/api` (vacío = es esto) y se
arregla recreando los contenedores, sin perder datos (están en volúmenes):

```bash
docker compose --profile core --profile orch --profile viz up -d --force-recreate
```

Mientras estuvo así, `archivar` no corrió: `v_cumplimiento_politica` puede
salir `INCUMPLE` hasta la siguiente pasada (o lanzadla a mano desde Airflow).

### `port is already allocated`

Tenéis un PostgreSQL local escuchando en el 5432. Cambiad `POSTGRES_PORT_HOST`
en `.env`.

**Los cambios del `.sql` no se aplican** — los scripts de `postgres/init/` solo
corren con el volumen vacío. `docker compose down -v` y volved a levantar.

**`minio-init` aparece como `exited (0)`** — es correcto: crea los buckets y
termina.

**Filas en `taxi_trips_default`** — falta crear particiones para ese rango de
`event_time`. `SELECT crear_particiones_adelanto(7);`

**Docker se queda sin memoria** — configurad `.wslconfig` y usad perfiles en vez
de levantarlo todo.
