# Guía de funcionamiento

Cómo levantar el proyecto completo, cargar los datos, comprobar que todo
funciona y ver el ciclo de vida de E8 en marcha. **Es la guía viva del
grupo**: se actualiza cada vez que entra una parte nueva o cambia cómo se hace
algo.

> **Última actualización:** 2026-09-24 · P1, P2 y P4 integradas en `main` y
> probadas juntas en una instalación limpia. **Falta P3** (API y router).

---

## Estado actual

| Bloque | Qué hace | Estado |
|---|---|---|
| **P1** · Almacenamiento y ciclo de vida | Job de archivado caliente→frío, purga del frío, carga inicial | ✅ en `main` |
| **P2** · Ingesta | Simulador con jitter, Kafka, consumidor → caliente + cuarentena | ✅ en `main` |
| **P3** · Acceso | API: `/trips` con router de tiers, métricas, `/lifecycle/*` | ❌ **pendiente** |
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

| Dónde | Qué mirar |
|---|---|
| **Airflow** · http://localhost:8080 | Los 4 DAGs en verde: `mantener_particiones` (diario), `archivar` (cada 5 min), `estadisticas_frio`, `purga_final` (diario) |
| **Grafana** · http://localhost:3000 (`admin` / `admin`) | Dashboard «E8 · Ciclo de vida de los datos» |
| **MinIO** · http://localhost:9001 (`minioadmin` / `minioadmin_dev_2026`) | Bucket `bronze` con los CSV y `lakehouse` con los Parquet de Iceberg |
| **API** · http://localhost:8000/docs | De momento solo `/health` (falta P3) |

Consultas útiles:

```bash
# Cuánto ocupa una fila en cada tier (la métrica estrella de E8)
docker compose exec postgres psql -U pids -d pids -c "SELECT * FROM v_coste_por_tier;"

# Flujo en vivo: tienen que ir subiendo
docker compose exec postgres psql -U pids -d pids -c "SELECT origen, count(*) FROM taxi_trips GROUP BY 1;"
docker compose logs --tail 3 consumidor

# Calidad: qué se rechaza y por qué
docker compose exec postgres psql -U pids -d pids -c "SELECT * FROM v_calidad;"
```

Referencia de la prueba en instalación limpia (3M de filas; con 1M, todo en
proporción): Kafka 35.800 emitidos = 35.800 recibidos; una fila ocupa
**~320 B en PostgreSQL y ~54 B en Iceberg** (unas 6 veces menos).

---

## 6. La demo: ver la migración caliente → frío

```bash
# 1. Bajar la política. Es un UPDATE: sin redesplegar nada
docker compose exec postgres psql -U pids -d pids -c \
  "UPDATE retention_policy SET umbral_valor=5, umbral_unidad='minutes' WHERE accion='ARCHIVE';"

# 2. En la siguiente pasada de "archivar" (cada 5 min) los días anteriores a
#    hoy pasan al frío. Seguirlo: todo en DESALOJADO y origen = escritas
docker compose exec postgres psql -U pids -d pids -c \
  "SELECT estado, count(*), sum(filas_origen) origen, sum(filas_escritas) escritas FROM archival_jobs GROUP BY 1;"

# 3. Al acabar, volver a la política real
docker compose exec postgres psql -U pids -d pids -c \
  "UPDATE retention_policy SET umbral_valor=30, umbral_unidad='days' WHERE accion='ARCHIVE';"
```

- Para no esperar a la siguiente pasada, se puede lanzar a mano desde Airflow
  (botón ▶ del DAG `archivar`) o con
  `docker compose exec airflow airflow dags trigger archivar`.
- **Lo de hoy no se archiva hasta mañana**: las particiones son diarias y solo
  se mueven los días anteriores al corte. Por eso la carga inicial deja los
  últimos 30 días en el caliente: son los que se ven migrar.
- En Grafana se ve bajar el caliente y subir el frío. Estado de cada partición:
  `SELECT * FROM archival_jobs ORDER BY particion;`

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

- [ ] **P3 · API** (`p3/api`): `/trips` con el router de tiers (`data_source` y
  `coverage`), `/metrics/{nombre}`, `/stats`, `/lifecycle/status`,
  `/lifecycle/policy` (GET y **PUT**) y la escritura en `query_log`. Cuando
  esté:
  - el paso 6 de la demo se hará con el `PUT /lifecycle/policy` en vez del
    `UPDATE`;
  - la gráfica de latencias por tier de Grafana tendrá datos reales;
  - habrá que añadir aquí las consultas de ejemplo (solo caliente, solo frío y
    una que cruce la frontera).
- [ ] **Mediciones (T5.1)**: las seis métricas de §7 con capturas, en
  `docs/MEDICIONES.md`.
- [ ] **Memoria (T5.2)** y **vídeo (T5.3)**.
