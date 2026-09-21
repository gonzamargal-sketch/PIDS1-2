# Reparto del trabajo

Los pasos 1 a 5 están cerrados y son los cimientos compartidos. A partir de
aquí los cuatro bloques son independientes: nadie tiene que esperar a que
otro termine para empezar.

Antes de tocar nada, leed las secciones **3.1, 3.2 y 3.3** de
[`ARQUITECTURA.md`](ARQUITECTURA.md). Son las tres ideas que condicionan lo
que escribe cada uno.

---

## Reglas comunes

**`common/` y `postgres/init/` son territorio compartido.** Si necesitáis
cambiar el contrato de datos o el esquema, avisad al grupo antes. No es
burocracia: tocar `postgres/init/` obliga a todos a hacer
`docker compose down -v`, que borra los datos. Enterarse por un conflicto de
git es perder la tarde.

**Una rama por bloque**: `p1/almacenamiento`, `p2/ingesta`, `p3/api`,
`p4/orquestacion`. Cada uno trabaja sobre todo en su carpeta.

**Antes de commitear, que pase la prueba de humo**:
`python scripts/prueba_humo.py` → `TODO CORRECTO`.

**Levantad solo vuestro perfil**, no hace falta todo el stack:

```bash
docker compose --profile core up -d                   # todos, siempre
docker compose --profile core --profile stream up -d  # P2
docker compose --profile core --profile orch up -d    # P4
```

---

## P1 — Almacenamiento y ciclo de vida

**Ya hecho**: pasos 4 y 5. Bronze y la tabla Iceberg funcionan.

**Pendiente: paso 8, el job de archivado.** Es la pieza que hace visible E8:
la que mueve datos del caliente al frío recorriendo la máquina de estados.

Fichero: `archivado/job_archivado.py`

El orden importa y no se puede saltar ningún paso:

1. Lee la política vigente y pide candidatas con `particiones_a_archivar()`
2. Por cada partición, marca `ESCRIBIENDO` en `archival_jobs`
3. Lee esas filas de PostgreSQL y las escribe en Iceberg con
   `lakehouse.df_a_arrow()` + `tabla.append()`
4. Marca `ESCRITO` guardando `snapshot_id` y el conteo
5. **VERIFICA**: `lakehouse.contar_particion()` tiene que dar exactamente lo
   mismo que había en PostgreSQL. Si no cuadra, se queda en `ERROR` y no se
   borra nada
6. Solo entonces, `desalojar_particion()`

**La regla que no se negocia**: nunca se borra sin haber verificado. Es lo
único que hace segura la mudanza, que es el modelo que elegimos.

Ya tenéis todas las piezas hechas: las funciones SQL, el conteo de Iceberg y
la tabla de estados. Es pegarlas en orden.

**Hecho cuando**: se puede bajar la retención a 5 minutos, lanzar el job, y
ver filas desaparecer del caliente y aparecer en el frío con los conteos
cuadrando.

---

## P2 — Ingesta y streaming

**De este bloque dependen todas las métricas del proyecto.** Es el más
crítico de los cuatro.

**Paso 6 primero: el simulador.** `simulador/simulador.py`

Lee las mil filas de `datos/muestra_1000.csv`, re-estampa `event_time` a
«ahora» y **aplica jitter**: perturba importes, distancias, zonas y número
de pasajeros para que las filas generadas sean realmente distintas.

**El jitter no es un adorno, es lo que hace válidas las métricas.** Si el
simulador repite las mismas mil filas sin variarlas, la codificación por
diccionario de Parquet comprime los valores idénticos casi a coste cero y el
ratio sale **35x en vez de ~7,5x**. Los benchmarks publicados para datos de
taxi están en 4-8x, así que un 35x es pedir que os pregunten por qué vuestro
resultado es cinco veces mejor que el estado del arte. Y la respuesta sería
«porque duplicamos las mismas mil filas».

Además, inyecta un porcentaje configurable de suciedad **que no aparezca de
forma natural**: nulos, fechas imposibles, esquemas rotos. Lo demás ya lo trae
el dataset (importes negativos, distancia cero, pasajeros a cero).

**Empezad por una versión que escriba directa a PostgreSQL, sin Kafka.** En
cuanto genere un millón de filas realistas, desbloquea las métricas de
compresión y latencia de todo el grupo. Kafka y Spark son fontanería y vienen
después.

**Paso 7: Spark Structured Streaming.** `ingesta/consumidor_spark.py`
Consume de Kafka, valida con `common.validacion`, y escribe en el tier
caliente, en Redis (agregados con TTL y el stream de eventos recientes) y en
cuarentena lo que no pasa.

**Hecho cuando**: el sistema se llena solo a un ritmo configurable y los
registros generados son realmente distintos entre sí.

---

## P3 — Acceso

**Paso 10: la API y el router de consultas.** `api/`

El router es lo más interesante del proyecto técnicamente. Recibe un rango de
fechas, lo corta por la frontera de retención, decide qué tiers tocar,
consulta, fusiona y **anota de dónde viene cada dato**.

Orden de preferencia: Redis si la métrica está cacheada → PostgreSQL si el
rango cae en caliente → Iceberg si cae en frío → los dos y fusión si cruza.

Toda respuesta lleva:

```json
{
  "data": [ ... ],
  "meta": {
    "data_source": "mixto",
    "coverage": [
      {"desde": "...", "hasta": "...", "tier": "hot"},
      {"desde": "...", "hasta": "...", "tier": "cold"}
    ],
    "as_of": "...", "latency_ms": 847, "rows": 1520
  }
}
```

Eso es requisito explícito de E8 y además es lo que la Parte 3 necesitará para
poder decirle al usuario si ve algo fresco o histórico.

**Instrumentad `query_log` desde el primer endpoint.** De esa tabla salen los
percentiles de latencia por tier, que son la prueba del «los datos recientes
son rápidos». Si la instrumentación existe desde el principio, las métricas de
la última semana salen solas.

Endpoints mínimos: `/trips`, `/metrics/{nombre}`, `/stats`,
`/lifecycle/status`, `/lifecycle/policy` (GET y PUT, para bajar el umbral en
el vídeo), `/health`.

**Puedes empezar ya**: el tier caliente y el frío existen los dos.

**Hecho cuando**: una consulta que cruza la frontera devuelve datos fusionados
de ambos tiers con el `coverage` correcto.

---

## P4 — Orquestación y observabilidad

**Paso 9: Airflow.** `airflow/dags/`

Airflow en modo `standalone`, un solo contenedor. Cuatro DAGs:

- `mantener_particiones` — diario, llama a `crear_particiones_adelanto(7)`
- `archivar` — envuelve el job de P1
- `estadisticas_frio` — vuelca filas, bytes y ficheros de Iceberg a
  `cold_stats`, **porque Grafana no sabe leer Iceberg**
- `purga_final` — borrado definitivo pasados los años de custodia

**No esperes a P1**: los dos primeros y el cuarto llaman a funciones SQL que
ya existen y están probadas.

**Paso 11: Grafana.** Datasources de PostgreSQL y Redis. Las siete métricas:

1. Filas por tier en el tiempo (área apilada) — hace visible la migración
2. **Bytes por fila: PostgreSQL vs Iceberg** — la métrica estrella de E8
3. Latencia p50/p95/p99 por tier frente a los SLAs
4. Ratio de compresión
5. Edad del registro más antiguo en caliente — cumplimiento de la política
6. Aciertos de caché en Redis
7. Porcentaje de registros en cuarentena

Casi todas salen de vistas que ya existen: `v_coste_por_tier`,
`v_latencia_por_tier`, `v_cumplimiento_politica`, `v_calidad`.

**Hecho cuando**: el dashboard cuenta la historia de E8 sin que nadie tenga
que explicarla.

---

## Las dos únicas dependencias

**P4 necesita el job de P1** para cerrar el DAG `archivar`. Mientras tanto,
tiene otros tres DAGs que puede hacer ya.

**P3 y P4 lucen más con datos de P2.** Pero ambos pueden trabajar con lo que
deja la prueba de humo.

Nada más. El resto es paralelo de verdad.

---

## Calendario

**Semana 2** — P2 cierra el simulador (paso 6) y P1 el job de archivado
(paso 8). Son los dos que desbloquean al resto. P3 y P4 avanzan en paralelo.

**Semana 3** — Mediciones, memoria y vídeo. **No se programa nada nuevo.**
Ese margen es lo que separa un proyecto que se entrega bien de uno que se
entrega a medias.
