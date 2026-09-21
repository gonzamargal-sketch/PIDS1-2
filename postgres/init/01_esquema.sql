-- ============================================================
-- PIDS Parte 2 — Esquema del tier caliente
-- Escenario E8: Retención y ciclo de vida
--
-- CONCEPTO CLAVE: hay DOS columnas de tiempo y hacen cosas distintas.
--
--   tpep_pickup_datetime  cuándo ocurrió el viaje de verdad (2020).
--                         Es el dato de NEGOCIO. Se usa para consultas
--                         analíticas ("viajes de marzo"). NO gobierna nada
--                         del ciclo de vida.
--
--   event_time            cuándo el sistema considera que el evento entró.
--                         Es el dato de SISTEMA. Particiona la tabla,
--                         determina la edad y decide el desalojo.
--
-- Sin esta separación el proyecto no funciona: los datos son de 2020 y
-- estamos en 2026, así que por fecha de viaje TODO estaría caducado desde
-- el primer segundo y nunca veríamos nada moverse.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()

-- ════════════════════════════════════════════════════════════
-- TABLA PRINCIPAL: taxi_trips  (TIER CALIENTE)
-- Particionada por día sobre event_time.
-- Desalojar una partición es DROP TABLE: O(1), sin VACUUM, sin bloat.
-- ════════════════════════════════════════════════════════════
CREATE TABLE taxi_trips (
    trip_id                 UUID        NOT NULL DEFAULT gen_random_uuid(),

    -- Tiempo de sistema: gobierna el ciclo de vida (E8)
    event_time              TIMESTAMPTZ NOT NULL,

    -- Tiempo de negocio: cuándo ocurrió el viaje
    tpep_pickup_datetime    TIMESTAMPTZ NOT NULL,
    tpep_dropoff_datetime   TIMESTAMPTZ,

    -- Las 18 columnas del dataset NYC Yellow Taxi
    vendor_id               SMALLINT,
    passenger_count         SMALLINT,          -- nullable: hay ~800k nulos en 2020
    trip_distance           NUMERIC(10,2),
    ratecode_id             SMALLINT,          -- nullable
    store_and_fwd_flag      CHAR(1),           -- nullable
    pu_location_id          SMALLINT,
    do_location_id          SMALLINT,
    payment_type            SMALLINT,
    fare_amount             NUMERIC(12,2),
    extra                   NUMERIC(10,2),
    mta_tax                 NUMERIC(10,2),
    tip_amount              NUMERIC(12,2),
    tolls_amount            NUMERIC(12,2),
    improvement_surcharge   NUMERIC(10,2),
    total_amount            NUMERIC(12,2),
    congestion_surcharge    NUMERIC(10,2),     -- nullable

    -- Metadatos de procedencia
    origen                  TEXT        NOT NULL,   -- 'carga_inicial' | 'stream'
    fichero_origen          TEXT,
    esquema_version         TEXT        NOT NULL DEFAULT '1.0',
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Avisos de calidad: el registro se acepta pero queda marcado.
    -- Los rechazos van a trips_cuarentena, no aquí.
    avisos                  TEXT[],

    -- En una tabla particionada la clave de partición debe formar parte de la PK
    PRIMARY KEY (trip_id, event_time)
) PARTITION BY RANGE (event_time);

COMMENT ON TABLE  taxi_trips IS 'Tier caliente. Solo los últimos N días según retention_policy.';
COMMENT ON COLUMN taxi_trips.event_time IS 'Tiempo de sistema. Particiona y gobierna el ciclo de vida (E8).';
COMMENT ON COLUMN taxi_trips.tpep_pickup_datetime IS 'Tiempo de negocio. Cuándo ocurrió el viaje realmente.';

-- Índices: se crean sobre la tabla padre y Postgres los propaga a cada partición
CREATE INDEX idx_trips_pickup    ON taxi_trips (tpep_pickup_datetime);
CREATE INDEX idx_trips_pu_zona   ON taxi_trips (pu_location_id, event_time);
CREATE INDEX idx_trips_origen    ON taxi_trips (origen);

-- Partición por defecto: recoge cualquier event_time sin partición propia.
-- Es una red de seguridad para que una inserción nunca falle. Si acumula
-- filas, es que falta crear particiones (lo vigila el DAG del paso 9).
CREATE TABLE taxi_trips_default PARTITION OF taxi_trips DEFAULT;


-- ════════════════════════════════════════════════════════════
-- CUARENTENA: registros que NO pasan la validación
-- No se tiran: se guardan con el motivo, para poder contarlos y enseñarlos.
-- ════════════════════════════════════════════════════════════
CREATE TABLE trips_cuarentena (
    id              BIGSERIAL PRIMARY KEY,
    recibido_en     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    origen          TEXT        NOT NULL,
    fichero_origen  TEXT,
    -- Motivos de rechazo, p.ej. {'fecha_fuera_de_rango','importe_excesivo'}
    motivos         TEXT[]      NOT NULL,
    -- La fila original tal cual llegó, sin interpretar
    payload         JSONB       NOT NULL
);

CREATE INDEX idx_cuarentena_recibido ON trips_cuarentena (recibido_en);
CREATE INDEX idx_cuarentena_motivos  ON trips_cuarentena USING GIN (motivos);

COMMENT ON TABLE trips_cuarentena IS
  'Registros rechazados por el contrato de datos. Alimenta la métrica de calidad.';


-- ════════════════════════════════════════════════════════════
-- E8 · POLÍTICA DE RETENCIÓN
-- La política es un DATO, no código. Cambiarla es un UPDATE, no un redeploy.
-- Airflow lee esta tabla en cada ejecución.
-- ════════════════════════════════════════════════════════════
CREATE TABLE retention_policy (
    id              SERIAL PRIMARY KEY,
    dataset         TEXT    NOT NULL,
    tier_origen     TEXT    NOT NULL CHECK (tier_origen IN ('hot','cold','redis')),
    tier_destino    TEXT             CHECK (tier_destino IN ('cold', NULL)),
    umbral_valor    INT     NOT NULL CHECK (umbral_valor > 0),
    umbral_unidad   TEXT    NOT NULL CHECK (umbral_unidad IN ('minutes','hours','days','years')),
    accion          TEXT    NOT NULL CHECK (accion IN ('ARCHIVE','DELETE','EXPIRE')),
    activa          BOOLEAN NOT NULL DEFAULT TRUE,
    descripcion     TEXT,
    actualizado_en  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dataset, tier_origen, accion)
);

COMMENT ON TABLE retention_policy IS
  'E8: política de ciclo de vida. Para la demo: UPDATE a 5 minutes y se dispara en directo.';

-- Devuelve el umbral de una política como INTERVAL, listo para restar a NOW()
CREATE OR REPLACE FUNCTION umbral_intervalo(p_dataset TEXT, p_accion TEXT)
RETURNS INTERVAL LANGUAGE sql STABLE AS $$
    SELECT (umbral_valor || ' ' || umbral_unidad)::INTERVAL
    FROM retention_policy
    WHERE dataset = p_dataset AND accion = p_accion AND activa
    LIMIT 1;
$$;


-- ════════════════════════════════════════════════════════════
-- E8 · MÁQUINA DE ESTADOS DEL ARCHIVADO
--
-- Elegimos el modelo "mudanza" (Postgres y Iceberg sin solape), que no es
-- idempotente por defecto: si el job muere entre escribir en Iceberg y
-- borrar de Postgres, duplicamos o perdemos datos.
--
-- Esta tabla lo resuelve. REGLA DE ORO:
--   NUNCA se hace DROP de una partición sin haber pasado por VERIFICADO.
--
--   PENDIENTE -> ESCRIBIENDO -> ESCRITO -> VERIFICADO -> DESALOJADO
--
-- Si el DAG se cae, al reejecutarse retoma desde el estado guardado.
-- Si se lanza dos veces seguidas, la segunda no hace nada.
-- ════════════════════════════════════════════════════════════
CREATE TABLE archival_jobs (
    particion       DATE        PRIMARY KEY,      -- día de event_time
    estado          TEXT        NOT NULL DEFAULT 'PENDIENTE'
                    CHECK (estado IN ('PENDIENTE','ESCRIBIENDO','ESCRITO',
                                      'VERIFICADO','DESALOJADO','ERROR')),
    filas_origen    BIGINT,                       -- filas contadas en Postgres
    filas_escritas  BIGINT,                       -- filas confirmadas en Iceberg
    bytes_escritos  BIGINT,
    snapshot_id     BIGINT,                       -- snapshot de Iceberg resultante
    intentos        INT         NOT NULL DEFAULT 0,
    error           TEXT,
    iniciado_en     TIMESTAMPTZ,
    terminado_en    TIMESTAMPTZ,
    actualizado_en  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_archival_estado ON archival_jobs (estado);

-- Mantiene actualizado_en al día sin que haya que acordarse en cada UPDATE
CREATE OR REPLACE FUNCTION tocar_actualizado_en() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.actualizado_en := NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_archival_touch
    BEFORE UPDATE ON archival_jobs
    FOR EACH ROW EXECUTE FUNCTION tocar_actualizado_en();


-- ════════════════════════════════════════════════════════════
-- E8 · ESTADÍSTICAS DEL TIER FRÍO
-- Grafana NO sabe leer Iceberg. Un DAG calcula estas cifras y las deja
-- aquí para que Grafana las pinte como cualquier otra tabla.
-- ════════════════════════════════════════════════════════════
CREATE TABLE cold_stats (
    id                  BIGSERIAL PRIMARY KEY,
    medido_en           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    particion_mes       DATE,                 -- NULL = total de la tabla
    filas               BIGINT,
    bytes               BIGINT,
    ficheros            INT,
    bytes_por_fila      NUMERIC(10,2)
                        GENERATED ALWAYS AS
                        (CASE WHEN filas > 0 THEN bytes::NUMERIC / filas END) STORED,
    snapshot_id         BIGINT
);

CREATE INDEX idx_cold_stats_medido ON cold_stats (medido_en DESC);


-- ════════════════════════════════════════════════════════════
-- MEDICIÓN · REGISTRO DE CONSULTAS
-- De aquí salen los percentiles de latencia por tier, que es la prueba
-- del "los datos recientes son rápidos" de E8.
-- Se crea YA, en la semana 1, aunque esté vacía: si la instrumentación
-- existe desde el principio, las métricas de la semana 3 salen solas.
-- ════════════════════════════════════════════════════════════
CREATE TABLE query_log (
    id              BIGSERIAL PRIMARY KEY,
    ocurrido_en     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    endpoint        TEXT        NOT NULL,
    data_source     TEXT        NOT NULL CHECK (data_source IN ('cache','hot','cold','mixto')),
    rango_desde     TIMESTAMPTZ,
    rango_hasta     TIMESTAMPTZ,
    filas           INT,
    latencia_ms     NUMERIC(10,2) NOT NULL,
    cache_hit       BOOLEAN     NOT NULL DEFAULT FALSE,
    parametros      JSONB
);

CREATE INDEX idx_query_log_tier   ON query_log (data_source, ocurrido_en DESC);
CREATE INDEX idx_query_log_cuando ON query_log (ocurrido_en DESC);
