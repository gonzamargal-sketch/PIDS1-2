-- ============================================================
-- PIDS Parte 2 — Funciones y vistas del ciclo de vida
-- ============================================================

-- ════════════════════════════════════════════════════════════
-- GESTIÓN DE PARTICIONES
-- ════════════════════════════════════════════════════════════

-- Crea la partición diaria de un día concreto. Idempotente.
CREATE OR REPLACE FUNCTION crear_particion_dia(p_dia DATE)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_nombre TEXT := 'taxi_trips_' || to_char(p_dia, 'YYYY_MM_DD');
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = v_nombre) THEN
        RETURN v_nombre || ' ya existía';
    END IF;

    EXECUTE format(
        'CREATE TABLE %I PARTITION OF taxi_trips FOR VALUES FROM (%L) TO (%L)',
        v_nombre, p_dia::TIMESTAMPTZ, (p_dia + 1)::TIMESTAMPTZ
    );
    RETURN v_nombre || ' creada';
END;
$$;

COMMENT ON FUNCTION crear_particion_dia IS 'Crea la partición diaria de taxi_trips. Idempotente.';


-- Crea las particiones de los próximos N días (más el de hoy y el de ayer,
-- por si llega algún evento con event_time ligeramente atrasado).
-- La llama un DAG de Airflow todos los días.
CREATE OR REPLACE FUNCTION crear_particiones_adelanto(p_dias INT DEFAULT 7)
RETURNS TABLE(resultado TEXT) LANGUAGE plpgsql AS $$
DECLARE
    d DATE;
BEGIN
    FOR d IN
        SELECT generate_series(CURRENT_DATE - 1, CURRENT_DATE + p_dias, '1 day')::DATE
    LOOP
        resultado := crear_particion_dia(d);
        RETURN NEXT;
    END LOOP;
END;
$$;


-- Inventario de particiones: tamaño, filas estimadas y edad.
-- filas_estimadas viene de reltuples (lo actualiza ANALYZE) y es barato.
-- Para conteos exactos, usar contar_particion().
CREATE OR REPLACE VIEW v_particiones AS
SELECT
    c.relname::TEXT AS particion,
    CASE WHEN c.relname ~ '^taxi_trips_\d{4}_\d{2}_\d{2}$'
         THEN to_date(right(c.relname, 10), 'YYYY_MM_DD')
    END                                        AS dia,
    GREATEST(c.reltuples, 0)::BIGINT           AS filas_estimadas,
    pg_total_relation_size(c.oid)              AS bytes_total,
    pg_relation_size(c.oid)                    AS bytes_datos,
    pg_indexes_size(c.oid)                     AS bytes_indices,
    CASE WHEN c.relname ~ '^taxi_trips_\d{4}_\d{2}_\d{2}$'
         THEN CURRENT_DATE - to_date(right(c.relname, 10), 'YYYY_MM_DD')
    END                                        AS edad_dias
FROM pg_class c
JOIN pg_inherits i ON i.inhrelid = c.oid
JOIN pg_class p    ON p.oid = i.inhparent
WHERE p.relname = 'taxi_trips'
ORDER BY 2 NULLS LAST;


-- Conteo exacto de una partición (el que se usa para VERIFICAR antes del DROP)
CREATE OR REPLACE FUNCTION contar_particion(p_dia DATE)
RETURNS BIGINT LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_nombre TEXT := 'taxi_trips_' || to_char(p_dia, 'YYYY_MM_DD');
    v_n      BIGINT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = v_nombre) THEN
        RETURN NULL;
    END IF;
    EXECUTE format('SELECT count(*) FROM %I', v_nombre) INTO v_n;
    RETURN v_n;
END;
$$;


-- ════════════════════════════════════════════════════════════
-- E8 · SELECCIÓN DE CANDIDATAS AL ARCHIVADO
-- Devuelve las particiones que superan el umbral de la política vigente
-- y que aún no están desalojadas.
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION particiones_a_archivar()
RETURNS TABLE(
    particion       TEXT,
    dia             DATE,
    edad_dias       INT,
    filas_estimadas BIGINT,
    bytes_total     BIGINT,
    estado          TEXT
) LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_umbral INTERVAL := umbral_intervalo('trips', 'ARCHIVE');
    v_corte  DATE;
BEGIN
    IF v_umbral IS NULL THEN
        RAISE EXCEPTION 'No hay política ARCHIVE activa para el dataset trips';
    END IF;

    -- Fecha de corte: todo lo anterior es candidato
    v_corte := (NOW() - v_umbral)::DATE;

    RETURN QUERY
    SELECT
        p.particion,
        p.dia,
        p.edad_dias::INT,
        p.filas_estimadas,
        p.bytes_total,
        COALESCE(a.estado, 'PENDIENTE')
    FROM v_particiones p
    LEFT JOIN archival_jobs a ON a.particion = p.dia
    WHERE p.dia IS NOT NULL
      AND p.dia < v_corte
      AND COALESCE(a.estado, 'PENDIENTE') <> 'DESALOJADO'
    ORDER BY p.dia;
END;
$$;

COMMENT ON FUNCTION particiones_a_archivar IS
  'E8: particiones que superan el umbral de retention_policy y siguen en caliente.';


-- ════════════════════════════════════════════════════════════
-- E8 · DESALOJO
-- Solo borra si la partición está en estado VERIFICADO, es decir, si ya se
-- comprobó que las filas están íntegras en Iceberg. Esta comprobación es
-- lo que hace segura la mudanza.
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION desalojar_particion(p_dia DATE, p_forzar BOOLEAN DEFAULT FALSE)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_nombre TEXT := 'taxi_trips_' || to_char(p_dia, 'YYYY_MM_DD');
    v_estado TEXT;
    v_bytes  BIGINT;
BEGIN
    SELECT estado INTO v_estado FROM archival_jobs WHERE particion = p_dia;

    -- Idempotencia: si ya está desalojada, no es un error. Airflow reintenta
    -- tareas y una segunda ejecución no puede tumbar el DAG.
    IF v_estado = 'DESALOJADO' THEN
        RETURN v_nombre || ' ya estaba desalojada; no se hace nada';
    END IF;

    IF NOT p_forzar AND COALESCE(v_estado, 'PENDIENTE') <> 'VERIFICADO' THEN
        RAISE EXCEPTION
            'Se ha intentado desalojar % en estado %. Solo se desaloja desde VERIFICADO.',
            p_dia, COALESCE(v_estado, 'PENDIENTE');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = v_nombre) THEN
        UPDATE archival_jobs SET estado = 'DESALOJADO', terminado_en = NOW()
        WHERE particion = p_dia;
        RETURN v_nombre || ' no existía; marcada como DESALOJADO';
    END IF;

    SELECT pg_total_relation_size(v_nombre::regclass) INTO v_bytes;

    -- DROP de la partición: O(1), sin dead tuples, sin VACUUM.
    -- Esto es lo que hace que el desalojo sea barato frente a un DELETE.
    EXECUTE format('DROP TABLE %I', v_nombre);

    UPDATE archival_jobs
       SET estado = 'DESALOJADO', terminado_en = NOW()
     WHERE particion = p_dia;

    RETURN format('%s desalojada (%s liberados)', v_nombre, pg_size_pretty(v_bytes));
END;
$$;


-- ════════════════════════════════════════════════════════════
-- MÉTRICAS E8
-- ════════════════════════════════════════════════════════════

-- Ocupación del tier caliente
CREATE OR REPLACE VIEW v_metricas_caliente AS
SELECT
    COUNT(*) FILTER (WHERE dia IS NOT NULL)              AS particiones,
    COALESCE(SUM(filas_estimadas), 0)                    AS filas,
    COALESCE(SUM(bytes_total), 0)                        AS bytes,
    COALESCE(SUM(bytes_indices), 0)                      AS bytes_indices,
    ROUND(COALESCE(SUM(bytes_total), 0)::NUMERIC
          / NULLIF(SUM(filas_estimadas), 0), 1)          AS bytes_por_fila,
    MIN(dia)                                             AS dia_mas_antiguo,
    MAX(edad_dias)                                       AS edad_maxima_dias
FROM v_particiones;


-- LA métrica de E8: lo que cuesta una fila en caliente frente a en frío.
-- Con la cabecera de tupla, el alineamiento y los índices, una fila en
-- Postgres sale por unos 250-350 B frente a ~16 B en Iceberg. Quince o
-- veinte veces. Eso es "los datos recientes son caros y los antiguos baratos"
-- convertido en número.
CREATE OR REPLACE VIEW v_coste_por_tier AS
WITH caliente AS (
    SELECT filas, bytes, bytes_por_fila FROM v_metricas_caliente
),
frio AS (
    SELECT filas, bytes, bytes_por_fila
    FROM cold_stats
    WHERE particion_mes IS NULL
    ORDER BY medido_en DESC
    LIMIT 1
)
SELECT 'hot'  AS tier, 'PostgreSQL' AS motor, filas, bytes, bytes_por_fila FROM caliente
UNION ALL
SELECT 'cold' AS tier, 'Iceberg'    AS motor, filas, bytes, bytes_por_fila FROM frio;


-- Cumplimiento de la política: la edad del registro más antiguo en caliente
-- no debería superar nunca el umbral. Si lo supera, el archivado va atrasado.
CREATE OR REPLACE VIEW v_cumplimiento_politica AS
SELECT
    (SELECT umbral_valor || ' ' || umbral_unidad
       FROM retention_policy
      WHERE dataset='trips' AND accion='ARCHIVE' AND activa)  AS umbral,
    m.edad_maxima_dias,
    EXTRACT(DAY FROM umbral_intervalo('trips','ARCHIVE'))::INT AS umbral_dias,
    CASE
        WHEN m.edad_maxima_dias IS NULL THEN 'SIN DATOS'
        WHEN m.edad_maxima_dias <= EXTRACT(DAY FROM umbral_intervalo('trips','ARCHIVE'))
             THEN 'OK'
        ELSE 'INCUMPLE'
    END                                                       AS estado
FROM v_metricas_caliente m;


-- Latencia por tier: la prueba del "los datos recientes son rápidos"
CREATE OR REPLACE VIEW v_latencia_por_tier AS
SELECT
    data_source                                                       AS tier,
    COUNT(*)                                                          AS consultas,
    ROUND(AVG(latencia_ms), 1)                                        AS media_ms,
    ROUND((PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latencia_ms))::NUMERIC, 1) AS p50_ms,
    ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latencia_ms))::NUMERIC, 1) AS p95_ms,
    ROUND((PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latencia_ms))::NUMERIC, 1) AS p99_ms
FROM query_log
WHERE ocurrido_en > NOW() - INTERVAL '24 hours'
GROUP BY data_source;


-- Calidad: cuánto se rechaza y por qué
CREATE OR REPLACE VIEW v_calidad AS
SELECT
    motivo,
    COUNT(*) AS registros
FROM trips_cuarentena, UNNEST(motivos) AS motivo
GROUP BY motivo
ORDER BY registros DESC;
