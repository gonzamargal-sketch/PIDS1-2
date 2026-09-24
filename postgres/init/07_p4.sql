-- ============================================================
-- PIDS Parte 2 — Añadidos de P4 (orquestación y observabilidad)
--
-- Idempotente a propósito: se puede aplicar sobre una base ya creada
-- SIN docker compose down -v, así que nadie pierde sus datos:
--
--   docker compose exec -T postgres psql -U pids -d pids < postgres/init/07_p4.sql
--
-- En una base nueva lo ejecuta el entrypoint de Postgres como los demás.
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- FOTOS DEL TIER CALIENTE
-- cold_stats guarda historia del frío, pero del caliente solo hay vistas
-- que dicen cómo está AHORA. Para pintar "filas por tier en el tiempo"
-- (métrica 1 de §7) hace falta la misma serie en los dos lados: el DAG
-- estadisticas_frio deja aquí una foto en cada ejecución, justo al lado
-- de la de cold_stats.
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS hot_stats (
    id                  BIGSERIAL PRIMARY KEY,
    medido_en           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    particiones         INT,
    filas               BIGINT,
    bytes               BIGINT,
    bytes_indices       BIGINT,
    bytes_por_fila      NUMERIC(10,2)
                        GENERATED ALWAYS AS
                        (CASE WHEN filas > 0 THEN bytes::NUMERIC / filas END) STORED
);

CREATE INDEX IF NOT EXISTS idx_hot_stats_medido ON hot_stats (medido_en DESC);

COMMENT ON TABLE hot_stats IS
  'Serie temporal del tier caliente. La rellena el DAG estadisticas_frio.';


-- Serie de filas y bytes por tier, lista para un área apilada en Grafana
CREATE OR REPLACE VIEW v_historico_por_tier AS
SELECT medido_en, 'hot'::TEXT AS tier, filas, bytes
FROM hot_stats
UNION ALL
SELECT medido_en, 'cold'::TEXT AS tier, filas, bytes
FROM cold_stats
WHERE particion_mes IS NULL;
