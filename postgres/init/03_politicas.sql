-- ============================================================
-- PIDS Parte 2 — Políticas de retención iniciales (E8)
--
-- Esto es solo la siembra. La política vive en la tabla y se cambia
-- con un UPDATE, no editando este fichero ni redesplegando nada.
-- ============================================================

INSERT INTO retention_policy
    (dataset, tier_origen, tier_destino, umbral_valor, umbral_unidad, accion, descripcion)
VALUES
    ('trips',   'hot',   'cold', 30, 'days',    'ARCHIVE',
     'Los viajes salen de PostgreSQL a Iceberg a los 30 días de su event_time.'),

    ('trips',   'cold',  NULL,    7, 'years',   'DELETE',
     'Borrado definitivo del histórico pasados 7 años. Cierra el ciclo de vida.'),

    ('metrics', 'redis', NULL,   24, 'hours',   'EXPIRE',
     'Las métricas cacheadas expiran por TTL nativo de Redis.');

-- ── Particiones iniciales ───────────────────────────────────
-- De ayer a dentro de una semana, para que la primera inserción no caiga
-- en la partición por defecto. A partir de aquí lo mantiene un DAG.
SELECT crear_particiones_adelanto(7);


-- ════════════════════════════════════════════════════════════
-- CÓMO CAMBIAR LA POLÍTICA PARA LA DEMO
--
-- Con 30 días de retención no se archiva nada durante una grabación de
-- diez minutos. Para que el ciclo se dispare en directo:
--
--   UPDATE retention_policy
--      SET umbral_valor = 5, umbral_unidad = 'minutes'
--    WHERE dataset = 'trips' AND accion = 'ARCHIVE';
--
-- Y para volver al valor real:
--
--   UPDATE retention_policy
--      SET umbral_valor = 30, umbral_unidad = 'days'
--    WHERE dataset = 'trips' AND accion = 'ARCHIVE';
--
-- Lo mismo se puede hacer desde la API con PUT /lifecycle/policy, que es
-- lo que se graba en el vídeo.
-- ════════════════════════════════════════════════════════════
