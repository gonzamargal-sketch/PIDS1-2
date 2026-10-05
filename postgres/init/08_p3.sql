-- ============================================================
-- PIDS Parte 2 — Añadidos de P3 (pestaña «En vivo» del frontend)
--
-- Idempotente a propósito: se puede aplicar sobre una base ya creada
-- SIN docker compose down -v, así que nadie pierde sus datos:
--
--   docker compose exec -T postgres psql -U pids -d pids < postgres/init/08_p3.sql
--
-- En una base nueva lo ejecuta el entrypoint de Postgres como los demás.
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- LLEGADAS RECIENTES AL CALIENTE
-- GET /ingesta pregunta cada 2 s "qué ha llegado en los últimos minutos".
-- event_time no sirve para eso: es la hora del evento, y tras una caída
-- del consumidor llegan de golpe eventos de hace un rato. La hora de
-- llegada es ingested_at, y sin índice cada consulta recorrería todas las
-- particiones. Se crea en la tabla padre y Postgres lo propaga a cada
-- partición, también a las que cree mantener_particiones.
-- ════════════════════════════════════════════════════════════
CREATE INDEX IF NOT EXISTS idx_trips_ingested ON taxi_trips (ingested_at);


-- ════════════════════════════════════════════════════════════
-- LO METIDO A MANO
-- La pestaña fija aparte lo que mete una persona (scripts/anadir_viaje.py,
-- directo o por Kafka, y los mensajes que ni siquiera son JSON): con el
-- simulador a 200/s, en la lista de "últimos llegados" duraría un instante.
-- Índices parciales: solo contienen esas pocas filas, así que encontrarlas
-- no cuesta nada aunque el caliente tenga millones. Las consultas de
-- api/rutas_ingesta.py repiten estas mismas condiciones para que Postgres
-- pueda usarlos.
-- ════════════════════════════════════════════════════════════
CREATE INDEX IF NOT EXISTS idx_trips_a_mano ON taxi_trips (ingested_at)
    WHERE origen = 'manual' OR fichero_origen = 'anadir_viaje.py';

CREATE INDEX IF NOT EXISTS idx_cuarentena_a_mano ON trips_cuarentena (recibido_en)
    WHERE origen = 'manual'
       OR payload->>'fichero_origen' = 'anadir_viaje.py'
       OR motivos @> ARRAY['mensaje_ilegible'];
