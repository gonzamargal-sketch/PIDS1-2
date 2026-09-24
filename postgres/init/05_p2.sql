-- ============================================================
-- PIDS Parte 2 — Añadidos de P2 (ingesta)
--
-- Idempotente: se puede aplicar sobre una base ya creada SIN
-- docker compose down -v, así que nadie pierde sus datos:
--
--   docker compose exec -T postgres psql -U pids -d pids < postgres/init/05_p2.sql
--
-- En una base nueva lo ejecuta el entrypoint de Postgres como los demás.
-- ============================================================

-- Kafka entrega "al menos una vez": si el consumidor cae entre el commit
-- en PostgreSQL y la confirmación del offset, recibe el lote otra vez.
-- En taxi_trips la clave primaria (trip_id, event_time) ya evita el
-- duplicado, porque el trip_id lo pone el simulador. En cuarentena no
-- había nada que lo evitara: este índice cumple ese papel.
--
-- Es parcial porque los rechazos de la carga inicial y de la prueba de
-- humo no llevan trip_id en el payload, y esos no pasan por Kafka.
CREATE UNIQUE INDEX IF NOT EXISTS uq_cuarentena_trip_id
    ON trips_cuarentena ((payload->>'trip_id'))
    WHERE payload ? 'trip_id';
