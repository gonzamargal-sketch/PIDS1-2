"""
PIDS Parte 2 — Interruptor del simulador (botón de la pestaña En vivo).

El frontend no puede parar un contenedor (la API no tiene acceso a
Docker), así que el interruptor es una fila de PostgreSQL:
simulador_control.activo (postgres/init/08_p3.sql). El simulador la lee
una vez por segundo y, si está apagado, deja de emitir sin salir.

En la misma consulta deja un latido (hora, emitidos, ritmo y sumidero)
para que la API sepa si el simulador está levantado.

Solo lo usa el simulador sin fin (--total 0), que es el del perfil stream.
Una tanda con --total N la ha pedido alguien a mano y no debe quedarse
esperando a un botón.

Si PostgreSQL falla o la tabla no existe (base sin 08_p3.sql), el
simulador sigue emitiendo como siempre: el interruptor es una comodidad,
no puede tumbar el flujo en vivo.
"""

from __future__ import annotations

import logging
import time

import psycopg2

from common.config import PG

log = logging.getLogger("simulador.control")

CADA_S = 1.0
REINTENTO_S = 30.0


class Control:
    def __init__(self, eps: float, sumidero: str):
        self.eps = eps
        self.sumidero = sumidero
        self.conn = None
        self.activo = True
        self._ultima = 0.0
        self._fallo = 0.0
        # Arrancar el contenedor es querer que emita: se enciende
        self._consultar(emitidos=0, encender=True)

    def _conectar(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**PG.kwargs, connect_timeout=3)
            self.conn.autocommit = True
        return self.conn

    def _consultar(self, emitidos: int, encender: bool = False) -> None:
        if self._fallo and time.monotonic() - self._fallo < REINTENTO_S:
            return
        try:
            with self._conectar().cursor() as cur:
                cur.execute(
                    """UPDATE simulador_control
                          SET latido = NOW(), emitidos = %s, eps = %s, sumidero = %s,
                              activo = activo OR %s,
                              cambiado_en = CASE WHEN %s AND NOT activo THEN NOW() ELSE cambiado_en END
                        WHERE id = 1
                    RETURNING activo""",
                    (emitidos, self.eps, self.sumidero, encender, encender),
                )
                fila = cur.fetchone()
            nuevo = True if fila is None else fila[0]
            if nuevo != self.activo:
                log.info("Interruptor: %s", "ENCENDIDO" if nuevo else "APAGADO (no se emite)")
            self.activo = nuevo
            self._fallo = 0.0
        except Exception as e:  # noqa: BLE001 — cualquier fallo: seguir emitiendo
            if not self._fallo:
                log.warning("Sin interruptor (%s): se sigue emitiendo. Reintento en %.0f s",
                            str(e).strip().splitlines()[0], REINTENTO_S)
            self._fallo = time.monotonic()
            self.activo = True
            if self.conn is not None:
                self.conn.close()
                self.conn = None

    def encendido(self, emitidos: int) -> bool:
        """¿Hay que emitir? Consulta como mucho una vez por segundo."""
        ahora = time.monotonic()
        if ahora - self._ultima >= CADA_S:
            self._ultima = ahora
            self._consultar(emitidos)
        return self.activo

    def cerrar(self) -> None:
        if self.conn is not None and not self.conn.closed:
            self.conn.close()
