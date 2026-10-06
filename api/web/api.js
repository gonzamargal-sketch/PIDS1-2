// Cliente de la API. Todas las pantallas llaman por aquí: una función por
// ruta, y cada llamada queda apuntada en `registro` (lo enseña la pestaña
// «Explorador» para ver qué pide el frontend y qué le contesta la API).

export const registro = [];
const oyentes = new Set();

export function alRegistrar(fn) {
  oyentes.add(fn);
  return () => oyentes.delete(fn);
}

export class ErrorApi extends Error {
  constructor(status, mensaje, cuerpo) {
    super(mensaje);
    this.status = status;
    this.cuerpo = cuerpo;
  }
}

// Convierte el `detail` de FastAPI en una frase: en los 422 es una lista
// de errores de validación, en el resto un texto.
function detalle(cuerpo) {
  const d = cuerpo?.detail;
  if (Array.isArray(d)) {
    return d.map((e) => `${(e.loc || []).filter((x) => x !== "body").join(".")}: ${e.msg}`).join(" · ");
  }
  return typeof d === "string" ? d : JSON.stringify(cuerpo);
}

export async function llamar(metodo, ruta, cuerpo, { apuntar = true } = {}) {
  const t0 = performance.now();
  const apunte = { metodo, ruta, cuerpo, cuando: new Date(), status: 0, ms: 0, respuesta: null };
  try {
    const r = await fetch(ruta, {
      method: metodo,
      headers: cuerpo ? { "content-type": "application/json" } : {},
      body: cuerpo ? JSON.stringify(cuerpo) : undefined,
    });
    apunte.status = r.status;
    apunte.respuesta = await r.json().catch(() => null);
    if (!r.ok) throw new ErrorApi(r.status, `Error ${r.status}: ${detalle(apunte.respuesta)}`, apunte.respuesta);
    return apunte.respuesta;
  } catch (e) {
    if (e instanceof ErrorApi) throw e;
    throw new ErrorApi(0, "La API no responde. ¿Está levantado el contenedor api? (docker compose ps)");
  } finally {
    apunte.ms = performance.now() - t0;
    if (apuntar) {
      registro.unshift(apunte);
      registro.length = Math.min(registro.length, 100);
      oyentes.forEach((fn) => fn(apunte));
    }
  }
}

const qs = (o) => new URLSearchParams(o).toString();

export const api = {
  health: (opciones) => llamar("GET", "/health", undefined, opciones),
  stats: () => llamar("GET", "/stats"),
  trips: (desde, hasta, limite) => llamar("GET", "/trips?" + qs({ desde, hasta, limite })),
  politica: () => llamar("GET", "/lifecycle/policy"),
  cambiarPolitica: (umbral_valor, umbral_unidad, accion = "ARCHIVE") =>
    llamar("PUT", "/lifecycle/policy?" + qs({ accion }), { umbral_valor, umbral_unidad }),
  estado: () => llamar("GET", "/lifecycle/status"),
  ingesta: (ventana, paso, opciones) => llamar("GET", "/ingesta?" + qs({ ventana, paso }), undefined, opciones),
  simulador: (opciones) => llamar("GET", "/simulador", undefined, opciones),
  cambiarSimulador: (activo) => llamar("PUT", "/simulador", { activo }),
  muestras: () => llamar("POST", "/ingesta/muestras", {}),
  metrica: (nombre) => llamar("GET", `/metrics/${encodeURIComponent(nombre)}`),
};
