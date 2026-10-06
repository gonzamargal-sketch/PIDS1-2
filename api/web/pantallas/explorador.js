// Explorador: todas las rutas de la API con un formulario, el curl
// equivalente y la respuesta tal cual. Abajo, el registro de todas las
// llamadas que ha hecho el frontend en esta sesión.

import { alRegistrar, llamar, registro } from "../api.js";
import { $, esc, fmtFecha, fmtMs } from "../comun.js";

export const titulo = "Explorador de la API";

const hoy = () => new Date().toISOString().slice(0, 10);
const haceDias = (n) => new Date(Date.now() - n * 86400000).toISOString().slice(0, 10);

const RUTAS = [
  { id: "health", metodo: "GET", ruta: "/health", desc: "¿Está viva la API?", campos: [] },
  { id: "stats", metodo: "GET", ruta: "/stats", desc: "Filas en caliente y estadísticas del frío (sin leer los datos de Iceberg, solo sus manifiestos)", campos: [] },
  {
    id: "trips", metodo: "GET", ruta: "/trips", desc: "Viajes en [desde, hasta) por event_time. El router decide el tier",
    campos: [
      { n: "desde", tipo: "text", v: () => haceDias(32) },
      { n: "hasta", tipo: "text", v: () => haceDias(28) },
      { n: "limite", tipo: "number", v: () => 10 },
    ],
    construir: (c) => ({ ruta: "/trips?" + new URLSearchParams(c) }),
  },
  {
    id: "resumen", metodo: "GET", ruta: "/trips/resumen", desc: "Totales de un periodo (viajes, ingresos, propinas…) sumados en los dos tiers. La usa el chatbot",
    campos: [
      { n: "desde", tipo: "text", v: () => haceDias(40) },
      { n: "hasta", tipo: "text", v: () => hoy() },
      { n: "zona", tipo: "text", v: () => "" },
      { n: "agrupar", tipo: "select", opciones: ["ninguno", "dia", "zona"], v: () => "ninguno" },
    ],
    construir: (c) => ({ ruta: "/trips/resumen?" + new URLSearchParams(Object.entries(c).filter(([, v]) => v !== "")) }),
  },
  { id: "policy-get", metodo: "GET", ruta: "/lifecycle/policy", desc: "Las políticas de retención vigentes", campos: [] },
  {
    id: "policy-put", metodo: "PUT", ruta: "/lifecycle/policy", desc: "Cambia el umbral de una política. Afecta al archivado real",
    campos: [
      { n: "umbral_valor", tipo: "number", v: () => 30 },
      { n: "umbral_unidad", tipo: "select", opciones: ["minutes", "hours", "days", "years"], v: () => "days" },
      { n: "accion", tipo: "select", opciones: ["ARCHIVE", "DELETE"], v: () => "ARCHIVE" },
    ],
    construir: (c) => ({
      ruta: "/lifecycle/policy?" + new URLSearchParams({ accion: c.accion }),
      cuerpo: { umbral_valor: Number(c.umbral_valor), umbral_unidad: c.umbral_unidad },
    }),
    confirmar: true,
  },
  { id: "status", metodo: "GET", ruta: "/lifecycle/status", desc: "Jobs de archivado, candidatas y cumplimiento", campos: [] },
  {
    id: "ingesta", metodo: "GET", ruta: "/ingesta", desc: "Lo que llega ahora del flujo en vivo: llegadas por intervalo, ritmo, retraso y lo metido a mano. La usa la pestaña En vivo",
    campos: [{ n: "ventana", tipo: "number", v: () => 300 }, { n: "paso", tipo: "number", v: () => 5 }],
    construir: (c) => ({ ruta: "/ingesta?" + new URLSearchParams(c) }),
  },
  { id: "muestras", metodo: "POST", ruta: "/ingesta/muestras", desc: "Mete 8 viajes de ejemplo (uno por cada salida del contrato) y dice dónde acaba cada uno", campos: [], construir: () => ({ ruta: "/ingesta/muestras", cuerpo: {} }) },
  { id: "sim-get", metodo: "GET", ruta: "/simulador", desc: "Si el simulador está encendido y si está levantado (latido de menos de 5 s)", campos: [] },
  {
    id: "sim-put", metodo: "PUT", ruta: "/simulador", desc: "Enciende o apaga el simulador sin parar el contenedor",
    campos: [{ n: "activo", tipo: "select", opciones: ["true", "false"], v: () => "true" }],
    construir: (c) => ({ ruta: "/simulador", cuerpo: { activo: c.activo === "true" } }),
  },
  {
    id: "metrics", metodo: "GET", ruta: "/metrics/{nombre}", desc: "Una vista de métricas de PostgreSQL",
    campos: [{ n: "nombre", tipo: "select", opciones: ["coste", "latencia", "calidad", "caliente", "noexiste"], v: () => "coste" }],
    construir: (c) => ({ ruta: `/metrics/${encodeURIComponent(c.nombre)}` }),
  },
];

// Errores provocados a propósito: la API tiene que rechazarlos bien
const ERRORES = [
  { t: "400 · fechas al revés", id: "trips", c: { desde: hoy(), hasta: haceDias(2), limite: 10 } },
  { t: "404 · métrica que no existe", id: "metrics", c: { nombre: "noexiste" } },
  { t: "422 · umbral 0 (no toca la base)", id: "policy-put", c: { umbral_valor: 0, umbral_unidad: "weeks", accion: "ARCHIVE" }, sinConfirmar: true },
];

function curl(metodo, ruta, cuerpo) {
  const url = `${location.origin}${ruta}`;
  if (!cuerpo) return `curl -s '${url}'`;
  return `curl -s -X ${metodo} '${url}' \\\n  -H 'content-type: application/json' \\\n  -d '${JSON.stringify(cuerpo)}'`;
}

function campo(r, c) {
  const id = `${r.id}-${c.n}`;
  const control = c.tipo === "select"
    ? `<select id="${id}">${c.opciones.map((o) => `<option ${o === c.v() ? "selected" : ""}>${o}</option>`).join("")}</select>`
    : `<input id="${id}" type="${c.tipo}" value="${esc(c.v())}">`;
  return `<label>${c.n}${control}</label>`;
}

export function montar(vista) {
  vista.innerHTML = `
    <h1>Explorador de la API</h1>
    <p class="sub">Cada ruta con sus parámetros: pulsa <b>Enviar</b> y verás el código de respuesta, el tiempo, el <code>curl</code> equivalente y el JSON que devuelve. El Swagger completo está en <a href="/docs" target="_blank">/docs</a>.</p>
    <section class="panel">
      <div class="panel-cab"><h2>Errores a propósito</h2><p>La API tiene que rechazarlos con el código correcto</p></div>
      <div class="chips" id="errores"></div>
    </section>
    <div id="rutas"></div>
    <section class="panel">
      <div class="panel-cab"><h2>Registro de llamadas</h2><p>Todo lo que ha pedido el frontend en esta sesión, en cualquier pantalla</p></div>
      <div class="tabla-wrap" id="registro"></div>
    </section>`;

  $("#rutas", vista).innerHTML = RUTAS.map((r) => `
    <details class="endpoint" id="ep-${r.id}">
      <summary><span class="metodo ${r.metodo}">${r.metodo}</span><span class="ruta">${esc(r.ruta)}</span><span class="desc">${esc(r.desc)}</span></summary>
      <div class="cuerpo">
        <form class="fila">${r.campos.map((c) => campo(r, c)).join("")}<button type="submit">Enviar</button></form>
        <div class="salida"></div>
      </div>
    </details>`).join("");

  for (const r of RUTAS) {
    const ep = $(`#ep-${r.id}`, vista);
    $("form", ep).addEventListener("submit", (ev) => { ev.preventDefault(); enviar(r, ep); });
  }

  $("#errores", vista).innerHTML = ERRORES.map((e, i) => `<button type="button" class="chip-btn" data-i="${i}">${esc(e.t)}</button>`).join("");
  vista.querySelectorAll("#errores [data-i]").forEach((b) => (b.onclick = () => {
    const e = ERRORES[b.dataset.i];
    const r = RUTAS.find((x) => x.id === e.id);
    const ep = $(`#ep-${r.id}`, vista);
    for (const [k, v] of Object.entries(e.c)) {
      const el = $(`#${r.id}-${k}`, ep);
      if (el.tagName === "SELECT" && ![...el.options].some((o) => o.value === String(v))) el.add(new Option(v));
      el.value = v;
    }
    ep.open = true;
    ep.scrollIntoView({ behavior: "smooth", block: "start" });
    enviar(r, ep, e.sinConfirmar);
  }));

  const pintarRegistro = () => {
    $("#registro", vista).innerHTML = registro.length
      ? `<table class="registro"><thead><tr><th>Hora (UTC)</th><th>Método</th><th>Ruta</th><th class="n">Código</th><th class="n">Tiempo</th></tr></thead><tbody>` +
        registro.map((a) => `<tr><td>${fmtFecha(a.cuando.toISOString()).slice(11)}</td><td><span class="metodo ${a.metodo}">${a.metodo}</span></td><td><code>${esc(decodeURIComponent(a.ruta))}</code></td><td class="n"><span class="status s${String(a.status)[0]}">${a.status || "sin respuesta"}</span></td><td class="n">${fmtMs(a.ms)}</td></tr>`).join("") +
        "</tbody></table>"
      : '<p class="muted" style="margin:0">Todavía no hay llamadas.</p>';
  };
  pintarRegistro();
  return alRegistrar(pintarRegistro);
}

async function enviar(r, ep, sinConfirmar = false) {
  const valores = Object.fromEntries(r.campos.map((c) => [c.n, $(`#${r.id}-${c.n}`, ep).value]));
  const { ruta, cuerpo } = r.construir ? r.construir(valores) : { ruta: r.ruta };
  if (r.confirmar && !sinConfirmar && !confirm(`¿Enviar ${r.metodo} ${ruta}?\n\n${JSON.stringify(cuerpo)}\n\nCambia la política real del sistema.`)) return;
  const salida = $(".salida", ep);
  salida.innerHTML = '<div class="cargando">Enviando…</div>';
  const t0 = performance.now();
  let status, json;
  try {
    json = await llamar(r.metodo, ruta, cuerpo);
    status = registro[0]?.status ?? 200;
  } catch (e) {
    status = e.status;
    json = e.cuerpo ?? { error: e.message };
  }
  const ms = performance.now() - t0;
  const texto = curl(r.metodo, ruta, cuerpo);
  salida.innerHTML = `
    <div class="curl"><pre class="json">${esc(texto)}</pre><button type="button" class="sec copiar">Copiar</button></div>
    <p style="margin:12px 0 0">Respuesta: <span class="status s${String(status)[0]}">${status || "sin respuesta"}</span> · ${fmtMs(ms)} en el navegador</p>
    <pre class="json">${esc(JSON.stringify(json, null, 2))}</pre>`;
  $(".copiar", salida).onclick = async (ev) => {
    try { await navigator.clipboard.writeText(texto); ev.target.textContent = "Copiado"; }
    catch { ev.target.textContent = "Selecciónalo a mano"; }
  };
}
