// Utilidades compartidas por las pantallas: formato, chips, el bloque
// «meta» de cada respuesta, barras horizontales y el tooltip.

export const $ = (sel, raiz = document) => raiz.querySelector(sel);
export const $$ = (sel, raiz = document) => [...raiz.querySelectorAll(sel)];

export function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

export const fmtNum = (v, dec = 0) =>
  v == null ? "—" : new Intl.NumberFormat("es-ES", { maximumFractionDigits: dec, minimumFractionDigits: dec }).format(v);
export const fmtUsd = (v) =>
  v == null ? "—" : new Intl.NumberFormat("es-ES", { style: "currency", currency: "USD" }).format(v);
export const fmtFecha = (iso) => (iso ? String(iso).slice(0, 19).replace("T", " ") : "—");
export function fmtBytes(b) {
  if (b == null) return "—";
  const u = ["B", "KB", "MB", "GB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return `${fmtNum(b, i ? 1 : 0)} ${u[i]}`;
}
export function fmtMs(ms) {
  if (ms == null) return "—";
  return ms >= 1000 ? `${fmtNum(ms / 1000, 2)} s` : `${fmtNum(ms)} ms`;
}

export const NOMBRE_TIER = { hot: "caliente", cold: "frío", mixto: "mixto" };
export const MOTOR_TIER = { hot: "PostgreSQL", cold: "Iceberg en MinIO", mixto: "los dos tiers" };
export const chipTier = (t) => `<span class="tier ${esc(t)}">${esc(NOMBRE_TIER[t] ?? t)}</span>`;

// Estado con icono + texto: el color nunca va solo
export function chipEstado(tipo, texto) {
  const icono = { ok: "✓", aviso: "!", mal: "✕" }[tipo];
  return `<span class="estado ${tipo}"><b aria-hidden="true">${icono}</b>${esc(texto)}</span>`;
}

// El pie de cada bloque: ruta llamada, tier, latencia, as_of y el JSON entero
export function bloqueMeta(ruta, respuesta, metodo = "GET") {
  const m = respuesta?.meta;
  if (!m) return "";
  return `<details class="meta">
    <summary><span><code>${esc(metodo)} ${esc(ruta)}</code></span><span>data_source: ${chipTier(m.data_source)}</span><span>${fmtMs(m.latency_ms)}</span><span>${fmtNum(m.rows)} filas</span><span>as_of ${fmtFecha(m.as_of)} UTC</span></summary>
    <pre class="json">${esc(JSON.stringify(m, null, 2))}</pre>
  </details>`;
}

export function mensaje(tipo, html) {
  return `<div class="msg ${tipo}">${html}</div>`;
}

export function error(e) {
  return mensaje("err", esc(e.message));
}

/* Barras horizontales con etiqueta directa del valor.
   filas: [{etiqueta (html), valor, color (css), texto, tip}] */
export function barras(filas, { max } = {}) {
  const tope = max ?? Math.max(1, ...filas.map((f) => f.valor || 0));
  return `<div class="barras">${filas.map((f) => `
    <div class="et">${f.etiqueta}</div>
    <div class="pista" data-tip="${esc(f.tip ?? "")}"><div class="relleno" style="width:${Math.max(0, (f.valor || 0) / tope * 100)}%;background:${f.color}"></div></div>
    <div class="val">${esc(f.texto)}</div>`).join("")}</div>`;
}

// Un solo tooltip para toda la página: cualquier elemento con data-tip
const tip = document.createElement("div");
tip.className = "tip";
document.body.appendChild(tip);
document.addEventListener("mousemove", (ev) => {
  const el = ev.target.closest?.("[data-tip]");
  const texto = el?.getAttribute("data-tip");
  if (!texto) { tip.style.display = "none"; return; }
  tip.textContent = texto;
  tip.style.display = "block";
  const x = Math.min(ev.clientX + 14, window.innerWidth - tip.offsetWidth - 8);
  tip.style.left = `${x}px`;
  tip.style.top = `${ev.clientY + 14}px`;
});
