// Métricas: las cuatro vistas v_* de PostgreSQL servidas por
// GET /metrics/{coste,latencia,calidad,caliente}.

import { api } from "../api.js";
import { $, barras, bloqueMeta, chipEstado, chipTier, error, esc, fmtBytes, fmtMs, fmtNum } from "../comun.js";

export const titulo = "Métricas";

// Objetivos de latencia p95 (los mismos que colorea Grafana)
const OBJETIVO_P95 = { hot: 500, cold: 10000 };

export function montar(vista) {
  vista.innerHTML = `
    <div class="panel-cab" style="margin-bottom:4px"><h1>Métricas</h1><button type="button" class="chip-btn der" id="recargar">Recargar</button></div>
    <p class="sub">Lo que mide el sistema sobre sí mismo. Cada bloque es una llamada a <code>GET /metrics/…</code>, que lee una vista de PostgreSQL.</p>
    <div class="rejilla">
      <section class="panel" id="coste"></section>
      <section class="panel" id="latencia"></section>
      <section class="panel" id="calidad"></section>
      <section class="panel" id="caliente"></section>
    </div>`;
  const q = (id) => $(`#${id}`, vista);
  const cargar = () => {
    for (const id of ["coste", "latencia", "calidad", "caliente"]) q(id).innerHTML = '<div class="cargando">Cargando…</div>';
    bloque(q("coste"), "coste", pintarCoste);
    bloque(q("latencia"), "latencia", pintarLatencia);
    bloque(q("calidad"), "calidad", pintarCalidad);
    bloque(q("caliente"), "caliente", pintarCaliente);
  };
  q("recargar").onclick = cargar;
  cargar();
}

async function bloque(el, nombre, pintar) {
  try {
    const r = await api.metrica(nombre);
    el.innerHTML = pintar(r.data) + bloqueMeta(`/metrics/${nombre}`, r);
  } catch (e) {
    el.innerHTML = error(e);
  }
}

function pintarCoste(filas) {
  const hot = filas.find((f) => f.tier === "hot"), cold = filas.find((f) => f.tier === "cold");
  const factor = hot && cold && cold.bytes_por_fila ? hot.bytes_por_fila / cold.bytes_por_fila : null;
  return `
    <div class="panel-cab"><h2>Coste por tier</h2><p>Bytes que ocupa un viaje en cada motor</p></div>
    ${factor ? `<p style="margin:0 0 12px">Iceberg guarda cada viaje en <b>${fmtNum(factor, 1)} veces menos</b> espacio que PostgreSQL.</p>` : ""}
    ${barras(filas.map((f) => ({
      etiqueta: `${chipTier(f.tier)} ${esc(f.motor)}`, valor: f.bytes_por_fila, color: `var(--${f.tier})`,
      texto: `${fmtNum(f.bytes_por_fila, 0)} B/fila`, tip: `${fmtNum(f.filas)} filas · ${fmtBytes(f.bytes)} en total`,
    })))}
    <div class="tabla-wrap" style="margin-top:12px"><table><thead><tr><th>Tier</th><th class="n">Filas</th><th class="n">Tamaño</th><th class="n">B/fila</th></tr></thead><tbody>
      ${filas.map((f) => `<tr><td>${chipTier(f.tier)}</td><td class="n">${fmtNum(f.filas)}</td><td class="n">${fmtBytes(f.bytes)}</td><td class="n">${fmtNum(f.bytes_por_fila, 1)}</td></tr>`).join("")}
    </tbody></table></div>`;
}

function pintarLatencia(filas) {
  if (!filas.length) return `<div class="panel-cab"><h2>Latencia por tier</h2></div><p class="muted">Aún no hay consultas. Haz alguna en <a href="#/viajes">Viajes</a>.</p>`;
  const orden = ["hot", "cold", "mixto"];
  filas = [...filas].sort((a, b) => orden.indexOf(a.tier) - orden.indexOf(b.tier));
  return `
    <div class="panel-cab"><h2>Latencia por tier</h2><p>p95 de las consultas a la API (tabla <code>query_log</code>)</p></div>
    ${barras(filas.map((f) => ({
      etiqueta: chipTier(f.tier), valor: f.p95_ms, color: `var(--${f.tier})`, texto: `p95 ${fmtMs(f.p95_ms)}`,
      tip: `${fmtNum(f.consultas)} consultas · media ${fmtMs(f.media_ms)} · p50 ${fmtMs(f.p50_ms)} · p99 ${fmtMs(f.p99_ms)}`,
    })))}
    <div class="tabla-wrap" style="margin-top:12px"><table><thead><tr><th>Tier</th><th class="n">Consultas</th><th class="n">p50</th><th class="n">p95</th><th class="n">p99</th><th>Objetivo p95</th></tr></thead><tbody>
      ${filas.map((f) => {
        const obj = OBJETIVO_P95[f.tier];
        const estado = obj == null ? '<span class="muted">—</span>' : chipEstado(f.p95_ms <= obj ? "ok" : "mal", `< ${fmtMs(obj)}`);
        return `<tr><td>${chipTier(f.tier)}</td><td class="n">${fmtNum(f.consultas)}</td><td class="n">${fmtMs(f.p50_ms)}</td><td class="n">${fmtMs(f.p95_ms)}</td><td class="n">${fmtMs(f.p99_ms)}</td><td>${estado}</td></tr>`;
      }).join("")}
    </tbody></table></div>`;
}

function pintarCalidad(filas) {
  const total = filas.reduce((t, f) => t + (f.registros || 0), 0);
  return `
    <div class="panel-cab"><h2>Calidad de los datos</h2><p>Registros rechazados por el contrato, por motivo</p></div>
    <p style="margin:0 0 12px"><b>${fmtNum(total)}</b> registros en cuarentena (<code>trips_cuarentena</code>). No entran en ningún tier.</p>
    ${filas.length ? barras(filas.map((f) => ({
      etiqueta: `<code>${esc(f.motivo)}</code>`, valor: f.registros, color: "var(--muted)",
      texto: fmtNum(f.registros), tip: `${fmtNum(f.registros / total * 100, 1)} % de la cuarentena`,
    }))) : '<p class="muted">Nada en cuarentena.</p>'}`;
}

function pintarCaliente(filas) {
  const c = filas[0];
  if (!c) return '<div class="panel-cab"><h2>Tier caliente</h2></div><p class="muted">Sin datos.</p>';
  const dato = (k, v, d = "") => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div>${d ? `<div class="d">${d}</div>` : ""}</div>`;
  return `
    <div class="panel-cab"><h2>Tier caliente</h2><p>Estado de PostgreSQL</p></div>
    <div class="cards" style="grid-template-columns:repeat(auto-fit,minmax(130px,1fr));margin-bottom:0">
      ${dato("Viajes", fmtNum(c.filas))}
      ${dato("Particiones diarias", fmtNum(c.particiones), "incluye las creadas por adelantado")}
      ${dato("Datos", fmtBytes(c.bytes))}
      ${dato("Índices", fmtBytes(c.bytes_indices))}
      ${dato("Día más antiguo", esc(c.dia_mas_antiguo ?? "—"), `${fmtNum(c.edad_maxima_dias)} días`)}
      ${dato("B por fila", fmtNum(c.bytes_por_fila, 0))}
    </div>`;
}
