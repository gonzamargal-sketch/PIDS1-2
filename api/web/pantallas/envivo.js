// En vivo: lo que está entrando ahora por Kafka (GET /ingesta, cada 2 s).
// Cuenta por hora de llegada, así que si se para el consumidor se ve el
// hueco, luego el pico al ponerse al día y el retraso subir y bajar.

import { api } from "../api.js";
import { $, bloqueMeta, chipEstado, error, esc, fmtFecha, fmtNum, fmtUsd, mensaje } from "../comun.js";

export const titulo = "En vivo";

const REFRESCO_MS = 2000;
// ventana (s) → paso (s): siempre 60 barras
const VENTANAS = [[60, 1, "1 min"], [300, 5, "5 min"], [900, 15, "15 min"]];

const hora = (iso) => (iso ? String(iso).slice(11, 19) : "—");
const fmtSeg = (s) => (s == null ? "—" : s < 60 ? `${fmtNum(s, 1)} s` : `${fmtNum(s / 60, 1)} min`);

export function montar(vista) {
  vista.innerHTML = `
    <div class="panel-cab" style="margin-bottom:4px"><h1>En vivo</h1>
      <label class="interruptor der"><input type="checkbox" id="auto" checked> Actualizar cada 2 s</label></div>
    <p class="sub">Viajes que <b>llegan ahora</b> del flujo en vivo (simulador → Kafka → consumidor) y de los que se meten a mano. Pasan por el contrato de datos y van al <b>caliente</b> o a <b>cuarentena</b>. Se cuentan por hora de llegada a PostgreSQL. Horas en UTC.</p>
    <div id="mensaje"></div>
    <div class="cards" id="cards"><div class="cargando">Cargando…</div></div>
    <section class="panel">
      <div class="panel-cab"><h2>Llegadas</h2><p id="sub-serie"></p>
        <div class="chips der">${VENTANAS.map(([v, p, t]) => `<button type="button" class="chip-btn" data-v="${v}" data-p="${p}">${t}</button>`).join("")}</div></div>
      <div class="leyenda"><span><i style="background:var(--hot)"></i>al caliente</span><span><i style="background:var(--muted)"></i>a cuarentena</span></div>
      <svg class="hist" id="barras" role="img" aria-label="Viajes llegados por intervalo"></svg>
      <div class="eje"><span id="ax-ini"></span><span id="ax-fin"></span></div>
      <div class="panel-cab" style="margin:16px 0 4px"><h2 style="font-size:15px">Retraso</h2><p>Segundos entre que ocurre el evento y llega a PostgreSQL. Si sube, el consumidor va atrasado (es el LAG de Kafka, en tiempo)</p></div>
      <svg class="hist" id="retraso" style="height:90px" role="img" aria-label="Retraso medio por intervalo"></svg>
    </section>
    <section class="panel"><div class="panel-cab"><h2>Metido a mano</h2><p>Lo que manda una persona en la última hora (<code>scripts/anadir_viaje.py</code>, directo o por Kafka, y los mensajes ilegibles). Se queda aquí aunque el simulador vaya a 200/s</p></div><div class="tabla-wrap" id="a-mano"></div></section>
    <div class="rejilla">
      <section class="panel"><div class="panel-cab"><h2>Últimos viajes llegados</h2><p>Tabla <code>taxi_trips</code></p></div><div class="tabla-wrap" id="viajes"></div></section>
      <section class="panel"><div class="panel-cab"><h2>Últimos rechazados</h2><p>Tabla <code>trips_cuarentena</code></p></div><div class="tabla-wrap" id="rechazos"></div></section>
    </div>
    <div id="meta"></div>`;

  const q = (id) => $(`#${id}`, vista);
  let [ventana, paso] = VENTANAS[1];
  let temporizador = null;
  let primera = true;

  async function cargar() {
    try {
      // Solo la primera llamada va al registro del Explorador: el refresco lo llenaría
      const r = await api.ingesta(ventana, paso, { apuntar: primera });
      primera = false;
      const d = r.data;
      q("mensaje").innerHTML = d.ultimo.hace_s == null
        ? mensaje("warn", "No ha llegado nada del flujo en vivo en el último día. ¿Está levantado el perfil <code>stream</code>? (<code>docker compose --profile core --profile stream up -d</code>)")
        : "";
      pintarCards(d);
      pintarBarras(d);
      pintarRetraso(d);
      pintarTablas(d);
      q("meta").innerHTML = bloqueMeta(`/ingesta?ventana=${ventana}&paso=${paso}`, r);
    } catch (e) {
      q("mensaje").innerHTML = error(e);
    }
  }

  function pintarCards(d) {
    const { ritmo, ultimo, totales } = d;
    const total = totales.caliente + totales.cuarentena;
    const pct = total ? (totales.cuarentena / total) * 100 : null;
    const hace = ultimo.hace_s;
    const estado = hace == null ? chipEstado("mal", "sin flujo")
      : hace <= 5 ? chipEstado("ok", "llegando")
      : hace <= 30 ? chipEstado("aviso", "más lento")
      : chipEstado("mal", "parado");
    const ret = ultimo.retraso_s;
    const estRet = ret == null ? "" : ret <= 5 ? chipEstado("ok", "al día") : chipEstado("aviso", "atrasado");
    const card = (k, v, det) => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div><div class="d">${det}</div></div>`;
    q("cards").innerHTML =
      card("Ritmo ahora", `${fmtNum(ritmo.caliente_por_s, 1)} /s`, `viajes al caliente, media de los últimos ${ritmo.segundos} s`) +
      card("Último viaje llegó hace", fmtSeg(hace), estado) +
      card("Retraso del último", fmtSeg(ret), estRet || "llegada − evento") +
      card(`En los últimos ${VENTANAS.find(([v]) => v === ventana)[2]}`, fmtNum(totales.caliente), "al caliente") +
      card("Rechazados", fmtNum(totales.cuarentena), pct == null ? "a cuarentena" : `a cuarentena · ${fmtNum(pct, 1)} % de lo llegado`);
  }

  function svgVacio(svg, alto) {
    const ancho = svg.clientWidth || 800;
    svg.setAttribute("viewBox", `0 0 ${ancho} ${alto}`);
    return ancho;
  }

  function pintarBarras(d) {
    const svg = q("barras"), alto = 160, serie = d.serie;
    const ancho = svgVacio(svg, alto);
    const max = Math.max(1, ...serie.map((c) => c.caliente + c.cuarentena));
    const w = ancho / serie.length;
    svg.innerHTML = `<line x1="0" x2="${ancho}" y1="${alto - 0.5}" y2="${alto - 0.5}" stroke="var(--grid)"/>` + serie.map((c, i) => {
      const hh = (c.caliente / max) * (alto - 8), hq = (c.cuarentena / max) * (alto - 8);
      const x = i * w + 1, bw = Math.max(1, w - 2), gap = c.caliente && c.cuarentena ? 2 : 0;
      const tip = `${hora(c.t)} UTC: ${fmtNum(c.caliente)} al caliente · ${fmtNum(c.cuarentena)} a cuarentena` +
        (c.retraso_s != null ? ` · retraso ${fmtSeg(c.retraso_s)}` : "");
      return `<g data-tip="${esc(tip)}"><rect x="${i * w}" y="0" width="${w}" height="${alto}" fill="transparent"/>` +
        (c.caliente ? `<rect x="${x}" y="${alto - hh}" width="${bw}" height="${hh}" fill="var(--hot)" rx="2"/>` : "") +
        (c.cuarentena ? `<rect x="${x}" y="${alto - hh - hq - gap}" width="${bw}" height="${hq}" fill="var(--muted)" rx="2"/>` : "") +
        `</g>`;
    }).join("");
    q("ax-ini").textContent = hora(serie[0]?.t);
    q("ax-fin").textContent = `${hora(serie.at(-1)?.t)} UTC`;
    q("sub-serie").textContent = `Viajes por intervalo de ${d.paso_s} s · pico ${fmtNum(max / d.paso_s, 0)} /s`;
  }

  function pintarRetraso(d) {
    const svg = q("retraso"), alto = 90, serie = d.serie;
    const ancho = svgVacio(svg, alto);
    const max = Math.max(5, ...serie.map((c) => c.retraso_s ?? 0));
    const w = ancho / serie.length;
    const y = (v) => alto - 6 - (Math.max(0, v) / max) * (alto - 14);
    // Una polilínea por tramo con datos: donde no llega nada no hay retraso que pintar
    const tramos = [];
    let actual = [];
    serie.forEach((c, i) => {
      if (c.retraso_s == null) { if (actual.length) tramos.push(actual); actual = []; return; }
      actual.push(`${(i + 0.5) * w},${y(c.retraso_s)}`);
    });
    if (actual.length) tramos.push(actual);
    svg.innerHTML =
      `<line x1="0" x2="${ancho}" y1="${alto - 0.5}" y2="${alto - 0.5}" stroke="var(--grid)"/>` +
      `<text x="4" y="12" font-size="11" fill="var(--muted)">${fmtSeg(max)}</text>` +
      tramos.map((t) => t.length === 1
        ? `<circle cx="${t[0].split(",")[0]}" cy="${t[0].split(",")[1]}" r="2.5" fill="var(--accent)"/>`
        : `<polyline points="${t.join(" ")}" fill="none" stroke="var(--accent)" stroke-width="2" stroke-linejoin="round"/>`).join("") +
      serie.map((c, i) => `<rect x="${i * w}" y="0" width="${w}" height="${alto}" fill="transparent" data-tip="${esc(`${hora(c.t)} UTC: ${c.retraso_s == null ? "sin llegadas" : `retraso medio ${fmtSeg(c.retraso_s)}`}`)}"/>`).join("");
  }

  function pintarTablas(d) {
    const codigos = (xs) => (xs || []).map((x) => `<code>${esc(x)}</code>`).join(" ");
    q("a-mano").innerHTML = d.a_mano.length
      ? `<table><thead><tr><th>Llegó</th><th>Acabó en</th><th>Motivos / avisos</th><th>Viaje</th></tr></thead><tbody>` +
        d.a_mano.map((m) => `<tr><td>${hora(m.llego)}</td>` +
          `<td>${m.destino === "caliente" ? chipEstado("ok", "caliente") : chipEstado("mal", "cuarentena")}</td>` +
          `<td>${codigos(m.motivos) || '<span class="muted">ninguno</span>'}</td>` +
          `<td>${m.destino === "caliente"
            ? `${esc(m.pu_location_id)} → ${esc(m.do_location_id)} · ${fmtNum(m.trip_distance, 1)} mi · ${fmtUsd(m.total_amount)}`
            : `<code class="muted" title="${esc(m.payload)}">${esc(m.payload.length > 60 ? m.payload.slice(0, 60) + "…" : m.payload)}</code>`}</td></tr>`).join("") +
        "</tbody></table>"
      : '<p class="muted" style="margin:0">Nada en la última hora. Probad <code>python scripts/anadir_viaje.py</code> (ver paso 7.1 y 7.2 de la guía).</p>';
    q("viajes").innerHTML = d.ultimos_viajes.length
      ? `<table><thead><tr><th>Llegó</th><th>Evento</th><th>Origen</th><th class="n">Zonas</th><th class="n">Millas</th><th class="n">Total</th><th>Avisos</th></tr></thead><tbody>` +
        d.ultimos_viajes.map((v) => `<tr><td>${hora(v.ingested_at)}</td><td>${hora(v.event_time)}</td>` +
          `<td>${esc(v.origen)}${v.fichero_origen === "anadir_viaje.py" ? ' <span class="muted">(a mano)</span>' : ""}</td>` +
          `<td class="n">${esc(v.pu_location_id)} → ${esc(v.do_location_id)}</td><td class="n">${fmtNum(v.trip_distance, 1)}</td>` +
          `<td class="n">${fmtUsd(v.total_amount)}</td><td>${(v.avisos || []).map((a) => `<code>${esc(a)}</code>`).join(" ")}</td></tr>`).join("") +
        "</tbody></table>"
      : '<p class="muted" style="margin:0">Todavía no ha llegado nada.</p>';
    q("rechazos").innerHTML = d.ultimos_rechazos.length
      ? `<table><thead><tr><th>Llegó</th><th>Origen</th><th>Motivos</th><th>Lo que llegó</th></tr></thead><tbody>` +
        d.ultimos_rechazos.map((r) => `<tr><td>${hora(r.recibido_en)}</td><td>${esc(r.origen)}</td>` +
          `<td>${(r.motivos || []).map((m) => `<code>${esc(m)}</code>`).join(" ")}</td>` +
          `<td><code class="muted" title="${esc(r.payload)}">${esc(r.payload.length > 48 ? r.payload.slice(0, 48) + "…" : r.payload)}</code></td></tr>`).join("") +
        "</tbody></table>"
      : '<p class="muted" style="margin:0">Nada rechazado en el último día.</p>';
  }

  function marcarVentana() {
    vista.querySelectorAll("[data-v]").forEach((b) => b.classList.toggle("activo", +b.dataset.v === ventana));
  }

  function programar() {
    clearInterval(temporizador);
    temporizador = q("auto").checked ? setInterval(cargar, REFRESCO_MS) : null;
  }

  vista.querySelectorAll("[data-v]").forEach((b) => (b.onclick = () => {
    ventana = +b.dataset.v; paso = +b.dataset.p;
    marcarVentana();
    cargar();
  }));
  q("auto").onchange = programar;

  marcarVentana();
  cargar();
  programar();
  return () => clearInterval(temporizador);
}
