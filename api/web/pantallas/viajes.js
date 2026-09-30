// Viajes: GET /trips por rango de event_time. Enseña qué tier sirve cada
// tramo (coverage), un histograma coloreado por tier y la tabla de viajes.

import { api } from "../api.js";
import { $, bloqueMeta, chipTier, error, esc, fmtFecha, fmtMs, fmtNum, fmtUsd, mensaje, NOMBRE_TIER } from "../comun.js";

export const titulo = "Viajes";

const DIA = 86400000;
const POR_PAGINA = 50;
const PAGOS = { 1: "Tarjeta", 2: "Efectivo", 3: "Sin cargo", 4: "Disputa", 5: "Desconocido", 6: "Anulado" };
const dinero = (v) => (v == null ? "—" : fmtUsd(v));

// [clave, título, formato, numérica]
const COLUMNAS = [
  ["_tier", "Tier", chipTier],
  ["event_time", "event_time (UTC)", fmtFecha],
  ["tpep_pickup_datetime", "Recogida", fmtFecha],
  ["tpep_dropoff_datetime", "Llegada", fmtFecha],
  ["passenger_count", "Pasajeros", (v) => v ?? "—", true],
  ["trip_distance", "Distancia (mi)", (v) => (v == null ? "—" : fmtNum(v, 2)), true],
  ["pu_location_id", "Zona origen", (v) => v ?? "—", true],
  ["do_location_id", "Zona destino", (v) => v ?? "—", true],
  ["payment_type", "Pago", (v) => esc(PAGOS[v] ?? v ?? "—")],
  ["fare_amount", "Tarifa", dinero, true],
  ["tip_amount", "Propina", dinero, true],
  ["total_amount", "Total", dinero, true],
  ["origen", "Origen", (v) => esc(v ?? "—")],
];

// datetime-local <-> Date, leyendo siempre el valor como UTC
const aInput = (d) => d.toISOString().slice(0, 16);
const deInput = (v) => new Date(v + ":00Z");
const inicioDia = (d) => new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));

function rangosRapidos() {
  const ahora = new Date();
  const hoy = inicioDia(ahora);
  const minuto = new Date(Math.ceil(ahora / 60000) * 60000);
  return [
    ["Última hora", new Date(minuto - 3600000), minuto],
    ["Últimos 7 días", new Date(hoy - 6 * DIA), new Date(+hoy + DIA)],
    ["Hace 45 días", new Date(hoy - 45 * DIA), new Date(hoy - 43 * DIA)],
    ["Cruzando la frontera", new Date(hoy - 34 * DIA), new Date(hoy - 26 * DIA)],
    ["Todo 2026", new Date(Date.UTC(ahora.getUTCFullYear(), 0, 1)), new Date(+hoy + DIA)],
  ];
}

export function montar(vista, params) {
  vista.innerHTML = `
    <h1>Viajes</h1>
    <p class="sub">Pide a <code>GET /trips</code> los viajes de un rango de <code>event_time</code>. El router de la API decide qué días lee de PostgreSQL y cuáles de Iceberg. Todas las horas en <b>UTC</b>.</p>
    <section class="panel">
      <form class="fila" id="form">
        <label>Desde (incluido)<input type="datetime-local" id="desde" step="60" required></label>
        <label>Hasta (excluido)<input type="datetime-local" id="hasta" step="60" required></label>
        <label>Límite de filas<input type="number" id="limite" min="1" max="100000" value="1000"></label>
        <button type="submit" id="consultar">Consultar</button>
      </form>
      <div class="chips" id="rapidos" style="margin-top:12px"><span class="muted" style="font-size:12px">Rangos rápidos:</span></div>
    </section>
    <div id="mensaje"></div>
    <div id="resultado" class="oculto">
      <div class="cards">
        <div class="card"><div class="k">Origen de los datos</div><div class="v" id="c-source"></div></div>
        <div class="card"><div class="k">Viajes devueltos</div><div class="v" id="c-rows"></div></div>
        <div class="card"><div class="k">Latencia</div><div class="v" id="c-lat"></div></div>
        <div class="card"><div class="k">Importe total</div><div class="v" id="c-total"></div></div>
        <div class="card"><div class="k">Distancia media</div><div class="v" id="c-dist"></div></div>
      </div>
      <section class="panel">
        <div class="panel-cab"><h2>¿De qué tier sale cada tramo?</h2><p>El <code>coverage</code> de la respuesta</p></div>
        <div class="leyenda"><span><i style="background:var(--cold)"></i>Frío · Iceberg en MinIO</span><span><i style="background:var(--hot)"></i>Caliente · PostgreSQL</span></div>
        <div class="bar-tramos" id="barra"></div>
        <div class="eje"><span id="ax-ini"></span><span id="ax-fin"></span></div>
        <div id="tramos" style="margin-top:10px;font-size:13px"></div>
      </section>
      <section class="panel">
        <div class="panel-cab"><h2>Viajes en el tiempo</h2><p>Viajes devueltos por intervalo, según su tier</p></div>
        <div class="leyenda"><span><i style="background:var(--cold)"></i>Frío</span><span><i style="background:var(--hot)"></i>Caliente</span></div>
        <svg class="hist" id="hist" role="img" aria-label="Viajes por intervalo de tiempo, coloreados por tier"></svg>
        <div class="eje"><span id="hx-ini"></span><span id="hx-fin"></span></div>
      </section>
      <section class="panel">
        <div class="panel-cab"><h2>Viajes</h2><p>Pulsa una cabecera para ordenar</p></div>
        <div class="tabla-wrap"><table><thead><tr id="cabecera"></tr></thead><tbody id="cuerpo"></tbody></table></div>
        <div class="pager"><span id="pag-info"></span><button class="sec" id="pag-ant" type="button">‹ Anterior</button><button class="sec" id="pag-sig" type="button">Siguiente ›</button></div>
        <div id="meta"></div>
      </section>
    </div>`;

  const estado = { filas: [], pagina: 0, orden: { clave: "event_time", asc: true } };
  const q = (id) => $(`#${id}`, vista);

  for (const [nombre, d, h] of rangosRapidos()) {
    const b = document.createElement("button");
    b.type = "button"; b.className = "chip-btn"; b.textContent = nombre;
    b.onclick = () => { q("desde").value = aInput(d); q("hasta").value = aInput(h); consultar(); };
    q("rapidos").appendChild(b);
  }

  async function consultar(ev) {
    ev?.preventDefault();
    const desde = deInput(q("desde").value), hasta = deInput(q("hasta").value);
    const limite = Number(q("limite").value || 1000);
    // El rango queda en la URL para poder compartirlo
    history.replaceState(null, "", "#/viajes?" + new URLSearchParams({ desde: q("desde").value, hasta: q("hasta").value, limite }));
    q("consultar").disabled = true; q("consultar").textContent = "Consultando…";
    q("mensaje").innerHTML = "";
    try {
      const r = await api.trips(desde.toISOString(), hasta.toISOString(), limite);
      pintar(r, desde, hasta, limite);
    } catch (e) {
      q("resultado").classList.add("oculto");
      q("mensaje").innerHTML = error(e);
    } finally {
      q("consultar").disabled = false; q("consultar").textContent = "Consultar";
    }
  }

  function tierDe(momento, coverage) {
    for (const t of coverage) if (momento >= new Date(t.desde) && momento < new Date(t.hasta)) return t.tier;
    return coverage.at(-1)?.tier ?? "hot";
  }

  function pintar(respuesta, desde, hasta, limite) {
    const { data, meta } = respuesta;
    estado.filas = data.map((f) => ({ ...f, _tier: tierDe(new Date(f.event_time), meta.coverage) }));
    estado.pagina = 0;
    const filas = estado.filas;

    q("c-source").innerHTML = `<span style="font-size:16px">${chipTier(meta.data_source)}</span>`;
    q("c-rows").textContent = fmtNum(meta.rows);
    q("c-lat").textContent = fmtMs(meta.latency_ms);
    q("c-total").textContent = fmtUsd(filas.reduce((s, f) => s + (f.total_amount || 0), 0));
    q("c-dist").textContent = `${fmtNum(filas.length ? filas.reduce((s, f) => s + (f.trip_distance || 0), 0) / filas.length : 0, 2)} mi`;

    if (meta.rows >= limite) {
      q("mensaje").innerHTML = mensaje("warn", `Se ha llegado al límite de ${fmtNum(limite)} filas: hay más viajes en el rango de los que se muestran. Sube el límite o acota el rango.`);
    } else if (!meta.rows) {
      q("mensaje").innerHTML = mensaje("warn", "No hay viajes en ese rango. Si esperabas datos recientes, comprueba que el flujo en vivo (perfil <code>stream</code>) está encendido.");
    }

    q("resultado").classList.remove("oculto");  // antes de medir el SVG
    pintarTramos(meta.coverage, desde, hasta);
    pintarHistograma(desde, hasta);
    pintarTabla();
    q("meta").innerHTML = bloqueMeta(`/trips?desde=${desde.toISOString()}&hasta=${hasta.toISOString()}&limite=${limite}`, respuesta);
  }

  function pintarTramos(coverage, desde, hasta) {
    const span = hasta - desde;
    const cuenta = (t) => estado.filas.filter((f) => { const m = new Date(f.event_time); return m >= new Date(t.desde) && m < new Date(t.hasta); }).length;
    q("barra").innerHTML = coverage.map((t) => {
      const ini = Math.max(new Date(t.desde), desde), fin = Math.min(new Date(t.hasta), hasta);
      const pct = Math.max(0, (fin - ini) / span * 100);
      return `<div style="width:${pct}%;background:var(--${t.tier})" data-tip="${NOMBRE_TIER[t.tier]}: ${fmtFecha(t.desde)} → ${fmtFecha(t.hasta)} · ${fmtNum(cuenta(t))} viajes">${pct > 12 ? NOMBRE_TIER[t.tier] : ""}</div>`;
    }).join("");
    q("ax-ini").textContent = fmtFecha(desde.toISOString());
    q("ax-fin").textContent = fmtFecha(hasta.toISOString());
    q("tramos").innerHTML = coverage.length
      ? coverage.map((t) => `<div style="padding:2px 0">${chipTier(t.tier)} <span class="num">${fmtFecha(t.desde)} → ${fmtFecha(t.hasta)}</span> · ${fmtNum(cuenta(t))} viajes</div>`).join("")
      : '<span class="muted">Sin tramos: el rango no toca ningún tier.</span>';
  }

  function pintarHistograma(desde, hasta) {
    const svg = q("hist");
    const ancho = svg.clientWidth || 800, alto = 160, n = 60;
    const paso = (hasta - desde) / n;
    const cubos = Array.from({ length: n }, () => ({ hot: 0, cold: 0 }));
    for (const f of estado.filas) {
      const i = Math.min(n - 1, Math.floor((new Date(f.event_time) - desde) / paso));
      if (i >= 0) cubos[i][f._tier]++;
    }
    const max = Math.max(1, ...cubos.map((c) => c.hot + c.cold));
    const w = ancho / n;
    svg.setAttribute("viewBox", `0 0 ${ancho} ${alto}`);
    svg.innerHTML = `<line x1="0" x2="${ancho}" y1="${alto - 0.5}" y2="${alto - 0.5}" stroke="var(--grid)"/>` + cubos.map((c, i) => {
      const hc = (c.cold / max) * (alto - 8), hh = (c.hot / max) * (alto - 8);
      const x = i * w + 1, bw = Math.max(1, w - 2);
      const ini = new Date(+desde + i * paso), fin = new Date(+desde + (i + 1) * paso);
      const partes = [c.cold && `${fmtNum(c.cold)} frío`, c.hot && `${fmtNum(c.hot)} caliente`].filter(Boolean).join(" · ") || "sin viajes";
      const tip = `${fmtFecha(ini.toISOString())} → ${fmtFecha(fin.toISOString())}: ${partes}`;
      // Frío abajo, caliente encima, con 2 px de separación entre los dos
      const gap = c.cold && c.hot ? 2 : 0;
      return `<g data-tip="${tip}">` +
        `<rect x="${i * w}" y="0" width="${w}" height="${alto}" fill="transparent"/>` +
        (c.cold ? `<rect x="${x}" y="${alto - hc}" width="${bw}" height="${hc}" fill="var(--cold)" rx="2"/>` : "") +
        (c.hot ? `<rect x="${x}" y="${alto - hc - hh - gap}" width="${bw}" height="${Math.max(0, hh - 0)}" fill="var(--hot)" rx="2"/>` : "") +
        `</g>`;
    }).join("");
    q("hx-ini").textContent = fmtFecha(desde.toISOString());
    q("hx-fin").textContent = fmtFecha(hasta.toISOString());
  }

  function pintarTabla() {
    const { orden } = estado;
    q("cabecera").innerHTML = COLUMNAS.map(([clave, tit, , num]) =>
      `<th class="ord ${num ? "n" : ""}" data-clave="${clave}">${tit}${orden.clave === clave ? (orden.asc ? " ▲" : " ▼") : ""}</th>`).join("");
    for (const th of q("cabecera").children) {
      th.onclick = () => {
        const c = th.dataset.clave;
        estado.orden = { clave: c, asc: orden.clave === c ? !orden.asc : true };
        estado.pagina = 0;
        pintarTabla();
      };
    }
    const ordenadas = [...estado.filas].sort((a, b) => {
      const x = a[orden.clave], y = b[orden.clave];
      if (x == null) return 1;
      if (y == null) return -1;
      return (x < y ? -1 : x > y ? 1 : 0) * (orden.asc ? 1 : -1);
    });
    const paginas = Math.max(1, Math.ceil(ordenadas.length / POR_PAGINA));
    const trozo = ordenadas.slice(estado.pagina * POR_PAGINA, (estado.pagina + 1) * POR_PAGINA);
    q("cuerpo").innerHTML = trozo.length
      ? trozo.map((f) => "<tr>" + COLUMNAS.map(([clave, , fmt, num]) => `<td class="${num ? "n" : ""}">${fmt(f[clave])}</td>`).join("") + "</tr>").join("")
      : `<tr><td class="vacio" colspan="${COLUMNAS.length}">Sin viajes en este rango</td></tr>`;
    q("pag-info").textContent = estado.filas.length
      ? `${fmtNum(estado.pagina * POR_PAGINA + 1)}–${fmtNum(estado.pagina * POR_PAGINA + trozo.length)} de ${fmtNum(estado.filas.length)} · página ${estado.pagina + 1} de ${paginas}`
      : "";
    q("pag-ant").disabled = estado.pagina === 0;
    q("pag-sig").disabled = estado.pagina >= paginas - 1;
  }

  q("pag-ant").onclick = () => { estado.pagina--; pintarTabla(); };
  q("pag-sig").onclick = () => { estado.pagina++; pintarTabla(); };
  q("form").addEventListener("submit", consultar);

  // Arranque: el rango de la URL o, si no hay, uno que cruza la frontera
  const [, d0, h0] = rangosRapidos()[3];
  q("desde").value = params.get("desde") || aInput(d0);
  q("hasta").value = params.get("hasta") || aInput(h0);
  if (params.get("limite")) q("limite").value = params.get("limite");
  consultar();
}
