// Inicio: el recorrido de un dato de punta a punta, con los números reales
// de cada tier, y accesos a lo que se puede probar.

import { api } from "../api.js";
import { barras, bloqueMeta, chipEstado, esc, error, fmtBytes, fmtFecha, fmtNum } from "../comun.js";

export const titulo = "Inicio";

const UNIDADES = { minutes: "minutos", hours: "horas", days: "días", years: "años" };
export const frasePolitica = (p) => (p ? `${p.umbral_valor} ${UNIDADES[p.umbral_unidad] ?? p.umbral_unidad}` : "—");

export function montar(vista) {
  vista.innerHTML = `
    <h1>Cómo viaja un dato</h1>
    <p class="sub">Cada viaje entra al tier <b>caliente</b> (PostgreSQL, rápido y caro), pasa al <b>frío</b> (Iceberg en MinIO, lento y barato) cuando cumple la política, y se borra al final de su custodia. Todo lo de esta página sale de la API en este momento.</p>
    <div id="cuerpo"><div class="cargando">Cargando…</div></div>`;
  cargar(vista.querySelector("#cuerpo"));
}

async function cargar(el) {
  const [stats, estado, politica, coste, calidad] = await Promise.allSettled([
    api.stats(), api.estado(), api.politica(), api.metrica("coste"), api.metrica("calidad"),
  ]);
  const fallo = [stats, estado, politica, coste, calidad].find((r) => r.status === "rejected");
  if (fallo && stats.status === "rejected") { el.innerHTML = error(fallo.reason); return; }

  const s = stats.value?.data;
  const est = estado.value?.data;
  const pol = politica.value?.data ?? [];
  const archivo = pol.find((p) => p.accion === "ARCHIVE");
  const borrado = pol.find((p) => p.accion === "DELETE");
  const costes = coste.value?.data ?? [];
  const cHot = costes.find((c) => c.tier === "hot");
  const cCold = costes.find((c) => c.tier === "cold");
  const cuarentena = (calidad.value?.data ?? []).reduce((t, f) => t + (f.registros || 0), 0);
  const cumple = est?.cumplimiento;
  const candidatas = est?.candidatas_a_archivar?.length ?? 0;
  const factor = cHot && cCold && cCold.bytes_por_fila ? cHot.bytes_por_fila / cCold.bytes_por_fila : null;

  el.innerHTML = `
    ${fallo ? error(fallo.reason) : ""}
    <section class="panel">
      <div class="flujo">
        <div class="paso">
          <div class="t">Entrada</div>
          <div class="n">Kafka + carga</div>
          <div class="muted">El simulador emite viajes en vivo; la carga inicial trae el histórico.</div>
          <div class="muted" style="margin-top:6px">${fmtNum(cuarentena)} rechazados en cuarentena</div>
        </div>
        <div class="flecha"><b>→</b>valida el contrato</div>
        <div class="paso hot">
          <div class="t">Caliente · PostgreSQL</div>
          <div class="n">${fmtNum(s?.hot?.filas)} viajes</div>
          <div class="muted">Los últimos ${esc(frasePolitica(archivo))}. Particiones diarias.</div>
          <div class="muted" style="margin-top:6px">${cHot ? `${fmtNum(cHot.bytes_por_fila, 0)} B por fila` : ""}</div>
        </div>
        <div class="flecha"><b>→</b>DAG archivar, cada 5 min, a los ${esc(frasePolitica(archivo))}</div>
        <div class="paso cold">
          <div class="t">Frío · Iceberg en MinIO</div>
          <div class="n">${fmtNum(s?.cold?.filas)} viajes</div>
          <div class="muted">${fmtNum(s?.cold?.particiones)} particiones mensuales · ${fmtNum(s?.cold?.ficheros)} ficheros Parquet · ${fmtBytes(s?.cold?.bytes)}</div>
          <div class="muted" style="margin-top:6px">${s?.cold?.bytes_por_fila ? `${fmtNum(s.cold.bytes_por_fila, 0)} B por fila` : ""}</div>
        </div>
        <div class="flecha"><b>→</b>DAG purga_final, a los ${esc(frasePolitica(borrado))}</div>
        <div class="paso">
          <div class="t">Fin</div>
          <div class="n">Borrado</div>
          <div class="muted">Termina la custodia y el dato se elimina.</div>
        </div>
      </div>
    </section>

    <div class="cards">
      <div class="card"><div class="k">¿Va el archivado al día?</div><div class="v" style="font-size:18px">${cumple ? chipEstado(cumple.estado === "OK" ? "ok" : "mal", cumple.estado) : "—"}</div><div class="d">${cumple ? `el dato más viejo del caliente tiene ${fmtNum(cumple.edad_maxima_dias)} días (límite ${fmtNum(cumple.umbral_dias)})` : ""}</div></div>
      <div class="card"><div class="k">Pendientes de archivar</div><div class="v">${fmtNum(candidatas)}</div><div class="d">particiones que ya cumplen la política</div></div>
      <div class="card"><div class="k">Total de viajes</div><div class="v">${fmtNum((s?.hot?.filas || 0) + (s?.cold?.filas || 0))}</div><div class="d">caliente + frío, sin solaparse</div></div>
      <div class="card"><div class="k">Ahorro de espacio del frío</div><div class="v">${factor ? `${fmtNum(factor, 1)}×` : "—"}</div><div class="d">menos bytes por fila que PostgreSQL</div></div>
    </div>

    <div class="rejilla">
      <section class="panel">
        <div class="panel-cab"><h2>Bytes por fila</h2><p>Lo que cuesta guardar un viaje en cada tier</p></div>
        ${cHot && cCold ? barras([
          { etiqueta: "Caliente · PostgreSQL", valor: cHot.bytes_por_fila, color: "var(--hot)", texto: `${fmtNum(cHot.bytes_por_fila, 0)} B`, tip: `${fmtNum(cHot.filas)} filas · ${fmtBytes(cHot.bytes)}` },
          { etiqueta: "Frío · Iceberg", valor: cCold.bytes_por_fila, color: "var(--cold)", texto: `${fmtNum(cCold.bytes_por_fila, 0)} B`, tip: `${fmtNum(cCold.filas)} filas · ${fmtBytes(cCold.bytes)}` },
        ]) : '<p class="muted">Sin datos de coste.</p>'}
        ${bloqueMeta("/metrics/coste", coste.value)}
      </section>
      <section class="panel">
        <div class="panel-cab"><h2>Qué puedes probar</h2></div>
        <div class="accesos">
          <a class="acceso" href="#/viajes"><b>Consultar viajes →</b><span>Elige un rango y mira de qué tier sale cada viaje.</span></a>
          <a class="acceso" href="#/ciclo"><b>Cambiar la política →</b><span>Bájala a 5 minutos y mira cómo se mueven los días al frío.</span></a>
          <a class="acceso" href="#/metricas"><b>Ver métricas →</b><span>Coste, latencia por tier y calidad de los datos.</span></a>
          <a class="acceso" href="#/explorador"><b>Probar cada ruta →</b><span>Todas las rutas de la API, con su respuesta y su curl.</span></a>
        </div>
        ${bloqueMeta("/stats", stats.value)}
      </section>
    </div>
    <p class="muted" style="font-size:12px">Datos a ${fmtFecha(stats.value?.meta?.as_of)} UTC. <button type="button" class="chip-btn" id="recargar">Recargar</button></p>`;
  el.querySelector("#recargar").onclick = () => cargar(el);
}
