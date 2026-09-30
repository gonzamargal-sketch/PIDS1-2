// Ciclo de vida: la política (GET/PUT /lifecycle/policy) y cómo la aplica
// el DAG archivar, partición a partición (GET /lifecycle/status). Se
// refresca solo para ver avanzar la máquina de estados durante la demo.

import { api } from "../api.js";
import { $, bloqueMeta, chipEstado, error, esc, fmtBytes, fmtFecha, fmtNum, mensaje } from "../comun.js";
import { frasePolitica } from "./inicio.js";

export const titulo = "Ciclo de vida";

const ESTADOS = ["PENDIENTE", "ESCRIBIENDO", "ESCRITO", "VERIFICADO", "DESALOJADO"];
const EXPLICA = {
  PENDIENTE: "cumple la política, aún no ha empezado",
  ESCRIBIENDO: "copiando la partición a Iceberg",
  ESCRITO: "copiada, falta comprobarla",
  VERIFICADO: "conteos cuadran, falta borrarla",
  DESALOJADO: "ya solo está en el frío",
  ERROR: "algo falló: se reintenta y no se borra nada",
};
const REFRESCO_MS = 5000;

export function montar(vista) {
  vista.innerHTML = `
    <h1>Ciclo de vida</h1>
    <p class="sub">La política dice <b>cuándo</b> sale un dato del caliente; el DAG <code>archivar</code> de Airflow (cada 5 min) lo copia a Iceberg, comprueba que cuadra y solo entonces lo borra de PostgreSQL.</p>
    <div class="rejilla">
      <section class="panel" id="politica"><div class="cargando">Cargando…</div></section>
      <section class="panel">
        <div class="panel-cab"><h2>Cambiar el umbral de archivado</h2><p><code>PUT /lifecycle/policy</code></p></div>
        <form class="fila" id="form">
          <label>Valor<input type="number" id="valor" min="1" value="30" required></label>
          <label>Unidad<select id="unidad"><option value="minutes">minutos</option><option value="hours">horas</option><option value="days" selected>días</option><option value="years">años</option></select></label>
          <button type="submit">Guardar</button>
        </form>
        <div class="chips" style="margin-top:12px">
          <span class="muted" style="font-size:12px">Atajos:</span>
          <button type="button" class="chip-btn" data-v="5" data-u="minutes">Demo: 5 minutos</button>
          <button type="button" class="chip-btn" data-v="30" data-u="days">Volver a 30 días (la real)</button>
        </div>
        <div id="resultado-put" style="margin-top:12px"></div>
      </section>
    </div>
    <div style="height:16px"></div>
    <div id="mensaje"></div>
    <section class="panel">
      <div class="panel-cab"><h2>Máquina de estados del archivado</h2><p>Particiones diarias en cada estado</p>
        <label class="interruptor der"><input type="checkbox" id="auto" checked> Actualizar cada 5 s</label></div>
      <div class="maquina" id="maquina"></div>
      <p class="muted" id="cumple" style="margin:12px 0 0"></p>
    </section>
    <section class="panel">
      <div class="panel-cab"><h2>Pendientes de archivar</h2><p>Lo que <code>archivar</code> se llevará en su próxima pasada</p></div>
      <div id="candidatas"></div>
    </section>
    <section class="panel">
      <div class="panel-cab"><h2>Historial de archivado</h2><p>Una fila por partición (tabla <code>archival_jobs</code>, las 200 más recientes)</p></div>
      <div class="tabla-wrap" id="jobs"></div>
      <div id="meta-estado"></div>
    </section>`;

  const q = (id) => $(`#${id}`, vista);
  let temporizador = null;

  async function cargarPolitica() {
    try {
      const r = await api.politica();
      const archivo = r.data.find((p) => p.accion === "ARCHIVE");
      const borrado = r.data.find((p) => p.accion === "DELETE");
      q("politica").innerHTML = `
        <div class="panel-cab"><h2>Política vigente</h2><p><code>GET /lifecycle/policy</code></p></div>
        <p class="politica-frase">Un viaje pasa del <span class="tier hot">caliente</span> al <span class="tier cold">frío</span> a los <b>${esc(frasePolitica(archivo))}</b>.</p>
        <p class="politica-frase">Se borra del frío a los <b>${esc(frasePolitica(borrado))}</b>.</p>
        <p class="muted" style="font-size:12px;margin:8px 0 0">Última modificación: ${fmtFecha(archivo?.actualizado_en)} UTC. Es un dato en la tabla <code>retention_policy</code>, no código: se cambia sin redesplegar.</p>
        ${bloqueMeta("/lifecycle/policy", r)}`;
      if (archivo) { q("valor").value = archivo.umbral_valor; q("unidad").value = archivo.umbral_unidad; }
    } catch (e) {
      q("politica").innerHTML = error(e);
    }
  }

  async function cambiar(valor, unidad) {
    const texto = frasePolitica({ umbral_valor: valor, umbral_unidad: unidad });
    if (!confirm(`¿Cambiar el umbral de archivado a ${texto}?\n\nEs la política real: el DAG archivar moverá al frío todo lo que la cumpla en su próxima pasada.`)) return;
    q("resultado-put").innerHTML = "";
    try {
      const r = await api.cambiarPolitica(Number(valor), unidad);
      const n = r.data.particiones_candidatas_ahora;
      q("resultado-put").innerHTML = mensaje("ok",
        `Guardado: ahora se archiva a los <b>${esc(texto)}</b>. ` +
        (n ? `<b>${fmtNum(n)}</b> particiones pasan a estar pendientes; el DAG <code>archivar</code> las moverá en su próxima pasada (como mucho 5 min). Míralo abajo.`
           : "Ninguna partición cumple todavía la nueva política.") +
        bloqueMeta("/lifecycle/policy", r, "PUT"));
      await Promise.all([cargarPolitica(), cargarEstado()]);
    } catch (e) {
      q("resultado-put").innerHTML = error(e);
    }
  }

  async function cargarEstado() {
    try {
      const r = await api.estado();
      const d = r.data;
      q("mensaje").innerHTML = "";
      // Las candidatas traen su estado (PENDIENTE si aún no tienen job):
      // de ahí salen los estados en curso; DESALOJADO, del resumen.
      const cuenta = {};
      for (const p of d.candidatas_a_archivar) cuenta[p.estado] = (cuenta[p.estado] || 0) + 1;
      cuenta.DESALOJADO = d.resumen_por_estado.find((f) => f.estado === "DESALOJADO")?.particiones || 0;
      const pendientes = d.candidatas_a_archivar.length;
      const caja = (e, extra = "") => `<div class="st ${extra}" data-tip="${esc(EXPLICA[e])}"><div class="c">${fmtNum(cuenta[e] || 0)}</div><div class="e">${e}</div></div>`;
      q("maquina").innerHTML = ESTADOS.map((e) => caja(e, e === "DESALOJADO" ? "fin" : cuenta[e] ? "activo" : "")).join('<span class="sep">→</span>')
        + (cuenta.ERROR ? `<span class="sep" style="margin-left:12px">·</span>${caja("ERROR", "error")}` : "");

      const c = d.cumplimiento;
      q("cumple").innerHTML = c
        ? `${chipEstado(c.estado === "OK" ? "ok" : "mal", c.estado)} &nbsp;El dato más viejo del caliente tiene ${fmtNum(c.edad_maxima_dias)} días; la política permite ${fmtNum(c.umbral_dias)}.` +
          (c.estado === "OK" ? "" : " El archivado va con retraso: espera a la siguiente pasada o lánzala a mano en Airflow.")
        : "";

      q("candidatas").innerHTML = pendientes
        ? `<div class="tabla-wrap"><table><thead><tr><th>Partición</th><th>Día</th><th class="n">Edad (días)</th><th class="n">Filas (aprox.)</th><th class="n">Tamaño</th><th>Estado</th></tr></thead><tbody>` +
          d.candidatas_a_archivar.map((p) => `<tr><td><code>${esc(p.particion)}</code></td><td>${esc(p.dia)}</td><td class="n">${fmtNum(p.edad_dias)}</td><td class="n">${fmtNum(p.filas_estimadas)}</td><td class="n">${fmtBytes(p.bytes_total)}</td><td>${esc(p.estado ?? "—")}</td></tr>`).join("") +
          "</tbody></table></div>"
        : '<p class="muted" style="margin:0">Nada pendiente: todo lo que cumple la política ya está en el frío.</p>';

      q("jobs").innerHTML = d.jobs.length
        ? `<table><thead><tr><th>Partición</th><th>Estado</th><th class="n">Filas en PostgreSQL</th><th class="n">Filas en Iceberg</th><th>¿Cuadran?</th><th class="n">Bytes escritos</th><th class="n">Intentos</th><th>Terminado (UTC)</th><th>Error</th></tr></thead><tbody>` +
          d.jobs.map((j) => {
            const cuadra = j.filas_origen != null && j.filas_origen === j.filas_escritas;
            return `<tr><td>${esc(j.particion)}</td><td>${esc(j.estado)}</td><td class="n">${fmtNum(j.filas_origen)}</td><td class="n">${fmtNum(j.filas_escritas)}</td>` +
              `<td>${j.filas_escritas == null ? "—" : chipEstado(cuadra ? "ok" : "mal", cuadra ? "sí" : "no")}</td>` +
              `<td class="n">${fmtBytes(j.bytes_escritos)}</td><td class="n">${fmtNum(j.intentos)}</td><td>${fmtFecha(j.terminado_en)}</td><td>${esc(j.error ?? "")}</td></tr>`;
          }).join("") + "</tbody></table>"
        : '<p class="muted">Todavía no se ha archivado ninguna partición.</p>';
      q("meta-estado").innerHTML = bloqueMeta("/lifecycle/status", r);
    } catch (e) {
      q("mensaje").innerHTML = error(e);
    }
  }

  function programar() {
    clearInterval(temporizador);
    temporizador = q("auto").checked ? setInterval(cargarEstado, REFRESCO_MS) : null;
  }

  q("form").addEventListener("submit", (ev) => { ev.preventDefault(); cambiar(q("valor").value, q("unidad").value); });
  vista.querySelectorAll("[data-v]").forEach((b) => (b.onclick = () => cambiar(b.dataset.v, b.dataset.u)));
  q("auto").onchange = programar;

  cargarPolitica();
  cargarEstado();
  programar();
  return () => clearInterval(temporizador);
}
