// Enrutado por hash (#/viajes?desde=…): cada pantalla es un módulo con
// montar(contenedor, params) que devuelve una función para desmontarse
// (parar refrescos automáticos, etc.).

import { api } from "./api.js";
import { $, $$ } from "./comun.js";
import * as inicio from "./pantallas/inicio.js";
import * as viajes from "./pantallas/viajes.js";
import * as envivo from "./pantallas/envivo.js";
import * as ciclo from "./pantallas/ciclo.js";
import * as metricas from "./pantallas/metricas.js";
import * as explorador from "./pantallas/explorador.js";

const PANTALLAS = { "": inicio, envivo, viajes, ciclo, metricas, explorador };

let desmontar = null;

function ir() {
  const [ruta, query = ""] = location.hash.replace(/^#\/?/, "").split("?");
  const pantalla = PANTALLAS[ruta] ?? inicio;
  if (desmontar) desmontar();
  $$("#menu a").forEach((a) => a.classList.toggle("activo", a.getAttribute("href") === `#/${PANTALLAS[ruta] ? ruta : ""}`));
  document.title = `${pantalla.titulo} · PIDS E8`;
  // Un contenedor nuevo por pantalla: si una respuesta llega después de
  // cambiar de pestaña, se pinta en el viejo, que ya no está en la página
  const contenedor = document.createElement("div");
  $("#vista").replaceChildren(contenedor);
  window.scrollTo(0, 0);
  desmontar = pantalla.montar(contenedor, new URLSearchParams(query)) || null;
}

async function salud() {
  const el = $("#salud");
  try {
    await api.health({ apuntar: false });  // el latido no ensucia el registro
    el.className = "salud ok";
    el.lastElementChild.textContent = "API en marcha";
  } catch {
    el.className = "salud ko";
    el.lastElementChild.textContent = "API sin respuesta";
  }
}

window.addEventListener("hashchange", ir);
ir();
salud();
setInterval(salud, 15000);
