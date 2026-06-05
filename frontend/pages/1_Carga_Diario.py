"""
frontend/pages/1_Carga_Diario.py
Carga del Libro Diario via API — sin acceso directo a la DB.
"""
import json
import re

import pandas as pd
import streamlit as st

from services.api_client import (
    EMPRESAS,
    apply_styles,
    crear_centro_costo,
    crear_cuenta,
    get_empresas,
    get_periodos_diario,
    render_sidebar,
    upload_diario,
    validar_diario,
)

st.set_page_config(
    page_title="Carga Diario · ReporteApp",
    page_icon="📤",
    layout="wide",
)
apply_styles()
render_sidebar()

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}


# ── Helpers de estado ──────────────────────────────────────────────────────────

def reset_estado():
    for k in ["parse_result", "periodos_info", "decisiones", "centros_agregados"]:
        st.session_state.pop(k, None)


def reset_completo():
    for k in [
        "parse_result", "periodos_info", "decisiones", "centros_agregados",
        "carga_exitosa", "empresa_sugerida",
    ]:
        st.session_state.pop(k, None)


def _detectar_empresa(filename: str) -> str | None:
    """Detecta empresa desde el nombre del archivo (mismo criterio que el backend)."""
    nombre = filename.upper()
    for emp in EMPRESAS:
        if emp in nombre:
            return emp
    return None


# ── Pantalla de éxito ──────────────────────────────────────────────────────────

if "carga_exitosa" in st.session_state:
    r = st.session_state["carga_exitosa"]
    periodos = r["periodos_cargados"]  # [[anio, mes, n], ...]

    st.title("📤 Carga de Libro Diario")
    st.success(f"✅ Carga completada para **{r['empresa']}**")
    st.divider()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Períodos cargados",   len(periodos))
    c2.metric("Registros en diario", f"{r['registros_cargados']:,}")
    c3.metric("Registros en mayor",  f"{r['registros_mayor']:,}")
    c4.metric("Tiempo total",        f"{r['duracion_ms']:,} ms")

    st.divider()
    with st.expander("📋 Detalle por período", expanded=True):
        for anio, mes, n in periodos:
            st.write(f"  • {MESES[int(mes)]} {anio}: {n:,} registros")

    if periodos:
        anio_desde, mes_desde, _ = periodos[0]
        anio_hasta, mes_hasta, _ = periodos[-1]
        desde_label = f"{MESES[int(mes_desde)]} {anio_desde}"
        hasta_label  = f"{MESES[int(mes_hasta)]} {anio_hasta}"
        st.info(
            f"📊 **Mayor recalculado** desde **{desde_label}** hasta **{hasta_label}** "
            f"— {r['registros_mayor']:,} registros generados en `libro_mayor`."
        )

    st.divider()
    if st.button("📤 Cargar otro archivo", type="primary"):
        reset_completo()
        st.rerun()
    st.stop()


# ── Título ─────────────────────────────────────────────────────────────────────

st.title("📤 Carga de Libro Diario")
st.caption("Cargá el CSV del diario contable. Se valida, procesa y recalcula el Mayor automáticamente.")
st.divider()


# ── Paso 1: Empresa y archivo ──────────────────────────────────────────────────

st.subheader("Paso 1 — Seleccioná la empresa y el archivo")

# Cargar lista de empresas desde la API
try:
    empresas_api = get_empresas()
    empresas_lista = [e["empresa_nombre"] for e in empresas_api if e.get("activa", True)]
    if not empresas_lista:
        empresas_lista = list(EMPRESAS.keys())
except Exception:
    empresas_lista = list(EMPRESAS.keys())

c1, c2 = st.columns([1, 3])

idx_default = 0
if st.session_state.get("empresa_sugerida") in empresas_lista:
    idx_default = empresas_lista.index(st.session_state["empresa_sugerida"])

empresa_nombre = c1.selectbox(
    "Empresa",
    empresas_lista,
    index=idx_default,
    on_change=reset_estado,
)
empresa_id = EMPRESAS.get(empresa_nombre, 0)

archivo = c2.file_uploader(
    "Archivo CSV del libro diario",
    type=["csv"],
    on_change=reset_estado,
    help="Formatos soportados: 22 cols posicional o 17 cols con id_empresa",
)

# Auto-detectar empresa desde el nombre del archivo
if archivo and "empresa_sugerida" not in st.session_state:
    detectada = _detectar_empresa(archivo.name)
    if detectada and detectada != empresa_nombre and detectada in empresas_lista:
        st.session_state["empresa_sugerida"] = detectada
        st.rerun()

if not archivo:
    st.info("👆 Seleccioná una empresa y subí el archivo CSV para continuar.")
    st.stop()


# ── Paso 2: Validación via API ─────────────────────────────────────────────────

if "parse_result" not in st.session_state:
    with st.spinner("Analizando archivo..."):
        try:
            file_bytes = archivo.read()
            result = validar_diario(file_bytes, archivo.name, empresa_nombre)
            st.session_state["parse_result"] = result
            st.session_state["_file_bytes"]  = file_bytes
        except Exception as e:
            st.error(f"❌ Error al contactar la API: {e}")
            st.stop()

result = st.session_state["parse_result"]

# Advertencia de empresa detectada
empresa_detectada = result.get("empresa_detectada")
if empresa_detectada and empresa_detectada != empresa_nombre:
    st.warning(
        f"⚠️ El nombre del archivo sugiere **{empresa_detectada}** "
        f"pero seleccionaste **{empresa_nombre}**. Verificá antes de continuar."
    )

st.subheader("Paso 2 — Resultado del análisis")

for adv in result.get("advertencias", []):
    st.warning(f"⚠️ {adv}")

if not result.get("ok"):
    for err in result.get("errores", []):
        if isinstance(err, dict) and err.get("__tipo__") == "descuadre":
            st.error(f"❌ {err['resumen']}")
            for asiento in err.get("asientos", []):
                with st.container(border=True):
                    ca, cb, cc, cd = st.columns(4)
                    ca.markdown(f"**Asiento**  \n{asiento['nro_asiento']}")
                    cb.markdown(f"**Tipo**  \n{asiento['tipo']}")
                    cc.markdown(f"**Fecha**  \n{asiento['fecha']}")
                    cd.markdown(f"**Diferencia**  \n:red[{asiento['diff']:+,.2f}]")
                    st.divider()

                    df_reng = pd.DataFrame(asiento["renglones"])
                    total_debe  = df_reng["Debe"].sum()
                    total_haber = df_reng["Haber"].sum()
                    diff        = round(total_debe + total_haber, 2)

                    fila_total = pd.DataFrame([{
                        "Renglón": "TOTAL", "Cuenta": "",
                        "Debe": total_debe, "Haber": total_haber,
                    }])
                    df_display = pd.concat([df_reng, fila_total], ignore_index=True)
                    for col in ["Debe", "Haber"]:
                        df_display[col] = df_display[col].apply(
                            lambda x: f"{x:+,.2f}" if isinstance(x, (int, float)) else x
                        )
                    st.dataframe(df_display, use_container_width=True, hide_index=True)

                    color = "red" if abs(diff) > 0.01 else "green"
                    st.markdown(
                        f":{color}[**Diferencia: {diff:+,.2f}**] — "
                        f"Total Debe: `{total_debe:+,.2f}` | "
                        f"Total Haber: `{total_haber:+,.2f}`"
                    )
        else:
            # Mostrar errores de cuentas en el bloque de cuentas inválidas más abajo
            if not (isinstance(err, str) and "no existen en el plan" in err):
                st.error(f"❌ {err}")

    if st.button("🔄 Reintentar con otro archivo"):
        reset_estado()
        st.rerun()

    # Si el único error es cuentas inválidas, no cortar el flujo aquí
    solo_error_cuentas = all(
        isinstance(err, str) and "no existen en el plan" in err
        for err in result.get("errores", [])
    )
    if not solo_error_cuentas:
        st.stop()

# Métricas del archivo (solo si ok)
if result.get("ok"):
    c1, c2 = st.columns(2)
    c1.metric("Filas válidas",  f"{result.get('total_filas_validas', 0):,}")
    c2.metric("Filas raw",      f"{result.get('total_filas_raw', 0):,}")


# ── Paso 3: Centros de costo no registrados ────────────────────────────────────

centros_faltantes: list[str] = []
for adv in result.get("advertencias", []):
    if "centro" in adv.lower() and "no registrados" in adv.lower():
        match = re.search(r"\[(.+)\]", adv)
        if match:
            centros_faltantes = [c.strip().strip("'") for c in match.group(1).split(",")]

if centros_faltantes:
    st.divider()
    st.subheader("Paso 3 — Centros de costo no registrados")
    st.info(f"Los siguientes {len(centros_faltantes)} centros no están en la base. Podés agregarlos ahora:")
    centros_agregados: set = st.session_state.get("centros_agregados", set())

    for cc in centros_faltantes:
        if cc in centros_agregados:
            st.success(f"✅ Centro **{cc}** ya agregado")
            continue
        with st.expander(f"➕ Agregar centro: **{cc}**", expanded=False):
            col1, col2, col3 = st.columns(3)
            col1.text_input("Código", value=cc, disabled=True, key=f"cc_cod_{cc}")
            desc_val = col2.text_input("Descripción", key=f"cc_desc_{cc}")
            emp_val  = col3.selectbox("Empresa (opcional)", ["—"] + empresas_lista, key=f"cc_emp_{cc}")
            if st.button(f"Guardar centro '{cc}'", key=f"btn_cc_{cc}"):
                try:
                    emp_id_cc = EMPRESAS.get(emp_val) if emp_val != "—" else None
                    crear_centro_costo({
                        "codigo": cc,
                        "descripcion": desc_val or cc,
                        "empresa_id": emp_id_cc,
                    })
                    centros_agregados.add(cc)
                    st.session_state["centros_agregados"] = centros_agregados
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")


# ── Paso 4: Decisión por período ───────────────────────────────────────────────
# Se muestra siempre que el backend haya podido detectar períodos en el archivo.
# ok=False por cuentas inválidas NO oculta esta sección — el usuario ve los
# períodos que se cargarán una vez que resuelva las cuentas faltantes.

periodos_reemplazar: dict[tuple, bool] = {}
periodos_bloqueados: list = []
periodos_del_archivo: list[dict] = result.get("periodos") or []
paso_num = 4 if centros_faltantes else 3

if periodos_del_archivo:
    st.divider()
    st.subheader(f"Paso {paso_num} — Decisión por período")

    periodos_existentes = [p for p in periodos_del_archivo if p.get("existe")]
    periodos_nuevos     = [p for p in periodos_del_archivo if not p.get("existe")]
    decisiones: dict[tuple, bool] = {}

    if periodos_existentes:
        # Guardamos las keys en session_state para que el callback on_change
        # pueda forzar el valor de cada checkbox individual.
        _keys_exist = [
            f"reemplazar_({int(p['anio'])}, {int(p['mes'])})"
            for p in periodos_existentes
        ]
        st.session_state["_periodo_keys_exist"] = _keys_exist

        def _marcar_todos_cb():
            val = st.session_state.get("reemplazar_todos", False)
            for k in st.session_state.get("_periodo_keys_exist", []):
                st.session_state[k] = val

        reemplazar_todos = st.checkbox(
            f"✅ Reemplazar todos los períodos existentes ({len(periodos_existentes)})",
            key="reemplazar_todos",
            on_change=_marcar_todos_cb,
        )
    else:
        reemplazar_todos = False

    if reemplazar_todos and periodos_existentes:
        # Vista colapsada: un solo mensaje en lugar de la lista entera
        msg_nuevos = (
            f" Se agregarán también **{len(periodos_nuevos)}** período(s) nuevo(s)."
            if periodos_nuevos else ""
        )
        st.success(
            f"✅ Los **{len(periodos_existentes)}** período(s) existentes serán "
            f"reemplazados.{msg_nuevos}"
        )
        for info in periodos_del_archivo:
            decisiones[(int(info["anio"]), int(info["mes"]))] = True
    else:
        # Grid 3 columnas
        NCOLS = 3
        filas_grid = [
            periodos_del_archivo[i : i + NCOLS]
            for i in range(0, len(periodos_del_archivo), NCOLS)
        ]

        for fila in filas_grid:
            cols = st.columns(NCOLS)
            for j, info in enumerate(fila):
                anio  = int(info["anio"])
                mes   = int(info["mes"])
                key   = (anio, mes)
                label = f"{MESES.get(mes, str(mes))} {anio}"

                with cols[j]:
                    with st.container(border=True):
                        if info.get("existe"):
                            n_regs    = info.get("total_registros", 0)
                            fecha_raw = info.get("fecha_carga") or ""
                            fecha_str = fecha_raw[:10] if fecha_raw else "—"
                            st.markdown(f"⚠️ **{label}**")
                            st.caption(f"{n_regs:,} registros · {fecha_str}")
                            decisiones[key] = st.checkbox(
                                "Reemplazar",
                                key=f"reemplazar_{key}",
                            )
                        else:
                            st.markdown(f"✅ **{label}**")
                            st.caption("Período nuevo")
                            decisiones[key] = True

            # Rellena celdas vacías en la última fila
            for j in range(len(fila), NCOLS):
                cols[j].empty()

    periodos_bloqueados = [k for k, v in decisiones.items() if not v]
    periodos_reemplazar = {k: v for k, v in decisiones.items() if v}

elif not result.get("ok"):
    # ok=False y el backend no devolvió períodos (versión vieja sin el fix de períodos).
    # Mostramos el paso como placeholder para que el usuario entienda el flujo.
    st.divider()
    st.subheader(f"Paso {paso_num} — Decisión por período")
    st.info(
        "ℹ️ Los períodos se confirmarán una vez resueltas las cuentas faltantes. "
        "Agregá las cuentas en el paso siguiente y hacé clic en **🔄 Reintentar con otro archivo**."
    )


# ── Paso 5: Cuentas no registradas ────────────────────────────────────────────

cuentas_invalidas: list[int] = []
for err in result.get("errores", []):
    if isinstance(err, str) and "no existen en el plan" in err:
        # Formato nuevo: "... [1234, 5678, 9012]\n  detalles..."
        match = re.search(r"\[(\d[\d,\s]*)\]", err)
        if match:
            try:
                cuentas_invalidas = [
                    int(c.strip()) for c in match.group(1).split(",") if c.strip()
                ]
            except ValueError:
                pass
        if not cuentas_invalidas:
            # Formato viejo: "  tipo=X | cta=1234 | ..." — extrae de cada línea
            encontrados = re.findall(r"cta=(\d+)", err)
            cuentas_invalidas = sorted(set(int(c) for c in encontrados))

if cuentas_invalidas:
    st.divider()
    paso_cta = paso_num + 1
    st.subheader(f"Paso {paso_cta} — Cuentas no registradas en el plan")
    st.warning(f"Las siguientes cuentas no están en `dim_cuenta`: {cuentas_invalidas}")

    for cta in cuentas_invalidas:
        with st.expander(f"➕ Agregar cuenta {cta}", expanded=False):
            cc1, cc2, cc3 = st.columns(3)
            ext_v = cc1.text_input("Extendido", key=f"cta_ext_{cta}")
            nom_v = cc2.text_input("Nombre",    key=f"cta_nom_{cta}")
            rub_v = cc3.text_input("Rubro",     key=f"cta_rub_{cta}")
            cc4, cc5 = st.columns(2)
            tip_v = cc4.selectbox("Tipo", ["Activo", "Pasivo", "Patrimonio", "Resultado"], key=f"cta_tip_{cta}")
            mon_v = cc5.selectbox("Moneda", ["ARS", "USD", "EUR"], key=f"cta_mon_{cta}")
            if st.button(f"Guardar cuenta {cta}", key=f"btn_cta_{cta}"):
                try:
                    crear_cuenta({
                        "nro_cta":   cta,
                        "extendido": ext_v or None,
                        "nombre":    nom_v,
                        "rubro":     rub_v or None,
                        "tipo":      tip_v,
                        "moneda":    mon_v,
                    })
                    # Re-validar: borrar resultado y rerun
                    st.session_state.pop("parse_result", None)
                    st.success(f"✅ Cuenta {cta} agregada. Re-validando...")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")


# ── Botón de carga final ───────────────────────────────────────────────────────

st.divider()

hay_errores_pendientes = bool(cuentas_invalidas) and not result.get("ok")

if hay_errores_pendientes:
    st.error("❌ Hay cuentas inválidas pendientes de resolver antes de poder cargar.")
elif periodos_bloqueados:
    st.warning(f"⚠️ {len(periodos_bloqueados)} período(s) no serán cargados por no estar marcados para reemplazar.")

if periodos_reemplazar and result.get("ok") and not hay_errores_pendientes:
    if st.button(f"📥 Cargar {len(periodos_reemplazar)} período(s)", type="primary"):
        periodos_dict = {
            f"{anio}/{str(mes).zfill(2)}": reemplazar
            for (anio, mes), reemplazar in periodos_reemplazar.items()
        }
        periodos_json_str = json.dumps(periodos_dict)

        file_bytes = st.session_state.get("_file_bytes") or archivo.read()

        with st.spinner("Cargando datos y recalculando Mayor..."):
            try:
                resultado = upload_diario(
                    file_bytes,
                    archivo.name,
                    empresa_nombre,
                    periodos_json_str,
                )
            except Exception as e:
                st.error(f"❌ {e}")
                st.stop()

        if resultado.get("ok"):
            for k in ["parse_result", "periodos_info", "decisiones", "centros_agregados", "_file_bytes"]:
                st.session_state.pop(k, None)
            st.session_state["carga_exitosa"] = {
                "registros_cargados": resultado["registros_cargados"],
                "registros_mayor":    resultado["registros_mayor"],
                "duracion_ms":        resultado["duracion_ms"],
                "periodos_cargados":  resultado["periodos_cargados"],
                "empresa":            empresa_nombre,
            }
            st.rerun()
        else:
            for err in resultado.get("errores", []):
                st.error(f"❌ {err}")
