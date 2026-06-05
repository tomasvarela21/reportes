"""
frontend/pages/3_Consulta_Diario.py
"""
import io

import pandas as pd
import streamlit as st

from services.api_client import (
    EMPRESAS,
    apply_styles,
    get_diario,
    get_empresas,
    get_periodos_diario,
    render_sidebar,
)

st.set_page_config(page_title="Consulta Diario · ReporteApp", page_icon="📋", layout="wide")
apply_styles()
render_sidebar()

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

PAGE_SIZE = 500

st.title("📋 Consulta Diario")
st.caption("Búsqueda de asientos del libro diario por empresa y período.")
st.divider()

# ── Empresa ────────────────────────────────────────────────────────────────────
try:
    empresas_api  = get_empresas()
    empresas_lista = [e["empresa_nombre"] for e in empresas_api if e.get("activa", True)]
    if not empresas_lista:
        empresas_lista = list(EMPRESAS.keys())
except Exception:
    empresas_lista = list(EMPRESAS.keys())

c1, c2, c3, c4, c5 = st.columns(5)
empresa_nombre = c1.selectbox("Empresa", empresas_lista)
empresa_id     = EMPRESAS.get(empresa_nombre, 0)

# ── Períodos ───────────────────────────────────────────────────────────────────
try:
    periodos_raw = get_periodos_diario(empresa_id)
except Exception as e:
    st.error(f"❌ {e}")
    st.stop()

if not periodos_raw:
    st.info(f"No hay datos para **{empresa_nombre}**.")
    st.stop()

opciones = [
    f"{MESES[p['periodo_mes']]} {p['periodo_anio']}" for p in periodos_raw
]
sel   = c2.selectbox("Período", opciones, index=len(opciones) - 1)
idx   = opciones.index(sel)
anio_sel = periodos_raw[idx]["periodo_anio"]
mes_sel  = periodos_raw[idx]["periodo_mes"]

# ── Filtros adicionales ────────────────────────────────────────────────────────
cuenta_raw    = c3.text_input("Cuenta", placeholder="ej: 38")
cuenta_filtro = int(cuenta_raw.strip()) if cuenta_raw.strip().isdigit() else None
cc_filtro     = c4.text_input("Centro costo", placeholder="ej: 1101").strip() or None
desc_filtro   = c5.text_input("Descripción", placeholder="búsqueda parcial").strip() or None

# ── Paginación ─────────────────────────────────────────────────────────────────
if "diario_offset" not in st.session_state:
    st.session_state["diario_offset"] = 0

# Resetear paginación si cambian los filtros
filtros_key = (empresa_id, anio_sel, mes_sel, cuenta_filtro, cc_filtro, desc_filtro)
if st.session_state.get("_diario_filtros_key") != filtros_key:
    st.session_state["diario_offset"]     = 0
    st.session_state["_diario_filtros_key"] = filtros_key

offset = st.session_state["diario_offset"]

st.divider()

# ── Consulta ───────────────────────────────────────────────────────────────────
try:
    resp = get_diario(empresa_id, {
        "anio":          anio_sel,
        "mes":           mes_sel,
        "cuenta_codigo": cuenta_filtro,
        "centro_costo":  cc_filtro,
        "descripcion":   desc_filtro,
        "limit":         PAGE_SIZE,
        "offset":        offset,
    })
except Exception as e:
    st.error(f"❌ {e}")
    st.stop()

filas  = resp.get("data", []) if isinstance(resp, dict) else resp
total  = resp.get("total", len(filas)) if isinstance(resp, dict) else len(filas)

if not filas and offset == 0:
    st.info("Sin registros con esos filtros.")
    st.stop()

df = pd.DataFrame(filas)

# ── Métricas ───────────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Registros (página)",  f"{len(df):,}")
m2.metric("Total (sin paginar)", f"{total:,}")
debe_sum  = pd.to_numeric(df.get("debe",  pd.Series()), errors="coerce").sum() if "debe"  in df.columns else 0
haber_sum = pd.to_numeric(df.get("haber", pd.Series()), errors="coerce").sum() if "haber" in df.columns else 0
m3.metric("Total Debe",  f"{debe_sum:,.2f}")
m4.metric("Total Haber", f"{haber_sum:,.2f}")
st.divider()

# ── Tabla ──────────────────────────────────────────────────────────────────────
col_config = {}
if "debe"  in df.columns: col_config["debe"]  = st.column_config.NumberColumn("Debe",  format="%.2f")
if "haber" in df.columns: col_config["haber"] = st.column_config.NumberColumn("Haber", format="%.2f")

st.dataframe(df, use_container_width=True, hide_index=True, column_config=col_config)

# ── Paginación ─────────────────────────────────────────────────────────────────
has_more = resp.get("has_more", False) if isinstance(resp, dict) else False
pg1, pg2, pg3 = st.columns([1, 2, 1])

if offset > 0:
    if pg1.button("← Anterior"):
        st.session_state["diario_offset"] = max(0, offset - PAGE_SIZE)
        st.rerun()

pg2.caption(
    f"Mostrando {offset + 1}–{min(offset + len(df), total)} de {total:,} registros"
)

if has_more:
    if pg3.button("Siguiente →"):
        st.session_state["diario_offset"] = offset + PAGE_SIZE
        st.rerun()

# ── Exportar ───────────────────────────────────────────────────────────────────
st.divider()
dl1, dl2 = st.columns(2)
dl1.download_button(
    "⬇️ Descargar CSV (página actual)",
    df.to_csv(index=False).encode("utf-8"),
    f"diario_{empresa_nombre}_{anio_sel}_{mes_sel:02d}.csv",
    "text/csv",
)

output = io.BytesIO()
with pd.ExcelWriter(output, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Diario", index=False)
output.seek(0)
dl2.download_button(
    "⬇️ Descargar Excel (página actual)",
    output,
    f"diario_{empresa_nombre}_{anio_sel}_{mes_sel:02d}.xlsx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
