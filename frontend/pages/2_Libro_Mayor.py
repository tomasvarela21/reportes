"""
frontend/pages/2_Libro_Mayor.py
"""
import io

import pandas as pd
import streamlit as st

from services.api_client import (
    EMPRESAS,
    apply_styles,
    get_empresas,
    get_mayor,
    get_mayor_periodos,
    get_mayor_resumen,
    render_sidebar,
)

st.set_page_config(page_title="Libro Mayor · ReporteApp", page_icon="📚", layout="wide")
apply_styles()
render_sidebar()

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

st.title("📚 Libro Mayor")
st.caption("Saldos acumulados por empresa, período, cuenta y centro de costo.")
st.divider()

# ── Selector empresa ───────────────────────────────────────────────────────────
try:
    empresas_api = get_empresas()
    empresas_lista = [e["empresa_nombre"] for e in empresas_api if e.get("activa", True)]
    if not empresas_lista:
        empresas_lista = list(EMPRESAS.keys())
except Exception as e:
    st.error(f"❌ No se pudo cargar la lista de empresas: {e}")
    empresas_lista = list(EMPRESAS.keys())

c1, c2, c3, c4, c5 = st.columns(5)
empresa_nombre = c1.selectbox("Empresa", empresas_lista)
empresa_id     = EMPRESAS.get(empresa_nombre, 0)

# ── Períodos disponibles ───────────────────────────────────────────────────────
try:
    periodos_raw = get_mayor_periodos(empresa_id)
except Exception as e:
    st.error(f"❌ {e}")
    st.stop()

if not periodos_raw:
    st.info(f"No hay datos de mayor para **{empresa_nombre}**.")
    st.stop()

opciones_periodo = [
    f"{MESES[p['periodo_mes']]} {p['periodo_anio']}" for p in periodos_raw
]
sel = c2.selectbox("Período", opciones_periodo, index=len(opciones_periodo) - 1)
idx_sel      = opciones_periodo.index(sel)
anio_sel     = periodos_raw[idx_sel]["periodo_anio"]
mes_sel      = periodos_raw[idx_sel]["periodo_mes"]

nivel         = c3.selectbox("Nivel", ["cuenta", "subcuenta"])
cuenta_raw    = c4.text_input("Cuenta", placeholder="ej: 38")
cuenta_filtro = int(cuenta_raw.strip()) if cuenta_raw.strip().isdigit() else None
cc_filtro     = c5.text_input("Centro costo", placeholder="ej: 1101").strip() or None

st.divider()

# ── Llamada al mayor ───────────────────────────────────────────────────────────
filtros = {
    "anio":          anio_sel,
    "mes":           mes_sel,
    "nivel":         nivel,
    "cuenta_codigo": cuenta_filtro,
    "centro_costo":  cc_filtro,
    "limit":         5000,
    "offset":        0,
}

try:
    filas = get_mayor(empresa_id, filtros)
    # get_mayor puede devolver list o dict (PaginatedResponse)
    if isinstance(filas, dict):
        filas = filas.get("data", [])
except Exception as e:
    st.error(f"❌ {e}")
    st.stop()

if not filas:
    st.info("Sin registros con esos filtros.")
    st.stop()

df = pd.DataFrame(filas)

# Renombrar columnas para display
col_rename = {
    "cuenta_codigo":   "Cuenta",
    "cuenta_nombre":   "Nombre",
    "tipo_subcuenta":  "Tipo Subcta",
    "nro_subcuenta":   "Nro Subcta",
    "centro_costo":    "Centro Costo",
    "total_debe":      "Total Debe",
    "total_haber":     "Total Haber",
    "saldo_anterior":  "Saldo Anterior",
    "saldo_periodo":   "Saldo Período",
    "saldo_acumulado": "Saldo Acumulado",
}
df = df.rename(columns={k: v for k, v in col_rename.items() if k in df.columns})

# ── Métricas ───────────────────────────────────────────────────────────────────
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Cuentas",         f"{len(df):,}")
m2.metric("Total Debe",      f"{df.get('Total Debe', pd.Series([0])).sum():,.2f}" if "Total Debe" in df.columns else "—")
m3.metric("Total Haber",     f"{df['Total Haber'].sum():,.2f}" if "Total Haber" in df.columns else "—")
m4.metric("Saldo Anterior",  f"{df['Saldo Anterior'].sum():,.2f}" if "Saldo Anterior" in df.columns else "—")
m5.metric("Saldo Acumulado", f"{df['Saldo Acumulado'].sum():,.2f}" if "Saldo Acumulado" in df.columns else "—")
st.divider()

# Formatear columnas numéricas
cols_num = ["Total Debe", "Total Haber", "Saldo Anterior", "Saldo Período", "Saldo Acumulado"]
df_show = df.copy()
for col in cols_num:
    if col in df_show.columns:
        df_show[col] = pd.to_numeric(df_show[col], errors="coerce").apply(
            lambda x: f"{x:,.2f}" if pd.notna(x) else ""
        )

st.dataframe(df_show, use_container_width=True, hide_index=True)

# ── Exportar ───────────────────────────────────────────────────────────────────
col_dl1, col_dl2 = st.columns(2)
col_dl1.download_button(
    "⬇️ Descargar CSV",
    df.to_csv(index=False).encode("utf-8"),
    f"mayor_{empresa_nombre}_{anio_sel}_{mes_sel:02d}_{nivel}.csv",
    "text/csv",
)

output = io.BytesIO()
with pd.ExcelWriter(output, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="Mayor", index=False)
output.seek(0)
col_dl2.download_button(
    "⬇️ Descargar Excel",
    output,
    f"mayor_{empresa_nombre}_{anio_sel}_{mes_sel:02d}_{nivel}.xlsx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

# ── Resumen por cuenta ─────────────────────────────────────────────────────────
with st.expander("📊 Resumen por cuenta (nivel='cuenta')", expanded=False):
    try:
        resumen = get_mayor_resumen(empresa_id, {"anio": anio_sel, "mes": mes_sel})
        if resumen:
            df_res = pd.DataFrame(resumen)
            st.dataframe(df_res, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"❌ {e}")
