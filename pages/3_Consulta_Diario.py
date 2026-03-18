import os, sys, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

st.set_page_config(page_title="Consulta Diario · ReporteApp", page_icon="📋", layout="wide")
apply_styles()
render_sidebar()

EMPRESAS = {
    'BATIA':     1,
    'GUARE':     3,
    'NORFORK':   2,
    'TORRES':    4,
    'WERCOLICH': 5,
}
MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

def get_diario(conn, empresa_id, anio, mes, cuenta_filtro, cc_filtro):
    filtros = ["ld.empresa_id=%s","ld.periodo_anio=%s","ld.periodo_mes=%s"]
    params  = [empresa_id, anio, mes]
    if cuenta_filtro: filtros.append("ld.cuenta_codigo=%s"); params.append(cuenta_filtro)
    if cc_filtro:     filtros.append("ld.centro_costo=%s");  params.append(cc_filtro)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT ld.fecha, ld.tipo_asiento, ld.nro_asiento, ld.nro_renglon,
               ld.cuenta_codigo, dc.nombre, ld.tipo_subcuenta, ld.nro_subcuenta,
               ld.centro_costo, ld.debe, ld.haber, ld.descripcion
        FROM libro_diario ld
        LEFT JOIN dim_cuenta dc ON dc.nro_cta = ld.cuenta_codigo
        WHERE {' AND '.join(filtros)}
        ORDER BY ld.fecha, ld.nro_asiento, ld.nro_renglon
    """, params)
    cols = ['Fecha','Tipo Asiento','Nro Asiento','Renglon','Cuenta','Nombre',
            'Tipo Subcta','Nro Subcta','Centro Costo','Debe','Haber','Descripción']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def get_periodos(conn, empresa_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT periodo_anio, periodo_mes FROM libro_diario
        WHERE empresa_id=%s ORDER BY 1,2
    """, (empresa_id,))
    rows = cur.fetchall(); cur.close(); return rows

st.title("📋 Consulta Diario")
st.caption("Búsqueda de asientos del libro diario por empresa y período.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

c1,c2,c3,c4 = st.columns(4)
empresa_nombre = c1.selectbox("Empresa", list(EMPRESAS.keys()))
empresa_id = EMPRESAS[empresa_nombre]

try:
    periodos = get_periodos(conn, empresa_id)
except Exception:
    conn = get_conn()
    if conn is None: st.stop()
    periodos = get_periodos(conn, empresa_id)

if not periodos:
    st.info(f"No hay datos para **{empresa_nombre}**."); st.stop()

opciones = [f"{MESES[m]} {a}" for a,m in periodos]
sel = c2.selectbox("Período", opciones, index=len(opciones)-1)
anio, mes = periodos[opciones.index(sel)]
cuenta_raw  = c3.text_input("Cuenta", placeholder="ej: 38")
cuenta_filtro = int(cuenta_raw) if cuenta_raw.strip().isdigit() else None
cc_filtro   = c4.text_input("Centro costo", placeholder="ej: 1101").strip() or None

st.divider()

try:
    df = get_diario(conn, empresa_id, anio, mes, cuenta_filtro, cc_filtro)
except Exception:
    conn = get_conn()
    if conn is None: st.stop()
    df = get_diario(conn, empresa_id, anio, mes, cuenta_filtro, cc_filtro)

if df.empty:
    st.info("Sin registros con esos filtros."); st.stop()

c1,c2,c3 = st.columns(3)
c1.metric("Registros", f"{len(df):,}")
c2.metric("Total Debe",  f"{df['Debe'].sum():,.2f}")
c3.metric("Total Haber", f"{df['Haber'].sum():,.2f}")
st.divider()

fmt = {
    "Debe":  st.column_config.NumberColumn(format="%.2f"),
    "Haber": st.column_config.NumberColumn(format="%.2f"),
}
st.dataframe(df, use_container_width=True, hide_index=True, column_config=fmt)
st.download_button(
    "⬇️ Descargar CSV",
    df.to_csv(index=False).encode('utf-8'),
    f"diario_{empresa_nombre}_{anio}_{mes:02d}.csv",
    "text/csv"
)