import os, sys, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

st.set_page_config(page_title="Libro Mayor · ReporteApp", page_icon="📚", layout="wide")
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

def get_periodos(conn, empresa_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT periodo_anio, periodo_mes
        FROM libro_mayor
        WHERE empresa_id = %s
        ORDER BY 1, 2
    """, (empresa_id,))
    rows = cur.fetchall(); cur.close(); return rows

def get_mayor(conn, empresa_id, anio, mes, nivel, cuenta_filtro, cc_filtro):
    filtros = ["lm.empresa_id=%s","lm.periodo_anio=%s","lm.periodo_mes=%s","lm.nivel=%s"]
    params  = [empresa_id, anio, mes, nivel]
    if cuenta_filtro: filtros.append("lm.cuenta_codigo=%s"); params.append(cuenta_filtro)
    if cc_filtro:     filtros.append("lm.centro_costo=%s");  params.append(cc_filtro)

    cur = conn.cursor()
    cur.execute(f"""
        SELECT lm.cuenta_codigo, dc.nombre,
               lm.tipo_subcuenta, lm.nro_subcuenta, lm.centro_costo,
               lm.total_debe, lm.total_haber,
               lm.saldo_anterior, lm.saldo_periodo, lm.saldo_acumulado
        FROM libro_mayor lm
        LEFT JOIN dim_cuenta dc ON dc.nro_cta = lm.cuenta_codigo
        WHERE {' AND '.join(filtros)}
        ORDER BY lm.cuenta_codigo, lm.tipo_subcuenta, lm.nro_subcuenta
    """, params)
    cols = ['Cuenta','Nombre','Tipo Subcta','Nro Subcta','Centro Costo',
            'Total Debe','Total Haber','Saldo Anterior','Saldo Período','Saldo Acumulado']
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    return df

st.title("📚 Libro Mayor")
st.caption("Saldos acumulados por empresa, período, cuenta y centro de costo.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

c1,c2,c3,c4,c5 = st.columns(5)
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
sel   = c2.selectbox("Período", opciones, index=len(opciones)-1)
anio, mes = periodos[opciones.index(sel)]
nivel = c3.selectbox("Nivel", ["cuenta","subcuenta"])
cuenta_raw    = c4.text_input("Cuenta", placeholder="ej: 38")
cuenta_filtro = int(cuenta_raw) if cuenta_raw.strip().isdigit() else None
cc_filtro     = c5.text_input("Centro costo", placeholder="ej: 1101").strip() or None

st.divider()

try:
    df = get_mayor(conn, empresa_id, anio, mes, nivel, cuenta_filtro, cc_filtro)
except Exception:
    conn = get_conn()
    if conn is None: st.stop()
    df = get_mayor(conn, empresa_id, anio, mes, nivel, cuenta_filtro, cc_filtro)

if df.empty:
    st.info("Sin registros con esos filtros."); st.stop()

c1,c2,c3,c4,c5 = st.columns(5)
c1.metric("Cuentas",         f"{len(df):,}")
c2.metric("Total Debe",      f"{df['Total Debe'].sum():,.2f}")
c3.metric("Total Haber",     f"{df['Total Haber'].sum():,.2f}")
c4.metric("Saldo Anterior",  f"{df['Saldo Anterior'].sum():,.2f}")
c5.metric("Saldo Acumulado", f"{df['Saldo Acumulado'].sum():,.2f}")
st.divider()

cols_num = ['Total Debe','Total Haber','Saldo Anterior','Saldo Período','Saldo Acumulado']
df_show = df.copy()
for c in cols_num:
    df_show[c] = df_show[c].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "")

st.dataframe(df_show, use_container_width=True, hide_index=True)
st.download_button(
    "⬇️ Descargar CSV",
    df.to_csv(index=False).encode('utf-8'),
    f"mayor_{empresa_nombre}_{anio}_{mes:02d}_{nivel}.csv",
    "text/csv"
)