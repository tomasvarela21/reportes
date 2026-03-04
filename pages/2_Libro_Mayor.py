import os, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
load_dotenv()

st.set_page_config(page_title="Libro Mayor · ReporteApp", page_icon="📚", layout="wide")
st.markdown("""<style>
[data-testid="stSidebar"]{background:#1a1f2e}
[data-testid="stSidebar"] *{color:#e0e4ef!important}
h1,h2,h3{color:#1a1f2e} #MainMenu{visibility:hidden} footer{visibility:hidden}
</style>""", unsafe_allow_html=True)

EMPRESAS = ["BATIA","GUARE","NORFORK","TORRES","WERCOLICH"]
MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

def get_periodos(conn, empresa):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT periodo_anio, periodo_mes FROM libro_mayor WHERE empresa=%s ORDER BY 1,2", (empresa,))
    rows = cur.fetchall(); cur.close(); return rows

def get_mayor(conn, empresa, anio, mes, nivel, cuenta_filtro, cc_filtro):
    filtros = ["lm.empresa=%s","lm.periodo_anio=%s","lm.periodo_mes=%s","lm.nivel=%s"]
    params  = [empresa, anio, mes, nivel]
    if cuenta_filtro: filtros.append("lm.cuenta_codigo=%s"); params.append(cuenta_filtro)
    if cc_filtro:     filtros.append("lm.centro_costo=%s");  params.append(cc_filtro)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT lm.cuenta_codigo, dc.nombre, lm.tipo_subcuenta, lm.nro_subcuenta,
               lm.centro_costo, lm.total_debe, lm.total_haber, lm.saldo_periodo, lm.saldo_acumulado
        FROM libro_mayor lm LEFT JOIN dim_cuenta dc ON dc.codigo=lm.cuenta_codigo
        WHERE {' AND '.join(filtros)}
        ORDER BY lm.cuenta_codigo, lm.tipo_subcuenta, lm.nro_subcuenta
    """, params)
    cols = ['Cuenta','Nombre','Tipo Subcta','Nro Subcta','Centro Costo',
            'Total Debe','Total Haber','Saldo Período','Saldo Acumulado']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def ejecutar_con_reconexion(fn, *args):
    """Ejecuta fn(conn, *args) reconectando una vez si la conexión está caída."""
    conn = get_conn()
    if conn is None:
        return None
    try:
        return fn(conn, *args)
    except Exception:
        conn = get_conn()
        if conn is None:
            return None
        return fn(conn, *args)

st.title("📚 Libro Mayor")
st.caption("Saldos acumulados por empresa, período, cuenta y centro de costo.")
st.divider()

conn = get_conn()
if conn is None:
    st.stop()

c1,c2,c3,c4,c5 = st.columns(5)
empresa = c1.selectbox("Empresa", EMPRESAS)
periodos = ejecutar_con_reconexion(get_periodos, empresa)
if periodos is None:
    st.stop()
if not periodos:
    st.info(f"No hay datos para **{empresa}**."); st.stop()
opciones = [f"{MESES[m]} {a}" for a,m in periodos]
sel = c2.selectbox("Período", opciones, index=len(opciones)-1)
anio, mes = periodos[opciones.index(sel)]
nivel = c3.selectbox("Nivel", ["cuenta","subcuenta"])
cuenta_raw = c4.text_input("Cuenta", placeholder="ej: 38")
cuenta_filtro = int(cuenta_raw) if cuenta_raw.strip().isdigit() else None
cc_filtro = c5.text_input("Centro costo", placeholder="ej: 1101").strip() or None

st.divider()
df = ejecutar_con_reconexion(get_mayor, empresa, anio, mes, nivel, cuenta_filtro, cc_filtro)
if df is None:
    st.stop()
if df.empty:
    st.info("Sin registros con esos filtros."); st.stop()

c1,c2,c3,c4 = st.columns(4)
c1.metric("Cuentas",         f"{len(df):,}")
c2.metric("Total Debe",      f"{df['Total Debe'].sum():,.2f}")
c3.metric("Total Haber",     f"{df['Total Haber'].sum():,.2f}")
c4.metric("Saldo Acumulado", f"{df['Saldo Acumulado'].sum():,.2f}")
st.divider()

fmt = {"Total Debe":st.column_config.NumberColumn(format="%.2f"),
       "Total Haber":st.column_config.NumberColumn(format="%.2f"),
       "Saldo Período":st.column_config.NumberColumn(format="%.2f"),
       "Saldo Acumulado":st.column_config.NumberColumn(format="%.2f")}
st.dataframe(df, use_container_width=True, hide_index=True, column_config=fmt)
st.download_button("⬇️ Descargar CSV",
    df.to_csv(index=False).encode('utf-8'),
    f"mayor_{empresa}_{anio}_{mes:02d}_{nivel}.csv", "text/csv")