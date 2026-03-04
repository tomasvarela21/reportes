import os, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

st.set_page_config(page_title="Consulta Diario · ReporteApp", page_icon="📋", layout="wide")
apply_styles()
render_sidebar()

EMPRESAS = ["BATIA","GUARE","NORFORK","TORRES","WERCOLICH"]
MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

def ejecutar_con_reconexion(fn, *args):
    conn = get_conn()
    if conn is None: return None
    try:
        return fn(conn, *args)
    except Exception:
        conn = get_conn()
        if conn is None: return None
        return fn(conn, *args)

def get_diario(conn, filtros, params):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT ld.empresa, ld.fecha, ld.periodo_anio, ld.periodo_mes,
               ld.nro_asiento, ld.cuenta_codigo, dc.nombre,
               ld.debe, ld.haber, ld.descripcion,
               ld.tipo_subcuenta, ld.nro_subcuenta, ld.centro_costo
        FROM libro_diario ld
        LEFT JOIN dim_cuenta dc ON dc.codigo = ld.cuenta_codigo
        WHERE {' AND '.join(filtros)}
        ORDER BY ld.fecha, ld.nro_asiento, ld.cuenta_codigo
        LIMIT 5000
    """, params)
    cols = ['Empresa','Fecha','Año','Mes','Nro Asiento','Cuenta','Nombre Cuenta',
            'Debe','Haber','Descripción','Tipo Subcta','Nro Subcta','Centro Costo']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

st.title("📋 Consulta de Libro Diario")
st.caption("Consultá los asientos cargados con filtros por empresa, período y cuenta.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

c1,c2,c3,c4 = st.columns(4)
empresa    = c1.selectbox("Empresa", ["Todas"] + EMPRESAS)
anio       = c2.number_input("Año", min_value=2015, max_value=2030, value=2024)
mes_sel    = c3.selectbox("Mes", ["Todos"] + list(MESES.values()))
cuenta_raw = c4.text_input("Cuenta", placeholder="ej: 38")
cuenta_filtro = int(cuenta_raw) if cuenta_raw.strip().isdigit() else None

filtros = ["periodo_anio = %s"]; params = [anio]
if empresa != "Todas":  filtros.append("empresa = %s");      params.append(empresa)
if mes_sel != "Todos":
    mes_num = [k for k,v in MESES.items() if v == mes_sel][0]
    filtros.append("periodo_mes = %s"); params.append(mes_num)
if cuenta_filtro:       filtros.append("cuenta_codigo = %s"); params.append(cuenta_filtro)

st.divider()
df = ejecutar_con_reconexion(get_diario, filtros, params)
if df is None: st.stop()
if df.empty:
    st.info("Sin registros con esos filtros."); st.stop()

c1,c2,c3 = st.columns(3)
c1.metric("Registros",   f"{len(df):,}")
c2.metric("Total Debe",  f"{df['Debe'].sum():,.2f}")
c3.metric("Total Haber", f"{df['Haber'].sum():,.2f}")
st.divider()

fmt = {"Debe":st.column_config.NumberColumn(format="%.2f"),
       "Haber":st.column_config.NumberColumn(format="%.2f"),
       "Fecha":st.column_config.DateColumn("Fecha")}
st.dataframe(df, use_container_width=True, hide_index=True, column_config=fmt)
if len(df) == 5000:
    st.warning("Se muestran los primeros 5.000 registros. Aplicá más filtros para ver todos.")
st.download_button("⬇️ Descargar CSV",
    df.to_csv(index=False).encode('utf-8'),
    f"diario_{empresa}_{anio}.csv", "text/csv")