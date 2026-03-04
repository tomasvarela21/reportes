import os, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
load_dotenv()

st.set_page_config(page_title="Administración · ReporteApp", page_icon="⚙️", layout="wide")
st.markdown("""<style>
[data-testid="stSidebar"]{background:#1a1f2e}
[data-testid="stSidebar"] *{color:#e0e4ef!important}
h1,h2,h3{color:#1a1f2e} #MainMenu{visibility:hidden} footer{visibility:hidden}
</style>""", unsafe_allow_html=True)

def ejecutar_con_reconexion(fn, *args):
    conn = get_conn()
    if conn is None: return None
    try:
        return fn(conn, *args)
    except Exception:
        conn = get_conn()
        if conn is None: return None
        return fn(conn, *args)

def get_empresas(conn):
    cur = conn.cursor()
    cur.execute("SELECT codigo, nombre, razon_social, cuit, activa FROM dim_empresa ORDER BY codigo")
    df = pd.DataFrame(cur.fetchall(), columns=['Código','Nombre','Razón Social','CUIT','Activa'])
    cur.close(); return df

def get_centros(conn):
    cur = conn.cursor()
    cur.execute("SELECT codigo, descripcion, empresa, activo FROM dim_centro_costo ORDER BY codigo")
    df = pd.DataFrame(cur.fetchall(), columns=['Código','Descripción','Empresa','Activo'])
    cur.close(); return df

def get_log(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT empresa, desde_anio, desde_mes, hasta_anio, hasta_mes,
               motivo, registros_afectados, duracion_ms, ejecutado_en
        FROM mayor_recalculo_log ORDER BY ejecutado_en DESC LIMIT 100
    """)
    df = pd.DataFrame(cur.fetchall(), columns=[
        'Empresa','Desde Año','Desde Mes','Hasta Año','Hasta Mes',
        'Motivo','Registros','Duración (ms)','Ejecutado en'])
    cur.close(); return df

st.title("⚙️ Administración")
st.caption("Gestión de maestros y operaciones administrativas.")
st.divider()

conn = get_conn()
if conn is None:
    st.stop()

tab1, tab2, tab3 = st.tabs(["🏢 Empresas", "🏷️ Centros de Costo", "📋 Log de recálculos"])

# ── Tab 1: Empresas ───────────────────────────────────────────────────────────
with tab1:
    df_emp = ejecutar_con_reconexion(get_empresas)
    if df_emp is None: st.stop()
    st.dataframe(df_emp, use_container_width=True, hide_index=True)

# ── Tab 2: Centros de Costo ───────────────────────────────────────────────────
with tab2:
    st.markdown("**Cargar maestro de centros de costo (CSV):**")
    st.code("codigo;descripcion;empresa")
    st.caption("La columna 'empresa' puede estar vacía para centros compartidos.")
    archivo_cc = st.file_uploader("CSV centros de costo", type=["csv"], key="cc_upload")

    if archivo_cc:
        df_cc = pd.read_csv(archivo_cc, sep=';', dtype=str, keep_default_na=False)
        df_cc.columns = [c.strip().lower() for c in df_cc.columns]
        st.dataframe(df_cc.head(20), use_container_width=True, hide_index=True)

        if st.button("💾 Cargar centros de costo", type="primary"):
            conn = get_conn()
            if conn is None: st.stop()
            cur = conn.cursor()
            ok_count = 0
            try:
                for _, row in df_cc.iterrows():
                    cur.execute("""
                        INSERT INTO dim_centro_costo (codigo, descripcion, empresa)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (codigo) DO UPDATE
                            SET descripcion = EXCLUDED.descripcion,
                                empresa     = EXCLUDED.empresa
                    """, (
                        str(row['codigo']).strip(),
                        str(row.get('descripcion','')).strip(),
                        str(row.get('empresa','')).strip() or None,
                    ))
                    ok_count += 1
                conn.commit()
                st.success(f"✅ {ok_count} centros de costo cargados/actualizados.")
            except Exception as e:
                conn.rollback()
                st.error(f"Error: {e}")
            finally:
                cur.close()

    st.divider()
    df_cc_db = ejecutar_con_reconexion(get_centros)
    if df_cc_db is None: st.stop()
    if not df_cc_db.empty:
        st.dataframe(df_cc_db, use_container_width=True, hide_index=True)
    else:
        st.info("No hay centros de costo cargados todavía.")

# ── Tab 3: Log de recálculos ──────────────────────────────────────────────────
with tab3:
    df_log = ejecutar_con_reconexion(get_log)
    if df_log is None: st.stop()
    if df_log.empty:
        st.info("No hay recálculos registrados todavía.")
    else:
        st.dataframe(df_log, use_container_width=True, hide_index=True)