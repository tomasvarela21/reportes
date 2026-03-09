import os, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

st.set_page_config(page_title="Administración · ReporteApp", page_icon="⚙️", layout="wide")
apply_styles()
render_sidebar()

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

def get_plan_cuentas(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT codigo, nombre, rubro, tipo, es_resultado, moneda, tipo_subcta, activa
        FROM dim_cuenta
        ORDER BY orden_rubro NULLS LAST, codigo
    """)
    rows = cur.fetchall()
    cur.close()
    df = pd.DataFrame(rows, columns=[
        'Código','Nombre','Rubro','Tipo','Es Resultado','Moneda','Tipo Subcta','Activa'
    ])
    moneda_map = {1: 'ARS', 2: 'USD', 3: 'EUR'}
    df['Moneda'] = df['Moneda'].map(moneda_map).fillna(df['Moneda'])
    return df

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
if conn is None: st.stop()

tab1, tab2, tab3, tab4 = st.tabs([
    "🏢 Empresas",
    "📒 Plan de Cuentas",
    "🏷️ Centros de Costo",
    "📋 Log de recálculos",
])

# ─────────────────────────────────────────────
# TAB 1 — Empresas
# ─────────────────────────────────────────────
with tab1:
    df_emp = ejecutar_con_reconexion(get_empresas)
    if df_emp is None: st.stop()
    st.dataframe(df_emp, use_container_width=True, hide_index=True)

# ─────────────────────────────────────────────
# TAB 2 — Plan de Cuentas
# ─────────────────────────────────────────────
with tab2:
    df_pc = ejecutar_con_reconexion(get_plan_cuentas)
    if df_pc is None: st.stop()

    # Métricas
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total cuentas",    len(df_pc))
    col2.metric("Activas",          int(df_pc['Activa'].sum()) if 'Activa' in df_pc else "—")
    col3.metric("Cuentas resultado", int(df_pc['Es Resultado'].sum()) if 'Es Resultado' in df_pc else "—")
    col4.metric("Rubros",           df_pc['Rubro'].nunique())
    st.divider()

    # Filtros
    col_f1, col_f2, col_f3 = st.columns([1, 2, 2])
    with col_f1:
        filtro_codigo = st.text_input(
            "🔍 Filtrar por código",
            placeholder="ej: 1, 38, 176",
            help="Ingresá un código exacto o parte de él"
        )
    with col_f2:
        rubros_disponibles = ["Todos"] + sorted(df_pc['Rubro'].dropna().unique().tolist())
        filtro_rubro = st.selectbox("Filtrar por rubro", rubros_disponibles)
    with col_f3:
        tipos_disponibles = ["Todos"] + sorted(df_pc['Tipo'].dropna().unique().tolist())
        filtro_tipo = st.selectbox("Filtrar por tipo", tipos_disponibles)

    # Aplicar filtros
    df_filtrado = df_pc.copy()
    if filtro_codigo.strip():
        df_filtrado = df_filtrado[
            df_filtrado['Código'].astype(str).str.startswith(filtro_codigo.strip())
        ]
    if filtro_rubro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Rubro'] == filtro_rubro]
    if filtro_tipo != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Tipo'] == filtro_tipo]

    st.caption(f"Mostrando **{len(df_filtrado)}** de {len(df_pc)} cuentas")
    st.dataframe(
        df_filtrado,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Código":        st.column_config.NumberColumn("Código", format="%d"),
            "Es Resultado":  st.column_config.CheckboxColumn("Es Resultado"),
            "Activa":        st.column_config.CheckboxColumn("Activa"),
        }
    )

# ─────────────────────────────────────────────
# TAB 3 — Centros de Costo
# ─────────────────────────────────────────────
with tab3:
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
            cur = conn.cursor(); ok_count = 0
            try:
                for _, row in df_cc.iterrows():
                    cur.execute("""
                        INSERT INTO dim_centro_costo (codigo, descripcion, empresa)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (codigo) DO UPDATE
                            SET descripcion = EXCLUDED.descripcion,
                                empresa     = EXCLUDED.empresa
                    """, (str(row['codigo']).strip(), str(row.get('descripcion','')).strip(),
                          str(row.get('empresa','')).strip() or None))
                    ok_count += 1
                conn.commit()
                st.success(f"✅ {ok_count} centros de costo cargados/actualizados.")
            except Exception as e:
                conn.rollback(); st.error(f"Error: {e}")
            finally:
                cur.close()
    st.divider()
    df_cc_db = ejecutar_con_reconexion(get_centros)
    if df_cc_db is None: st.stop()
    if not df_cc_db.empty:
        st.dataframe(df_cc_db, use_container_width=True, hide_index=True)
    else:
        st.info("No hay centros de costo cargados todavía.")

# ─────────────────────────────────────────────
# TAB 4 — Log de recálculos
# ─────────────────────────────────────────────
with tab4:
    df_log = ejecutar_con_reconexion(get_log)
    if df_log is None: st.stop()
    if df_log.empty:
        st.info("No hay recálculos registrados todavía.")
    else:
        st.dataframe(df_log, use_container_width=True, hide_index=True)