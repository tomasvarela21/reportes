import os, sys, streamlit as st, pandas as pd, psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

st.set_page_config(page_title="Saldos Apertura · ReporteApp", page_icon="🏦", layout="wide")
apply_styles()

EMPRESAS = ["BATIA","GUARE","NORFORK","TORRES","WERCOLICH"]

def cargar_apertura_csv(conn, df, anio_fiscal, archivo_nombre):
    from mayor_calculator import MayorCalculator  # type: ignore

    errores = []
    for col in ['empresa','cuenta_codigo','saldo']:
        if col not in df.columns: errores.append(f"Falta columna '{col}'")
    if errores: return False, errores

    empresas_en_archivo = df['empresa'].unique().tolist()
    cur = conn.cursor()
    try:
        for emp in empresas_en_archivo:
            cur.execute("DELETE FROM saldos_apertura WHERE empresa=%s AND anio_fiscal=%s",
                        (emp, anio_fiscal))
        registros = []
        for _, row in df.iterrows():
            registros.append((
                str(row['empresa']),
                int(anio_fiscal),
                int(row['cuenta_codigo']),
                str(row.get('tipo_subcuenta','')) or None,
                str(row.get('nro_subcuenta',''))  or None,
                str(row.get('centro_costo',''))   or None,
                float(row['saldo']),
                datetime.now(),
                archivo_nombre,
            ))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO saldos_apertura
                (empresa, anio_fiscal, cuenta_codigo, tipo_subcuenta,
                 nro_subcuenta, centro_costo, saldo, cargado_en, archivo_origen)
            VALUES %s
        """, registros, page_size=500)
        conn.commit()
        for emp in empresas_en_archivo:
            MayorCalculator(conn).recalcular(emp, anio_fiscal, 1, motivo='recarga_apertura')
        return True, []
    except Exception as e:
        conn.rollback()
        return False, [str(e)]

def get_aperturas(conn, filtros, params):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT sa.empresa, sa.anio_fiscal, sa.cuenta_codigo, dc.nombre,
               sa.tipo_subcuenta, sa.nro_subcuenta, sa.centro_costo, sa.saldo, sa.cargado_en
        FROM saldos_apertura sa
        LEFT JOIN dim_cuenta dc ON dc.codigo = sa.cuenta_codigo
        WHERE {' AND '.join(filtros)}
        ORDER BY sa.empresa, sa.cuenta_codigo
    """, params)
    cols = ['Empresa','Año','Cuenta','Nombre','Tipo Subcta','Nro Subcta','Centro Costo','Saldo','Cargado en']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close()
    return df

st.title("🏦 Gestión de Saldos de Apertura")
st.caption("Cargá los saldos iniciales por año fiscal. Al recargar se recalcula el Mayor completo.")
st.divider()

conn = get_conn()
if conn is None:
    st.stop()

tab1, tab2 = st.tabs(["📤 Cargar apertura", "🔍 Ver aperturas"])

# ── Tab 1: Carga ──────────────────────────────────────────────────────────────
with tab1:
    st.markdown("**Formato CSV esperado:**")
    st.code("empresa;cuenta_codigo;tipo_subcuenta;nro_subcuenta;centro_costo;saldo")
    st.caption("tipo_subcuenta, nro_subcuenta y centro_costo pueden estar vacíos.")
    st.divider()

    c1, c2 = st.columns([1,3])
    anio_fiscal = c1.number_input("Año fiscal", min_value=2015, max_value=2030, value=2024)
    archivo     = c2.file_uploader("CSV consolidado de saldos de apertura", type=["csv"])

    if archivo:
        df_prev = pd.read_csv(archivo, sep=';', dtype=str, keep_default_na=False)
        df_prev.columns = [c.strip().lower() for c in df_prev.columns]
        st.dataframe(df_prev.head(20), use_container_width=True, hide_index=True)
        st.caption(f"{len(df_prev):,} registros — empresas: {', '.join(df_prev['empresa'].unique()) if 'empresa' in df_prev.columns else '?'}")

        st.warning(
            f"⚠️ Esto reemplazará todos los saldos de apertura del año **{anio_fiscal}** "
            f"para las empresas del archivo y recalculará el Libro Mayor completo desde enero {anio_fiscal}.")
        confirmar = st.checkbox(f"Confirmo que quiero reemplazar los saldos de apertura {anio_fiscal}.")

        if st.button("🏦 Cargar saldos de apertura", type="primary", disabled=not confirmar):
            archivo.seek(0)
            df_carga = pd.read_csv(archivo, sep=';', dtype=str, keep_default_na=False)
            df_carga.columns = [c.strip().lower() for c in df_carga.columns]
            # Reconectar si es necesario antes de la operación larga
            conn = get_conn()
            if conn is None: st.stop()
            with st.spinner("Cargando y recalculando Mayor..."):
                ok, errores = cargar_apertura_csv(conn, df_carga, anio_fiscal, archivo.name)
            if ok:
                st.success(f"✅ Saldos de apertura {anio_fiscal} cargados y Mayor recalculado.")
            else:
                for e in errores: st.error(f"• {e}")

# ── Tab 2: Consulta ───────────────────────────────────────────────────────────
with tab2:
    c1, c2 = st.columns(2)
    empresa_q = c1.selectbox("Empresa", ["Todas"] + EMPRESAS, key="ap_emp")
    anio_q    = c2.number_input("Año fiscal", min_value=2015, max_value=2030, value=2024, key="ap_anio")

    filtros = ["anio_fiscal = %s"]; params = [anio_q]
    if empresa_q != "Todas": filtros.append("empresa = %s"); params.append(empresa_q)

    try:
        df_ap = get_aperturas(conn, filtros, params)
    except Exception:
        conn = get_conn()
        if conn is None: st.stop()
        df_ap = get_aperturas(conn, filtros, params)

    if df_ap.empty:
        st.info("No hay saldos de apertura cargados con esos filtros.")
    else:
        st.metric("Registros", f"{len(df_ap):,}")
        st.dataframe(df_ap, use_container_width=True, hide_index=True,
            column_config={"Saldo": st.column_config.NumberColumn(format="%.2f")})