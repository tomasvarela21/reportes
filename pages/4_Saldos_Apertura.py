import os, sys, streamlit as st, pandas as pd, psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

st.set_page_config(page_title="Saldos Apertura · ReporteApp", page_icon="🏦", layout="wide")
apply_styles()
render_sidebar()

EMPRESAS = {
    'BATIA':     1,
    'GUARE':     3,
    'NORFORK':   2,
    'TORRES':    4,
    'WERCOLICH': 5,
}

def cargar_apertura_csv(conn, df, empresa_id, anio_fiscal, archivo_nombre):
    from mayor_calculator import MayorCalculator
    errores = []
    for col in ['cuenta_codigo', 'saldo']:
        if col not in df.columns:
            errores.append(f"Falta columna '{col}'")
    if errores:
        return False, [], errores

    # Si el archivo tiene columna empresa_id o empresa, mapear
    if 'empresa_id' in df.columns:
        empresas_ids = df['empresa_id'].astype(int).unique().tolist()
    elif 'empresa' in df.columns:
        emp_map = {v: k for k, v in EMPRESAS.items()}  # nombre→id invertido
        emp_map_fwd = EMPRESAS
        df = df.copy()
        df['empresa_id'] = df['empresa'].str.strip().str.upper().map(emp_map_fwd)
        invalidas = df['empresa_id'].isna()
        if invalidas.any():
            errores.append(f"Empresas desconocidas: {df[invalidas]['empresa'].unique().tolist()}")
            return False, [], errores
        df['empresa_id'] = df['empresa_id'].astype(int)
        empresas_ids = df['empresa_id'].unique().tolist()
    else:
        empresas_ids = [empresa_id]
        df = df.copy()
        df['empresa_id'] = empresa_id

    cur = conn.cursor()
    try:
        for eid in empresas_ids:
            cur.execute("DELETE FROM saldos_apertura WHERE empresa_id=%s AND anio_fiscal=%s", (eid, anio_fiscal))

        registros = []
        for _, row in df.iterrows():
            registros.append((
                int(row['empresa_id']),
                int(anio_fiscal),
                int(row['cuenta_codigo']),
                str(row.get('tipo_subcuenta', '') or row.get('tipo_subcta', '')).strip() or None,
                str(row.get('nro_subcuenta', '')).strip() or None,
                str(row.get('centro_costo', '') or row.get('ccosto', '')).strip() or None,
                float(row['saldo']),
                datetime.now(),
                archivo_nombre,
            ))

        psycopg2.extras.execute_values(cur, """
            INSERT INTO saldos_apertura
                (empresa_id, anio_fiscal, cuenta_codigo, tipo_subcuenta,
                 nro_subcuenta, centro_costo, saldo, cargado_en, archivo_origen)
            VALUES %s
        """, registros, page_size=500)
        conn.commit()

        for eid in empresas_ids:
            from mayor_calculator import MayorCalculator
            MayorCalculator(conn).recalcular(eid, anio_fiscal, 1, motivo='recarga_apertura')

        return True, empresas_ids, []
    except Exception as e:
        conn.rollback()
        return False, [], [str(e)]

def get_aperturas(conn, filtros, params):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT de.empresa_nombre, sa.anio_fiscal, sa.cuenta_codigo, dc.nombre,
               sa.tipo_subcuenta, sa.nro_subcuenta, sa.centro_costo, sa.saldo, sa.cargado_en
        FROM saldos_apertura sa
        LEFT JOIN dim_cuenta dc ON dc.nro_cta = sa.cuenta_codigo
        LEFT JOIN dim_empresa de ON de.empresa_id = sa.empresa_id
        WHERE {' AND '.join(filtros)}
        ORDER BY de.empresa_nombre, sa.cuenta_codigo
    """, params)
    cols = ['Empresa','Año','Cuenta','Nombre','Tipo Subcta','Nro Subcta','Centro Costo','Saldo','Cargado en']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

st.title("🏦 Gestión de Saldos de Apertura")
st.caption("Cargá los saldos iniciales por año fiscal. Al recargar se recalcula el Mayor completo.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

tab1, tab2 = st.tabs(["📤 Cargar apertura", "🔍 Ver aperturas"])

with tab1:
    c1, c2 = st.columns([1, 3])
    anio_fiscal = c1.number_input("Año fiscal", min_value=2015, max_value=2030, value=2024)

    modo = c2.radio(
        "Modo de carga",
        ["📋 Archivo consolidado (múltiples empresas)", "🏢 Archivo por empresa"],
        horizontal=True,
    )
    st.divider()

    if modo == "📋 Archivo consolidado (múltiples empresas)":
        st.markdown("**Formato CSV esperado:**")
        st.code("empresa;cuenta_codigo;tipo_subcuenta;nro_subcuenta;centro_costo;saldo")
        st.caption("tipo_subcuenta, nro_subcuenta y centro_costo pueden estar vacíos.")
        empresa_id_sel = None
        empresa_nombre_sel = None
    else:
        st.markdown("**Formato CSV esperado** (sin columna empresa):")
        st.code("cuenta_codigo;tipo_subcuenta;nro_subcuenta;centro_costo;saldo")
        st.caption("También se acepta el formato original: nro_cta;tipo_subcta;nro_subcuenta;ccosto;sdInicial;totalDebe;totalHaber;sdFinal")
        empresa_nombre_sel = st.selectbox("Empresa", list(EMPRESAS.keys()))
        empresa_id_sel = EMPRESAS[empresa_nombre_sel]

    archivo = st.file_uploader("CSV de saldos de apertura", type=["csv"])

    if archivo:
        df_prev = pd.read_csv(archivo, sep=';', dtype=str, keep_default_na=False)
        df_prev.columns = [c.strip().lower() for c in df_prev.columns]

        es_formato_original = 'nro_cta' in df_prev.columns and 'sdfinal' in df_prev.columns
        if es_formato_original:
            df_prev = df_prev.rename(columns={
                'nro_cta':     'cuenta_codigo',
                'tipo_subcta': 'tipo_subcuenta',
                'ccosto':      'centro_costo',
                'sdfinal':     'saldo',
            })
            df_prev['saldo_num'] = pd.to_numeric(df_prev['saldo'], errors='coerce').fillna(0)
            mask = (
                (df_prev['saldo_num'] != 0) |
                (df_prev.get('tipo_subcuenta', pd.Series(dtype=str)).str.strip() != '') |
                (df_prev.get('nro_subcuenta',  pd.Series(dtype=str)).str.strip() != '') |
                (df_prev.get('centro_costo',   pd.Series(dtype=str)).str.strip() != '')
            )
            df_prev = df_prev[mask].drop(columns=['saldo_num'], errors='ignore')
            st.info(f"📋 Formato original detectado — usando `sdFinal` como saldo. {len(df_prev):,} filas.")

        if empresa_id_sel and 'empresa_id' not in df_prev.columns and 'empresa' not in df_prev.columns:
            df_prev.insert(0, 'empresa_id', empresa_id_sel)

        st.dataframe(df_prev.head(20), use_container_width=True, hide_index=True)

        if 'empresa_id' in df_prev.columns:
            ids_detectados = df_prev['empresa_id'].astype(str).unique().tolist()
            label_empresas = ', '.join(ids_detectados)
        elif 'empresa' in df_prev.columns:
            label_empresas = ', '.join(df_prev['empresa'].str.upper().unique().tolist())
        else:
            label_empresas = empresa_nombre_sel

        st.caption(f"{len(df_prev):,} registros — empresa(s): {label_empresas}")
        st.warning(
            f"⚠️ Esto reemplazará todos los saldos de apertura del año **{anio_fiscal}** "
            f"para: **{label_empresas}** y recalculará el Libro Mayor."
        )
        confirmar = st.checkbox(f"Confirmo que quiero reemplazar los saldos de apertura {anio_fiscal}.")

        if st.button("🏦 Cargar saldos de apertura", type="primary", disabled=not confirmar):
            archivo.seek(0)
            df_carga = pd.read_csv(archivo, sep=';', dtype=str, keep_default_na=False)
            df_carga.columns = [c.strip().lower() for c in df_carga.columns]

            if es_formato_original:
                df_carga = df_carga.rename(columns={
                    'nro_cta':     'cuenta_codigo',
                    'tipo_subcta': 'tipo_subcuenta',
                    'ccosto':      'centro_costo',
                    'sdfinal':     'saldo',
                })
                df_carga['saldo_num'] = pd.to_numeric(df_carga['saldo'], errors='coerce').fillna(0)
                mask = (
                    (df_carga['saldo_num'] != 0) |
                    (df_carga.get('tipo_subcuenta', pd.Series(dtype=str)).str.strip() != '') |
                    (df_carga.get('nro_subcuenta',  pd.Series(dtype=str)).str.strip() != '') |
                    (df_carga.get('centro_costo',   pd.Series(dtype=str)).str.strip() != '')
                )
                df_carga = df_carga[mask].drop(columns=['saldo_num'], errors='ignore')

            if empresa_id_sel and 'empresa_id' not in df_carga.columns and 'empresa' not in df_carga.columns:
                df_carga.insert(0, 'empresa_id', empresa_id_sel)

            conn = get_conn()
            if conn is None: st.stop()
            with st.spinner("Cargando y recalculando Mayor..."):
                ok, empresas_cargadas, errores = cargar_apertura_csv(
                    conn, df_carga, empresa_id_sel, anio_fiscal, archivo.name
                )
            if ok:
                st.success(f"✅ Saldos {anio_fiscal} cargados para empresa_id(s): **{empresas_cargadas}**. Mayor recalculado.")
            else:
                for e in errores: st.error(f"• {e}")

with tab2:
    c1, c2 = st.columns(2)
    empresa_q  = c1.selectbox("Empresa", ["Todas"] + list(EMPRESAS.keys()), key="ap_emp")
    anio_q     = c2.number_input("Año fiscal", min_value=2015, max_value=2030, value=2024, key="ap_anio")
    filtros = ["sa.anio_fiscal = %s"]; params = [anio_q]
    if empresa_q != "Todas":
        filtros.append("sa.empresa_id = %s"); params.append(EMPRESAS[empresa_q])
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