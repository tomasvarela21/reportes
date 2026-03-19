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

# ── Helpers de normalización ──────────────────────────────────────────────────

def detectar_empresa_archivo(nombre_archivo: str) -> str | None:
    """Detecta la empresa a partir del nombre del archivo (igual que FileParser)."""
    nombre = nombre_archivo.upper()
    for emp in EMPRESAS:
        if emp in nombre:
            return emp
    return None

def reset_carga():
    for k in ['carga_exitosa_apertura']:
        st.session_state.pop(k, None)

def limpiar_tipo_subcta(v) -> str | None:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s in ('', 'nan', 'NaN', '0', '0.0'):
        return None
    try:
        f = float(s)
        if f == 0:
            return None
        return str(int(f))
    except (ValueError, TypeError):
        return s if s else None


def limpiar_ccosto(v) -> str | None:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s in ('', 'nan', 'NaN', '0', '0.0'):
        return None
    try:
        f = float(s)
        if f == 0:
            return None
        return str(int(f))
    except (ValueError, TypeError):
        return s if s else None


def parsear_csv_apertura(df_raw: pd.DataFrame, empresa_id: int | None) -> tuple[pd.DataFrame, list, list]:
    errores = []
    advertencias = []
    df = df_raw.copy()

    df.columns = [c.strip().lower().replace('ï»¿', '').replace('\ufeff', '') for c in df.columns]

    es_formato_a = 'nro_cta' in df.columns and 'sdfinal' in df.columns
    if es_formato_a:
        df = df.rename(columns={
            'nro_cta':     'cuenta_codigo',
            'tipo_subcta': 'tipo_subcuenta',
            'ccosto':      'centro_costo',
            'sdfinal':     'saldo',
        })
        advertencias.append("Formato A detectado — usando columna `sdFinal` como saldo de apertura.")

    if 'cuenta_codigo' not in df.columns:
        errores.append("Columna 'cuenta_codigo' (o 'nro_cta') no encontrada.")
        return pd.DataFrame(), errores, advertencias
    if 'saldo' not in df.columns:
        errores.append("Columna 'saldo' (o 'sdFinal') no encontrada.")
        return pd.DataFrame(), errores, advertencias

    if 'empresa_id' not in df.columns:
        if 'empresa' in df.columns:
            df['empresa_id'] = df['empresa'].str.strip().str.upper().map(EMPRESAS)
            inv = df['empresa_id'].isna()
            if inv.any():
                errores.append(
                    f"Empresas desconocidas en columna 'empresa': "
                    f"{df[inv]['empresa'].unique().tolist()}"
                )
                return pd.DataFrame(), errores, advertencias
            df['empresa_id'] = df['empresa_id'].astype(int)
        elif empresa_id is not None:
            df['empresa_id'] = empresa_id
        else:
            errores.append(
                "No se pudo determinar la empresa. "
                "Usá modo 'por empresa' o incluí columna 'empresa' en el CSV."
            )
            return pd.DataFrame(), errores, advertencias

    df['empresa_id'] = pd.to_numeric(df['empresa_id'], errors='coerce').astype('Int64')

    df['cuenta_codigo'] = pd.to_numeric(
        df['cuenta_codigo'].astype(str).str.strip(),
        errors='coerce'
    )
    n_cta_inv = df['cuenta_codigo'].isna().sum()
    if n_cta_inv > 0:
        advertencias.append(
            f"{n_cta_inv} fila(s) con cuenta_codigo inválido o vacío — serán descartadas."
        )
    df = df[df['cuenta_codigo'].notna()].copy()
    df['cuenta_codigo'] = df['cuenta_codigo'].astype(int)

    df['saldo'] = pd.to_numeric(
        df['saldo'].astype(str)
        .str.strip()
        .str.replace(',', '.', regex=False),
        errors='coerce'
    ).fillna(0.0).round(2)

    for col in ['tipo_subcuenta', 'nro_subcuenta']:
        if col in df.columns:
            df[col] = df[col].apply(limpiar_tipo_subcta)
        else:
            df[col] = None

    if 'centro_costo' in df.columns:
        df['centro_costo'] = df['centro_costo'].apply(limpiar_ccosto)
    else:
        df['centro_costo'] = None

    if es_formato_a and 'sdinicial' in df.columns:
        df['_sdinicial'] = pd.to_numeric(
            df['sdinicial'].astype(str).str.replace(',', '.', regex=False),
            errors='coerce'
        ).fillna(0.0)
        n_diff = (df['saldo'] != df['_sdinicial']).sum()
        if n_diff > 0:
            advertencias.append(
                f"ℹ️ {n_diff} fila(s) tienen sdFinal ≠ sdInicial — "
                f"se usa sdFinal como saldo de apertura."
            )
        df = df.drop(columns=['_sdinicial'], errors='ignore')

    keys = ['empresa_id', 'cuenta_codigo', 'tipo_subcuenta', 'nro_subcuenta', 'centro_costo']
    dupes = df.duplicated(subset=keys, keep=False)
    if dupes.any():
        n_dupes = dupes.sum()
        advertencias.append(
            f"⚠️ {n_dupes} fila(s) duplicadas en el CSV para la misma clave. "
            f"Se tomará la última ocurrencia."
        )
        df = df.drop_duplicates(subset=keys, keep='last')

    df_final = df[[
        'empresa_id', 'cuenta_codigo',
        'tipo_subcuenta', 'nro_subcuenta', 'centro_costo', 'saldo'
    ]].copy()

    return df_final, errores, advertencias


def cargar_apertura_db(conn, df: pd.DataFrame, anio_fiscal: int, archivo_nombre: str) -> tuple[bool, list, list]:
    from mayor_calculator import MayorCalculator
    errores = []
    empresas_ids = [int(x) for x in df['empresa_id'].unique().tolist()]
    cur = conn.cursor()

    try:
        for eid in empresas_ids:
            cur.execute(
                "DELETE FROM saldos_apertura WHERE empresa_id=%s AND anio_fiscal=%s",
                (eid, anio_fiscal)
            )

        ahora = datetime.now()
        registros = []
        for _, row in df.iterrows():
            registros.append((
                int(row['empresa_id']),
                int(anio_fiscal),
                int(row['cuenta_codigo']),
                row['tipo_subcuenta'],
                row['nro_subcuenta'],
                row['centro_costo'],
                float(row['saldo']),
                ahora,
                archivo_nombre,
            ))

        psycopg2.extras.execute_values(cur, """
            INSERT INTO saldos_apertura
                (empresa_id, anio_fiscal, cuenta_codigo,
                 tipo_subcuenta, nro_subcuenta, centro_costo,
                 saldo, cargado_en, archivo_origen)
            VALUES %s
        """, registros, page_size=500)

        conn.commit()

        registros_mayor = 0
        for eid in empresas_ids:
            registros_mayor += MayorCalculator(conn).recalcular(
                eid, anio_fiscal, 1,
                motivo='recarga_apertura'
            )

        return True, empresas_ids, registros_mayor

    except Exception as e:
        conn.rollback()
        return False, [], str(e)
    finally:
        cur.close()


def get_aperturas(conn, filtros, params):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            de.empresa_nombre,
            sa.anio_fiscal,
            sa.cuenta_codigo,
            dc.nombre,
            sa.tipo_subcuenta,
            sa.nro_subcuenta,
            sa.centro_costo,
            sa.saldo,
            sa.cargado_en,
            sa.archivo_origen
        FROM saldos_apertura sa
        LEFT JOIN dim_cuenta  dc ON dc.nro_cta    = sa.cuenta_codigo
        LEFT JOIN dim_empresa de ON de.empresa_id = sa.empresa_id
        WHERE {' AND '.join(filtros)}
        ORDER BY de.empresa_nombre, sa.cuenta_codigo
    """, params)
    cols = [
        'Empresa', 'Año', 'Cuenta', 'Nombre',
        'Tipo Subcta', 'Nro Subcta', 'Centro Costo',
        'Saldo', 'Cargado en', 'Archivo origen'
    ]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    return df


def verificar_apertura_db(conn, empresa_id: int, anio_fiscal: int) -> dict:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)                                              AS total,
            COUNT(DISTINCT cuenta_codigo)                        AS cuentas_unicas,
            SUM(CASE WHEN tipo_subcuenta IS NOT NULL THEN 1 ELSE 0 END) AS con_subcuenta,
            SUM(CASE WHEN centro_costo   IS NOT NULL THEN 1 ELSE 0 END) AS con_ccosto,
            SUM(CASE WHEN saldo != 0     THEN 1 ELSE 0 END)     AS con_saldo_distinto_cero,
            ROUND(SUM(saldo)::numeric, 2)                        AS saldo_total,
            MIN(cargado_en)                                      AS primera_carga,
            MAX(cargado_en)                                      AS ultima_carga
        FROM saldos_apertura
        WHERE empresa_id = %s AND anio_fiscal = %s
    """, (empresa_id, anio_fiscal))
    row = cur.fetchone()
    cur.close()
    if not row or row[0] == 0:
        return {}
    return {
        'total': row[0], 'cuentas_unicas': row[1],
        'con_subcuenta': row[2], 'con_ccosto': row[3],
        'con_saldo_distinto_cero': row[4], 'saldo_total': row[5],
        'primera_carga': row[6], 'ultima_carga': row[7],
    }


# ── UI ────────────────────────────────────────────────────────────────────────

st.title("🏦 Gestión de Saldos de Apertura")
st.caption("Cargá los saldos iniciales por año fiscal. Al recargar se recalcula el Mayor completo.")
st.divider()

conn = get_conn()
if conn is None:
    st.stop()

tab1, tab2 = st.tabs(["📤 Cargar apertura", "🔍 Ver aperturas"])

# ── Tab 1: Cargar ─────────────────────────────────────────────────────────────
with tab1:

    # ── Pantalla de éxito — se evalúa PRIMERO para cortar el flujo ───────────
    if 'carga_exitosa_apertura' in st.session_state:
        r = st.session_state['carga_exitosa_apertura']

        st.success(f"✅ Apertura {r['anio_fiscal']} cargada para **{r['empresas']}**")
        st.divider()

        c1, c2, c3 = st.columns(3)
        c1.metric("Registros cargados", f"{r['registros']:,}")
        c2.metric("Registros en mayor", f"{r['registros_mayor']:,}")
        c3.metric("Archivo",            r['archivo'])

        st.divider()

        # Verificación post-carga
        st.subheader("Verificación post-carga")
        conn2 = get_conn()
        for eid in r['empresas_ids']:
            emp_n = {v: k for k, v in EMPRESAS.items()}.get(eid, str(eid))
            info_post = verificar_apertura_db(conn2, eid, r['anio_fiscal'])
            if info_post:
                vc1, vc2, vc3, vc4 = st.columns(4)
                vc1.metric(f"{emp_n} — Registros",  f"{info_post['total']:,}")
                vc2.metric("Cuentas únicas",         f"{info_post['cuentas_unicas']:,}")
                vc3.metric("Con subcuenta",           f"{info_post['con_subcuenta']:,}")
                vc4.metric("Saldo total",             f"{info_post['saldo_total']:,.2f}")

        st.divider()
        if st.button("🏦 Cargar otro archivo", type="primary"):
            reset_carga()
            st.rerun()
        st.stop()

    # ── Configuración ─────────────────────────────────────────────────────────
    c1, c2 = st.columns([1, 3])
    anio_fiscal = c1.number_input("Año fiscal", min_value=2015, max_value=2030, value=2024)

    modo = c2.radio(
        "Modo de carga",
        ["📋 Archivo consolidado (múltiples empresas)", "🏢 Archivo por empresa"],
        horizontal=True,
    )
    st.divider()

    if modo == "📋 Archivo consolidado (múltiples empresas)":
        st.markdown("**Formato CSV esperado (separador `;`):**")
        st.code("empresa;cuenta_codigo;tipo_subcuenta;nro_subcuenta;centro_costo;saldo")
        st.caption("empresa = nombre de la empresa (BATIA, NORFORK, etc.)")
        empresa_id_sel = None
        empresa_nombre_sel = None

        archivo = st.file_uploader("CSV de saldos de apertura", type=["csv"], key="ap_file_multi")

    else:
        st.markdown("**Formatos CSV aceptados (separador `;`):**")
        col_a, col_b = st.columns(2)
        with col_a:
            st.caption("Formato estándar:")
            st.code("cuenta_codigo;tipo_subcuenta;nro_subcuenta;centro_costo;saldo")
        with col_b:
            st.caption("Formato original del sistema:")
            st.code("nro_cta;tipo_subcta;nro_subcuenta;ccosto;sdInicial;totalDebe;totalHaber;sdFinal")

        # ── File uploader primero para poder detectar empresa ─────────────────
        archivo = st.file_uploader("CSV de saldos de apertura", type=["csv"], key="ap_file_single")

        # Autodetección de empresa desde nombre del archivo
        if archivo and 'empresa_sugerida_apertura' not in st.session_state:
            detectada = detectar_empresa_archivo(archivo.name)
            if detectada:
                st.session_state['empresa_sugerida_apertura'] = detectada
                st.rerun()

        # Selectbox de empresa con sugerencia aplicada
        empresas_lista = list(EMPRESAS.keys())
        idx_default = empresas_lista.index(st.session_state['empresa_sugerida_apertura']) \
            if st.session_state.get('empresa_sugerida_apertura') in empresas_lista else 0

        empresa_nombre_sel = st.selectbox(
            "Empresa", empresas_lista, index=idx_default,
            key="ap_empresa_sel"
        )
        empresa_id_sel = EMPRESAS[empresa_nombre_sel]

        # Limpiar sugerencia si el usuario cambia el archivo
        if not archivo:
            st.session_state.pop('empresa_sugerida_apertura', None)

        # Mostrar estado actual en DB
        info_actual = verificar_apertura_db(conn, empresa_id_sel, anio_fiscal)
        if info_actual:
            with st.expander(
                f"📊 Apertura actual en DB — {empresa_nombre_sel} {anio_fiscal} "
                f"({info_actual['total']:,} registros)",
                expanded=False
            ):
                cc1, cc2, cc3, cc4 = st.columns(4)
                cc1.metric("Registros",      f"{info_actual['total']:,}")
                cc2.metric("Cuentas únicas", f"{info_actual['cuentas_unicas']:,}")
                cc3.metric("Con subcuenta",  f"{info_actual['con_subcuenta']:,}")
                cc4.metric("Con ccosto",     f"{info_actual['con_ccosto']:,}")
                cc5, cc6 = st.columns(2)
                cc5.metric("Saldo total",   f"{info_actual['saldo_total']:,.2f}")
                cc6.metric("Última carga",  str(info_actual['ultima_carga'])[:16] if info_actual['ultima_carga'] else '—')
        else:
            st.info(f"ℹ️ No hay apertura cargada para {empresa_nombre_sel} {anio_fiscal}.")

    if archivo:
        # ── Parseo y preview ──────────────────────────────────────────────────
        df_raw = pd.read_csv(
            archivo, sep=';', dtype=str,
            keep_default_na=False, encoding='utf-8-sig'
        )
        df_prev, errores_prev, advertencias_prev = parsear_csv_apertura(df_raw, empresa_id_sel)

        for adv in advertencias_prev:
            st.info(adv)

        if errores_prev:
            for e in errores_prev:
                st.error(f"❌ {e}")
            st.stop()

        if df_prev.empty:
            st.warning("El archivo no contiene registros válidos.")
            st.stop()

        # ── Métricas de preview ───────────────────────────────────────────────
        empresas_preview = [int(x) for x in df_prev['empresa_id'].unique().tolist()]
        nombres_prev = [k for k, v in EMPRESAS.items() if v in empresas_preview]

        st.subheader("Vista previa del archivo")
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Registros",      f"{len(df_prev):,}")
        col2.metric("Empresa(s)",     ', '.join(nombres_prev))
        col3.metric("Cuentas únicas", f"{df_prev['cuenta_codigo'].nunique():,}")
        col4.metric("Con subcuenta",  f"{df_prev['tipo_subcuenta'].notna().sum():,}")
        col5.metric("Con ccosto",     f"{df_prev['centro_costo'].notna().sum():,}")

        with st.expander("📊 Resumen por empresa", expanded=True):
            resumen = df_prev.groupby('empresa_id').agg(
                registros=('cuenta_codigo', 'count'),
                cuentas_unicas=('cuenta_codigo', 'nunique'),
                saldo_total=('saldo', 'sum'),
                con_saldo_cero=('saldo', lambda x: (x == 0).sum()),
                con_subcuenta=('tipo_subcuenta', lambda x: x.notna().sum()),
                con_ccosto=('centro_costo', lambda x: x.notna().sum()),
            ).reset_index()
            resumen['empresa_id'] = resumen['empresa_id'].map(
                {v: k for k, v in EMPRESAS.items()}
            )
            resumen.columns = [
                'Empresa', 'Registros', 'Cuentas únicas',
                'Saldo total', 'Saldo = 0', 'Con subcuenta', 'Con ccosto'
            ]
            st.dataframe(resumen, use_container_width=True, hide_index=True,
                column_config={"Saldo total": st.column_config.NumberColumn(format="%.2f")})

        st.dataframe(df_prev.head(30), use_container_width=True, hide_index=True)

        # ── Validación cruzada vs DB actual ───────────────────────────────────
        for eid in empresas_preview:
            info = verificar_apertura_db(conn, eid, anio_fiscal)
            emp_nombre = {v: k for k, v in EMPRESAS.items()}.get(eid, str(eid))
            if info:
                diff_registros = len(df_prev[df_prev['empresa_id'] == eid]) - info['total']
                if abs(diff_registros) > 0:
                    signo = '+' if diff_registros > 0 else ''
                    st.warning(
                        f"⚠️ **{emp_nombre}**: el archivo tiene "
                        f"{signo}{diff_registros:,} registros respecto a la apertura actual en DB."
                    )

        # ── Confirmación y carga ──────────────────────────────────────────────
        st.divider()
        st.warning(
            f"⚠️ Esto **eliminará** todos los saldos de apertura del año **{anio_fiscal}** "
            f"para **{', '.join(nombres_prev)}** y recalculará el Libro Mayor completo "
            f"desde {anio_fiscal}/01 en adelante."
        )
        confirmar = st.checkbox(
            f"✅ Confirmo que quiero reemplazar los saldos de apertura "
            f"{anio_fiscal} para {', '.join(nombres_prev)}."
        )

        if confirmar:
            if st.button("🏦 Cargar saldos de apertura", type="primary"):
                archivo.seek(0)
                conn2 = get_conn()
                if conn2 is None:
                    st.stop()
                with st.spinner("Cargando saldos y recalculando Mayor..."):
                    ok, empresas_cargadas, registros_mayor = cargar_apertura_db(
                        conn2, df_prev, anio_fiscal, archivo.name
                    )
                if ok:
                    nombres_ok = [k for k, v in EMPRESAS.items() if v in empresas_cargadas]
                    st.session_state['carga_exitosa_apertura'] = {
                        'anio_fiscal':    anio_fiscal,
                        'empresas':       ', '.join(nombres_ok),
                        'empresas_ids':   empresas_cargadas,
                        'registros':      len(df_prev),
                        'registros_mayor': registros_mayor,
                        'archivo':        archivo.name,
                    }
                    st.session_state.pop('empresa_sugerida_apertura', None)
                    st.rerun()
                else:
                    st.error(f"❌ {registros_mayor}")  # registros_mayor contiene el error en este caso

# ── Tab 2: Ver aperturas ──────────────────────────────────────────────────────
with tab2:
    c1, c2, c3 = st.columns(3)
    empresa_q  = c1.selectbox("Empresa", ["Todas"] + list(EMPRESAS.keys()), key="ap_emp")
    anio_q     = c2.number_input("Año fiscal", min_value=2015, max_value=2030, value=2024, key="ap_anio")
    solo_saldo = c3.checkbox("Solo registros con saldo ≠ 0", value=False, key="ap_saldo")

    filtros = ["sa.anio_fiscal = %s"]
    params  = [anio_q]
    if empresa_q != "Todas":
        filtros.append("sa.empresa_id = %s")
        params.append(EMPRESAS[empresa_q])
    if solo_saldo:
        filtros.append("sa.saldo != 0")

    try:
        df_ap = get_aperturas(conn, filtros, params)
    except Exception:
        conn = get_conn()
        if conn is None:
            st.stop()
        df_ap = get_aperturas(conn, filtros, params)

    if df_ap.empty:
        st.info("No hay saldos de apertura cargados con esos filtros.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registros",      f"{len(df_ap):,}")
        c2.metric("Cuentas únicas", f"{df_ap['Cuenta'].nunique():,}")
        c3.metric("Con subcuenta",  f"{df_ap['Tipo Subcta'].notna().sum():,}")
        c4.metric("Saldo total",    f"{df_ap['Saldo'].sum():,.2f}")

        st.dataframe(
            df_ap, use_container_width=True, hide_index=True,
            column_config={
                "Saldo":      st.column_config.NumberColumn(format="%.2f"),
                "Cargado en": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm"),
            }
        )