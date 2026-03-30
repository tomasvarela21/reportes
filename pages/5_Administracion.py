import os, sys, streamlit as st, pandas as pd, psycopg2.extras
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

st.set_page_config(page_title="Administración · ReporteApp", page_icon="⚙️", layout="wide")
apply_styles()
render_sidebar()

EMPRESAS = {
    'BATIA':     1,
    'GUARE':     3,
    'NORFORK':   2,
    'TORRES':    4,
    'WERCOLICH': 5,
}

# ── Helpers de DB ──────────────────────────────────────────────────────────────

def get_plan_cuentas(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT nro_cta, extendido, nombre, rubro, sub_rubro, analisis, fases,
               tipo, moneda, activa, es_resultado, nivel_1, nivel_2, nivel_3
        FROM dim_cuenta ORDER BY nro_cta
    """)
    cols = ['Nro Cta','Extendido','Nombre','Rubro','Sub-rubro','Analisis','Fases',
            'Tipo','Moneda','Activa','Es Resultado','Nivel 1','Nivel 2','Nivel 3']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def get_rubros(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT rubro FROM dim_cuenta WHERE rubro IS NOT NULL AND rubro != '' ORDER BY rubro")
    rubros = [r[0] for r in cur.fetchall()]; cur.close(); return rubros

def get_subrubros_por_rubro(conn, rubro):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT sub_rubro FROM dim_cuenta
        WHERE rubro = %s AND sub_rubro IS NOT NULL AND sub_rubro != ''
        ORDER BY sub_rubro
    """, (rubro,))
    subs = [r[0] for r in cur.fetchall()]; cur.close(); return subs

def get_analisis_por_subrubro(conn, subrubro):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT analisis FROM dim_cuenta
        WHERE sub_rubro = %s AND analisis IS NOT NULL AND analisis != ''
        ORDER BY analisis
    """, (subrubro,))
    vals = [r[0] for r in cur.fetchall()]; cur.close(); return vals

def get_fases(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT fases FROM dim_cuenta WHERE fases IS NOT NULL AND fases != '' ORDER BY fases")
    vals = [r[0] for r in cur.fetchall()]; cur.close(); return vals

def get_empresas(conn):
    cur = conn.cursor()
    cur.execute("SELECT empresa_id, empresa_nombre, grupo, activa FROM dim_empresa ORDER BY empresa_id")
    cols = ['ID','Empresa','Grupo','Activa']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def get_centros(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT cc.codigo, cc.descripcion, de.empresa_nombre, cc.activo
        FROM dim_centro_costo cc
        LEFT JOIN dim_empresa de ON de.empresa_id = cc.empresa_id
        ORDER BY cc.codigo
    """)
    cols = ['Código','Descripción','Empresa','Activo']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def get_log(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT de.empresa_nombre, mrl.desde_anio, mrl.desde_mes, mrl.hasta_anio, mrl.hasta_mes,
               mrl.motivo, mrl.registros_afectados, mrl.duracion_ms, mrl.ejecutado_en
        FROM mayor_recalculo_log mrl
        LEFT JOIN dim_empresa de ON de.empresa_id = mrl.empresa_id
        ORDER BY mrl.ejecutado_en DESC LIMIT 100
    """)
    cols = ['Empresa','Desde Año','Desde Mes','Hasta Año','Hasta Mes',
            'Motivo','Registros','Duración ms','Ejecutado en']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def get_cuentas_faltantes_diario(conn, nros_plan: set) -> list:
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT cuenta_codigo FROM libro_diario ORDER BY cuenta_codigo")
    en_diario = {r[0] for r in cur.fetchall()}
    cur.close()
    return sorted(en_diario - nros_plan)

def get_proyectos(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT ccosto, nombre, fc_inicio, fc_fin, ingresos,
               cto_mo_propia, cto_mo_terceros, cto_materiales,
               cto_herramientas, cto_diversos,
               superficie, avance, horas, actualizado_en
        FROM proyectos ORDER BY ccosto
    """)
    cols = ['Centro Costo','Nombre','Inicio','Fin','Ingresos',
            'Cto MO Propia','Cto MO Terceros','Cto Materiales',
            'Cto Herramientas','Cto Diversos',
            'Superficie','Avance','Horas','Actualizado']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def validar_cuenta_nueva(conn, nro_cta, nombre):
    errores = []
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM dim_cuenta WHERE nro_cta = %s", (nro_cta,))
    if cur.fetchone():
        errores.append(f"El Nro de cuenta **{nro_cta}** ya existe en el plan.")
    cur.execute("SELECT 1 FROM dim_cuenta WHERE LOWER(nombre) = LOWER(%s)", (nombre,))
    if cur.fetchone():
        errores.append(f"Ya existe una cuenta con el nombre **{nombre}**.")
    cur.close(); return errores

def validar_rubro_nuevo(conn, rubro_nombre):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM dim_cuenta WHERE LOWER(rubro) = LOWER(%s)", (rubro_nombre,))
    existe = cur.fetchone() is not None; cur.close(); return existe

def validar_subrubro_nuevo(conn, rubro, subrubro_nombre):
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM dim_cuenta
        WHERE LOWER(rubro) = LOWER(%s) AND LOWER(sub_rubro) = LOWER(%s)
    """, (rubro, subrubro_nombre))
    existe = cur.fetchone() is not None; cur.close(); return existe

def validar_analisis_nuevo(conn, subrubro, analisis_nombre):
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM dim_cuenta
        WHERE LOWER(sub_rubro) = LOWER(%s) AND LOWER(analisis) = LOWER(%s)
    """, (subrubro, analisis_nombre))
    existe = cur.fetchone() is not None; cur.close(); return existe


# ── Parsers ───────────────────────────────────────────────────────────────────

def parsear_plan_cuentas(archivo) -> tuple:
    errores = []; advertencias = []
    nombre = getattr(archivo, 'name', '')
    try:
        if nombre.endswith('.xlsx') or nombre.endswith('.xls'):
            df = pd.read_excel(archivo, dtype=str)
            advertencias.append("Formato Excel detectado.")
        else:
            for enc in ['utf-8-sig', 'latin-1', 'utf-8']:
                try:
                    archivo.seek(0)
                    df = pd.read_csv(archivo, sep=';', dtype=str, keep_default_na=False, encoding=enc)
                    advertencias.append(f"Formato CSV detectado (encoding: {enc}).")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                errores.append("No se pudo leer el archivo.")
                return pd.DataFrame(), errores, advertencias
    except Exception as e:
        errores.append(f"Error al leer el archivo: {e}")
        return pd.DataFrame(), errores, advertencias

    df.columns = [c.strip() for c in df.columns]
    col_map = {
        'nro_cta':'nro_cta','Nro Cta':'nro_cta','NroCta':'nro_cta',
        'Extendido':'extendido','extendido':'extendido',
        'Nombre':'nombre','nombre':'nombre',
        'Rubro':'rubro','rubro':'rubro',
        'SubRubro':'sub_rubro','Sub-rubro':'sub_rubro','sub_rubro':'sub_rubro',
        'Analisis':'analisis','analisis':'analisis','Análisis':'analisis',
        'Fases':'fases','fases':'fases',
        'Tipo':'tipo','tipo':'tipo',
        'Moneda':'moneda','moneda':'moneda',
        'Activa':'activa','activa':'activa',
        'EsResultado':'es_resultado','Es Resultado':'es_resultado','es_resultado':'es_resultado',
        'Nivel 1':'nivel_1','nivel_1':'nivel_1',
        'Nivel 2':'nivel_2','nivel_2':'nivel_2',
        'Nivel 3':'nivel_3','nivel_3':'nivel_3',
    }
    df = df.rename(columns={c: col_map[c] for c in df.columns if c in col_map})
    if 'nro_cta' not in df.columns:
        errores.append("No se encontró columna de número de cuenta.")
        return pd.DataFrame(), errores, advertencias
    if 'nombre' not in df.columns:
        errores.append("No se encontró columna 'Nombre'.")
        return pd.DataFrame(), errores, advertencias

    df['nro_cta'] = pd.to_numeric(df['nro_cta'].astype(str).str.strip(), errors='coerce')
    n_inv = df['nro_cta'].isna().sum()
    if n_inv > 0:
        advertencias.append(f"{n_inv} fila(s) con nro_cta inválido — serán descartadas.")
    df = df[df['nro_cta'].notna()].copy()
    df['nro_cta'] = df['nro_cta'].astype(int)

    for col in ['extendido','nombre','rubro','sub_rubro','analisis','fases','tipo','moneda']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({'': None, 'nan': None, 'NaN': None})
        else:
            df[col] = None

    def parse_bool_activa(v):
        # activa es boolean en DB
        if v is None: return None
        return str(v).strip().upper() in ('S', 'SI', 'TRUE', '1', 'YES')

    def parse_bool_sn(v):
        # es_resultado es VARCHAR(1) 'S'/'N' en DB
        if v is None: return None
        return 'S' if str(v).strip().upper() in ('S', 'SI', 'TRUE', '1', 'YES') else 'N'

    if 'activa' in df.columns:
        df['activa'] = df['activa'].apply(parse_bool_activa)
    else:
        df['activa'] = None

    if 'es_resultado' in df.columns:
        df['es_resultado'] = df['es_resultado'].apply(parse_bool_sn)
    else:
        df['es_resultado'] = None

    for col in ['nivel_1', 'nivel_2', 'nivel_3']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        else:
            df[col] = None

    df = df.drop_duplicates(subset=['nro_cta'], keep='last')
    cols_out = ['nro_cta','extendido','nombre','rubro','sub_rubro','analisis','fases',
                'tipo','moneda','activa','es_resultado','nivel_1','nivel_2','nivel_3']
    return df[[c for c in cols_out if c in df.columns]].copy(), errores, advertencias


def parsear_proyectos(archivo) -> tuple:
    errores = []; advertencias = []
    nombre_arch = getattr(archivo, 'name', '')
    try:
        if nombre_arch.endswith('.xlsx') or nombre_arch.endswith('.xls'):
            df = pd.read_excel(archivo, dtype=str)
        else:
            for enc in ['utf-8-sig', 'latin-1', 'utf-8']:
                try:
                    archivo.seek(0)
                    df = pd.read_csv(archivo, sep=';', dtype=str, keep_default_na=False, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                errores.append("No se pudo leer el archivo.")
                return pd.DataFrame(), errores, advertencias
    except Exception as e:
        errores.append(f"Error: {e}")
        return pd.DataFrame(), errores, advertencias

    df.columns = [c.strip() for c in df.columns]
    col_map = {
        'ccosto':'ccosto','Ccosto':'ccosto',
        'Nombre':'nombre','nombre':'nombre',
        'fcInicio':'fc_inicio','fc_inicio':'fc_inicio',
        'fcFin':'fc_fin','fc_fin':'fc_fin',
        'Ingresos':'ingresos','ingresos':'ingresos',
        'cto_Mo_Propia':'cto_mo_propia','cto_mo_propia':'cto_mo_propia',
        'cto_Mo_Terceros':'cto_mo_terceros','cto_mo_terceros':'cto_mo_terceros',
        'cto_Materiales':'cto_materiales','cto_materiales':'cto_materiales',
        'cto_Herramientas':'cto_herramientas','cto_herramientas':'cto_herramientas',
        'cto_Diversos':'cto_diversos','cto_diversos':'cto_diversos',
        'Superficie':'superficie','superficie':'superficie',
        'Avance':'avance','avance':'avance',
        'Horas':'horas','horas':'horas',
    }
    df = df.rename(columns={c: col_map[c] for c in df.columns if c in col_map})

    if 'ccosto' not in df.columns:
        errores.append("No se encontró columna 'ccosto'.")
        return pd.DataFrame(), errores, advertencias
    if 'nombre' not in df.columns:
        errores.append("No se encontró columna 'Nombre'.")
        return pd.DataFrame(), errores, advertencias

    df['ccosto'] = df['ccosto'].astype(str).str.strip()
    df = df[df['ccosto'].str.len() > 0].copy()

    for col in ['fc_inicio', 'fc_fin']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        else:
            df[col] = None

    def parse_num(v):
        if v is None or str(v).strip() in ('', 'nan', 'NaN', 'None'): return None
        try: return float(str(v).replace(',', '.'))
        except: return None

    for col in ['ingresos','cto_mo_propia','cto_mo_terceros','cto_materiales',
                'cto_herramientas','cto_diversos','superficie','avance','horas']:
        if col in df.columns:
            df[col] = df[col].apply(parse_num)
        else:
            df[col] = None

    df['nombre'] = df['nombre'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['ccosto'], keep='last')
    advertencias.append(f"{len(df)} proyectos encontrados en el archivo.")

    cols_out = ['ccosto','nombre','fc_inicio','fc_fin','ingresos',
                'cto_mo_propia','cto_mo_terceros','cto_materiales',
                'cto_herramientas','cto_diversos','superficie','avance','horas']
    return df[[c for c in cols_out if c in df.columns]].copy(), errores, advertencias


# ── Upserts ───────────────────────────────────────────────────────────────────

def aplicar_upsert_plan(conn, df: pd.DataFrame) -> tuple:
    cur = conn.cursor()
    cur.execute("SELECT nro_cta FROM dim_cuenta")
    existentes = {r[0] for r in cur.fetchall()}
    nuevas = len(df[~df['nro_cta'].isin(existentes)])
    actualizadas = len(df[df['nro_cta'].isin(existentes)])

    psycopg2.extras.execute_values(cur, """
        INSERT INTO dim_cuenta
            (nro_cta, extendido, nombre, rubro, sub_rubro, analisis, fases,
             tipo, moneda, activa, es_resultado, nivel_1, nivel_2, nivel_3)
        VALUES %s
        ON CONFLICT (nro_cta) DO UPDATE SET
            extendido    = EXCLUDED.extendido,
            nombre       = EXCLUDED.nombre,
            rubro        = EXCLUDED.rubro,
            sub_rubro    = EXCLUDED.sub_rubro,
            analisis     = EXCLUDED.analisis,
            fases        = EXCLUDED.fases,
            tipo         = EXCLUDED.tipo,
            moneda       = EXCLUDED.moneda,
            activa       = COALESCE(EXCLUDED.activa,       dim_cuenta.activa),
            es_resultado = EXCLUDED.es_resultado,
            nivel_1      = COALESCE(EXCLUDED.nivel_1,      dim_cuenta.nivel_1),
            nivel_2      = COALESCE(EXCLUDED.nivel_2,      dim_cuenta.nivel_2),
            nivel_3      = COALESCE(EXCLUDED.nivel_3,      dim_cuenta.nivel_3)
    """, [
        (int(r['nro_cta']), r.get('extendido'), r.get('nombre'),
         r.get('rubro'), r.get('sub_rubro'), r.get('analisis'), r.get('fases'),
         r.get('tipo'), r.get('moneda'), r.get('activa'), r.get('es_resultado'),
         r.get('nivel_1') if pd.notna(r.get('nivel_1','')) else None,
         r.get('nivel_2') if pd.notna(r.get('nivel_2','')) else None,
         r.get('nivel_3') if pd.notna(r.get('nivel_3','')) else None)
        for _, r in df.iterrows()
    ], page_size=200)
    conn.commit(); cur.close()
    return nuevas, actualizadas


def aplicar_upsert_proyectos(conn, df: pd.DataFrame) -> tuple:
    from datetime import datetime
    cur = conn.cursor()
    cur.execute("SELECT ccosto FROM proyectos")
    existentes = {r[0] for r in cur.fetchall()}
    nuevos = len(df[~df['ccosto'].isin(existentes)])
    actualizados = len(df[df['ccosto'].isin(existentes)])

    psycopg2.extras.execute_values(cur, """
        INSERT INTO proyectos
            (ccosto, nombre, fc_inicio, fc_fin, ingresos,
             cto_mo_propia, cto_mo_terceros, cto_materiales,
             cto_herramientas, cto_diversos,
             superficie, avance, horas, actualizado_en)
        VALUES %s
        ON CONFLICT (ccosto) DO UPDATE SET
            nombre           = EXCLUDED.nombre,
            fc_inicio        = EXCLUDED.fc_inicio,
            fc_fin           = EXCLUDED.fc_fin,
            ingresos         = EXCLUDED.ingresos,
            cto_mo_propia    = EXCLUDED.cto_mo_propia,
            cto_mo_terceros  = EXCLUDED.cto_mo_terceros,
            cto_materiales   = EXCLUDED.cto_materiales,
            cto_herramientas = EXCLUDED.cto_herramientas,
            cto_diversos     = EXCLUDED.cto_diversos,
            superficie       = EXCLUDED.superficie,
            avance           = EXCLUDED.avance,
            horas            = EXCLUDED.horas,
            actualizado_en   = now()
    """, [
        (r['ccosto'], r['nombre'], r.get('fc_inicio'), r.get('fc_fin'),
         r.get('ingresos'), r.get('cto_mo_propia'), r.get('cto_mo_terceros'),
         r.get('cto_materiales'), r.get('cto_herramientas'), r.get('cto_diversos'),
         r.get('superficie'), r.get('avance'), r.get('horas'), datetime.now())
        for _, r in df.iterrows()
    ], page_size=100)
    conn.commit(); cur.close()
    return nuevos, actualizados


# ── UI ─────────────────────────────────────────────────────────────────────────

st.title("⚙️ Administración")
st.caption("Gestión de maestros y configuración del sistema.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

tabs = st.tabs(["🏢 Empresas", "📒 Plan de Cuentas", "📥 Actualizar Plan",
                "🏗️ Proyectos", "🎯 Centros de Costo", "📜 Log Recálculos"])

# ── Tab 1: Empresas ────────────────────────────────────────────────────────────
with tabs[0]:
    st.subheader("Empresas activas")
    try:
        df_emp = get_empresas(conn)
    except Exception:
        conn = get_conn(); df_emp = get_empresas(conn)
    st.dataframe(df_emp, use_container_width=True, hide_index=True)

# ── Tab 2: Plan de Cuentas ─────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("Plan de Cuentas")
    try:
        df_cta = get_plan_cuentas(conn)
        rubros = get_rubros(conn)
        fases_all = get_fases(conn)
    except Exception:
        conn = get_conn()
        df_cta = get_plan_cuentas(conn)
        rubros = get_rubros(conn)
        fases_all = get_fases(conn)

    c1, c2, c3, c4 = st.columns(4)
    filt_cod  = c1.text_input("Buscar Nro Cta",  placeholder="ej: 1024", key="filt_cod")
    filt_nom  = c2.text_input("Buscar Nombre",   placeholder="ej: Caja",  key="filt_nom")
    filt_rub  = c3.text_input("Buscar Rubro",    placeholder="ej: DISPONIBILIDADES", key="filt_rub")
    filt_tipo = c4.selectbox("Tipo", ["Todos","Activo","Pasivo","Patrimonio","Resultado"], key="filt_tipo")

    df_show = df_cta.copy()
    if filt_cod.strip():
        df_show = df_show[df_show['Nro Cta'].astype(str).str.contains(filt_cod.strip())]
    if filt_nom.strip():
        df_show = df_show[df_show['Nombre'].str.contains(filt_nom.strip(), case=False, na=False)]
    if filt_rub.strip():
        df_show = df_show[df_show['Rubro'].str.contains(filt_rub.strip(), case=False, na=False)]
    if filt_tipo != "Todos":
        df_show = df_show[df_show['Tipo'].str.lower() == filt_tipo.lower()]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total cuentas",    len(df_cta))
    c2.metric("Mostradas",        len(df_show))
    c3.metric("Activas",          int(df_cta['Activa'].sum()))
    c4.metric("Rubros distintos", df_cta['Rubro'].nunique())
    st.divider()
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    # ── Editar cuenta ──────────────────────────────────────────────────────────
    st.divider()
    st.markdown("#### ✏️ Editar cuenta existente")
    opciones_editar = ["— Seleccioná una cuenta —"] + [
        f"{int(r['Nro Cta'])} — {r['Nombre']}" for _, r in df_show.iterrows()
    ]
    sel_editar = st.selectbox("Seleccioná la cuenta a editar", opciones_editar, key="sel_editar")

    if sel_editar != "— Seleccioná una cuenta —":
        nro_edit = int(sel_editar.split(" — ")[0])
        cuenta = df_show[df_show['Nro Cta'] == nro_edit].iloc[0]
        st.divider()
        st.markdown(f"### ✏️ Editando cuenta **{nro_edit}** — {cuenta['Nombre']}")

        rubro_actual    = cuenta['Rubro']    or ""
        subrubro_actual = cuenta['Sub-rubro'] or ""
        analisis_actual = cuenta['Analisis'] or ""
        fases_actual    = cuenta['Fases']    or ""

        c1, c2 = st.columns([2, 1])
        nombre_edit    = c1.text_input("Nombre *", value=cuenta['Nombre'] or "", key=f"edit_nombre_{nro_edit}")
        extendido_edit = c2.text_input("Extendido", value=cuenta['Extendido'] or "", key=f"edit_ext_{nro_edit}")

        c1, c2, c3, c4 = st.columns(4)
        opciones_rubro = rubros + ["✨ + Nuevo rubro..."]
        idx_rubro = rubros.index(rubro_actual) if rubro_actual in rubros else 0
        rubro_sel_edit = c1.selectbox("Rubro *", opciones_rubro, index=idx_rubro, key=f"edit_rubro_{nro_edit}")
        es_rubro_nuevo = rubro_sel_edit == "✨ + Nuevo rubro..."
        rubro_final_edit = rubro_actual

        if es_rubro_nuevo:
            st.markdown("**✨ Nuevo rubro**")
            nuevo_rubro_edit = st.text_input("Nombre del rubro *", key=f"edit_nuevo_rubro_{nro_edit}")
            if nuevo_rubro_edit and validar_rubro_nuevo(conn, nuevo_rubro_edit):
                st.error(f"❌ El rubro **{nuevo_rubro_edit}** ya existe.")
            rubro_final_edit = nuevo_rubro_edit
            subrubros_edit = []; analisis_edit_list = []
        else:
            rubro_final_edit = rubro_sel_edit
            subrubros_edit = get_subrubros_por_rubro(conn, rubro_sel_edit)
            analisis_edit_list = []

        subrubro_final_edit = ""
        if rubro_final_edit and not es_rubro_nuevo:
            opts_sub = ["— Sin sub-rubro —"] + subrubros_edit + ["✨ + Nuevo sub-rubro..."]
            idx_sub = (subrubros_edit.index(subrubro_actual) + 1) if subrubro_actual in subrubros_edit else 0
            sub_sel = c2.selectbox("Sub-rubro", opts_sub, index=idx_sub, key=f"edit_sub_{nro_edit}")
            if sub_sel == "✨ + Nuevo sub-rubro...":
                nuevo_sub_edit = st.text_input("Nombre del sub-rubro *", key=f"edit_nuevo_sub_{nro_edit}")
                if nuevo_sub_edit and validar_subrubro_nuevo(conn, rubro_final_edit, nuevo_sub_edit):
                    st.error(f"❌ El sub-rubro **{nuevo_sub_edit}** ya existe en **{rubro_final_edit}**.")
                subrubro_final_edit = nuevo_sub_edit; analisis_edit_list = []
            elif sub_sel == "— Sin sub-rubro —":
                subrubro_final_edit = ""; analisis_edit_list = []
            else:
                subrubro_final_edit = sub_sel
                analisis_edit_list = get_analisis_por_subrubro(conn, sub_sel)
        elif es_rubro_nuevo:
            subrubro_final_edit = c2.text_input("Sub-rubro (opcional)", key=f"edit_sub_libre_{nro_edit}")

        analisis_final_edit = ""
        if subrubro_final_edit and not es_rubro_nuevo:
            opts_an = ["— Sin análisis —"] + analisis_edit_list + ["✨ + Nuevo análisis..."]
            idx_an = (analisis_edit_list.index(analisis_actual) + 1) if analisis_actual in analisis_edit_list else 0
            an_sel = c3.selectbox("Análisis", opts_an, index=idx_an, key=f"edit_an_{nro_edit}")
            if an_sel == "✨ + Nuevo análisis...":
                nuevo_an_edit = st.text_input("Nombre del análisis *", key=f"edit_nuevo_an_{nro_edit}")
                if nuevo_an_edit and validar_analisis_nuevo(conn, subrubro_final_edit, nuevo_an_edit):
                    st.error(f"❌ El análisis **{nuevo_an_edit}** ya existe en **{subrubro_final_edit}**.")
                analisis_final_edit = nuevo_an_edit
            elif an_sel == "— Sin análisis —":
                analisis_final_edit = ""
            else:
                analisis_final_edit = an_sel
        elif subrubro_final_edit:
            analisis_final_edit = c3.text_input("Análisis (opcional)", key=f"edit_an_libre_{nro_edit}")

        opts_fases = ["— Sin fases —"] + fases_all + ["✨ + Nueva fase..."]
        idx_fases = (fases_all.index(fases_actual) + 1) if fases_actual in fases_all else 0
        fases_sel = c4.selectbox("Fases", opts_fases, index=idx_fases, key=f"edit_fases_{nro_edit}")
        if fases_sel == "✨ + Nueva fase...":
            fases_final_edit = st.text_input("Nombre de la fase *", key=f"edit_nueva_fase_{nro_edit}")
        elif fases_sel == "— Sin fases —":
            fases_final_edit = ""
        else:
            fases_final_edit = fases_sel

        c1, c2, c3 = st.columns(3)
        tipo_edit = c1.selectbox("Tipo", ["Activo","Pasivo","Patrimonio","Resultado"],
                                 index=["Activo","Pasivo","Patrimonio","Resultado"].index(cuenta['Tipo'])
                                 if cuenta['Tipo'] in ["Activo","Pasivo","Patrimonio","Resultado"] else 0,
                                 key=f"edit_tipo_{nro_edit}")
        moneda_edit = c2.selectbox("Moneda", ["ARS","USD","EUR"],
                                   index=["ARS","USD","EUR"].index(cuenta['Moneda'])
                                   if cuenta['Moneda'] in ["ARS","USD","EUR"] else 0,
                                   key=f"edit_moneda_{nro_edit}")

        if c3.button("💾 Guardar cambios", type="primary", key=f"btn_edit_{nro_edit}"):
            errores_edit = []
            if not nombre_edit.strip(): errores_edit.append("El nombre es obligatorio.")
            if not rubro_final_edit:    errores_edit.append("El rubro es obligatorio.")
            if es_rubro_nuevo and validar_rubro_nuevo(conn, rubro_final_edit):
                errores_edit.append(f"El rubro **{rubro_final_edit}** ya existe.")
            if errores_edit:
                for e in errores_edit: st.error(f"❌ {e}")
            else:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        UPDATE dim_cuenta
                        SET nombre=%s, extendido=%s, rubro=%s, sub_rubro=%s,
                            analisis=%s, fases=%s, tipo=%s, moneda=%s
                        WHERE nro_cta=%s
                    """, (nombre_edit.strip() or None, extendido_edit.strip() or None,
                          rubro_final_edit or None, subrubro_final_edit or None,
                          analisis_final_edit or None, fases_final_edit or None,
                          tipo_edit, moneda_edit, nro_edit))
                    conn.commit(); cur.close()
                    st.success(f"✅ Cuenta **{nro_edit}** actualizada correctamente.")
                    st.rerun()
                except Exception as e:
                    conn.rollback(); st.error(f"Error: {e}")

    st.divider()

    # ── Alta de cuenta nueva ───────────────────────────────────────────────────
    with st.expander("➕ Agregar nueva cuenta"):
        c1, c2, c3 = st.columns(3)
        nro_cta_new   = c1.number_input("Nro Cta *", min_value=1, step=1, key="new_nro_cta")
        extendido_new = c2.text_input("Extendido", placeholder="ej: 1.05.01.001", key="new_extendido")
        nombre_new    = c3.text_input("Nombre *", placeholder="ej: Maquinaria y Equipo", key="new_nombre")

        c1, c2, c3, c4, c5 = st.columns(5)
        tipo_new   = c4.selectbox("Tipo *",  ["Activo","Pasivo","Patrimonio","Resultado"], key="new_tipo")
        moneda_new = c5.selectbox("Moneda",  ["ARS","USD","EUR"], key="new_moneda")

        opts_rubro_new = rubros + ["✨ + Nuevo rubro..."]
        rubro_sel_new = c1.selectbox("Rubro *", opts_rubro_new, index=0, key="new_rubro_sel")
        es_rubro_nuevo_new = rubro_sel_new == "✨ + Nuevo rubro..."
        rubro_final_new = ""

        if es_rubro_nuevo_new:
            st.markdown("**✨ Nuevo rubro**")
            nuevo_rubro_new = st.text_input("Nombre del rubro *", key="new_nuevo_rubro_nom")
            if nuevo_rubro_new and validar_rubro_nuevo(conn, nuevo_rubro_new):
                st.error(f"❌ El rubro **{nuevo_rubro_new}** ya existe.")
            rubro_final_new = nuevo_rubro_new; subrubros_new = []
        else:
            rubro_final_new = rubro_sel_new
            subrubros_new = get_subrubros_por_rubro(conn, rubro_sel_new) if rubro_sel_new else []

        subrubro_final_new = ""; analisis_new_list = []
        if rubro_final_new and not es_rubro_nuevo_new:
            opts_sub_new = ["— Sin sub-rubro —"] + subrubros_new + ["✨ + Nuevo sub-rubro..."]
            sub_sel_new = c2.selectbox("Sub-rubro", opts_sub_new, key="new_subrubro_sel")
            if sub_sel_new == "✨ + Nuevo sub-rubro...":
                nuevo_sub_new = st.text_input("Nombre del sub-rubro *", key="new_nuevo_sub_nom")
                if nuevo_sub_new and validar_subrubro_nuevo(conn, rubro_final_new, nuevo_sub_new):
                    st.error(f"❌ El sub-rubro **{nuevo_sub_new}** ya existe en **{rubro_final_new}**.")
                subrubro_final_new = nuevo_sub_new
            elif sub_sel_new == "— Sin sub-rubro —":
                subrubro_final_new = ""
            else:
                subrubro_final_new = sub_sel_new
                analisis_new_list = get_analisis_por_subrubro(conn, sub_sel_new)
        elif es_rubro_nuevo_new:
            subrubro_final_new = c2.text_input("Sub-rubro (opcional)", key="new_subrubro_libre")

        analisis_final_new = ""
        if subrubro_final_new and not es_rubro_nuevo_new:
            opts_an_new = ["— Sin análisis —"] + analisis_new_list + ["✨ + Nuevo análisis..."]
            an_sel_new = c3.selectbox("Análisis", opts_an_new, key="new_analisis_sel")
            if an_sel_new == "✨ + Nuevo análisis...":
                nuevo_an_new = st.text_input("Nombre del análisis *", key="new_nuevo_an_nom")
                if nuevo_an_new and validar_analisis_nuevo(conn, subrubro_final_new, nuevo_an_new):
                    st.error(f"❌ El análisis **{nuevo_an_new}** ya existe en **{subrubro_final_new}**.")
                analisis_final_new = nuevo_an_new
            elif an_sel_new == "— Sin análisis —":
                analisis_final_new = ""
            else:
                analisis_final_new = an_sel_new
        elif subrubro_final_new:
            analisis_final_new = c3.text_input("Análisis (opcional)", key="new_analisis_libre")

        opts_fases_new = ["— Sin fases —"] + fases_all + ["✨ + Nueva fase..."]
        fases_sel_new = c3.selectbox("Fases", opts_fases_new, key="new_fases_sel") if not subrubro_final_new \
                        else st.selectbox("Fases", opts_fases_new, key="new_fases_sel2")
        if fases_sel_new == "✨ + Nueva fase...":
            fases_final_new = st.text_input("Nombre de la fase *", key="new_nueva_fase")
        elif fases_sel_new == "— Sin fases —":
            fases_final_new = ""
        else:
            fases_final_new = fases_sel_new

        if st.button("💾 Guardar cuenta", key="btn_nueva_cta", type="primary"):
            errores_new = []
            if not nombre_new.strip():      errores_new.append("El nombre es obligatorio.")
            if not rubro_final_new.strip(): errores_new.append("El rubro es obligatorio.")
            errores_new += validar_cuenta_nueva(conn, nro_cta_new, nombre_new.strip())
            if errores_new:
                for e in errores_new: st.error(f"❌ {e}")
            else:
                try:
                    cur = conn.cursor()
                    cur.execute("""
                        INSERT INTO dim_cuenta
                            (nro_cta, extendido, nombre, rubro, sub_rubro, analisis, fases, tipo, moneda)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (nro_cta_new, extendido_new.strip() or None, nombre_new.strip(),
                          rubro_final_new.strip() or None, subrubro_final_new.strip() or None,
                          analisis_final_new.strip() or None, fases_final_new.strip() or None,
                          tipo_new, moneda_new))
                    conn.commit(); cur.close()
                    st.success(f"✅ Cuenta **{nro_cta_new} — {nombre_new}** agregada al plan.")
                    st.rerun()
                except Exception as e:
                    conn.rollback(); st.error(f"Error: {e}")

# ── Tab 3: Actualizar Plan ─────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("📥 Actualizar Plan de Cuentas")
    st.caption("Cargá un Excel o CSV para agregar y actualizar cuentas. Las cuentas existentes no se eliminan.")

    if 'plan_cargado' in st.session_state:
        r = st.session_state['plan_cargado']
        st.success(f"✅ Plan actualizado correctamente desde **{r['archivo']}**")
        st.divider()
        c1, c2, c3 = st.columns(3)
        c1.metric("Cuentas nuevas",       r['nuevas'])
        c2.metric("Cuentas actualizadas", r['actualizadas'])
        c3.metric("Total procesadas",     r['nuevas'] + r['actualizadas'])
        st.divider()
        if st.button("📥 Cargar otro archivo", type="primary"):
            st.session_state.pop('plan_cargado', None); st.rerun()
        st.stop()

    st.markdown("**Formatos aceptados:**")
    c1, c2 = st.columns(2)
    with c1:
        st.caption("Excel (.xlsx):")
        st.code("nro_cta | Nombre | Rubro | SubRubro | Analisis | Fases | Tipo | Moneda | ...")
    with c2:
        st.caption("CSV del sistema (separador ;):")
        st.code("nro_cta;Extendido;Nombre;Rubro;Tipo;Moneda;Activa;EsResultado;...")

    archivo_plan = st.file_uploader("Archivo del plan de cuentas", type=["xlsx","xls","csv"], key="plan_uploader")

    if archivo_plan:
        df_plan, errores_plan, adv_plan = parsear_plan_cuentas(archivo_plan)
        for adv in adv_plan: st.info(adv)
        if errores_plan:
            for e in errores_plan: st.error(f"❌ {e}")
            st.stop()
        if df_plan.empty:
            st.warning("El archivo no contiene cuentas válidas."); st.stop()

        st.subheader("Vista previa")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Cuentas en archivo", len(df_plan))
        c2.metric("Con rubro",          df_plan['rubro'].notna().sum())
        c3.metric("Con sub-rubro",      df_plan['sub_rubro'].notna().sum())
        c4.metric("Con análisis",       df_plan['analisis'].notna().sum())
        st.dataframe(df_plan.head(20), use_container_width=True, hide_index=True)

        cur = conn.cursor()
        cur.execute("SELECT nro_cta FROM dim_cuenta")
        en_db = {r[0] for r in cur.fetchall()}; cur.close()
        en_archivo = set(df_plan['nro_cta'].tolist())

        st.divider()
        c1, c2 = st.columns(2)
        c1.metric("Cuentas nuevas a agregar",        len(en_archivo - en_db))
        c2.metric("Cuentas existentes a actualizar", len(en_archivo & en_db))

        st.divider()
        st.markdown("#### 🔍 Validación contra Libro Diario")
        faltantes = get_cuentas_faltantes_diario(conn, en_db | en_archivo)

        if faltantes:
            st.warning(f"⚠️ **{len(faltantes)} cuenta(s)** del Libro Diario no están en el plan resultante.")
            with st.expander(f"Ver {len(faltantes)} cuentas faltantes", expanded=True):
                cur = conn.cursor()
                cur.execute("""
                    SELECT cuenta_codigo, COUNT(*) AS movimientos,
                           MIN(periodo_anio||'/'||LPAD(periodo_mes::text,2,'0')),
                           MAX(periodo_anio||'/'||LPAD(periodo_mes::text,2,'0'))
                    FROM libro_diario WHERE cuenta_codigo = ANY(%s)
                    GROUP BY cuenta_codigo ORDER BY cuenta_codigo
                """, (faltantes,))
                rows = cur.fetchall(); cur.close()
                st.dataframe(
                    pd.DataFrame(rows, columns=['Nro Cuenta','Movimientos','Primer período','Último período']),
                    use_container_width=True, hide_index=True)
            continuar = st.checkbox(
                "✅ Entiendo que estas cuentas no tendrán clasificación. Continuar de todas formas.",
                key="plan_continuar_con_faltantes")
        else:
            st.success("✅ Todas las cuentas del Libro Diario están cubiertas por el plan.")
            continuar = True

        st.divider()
        if continuar:
            if st.button("📥 Aplicar actualización del plan", type="primary"):
                conn2 = get_conn()
                with st.spinner("Actualizando plan de cuentas..."):
                    try:
                        n_nuevas, n_act = aplicar_upsert_plan(conn2, df_plan)
                        st.session_state['plan_cargado'] = {
                            'archivo': archivo_plan.name, 'nuevas': n_nuevas, 'actualizadas': n_act}
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error al actualizar: {e}")

# ── Tab 4: Proyectos ───────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("🏗️ Proyectos")
    st.caption("Gestión del presupuesto y avance de proyectos. Se vincula con libro_mayor por centro de costo.")

    if 'proyectos_cargados' in st.session_state:
        r = st.session_state['proyectos_cargados']
        st.success(f"✅ Proyectos actualizados desde **{r['archivo']}**")
        st.divider()
        c1, c2, c3 = st.columns(3)
        c1.metric("Proyectos nuevos",       r['nuevos'])
        c2.metric("Proyectos actualizados", r['actualizados'])
        c3.metric("Total procesados",       r['nuevos'] + r['actualizados'])
        st.divider()
        if st.button("🏗️ Cargar otro archivo", type="primary"):
            st.session_state.pop('proyectos_cargados', None); st.rerun()
        st.stop()

    try:
        df_proy = get_proyectos(conn)
    except Exception:
        conn = get_conn(); df_proy = get_proyectos(conn)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total proyectos", len(df_proy))
    c2.metric("Superficie total m²",
              f"{df_proy['Superficie'].sum():,.0f}" if not df_proy.empty else "—")
    c3.metric("Ingresos presup. total",
              f"${df_proy['Ingresos'].sum():,.0f}" if not df_proy.empty else "—")

    st.dataframe(
        df_proy, use_container_width=True, hide_index=True,
        column_config={
            "Ingresos":         st.column_config.NumberColumn(format="$ %.0f"),
            "Cto MO Propia":    st.column_config.NumberColumn(format="$ %.0f"),
            "Cto MO Terceros":  st.column_config.NumberColumn(format="$ %.0f"),
            "Cto Materiales":   st.column_config.NumberColumn(format="$ %.0f"),
            "Cto Herramientas": st.column_config.NumberColumn(format="$ %.0f"),
            "Cto Diversos":     st.column_config.NumberColumn(format="$ %.0f"),
            "Avance":           st.column_config.NumberColumn(format="%.0f %%"),
            "Actualizado":      st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm"),
        }
    )

    st.divider()
    st.markdown("#### 📥 Actualizar desde Excel")
    st.caption("Los proyectos existentes se actualizan, los nuevos se agregan.")
    st.code("ccosto | Nombre | fcInicio | fcFin | Ingresos | cto_Mo_Propia | ... | Superficie | Avance | Horas")

    archivo_proy = st.file_uploader("Excel de proyectos", type=["xlsx","xls","csv"], key="proy_uploader")

    if archivo_proy:
        df_p, errores_p, adv_p = parsear_proyectos(archivo_proy)
        for adv in adv_p: st.info(adv)
        if errores_p:
            for e in errores_p: st.error(f"❌ {e}")
            st.stop()
        if df_p.empty:
            st.warning("El archivo no contiene proyectos válidos."); st.stop()

        cur = conn.cursor()
        cur.execute("SELECT ccosto FROM proyectos")
        en_db_p = {r[0] for r in cur.fetchall()}; cur.close()
        en_arch_p = set(df_p['ccosto'].tolist())

        c1, c2, c3 = st.columns(3)
        c1.metric("Proyectos en archivo", len(df_p))
        c2.metric("Nuevos",              len(en_arch_p - en_db_p))
        c3.metric("A actualizar",        len(en_arch_p & en_db_p))

        st.dataframe(df_p, use_container_width=True, hide_index=True)
        st.divider()

        if st.button("🏗️ Aplicar actualización de proyectos", type="primary"):
            conn2 = get_conn()
            with st.spinner("Actualizando proyectos..."):
                try:
                    n_nuevos, n_act = aplicar_upsert_proyectos(conn2, df_p)
                    st.session_state['proyectos_cargados'] = {
                        'archivo': archivo_proy.name, 'nuevos': n_nuevos, 'actualizados': n_act}
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al actualizar: {e}")

# ── Tab 5: Centros de Costo ────────────────────────────────────────────────────
with tabs[4]:
    st.subheader("Centros de Costo")
    try:
        df_cc = get_centros(conn)
    except Exception:
        conn = get_conn(); df_cc = get_centros(conn)
    st.metric("Total centros", len(df_cc))
    st.dataframe(df_cc, use_container_width=True, hide_index=True)

    with st.expander("➕ Agregar centro de costo"):
        ca1, ca2, ca3 = st.columns(3)
        cod_new  = ca1.text_input("Código",      key="new_cc_cod")
        desc_new = ca2.text_input("Descripción", key="new_cc_desc")
        emp_new  = ca3.selectbox("Empresa (opcional)", ["—"] + list(EMPRESAS.keys()), key="new_cc_emp")
        if st.button("Guardar centro", key="btn_nuevo_cc"):
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM dim_centro_costo WHERE codigo = %s", (cod_new.strip(),))
            if cur.fetchone():
                st.error(f"❌ El código **{cod_new}** ya existe en centros de costo.")
            else:
                try:
                    emp_id_new = EMPRESAS[emp_new] if emp_new != "—" else None
                    cur.execute("""
                        INSERT INTO dim_centro_costo (codigo, descripcion, empresa_id)
                        VALUES (%s,%s,%s)
                    """, (cod_new.strip(), desc_new.strip(), emp_id_new))
                    conn.commit()
                    st.success(f"✅ Centro **{cod_new}** agregado.")
                    st.rerun()
                except Exception as e:
                    conn.rollback(); st.error(f"Error: {e}")
            cur.close()

# ── Tab 6: Log Recálculos ──────────────────────────────────────────────────────
with tabs[5]:
    st.subheader("Log de recálculos del Mayor")
    try:
        df_log = get_log(conn)
    except Exception:
        conn = get_conn(); df_log = get_log(conn)
    if df_log.empty:
        st.info("No hay recálculos registrados.")
    else:
        st.dataframe(df_log, use_container_width=True, hide_index=True)