import os, sys, re, streamlit as st, pandas as pd, psycopg2.extras
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

TIPOS_CUENTA = ["Activo","Pasivo","Patrimonio","Resultado"]

# Columnas fijas de dim_cuenta — orden y etiqueta de display
COLS_FIJAS = [
    ('nro_cta',      'Nro Cta'),
    ('extendido',    'Extendido'),
    ('nombre',       'Nombre'),
    ('rubro',        'Rubro'),
    ('sub_rubro',    'Sub-rubro'),
    ('analisis',     'Analisis'),
    ('fases',        'Fases'),
    ('tipo',         'Tipo'),
    ('moneda',       'Moneda'),
    ('activa',       'Activa'),
    ('es_resultado', 'Es Resultado'),
    ('nivel_1',      'Nivel 1'),
    ('nivel_2',      'Nivel 2'),
    ('nivel_3',      'Nivel 3'),
]
COLS_FIJAS_NAMES = {c for c, _ in COLS_FIJAS}

def tipo_es_resultado(tipo: str) -> bool:
    return tipo == "Resultado"

# ── Helpers de DB ──────────────────────────────────────────────────────────────

def get_columnas_dim_cuenta(conn) -> list:
    """Devuelve todas las columnas actuales de dim_cuenta en Neon."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'dim_cuenta'
        ORDER BY ordinal_position
    """)
    cols = [r[0] for r in cur.fetchall()]; cur.close(); return cols

def get_plan_cuentas(conn):
    """Trae todas las columnas de dim_cuenta, incluyendo las extra agregadas dinámicamente."""
    cols_db = get_columnas_dim_cuenta(conn)
    cols_extra = [c for c in cols_db if c not in COLS_FIJAS_NAMES]
    todas  = [c for c, _ in COLS_FIJAS] + cols_extra
    labels = [lbl for _, lbl in COLS_FIJAS] + [c.replace('_',' ').title() for c in cols_extra]
    cur = conn.cursor()
    cur.execute(f"SELECT {', '.join(todas)} FROM dim_cuenta ORDER BY nro_cta")
    df = pd.DataFrame(cur.fetchall(), columns=labels); cur.close(); return df

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

def get_nombres_actuales(conn, nros_cta: list) -> dict:
    """Devuelve {nro_cta: nombre} para las cuentas indicadas."""
    if not nros_cta: return {}
    cur = conn.cursor()
    cur.execute("SELECT nro_cta, nombre FROM dim_cuenta WHERE nro_cta = ANY(%s)", (nros_cta,))
    result = {r[0]: r[1] for r in cur.fetchall()}; cur.close(); return result

def get_proyectos(conn):
    """Mantenida por compatibilidad — el Tab 4 usa su propia query."""
    cur = conn.cursor()
    cur.execute("""
        SELECT ccosto, nombre, fc_inicio, fc_fin, ingresos,
               cto_mo_propia, cto_mo_terceros, cto_materiales,
               cto_herramientas, superficie, actualizado_en
        FROM proyectos WHERE activo = true OR activo IS NULL ORDER BY nombre
    """)
    cols = ['CCosto','Nombre','Inicio','Fin','Ingresos',
            'Cto MO Propia','Cto MO Terceros','Cto Materiales',
            'Cto Herramientas','Superficie','Actualizado']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def validar_cuenta_nueva(conn, nro_cta, nombre):
    errores = []
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM dim_cuenta WHERE nro_cta = %s", (nro_cta,))
    if cur.fetchone(): errores.append(f"El Nro de cuenta **{nro_cta}** ya existe en el plan.")
    cur.execute("SELECT 1 FROM dim_cuenta WHERE LOWER(nombre) = LOWER(%s)", (nombre,))
    if cur.fetchone(): errores.append(f"Ya existe una cuenta con el nombre **{nombre}**.")
    cur.close(); return errores

def cuenta_tiene_movimientos(conn, nro_cta: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM libro_diario WHERE cuenta_codigo = %s", (nro_cta,))
    count = cur.fetchone()[0]; cur.close(); return count

def validar_rubro_nuevo(conn, rubro_nombre):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM dim_cuenta WHERE LOWER(rubro) = LOWER(%s)", (rubro_nombre,))
    existe = cur.fetchone() is not None; cur.close(); return existe

def validar_subrubro_nuevo(conn, rubro, subrubro_nombre):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM dim_cuenta WHERE LOWER(rubro)=%s AND LOWER(sub_rubro)=%s",
                (rubro, subrubro_nombre))
    existe = cur.fetchone() is not None; cur.close(); return existe

def validar_analisis_nuevo(conn, subrubro, analisis_nombre):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM dim_cuenta WHERE LOWER(sub_rubro)=%s AND LOWER(analisis)=%s",
                (subrubro, analisis_nombre))
    existe = cur.fetchone() is not None; cur.close(); return existe

# ── Helpers para columnas dinámicas ───────────────────────────────────────────

def inferir_tipo_sql(serie: pd.Series) -> str:
    """Infiere el tipo SQL más adecuado para una columna nueva del archivo."""
    valores = serie.dropna().astype(str).str.strip()
    valores = valores[valores != '']
    if valores.empty: return "TEXT"
    if set(valores.str.upper().unique()) <= {'S','N','SI','NO','TRUE','FALSE','1','0'}:
        return "VARCHAR(1)"
    if valores.str.len().max() <= 1: return "VARCHAR(1)"
    try: valores.astype(int); return "INTEGER"
    except (ValueError, TypeError): pass
    try: valores.str.replace(',','.').astype(float); return "NUMERIC(18,2)"
    except (ValueError, TypeError): pass
    return "VARCHAR(100)" if valores.str.len().max() <= 50 else "TEXT"

def normalizar_nombre_columna(nombre: str) -> str:
    """Convierte nombre de columna del archivo a snake_case para la DB."""
    s = nombre.strip().lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_')

def agregar_columnas_nuevas(conn, cols_nuevas: list) -> list:
    """Ejecuta ALTER TABLE para cada columna nueva. Retorna las agregadas."""
    if not cols_nuevas: return []
    cur = conn.cursor()
    agregadas = []
    for col_db, tipo_sql in cols_nuevas:
        try:
            cur.execute(
                f"ALTER TABLE dim_cuenta ADD COLUMN IF NOT EXISTS {col_db} {tipo_sql}"
            )
            agregadas.append((col_db, tipo_sql))
        except Exception as e:
            conn.rollback(); cur.close()
            raise RuntimeError(f"Error al agregar columna '{col_db}': {e}")
    conn.commit(); cur.close(); return agregadas

# ── Parsers ───────────────────────────────────────────────────────────────────

def parsear_plan_cuentas(archivo) -> tuple:
    """Parsea el archivo del plan de cuentas.
    Retorna: (df, errores, advertencias, cols_extra_normalized)
    cols_extra_normalized: {nombre_archivo: nombre_db} para columnas nuevas detectadas.
    """
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
                return pd.DataFrame(), errores, advertencias, {}
    except Exception as e:
        errores.append(f"Error al leer el archivo: {e}")
        return pd.DataFrame(), errores, advertencias, {}

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
        return pd.DataFrame(), errores, advertencias, {}
    if 'nombre' not in df.columns:
        errores.append("No se encontró columna 'Nombre'.")
        return pd.DataFrame(), errores, advertencias, {}

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
        if v is None: return None
        return str(v).strip().upper() in ('S', 'SI', 'TRUE', '1', 'YES')

    def parse_bool_sn(v):
        if v is None: return None
        return 'S' if str(v).strip().upper() in ('S', 'SI', 'TRUE', '1', 'YES') else 'N'

    df['activa']       = df['activa'].apply(parse_bool_activa)       if 'activa'       in df.columns else None
    df['es_resultado'] = df['es_resultado'].apply(parse_bool_sn)     if 'es_resultado' in df.columns else None

    for col in ['nivel_1', 'nivel_2', 'nivel_3']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        else:
            df[col] = None

    df = df.drop_duplicates(subset=['nro_cta'], keep='last')

    # ── Detectar columnas extra (no mapeadas) ──────────────────────────────────
    cols_ya_en_fijas = set(col_map.values()) | COLS_FIJAS_NAMES
    cols_extra_archivo = [
        c for c in df.columns
        if c not in cols_ya_en_fijas and not c.startswith('_')
    ]

    cols_extra_normalized = {}  # {nombre_archivo: nombre_db}
    for c in cols_extra_archivo:
        col_db = normalizar_nombre_columna(c)
        if col_db and col_db not in COLS_FIJAS_NAMES:
            cols_extra_normalized[c] = col_db
            df = df.rename(columns={c: col_db})

    if cols_extra_normalized:
        advertencias.append(
            f"Columnas extra detectadas: "
            f"{', '.join(f'{k} → {v}' for k,v in cols_extra_normalized.items())}"
        )

    cols_fijas_out = [c for c, _ in COLS_FIJAS]
    cols_out = cols_fijas_out + list(cols_extra_normalized.values())
    return df[[c for c in cols_out if c in df.columns]].copy(), errores, advertencias, cols_extra_normalized


def parsear_proyectos(archivo) -> tuple:
    """Mantenida por compatibilidad con el flujo legacy."""
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
        errores.append(f"Error: {e}"); return pd.DataFrame(), errores, advertencias

    df.columns = [c.strip() for c in df.columns]
    col_map = {
        'ccosto':'ccosto','Ccosto':'ccosto','Nombre':'nombre','nombre':'nombre',
        'fcInicio':'fc_inicio','fc_inicio':'fc_inicio','fcFin':'fc_fin','fc_fin':'fc_fin',
        'Ingresos':'ingresos','ingresos':'ingresos',
        'cto_Mo_Propia':'cto_mo_propia','cto_mo_propia':'cto_mo_propia',
        'cto_Mo_Terceros':'cto_mo_terceros','cto_mo_terceros':'cto_mo_terceros',
        'cto_Materiales':'cto_materiales','cto_materiales':'cto_materiales',
        'cto_Herramientas':'cto_herramientas','cto_herramientas':'cto_herramientas',
        'Superficie':'superficie','superficie':'superficie',
    }
    df = df.rename(columns={c: col_map[c] for c in df.columns if c in col_map})
    if 'ccosto' not in df.columns:
        errores.append("No se encontró columna 'ccosto'."); return pd.DataFrame(), errores, advertencias
    if 'nombre' not in df.columns:
        errores.append("No se encontró columna 'Nombre'."); return pd.DataFrame(), errores, advertencias

    df['ccosto'] = df['ccosto'].astype(str).str.strip()
    df = df[df['ccosto'].str.len() > 0].copy()

    for col in ['fc_inicio','fc_fin']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date if col in df.columns else None

    def parse_num(v):
        if v is None or str(v).strip() in ('','nan','NaN','None'): return None
        try: return float(str(v).replace(',','.'))
        except: return None

    for col in ['ingresos','cto_mo_propia','cto_mo_terceros','cto_materiales',
                'cto_herramientas','superficie']:
        df[col] = df[col].apply(parse_num) if col in df.columns else None

    df['nombre'] = df['nombre'].astype(str).str.strip()
    df = df.drop_duplicates(subset=['ccosto'], keep='last')
    advertencias.append(f"{len(df)} proyectos encontrados en el archivo.")
    cols_out = ['ccosto','nombre','fc_inicio','fc_fin','ingresos','cto_mo_propia',
                'cto_mo_terceros','cto_materiales','cto_herramientas','superficie']
    return df[[c for c in cols_out if c in df.columns]].copy(), errores, advertencias

# ── Upserts ───────────────────────────────────────────────────────────────────

def aplicar_upsert_plan(conn, df: pd.DataFrame,
                        nros_excluir_renombre: set = None,
                        cols_excluir: set = None) -> tuple:
    """Upsert dinámico. Respeta exclusiones de renombres y columnas seleccionadas."""
    nros_excluir_renombre = nros_excluir_renombre or set()
    cols_excluir          = cols_excluir or set()

    cur = conn.cursor()
    cur.execute("SELECT nro_cta, nombre FROM dim_cuenta")
    existentes = {r[0]: r[1] for r in cur.fetchall()}
    nuevas       = len(df[~df['nro_cta'].isin(existentes)])
    actualizadas = len(df[df['nro_cta'].isin(existentes)])

    COLS_FIJAS_INSERT = ['nro_cta','extendido','nombre','rubro','sub_rubro','analisis',
                         'fases','tipo','moneda','activa','es_resultado',
                         'nivel_1','nivel_2','nivel_3']

    # Columnas extra activas (no excluidas por el usuario)
    cols_extra_activas = [
        c for c in df.columns
        if c not in COLS_FIJAS_INSERT and c not in cols_excluir
    ]
    todas_cols = COLS_FIJAS_INSERT + cols_extra_activas

    set_fijas = """
            extendido    = EXCLUDED.extendido,
            rubro        = EXCLUDED.rubro,
            sub_rubro    = EXCLUDED.sub_rubro,
            analisis     = EXCLUDED.analisis,
            fases        = EXCLUDED.fases,
            tipo         = EXCLUDED.tipo,
            moneda       = EXCLUDED.moneda,
            activa       = COALESCE(EXCLUDED.activa,    dim_cuenta.activa),
            es_resultado = EXCLUDED.es_resultado,
            nivel_1      = COALESCE(EXCLUDED.nivel_1,   dim_cuenta.nivel_1),
            nivel_2      = COALESCE(EXCLUDED.nivel_2,   dim_cuenta.nivel_2),
            nivel_3      = COALESCE(EXCLUDED.nivel_3,   dim_cuenta.nivel_3)"""
    set_extra = ''.join(f',\n            {c} = EXCLUDED.{c}' for c in cols_extra_activas)

    def limpiar_extra(v):
        if v is None: return None
        if isinstance(v, float) and pd.isna(v): return None
        s = str(v).strip()
        return None if s in ('','nan','NaN') else s

    rows = []
    for _, r in df.iterrows():
        nro = int(r['nro_cta'])
        # Si este nro está excluido de renombre, preservar nombre actual de la DB
        nombre_final = existentes.get(nro, r.get('nombre')) \
            if nro in nros_excluir_renombre else r.get('nombre')

        row = [nro, r.get('extendido'), nombre_final,
               r.get('rubro'), r.get('sub_rubro'), r.get('analisis'), r.get('fases'),
               r.get('tipo'), r.get('moneda'), r.get('activa'), r.get('es_resultado'),
               r.get('nivel_1') if pd.notna(r.get('nivel_1','')) else None,
               r.get('nivel_2') if pd.notna(r.get('nivel_2','')) else None,
               r.get('nivel_3') if pd.notna(r.get('nivel_3','')) else None]

        for c in cols_extra_activas:
            row.append(limpiar_extra(r.get(c)))
        rows.append(tuple(row))

    sql = f"""
        INSERT INTO dim_cuenta ({', '.join(todas_cols)})
        VALUES %s
        ON CONFLICT (nro_cta) DO UPDATE SET
            nombre       = EXCLUDED.nombre,
            {set_fijas}{set_extra}
    """
    psycopg2.extras.execute_values(cur, sql, rows, page_size=200)
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
             cto_herramientas, superficie, actualizado_en)
        VALUES %s
        ON CONFLICT (ccosto) DO UPDATE SET
            nombre=EXCLUDED.nombre, fc_inicio=EXCLUDED.fc_inicio, fc_fin=EXCLUDED.fc_fin,
            ingresos=EXCLUDED.ingresos, cto_mo_propia=EXCLUDED.cto_mo_propia,
            cto_mo_terceros=EXCLUDED.cto_mo_terceros, cto_materiales=EXCLUDED.cto_materiales,
            cto_herramientas=EXCLUDED.cto_herramientas,
            superficie=EXCLUDED.superficie, actualizado_en=now()
    """, [
        (r['ccosto'], r['nombre'], r.get('fc_inicio'), r.get('fc_fin'),
         r.get('ingresos'), r.get('cto_mo_propia'), r.get('cto_mo_terceros'),
         r.get('cto_materiales'), r.get('cto_herramientas'),
         r.get('superficie'), datetime.now())
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
    try: df_emp = get_empresas(conn)
    except Exception: conn = get_conn(); df_emp = get_empresas(conn)
    st.dataframe(df_emp, use_container_width=True, hide_index=True)

# ── Tab 2: Plan de Cuentas ─────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("Plan de Cuentas")
    try:
        df_cta = get_plan_cuentas(conn); rubros = get_rubros(conn); fases_all = get_fases(conn)
    except Exception:
        conn = get_conn(); df_cta = get_plan_cuentas(conn)
        rubros = get_rubros(conn); fases_all = get_fases(conn)

    c1, c2, c3, c4 = st.columns(4)
    filt_cod  = c1.text_input("Buscar Nro Cta",  placeholder="ej: 1024", key="filt_cod")
    filt_nom  = c2.text_input("Buscar Nombre",   placeholder="ej: Caja",  key="filt_nom")
    filt_rub  = c3.text_input("Buscar Rubro",    placeholder="ej: DISPONIBILIDADES", key="filt_rub")
    filt_tipo = c4.selectbox("Tipo", ["Todos","Activo","Pasivo","Patrimonio","Resultado"], key="filt_tipo")

    df_show = df_cta.copy()
    if filt_cod.strip(): df_show = df_show[df_show['Nro Cta'].astype(str).str.contains(filt_cod.strip())]
    if filt_nom.strip(): df_show = df_show[df_show['Nombre'].str.contains(filt_nom.strip(), case=False, na=False)]
    if filt_rub.strip(): df_show = df_show[df_show['Rubro'].str.contains(filt_rub.strip(), case=False, na=False)]
    if filt_tipo != "Todos": df_show = df_show[df_show['Tipo'].str.lower() == filt_tipo.lower()]

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

        rubro_actual    = cuenta['Rubro']     or ""
        subrubro_actual = cuenta['Sub-rubro'] or ""
        analisis_actual = cuenta['Analisis']  or ""
        fases_actual    = cuenta['Fases']     or ""

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
            idx_sub = (subrubros_edit.index(subrubro_actual)+1) if subrubro_actual in subrubros_edit else 0
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
            idx_an = (analisis_edit_list.index(analisis_actual)+1) if analisis_actual in analisis_edit_list else 0
            an_sel = c3.selectbox("Análisis", opts_an, index=idx_an, key=f"edit_an_{nro_edit}")
            if an_sel == "✨ + Nuevo análisis...":
                nuevo_an_edit = st.text_input("Nombre del análisis *", key=f"edit_nuevo_an_{nro_edit}")
                if nuevo_an_edit and validar_analisis_nuevo(conn, subrubro_final_edit, nuevo_an_edit):
                    st.error(f"❌ El análisis **{nuevo_an_edit}** ya existe en **{subrubro_final_edit}**.")
                analisis_final_edit = nuevo_an_edit
            elif an_sel == "— Sin análisis —": analisis_final_edit = ""
            else: analisis_final_edit = an_sel
        elif subrubro_final_edit:
            analisis_final_edit = c3.text_input("Análisis (opcional)", key=f"edit_an_libre_{nro_edit}")

        opts_fases = ["— Sin fases —"] + fases_all + ["✨ + Nueva fase..."]
        idx_fases = (fases_all.index(fases_actual)+1) if fases_actual in fases_all else 0
        fases_sel = c4.selectbox("Fases", opts_fases, index=idx_fases, key=f"edit_fases_{nro_edit}")
        if fases_sel == "✨ + Nueva fase...":
            fases_final_edit = st.text_input("Nombre de la fase *", key=f"edit_nueva_fase_{nro_edit}")
        elif fases_sel == "— Sin fases —": fases_final_edit = ""
        else: fases_final_edit = fases_sel

        c1, c2, c3, c4 = st.columns(4)
        tipo_actual_edit = cuenta['Tipo'] if cuenta['Tipo'] in TIPOS_CUENTA else "Activo"
        tipo_edit = c1.selectbox("Tipo *", TIPOS_CUENTA,
                                  index=TIPOS_CUENTA.index(tipo_actual_edit),
                                  key=f"edit_tipo_{nro_edit}")
        moneda_edit = c2.selectbox("Moneda", ["ARS","USD","EUR"],
                                    index=["ARS","USD","EUR"].index(cuenta['Moneda'])
                                    if cuenta['Moneda'] in ["ARS","USD","EUR"] else 0,
                                    key=f"edit_moneda_{nro_edit}")
        es_resultado_por_tipo = "Resultado" if tipo_es_resultado(tipo_edit) else "No Resultado"
        es_resultado_sel_edit = c3.selectbox("Es Resultado", ["No Resultado","Resultado"],
                                              index=["No Resultado","Resultado"].index(es_resultado_por_tipo),
                                              key=f"edit_es_resultado_{nro_edit}")
        es_resultado_edit = "S" if es_resultado_sel_edit == "Resultado" else "N"

        if c4.button("💾 Guardar cambios", type="primary", key=f"btn_edit_{nro_edit}"):
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
                            analisis=%s, fases=%s, tipo=%s, moneda=%s, es_resultado=%s
                        WHERE nro_cta=%s
                    """, (nombre_edit.strip() or None, extendido_edit.strip() or None,
                          rubro_final_edit or None, subrubro_final_edit or None,
                          analisis_final_edit or None, fases_final_edit or None,
                          tipo_edit, moneda_edit, es_resultado_edit, nro_edit))
                    conn.commit(); cur.close()
                    st.session_state['msg_cuenta_edit'] = f"✅ Cuenta **{nro_edit} — {nombre_edit.strip()}** actualizada correctamente."
                    st.rerun()
                except Exception as e:
                    conn.rollback(); st.error(f"Error: {e}")

    st.divider()
    for _mk in ['msg_cuenta_edit','msg_cuenta_nueva','msg_cuenta_eliminada']:
        if _mk in st.session_state: st.success(st.session_state.pop(_mk))

    # ── Alta de cuenta nueva ───────────────────────────────────────────────────
    with st.expander("➕ Agregar nueva cuenta"):
        c1, c2, c3 = st.columns(3)
        nro_cta_new   = c1.number_input("Nro Cta *", min_value=1, step=1, key="new_nro_cta")
        extendido_new = c2.text_input("Extendido", placeholder="ej: 1.05.01.001", key="new_extendido")
        nombre_new    = c3.text_input("Nombre *", placeholder="ej: Maquinaria y Equipo", key="new_nombre")

        c1, c2, c3, c4, c5 = st.columns(5)
        tipo_new = c3.selectbox("Tipo *", TIPOS_CUENTA, key="new_tipo")
        es_resultado_por_tipo_new = "Resultado" if tipo_es_resultado(tipo_new) else "No Resultado"
        es_resultado_sel_new = c4.selectbox("Es Resultado", ["No Resultado","Resultado"],
                                             index=["No Resultado","Resultado"].index(es_resultado_por_tipo_new),
                                             key="new_es_resultado")
        es_resultado_new = "S" if es_resultado_sel_new == "Resultado" else "N"
        moneda_new = c5.selectbox("Moneda", ["ARS","USD","EUR"], key="new_moneda")

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
            elif sub_sel_new == "— Sin sub-rubro —": subrubro_final_new = ""
            else:
                subrubro_final_new = sub_sel_new
                analisis_new_list = get_analisis_por_subrubro(conn, sub_sel_new)
        elif es_rubro_nuevo_new:
            subrubro_final_new = c2.text_input("Sub-rubro (opcional)", key="new_subrubro_libre")

        analisis_final_new = ""
        if subrubro_final_new and not es_rubro_nuevo_new:
            opts_an_new = ["— Sin análisis —"] + analisis_new_list + ["✨ + Nuevo análisis..."]
            an_sel_new = st.selectbox("Análisis", opts_an_new, key="new_analisis_sel")
            if an_sel_new == "✨ + Nuevo análisis...":
                nuevo_an_new = st.text_input("Nombre del análisis *", key="new_nuevo_an_nom")
                if nuevo_an_new and validar_analisis_nuevo(conn, subrubro_final_new, nuevo_an_new):
                    st.error(f"❌ El análisis **{nuevo_an_new}** ya existe en **{subrubro_final_new}**.")
                analisis_final_new = nuevo_an_new
            elif an_sel_new == "— Sin análisis —": analisis_final_new = ""
            else: analisis_final_new = an_sel_new
        elif subrubro_final_new:
            analisis_final_new = st.text_input("Análisis (opcional)", key="new_analisis_libre")

        opts_fases_new = ["— Sin fases —"] + fases_all + ["✨ + Nueva fase..."]
        fases_sel_new = st.selectbox("Fases", opts_fases_new, key="new_fases_sel")
        if fases_sel_new == "✨ + Nueva fase...":
            fases_final_new = st.text_input("Nombre de la fase *", key="new_nueva_fase")
        elif fases_sel_new == "— Sin fases —": fases_final_new = ""
        else: fases_final_new = fases_sel_new

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
                            (nro_cta, extendido, nombre, rubro, sub_rubro, analisis, fases,
                             tipo, moneda, es_resultado)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (nro_cta_new, extendido_new.strip() or None, nombre_new.strip(),
                          rubro_final_new.strip() or None, subrubro_final_new.strip() or None,
                          analisis_final_new.strip() or None, fases_final_new.strip() or None,
                          tipo_new, moneda_new, es_resultado_new))
                    conn.commit(); cur.close()
                    st.session_state['msg_cuenta_nueva'] = f"✅ Cuenta **{nro_cta_new} — {nombre_new.strip()}** agregada al plan de cuentas."
                    st.rerun()
                except Exception as e:
                    conn.rollback(); st.error(f"Error: {e}")

    # ── Eliminar cuenta ────────────────────────────────────────────────────────
    st.divider()
    with st.expander("🗑️ Eliminar cuenta"):
        st.caption("Solo se pueden eliminar cuentas que no tengan movimientos en el Libro Diario.")
        opciones_eliminar = ["— Seleccioná una cuenta —"] + [
            f"{int(r['Nro Cta'])} — {r['Nombre']}" for _, r in df_cta.iterrows()
        ]
        sel_eliminar = st.selectbox("Cuenta a eliminar", opciones_eliminar, key="sel_eliminar")
        if sel_eliminar != "— Seleccioná una cuenta —":
            nro_del = int(sel_eliminar.split(" — ")[0])
            cuenta_del = df_cta[df_cta['Nro Cta'] == nro_del].iloc[0]
            st.markdown(f"""
            | Campo | Valor |
            |---|---|
            | **Nro Cta** | {nro_del} |
            | **Nombre** | {cuenta_del['Nombre']} |
            | **Rubro** | {cuenta_del['Rubro'] or '—'} |
            | **Tipo** | {cuenta_del['Tipo'] or '—'} |
            | **Extendido** | {cuenta_del['Extendido'] or '—'} |
            """)
            n_mov = cuenta_tiene_movimientos(conn, nro_del)
            if n_mov > 0:
                st.error(f"❌ La cuenta **{nro_del}** tiene **{n_mov} movimiento(s)** y no puede eliminarse.")
            else:
                st.warning(f"⚠️ Esta acción es **irreversible**. La cuenta **{nro_del} — {cuenta_del['Nombre']}** será eliminada permanentemente.")
                if st.checkbox(f"Confirmo que quiero eliminar la cuenta **{nro_del} — {cuenta_del['Nombre']}**", key=f"confirm_del_{nro_del}"):
                    if st.button("🗑️ Eliminar cuenta", type="primary", key=f"btn_del_{nro_del}"):
                        try:
                            cur = conn.cursor()
                            cur.execute("DELETE FROM dim_cuenta WHERE nro_cta = %s", (nro_del,))
                            conn.commit(); cur.close()
                            st.session_state['msg_cuenta_eliminada'] = f"✅ Cuenta **{nro_del} — {cuenta_del['Nombre']}** eliminada."
                            st.rerun()
                        except Exception as e:
                            conn.rollback(); st.error(f"❌ Error: {e}")

# ── Tab 3: Actualizar Plan ─────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("📥 Actualizar Plan de Cuentas")
    st.caption("Cargá un Excel o CSV para agregar y actualizar cuentas. Las cuentas existentes no se eliminan.")

    if 'plan_cargado' in st.session_state:
        r = st.session_state['plan_cargado']
        cols_agr = r.get('cols_agregadas', [])
        msg = (f"✅ Plan actualizado desde **{r['archivo']}** — "
               f"{r['nuevas']} nuevas, {r['actualizadas']} actualizadas, "
               f"{r['renombradas']} renombradas, {r['excluidas']} renombres omitidos")
        if cols_agr:
            msg += f", {len(cols_agr)} columna(s) nueva(s) agregada(s) a la DB"
        st.success(msg + ".")
        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Nuevas",       r['nuevas'])
        c2.metric("Actualizadas", r['actualizadas'])
        c3.metric("Renombradas",  r['renombradas'])
        c4.metric("Total",        r['nuevas'] + r['actualizadas'])
        if cols_agr:
            st.info(f"🆕 Columnas agregadas a dim_cuenta: {', '.join(f'{c} ({t})' for c,t in cols_agr)}")
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
        df_plan, errores_plan, adv_plan, cols_extra_arch = parsear_plan_cuentas(archivo_plan)
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
        cuentas_existentes = en_archivo & en_db
        cuentas_nuevas     = en_archivo - en_db

        st.divider()
        c1, c2 = st.columns(2)
        c1.metric("Cuentas nuevas a agregar",        len(cuentas_nuevas))
        c2.metric("Cuentas existentes a actualizar", len(cuentas_existentes))

        # ── Detección y selección de columnas nuevas ──────────────────────────
        cols_nuevas_a_agregar = []   # [(col_db, tipo_sql, col_archivo)]
        cols_excluir_set      = set()

        if cols_extra_arch:
            cols_db_actuales = get_columnas_dim_cuenta(conn)
            for col_archivo, col_db in cols_extra_arch.items():
                if col_db not in cols_db_actuales:
                    tipo_sql = inferir_tipo_sql(df_plan[col_db])
                    cols_nuevas_a_agregar.append((col_db, tipo_sql, col_archivo))

            if cols_nuevas_a_agregar:
                st.divider()
                st.markdown("#### 🆕 Columnas nuevas detectadas")
                st.caption("Estas columnas no existen en la DB. Destildá las que no querés agregar.")
                for col_db, tipo_sql, col_archivo in cols_nuevas_a_agregar:
                    incluir = st.checkbox(
                        f"Agregar **{col_archivo}** → `{col_db}` ({tipo_sql})",
                        value=True, key=f"chk_col_{col_db}"
                    )
                    if not incluir:
                        cols_excluir_set.add(col_db)
            else:
                cols_ya_en_db = list(cols_extra_arch.values())
                if cols_ya_en_db:
                    st.info(f"ℹ️ Columnas extra del archivo ya existen en la DB: {', '.join(cols_ya_en_db)}")

        # ── Detección y selección de cambios de nombre ────────────────────────
        nombres_db    = get_nombres_actuales(conn, list(cuentas_existentes))
        df_existentes = df_plan[df_plan['nro_cta'].isin(cuentas_existentes)].copy()

        renombradas_lista = []
        for _, row in df_existentes.iterrows():
            nro = int(row['nro_cta'])
            nombre_nuevo  = row.get('nombre') or ''
            nombre_actual = nombres_db.get(nro) or ''
            if nombre_nuevo.strip().lower() != nombre_actual.strip().lower() and nombre_nuevo.strip():
                renombradas_lista.append({
                    'nro':           nro,
                    'Nombre actual': nombre_actual,
                    'Nombre nuevo':  nombre_nuevo.strip(),
                })

        nros_excluir_renombre = set()

        if renombradas_lista:
            st.divider()
            st.markdown("#### ✏️ Cambios de nombre detectados")
            st.caption("Destildá los cambios que no querés aplicar.")
            for item in renombradas_lista:
                aplicar = st.checkbox(
                    f"**{item['nro']}** — ~~{item['Nombre actual']}~~ → **{item['Nombre nuevo']}**",
                    value=True, key=f"chk_rename_{item['nro']}"
                )
                if not aplicar:
                    nros_excluir_renombre.add(item['nro'])

            n_aplicar = len(renombradas_lista) - len(nros_excluir_renombre)
            n_omitir  = len(nros_excluir_renombre)
            if n_omitir:
                st.info(f"Se aplicarán **{n_aplicar}** renombres y se omitirán **{n_omitir}**.")

        # ── Validación contra Libro Diario ────────────────────────────────────
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
                st.dataframe(pd.DataFrame(rows, columns=['Nro Cuenta','Movimientos','Primer período','Último período']),
                             use_container_width=True, hide_index=True)
            continuar = st.checkbox(
                "✅ Entiendo que estas cuentas no tendrán clasificación. Continuar de todas formas.",
                key="plan_continuar_con_faltantes")
        else:
            st.success("✅ Todas las cuentas del Libro Diario están cubiertas por el plan.")
            continuar = True

        # ── Botón aplicar ─────────────────────────────────────────────────────
        st.divider()
        if continuar:
            if st.button("📥 Aplicar actualización del plan", type="primary"):
                conn2 = get_conn()
                with st.spinner("Actualizando plan de cuentas..."):
                    try:
                        # 1. Agregar columnas nuevas seleccionadas
                        cols_agregadas = []
                        if cols_nuevas_a_agregar:
                            cols_para_alter = [
                                (c, t) for c, t, _ in cols_nuevas_a_agregar
                                if c not in cols_excluir_set
                            ]
                            if cols_para_alter:
                                cols_agregadas = agregar_columnas_nuevas(conn2, cols_para_alter)

                        # 2. Upsert con exclusiones seleccionadas por el usuario
                        n_nuevas, n_act = aplicar_upsert_plan(
                            conn2, df_plan,
                            nros_excluir_renombre=nros_excluir_renombre,
                            cols_excluir=cols_excluir_set,
                        )
                        st.session_state['plan_cargado'] = {
                            'archivo':        archivo_plan.name,
                            'nuevas':         n_nuevas,
                            'actualizadas':   n_act,
                            'renombradas':    len(renombradas_lista) - len(nros_excluir_renombre),
                            'excluidas':      len(nros_excluir_renombre),
                            'cols_agregadas': cols_agregadas,
                        }
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Error al actualizar: {e}")

# ── Tab 4: Proyectos ───────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("🏗️ Proyectos")
    st.caption("Gestión de proyectos y presupuestos. Se vincula con libro_mayor por centro de costo.")

    # ── Pantallas de éxito — ANTES de los subtabs para que el rerun las muestre ──
    if 'proyectos_cargados' in st.session_state:
        r = st.session_state['proyectos_cargados']
        st.success(
            f"✅ Proyectos actualizados desde **{r['archivo']}** — "
            f"{r['nuevos']} nuevos, {r['actualizados']} actualizados, "
            f"{r['inactivos']} marcados inactivos."
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("Nuevos",       r['nuevos'])
        c2.metric("Actualizados", r['actualizados'])
        c3.metric("Inactivos",    r['inactivos'])
        st.divider()
        if st.button("📥 Cargar otro archivo de proyectos", type="primary", key="btn_otro_proy"):
            st.session_state.pop('proyectos_cargados', None); st.rerun()
        st.stop()

    if 'presupuestos_cargados' in st.session_state:
        r = st.session_state['presupuestos_cargados']
        st.success(
            f"✅ Presupuestos actualizados desde **{r['archivo']}** — "
            f"{r['nuevos']} nuevos, {r['actualizados']} actualizados."
        )
        c1, c2 = st.columns(2)
        c1.metric("Nuevos",       r['nuevos'])
        c2.metric("Actualizados", r['actualizados'])
        st.divider()
        if st.button("📊 Cargar otro archivo de presupuestos", type="primary", key="btn_otro_presp"):
            st.session_state.pop('presupuestos_cargados', None); st.rerun()
        st.stop()

    subtab_lista, subtab_proyectos, subtab_presupuestos = st.tabs([
        "📋 Listado", "📥 Actualizar Proyectos", "📊 Cargar Presupuestos"
    ])

    # ── Subtab: Listado ────────────────────────────────────────────────────────
    with subtab_lista:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT id_origen, ccosto, nombre, estado, fc_inicio, fc_fin,
                       ingresos, cto_mo_propia, cto_mo_terceros, cto_materiales,
                       cto_herramientas, superficie, comentario, activo, actualizado_en
                FROM proyectos ORDER BY nombre
            """)
            cols_p = ['ID Origen','CCosto','Nombre','Estado','Inicio','Fin',
                      'Ingresos','Cto MO Propia','Cto MO Terceros','Cto Materiales',
                      'Cto Herramientas','Superficie m²','Comentario','Activo','Actualizado']
            df_proy = pd.DataFrame(cur.fetchall(), columns=cols_p); cur.close()
        except Exception:
            conn = get_conn(); df_proy = pd.DataFrame()

        if not df_proy.empty:
            activos   = df_proy['Activo'].sum() if 'Activo' in df_proy.columns else 0
            inactivos = len(df_proy) - activos
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total proyectos", len(df_proy))
            c2.metric("Activos",         int(activos))
            c3.metric("Inactivos",       int(inactivos))
            c4.metric("Ingresos total",  f"${df_proy['Ingresos'].astype(float).sum():,.0f}")

            mostrar_inactivos = st.checkbox("Mostrar inactivos", value=False, key="chk_inactivos_proy")
            df_show_p = df_proy if mostrar_inactivos else df_proy[df_proy['Activo'] == True]
            st.dataframe(df_show_p, use_container_width=True, hide_index=True,
                column_config={
                    "Ingresos":        st.column_config.NumberColumn(format="$ %.0f"),
                    "Cto MO Propia":   st.column_config.NumberColumn(format="$ %.0f"),
                    "Cto MO Terceros": st.column_config.NumberColumn(format="$ %.0f"),
                    "Cto Materiales":  st.column_config.NumberColumn(format="$ %.0f"),
                    "Cto Herramientas":st.column_config.NumberColumn(format="$ %.0f"),
                    "Actualizado":     st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm"),
                })

            # Presupuestos
            st.divider()
            st.markdown("#### 📊 Presupuestos cargados")
            try:
                cur2 = conn.cursor()
                cur2.execute("""
                    SELECT pp.id, p.nombre, pp.fecha, pp.mo_propia, pp.mo_terceros,
                           pp.materiales, pp.herramientas, pp.horas, pp.metros, pp.importe
                    FROM proy_presupuestos pp
                    JOIN proyectos p ON p.id_origen = pp.proyecto_id
                    ORDER BY pp.fecha DESC, p.nombre
                """)
                cols_pp = ['ID','Proyecto','Fecha','MO Propia','MO Terceros',
                           'Materiales','Herramientas','Horas','Metros','Importe']
                df_pp = pd.DataFrame(cur2.fetchall(), columns=cols_pp); cur2.close()
            except Exception:
                df_pp = pd.DataFrame()

            if df_pp.empty:
                st.info("No hay presupuestos cargados aún.")
            else:
                c1, c2 = st.columns(2)
                c1.metric("Registros", len(df_pp))
                c2.metric("Importe total", f"${df_pp['Importe'].astype(float).sum():,.0f}")
                st.dataframe(df_pp, use_container_width=True, hide_index=True,
                    column_config={
                        "MO Propia":    st.column_config.NumberColumn(format="$ %.0f"),
                        "MO Terceros":  st.column_config.NumberColumn(format="$ %.0f"),
                        "Materiales":   st.column_config.NumberColumn(format="$ %.0f"),
                        "Herramientas": st.column_config.NumberColumn(format="$ %.0f"),
                        "Importe":      st.column_config.NumberColumn(format="$ %.0f"),
                        "Fecha":        st.column_config.DateColumn(format="DD/MM/YYYY"),
                    })
        else:
            st.info("No hay proyectos cargados aún.")

    # ── Subtab: Actualizar Proyectos ───────────────────────────────────────────
    with subtab_proyectos:
        st.caption("Subí el CSV de proyectos. Los existentes se actualizan, los nuevos se agregan, los eliminados se marcan inactivos.")
        st.code("id, nombre, fecha_inicio, fecha_final, estado, importe_mat, importe_mo, terceros, herramientas, superficie, ingresos, centro_costo, comentario, deleted_at, ...")

        archivo_proy = st.file_uploader(
            "CSV de proyectos", type=["csv","xlsx"], key="proy_uploader_new"
        )
        if archivo_proy:
                # Parsear
                try:
                    if archivo_proy.name.endswith('.xlsx'):
                        df_raw_p = pd.read_excel(archivo_proy, dtype=str)
                    else:
                        for enc in ['latin-1','utf-8-sig','utf-8']:
                            try:
                                archivo_proy.seek(0)
                                df_raw_p = pd.read_csv(archivo_proy, sep=',', dtype=str, encoding=enc).fillna('')
                                break
                            except UnicodeDecodeError: continue
                except Exception as e:
                    st.error(f"❌ Error al leer archivo: {e}"); st.stop()

                df_raw_p.columns = [c.strip() for c in df_raw_p.columns]

                # Validar columnas mínimas
                required = {'id','nombre'}
                missing = required - set(df_raw_p.columns)
                if missing:
                    st.error(f"❌ Faltan columnas requeridas: {missing}"); st.stop()

                def pnum(v):
                    try: return float(str(v).strip().replace(',','.')) if str(v).strip() else None
                    except: return None

                def pdate(v):
                    try: return pd.Timestamp(str(v).strip()).date() if str(v).strip() else None
                    except: return None

                df_raw_p['id']           = pd.to_numeric(df_raw_p['id'], errors='coerce')
                df_raw_p = df_raw_p[df_raw_p['id'].notna()].copy()
                df_raw_p['id'] = df_raw_p['id'].astype(int)

                # Mapeo de columnas CSV → DB
                col_map_p = {
                    'fecha_inicio':  'fc_inicio',
                    'fecha_final':   'fc_fin',
                    'importe_mat':   'cto_materiales',
                    'importe_mo':    'cto_mo_propia',
                    'terceros':      'cto_mo_terceros',
                    'herramientas':  'cto_herramientas',
                    'centro_costo':  'ccosto',
                }
                df_raw_p = df_raw_p.rename(columns=col_map_p)

                # Métricas preview
                cur3 = conn.cursor()
                cur3.execute("SELECT id_origen FROM proyectos WHERE id_origen IS NOT NULL")
                ids_db = {r[0] for r in cur3.fetchall()}; cur3.close()

                ids_arch    = set(df_raw_p['id'].tolist())
                deleted     = df_raw_p[df_raw_p.get('deleted_at', pd.Series([''] * len(df_raw_p))) != '']['id'].tolist() if 'deleted_at' in df_raw_p.columns else []
                nuevos_ids  = ids_arch - ids_db - set(deleted)
                act_ids     = ids_arch & ids_db - set(deleted)

                st.divider()
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total en archivo",  len(df_raw_p))
                c2.metric("Proyectos nuevos",  len(nuevos_ids))
                c3.metric("A actualizar",      len(act_ids))
                c4.metric("A marcar inactivos", len(deleted))

                # Preview
                cols_show = [c for c in ['id','nombre','estado','fc_inicio','fc_fin',
                              'ccosto','ingresos','cto_materiales','cto_mo_propia',
                              'cto_mo_terceros','cto_herramientas','superficie','comentario']
                             if c in df_raw_p.columns]
                st.dataframe(df_raw_p[cols_show].head(20), use_container_width=True, hide_index=True)

                # Advertencia inactivos
                if deleted:
                    st.warning(f"⚠️ {len(deleted)} proyecto(s) con `deleted_at` serán marcados como **inactivos**: IDs {deleted}")

                st.divider()
                if st.button("🏗️ Aplicar actualización de proyectos", type="primary", key="btn_aplicar_proy"):
                    conn2 = get_conn()
                    cur4 = conn2.cursor()
                    n_nuevos = n_act = n_inact = 0
                    try:
                        from datetime import datetime
                        ahora = datetime.now()
                        for _, row in df_raw_p.iterrows():
                            id_orig = int(row['id'])
                            es_deleted = bool(row.get('deleted_at','').strip()) if 'deleted_at' in row else False

                            vals = {
                                'id_origen':       id_orig,
                                'nombre':          str(row.get('nombre','')).strip(),
                                'ccosto':          str(row.get('ccosto','')).strip() or None,
                                'fc_inicio':       pdate(row.get('fc_inicio','')),
                                'fc_fin':          pdate(row.get('fc_fin','')),
                                'estado':          str(row.get('estado','')).strip() or None,
                                'ingresos':        pnum(row.get('ingresos',0)),
                                'cto_mo_propia':   pnum(row.get('cto_mo_propia',0)),
                                'cto_mo_terceros': pnum(row.get('cto_mo_terceros',0)),
                                'cto_materiales':  pnum(row.get('cto_materiales',0)),
                                'cto_herramientas':pnum(row.get('cto_herramientas',0)),
                                'superficie':      pnum(row.get('superficie','')),
                                'comentario':      str(row.get('comentario','')).strip() or None,
                                'oportunidad_id':  int(float(row['oportunidad_id'])) if str(row.get('oportunidad_id','')).strip() else None,
                                'responsable_id':  int(float(row['responsable_id'])) if str(row.get('responsable_id','')).strip() else None,
                                'version':         int(float(row['version'])) if str(row.get('version','')).strip() else None,
                                'activo':          not es_deleted,
                                'deleted_at':      pdate(row.get('deleted_at','')) if es_deleted else None,
                                'actualizado_en':  ahora,
                            }

                            # ccosto puede ser None para proyectos sin centro de costo
                            # usar id_origen como clave de upsert
                            cur4.execute("SELECT ccosto FROM proyectos WHERE id_origen = %s", (id_orig,))
                            existe = cur4.fetchone()

                            if existe:
                                cur4.execute("""
                                    UPDATE proyectos SET
                                        nombre=%s, ccosto=%s, fc_inicio=%s, fc_fin=%s,
                                        estado=%s, ingresos=%s, cto_mo_propia=%s,
                                        cto_mo_terceros=%s, cto_materiales=%s, cto_herramientas=%s,
                                        superficie=%s, comentario=%s, oportunidad_id=%s,
                                        responsable_id=%s, version=%s, activo=%s,
                                        deleted_at=%s, actualizado_en=%s
                                    WHERE id_origen=%s
                                """, (vals['nombre'], vals['ccosto'], vals['fc_inicio'], vals['fc_fin'],
                                      vals['estado'], vals['ingresos'], vals['cto_mo_propia'],
                                      vals['cto_mo_terceros'], vals['cto_materiales'], vals['cto_herramientas'],
                                      vals['superficie'], vals['comentario'], vals['oportunidad_id'],
                                      vals['responsable_id'], vals['version'], vals['activo'],
                                      vals['deleted_at'], vals['actualizado_en'], id_orig))
                                if es_deleted: n_inact += 1
                                else: n_act += 1
                            else:
                                # Para proyectos sin ccosto usar id como ccosto temporalmente
                                ccosto_final = vals['ccosto'] or str(id_orig)
                                cur4.execute("""
                                    INSERT INTO proyectos (
                                        id_origen, ccosto, nombre, fc_inicio, fc_fin,
                                        estado, ingresos, cto_mo_propia, cto_mo_terceros,
                                        cto_materiales, cto_herramientas, superficie,
                                        comentario, oportunidad_id, responsable_id,
                                        version, activo, deleted_at, actualizado_en
                                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """, (id_orig, ccosto_final, vals['nombre'], vals['fc_inicio'], vals['fc_fin'],
                                      vals['estado'], vals['ingresos'], vals['cto_mo_propia'], vals['cto_mo_terceros'],
                                      vals['cto_materiales'], vals['cto_herramientas'], vals['superficie'],
                                      vals['comentario'], vals['oportunidad_id'], vals['responsable_id'],
                                      vals['version'], vals['activo'], vals['deleted_at'], vals['actualizado_en']))
                                if es_deleted: n_inact += 1
                                else: n_nuevos += 1

                        conn2.commit(); cur4.close()
                        st.session_state['proyectos_cargados'] = {
                            'archivo': archivo_proy.name,
                            'nuevos': n_nuevos, 'actualizados': n_act, 'inactivos': n_inact
                        }
                        st.rerun()
                    except Exception as e:
                        conn2.rollback(); cur4.close()
                        st.error(f"❌ Error al actualizar proyectos: {e}")

    # ── Subtab: Cargar Presupuestos ────────────────────────────────────────────
    with subtab_presupuestos:
        st.caption("Subí el CSV de presupuestos por período. Los registros existentes (mismo ID) se actualizan, los nuevos se agregan.")
        st.code("id, proyecto_id, fecha, mo_propia, mo_terceros, materiales, herramientas, horas, metros, importe, descripcion")

        archivo_presp = st.file_uploader(
            "CSV de presupuestos", type=["csv","xlsx"], key="presp_uploader"
        )
        if archivo_presp:
                try:
                    if archivo_presp.name.endswith('.xlsx'):
                        df_presp = pd.read_excel(archivo_presp, dtype=str)
                    else:
                        for enc in ['latin-1','utf-8-sig','utf-8']:
                            try:
                                archivo_presp.seek(0)
                                df_presp = pd.read_csv(archivo_presp, sep=',', dtype=str, encoding=enc).fillna('')
                                break
                            except UnicodeDecodeError: continue
                except Exception as e:
                    st.error(f"❌ Error al leer archivo: {e}"); st.stop()

                df_presp.columns = [c.strip() for c in df_presp.columns]

                # Validar columnas mínimas
                req_pp = {'id','proyecto_id','fecha'}
                miss_pp = req_pp - set(df_presp.columns)
                if miss_pp:
                    st.error(f"❌ Faltan columnas requeridas: {miss_pp}"); st.stop()

                df_presp['id']          = pd.to_numeric(df_presp['id'], errors='coerce')
                df_presp['proyecto_id'] = pd.to_numeric(df_presp['proyecto_id'], errors='coerce')
                df_presp = df_presp[df_presp['id'].notna() & df_presp['proyecto_id'].notna()].copy()
                df_presp['id']          = df_presp['id'].astype(int)
                df_presp['proyecto_id'] = df_presp['proyecto_id'].astype(int)

                # Validar que los proyecto_id existan en DB
                cur5 = conn.cursor()
                cur5.execute("SELECT DISTINCT id_origen FROM proyectos WHERE id_origen IS NOT NULL")
                ids_proy_db = {r[0] for r in cur5.fetchall()}

                cur5.execute("SELECT id FROM proy_presupuestos")
                ids_presp_db = {r[0] for r in cur5.fetchall()}
                cur5.close()

                proy_ids_arch    = set(df_presp['proyecto_id'].unique())
                proy_invalidos   = proy_ids_arch - ids_proy_db
                ids_presp_nuevos = set(df_presp['id'].tolist()) - ids_presp_db
                ids_presp_act    = set(df_presp['id'].tolist()) & ids_presp_db

                # Métricas
                c1, c2, c3 = st.columns(3)
                c1.metric("Registros en archivo", len(df_presp))
                c2.metric("Nuevos",               len(ids_presp_nuevos))
                c3.metric("A actualizar",         len(ids_presp_act))

                if proy_invalidos:
                    st.error(f"❌ proyecto_id no encontrados en la DB: {sorted(proy_invalidos)}. Cargá primero los proyectos.")
                    st.stop()

                # Preview
                st.dataframe(df_presp.head(20), use_container_width=True, hide_index=True)
                st.divider()

                if st.button("📊 Aplicar carga de presupuestos", type="primary", key="btn_aplicar_presp"):
                    conn2 = get_conn(); cur6 = conn2.cursor()
                    n_nuevos_pp = n_act_pp = 0
                    try:
                        def pnum2(v):
                            try: return float(str(v).strip().replace(',','.')) if str(v).strip() else 0.0
                            except: return 0.0

                        for _, row in df_presp.iterrows():
                            rid  = int(row['id'])
                            vals = (
                                int(row['proyecto_id']),
                                row.get('fecha','').strip(),
                                pnum2(row.get('mo_propia',0)),
                                pnum2(row.get('mo_terceros',0)),
                                pnum2(row.get('materiales',0)),
                                pnum2(row.get('herramientas',0)),
                                pnum2(row.get('horas',0)),
                                pnum2(row.get('metros',0)),
                                pnum2(row.get('importe',0)),
                                str(row.get('descripcion','')).strip() or None,
                            )
                            if rid in ids_presp_db:
                                cur6.execute("""
                                    UPDATE proy_presupuestos SET
                                        proyecto_id=%s, fecha=%s, mo_propia=%s, mo_terceros=%s,
                                        materiales=%s, herramientas=%s, horas=%s, metros=%s,
                                        importe=%s, descripcion=%s
                                    WHERE id=%s
                                """, (*vals, rid))
                                n_act_pp += 1
                            else:
                                cur6.execute("""
                                    INSERT INTO proy_presupuestos
                                        (id, proyecto_id, fecha, mo_propia, mo_terceros,
                                         materiales, herramientas, horas, metros, importe, descripcion)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                """, (rid, *vals))
                                n_nuevos_pp += 1

                        conn2.commit(); cur6.close()
                        st.session_state['presupuestos_cargados'] = {
                            'archivo': archivo_presp.name,
                            'nuevos': n_nuevos_pp, 'actualizados': n_act_pp
                        }
                        st.rerun()
                    except Exception as e:
                        conn2.rollback(); cur6.close()
                        st.error(f"❌ Error al cargar presupuestos: {e}")

# ── Tab 5: Centros de Costo ────────────────────────────────────────────────────
with tabs[4]:
    st.subheader("Centros de Costo")
    try: df_cc = get_centros(conn)
    except Exception: conn = get_conn(); df_cc = get_centros(conn)
    st.metric("Total centros", len(df_cc))
    st.dataframe(df_cc, use_container_width=True, hide_index=True)

    if 'msg_centro_nuevo' in st.session_state:
        st.success(st.session_state.pop('msg_centro_nuevo'))

    with st.expander("➕ Agregar centro de costo"):
        ca1, ca2, ca3 = st.columns(3)
        cod_new  = ca1.text_input("Código",      key="new_cc_cod")
        desc_new = ca2.text_input("Descripción", key="new_cc_desc")
        emp_new  = ca3.selectbox("Empresa (opcional)", ["—"] + list(EMPRESAS.keys()), key="new_cc_emp")
        if st.button("Guardar centro", key="btn_nuevo_cc"):
            errores_cc = []
            if not cod_new.strip():  errores_cc.append("El código es obligatorio.")
            if not desc_new.strip(): errores_cc.append("La descripción es obligatoria.")
            if errores_cc:
                for e in errores_cc: st.error(f"❌ {e}")
            else:
                cur = conn.cursor()
                try:
                    cur.execute("SELECT 1 FROM dim_centro_costo WHERE codigo = %s", (cod_new.strip(),))
                    if cur.fetchone():
                        st.error(f"❌ El código **{cod_new.strip()}** ya existe.")
                    else:
                        emp_id_new = EMPRESAS[emp_new] if emp_new != "—" else None
                        cur.execute("INSERT INTO dim_centro_costo (codigo, descripcion, empresa_id) VALUES (%s,%s,%s)",
                                    (cod_new.strip(), desc_new.strip(), emp_id_new))
                        conn.commit()
                        st.session_state['msg_centro_nuevo'] = f"✅ Centro de costo **{cod_new.strip()}** agregado correctamente."
                        st.rerun()
                except Exception as e:
                    conn.rollback(); st.error(f"❌ Error: {e}")
                finally:
                    cur.close()

# ── Tab 6: Log Recálculos ──────────────────────────────────────────────────────
with tabs[5]:
    st.subheader("Log de recálculos del Mayor")
    try: df_log = get_log(conn)
    except Exception: conn = get_conn(); df_log = get_log(conn)
    if df_log.empty: st.info("No hay recálculos registrados.")
    else: st.dataframe(df_log, use_container_width=True, hide_index=True)