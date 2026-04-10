import re, os, sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))
from services.db import get_conn
from services.styles import apply_styles, render_sidebar

st.set_page_config(page_title="Chequeo Consistencia · ReporteApp", page_icon="🔍", layout="wide")
apply_styles()
render_sidebar()

# ── Constantes ─────────────────────────────────────────────────────────────────
EMPRESAS = {1: 'BATIA', 2: 'NORFORK', 3: 'GUARE', 4: 'TORRES', 5: 'WERCOLICH'}
EMPRESA_KEYS = {v.lower(): k for k, v in EMPRESAS.items()}  # {'batia':1, ...}
TODAS_LAS_EMPRESAS = set(EMPRESAS.keys())
TOL = 1.0  # tolerancia $1

# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_num(v: str) -> float:
    s = str(v).strip()
    if not s: return 0.0
    s = s.replace('.', '').replace(',', '.')
    try: return float(s)
    except: return 0.0

def detectar_empresa(nombre: str):
    """Retorna (empresa_id, nombre_empresa) o (None, None)."""
    n = nombre.lower()
    for key, eid in EMPRESA_KEYS.items():
        if key in n:
            return eid, EMPRESAS[eid]
    return None, None

def detectar_periodo(nombre: str):
    """Busca patrón DD-MM o DD/MM en el nombre. Retorna (anio, mes) o (None, None)."""
    m = re.search(r'(\d{2})[-/](\d{2})', nombre)
    if m:
        mes = int(m.group(2))
        if 1 <= mes <= 12:
            return 2026, mes   # año hardcodeado; si se necesita dinámico se puede extender
    return None, None

def parsear_csv(archivo, empresa_id: int, anio: int, mes: int) -> pd.DataFrame:
    """Parsea un CSV del sistema contable. Retorna DataFrame con las cuentas hoja."""
    for enc in ['latin-1', 'utf-8-sig', 'utf-8']:
        try:
            archivo.seek(0)
            df = pd.read_csv(archivo, sep=';', dtype=str, encoding=enc).fillna('')
            break
        except (UnicodeDecodeError, Exception):
            continue
    else:
        raise ValueError("No se pudo leer el archivo con ningún encoding conocido.")

    df.columns = [c.strip() for c in df.columns]

    # Normalizar nombres de columnas
    if 'nrocta' not in df.columns:
        raise ValueError("El archivo no tiene columna 'nrocta'.")
    if 'descrip' not in df.columns and 'descripcion' not in df.columns:
        raise ValueError("El archivo no tiene columna 'descrip' o 'descripcion'.")
    if 'saldo_no_ajustado' not in df.columns:
        raise ValueError("El archivo no tiene columna 'saldo_no_ajustado'.")

    desc_col = 'descripcion' if 'descripcion' in df.columns else 'descrip'

    # Solo cuentas hoja (nrocta no vacío)
    hojas = df[df['nrocta'].str.strip() != ''].copy()

    hojas['cuenta_codigo']     = pd.to_numeric(hojas['nrocta'], errors='coerce')
    hojas['saldo_no_ajustado'] = hojas['saldo_no_ajustado'].apply(parse_num)
    hojas['descripcion']       = hojas[desc_col].astype(str).str.strip()
    hojas['empresa_id']        = empresa_id
    hojas['periodo_anio']      = anio
    hojas['periodo_mes']       = mes
    hojas['archivo_origen']    = getattr(archivo, 'name', 'desconocido')

    hojas = hojas[hojas['cuenta_codigo'].notna()].copy()
    hojas['cuenta_codigo'] = hojas['cuenta_codigo'].astype(int)

    return hojas[['archivo_origen','empresa_id','periodo_anio','periodo_mes',
                  'cuenta_codigo','descripcion','saldo_no_ajustado']].copy()

def get_estado_staging(conn) -> pd.DataFrame:
    """Devuelve resumen de qué empresas/períodos hay en stg_mayor_csv_cuenta."""
    cur = conn.cursor()
    cur.execute("""
        SELECT empresa_id, periodo_anio, periodo_mes,
               COUNT(*) AS cuentas, archivo_origen,
               MAX(cargado_en) AS ultima_carga
        FROM stg_mayor_csv_cuenta
        GROUP BY empresa_id, periodo_anio, periodo_mes, archivo_origen
        ORDER BY empresa_id
    """)
    rows = cur.fetchall(); cur.close()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=['empresa_id','anio','mes','cuentas','archivo','ultima_carga'])
    df['empresa'] = df['empresa_id'].map(EMPRESAS)
    df['periodo'] = df['anio'].astype(str) + '/' + df['mes'].astype(str).str.zfill(2)
    return df[['empresa','periodo','cuentas','archivo','ultima_carga']]

def cargar_staging(conn, df_total: pd.DataFrame):
    """TRUNCATE + INSERT de todos los datos en stg_mayor_csv_cuenta."""
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE stg_mayor_csv_cuenta;")
    rows = list(df_total[[
        'archivo_origen','empresa_id','periodo_anio','periodo_mes',
        'cuenta_codigo','descripcion','saldo_no_ajustado'
    ]].itertuples(index=False, name=None))
    execute_values(cur, """
        INSERT INTO stg_mayor_csv_cuenta
            (archivo_origen, empresa_id, periodo_anio, periodo_mes,
             cuenta_codigo, descripcion, saldo_no_ajustado)
        VALUES %s
    """, rows, page_size=500)
    conn.commit(); cur.close()

def run_comparacion(conn, empresas_ids: list, anio: int, mes: int) -> pd.DataFrame:
    """Ejecuta el FULL OUTER JOIN y devuelve todas las diferencias."""
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COALESCE(csv.empresa_id, lm.empresa_id)                     AS empresa_id,
            COALESCE(csv.cuenta_codigo, lm.cuenta_codigo)               AS cuenta_codigo,
            COALESCE(csv.descripcion, dc.nombre, lm.cuenta_codigo::text) AS descripcion,
            csv.saldo_no_ajustado                                        AS saldo_csv,
            lm.saldo_acumulado                                           AS saldo_db,
            ROUND((COALESCE(csv.saldo_no_ajustado,0)
                   - COALESCE(lm.saldo_acumulado,0))::numeric, 2)       AS diferencia,
            CASE
                WHEN csv.cuenta_codigo IS NULL THEN 'Solo en DB'
                WHEN lm.cuenta_codigo  IS NULL THEN 'Solo en CSV'
                ELSE 'Diferencia'
            END AS tipo
        FROM stg_mayor_csv_cuenta csv
        FULL OUTER JOIN libro_mayor lm
            ON  lm.empresa_id    = csv.empresa_id
            AND lm.cuenta_codigo = csv.cuenta_codigo
            AND lm.periodo_anio  = csv.periodo_anio
            AND lm.periodo_mes   = csv.periodo_mes
            AND lm.nivel         = 'cuenta'
        LEFT JOIN dim_cuenta dc
            ON  dc.nro_cta = COALESCE(csv.cuenta_codigo, lm.cuenta_codigo)
        WHERE
            csv.empresa_id = ANY(%s) OR lm.empresa_id = ANY(%s)
        ORDER BY
            COALESCE(csv.empresa_id, lm.empresa_id),
            ABS(COALESCE(csv.saldo_no_ajustado,0) - COALESCE(lm.saldo_acumulado,0)) DESC
    """, (empresas_ids, empresas_ids))
    rows = cur.fetchall(); cur.close()
    df = pd.DataFrame(rows, columns=[
        'empresa_id','cuenta_codigo','descripcion',
        'saldo_csv','saldo_db','diferencia','tipo'
    ])
    df['empresa'] = df['empresa_id'].map(EMPRESAS)
    return df

# ── UI ─────────────────────────────────────────────────────────────────────────
st.title("🔍 Chequeo de Consistencia")
st.caption("Comparación entre mayores del sistema contable y el Libro Mayor de la DB.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

tabs = st.tabs(["📁 Carga de Archivos", "📊 Comparación"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CARGA
# ══════════════════════════════════════════════════════════════════════════════
with tabs[0]:

    # ── Estado actual de la staging ───────────────────────────────────────────
    st.subheader("Estado actual de la tabla staging")
    try:
        df_estado = get_estado_staging(conn)
    except Exception:
        conn = get_conn(); df_estado = get_estado_staging(conn)

    empresas_en_staging = set()
    periodo_staging = (None, None)

    if df_estado.empty:
        st.info("La tabla staging está vacía. Subí los archivos para comenzar.")
    else:
        # Detectar empresas y período cargados
        try:
            cur2 = conn.cursor()
            cur2.execute("SELECT DISTINCT empresa_id, periodo_anio, periodo_mes FROM stg_mayor_csv_cuenta")
            for row in cur2.fetchall():
                empresas_en_staging.add(row[0])
                periodo_staging = (row[1], row[2])
            cur2.close()
        except Exception:
            pass

        c1, c2 = st.columns([3,1])
        with c1:
            st.dataframe(df_estado, use_container_width=True, hide_index=True,
                column_config={"ultima_carga": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")})
        with c2:
            st.markdown("**Empresas cargadas:**")
            for eid, enombre in EMPRESAS.items():
                if eid in empresas_en_staging:
                    st.success(f"✅ {enombre}")
                else:
                    st.warning(f"⚠️ {enombre}")

        if st.button("🗑️ Limpiar staging", type="secondary"):
            cur = conn.cursor()
            cur.execute("TRUNCATE TABLE stg_mayor_csv_cuenta;")
            conn.commit(); cur.close()
            st.success("✅ Staging limpiada correctamente.")
            st.rerun()

    st.divider()

    # ── Upload de archivos ────────────────────────────────────────────────────
    st.subheader("Subir archivos del sistema contable")
    st.caption("Subí los CSVs de todas las empresas. El sistema detecta empresa y período del nombre del archivo.")

    archivos = st.file_uploader(
        "Seleccioná los archivos (podés subir varios a la vez)",
        type=["csv","CSV"],
        accept_multiple_files=True,
        key="uploader_mayores"
    )

    if archivos:
        st.markdown("#### Vista previa de archivos detectados")

        previews = []      # lista de dicts con metadata
        dfs_validos = []   # DataFrames parseados
        hay_errores = False

        for arch in archivos:
            nombre = arch.name
            emp_id, emp_nombre = detectar_empresa(nombre)
            anio, mes = detectar_periodo(nombre)

            row = {
                'archivo': nombre,
                'empresa_id': emp_id,
                'empresa': emp_nombre or '❓ No detectada',
                'anio': anio,
                'mes': mes,
                'periodo': f"{anio}/{str(mes).zfill(2)}" if anio and mes else '❓ No detectado',
                'estado': '',
                'cuentas': 0,
                'error': '',
            }

            # Override manual si no se detectó empresa
            if emp_id is None:
                st.warning(f"⚠️ No se detectó empresa para **{nombre}**. Seleccioná manualmente:")
                emp_sel = st.selectbox(
                    f"Empresa para {nombre}",
                    options=list(EMPRESAS.keys()),
                    format_func=lambda x: EMPRESAS[x],
                    key=f"emp_manual_{nombre}"
                )
                emp_id = emp_sel
                emp_nombre = EMPRESAS[emp_sel]
                row['empresa_id'] = emp_id
                row['empresa'] = emp_nombre

            # Override manual si no se detectó período
            if anio is None or mes is None:
                st.warning(f"⚠️ No se detectó período para **{nombre}**. Ingresalo manualmente:")
                col1, col2 = st.columns(2)
                anio = col1.number_input("Año", min_value=2020, max_value=2030, value=2026, key=f"anio_{nombre}")
                mes  = col2.number_input("Mes", min_value=1, max_value=12, value=1, key=f"mes_{nombre}")
                row['anio'] = anio; row['mes'] = mes
                row['periodo'] = f"{anio}/{str(mes).zfill(2)}"

            # Parsear
            try:
                df_parsed = parsear_csv(arch, emp_id, int(anio), int(mes))
                row['cuentas'] = len(df_parsed)
                row['estado']  = '✅ OK'
                dfs_validos.append(df_parsed)
            except Exception as e:
                row['estado'] = '❌ Error'
                row['error']  = str(e)
                hay_errores   = True

            previews.append(row)

        # Tabla resumen de archivos
        df_preview = pd.DataFrame(previews)[['archivo','empresa','periodo','cuentas','estado','error']]
        st.dataframe(df_preview, use_container_width=True, hide_index=True)

        if hay_errores:
            st.error("❌ Hay archivos con errores. Corregílos antes de cargar.")
        else:
            # Verificar que estén las 5 empresas
            empresas_en_archivos = {p['empresa_id'] for p in previews}
            empresas_faltantes   = TODAS_LAS_EMPRESAS - empresas_en_archivos

            # Verificar período único
            periodos = {(p['anio'], p['mes']) for p in previews}
            periodo_mixto = len(periodos) > 1

            if periodo_mixto:
                st.error("❌ Los archivos tienen períodos distintos. Todos deben ser del mismo mes.")
            elif empresas_faltantes:
                nombres_faltantes = [EMPRESAS[e] for e in sorted(empresas_faltantes)]
                st.warning(
                    f"⚠️ Faltan archivos de: **{', '.join(nombres_faltantes)}**. "
                    f"No se puede cargar hasta tener las 5 empresas."
                )
            else:
                # Todo OK — mostrar resumen y botón de carga
                anio_final, mes_final = list(periodos)[0]
                total_cuentas = sum(p['cuentas'] for p in previews)
                st.success(f"✅ Las 5 empresas detectadas. Período: {anio_final}/{str(mes_final).zfill(2)} | Total cuentas: {total_cuentas}")

                if st.button("📥 Cargar en staging (reemplaza todo)", type="primary", key="btn_cargar"):
                    df_total = pd.concat(dfs_validos, ignore_index=True)
                    conn2 = get_conn()
                    with st.spinner("Cargando en base de datos..."):
                        try:
                            cargar_staging(conn2, df_total)
                            st.session_state['carga_ok'] = (
                                f"✅ Staging actualizada correctamente — "
                                f"5 empresas, período {anio_final}/{str(mes_final).zfill(2)}, "
                                f"{total_cuentas} cuentas cargadas."
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al cargar: {e}")

    if 'carga_ok' in st.session_state:
        st.success(st.session_state.pop('carga_ok'))

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — COMPARACIÓN
# ══════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.subheader("Comparación CSV vs Libro Mayor DB")

    # Verificar que haya datos en staging
    try:
        cur_chk = conn.cursor()
        cur_chk.execute("""
            SELECT empresa_id, periodo_anio, periodo_mes, COUNT(*) 
            FROM stg_mayor_csv_cuenta 
            GROUP BY empresa_id, periodo_anio, periodo_mes
            ORDER BY empresa_id
        """)
        staging_rows = cur_chk.fetchall(); cur_chk.close()
    except Exception:
        staging_rows = []

    if not staging_rows:
        st.warning("⚠️ La staging está vacía. Primero cargá los archivos en la pestaña anterior.")
        st.stop()

    # Verificar que estén las 5 empresas
    empresas_staging = {r[0] for r in staging_rows}
    faltantes_comp   = TODAS_LAS_EMPRESAS - empresas_staging
    anio_stg         = staging_rows[0][1]
    mes_stg          = staging_rows[0][2]

    # Info del staging actual
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Empresas cargadas", f"{len(empresas_staging)}/5")
    c2.metric("Período", f"{anio_stg}/{str(mes_stg).zfill(2)}")
    c3.metric("Total cuentas CSV", sum(r[3] for r in staging_rows))
    c4.metric("Tolerancia", f"${TOL:.0f}")

    if faltantes_comp:
        nombres_falt = [EMPRESAS[e] for e in sorted(faltantes_comp)]
        st.error(f"❌ Faltan empresas en staging: **{', '.join(nombres_falt)}**. No se puede comparar.")
        st.stop()

    st.divider()

    if st.button("▶️ Ejecutar comparación", type="primary", key="btn_comparar"):
        with st.spinner("Comparando contra libro_mayor..."):
            try:
                df_comp = run_comparacion(conn, list(empresas_staging), anio_stg, mes_stg)
                st.session_state['resultado_comparacion'] = df_comp
                st.session_state['periodo_comparacion']   = (anio_stg, mes_stg)
            except Exception as e:
                st.error(f"❌ Error en comparación: {e}")

    if 'resultado_comparacion' in st.session_state:
        df_comp  = st.session_state['resultado_comparacion']
        anio_c, mes_c = st.session_state['periodo_comparacion']

        df_difs  = df_comp[df_comp['diferencia'].abs() > TOL]
        df_ok    = df_comp[df_comp['diferencia'].abs() <= TOL]

        # ── Resumen por empresa ───────────────────────────────────────────────
        st.markdown(f"### Resultados — Período {anio_c}/{str(mes_c).zfill(2)}")
        st.divider()

        resumen_cols = st.columns(len(EMPRESAS))
        for i, (eid, enombre) in enumerate(EMPRESAS.items()):
            df_e     = df_comp[df_comp['empresa_id'] == eid]
            df_e_dif = df_e[df_e['diferencia'].abs() > TOL]
            n_ok     = len(df_e) - len(df_e_dif)
            n_dif    = len(df_e_dif[df_e_dif['tipo'] == 'Diferencia'])
            n_csv    = len(df_e_dif[df_e_dif['tipo'] == 'Solo en CSV'])
            n_db     = len(df_e_dif[df_e_dif['tipo'] == 'Solo en DB'])
            estado   = "✅" if len(df_e_dif) == 0 else "⚠️"
            with resumen_cols[i]:
                st.markdown(f"**{estado} {enombre}**")
                st.metric("✅ OK",        n_ok)
                st.metric("⚠️ Difs",      n_dif)
                st.metric("📋 Solo CSV",  n_csv)
                st.metric("📋 Solo DB",   n_db)

        st.divider()

        # ── Detalle de diferencias ────────────────────────────────────────────
        if df_difs.empty:
            st.success("🎉 ¡Sin diferencias! Todos los saldos coinciden entre el sistema contable y la DB.")
        else:
            st.markdown(f"### ⚠️ Diferencias encontradas ({len(df_difs)} cuentas)")

            # Filtros
            fc1, fc2 = st.columns(2)
            filtro_emp  = fc1.multiselect(
                "Filtrar por empresa",
                options=list(EMPRESAS.values()),
                default=list(EMPRESAS.values()),
                key="filtro_emp_comp"
            )
            filtro_tipo = fc2.multiselect(
                "Tipo de diferencia",
                options=['Diferencia','Solo en CSV','Solo en DB'],
                default=['Diferencia','Solo en CSV','Solo en DB'],
                key="filtro_tipo_comp"
            )

            df_show = df_difs[
                df_difs['empresa'].isin(filtro_emp) &
                df_difs['tipo'].isin(filtro_tipo)
            ].copy()

            # Formatear para mostrar
            df_display = df_show[[
                'empresa','cuenta_codigo','descripcion',
                'saldo_csv','saldo_db','diferencia','tipo'
            ]].copy()

            st.dataframe(
                df_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "empresa":        st.column_config.TextColumn("Empresa"),
                    "cuenta_codigo":  st.column_config.NumberColumn("Cta", format="%d"),
                    "descripcion":    st.column_config.TextColumn("Descripción"),
                    "saldo_csv":      st.column_config.NumberColumn("Saldo CSV",  format="$ %.2f"),
                    "saldo_db":       st.column_config.NumberColumn("Saldo DB",   format="$ %.2f"),
                    "diferencia":     st.column_config.NumberColumn("Diferencia", format="$ %.2f"),
                    "tipo":           st.column_config.TextColumn("Tipo"),
                }
            )

            # ── Exportar a Excel ──────────────────────────────────────────────
            import io
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Hoja resumen
                resumen_rows = []
                for eid, enombre in EMPRESAS.items():
                    df_e     = df_comp[df_comp['empresa_id'] == eid]
                    df_e_dif = df_e[df_e['diferencia'].abs() > TOL]
                    resumen_rows.append({
                        'Empresa':     enombre,
                        'Período':     f"{anio_c}/{str(mes_c).zfill(2)}",
                        'Total CSV':   len(df_e[df_e['saldo_csv'].notna()]),
                        'OK':          len(df_e) - len(df_e_dif),
                        'Diferencias': len(df_e_dif[df_e_dif['tipo']=='Diferencia']),
                        'Solo CSV':    len(df_e_dif[df_e_dif['tipo']=='Solo en CSV']),
                        'Solo DB':     len(df_e_dif[df_e_dif['tipo']=='Solo en DB']),
                    })
                pd.DataFrame(resumen_rows).to_excel(writer, sheet_name='Resumen', index=False)

                # Hoja detalle
                df_display.to_excel(writer, sheet_name='Diferencias', index=False)

                # Una hoja por empresa con diferencias
                for enombre in EMPRESAS.values():
                    df_e_dif = df_difs[df_difs['empresa'] == enombre]
                    if not df_e_dif.empty:
                        df_e_dif[['cuenta_codigo','descripcion','saldo_csv','saldo_db','diferencia','tipo']]\
                            .to_excel(writer, sheet_name=enombre, index=False)

            output.seek(0)
            st.download_button(
                label="📥 Exportar diferencias a Excel",
                data=output,
                file_name=f"chequeo_consistencia_{anio_c}{str(mes_c).zfill(2)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="secondary"
            )