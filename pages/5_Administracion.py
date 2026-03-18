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
        SELECT nro_cta, extendido, nombre, rubro, sub_rubro, tipo, moneda, activa, es_resultado,
               nivel_1, nivel_2, nivel_3
        FROM dim_cuenta ORDER BY nro_cta
    """)
    cols = ['Nro Cta','Extendido','Nombre','Rubro','Sub-rubro','Tipo','Moneda','Activa',
            'Es Resultado','Nivel 1','Nivel 2','Nivel 3']
    df = pd.DataFrame(cur.fetchall(), columns=cols); cur.close(); return df

def get_rubros(conn):
    """Devuelve lista de rubros distintos existentes en dim_cuenta."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT rubro FROM dim_cuenta WHERE rubro IS NOT NULL AND rubro != '' ORDER BY rubro")
    rubros = [r[0] for r in cur.fetchall()]; cur.close(); return rubros

def get_subrubros_por_rubro(conn, rubro):
    """Devuelve sub-rubros distintos para un rubro dado."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT sub_rubro FROM dim_cuenta
        WHERE rubro = %s AND sub_rubro IS NOT NULL AND sub_rubro != ''
        ORDER BY sub_rubro
    """, (rubro,))
    subs = [r[0] for r in cur.fetchall()]; cur.close(); return subs

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

def validar_cuenta_nueva(conn, nro_cta, nombre):
    """Retorna lista de errores de validación para una cuenta nueva."""
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
    """Retorna True si el rubro ya existe."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM dim_cuenta WHERE LOWER(rubro) = LOWER(%s)", (rubro_nombre,))
    existe = cur.fetchone() is not None; cur.close(); return existe

def validar_subrubro_nuevo(conn, rubro, subrubro_nombre):
    """Retorna True si el sub-rubro ya existe en ese rubro."""
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM dim_cuenta
        WHERE LOWER(rubro) = LOWER(%s) AND LOWER(sub_rubro) = LOWER(%s)
    """, (rubro, subrubro_nombre))
    existe = cur.fetchone() is not None; cur.close(); return existe

# ── UI ─────────────────────────────────────────────────────────────────────────

st.title("⚙️ Administración")
st.caption("Gestión de maestros y configuración del sistema.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

tabs = st.tabs(["🏢 Empresas", "📒 Plan de Cuentas", "🎯 Centros de Costo", "📜 Log Recálculos"])

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
    except Exception:
        conn = get_conn()
        df_cta = get_plan_cuentas(conn)
        rubros = get_rubros(conn)

    # ── Filtros ────────────────────────────────────────────────────────────────
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

    # ── Métricas ───────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total cuentas",    len(df_cta))
    c2.metric("Mostradas",        len(df_show))
    c3.metric("Activas",          int(df_cta['Activa'].sum()))
    c4.metric("Rubros distintos", df_cta['Rubro'].nunique())
    st.divider()

    # ── Tabla ─────────────────────────────────────────────────────────────────
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    # ── Selector para editar ───────────────────────────────────────────────────
    st.divider()
    st.markdown("#### ✏️ Editar cuenta existente")
    opciones_editar = ["— Seleccioná una cuenta —"] + [
        f"{int(r['Nro Cta'])} — {r['Nombre']}" for _, r in df_show.iterrows()
    ]
    sel_editar = st.selectbox("Seleccioná la cuenta a editar", opciones_editar, key="sel_editar")

    if sel_editar != "— Seleccioná una cuenta —":
        nro_edit = int(sel_editar.split(" — ")[0])
        cuenta = df_show[df_show['Nro Cta'] == nro_edit].iloc[0]
        if True:

            st.divider()
            st.markdown(f"### ✏️ Editando cuenta **{nro_edit}** — {cuenta['Nombre']}")

            rubro_actual    = cuenta['Rubro'] or ""
            subrubro_actual = cuenta['Sub-rubro'] or ""

            subrubros_actuales = get_subrubros_por_rubro(conn, rubro_actual) if rubro_actual else []

            c1, c2, c3, c4 = st.columns(4)

            # Selector de rubro
            opciones_rubro = rubros + ["✨ + Nuevo rubro..."]
            idx_rubro = rubros.index(rubro_actual) if rubro_actual in rubros else 0
            rubro_sel_edit = c1.selectbox(
                "Rubro *", opciones_rubro,
                index=idx_rubro,
                key=f"edit_rubro_{nro_edit}"
            )

            es_rubro_nuevo_edit = rubro_sel_edit == "✨ + Nuevo rubro..."
            rubro_final_edit = rubro_actual  # default

            if es_rubro_nuevo_edit:
                with st.container():
                    st.markdown("**✨ Nuevo rubro**")
                    st.info("El rubro se creará al guardar los cambios de la cuenta.")
                    cn1, cn2 = st.columns([3, 1])
                    nuevo_rubro_nombre_edit = cn1.text_input(
                        "Nombre del rubro *", placeholder="ej: 05 - BIENES DE USO",
                        key=f"edit_nuevo_rubro_nom_{nro_edit}"
                    )
                    nuevo_rubro_tipo_edit = cn2.selectbox(
                        "Tipo", ["Activo","Pasivo","Patrimonio","Resultado"],
                        key=f"edit_nuevo_rubro_tipo_{nro_edit}"
                    )
                    if nuevo_rubro_nombre_edit and validar_rubro_nuevo(conn, nuevo_rubro_nombre_edit):
                        st.error(f"❌ El rubro **{nuevo_rubro_nombre_edit}** ya existe en el plan.")
                    rubro_final_edit = nuevo_rubro_nombre_edit
                subrubros_edit = []
            else:
                rubro_final_edit = rubro_sel_edit
                subrubros_edit = get_subrubros_por_rubro(conn, rubro_sel_edit)

            # Selector de sub-rubro (solo si hay rubro)
            subrubro_final_edit = ""
            if rubro_final_edit and not es_rubro_nuevo_edit:
                opciones_sub_edit = ["— Sin sub-rubro —"] + subrubros_edit + ["✨ + Nuevo sub-rubro..."]
                idx_sub = (subrubros_edit.index(subrubro_actual) + 1) if subrubro_actual in subrubros_edit else 0
                subrubro_sel_edit = c2.selectbox(
                    "Sub-rubro", opciones_sub_edit,
                    index=idx_sub,
                    key=f"edit_subrubro_{nro_edit}"
                )

                if subrubro_sel_edit == "✨ + Nuevo sub-rubro...":
                    with st.container():
                        st.markdown("**✨ Nuevo sub-rubro**")
                        st.info(f"Pertenecerá al rubro **{rubro_final_edit}**. Se crea al guardar.")
                        nuevo_sub_nombre_edit = st.text_input(
                            "Nombre del sub-rubro *", placeholder="ej: 06-Leasing",
                            key=f"edit_nuevo_sub_nom_{nro_edit}"
                        )
                        if nuevo_sub_nombre_edit and validar_subrubro_nuevo(conn, rubro_final_edit, nuevo_sub_nombre_edit):
                            st.error(f"❌ El sub-rubro **{nuevo_sub_nombre_edit}** ya existe en **{rubro_final_edit}**.")
                        subrubro_final_edit = nuevo_sub_nombre_edit
                elif subrubro_sel_edit == "— Sin sub-rubro —":
                    subrubro_final_edit = ""
                else:
                    subrubro_final_edit = subrubro_sel_edit
            elif es_rubro_nuevo_edit:
                with st.container():
                    nuevo_sub_nombre_edit2 = c2.text_input(
                        "Sub-rubro (opcional)", placeholder="ej: 01-General",
                        key=f"edit_nuevo_sub_nom2_{nro_edit}"
                    )
                    subrubro_final_edit = nuevo_sub_nombre_edit2

            tipo_edit   = c3.selectbox("Tipo", ["Activo","Pasivo","Patrimonio","Resultado"],
                                       index=["Activo","Pasivo","Patrimonio","Resultado"].index(cuenta['Tipo']) if cuenta['Tipo'] in ["Activo","Pasivo","Patrimonio","Resultado"] else 0,
                                       key=f"edit_tipo_{nro_edit}")
            moneda_edit = c4.selectbox("Moneda", ["ARS","USD","EUR"],
                                       index=["ARS","USD","EUR"].index(cuenta['Moneda']) if cuenta['Moneda'] in ["ARS","USD","EUR"] else 0,
                                       key=f"edit_moneda_{nro_edit}")

            cb1, cb2 = st.columns([1, 5])
            if cb1.button("💾 Guardar cambios", type="primary", key=f"btn_edit_{nro_edit}"):
                errores_edit = []
                if not rubro_final_edit:
                    errores_edit.append("El rubro es obligatorio.")
                if es_rubro_nuevo_edit and validar_rubro_nuevo(conn, rubro_final_edit):
                    errores_edit.append(f"El rubro **{rubro_final_edit}** ya existe.")
                if subrubro_final_edit and validar_subrubro_nuevo(conn, rubro_final_edit, subrubro_final_edit) and (
                    subrubro_final_edit not in subrubros_edit
                ):
                    errores_edit.append(f"El sub-rubro **{subrubro_final_edit}** ya existe en **{rubro_final_edit}**.")

                if errores_edit:
                    for e in errores_edit: st.error(f"❌ {e}")
                else:
                    try:
                        cur = conn.cursor()
                        cur.execute("""
                            UPDATE dim_cuenta
                            SET rubro = %s, sub_rubro = %s, tipo = %s, moneda = %s
                            WHERE nro_cta = %s
                        """, (
                            rubro_final_edit or None,
                            subrubro_final_edit or None,
                            tipo_edit,
                            moneda_edit,
                            nro_edit
                        ))
                        conn.commit(); cur.close()
                        st.success(f"✅ Cuenta **{nro_edit}** actualizada correctamente.")
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

    st.divider()

    # ── Alta de cuenta nueva ───────────────────────────────────────────────────
    with st.expander("➕ Agregar nueva cuenta"):

            c1, c2, c3 = st.columns(3)
            nro_cta_new    = c1.number_input("Nro Cta *", min_value=1, step=1, key="new_nro_cta")
            extendido_new  = c2.text_input("Extendido",   placeholder="ej: 1.05.01.001", key="new_extendido")
            nombre_new     = c3.text_input("Nombre *",    placeholder="ej: Maquinaria y Equipo", key="new_nombre")

            c1, c2, c3, c4 = st.columns(4)

            # Selector de rubro
            opciones_rubro_new = rubros + ["✨ + Nuevo rubro..."]
            rubro_sel_new = c1.selectbox("Rubro *", opciones_rubro_new, index=0, key="new_rubro_sel")
            tipo_new   = c3.selectbox("Tipo *",   ["Activo","Pasivo","Patrimonio","Resultado"], key="new_tipo")
            moneda_new = c4.selectbox("Moneda",   ["ARS","USD","EUR"], key="new_moneda")

            es_rubro_nuevo_new = rubro_sel_new == "✨ + Nuevo rubro..."
            rubro_final_new = ""
            nuevo_rubro_nombre_new = ""

            if es_rubro_nuevo_new:
                st.markdown("**✨ Nuevo rubro**")
                st.info("El rubro se creará junto con la cuenta.")
                cn1, cn2 = st.columns([3, 1])
                nuevo_rubro_nombre_new = cn1.text_input(
                    "Nombre del rubro *", placeholder="ej: 05 - BIENES DE USO", key="new_nuevo_rubro_nom"
                )
                nuevo_rubro_tipo_new = cn2.selectbox(
                    "Tipo del rubro", ["Activo","Pasivo","Patrimonio","Resultado"], key="new_nuevo_rubro_tipo"
                )
                if nuevo_rubro_nombre_new and validar_rubro_nuevo(conn, nuevo_rubro_nombre_new):
                    st.error(f"❌ El rubro **{nuevo_rubro_nombre_new}** ya existe en el plan.")
                rubro_final_new = nuevo_rubro_nombre_new
                subrubros_new = []
            else:
                rubro_final_new = rubro_sel_new
                subrubros_new = get_subrubros_por_rubro(conn, rubro_sel_new) if rubro_sel_new else []

            # Selector de sub-rubro
            subrubro_final_new = ""
            if rubro_final_new and not es_rubro_nuevo_new:
                opciones_sub_new = ["— Sin sub-rubro —"] + subrubros_new + ["✨ + Nuevo sub-rubro..."]
                subrubro_sel_new = c2.selectbox("Sub-rubro", opciones_sub_new, key="new_subrubro_sel")

                if subrubro_sel_new == "✨ + Nuevo sub-rubro...":
                    st.markdown("**✨ Nuevo sub-rubro**")
                    st.info(f"Pertenecerá al rubro **{rubro_final_new}**. Se crea junto con la cuenta.")
                    nuevo_sub_nombre_new = st.text_input(
                        "Nombre del sub-rubro *", placeholder="ej: 06-Leasing", key="new_nuevo_sub_nom"
                    )
                    if nuevo_sub_nombre_new and validar_subrubro_nuevo(conn, rubro_final_new, nuevo_sub_nombre_new):
                        st.error(f"❌ El sub-rubro **{nuevo_sub_nombre_new}** ya existe en **{rubro_final_new}**.")
                    subrubro_final_new = nuevo_sub_nombre_new
                elif subrubro_sel_new == "— Sin sub-rubro —":
                    subrubro_final_new = ""
                else:
                    subrubro_final_new = subrubro_sel_new
            elif es_rubro_nuevo_new:
                subrubro_final_new = c2.text_input(
                    "Sub-rubro (opcional)", placeholder="ej: 01-General", key="new_subrubro_libre"
                )

            if st.button("💾 Guardar cuenta", key="btn_nueva_cta", type="primary"):
                errores_new = []
                if not nombre_new.strip():
                    errores_new.append("El nombre es obligatorio.")
                if not rubro_final_new.strip():
                    errores_new.append("El rubro es obligatorio.")

                # Validar duplicados
                errores_new += validar_cuenta_nueva(conn, nro_cta_new, nombre_new.strip())

                if es_rubro_nuevo_new and validar_rubro_nuevo(conn, rubro_final_new):
                    errores_new.append(f"El rubro **{rubro_final_new}** ya existe en el plan.")

                if subrubro_final_new and es_rubro_nuevo_new is False and validar_subrubro_nuevo(conn, rubro_final_new, subrubro_final_new):
                    if subrubro_final_new not in subrubros_new:
                        errores_new.append(f"El sub-rubro **{subrubro_final_new}** ya existe en **{rubro_final_new}**.")

                if errores_new:
                    for e in errores_new: st.error(f"❌ {e}")
                else:
                    try:
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO dim_cuenta (nro_cta, extendido, nombre, rubro, sub_rubro, tipo, moneda)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            nro_cta_new,
                            extendido_new.strip() or None,
                            nombre_new.strip(),
                            rubro_final_new.strip() or None,
                            subrubro_final_new.strip() or None,
                            tipo_new,
                            moneda_new,
                        ))
                        conn.commit(); cur.close()
                        st.success(f"✅ Cuenta **{nro_cta_new} — {nombre_new}** agregada al plan.")
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")

# ── Tab 3: Centros de Costo ────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("Centros de Costo")
    try:
            df_cc = get_centros(conn)
    except Exception:
            conn = get_conn(); df_cc = get_centros(conn)
    st.metric("Total centros", len(df_cc))
    st.dataframe(df_cc, use_container_width=True, hide_index=True)

    with st.expander("➕ Agregar centro de costo"):
            ca1, ca2, ca3 = st.columns(3)
            cod_new  = ca1.text_input("Código",       key="new_cc_cod")
            desc_new = ca2.text_input("Descripción",  key="new_cc_desc")
            emp_new  = ca3.selectbox("Empresa (opcional)", ["—"] + list(EMPRESAS.keys()), key="new_cc_emp")
            if st.button("Guardar centro", key="btn_nuevo_cc"):
                # Validar duplicado
                cur = conn.cursor()
                cur.execute("SELECT 1 FROM dim_centro_costo WHERE codigo = %s", (cod_new.strip(),))
                if cur.fetchone():
                    st.error(f"❌ El código **{cod_new}** ya existe en centros de costo.")
                else:
                    try:
                        emp_id_new = EMPRESAS[emp_new] if emp_new != "—" else None
                        cur.execute("""
                            INSERT INTO dim_centro_costo (codigo, descripcion, empresa_id)
                            VALUES (%s, %s, %s)
                        """, (cod_new.strip(), desc_new.strip(), emp_id_new))
                        conn.commit()
                        st.success(f"✅ Centro **{cod_new}** agregado.")
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"Error: {e}")
                cur.close()

# ── Tab 4: Log Recálculos ──────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("Log de recálculos del Mayor")
    try:
            df_log = get_log(conn)
    except Exception:
            conn = get_conn(); df_log = get_log(conn)
    if df_log.empty:
            st.info("No hay recálculos registrados.")
    else:
            st.dataframe(df_log, use_container_width=True, hide_index=True)