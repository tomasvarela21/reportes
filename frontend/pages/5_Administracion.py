"""
frontend/pages/5_Administracion.py
"""
import io

import pandas as pd
import streamlit as st

from services.api_client import (
    EMPRESAS,
    actualizar_cuenta,
    apply_styles,
    crear_centro_costo,
    crear_cuenta,
    eliminar_cuenta,
    get_centros_costo,
    get_cuentas,
    get_empresas,
    get_movimientos_cuenta,
    get_presupuestos,
    get_proyectos,
    get_recalculos,
    get_rubros,
    render_sidebar,
    upload_plan_cuentas,
    upload_presupuestos,
    upload_proyectos,
)

st.set_page_config(page_title="Administración · ReporteApp", page_icon="⚙️", layout="wide")
apply_styles()
render_sidebar()

TIPOS_CUENTA = ["Activo", "Pasivo", "Patrimonio", "Resultado"]

st.title("⚙️ Administración")
st.caption("Gestión de maestros y configuración del sistema.")
st.divider()

tabs = st.tabs([
    "🏢 Empresas",
    "📒 Plan de Cuentas",
    "📥 Actualizar Plan",
    "🏗️ Proyectos",
    "🎯 Centros de Costo",
    "📜 Log Recálculos",
])

# ── Tab 1: Empresas ────────────────────────────────────────────────────────────
with tabs[0]:
    st.subheader("Empresas activas")
    try:
        empresas = get_empresas()
        df_emp = pd.DataFrame(empresas)
        if not df_emp.empty:
            df_emp = df_emp.rename(columns={
                "empresa_id": "ID", "empresa_nombre": "Empresa",
                "grupo": "Grupo", "activa": "Activa",
            })
        st.dataframe(df_emp, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"❌ {e}")


# ── Tab 2: Plan de Cuentas ─────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("Plan de Cuentas")

    # Mensajes de feedback
    for mk in ["msg_cuenta_edit", "msg_cuenta_nueva", "msg_cuenta_eliminada"]:
        if mk in st.session_state:
            st.success(st.session_state.pop(mk))

    try:
        rubros = get_rubros()
    except Exception:
        rubros = []

    c1, c2, c3, c4 = st.columns(4)
    filt_search = c1.text_input("Buscar (nombre o nro)", placeholder="ej: Caja", key="filt_search")
    filt_rub    = c2.text_input("Rubro",  placeholder="ej: DISPONIBILIDADES", key="filt_rub")
    filt_tipo   = c3.selectbox("Tipo", ["Todos"] + TIPOS_CUENTA, key="filt_tipo")
    filt_activa = c4.selectbox("Estado", ["Todos", "Activa", "Inactiva"], key="filt_activa")

    filtros_api = {
        "search": filt_search.strip() or None,
        "rubro":  filt_rub.strip() or None,
        "tipo":   None if filt_tipo == "Todos" else filt_tipo,
        "activa": None if filt_activa == "Todos" else (filt_activa == "Activa"),
        "limit":  2000,
        "offset": 0,
    }

    try:
        cuentas = get_cuentas(filtros_api)
        df_cta  = pd.DataFrame(cuentas)
    except Exception as e:
        st.error(f"❌ {e}")
        df_cta = pd.DataFrame()

    if not df_cta.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Mostradas",     len(df_cta))
        c2.metric("Total cuentas", len(df_cta))
        activas = df_cta["activa"].sum() if "activa" in df_cta.columns else 0
        c3.metric("Activas",  int(activas))
        c4.metric("Rubros distintos", df_cta["rubro"].nunique() if "rubro" in df_cta.columns else 0)
        st.divider()
        st.dataframe(df_cta, use_container_width=True, hide_index=True)
    else:
        st.info("Sin cuentas con esos filtros.")

    # ── Editar cuenta ──────────────────────────────────────────────────────────
    if not df_cta.empty:
        st.divider()
        st.markdown("#### ✏️ Editar cuenta existente")
        opciones_editar = ["— Seleccioná una cuenta —"] + [
            f"{r['nro_cta']} — {r.get('nombre', '')}" for r in cuentas
        ]
        sel_editar = st.selectbox("Cuenta a editar", opciones_editar, key="sel_editar")

        if sel_editar != "— Seleccioná una cuenta —":
            nro_edit = int(sel_editar.split(" — ")[0])
            cuenta   = next((c for c in cuentas if c["nro_cta"] == nro_edit), None)
            if cuenta:
                st.markdown(f"### ✏️ Editando cuenta **{nro_edit}** — {cuenta.get('nombre')}")
                cx1, cx2 = st.columns([2, 1])
                nombre_edit    = cx1.text_input("Nombre *", value=cuenta.get("nombre") or "", key=f"en_{nro_edit}")
                extendido_edit = cx2.text_input("Extendido", value=cuenta.get("extendido") or "", key=f"ee_{nro_edit}")
                cx1, cx2, cx3, cx4 = st.columns(4)
                rubro_edit    = cx1.text_input("Rubro",     value=cuenta.get("rubro")     or "", key=f"er_{nro_edit}")
                subrub_edit   = cx2.text_input("Sub-rubro", value=cuenta.get("sub_rubro") or "", key=f"esr_{nro_edit}")
                tipo_idx      = TIPOS_CUENTA.index(cuenta["tipo"]) if cuenta.get("tipo") in TIPOS_CUENTA else 0
                tipo_edit     = cx3.selectbox("Tipo *", TIPOS_CUENTA, index=tipo_idx, key=f"et_{nro_edit}")
                moneda_opts   = ["ARS", "USD", "EUR"]
                mon_idx       = moneda_opts.index(cuenta["moneda"]) if cuenta.get("moneda") in moneda_opts else 0
                moneda_edit   = cx4.selectbox("Moneda", moneda_opts, index=mon_idx, key=f"em_{nro_edit}")

                if st.button("💾 Guardar cambios", type="primary", key=f"btn_edit_{nro_edit}"):
                    if not nombre_edit.strip():
                        st.error("❌ El nombre es obligatorio.")
                    else:
                        try:
                            actualizar_cuenta(nro_edit, {
                                "nombre":    nombre_edit.strip(),
                                "extendido": extendido_edit.strip() or None,
                                "rubro":     rubro_edit.strip() or None,
                                "sub_rubro": subrub_edit.strip() or None,
                                "tipo":      tipo_edit,
                                "moneda":    moneda_edit,
                            })
                            st.session_state["msg_cuenta_edit"] = f"✅ Cuenta **{nro_edit}** actualizada."
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")

    # ── Alta cuenta nueva ──────────────────────────────────────────────────────
    st.divider()
    with st.expander("➕ Agregar nueva cuenta"):
        cc1, cc2, cc3 = st.columns(3)
        nro_new  = cc1.number_input("Nro Cta *", min_value=1, step=1, key="new_nro")
        ext_new  = cc2.text_input("Extendido", placeholder="ej: 1.05.01.001", key="new_ext")
        nom_new  = cc3.text_input("Nombre *", placeholder="ej: Maquinaria", key="new_nom")
        cc1, cc2, cc3, cc4, cc5 = st.columns(5)
        rub_new  = cc1.text_input("Rubro *",    key="new_rub")
        sub_new  = cc2.text_input("Sub-rubro",  key="new_sub")
        tip_new  = cc3.selectbox("Tipo *",      TIPOS_CUENTA, key="new_tip")
        mon_new  = cc4.selectbox("Moneda",      ["ARS", "USD", "EUR"], key="new_mon")
        act_new  = cc5.checkbox("Activa",       value=True, key="new_act")
        if st.button("💾 Guardar cuenta", type="primary", key="btn_nueva_cta"):
            if not nom_new.strip():
                st.error("❌ El nombre es obligatorio.")
            elif not rub_new.strip():
                st.error("❌ El rubro es obligatorio.")
            else:
                try:
                    crear_cuenta({
                        "nro_cta":   int(nro_new),
                        "extendido": ext_new.strip() or None,
                        "nombre":    nom_new.strip(),
                        "rubro":     rub_new.strip(),
                        "sub_rubro": sub_new.strip() or None,
                        "tipo":      tip_new,
                        "moneda":    mon_new,
                        "activa":    act_new,
                    })
                    st.session_state["msg_cuenta_nueva"] = f"✅ Cuenta **{int(nro_new)} — {nom_new.strip()}** agregada."
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

    # ── Eliminar cuenta ────────────────────────────────────────────────────────
    st.divider()
    with st.expander("🗑️ Eliminar cuenta"):
        st.caption("Solo se pueden eliminar cuentas sin movimientos en el Libro Diario.")
        if not df_cta.empty:
            opciones_del = ["— Seleccioná una cuenta —"] + [
                f"{r['nro_cta']} — {r.get('nombre', '')}" for r in cuentas
            ]
            sel_del = st.selectbox("Cuenta a eliminar", opciones_del, key="sel_del")
            if sel_del != "— Seleccioná una cuenta —":
                nro_del  = int(sel_del.split(" — ")[0])
                cuenta_d = next((c for c in cuentas if c["nro_cta"] == nro_del), {})
                st.markdown(f"""
| Campo | Valor |
|---|---|
| **Nro Cta** | {nro_del} |
| **Nombre** | {cuenta_d.get('nombre', '—')} |
| **Rubro** | {cuenta_d.get('rubro', '—')} |
| **Tipo** | {cuenta_d.get('tipo', '—')} |
""")
                try:
                    mov_resp = get_movimientos_cuenta(nro_del)
                    n_mov    = mov_resp.get("movimientos", 0)
                except Exception:
                    n_mov = -1

                if n_mov > 0:
                    st.error(f"❌ La cuenta **{nro_del}** tiene **{n_mov} movimiento(s)** y no puede eliminarse.")
                elif n_mov == -1:
                    st.warning("⚠️ No se pudo verificar movimientos.")
                else:
                    st.warning(f"⚠️ Esta acción es **irreversible**.")
                    if st.checkbox(f"Confirmo eliminar la cuenta **{nro_del}**", key=f"confirm_del_{nro_del}"):
                        if st.button("🗑️ Eliminar", type="primary", key=f"btn_del_{nro_del}"):
                            try:
                                eliminar_cuenta(nro_del)
                                st.session_state["msg_cuenta_eliminada"] = f"✅ Cuenta **{nro_del}** eliminada."
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ {e}")


# ── Tab 3: Actualizar Plan ─────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("📥 Actualizar Plan de Cuentas")
    st.caption("Cargá un Excel o CSV para agregar y actualizar cuentas. Las cuentas existentes no se eliminan.")

    if "plan_cargado" in st.session_state:
        r = st.session_state["plan_cargado"]
        st.success(
            f"✅ Plan actualizado desde **{r['archivo']}** — "
            f"{r['nuevas']} nuevas, {r['actualizadas']} actualizadas, "
            f"{r['renombradas']} renombradas."
        )
        if r.get("cols_agregadas"):
            st.info(f"🆕 Columnas nuevas agregadas: {', '.join(r['cols_agregadas'])}")
        c1, c2, c3 = st.columns(3)
        c1.metric("Nuevas",       r["nuevas"])
        c2.metric("Actualizadas", r["actualizadas"])
        c3.metric("Renombradas",  r["renombradas"])
        st.divider()
        if st.button("📥 Cargar otro archivo", type="primary", key="btn_otro_plan"):
            st.session_state.pop("plan_cargado", None)
            st.rerun()
        st.stop()

    st.markdown("**Formatos aceptados:** Excel (.xlsx) o CSV separado por `;`")
    st.code("nro_cta | Nombre | Rubro | SubRubro | Tipo | Moneda | Activa | ...")

    archivo_plan = st.file_uploader(
        "Archivo del plan de cuentas",
        type=["xlsx", "xls", "csv"],
        key="plan_uploader",
    )

    if archivo_plan:
        if st.button("📥 Aplicar actualización del plan", type="primary", key="btn_aplicar_plan"):
            file_bytes = archivo_plan.read()
            with st.spinner("Actualizando plan de cuentas..."):
                try:
                    resultado = upload_plan_cuentas(file_bytes, archivo_plan.name)
                    st.session_state["plan_cargado"] = resultado
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")


# ── Tab 4: Proyectos ───────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("🏗️ Proyectos")
    st.caption("Gestión de proyectos y presupuestos.")

    if "proyectos_cargados" in st.session_state:
        r = st.session_state["proyectos_cargados"]
        st.success(f"✅ Proyectos actualizados desde **{r['archivo']}**")
        c1, c2 = st.columns(2)
        c1.metric("Registros procesados", r.get("registros", 0))
        c2.metric("Advertencias",         len(r.get("advertencias", [])))
        for a in r.get("advertencias", []):
            st.warning(f"⚠️ {a}")
        st.divider()
        if st.button("📥 Cargar otro archivo", type="primary", key="btn_otro_proy"):
            st.session_state.pop("proyectos_cargados", None)
            st.rerun()
        st.stop()

    if "presupuestos_cargados" in st.session_state:
        r = st.session_state["presupuestos_cargados"]
        st.success(f"✅ Presupuestos actualizados desde **{r['archivo']}**")
        st.divider()
        if st.button("📥 Cargar otro archivo", type="primary", key="btn_otro_presp"):
            st.session_state.pop("presupuestos_cargados", None)
            st.rerun()
        st.stop()

    subtab_lista, subtab_proyectos, subtab_presupuestos = st.tabs([
        "📋 Listado", "📥 Actualizar Proyectos", "📊 Cargar Presupuestos",
    ])

    with subtab_lista:
        try:
            proyectos = get_proyectos(incluir_inactivos=False)
            df_proy   = pd.DataFrame(proyectos)
        except Exception as e:
            st.error(f"❌ {e}")
            df_proy = pd.DataFrame()

        if not df_proy.empty:
            c1, c2 = st.columns(2)
            c1.metric("Total proyectos", len(df_proy))
            if "activo" in df_proy.columns:
                c2.metric("Activos", int(df_proy["activo"].sum()))
            mostrar_inact = st.checkbox("Mostrar inactivos", value=False, key="chk_inact")
            if mostrar_inact:
                df_proy_todo = get_proyectos(incluir_inactivos=True)
                st.dataframe(pd.DataFrame(df_proy_todo), use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_proy, use_container_width=True, hide_index=True)

            # Presupuestos
            st.divider()
            st.markdown("#### 📊 Presupuestos cargados")
            try:
                presp = get_presupuestos()
                if presp:
                    df_pp = pd.DataFrame(presp)
                    st.metric("Registros", len(df_pp))
                    st.dataframe(df_pp, use_container_width=True, hide_index=True)
                else:
                    st.info("No hay presupuestos cargados aún.")
            except Exception as e:
                st.error(f"❌ {e}")
        else:
            st.info("No hay proyectos cargados aún.")

    with subtab_proyectos:
        st.caption("Subí el CSV de proyectos.")
        archivo_proy = st.file_uploader("CSV/Excel de proyectos", type=["csv", "xlsx"], key="proy_up")
        if archivo_proy:
            if st.button("🏗️ Aplicar actualización", type="primary", key="btn_ap_proy"):
                file_bytes = archivo_proy.read()
                with st.spinner("Actualizando proyectos..."):
                    try:
                        r = upload_proyectos(file_bytes, archivo_proy.name)
                        st.session_state["proyectos_cargados"] = {"archivo": archivo_proy.name, **r}
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")

    with subtab_presupuestos:
        st.caption("Subí el CSV de presupuestos.")
        archivo_presp = st.file_uploader("CSV/Excel de presupuestos", type=["csv", "xlsx"], key="presp_up")
        if archivo_presp:
            if st.button("📊 Aplicar carga", type="primary", key="btn_ap_presp"):
                file_bytes = archivo_presp.read()
                with st.spinner("Cargando presupuestos..."):
                    try:
                        r = upload_presupuestos(file_bytes, archivo_presp.name)
                        st.session_state["presupuestos_cargados"] = {"archivo": archivo_presp.name, **r}
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {e}")


# ── Tab 5: Centros de Costo ────────────────────────────────────────────────────
with tabs[4]:
    st.subheader("Centros de Costo")

    if "msg_centro_nuevo" in st.session_state:
        st.success(st.session_state.pop("msg_centro_nuevo"))

    try:
        centros   = get_centros_costo()
        df_cc     = pd.DataFrame(centros)
    except Exception as e:
        st.error(f"❌ {e}")
        df_cc = pd.DataFrame()

    if not df_cc.empty:
        st.metric("Total centros", len(df_cc))
        st.dataframe(df_cc, use_container_width=True, hide_index=True)
    else:
        st.info("No hay centros de costo registrados.")

    try:
        empresas_cc  = get_empresas()
        empresas_cc_lista = [e["empresa_nombre"] for e in empresas_cc]
    except Exception:
        empresas_cc_lista = list(EMPRESAS.keys())

    with st.expander("➕ Agregar centro de costo"):
        ca1, ca2, ca3 = st.columns(3)
        cod_new  = ca1.text_input("Código *",     key="new_cc_cod")
        desc_new = ca2.text_input("Descripción *", key="new_cc_desc")
        emp_new  = ca3.selectbox("Empresa (opcional)", ["—"] + empresas_cc_lista, key="new_cc_emp")

        if st.button("Guardar centro", key="btn_nuevo_cc"):
            if not cod_new.strip():
                st.error("❌ El código es obligatorio.")
            elif not desc_new.strip():
                st.error("❌ La descripción es obligatoria.")
            else:
                try:
                    crear_centro_costo({
                        "codigo":      cod_new.strip(),
                        "descripcion": desc_new.strip(),
                        "empresa_id":  EMPRESAS.get(emp_new) if emp_new != "—" else None,
                    })
                    st.session_state["msg_centro_nuevo"] = f"✅ Centro **{cod_new.strip()}** agregado."
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")


# ── Tab 6: Log Recálculos ──────────────────────────────────────────────────────
with tabs[5]:
    st.subheader("Log de recálculos del Mayor")
    try:
        log = get_recalculos()
        if not log:
            st.info("No hay recálculos registrados.")
        else:
            df_log = pd.DataFrame(log)
            st.metric("Entradas", len(df_log))
            st.dataframe(df_log, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"❌ {e}")
