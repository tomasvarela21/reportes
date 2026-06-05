"""
frontend/pages/6_Chequeo_Consistencia.py
"""
import io

import pandas as pd
import streamlit as st

from services.api_client import (
    EMPRESAS,
    apply_styles,
    comparar_consistencia,
    get_consistencia_estado,
    limpiar_consistencia_staging,
    render_sidebar,
    upload_consistencia,
)

st.set_page_config(page_title="Chequeo Consistencia · ReporteApp", page_icon="🔍", layout="wide")
apply_styles()
render_sidebar()

EMPRESAS_INV = {v: k for k, v in EMPRESAS.items()}   # nombre → id
TODAS        = set(EMPRESAS.values())                  # {1,2,3,4,5}
TOL          = 1.0

st.title("🔍 Chequeo de Consistencia")
st.caption("Comparación entre mayores del sistema contable y el Libro Mayor de la DB.")
st.divider()

tabs = st.tabs(["📁 Carga de Archivos", "📊 Comparación"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CARGA
# ══════════════════════════════════════════════════════════════════════════════
with tabs[0]:

    # ── Estado actual del staging ─────────────────────────────────────────────
    st.subheader("Estado actual del staging")

    try:
        estado = get_consistencia_estado()
    except Exception as e:
        st.error(f"❌ No se pudo obtener el estado: {e}")
        estado = {"cargado": False, "empresas": [], "periodo": None, "listo_para_comparar": False}

    empresas_en_staging: set = set()

    if not estado.get("cargado"):
        st.info("La tabla staging está vacía. Subí los archivos para comenzar.")
    else:
        empresas_staging = estado.get("empresas", [])
        empresas_en_staging = {e["empresa_id"] for e in empresas_staging}
        periodo_str = estado.get("periodo", "—")

        df_estado_display = pd.DataFrame([{
            "Empresa":      EMPRESAS.get(e["empresa_id"], e["empresa_id"]),
            "Período":      f"{e['periodo_anio']}/{str(e['periodo_mes']).zfill(2)}",
            "Cuentas":      e.get("total_cuentas", 0),
            "Archivo":      e.get("archivo", "—"),
            "Última carga": e.get("ultima_carga", "—"),
        } for e in empresas_staging])

        c1, c2 = st.columns([3, 1])
        with c1:
            st.dataframe(df_estado_display, use_container_width=True, hide_index=True)
        with c2:
            st.markdown("**Empresas cargadas:**")
            for nombre, eid in EMPRESAS_INV.items():
                if eid in empresas_en_staging:
                    st.success(f"✅ {nombre}")
                else:
                    st.warning(f"⚠️ {nombre}")

        if st.button("🗑️ Limpiar staging", type="secondary", key="btn_limpiar"):
            try:
                limpiar_consistencia_staging()
                st.success("✅ Staging vaciada correctamente.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")

    st.divider()

    # ── Upload de archivos ────────────────────────────────────────────────────
    st.subheader("Subir archivos del sistema contable")
    st.caption("Subí los CSVs de las 5 empresas. El sistema detecta empresa y período del nombre del archivo.")

    if "carga_staging_ok" in st.session_state:
        st.success(st.session_state.pop("carga_staging_ok"))

    archivos = st.file_uploader(
        "Seleccioná los archivos (podés subir varios a la vez)",
        type=["csv", "CSV"],
        accept_multiple_files=True,
        key="uploader_cons",
    )

    if archivos:
        st.markdown("#### Archivos seleccionados")
        for arch in archivos:
            st.caption(f"📄 {arch.name} ({arch.size:,} bytes)")

        if st.button("📥 Cargar en staging (reemplaza todo)", type="primary", key="btn_cargar_stg"):
            files_list = []
            for arch in archivos:
                arch.seek(0)
                files_list.append((arch.read(), arch.name))

            with st.spinner("Cargando en base de datos..."):
                try:
                    resultado = upload_consistencia(files_list)
                    periodo   = resultado.get("periodo", "—")
                    total_cta = resultado.get("total_cuentas", 0)
                    adv       = resultado.get("advertencias", [])
                    st.session_state["carga_staging_ok"] = (
                        f"✅ Staging actualizada — "
                        f"5 empresas, período {periodo}, {total_cta:,} cuentas cargadas."
                    )
                    for a in adv:
                        st.session_state.setdefault("_cons_adv", []).append(a)
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")

    for adv in st.session_state.pop("_cons_adv", []):
        st.info(f"ℹ️ {adv}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — COMPARACIÓN
# ══════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.subheader("Comparación CSV vs Libro Mayor DB")

    # Verificar estado
    try:
        estado_comp = get_consistencia_estado()
    except Exception as e:
        st.error(f"❌ {e}")
        st.stop()

    if not estado_comp.get("cargado"):
        st.warning("⚠️ El staging está vacío. Primero cargá los archivos en la pestaña anterior.")
        st.stop()

    empresas_stg    = {e["empresa_id"] for e in estado_comp.get("empresas", [])}
    faltantes_comp  = TODAS - empresas_stg
    periodo_comp    = estado_comp.get("periodo", "—")
    total_cta_stg   = sum(e.get("total_cuentas", 0) for e in estado_comp.get("empresas", []))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Empresas cargadas", f"{len(empresas_stg)}/5")
    c2.metric("Período",           periodo_comp)
    c3.metric("Total cuentas CSV", f"{total_cta_stg:,}")
    c4.metric("Tolerancia",        f"${TOL:.0f}")

    if faltantes_comp:
        nombres_falt = [EMPRESAS.get(e, str(e)) for e in sorted(faltantes_comp)]
        st.error(f"❌ Faltan empresas: **{', '.join(nombres_falt)}**. No se puede comparar.")
        st.stop()

    if not estado_comp.get("listo_para_comparar"):
        st.warning("⚠️ El staging no está listo para comparar.")
        st.stop()

    st.divider()

    if st.button("▶️ Ejecutar comparación", type="primary", key="btn_comparar"):
        with st.spinner("Comparando contra libro_mayor..."):
            try:
                resultado_comp = comparar_consistencia()
                st.session_state["resultado_comparacion"] = resultado_comp
            except Exception as e:
                st.error(f"❌ {e}")

    if "resultado_comparacion" not in st.session_state:
        st.stop()

    res          = st.session_state["resultado_comparacion"]
    diferencias  = res.get("diferencias", [])
    resumen      = res.get("resumen", [])
    periodo_res  = res.get("periodo", periodo_comp)

    # ── Resumen por empresa ────────────────────────────────────────────────────
    st.markdown(f"### Resultados — Período {periodo_res}")
    st.divider()

    resumen_cols = st.columns(len(resumen) if resumen else 5)
    for i, emp in enumerate(resumen):
        with resumen_cols[i]:
            nombre = emp.get("empresa_nombre") or EMPRESAS.get(emp.get("empresa_id"), "—")
            estado_icon = "✅" if emp.get("ok") else "⚠️"
            st.markdown(f"**{estado_icon} {nombre}**")
            st.metric("⚠️ Difs",    emp.get("diferencias", 0))
            st.metric("📋 Solo CSV", emp.get("solo_csv", 0))
            st.metric("📋 Solo DB",  emp.get("solo_db", 0))

    st.divider()

    # ── Detalle de diferencias ─────────────────────────────────────────────────
    if not diferencias:
        st.success("🎉 ¡Sin diferencias! Todos los saldos coinciden entre el sistema contable y la DB.")
        st.stop()

    st.markdown(f"### ⚠️ Diferencias encontradas ({res.get('total_diferencias', len(diferencias))} cuentas)")

    df_difs = pd.DataFrame(diferencias)

    # Filtros
    fc1, fc2 = st.columns(2)
    empresas_en_difs = sorted(df_difs["empresa_nombre"].dropna().unique().tolist()) if "empresa_nombre" in df_difs.columns else []
    tipos_en_difs    = sorted(df_difs["tipo"].dropna().unique().tolist()) if "tipo" in df_difs.columns else []

    filtro_emp  = fc1.multiselect("Filtrar empresa",    options=empresas_en_difs, default=empresas_en_difs, key="f_emp_c")
    filtro_tipo = fc2.multiselect("Tipo de diferencia", options=tipos_en_difs,    default=tipos_en_difs,    key="f_tip_c")

    df_show = df_difs.copy()
    if filtro_emp  and "empresa_nombre" in df_show.columns:
        df_show = df_show[df_show["empresa_nombre"].isin(filtro_emp)]
    if filtro_tipo and "tipo" in df_show.columns:
        df_show = df_show[df_show["tipo"].isin(filtro_tipo)]

    col_config_c = {}
    if "cuenta_codigo" in df_show.columns:
        col_config_c["cuenta_codigo"] = st.column_config.NumberColumn("Cta", format="%d")
    if "saldo_csv"     in df_show.columns:
        col_config_c["saldo_csv"]     = st.column_config.NumberColumn("Saldo CSV", format="$ %.2f")
    if "saldo_db"      in df_show.columns:
        col_config_c["saldo_db"]      = st.column_config.NumberColumn("Saldo DB",  format="$ %.2f")
    if "diferencia"    in df_show.columns:
        col_config_c["diferencia"]    = st.column_config.NumberColumn("Diferencia", format="$ %.2f")

    st.dataframe(df_show, use_container_width=True, hide_index=True, column_config=col_config_c)

    # ── Exportar a Excel ───────────────────────────────────────────────────────
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Hoja resumen
        pd.DataFrame(resumen).to_excel(writer, sheet_name="Resumen", index=False)
        # Hoja detalle
        df_difs.to_excel(writer, sheet_name="Diferencias", index=False)
        # Una hoja por empresa
        if "empresa_nombre" in df_difs.columns:
            for nombre_emp in df_difs["empresa_nombre"].dropna().unique():
                df_e = df_difs[df_difs["empresa_nombre"] == nombre_emp]
                sheet_name = str(nombre_emp)[:31]
                df_e.to_excel(writer, sheet_name=sheet_name, index=False)
    output.seek(0)

    periodo_fn = periodo_res.replace("/", "") if periodo_res else "periodo"
    st.download_button(
        "📥 Exportar diferencias a Excel",
        output,
        f"chequeo_consistencia_{periodo_fn}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="secondary",
    )
