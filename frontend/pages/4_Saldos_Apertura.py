"""
frontend/pages/4_Saldos_Apertura.py
"""
import io

import pandas as pd
import streamlit as st

from services.api_client import (
    EMPRESAS,
    apply_styles,
    get_apertura,
    get_apertura_stats,
    get_empresas,
    render_sidebar,
    upload_apertura,
)

st.set_page_config(page_title="Saldos Apertura · ReporteApp", page_icon="🗂️", layout="wide")
apply_styles()
render_sidebar()

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

st.title("🗂️ Saldos de Apertura")
st.caption("Carga y consulta de saldos iniciales por empresa y año fiscal.")
st.divider()

# ── Empresa ────────────────────────────────────────────────────────────────────
try:
    empresas_api   = get_empresas()
    empresas_lista = [e["empresa_nombre"] for e in empresas_api if e.get("activa", True)]
    if not empresas_lista:
        empresas_lista = list(EMPRESAS.keys())
except Exception:
    empresas_lista = list(EMPRESAS.keys())

tabs = st.tabs(["📥 Cargar Apertura", "🔍 Consultar Saldos"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CARGA
# ══════════════════════════════════════════════════════════════════════════════
with tabs[0]:
    # ── Pantalla de éxito ──────────────────────────────────────────────────────
    if "apertura_cargada" in st.session_state:
        r = st.session_state["apertura_cargada"]
        st.success(f"✅ Apertura cargada correctamente")
        st.divider()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Registros cargados",  f"{r.get('registros', 0):,}")
        c2.metric("Suma saldo",          f"{r.get('suma_saldo', 0):,.2f}")
        c3.metric("Registros Mayor",     f"{r.get('registros_mayor', 0):,}")
        c4.metric("Año fiscal",          r.get("anio_fiscal", "—"))

        adv = r.get("advertencias", [])
        for a in adv:
            st.warning(f"⚠️ {a}")

        inv = r.get("cuentas_invalidas", [])
        if inv:
            st.warning(f"⚠️ {len(inv)} cuenta(s) no encontrada(s) en dim_cuenta: {inv[:20]}")

        st.divider()
        if st.button("📥 Cargar otra apertura", type="primary"):
            st.session_state.pop("apertura_cargada", None)
            st.rerun()
        st.stop()

    # ── Formulario de carga ────────────────────────────────────────────────────
    st.subheader("Paso 1 — Seleccioná empresa, año y archivo")
    c1, c2, c3 = st.columns(3)

    empresa_up = c1.selectbox(
        "Empresa (opcional si el CSV incluye columna 'empresa')",
        ["— Detectar del CSV —"] + empresas_lista,
        key="ap_empresa_up",
    )
    anio_fiscal = c2.number_input("Año fiscal *", min_value=2000, max_value=2099,
                                   value=2024, step=1, key="ap_anio")
    archivo_ap  = c3.file_uploader(
        "CSV de saldos de apertura",
        type=["csv", "xlsx", "xls"],
        key="ap_uploader",
        help="Formato A (nro_cta;sdfinal;...) o Formato B (cuenta_codigo;saldo;...)",
    )

    if not archivo_ap:
        st.info("👆 Subí el archivo CSV para continuar.")
        st.stop()

    st.divider()
    st.subheader("Paso 2 — Confirmación")
    emp_nombre_param = None if empresa_up == "— Detectar del CSV —" else empresa_up
    st.markdown(f"- **Empresa:** {emp_nombre_param or 'Detectar del CSV'}")
    st.markdown(f"- **Año fiscal:** {anio_fiscal}")
    st.markdown(f"- **Archivo:** {archivo_ap.name}")

    if st.button("📥 Cargar saldos de apertura", type="primary", key="btn_cargar_ap"):
        file_bytes = archivo_ap.read()
        with st.spinner("Cargando saldos y recalculando Mayor..."):
            try:
                resultado = upload_apertura(
                    file_bytes,
                    archivo_ap.name,
                    emp_nombre_param,
                    int(anio_fiscal),
                )
                st.session_state["apertura_cargada"] = resultado
                st.rerun()
            except Exception as e:
                st.error(f"❌ {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — CONSULTA
# ══════════════════════════════════════════════════════════════════════════════
with tabs[1]:
    st.subheader("Consultar saldos cargados")

    c1, c2, c3, c4 = st.columns(4)
    empresa_q  = c1.selectbox("Empresa", empresas_lista, key="ap_q_empresa")
    empresa_id_q = EMPRESAS.get(empresa_q, 0)
    anio_q     = c2.number_input("Año fiscal", min_value=2000, max_value=2099,
                                  value=2024, step=1, key="ap_q_anio")
    cuenta_q   = c3.text_input("Cuenta", placeholder="ej: 1010", key="ap_q_cta")
    cc_q       = c4.text_input("Centro costo", placeholder="ej: 1101", key="ap_q_cc")
    solo_nonzero = st.checkbox("Solo saldos ≠ 0", value=False, key="ap_q_nonzero")

    filtros_q = {
        "anio_fiscal":        int(anio_q),
        "cuenta_codigo":      int(cuenta_q.strip()) if cuenta_q.strip().isdigit() else None,
        "centro_costo":       cc_q.strip() or None,
        "solo_saldo_nonzero": solo_nonzero,
        "limit":              2000,
        "offset":             0,
    }

    col_btn, col_stats = st.columns([1, 3])
    if col_btn.button("🔍 Consultar", type="primary", key="btn_q_ap"):
        st.session_state["ap_q_result"] = None
        try:
            resp = get_apertura(empresa_id_q, filtros_q)
            filas_q = resp.get("data", []) if isinstance(resp, dict) else resp
            total_q = resp.get("total", len(filas_q)) if isinstance(resp, dict) else len(filas_q)
            st.session_state["ap_q_result"] = (filas_q, total_q)

            # Stats
            try:
                stats = get_apertura_stats(empresa_id_q, int(anio_q))
                st.session_state["ap_q_stats"] = stats
            except Exception:
                st.session_state["ap_q_stats"] = None
        except Exception as e:
            st.error(f"❌ {e}")

    if st.session_state.get("ap_q_result") is not None:
        filas_q, total_q = st.session_state["ap_q_result"]
        stats_q          = st.session_state.get("ap_q_stats")

        st.divider()
        if stats_q:
            s1, s2, s3, s4, s5 = st.columns(5)
            s1.metric("Total registros",   f"{stats_q.get('total_registros', 0):,}")
            s2.metric("Cuentas únicas",    f"{stats_q.get('cuentas_unicas', 0):,}")
            s3.metric("Suma saldo",        f"{stats_q.get('suma_saldo', 0):,.2f}")
            s4.metric("Saldo cero",        f"{stats_q.get('cuentas_con_saldo_cero', 0):,}")
            s5.metric("Con subcuenta",     f"{stats_q.get('con_subcuenta', 0):,}")

        if not filas_q:
            st.info("Sin registros con esos filtros.")
        else:
            df_q = pd.DataFrame(filas_q)
            st.caption(f"Mostrando {len(df_q):,} de {total_q:,} registros")
            st.dataframe(df_q, use_container_width=True, hide_index=True)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_q.to_excel(writer, sheet_name="Apertura", index=False)
            output.seek(0)
            st.download_button(
                "⬇️ Exportar a Excel",
                output,
                f"apertura_{empresa_q}_{anio_q}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
