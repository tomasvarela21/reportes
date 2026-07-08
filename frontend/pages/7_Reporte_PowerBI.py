"""
frontend/pages/7_Reporte_PowerBI.py
"""
import streamlit as st
import streamlit.components.v1 as components

from services.api_client import apply_styles, render_sidebar, refresh_powerbi

st.set_page_config(page_title="Reporte Power BI · ReporteApp", page_icon="📈", layout="wide")
apply_styles()
render_sidebar()

st.title("📈 Reporte Power BI")
st.divider()

POWERBI_REPORT_URL = (
    "https://app.powerbi.com/reportEmbed?"
    "reportId=5111aeb8-5bac-4805-bc50-08f787d8e8e6"
    "&autoAuth=true"
    "&ctid=0a6639d4-80fa-4acc-ad8c-031db6bf9cf3"
)

# ── Controles ─────────────────────────────────────────────────────────────────

col_info, col_btn = st.columns([5, 1])

with col_info:
    st.info(
        "Este reporte usa autenticación automática (`autoAuth=true`): si no se ve, "
        "iniciá sesión en Power BI (app.powerbi.com) con la cuenta de la organización "
        "en otra pestaña del mismo navegador y volvé a cargar esta página."
    )

with col_btn:
    st.markdown("&nbsp;", unsafe_allow_html=True)
    if st.button("🔄 Actualizar dataset", use_container_width=True):
        with st.spinner("Solicitando refresh a Power BI Service..."):
            try:
                resultado = refresh_powerbi()
                if resultado.get("ok"):
                    st.success(resultado.get("message", "Refresh iniciado correctamente."))
                else:
                    err = resultado.get("error", "Error desconocido.")
                    if "Variables de entorno" in err:
                        st.error(
                            "Las variables de Power BI no están configuradas en el servidor. "
                            "Configurá `POWERBI_*` en las env vars de Render."
                        )
                    elif "en curso" in err:
                        st.warning(err)
                    else:
                        st.error(err)
            except Exception as e:
                st.error(f"Error al conectar con la API: {e}")

# ── Reporte embebido ──────────────────────────────────────────────────────────

components.iframe(POWERBI_REPORT_URL, height=650, scrolling=True)
