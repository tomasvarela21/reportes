"""
Home.py — ReporteApp v2
"""
import streamlit as st
from services.styles import apply_styles, render_sidebar, render_resultado_sync

st.set_page_config(
    page_title="ReporteApp",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

apply_styles()
render_sidebar()

st.title("📊 ReporteApp")
st.markdown("### Sistema de Contabilidad Multi-Empresa")
st.divider()

cards = [
    ("📤", "Carga de Diario",  "#eff6ff", "#bfdbfe", "#1e40af", "Subir y validar CSV mensual"),
    ("📚", "Libro Mayor",      "#f0fdf4", "#bbf7d0", "#166534", "Consultar saldos acumulados"),
    ("📋", "Consulta Diario",  "#fdf4ff", "#e9d5ff", "#6b21a8", "Buscar asientos cargados"),
    ("🔍", "Data Check",       "#f0f9ff", "#bae6fd", "#0c4a6e", "Consistencia sistema vs DB"),
    ("⚙️", "Administración",   "#f8fafc", "#e2e8f0", "#1e293b", "Maestros y configuración"),
]

cols = st.columns(5)
for col, (icon, titulo, bg, border, color, desc) in zip(cols, cards):
    col.markdown(f"""
    <div class="reporte-card" style="background:{bg};border:1px solid {border}">
        <div class="card-icon">{icon}</div>
        <div class="card-title" style="color:{color}">{titulo}</div>
        <div class="card-desc">{desc}</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()
st.markdown("#### 👈 Seleccioná una sección desde el menú lateral para comenzar.")

# ── Power BI Refresh ───────────────────────────────────────────────────────────
import os
from dotenv import load_dotenv
load_dotenv()

pbi_configurado = all([
    os.getenv("POWERBI_TENANT_ID"),
    os.getenv("POWERBI_CLIENT_ID"),
    os.getenv("POWERBI_CLIENT_SECRET"),
    os.getenv("POWERBI_USERNAME"),
    os.getenv("POWERBI_PASSWORD"),
    os.getenv("POWERBI_DATASET_ID"),
])

if pbi_configurado:
    st.divider()
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.markdown("#### 🔄 Power BI")
        st.caption("Actualizá el dataset de Power BI Service con los últimos datos cargados.")
        if st.button("🔄 Actualizar dataset en Power BI", type="secondary", use_container_width=True):
            import sys
            sys.path.append(os.path.join(os.path.dirname(__file__), 'services'))
            from powerbi_refresh import trigger_refresh
            with st.spinner("Conectando con Power BI Service..."):
                resultado = trigger_refresh()
            if resultado["ok"]:
                st.success("✅ Refresh iniciado. El dataset se actualizará en los próximos minutos.")
            else:
                st.error(f"❌ {resultado['error']}")

        st.caption("Sincronizá proyectos y presupuestos desde el sistema de gestión externo.")
        if st.button("🔄 Sincronizar proyectos", type="secondary", use_container_width=True):
            import sys
            sys.path.append(os.path.join(os.path.dirname(__file__), 'services'))
            from proyecto_sync_service import sincronizar
            with st.spinner("Sincronizando proyectos y presupuestos..."):
                try:
                    resultado = sincronizar(origen="manual")
                except Exception as e:
                    st.error(f"❌ Error al sincronizar: {e}")
                else:
                    render_resultado_sync(resultado)