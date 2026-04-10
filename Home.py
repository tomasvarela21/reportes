"""
Home.py — ReporteApp v2
"""
import streamlit as st
from services.styles import apply_styles, render_sidebar

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