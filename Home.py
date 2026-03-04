"""
Home.py
=======
Página principal de ReporteApp v2.
Solo bienvenida y navegación — la lógica está en pages/.
"""

import streamlit as st

st.set_page_config(
    page_title="ReporteApp",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="stSidebar"] { background-color: #1a1f2e; }
    [data-testid="stSidebar"] * { color: #e0e4ef !important; }
    h1, h2, h3 { color: #1a1f2e; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 📊 ReporteApp")
    st.markdown("**v2.0** · Grupo Corporativo")
    st.divider()
    st.markdown("""
    **Empresas activas:**
    - BATIA
    - GUARE
    - NORFORK
    - TORRES
    - WERCOLICH
    """)
    st.divider()
    st.caption("Navegá desde el menú de arriba")

st.title("📊 ReporteApp")
st.markdown("### Sistema de Contabilidad Multi-Empresa")
st.divider()

col1, col2, col3, col4, col5 = st.columns(5)

cards = [
    ("📤", "Carga de Diario",      "#eff6ff", "#bfdbfe", "#1e40af", "Subir y validar CSV mensual"),
    ("📚", "Libro Mayor",          "#f0fdf4", "#bbf7d0", "#166534", "Consultar saldos acumulados"),
    ("📋", "Consulta Diario",      "#fdf4ff", "#e9d5ff", "#6b21a8", "Buscar asientos cargados"),
    ("🏦", "Saldos de Apertura",   "#fefce8", "#fde68a", "#92400e", "Gestionar aperturas anuales"),
    ("⚙️", "Administración",       "#f8fafc", "#e2e8f0", "#1e293b", "Maestros y configuración"),
]

for col, (icon, titulo, bg, border, color, desc) in zip(
    [col1, col2, col3, col4, col5], cards
):
    col.markdown(f"""
    <div style="background:{bg};border:1px solid {border};border-radius:12px;
                padding:20px;text-align:center;height:130px">
        <div style="font-size:1.8rem">{icon}</div>
        <div style="font-weight:700;color:{color};margin-top:6px;font-size:0.9rem">{titulo}</div>
        <div style="color:#6b7280;font-size:0.75rem;margin-top:4px">{desc}</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()
st.markdown("#### 👈 Seleccioná una sección desde el menú lateral para comenzar.")