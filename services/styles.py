"""
services/styles.py
==================
Estilos CSS globales para ReporteApp.
Uso: from services.styles import apply_styles
"""
import streamlit as st

SIDEBAR_CSS = """
[data-testid="stSidebar"] { background-color: #1a1f2e; }
[data-testid="stSidebar"] * { color: #e0e4ef !important; }

/* Ocultar el nav automático de páginas (el que hace el dropdown) */
[data-testid="stSidebarNav"] { display: none !important; }
"""

GLOBAL_CSS = """
/* Ocultar toolbar (Share, estrella, GitHub) y decoraciones */
[data-testid="stToolbar"]    { visibility: hidden; }
[data-testid="stDecoration"] { display: none; }
#MainMenu                    { visibility: hidden; }
footer                       { visibility: hidden; }

/* Ocultar header pero mantener el botón de colapsar sidebar */
header[data-testid="stHeader"] { background: none; }
header[data-testid="stHeader"] > * { visibility: hidden; }
[data-testid="stSidebarCollapseButton"] { visibility: visible !important; }
[data-testid="collapsedControl"]        { visibility: visible !important; }

/* Eliminar padding superior vacío */
[data-testid="stAppViewContainer"] > .main > .block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    max-width: 100%;
}

/* Títulos */
h1, h2, h3 { color: #1a1f2e; }

/* Cards home */
.reporte-card {
    border-radius: 12px;
    padding: 20px 16px;
    text-align: center;
    min-height: 130px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 6px;
}
.reporte-card .card-icon  { font-size: 1.8rem; }
.reporte-card .card-title { font-weight: 700; font-size: 0.9rem; }
.reporte-card .card-desc  { color: #6b7280; font-size: 0.75rem; }
"""

NAV_LINKS = [
    ("🏠", "Home",             "Home.py"),
    ("📤", "Carga Diario",     "pages/1_Carga_Diario.py"),
    ("📚", "Libro Mayor",      "pages/2_Libro_Mayor.py"),
    ("📋", "Consulta Diario",  "pages/3_Consulta_Diario.py"),
    ("🏦", "Saldos Apertura",  "pages/4_Saldos_Apertura.py"),
    ("⚙️", "Administracion",   "pages/5_Administracion.py"),
]

def render_sidebar():
    with st.sidebar:
        st.markdown("## 📊 ReporteApp")
        st.markdown("**v2.0** · Grupo Corporativo")
        st.divider()
        for icon, label, path in NAV_LINKS:
            st.page_link(path, label=f"{icon} {label}")
        st.divider()
        st.markdown("""
        **Empresas activas:**
        - BATIA · GUARE · NORFORK
        - TORRES · WERCOLICH
        """)

def apply_styles(extra_css: str = "") -> None:
    st.markdown(
        f"<style>{SIDEBAR_CSS}{GLOBAL_CSS}{extra_css}</style>",
        unsafe_allow_html=True
    )