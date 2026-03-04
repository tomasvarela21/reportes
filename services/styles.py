"""
services/styles.py
"""
import os
import streamlit as st

# Directorio raíz del proyecto (donde está Home.py)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SIDEBAR_CSS = """
[data-testid="stSidebar"] { background-color: #1a1f2e; }
[data-testid="stSidebar"] * { color: #e0e4ef !important; }
[data-testid="stSidebarNav"] { display: none !important; }
"""

GLOBAL_CSS = """
[data-testid="stToolbar"]    { visibility: hidden; }
[data-testid="stDecoration"] { display: none; }
#MainMenu                    { visibility: hidden; }
footer                       { visibility: hidden; }

[data-testid="stAppViewContainer"] > .main > .block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    max-width: 100%;
}

h1, h2, h3 { color: #1a1f2e; }

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

# Paths relativos al proyecto para st.page_link
NAV_LINKS = [
    ("🏠", "Home",            "Home.py"),
    ("📤", "Carga Diario",    os.path.join("pages", "1_Carga_Diario.py")),
    ("📚", "Libro Mayor",     os.path.join("pages", "2_Libro_Mayor.py")),
    ("📋", "Consulta Diario", os.path.join("pages", "3-Consulta_Diario.py")),
    ("🏦", "Saldos Apertura", os.path.join("pages", "4-Saldos_Apertura.py")),
    ("⚙️", "Administracion",  os.path.join("pages", "5-Administracion.py")),
]

def render_sidebar():
    with st.sidebar:
        st.markdown("## 📊 ReporteApp")
        st.markdown("**v2.0** · Grupo Corporativo")
        st.divider()
        for icon, label, path in NAV_LINKS:
            abs_path = os.path.join(_ROOT, path)
            if os.path.exists(abs_path):
                st.page_link(path, label=f"{icon} {label}")
            else:
                st.caption(f"⚠️ {label} ({path})")
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