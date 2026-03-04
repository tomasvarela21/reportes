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

/* Ocultar el chevron/dropdown de agrupación de páginas en el sidebar */
[data-testid="stSidebarNavItems"] summary         { display: none !important; }
[data-testid="stSidebarNavItems"] details > ul    { display: block !important; }
[data-testid="stSidebarNavItems"] details         { open: true; }
section[data-testid="stSidebarNav"] ul li details { list-style: none; }
/* Alternativa por si Streamlit usa otra estructura */
.st-emotion-cache-1gwvy71 summary { display: none !important; }
nav[data-testid="stSidebarNav"] details summary   { display: none !important; }
nav[data-testid="stSidebarNav"] details > ul      { display: block !important; }
"""

GLOBAL_CSS = """
/* Ocultar toolbar (Share, estrella, GitHub) y decoraciones */
[data-testid="stToolbar"]    { visibility: hidden; }
[data-testid="stDecoration"] { display: none; }
#MainMenu                    { visibility: hidden; }
footer                       { visibility: hidden; }
header                       { visibility: hidden; }

/* Eliminar padding superior vacío */
[data-testid="stAppViewContainer"] > .main > .block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    max-width: 100%;
}

/* Títulos */
h1, h2, h3 { color: #1a1f2e; }

/* Cards home — min-height flexible */
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

def apply_styles(extra_css: str = "") -> None:
    st.markdown(
        f"<style>{SIDEBAR_CSS}{GLOBAL_CSS}{extra_css}</style>",
        unsafe_allow_html=True
    )