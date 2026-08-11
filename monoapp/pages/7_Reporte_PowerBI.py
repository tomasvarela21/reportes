import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from services.styles import apply_styles, render_sidebar
load_dotenv()

st.set_page_config(page_title="Reporte Power BI · ReporteApp", page_icon="📈", layout="wide")
apply_styles()
render_sidebar()

st.title("📈 Reporte Power BI")
st.divider()

POWERBI_REPORT_URL = (
    "https://app.powerbi.com/view?"
    "r=eyJrIjoiZGRhODk0ZDMtNmViMC00MWE1LTgzZjAtOWM3ODhkYTNmZjJkIiwidCI6IjBhNjYzOWQ0LTgwZmEtNGFjYy1hZDhjLTAzMWRiNmJmOWNmMyIsImMiOjR9"
)

components.iframe(POWERBI_REPORT_URL, height=650, scrolling=True)
