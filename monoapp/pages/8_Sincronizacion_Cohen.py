import os
import sys
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

from services.db import get_conn
from services.styles import apply_styles, render_sidebar

st.set_page_config(page_title="Sync Cohen · ReporteApp", page_icon="🔄", layout="wide")
apply_styles()
render_sidebar()

from staging_service import EMPRESAS
from cohen_service import CODEMP_MAP, get_cohen_token, sincronizar_periodo

MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

CODEMP_POR_NOMBRE = {nombre: codemp for codemp, nombre in CODEMP_MAP.items()}

st.title("🔄 Sincronización con API Cohen")
st.caption(
    "Descarga el libro diario directamente desde el sistema contable (Cohen/ORDS) "
    "y lo carga en Neon, reemplazando la carga manual de CSV."
)
st.info(
    "🚧 **Solo BATIA por ahora.** El parámetro `p_codemp` de la API no filtra por empresa "
    "con las credenciales de test actuales — siempre devuelve los datos de BATIA sin importar "
    "qué código se pida. Hasta que esto se resuelva con quien administra Cohen, el resto de "
    "las empresas no están habilitadas para evitar cargar datos de una empresa bajo el nombre de otra."
)
st.divider()

client_id = os.getenv("COHEN_CLIENT_ID")
client_secret = os.getenv("COHEN_CLIENT_SECRET")

if not client_id or not client_secret:
    st.warning(
        "⚠️ Faltan configurar `COHEN_CLIENT_ID` / `COHEN_CLIENT_SECRET` en el `.env`. "
        "No se puede sincronizar hasta que estén definidos."
    )
    st.stop()

conn = get_conn()
if conn is None:
    st.stop()

if 'sync_cohen_resultado' in st.session_state:
    r = st.session_state['sync_cohen_resultado']
    st.success(f"✅ Sincronización completada para **{r['empresa']}** — período **{r['periodo']}**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Registros en diario", f"{r['registros_cargados']:,}")
    c2.metric("Registros en mayor",  f"{r['registros_mayor']:,}")
    c3.metric("Tiempo total",        f"{r['duracion_ms']:,} ms")
    st.divider()
    if st.button("🔄 Sincronizar otro período", type="primary"):
        st.session_state.pop('sync_cohen_resultado', None)
        st.rerun()
    st.stop()

st.subheader("Seleccioná empresa y período")
c1, c2, c3 = st.columns([2, 1, 1])

empresas_lista = sorted(CODEMP_POR_NOMBRE.keys())
empresa_nombre = c1.selectbox("Empresa", empresas_lista)
anio = c2.number_input("Año", min_value=2000, max_value=2100, value=2026, step=1)
mes = c3.selectbox("Mes", list(MESES.keys()), format_func=lambda m: MESES[m])

empresa_id = EMPRESAS.get(empresa_nombre)
codemp = CODEMP_POR_NOMBRE[empresa_nombre]

st.caption(f"codemp Cohen: `{codemp}` → empresa_id Neon: `{empresa_id}`")
st.divider()

if st.button(f"📥 Sincronizar {MESES[mes]} {anio} — {empresa_nombre}", type="primary"):
    try:
        with st.spinner("Obteniendo token de Cohen..."):
            token = get_cohen_token(client_id, client_secret)
        with st.spinner("Descargando y cargando el libro diario..."):
            resultado = sincronizar_periodo(
                conn=conn, empresa_id=empresa_id, codemp=codemp,
                anio=int(anio), mes=int(mes), token=token,
            )
        st.session_state['sync_cohen_resultado'] = {
            **resultado,
            "empresa": empresa_nombre,
        }
        st.rerun()
    except Exception as e:
        st.error(f"❌ Error al sincronizar: {e}")
