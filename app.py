"""
Aplicación Streamlit para cargar libros diarios
"""
import streamlit as st
import pandas as pd
import sys
import os

# Agregar path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from services.file_parser import FileParser
from services.validator import Validator
from services.normalizer import Normalizer
from services.db_service import DBService
from utils.helpers import get_nombre_mes, format_currency


# Configuración de la página
st.set_page_config(
    page_title="Sistema de Reportes Contables",
    page_icon="📊",
    layout="wide"
)

# Título
st.title("📊 Sistema de Reportes Contables")
st.markdown("---")

# Sidebar con información
with st.sidebar:
    st.header("ℹ️ Información")
    st.markdown("""
    ### Formato de archivo
    
    **Nombre:** `diario_EmpresaA_01-2025.csv`
    
    - `EmpresaA`: Código de empresa
    - `01`: Mes (01-12)
    - `2025`: Año
    
    ### Proceso
    1. Subir archivo CSV
    2. Validación automática
    3. Preview de datos
    4. Confirmar carga
    5. Cálculo automático del Libro Mayor
    """)


# Inicializar servicios
@st.cache_resource
def get_services():
    return {
        'parser': FileParser(),
        'validator': Validator(),
        'normalizer': Normalizer(),
        'db': DBService()
    }

services = get_services()


# Sección de carga de archivo
st.header("1️⃣ Cargar Archivo del Libro Diario")

uploaded_file = st.file_uploader(
    "Subir archivo CSV",
    type=['csv'],
    help="Formato: diario_EmpresaA_01-2025.csv"
)

if uploaded_file is not None:
    filename = uploaded_file.name
    
    st.success(f"✅ Archivo cargado: **{filename}**")
    
    # Parsear archivo
    with st.spinner("📖 Parseando archivo..."):
        # Guardar temporalmente
        temp_path = f"data/{filename}"
        os.makedirs('data', exist_ok=True)
        
        with open(temp_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())
        
        # Parsear
        exito, mensaje, df = services['parser'].parse_csv(temp_path, filename)
    
    if not exito:
        st.error(f"❌ Error: {mensaje}")
        st.stop()
    
    st.success(mensaje)
    
    # Mostrar resumen
    resumen = services['parser'].get_resumen()
    
    st.markdown("---")
    st.header("2️⃣ Resumen del Archivo")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Empresa", resumen['empresa'])
        st.metric("Período", f"{get_nombre_mes(resumen['mes'])} {resumen['anio']}")
    
    with col2:
        st.metric("Total Registros", f"{resumen['total_registros']:,}")
        st.metric("Asientos Únicos", f"{resumen['asientos_unicos']:,}")
    
    with col3:
        st.metric("Total Debe", f"${format_currency(resumen['total_debe'])}")
        st.metric("Total Haber", f"${format_currency(resumen['total_haber'])}")
    
    with col4:
        diferencia = resumen['diferencia']
        color = "normal" if diferencia < 0.01 else "inverse"
        st.metric(
            "Diferencia", 
            f"${format_currency(diferencia)}",
            delta=None,
            delta_color=color
        )
        st.metric("Cuentas Únicas", f"{resumen['cuentas_unicas']:,}")
    
    # Validaciones
    st.markdown("---")
    st.header("3️⃣ Validaciones")
    
    with st.spinner("🔍 Validando datos..."):
        df_limpio = services['parser'].get_dataframe_limpio()
        
        es_valido, mensaje_validacion, id_empresa = services['validator'].validar_todo(
            df_limpio,
            resumen['empresa'],
            resumen['mes'],
            resumen['anio']
        )
    
    if es_valido:
        st.success(f"✅ {mensaje_validacion}")
    else:
        st.error(f"❌ Errores de validación:\n\n{mensaje_validacion}")
        st.stop()
    
    # Preview de datos
    st.markdown("---")
    st.header("4️⃣ Preview de Datos")
    
    st.dataframe(
        df_limpio.head(20),
        use_container_width=True,
        height=400
    )
    
    # Botón de confirmación
    st.markdown("---")
    st.header("5️⃣ Confirmar Carga")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.info("""
        ⚠️ **Importante:**
        - Los datos se insertarán en `libro_diario_abierto`
        - Se calculará automáticamente el `libro_mayor_abierto`
        - Esta acción no se puede deshacer desde la interfaz
        """)
    
    with col2:
        confirmar = st.button(
            "🚀 Confirmar e Insertar",
            type="primary",
            use_container_width=True
        )
    
    if confirmar:
        st.markdown("---")
        st.header("6️⃣ Procesando...")
        
        # Normalizar datos
        with st.spinner("🔄 Normalizando datos..."):
            df_normalizado = services['normalizer'].normalizar_para_db(df_limpio, id_empresa)
        
        st.success("✅ Datos normalizados")
        
        # Insertar en DB
        progress_bar = st.progress(0, text="Insertando en base de datos...")
        
        exito_insert, mensaje_insert, registros = services['db'].insertar_libro_diario(
            df_normalizado,
            filename
        )
        
        progress_bar.progress(50, text="Calculando libro mayor...")
        
        if exito_insert:
            st.success(mensaje_insert)
            
            # Calcular libro mayor
            exito_mayor, mensaje_mayor = services['db'].calcular_libro_mayor(
                id_empresa,
                resumen['anio'],
                resumen['mes']
            )
            
            progress_bar.progress(100, text="Completado!")
            
            if exito_mayor:
                st.success(mensaje_mayor)
            else:
                st.warning(f"⚠️ Libro mayor: {mensaje_mayor}")
            
            # Estadísticas finales
            stats = services['db'].obtener_estadisticas_periodo(
                id_empresa,
                resumen['anio'],
                resumen['mes']
            )
            
            st.markdown("---")
            st.header("✅ Carga Completada")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Movimientos Insertados", f"{stats['total_movimientos']:,}")
            
            with col2:
                st.metric("Total Debe", f"${format_currency(stats['total_debe'])}")
            
            with col3:
                st.metric("Total Haber", f"${format_currency(stats['total_haber'])}")
            
            st.balloons()
            
        else:
            progress_bar.empty()
            st.error(f"❌ Error: {mensaje_insert}")
        
        # Limpiar archivo temporal
        if os.path.exists(temp_path):
            os.remove(temp_path)

else:
    st.info("👆 Subí un archivo CSV para comenzar")


# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <small>Sistema de Reportes Contables v1.0 | Desarrollado para automatización de procesos contables</small>
</div>
""", unsafe_allow_html=True)