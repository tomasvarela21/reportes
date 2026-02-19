"""
Aplicación Streamlit para cargar libros diarios
"""
import streamlit as st
import pandas as pd
import sys
import os
import io
import plotly.express as px

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
    6. Vista previa del Libro Mayor
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
        
        # Contenedores para el progreso
        progress_container = st.container()
        status_container = st.container()
        
        with progress_container:
            st.write("📊 Insertando registros en la base de datos...")
            progress_bar = st.progress(0)
            progress_text = st.empty()
        
        # Insertar en DB con callback de progreso
        total_registros = len(df_normalizado)
        
        def actualizar_progreso(actual, total):
            """Callback para actualizar el progress bar"""
            progreso = int((actual / total) * 100)
            progress_bar.progress(progreso)
            progress_text.text(f"Procesando: {actual}/{total} registros ({progreso}%)")
        
        # Insertar con progreso
        exito_insert, mensaje_insert, registros = services['db'].insertar_libro_diario(
            df_normalizado,
            filename,
            callback_progreso=actualizar_progreso
        )
        
        if exito_insert:
            progress_bar.progress(100)
            progress_text.text(f"✅ Completado: {registros}/{total_registros} registros")
            st.success(mensaje_insert)
            
            # Calcular libro mayor
            with status_container:
                with st.spinner("📊 Calculando libro mayor..."):
                    exito_mayor, mensaje_mayor = services['db'].calcular_libro_mayor(
                        id_empresa,
                        resumen['anio'],
                        resumen['mes']
                    )
            
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
            
            # ========================================
            # VISTA PREVIA LIBRO MAYOR
            # ========================================
            
            if exito_mayor:
                st.markdown("---")
                st.header("📚 Libro Mayor Calculado")
                
                # Obtener libro mayor
                with st.spinner("📖 Cargando libro mayor..."):
                    df_mayor = services['db'].obtener_libro_mayor(
                        id_empresa,
                        resumen['anio'],
                        resumen['mes']
                    )
                
                if not df_mayor.empty:
                    # Métricas del libro mayor
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Cuentas", len(df_mayor))
                    
                    with col2:
                        total_debe_mayor = df_mayor['total_debe'].sum()
                        st.metric("Total Debe", f"${format_currency(total_debe_mayor)}")
                    
                    with col3:
                        total_haber_mayor = df_mayor['total_haber'].sum()
                        st.metric("Total Haber", f"${format_currency(total_haber_mayor)}")
                    
                    with col4:
                        saldo_final_total = df_mayor['saldo_final'].sum()
                        st.metric("Saldo Final Total", f"${format_currency(saldo_final_total)}")
                    
                    # Filtros
                    st.markdown("### 🔍 Filtros")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Filtro por código de cuenta
                        buscar_cuenta = st.text_input(
                            "Buscar por código de cuenta",
                            placeholder="Ej: 1.01.01",
                            help="Busca cuentas que contengan este texto"
                        )
                    
                    with col2:
                        # Filtro por nombre
                        buscar_nombre = st.text_input(
                            "Buscar por nombre de cuenta",
                            placeholder="Ej: Caja",
                            help="Busca cuentas cuyo nombre contenga este texto"
                        )
                    
                    # Aplicar filtros
                    df_filtrado = df_mayor.copy()
                    
                    if buscar_cuenta:
                        df_filtrado = df_filtrado[
                            df_filtrado['codigo_cuenta'].astype(str).str.contains(buscar_cuenta, case=False, na=False)
                        ]
                    
                    if buscar_nombre:
                        df_filtrado = df_filtrado[
                            df_filtrado['nombre_cuenta'].astype(str).str.contains(buscar_nombre, case=False, na=False)
                        ]
                    
                    # Preparar datos para mostrar
                    df_display = df_filtrado.copy()
                    
                    # Formatear valores numéricos
                    df_display['saldo_inicial'] = df_display['saldo_inicial'].apply(
                        lambda x: f"${format_currency(x)}"
                    )
                    df_display['total_debe'] = df_display['total_debe'].apply(
                        lambda x: f"${format_currency(x)}"
                    )
                    df_display['total_haber'] = df_display['total_haber'].apply(
                        lambda x: f"${format_currency(x)}"
                    )
                    df_display['saldo_final'] = df_display['saldo_final'].apply(
                        lambda x: f"${format_currency(x)}"
                    )
                    
                    # Renombrar columnas para display
                    df_display = df_display.rename(columns={
                        'codigo_cuenta': 'Código',
                        'nombre_cuenta': 'Cuenta',
                        'saldo_inicial': 'Saldo Inicial',
                        'total_debe': 'Debe',
                        'total_haber': 'Haber',
                        'saldo_final': 'Saldo Final'
                    })
                    
                    # Mostrar cantidad de resultados
                    st.info(f"📊 Mostrando {len(df_filtrado)} de {len(df_mayor)} cuentas")
                    
                    # Tabla del libro mayor
                    st.markdown("### 📋 Detalle del Libro Mayor")
                    
                    st.dataframe(
                        df_display[['Código', 'Cuenta', 'Saldo Inicial', 'Debe', 'Haber', 'Saldo Final']],
                        use_container_width=True,
                        height=500,
                        hide_index=True
                    )
                    
                    # Opciones de descarga
                    st.markdown("### 💾 Descargar")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        # Descargar como CSV
                        csv = df_filtrado.to_csv(index=False)
                        st.download_button(
                            label="📄 Descargar CSV",
                            data=csv,
                            file_name=f"libro_mayor_{resumen['empresa']}_{resumen['mes']:02d}_{resumen['anio']}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    with col2:
                        # Descargar como Excel
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            df_filtrado.to_excel(writer, index=False, sheet_name='Libro Mayor')
                        
                        st.download_button(
                            label="📊 Descargar Excel",
                            data=buffer.getvalue(),
                            file_name=f"libro_mayor_{resumen['empresa']}_{resumen['mes']:02d}_{resumen['anio']}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    
                    with col3:
                        # Ver top cuentas
                        ver_top = st.toggle("Ver Top 10 Cuentas", value=False)
                    
                    # Top 10 cuentas por movimiento
                    if ver_top:
                        st.markdown("### 📈 Top 10 Cuentas por Movimiento")
                        
                        df_top = df_mayor.copy()
                        df_top['total_movimiento'] = df_top['total_debe'] + df_top['total_haber']
                        df_top = df_top.nlargest(10, 'total_movimiento')
                        
                        # Crear gráfico
                        fig = px.bar(
                            df_top,
                            x='codigo_cuenta',
                            y='total_movimiento',
                            title='Top 10 Cuentas con Mayor Movimiento',
                            labels={'codigo_cuenta': 'Cuenta', 'total_movimiento': 'Total Movimiento ($)'},
                            color='total_movimiento',
                            color_continuous_scale='Blues'
                        )
                        
                        fig.update_layout(
                            xaxis_tickangle=-45,
                            height=400
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Tabla del top 10
                        df_top_display = df_top[['codigo_cuenta', 'nombre_cuenta', 'total_movimiento']].copy()
                        df_top_display['total_movimiento'] = df_top_display['total_movimiento'].apply(
                            lambda x: f"${format_currency(x)}"
                        )
                        df_top_display = df_top_display.rename(columns={
                            'codigo_cuenta': 'Código',
                            'nombre_cuenta': 'Cuenta',
                            'total_movimiento': 'Total Movimiento'
                        })
                        
                        st.dataframe(
                            df_top_display,
                            use_container_width=True,
                            hide_index=True
                        )
                
                else:
                    st.warning("⚠️ No se encontraron datos en el libro mayor")
            
            # Botón para cargar otro archivo
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button("🔄 Cargar Otro Archivo", type="primary", use_container_width=True):
                    st.session_state.clear()
                    st.rerun()
            
        else:
            progress_bar.empty()
            progress_text.empty()
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