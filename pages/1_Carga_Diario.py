"""
pages/1_Carga_Diario.py
"""
import os, sys, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))
from file_parser import FileParser          # type: ignore
from validator import Validator             # type: ignore
from staging_service import StagingService  # type: ignore

st.set_page_config(page_title="Carga Diario · ReporteApp", page_icon="📤", layout="wide")
st.markdown("""<style>
[data-testid="stSidebar"]{background:#1a1f2e}
[data-testid="stSidebar"] *{color:#e0e4ef!important}
h1,h2,h3{color:#1a1f2e} #MainMenu{visibility:hidden} footer{visibility:hidden}
.step-bar{display:flex;gap:8px;margin-bottom:28px}
.step{flex:1;padding:10px 8px;border-radius:8px;text-align:center;font-size:.8rem;
      font-weight:600;border:2px solid #e5e7eb;color:#9ca3af;background:#f9fafb}
.step.active{border-color:#2563eb;color:#2563eb;background:#eff6ff}
.step.done{border-color:#16a34a;color:#16a34a;background:#f0fdf4}
.check-ok{color:#16a34a;font-weight:600}
.check-warn{color:#d97706;font-weight:600}
.check-err{color:#dc2626;font-weight:600}
.empresa-detectada{background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;
    padding:8px 14px;color:#1e40af;font-size:.85rem;margin-bottom:8px}
.modal-warning{background:#fff7ed;border:2px solid #f97316;border-radius:12px;
    padding:20px 24px;margin:16px 0}
</style>""", unsafe_allow_html=True)

EMPRESAS = ["BATIA","GUARE","NORFORK","TORRES","WERCOLICH"]
MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

def step_bar(paso_actual):
    pasos = ["1 · Archivo","2 · Preview","3 · Validación","4 · Confirmar","5 · Resultado"]
    html = '<div class="step-bar">'
    for i, nombre in enumerate(pasos, 1):
        css = "step done" if i < paso_actual else ("step active" if i == paso_actual else "step")
        html += f'<div class="{css}">{nombre}</div>'
    st.markdown(html + '</div>', unsafe_allow_html=True)

def badge_check(check):
    if check["ok"]:
        return f'<span class="check-ok">✅ {check["nombre"]}</span> — {check["detalle"]}'
    elif not check.get("bloquea", True):
        return f'<span class="check-warn">⚠️ {check["nombre"]}</span> — {check["detalle"]}'
    else:
        return f'<span class="check-err">❌ {check["nombre"]}</span> — {check["detalle"]}'

def reset_estado():
    for key in ['paso','parse_result','val_result','val_resumen','periodo_info',
                'empresa_sel','archivo_nombre','reemplazar','empresa_detectada']:
        st.session_state.pop(key, None)

if 'paso' not in st.session_state:
    st.session_state.paso = 1

st.title("📤 Carga de Libro Diario")
st.caption("Subí el CSV mensual, validalo y confirmá la carga.")
st.divider()
step_bar(st.session_state.paso)

# =============================================================================
# PASO 1
# =============================================================================
if st.session_state.paso == 1:
    st.subheader("Paso 1 · Seleccioná la empresa y el archivo")

    archivo = st.file_uploader("Archivo CSV del Libro Diario", type=["csv"],
                                help="Separador punto y coma (;), encoding latin-1/cp1252")

    empresa_sugerida = None
    if archivo is not None:
        empresa_sugerida = FileParser.detectar_empresa(archivo.name)
        if empresa_sugerida:
            st.markdown(
                f'<div class="empresa-detectada">💡 Empresa detectada del nombre del archivo: '
                f'<strong>{empresa_sugerida}</strong></div>', unsafe_allow_html=True)

    idx_empresa = EMPRESAS.index(empresa_sugerida) if empresa_sugerida in EMPRESAS else 0
    empresa = st.selectbox("Empresa", EMPRESAS, index=idx_empresa,
                           help="Podés cambiar si la detección no fue correcta")

    if archivo:
        st.success(f"Archivo cargado: **{archivo.name}** ({archivo.size/1024:.1f} KB)")
        if st.button("Parsear archivo →", type="primary"):
            with st.spinner("Parseando archivo..."):
                result = FileParser().parsear(archivo, empresa)
            st.session_state.parse_result   = result
            st.session_state.empresa_sel    = empresa
            st.session_state.archivo_nombre = archivo.name
            if result.ok:
                st.session_state.paso = 2; st.rerun()
            else:
                st.error("El archivo tiene errores que impiden continuar:")
                for e in result.errores: st.error(f"• {e}")

# =============================================================================
# PASO 2
# =============================================================================
elif st.session_state.paso == 2:
    result  = st.session_state.parse_result
    empresa = st.session_state.empresa_sel
    df      = result.dataframe

    st.subheader("Paso 2 · Preview del archivo")
    col1,col2,col3,col4 = st.columns(4)
    col1.metric("Empresa",       empresa)
    col2.metric("Período",       f"{MESES.get(result.periodo_mes,'?')} {result.periodo_anio}" if result.periodo_mes else "—")
    col3.metric("Filas válidas", f"{result.total_filas_validas:,}")
    col4.metric("Filas raw",     f"{result.total_filas_raw:,}")
    st.divider()
    st.markdown("**Primeras 50 filas:**")
    st.dataframe(df.head(50), use_container_width=True, hide_index=True, column_config={
        "debe":  st.column_config.NumberColumn("Debe",  format="%.2f"),
        "haber": st.column_config.NumberColumn("Haber", format="%.2f"),
        "fecha": st.column_config.DateColumn("Fecha"),
    })
    for adv in result.advertencias: st.warning(f"⚠️ {adv}")

    col_back, col_next = st.columns([1,5])
    with col_back:
        if st.button("← Volver"): reset_estado(); st.rerun()
    with col_next:
        if st.button("Validar →", type="primary"):
            with st.spinner("Ejecutando validaciones..."):
                conn = get_conn()
                if conn is None: st.stop()
                validator  = Validator(conn)
                df_raw     = getattr(result, 'dataframe_raw', pd.DataFrame())
                val_result = validator.validar(df, df_raw, empresa, result.periodo_anio, result.periodo_mes)
                resumen    = validator.resumen_texto(val_result)
            st.session_state.val_result  = val_result
            st.session_state.val_resumen = resumen
            st.session_state.paso = 3; st.rerun()

# =============================================================================
# PASO 3
# =============================================================================
elif st.session_state.paso == 3:
    result  = st.session_state.parse_result
    val     = st.session_state.val_result
    resumen = st.session_state.val_resumen
    empresa = st.session_state.empresa_sel

    st.subheader("Paso 3 · Resultado de validación")
    col1,col2,col3 = st.columns(3)
    col1.metric("Registros",   f"{resumen['total_registros']:,}")
    col2.metric("Total Debe",  f"{resumen['total_debe']:,.2f}")
    col3.metric("Total Haber", f"{resumen['total_haber']:,.2f}")
    st.divider()
    st.markdown("**Resultado por validación:**")
    for check in resumen['checks']:
        st.markdown(badge_check(check), unsafe_allow_html=True)
    if not val.tipos_ok and val.tipos_problemas:
        with st.expander(f"📋 Ver valores no numéricos ({len(val.tipos_problemas)} filas)"):
            st.dataframe(pd.DataFrame(val.tipos_problemas), use_container_width=True, hide_index=True)
    if not val.balance_ok and val.asientos_desbalanceados:
        with st.expander(f"📋 Ver asientos desbalanceados ({len(val.asientos_desbalanceados)})"):
            st.dataframe(pd.DataFrame(val.asientos_desbalanceados), use_container_width=True, hide_index=True)
    st.divider()
    if resumen['advertencias']:
        st.markdown("**Advertencias (no bloquean la carga):**")
        for adv in resumen['advertencias']: st.warning(f"⚠️ {adv}")

    col_back, col_next = st.columns([1,5])
    with col_back:
        if st.button("← Volver"): st.session_state.paso = 2; st.rerun()
    with col_next:
        hay_errores = any(not c["ok"] and c.get("bloquea", True) for c in resumen['checks'])
        if hay_errores:
            st.error("Hay errores bloqueantes. Corregí el archivo y volvé a cargar.")
        else:
            if st.button("Continuar →", type="primary"):
                conn = get_conn()
                if conn is None: st.stop()
                staging      = StagingService(conn)
                periodo_info = staging.verificar_periodo(empresa, result.periodo_anio, result.periodo_mes)
                st.session_state.periodo_info = periodo_info
                st.session_state.paso = 4; st.rerun()

# =============================================================================
# PASO 4
# =============================================================================
elif st.session_state.paso == 4:
    result         = st.session_state.parse_result
    empresa        = st.session_state.empresa_sel
    periodo_info   = st.session_state.periodo_info
    archivo_nombre = st.session_state.archivo_nombre

    st.subheader("Paso 4 · Confirmación")
    col1,col2,col3 = st.columns(3)
    col1.metric("Empresa",   empresa)
    col2.metric("Período",   f"{MESES.get(result.periodo_mes,'?')} {result.periodo_anio}")
    col3.metric("Registros", f"{result.total_filas_validas:,}")
    st.divider()

    if periodo_info.existe:
        st.markdown(f"""<div class="modal-warning">
            <div style="font-size:1.1rem;font-weight:700;color:#c2410c">⚠️ El período ya tiene datos cargados</div>
            <div style="margin-top:12px;color:#374151;line-height:1.8">
                <b>Empresa:</b> {empresa} &nbsp;|&nbsp;
                <b>Período:</b> {MESES.get(periodo_info.periodo_mes,'?')} {periodo_info.periodo_anio}<br>
                <b>Registros existentes:</b> {periodo_info.total_registros:,}<br>
                <b>Cargado el:</b> {periodo_info.fecha_carga.strftime('%d/%m/%Y %H:%M') if periodo_info.fecha_carga else '—'}<br>
                <b>Archivo original:</b> {periodo_info.archivo_origen or '—'}
            </div>
            <div style="margin-top:12px;color:#c2410c;font-weight:600">
                Si confirmás, los datos existentes serán ELIMINADOS y reemplazados.
                El Libro Mayor será recalculado desde este período en adelante.
            </div>
        </div>""", unsafe_allow_html=True)
        confirmar = st.checkbox("Entiendo que los datos existentes serán eliminados y reemplazados por los nuevos.")
        col_back, col_proc = st.columns([1,5])
        with col_back:
            if st.button("← Volver"): st.session_state.paso = 3; st.rerun()
        with col_proc:
            if st.button("🔄 Reemplazar y recalcular →", type="primary", disabled=not confirmar):
                st.session_state.reemplazar = True; st.session_state.paso = 5; st.rerun()
    else:
        st.success(f"✅ El período **{MESES.get(result.periodo_mes,'?')} {result.periodo_anio}** de **{empresa}** no tiene datos previos.")
        col_back, col_proc = st.columns([1,5])
        with col_back:
            if st.button("← Volver"): st.session_state.paso = 3; st.rerun()
        with col_proc:
            if st.button("✅ Confirmar carga →", type="primary"):
                st.session_state.reemplazar = False; st.session_state.paso = 5; st.rerun()

# =============================================================================
# PASO 5
# =============================================================================
elif st.session_state.paso == 5:
    result         = st.session_state.parse_result
    empresa        = st.session_state.empresa_sel
    archivo_nombre = st.session_state.archivo_nombre
    reemplazar     = st.session_state.get('reemplazar', False)

    st.subheader("Paso 5 · Procesando...")
    with st.spinner("Cargando datos y recalculando Libro Mayor..."):
        conn = get_conn()
        if conn is None: st.stop()
        staging = StagingService(conn)
        carga   = staging.ejecutar_carga(
            df=result.dataframe, empresa=empresa,
            periodo_anio=result.periodo_anio, periodo_mes=result.periodo_mes,
            archivo_nombre=archivo_nombre, reemplazar=reemplazar,
        )
    st.divider()
    if carga.ok:
        accion_txt = "Reemplazo" if carga.accion == 'reemplazo' else "Carga nueva"
        st.success(f"✅ {accion_txt} completada exitosamente.")
        col1,col2,col3,col4 = st.columns(4)
        col1.metric("Registros cargados", f"{carga.registros_cargados:,}")
        col2.metric("Registros en Mayor", f"{carga.registros_mayor:,}")
        col3.metric("Acción",             accion_txt)
        col4.metric("Tiempo",             f"{carga.duracion_ms/1000:.1f}s")
        st.info(f"📊 El Libro Mayor fue recalculado desde **{MESES.get(result.periodo_mes,'?')} {result.periodo_anio}** en adelante.")
    else:
        st.error("❌ La carga falló.")
        for e in carga.errores: st.error(f"• {e}")
    st.divider()
    if st.button("📤 Cargar otro archivo", type="primary"):
        reset_estado(); st.rerun()