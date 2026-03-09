"""
pages/1_Carga_Diario.py
"""
import os, sys, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))
from file_parser import FileParser          # type: ignore
from validator import Validator             # type: ignore
from staging_service import StagingService  # type: ignore

st.set_page_config(page_title="Carga Diario · ReporteApp", page_icon="📤", layout="wide")
apply_styles(extra_css="""
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
.periodo-badge{display:inline-block;background:#f0fdf4;border:1px solid #bbf7d0;
    border-radius:6px;padding:3px 10px;color:#15803d;font-size:.8rem;font-weight:600;margin:2px}
.periodo-badge.existe{background:#fff7ed;border-color:#fed7aa;color:#c2410c}
""")
render_sidebar()

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

def nombre_periodo(anio, mes):
    return f"{MESES.get(mes, mes)} {anio}"

def reset_estado():
    for key in ['paso','parse_result','val_result','val_resumen','periodos_info',
                'empresa_sel','archivo_nombre','periodos_reemplazar','empresa_detectada']:
        st.session_state.pop(key, None)

if 'paso' not in st.session_state:
    st.session_state.paso = 1

st.title("📤 Carga de Libro Diario")
st.caption("Subí el CSV mensual o multi-período, validalo y confirmá la carga.")
st.divider()
step_bar(st.session_state.paso)

# =============================================================================
# PASO 1 — Archivo
# =============================================================================
if st.session_state.paso == 1:
    st.subheader("Paso 1 · Seleccioná la empresa y el archivo")

    archivo = st.file_uploader("Archivo CSV del Libro Diario", type=["csv"],
                                help="Separador punto y coma (;), encoding latin-1/cp1252. "
                                     "Puede contener uno o varios meses.")

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
# PASO 2 — Preview
# =============================================================================
elif st.session_state.paso == 2:
    result  = st.session_state.parse_result
    empresa = st.session_state.empresa_sel
    df      = result.dataframe

    # Períodos detectados
    periodos = sorted(df[['periodo_anio','periodo_mes']].drop_duplicates().values.tolist())
    es_multiperiodo = len(periodos) > 1

    st.subheader("Paso 2 · Preview del archivo")

    # Métricas generales
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Empresa",        empresa)
    col2.metric("Períodos",       len(periodos))
    col3.metric("Filas válidas",  f"{result.total_filas_validas:,}")
    col4.metric("Filas raw",      f"{result.total_filas_raw:,}")

    # Badges de períodos detectados
    badges_html = "**Períodos detectados:** "
    for anio, mes in periodos:
        n = len(df[(df['periodo_anio']==anio) & (df['periodo_mes']==mes)])
        badges_html += f'<span class="periodo-badge">{nombre_periodo(anio, mes)} ({n:,} filas)</span> '
    st.markdown(badges_html, unsafe_allow_html=True)

    st.divider()

    # Resumen por período si hay más de uno
    if es_multiperiodo:
        st.markdown("**Resumen por período:**")
        resumen_rows = []
        for anio, mes in periodos:
            df_mes = df[(df['periodo_anio']==anio) & (df['periodo_mes']==mes)]
            resumen_rows.append({
                "Período":   nombre_periodo(anio, mes),
                "Filas":     len(df_mes),
                "Total Debe":  f"{df_mes['debe'].sum():,.2f}",
                "Total Haber": f"{df_mes['haber'].sum():,.2f}",
            })
        st.dataframe(pd.DataFrame(resumen_rows), use_container_width=True, hide_index=True)
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
                # Validar sobre el DataFrame completo (todos los períodos juntos)
                val_result = validator.validar(
                    df, df_raw, empresa,
                    periodos[0][0], periodos[0][1]   # anio/mes del primero (solo para log)
                )
                resumen = validator.resumen_texto(val_result)
            st.session_state.val_result  = val_result
            st.session_state.val_resumen = resumen
            st.session_state.paso = 3; st.rerun()

# =============================================================================
# PASO 3 — Validación
# =============================================================================
elif st.session_state.paso == 3:
    result  = st.session_state.parse_result
    val     = st.session_state.val_result
    resumen = st.session_state.val_resumen
    empresa = st.session_state.empresa_sel
    df      = result.dataframe
    periodos = sorted(df[['periodo_anio','periodo_mes']].drop_duplicates().values.tolist())

    st.subheader("Paso 3 · Resultado de validación")
    col1, col2, col3 = st.columns(3)
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
                staging = StagingService(conn)
                periodos_info = staging.verificar_periodos_df(df, empresa)
                st.session_state.periodos_info = periodos_info
                st.session_state.paso = 4; st.rerun()

# =============================================================================
# PASO 4 — Confirmación
# =============================================================================
elif st.session_state.paso == 4:
    result         = st.session_state.parse_result
    empresa        = st.session_state.empresa_sel
    periodos_info  = st.session_state.periodos_info   # List[PeriodoInfo]
    archivo_nombre = st.session_state.archivo_nombre
    df             = result.dataframe

    periodos_existentes = [p for p in periodos_info if p.existe]
    periodos_nuevos     = [p for p in periodos_info if not p.existe]

    st.subheader("Paso 4 · Confirmación")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Empresa",    empresa)
    col2.metric("Períodos",   len(periodos_info))
    col3.metric("Nuevos",     len(periodos_nuevos))
    col4.metric("Ya existen", len(periodos_existentes))
    st.divider()

    # Períodos nuevos — sin warning
    if periodos_nuevos:
        st.markdown("**✅ Períodos nuevos (se cargarán sin reemplazar nada):**")
        for p in periodos_nuevos:
            n_filas = len(df[(df['periodo_anio']==p.periodo_anio) & (df['periodo_mes']==p.periodo_mes)])
            st.markdown(
                f'<span class="periodo-badge">{nombre_periodo(p.periodo_anio, p.periodo_mes)} '
                f'— {n_filas:,} filas</span>', unsafe_allow_html=True
            )
        st.write("")

    # Períodos existentes — warning individual con checkbox
    confirmaciones = {}
    if periodos_existentes:
        st.markdown("**⚠️ Períodos que ya tienen datos — confirmá reemplazo individual:**")
        for p in periodos_existentes:
            n_filas = len(df[(df['periodo_anio']==p.periodo_anio) & (df['periodo_mes']==p.periodo_mes)])
            with st.container():
                st.markdown(f"""<div class="modal-warning">
                    <div style="font-size:1rem;font-weight:700;color:#c2410c">
                        ⚠️ {nombre_periodo(p.periodo_anio, p.periodo_mes)} ya tiene datos cargados
                    </div>
                    <div style="margin-top:8px;color:#374151;line-height:1.8;font-size:.85rem">
                        <b>Registros existentes:</b> {p.total_registros:,} &nbsp;|&nbsp;
                        <b>Cargado el:</b> {p.fecha_carga.strftime('%d/%m/%Y %H:%M') if p.fecha_carga else '—'}<br>
                        <b>Archivo original:</b> {p.archivo_origen or '—'}<br>
                        <b>Filas nuevas a cargar:</b> {n_filas:,}
                    </div>
                </div>""", unsafe_allow_html=True)
                confirmaciones[(p.periodo_anio, p.periodo_mes)] = st.checkbox(
                    f"Entiendo que los datos de {nombre_periodo(p.periodo_anio, p.periodo_mes)} "
                    f"serán eliminados y reemplazados.",
                    key=f"confirm_{p.periodo_anio}_{p.periodo_mes}"
                )

    st.divider()

    # Info del recálculo
    anio_desde = periodos_info[0].periodo_anio
    mes_desde  = periodos_info[0].periodo_mes
    st.info(
        f"📊 El Libro Mayor será recalculado desde **{nombre_periodo(anio_desde, mes_desde)}** "
        f"en adelante, respetando el saldo acumulado mes a mes."
    )

    col_back, col_proc = st.columns([1,5])
    with col_back:
        if st.button("← Volver"): st.session_state.paso = 3; st.rerun()
    with col_proc:
        # Botón habilitado solo si todos los períodos existentes fueron confirmados
        todos_confirmados = all(confirmaciones.get((p.periodo_anio, p.periodo_mes), False)
                                for p in periodos_existentes)
        puede_continuar   = len(periodos_existentes) == 0 or todos_confirmados

        label_btn = "✅ Confirmar carga →" if not periodos_existentes else "🔄 Confirmar y reemplazar →"
        if st.button(label_btn, type="primary", disabled=not puede_continuar):
            # Armar dict periodos_reemplazar
            periodos_reemplazar = {}
            for p in periodos_info:
                key = (p.periodo_anio, p.periodo_mes)
                periodos_reemplazar[key] = p.existe  # True si existe (confirmado), False si es nuevo
            st.session_state.periodos_reemplazar = periodos_reemplazar
            st.session_state.paso = 5; st.rerun()

# =============================================================================
# PASO 5 — Procesamiento
# =============================================================================
elif st.session_state.paso == 5:
    result              = st.session_state.parse_result
    empresa             = st.session_state.empresa_sel
    archivo_nombre      = st.session_state.archivo_nombre
    periodos_reemplazar = st.session_state.periodos_reemplazar
    df                  = result.dataframe

    periodos_ordenados = sorted(periodos_reemplazar.keys())
    primer_periodo     = periodos_ordenados[0]

    st.subheader("Paso 5 · Procesando...")

    with st.spinner(
        f"Cargando {len(periodos_ordenados)} período(s) y recalculando Libro Mayor "
        f"desde {nombre_periodo(*primer_periodo)}..."
    ):
        conn = get_conn()
        if conn is None: st.stop()
        staging = StagingService(conn)
        carga = staging.ejecutar_carga_multiperiodo(
            df=df,
            empresa=empresa,
            archivo_nombre=archivo_nombre,
            periodos_reemplazar=periodos_reemplazar,
        )

    st.divider()

    if carga.ok:
        accion_txt = {
            'carga_nueva': "Carga nueva",
            'reemplazo':   "Reemplazo",
            'mixto':       "Carga mixta (nuevos + reemplazos)",
        }.get(carga.accion, carga.accion)

        st.success(f"✅ {accion_txt} completada exitosamente.")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Registros cargados", f"{carga.registros_cargados:,}")
        col2.metric("Registros en Mayor", f"{carga.registros_mayor:,}")
        col3.metric("Períodos cargados",  len(carga.periodos_cargados))
        col4.metric("Tiempo",             f"{carga.duracion_ms/1000:.1f}s")

        # Detalle por período
        if len(carga.periodos_cargados) > 1:
            st.markdown("**Detalle por período:**")
            rows = []
            for anio, mes, n in carga.periodos_cargados:
                fue_reemplazo = (anio, mes) in [(a, m) for a, m in carga.periodos_reemplazados]
                rows.append({
                    "Período":  nombre_periodo(anio, mes),
                    "Registros": n,
                    "Acción":   "🔄 Reemplazo" if fue_reemplazo else "✅ Nuevo",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.info(
            f"📊 El Libro Mayor fue recalculado desde "
            f"**{nombre_periodo(*primer_periodo)}** en adelante, "
            f"con saldos acumulados correctos mes a mes."
        )
    else:
        st.error("❌ La carga falló.")
        for e in carga.errores: st.error(f"• {e}")

    st.divider()
    if st.button("📤 Cargar otro archivo", type="primary"):
        reset_estado(); st.rerun()