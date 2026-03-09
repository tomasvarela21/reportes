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
.cuenta-nueva-form{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;
    padding:16px;margin:8px 0}
""")
render_sidebar()

EMPRESAS = ["BATIA","GUARE","NORFORK","TORRES","WERCOLICH"]
MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

# Rubros y tipos del plan de cuentas
RUBROS_TIPOS = [
    ("Disponibilidades",                "Activo",     1),
    ("Inversiones",                     "Activo",     2),
    ("Créditos por Ventas",             "Activo",     3),
    ("Créditos Impositivos",            "Activo",     4),
    ("Otros Créditos",                  "Activo",     5),
    ("Bienes de Cambio",                "Activo",     6),
    ("Bienes de Uso",                   "Activo",     7),
    ("Deudas Comerciales",              "Pasivo",     8),
    ("Deudas Sociales",                 "Pasivo",     9),
    ("Deudas Fiscales",                 "Pasivo",    10),
    ("Deudas Financieras",              "Pasivo",    11),
    ("Otras Deudas",                    "Pasivo",    12),
    ("Patrimonio Neto",                 "Patrimonio",13),
    ("Resultado - Ingresos",            "Resultado", 14),
    ("Resultado - Ingresos Obra",       "Resultado", 15),
    ("Resultado - Ingresos Inmobiliaria","Resultado",16),
    ("Resultado - Otros Ingresos",      "Resultado", 17),
    ("Resultado - Gto Obra",            "Resultado", 18),
    ("Resultado - Gto Inmob",           "Resultado", 19),
    ("Resultado - Administ",            "Resultado", 20),
    ("Resultado - Comerciales",         "Resultado", 21),
    ("Resultado - Financiero",          "Resultado", 22),
]
RUBROS      = [r[0] for r in RUBROS_TIPOS]
RUBRO_TIPO  = {r[0]: r[1] for r in RUBROS_TIPOS}
RUBRO_ORDEN = {r[0]: r[2] for r in RUBROS_TIPOS}
MONEDAS     = {1: "Pesos ARS", 2: "Dólares USD", 3: "Euros EUR"}

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
                'empresa_sel','archivo_nombre','periodos_reemplazar','empresa_detectada',
                'cuentas_agregadas']:
        st.session_state.pop(key, None)

def agregar_cuenta_db(conn, codigo, nombre, rubro, tipo, es_resultado, moneda, orden_rubro) -> tuple:
    """Inserta una cuenta nueva en dim_cuenta. Retorna (ok, mensaje)."""
    try:
        cur = conn.cursor()
        # Verificar que no exista
        cur.execute("SELECT id FROM dim_cuenta WHERE codigo = %s", (codigo,))
        if cur.fetchone():
            cur.close()
            return False, f"La cuenta {codigo} ya existe en el plan de cuentas."
        cur.execute("""
            INSERT INTO dim_cuenta (codigo, nombre, rubro, tipo, es_resultado, moneda, orden_rubro, activa)
            VALUES (%s, %s, %s, %s, %s, %s, %s, true)
        """, (codigo, nombre, rubro, tipo, es_resultado, moneda, orden_rubro))
        conn.commit()
        cur.close()
        return True, f"Cuenta {codigo} — {nombre} agregada correctamente."
    except Exception as e:
        conn.rollback()
        return False, f"Error al agregar cuenta: {e}"

def revalidar(df, df_raw, empresa, periodos):
    """Re-ejecuta la validación y actualiza session_state."""
    conn = get_conn()
    if conn is None:
        return
    validator  = Validator(conn)
    val_result = validator.validar(df, df_raw, empresa, periodos[0][0], periodos[0][1])
    resumen    = validator.resumen_texto(val_result)
    st.session_state.val_result  = val_result
    st.session_state.val_resumen = resumen

if 'paso' not in st.session_state:
    st.session_state.paso = 1
if 'cuentas_agregadas' not in st.session_state:
    st.session_state.cuentas_agregadas = []

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
                                help="Separador punto y coma (;), encoding latin-1/cp1252.")

    empresa_sugerida = None
    if archivo is not None:
        empresa_sugerida = FileParser.detectar_empresa(archivo.name)
        if empresa_sugerida:
            st.markdown(
                f'<div class="empresa-detectada">💡 Empresa detectada del nombre del archivo: '
                f'<strong>{empresa_sugerida}</strong></div>', unsafe_allow_html=True)

    idx_empresa = EMPRESAS.index(empresa_sugerida) if empresa_sugerida in EMPRESAS else 0
    empresa = st.selectbox("Empresa", EMPRESAS, index=idx_empresa)

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
    periodos = sorted(df[['periodo_anio','periodo_mes']].drop_duplicates().values.tolist())

    st.subheader("Paso 2 · Preview del archivo")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Empresa",       empresa)
    col2.metric("Períodos",      len(periodos))
    col3.metric("Filas válidas", f"{result.total_filas_validas:,}")
    col4.metric("Filas raw",     f"{result.total_filas_raw:,}")

    badges_html = "**Períodos detectados:** "
    for anio, mes in periodos:
        n = len(df[(df['periodo_anio']==anio) & (df['periodo_mes']==mes)])
        badges_html += f'<span class="periodo-badge">{nombre_periodo(anio, mes)} ({n:,} filas)</span> '
    st.markdown(badges_html, unsafe_allow_html=True)
    st.divider()

    if len(periodos) > 1:
        st.markdown("**Resumen por período:**")
        rows = []
        for anio, mes in periodos:
            df_mes = df[(df['periodo_anio']==anio) & (df['periodo_mes']==mes)]
            rows.append({
                "Período":     nombre_periodo(anio, mes),
                "Filas":       len(df_mes),
                "Total Debe":  f"{df_mes['debe'].sum():,.2f}",
                "Total Haber": f"{df_mes['haber'].sum():,.2f}",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
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
                val_result = validator.validar(df, df_raw, empresa, periodos[0][0], periodos[0][1])
                resumen    = validator.resumen_texto(val_result)
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
    df_raw  = getattr(result, 'dataframe_raw', pd.DataFrame())
    periodos = sorted(df[['periodo_anio','periodo_mes']].drop_duplicates().values.tolist())

    st.subheader("Paso 3 · Resultado de validación")
    col1, col2, col3 = st.columns(3)
    col1.metric("Registros",   f"{resumen['total_registros']:,}")
    col2.metric("Total Debe",  f"{resumen['total_debe']:,.2f}")
    col3.metric("Total Haber", f"{resumen['total_haber']:,.2f}")
    st.divider()

    # Mostrar cuentas agregadas en esta sesión
    if st.session_state.cuentas_agregadas:
        st.success(
            f"✅ Cuentas agregadas al plan en esta sesión: "
            f"{', '.join(str(c) for c in st.session_state.cuentas_agregadas)}"
        )

    st.markdown("**Resultado por validación:**")
    for check in resumen['checks']:
        st.markdown(badge_check(check), unsafe_allow_html=True)

        # Expander especial para cuentas inexistentes con formulario de alta
        if check['nombre'] == 'Plan de cuentas' and not check['ok'] and val.cuentas_inexistentes:
            with st.expander(
                f"📋 Ver cuentas inexistentes ({len(val.cuentas_inexistentes)}) "
                f"— podés agregarlas al plan aquí"
            ):
                for cod in val.cuentas_inexistentes:
                    st.markdown(f"**Cuenta {cod}**")
                    form_key = f"form_cuenta_{cod}"

                    # Detectar filas del archivo que usan esta cuenta (para contexto)
                    filas_cuenta = df[df['cuenta_codigo'] == cod]
                    desc_ejemplo = None
                    if not filas_cuenta.empty:
                        # Tomar la descripción más frecuente como sugerencia de nombre
                        desc_ejemplo = (
                            filas_cuenta['descripcion'].dropna()
                            .value_counts().index[0]
                            if filas_cuenta['descripcion'].notna().any() else None
                        )
                        tiene_subcta = filas_cuenta['nro_subcuenta'].notna().any()
                        tiene_tipo   = filas_cuenta['tipo_subcuenta'].notna().any()
                        n_filas      = len(filas_cuenta)
                        st.caption(
                            f"Usada en {n_filas} fila(s) del archivo"
                            + (f" · con subcuenta" if tiene_subcta else "")
                            + (f" · con tipo subcuenta" if tiene_tipo else "")
                        )

                    with st.form(key=form_key):
                        st.markdown(f'<div class="cuenta-nueva-form">', unsafe_allow_html=True)

                        col_a, col_b = st.columns([1, 3])
                        with col_a:
                            codigo_input = st.number_input(
                                "Código *", value=int(cod), disabled=True,
                                key=f"cod_{cod}"
                            )
                        with col_b:
                            nombre_input = st.text_input(
                                "Nombre *",
                                value=desc_ejemplo[:60] if desc_ejemplo else "",
                                key=f"nom_{cod}",
                                placeholder="Nombre de la cuenta"
                            )

                        col_c, col_d = st.columns(2)
                        with col_c:
                            rubro_input = st.selectbox(
                                "Rubro *", RUBROS, key=f"rub_{cod}"
                            )
                        with col_d:
                            tipo_input = st.text_input(
                                "Tipo", value=RUBRO_TIPO.get(rubro_input, ""),
                                disabled=True, key=f"tip_{cod}"
                            )

                        col_e, col_f, col_g = st.columns(3)
                        with col_e:
                            es_resultado_input = st.checkbox(
                                "¿Es cuenta de resultado?",
                                value=RUBRO_TIPO.get(rubro_input, "") == "Resultado",
                                key=f"res_{cod}"
                            )
                        with col_f:
                            moneda_input = st.selectbox(
                                "Moneda",
                                options=list(MONEDAS.keys()),
                                format_func=lambda x: MONEDAS[x],
                                key=f"mon_{cod}"
                            )
                        with col_g:
                            tipo_subcta_input = st.number_input(
                                "Tipo subcuenta", value=0, min_value=0,
                                help="0 = sin subcuenta",
                                key=f"tsc_{cod}"
                            )

                        st.markdown('</div>', unsafe_allow_html=True)

                        submitted = st.form_submit_button(
                            f"➕ Agregar cuenta {cod} al plan de cuentas",
                            type="primary"
                        )

                        if submitted:
                            if not nombre_input.strip():
                                st.error("El nombre es obligatorio.")
                            else:
                                conn = get_conn()
                                if conn is None: st.stop()
                                ok, msg = agregar_cuenta_db(
                                    conn=conn,
                                    codigo=int(cod),
                                    nombre=nombre_input.strip(),
                                    rubro=rubro_input,
                                    tipo=RUBRO_TIPO[rubro_input],
                                    es_resultado=es_resultado_input,
                                    moneda=moneda_input,
                                    orden_rubro=RUBRO_ORDEN[rubro_input],
                                )
                                if ok:
                                    st.session_state.cuentas_agregadas.append(cod)
                                    # Re-validar para actualizar el check de plan de cuentas
                                    revalidar(df, df_raw, empresa, periodos)
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(msg)

                    st.divider()

        # Otros expanders existentes
        elif check['nombre'] == 'Tipos de montos' and not check['ok'] and val.tipos_problemas:
            with st.expander(f"📋 Ver valores no numéricos ({len(val.tipos_problemas)} filas)"):
                st.dataframe(pd.DataFrame(val.tipos_problemas), use_container_width=True, hide_index=True)

        elif check['nombre'] == 'Balance por asiento' and not check['ok'] and val.asientos_desbalanceados:
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
            st.error("Hay errores bloqueantes. Corregí el archivo o agregá las cuentas faltantes.")
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
    periodos_info  = st.session_state.periodos_info
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

    if periodos_nuevos:
        st.markdown("**✅ Períodos nuevos (se cargarán sin reemplazar nada):**")
        for p in periodos_nuevos:
            n_filas = len(df[(df['periodo_anio']==p.periodo_anio) & (df['periodo_mes']==p.periodo_mes)])
            st.markdown(
                f'<span class="periodo-badge">{nombre_periodo(p.periodo_anio, p.periodo_mes)} '
                f'— {n_filas:,} filas</span>', unsafe_allow_html=True)
        st.write("")

    confirmaciones = {}
    if periodos_existentes:
        st.markdown("**⚠️ Períodos que ya tienen datos — confirmá reemplazo individual:**")
        for p in periodos_existentes:
            n_filas = len(df[(df['periodo_anio']==p.periodo_anio) & (df['periodo_mes']==p.periodo_mes)])
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
    anio_desde = periodos_info[0].periodo_anio
    mes_desde  = periodos_info[0].periodo_mes
    st.info(
        f"📊 El Libro Mayor será recalculado desde **{nombre_periodo(anio_desde, mes_desde)}** "
        f"en adelante, respetando el saldo acumulado mes a mes."
    )

    # Mostrar cuentas agregadas en esta sesión si las hay
    if st.session_state.cuentas_agregadas:
        st.success(
            f"✅ Se agregaron {len(st.session_state.cuentas_agregadas)} cuenta(s) nuevas al plan: "
            f"{', '.join(str(c) for c in st.session_state.cuentas_agregadas)}"
        )

    col_back, col_proc = st.columns([1,5])
    with col_back:
        if st.button("← Volver"): st.session_state.paso = 3; st.rerun()
    with col_proc:
        todos_confirmados = all(
            confirmaciones.get((p.periodo_anio, p.periodo_mes), False)
            for p in periodos_existentes
        )
        puede_continuar = len(periodos_existentes) == 0 or todos_confirmados
        label_btn = "✅ Confirmar carga →" if not periodos_existentes else "🔄 Confirmar y reemplazar →"
        if st.button(label_btn, type="primary", disabled=not puede_continuar):
            periodos_reemplazar = {
                (p.periodo_anio, p.periodo_mes): p.existe
                for p in periodos_info
            }
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

        if len(carga.periodos_cargados) > 1:
            st.markdown("**Detalle por período:**")
            rows = []
            for anio, mes, n in carga.periodos_cargados:
                fue_reemplazo = (anio, mes) in carga.periodos_reemplazados
                rows.append({
                    "Período":   nombre_periodo(anio, mes),
                    "Registros": n,
                    "Acción":    "🔄 Reemplazo" if fue_reemplazo else "✅ Nuevo",
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