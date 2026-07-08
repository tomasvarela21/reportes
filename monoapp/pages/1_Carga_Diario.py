import os, sys, streamlit as st, pandas as pd
from dotenv import load_dotenv
from services.db import get_conn
from services.styles import apply_styles, render_sidebar
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'services'))

st.set_page_config(page_title="Carga Diario · ReporteApp", page_icon="📤", layout="wide")
apply_styles()
render_sidebar()

from file_parser import FileParser, EMPRESAS
from validator import Validator
from staging_service import StagingService

MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

def reset_estado():
    for k in ['parse_result','periodos_info','decisiones','centros_agregados']:
        st.session_state.pop(k, None)

def reset_completo():
    for k in ['parse_result','periodos_info','decisiones','centros_agregados',
              'carga_exitosa','empresa_sugerida']:
        st.session_state.pop(k, None)

def agregar_cuenta_db(conn, nro_cta, extendido, nombre, rubro, tipo, moneda):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_cuenta (nro_cta, extendido, nombre, rubro, tipo, moneda)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (nro_cta) DO NOTHING
    """, (nro_cta, extendido, nombre, rubro, tipo, moneda))
    conn.commit(); cur.close()

def agregar_centro_db(conn, codigo, descripcion, empresa_id=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_centro_costo (codigo, descripcion, empresa_id)
        VALUES (%s,%s,%s)
        ON CONFLICT (codigo) DO NOTHING
    """, (codigo, descripcion, empresa_id))
    conn.commit(); cur.close()

st.title("📤 Carga de Libro Diario")
st.caption("Cargá el CSV del diario contable. Se valida, procesa y recalcula el Mayor automáticamente.")
st.divider()

conn = get_conn()
if conn is None: st.stop()

# ── Pantalla de éxito — se evalúa PRIMERO para cortar el flujo ─────────────────
if 'carga_exitosa' in st.session_state:
    r = st.session_state['carga_exitosa']
    anio_desde, mes_desde, _ = r['periodos'][0]
    anio_hasta, mes_hasta, _ = r['periodos'][-1]
    desde_label = f"{MESES[mes_desde]} {anio_desde}"
    hasta_label = f"{MESES[mes_hasta]} {anio_hasta}"

    st.success(f"✅ Carga completada para **{r['empresa']}**")
    st.divider()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Períodos cargados",   len(r['periodos']))
    c2.metric("Registros en diario", f"{r['registros_cargados']:,}")
    c3.metric("Registros en mayor",  f"{r['registros_mayor']:,}")
    c4.metric("Tiempo total",        f"{r['duracion_ms']:,} ms")

    st.divider()
    with st.expander("📋 Detalle por período", expanded=True):
        for anio, mes, n in r['periodos']:
            st.write(f"  • {MESES[mes]} {anio}: {n:,} registros")

    st.info(
        f"📊 **Mayor recalculado** desde **{desde_label}** hasta **{hasta_label}** "
        f"— {r['registros_mayor']:,} registros generados en `libro_mayor`."
    )
    st.divider()
    if st.button("📤 Cargar otro archivo", type="primary"):
        reset_completo()
        st.rerun()
    st.stop()

# ── Paso 1: Selección de empresa y archivo ─────────────────────────────────────
st.subheader("Paso 1 — Seleccioná la empresa y el archivo")
c1, c2 = st.columns([1,3])

empresas_lista = list(EMPRESAS.keys())
idx_default = empresas_lista.index(st.session_state['empresa_sugerida']) \
    if st.session_state.get('empresa_sugerida') in empresas_lista else 0

empresa_nombre = c1.selectbox("Empresa", empresas_lista, index=idx_default, on_change=reset_estado)
empresa_id = EMPRESAS[empresa_nombre]

archivo = c2.file_uploader(
    "Archivo CSV del libro diario",
    type=["csv"],
    on_change=reset_estado,
    help="Formatos soportados: 22 cols posicional o 17 cols con id_empresa"
)

# Detectar empresa del nombre del archivo apenas se sube
if archivo and 'empresa_sugerida' not in st.session_state:
    detectada = FileParser.detectar_empresa(archivo.name)
    if detectada and detectada != empresa_nombre:
        st.session_state['empresa_sugerida'] = detectada
        st.rerun()

if not archivo:
    st.info("👆 Seleccioná una empresa y subí el archivo CSV para continuar.")
    st.stop()

# ── Paso 2: Parseo y validación ────────────────────────────────────────────────
if 'parse_result' not in st.session_state:
    with st.spinner("Analizando archivo..."):
        parser = FileParser()
        result = parser.parsear(archivo, empresa_nombre)

        if result.empresa_detectada and result.empresa_detectada != empresa_nombre:
            st.warning(
                f"⚠️ El nombre del archivo sugiere **{result.empresa_detectada}** "
                f"pero seleccionaste **{empresa_nombre}**. Verificá antes de continuar."
            )

        validator = Validator(conn)
        errores_val, advertencias_val = validator.validar(result.dataframe, empresa_id) if result.ok else ([], [])
        result.errores      += errores_val
        result.advertencias += advertencias_val
        result.ok = len(result.errores) == 0

        if result.ok:
            svc = StagingService(conn)
            periodos_info = svc.verificar_periodos_df(result.dataframe, empresa_id)
        else:
            periodos_info = []

        st.session_state['parse_result']  = result
        st.session_state['periodos_info'] = periodos_info

result        = st.session_state['parse_result']
periodos_info = st.session_state.get('periodos_info', [])

st.subheader("Paso 2 — Resultado del análisis")

if result.advertencias:
    for adv in result.advertencias:
        st.warning(f"⚠️ {adv}")

if not result.ok:
    for err in result.errores:
        if isinstance(err, dict) and err.get('__tipo__') == 'descuadre':
            # ── Resumen del error ──────────────────────────────────────────
            st.error(f"❌ {err['resumen']}")

            for asiento in err['asientos']:
                with st.container(border=True):
                    # Encabezado del asiento
                    ca, cb, cc, cd = st.columns(4)
                    ca.markdown(f"**Asiento**  \n{asiento['nro_asiento']}")
                    cb.markdown(f"**Tipo**  \n{asiento['tipo']}")
                    cc.markdown(f"**Fecha**  \n{asiento['fecha']}")
                    cd.markdown(f"**Diferencia**  \n:red[{asiento['diff']:+,.2f}]")

                    st.divider()

                    # Tabla de renglones
                    df_reng = pd.DataFrame(asiento['renglones'])
                    total_debe  = df_reng['Debe'].sum()
                    total_haber = df_reng['Haber'].sum()
                    diff        = round(total_debe + total_haber, 2)

                    # Fila de totales
                    fila_total = pd.DataFrame([{
                        'Renglón': 'TOTAL',
                        'Cuenta':  '',
                        'Debe':    total_debe,
                        'Haber':   total_haber,
                    }])
                    df_display = pd.concat([df_reng, fila_total], ignore_index=True)

                    # Formatear montos
                    for col in ['Debe', 'Haber']:
                        df_display[col] = df_display[col].apply(
                            lambda x: f"{x:+,.2f}" if isinstance(x, (int, float)) else x
                        )

                    st.dataframe(df_display, use_container_width=True, hide_index=True)

                    # Fila de diferencia destacada
                    color = "red" if abs(diff) > 0.01 else "green"
                    st.markdown(
                        f":{color}[**Diferencia: {diff:+,.2f}**] — "
                        f"Total Debe: `{total_debe:+,.2f}` | "
                        f"Total Haber: `{total_haber:+,.2f}`"
                    )
        else:
            st.error(f"❌ {err}")

    if st.button("🔄 Reintentar con otro archivo"):
        reset_estado(); st.rerun()
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Filas válidas",   f"{result.total_filas_validas:,}")
c2.metric("Total Debe",      f"{result.dataframe['debe'].sum():,.2f}")
c3.metric("Total Haber",     f"{result.dataframe['haber'].sum():,.2f}")

# ── Paso 3: Centros de costo no registrados ────────────────────────────────────
centros_faltantes = []
if result.advertencias:
    for adv in result.advertencias:
        if 'centro' in adv.lower() and 'no registrados' in adv.lower():
            import re
            match = re.search(r'\[(.+)\]', adv)
            if match:
                centros_faltantes = [c.strip().strip("'") for c in match.group(1).split(',')]

if centros_faltantes:
    st.divider()
    st.subheader("Paso 3 — Centros de costo no registrados")
    st.info(f"Los siguientes {len(centros_faltantes)} centros no están en la base. Podés agregarlos ahora:")
    centros_agregados = st.session_state.get('centros_agregados', set())
    for cc in centros_faltantes:
        if cc in centros_agregados:
            st.success(f"✅ Centro **{cc}** ya agregado")
            continue
        with st.expander(f"➕ Agregar centro: **{cc}**", expanded=False):
            col1, col2, col3 = st.columns(3)
            cod_val  = col1.text_input("Código", value=cc, disabled=True, key=f"cc_cod_{cc}")
            desc_val = col2.text_input("Descripción", key=f"cc_desc_{cc}")
            emp_val  = col3.selectbox("Empresa (opcional)", ["—"] + list(EMPRESAS.keys()), key=f"cc_emp_{cc}")
            if st.button(f"Guardar centro '{cc}'", key=f"btn_cc_{cc}"):
                emp_id_cc = EMPRESAS[emp_val] if emp_val != "—" else None
                agregar_centro_db(conn, cc, desc_val or cc, emp_id_cc)
                centros_agregados.add(cc)
                st.session_state['centros_agregados'] = centros_agregados
                st.rerun()

# ── Paso 4: Decisión por período ───────────────────────────────────────────────
st.divider()
paso_num = 4 if centros_faltantes else 3
st.subheader(f"Paso {paso_num} — Decisión por período")

periodos_existentes = [info for info in periodos_info if info.existe]
decisiones = {}

if periodos_existentes:
    reemplazar_todos = st.checkbox(
        f"✅ Reemplazar todos los períodos existentes ({len(periodos_existentes)})",
        value=False,
        key="reemplazar_todos"
    )
    st.divider()
else:
    reemplazar_todos = False

for info in periodos_info:
    key   = (info.periodo_anio, info.periodo_mes)
    label = f"{MESES[info.periodo_mes]} {info.periodo_anio}"
    if info.existe:
        st.warning(
            f"⚠️ **{label}** ya tiene {info.total_registros:,} registros "
            f"(cargado el {info.fecha_carga.strftime('%d/%m/%Y') if info.fecha_carga else '—'} "
            f"desde `{info.archivo_origen or '—'}`)"
        )
        decisiones[key] = st.checkbox(
            f"Reemplazar {label}",
            value=reemplazar_todos,
            key=f"reemplazar_{key}"
        )
    else:
        st.success(f"✅ **{label}** — período nuevo, se cargará sin conflictos.")
        decisiones[key] = True

periodos_bloqueados  = [k for k, v in decisiones.items() if not v]
periodos_reemplazar  = {k: v for k, v in decisiones.items() if v}

# ── Paso 5: Cuentas no registradas ────────────────────────────────────────────
cuentas_invalidas = []
for err in result.errores:
    if 'no existen en el plan' in err:
        import re
        match = re.search(r'\[(.+)\]', err)
        if match:
            cuentas_invalidas = [int(c.strip()) for c in match.group(1).split(',')]

if cuentas_invalidas:
    st.divider()
    paso_cta = paso_num + 1
    st.subheader(f"Paso {paso_cta} — Cuentas no registradas en el plan")
    st.warning(f"Las siguientes cuentas no están en `dim_cuenta`: {cuentas_invalidas}")
    for cta in cuentas_invalidas:
        with st.expander(f"➕ Agregar cuenta {cta}", expanded=False):
            cc1, cc2, cc3 = st.columns(3)
            ext_v = cc1.text_input("Extendido", key=f"cta_ext_{cta}")
            nom_v = cc2.text_input("Nombre",    key=f"cta_nom_{cta}")
            rub_v = cc3.text_input("Rubro",     key=f"cta_rub_{cta}")
            cc4, cc5 = st.columns(2)
            tip_v = cc4.selectbox("Tipo", ["Activo","Pasivo","Patrimonio","Resultado"], key=f"cta_tip_{cta}")
            mon_v = cc5.selectbox("Moneda", ["ARS","USD","EUR"], key=f"cta_mon_{cta}")
            if st.button(f"Guardar cuenta {cta}", key=f"btn_cta_{cta}"):
                agregar_cuenta_db(conn, cta, ext_v, nom_v, rub_v, tip_v, mon_v)
                validator2 = Validator(conn)
                errs2, advs2 = validator2.validar(result.dataframe, empresa_id)
                result.errores      = [e for e in result.errores if 'no existen en el plan' not in e] + errs2
                result.advertencias = advs2
                result.ok = len(result.errores) == 0
                st.session_state['parse_result'] = result
                st.success(f"✅ Cuenta {cta} agregada.")
                st.rerun()

# ── Botón de carga final ───────────────────────────────────────────────────────
st.divider()
hay_errores_pendientes = not result.ok and bool(cuentas_invalidas)

if hay_errores_pendientes:
    st.error("❌ Hay cuentas inválidas pendientes de resolver antes de poder cargar.")
elif periodos_bloqueados:
    st.warning(f"⚠️ {len(periodos_bloqueados)} período(s) no serán cargados por no estar marcados para reemplazar.")

if periodos_reemplazar and not hay_errores_pendientes:
    if st.button(f"📥 Cargar {len(periodos_reemplazar)} período(s)", type="primary"):
        svc = StagingService(conn)
        with st.spinner("Cargando datos y recalculando Mayor..."):
            resultado = svc.ejecutar_carga_multiperiodo(
                df=result.dataframe,
                empresa_id=empresa_id,
                archivo_nombre=archivo.name,
                periodos_reemplazar=periodos_reemplazar,
            )
        if resultado.ok:
            # Guardar resultado y reiniciar — NO limpiar carga_exitosa
            for k in ['parse_result','periodos_info','decisiones','centros_agregados']:
                st.session_state.pop(k, None)
            st.session_state['carga_exitosa'] = {
                'registros_cargados': resultado.registros_cargados,
                'registros_mayor':    resultado.registros_mayor,
                'duracion_ms':        resultado.duracion_ms,
                'periodos':           resultado.periodos_cargados,
                'empresa':            empresa_nombre,
            }
            st.rerun()
        else:
            for err in resultado.errores:
                st.error(f"❌ {err}")