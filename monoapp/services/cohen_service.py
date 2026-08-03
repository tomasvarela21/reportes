"""
cohen_service.py
================
Sincronización del Libro Diario desde la API Cohen (ORDS - Oracle REST
Data Services) del sistema contable, para reemplazar la carga manual de CSV.

Usa el endpoint `cnt_movs` (detalle línea por línea de cada asiento) en
lugar de `libro_diario_operativo`, porque este último resultó devolver
una vista parcial/resumida que no balancea debe/haber — confirmado
comparando ambos para BATIA marzo/2025 (libro_diario_operativo quedó
desbalanceado en ~$180M/día; cnt_movs balanceó a centavos).

LIMITACIÓN CONOCIDA: el parámetro `p_codemp` no filtra por empresa con
las credenciales de test actuales — cualquier valor (incluso inválido)
devuelve siempre los datos de BATIA. Por eso, hasta que esto se resuelva
con quien administra Cohen, este servicio solo permite sincronizar BATIA
y rechaza explícitamente cualquier otro codemp para evitar cargar datos
de una empresa bajo el nombre de otra (como ocurrió en una prueba con
GUARE, que terminó recibiendo datos de BATIA).
"""

import calendar
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

from mayor_calculator import MayorCalculator

load_dotenv()

log = logging.getLogger(__name__)

COHEN_BASE_URL = os.getenv("COHEN_BASE_URL", "https://bollati.com.ar/ords")
COHEN_CLIENT_ID = os.getenv("COHEN_CLIENT_ID")
COHEN_CLIENT_SECRET = os.getenv("COHEN_CLIENT_SECRET")

# Código de empresa Cohen (p_codemp) → nombre de empresa.
# Solo BATIA está habilitada — ver limitación conocida en el docstring.
CODEMP_MAP = {
    1: "BATIA",
}
CODEMP_HABILITADOS = set(CODEMP_MAP.keys())

PAGE_SIZE = 1000


def get_cohen_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        f"{COHEN_BASE_URL}/cohen/oauth/token",
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"No se pudo obtener el token de Cohen (HTTP {resp.status_code}): {resp.text[:300]}"
        )
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("La respuesta de Cohen no incluyó access_token.")
    log.info("Token Cohen obtenido OK")
    return token


def fetch_movimientos(token: str, codemp: int, anio: int, mes: int) -> list:
    if codemp not in CODEMP_HABILITADOS:
        raise ValueError(
            f"codemp={codemp} no está habilitado. Por ahora solo se soporta BATIA (codemp=1), "
            f"porque p_codemp no filtra correctamente con las credenciales de test actuales."
        )

    ultimo_dia = calendar.monthrange(anio, mes)[1]
    desde = f"01/{mes:02d}/{anio}"
    hasta = f"{ultimo_dia:02d}/{mes:02d}/{anio}"

    url = f"{COHEN_BASE_URL}/cohen/cohen/cnt_movs/"
    headers = {"Authorization": f"Bearer {token}"}

    filas = []
    offset = 0
    pagina = 0
    while True:
        resp = requests.get(
            url,
            headers=headers,
            params={"p_codemp": codemp, "desde": desde, "hasta": hasta, "limit": PAGE_SIZE, "offset": offset},
            timeout=60,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Error consultando cnt_movs (HTTP {resp.status_code}): {resp.text[:300]}"
            )
        data = resp.json()
        items = data.get("items", [])
        filas.extend(items)
        pagina += 1
        log.info(f"  Página {pagina} — {len(items)} filas (offset={offset})")

        if not data.get("hasMore"):
            break
        offset += PAGE_SIZE

    log.info(f"Descarga completa — {len(filas)} filas — codemp={codemp} periodo={anio}/{mes:02d}")
    return filas


def fetch_pdc(token: str) -> dict:
    """Descarga el Plan de Cuentas completo de Cohen: {nro_cta: descripcion}."""
    url = f"{COHEN_BASE_URL}/cohen/cohen/pdc/"
    headers = {"Authorization": f"Bearer {token}"}

    items = []
    offset = 0
    while True:
        resp = requests.get(url, headers=headers, params={"limit": PAGE_SIZE, "offset": offset}, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Error consultando pdc (HTTP {resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        items.extend(data.get("items", []))
        if not data.get("hasMore"):
            break
        offset += PAGE_SIZE

    return {i["nrocta"]: i["descrip"] for i in items if i.get("nrocta") is not None}


def verificar_cuentas(conn, filas: list) -> list:
    """Devuelve los cuenta_codigo de `filas` que no existen en dim_cuenta."""
    codigos = {f["cuenta_codigo"] for f in filas}
    if not codigos:
        return []
    cur = conn.cursor()
    try:
        cur.execute("SELECT nro_cta FROM dim_cuenta WHERE nro_cta = ANY(%s)", (list(codigos),))
        existentes = {r[0] for r in cur.fetchall()}
    finally:
        cur.close()
    return sorted(codigos - existentes)


def _limpiar(valor):
    if valor in (None, "", 0, "0"):
        return None
    return str(valor)


def mapear_filas(rows: list, empresa_id: int) -> list:
    ahora = datetime.now(timezone.utc)
    mapeadas = []
    for r in rows:
        fecasi = datetime.fromisoformat(r["fecasi"].replace("Z", "+00:00"))

        mapeadas.append({
            "empresa_id":     empresa_id,
            "fecha":          fecasi.date(),
            "periodo_anio":   fecasi.year,
            "periodo_mes":    fecasi.month,
            "tipo_asiento":   str(r["tipasi"]) if r.get("tipasi") is not None else None,
            "nro_asiento":    str(r["nroasi"]) if r.get("nroasi") is not None else None,
            "nro_renglon":    str(r["nroreng"]) if r.get("nroreng") is not None else None,
            "cuenta_codigo":  int(r["nrocta"]),
            "debe":           float(r["debe"]) if r.get("debe") is not None else 0.0,
            "haber":          float(r["haber"]) if r.get("haber") is not None else 0.0,
            "descripcion":    r.get("descrip"),
            "tipo_subcuenta": _limpiar(r.get("tipsub")),
            "nro_subcuenta":  _limpiar(r.get("nrosub")),
            "centro_costo":   _limpiar(r.get("ccosto")),
            # cnt_movs no trae fecha de carga original (feccar) como
            # libro_diario_operativo; se usa el momento de la sincronización.
            "cargado_en":     ahora,
        })
    return mapeadas


def _bulk_insert(cur, filas: list, archivo_origen: str) -> int:
    registros = [(
        f["empresa_id"], f["fecha"], f["periodo_anio"], f["periodo_mes"],
        f["tipo_asiento"], f["nro_asiento"], f["nro_renglon"], f["cuenta_codigo"],
        f["debe"], f["haber"], f["descripcion"], f["tipo_subcuenta"],
        f["nro_subcuenta"], f["centro_costo"], f["cargado_en"], archivo_origen,
    ) for f in filas]

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO libro_diario (
            empresa_id, fecha, periodo_anio, periodo_mes,
            tipo_asiento, nro_asiento, nro_renglon,
            cuenta_codigo, debe, haber,
            descripcion, tipo_subcuenta, nro_subcuenta, centro_costo,
            cargado_en, archivo_origen
        ) VALUES %s
        """,
        registros,
        page_size=1000,
    )
    return len(registros)


def sincronizar_periodo(conn, empresa_id: int, codemp: int, anio: int, mes: int, token: str) -> dict:
    if codemp not in CODEMP_HABILITADOS:
        raise ValueError(
            f"codemp={codemp} no está habilitado. Por ahora solo se soporta BATIA (codemp=1)."
        )

    inicio = time.time()
    archivo_origen = f"cohen_api_{codemp}_{anio}{mes:02d}"

    log.info(f"▶ Sincronizando Cohen — codemp={codemp} empresa_id={empresa_id} periodo={anio}/{mes:02d}")
    rows = fetch_movimientos(token, codemp, anio, mes)
    filas = mapear_filas(rows, empresa_id)

    cuentas_faltantes = verificar_cuentas(conn, filas)
    if cuentas_faltantes:
        pdc = fetch_pdc(token)
        detalle = ", ".join(f"{c} ({pdc.get(c, 'sin descripción en PDC')})" for c in cuentas_faltantes)
        raise ValueError(
            f"{len(cuentas_faltantes)} cuenta(s) no registrada(s) en el plan de cuentas: {detalle}. "
            f"Registralas en Administración antes de sincronizar este período."
        )

    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM libro_diario
            WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s
        """, (empresa_id, anio, mes))

        registros_cargados = _bulk_insert(cur, filas, archivo_origen) if filas else 0

        conn.commit()
        log.info(f"  ✅ {registros_cargados} registros insertados en libro_diario")

        calc = MayorCalculator(conn)
        registros_mayor = calc.recalcular(
            empresa_id=empresa_id,
            desde_anio=anio,
            desde_mes=mes,
            motivo=f"sync_cohen_api_{codemp}",
        )

        duracion_ms = int((time.time() - inicio) * 1000)
        return {
            "registros_cargados": registros_cargados,
            "registros_mayor":    registros_mayor,
            "duracion_ms":        duracion_ms,
            "periodo":            f"{anio}/{mes:02d}",
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
