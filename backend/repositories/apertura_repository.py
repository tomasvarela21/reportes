"""
Acceso a datos para saldos_apertura.
Solo contiene SQL — sin lógica de negocio ni excepciones HTTP.
"""
import logging
from datetime import datetime

from psycopg2.extras import RealDictCursor

from core.db_utils import execute_batch

log = logging.getLogger(__name__)

_SELECT_APERTURA = """
    SELECT
        sa.empresa_id,
        de.empresa_nombre,
        sa.anio_fiscal,
        sa.cuenta_codigo,
        dc.nombre   AS cuenta_nombre,
        sa.tipo_subcuenta,
        sa.nro_subcuenta,
        sa.centro_costo,
        sa.saldo,
        sa.cargado_en,
        sa.archivo_origen
    FROM saldos_apertura sa
    LEFT JOIN dim_cuenta  dc ON dc.nro_cta    = sa.cuenta_codigo
    LEFT JOIN dim_empresa de ON de.empresa_id = sa.empresa_id
"""


def _build_where(empresa_id: int, filtros: dict) -> tuple[str, list]:
    conditions = ["sa.empresa_id = %s"]
    params: list = [empresa_id]

    if filtros.get("anio_fiscal") is not None:
        conditions.append("sa.anio_fiscal = %s")
        params.append(filtros["anio_fiscal"])
    if filtros.get("cuenta_codigo") is not None:
        conditions.append("sa.cuenta_codigo = %s")
        params.append(filtros["cuenta_codigo"])
    if filtros.get("centro_costo") is not None:
        conditions.append("sa.centro_costo = %s")
        params.append(filtros["centro_costo"])
    if filtros.get("solo_saldo_nonzero"):
        conditions.append("sa.saldo != 0")

    return f"WHERE {' AND '.join(conditions)}", params


def find_by_empresa(conn, empresa_id: int, filtros: dict) -> list[dict]:
    """Filas de saldos_apertura con JOIN dim_cuenta y dim_empresa."""
    where, params = _build_where(empresa_id, filtros)
    limit  = filtros.get("limit", 100)
    offset = filtros.get("offset", 0)
    sql = (
        f"{_SELECT_APERTURA} {where} "
        f"ORDER BY sa.anio_fiscal DESC, sa.cuenta_codigo "
        f"LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def count_by_empresa(conn, empresa_id: int, filtros: dict) -> int:
    where, params = _build_where(empresa_id, filtros)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM saldos_apertura sa {where}", params
        )
        return cur.fetchone()[0]


def find_stats(conn, empresa_id: int, anio_fiscal: int) -> dict:
    """Estadísticas de verificación para empresa + año fiscal."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                                                  AS total_registros,
                COUNT(DISTINCT cuenta_codigo)                             AS cuentas_unicas,
                COALESCE(ROUND(SUM(saldo)::numeric, 2), 0)               AS suma_saldo,
                COUNT(CASE WHEN saldo = 0     THEN 1 END)                AS cuentas_con_saldo_cero,
                COUNT(CASE WHEN tipo_subcuenta IS NOT NULL THEN 1 END)   AS con_subcuenta,
                COUNT(CASE WHEN centro_costo   IS NOT NULL THEN 1 END)   AS con_ccosto
            FROM saldos_apertura
            WHERE empresa_id = %s AND anio_fiscal = %s
            """,
            (empresa_id, anio_fiscal),
        )
        return dict(cur.fetchone())


def delete_periodo(conn, empresa_id: int, anio_fiscal: int) -> int:
    """DELETE saldos de una empresa para un año fiscal. Retorna filas eliminadas."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM saldos_apertura WHERE empresa_id = %s AND anio_fiscal = %s",
            (empresa_id, anio_fiscal),
        )
        return cur.rowcount


def insert_batch(conn, rows: list[tuple]) -> int:
    """
    INSERT batch en saldos_apertura.
    Cada tupla: (empresa_id, anio_fiscal, cuenta_codigo, tipo_subcuenta,
                 nro_subcuenta, centro_costo, saldo, cargado_en, archivo_origen)
    No hace commit — el service es responsable.
    """
    sql = """
        INSERT INTO saldos_apertura
            (empresa_id, anio_fiscal, cuenta_codigo,
             tipo_subcuenta, nro_subcuenta, centro_costo,
             saldo, cargado_en, archivo_origen)
        VALUES %s
    """
    return execute_batch(conn, sql, rows, page_size=500)
