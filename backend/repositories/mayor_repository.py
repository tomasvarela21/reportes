"""
Acceso a datos para libro_mayor y mayor_recalculo_log.
Solo contiene SQL — sin lógica de negocio ni excepciones HTTP.
"""
import logging

from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)


def find_by_empresa(conn, empresa_id: int, filtros: dict) -> list[dict]:
    """
    Retorna filas del libro_mayor.
    filtros: anio, mes, nivel, cuenta_codigo, centro_costo, limit, offset.
    """
    conditions = ["empresa_id = %s"]
    params: list = [empresa_id]

    if filtros.get("anio") is not None:
        conditions.append("periodo_anio = %s")
        params.append(filtros["anio"])
    if filtros.get("mes") is not None:
        conditions.append("periodo_mes = %s")
        params.append(filtros["mes"])
    if filtros.get("nivel") is not None:
        conditions.append("nivel = %s")
        params.append(filtros["nivel"])
    if filtros.get("cuenta_codigo") is not None:
        conditions.append("cuenta_codigo = %s")
        params.append(filtros["cuenta_codigo"])
    if filtros.get("centro_costo") is not None:
        conditions.append("centro_costo = %s")
        params.append(filtros["centro_costo"])

    where  = " AND ".join(conditions)
    limit  = filtros.get("limit", 1000)
    offset = filtros.get("offset", 0)
    params.extend([limit, offset])

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                id,
                periodo_anio, periodo_mes, fecha_periodo,
                nivel,
                cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo,
                total_debe, total_haber,
                saldo_anterior, saldo_periodo, saldo_acumulado
            FROM libro_mayor
            WHERE {where}
            ORDER BY periodo_anio, periodo_mes, cuenta_codigo, nivel
            LIMIT %s OFFSET %s
            """,
            params,
        )
        return cur.fetchall()


def find_periodos(conn, empresa_id: int) -> list[dict]:
    """DISTINCT (periodo_anio, periodo_mes) en libro_mayor para la empresa."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT periodo_anio, periodo_mes
            FROM libro_mayor
            WHERE empresa_id = %s
            ORDER BY periodo_anio, periodo_mes
            """,
            (empresa_id,),
        )
        return cur.fetchall()


def find_recalculos(conn, limit: int = 100) -> list[dict]:
    """Últimas N entradas del log de recálculos, con JOIN a dim_empresa."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                de.empresa_nombre,
                mrl.desde_anio,
                mrl.desde_mes,
                mrl.hasta_anio,
                mrl.hasta_mes,
                mrl.motivo,
                mrl.registros_afectados,
                mrl.duracion_ms,
                mrl.ejecutado_en
            FROM mayor_recalculo_log mrl
            LEFT JOIN dim_empresa de ON de.empresa_id = mrl.empresa_id
            ORDER BY mrl.ejecutado_en DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def find_resumen(conn, empresa_id: int, filtros: dict) -> list[dict]:
    """
    Totales por cuenta (nivel='cuenta') con nombre y rubro de dim_cuenta.
    filtros: anio, mes.
    """
    conditions = ["lm.empresa_id = %s", "lm.nivel = 'cuenta'"]
    params: list = [empresa_id]

    if filtros.get("anio") is not None:
        conditions.append("lm.periodo_anio = %s")
        params.append(filtros["anio"])
    if filtros.get("mes") is not None:
        conditions.append("lm.periodo_mes = %s")
        params.append(filtros["mes"])

    where = " AND ".join(conditions)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                lm.cuenta_codigo,
                dc.nombre,
                dc.rubro,
                dc.tipo,
                SUM(lm.total_debe)      AS total_debe,
                SUM(lm.total_haber)     AS total_haber,
                SUM(lm.saldo_acumulado) AS saldo_acumulado
            FROM libro_mayor lm
            LEFT JOIN dim_cuenta dc ON dc.nro_cta = lm.cuenta_codigo
            WHERE {where}
            GROUP BY lm.cuenta_codigo, dc.nombre, dc.rubro, dc.tipo
            ORDER BY lm.cuenta_codigo
            """,
            params,
        )
        return cur.fetchall()
