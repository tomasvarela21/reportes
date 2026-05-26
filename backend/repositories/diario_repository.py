"""
Acceso a datos para libro_diario.
Solo contiene SQL — sin lógica de negocio ni excepciones HTTP.
"""
import logging

from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

# Columnas de libro_diario que se devuelven en find_by_empresa (incluye JOIN dim_cuenta)
_SELECT_DIARIO = """
    SELECT
        ld.id,
        ld.empresa_id,
        ld.fecha,
        ld.periodo_anio,
        ld.periodo_mes,
        ld.tipo_asiento,
        ld.nro_asiento,
        ld.nro_renglon,
        ld.cuenta_codigo,
        dc.nombre        AS cuenta_nombre,
        dc.rubro         AS cuenta_rubro,
        ld.tipo_subcuenta,
        ld.nro_subcuenta,
        ld.centro_costo,
        ld.debe,
        ld.haber,
        ld.descripcion,
        ld.archivo_origen
    FROM libro_diario ld
    LEFT JOIN dim_cuenta dc ON dc.nro_cta = ld.cuenta_codigo
"""


def _build_where(empresa_id: int, filtros: dict) -> tuple[str, list]:
    """Construye cláusula WHERE y lista de parámetros para queries de diario."""
    conditions = ["ld.empresa_id = %s"]
    params: list = [empresa_id]

    if filtros.get("anio") is not None:
        conditions.append("ld.periodo_anio = %s")
        params.append(filtros["anio"])
    if filtros.get("mes") is not None:
        conditions.append("ld.periodo_mes = %s")
        params.append(filtros["mes"])
    if filtros.get("cuenta_codigo") is not None:
        conditions.append("ld.cuenta_codigo = %s")
        params.append(filtros["cuenta_codigo"])
    if filtros.get("centro_costo") is not None:
        conditions.append("ld.centro_costo = %s")
        params.append(filtros["centro_costo"])
    if filtros.get("descripcion"):
        conditions.append("ld.descripcion ILIKE %s")
        params.append(f"%{filtros['descripcion']}%")

    return f"WHERE {' AND '.join(conditions)}", params


def find_by_empresa(conn, empresa_id: int, filtros: dict) -> list[dict]:
    """
    Retorna filas del libro_diario con JOIN a dim_cuenta.
    filtros: anio, mes, cuenta_codigo, centro_costo, descripcion, limit, offset.
    """
    where, params = _build_where(empresa_id, filtros)
    limit  = filtros.get("limit", 100)
    offset = filtros.get("offset", 0)
    sql = (
        f"{_SELECT_DIARIO} {where} "
        f"ORDER BY ld.fecha, ld.nro_asiento, ld.nro_renglon "
        f"LIMIT %s OFFSET %s"
    )
    params.extend([limit, offset])

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def count_by_empresa(conn, empresa_id: int, filtros: dict) -> int:
    """Cuenta las filas que matchean los filtros (sin paginación)."""
    where, params = _build_where(empresa_id, filtros)
    sql = f"SELECT COUNT(*) FROM libro_diario ld {where}"

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def find_periodos(conn, empresa_id: int) -> list[dict]:
    """DISTINCT (periodo_anio, periodo_mes) en libro_diario para la empresa."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT periodo_anio, periodo_mes
            FROM libro_diario
            WHERE empresa_id = %s
            ORDER BY periodo_anio, periodo_mes
            """,
            (empresa_id,),
        )
        return cur.fetchall()


def find_resumen_periodos(conn, empresa_id: int) -> list[dict]:
    """
    Agrupado por período: total_registros, debe, haber, ultima_carga, archivo_origen.
    Equivale a la query de listar_periodos del diario_service original.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                empresa_id,
                periodo_anio,
                periodo_mes,
                COUNT(*)                        AS total_registros,
                COALESCE(SUM(debe), 0)          AS total_debe,
                COALESCE(SUM(haber), 0)         AS total_haber,
                MAX(cargado_en)::text           AS ultima_carga,
                MIN(archivo_origen)             AS archivo_origen
            FROM libro_diario
            WHERE empresa_id = %s
            GROUP BY empresa_id, periodo_anio, periodo_mes
            ORDER BY periodo_anio, periodo_mes
            """,
            (empresa_id,),
        )
        return cur.fetchall()
