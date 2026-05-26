from fastapi import APIRouter, Depends, Query
from psycopg2.extras import RealDictCursor

from backend.database import get_conn
from backend.schemas.mayor import MayorRow, MayorResumenRow

router = APIRouter(prefix="/mayor")

_NIVEL_DESC = "'cuenta' | 'subcuenta' | 'centro_costo'"


@router.get("/{empresa_id}", response_model=list[MayorRow])
def listar_mayor(
    empresa_id: int,
    anio: int | None          = Query(None, description="Filtrar por año"),
    mes: int | None           = Query(None, description="Filtrar por mes"),
    nivel: str | None         = Query(None, description=_NIVEL_DESC),
    cuenta_codigo: int | None = Query(None, description="Filtrar por número de cuenta"),
    centro_costo: str | None  = Query(None, description="Filtrar por centro de costo"),
    limit: int  = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Devuelve filas del libro_mayor para una empresa, con filtros opcionales."""
    conditions = ["empresa_id = %s"]
    params: list = [empresa_id]

    if anio is not None:
        conditions.append("periodo_anio = %s")
        params.append(anio)
    if mes is not None:
        conditions.append("periodo_mes = %s")
        params.append(mes)
    if nivel is not None:
        conditions.append("nivel = %s")
        params.append(nivel)
    if cuenta_codigo is not None:
        conditions.append("cuenta_codigo = %s")
        params.append(cuenta_codigo)
    if centro_costo is not None:
        conditions.append("centro_costo = %s")
        params.append(centro_costo)

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
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
        """, params)
        return cur.fetchall()


@router.get("/{empresa_id}/resumen", response_model=list[MayorResumenRow])
def resumen_mayor(
    empresa_id: int,
    anio: int | None = Query(None, description="Filtrar por año"),
    mes: int | None  = Query(None, description="Filtrar por mes"),
    conn=Depends(get_conn),
):
    """
    Totales por cuenta (nivel='cuenta') con nombre y rubro de dim_cuenta.
    Si no se filtra por período, suma todos los períodos disponibles.
    """
    conditions = ["lm.empresa_id = %s", "lm.nivel = 'cuenta'"]
    params: list = [empresa_id]

    if anio is not None:
        conditions.append("lm.periodo_anio = %s")
        params.append(anio)
    if mes is not None:
        conditions.append("lm.periodo_mes = %s")
        params.append(mes)

    where = " AND ".join(conditions)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
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
        """, params)
        return cur.fetchall()
