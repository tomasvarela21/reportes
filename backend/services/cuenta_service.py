"""
Lógica de negocio para dim_cuenta.
Copiada y adaptada de pages/5_Administracion.py — no importa desde fuera de /backend.
"""
import logging
import traceback

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException

from backend.schemas.cuenta import CuentaCreate, CuentaUpdate

log = logging.getLogger(__name__)

# Columnas fijas conocidas de dim_cuenta
_COLS_FIJAS = {
    "nro_cta", "extendido", "nombre", "rubro", "sub_rubro", "analisis",
    "fases", "tipo", "moneda", "activa", "es_resultado",
    "nivel_1", "nivel_2", "nivel_3",
}


def _base_select() -> str:
    return (
        "SELECT nro_cta, extendido, nombre, rubro, sub_rubro, analisis, "
        "fases, tipo, moneda, activa, es_resultado, nivel_1, nivel_2, nivel_3 "
        "FROM dim_cuenta"
    )


def listar_cuentas(
    conn,
    *,
    rubro: str | None = None,
    tipo: str | None = None,
    activa: bool | None = None,
    search: str | None = None,
    limit: int = 500,
    offset: int = 0,
) -> list[dict]:
    conditions: list[str] = []
    params: list = []

    if rubro is not None:
        conditions.append("rubro = %s")
        params.append(rubro)
    if tipo is not None:
        conditions.append("tipo = %s")
        params.append(tipo)
    if activa is not None:
        conditions.append("activa = %s")
        params.append(activa)
    if search:
        conditions.append("(nombre ILIKE %s OR nro_cta::text LIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"{_base_select()} {where} ORDER BY nro_cta LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception:
        log.error("Error en listar_cuentas:\n%s", traceback.format_exc())
        raise


def obtener_cuenta(conn, nro_cta: int) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"{_base_select()} WHERE nro_cta = %s", (nro_cta,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Cuenta {nro_cta} no encontrada")
    return row


def crear_cuenta(conn, body: CuentaCreate) -> dict:
    _verificar_nro_disponible(conn, body.nro_cta)
    _verificar_nombre_disponible(conn, body.nombre)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO dim_cuenta
                    (nro_cta, extendido, nombre, rubro, sub_rubro, analisis,
                     fases, tipo, moneda, activa, es_resultado,
                     nivel_1, nivel_2, nivel_3)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING nro_cta, extendido, nombre, rubro, sub_rubro, analisis,
                          fases, tipo, moneda, activa, es_resultado,
                          nivel_1, nivel_2, nivel_3
                """,
                (
                    body.nro_cta, body.extendido, body.nombre, body.rubro,
                    body.sub_rubro, body.analisis, body.fases, body.tipo,
                    body.moneda, body.activa, body.es_resultado,
                    body.nivel_1, body.nivel_2, body.nivel_3,
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return row
    except psycopg2.IntegrityError as e:
        conn.rollback()
        raise HTTPException(status_code=409, detail=f"Conflicto al insertar: {e}")
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))


def actualizar_cuenta(conn, nro_cta: int, body: CuentaUpdate) -> dict:
    obtener_cuenta(conn, nro_cta)  # lanza 404 si no existe

    campos = {k: v for k, v in body.model_dump(exclude_none=True).items()}
    if not campos:
        raise HTTPException(status_code=400, detail="No se enviaron campos a actualizar")

    sets = ", ".join(f"{k} = %s" for k in campos)
    valores = list(campos.values()) + [nro_cta]

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE dim_cuenta SET {sets}
                WHERE nro_cta = %s
                RETURNING nro_cta, extendido, nombre, rubro, sub_rubro, analisis,
                          fases, tipo, moneda, activa, es_resultado,
                          nivel_1, nivel_2, nivel_3
                """,
                valores,
            )
            row = cur.fetchone()
        conn.commit()
        return row
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))


def eliminar_cuenta(conn, nro_cta: int) -> None:
    obtener_cuenta(conn, nro_cta)  # lanza 404 si no existe
    _verificar_sin_movimientos(conn, nro_cta)

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM dim_cuenta WHERE nro_cta = %s", (nro_cta,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))


def listar_rubros(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT rubro FROM dim_cuenta "
            "WHERE rubro IS NOT NULL AND rubro != '' ORDER BY rubro"
        )
        return [r[0] for r in cur.fetchall()]


# ── Helpers de validación ──────────────────────────────────────────────────────

def _verificar_nro_disponible(conn, nro_cta: int) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM dim_cuenta WHERE nro_cta = %s", (nro_cta,))
        if cur.fetchone():
            raise HTTPException(
                status_code=409,
                detail=f"El número de cuenta {nro_cta} ya existe",
            )


def _verificar_nombre_disponible(conn, nombre: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM dim_cuenta WHERE LOWER(nombre) = LOWER(%s)", (nombre,)
        )
        if cur.fetchone():
            raise HTTPException(
                status_code=409,
                detail=f"Ya existe una cuenta con el nombre '{nombre}'",
            )


def _verificar_sin_movimientos(conn, nro_cta: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM libro_diario WHERE cuenta_codigo = %s", (nro_cta,)
        )
        count = cur.fetchone()[0]
    if count > 0:
        raise HTTPException(
            status_code=409,
            detail=f"La cuenta {nro_cta} tiene {count} movimiento(s) y no puede eliminarse",
        )
