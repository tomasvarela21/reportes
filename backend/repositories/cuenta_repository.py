"""
Acceso a datos para dim_cuenta.
Solo contiene SQL — sin lógica de negocio ni excepciones HTTP.
"""
import logging

import psycopg2.extras
from psycopg2.extras import RealDictCursor

from backend.core.db_utils import get_columnas_tabla, execute_batch

log = logging.getLogger(__name__)

# Columnas fijas conocidas de dim_cuenta (en orden canónico)
_COLS_FIJAS = [
    "nro_cta", "extendido", "nombre", "rubro", "sub_rubro", "analisis",
    "fases", "tipo", "moneda", "activa", "es_resultado",
    "nivel_1", "nivel_2", "nivel_3",
]
_COLS_FIJAS_SET = set(_COLS_FIJAS)

# Columnas que usan COALESCE en el ON CONFLICT DO UPDATE (preservan valor DB si EXCLUDED es NULL)
_COLS_COALESCE = {"activa", "nivel_1", "nivel_2", "nivel_3"}

_SELECT_FIJAS = (
    "SELECT nro_cta, extendido, nombre, rubro, sub_rubro, analisis, "
    "fases, tipo, moneda, activa, es_resultado, nivel_1, nivel_2, nivel_3 "
    "FROM dim_cuenta"
)


# ── Queries ───────────────────────────────────────────────────────────────────

def find_all(conn, filtros: dict) -> list[dict]:
    """
    filtros admitidos: rubro, tipo, activa, search, limit, offset.
    """
    conditions: list[str] = []
    params: list = []

    if filtros.get("rubro") is not None:
        conditions.append("rubro = %s")
        params.append(filtros["rubro"])
    if filtros.get("tipo") is not None:
        conditions.append("tipo = %s")
        params.append(filtros["tipo"])
    if filtros.get("activa") is not None:
        conditions.append("activa = %s")
        params.append(filtros["activa"])
    if filtros.get("search"):
        conditions.append("(nombre ILIKE %s OR nro_cta::text LIKE %s)")
        s = filtros["search"]
        params.extend([f"%{s}%", f"%{s}%"])

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit  = filtros.get("limit", 500)
    offset = filtros.get("offset", 0)
    sql = f"{_SELECT_FIJAS} {where} ORDER BY nro_cta LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def find_by_id(conn, nro_cta: int) -> dict | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"{_SELECT_FIJAS} WHERE nro_cta = %s", (nro_cta,))
        return cur.fetchone()


def find_rubros(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT rubro FROM dim_cuenta "
            "WHERE rubro IS NOT NULL AND rubro != '' ORDER BY rubro"
        )
        return [r[0] for r in cur.fetchall()]


def count_movimientos(conn, nro_cta: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM libro_diario WHERE cuenta_codigo = %s",
            (nro_cta,),
        )
        return cur.fetchone()[0]


def create(conn, data: dict) -> dict:
    """
    INSERT INTO dim_cuenta ... RETURNING *.
    Puede lanzar psycopg2.IntegrityError si hay conflicto de PK.
    """
    cols = _COLS_FIJAS
    placeholders = ", ".join(["%s"] * len(cols))
    returning = ", ".join(cols)
    sql = (
        f"INSERT INTO dim_cuenta ({', '.join(cols)}) "
        f"VALUES ({placeholders}) "
        f"RETURNING {returning}"
    )
    values = tuple(data.get(c) for c in cols)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, values)
        row = cur.fetchone()
    conn.commit()
    return row


def update(conn, nro_cta: int, campos: dict) -> dict | None:
    """
    UPDATE dim_cuenta SET ... WHERE nro_cta = %s RETURNING *.
    campos: dict con solo los campos a actualizar (ya filtrado por el service).
    """
    if not campos:
        return find_by_id(conn, nro_cta)

    set_clause = ", ".join(f"{k} = %s" for k in campos)
    returning  = ", ".join(_COLS_FIJAS)
    sql = (
        f"UPDATE dim_cuenta SET {set_clause} "
        f"WHERE nro_cta = %s "
        f"RETURNING {returning}"
    )
    values = list(campos.values()) + [nro_cta]

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, values)
        row = cur.fetchone()
    conn.commit()
    return row


def delete(conn, nro_cta: int) -> bool:
    """DELETE FROM dim_cuenta WHERE nro_cta = %s. Retorna True si se eliminó."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM dim_cuenta WHERE nro_cta = %s", (nro_cta,))
        deleted = cur.rowcount > 0
    conn.commit()
    return deleted


def find_nombres_actuales(conn, nros_cta: list[int]) -> dict[int, str]:
    """Devuelve {nro_cta: nombre} para las cuentas indicadas."""
    if not nros_cta:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT nro_cta, nombre FROM dim_cuenta WHERE nro_cta = ANY(%s)",
            (nros_cta,),
        )
        return {r[0]: r[1] for r in cur.fetchall()}


def find_all_ids(conn) -> set[int]:
    """Devuelve el conjunto de todos los nro_cta existentes."""
    with conn.cursor() as cur:
        cur.execute("SELECT nro_cta FROM dim_cuenta")
        return {r[0] for r in cur.fetchall()}


def upsert_batch(conn, cols: list[str], rows: list[tuple]) -> int:
    """
    INSERT INTO dim_cuenta (...cols...) VALUES %s
    ON CONFLICT (nro_cta) DO UPDATE SET ...

    - cols: lista de nombres de columna, en el mismo orden que cada tupla en rows.
    - Usa COALESCE para activa, nivel_1, nivel_2, nivel_3 (preserva valor DB si EXCLUDED es NULL).
    - No hace commit — el service es responsable.
    - Retorna cantidad de filas procesadas.
    """
    if not rows:
        return 0

    set_parts = []
    for c in cols:
        if c == "nro_cta":
            continue
        if c in _COLS_COALESCE:
            set_parts.append(f"{c} = COALESCE(EXCLUDED.{c}, dim_cuenta.{c})")
        else:
            set_parts.append(f"{c} = EXCLUDED.{c}")

    sql = (
        f"INSERT INTO dim_cuenta ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT (nro_cta) DO UPDATE SET {', '.join(set_parts)}"
    )
    return execute_batch(conn, sql, rows, page_size=200)
