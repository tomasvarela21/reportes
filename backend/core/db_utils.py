"""
Helpers de base de datos reutilizables en toda la capa de repositorios.
"""
import logging

import psycopg2.extras
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)


def get_columnas_tabla(conn, tabla: str) -> set[str]:
    """
    Devuelve el conjunto de nombres de columna actuales de una tabla.
    Consulta information_schema para no depender de caché.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (tabla,),
        )
        return {row[0] for row in cur.fetchall()}


def execute_batch(conn, sql: str, rows: list, page_size: int = 200) -> int:
    """
    Ejecuta un INSERT (o UPDATE) en batch con psycopg2 execute_values.
    Retorna la cantidad de filas procesadas.
    Nota: no hace commit — el llamador es responsable del commit/rollback.
    """
    if not rows:
        return 0
    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
        # rowcount puede ser -1 con algunos drivers; usamos len(rows) como fallback seguro
        affected = cur.rowcount if cur.rowcount >= 0 else len(rows)
        return affected
    finally:
        cur.close()
