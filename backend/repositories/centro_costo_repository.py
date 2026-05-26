"""
Acceso a datos para dim_centro_costo.
Solo contiene SQL — sin lógica de negocio ni excepciones HTTP.
"""
import logging

from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

_SELECT_CENTROS = """
    SELECT
        cc.codigo,
        cc.descripcion,
        de.empresa_nombre,
        cc.activo
    FROM dim_centro_costo cc
    LEFT JOIN dim_empresa de ON de.empresa_id = cc.empresa_id
    ORDER BY cc.codigo
"""


def find_all(conn) -> list[dict]:
    """Todos los centros de costo con nombre de empresa (JOIN)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_SELECT_CENTROS)
        return cur.fetchall()


def find_by_codigo(conn, codigo: str) -> dict | None:
    """Busca un centro por su código (PK)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT cc.codigo, cc.descripcion, de.empresa_nombre, cc.activo
            FROM dim_centro_costo cc
            LEFT JOIN dim_empresa de ON de.empresa_id = cc.empresa_id
            WHERE cc.codigo = %s
            """,
            (codigo,),
        )
        return cur.fetchone()


def exists(conn, codigo: str) -> bool:
    """True si ya existe un centro con ese código."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM dim_centro_costo WHERE codigo = %s",
            (codigo,),
        )
        return cur.fetchone() is not None


def create(conn, data: dict) -> dict:
    """
    INSERT INTO dim_centro_costo (codigo, descripcion, empresa_id).
    No usa ON CONFLICT — el service verifica existencia primero.
    Retorna el registro creado (con empresa_nombre via JOIN).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_centro_costo (codigo, descripcion, empresa_id)
            VALUES (%s, %s, %s)
            """,
            (data["codigo"], data.get("descripcion"), data.get("empresa_id")),
        )
    conn.commit()
    # Re-fetch con JOIN para incluir empresa_nombre
    return find_by_codigo(conn, data["codigo"])
