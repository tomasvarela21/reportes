import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException
from config import settings

log = logging.getLogger(__name__)

_CONNECT_ARGS = dict(
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5,
    connect_timeout=10,
)


def get_conn():
    """
    Dependency injection que devuelve una conexión psycopg2 por request.
    Cierra automáticamente al finalizar el request.
    """
    conn = None
    try:
        conn = psycopg2.connect(settings.database_url, **_CONNECT_ARGS)
        yield conn
    except psycopg2.OperationalError as e:
        log.error(f"No se pudo conectar a la base de datos: {e}")
        raise HTTPException(status_code=503, detail="Base de datos no disponible")
    finally:
        if conn is not None:
            conn.close()


def get_dict_conn():
    """
    Igual que get_conn() pero usa RealDictCursor para retornar filas como dicts.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            settings.database_url,
            cursor_factory=RealDictCursor,
            **_CONNECT_ARGS,
        )
        yield conn
    except psycopg2.OperationalError as e:
        log.error(f"No se pudo conectar a la base de datos: {e}")
        raise HTTPException(status_code=503, detail="Base de datos no disponible")
    finally:
        if conn is not None:
            conn.close()
