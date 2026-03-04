"""
services/db.py
==============
Manejo centralizado de conexión a Neon (PostgreSQL).

Estrategia:
  - Keepalives TCP cada 30s para mantener la conexión viva indefinidamente
  - Reconexión automática y silenciosa si Neon cierra la conexión
  - Si no puede reconectar muestra mensaje amigable en vez del traceback

Uso en cualquier página:
    from services.db import get_conn

    conn = get_conn()
    if conn is None:
        st.stop()  # get_conn ya mostró el mensaje de error

    cur = conn.cursor()
    cur.execute("SELECT ...")
"""

import os
import logging
import streamlit as st
import psycopg2
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

# Parámetros de keepalive TCP
# keepalives_idle=30   -> manda ping si no hubo actividad en 30s
# keepalives_interval=10 -> reintenta el ping cada 10s si no responde
# keepalives_count=5   -> después de 5 pings sin respuesta, cierra
_CONNECT_ARGS = dict(
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5,
    connect_timeout=10,
)

_CONN_KEY = "_neon_conn"  # clave en st.session_state


def _nueva_conexion():
    """Crea una conexión nueva a Neon con keepalives activos."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("DATABASE_URL no está definida en el .env")
    return psycopg2.connect(dsn, **_CONNECT_ARGS)


def _conexion_viva(conn) -> bool:
    """Verifica que la conexión responde con un ping liviano."""
    try:
        if conn.closed:
            return False
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return True
    except Exception:
        return False


def get_conn():
    """
    Devuelve una conexión activa a Neon.

    - Si ya existe una conexión viva en session_state la reutiliza.
    - Si está muerta reconecta automáticamente y en silencio.
    - Si no puede conectar muestra un mensaje amigable y retorna None.

    Uso:
        conn = get_conn()
        if conn is None:
            st.stop()
    """
    conn = st.session_state.get(_CONN_KEY)

    # Conexión existente y viva -> reutilizar
    if conn is not None and _conexion_viva(conn):
        return conn

    # Conexión muerta o no existe -> reconectar
    if conn is not None:
        log.warning("Conexión a Neon cerrada, reconectando...")
        try:
            conn.close()
        except Exception:
            pass

    try:
        conn = _nueva_conexion()
        st.session_state[_CONN_KEY] = conn
        log.info("Conexión a Neon establecida OK")
        return conn
    except Exception as e:
        log.error(f"No se pudo conectar a Neon: {e}")
        st.session_state.pop(_CONN_KEY, None)

        # Mensaje amigable en lugar del traceback
        st.warning(
            "⚠️ Se desconectó por inactividad. "
            "Intentá recargar la página para reconectar."
        )
        if st.button("🔄 Reconectar", key="_btn_reconectar"):
            st.rerun()

        return None