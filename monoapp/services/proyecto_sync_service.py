"""
services/proyecto_sync_service.py
==================================
Sincroniza las tablas 'proyectos' y 'proy_presupuestos' desde la base de datos
externa (sistema de gestión) hacia la base de datos de ReporteApp (Neon).

El esquema de ambas tablas en ReporteApp es un espejo exacto del de la DB
externa (mismas columnas y tipos) — no hay mapeo de nombres.

Lógica:
    1. Conecta a ambas bases.
    2. Lee proyectos y proy_presupuestos desde la DB externa (todas las filas).
    3. Separa activos (deleted_at IS NULL) de dados de baja (deleted_at seteado).
    4. Borra de ReporteApp los proyectos dados de baja (cascadea sus presupuestos).
    5. Hace upsert de los proyectos activos -> ON CONFLICT (id).
    6. Borra de ReporteApp los presupuestos dados de baja individualmente
       (presupuesto de baja en un proyecto que sigue activo).
    7. Hace upsert de los presupuestos activos -> ON CONFLICT (id).
    8. Registra el resultado en sync_proyectos_log.

Uso:
    from services.proyecto_sync_service import sincronizar
    resultado = sincronizar(origen="auto_carga_diario")
"""
import logging
import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("proyecto_sync_service")

_PROYECTOS_COLS = [
    "id", "created_at", "updated_at", "deleted_at", "version", "nombre",
    "fecha_inicio", "fecha_final", "estado", "importe_mat", "importe_mo",
    "comentario", "centro_costo", "terceros", "herramientas", "superficie",
    "ingresos", "oportunidad_id", "responsable_id",
]

_PRESUPUESTOS_COLS = [
    "id", "proyecto_id", "fecha", "mo_propia", "mo_terceros", "materiales",
    "horas", "metros", "created_at", "updated_at", "deleted_at", "version",
    "importe", "herramientas", "descripcion",
]


def _conn_externa() -> str:
    dsn = os.environ.get("EXTERNAL_DB_URL")
    if not dsn:
        raise ValueError("EXTERNAL_DB_URL no está configurada en el .env")
    return dsn


def _conn_reporteapp() -> str:
    return os.environ["DATABASE_URL"]


# ── Helpers de validación ───────────────────────────────────────────────────

def validar_proyecto(row: dict) -> list[str]:
    """Valida una fila de proyectos. Devuelve lista de errores (vacía si OK)."""
    errores = []
    if row.get("id") is None:
        errores.append("Fila sin id")
    if not row.get("nombre") or not str(row["nombre"]).strip():
        errores.append(f"id={row.get('id')}: nombre vacío")
    if row.get("responsable_id") is None:
        errores.append(f"id={row.get('id')}: responsable_id vacío")
    return errores


def validar_presupuesto(row: dict, proyectos_validos: set) -> list[str]:
    """Valida una fila de proy_presupuestos. Devuelve lista de errores."""
    errores = []
    if row.get("id") is None:
        errores.append("Fila sin id")
    if row.get("proyecto_id") not in proyectos_validos:
        errores.append(
            f"id={row.get('id')}: proyecto_id={row.get('proyecto_id')} "
            f"no existe entre los proyectos activos sincronizados"
        )
    if row.get("fecha") is None:
        errores.append(f"id={row.get('id')}: fecha vacía")
    return errores


# ── Lectura desde la DB externa ─────────────────────────────────────────────

def leer_proyectos_externa(conn) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"SELECT {', '.join(_PROYECTOS_COLS)} FROM proyectos ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def leer_presupuestos_externa(conn) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"SELECT {', '.join(_PRESUPUESTOS_COLS)} FROM proy_presupuestos ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


# ── Baja lógica: borrado en ReporteApp ──────────────────────────────────────

def eliminar_proyectos_baja(conn, ids: set) -> int:
    """Borra de ReporteApp los proyectos dados de baja en origen (cascadea presupuestos)."""
    if not ids:
        return 0
    cur = conn.cursor()
    cur.execute("DELETE FROM proyectos WHERE id = ANY(%s)", (list(ids),))
    borrados = cur.rowcount
    conn.commit()
    cur.close()
    return borrados


def eliminar_presupuestos_baja(conn, ids: set) -> int:
    """Borra de ReporteApp los presupuestos dados de baja individualmente en origen."""
    if not ids:
        return 0
    cur = conn.cursor()
    cur.execute("DELETE FROM proy_presupuestos WHERE id = ANY(%s)", (list(ids),))
    borrados = cur.rowcount
    conn.commit()
    cur.close()
    return borrados


# ── Upsert en ReporteApp ─────────────────────────────────────────────────────

def upsert_proyectos(conn, proyectos: list[dict]) -> tuple[int, int, list[str]]:
    """
    Hace upsert en la tabla proyectos de ReporteApp (solo filas activas).
    Devuelve (ok, error, lista_de_errores).
    """
    cur = conn.cursor()
    ok, error = 0, 0
    detalle_errores = []

    sql = """
        INSERT INTO proyectos (
            id, created_at, updated_at, deleted_at, version, nombre,
            fecha_inicio, fecha_final, estado, importe_mat, importe_mo,
            comentario, centro_costo, terceros, herramientas, superficie,
            ingresos, oportunidad_id, responsable_id
        ) VALUES (
            %(id)s, %(created_at)s, %(updated_at)s, %(deleted_at)s, %(version)s, %(nombre)s,
            %(fecha_inicio)s, %(fecha_final)s, %(estado)s, %(importe_mat)s, %(importe_mo)s,
            %(comentario)s, %(centro_costo)s, %(terceros)s, %(herramientas)s, %(superficie)s,
            %(ingresos)s, %(oportunidad_id)s, %(responsable_id)s
        )
        ON CONFLICT (id) DO UPDATE SET
            created_at      = EXCLUDED.created_at,
            updated_at      = EXCLUDED.updated_at,
            deleted_at      = EXCLUDED.deleted_at,
            version         = EXCLUDED.version,
            nombre          = EXCLUDED.nombre,
            fecha_inicio    = EXCLUDED.fecha_inicio,
            fecha_final     = EXCLUDED.fecha_final,
            estado          = EXCLUDED.estado,
            importe_mat     = EXCLUDED.importe_mat,
            importe_mo      = EXCLUDED.importe_mo,
            comentario      = EXCLUDED.comentario,
            centro_costo    = EXCLUDED.centro_costo,
            terceros        = EXCLUDED.terceros,
            herramientas    = EXCLUDED.herramientas,
            superficie      = EXCLUDED.superficie,
            ingresos        = EXCLUDED.ingresos,
            oportunidad_id  = EXCLUDED.oportunidad_id,
            responsable_id  = EXCLUDED.responsable_id
    """

    for row in proyectos:
        errores = validar_proyecto(row)
        if errores:
            error += 1
            detalle_errores.extend(errores)
            log.warning(f"Proyecto inválido, se omite: {errores}")
            continue

        try:
            cur.execute(sql, row)
            ok += 1
        except Exception as e:
            conn.rollback()
            error += 1
            msg = f"id={row.get('id')}: {e}"
            detalle_errores.append(msg)
            log.error(f"Error al upsertear proyecto: {msg}")
        else:
            conn.commit()

    cur.close()
    return ok, error, detalle_errores


def upsert_presupuestos(conn, presupuestos: list[dict], proyectos_validos: set) -> tuple[int, int, list[str]]:
    """
    Hace upsert en la tabla proy_presupuestos de ReporteApp (solo filas activas).
    Devuelve (ok, error, lista_de_errores).
    """
    cur = conn.cursor()
    ok, error = 0, 0
    detalle_errores = []

    sql = """
        INSERT INTO proy_presupuestos (
            id, proyecto_id, fecha, mo_propia, mo_terceros, materiales,
            horas, metros, created_at, updated_at, deleted_at, version,
            importe, herramientas, descripcion
        ) VALUES (
            %(id)s, %(proyecto_id)s, %(fecha)s, %(mo_propia)s, %(mo_terceros)s, %(materiales)s,
            %(horas)s, %(metros)s, %(created_at)s, %(updated_at)s, %(deleted_at)s, %(version)s,
            %(importe)s, %(herramientas)s, %(descripcion)s
        )
        ON CONFLICT (id) DO UPDATE SET
            proyecto_id  = EXCLUDED.proyecto_id,
            fecha        = EXCLUDED.fecha,
            mo_propia    = EXCLUDED.mo_propia,
            mo_terceros  = EXCLUDED.mo_terceros,
            materiales   = EXCLUDED.materiales,
            horas        = EXCLUDED.horas,
            metros       = EXCLUDED.metros,
            created_at   = EXCLUDED.created_at,
            updated_at   = EXCLUDED.updated_at,
            deleted_at   = EXCLUDED.deleted_at,
            version      = EXCLUDED.version,
            importe      = EXCLUDED.importe,
            herramientas = EXCLUDED.herramientas,
            descripcion  = EXCLUDED.descripcion
    """

    for row in presupuestos:
        errores = validar_presupuesto(row, proyectos_validos)
        if errores:
            error += 1
            detalle_errores.extend(errores)
            log.warning(f"Presupuesto inválido, se omite: {errores}")
            continue

        try:
            cur.execute(sql, row)
            ok += 1
        except Exception as e:
            conn.rollback()
            error += 1
            msg = f"id={row.get('id')}: {e}"
            detalle_errores.append(msg)
            log.error(f"Error al upsertear presupuesto: {msg}")
        else:
            conn.commit()

    cur.close()
    return ok, error, detalle_errores


# ── Log de sincronización ────────────────────────────────────────────────────

def registrar_log(conn, origen: str, p_ok, p_err, b_ok, b_err, detalle: list[str]):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sync_proyectos_log
            (origen, proyectos_ok, proyectos_error,
             presupuestos_ok, presupuestos_error, detalle_errores)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (origen, p_ok, p_err, b_ok, b_err, "\n".join(detalle) if detalle else None))
    conn.commit()
    cur.close()


# ── Orquestación ───────────────────────────────────────────────────────────

def sincronizar(origen: str = "manual") -> dict:
    """
    Ejecuta la sincronización completa (proyectos + presupuestos) y devuelve
    un resumen. Abre y cierra sus propias conexiones — no depende del caller.
    """
    conn_ext = psycopg2.connect(_conn_externa())
    conn_app = psycopg2.connect(_conn_reporteapp())

    try:
        proyectos_todos = leer_proyectos_externa(conn_ext)
        presupuestos_todos = leer_presupuestos_externa(conn_ext)

        proyectos_activos = [r for r in proyectos_todos if r["deleted_at"] is None]
        proyectos_baja_ids = {r["id"] for r in proyectos_todos if r["deleted_at"] is not None}

        presupuestos_activos = [r for r in presupuestos_todos if r["deleted_at"] is None]
        presupuestos_baja_ids = {r["id"] for r in presupuestos_todos if r["deleted_at"] is not None}

        # Baja lógica primero: saca de ReporteApp lo que ya no está activo en origen.
        # Borrar un proyecto cascadea sus presupuestos (FK ON DELETE CASCADE).
        p_baja = eliminar_proyectos_baja(conn_app, proyectos_baja_ids)

        p_ok, p_err, p_errores = upsert_proyectos(conn_app, proyectos_activos)

        # IDs de proyectos activos que sí se sincronizaron correctamente (para validar FK)
        proyectos_validos = {row["id"] for row in proyectos_activos if not validar_proyecto(row)}

        # Presupuestos de baja individual (proyecto sigue activo, la línea de presupuesto no).
        b_baja = eliminar_presupuestos_baja(conn_app, presupuestos_baja_ids)

        b_ok, b_err, b_errores = upsert_presupuestos(conn_app, presupuestos_activos, proyectos_validos)

        detalle_total = p_errores + b_errores
        registrar_log(conn_app, origen, p_ok, p_err, b_ok, b_err, detalle_total)

        return {
            "proyectos_ok": p_ok,
            "proyectos_error": p_err,
            "proyectos_baja": p_baja,
            "presupuestos_ok": b_ok,
            "presupuestos_error": b_err,
            "presupuestos_baja": b_baja,
            "errores": detalle_total,
        }

    finally:
        conn_ext.close()
        conn_app.close()
