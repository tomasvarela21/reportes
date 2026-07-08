"""
sync_proyectos.py
==================
Sincroniza las tablas 'proyectos' y 'proy_presupuestos' desde la base de datos
externa (sistema de gestión) hacia la base de datos de ReporteApp (Neon).

Lógica:
    1. Conecta a ambas bases.
    2. Lee proyectos y proy_presupuestos desde la DB externa.
    3. Valida los datos antes de migrar.
    4. Hace upsert en ReporteApp:
         - proyectos       -> ON CONFLICT (ccosto)
         - proy_presupuestos -> ON CONFLICT (id)
    5. Registra el resultado en sync_proyectos_log.

Uso:
    python sync_proyectos.py                  # origen='manual'
    python sync_proyectos.py --origen auto_carga_diario
"""
import argparse
import logging
import os
import sys
from datetime import datetime, date
from decimal import Decimal

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sync_proyectos")

# ── Configuración de conexiones ────────────────────────────────────────────
CONN_REPORTEAPP = os.environ["DATABASE_URL"]

CONN_EXTERNA = os.environ.get("EXTERNAL_DB_URL")
if not CONN_EXTERNA:
    log.error(
        "EXTERNAL_DB_URL no está configurada. "
        "Agregá la variable al archivo .env antes de ejecutar este script."
    )
    sys.exit(1)


# ── Helpers de validación ───────────────────────────────────────────────────

def validar_proyecto(row: dict) -> list[str]:
    """Valida una fila de proyectos. Devuelve lista de errores (vacía si OK)."""
    errores = []
    if not row.get("nombre") or not str(row["nombre"]).strip():
        errores.append(f"id={row.get('id')}: nombre vacío")
    if row.get("id") is None:
        errores.append("Fila sin id (no se puede usar como id_origen)")
    return errores


def validar_presupuesto(row: dict, proyectos_validos: set) -> list[str]:
    """Valida una fila de proy_presupuestos. Devuelve lista de errores."""
    errores = []
    if row.get("id") is None:
        errores.append("Fila sin id")
    if row.get("proyecto_id") not in proyectos_validos:
        errores.append(
            f"id={row.get('id')}: proyecto_id={row.get('proyecto_id')} "
            f"no existe entre los proyectos sincronizados"
        )
    if row.get("fecha") is None:
        errores.append(f"id={row.get('id')}: fecha vacía")
    return errores


def resolver_ccosto(row: dict) -> str:
    """
    Determina el ccosto a usar en ReporteApp.
    Si centro_costo viene NULL en origen, usa el id como fallback.
    """
    cc = row.get("centro_costo")
    if cc is not None:
        return str(cc)
    return str(row["id"])


# ── Lectura desde la DB externa ─────────────────────────────────────────────

def leer_proyectos_externa(conn) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id, created_at, updated_at, deleted_at, version, nombre,
               fecha_inicio, fecha_final, estado, importe_mat, importe_mo,
               comentario, centro_costo, terceros, herramientas,
               superficie, ingresos, oportunidad_id, responsable_id
        FROM proyectos
        ORDER BY id;
    """)
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def leer_presupuestos_externa(conn) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id, proyecto_id, fecha, mo_propia, mo_terceros, materiales,
               horas, metros, created_at, updated_at, deleted_at, version,
               importe, herramientas, descripcion
        FROM proy_presupuestos
        ORDER BY id;
    """)
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


# ── Upsert en ReporteApp ─────────────────────────────────────────────────────

def upsert_proyectos(conn, proyectos: list[dict]) -> tuple[int, int, list[str]]:
    """
    Hace upsert en la tabla proyectos de ReporteApp.
    Devuelve (ok, error, lista_de_errores).
    """
    cur = conn.cursor()
    ok, error = 0, 0
    detalle_errores = []

    sql = """
        INSERT INTO proyectos (
            ccosto, nombre, fc_inicio, fc_fin, estado,
            cto_materiales, cto_mo_propia, cto_mo_terceros, cto_herramientas,
            superficie, ingresos, comentario, oportunidad_id, responsable_id,
            version, deleted_at, activo, id_origen, actualizado_en
        ) VALUES (
            %(ccosto)s, %(nombre)s, %(fc_inicio)s, %(fc_fin)s, %(estado)s,
            %(cto_materiales)s, %(cto_mo_propia)s, %(cto_mo_terceros)s, %(cto_herramientas)s,
            %(superficie)s, %(ingresos)s, %(comentario)s, %(oportunidad_id)s, %(responsable_id)s,
            %(version)s, %(deleted_at)s, %(activo)s, %(id_origen)s, %(actualizado_en)s
        )
        ON CONFLICT (ccosto) DO UPDATE SET
            nombre            = EXCLUDED.nombre,
            fc_inicio         = EXCLUDED.fc_inicio,
            fc_fin            = EXCLUDED.fc_fin,
            estado            = EXCLUDED.estado,
            cto_materiales    = EXCLUDED.cto_materiales,
            cto_mo_propia     = EXCLUDED.cto_mo_propia,
            cto_mo_terceros   = EXCLUDED.cto_mo_terceros,
            cto_herramientas  = EXCLUDED.cto_herramientas,
            superficie        = EXCLUDED.superficie,
            ingresos          = EXCLUDED.ingresos,
            comentario        = EXCLUDED.comentario,
            oportunidad_id    = EXCLUDED.oportunidad_id,
            responsable_id    = EXCLUDED.responsable_id,
            version           = EXCLUDED.version,
            deleted_at        = EXCLUDED.deleted_at,
            activo            = EXCLUDED.activo,
            id_origen         = EXCLUDED.id_origen,
            actualizado_en    = EXCLUDED.actualizado_en
        -- cto_diversos y avance NO se tocan: no existen en el origen
    """

    for row in proyectos:
        errores = validar_proyecto(row)
        if errores:
            error += 1
            detalle_errores.extend(errores)
            log.warning(f"Proyecto inválido, se omite: {errores}")
            continue

        params = {
            "ccosto":           resolver_ccosto(row),
            "nombre":           row["nombre"],
            "fc_inicio":        row.get("fecha_inicio"),
            "fc_fin":           row.get("fecha_final"),
            "estado":           row.get("estado"),
            "cto_materiales":   row.get("importe_mat") or Decimal("0"),
            "cto_mo_propia":    row.get("importe_mo") or Decimal("0"),
            "cto_mo_terceros":  row.get("terceros") or Decimal("0"),
            "cto_herramientas": row.get("herramientas") or Decimal("0"),
            "superficie":       row.get("superficie"),
            "ingresos":         row.get("ingresos") or Decimal("0"),
            "comentario":       row.get("comentario"),
            "oportunidad_id":   row.get("oportunidad_id"),
            "responsable_id":   row.get("responsable_id"),
            "version":          row.get("version") or 1,
            "deleted_at":       row.get("deleted_at"),
            "activo":           row.get("deleted_at") is None,
            "id_origen":        row["id"],
            "actualizado_en":   row.get("updated_at") or datetime.now(),
        }

        try:
            cur.execute(sql, params)
            ok += 1
        except Exception as e:
            conn.rollback()
            error += 1
            msg = f"id={row.get('id')} ccosto={params['ccosto']}: {e}"
            detalle_errores.append(msg)
            log.error(f"Error al upsertear proyecto: {msg}")
        else:
            conn.commit()

    cur.close()
    return ok, error, detalle_errores


def upsert_presupuestos(conn, presupuestos: list[dict], proyectos_validos: set) -> tuple[int, int, list[str]]:
    cur = conn.cursor()
    ok, error = 0, 0
    detalle_errores = []

    sql = """
        INSERT INTO proy_presupuestos (
            id, proyecto_id, fecha, mo_propia, mo_terceros, materiales,
            herramientas, horas, metros, importe, descripcion, cargado_en
        ) VALUES (
            %(id)s, %(proyecto_id)s, %(fecha)s, %(mo_propia)s, %(mo_terceros)s,
            %(materiales)s, %(herramientas)s, %(horas)s, %(metros)s,
            %(importe)s, %(descripcion)s, %(cargado_en)s
        )
        ON CONFLICT (id) DO UPDATE SET
            proyecto_id  = EXCLUDED.proyecto_id,
            fecha        = EXCLUDED.fecha,
            mo_propia    = EXCLUDED.mo_propia,
            mo_terceros  = EXCLUDED.mo_terceros,
            materiales   = EXCLUDED.materiales,
            herramientas = EXCLUDED.herramientas,
            horas        = EXCLUDED.horas,
            metros       = EXCLUDED.metros,
            importe      = EXCLUDED.importe,
            descripcion  = EXCLUDED.descripcion,
            cargado_en   = EXCLUDED.cargado_en
    """

    for row in presupuestos:
        errores = validar_presupuesto(row, proyectos_validos)
        if errores:
            error += 1
            detalle_errores.extend(errores)
            log.warning(f"Presupuesto inválido, se omite: {errores}")
            continue

        params = {
            "id":           row["id"],
            "proyecto_id":  row["proyecto_id"],
            "fecha":        row["fecha"],
            "mo_propia":    row.get("mo_propia") or Decimal("0"),
            "mo_terceros":  row.get("mo_terceros") or Decimal("0"),
            "materiales":   row.get("materiales") or Decimal("0"),
            "herramientas": row.get("herramientas") or Decimal("0"),
            "horas":        row.get("horas") or Decimal("0"),
            "metros":       row.get("metros") or Decimal("0"),
            "importe":      row.get("importe") or Decimal("0"),
            "descripcion":  row.get("descripcion"),
            "cargado_en":   row.get("created_at") or datetime.now(),
        }

        try:
            cur.execute(sql, params)
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


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Sincroniza proyectos y presupuestos desde DB externa.")
    parser.add_argument(
        "--origen", default="manual",
        choices=["manual", "auto_carga_diario"],
        help="Origen de la sincronización (para el log).",
    )
    args = parser.parse_args()

    log.info("Conectando a la base de datos externa...")
    conn_ext = psycopg2.connect(CONN_EXTERNA)

    log.info("Conectando a la base de datos de ReporteApp...")
    conn_app = psycopg2.connect(CONN_REPORTEAPP)

    try:
        log.info("Leyendo proyectos desde la DB externa...")
        proyectos = leer_proyectos_externa(conn_ext)
        log.info(f"  {len(proyectos)} proyectos leídos.")

        log.info("Leyendo proy_presupuestos desde la DB externa...")
        presupuestos = leer_presupuestos_externa(conn_ext)
        log.info(f"  {len(presupuestos)} presupuestos leídos.")

        log.info("Sincronizando proyectos hacia ReporteApp...")
        p_ok, p_err, p_errores = upsert_proyectos(conn_app, proyectos)
        log.info(f"  Proyectos: {p_ok} OK, {p_err} con error.")

        # IDs de proyectos que sí se sincronizaron correctamente (para validar FK)
        proyectos_validos = {row["id"] for row in proyectos if not validar_proyecto(row)}

        log.info("Sincronizando presupuestos hacia ReporteApp...")
        b_ok, b_err, b_errores = upsert_presupuestos(conn_app, presupuestos, proyectos_validos)
        log.info(f"  Presupuestos: {b_ok} OK, {b_err} con error.")

        detalle_total = p_errores + b_errores
        registrar_log(conn_app, args.origen, p_ok, p_err, b_ok, b_err, detalle_total)

        log.info("Sincronización completa. Log registrado en sync_proyectos_log.")

        if p_err or b_err:
            log.warning(f"Hubo {p_err + b_err} errores. Revisá el detalle en sync_proyectos_log.")
            sys.exit(1)

    finally:
        conn_ext.close()
        conn_app.close()


if __name__ == "__main__":
    main()