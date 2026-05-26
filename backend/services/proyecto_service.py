"""
Lógica para proyectos y presupuestos.
Adaptado de pages/5_Administracion.py (Tab 4 — Proyectos).
Schema de producción es la fuente de verdad — columnas fijas, sin detección dinámica.
"""
import io
import logging
from datetime import datetime

import pandas as pd
import psycopg2.extras
from fastapi import HTTPException
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

# Columnas completas de SELECT para proyectos (todas las del schema de producción)
_PROYECTOS_COLS = """
    ccosto, nombre, id_origen, oportunidad_id, responsable_id, version,
    fc_inicio, fc_fin, deleted_at, actualizado_en,
    estado, comentario, activo,
    ingresos, cto_mo_propia, cto_mo_terceros, cto_materiales,
    cto_herramientas, cto_diversos, superficie, avance, horas
"""

# Columnas completas de SELECT para presupuestos
_PRESUPUESTOS_COLS = """
    id, proyecto_id, fecha,
    mo_propia, mo_terceros, materiales, herramientas,
    horas, metros, importe, descripcion, cargado_en
"""


# ── Proyectos ─────────────────────────────────────────────────────────────────

def listar_proyectos(conn, incluir_inactivos: bool = False) -> list[dict]:
    where = "" if incluir_inactivos else "WHERE activo = true OR activo IS NULL"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT {_PROYECTOS_COLS} FROM proyectos {where} ORDER BY nombre")
        return cur.fetchall()


def obtener_proyecto(conn, id_origen: int) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_PROYECTOS_COLS} FROM proyectos WHERE id_origen = %s",
            (id_origen,),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Proyecto {id_origen} no encontrado")
    return row


def upsert_proyectos_desde_csv(conn, contenido: bytes, nombre_archivo: str) -> dict:
    df = _parsear_csv_proyectos(contenido, nombre_archivo)
    ahora = datetime.now()

    # Capturar ccostos existentes ANTES del upsert para contar nuevos vs actualizados
    with conn.cursor() as cur:
        cur.execute("SELECT ccosto FROM proyectos")
        ccostos_existentes = {r[0] for r in cur.fetchall()}

    # Construir tuplas para execute_values — orden debe coincidir con INSERT
    rows: list[tuple] = []
    n_inact = 0
    for _, row in df.iterrows():
        id_orig    = int(row['id'])
        es_deleted = bool(str(row.get('deleted_at', '')).strip())
        # ccosto es PK NOT NULL — fallback al id como string si no viene en el CSV
        ccosto = str(row.get('ccosto', '')).strip() or str(id_orig)

        if es_deleted:
            n_inact += 1

        rows.append((
            ccosto,
            str(row.get('nombre', '')).strip(),
            id_orig,
            _pdate(row.get('fc_inicio', '')),
            _pdate(row.get('fc_fin', '')),
            str(row.get('estado', '')).strip() or None,
            _pnum(row.get('ingresos',         0)),
            _pnum(row.get('cto_mo_propia',    0)),
            _pnum(row.get('cto_mo_terceros',  0)),
            _pnum(row.get('cto_materiales',   0)),
            _pnum(row.get('cto_herramientas', 0)),
            _pint(row.get('cto_diversos',    '')),
            _pnum(row.get('avance',          '')),
            _pnum(row.get('horas',           '')),
            _pnum(row.get('superficie',      '')),
            str(row.get('comentario', '')).strip() or None,
            _pint(row.get('oportunidad_id',  '')),
            _pint(row.get('responsable_id',  '')),
            _pint(row.get('version',         '')),
            not es_deleted,
            _pdate(row.get('deleted_at', '')) if es_deleted else None,
            ahora,
        ))

    ccostos_csv = {r[0] for r in rows}
    n_nuevos    = len(ccostos_csv - ccostos_existentes)
    n_act       = len(ccostos_csv & ccostos_existentes) - n_inact

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO proyectos (
                ccosto, nombre, id_origen,
                fc_inicio, fc_fin, estado,
                ingresos, cto_mo_propia, cto_mo_terceros,
                cto_materiales, cto_herramientas, cto_diversos,
                avance, horas, superficie, comentario,
                oportunidad_id, responsable_id, version,
                activo, deleted_at, actualizado_en
            ) VALUES %s
            ON CONFLICT (ccosto) DO UPDATE SET
                nombre           = EXCLUDED.nombre,
                id_origen        = EXCLUDED.id_origen,
                fc_inicio        = EXCLUDED.fc_inicio,
                fc_fin           = EXCLUDED.fc_fin,
                estado           = EXCLUDED.estado,
                ingresos         = EXCLUDED.ingresos,
                cto_mo_propia    = EXCLUDED.cto_mo_propia,
                cto_mo_terceros  = EXCLUDED.cto_mo_terceros,
                cto_materiales   = EXCLUDED.cto_materiales,
                cto_herramientas = EXCLUDED.cto_herramientas,
                cto_diversos     = EXCLUDED.cto_diversos,
                avance           = EXCLUDED.avance,
                horas            = EXCLUDED.horas,
                superficie       = EXCLUDED.superficie,
                comentario       = EXCLUDED.comentario,
                oportunidad_id   = EXCLUDED.oportunidad_id,
                responsable_id   = EXCLUDED.responsable_id,
                version          = EXCLUDED.version,
                activo           = EXCLUDED.activo,
                deleted_at       = EXCLUDED.deleted_at,
                actualizado_en   = EXCLUDED.actualizado_en
            """,
            rows,
            page_size=100,
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error al procesar proyectos: {e}")
    finally:
        cur.close()

    return {'nuevos': n_nuevos, 'actualizados': n_act, 'inactivos': n_inact, 'archivo': nombre_archivo}


# ── Presupuestos ──────────────────────────────────────────────────────────────

def listar_presupuestos(conn, proyecto_id: int | None = None) -> list[dict]:
    where  = "WHERE pp.proyecto_id = %s" if proyecto_id is not None else ""
    params = (proyecto_id,) if proyecto_id is not None else ()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_PRESUPUESTOS_COLS} FROM proy_presupuestos pp "
            f"{where} ORDER BY pp.fecha DESC, pp.proyecto_id",
            params,
        )
        return cur.fetchall()


def upsert_presupuestos_desde_csv(conn, contenido: bytes, nombre_archivo: str) -> dict:
    df = _parsear_csv_presupuestos(contenido, nombre_archivo)

    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT id_origen FROM proyectos WHERE id_origen IS NOT NULL")
        ids_proy_db = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT id FROM proy_presupuestos")
        ids_presp_db = {r[0] for r in cur.fetchall()}

    proy_invalidos = set(df['proyecto_id'].unique()) - ids_proy_db
    if proy_invalidos:
        raise HTTPException(
            status_code=400,
            detail=f"proyecto_id no encontrados en la DB: {sorted(proy_invalidos)}. Cargá primero los proyectos.",
        )

    n_nuevos = n_act = 0
    cur = conn.cursor()

    try:
        for _, row in df.iterrows():
            rid  = int(row['id'])
            vals = (
                int(row['proyecto_id']),
                str(row.get('fecha', '')).strip() or None,
                _pnum2(row.get('mo_propia',    0)),
                _pnum2(row.get('mo_terceros',  0)),
                _pnum2(row.get('materiales',   0)),
                _pnum2(row.get('herramientas', 0)),
                _pnum2(row.get('horas',        0)),
                _pnum2(row.get('metros',       0)),
                _pnum2(row.get('importe',      0)),
                str(row.get('descripcion', '')).strip() or None,
            )
            if rid in ids_presp_db:
                cur.execute("""
                    UPDATE proy_presupuestos SET
                        proyecto_id=%s, fecha=%s, mo_propia=%s, mo_terceros=%s,
                        materiales=%s, herramientas=%s, horas=%s, metros=%s,
                        importe=%s, descripcion=%s
                    WHERE id = %s
                """, (*vals, rid))
                n_act += 1
            else:
                cur.execute("""
                    INSERT INTO proy_presupuestos
                        (id, proyecto_id, fecha, mo_propia, mo_terceros,
                         materiales, herramientas, horas, metros, importe,
                         descripcion, cargado_en)
                    VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
                """, (rid, *vals))
                n_nuevos += 1

        conn.commit()
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error al procesar presupuestos: {e}")
    finally:
        cur.close()

    return {'nuevos': n_nuevos, 'actualizados': n_act, 'archivo': nombre_archivo}


# ── Helpers de parsing ────────────────────────────────────────────────────────

def _leer_df(contenido: bytes, nombre_archivo: str) -> pd.DataFrame:
    if nombre_archivo.endswith(('.xlsx', '.xls')):
        return pd.read_excel(io.BytesIO(contenido), dtype=str)
    for enc in ['latin-1', 'utf-8-sig', 'utf-8']:
        try:
            return pd.read_csv(
                io.BytesIO(contenido), sep=',', dtype=str, encoding=enc,
            ).fillna('')
        except UnicodeDecodeError:
            continue
    raise HTTPException(status_code=400, detail="No se pudo leer el archivo con ningún encoding conocido.")


def _parsear_csv_proyectos(contenido: bytes, nombre_archivo: str) -> pd.DataFrame:
    df = _leer_df(contenido, nombre_archivo)
    df.columns = [c.strip() for c in df.columns]

    col_map = {
        'fecha_inicio': 'fc_inicio',
        'fecha_final':  'fc_fin',
        'importe_mat':  'cto_materiales',
        'importe_mo':   'cto_mo_propia',
        'terceros':     'cto_mo_terceros',
        'herramientas': 'cto_herramientas',
        'centro_costo': 'ccosto',
    }
    df = df.rename(columns={c: col_map[c] for c in df.columns if c in col_map})

    missing = {'id', 'nombre'} - set(df.columns)
    if missing:
        raise HTTPException(status_code=400, detail=f"Faltan columnas requeridas: {missing}")

    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df[df['id'].notna()].copy()
    df['id'] = df['id'].astype(int)
    return df


def _parsear_csv_presupuestos(contenido: bytes, nombre_archivo: str) -> pd.DataFrame:
    df = _leer_df(contenido, nombre_archivo)
    df.columns = [c.strip() for c in df.columns]

    missing = {'id', 'proyecto_id', 'fecha'} - set(df.columns)
    if missing:
        raise HTTPException(status_code=400, detail=f"Faltan columnas requeridas: {missing}")

    df['id']          = pd.to_numeric(df['id'],          errors='coerce')
    df['proyecto_id'] = pd.to_numeric(df['proyecto_id'], errors='coerce')
    df = df[df['id'].notna() & df['proyecto_id'].notna()].copy()
    df['id']          = df['id'].astype(int)
    df['proyecto_id'] = df['proyecto_id'].astype(int)
    return df


def _pnum(v) -> float | None:
    try:
        s = str(v).strip().replace(',', '.')
        return float(s) if s else None
    except Exception:
        return None


def _pnum2(v) -> float:
    try:
        return float(str(v).strip().replace(',', '.')) if str(v).strip() else 0.0
    except Exception:
        return 0.0


def _pdate(v):
    try:
        s = str(v).strip()
        return pd.Timestamp(s).date() if s else None
    except Exception:
        return None


def _pint(v) -> int | None:
    try:
        s = str(v).strip()
        return int(float(s)) if s else None
    except Exception:
        return None
