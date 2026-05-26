from fastapi import APIRouter, Depends, HTTPException
from psycopg2.extras import RealDictCursor

from backend.database import get_conn
from backend.schemas.empresa import EmpresaCreate, EmpresaResponse, EmpresaUpdate

router = APIRouter(prefix="/empresas")


@router.get("", response_model=list[EmpresaResponse])
def listar_empresas(conn=Depends(get_conn)):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT empresa_id, empresa_nombre, grupo, activa "
            "FROM dim_empresa ORDER BY empresa_id"
        )
        return cur.fetchall()


@router.get("/{empresa_id}", response_model=EmpresaResponse)
def obtener_empresa(empresa_id: int, conn=Depends(get_conn)):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT empresa_id, empresa_nombre, grupo, activa "
            "FROM dim_empresa WHERE empresa_id = %s",
            (empresa_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    return row


@router.post("", response_model=EmpresaResponse, status_code=201)
def crear_empresa(body: EmpresaCreate, conn=Depends(get_conn)):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO dim_empresa (empresa_id, empresa_nombre, grupo, activa)
                VALUES (%s, %s, %s, %s)
                RETURNING empresa_id, empresa_nombre, grupo, activa
                """,
                (body.empresa_id, body.empresa_nombre, body.grupo, body.activa),
            )
            row = cur.fetchone()
        conn.commit()
        return row
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/{empresa_id}", response_model=EmpresaResponse)
def actualizar_empresa(empresa_id: int, body: EmpresaUpdate, conn=Depends(get_conn)):
    campos = {k: v for k, v in body.model_dump(exclude_none=True).items()}
    if not campos:
        raise HTTPException(status_code=400, detail="No se enviaron campos a actualizar")

    sets = ", ".join(f"{k} = %s" for k in campos)
    valores = list(campos.values()) + [empresa_id]

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE dim_empresa SET {sets}
                WHERE empresa_id = %s
                RETURNING empresa_id, empresa_nombre, grupo, activa
                """,
                valores,
            )
            row = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))

    if row is None:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    return row
