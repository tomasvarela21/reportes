"""
frontend/services/api_client.py
Cliente HTTP centralizado para la API de ReporteApp.
Todas las llamadas van a API_BASE_URL con header X-API-Key automático.
"""
import os
import streamlit as st
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL: str = os.getenv("API_BASE_URL", "https://reporteapp-api.onrender.com").rstrip("/")
API_KEY: str = os.getenv("API_KEY", "")

# Mapa local para detección de empresa desde nombre de archivo
EMPRESAS: dict[str, int] = {
    "BATIA": 1, "NORFORK": 2, "GUARE": 3, "TORRES": 4, "WERCOLICH": 5,
}

_TIMEOUT = 30
_UPLOAD_TIMEOUT = 120

# ── CSS ────────────────────────────────────────────────────────────────────────

_SIDEBAR_CSS = """
[data-testid="stSidebar"] { background-color: #1a1f2e; }
[data-testid="stSidebar"] * { color: #e0e4ef !important; }
/* Contraer el nav automático sin usar display:none (evita auto-collapse de Streamlit) */
[data-testid="stSidebarNav"] { max-height: 0 !important; overflow: hidden !important; }
"""

_GLOBAL_CSS = """
[data-testid="stDecoration"] { display: none; }
footer                       { visibility: hidden; }
[data-testid="manage-app-button"] { display: none !important; }

[data-testid="stAppViewContainer"] > .main > .block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    max-width: 100%;
}

h1, h2, h3 { color: #1a1f2e; }

.reporte-card {
    border-radius: 12px;
    padding: 20px 16px;
    text-align: center;
    min-height: 130px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 6px;
}
.reporte-card .card-icon  { font-size: 1.8rem; }
.reporte-card .card-title { font-weight: 700; font-size: 0.9rem; }
.reporte-card .card-desc  { color: #6b7280; font-size: 0.75rem; }
"""

_NAV_LINKS = [
    ("🏠", "Home",              "Home.py"),
    ("📤", "Carga Diario",      "pages/1_Carga_Diario.py"),
    ("📚", "Libro Mayor",       "pages/2_Libro_Mayor.py"),
    ("📋", "Consulta Diario",   "pages/3_Consulta_Diario.py"),
    ("🗂️", "Saldos Apertura",   "pages/4_Saldos_Apertura.py"),
    ("⚙️", "Administración",    "pages/5_Administracion.py"),
    ("🔍", "Data Check",        "pages/6_Chequeo_Consistencia.py"),
]


# ── UI helpers ─────────────────────────────────────────────────────────────────

def apply_styles(extra_css: str = "") -> None:
    st.markdown(
        f"<style>{_SIDEBAR_CSS}{_GLOBAL_CSS}{extra_css}</style>",
        unsafe_allow_html=True,
    )


def render_sidebar() -> None:
    with st.sidebar:
        st.markdown("## 📊 ReporteApp")
        st.markdown("**v2.0** · Grupo Corporativo")
        st.divider()
        for _icon, label, path in _NAV_LINKS:
            try:
                st.page_link(path, label=f"{_icon} {label}")
            except Exception:
                st.caption(f"⚠️ {label}")
        st.divider()
        st.markdown("""
        **Empresas activas:**
        - BATIA · GUARE · NORFORK
        - TORRES · WERCOLICH
        """)
        st.divider()
        st.caption(f"🔗 API: `{API_BASE_URL}`")


# ── HTTP core ──────────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {"X-API-Key": API_KEY}


def _handle(resp: requests.Response):
    if not resp.ok:
        try:
            detail = resp.json().get("detail", resp.text)
        except Exception:
            detail = resp.text
        raise RuntimeError(f"Error {resp.status_code}: {detail}")
    if resp.status_code == 204:
        return {}
    return resp.json()


def get(path: str, params: dict | None = None):
    resp = requests.get(
        f"{API_BASE_URL}/api/v1{path}",
        headers=_headers(),
        params={k: v for k, v in (params or {}).items() if v is not None},
        timeout=_TIMEOUT,
    )
    return _handle(resp)


def post(path: str, json: dict | None = None):
    resp = requests.post(
        f"{API_BASE_URL}/api/v1{path}",
        headers=_headers(),
        json=json,
        timeout=_TIMEOUT,
    )
    return _handle(resp)


def post_file(path: str, files: dict, data: dict | None = None):
    resp = requests.post(
        f"{API_BASE_URL}/api/v1{path}",
        headers=_headers(),
        files=files,
        data={k: v for k, v in (data or {}).items() if v is not None},
        timeout=_UPLOAD_TIMEOUT,
    )
    return _handle(resp)


def delete(path: str):
    resp = requests.delete(
        f"{API_BASE_URL}/api/v1{path}",
        headers=_headers(),
        timeout=_TIMEOUT,
    )
    return _handle(resp)


# ── Empresas ───────────────────────────────────────────────────────────────────

def get_empresas() -> list:
    return get("/empresas")


# ── Cuentas ────────────────────────────────────────────────────────────────────

def get_cuentas(filtros: dict | None = None) -> list:
    return get("/cuentas", params=filtros)


def get_cuenta(nro_cta: int) -> dict:
    return get(f"/cuentas/{nro_cta}")


def get_rubros() -> list:
    return get("/cuentas/rubros")


def get_movimientos_cuenta(nro_cta: int) -> dict:
    return get(f"/cuentas/{nro_cta}/movimientos")


def crear_cuenta(data: dict) -> dict:
    return post("/cuentas", json=data)


def actualizar_cuenta(nro_cta: int, data: dict) -> dict:
    resp = requests.put(
        f"{API_BASE_URL}/api/v1/cuentas/{nro_cta}",
        headers=_headers(),
        json=data,
        timeout=_TIMEOUT,
    )
    return _handle(resp)


def eliminar_cuenta(nro_cta: int) -> dict:
    return delete(f"/cuentas/{nro_cta}")


def upload_plan_cuentas(file_bytes: bytes, filename: str) -> dict:
    return post_file(
        "/cuentas/upload",
        files={"file": (filename, file_bytes, "text/csv")},
    )


# ── Centros de costo ───────────────────────────────────────────────────────────

def get_centros_costo() -> list:
    return get("/centros-costo")


def crear_centro_costo(data: dict) -> dict:
    return post("/centros-costo", json=data)


# ── Diario ─────────────────────────────────────────────────────────────────────

def validar_diario(file_bytes: bytes, filename: str, empresa_nombre: str) -> dict:
    return post_file(
        "/diario/validate",
        files={"file": (filename, file_bytes, "text/csv")},
        data={"empresa_nombre": empresa_nombre},
    )


def upload_diario(
    file_bytes: bytes,
    filename: str,
    empresa_nombre: str,
    periodos_json: str,
) -> dict:
    return post_file(
        "/diario/upload",
        files={"file": (filename, file_bytes, "text/csv")},
        data={"empresa_nombre": empresa_nombre, "periodos_json": periodos_json},
    )


def get_periodos_diario(empresa_id: int) -> list:
    return get(f"/diario/periodos/{empresa_id}")


def get_diario(empresa_id: int, filtros: dict | None = None) -> dict:
    return get(f"/diario/{empresa_id}", params=filtros)


# ── Mayor ──────────────────────────────────────────────────────────────────────

def get_mayor(empresa_id: int, filtros: dict | None = None) -> dict:
    return get(f"/mayor/{empresa_id}", params=filtros)


def get_mayor_resumen(empresa_id: int, filtros: dict | None = None) -> list:
    return get(f"/mayor/{empresa_id}/resumen", params=filtros)


def get_mayor_periodos(empresa_id: int) -> list:
    return get(f"/mayor/{empresa_id}/periodos")


def get_recalculos() -> list:
    return get("/mayor/recalculos")


# ── Proyectos ──────────────────────────────────────────────────────────────────

def get_proyectos(incluir_inactivos: bool = False) -> list:
    return get("/proyectos", params={"incluir_inactivos": incluir_inactivos})


def upload_proyectos(file_bytes: bytes, filename: str) -> dict:
    return post_file(
        "/proyectos/upload",
        files={"file": (filename, file_bytes, "text/csv")},
    )


def get_presupuestos(proyecto_id: int | None = None) -> list:
    params = {"proyecto_id": proyecto_id} if proyecto_id is not None else None
    return get("/presupuestos", params=params)


def upload_presupuestos(file_bytes: bytes, filename: str) -> dict:
    return post_file(
        "/presupuestos/upload",
        files={"file": (filename, file_bytes, "text/csv")},
    )


# ── Apertura ───────────────────────────────────────────────────────────────────

def upload_apertura(
    file_bytes: bytes,
    filename: str,
    empresa_nombre: str | None,
    anio_fiscal: int,
) -> dict:
    data: dict = {"anio_fiscal": str(anio_fiscal)}
    if empresa_nombre:
        data["empresa_nombre"] = empresa_nombre
    return post_file(
        "/apertura/upload",
        files={"file": (filename, file_bytes, "text/csv")},
        data=data,
    )


def get_apertura(empresa_id: int, filtros: dict | None = None) -> dict:
    return get(f"/apertura/{empresa_id}", params=filtros)


def get_apertura_stats(empresa_id: int, anio_fiscal: int) -> dict:
    return get(f"/apertura/{empresa_id}/{anio_fiscal}/stats")


# ── Consistencia ───────────────────────────────────────────────────────────────

def upload_consistencia(files: list[tuple[bytes, str]]) -> dict:
    """files: lista de (bytes, nombre_archivo)"""
    files_param = [
        ("files", (nombre, contenido, "text/csv"))
        for contenido, nombre in files
    ]
    resp = requests.post(
        f"{API_BASE_URL}/api/v1/consistencia/upload",
        headers=_headers(),
        files=files_param,
        timeout=_UPLOAD_TIMEOUT,
    )
    return _handle(resp)


def get_consistencia_estado() -> dict:
    return get("/consistencia/estado")


def comparar_consistencia() -> dict:
    return post("/consistencia/comparar")


def limpiar_consistencia_staging() -> dict:
    return delete("/consistencia/staging")


# ── Power BI ───────────────────────────────────────────────────────────────────

def refresh_powerbi() -> dict:
    return post("/powerbi/refresh")
