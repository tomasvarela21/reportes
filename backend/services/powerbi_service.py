"""
Servicio para disparar el refresh de un dataset en Power BI Service.
Adaptado de services/powerbi_refresh.py — no importa desde fuera de /backend.
Lee credenciales desde backend/config.py (pydantic-settings), no desde os.getenv.
"""
import logging

import requests

from backend.config import settings

log = logging.getLogger(__name__)

_AUTHORITY_URL   = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
_PBI_SCOPE       = "https://analysis.windows.net/powerbi/api/.default"
_PBI_REFRESH_URL = "https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"


def _verificar_configuracion() -> list[str]:
    """
    Verifica que todas las variables necesarias estén definidas en settings.
    Retorna lista de variables faltantes (vacía si todo está OK).
    """
    requeridas = {
        "POWERBI_TENANT_ID":     settings.powerbi_tenant_id,
        "POWERBI_CLIENT_ID":     settings.powerbi_client_id,
        "POWERBI_CLIENT_SECRET": settings.powerbi_client_secret,
        "POWERBI_USERNAME":      settings.powerbi_username,
        "POWERBI_PASSWORD":      settings.powerbi_password,
        "POWERBI_DATASET_ID":    settings.powerbi_dataset_id,
    }
    return [k for k, v in requeridas.items() if not v]


def _get_access_token() -> str:
    """Obtiene un access token usando ROPC (usuario + contraseña)."""
    resp = requests.post(
        _AUTHORITY_URL.format(tenant_id=settings.powerbi_tenant_id),
        data={
            "grant_type":    "password",
            "client_id":     settings.powerbi_client_id,
            "client_secret": settings.powerbi_client_secret,
            "username":      settings.powerbi_username,
            "password":      settings.powerbi_password,
            "scope":         _PBI_SCOPE,
        },
        timeout=30,
    )

    if resp.status_code != 200:
        error_desc = resp.json().get("error_description", resp.text)
        raise RuntimeError(
            f"Error al obtener token de Power BI: HTTP {resp.status_code} — {error_desc}"
        )

    return resp.json()["access_token"]


def trigger_refresh() -> dict:
    """
    Dispara el refresh del dataset configurado en settings.

    Retorna:
        {"ok": True, "message": "Refresh iniciado"}
      o {"ok": False, "error": "descripción del error"}

    Puede lanzar ServiceUnavailableError (via el router) si faltan variables.
    """
    # Verificar configuración antes de intentar cualquier llamada
    faltantes = _verificar_configuracion()
    if faltantes:
        return {
            "ok":    False,
            "error": f"Variables de entorno no configuradas: {', '.join(faltantes)}",
        }

    try:
        token = _get_access_token()

        resp = requests.post(
            _PBI_REFRESH_URL.format(dataset_id=settings.powerbi_dataset_id),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            },
            json={"notifyOption": "NoNotification"},
            timeout=30,
        )

        # 202 Accepted → refresh iniciado
        if resp.status_code == 202:
            log.info(
                "Refresh de Power BI iniciado para dataset %s",
                settings.powerbi_dataset_id,
            )
            return {"ok": True, "message": "Refresh iniciado. El dataset se actualizará en los próximos minutos."}

        # 400 puede significar que ya hay un refresh en curso
        if resp.status_code == 400:
            detail = resp.json().get("error", {}).get("message", resp.text)
            if "already in progress" in detail.lower() or "in progress" in detail.lower():
                return {
                    "ok":    False,
                    "error": "Ya hay un refresh en curso. Esperá que termine e intentá de nuevo.",
                }
            return {"ok": False, "error": f"Error 400: {detail}"}

        # Otros códigos de error
        detail = ""
        if resp.text:
            try:
                detail = resp.json().get("error", {}).get("message", resp.text)
            except Exception:
                detail = resp.text
        return {"ok": False, "error": f"Error {resp.status_code}: {detail}"}

    except requests.exceptions.Timeout:
        return {
            "ok":    False,
            "error": "Timeout al conectar con Power BI Service. Verificá la conexión.",
        }
    except Exception as e:
        log.error("Error en trigger_refresh: %s", e)
        return {"ok": False, "error": str(e)}
