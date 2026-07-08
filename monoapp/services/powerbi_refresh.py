"""
powerbi_refresh.py
==================
Servicio para disparar el refresh de un dataset en Power BI Service
usando autenticación ROPC (Resource Owner Password Credentials).
"""

import os
import logging
import requests

log = logging.getLogger(__name__)

AUTHORITY_URL   = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
PBI_SCOPE       = "https://analysis.windows.net/powerbi/api/.default"
PBI_REFRESH_URL = "https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"


def _get_access_token() -> str:
    """Obtiene un access token usando ROPC (usuario + contraseña)."""
    tenant_id     = os.getenv("POWERBI_TENANT_ID")
    client_id     = os.getenv("POWERBI_CLIENT_ID")
    client_secret = os.getenv("POWERBI_CLIENT_SECRET")
    username      = os.getenv("POWERBI_USERNAME")
    password      = os.getenv("POWERBI_PASSWORD")

    missing = [k for k, v in {
        "POWERBI_TENANT_ID":     tenant_id,
        "POWERBI_CLIENT_ID":     client_id,
        "POWERBI_CLIENT_SECRET": client_secret,
        "POWERBI_USERNAME":      username,
        "POWERBI_PASSWORD":      password,
    }.items() if not v]

    if missing:
        raise ValueError(f"Variables de entorno faltantes: {', '.join(missing)}")

    resp = requests.post(
        AUTHORITY_URL.format(tenant_id=tenant_id),
        data={
            "grant_type":    "password",
            "client_id":     client_id,
            "client_secret": client_secret,
            "username":      username,
            "password":      password,
            "scope":         PBI_SCOPE,
        },
        timeout=30,
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Error al obtener token: {resp.status_code} — {resp.json().get('error_description', resp.text)}"
        )

    return resp.json()["access_token"]


def trigger_refresh() -> dict:
    """
    Dispara el refresh del dataset configurado en .env.
    Retorna: {"ok": True} o {"ok": False, "error": "mensaje"}
    """
    dataset_id = os.getenv("POWERBI_DATASET_ID")
    if not dataset_id:
        return {"ok": False, "error": "POWERBI_DATASET_ID no configurado en .env"}

    try:
        token = _get_access_token()
        resp  = requests.post(
            PBI_REFRESH_URL.format(dataset_id=dataset_id),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/json",
            },
            json={"notifyOption": "NoNotification"},
            timeout=30,
        )

        # 202 Accepted = refresh iniciado correctamente
        if resp.status_code == 202:
            log.info(f"Refresh de Power BI iniciado para dataset {dataset_id}")
            return {"ok": True}

        # 400 puede significar que ya hay un refresh en curso
        if resp.status_code == 400:
            detail = resp.json().get("error", {}).get("message", resp.text)
            if "already in progress" in detail.lower() or "in progress" in detail.lower():
                return {"ok": False, "error": "Ya hay un refresh en curso. Esperá que termine e intentá de nuevo."}
            return {"ok": False, "error": f"Error 400: {detail}"}

        detail = resp.json().get("error", {}).get("message", resp.text) if resp.text else str(resp.status_code)
        return {"ok": False, "error": f"Error {resp.status_code}: {detail}"}

    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except requests.exceptions.Timeout:
        return {"ok": False, "error": "Timeout al conectar con Power BI Service. Verificá la conexión."}
    except Exception as e:
        log.error(f"Error en trigger_refresh: {e}")
        return {"ok": False, "error": str(e)}