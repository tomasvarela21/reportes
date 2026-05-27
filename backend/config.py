from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Base de datos ─────────────────────────────────────────────────────────
    database_url: str

    # ── API ───────────────────────────────────────────────────────────────────
    api_key: str = "changeme"
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:3001"]

    # ── Power BI (opcionales — el feature se desactiva si faltan) ─────────────
    powerbi_tenant_id: str | None = None
    powerbi_client_id: str | None = None
    powerbi_client_secret: str | None = None
    powerbi_username: str | None = None
    powerbi_password: str | None = None
    powerbi_dataset_id: str | None = None


settings = Settings()
