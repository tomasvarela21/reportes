from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str
    api_key: str = "changeme"
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:3001"]


settings = Settings()
