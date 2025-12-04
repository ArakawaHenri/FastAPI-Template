from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="allow",
        env_nested_delimiter="__"
    )

    app_name: str = "fastapi_app"
    app_version: str = "1.0.0"

    LOG_DIR: str = "./log"
    TMP_DIR: str = "./tmp"
    TMP_RETENTION_DAYS: int = 3
    DEBUG_MODE: bool = True

    semaphores: dict[str, int] = Field(default_factory=dict)


settings = Settings()
