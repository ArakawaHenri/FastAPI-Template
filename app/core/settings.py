from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    """Configuration for a single database connection."""
    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_recycle: int = 3600  # Connection recycle time (seconds)
    pool_pre_ping: bool = True  # Check connection validity before use


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Allow extra env vars but don't use them
        env_nested_delimiter="__"
    )

    app_name: str = "fastapi_app"
    app_version: str = "1.0.0"

    LOG_DIR: str = "./logs"
    TMP_DIR: str = "./tmp"
    TMP_RETENTION_DAYS: int = 3
    DEBUG_MODE: bool = False
    RELOAD: bool = False
    USE_PROXY_HEADERS: bool = False
    FORWARDED_ALLOW_IPS: str = Field(
        default="127.0.0.1",
        description="Comma-separated list of trusted proxy IPs/hosts for X-Forwarded-*"
    )

    # CORS configuration
    CORS_ORIGINS: list[str] = Field(
        default=[],
        description="Allowed CORS origins"
    )

    # Database configuration (multiple databases supported)
    database: dict[str, DatabaseSettings] = Field(default_factory=dict)

    semaphores: dict[str, int] = Field(default_factory=dict)

    @field_validator('semaphores')
    @classmethod
    def validate_semaphores(cls, v):
        for key, value in v.items():
            if value < 1:
                raise ValueError(f"Semaphore '{key}' must be >= 1, got {value}")
        return v


settings = Settings()
