from __future__ import annotations

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    """Configuration for a single database connection."""
    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_recycle: int = 3600  # Connection recycle time (seconds)
    pool_pre_ping: bool = True  # Check connection validity before use


class StoreLMDBSettings(BaseModel):
    """Configuration for LMDB key-value store."""
    path: str = "./store_lmdb"
    map_size_mb: int = 1024
    map_size_growth_factor: int = 2
    map_high_watermark: float = 0.9
    max_dbs: int = 256
    max_readers: int = 512
    sync: bool = False
    metasync: bool = True
    writemap: bool = True
    map_async: bool = True
    max_key_bytes: int = 256
    max_namespace_bytes: int = 256
    max_value_bytes: int = 100 * 1024 * 1024
    cleanup_max_deletes: int = 100_000
    # Size of the fixed callback execution thread pool.
    callback_worker_threads: int = Field(default=4, ge=1)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Allow extra env vars but don't use them
        env_nested_delimiter="__"
    )

    app_name: str = "fastapi_app"
    app_version: str = "1.0.0"

    log_dir: str = "./logs"
    tmp_dir: str = "./tmp"
    tmp_retention_days: int = 3
    tmp_cleanup_interval_seconds: int = 60
    tmp_total_size_recalc_seconds: int = 3600
    # Size of TempFileService fixed thread pool.
    tmp_worker_threads: int = Field(default=4, ge=1)
    tmp_max_file_size_mb: int = 1024
    tmp_max_total_size_mb: int = 0
    debug_mode: bool = False
    reload: bool = False

    # CORS configuration
    cors_origins: list[str] = Field(
        default=[],
        description="Allowed CORS origins"
    )

    # Database configuration (multiple databases supported)
    database: dict[str, DatabaseSettings] = Field(default_factory=dict)

    semaphores: dict[str, int] = Field(default_factory=dict)

    # Store (zLMDB key-value) configuration
    store_lmdb: StoreLMDBSettings = Field(default_factory=StoreLMDBSettings)

    @field_validator('semaphores')
    @classmethod
    def validate_semaphores(cls, v):
        normalized: dict[str, int] = {}
        for key, value in v.items():
            normalized_key = key.lower()
            if value < 1:
                raise ValueError(
                    f"Semaphore '{key}' must be >= 1, got {value}")
            normalized[normalized_key] = value
        return normalized

    @field_validator("database")
    @classmethod
    def normalize_database_keys(cls, v):
        return {key.lower(): value for key, value in v.items()}


settings = Settings()
