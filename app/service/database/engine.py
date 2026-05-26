from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

from fastapiex.di import BaseService, ServiceMap
from fastapiex.settings import BaseSettings, GetSettingsMap, SettingsMap
from pydantic import Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

logger = logging.getLogger(__name__)


@SettingsMap("database")
class DatabaseSettings(BaseSettings):
    url: str
    echo: bool = False
    pool_size: int = Field(default=5, ge=1)
    max_overflow: int = Field(default=10, ge=0)
    pool_recycle: int = Field(default=3600, ge=1)
    pool_pre_ping: bool = True


@ServiceMap("{}_database_service", mapping=lambda: GetSettingsMap("database"))
class DatabaseEngineService(BaseService):
    """Async SQLAlchemy engine service, one instance per configured database."""

    @classmethod
    async def create(
        cls,
        url: str,
        echo: bool = False,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True,
    ) -> DatabaseEngineService:
        return cls(
            url=url,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            pool_pre_ping=pool_pre_ping,
        )

    @classmethod
    async def destroy(cls, instance: DatabaseEngineService) -> None:
        await instance.close()

    def __init__(
        self,
        url: str,
        echo: bool = False,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True,
    ) -> None:
        if not url:
            raise ValueError("Database URL must be non-empty")

        self.url = url
        self.db_name = _database_name(url)
        self._ensure_sqlite_parent(url)
        self.engine: AsyncEngine = create_async_engine(
            url,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            pool_pre_ping=pool_pre_ping,
        )
        self.async_sessionmaker = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        logger.debug("Database engine initialized db_name=%s", self.db_name)

    async def close(self) -> None:
        await self.engine.dispose()
        logger.debug("Database engine disposed db_name=%s", self.db_name)

    async def is_healthy(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            logger.exception("Database health check failed db_name=%s", self.db_name)
            return False

    @staticmethod
    def _ensure_sqlite_parent(url: str) -> None:
        sqlite_path = _sqlite_filesystem_path(url)
        if sqlite_path is None:
            return
        sqlite_path.expanduser().parent.mkdir(parents=True, exist_ok=True)


def _database_name(url: str) -> str:
    sqlite_path = _sqlite_filesystem_path(url)
    if sqlite_path is not None:
        return sqlite_path.name

    parsed = urlparse(url)
    if parsed.scheme.startswith("sqlite"):
        return ":memory:"
    if parsed.path:
        return parsed.path.rstrip("/").split("/")[-1]
    return parsed.netloc or parsed.scheme


def _sqlite_filesystem_path(url: str) -> Path | None:
    parsed = urlparse(url)
    if not parsed.scheme.startswith("sqlite"):
        return None

    bare_url = url.split("?", 1)[0]
    if bare_url.endswith(":memory:"):
        return None
    marker = ":///"
    if marker not in bare_url:
        return None
    path_part = bare_url.split(marker, 1)[1]
    if not path_part or path_part == ":memory:":
        return None
    return Path(path_part)
