from __future__ import annotations

from collections.abc import AsyncIterator
from urllib.parse import urlparse

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.settings import settings
from app.services import BaseService, Service, ServiceDict


@ServiceDict("{}_database_service", dict=settings.database)
class DatabaseEngineService(BaseService):
    """
    Async database engine service with connection pooling.

    Registered as a singleton; the engine is disposed on shutdown.
    """

    @classmethod
    async def create(
        cls,
        url: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True,
        pool_use_lifo: bool = False,
    ) -> DatabaseEngineService:
        return cls(
            url=url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=pool_pre_ping,
            pool_use_lifo=pool_use_lifo,
        )

    @classmethod
    async def destroy(cls, instance: DatabaseEngineService) -> None:
        _ = cls
        await instance.engine.dispose()
        logger.debug("[DB] Database engine disposed", db_name=instance.db_name)

    def __init__(
        self,
        url: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True,
        pool_use_lifo: bool = False,
    ) -> None:
        if not url:
            raise ValueError("Invalid database URL: empty")

        parsed = urlparse(url)
        db_name = parsed.path.rstrip("/").split("/")[-1] if parsed.path else ""
        if not db_name:
            raise ValueError(
                f"Invalid database URL: missing database name ({url!r})")

        self.url = url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.pool_pre_ping = pool_pre_ping
        self.pool_use_lifo = pool_use_lifo
        self.db_name = db_name

        self.engine: AsyncEngine = create_async_engine(
            self.url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_timeout=self.pool_timeout,
            pool_recycle=self.pool_recycle,
            pool_pre_ping=self.pool_pre_ping,
            pool_use_lifo=self.pool_use_lifo,
            echo=False,
        )
        self.async_sessionmaker = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        logger.debug("[DB] Database engine created", db_name=self.db_name)

    async def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            logger.warning("[DB] Database health check failed", db_name=self.db_name)
            return False


@Service(
    "database_session_service",
    lifetime="transient",
    exposed_type=AsyncSession,
)
class DatabaseSessionServiceT(BaseService):
    """
    Transient database session service.

    Yields an AsyncSession with automatic commit/rollback.

    Usage in endpoints:
        session: AsyncSession = Inject(AsyncSession, Inject("main_database_service"))

    Note:
        ServiceContainer infers the injectable service type from the
        factory return annotation. Since ctor returns
        AsyncIterator[AsyncSession], type-based injection target
        is AsyncSession (not DatabaseSessionServiceT).
    """

    @classmethod
    async def create(cls, engine: DatabaseEngineService) -> AsyncIterator[AsyncSession]:
        """
        Async contextmanager-style factory for database sessions.

        The engine is injected via nested Inject() at endpoint resolution.
        """
        _ = cls
        async with engine.async_sessionmaker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                logger.exception("Database transaction failed")
                raise
            else:
                await session.commit()
