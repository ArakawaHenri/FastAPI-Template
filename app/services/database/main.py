from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator
from urllib.parse import urlparse

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.services import BaseService


class DatabaseEngineService(BaseService):
    """
    Async database engine service with connection pooling.

    Registered as a singleton; the engine is disposed on shutdown.
    Use `get_session()` context manager for transactional operations.
    """

    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(
            url: str,
            pool_size: int = 5,
            max_overflow: int = 10,
            pool_recycle: int = 3600,
            pool_pre_ping: bool = True
        ) -> "DatabaseEngineService":
            return DatabaseEngineService(
                url=url,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=pool_recycle,
                pool_pre_ping=pool_pre_ping
            )

        @staticmethod
        async def dtor(instance: "DatabaseEngineService") -> None:
            await instance.engine.dispose()
            logger.debug("[DB] Database engine disposed", db_name=instance.db_name)

    def __init__(
        self,
        url: str,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True
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
        self.pool_recycle = pool_recycle
        self.pool_pre_ping = pool_pre_ping
        self.db_name = db_name

        self.engine: AsyncEngine = create_async_engine(
            self.url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_recycle=self.pool_recycle,
            pool_pre_ping=self.pool_pre_ping,
            echo=False,
        )
        self.async_sessionmaker = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        logger.debug("[DB] Database engine created", db_name=self.db_name)

    @asynccontextmanager
    async def get_session(self) -> AsyncIterator[AsyncSession]:
        """Get a database session with automatic commit/rollback."""
        async with self.async_sessionmaker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                logger.exception("Database transaction failed")
                raise
            else:
                await session.commit()

    async def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            logger.warning("[DB] Database health check failed", db_name=self.db_name)
            return False


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

    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(engine: DatabaseEngineService) -> AsyncIterator[AsyncSession]:
            """
            Async contextmanager-style factory for database sessions.

            The engine is injected via nested Inject() at endpoint resolution.
            """
            async with engine.async_sessionmaker() as session:
                try:
                    yield session
                except Exception:
                    await session.rollback()
                    logger.exception("Database transaction failed")
                    raise
                else:
                    await session.commit()
