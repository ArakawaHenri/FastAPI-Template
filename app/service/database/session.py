from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapiex.di import BaseService, Service
from sqlalchemy.ext.asyncio import AsyncSession

from app.service.database.engine import DatabaseEngineService

logger = logging.getLogger(__name__)


@Service(lifetime="transient")
class DatabaseSessionServiceT(BaseService):
    """Transient transactional AsyncSession service.

    Endpoint usage:
        session: Annotated[
            AsyncSession,
            Inject(AsyncSession, Inject("main_database_service")),
        ]
    """

    @classmethod
    @asynccontextmanager
    async def create(cls, engine: DatabaseEngineService) -> AsyncIterator[AsyncSession]:
        async with engine.async_sessionmaker() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                logger.exception("Database transaction failed")
                raise
            else:
                await session.commit()

