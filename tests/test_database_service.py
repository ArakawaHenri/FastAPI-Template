from __future__ import annotations

import pytest
from sqlalchemy import text

from app.service.database.engine import DatabaseEngineService
from app.service.database.session import DatabaseSessionServiceT


@pytest.mark.asyncio
async def test_database_engine_health_and_transactional_session(tmp_path) -> None:
    database = DatabaseEngineService(
        url=f"sqlite+aiosqlite:///{tmp_path / 'app.sqlite3'}",
        pool_size=1,
        max_overflow=0,
    )
    try:
        assert await database.is_healthy()

        async with DatabaseSessionServiceT.create(database) as session:
            await session.execute(text("CREATE TABLE example (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
            await session.execute(text("INSERT INTO example (name) VALUES ('alpha')"))

        async with database.engine.connect() as connection:
            value = await connection.scalar(text("SELECT name FROM example WHERE id = 1"))
        assert value == "alpha"
    finally:
        await database.close()

