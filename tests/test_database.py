from __future__ import annotations

from app.services.database import main as database_module


class _FakeEngine:
    async def dispose(self) -> None:
        return None


def test_database_engine_forwards_pool_config_to_sqlalchemy(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_async_engine(url: str, **kwargs: object) -> _FakeEngine:
        captured["url"] = url
        captured["engine_kwargs"] = kwargs
        return _FakeEngine()

    def fake_async_sessionmaker(*, bind: object, class_: object, expire_on_commit: bool) -> object:
        captured["sessionmaker_bind"] = bind
        captured["sessionmaker_class"] = class_
        captured["sessionmaker_expire_on_commit"] = expire_on_commit
        return object()

    monkeypatch.setattr(database_module, "create_async_engine", fake_create_async_engine)
    monkeypatch.setattr(database_module, "async_sessionmaker", fake_async_sessionmaker)

    service = database_module.DatabaseEngineService(
        url="mysql+aiomysql://root:password@127.0.0.1:3306/app_db",
        pool_size=7,
        max_overflow=9,
        pool_timeout=13,
        pool_recycle=77,
        pool_pre_ping=False,
        pool_use_lifo=True,
    )

    assert captured["url"] == "mysql+aiomysql://root:password@127.0.0.1:3306/app_db"
    assert captured["engine_kwargs"] == {
        "pool_size": 7,
        "max_overflow": 9,
        "pool_timeout": 13,
        "pool_recycle": 77,
        "pool_pre_ping": False,
        "pool_use_lifo": True,
        "echo": False,
    }
    assert captured["sessionmaker_bind"] is service.engine
    assert captured["sessionmaker_expire_on_commit"] is False
