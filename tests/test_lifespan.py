from __future__ import annotations

import pytest
from fastapi import FastAPI

from app.lifespan.main import lifespan


@pytest.mark.asyncio
async def test_lifespan_startup_error_is_not_masked(monkeypatch):
    def fail_setup_logging(*args, **kwargs):
        raise RuntimeError("setup logging failed")

    monkeypatch.setattr("app.lifespan.main.setup_logging", fail_setup_logging)

    app = FastAPI()
    cm = lifespan(app)

    with pytest.raises(RuntimeError, match="setup logging failed"):
        await cm.__aenter__()
