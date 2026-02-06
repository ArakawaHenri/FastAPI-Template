from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Test settings are injected before importing app/main so they override
# any local `.env` values and keep tests deterministic across machines.
_TEST_ROOT = Path(tempfile.mkdtemp(prefix="fastapi-template-tests-"))
os.environ.update(
    {
        "APP_NAME": "fastapi_template_test",
        "APP_VERSION": "1.0.0-test",
        "DEBUG_MODE": "false",
        "RELOAD": "false",
        "CORS_ORIGINS": '["http://testserver"]',
        "LOG_DIR": str(_TEST_ROOT / "logs"),
        "TMP_DIR": str(_TEST_ROOT / "tmp"),
        "TMP_RETENTION_DAYS": "1",
        "TMP_MAX_FILE_SIZE_MB": "16",
        "TMP_MAX_TOTAL_SIZE_MB": "32",
        "DATABASE": "{}",
        "SEMAPHORES": '{"example": 3}',
        "STORE_LMDB__PATH": str(_TEST_ROOT / "store_lmdb"),
        "STORE_LMDB__MAP_SIZE_MB": "16",
        "STORE_LMDB__MAX_VALUE_BYTES": str(4 * 1024 * 1024),
        "STORE_LMDB__MAX_DBS": "256",
    }
)

from app.main import app  # noqa: E402


@pytest.fixture
def client():
    """
    Create a TestClient instance.
    The TestClient context manager triggers the FastAPI lifespan events
    (startup and shutdown), which initializes the ServiceContainer.
    """
    with TestClient(app) as c:
        yield c
