import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    """
    Create a TestClient instance.
    The TestClient context manager triggers the FastAPI lifespan events
    (startup and shutdown), which initializes the ServiceContainer.
    """
    with TestClient(app) as c:
        yield c
