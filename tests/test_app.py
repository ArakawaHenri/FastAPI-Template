from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from fastapiex.settings import init_settings


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_app_lifespan_starts_services_and_errors_are_wrapped(tmp_path) -> None:
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(
        (PROJECT_ROOT / "settings.yaml.example")
        .read_text(encoding="utf-8")
        .replace("sqlite+aiosqlite:///./data/app.sqlite3", f"sqlite+aiosqlite:///{tmp_path / 'app.sqlite3'}")
        .replace("./data/store.zlmdb", str(tmp_path / "store.zlmdb"))
        .replace("./temp", str(tmp_path / "temp"))
        .replace("./logs", str(tmp_path / "logs")),
        encoding="utf-8",
    )
    init_settings(settings_path=settings_path)

    from app.main import create_application

    app = create_application()
    with TestClient(app) as client:
        response = client.get("/v1/healthz", headers={"X-Request-ID": "test-request"})
        assert response.status_code == 200
        assert response.headers["X-Request-ID"] == "test-request"
        assert response.json() == {"ok": True}

        put_response = client.put("/v1/kv/demo/key", json={"value": {"ok": True}})
        assert put_response.status_code == 200
        assert put_response.json() == {"stored": True}

        get_response = client.get("/v1/kv/demo/key")
        assert get_response.status_code == 200
        assert get_response.json() == {"found": True, "value": {"ok": True}}

        diagnostics_response = client.get("/v1/diagnostics/dependencies")
        assert diagnostics_response.status_code == 200
        diagnostics = diagnostics_response.json()
        assert diagnostics["database_ok"] is True
        assert diagnostics["database_name"] == "app.sqlite3"
        assert diagnostics["openai_model"] == "gpt-4o-mini"
        assert diagnostics["openai_concurrency_limit"] == 20

        response = client.put("/v1/kv/demo/key", json={"retention_seconds": 0})
        assert response.status_code == 422
        payload = response.json()
        assert payload["error"]["code"] == "VALIDATION_ERROR"
        assert payload["error"]["details"]["validation_errors"]
