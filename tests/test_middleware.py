"""Tests for middleware functionality"""
from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from fastapi import BackgroundTasks, FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from starlette.types import Message

from app.core.dependencies import (
    REQUEST_FAILED_STATE_KEY,
    Inject,
    ServiceContainer,
    ServiceLifetime,
    TransientServiceFinalizerMiddleware,
    get_or_create_service_container_registry,
)
from app.core.logger import request_id_ctx
from app.core.settings import settings
from app.middleware.exception import NotFoundException, validation_exception_handler
from app.middleware.logging import RequestLoggingMiddleware


class TestRequestLoggingMiddleware:
    """Tests for RequestLoggingMiddleware"""

    def test_request_id_generated_when_not_provided(self, client):
        """Test that a request ID is generated when not provided in headers"""
        response = client.get("/api/")
        assert response.status_code == 200

        # Verify X-Request-ID is in response
        assert "X-Request-ID" in response.headers
        request_id = response.headers["X-Request-ID"]

        # Should be a valid UUID format (36 chars with hyphens)
        assert len(request_id) == 36
        assert request_id.count("-") == 4

    def test_request_id_preserved_when_provided(self, client):
        """Test that provided X-Request-ID is preserved"""
        custom_id = "test-request-id-12345"
        response = client.get("/api/", headers={"X-Request-ID": custom_id})
        assert response.status_code == 200

        # Verify the same ID is returned
        assert response.headers["X-Request-ID"] == custom_id

    def test_request_id_unique_per_request(self, client):
        """Test that each request without X-Request-ID gets a unique ID"""
        response1 = client.get("/api/")
        response2 = client.get("/api/")

        id1 = response1.headers.get("X-Request-ID")
        id2 = response2.headers.get("X-Request-ID")

        assert id1 is not None
        assert id2 is not None
        assert id1 != id2

    def test_request_id_replaced_when_header_contains_invalid_chars(self, client):
        response = client.get("/api/", headers={"X-Request-ID": "bad id\r\nx"})
        assert response.status_code == 200
        request_id = response.headers["X-Request-ID"]
        assert len(request_id) == 36
        assert request_id.count("-") == 4

    def test_request_id_replaced_when_header_too_long(self, client):
        response = client.get("/api/", headers={"X-Request-ID": "a" * 256})
        assert response.status_code == 200
        request_id = response.headers["X-Request-ID"]
        assert len(request_id) == 36
        assert request_id.count("-") == 4

    @pytest.mark.asyncio
    async def test_request_id_context_is_reset_after_dispatch(self):
        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok", "more_body": False})

        middleware = RequestLoggingMiddleware(app=app)
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "path": "/api/",
            "query_string": b"",
            "headers": [(b"x-request-id", b"ctx-reset-id")],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "state": {},
        }
        request_messages = [
            {"type": "http.request", "body": b"", "more_body": False},
        ]
        response_messages: list[Message] = []

        seed = request_id_ctx.set("outer-scope")
        try:
            async def receive():
                if request_messages:
                    return request_messages.pop(0)
                return {"type": "http.disconnect"}

            async def send(message: Message):
                response_messages.append(message)

            await middleware(scope, receive, send)
            assert request_id_ctx.get() == "outer-scope"
            assert scope["state"]["request_id"] == "ctx-reset-id"

            response_start = next(
                msg for msg in response_messages if msg["type"] == "http.response.start"
            )
            headers = dict(response_start["headers"])
            assert headers[b"x-request-id"] == b"ctx-reset-id"
        finally:
            request_id_ctx.reset(seed)

    @pytest.mark.asyncio
    async def test_failed_request_completion_logs_warning(self, monkeypatch):
        records: list[tuple[str, str, dict[str, object] | None]] = []

        def _capture(level: str):
            def _log(message: str, *args, extra=None, **kwargs):
                _ = args, kwargs
                records.append((level, message, extra))

            return _log

        monkeypatch.setattr("app.middleware.logging.logger.info", _capture("info"))
        monkeypatch.setattr("app.middleware.logging.logger.warning", _capture("warning"))
        monkeypatch.setattr("app.middleware.logging.logger.error", _capture("error"))

        async def app(scope, receive, send):
            scope["state"][REQUEST_FAILED_STATE_KEY] = True
            await send({"type": "http.response.start", "status": 422, "headers": []})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        middleware = RequestLoggingMiddleware(app=app)
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "path": "/api/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "state": {},
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(_message: Message):
            return None

        await middleware(scope, receive, send)

        failure_records = [r for r in records if r[1] == "Request failed: GET /api/"]
        assert len(failure_records) == 1
        level, _, extra = failure_records[0]
        assert level == "warning"
        assert extra is not None
        assert extra["status_code"] == 422
        assert extra["request_failed"] is True

    @pytest.mark.asyncio
    async def test_server_error_completion_logs_error(self, monkeypatch):
        records: list[tuple[str, str, dict[str, object] | None]] = []

        def _capture(level: str):
            def _log(message: str, *args, extra=None, **kwargs):
                _ = args, kwargs
                records.append((level, message, extra))

            return _log

        monkeypatch.setattr("app.middleware.logging.logger.info", _capture("info"))
        monkeypatch.setattr("app.middleware.logging.logger.warning", _capture("warning"))
        monkeypatch.setattr("app.middleware.logging.logger.error", _capture("error"))

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500, "headers": []})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        middleware = RequestLoggingMiddleware(app=app)
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "path": "/api/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "state": {},
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(_message: Message):
            return None

        await middleware(scope, receive, send)

        failure_records = [r for r in records if r[1] == "Request failed: GET /api/"]
        assert len(failure_records) == 1
        level, _, extra = failure_records[0]
        assert level == "error"
        assert extra is not None
        assert extra["status_code"] == 500
        assert extra["request_failed"] is True

    @pytest.mark.asyncio
    async def test_successful_request_completion_logs_info(self, monkeypatch):
        records: list[tuple[str, str, dict[str, object] | None]] = []

        def _capture(level: str):
            def _log(message: str, *args, extra=None, **kwargs):
                _ = args, kwargs
                records.append((level, message, extra))

            return _log

        monkeypatch.setattr("app.middleware.logging.logger.info", _capture("info"))
        monkeypatch.setattr("app.middleware.logging.logger.warning", _capture("warning"))
        monkeypatch.setattr("app.middleware.logging.logger.error", _capture("error"))

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        middleware = RequestLoggingMiddleware(app=app)
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "path": "/api/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "state": {},
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(_message: Message):
            return None

        await middleware(scope, receive, send)

        completed_records = [r for r in records if r[1] == "Request completed: GET /api/"]
        assert len(completed_records) == 1
        level, _, extra = completed_records[0]
        assert level == "info"
        assert extra is not None
        assert extra["status_code"] == 200
        assert extra["request_failed"] is False


class TestTransientServiceFinalizerMiddleware:
    """Tests for TransientServiceFinalizerMiddleware"""

    def test_transient_service_finalizer_runs(self, client):
        """Test that transient service finalizers are executed after request"""
        # The example endpoint uses transient services
        response = client.get("/api/v1/example/")
        assert response.status_code == 200

        # If finalizers didn't run properly, we'd see leaked resources
        # or errors in subsequent requests
        response2 = client.get("/api/v1/example/")
        assert response2.status_code == 200

    def test_generator_service_cleanup(self, client):
        """Test that contextmanager-style transient services are cleaned up"""
        # ExampleGeneratorServiceT uses async contextmanager-style pattern
        response = client.get("/api/v1/example/")
        assert response.status_code == 200

        data = response.json()
        assert "index_of_item_enumerated_from_example_generator" in data

    def test_finalizer_runs_after_background_tasks(self):
        call_order: list[str] = []

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            services = ServiceContainer()
            registry = get_or_create_service_container_registry(app.state)
            registry.register_current(services)

            async def factory():
                try:
                    yield {"ok": True}
                finally:
                    call_order.append("finalizer")

            await services.register("transient", ServiceLifetime.TRANSIENT, factory, None)
            try:
                yield
            finally:
                await services.destruct_all_singletons()
                registry.unregister_current(expected=services)

        app = FastAPI(lifespan=lifespan)
        app.add_middleware(TransientServiceFinalizerMiddleware)

        @app.get("/order")
        async def order_endpoint(
            background_tasks: BackgroundTasks,
            _svc=Inject("transient"),
        ):
            background_tasks.add_task(call_order.append, "background")
            call_order.append("handler")
            return {"ok": True}

        with TestClient(app) as local_client:
            response = local_client.get("/order")
            assert response.status_code == 200

        assert call_order == ["handler", "background", "finalizer"]


class TestExceptionMiddleware:
    """Tests for exception handling middleware"""

    def test_validation_error_format(self, client):
        """Test that validation errors return proper format"""
        # This endpoint expects path parameter validation
        response = client.get("/api/v1/example/users/invalid")
        # Should get a validation error or 404
        assert response.status_code in (404, 422)

    def test_not_found_error_format(self, client):
        """Test that 404 errors return unified format"""
        response = client.get("/nonexistent/path")
        assert response.status_code == 404

        data = response.json()
        assert "error" in data
        assert "code" in data["error"]
        assert data["error"]["code"] == "NOT_FOUND"

    def test_not_found_exception_keeps_zero_identifier(self):
        exc = NotFoundException("User", 0)
        assert exc.message == "User not found: 0"
        assert exc.error_code == "NOT_FOUND"
        assert exc.details["identifier"] == "0"

    def test_validation_error_debug_body_bytes_are_serializable(self, monkeypatch):
        monkeypatch.setattr(settings, "debug_mode", True)

        app = FastAPI()
        app.add_exception_handler(
            RequestValidationError, validation_exception_handler
        )

        @app.post("/payload")
        async def create_payload(payload: dict):
            return payload

        with TestClient(app, raise_server_exceptions=False) as local_client:
            response = local_client.post(
                "/payload",
                content=b'{"bad_json":',
                headers={"Content-Type": "application/json"},
            )

        assert response.status_code == 422
        data = response.json()
        assert data["error"]["code"] == "VALIDATION_ERROR"
        assert isinstance(data["error"]["details"]["body"], str)
