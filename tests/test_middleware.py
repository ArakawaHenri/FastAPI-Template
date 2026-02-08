"""Tests for middleware functionality"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import Response

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
        middleware = RequestLoggingMiddleware(app=lambda scope, receive, send: None)
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
        }
        request = Request(scope)

        seed = request_id_ctx.set("outer-scope")
        try:
            async def call_next(_request):
                return Response(content="ok")

            response = await middleware.dispatch(request, call_next)

            assert response.status_code == 200
            assert request_id_ctx.get() == "outer-scope"
        finally:
            request_id_ctx.reset(seed)


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
        """Test that generator-based transient services are cleaned up"""
        # ExampleGeneratorServiceT uses async generator pattern
        response = client.get("/api/v1/example/")
        assert response.status_code == 200

        data = response.json()
        assert "index_of_item_enumerated_from_example_generator" in data


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
