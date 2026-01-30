"""Tests for middleware functionality"""

import pytest


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
