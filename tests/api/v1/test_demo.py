def test_example_endpoint(client):
    """
    Test the /api/v1/example/ endpoint.
    This verifies that the dependency injection (Singleton, Transient, Generator) works correctly.
    """
    response = client.get("/api/v1/example/")
    assert response.status_code == 200

    data = response.json()

    # Verify transient service output
    assert data["msg_from_example_transient"] == "Hello!"

    # Verify generator service output
    assert data["index_of_item_enumerated_from_example_generator"] == "Hello from example generator!"


def test_root_endpoint(client):
    """Test the root endpoint at /api/"""
    response = client.get("/api/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}


def test_root_endpoint_has_request_id(client):
    """Test that root endpoint includes X-Request-ID header"""
    response = client.get("/api/")
    assert response.status_code == 200
    assert "X-Request-ID" in response.headers


def test_invalid_endpoint(client):
    """Test that invalid endpoints return 404"""
    response = client.get("/this/does/not/exist")
    assert response.status_code == 404
