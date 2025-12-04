def test_demo_endpoint(client):
    """
    Test the /api/v1/demo/ endpoint.
    This verifies that the dependency injection (Singleton, Transient, Generator) works correctly.
    """
    response = client.get("/api/v1/demo/")
    assert response.status_code == 200

    data = response.json()

    # Verify transient service output
    assert data["msg_from_example_transient"] == "Hello!"

    # Verify generator service output
    assert data["index_of_item_enumerated_from_example_generator"] == "Hello from example generator!"
