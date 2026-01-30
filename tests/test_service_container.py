import os
from typing import AsyncGenerator

import pytest

from app.core.dependencies import ServiceContainer, ServiceLifetime


@pytest.mark.asyncio
async def test_singleton_lifecycle():
    """Test that singleton services are created only once"""
    container = ServiceContainer()

    call_count = 0

    async def factory():
        nonlocal call_count
        call_count += 1
        return {"instance": call_count}

    await container.register("test", ServiceLifetime.SINGLETON, factory, None)

    instance1 = await container.aget_by_key("test")
    instance2 = await container.aget_by_key("test")

    assert instance1 is instance2
    assert call_count == 1
    assert instance1["instance"] == 1


@pytest.mark.asyncio
async def test_transient_lifecycle():
    """Test that transient services create new instances each time"""
    container = ServiceContainer()

    call_count = 0

    async def factory():
        nonlocal call_count
        call_count += 1
        return {"instance": call_count}

    await container.register("test", ServiceLifetime.TRANSIENT, factory, None)

    # Transient services create new instances
    instance1 = await container.aget_by_key("test")
    instance2 = await container.aget_by_key("test")

    assert instance1 is not instance2
    assert call_count == 2


@pytest.mark.asyncio
async def test_type_based_injection():
    """Test type-based service injection"""
    container = ServiceContainer()

    class MyService:
        value: int = 42

    async def factory() -> MyService:
        return MyService()

    # Anonymous registration (accessible only by type)
    await container.register(None, ServiceLifetime.SINGLETON, factory, None)

    instance = await container.aget_by_type(MyService)
    assert isinstance(instance, MyService)
    assert instance.value == 42


@pytest.mark.asyncio
async def test_anonymous_service_without_type_fails():
    """Test that anonymous service registration fails when type cannot be inferred"""
    container = ServiceContainer()

    # Factory function without type annotation
    async def factory():
        return {"data": "test"}

    with pytest.raises(TypeError, match="unable to infer service type"):
        await container.register(None, ServiceLifetime.SINGLETON, factory, None)


@pytest.mark.asyncio
async def test_async_generator_service():
    """Test async generator service"""
    container = ServiceContainer()

    cleanup_called = False

    async def factory() -> AsyncGenerator[dict, None]:
        nonlocal cleanup_called
        try:
            yield {"data": "test"}
        finally:
            cleanup_called = True

    await container.register("gen", ServiceLifetime.TRANSIENT, factory, None)

    instance = await container.aget_by_key("gen")
    assert instance["data"] == "test"

    # Note: In actual usage, cleanup is called automatically at request end


@pytest.mark.asyncio
async def test_process_isolation():
    """Test process isolation check (using mock simulation)"""
    container = ServiceContainer()

    # Save original PID
    original_pid = container._pid

    async def factory():
        return {}

    await container.register("test", ServiceLifetime.SINGLETON, factory, None)

    # Restore correct PID to avoid registration failure
    container._pid = os.getpid()

    # Now modify PID to simulate cross-process access
    container._pid = original_pid + 1

    with pytest.raises(RuntimeError, match="accessed from different process"):
        await container.aget_by_key("test")


@pytest.mark.asyncio
async def test_singleton_destruction():
    """Test singleton service destruction"""
    container = ServiceContainer()

    destroyed = []

    async def factory():
        return {"id": 1}

    async def destructor(instance):
        destroyed.append(instance["id"])

    await container.register("test", ServiceLifetime.SINGLETON, factory, destructor)

    instance = await container.aget_by_key("test")
    assert instance["id"] == 1

    await container.destruct_all_singletons()

    assert destroyed == [1]


@pytest.mark.asyncio
async def test_duplicate_key_registration_fails():
    """Test that registering the same key twice fails"""
    container = ServiceContainer()

    async def factory():
        return {}

    await container.register("test", ServiceLifetime.SINGLETON, factory, None)

    with pytest.raises(RuntimeError, match="Duplicate service registration"):
        await container.register("test", ServiceLifetime.SINGLETON, factory, None)


@pytest.mark.asyncio
async def test_key_based_injection():
    """Test key-based service injection"""
    container = ServiceContainer()

    async def factory():
        return {"message": "Hello from service"}

    await container.register("my_service", ServiceLifetime.SINGLETON, factory, None)

    instance = await container.aget_by_key("my_service")
    assert instance["message"] == "Hello from service"
