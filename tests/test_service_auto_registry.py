from __future__ import annotations

import pytest

from app.core.dependencies import ServiceContainer, ServiceLifetime
from app.core.service_registry import (
    RegisteredService,
    ServiceRegistry,
    _topological_registration_order,
    build_service_plan,
    register_services_from_registry,
)
from app.services import BaseService, Service, ServiceDict, require


@pytest.fixture
def isolated_service_registry():
    snapshot = ServiceRegistry.definitions()
    ServiceRegistry.clear_for_tests()
    try:
        yield
    finally:
        ServiceRegistry.clear_for_tests()
        for definition in snapshot:
            ServiceRegistry.register(definition)


def test_service_dict_expansion_and_require_placeholder_resolution(
    isolated_service_registry,
):
    @Service("shared_dep")
    class SharedDepService(BaseService):
        @classmethod
        async def create(cls) -> SharedDepService:
            return cls()

    @ServiceDict("root_service", dict={"alpha": {"name": "a"}, "beta": {"name": "b"}})
    class RootService(BaseService):
        @classmethod
        async def create(cls, name: str) -> RootService:
            _ = name
            return cls()

    @ServiceDict("{}_child_service", dict={"alpha": {}, "beta": {}})
    class ChildService(BaseService):
        @classmethod
        async def create(
            cls,
            shared=require("shared_dep"),
            scoped=require("{}_root_service"),
        ) -> ChildService:
            _ = shared, scoped
            return cls()

    plan = build_service_plan()
    by_key = {spec.key: spec for spec in plan}

    assert "alpha_root_service" in by_key
    assert "beta_root_service" in by_key
    assert "alpha_child_service" in by_key
    assert "beta_child_service" in by_key

    alpha_deps = {
        dep.param_name: dep.dep_key for dep in by_key["alpha_child_service"].dependencies
    }
    beta_deps = {
        dep.param_name: dep.dep_key for dep in by_key["beta_child_service"].dependencies
    }

    assert alpha_deps["shared"] == "shared_dep"
    assert beta_deps["shared"] == "shared_dep"
    assert alpha_deps["scoped"] == "alpha_root_service"
    assert beta_deps["scoped"] == "beta_root_service"


def test_topological_registration_order_detects_cycle():
    with pytest.raises(RuntimeError, match="circular"):
        _topological_registration_order(
            {
                "service_a": {"service_b"},
                "service_b": {"service_a"},
            }
        )


def test_service_lifetime_variants_and_eager_flag(isolated_service_registry):
    @Service("singleton_by_int", lifetime=0, eager=True)
    class SingletonByIntService(BaseService):
        @classmethod
        async def create(cls) -> SingletonByIntService:
            return cls()

    @Service("transient_by_int", lifetime=1)
    class TransientByIntService(BaseService):
        @classmethod
        async def create(cls) -> TransientByIntService:
            return cls()

    @Service("singleton_by_name", lifetime="Singleton")
    class SingletonByNameService(BaseService):
        @classmethod
        async def create(cls) -> SingletonByNameService:
            return cls()

    @Service("transient_by_name", lifetime="transient")
    class TransientByNameService(BaseService):
        @classmethod
        async def create(cls) -> TransientByNameService:
            return cls()

    plan = build_service_plan()
    by_key = {spec.key: spec for spec in plan}

    assert by_key["singleton_by_int"].lifetime == ServiceLifetime.SINGLETON
    assert by_key["singleton_by_int"].eager is True
    assert by_key["transient_by_int"].lifetime == ServiceLifetime.TRANSIENT
    assert by_key["singleton_by_name"].lifetime == ServiceLifetime.SINGLETON
    assert by_key["transient_by_name"].lifetime == ServiceLifetime.TRANSIENT


def test_eager_transient_is_rejected(isolated_service_registry):
    with pytest.raises(ValueError, match="cannot be eager with transient"):
        @Service("bad", lifetime="Transient", eager=True)
        class BadTransientService(BaseService):
            @classmethod
            async def create(cls) -> BadTransientService:
                return cls()


@pytest.mark.asyncio
async def test_service_decorator_supports_anonymous_registration_defaults(
    isolated_service_registry,
):
    class AnonymousPayloadA:
        pass

    class AnonymousPayloadB:
        pass

    @Service
    class AnonymousServiceA(BaseService):
        @classmethod
        async def create(cls) -> AnonymousPayloadA:
            _ = cls
            return AnonymousPayloadA()

    @Service()
    class AnonymousServiceB(BaseService):
        @classmethod
        async def create(cls) -> AnonymousPayloadB:
            _ = cls
            return AnonymousPayloadB()

    plan = build_service_plan()
    anonymous_specs = [spec for spec in plan if spec.key is None]
    assert len(anonymous_specs) == 2
    for spec in anonymous_specs:
        assert spec.lifetime == ServiceLifetime.SINGLETON

    container = ServiceContainer()
    registered = await register_services_from_registry(container)
    assert len(registered) == 2
    assert all(isinstance(item, RegisteredService) for item in registered)
    assert all(item.key is None for item in registered)
    assert all(item.origin.endswith(("AnonymousServiceA", "AnonymousServiceB")) for item in registered)

    a1 = await container.aget_by_type(AnonymousPayloadA)
    a2 = await container.aget_by_type(AnonymousPayloadA)
    b1 = await container.aget_by_type(AnonymousPayloadB)
    b2 = await container.aget_by_type(AnonymousPayloadB)

    assert a1 is a2
    assert b1 is b2


@pytest.mark.asyncio
async def test_named_service_can_depend_on_anonymous_service_by_type(
    isolated_service_registry,
):
    class SharedPayload:
        pass

    @Service
    class AnonymousSharedService(BaseService):
        @classmethod
        async def create(cls) -> SharedPayload:
            _ = cls
            return SharedPayload()

    @Service("consumer")
    class ConsumerService(BaseService):
        shared: SharedPayload

        def __init__(self, shared: SharedPayload) -> None:
            self.shared = shared

        @classmethod
        async def create(cls, shared=require(SharedPayload)) -> ConsumerService:
            return cls(shared)

    container = ServiceContainer()
    await register_services_from_registry(container)

    consumer = await container.aget_by_key("consumer")
    shared = await container.aget_by_type(SharedPayload)
    assert isinstance(consumer, ConsumerService)
    assert consumer.shared is shared


@pytest.mark.asyncio
async def test_anonymous_services_with_same_inferred_type_conflict_on_register(
    isolated_service_registry,
):
    class SharedType:
        pass

    @Service
    class AnonymousOne(BaseService):
        @classmethod
        async def create(cls) -> SharedType:
            _ = cls
            return SharedType()

    @Service()
    class AnonymousTwo(BaseService):
        @classmethod
        async def create(cls) -> SharedType:
            _ = cls
            return SharedType()

    container = ServiceContainer()
    with pytest.raises(RuntimeError, match="Anonymous registration for type"):
        await register_services_from_registry(container)


@pytest.mark.asyncio
async def test_inherited_destroy_hook_is_preserved(isolated_service_registry):
    destroy_calls = 0

    class ParentService(BaseService):
        @classmethod
        async def create(cls) -> ParentService:
            return cls()

        @classmethod
        async def destroy(cls, instance: object) -> None:
            nonlocal destroy_calls
            _ = cls
            _ = instance
            destroy_calls += 1

    @Service("child_service")
    class ChildService(ParentService):
        @classmethod
        async def create(cls) -> ChildService:
            return cls()

    plan = build_service_plan()
    by_key = {spec.key: spec for spec in plan}
    assert by_key["child_service"].dtor is not None

    container = ServiceContainer()
    await register_services_from_registry(container)
    await container.aget_by_key("child_service")
    await container.destruct_all_singletons()

    assert destroy_calls == 1


def test_require_string_key_cannot_target_anonymous_service(isolated_service_registry):
    @Service
    class AnonymousOnly(BaseService):
        @classmethod
        async def create(cls) -> AnonymousOnly:
            return cls()

    @Service("consumer")
    class ConsumerByKey(BaseService):
        @classmethod
        async def create(cls, dep=require("anonymous_only")) -> ConsumerByKey:
            _ = dep
            return cls()

    with pytest.raises(RuntimeError, match="depends on 'anonymous_only'"):
        build_service_plan()
