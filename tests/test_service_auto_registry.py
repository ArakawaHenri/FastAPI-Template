from __future__ import annotations

import pytest

from app.core.dependencies import ServiceLifetime
from app.core.service_registry import (
    ServiceRegistry,
    _topological_registration_order,
    build_service_plan,
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
        class LifespanTasks(BaseService.LifespanTasks):
            @staticmethod
            async def ctor() -> "SharedDepService":
                return SharedDepService()

    @ServiceDict("root_service", dict={"alpha": {"name": "a"}, "beta": {"name": "b"}})
    class RootService(BaseService):
        class LifespanTasks(BaseService.LifespanTasks):
            @staticmethod
            async def ctor(name: str) -> "RootService":
                _ = name
                return RootService()

    @ServiceDict("{}_child_service", dict={"alpha": {}, "beta": {}})
    class ChildService(BaseService):
        class LifespanTasks(BaseService.LifespanTasks):
            @staticmethod
            async def ctor(
                shared=require("shared_dep"),
                scoped=require("{}_root_service"),
            ) -> "ChildService":
                _ = shared, scoped
                return ChildService()

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
        class LifespanTasks(BaseService.LifespanTasks):
            @staticmethod
            async def ctor() -> "SingletonByIntService":
                return SingletonByIntService()

    @Service("transient_by_int", lifetime=1)
    class TransientByIntService(BaseService):
        class LifespanTasks(BaseService.LifespanTasks):
            @staticmethod
            async def ctor() -> "TransientByIntService":
                return TransientByIntService()

    @Service("singleton_by_name", lifetime="Singleton")
    class SingletonByNameService(BaseService):
        class LifespanTasks(BaseService.LifespanTasks):
            @staticmethod
            async def ctor() -> "SingletonByNameService":
                return SingletonByNameService()

    @Service("transient_by_name", lifetime="transient")
    class TransientByNameService(BaseService):
        class LifespanTasks(BaseService.LifespanTasks):
            @staticmethod
            async def ctor() -> "TransientByNameService":
                return TransientByNameService()

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
            class LifespanTasks(BaseService.LifespanTasks):
                @staticmethod
                async def ctor() -> "BadTransientService":
                    return BadTransientService()
