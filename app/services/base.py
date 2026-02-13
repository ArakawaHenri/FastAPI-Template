from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.core.service_registry import Service, ServiceDict, require


class BaseService(ABC):
    """
    Base class for services.

    Singleton services should be named with the suffix "Service".
    Transient services should be named with the suffix "ServiceT".
    """

    @classmethod
    @abstractmethod
    def create(cls, *args: Any, **kwargs: Any) -> object:
        """Factory hook used by the service registry."""
        raise NotImplementedError

    @classmethod
    async def destroy(cls, instance: Any) -> None:  # noqa: B027
        """Optional cleanup hook. Override in services that need teardown."""
        _ = instance


__all__ = [
    "BaseService",
    "Service",
    "ServiceDict",
    "require",
]
