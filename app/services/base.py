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

    class LifespanTasks(ABC):
        @staticmethod
        @abstractmethod
        def ctor(*args: Any, **kwargs: Any) -> object:
            raise NotImplementedError

        @staticmethod
        async def dtor(instance: Any) -> None:  # noqa: B027
            """Override for cleanup. Default is no-op."""
            pass


type LifespanTasks = BaseService.LifespanTasks


__all__ = [
    "BaseService",
    "LifespanTasks",
    "Service",
    "ServiceDict",
    "require",
]
