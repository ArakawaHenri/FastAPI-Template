from __future__ import annotations

from abc import ABC, abstractmethod


class BaseService(ABC):
    """
    Base class for services.

    Singleton services should be named with the suffix "Service".
    Transient services should be named with the suffix "ServiceT".
    """

    class LifespanTasks(ABC):
        @staticmethod
        @abstractmethod
        async def ctor(*args, **kwargs) -> BaseService:
            raise NotImplementedError

        @staticmethod
        async def dtor(instance: BaseService) -> None:  # noqa: B027
            """Override for cleanup. Default is no-op."""
            pass


type LifespanTasks = BaseService.LifespanTasks
