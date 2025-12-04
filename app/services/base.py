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
        @abstractmethod
        async def dtor(instance: BaseService):
            raise NotImplementedError


type LifespanTasks = BaseService.LifespanTasks
