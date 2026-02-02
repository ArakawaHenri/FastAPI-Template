from __future__ import annotations

from asyncio import Semaphore

from app.services import BaseService


# Class name should have postfix "Service" if the service is a singleton service
class SemaphoreService(BaseService, Semaphore):
    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(*args, **kwargs) -> SemaphoreService:
            return SemaphoreService(*args, **kwargs)
