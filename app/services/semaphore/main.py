from __future__ import annotations

from asyncio import Semaphore

from app.core.settings import settings
from app.services import BaseService, ServiceDict


# Class name should have postfix "Service" if the service is a singleton service
@ServiceDict("{}_semaphore_service", dict=settings.semaphores)
class SemaphoreService(BaseService, Semaphore):
    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(value: int) -> SemaphoreService:
            return SemaphoreService(value)
