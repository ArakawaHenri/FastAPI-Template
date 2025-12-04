from __future__ import annotations

from typing import AsyncGenerator

from loguru import logger

from app.services import BaseService


class ExampleGeneratorServiceT(BaseService):
    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(msg: str) -> AsyncGenerator[ExampleGeneratorServiceT]:
            # If you want a iterable generator, return it via a method of a service instance
            try:
                yield ExampleGeneratorServiceT(msg)
            finally:
                logger.debug("generator finallized")

        dtor = None

    msg: str

    def __init__(self, msg):
        self.msg = msg
