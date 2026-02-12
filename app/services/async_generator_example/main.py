from __future__ import annotations

from typing import AsyncIterator

from loguru import logger

from app.services import BaseService, Service


@Service("example_generator_transient", lifetime="transient")
class ExampleGeneratorServiceT(BaseService):
    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(msg: str) -> AsyncIterator[ExampleGeneratorServiceT]:
            # If you need iteration, expose it from service methods instead.
            try:
                yield ExampleGeneratorServiceT(msg)
            finally:
                logger.debug("contextmanager finalized")

    msg: str

    def __init__(self, msg):
        self.msg = msg
