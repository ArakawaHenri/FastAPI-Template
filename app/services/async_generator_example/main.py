from __future__ import annotations

from collections.abc import AsyncIterator

from loguru import logger

from app.services import BaseService, Service


@Service("example_generator_transient", lifetime="transient")
class ExampleGeneratorServiceT(BaseService):
    @classmethod
    async def create(cls, msg: str) -> AsyncIterator[ExampleGeneratorServiceT]:
        # If you need iteration, expose it from service methods instead.
        try:
            yield cls(msg)
        finally:
            logger.debug("contextmanager finalized")

    msg: str

    def __init__(self, msg):
        self.msg = msg
