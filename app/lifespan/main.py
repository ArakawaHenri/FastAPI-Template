from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from app.core.dependencies import ServiceContainer, ServiceLifetime
from app.core.logger import setup_logging, shutdown_logging
from app.core.settings import settings
from app.services.async_generator_example import ExampleGeneratorServiceT
from app.services.semaphore import SemaphoreService
from app.services.transient_example import ExampleServiceT


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        setup_logging(settings.LOG_DIR, settings.DEBUG_MODE)

        logger.debug("[LIFESPAN] Initialising service container...")

        app.state.services = ServiceContainer()

        logger.debug("[LIFESPAN] Registering services...")

        # Keys for singleton services should be named with suffix `_service`
        # Keys for transient services should be named with suffix `_transient`

        # Create semaphore services as example singletons
        for key, value in settings.semaphores.items():
            await app.state.services.register(
                f"{key}_semaphore_service",
                ServiceLifetime.SINGLETON,
                SemaphoreService.LifespanTasks.ctor,
                SemaphoreService.LifespanTasks.dtor,
                value
            )

        await app.state.services.register(
            "example_transient",
            ServiceLifetime.TRANSIENT,
            ExampleServiceT.LifespanTasks.ctor,
            ExampleServiceT.LifespanTasks.dtor
        )

        await app.state.services.register(
            "example_generator_transient",
            ServiceLifetime.TRANSIENT,
            ExampleGeneratorServiceT.LifespanTasks.ctor,
            ExampleGeneratorServiceT.LifespanTasks.dtor
        )

        yield

    finally:
        logger.debug("[LIFESPAN] Shutting down...")

        await app.state.services.destruct_all_singletons()

        app.state.services = None

        logger.debug("[LIFESPAN] Service container released.")

        logger.debug("[LIFESPAN] Application shutdown completed.")

        await shutdown_logging()
