from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from app.core.dependencies import ServiceContainer, ServiceLifetime
from app.core.logger import setup_logging, shutdown_logging
from app.core.settings import settings
from app.services.async_generator_example import ExampleGeneratorServiceT
from app.services.database import DatabaseEngineService, DatabaseSessionServiceT
from app.services.semaphore import SemaphoreService
from app.services.transient_example import ExampleServiceT


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        setup_logging(settings.LOG_DIR, settings.DEBUG_MODE)

        # Production configuration validation
        if not settings.DEBUG_MODE:
            if not settings.CORS_ORIGINS:
                logger.warning(
                    "[LIFESPAN] CORS_ORIGINS is empty in production mode. "
                    "Browsers will be blocked from accessing the API."
                )
            if settings.RELOAD:
                logger.warning(
                    "[LIFESPAN] RELOAD=true in production mode is not recommended."
                )

        logger.debug("[LIFESPAN] Initialising service container...")

        app.state.services = ServiceContainer()

        logger.debug("[LIFESPAN] Registering services...")

        # Keys for singleton services should be named with suffix `_service`
        # Keys for transient services should be named with suffix `_transient`

        # Register database engines for each configured database
        for key, db_config in settings.database.items():
            await app.state.services.register(
                f"{key}_database_service",
                ServiceLifetime.SINGLETON,
                DatabaseEngineService.LifespanTasks.ctor,
                DatabaseEngineService.LifespanTasks.dtor,
                **db_config.model_dump()
            )

        # Register generic session transient (use nested inject to specify engine)
        # Usage: inject(DatabaseSessionServiceT, inject("main_database_service"))
        await app.state.services.register(
            None,  # Anonymous, resolve by type
            ServiceLifetime.TRANSIENT,
            DatabaseSessionServiceT.LifespanTasks.ctor,
            None  # Cleanup handled by async generator
        )

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
