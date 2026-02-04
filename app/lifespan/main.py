from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from loguru import logger

from app.core.dependencies import ServiceContainer, ServiceLifetime
from app.core.logger import setup_logging, shutdown_logging
from app.core.settings import settings
from app.services.async_generator_example import ExampleGeneratorServiceT
from app.services.database import DatabaseEngineService, DatabaseSessionServiceT
from app.services.semaphore import SemaphoreService
from app.services.store import StoreService
from app.services.temp_file import TempFileService
from app.services.transient_example import ExampleServiceT


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        setup_logging(Path(settings.log_dir), settings.debug_mode)

        # Production configuration validation
        if not settings.debug_mode:
            if not settings.cors_origins:
                logger.warning(
                    "[LIFESPAN] CORS_ORIGINS is empty in production mode. "
                    "Browsers will be blocked from accessing the API."
                )
            if settings.reload:
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

        # LMDB key-value store service (singleton per worker)
        await app.state.services.register(
            "store_service",
            ServiceLifetime.SINGLETON,
            StoreService.LifespanTasks.ctor,
            StoreService.LifespanTasks.dtor,
            settings.store_lmdb.path,
            settings.store_lmdb.map_size_mb,
            settings.store_lmdb.map_size_growth_factor,
            settings.store_lmdb.map_high_watermark,
            settings.store_lmdb.max_dbs,
            settings.store_lmdb.max_readers,
            settings.store_lmdb.sync,
            settings.store_lmdb.metasync,
            settings.store_lmdb.writemap,
            settings.store_lmdb.map_async,
            settings.store_lmdb.max_key_bytes,
            settings.store_lmdb.max_namespace_bytes,
            settings.store_lmdb.max_value_bytes,
            settings.store_lmdb.cleanup_max_deletes,
        )
        store_service: StoreService = await app.state.services.aget_by_key("store_service")
        await store_service.start_cleanup()

        # Temp file manager (singleton per worker)
        await app.state.services.register(
            "temp_file_service",
            ServiceLifetime.SINGLETON,
            TempFileService.LifespanTasks.ctor,
            TempFileService.LifespanTasks.dtor,
            settings.tmp_dir,
            settings.tmp_retention_days,
        )
        temp_file_service: TempFileService = await app.state.services.aget_by_key("temp_file_service")
        await temp_file_service.start_cleanup()

        yield

    finally:
        logger.debug("[LIFESPAN] Shutting down...")

        await app.state.services.destruct_all_singletons()

        app.state.services = None

        logger.debug("[LIFESPAN] Service container released.")

        logger.debug("[LIFESPAN] Application shutdown completed.")

        await shutdown_logging()
