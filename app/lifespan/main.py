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
    logging_initialized = False
    services: ServiceContainer | None = None
    try:
        setup_logging(Path(settings.log_dir), settings.debug_mode)
        logging_initialized = True

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

        services = ServiceContainer()
        app.state.services = services

        logger.debug("[LIFESPAN] Registering services...")

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
        # Usage: Inject(AsyncSession, Inject("main_database_service"))
        # ServiceContainer infers type from ctor return annotation.
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

        # LMDB key-value store service (singleton per worker, resolve by type)
        await app.state.services.register(
            None,
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
            settings.store_lmdb.callback_worker_threads,
        )
        store_service: StoreService = await app.state.services.aget_by_type(StoreService)

        # Temp file manager (singleton per worker, resolve by type)
        await app.state.services.register(
            None,
            ServiceLifetime.SINGLETON,
            TempFileService.LifespanTasks.ctor,
            TempFileService.LifespanTasks.dtor,
            settings.tmp_dir,
            settings.tmp_retention_days,
            settings.tmp_cleanup_interval_seconds,
            settings.tmp_total_size_recalc_seconds,
            settings.tmp_worker_threads,
            settings.tmp_max_file_size_mb,
            settings.tmp_max_total_size_mb,
            store_provider=lambda: app.state.services.aget_by_type(
                StoreService),
        )
        temp_file_service: TempFileService = await app.state.services.aget_by_type(
            TempFileService
        )
        await temp_file_service.start_cleanup()
        await store_service.start_cleanup()

        yield

    finally:
        logger.debug("[LIFESPAN] Shutting down...")

        active_services = services or getattr(app.state, "services", None)
        if active_services is not None:
            await active_services.destruct_all_singletons()
            app.state.services = None
            logger.debug("[LIFESPAN] Service container released.")
        else:
            logger.debug("[LIFESPAN] Service container was not initialized.")

        logger.debug("[LIFESPAN] Application shutdown completed.")
        if logging_initialized:
            await shutdown_logging()
