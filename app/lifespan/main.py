from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from loguru import logger

from app.core.dependencies import (
    ServiceContainer,
    get_or_create_service_container_registry,
)
from app.core.logger import setup_logging, shutdown_logging
from app.core.service_registry import import_service_modules, register_services_from_registry
from app.core.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging_initialized = False
    services: ServiceContainer | None = None
    sc_registry = None
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
        sc_registry = get_or_create_service_container_registry(app.state)
        sc_registry.register_current(services)

        logger.debug("[LIFESPAN] Registering services via decorators...")
        import_service_modules()
        registered_services = await register_services_from_registry(services)
        logger.debug(
            "[LIFESPAN] Auto registration completed",
            count=len(registered_services),
        )

        yield

    finally:
        logger.debug("[LIFESPAN] Shutting down...")

        if services is not None:
            try:
                await services.destruct_all_singletons()
            finally:
                if sc_registry is not None:
                    sc_registry.unregister_current(expected=services)
            logger.debug("[LIFESPAN] Service container released.")
        else:
            logger.debug("[LIFESPAN] Service container was not initialized.")

        logger.debug("[LIFESPAN] Application shutdown completed.")
        if logging_initialized:
            await shutdown_logging()
