from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from app.api import router
from app.core.dependencies import TransientServiceFinalizerMiddleware
from app.core.settings import settings
from app.lifespan.main import lifespan
from app.middleware.exception import (
    AppException,
    app_exception_handler,
    global_exception_handler,
    http_exception_handler,
    validation_exception_handler,
)
from app.middleware.logging import RequestLoggingMiddleware


def create_application() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
        default_response_class=JSONResponse,
    )

    # CORS middleware - read from configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
        allow_headers=["*"],
        max_age=3600,
    )

    # Exception handlers (order: specific -> general)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(AppException, app_exception_handler)
    app.add_exception_handler(Exception, global_exception_handler)

    # Starlette middleware executes in reverse registration order.
    # Register transient cleanup first so request logging wraps the full chain.
    app.add_middleware(TransientServiceFinalizerMiddleware)
    app.add_middleware(RequestLoggingMiddleware)
    app.include_router(router)
    return app


app = create_application()
