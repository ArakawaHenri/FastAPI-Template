from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapiex.di import install_di
from fastapiex.settings import BaseSettings, GetSettings, Settings
from pydantic import Field
from starlette.exceptions import HTTPException

from app.core.logging import setup_logging
from app.middleware.exceptions import (
    AppException,
    app_exception_handler,
    global_exception_handler,
    http_exception_handler,
    validation_exception_handler,
)
from app.middleware.logging import RequestLoggingMiddleware


@Settings("app")
class AppSettings(BaseSettings):
    title: str = "FastAPI Template"
    version: str = "0.1.0"
    description: str = "Production-ready FastAPI template with FastAPIEx DI/settings."
    debug_mode: bool = False
    log_dir: Path = Field(default_factory=lambda: Path("./logs"))
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])


def create_application() -> FastAPI:
    app_settings: AppSettings = GetSettings(AppSettings)
    setup_logging(log_dir=app_settings.log_dir, debug=app_settings.debug_mode)

    app = FastAPI(
        title=app_settings.title,
        version=app_settings.version,
        description=app_settings.description,
        debug=app_settings.debug_mode,
    )

    _install_cors(app, app_settings.cors_origins)
    _install_exception_handlers(app)

    app.add_middleware(RequestLoggingMiddleware)
    install_di(
        app,
        service_packages=["app.service"],
        strict=True,
        use_global_service_registry=False,
        allow_private_modules=False,
        eager_init_timeout_sec=30,
    )

    from app.api.main import router

    app.include_router(router)
    return app


def _install_cors(app: FastAPI, origins: list[str]) -> None:
    cors_origins = list(origins)
    if not cors_origins:
        return
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials="*" not in cors_origins,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        max_age=3600,
    )


def _install_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(HTTPException, http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(AppException, app_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, global_exception_handler)


app = create_application()

