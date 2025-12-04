from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware

from app.api import router
from app.core.dependencies import TransientServiceFinalizerMiddleware
from app.core.settings import settings
from app.lifespan.main import lifespan
from app.middleware.exception import validation_exception_handler


def create_application() -> FastAPI:
    app = FastAPI(title=settings.app_name,
                  version=settings.app_version, lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_exception_handler(RequestValidationError,
                              validation_exception_handler)
    app.add_middleware(TransientServiceFinalizerMiddleware)
    app.include_router(router)
    return app


app = create_application()


@app.get("/")
async def root():
    return {"message": "Hello World"}
