from __future__ import annotations

from fastapi import APIRouter

from .v1 import router as v1

router = APIRouter(prefix="/api")

router.include_router(v1)


@router.get("/")
async def root():
    """Root endpoint for health checking and basic API info."""
    return {"message": "Hello World"}
