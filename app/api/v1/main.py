from fastapi import APIRouter

router = APIRouter(prefix="/v1")

from .demo import router as demo

router.include_router(demo, tags=["demo"])
