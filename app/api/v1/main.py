from fastapi import APIRouter

from .example import router as example

router = APIRouter(prefix="/v1")

router.include_router(example)
