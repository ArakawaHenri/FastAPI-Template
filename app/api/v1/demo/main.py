from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.core.dependencies import inject
from app.services.async_generator_example import ExampleGeneratorServiceT
from app.services.semaphore import SemaphoreService
from app.services.transient_example import ExampleServiceT

router = APIRouter(prefix="/demo")


@router.get("/")
async def get(
        sem: SemaphoreService = inject("example_semaphore_service"),  # Injected by key, no args allowed
        example_t: ExampleServiceT = inject(ExampleServiceT),  # Injected by type (if unique)
        example_g: ExampleGeneratorServiceT = inject(
            ExampleGeneratorServiceT,
            "Hello from example generator!"  # Transient: arguments passed to ctor
        )
):
    async with sem:
        msg_from_example_transient = await example_t.hello()

    msg_from_example_generator = example_g.msg

    return JSONResponse({
        "msg_from_example_transient": msg_from_example_transient,
        "index_of_item_enumerated_from_example_generator": msg_from_example_generator
    })
