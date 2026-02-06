"""
Example demonstrating how to use custom exceptions in your API endpoints.

This file shows best practices and error handling in FastAPI applications.
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.core.dependencies import Inject
from app.middleware.exception import (
    BadRequestException,
    ConflictException,
    ForbiddenException,
    NotFoundException,
    UnauthorizedException,
)
from app.services.async_generator_example import ExampleGeneratorServiceT
from app.services.semaphore import SemaphoreService
from app.services.transient_example import ExampleServiceT

router = APIRouter(prefix="/example", tags=["Exception Examples"])


@router.get("/")
async def get(
        sem: SemaphoreService = Inject("example_semaphore_service"),  # Injected by key, no args allowed
        example_t: ExampleServiceT = Inject(ExampleServiceT),  # Injected by type (if unique)
        example_g: ExampleGeneratorServiceT = Inject(
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


@router.get("/not-found")
async def example_not_found():
    """Demonstrates NotFoundException usage"""
    # Simulating a database lookup that fails
    user_id = 123
    raise NotFoundException("User", user_id)


@router.get("/unauthorized")
async def example_unauthorized():
    """Demonstrates UnauthorizedException usage"""
    # Simulating missing or invalid authentication
    raise UnauthorizedException("Invalid or missing API token")


@router.get("/forbidden")
async def example_forbidden():
    """Demonstrates ForbiddenException usage"""
    # Simulating lack of permissions
    raise ForbiddenException("You don't have permission to access this resource")


@router.post("/bad-request")
async def example_bad_request():
    """Demonstrates BadRequestException usage"""
    # Simulating invalid request data
    raise BadRequestException(
        "Invalid request parameters",
        details={
            "field": "email",
            "issue": "Email domain not allowed"
        }
    )


@router.post("/conflict")
async def example_conflict():
    """Demonstrates ConflictException usage"""
    # Simulating duplicate resource creation
    raise ConflictException(
        "User with this email already exists",
        details={"email": "user@example.com"}
    )


# Example of proper error handling in business logic
@router.get("/users/{user_id}")
async def get_user(user_id: int):
    """Example of proper error handling in a real endpoint"""
    # Simulate database lookup
    user = await fetch_user_from_db(user_id)

    if user is None:
        raise NotFoundException("User", user_id)

    # Check permissions (example)
    if not user.get("is_active"):
        raise ForbiddenException("User account is inactive")

    return user


# Mock database function for demonstration
async def fetch_user_from_db(user_id: int):
    """Mock function to simulate database lookup"""
    # In real app, this would query your database
    if user_id == 999:
        return {"id": 999, "name": "Test User", "is_active": True}
    return None
