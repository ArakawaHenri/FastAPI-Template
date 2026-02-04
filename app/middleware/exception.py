from __future__ import annotations

from typing import Any

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.exceptions import HTTPException

from app.core.settings import settings


# =============================================================================
# Custom Exception Classes
# =============================================================================

class AppException(Exception):
    """Base exception for all application-specific exceptions"""
    def __init__(
        self,
        message: str,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR, 
        error_code: str = "INTERNAL_ERROR",
        details: dict[str, Any] | None = None
    ):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.details = details or {}
        super().__init__(self.message)


class NotFoundException(AppException):
    """Raised when a resource is not found"""
    def __init__(self, resource: str, identifier: Any = None):
        message = f"{resource} not found"
        if identifier:
            message += f": {identifier}"
        super().__init__(
            message=message,
            status_code=status.HTTP_404_NOT_FOUND,
            error_code="RESOURCE_NOT_FOUND",
            details={"resource": resource, "identifier": str(identifier) if identifier else None}
        )


class UnauthorizedException(AppException):
    """Raised when authentication is required but not provided"""
    def __init__(self, message: str = "Authentication required"):
        super().__init__(
            message=message,
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code="UNAUTHORIZED"
        )


class ForbiddenException(AppException):
    """Raised when user doesn't have permission to access resource"""
    def __init__(self, message: str = "Permission denied"):
        super().__init__(
            message=message,
            status_code=status.HTTP_403_FORBIDDEN,
            error_code="FORBIDDEN"
        )


class BadRequestException(AppException):
    """Raised for invalid client requests"""
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code="BAD_REQUEST",
            details=details
        )


class ConflictException(AppException):
    """Raised when there's a conflict with existing data"""
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            status_code=status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            details=details
        )


class RateLimitException(AppException):
    """Raised when rate limit is exceeded"""
    def __init__(self, retry_after: int = 60):
        super().__init__(
            message="Rate limit exceeded",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            error_code="RATE_LIMIT_EXCEEDED",
            details={"retry_after": retry_after}
        )


# =============================================================================
# Exception Handlers
# =============================================================================

def create_error_response(
    status_code: int,
    message: str,
    error_code: str = "ERROR",
    details: dict[str, Any] | None = None,
    request_path: str | None = None
) -> JSONResponse:
    """Create standardized error response"""
    content = {
        "error": {
            "code": error_code,
            "message": message,
        }
    }

    if details:
        content["error"]["details"] = details

    if settings.debug_mode and request_path:
        content["error"]["path"] = request_path

    return JSONResponse(status_code=status_code, content=content)


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors (422)"""
    logger.warning(
        f"Validation error on {request.method} {request.url.path}",
        errors=exc.errors(),
    )

    # Format validation errors for better readability
    formatted_errors = []
    for error in exc.errors():
        formatted_errors.append({
            "field": ".".join(str(x) for x in error["loc"]),
            "message": error["msg"],
            "type": error["type"]
        })

    details = {"validation_errors": formatted_errors}

    # Include request body only in debug mode
    if settings.debug_mode:
        details["body"] = exc.body

    return create_error_response(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        message="Validation error",
        error_code="VALIDATION_ERROR",
        details=details,
        request_path=str(request.url.path)
    )


async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle standard HTTP exceptions (404, 405, etc.)"""
    logger.info(
        f"HTTP exception on {request.method} {request.url.path}",
        status_code=exc.status_code,
        detail=exc.detail
    )

    # Map common status codes to error codes
    error_code_mapping = {
        status.HTTP_400_BAD_REQUEST: "BAD_REQUEST",
        status.HTTP_401_UNAUTHORIZED: "UNAUTHORIZED",
        status.HTTP_403_FORBIDDEN: "FORBIDDEN",
        status.HTTP_404_NOT_FOUND: "NOT_FOUND",
        status.HTTP_405_METHOD_NOT_ALLOWED: "METHOD_NOT_ALLOWED",
        status.HTTP_409_CONFLICT: "CONFLICT",
        status.HTTP_429_TOO_MANY_REQUESTS: "RATE_LIMIT_EXCEEDED",
    }

    error_code = error_code_mapping.get(exc.status_code, "HTTP_ERROR")

    return create_error_response(
        status_code=exc.status_code,
        message=exc.detail or "HTTP error",
        error_code=error_code,
        request_path=str(request.url.path)
    )


async def app_exception_handler(request: Request, exc: AppException):
    """Handle custom application exceptions"""
    logger.warning(
        f"Application exception on {request.method} {request.url.path}",
        error_code=exc.error_code,
        message=exc.message,
        details=exc.details
    )

    return create_error_response(
        status_code=exc.status_code,
        message=exc.message,
        error_code=exc.error_code,
        details=exc.details if exc.details else None,
        request_path=str(request.url.path)
    )


async def global_exception_handler(request: Request, exc: Exception):
    """Handle all uncaught exceptions"""
    logger.exception(
        f"Unhandled exception on {request.method} {request.url.path}",
        exc_info=exc
    )

    # Re-raise in development to show detailed errors
    if settings.debug_mode:
        raise

    # Return generic error in production
    return create_error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        message="Internal server error",
        error_code="INTERNAL_ERROR",
        request_path=str(request.url.path)
    )
