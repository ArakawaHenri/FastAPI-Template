from __future__ import annotations

import logging
import traceback
from typing import Any

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapiex.settings import GetSettings
from starlette.exceptions import HTTPException

logger = logging.getLogger(__name__)


class AppException(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code: str = "INTERNAL_ERROR",
        details: dict[str, Any] | None = None,
    ) -> None:
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.details = details or {}
        super().__init__(message)


class BadRequestException(AppException):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(
            message,
            status_code=status.HTTP_400_BAD_REQUEST,
            error_code="BAD_REQUEST",
            details=details,
        )


class UnauthorizedException(AppException):
    def __init__(self, message: str = "Authentication required") -> None:
        super().__init__(message, status_code=status.HTTP_401_UNAUTHORIZED, error_code="UNAUTHORIZED")


class ForbiddenException(AppException):
    def __init__(self, message: str = "Permission denied") -> None:
        super().__init__(message, status_code=status.HTTP_403_FORBIDDEN, error_code="FORBIDDEN")


class NotFoundException(AppException):
    def __init__(self, resource: str, identifier: Any | None = None) -> None:
        message = f"{resource} not found"
        if identifier is not None:
            message = f"{message}: {identifier}"
        super().__init__(
            message,
            status_code=status.HTTP_404_NOT_FOUND,
            error_code="RESOURCE_NOT_FOUND",
            details={
                "resource": resource,
                "identifier": str(identifier) if identifier is not None else None,
            },
        )


class ConflictException(AppException):
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(
            message,
            status_code=status.HTTP_409_CONFLICT,
            error_code="CONFLICT",
            details=details,
        )


class RateLimitException(AppException):
    def __init__(self, retry_after: int = 60) -> None:
        super().__init__(
            "Rate limit exceeded",
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            error_code="RATE_LIMIT_EXCEEDED",
            details={"retry_after": retry_after},
        )


class ServiceUnavailableException(AppException):
    def __init__(self, message: str = "Service temporarily unavailable") -> None:
        super().__init__(
            message,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_code="SERVICE_UNAVAILABLE",
        )


def create_error_response(
    request: Request,
    *,
    status_code: int,
    message: str,
    error_code: str = "ERROR",
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    content: dict[str, Any] = {
        "error": {
            "code": error_code,
            "message": message,
        }
    }
    if request_id:
        content["error"]["request_id"] = request_id
    if details:
        content["error"]["details"] = details
    if _debug_enabled():
        content["error"]["path"] = request.url.path
    return JSONResponse(status_code=status_code, content=content)


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    formatted_errors = [
        {
            "field": ".".join(str(item) for item in error["loc"]),
            "message": error["msg"],
            "type": error["type"],
        }
        for error in exc.errors()
    ]
    details: dict[str, Any] = {"validation_errors": formatted_errors}
    if _debug_enabled():
        details["body"] = exc.body

    logger.warning(
        "Validation error method=%s path=%s errors=%s",
        request.method,
        request.url.path,
        formatted_errors,
    )
    return create_error_response(
        request,
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        message="Validation error",
        error_code="VALIDATION_ERROR",
        details=details,
    )


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    error_code_mapping = {
        status.HTTP_400_BAD_REQUEST: "BAD_REQUEST",
        status.HTTP_401_UNAUTHORIZED: "UNAUTHORIZED",
        status.HTTP_403_FORBIDDEN: "FORBIDDEN",
        status.HTTP_404_NOT_FOUND: "NOT_FOUND",
        status.HTTP_405_METHOD_NOT_ALLOWED: "METHOD_NOT_ALLOWED",
        status.HTTP_409_CONFLICT: "CONFLICT",
        status.HTTP_429_TOO_MANY_REQUESTS: "RATE_LIMIT_EXCEEDED",
        status.HTTP_503_SERVICE_UNAVAILABLE: "SERVICE_UNAVAILABLE",
    }
    logger.info(
        "HTTP exception method=%s path=%s status=%s detail=%s",
        request.method,
        request.url.path,
        exc.status_code,
        exc.detail,
    )
    return create_error_response(
        request,
        status_code=exc.status_code,
        message=str(exc.detail or "HTTP error"),
        error_code=error_code_mapping.get(exc.status_code, "HTTP_ERROR"),
    )


async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
    logger.warning(
        "Application exception method=%s path=%s code=%s details=%s",
        request.method,
        request.url.path,
        exc.error_code,
        exc.details,
    )
    return create_error_response(
        request,
        status_code=exc.status_code,
        message=exc.message,
        error_code=exc.error_code,
        details=exc.details or None,
    )


async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception method=%s path=%s", request.method, request.url.path)
    details: dict[str, Any] | None = None
    if _debug_enabled():
        details = {"traceback": traceback.format_exc()}
    return create_error_response(
        request,
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        message="Internal server error",
        error_code="INTERNAL_ERROR",
        details=details,
    )


def _debug_enabled() -> bool:
    try:
        return bool(GetSettings("app", field="debug_mode", default=False))
    except Exception:
        return False

