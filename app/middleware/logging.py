from __future__ import annotations

import time
import uuid

from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from app.core.logger import request_id_ctx

_MAX_REQUEST_ID_LEN = 128


def _normalize_request_id(raw: str | None) -> str:
    if not raw:
        return str(uuid.uuid4())
    candidate = raw.strip()
    if not candidate:
        return str(uuid.uuid4())
    if len(candidate) > _MAX_REQUEST_ID_LEN:
        return str(uuid.uuid4())
    for ch in candidate:
        if not (ch.isalnum() or ch in "-_."):
            return str(uuid.uuid4())
    return candidate


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log all HTTP requests and responses
    """

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # Generate or use existing request ID for distributed tracing
        request_id = _normalize_request_id(request.headers.get("X-Request-ID"))
        request.state.request_id = request_id

        # Set contextvar for log correlation
        token = request_id_ctx.set(request_id)

        try:
            # Log request start
            logger.info(
                f"Request started: {request.method} {request.url.path}",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "client": request.client.host if request.client else None,
                    "request_id": request_id,
                }
            )

            try:
                response = await call_next(request)
            except Exception:
                process_time = time.time() - start_time
                logger.exception(
                    f"Request failed: {request.method} {request.url.path}",
                    extra={
                        "method": request.method,
                        "path": request.url.path,
                        "duration_ms": round(process_time * 1000, 2),
                        "request_id": request_id,
                    }
                )
                raise

            # Calculate processing time
            process_time = time.time() - start_time

            # Add request ID to response headers for client correlation
            response.headers["X-Request-ID"] = request_id

            # Log response
            logger.info(
                f"Request completed: {request.method} {request.url.path}",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": round(process_time * 1000, 2),
                    "request_id": request_id,
                }
            )

            return response
        finally:
            request_id_ctx.reset(token)
