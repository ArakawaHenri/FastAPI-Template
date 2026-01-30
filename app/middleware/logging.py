import time
import uuid

from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from app.core.logger import request_id_ctx


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log all HTTP requests and responses
    """

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # Generate or use existing request ID for distributed tracing
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request.state.request_id = request_id

        # Set contextvar for log correlation
        request_id_ctx.set(request_id)

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
