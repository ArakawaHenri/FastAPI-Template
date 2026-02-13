from __future__ import annotations

import time
import uuid

from loguru import logger
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.dependencies import REQUEST_FAILED_STATE_KEY
from app.core.logger import request_id_ctx

_MAX_REQUEST_ID_LEN = 128
_REQUEST_ID_HEADER = b"x-request-id"


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


class RequestLoggingMiddleware:
    """
    Middleware to log all HTTP requests and responses
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.perf_counter()
        method = scope.get("method", "")
        path = scope.get("path", "")
        client = scope.get("client")
        client_host = client[0] if client else None

        raw_request_id: str | None = None
        for key, value in scope.get("headers", []):
            if key.lower() == _REQUEST_ID_HEADER:
                raw_request_id = value.decode("latin-1")
                break

        # Generate or use existing request ID for distributed tracing
        request_id = _normalize_request_id(raw_request_id)

        state = scope.get("state")
        if state is None:
            state = {}
            scope["state"] = state
        state["request_id"] = request_id

        # Set contextvar for log correlation
        token = request_id_ctx.set(request_id)
        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal response_started

            if message["type"] == "http.response.start":
                headers = message.get("headers")
                if headers is None:
                    headers = []
                    message["headers"] = headers
                mutable_headers = MutableHeaders(raw=headers)
                mutable_headers["X-Request-ID"] = request_id

                if not response_started:
                    response_started = True
                    process_time = time.perf_counter() - start_time
                    status_code = int(message.get("status", 0))
                    request_failed = bool(
                        state.get(REQUEST_FAILED_STATE_KEY, False)
                    ) or status_code >= 400

                    if status_code >= 500:
                        log_fn = logger.error
                    elif request_failed:
                        log_fn = logger.warning
                    else:
                        log_fn = logger.info

                    log_event = "Request failed" if request_failed else "Request completed"
                    log_fn(
                        f"{log_event}: {method} {path}",
                        extra={
                            "method": method,
                            "path": path,
                            "status_code": status_code,
                            "duration_ms": round(process_time * 1000, 2),
                            "request_id": request_id,
                            "request_failed": request_failed,
                        }
                    )
            await send(message)

        try:
            # Log request start
            logger.info(
                f"Request started: {method} {path}",
                extra={
                    "method": method,
                    "path": path,
                    "client": client_host,
                    "request_id": request_id,
                }
            )

            try:
                await self.app(scope, receive, send_wrapper)
            except Exception:
                process_time = time.perf_counter() - start_time
                if not response_started:
                    logger.exception(
                        f"Request failed: {method} {path}",
                        extra={
                            "method": method,
                            "path": path,
                            "duration_ms": round(process_time * 1000, 2),
                            "request_id": request_id,
                            "request_failed": True,
                        }
                    )
                raise
        finally:
            request_id_ctx.reset(token)
