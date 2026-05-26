from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.core.logging import request_id_ctx

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware:
    """ASGI request logger with request-id propagation."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers") or [])
        request_id = headers.get(b"x-request-id", b"").decode("latin1") or str(uuid.uuid4())
        scope.setdefault("state", {})["request_id"] = request_id
        token = request_id_ctx.set(request_id)
        started_at = time.perf_counter()
        status_code = 500
        method = scope.get("method", "-")
        path = scope.get("path", "-")
        client = scope.get("client")
        client_host = client[0] if client else "-"

        logger.info("Request started method=%s path=%s client=%s", method, path, client_host)

        async def send_with_request_id(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                mutable_headers = MutableHeaders(scope=message)
                mutable_headers["X-Request-ID"] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except Exception:
            duration_ms = (time.perf_counter() - started_at) * 1000
            logger.exception("Request failed method=%s path=%s duration_ms=%.2f", method, path, duration_ms)
            raise
        finally:
            duration_ms = (time.perf_counter() - started_at) * 1000
            logger.info(
                "Request completed method=%s path=%s status_code=%s duration_ms=%.2f",
                method,
                path,
                status_code,
                duration_ms,
            )
            request_id_ctx.reset(token)


CallNext = Callable[[Any], Awaitable[Any]]

