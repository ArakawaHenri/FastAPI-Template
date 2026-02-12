from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable


@dataclass(frozen=True)
class ExpiryCallbackEvent:
    namespace: str
    key: str
    value: object | None
    expire_ts: int
    callback: str


class ExpiryCallbackDeferred(Exception):
    """Signal that callback processing should be deferred without consuming retries."""


ExpiryCallback = Callable[[ExpiryCallbackEvent], Awaitable[None]]


@dataclass(frozen=True)
class _CallbackJob:
    job_id: bytes
    due_ts: int
    event: ExpiryCallbackEvent
    attempts: int


@dataclass(frozen=True)
class _CallbackRegistration:
    fn: ExpiryCallback
    timeout_seconds: float


class _CallbackExecutionResult(Enum):
    COMPLETE = "complete"
    RETRY = "retry"
    DEFER = "defer"
