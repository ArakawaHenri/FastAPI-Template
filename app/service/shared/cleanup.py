from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TextIO

import portalocker
from portalocker.exceptions import LockException

logger = logging.getLogger(__name__)


def try_acquire_file_lock(path: Path) -> TextIO | None:
    """Acquire an exclusive non-blocking file lock, returning None if held."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
    except LockException:
        handle.close()
        return None
    except Exception:
        handle.close()
        raise
    return handle


def release_file_lock(handle: TextIO | None) -> None:
    if handle is None:
        return
    try:
        portalocker.unlock(handle)
    except Exception:
        logger.exception("Failed to release file lock")
    finally:
        handle.close()


async def cleanup_loop(
    name: str,
    cleanup_fn: Callable[[float | None], Awaitable[object]],
    stop_event: asyncio.Event,
    *,
    interval_seconds: int = 60,
) -> None:
    logger.debug("Cleanup loop started: %s", name)
    try:
        while not stop_event.is_set():
            try:
                await cleanup_fn(time.time())
            except Exception:
                logger.exception("Cleanup loop error: %s", name)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue
    finally:
        logger.debug("Cleanup loop stopped: %s", name)

