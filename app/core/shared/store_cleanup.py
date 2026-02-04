from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Optional, TextIO

import portalocker
from loguru import logger
from portalocker.exceptions import LockException


def try_acquire_file_lock(path: Path) -> Optional[TextIO]:
    """
    Try to acquire an exclusive, non-blocking file lock.

    Returns a file handle if lock is acquired, otherwise None.
    """
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


def release_file_lock(handle: Optional[TextIO]) -> None:
    if handle is None:
        return
    try:
        portalocker.unlock(handle)
    except Exception:
        logger.exception("[STORE] Failed to release file lock")
    finally:
        handle.close()


async def cleanup_loop(
    name: str,
    cleanup_fn,
    stop_event: asyncio.Event,
    interval_seconds: int = 60,
) -> None:
    """
    Periodically run cleanup for expired records.

    This loop runs until stop_event is set.
    """
    logger.debug(f"[STORE] Cleanup loop started: {name}")
    try:
        while not stop_event.is_set():
            try:
                await cleanup_fn(time.time())
            except Exception:
                logger.exception("[STORE] Cleanup loop failed")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue
    finally:
        logger.debug(f"[STORE] Cleanup loop stopped: {name}")
