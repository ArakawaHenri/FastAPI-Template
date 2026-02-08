from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Optional, TextIO

import portalocker
from loguru import logger
from portalocker.exceptions import LockException

_LOG_PREFIX = "TMP"


async def run_in_executor(
    executor: ThreadPoolExecutor,
    fn,
    *args,
    **kwargs,
):
    """Run sync work on the service-owned thread pool."""
    if kwargs:
        fn = partial(fn, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, fn, *args)


def try_acquire_file_lock(path: Path) -> Optional[TextIO]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    tighten_file_permissions(path)
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
        logger.exception(f"[{_LOG_PREFIX}] Failed to release file lock")
    finally:
        handle.close()


def tighten_directory_permissions(path: Path) -> None:
    try:
        os.chmod(path, 0o700)
    except Exception:
        logger.exception(f"[{_LOG_PREFIX}] Failed to set directory permissions")


def tighten_file_permissions(path: Path) -> None:
    try:
        os.chmod(path, 0o600)
    except Exception:
        logger.exception(
            f"[{_LOG_PREFIX}] Failed to set file permissions",
            name=path.name,
        )


async def cleanup_loop(
    name: str,
    cleanup_fn,
    stop_event: asyncio.Event,
    interval_seconds: int = 60,
) -> None:
    # Keep the loop tiny: one cleanup attempt + a cancellable sleep.
    logger.debug(f"[{_LOG_PREFIX}] Cleanup loop started: {name}")
    try:
        while not stop_event.is_set():
            try:
                await cleanup_fn(time.time())
            except Exception:
                logger.exception(f"[{_LOG_PREFIX}] Cleanup loop failed")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue
    finally:
        logger.debug(f"[{_LOG_PREFIX}] Cleanup loop stopped: {name}")
