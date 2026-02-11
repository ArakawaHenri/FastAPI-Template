from __future__ import annotations

import contextvars
import logging
import sys
import threading
from pathlib import Path

from loguru import logger

# Context variable for request tracing across async calls
request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-")

_LOGGING_STATE_LOCK = threading.Lock()
_LOGGING_STATE_COND = threading.Condition(_LOGGING_STATE_LOCK)
_LOGGING_REFCOUNT = 0
_LOGGING_SHUTTING_DOWN = False
_LOGGING_CONFIG: tuple[str, bool] | None = None


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging(log_dir: Path, debug: bool):
    global _LOGGING_REFCOUNT, _LOGGING_CONFIG

    normalized_log_dir = str(Path(log_dir).expanduser().resolve())

    with _LOGGING_STATE_COND:
        while _LOGGING_SHUTTING_DOWN:
            _LOGGING_STATE_COND.wait()

        if _LOGGING_REFCOUNT > 0:
            requested_config = (normalized_log_dir, debug)
            if _LOGGING_CONFIG != requested_config:
                logger.warning(
                    "setup_logging called with different config while logging is already initialized; "
                    "keeping existing config"
                )
            _LOGGING_REFCOUNT += 1
            return

        _LOGGING_REFCOUNT = 1
        _LOGGING_CONFIG = (normalized_log_dir, debug)

    logger.remove()

    console_level = "DEBUG" if debug else "INFO"

    log_dir_path = Path(normalized_log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)

    logger.add(
        sys.stderr,
        level=console_level,
        colorize=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> - "
            "<level>{message}</level>"
        ),
    )

    log_file_path = log_dir_path / "app_{time}.log"

    def request_id_patcher(record):
        record["extra"]["request_id"] = request_id_ctx.get()

    logger.configure(patcher=request_id_patcher)

    logger.add(
        log_file_path,
        level="DEBUG",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
            "{extra[request_id]} | {name}:{function}:{line} | {message}"
        ),
        rotation="00:00",
        retention="7 days",
        compression="zip",
        enqueue=True,
        backtrace=debug,
        diagnose=debug,
    )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.debug("Here logger stands.")


async def shutdown_logging():
    global _LOGGING_REFCOUNT, _LOGGING_SHUTTING_DOWN, _LOGGING_CONFIG

    with _LOGGING_STATE_COND:
        while _LOGGING_SHUTTING_DOWN:
            _LOGGING_STATE_COND.wait()

        if _LOGGING_REFCOUNT == 0:
            return

        _LOGGING_REFCOUNT -= 1
        if _LOGGING_REFCOUNT > 0:
            return

        _LOGGING_SHUTTING_DOWN = True

    try:
        logger.debug("Flushing all log messages before shutdown...")
        await logger.complete()
        logger.remove()
    finally:
        with _LOGGING_STATE_COND:
            _LOGGING_SHUTTING_DOWN = False
            _LOGGING_CONFIG = None
            _LOGGING_STATE_COND.notify_all()
