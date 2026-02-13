from __future__ import annotations

import contextvars
import logging
import sys
import threading
from pathlib import Path
from types import FrameType

from loguru import logger

# Context variable for request tracing across async calls
request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-")

_LOGGING_STATE_LOCK = threading.Lock()
_LOGGING_STATE_COND = threading.Condition(_LOGGING_STATE_LOCK)
_LOGGING_REFCOUNT = 0
_LOGGING_SHUTTING_DOWN = False
_LOGGING_INITIALIZING = False
_LOGGING_CONFIG: tuple[str, bool] | None = None

_CONFIG_MISMATCH_WARNING = (
    "setup_logging called with different config while logging is already initialized; "
    "keeping existing config"
)


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame: FrameType | None = logging.currentframe()
        depth = 2
        while frame is not None and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def _wait_for_logging_idle_locked() -> None:
    while _LOGGING_SHUTTING_DOWN or _LOGGING_INITIALIZING:
        _LOGGING_STATE_COND.wait()


def _configure_loguru(normalized_log_dir: str, debug: bool) -> None:
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


def setup_logging(log_dir: Path, debug: bool) -> None:
    global _LOGGING_REFCOUNT, _LOGGING_CONFIG, _LOGGING_INITIALIZING

    normalized_log_dir = str(Path(log_dir).expanduser().resolve())
    requested_config = (normalized_log_dir, debug)
    should_configure = False
    warn_config_mismatch = False

    with _LOGGING_STATE_COND:
        _wait_for_logging_idle_locked()

        if _LOGGING_REFCOUNT > 0:
            if _LOGGING_CONFIG != requested_config:
                warn_config_mismatch = True
            _LOGGING_REFCOUNT += 1
        else:
            _LOGGING_INITIALIZING = True
            should_configure = True

    if not should_configure:
        if warn_config_mismatch:
            logger.warning(_CONFIG_MISMATCH_WARNING)
        return

    try:
        _configure_loguru(normalized_log_dir, debug)
    except Exception:
        with _LOGGING_STATE_COND:
            _LOGGING_REFCOUNT = 0
            _LOGGING_INITIALIZING = False
            _LOGGING_CONFIG = None
            _LOGGING_STATE_COND.notify_all()
        raise

    with _LOGGING_STATE_COND:
        _LOGGING_REFCOUNT = 1
        _LOGGING_CONFIG = requested_config
        _LOGGING_INITIALIZING = False
        _LOGGING_STATE_COND.notify_all()


async def shutdown_logging() -> None:
    global _LOGGING_REFCOUNT, _LOGGING_SHUTTING_DOWN, _LOGGING_CONFIG

    with _LOGGING_STATE_COND:
        _wait_for_logging_idle_locked()

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
