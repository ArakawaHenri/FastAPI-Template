import logging
import sys
from pathlib import Path

from loguru import logger


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


def setup_logging(log_dir: str, debug: bool):
    logger.remove()

    console_level = "DEBUG" if debug else "INFO"

    logger.add(
        sys.stderr,
        level=console_level,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"
    )

    log_file_path = Path(log_dir) / "app_{time}.log"

    logger.add(
        log_file_path,
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation="00:00",
        retention="7 days",
        compression="zip",
        enqueue=True,
        backtrace=True,
        diagnose=True,
    )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    logger.debug("Here logger stands.")


async def shutdown_logging():
    logger.debug("Flushing all log messages before shutdown...")
    await logger.complete()
    logger.remove()
