from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from uuid import uuid4

from loguru import logger

from app.core.shared.tmpfile_cleanup import cleanup_loop, release_file_lock, try_acquire_file_lock
from app.services import BaseService


@dataclass(frozen=True)
class TempFileConfig:
    base_dir: str
    retention_days: int
    cleanup_interval_seconds: int = 60


class TempFileService(BaseService):
    """
    Temporary file manager.

    - Stores files under base_dir.
    - Maintains an in-memory table of (filename -> last_access, is_text).
    - Uses file mtime as a durable last-access indicator.
    - Periodically cleans up expired files.
    """

    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(
            base_dir: str,
            retention_days: int,
            cleanup_interval_seconds: int = 60,
        ) -> "TempFileService":
            service = TempFileService(
                TempFileConfig(
                    base_dir=base_dir,
                    retention_days=retention_days,
                    cleanup_interval_seconds=cleanup_interval_seconds,
                )
            )
            await service._bootstrap()
            return service

        @staticmethod
        async def dtor(instance: "TempFileService") -> None:
            await instance.shutdown()

    def __init__(self, config: TempFileConfig) -> None:
        self._validate_config(config)
        self._config = config
        self._base_dir = Path(config.base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

        self._retention_seconds = config.retention_days * 24 * 60 * 60
        self._cleanup_interval_seconds = config.cleanup_interval_seconds

        self._table: dict[str, tuple[float, bool]] = {}
        self._table_lock = asyncio.Lock()

        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_lock_handle = None
        self._closed = False

        logger.debug("[TMP] TempFileService initialized")

    async def _bootstrap(self) -> None:
        await asyncio.to_thread(self._load_existing_files)

    async def start_cleanup(self) -> None:
        self._assert_open()
        if self._cleanup_task is not None:
            return
        lock_path = self._cleanup_lock_path()
        self._cleanup_lock_handle = try_acquire_file_lock(lock_path)
        if self._cleanup_lock_handle is None:
            logger.info("[TMP] Cleanup loop skipped (lock not acquired)")
            return
        self._cleanup_task = asyncio.create_task(
            cleanup_loop(
                "tmp_files",
                self.cleanup_expired,
                self._cleanup_stop,
                interval_seconds=self._cleanup_interval_seconds,
            )
        )

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._cleanup_stop.set()
        if self._cleanup_task is not None:
            await self._cleanup_task
        release_file_lock(self._cleanup_lock_handle)
        self._cleanup_lock_handle = None
        self._closed = True
        logger.debug("[TMP] TempFileService shutdown completed")

    async def save(self, filename: str, content: str | bytes | bytearray | memoryview) -> str:
        self._assert_open()
        base_name = self._sanitize_filename(filename)
        is_text, data = self._normalize_content(content)
        now = time.time()

        saved_name = await asyncio.to_thread(self._write_unique, base_name, data, now)

        async with self._table_lock:
            self._table[saved_name] = (now, is_text)

        return saved_name

    async def read(self, filename: str) -> str | bytes:
        self._assert_open()
        safe_name = self._sanitize_filename(filename)
        path = self._base_dir / safe_name

        if not path.exists() or not path.is_file():
            raise FileNotFoundError(safe_name)

        data = await asyncio.to_thread(path.read_bytes)

        async with self._table_lock:
            meta = self._table.get(safe_name)

        if meta is None:
            is_text = self._is_utf8(data)
        else:
            is_text = meta[1]

        now = time.time()
        await asyncio.to_thread(self._touch_file, path, now)

        async with self._table_lock:
            self._table[safe_name] = (now, is_text)

        if is_text:
            return data.decode("utf-8")
        return data

    async def cleanup_expired(self, now: Optional[float] = None) -> None:
        self._assert_open()
        if now is None:
            now = time.time()

        cutoff = now - self._retention_seconds
        expired_names = []
        known_names: set[str]

        async with self._table_lock:
            for name, (last_access, _) in list(self._table.items()):
                if last_access < cutoff:
                    expired_names.append(name)
            known_names = set(self._table.keys())

        if expired_names:
            await asyncio.to_thread(self._delete_files, expired_names)
            async with self._table_lock:
                for name in expired_names:
                    self._table.pop(name, None)

        # Also remove any stale files not tracked in memory
        await asyncio.to_thread(self._cleanup_untracked, cutoff, known_names)

    def _sanitize_filename(self, filename: str) -> str:
        if not filename or filename.strip() == "":
            raise ValueError("Filename must be non-empty")
        if any(sep in filename for sep in ("/", "\\")):
            raise ValueError("Filename must not contain path separators")
        if filename in (".", ".."):
            raise ValueError("Invalid filename")
        if filename.startswith("."):
            filename = self._escape_leading_dots(filename)
        return filename

    def _normalize_content(self, content: str | bytes | bytearray | memoryview) -> tuple[bool, bytes]:
        if isinstance(content, str):
            return True, content.encode("utf-8")
        if isinstance(content, memoryview):
            return False, content.tobytes()
        if isinstance(content, (bytes, bytearray)):
            return False, bytes(content)
        raise TypeError("Content must be str or bytes-like")

    def _write_unique(self, base_name: str, data: bytes, now: float) -> str:
        base_path = self._base_dir / base_name
        stem = base_path.stem
        suffix = base_path.suffix

        attempt = 0
        while True:
            if attempt == 0:
                candidate = base_name
            else:
                if suffix:
                    candidate = f"{stem}.{attempt}{suffix}"
                else:
                    candidate = f"{base_name}.{attempt}"

            path = self._base_dir / candidate
            try:
                with path.open("xb") as handle:
                    handle.write(data)
                self._touch_file(path, now)
                return candidate
            except FileExistsError:
                attempt += 1
                continue
            except Exception:
                # Best-effort cleanup if file was created
                try:
                    if path.exists():
                        path.unlink()
                except Exception:
                    logger.exception("[TMP] Failed to remove partial file")
                raise

    def _touch_file(self, path: Path, now: float) -> None:
        os.utime(path, (now, now))

    def _delete_files(self, names: list[str]) -> None:
        for name in names:
            path = self._base_dir / name
            try:
                if path.exists() and path.is_file():
                    path.unlink()
            except Exception:
                logger.exception("[TMP] Failed to delete file", name=name)

    def _cleanup_untracked(self, cutoff: float, known_names: set[str]) -> None:
        try:
            for path in self._base_dir.iterdir():
                if not path.is_file():
                    continue
                if path.name == self._cleanup_lock_path().name:
                    continue
                if path.name in known_names:
                    continue
                try:
                    if path.stat().st_mtime < cutoff:
                        path.unlink()
                except Exception:
                    logger.exception(
                        "[TMP] Failed to delete stale file", name=path.name)
        except Exception:
            logger.exception("[TMP] Failed scanning temp dir for cleanup")

    def _load_existing_files(self) -> None:
        now = time.time()
        cutoff = now - self._retention_seconds
        for path in self._base_dir.iterdir():
            if not path.is_file():
                continue
            if path.name == self._cleanup_lock_path().name:
                continue
            try:
                mtime = path.stat().st_mtime
            except Exception:
                logger.exception("[TMP] Failed to stat file", name=path.name)
                continue

            if mtime < cutoff:
                try:
                    path.unlink()
                except Exception:
                    logger.exception(
                        "[TMP] Failed to delete expired file", name=path.name)
                continue

            self._table[path.name] = (mtime, False)

    def _cleanup_lock_path(self) -> Path:
        return self._base_dir / ".tmp_cleanup.lock"

    @staticmethod
    def _escape_leading_dots(name: str) -> str:
        i = 0
        while i < len(name) and name[i] == ".":
            i += 1
        if i == 0:
            return name
        return "%2E" * i + name[i:]

    @staticmethod
    def _is_utf8(data: bytes) -> bool:
        try:
            data.decode("utf-8")
            return True
        except UnicodeDecodeError:
            return False

    @staticmethod
    def _validate_config(config: TempFileConfig) -> None:
        if not config.base_dir:
            raise ValueError("base_dir must be non-empty")
        if config.retention_days < 1:
            raise ValueError("retention_days must be >= 1")
        if config.cleanup_interval_seconds < 1:
            raise ValueError("cleanup_interval_seconds must be >= 1")

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("TempFileService is closed")
