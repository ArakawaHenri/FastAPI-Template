from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapiex.di import BaseService, Service
from fastapiex.settings import BaseSettings, GetSettings, Settings

from app.service.shared.cleanup import cleanup_loop, release_file_lock, try_acquire_file_lock

logger = logging.getLogger(__name__)
DEFAULT_BUCKETS = ("logs", "uploads", "exports", "scratch")
_BUCKET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


@Settings("temp_file")
class TempFileSettings(BaseSettings):
    base_dir: str = "./temp"
    retention_days: int = 7
    cleanup_interval_seconds: int = 60
    buckets: list[str] = list(DEFAULT_BUCKETS)


@dataclass(frozen=True)
class TempFileConfig:
    base_dir: str = "./temp"
    retention_days: int = 7
    cleanup_interval_seconds: int = 60
    buckets: tuple[str, ...] = DEFAULT_BUCKETS

    def __post_init__(self) -> None:
        if not self.base_dir:
            raise ValueError("temp_file.base_dir must be non-empty")
        if self.retention_days < 1:
            raise ValueError("temp_file.retention_days must be >= 1")
        if self.cleanup_interval_seconds < 1:
            raise ValueError("temp_file.cleanup_interval_seconds must be >= 1")
        if not self.buckets:
            raise ValueError("temp_file.buckets must be non-empty")
        for bucket in self.buckets:
            _validate_bucket_name(bucket)


@Service("temp_file_service", eager=True)
class TempFileService(BaseService):
    """Temporary file manager with bucket isolation and retention cleanup."""

    @classmethod
    async def create(cls) -> TempFileService:
        cfg: TempFileSettings = GetSettings(TempFileSettings)
        service = cls(
            TempFileConfig(
                base_dir=cfg.base_dir,
                retention_days=cfg.retention_days,
                cleanup_interval_seconds=cfg.cleanup_interval_seconds,
                buckets=tuple(cfg.buckets),
            )
        )
        await service.bootstrap()
        await service.start_cleanup()
        return service

    @classmethod
    async def destroy(cls, instance: TempFileService) -> None:
        await instance.shutdown()

    def __init__(self, config: TempFileConfig | None = None) -> None:
        self._config = config or TempFileConfig()
        self._base_dir = Path(self._config.base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._buckets = tuple(dict.fromkeys(self._config.buckets))
        for bucket in self._buckets:
            (self._base_dir / bucket).mkdir(parents=True, exist_ok=True)
        self._retention_seconds = self._config.retention_days * 24 * 60 * 60
        self._table: dict[str, tuple[float, bool]] = {}
        self._table_lock = asyncio.Lock()
        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: asyncio.Task[Any] | None = None
        self._cleanup_lock_handle: Any | None = None
        self._closed = False
        logger.debug("TempFileService initialized base_dir=%s buckets=%s", self._base_dir, self._buckets)

    @property
    def config(self) -> TempFileConfig:
        return self._config

    async def bootstrap(self) -> None:
        self._assert_open()
        await asyncio.to_thread(self._load_existing_files)

    async def start_cleanup(self) -> None:
        self._assert_open()
        if self._cleanup_task is not None:
            return
        self._cleanup_lock_handle = try_acquire_file_lock(self._base_dir / ".tmp_cleanup.lock")
        if self._cleanup_lock_handle is None:
            logger.info("TempFileService cleanup loop skipped; lock is held")
            return
        self._cleanup_task = asyncio.create_task(
            cleanup_loop(
                "temp_file",
                self.cleanup_expired,
                self._cleanup_stop,
                interval_seconds=self._config.cleanup_interval_seconds,
            ),
            name="temp-file-cleanup",
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
        logger.debug("TempFileService shutdown completed")

    async def save(self, filename: str, content: str | bytes, bucket: str = "logs") -> str:
        """Write content to a unique file and return the actual filename used."""
        self._assert_open()
        bucket_name = self._sanitize_bucket(bucket)
        base_name = self._sanitize_filename(filename)
        is_text, data = self._to_bytes(content)
        now = time.time()
        saved_name = await asyncio.to_thread(self._write_unique, bucket_name, base_name, data, now)
        async with self._table_lock:
            self._table[self._table_key(bucket_name, saved_name)] = (now, is_text)
        return saved_name

    async def save_overwrite(self, filename: str, content: str | bytes, bucket: str = "logs") -> str:
        """Atomically replace a file with the provided content."""
        self._assert_open()
        bucket_name = self._sanitize_bucket(bucket)
        base_name = self._sanitize_filename(filename)
        is_text, data = self._to_bytes(content)
        now = time.time()
        await asyncio.to_thread(self._write_overwrite, bucket_name, base_name, data, now)
        async with self._table_lock:
            self._table[self._table_key(bucket_name, base_name)] = (now, is_text)
        return base_name

    async def read(self, filename: str, bucket: str = "logs") -> str | bytes:
        self._assert_open()
        bucket_name = self._sanitize_bucket(bucket)
        safe_name = self._sanitize_filename(filename)
        path = self._resolve_path(bucket_name, safe_name)
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(safe_name)
        data = await asyncio.to_thread(path.read_bytes)
        table_key = self._table_key(bucket_name, safe_name)
        async with self._table_lock:
            meta = self._table.get(table_key)
        is_text = meta[1] if meta is not None else self._is_utf8(data)
        now = time.time()
        await asyncio.to_thread(os.utime, path, (now, now))
        async with self._table_lock:
            self._table[table_key] = (now, is_text)
        return data.decode("utf-8") if is_text else data

    def path_for(self, filename: str, bucket: str = "logs") -> Path:
        self._assert_open()
        return self._resolve_path(self._sanitize_bucket(bucket), self._sanitize_filename(filename))

    async def cleanup_expired(self, now: float | None = None) -> int:
        self._assert_open()
        if now is None:
            now = time.time()
        cutoff = now - self._retention_seconds
        expired: list[str] = []
        async with self._table_lock:
            for name, (last_access, _) in list(self._table.items()):
                if last_access < cutoff:
                    expired.append(name)
            known = set(self._table)
        if expired:
            await asyncio.to_thread(self._delete_files, expired)
            async with self._table_lock:
                for name in expired:
                    self._table.pop(name, None)
        deleted_untracked = await asyncio.to_thread(self._cleanup_untracked, cutoff, known)
        return len(expired) + deleted_untracked

    def _write_unique(self, bucket: str, base_name: str, data: bytes, now: float) -> str:
        base_path = self._resolve_path(bucket, base_name)
        stem, suffix = base_path.stem, base_path.suffix
        attempt = 0
        while True:
            candidate = base_name if attempt == 0 else f"{stem}.{attempt}{suffix}" if suffix else f"{base_name}.{attempt}"
            path = self._resolve_path(bucket, candidate)
            try:
                with path.open("xb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.utime(path, (now, now))
                self._fsync_dir(path.parent)
                return candidate
            except FileExistsError:
                attempt += 1
            except Exception:
                path.unlink(missing_ok=True)
                raise

    def _write_overwrite(self, bucket: str, base_name: str, data: bytes, now: float) -> None:
        path = self._resolve_path(bucket, base_name)
        tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            with tmp_path.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
            os.utime(path, (now, now))
            self._fsync_dir(path.parent)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    def _delete_files(self, names: list[str]) -> None:
        for name in names:
            try:
                (self._base_dir / name).unlink(missing_ok=True)
            except Exception:
                logger.exception("TempFileService failed to delete file=%s", name)

    def _cleanup_untracked(self, cutoff: float, known: set[str]) -> int:
        deleted = 0
        for bucket in self._buckets:
            bucket_dir = self._base_dir / bucket
            for path in bucket_dir.iterdir():
                relative_key = self._table_key(bucket, path.name)
                if not path.is_file() or relative_key in known:
                    continue
                try:
                    if path.stat().st_mtime < cutoff:
                        path.unlink(missing_ok=True)
                        deleted += 1
                except Exception:
                    logger.exception("TempFileService failed cleanup of file=%s", relative_key)
        return deleted

    def _load_existing_files(self) -> None:
        now = time.time()
        cutoff = now - self._retention_seconds
        for bucket in self._buckets:
            bucket_dir = self._base_dir / bucket
            bucket_dir.mkdir(parents=True, exist_ok=True)
            for path in bucket_dir.iterdir():
                if not path.is_file():
                    continue
                try:
                    mtime = path.stat().st_mtime
                except Exception:
                    continue
                if mtime < cutoff:
                    path.unlink(missing_ok=True)
                    continue
                self._table[self._table_key(bucket, path.name)] = (mtime, False)

    def _sanitize_bucket(self, bucket: str) -> str:
        if bucket not in self._buckets:
            raise ValueError(f"Unsupported temp-file bucket: {bucket!r}")
        return bucket

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        if not filename or not filename.strip():
            raise ValueError("Filename must be non-empty")
        invalid = set('<>:"/\\|?*%')
        parts: list[str] = []
        for ch in filename:
            code = ord(ch)
            if ch in invalid or code == 0 or code < 32 or code == 127:
                parts.append("".join(f"%{byte:02X}" for byte in ch.encode()))
            else:
                parts.append(ch)
        safe_name = "".join(parts)
        if safe_name in {".", ".."}:
            raise ValueError("Filename must not be a path marker")
        return safe_name

    @staticmethod
    def _table_key(bucket: str, filename: str) -> str:
        return f"{bucket}/{filename}"

    def _resolve_path(self, bucket: str, filename: str) -> Path:
        return self._base_dir / bucket / filename

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        flags = getattr(os, "O_RDONLY", 0)
        dir_fd = os.open(path, flags)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    @staticmethod
    def _to_bytes(content: str | bytes) -> tuple[bool, bytes]:
        if isinstance(content, str):
            return True, content.encode("utf-8")
        return False, bytes(content)

    @staticmethod
    def _is_utf8(data: bytes) -> bool:
        try:
            data.decode("utf-8")
            return True
        except UnicodeDecodeError:
            return False

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("TempFileService is closed")


def _validate_bucket_name(bucket: str) -> None:
    if not _BUCKET_RE.fullmatch(bucket):
        raise ValueError(f"Invalid temp-file bucket: {bucket!r}")
    if bucket in {".", ".."}:
        raise ValueError(f"Invalid temp-file bucket: {bucket!r}")

