from __future__ import annotations

import asyncio
import errno
import inspect
import math
import os
import stat
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Optional, TextIO
from urllib.parse import quote

from loguru import logger

from app.services import BaseService
from app.services.store import ExpiryCallbackEvent, StoreService

from . import _runtime


@dataclass(frozen=True)
class TempFileConfig:
    base_dir: str
    retention_days: int
    cleanup_interval_seconds: int = 60
    worker_threads: int = 4
    max_file_size_mb: int = 0
    max_total_size_mb: int = 0
    namespace: str = "tmp_files"


class TempFileService(BaseService):
    """
    Temporary file manager.

    - Stores files under base_dir.
    - Persists metadata in StoreService (LMDB).
    - Uses StoreService expiry callbacks to delete expired files.
    - Periodically reconciles files missing metadata.
    """

    CALLBACK_NAME_PREFIX = "tmp_file_cleanup"

    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(
            base_dir: str,
            retention_days: int,
            cleanup_interval_seconds: int = 60,
            worker_threads: int = 4,
            max_file_size_mb: int = 0,
            max_total_size_mb: int = 0,
            store_provider=None,
        ) -> "TempFileService":
            store = await TempFileService._resolve_store_provider(store_provider)
            service = TempFileService(
                TempFileConfig(
                    base_dir=base_dir,
                    retention_days=retention_days,
                    cleanup_interval_seconds=cleanup_interval_seconds,
                    worker_threads=worker_threads,
                    max_file_size_mb=max_file_size_mb,
                    max_total_size_mb=max_total_size_mb,
                ),
                store,
            )
            await service._bootstrap()
            return service

        @staticmethod
        async def dtor(instance: "TempFileService") -> None:
            await instance.shutdown()

    def __init__(self, config: TempFileConfig, store: StoreService) -> None:
        self._validate_config(config)
        self._config = config
        self._store = store
        self._base_dir = Path(config.base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._tighten_directory_permissions(self._base_dir)

        self._retention_seconds = config.retention_days * 24 * 60 * 60
        self._retention_minutes = config.retention_days * 24 * 60
        self._max_file_size_bytes: Optional[int] = None
        if config.max_file_size_mb > 0:
            self._max_file_size_bytes = config.max_file_size_mb * 1024 * 1024
        self._max_total_size_bytes: Optional[int] = None
        if config.max_total_size_mb > 0:
            self._max_total_size_bytes = config.max_total_size_mb * 1024 * 1024
        self._cleanup_interval_seconds = config.cleanup_interval_seconds
        self._namespace = config.namespace
        self._callback_name = self._build_callback_name(self._namespace)
        self._executor = ThreadPoolExecutor(
            max_workers=config.worker_threads,
            thread_name_prefix="temp-files",
        )
        self._namespace_lock = self._store.create_namespace_lock(
            self._namespace)
        self._write_guard = asyncio.Lock()

        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_lock_handle: Optional[TextIO] = None
        self._closed = False
        self._executor_shutdown = False

        logger.debug("[TMP] TempFileService initialized")

    @classmethod
    def _build_callback_name(cls, namespace: str) -> str:
        # Stable across workers and restarts for the same namespace.
        return f"{cls.CALLBACK_NAME_PREFIX}:{quote(namespace, safe='')}"

    @staticmethod
    def _new_revision() -> str:
        return uuid.uuid4().hex

    @staticmethod
    def _encode_metadata(is_text: bool, revision: str) -> dict[str, object]:
        return {
            "is_text": is_text,
            "revision": revision,
        }

    @staticmethod
    def _decode_metadata(value: object) -> tuple[bool, str] | None:
        if not isinstance(value, dict):
            return None
        is_text = value.get("is_text")
        if not isinstance(is_text, bool):
            return None
        revision = value.get("revision")
        if not isinstance(revision, str):
            return None
        if revision.strip() == "":
            return None
        return is_text, revision

    @staticmethod
    async def _resolve_store_provider(store_provider) -> StoreService:
        if store_provider is None:
            raise ValueError(
                "TempFileService requires a StoreService dependency")
        if isinstance(store_provider, StoreService):
            return store_provider
        if callable(store_provider):
            result = store_provider()
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, StoreService):
                return result
        raise TypeError("store_provider must resolve to StoreService")

    async def _bootstrap(self) -> None:
        await self._store.mark_internal_namespace(self._namespace)
        await self._store.register_builtin_callback(self._callback_name)
        await self._store.register_expiry_callback(self._callback_name, self._on_expire)
        await self._adopt_existing_files()

    async def start_cleanup(self) -> None:
        self._assert_open()
        if self._cleanup_task is not None:
            return
        lock_path = self._cleanup_lock_path()
        self._cleanup_lock_handle = await self._run_in_executor(
            self._try_acquire_file_lock, lock_path
        )
        if self._cleanup_lock_handle is None:
            logger.info("[TMP] Cleanup loop skipped (lock not acquired)")
            return
        self._cleanup_task = asyncio.create_task(
            self._cleanup_loop(
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
        await self._run_in_executor(self._release_file_lock, self._cleanup_lock_handle)
        self._cleanup_lock_handle = None
        try:
            try:
                await asyncio.wait_for(self._store.wait_for_callbacks(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning(
                    "[TMP] Timed out waiting for expiry callbacks to drain")
            await self._store.unregister_expiry_callback(self._callback_name)
        except Exception:
            logger.exception("[TMP] Failed to unregister expiry callback")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            partial(self._executor.shutdown, wait=True, cancel_futures=True),
        )
        self._executor_shutdown = True
        self._closed = True
        logger.debug("[TMP] TempFileService shutdown completed")

    async def save(self, filename: str, content: str | bytes | bytearray | memoryview) -> str:
        self._assert_open()
        base_name = self._sanitize_filename(filename)
        is_text, data = self._normalize_content(content)
        revision = self._new_revision()
        self._assert_size_allowed(len(data), filename=base_name)
        async with self._write_guard:
            await self._run_in_executor(
                self._assert_total_capacity_for_write,
                base_name,
                len(data),
                False,
            )
            now = time.time()
            saved_name = await self._run_in_executor(
                self._write_unique, base_name, data, now
            )
        try:
            await self._set_metadata(
                saved_name,
                is_text,
                self._retention_minutes,
                revision=revision,
            )
        except Exception:
            await self._run_in_executor(self._delete_file, saved_name)
            logger.exception(
                "[TMP] Failed to write metadata for temp file", name=saved_name)
            raise

        return saved_name

    async def save_overwrite(
        self,
        filename: str,
        content: str | bytes | bytearray | memoryview,
    ) -> str:
        self._assert_open()
        base_name = self._sanitize_filename(filename)
        is_text, data = self._normalize_content(content)
        revision = self._new_revision()
        self._assert_size_allowed(len(data), filename=base_name)
        async with self._write_guard:
            await self._run_in_executor(
                self._assert_total_capacity_for_write,
                base_name,
                len(data),
                True,
            )
            now = time.time()
            await self._run_in_executor(self._write_overwrite, base_name, data, now)
        try:
            await self._set_metadata(
                base_name,
                is_text,
                self._retention_minutes,
                revision=revision,
            )
        except Exception:
            await self._run_in_executor(self._delete_file, base_name)
            logger.exception(
                "[TMP] Failed to write metadata for temp file", name=base_name)
            raise

        return base_name

    async def read(self, filename: str) -> Optional[str | bytes]:
        self._assert_open()
        safe_name = self._sanitize_filename(filename)
        path = self._base_dir / safe_name

        if path.is_symlink():
            logger.warning(
                "[TMP] Refusing to read symlink temp file", name=safe_name)
            return None
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(safe_name)
        try:
            size_bytes = await self._run_in_executor(self._get_size_bytes, path)
        except ValueError:
            if path.is_symlink():
                logger.warning(
                    "[TMP] Refusing to read symlink temp file", name=safe_name)
                return None
            raise
        self._assert_size_allowed(size_bytes, filename=safe_name)

        try:
            data = await self._run_in_executor(self._read_file_bytes, path)
        except ValueError:
            if path.is_symlink():
                logger.warning(
                    "[TMP] Refusing to read symlink temp file", name=safe_name)
                return None
            raise
        now = time.time()

        metadata = await self._resolve_metadata(safe_name, path, data, now)
        if metadata is None:
            raise FileNotFoundError(safe_name)
        is_text, revision = metadata

        await self._run_in_executor(self._touch_file, path, now)
        await self._set_metadata(
            safe_name,
            is_text,
            self._retention_minutes,
            revision=revision,
        )

        if is_text:
            return data.decode("utf-8")
        return data

    async def cleanup_expired(self, now: Optional[float] = None) -> None:
        self._assert_open()
        if now is None:
            now = time.time()

        await self._store.cleanup_expired(now)
        await self._reconcile_files(now, adopt_fresh=False)

    async def _set_metadata(
        self,
        name: str,
        is_text: bool,
        retention_minutes: int,
        revision: str,
    ) -> None:
        payload = self._encode_metadata(is_text, revision)
        async with self._namespace_lock:
            await self._store.set(
                self._namespace,
                name,
                payload,
                retention=retention_minutes,
                on_expire=self._callback_name,
            )

    async def _get_metadata(self, name: str) -> tuple[bool, str] | None:
        async with self._namespace_lock:
            value = await self._store.get(self._namespace, name)
        return self._decode_metadata(value)

    async def _resolve_metadata(
        self,
        name: str,
        path: Path,
        data: bytes,
        now: float,
    ) -> tuple[bool, str] | None:
        meta = await self._get_metadata(name)
        if meta is not None:
            return meta

        try:
            mtime = await self._run_in_executor(self._get_mtime, path)
        except Exception:
            logger.exception("[TMP] Failed to stat temp file", name=name)
            return None

        if now - mtime >= self._retention_seconds:
            await self._run_in_executor(self._delete_file, name)
            return None

        return self._is_utf8(data), self._new_revision()

    async def _adopt_existing_files(self) -> None:
        now = time.time()
        await self._reconcile_files(now, adopt_fresh=True)

    async def _reconcile_files(self, now: float, adopt_fresh: bool = True) -> None:
        entries = await self._run_in_executor(self._scan_files)
        if not entries:
            return

        cutoff = now - self._retention_seconds
        delete_names: list[str] = []
        adopt_entries: list[tuple[str, float]] = []

        async with self._namespace_lock:
            for name, mtime in entries:
                value = await self._store.get(self._namespace, name)
                if self._decode_metadata(value) is not None:
                    continue
                if mtime < cutoff:
                    delete_names.append(name)
                elif adopt_fresh:
                    adopt_entries.append((name, mtime))

        if delete_names:
            await self._run_in_executor(self._delete_files, delete_names)

        for name, mtime in adopt_entries:
            path = self._base_dir / name
            try:
                data = await self._run_in_executor(self._read_file_bytes, path)
            except Exception:
                logger.exception("[TMP] Failed to read temp file", name=name)
                continue
            is_text = self._is_utf8(data)
            remaining = max(1, math.ceil(
                (self._retention_seconds - (now - mtime)) / 60))
            try:
                await self._set_metadata(
                    name,
                    is_text,
                    remaining,
                    revision=self._new_revision(),
                )
            except Exception:
                logger.exception(
                    "[TMP] Failed to adopt temp file metadata", name=name)
                continue

    async def _on_expire(self, event: ExpiryCallbackEvent) -> None:
        if self._executor_shutdown:
            logger.debug(
                "[TMP] Skipping expiry callback after shutdown", key=event.key)
            return
        try:
            path = self._base_dir / event.key
            try:
                mtime = await self._run_in_executor(self._get_mtime, path)
            except FileNotFoundError:
                return
            except ValueError:
                logger.warning(
                    "[TMP] Expiry callback skipped non-regular file", key=event.key
                )
                return

            if event.expire_ts > 0 and mtime > float(event.expire_ts):
                logger.debug(
                    "[TMP] Skipping stale expiry callback for newer temp file",
                    key=event.key,
                    expire_ts=event.expire_ts,
                    mtime=mtime,
                )
                return

            event_metadata = self._decode_metadata(event.value)
            expected_revision = event_metadata[1] if event_metadata is not None else None
            if expected_revision is not None:
                current_meta = await self._get_metadata(event.key)
                if current_meta is not None:
                    _, current_revision = current_meta
                    if current_revision != expected_revision:
                        logger.debug(
                            "[TMP] Skipping stale expiry callback due to revision mismatch",
                            key=event.key,
                            expected_revision=expected_revision,
                            current_revision=current_revision,
                        )
                        return

            await self._run_in_executor(self._delete_file, event.key)
        except Exception:
            logger.exception("[TMP] Expiry callback failed", key=event.key)

    def _sanitize_filename(self, filename: str) -> str:
        if not filename or filename.strip() == "":
            raise ValueError("Filename must be non-empty")
        filename = self._encode_filename(filename)
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
                candidate = (
                    f"{stem}.{attempt}{suffix}"
                    if suffix
                    else f"{base_name}.{attempt}"
                )

            path = self._base_dir / candidate
            try:
                self._write_new_file(path, data)
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

    def _write_overwrite(self, base_name: str, data: bytes, now: float) -> None:
        path = self._base_dir / base_name
        try:
            # Explicitly remove symlink target and create a fresh regular file.
            if path.is_symlink():
                path.unlink()
            self._write_replace_file(path, data)
            self._touch_file(path, now)
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

    def _delete_file(self, name: str) -> None:
        path = self._base_dir / name
        try:
            st = path.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode):
            path.unlink()

    def _delete_files(self, names: list[str]) -> None:
        for name in names:
            try:
                self._delete_file(name)
            except Exception:
                logger.exception("[TMP] Failed to delete file", name=name)

    def _scan_files(self) -> list[tuple[str, float]]:
        entries: list[tuple[str, float]] = []
        lock_name = self._cleanup_lock_path().name
        try:
            for path in self._base_dir.iterdir():
                if path.name == lock_name:
                    continue
                try:
                    st = path.lstat()
                except Exception:
                    logger.exception(
                        "[TMP] Failed to stat file", name=path.name)
                    continue
                if stat.S_ISLNK(st.st_mode):
                    logger.warning(
                        "[TMP] Ignoring symlink in temp dir", name=path.name)
                    continue
                if not stat.S_ISREG(st.st_mode):
                    continue
                mtime = st.st_mtime
                entries.append((path.name, mtime))
        except Exception:
            logger.exception("[TMP] Failed scanning temp dir")
        return entries

    @staticmethod
    def _get_mtime(path: Path) -> float:
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode):
            raise ValueError(f"Path is not a regular file: {path.name}")
        return st.st_mtime

    @staticmethod
    def _get_size_bytes(path: Path) -> int:
        st = path.lstat()
        if stat.S_ISLNK(st.st_mode):
            raise ValueError(f"Path is symlink: {path.name}")
        if not stat.S_ISREG(st.st_mode):
            raise ValueError(f"Path is not a regular file: {path.name}")
        return st.st_size

    def _cleanup_lock_path(self) -> Path:
        return self._base_dir / ".tmp_cleanup.lock"

    @staticmethod
    def _encode_filename(name: str) -> str:
        # Encode Windows/Linux-invalid characters + '%' to avoid collisions.
        invalid_chars = set('<>:"/\\|?*%')

        parts: list[str] = []
        for ch in name:
            code = ord(ch)
            if ch in invalid_chars or code == 0 or code < 32 or code == 127:
                parts.append(TempFileService._pct_encode_char(ch))
            else:
                parts.append(ch)
        encoded = "".join(parts)

        # Encode trailing spaces/dots (invalid on Windows).
        i = len(encoded)
        while i > 0 and encoded[i - 1] in (" ", "."):
            i -= 1
        if i != len(encoded):
            trailing = encoded[i:]
            encoded = encoded[:i] + "".join(
                TempFileService._pct_encode_char(ch) for ch in trailing
            )

        # Encode Windows reserved device names (case-insensitive), e.g. CON, PRN, AUX, NUL, COM1-9, LPT1-9.
        base, dot, ext = encoded.partition(".")
        base_stripped = base.rstrip(" .")
        upper_base = base_stripped.upper()
        if TempFileService._is_windows_reserved_name(upper_base):
            encoded_base = "".join(
                TempFileService._pct_encode_char(ch) for ch in base)
            encoded = encoded_base + (dot + ext if dot else "")

        return encoded

    @staticmethod
    def _pct_encode_char(ch: str) -> str:
        return "".join(f"%{b:02X}" for b in ch.encode("utf-8"))

    @staticmethod
    def _is_windows_reserved_name(name: str) -> bool:
        if name in {"CON", "PRN", "AUX", "NUL"}:
            return True
        if len(name) == 4 and name[:3] in {"COM", "LPT"} and name[3].isdigit():
            return name[3] != "0"
        return False

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
        if config.worker_threads < 1:
            raise ValueError("worker_threads must be >= 1")
        if config.max_file_size_mb < 0:
            raise ValueError("max_file_size_mb must be >= 0")
        if config.max_total_size_mb < 0:
            raise ValueError("max_total_size_mb must be >= 0")
        if not config.namespace:
            raise ValueError("namespace must be non-empty")

    def _assert_size_allowed(self, size_bytes: int, filename: str) -> None:
        if self._max_file_size_bytes is None or size_bytes <= self._max_file_size_bytes:
            return
        logger.error(
            "[TMP] Temp file exceeds configured single-file size limit",
            name=filename,
            size_bytes=size_bytes,
            max_file_size_bytes=self._max_file_size_bytes,
        )
        raise ValueError(
            f"Temp file '{filename}' exceeds max size: "
            f"{size_bytes} bytes > {self._max_file_size_bytes} bytes"
        )

    def _assert_total_capacity_for_write(
        self, base_name: str, new_size_bytes: int, overwrite: bool
    ) -> None:
        if self._max_total_size_bytes is None:
            return
        current_total = self._get_total_size_bytes()
        replaced_size = 0
        if overwrite:
            target = self._base_dir / base_name
            try:
                st = target.lstat()
            except FileNotFoundError:
                st = None
            if st is not None and stat.S_ISREG(st.st_mode):
                replaced_size = st.st_size
        projected = current_total - replaced_size + new_size_bytes
        if projected <= self._max_total_size_bytes:
            return
        logger.error(
            "[TMP] Temp files exceed configured total size limit",
            filename=base_name,
            new_size_bytes=new_size_bytes,
            current_total_bytes=current_total,
            projected_total_bytes=projected,
            max_total_size_bytes=self._max_total_size_bytes,
            overwrite=overwrite,
        )
        raise ValueError(
            "Temp files total size exceeds max: "
            f"{projected} bytes > {self._max_total_size_bytes} bytes"
        )

    def _get_total_size_bytes(self) -> int:
        total = 0
        lock_name = self._cleanup_lock_path().name
        for path in self._base_dir.iterdir():
            if path.name == lock_name:
                continue
            try:
                st = path.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(st.st_mode):
                total += st.st_size
        return total

    @staticmethod
    def _tighten_directory_permissions(path: Path) -> None:
        _runtime.tighten_directory_permissions(path)

    @staticmethod
    def _tighten_file_permissions(path: Path) -> None:
        _runtime.tighten_file_permissions(path)

    def _write_new_file(self, path: Path, data: bytes) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
        finally:
            os.close(fd)
        self._tighten_file_permissions(path)

    def _write_replace_file(self, path: Path, data: bytes) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(path, flags, 0o600)
        except OSError as exc:
            if exc.errno == errno.ELOOP and path.is_symlink():
                path.unlink()
                fd = os.open(path, flags, 0o600)
            else:
                raise
        try:
            with os.fdopen(fd, "wb", closefd=False) as handle:
                handle.write(data)
                handle.flush()
        finally:
            os.close(fd)
        self._tighten_file_permissions(path)

    def _read_file_bytes(self, path: Path) -> bytes:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError(
                    f"Refusing to read symlink temp file '{path.name}'"
                ) from exc
            raise
        try:
            with os.fdopen(fd, "rb", closefd=False) as handle:
                return handle.read()
        finally:
            os.close(fd)

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("TempFileService is closed")

    async def _run_in_executor(self, fn, *args, **kwargs):
        return await _runtime.run_in_executor(self._executor, fn, *args, **kwargs)

    @staticmethod
    def _try_acquire_file_lock(path: Path) -> Optional[TextIO]:
        return _runtime.try_acquire_file_lock(path)

    @staticmethod
    def _release_file_lock(handle: Optional[TextIO]) -> None:
        _runtime.release_file_lock(handle)

    async def _cleanup_loop(
        self,
        name: str,
        cleanup_fn,
        stop_event: asyncio.Event,
        interval_seconds: int = 60,
    ) -> None:
        await _runtime.cleanup_loop(name, cleanup_fn, stop_event, interval_seconds)
