from __future__ import annotations

import asyncio
import errno
import inspect
import math
import os
import stat
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Callable, ClassVar, Optional, TextIO
from urllib.parse import quote

from loguru import logger

from app.services import BaseService
from app.services.store import ExpiryCallbackEvent, StoreService

from . import _runtime

_RUNTIME_THREAD_START_TIMEOUT_SECONDS = 5.0
_RUNTIME_THREAD_STOP_TIMEOUT_SECONDS = 5.0
_RUNTIME_SUBMIT_TIMEOUT_SECONDS = 30.0
_RUNTIME_MAX_PENDING_CALLS = 1024
_RUNTIME_BLOCKING_WAIT_POLL_SECONDS = 0.1


@dataclass(frozen=True)
class TempFileConfig:
    base_dir: str
    retention_days: int
    cleanup_interval_seconds: int = 60
    total_size_recalc_seconds: int = 60 * 60
    worker_threads: int = 4
    max_file_size_mb: int = 0
    max_total_size_mb: int = 0
    namespace: str = "tmp_files"


@dataclass
class _SharedTempFileEntry:
    instance: "TempFileService"
    signature: tuple[Any, ...]
    ref_count: int = 1
    ready: threading.Event = field(default_factory=threading.Event)
    init_error: BaseException | None = None


class TempFileService(BaseService):
    """
    Temporary file manager.

    - Stores files under base_dir.
    - Persists metadata in StoreService (LMDB).
    - Uses StoreService expiry callbacks to delete expired files.
    - Periodically reconciles files missing metadata.
    """

    CALLBACK_NAME_PREFIX = "tmp_file_cleanup"
    INTERNAL_ARTIFACT_PREFIX = ".tmp_internal"
    INTERNAL_STAGING_PREFIX = f"{INTERNAL_ARTIFACT_PREFIX}.staging."
    INTERNAL_BACKUP_PREFIX = f"{INTERNAL_ARTIFACT_PREFIX}.backup."
    INTERNAL_ARTIFACT_RETENTION_SECONDS = 10 * 60

    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(
            base_dir: str,
            retention_days: int,
            cleanup_interval_seconds: int = 60,
            total_size_recalc_seconds: int = 60 * 60,
            worker_threads: int = 4,
            max_file_size_mb: int = 0,
            max_total_size_mb: int = 0,
            store_provider=None,
        ) -> "TempFileService":
            store = await TempFileService._resolve_store_provider(store_provider)
            return await TempFileService.acquire_shared(
                TempFileConfig(
                    base_dir=base_dir,
                    retention_days=retention_days,
                    cleanup_interval_seconds=cleanup_interval_seconds,
                    total_size_recalc_seconds=total_size_recalc_seconds,
                    worker_threads=worker_threads,
                    max_file_size_mb=max_file_size_mb,
                    max_total_size_mb=max_total_size_mb,
                ),
                store,
            )

        @staticmethod
        async def dtor(instance: "TempFileService") -> None:
            await TempFileService.release_shared(instance)

    _shared_instances: ClassVar[dict[str, _SharedTempFileEntry]] = {}
    _shared_instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @staticmethod
    def _shared_temp_key(config: TempFileConfig, store: StoreService) -> str:
        store_key = getattr(store, "_shared_instance_key", None) or f"store:{id(store)}"
        base_dir = str(Path(config.base_dir).expanduser().resolve())
        return f"{store_key}|{base_dir}|{config.namespace}"

    @staticmethod
    def _shared_temp_signature(config: TempFileConfig) -> tuple[Any, ...]:
        return (
            config.retention_days,
            config.cleanup_interval_seconds,
            config.total_size_recalc_seconds,
            config.worker_threads,
            config.max_file_size_mb,
            config.max_total_size_mb,
        )

    @staticmethod
    async def _wait_shared_entry_ready(entry: _SharedTempFileEntry) -> None:
        await asyncio.to_thread(entry.ready.wait)
        if entry.init_error is not None:
            raise RuntimeError(
                "TempFileService shared instance failed to initialize"
            ) from entry.init_error

    @classmethod
    async def acquire_shared(
        cls,
        config: TempFileConfig,
        store: StoreService,
    ) -> "TempFileService":
        key = cls._shared_temp_key(config, store)
        signature = cls._shared_temp_signature(config)
        entry: _SharedTempFileEntry
        created = False
        with cls._shared_instances_lock:
            entry = cls._shared_instances.get(key)
            if entry is not None and entry.instance._closed:
                cls._shared_instances.pop(key, None)
                entry = None

            if entry is not None:
                if entry.signature != signature:
                    raise RuntimeError(
                        "TempFileService config mismatch for shared key "
                        f"'{key}'. Keep TMP_* settings identical across workers."
                    )
                entry.ref_count += 1
            else:
                service = TempFileService(config, store)
                service._shared_instance_key = key
                entry = _SharedTempFileEntry(
                    instance=service,
                    signature=signature,
                    ref_count=1,
                )
                cls._shared_instances[key] = entry
                created = True

        if not created:
            try:
                await cls._wait_shared_entry_ready(entry)
            except Exception:
                with cls._shared_instances_lock:
                    current = cls._shared_instances.get(key)
                    if current is entry:
                        entry.ref_count -= 1
                        if entry.ref_count <= 0:
                            cls._shared_instances.pop(key, None)
                raise
            return entry.instance

        service = entry.instance
        try:
            await service._bootstrap()
        except BaseException as exc:
            with cls._shared_instances_lock:
                current = cls._shared_instances.get(key)
                if current is entry:
                    cls._shared_instances.pop(key, None)
                entry.init_error = exc
                entry.ready.set()
            try:
                await service.shutdown()
            except Exception:
                logger.exception("[TMP] Failed to cleanup after bootstrap error")
            raise

        entry.ready.set()
        return service

    @classmethod
    async def release_shared(cls, instance: "TempFileService") -> None:
        key = getattr(instance, "_shared_instance_key", None)
        if key is None:
            await instance.shutdown()
            return

        should_shutdown = False
        with cls._shared_instances_lock:
            entry = cls._shared_instances.get(key)
            if entry is None or entry.instance is not instance:
                return
            entry.ref_count -= 1
            if entry.ref_count <= 0:
                cls._shared_instances.pop(key, None)
                should_shutdown = True
        if should_shutdown:
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
        self._total_size_recalc_seconds = config.total_size_recalc_seconds
        self._namespace = config.namespace
        self._callback_name = self._build_callback_name(self._namespace)
        self._executor = ThreadPoolExecutor(
            max_workers=config.worker_threads,
            thread_name_prefix="temp-files",
        )
        self._namespace_lock = self._store.create_namespace_lock(
            self._namespace)
        self._write_guard = asyncio.Lock()

        self._total_size_bytes: int = 0
        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._size_recalc_task: Optional[asyncio.Task] = None
        self._cleanup_lock_handle: Optional[TextIO] = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._runtime_thread: threading.Thread | None = None
        self._runtime_thread_id: int | None = None
        self._runtime_ready = threading.Event()
        self._runtime_submit_semaphore = threading.BoundedSemaphore(
            _RUNTIME_MAX_PENDING_CALLS
        )
        self._closed = False
        self._executor_shutdown = False
        self._shared_instance_key: str | None = None
        self._start_runtime_thread()

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
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self._bootstrap)
        await self._store.mark_internal_namespace(self._namespace)
        await self._store.register_builtin_callback(self._callback_name)
        await self._store.register_expiry_callback(self._callback_name, self._on_expire)
        await self._adopt_existing_files()
        if self._max_total_size_bytes is not None and self._size_recalc_task is None:
            self._size_recalc_task = asyncio.create_task(
                self._total_size_recalc_loop(
                    self._cleanup_stop,
                    interval_seconds=self._total_size_recalc_seconds,
                )
            )

    async def start_cleanup(self) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.start_cleanup)
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
        if self._closed and not self._is_runtime_thread():
            await self._stop_runtime_thread()
            return
        if not self._is_runtime_thread():
            try:
                await self._run_on_runtime_thread(self.shutdown)
            finally:
                await self._stop_runtime_thread()
            return
        if self._closed:
            return
        self._cleanup_stop.set()
        if self._size_recalc_task is not None:
            await self._size_recalc_task
            self._size_recalc_task = None
        if self._cleanup_task is not None:
            await self._cleanup_task
        await self._run_in_executor(self._release_file_lock, self._cleanup_lock_handle)
        self._cleanup_lock_handle = None
        try:
            try:
                drained = await self._store.wait_for_callbacks(
                    timeout=5, raise_on_incomplete=False
                )
            except Exception:
                drained = False
                logger.exception("[TMP] Failed waiting for expiry callbacks")
            if not drained:
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
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.save, filename, content)
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
            self._total_size_bytes += len(data)
        try:
            await self._set_metadata(
                saved_name,
                is_text,
                self._retention_minutes,
                revision=revision,
            )
        except Exception:
            deleted_size = await self._run_in_executor(self._delete_file, saved_name)
            self._total_size_bytes -= deleted_size
            logger.exception(
                "[TMP] Failed to write metadata for temp file", name=saved_name)
            raise

        return saved_name

    async def save_overwrite(
        self,
        filename: str,
        content: str | bytes | bytearray | memoryview,
    ) -> str:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.save_overwrite,
                filename,
                content,
            )
        self._assert_open()
        base_name = self._sanitize_filename(filename)
        is_text, data = self._normalize_content(content)
        revision = self._new_revision()
        self._assert_size_allowed(len(data), filename=base_name)
        backup_name: str | None = None
        async with self._write_guard:
            await self._run_in_executor(
                self._assert_total_capacity_for_write,
                base_name,
                len(data),
                True,
            )
            now = time.time()
            staging_name = self._new_staging_name(base_name)
            await self._run_in_executor(
                self._write_staging_file,
                staging_name,
                data,
                now,
            )
            self._total_size_bytes += len(data)  # Staging file added
            backup_name = await self._run_in_executor(
                self._swap_staged_overwrite,
                base_name,
                staging_name,
            )
            try:
                await self._set_metadata(
                    base_name,
                    is_text,
                    self._retention_minutes,
                    revision=revision,
                )
            except Exception:
                # Rollback deletes the failed 'base' (which was 'staging') and restores backup.
                # The rollback function returns the actual size deleted.
                deleted_size = await self._run_in_executor(
                    self._rollback_overwrite_after_metadata_failure,
                    base_name,
                    backup_name,
                )
                self._total_size_bytes -= deleted_size
                logger.exception(
                    "[TMP] Failed to write metadata for temp file", name=base_name
                )
                raise
            else:
                # Discard backup
                deleted_size = await self._run_in_executor(
                    self._discard_backup_file, backup_name
                )
                self._total_size_bytes -= deleted_size

        return base_name

    async def read(self, filename: str) -> Optional[str | bytes]:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.read, filename)
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
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.cleanup_expired, now)
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
            self._total_size_bytes = 0
            return

        cutoff = now - self._retention_seconds
        internal_artifact_cutoff = now - self.INTERNAL_ARTIFACT_RETENTION_SECONDS
        delete_names: list[str] = []
        delete_internal_names: list[str] = []
        adopt_entries: list[tuple[str, float]] = []

        current_total = 0
        async with self._namespace_lock:
            for name, mtime, size in entries:
                current_total += size
                if self._is_internal_artifact_name(name):
                    if mtime < internal_artifact_cutoff:
                        delete_internal_names.append(name)
                    continue

                value = await self._store.get(self._namespace, name)
                if self._decode_metadata(value) is not None:
                    continue
                if mtime < cutoff:
                    delete_names.append(name)
                elif adopt_fresh:
                    adopt_entries.append((name, mtime))

        self._total_size_bytes = current_total

        if delete_internal_names:
            deleted_internal_size = await self._run_in_executor(
                self._delete_files,
                delete_internal_names,
            )
            self._total_size_bytes = max(
                0,
                self._total_size_bytes - deleted_internal_size,
            )

        if delete_names:
            deleted_size = await self._run_in_executor(
                self._delete_files,
                delete_names,
            )
            self._total_size_bytes = max(
                0,
                self._total_size_bytes - deleted_size,
            )

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
        if not self._is_runtime_thread():
            if self._closed:
                logger.debug(
                    "[TMP] Skipping expiry callback after service close", key=event.key
                )
                return
            await self._run_on_runtime_thread(self._on_expire, event)
            return
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

            deleted_size = await self._run_in_executor(self._delete_file, event.key)
            self._total_size_bytes -= deleted_size
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

    @classmethod
    def _new_staging_name(cls, base_name: str) -> str:
        safe_base = quote(base_name, safe="")
        return f"{cls.INTERNAL_STAGING_PREFIX}{safe_base}.{uuid.uuid4().hex}"

    @classmethod
    def _new_backup_name(cls, base_name: str) -> str:
        safe_base = quote(base_name, safe="")
        return f"{cls.INTERNAL_BACKUP_PREFIX}{safe_base}.{uuid.uuid4().hex}"

    @classmethod
    def _is_internal_artifact_name(cls, name: str) -> bool:
        if name.startswith(cls.INTERNAL_STAGING_PREFIX):
            return True
        if name.startswith(cls.INTERNAL_BACKUP_PREFIX):
            return True
        # Backward compatibility: clean legacy artifact names produced before the
        # internal-prefix migration.
        if name.startswith(".") and (".staging." in name or ".bak." in name):
            return True
        return False

    def _write_staging_file(self, staging_name: str, data: bytes, now: float) -> None:
        path = self._base_dir / staging_name
        try:
            self._write_new_file(path, data)
            self._touch_file(path, now)
        except Exception:
            try:
                if path.exists() or path.is_symlink():
                    path.unlink()
            except Exception:
                logger.exception("[TMP] Failed to remove partial staging file")
            raise

    def _swap_staged_overwrite(self, base_name: str, staging_name: str) -> str | None:
        path = self._base_dir / base_name
        staging_path = self._base_dir / staging_name
        backup_name: str | None = None
        backup_path: Path | None = None
        backup_moved = False
        try:
            try:
                st = path.lstat()
            except FileNotFoundError:
                st = None
            if st is not None:
                if not (stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode)):
                    raise ValueError(f"Path is not a regular file: {base_name}")
                backup_name = self._new_backup_name(base_name)
                backup_path = self._base_dir / backup_name
                os.replace(path, backup_path)
                backup_moved = True

            os.replace(staging_path, path)
            self._tighten_file_permissions(path)
            return backup_name
        except Exception:
            try:
                if staging_path.exists() or staging_path.is_symlink():
                    staging_path.unlink()
            except Exception:
                logger.exception("[TMP] Failed to remove staging file")
            if backup_moved and backup_path is not None:
                try:
                    if path.exists() or path.is_symlink():
                        path.unlink()
                    os.replace(backup_path, path)
                    self._tighten_file_permissions(path)
                except Exception:
                    logger.exception(
                        "[TMP] Failed to restore backup after overwrite swap failure",
                        name=base_name,
                    )
            raise

    def _rollback_overwrite_after_metadata_failure(
        self,
        base_name: str,
        backup_name: str | None,
    ) -> int:
        """Rollback failed overwrite by deleting file and restoring backup.

        Returns:
            The size of the file that was deleted (0 if no file was deleted).
        """
        path = self._base_dir / base_name
        if backup_name is None:
            return self._delete_file(base_name)

        backup_path = self._base_dir / backup_name
        deleted_size = 0
        try:
            st = path.lstat()
        except FileNotFoundError:
            st = None
        if st is not None:
            if not (stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode)):
                raise ValueError(f"Path is not a regular file: {base_name}")
            deleted_size = st.st_size
            path.unlink()
        os.replace(backup_path, path)
        self._tighten_file_permissions(path)
        return deleted_size

    def _discard_backup_file(self, backup_name: str | None) -> int:
        if not backup_name:
            return 0
        return self._delete_file(backup_name)

    def _touch_file(self, path: Path, now: float) -> None:
        os.utime(path, (now, now))

    def _delete_file(self, name: str) -> int:
        path = self._base_dir / name
        try:
            st = path.lstat()
            if stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode):
                size = st.st_size
                path.unlink()
                return size
        except FileNotFoundError:
            pass
        return 0

    def _delete_files(self, names: list[str]) -> int:
        deleted_size = 0
        for name in names:
            try:
                deleted_size += self._delete_file(name)
            except Exception:
                logger.exception("[TMP] Failed to delete file", name=name)
        return deleted_size

    def _scan_files(self) -> list[tuple[str, float, int]]:
        entries: list[tuple[str, float, int]] = []
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
                size = st.st_size
                entries.append((path.name, mtime, size))
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
        if config.total_size_recalc_seconds < 60:
            raise ValueError("total_size_recalc_seconds must be >= 60")
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
        return self._total_size_bytes

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

    async def _await_cancellable_thread_call(
        self,
        fn: Callable[[threading.Event], Any],
    ) -> Any:
        cancel_event = threading.Event()
        waiter = asyncio.create_task(asyncio.to_thread(fn, cancel_event))
        try:
            return await waiter
        except asyncio.CancelledError:
            cancel_event.set()
            try:
                await waiter
            except Exception:
                logger.exception("[TMP] Cancellation cleanup for thread wait failed")
            raise

    def _acquire_runtime_submit_slot(
        self,
        cancel_event: threading.Event,
    ) -> bool:
        deadline = time.monotonic() + _RUNTIME_SUBMIT_TIMEOUT_SECONDS
        while not cancel_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            wait_seconds = min(_RUNTIME_BLOCKING_WAIT_POLL_SECONDS, remaining)
            acquired = self._runtime_submit_semaphore.acquire(timeout=wait_seconds)
            if not acquired:
                continue
            if cancel_event.is_set():
                self._runtime_submit_semaphore.release()
                return False
            return True
        return False

    def _is_runtime_thread(self) -> bool:
        return self._runtime_thread_id == threading.get_ident()

    def _start_runtime_thread(self) -> None:
        if self._runtime_thread is not None:
            return
        self._runtime_ready.clear()
        self._runtime_thread = threading.Thread(
            target=self._runtime_thread_main,
            name="temp-file-runtime",
            daemon=True,
        )
        self._runtime_thread.start()
        if not self._runtime_ready.wait(timeout=_RUNTIME_THREAD_START_TIMEOUT_SECONDS):
            loop = self._runtime_loop
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            self._runtime_thread.join(timeout=_RUNTIME_THREAD_STOP_TIMEOUT_SECONDS)
            if self._runtime_thread.is_alive():
                raise RuntimeError(
                    "TempFileService runtime thread failed to start and could not be stopped"
                )
            self._runtime_thread = None
            self._runtime_loop = None
            self._runtime_thread_id = None
            raise RuntimeError(
                "TempFileService runtime thread did not start within timeout"
            )
        if self._runtime_loop is None:
            raise RuntimeError("TempFileService runtime thread failed to start")

    def _runtime_thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._runtime_loop = loop
        self._runtime_thread_id = threading.get_ident()
        asyncio.set_event_loop(loop)
        self._runtime_ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
            asyncio.set_event_loop(None)
            loop.close()
            self._runtime_loop = None
            self._runtime_thread_id = None

    async def _run_on_runtime_thread(self, fn, *args, **kwargs):
        if self._is_runtime_thread():
            return await fn(*args, **kwargs)
        loop = self._runtime_loop
        if loop is None:
            if self._closed:
                raise RuntimeError("TempFileService is closed")
            raise RuntimeError("TempFileService runtime loop is unavailable")
        acquired = await self._await_cancellable_thread_call(
            self._acquire_runtime_submit_slot
        )
        if not acquired:
            raise TimeoutError("TempFileService runtime queue is saturated")

        released = False
        released_lock = threading.Lock()

        def _release_slot(_fut=None) -> None:
            nonlocal released
            with released_lock:
                if released:
                    return
                released = True
            self._runtime_submit_semaphore.release()

        try:
            coro = fn(*args, **kwargs)
        except Exception:
            _release_slot()
            raise
        if not inspect.isawaitable(coro):
            _release_slot()
            raise TypeError("Runtime-dispatched function must return an awaitable")
        try:
            future = asyncio.run_coroutine_threadsafe(coro, loop)
        except Exception:
            coro.close()
            _release_slot()
            raise
        future.add_done_callback(_release_slot)
        return await asyncio.wrap_future(future)

    async def _stop_runtime_thread(self) -> None:
        thread = self._runtime_thread
        loop = self._runtime_loop
        if thread is None:
            return
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        await asyncio.to_thread(thread.join, _RUNTIME_THREAD_STOP_TIMEOUT_SECONDS)
        if thread.is_alive():
            raise RuntimeError("TempFileService runtime thread did not stop in time")
        self._runtime_thread = None

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

    async def _total_size_recalc_loop(
        self,
        stop_event: asyncio.Event,
        interval_seconds: int = 60 * 60,
    ) -> None:
        logger.debug(
            "[TMP] Total size recalculation loop started",
            interval_seconds=interval_seconds,
        )
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
                    break
                except asyncio.TimeoutError:
                    pass
                try:
                    await self._recalculate_total_size_now()
                except Exception:
                    logger.exception("[TMP] Total size recalculation failed")
        finally:
            logger.debug("[TMP] Total size recalculation loop stopped")

    async def _recalculate_total_size_now(self) -> None:
        async with self._write_guard:
            entries = await self._run_in_executor(self._scan_files)
            self._total_size_bytes = sum(size for _, _, size in entries)
