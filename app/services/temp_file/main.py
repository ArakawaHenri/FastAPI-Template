from __future__ import annotations

import asyncio
import errno
import inspect
import os
import stat
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import ClassVar
from urllib.parse import quote

from filelock import FileLock
from loguru import logger

from app.core.settings import settings
from app.services import BaseService, Service, require
from app.services.store import StoreService

from . import _runtime
from ._file_ops import TempFileFileOpsMixin
from ._metadata import TempFileMetadataMixin
from ._reconciler import TempFileReconcilerMixin
from ._runtime_dispatch import TempFileRuntimeDispatchMixin


def _new_signaled_event() -> threading.Event:
    event = threading.Event()
    event.set()
    return event


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
    instance: TempFileService
    signature: tuple[object, ...]
    ref_count: int = 1
    ready: threading.Event = field(default_factory=threading.Event)
    init_error: BaseException | None = None
    closing: bool = False
    close_done: threading.Event = field(default_factory=_new_signaled_event)


type StoreProvider = (
    StoreService
    | Callable[[], StoreService | Awaitable[StoreService]]
)


@Service("temp_file_service", eager=True)
class TempFileService(
    TempFileRuntimeDispatchMixin,
    TempFileMetadataMixin,
    TempFileReconcilerMixin,
    TempFileFileOpsMixin,
    BaseService,
):
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
            base_dir: str = settings.tmp_dir,
            retention_days: int = settings.tmp_retention_days,
            cleanup_interval_seconds: int = settings.tmp_cleanup_interval_seconds,
            total_size_recalc_seconds: int = settings.tmp_total_size_recalc_seconds,
            worker_threads: int = settings.tmp_worker_threads,
            max_file_size_mb: int = settings.tmp_max_file_size_mb,
            max_total_size_mb: int = settings.tmp_max_total_size_mb,
            store_provider=require("store_service"),
        ) -> TempFileService:
            store = await TempFileService._resolve_store_provider(store_provider)
            service = await TempFileService.acquire_shared(
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
            await service.start_cleanup()
            return service

        @staticmethod
        async def dtor(instance: TempFileService) -> None:
            await TempFileService.release_shared(instance)

    # ------------------------------------------------------------------ #
    # Shared-instance lifecycle                                           #
    # ------------------------------------------------------------------ #

    _shared_instances: ClassVar[dict[str, _SharedTempFileEntry]] = {}
    _shared_instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @staticmethod
    def _shared_temp_key(config: TempFileConfig, store: StoreService) -> str:
        store_key = getattr(store, "_shared_instance_key", None) or f"store:{id(store)}"
        base_dir = str(Path(config.base_dir).expanduser().resolve())
        return f"{store_key}|{base_dir}|{config.namespace}"

    @staticmethod
    def _shared_temp_signature(config: TempFileConfig) -> tuple[object, ...]:
        return (
            config.retention_days,
            config.cleanup_interval_seconds,
            config.total_size_recalc_seconds,
            config.worker_threads,
            config.max_file_size_mb,
            config.max_total_size_mb,
        )

    @classmethod
    def _try_reuse_shared_locked(
        cls,
        key: str,
        signature: tuple[object, ...],
    ) -> tuple[_SharedTempFileEntry | None, threading.Event | None]:
        entry = cls._shared_instances.get(key)
        if entry is None:
            return None, None
        if entry.instance._closed:
            cls._shared_instances.pop(key, None)
            return None, None
        if entry.signature != signature:
            raise RuntimeError(
                "TempFileService config mismatch for shared key "
                f"'{key}'. Keep TMP_* settings identical across workers."
            )
        if entry.closing:
            return None, entry.close_done
        entry.ref_count += 1
        return entry, None

    @classmethod
    def _register_new_shared_locked(
        cls,
        key: str,
        signature: tuple[object, ...],
        config: TempFileConfig,
        store: StoreService,
    ) -> _SharedTempFileEntry:
        service = TempFileService(config, store)
        service._shared_registry_managed = True
        service._shared_instance_key = key
        entry = _SharedTempFileEntry(
            instance=service,
            signature=signature,
            ref_count=1,
        )
        cls._shared_instances[key] = entry
        return entry

    @classmethod
    def _rollback_waiter_ref_locked(
        cls,
        key: str,
        entry: _SharedTempFileEntry,
    ) -> None:
        current = cls._shared_instances.get(key)
        if current is not entry:
            return
        entry.ref_count -= 1
        if entry.ref_count <= 0:
            cls._shared_instances.pop(key, None)

    @classmethod
    def _release_shared_locked(
        cls,
        key: str,
        instance: TempFileService,
    ) -> tuple[bool, bool, _SharedTempFileEntry | None]:
        entry = cls._shared_instances.get(key)
        if entry is None or entry.instance is not instance:
            instance._shared_registry_managed = False
            return (not instance._closed, True, None)

        entry.ref_count -= 1
        if entry.ref_count > 0:
            return (False, False, None)

        entry.closing = True
        entry.close_done.clear()
        instance._shared_registry_managed = False
        return (True, False, entry)

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
    ) -> TempFileService:
        key = cls._shared_temp_key(config, store)
        signature = cls._shared_temp_signature(config)
        entry: _SharedTempFileEntry | None = None
        created = False
        while True:
            close_waiter: threading.Event | None = None
            with cls._shared_instances_lock:
                entry, close_waiter = cls._try_reuse_shared_locked(key, signature)
                if entry is not None:
                    created = False
                elif close_waiter is None:
                    entry = cls._register_new_shared_locked(
                        key,
                        signature,
                        config,
                        store,
                    )
                    created = True
                else:
                    created = False
            if close_waiter is not None:
                await asyncio.to_thread(close_waiter.wait)
                continue
            break

        if entry is None:
            raise RuntimeError("TempFileService shared entry resolution failed")

        if not created:
            try:
                await cls._wait_shared_entry_ready(entry)
            except BaseException:
                with cls._shared_instances_lock:
                    cls._rollback_waiter_ref_locked(key, entry)
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
    async def release_shared(cls, instance: TempFileService) -> None:
        if not getattr(instance, "_shared_registry_managed", False):
            await instance.shutdown()
            return

        key = getattr(instance, "_shared_instance_key", None)
        if key is None:
            instance._shared_registry_managed = False
            await instance.shutdown()
            return

        closing_entry: _SharedTempFileEntry | None = None
        with cls._shared_instances_lock:
            should_shutdown, inconsistent_registry_state, closing_entry = cls._release_shared_locked(
                key,
                instance,
            )

        if inconsistent_registry_state:
            logger.warning(
                "[TMP] Shared registry state mismatch during release; forcing shutdown",
                key=key,
            )
        if not should_shutdown:
            return

        try:
            await instance.shutdown()
        finally:
            if closing_entry is not None:
                with cls._shared_instances_lock:
                    current = cls._shared_instances.get(key)
                    if current is closing_entry:
                        cls._shared_instances.pop(key, None)
                    closing_entry.closing = False
                    closing_entry.close_done.set()

    def __init__(self, config: TempFileConfig, store: StoreService) -> None:
        self._validate_config(config)
        self._config = config
        self._store = store
        self._base_dir = Path(config.base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._tighten_directory_permissions(self._base_dir)

        self._retention_seconds = config.retention_days * 24 * 60 * 60
        self._retention_minutes = config.retention_days * 24 * 60
        self._max_file_size_bytes: int | None = None
        if config.max_file_size_mb > 0:
            self._max_file_size_bytes = config.max_file_size_mb * 1024 * 1024
        self._max_total_size_bytes: int | None = None
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
        self._cleanup_task: asyncio.Task | None = None
        self._size_recalc_task: asyncio.Task | None = None
        self._cleanup_lock_handle: FileLock | None = None
        self._init_runtime_dispatch_state()
        self._closed = False
        self._executor_shutdown = False
        self._shared_registry_managed = False
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
    async def _resolve_store_provider(
        store_provider: StoreProvider | None,
    ) -> StoreService:
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

    # ------------------------------------------------------------------ #
    # Public service API                                                  #
    # ------------------------------------------------------------------ #

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
                    timeout=5,
                    raise_on_incomplete=False,
                    callback_name=self._callback_name,
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
            saved_name: str | None = None
            try:
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
                await self._write_metadata(
                    saved_name,
                    is_text,
                    self._retention_minutes,
                    revision=revision,
                )
                return saved_name
            except Exception:
                if saved_name is not None:
                    try:
                        await self._run_in_executor(self._delete_file, saved_name)
                    except Exception:
                        logger.exception(
                            "[TMP] Failed deleting temp file during save rollback",
                            name=saved_name,
                        )
                    await self._clear_metadata_best_effort(saved_name)
                await self._refresh_total_size_counter_unlocked()
                logger.exception("[TMP] Save failed; rolled back temp-file state")
                raise

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
        async with self._write_guard:
            previous_metadata = await self._read_metadata(base_name)
            backup_name: str | None = None
            staging_name: str | None = None
            swap_completed = False
            try:
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
                swap_completed = True
                await self._write_metadata(
                    base_name,
                    is_text,
                    self._retention_minutes,
                    revision=revision,
                )
                # Discard backup
                deleted_size = await self._run_in_executor(
                    self._discard_backup_file, backup_name
                )
                self._total_size_bytes -= deleted_size
                return base_name
            except Exception:
                if swap_completed:
                    try:
                        await self._run_in_executor(
                            self._rollback_overwrite_after_metadata_failure,
                            base_name,
                            backup_name,
                        )
                    except Exception:
                        logger.exception(
                            "[TMP] Failed rollback after overwrite failure",
                            name=base_name,
                        )
                elif staging_name is not None:
                    try:
                        await self._run_in_executor(self._delete_file, staging_name)
                    except Exception:
                        logger.exception(
                            "[TMP] Failed deleting staging file during overwrite rollback",
                            name=staging_name,
                        )

                try:
                    await self._restore_metadata_after_overwrite_failure(
                        base_name,
                        previous_metadata,
                    )
                except Exception:
                    logger.exception(
                        "[TMP] Failed restoring metadata after overwrite failure",
                        name=base_name,
                    )
                await self._refresh_total_size_counter_unlocked()
                logger.exception(
                    "[TMP] save_overwrite failed; rolled back temp-file state",
                    name=base_name,
                )
                raise

    async def read(self, filename: str) -> str | bytes | None:
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

        metadata = await self._resolve_or_infer_metadata(safe_name, path, data, now)
        if metadata is None:
            raise FileNotFoundError(safe_name)
        is_text, revision = metadata

        await self._run_in_executor(self._touch_file, path, now)
        await self._write_metadata(
            safe_name,
            is_text,
            self._retention_minutes,
            revision=revision,
        )

        if is_text:
            return data.decode("utf-8")
        return data

    async def cleanup_expired(self, now: float | None = None) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.cleanup_expired, now)
        self._assert_open()
        if now is None:
            now = time.time()

        await self._store.cleanup_expired(now)
        await self._reconcile_files(now, adopt_fresh=False)

    # Metadata/reconcile/file operations are provided by mixins:
    # TempFileMetadataMixin, TempFileReconcilerMixin, TempFileFileOpsMixin.

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
        fd = os.open(path, flags, 0o600)
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

    @staticmethod
    def _try_acquire_file_lock(path: Path) -> FileLock | None:
        return _runtime.try_acquire_file_lock(path)

    @staticmethod
    def _release_file_lock(handle: FileLock | None) -> None:
        _runtime.release_file_lock(handle)

    async def _cleanup_loop(
        self,
        name: str,
        cleanup_fn: Callable[[float], Awaitable[None]],
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
            await self._refresh_total_size_counter_unlocked()

    async def _refresh_total_size_counter_unlocked(self) -> None:
        entries = await self._run_in_executor(self._scan_files)
        # _scan_files ignores symlinks and non-regular files by design.
        self._total_size_bytes = sum(size for _, _, size in entries)
