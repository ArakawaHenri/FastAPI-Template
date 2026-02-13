from __future__ import annotations

import asyncio
import contextvars
import inspect
import threading
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import zlmdb.lmdb as lmdb
from filelock import FileLock
from loguru import logger

from app.core.settings import settings
from app.services import BaseService, Service

from . import _runtime
from ._callback_engine import CallbackLogThrottler
from ._callback_pipeline import StoreCallbackPipelineMixin
from ._exclusive_locking import StoreExclusiveLockingMixin
from ._runtime_dispatch import StoreRuntimeDispatchMixin
from ._sizing import calc_new_map_size
from ._storage_path import StoreStorageDataPathMixin
from ._types import (
    ExpiryCallback,
    ExpiryCallbackDeferred,
    ExpiryCallbackEvent,
    _CallbackRegistration,
)

__all__ = [
    "StoreConfig",
    "StoreService",
    "ExpiryCallbackEvent",
    "ExpiryCallbackDeferred",
]

_INT64_MAX = 2 ** 63 - 1
_META_DB_NAME = b"__meta__"
_CALLBACK_DB_NAME = b"__callbacks__"
_CALLBACK_JOB_DB_NAME = b"__cb_jobs__"
_CALLBACK_SCHEDULE_DB_NAME = b"__cb_sched__"
_CALLBACK_DEAD_LETTER_DB_NAME = b"__cb_dlq__"
_CALLBACK_COUNT_DB_NAME = b"__cb_counts__"
_CALLBACK_WAIT_POLL_SECONDS = 0.05
_CALLBACK_LOG_THROTTLE_SECONDS = 30.0
_DEFAULT_CALLBACK_EXEC_TIMEOUT_SECONDS = 30.0
_CALLBACK_SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 5.0
_INTERNAL_OPEN_MAX_DBS = 65535

_MapFullError = getattr(lmdb, "MapFullError", None)
_BadValsizeError = getattr(lmdb, "BadValsizeError", None)
_DbsFullError = getattr(lmdb, "DbsFullError", None)


def _new_signaled_event() -> threading.Event:
    event = threading.Event()
    event.set()
    return event


@dataclass(frozen=True)
class StoreConfig:
    path: str
    map_size_mb: int
    map_size_growth_factor: int
    map_high_watermark: float
    max_dbs: int
    max_readers: int
    sync: bool
    metasync: bool
    writemap: bool
    map_async: bool
    max_key_bytes: int
    max_namespace_bytes: int
    max_value_bytes: int
    cleanup_max_deletes: int
    worker_threads: int


@dataclass
class _SharedStoreEntry:
    instance: StoreService
    signature: tuple[object, ...]
    ref_count: int = 1
    closing: bool = False
    close_done: threading.Event = field(default_factory=_new_signaled_event)


@Service("store_service", eager=True)
class StoreService(
    StoreStorageDataPathMixin,
    StoreCallbackPipelineMixin,
    StoreExclusiveLockingMixin,
    StoreRuntimeDispatchMixin,
    BaseService,
):
    """
    zLMDB-backed key-value store.

    - Single persistent LMDB environment; OS page cache serves as memory cache.
    - Namespace is mapped to a named LMDB database.
    - Expiration is managed via a secondary index (bucketed by minute).
    """

    @classmethod
    async def create(
        cls,
        path: str = settings.store_lmdb.path,
        map_size_mb: int = settings.store_lmdb.map_size_mb,
        map_size_growth_factor: int = settings.store_lmdb.map_size_growth_factor,
        map_high_watermark: float = settings.store_lmdb.map_high_watermark,
        max_dbs: int = settings.store_lmdb.max_dbs,
        max_readers: int = settings.store_lmdb.max_readers,
        sync: bool = settings.store_lmdb.sync,
        metasync: bool = settings.store_lmdb.metasync,
        writemap: bool = settings.store_lmdb.writemap,
        map_async: bool = settings.store_lmdb.map_async,
        max_key_bytes: int = settings.store_lmdb.max_key_bytes,
        max_namespace_bytes: int = settings.store_lmdb.max_namespace_bytes,
        max_value_bytes: int = settings.store_lmdb.max_value_bytes,
        cleanup_max_deletes: int = settings.store_lmdb.cleanup_max_deletes,
        worker_threads: int = settings.store_lmdb.callback_worker_threads,
    ) -> StoreService:
        _ = cls
        store = await StoreService.acquire_shared(
            StoreConfig(
                path=path,
                map_size_mb=map_size_mb,
                map_size_growth_factor=map_size_growth_factor,
                map_high_watermark=map_high_watermark,
                max_dbs=max_dbs,
                max_readers=max_readers,
                sync=sync,
                metasync=metasync,
                writemap=writemap,
                map_async=map_async,
                max_key_bytes=max_key_bytes,
                max_namespace_bytes=max_namespace_bytes,
                max_value_bytes=max_value_bytes,
                cleanup_max_deletes=cleanup_max_deletes,
                worker_threads=worker_threads,
            )
        )
        await store.start_cleanup()
        return store

    @classmethod
    async def destroy(cls, instance: StoreService) -> None:
        _ = cls
        await StoreService.release_shared(instance)

    # ------------------------------------------------------------------ #
    # Shared-instance lifecycle                                           #
    # ------------------------------------------------------------------ #

    _shared_instances: ClassVar[dict[str, _SharedStoreEntry]] = {}
    _shared_instances_lock: ClassVar[threading.Lock] = threading.Lock()

    @staticmethod
    def _shared_store_key(path: str) -> str:
        base_path = Path(path).expanduser()
        data_path = base_path if base_path.suffix else (base_path / "data.mdb")
        return str(data_path.resolve())

    @staticmethod
    def _shared_store_signature(config: StoreConfig) -> tuple[object, ...]:
        return (
            config.map_size_mb,
            config.map_size_growth_factor,
            config.map_high_watermark,
            config.max_dbs,
            config.max_readers,
            config.sync,
            config.metasync,
            config.writemap,
            config.map_async,
            config.max_key_bytes,
            config.max_namespace_bytes,
            config.max_value_bytes,
            config.cleanup_max_deletes,
            config.worker_threads,
        )

    @classmethod
    def _try_reuse_shared_locked(
        cls,
        key: str,
        signature: tuple[object, ...],
    ) -> tuple[StoreService | None, threading.Event | None]:
        entry = cls._shared_instances.get(key)
        if entry is None:
            return None, None
        if entry.instance._closed:
            cls._shared_instances.pop(key, None)
            return None, None
        if entry.signature != signature:
            raise RuntimeError(
                "StoreService config mismatch for shared path "
                f"'{key}'. Keep STORE_LMDB settings identical across workers."
            )
        if entry.closing:
            return None, entry.close_done
        entry.ref_count += 1
        return entry.instance, None

    @classmethod
    def _register_new_shared_locked(
        cls,
        key: str,
        signature: tuple[object, ...],
        config: StoreConfig,
    ) -> StoreService:
        instance = cls(config)
        instance._shared_registry_managed = True
        instance._shared_instance_key = key
        cls._shared_instances[key] = _SharedStoreEntry(
            instance=instance,
            signature=signature,
            ref_count=1,
        )
        return instance

    @classmethod
    def _release_shared_locked(
        cls,
        key: str,
        instance: StoreService,
    ) -> tuple[bool, bool, _SharedStoreEntry | None]:
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

    @classmethod
    async def acquire_shared(cls, config: StoreConfig) -> StoreService:
        key = cls._shared_store_key(config.path)
        signature = cls._shared_store_signature(config)
        while True:
            waiter: threading.Event | None = None
            with cls._shared_instances_lock:
                shared, waiter = cls._try_reuse_shared_locked(key, signature)
                if shared is not None:
                    return shared
                if waiter is None:
                    return cls._register_new_shared_locked(key, signature, config)
            await asyncio.to_thread(waiter.wait)

    @classmethod
    async def release_shared(cls, instance: StoreService) -> None:
        if not getattr(instance, "_shared_registry_managed", False):
            await instance.shutdown()
            return

        key = getattr(instance, "_shared_instance_key", None)
        if key is None:
            instance._shared_registry_managed = False
            await instance.shutdown()
            return

        closing_entry: _SharedStoreEntry | None = None
        with cls._shared_instances_lock:
            should_shutdown, inconsistent_registry_state, closing_entry = cls._release_shared_locked(
                key,
                instance,
            )

        if inconsistent_registry_state:
            logger.warning(
                "[STORE] Shared registry state mismatch during release; forcing shutdown",
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

    def __init__(self, config: StoreConfig) -> None:
        self._validate_config(config)
        self._config = config
        base_path = Path(config.path)
        if base_path.suffix:
            self._base_dir = base_path.parent
            self._data_path = base_path
        else:
            self._base_dir = base_path
            self._data_path = self._base_dir / "data.mdb"
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._tighten_directory_permissions(self._base_dir)
        self._meta_path = self._base_dir / "meta.mdb"

        self._env = lmdb.open(
            str(self._data_path),
            map_size=config.map_size_mb * 1024 * 1024,
            max_dbs=_INTERNAL_OPEN_MAX_DBS,
            max_readers=config.max_readers,
            sync=config.sync,
            metasync=config.metasync,
            writemap=config.writemap,
            map_async=config.map_async,
            lock=True,
            readahead=False,
            meminit=False,
            subdir=False,
        )
        self._meta_env = lmdb.open(
            str(self._meta_path),
            map_size=config.map_size_mb * 1024 * 1024,
            max_dbs=_INTERNAL_OPEN_MAX_DBS,
            max_readers=config.max_readers,
            sync=config.sync,
            metasync=config.metasync,
            writemap=config.writemap,
            map_async=config.map_async,
            lock=True,
            readahead=False,
            meminit=False,
            subdir=False,
        )
        self._tighten_file_permissions(self._data_path)
        self._tighten_file_permissions(self._meta_path)
        self._tighten_file_permissions(
            self._data_path.with_name(f"{self._data_path.name}-lock"))
        self._tighten_file_permissions(
            self._meta_path.with_name(f"{self._meta_path.name}-lock"))

        self._db_cache: dict[str, lmdb._Database] = {}
        self._exp_cache: dict[str, lmdb._Database] = {}
        self._expmeta_cache: dict[str, lmdb._Database] = {}
        self._callback_entry_cache: dict[str, lmdb._Database] = {}
        self._callback_db: lmdb._Database = self._open_meta_db(
            _CALLBACK_DB_NAME, create=True)
        self._callback_job_db: lmdb._Database = self._open_meta_db(
            _CALLBACK_JOB_DB_NAME, create=True)
        self._callback_schedule_db: lmdb._Database = self._open_meta_db(
            _CALLBACK_SCHEDULE_DB_NAME, create=True, dupsort=True)
        self._callback_dead_letter_db: lmdb._Database = self._open_meta_db(
            _CALLBACK_DEAD_LETTER_DB_NAME, create=True)
        self._callback_count_db: lmdb._Database = self._open_meta_db(
            _CALLBACK_COUNT_DB_NAME, create=True)
        self._meta_db: lmdb._Database = self._open_meta_db(
            _META_DB_NAME, create=True)
        self._db_cache_lock = threading.Lock()
        self._namespace_lock = asyncio.Lock()
        self._resize_lock = threading.Lock()
        self._meta_resize_lock = threading.Lock()
        self._write_lock = asyncio.Lock()
        self._exclusive_locks: dict[str, threading.Lock] = {}
        self._exclusive_registry_lock = threading.Lock()
        # Task-local namespace lease state:
        # namespace -> (lease_id, reentrant_depth)
        self._exclusive_guard = contextvars.ContextVar(
            "store_exclusive_guard", default=None)
        # Namespace -> currently active lease_id.
        # A lease is valid only while the underlying asyncio lock is held.
        self._exclusive_active_leases: dict[str, str] = {}
        self._namespaces: set[str] = set()
        self._internal_namespaces: set[str] = set()
        self._callback_registry: dict[str, _CallbackRegistration] = {}
        self._callback_registry_lock = threading.Lock()
        self._builtin_callbacks: set[str] = set()
        self._callback_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="store-callback",
        )
        self._callback_runner_executor = ThreadPoolExecutor(
            max_workers=config.worker_threads,
            thread_name_prefix="store-callback-runner",
        )
        self._callback_future: Future[None] | None = None
        self._callback_guard = contextvars.ContextVar(
            "store_callback_guard", default=False)
        self._callback_dispatch_stop = threading.Event()
        self._callback_wakeup = threading.Event()
        self._callback_active = threading.Event()
        self._callback_lock_handle: FileLock | None = None
        self._callback_log_throttler = CallbackLogThrottler(
            window_seconds=_CALLBACK_LOG_THROTTLE_SECONDS
        )
        self._callback_count_index_needs_rebuild = self._is_callback_count_index_stale()

        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: asyncio.Task | None = None
        self._cleanup_lock_handle: FileLock | None = None
        self._init_runtime_dispatch_state()
        self._closed = False
        self._shared_registry_managed = False
        self._shared_instance_key: str | None = self._shared_store_key(config.path)
        namespaces, internal_namespaces = self._list_namespace_metadata()
        self._namespaces.update(namespaces)
        self._internal_namespaces.update(internal_namespaces)
        self._start_runtime_thread()

        logger.debug("[STORE] StoreService initialized (zLMDB)")

    # ------------------------------------------------------------------ #
    # Public service API                                                  #
    # ------------------------------------------------------------------ #

    async def mark_internal_namespace(self, namespace: str) -> None:
        """
        Mark a namespace as internal, excluding it from user quota counting.

        If the namespace is already registered, it will be migrated to internal
        and no longer count against user quota for future capacity checks.

        Args:
            namespace: The namespace to mark as internal
        """
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.mark_internal_namespace,
                namespace,
            )
        self._assert_open()
        self._assert_not_in_callback()
        self._encode_namespace(namespace)
        async with self._namespace_lock:
            already_registered = namespace in self._namespaces
            already_internal = namespace in self._internal_namespaces
            self._internal_namespaces.add(namespace)
            if already_registered and not already_internal:
                async with self._write_lock:
                    await self._run_in_executor(
                        self._mark_namespace_internal_in_meta,
                        namespace,
                    )
            if already_registered:
                logger.info(
                    f"[STORE] Migrated existing namespace '{namespace}' to internal. "
                    f"It will no longer count against user quota."
                )
            else:
                logger.debug(
                    f"[STORE] Pre-marked namespace '{namespace}' as internal "
                    f"(not yet registered)"
                )

    async def start_cleanup(self) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.start_cleanup)
        self._assert_open()
        if self._cleanup_task is not None:
            return
        self._cleanup_lock_handle = await self._run_in_executor(
            self._try_acquire_file_lock,
            self._base_dir / ".store_cleanup.lock",
        )
        if self._cleanup_lock_handle is None:
            logger.info("[STORE] Cleanup loop skipped (lock not acquired)")
            return
        self._cleanup_task = asyncio.create_task(
            self._cleanup_loop(
                "store", self.cleanup_expired, self._cleanup_stop)
        )
        callback_worker_ready = await self._ensure_callback_worker()
        if not callback_worker_ready:
            pending_jobs = await self._run_in_executor(self._count_callback_jobs)
            if pending_jobs > 0:
                logger.warning(
                    "[STORE] Callback worker unavailable at startup; pending jobs will wait",
                    pending_jobs=pending_jobs,
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
        if self._cleanup_task is not None:
            await self._cleanup_task
        try:
            drained = await self.wait_for_callbacks(
                timeout=_CALLBACK_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                raise_on_incomplete=False,
            )
        except Exception:
            drained = False
            logger.exception("[STORE] Failed waiting for callback queue drain")
        if not drained:
            try:
                pending_jobs = await self._run_in_executor(self._count_callback_jobs)
            except Exception:
                pending_jobs = -1
            logger.warning(
                "[STORE] Callback queue not fully drained before shutdown",
                pending_jobs=pending_jobs,
                timeout_seconds=_CALLBACK_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
            )
        stopped = await self._stop_callback_worker()
        if not stopped:
            raise RuntimeError(
                "Callback worker failed to stop cleanly; "
                "aborting store shutdown to avoid unsafe teardown."
            )
        await self._run_in_executor(
            self._release_file_lock, self._cleanup_lock_handle
        )
        self._cleanup_lock_handle = None
        try:
            self._meta_env.sync()
        finally:
            self._meta_env.close()
        try:
            self._env.sync()
        finally:
            self._env.close()
        self._closed = True
        self._callback_executor.shutdown(wait=True, cancel_futures=True)
        self._callback_runner_executor.shutdown(wait=False, cancel_futures=True)
        logger.debug("[STORE] StoreService shutdown completed")

    async def register_expiry_callback(
        self,
        name: str,
        fn: ExpiryCallback,
        *,
        timeout_seconds: float | None = None,
    ) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.register_expiry_callback,
                name,
                fn,
                timeout_seconds=timeout_seconds,
            )
        self._assert_open()
        self._assert_not_in_callback()
        callback_name = self._normalize_callback_name(name)
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("Expiry callback must be an async function")
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")
        effective_timeout = (
            timeout_seconds
            if timeout_seconds is not None
            else _DEFAULT_CALLBACK_EXEC_TIMEOUT_SECONDS
        )
        if callback_name not in self._builtin_callbacks:
            raise ValueError(
                f"Expiry callback '{callback_name}' is not a builtin callback"
            )
        with self._callback_registry_lock:
            self._callback_registry[callback_name] = _CallbackRegistration(
                fn=fn,
                timeout_seconds=effective_timeout,
            )
        async with self._write_lock:
            await self._run_in_executor(
                self._register_callback_name,
                callback_name,
            )

    async def register_builtin_callback(self, name: str) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.register_builtin_callback, name
            )
        self._assert_open()
        self._assert_not_in_callback()
        callback_name = self._normalize_callback_name(name)
        self._builtin_callbacks.add(callback_name)

    async def unregister_expiry_callback(self, name: str) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.unregister_expiry_callback, name
            )
        self._assert_open()
        self._assert_not_in_callback()
        callback_name = self._normalize_callback_name(name)
        with self._callback_registry_lock:
            self._callback_registry.pop(callback_name, None)
        async with self._write_lock:
            await self._run_in_executor(
                self._unregister_callback_name,
                callback_name,
            )

    async def wait_for_callbacks(
        self,
        timeout: float | None = None,
        *,
        raise_on_incomplete: bool = True,
        callback_name: str | None = None,
    ) -> bool:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.wait_for_callbacks,
                timeout=timeout,
                raise_on_incomplete=raise_on_incomplete,
                callback_name=callback_name,
            )
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be >= 0")
        if callback_name is not None:
            callback_name = self._normalize_callback_name(callback_name)

        def _count_pending_jobs() -> int:
            if callback_name is None:
                return self._count_callback_jobs()
            return self._count_callback_jobs_for(callback_name)

        def _incomplete(reason: str, *, pending: int, elapsed: float) -> bool:
            logger.warning(
                "[STORE] Callback queue not fully drained",
                reason=reason,
                pending_jobs=pending,
                elapsed_seconds=round(elapsed, 3),
                timeout_seconds=timeout,
                callback_name=callback_name,
            )
            if raise_on_incomplete:
                raise TimeoutError(
                    f"Callback queue not drained: {reason} "
                    f"(pending_jobs={pending}, elapsed={elapsed:.3f}s, "
                    f"callback_name={callback_name!r})"
                )
            return False

        started = time.monotonic()
        worker_available = await self._ensure_callback_worker()
        while True:
            pending = await self._run_in_executor(_count_pending_jobs)
            callbacks_idle = pending == 0
            if callback_name is None:
                callbacks_idle = callbacks_idle and not self._callback_active.is_set()
            if callbacks_idle:
                return True

            if not worker_available:
                # Jobs exist but this process cannot run a callback worker now.
                # Caller can decide whether to retry later.
                elapsed = time.monotonic() - started
                return _incomplete("worker_unavailable", pending=pending, elapsed=elapsed)

            sleep_seconds = _CALLBACK_WAIT_POLL_SECONDS
            if timeout is not None:
                elapsed = time.monotonic() - started
                remaining = timeout - elapsed
                if remaining <= 0:
                    return _incomplete("timeout", pending=pending, elapsed=elapsed)
                sleep_seconds = min(sleep_seconds, remaining)

            await asyncio.sleep(sleep_seconds)
            worker_available = await self._ensure_callback_worker()

    async def set(
        self,
        namespace: str = "default",
        key: str = "",
        value: object = "",
        retention: int | None = None,
        on_expire: str | None = None,
    ) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.set,
                namespace,
                key,
                value,
                retention,
                on_expire,
            )
        self._assert_open()
        self._assert_not_in_callback()
        self._assert_namespace_access(namespace)
        if not key:
            raise ValueError("Store key must be non-empty")
        if retention is not None and retention < 1:
            raise ValueError("Retention must be a positive integer (minutes)")

        callback_name = None
        if on_expire is not None:
            if retention is None:
                raise ValueError(
                    "on_expire requires a positive retention (minutes)"
                )
            callback_name = self._normalize_callback_name(on_expire)
            if callback_name not in self._callback_registry:
                raise ValueError(
                    f"Expiry callback '{callback_name}' is not registered"
                )

        namespace_escaped = self._encode_namespace(namespace)
        key_bytes = self._encode_key(key)

        expire_ts = 0
        if retention is not None:
            expire_ts = int(time.time() + retention * 60)
            if expire_ts > _INT64_MAX:
                raise ValueError(
                    "Retention is too large; expiry timestamp exceeds int64")

        try:
            await self._register_namespace(namespace, namespace_escaped)

            async with self._write_lock:
                await self._run_in_executor(
                    self._write_value_and_metadata,
                    namespace,
                    namespace_escaped,
                    key_bytes,
                    value,
                    expire_ts,
                    callback_name,
                )
        except Exception:
            logger.exception("[STORE] Failed to set key",
                             namespace=namespace, key=key)
            raise

    async def get(self, namespace: str = "default", key: str = "") -> object | None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.get, namespace, key)
        self._assert_open()
        self._assert_namespace_access(namespace)
        if not key:
            raise ValueError("Store key must be non-empty")

        try:
            namespace_escaped = self._encode_namespace(namespace)
            key_bytes = self._encode_key(key)
            exists = await self._namespace_exists(namespace)
            if not exists:
                return None
            value, event, delete_ts = await self._run_in_executor(
                self._read_value_with_expiry_state, namespace, namespace_escaped, key_bytes
            )
            if value is None and event is None and delete_ts is None:
                return None
            enqueued_jobs = 0
            if delete_ts is not None and not self._callback_guard.get():
                async with self._write_lock:
                    enqueued_jobs = await self._run_in_executor(
                        self._delete_entry_and_schedule_callback,
                        namespace,
                        namespace_escaped,
                        key_bytes,
                        delete_ts,
                        event,
                    )
            if enqueued_jobs > 0:
                worker_available = await self._ensure_callback_worker()
                if worker_available:
                    self._callback_wakeup.set()
            return value
        except Exception:
            logger.exception("[STORE] Failed to get key",
                             namespace=namespace, key=key)
            raise

    async def get_many(
        self,
        namespace: str = "default",
        keys: list[str] | tuple[str, ...] = (),
    ) -> dict[str, object | None]:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.get_many, namespace, keys)
        self._assert_open()
        self._assert_namespace_access(namespace)
        if not keys:
            return {}
        namespace_escaped = self._encode_namespace(namespace)
        key_items: list[tuple[str, bytes]] = []
        for key in keys:
            if not key:
                raise ValueError("Store key must be non-empty")
            key_items.append((key, self._encode_key(key)))

        exists = await self._namespace_exists(namespace)
        if not exists:
            return {key: None for key, _ in key_items}

        values: dict[str, object | None] = {}
        deletes: list[tuple[bytes, int, ExpiryCallbackEvent | None]] = []
        for key, key_bytes in key_items:
            value, event, delete_ts = await self._run_in_executor(
                self._read_value_with_expiry_state,
                namespace,
                namespace_escaped,
                key_bytes,
            )
            if value is None and event is None and delete_ts is None:
                values[key] = None
                continue
            values[key] = value
            if delete_ts is not None and not self._callback_guard.get():
                deletes.append((key_bytes, delete_ts, event))

        enqueued_jobs = 0
        if deletes:
            async with self._write_lock:
                for key_bytes, delete_ts, event in deletes:
                    enqueued_jobs += await self._run_in_executor(
                        self._delete_entry_and_schedule_callback,
                        namespace,
                        namespace_escaped,
                        key_bytes,
                        delete_ts,
                        event,
                    )
        if enqueued_jobs > 0:
            worker_available = await self._ensure_callback_worker()
            if worker_available:
                self._callback_wakeup.set()
        return values

    async def cleanup_expired(self, now: float | None = None) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(self.cleanup_expired, now)
        self._assert_open()
        self._assert_not_in_callback()
        if now is None:
            now = time.time()

        async with self._namespace_lock:
            local_namespaces = list(self._namespaces)
        namespaces = await self._run_in_executor(self._list_namespaces)
        if local_namespaces:
            for ns in local_namespaces:
                if ns not in namespaces:
                    namespaces.append(ns)

        if not namespaces:
            return

        try:
            async with self._write_lock:
                enqueued_jobs = await self._run_in_executor(
                    self._cleanup_expired_entries_and_schedule_callbacks, namespaces, now
                )
            if enqueued_jobs > 0:
                worker_available = await self._ensure_callback_worker()
                if worker_available:
                    self._callback_wakeup.set()
        except Exception:
            logger.exception("[STORE] Cleanup expired records failed")
            raise

    def _ensure_capacity(
        self,
        env,
        resize_lock: threading.Lock,
        estimated_write_bytes: int,
        label: str,
    ) -> None:
        if estimated_write_bytes <= 0:
            return

        info = env.info()
        stat = env.stat()
        map_size = info["map_size"]
        used_bytes = (info["last_pgno"] + 1) * stat["psize"]
        projected = used_bytes + estimated_write_bytes
        if projected < map_size * self._config.map_high_watermark:
            return

        with resize_lock:
            info = env.info()
            stat = env.stat()
            map_size = info["map_size"]
            used_bytes = (info["last_pgno"] + 1) * stat["psize"]
            projected = used_bytes + estimated_write_bytes
            if projected < map_size * self._config.map_high_watermark:
                return

            new_size = self._calc_new_map_size(map_size, used_bytes, projected)
            logger.info(
                "[STORE] LMDB map size increased "
                f"({label}), old_size: {map_size}, "
                f"new_size: {new_size}, used_bytes: {used_bytes}."
            )
            env.set_mapsize(new_size)

    def _write_txn_with_resize(
        self,
        env,
        resize_lock: threading.Lock,
        fn,
        estimated_write_bytes: int,
        label: str,
    ) -> None:
        self._assert_not_in_callback()
        attempt = 0
        while True:
            try:
                if estimated_write_bytes:
                    self._ensure_capacity(
                        env, resize_lock, estimated_write_bytes, label
                    )
                with env.begin(write=True) as txn:
                    return fn(txn)
            except Exception as exc:
                if _MapFullError is not None and isinstance(exc, _MapFullError):
                    attempt += 1
                    if attempt > 3:
                        logger.exception("[STORE] LMDB map full after retries")
                        raise
                    self._force_resize(
                        env, resize_lock, estimated_write_bytes, label
                    )
                    continue
                if _BadValsizeError is not None and isinstance(exc, _BadValsizeError):
                    raise ValueError(
                        "Value or key exceeds LMDB limits") from exc
                raise

    def _write_data_txn_with_resize(self, fn, estimated_write_bytes: int) -> None:
        self._write_txn_with_resize(
            self._env,
            self._resize_lock,
            fn,
            estimated_write_bytes,
            "data",
        )

    def _write_meta_txn_with_resize(self, fn, estimated_write_bytes: int) -> None:
        self._write_txn_with_resize(
            self._meta_env,
            self._meta_resize_lock,
            fn,
            estimated_write_bytes,
            "meta",
        )

    def _force_resize(
        self,
        env,
        resize_lock: threading.Lock,
        estimated_write_bytes: int,
        label: str,
    ) -> None:
        if estimated_write_bytes <= 0:
            estimated_write_bytes = 1
        with resize_lock:
            info = env.info()
            stat = env.stat()
            map_size = info["map_size"]
            used_bytes = (info["last_pgno"] + 1) * stat["psize"]
            projected = used_bytes + estimated_write_bytes
            new_size = self._calc_new_map_size(
                map_size, used_bytes, projected, force=True)
            logger.warning(
                "[STORE] LMDB map size forced to grow "
                f"({label}), old_size: {map_size}, "
                f"new_size: {new_size}, used_bytes: {used_bytes}."
            )
            env.set_mapsize(new_size)

    def _calc_new_map_size(
        self,
        map_size: int,
        used_bytes: int,
        projected: int,
        force: bool = False,
    ) -> int:
        return calc_new_map_size(
            map_size,
            used_bytes,
            projected,
            self._config.map_size_growth_factor,
            force=force,
        )


    def _open_env_db(self, env, name: bytes, db_type: str, **kwargs) -> lmdb._Database:
        try:
            return env.open_db(name, **kwargs)
        except Exception as exc:
            if _DbsFullError is not None and isinstance(exc, _DbsFullError):
                if db_type == "meta":
                    raise RuntimeError(
                        "LMDB meta max_dbs internal high-water reached; increase _INTERNAL_OPEN_MAX_DBS."
                    ) from exc
                raise RuntimeError(
                    "LMDB data max_dbs internal high-water reached; increase _INTERNAL_OPEN_MAX_DBS."
                ) from exc
            raise

    def _open_data_db(self, name: bytes, **kwargs) -> lmdb._Database:
        return self._open_env_db(self._env, name, "data", **kwargs)

    def _open_meta_db(self, name: bytes, **kwargs) -> lmdb._Database:
        return self._open_env_db(self._meta_env, name, "meta", **kwargs)

    @staticmethod
    def _validate_config(config: StoreConfig) -> None:
        if not config.path:
            raise ValueError("path must be non-empty")
        if config.map_size_mb <= 0:
            raise ValueError("map_size_mb must be > 0")
        if config.map_size_growth_factor < 1:
            raise ValueError("map_size_growth_factor must be >= 1")
        if not (0.0 < config.map_high_watermark < 1.0):
            raise ValueError("map_high_watermark must be in (0, 1.0)")
        if config.max_dbs < 0:
            raise ValueError("max_dbs must be >= 0")
        if config.max_readers < 1:
            raise ValueError("max_readers must be >= 1")
        if config.max_key_bytes < 1:
            raise ValueError("max_key_bytes must be >= 1")
        if config.max_namespace_bytes < 1:
            raise ValueError("max_namespace_bytes must be >= 1")
        if config.max_value_bytes < 1:
            raise ValueError("max_value_bytes must be >= 1")
        if config.cleanup_max_deletes < 1:
            raise ValueError("cleanup_max_deletes must be >= 1")
        if config.worker_threads < 1:
            raise ValueError("worker_threads must be >= 1")

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("StoreService is closed")

    def _assert_not_in_callback(self) -> None:
        if self._callback_guard.get():
            raise RuntimeError(
                "Expiry callbacks cannot write to the store (LMDB) to avoid deadlocks"
            )

    @staticmethod
    def _try_acquire_file_lock(path: Path) -> FileLock | None:
        return _runtime.try_acquire_file_lock(path)

    @staticmethod
    def _release_file_lock(handle: FileLock | None) -> None:
        _runtime.release_file_lock(handle)

    @staticmethod
    def _tighten_directory_permissions(path: Path) -> None:
        _runtime.tighten_directory_permissions(path)

    @staticmethod
    def _tighten_file_permissions(path: Path) -> None:
        _runtime.tighten_file_permissions(path)

    async def _cleanup_loop(
        self,
        name: str,
        cleanup_fn: Callable[[float], Awaitable[None]],
        stop_event: asyncio.Event,
        interval_seconds: int = 60,
    ) -> None:
        await _runtime.cleanup_loop(name, cleanup_fn, stop_event, interval_seconds)
