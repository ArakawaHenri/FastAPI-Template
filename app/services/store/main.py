from __future__ import annotations

import asyncio
import contextvars
import inspect
import itertools
import struct
import threading
import time
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
)
from concurrent.futures import (
    TimeoutError as FutureTimeoutError,
)
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional, TextIO
from urllib.parse import quote, unquote
from uuid import uuid4

import cbor2
import zlmdb.lmdb as lmdb
from loguru import logger

from app.services import BaseService

from . import _runtime

_EXPIRY_STRUCT = struct.Struct(">q")  # int64, epoch seconds; 0 means no expiry
_BUCKET_STRUCT = struct.Struct(">q")  # int64, epoch minutes
_INT64_MAX = 2 ** 63 - 1
_META_DB_NAME = b"__meta__"
_META_NS_PREFIX = b"ns:"
_CALLBACK_DB_NAME = b"__callbacks__"
_EXP_DB_PREFIX = b"__exp__:"
_EXPMETA_DB_PREFIX = b"__expmeta__:"
_CALLBACK_ENTRY_DB_PREFIX = b"__cbentry__:"
_CALLBACK_REGISTRY_PREFIX = b"cb:"
_CALLBACK_JOB_DB_NAME = b"__cb_jobs__"
_CALLBACK_SCHEDULE_DB_NAME = b"__cb_sched__"
_CALLBACK_DEAD_LETTER_DB_NAME = b"__cb_dlq__"
_CALLBACK_POLL_INTERVAL_SECONDS = 1.0
_CALLBACK_WAIT_POLL_SECONDS = 0.05
_CALLBACK_MAX_RETRIES = 1
_CALLBACK_RETRY_BASE_SECONDS = 1
_CALLBACK_RETRY_MAX_SECONDS = 30
_DEFAULT_CALLBACK_EXEC_TIMEOUT_SECONDS = 30.0
_CALLBACK_SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 5.0
_RUNTIME_THREAD_START_TIMEOUT_SECONDS = 5.0
_RUNTIME_THREAD_STOP_TIMEOUT_SECONDS = 5.0
_RUNTIME_SUBMIT_TIMEOUT_SECONDS = 30.0
_RUNTIME_MAX_PENDING_CALLS = 1024
_RUNTIME_BLOCKING_WAIT_POLL_SECONDS = 0.1
_INTERNAL_OPEN_MAX_DBS = 65535

_MapFullError = getattr(lmdb, "MapFullError", None)
_BadValsizeError = getattr(lmdb, "BadValsizeError", None)
_DbsFullError = getattr(lmdb, "DbsFullError", None)


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


@dataclass(frozen=True)
class ExpiryCallbackEvent:
    namespace: str
    key: str
    value: object | None
    expire_ts: int
    callback: str


ExpiryCallback = Callable[[ExpiryCallbackEvent], Awaitable[None]]


@dataclass(frozen=True)
class _CallbackJob:
    job_id: bytes
    due_ts: int
    event: ExpiryCallbackEvent
    attempts: int


@dataclass(frozen=True)
class _CallbackRegistration:
    fn: ExpiryCallback
    timeout_seconds: float


class StoreService(BaseService):
    """
    zLMDB-backed key-value store.

    - Single persistent LMDB environment; OS page cache serves as memory cache.
    - Namespace is mapped to a named LMDB database.
    - Expiration is managed via a secondary index (bucketed by minute).
    """

    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor(
            path: str,
            map_size_mb: int = 1024,
            map_size_growth_factor: int = 2,
            map_high_watermark: float = 0.8,
            max_dbs: int = 256,
            max_readers: int = 512,
            sync: bool = False,
            metasync: bool = True,
            writemap: bool = True,
            map_async: bool = True,
            max_key_bytes: int = 256,
            max_namespace_bytes: int = 256,
            max_value_bytes: int = 100 * 1024 * 1024,
            cleanup_max_deletes: int = 1_000_000,
            worker_threads: int = 4,
        ) -> "StoreService":
            return StoreService(
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

        @staticmethod
        async def dtor(instance: "StoreService") -> None:
            await instance.shutdown()

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
        self._callback_future: Future[Any] | None = None
        self._callback_guard = contextvars.ContextVar(
            "store_callback_guard", default=False)
        self._callback_dispatch_stop = threading.Event()
        self._callback_wakeup = threading.Event()
        self._callback_active = threading.Event()
        self._callback_lock_handle = None

        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_lock_handle = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._runtime_thread: threading.Thread | None = None
        self._runtime_thread_id: int | None = None
        self._runtime_ready = threading.Event()
        self._runtime_submit_semaphore = threading.BoundedSemaphore(
            _RUNTIME_MAX_PENDING_CALLS
        )
        self._closed = False
        self._namespaces.update(self._list_namespaces())
        self._start_runtime_thread()

        logger.debug("[STORE] StoreService initialized (zLMDB)")

    class _ExclusiveNamespaceLock:
        def __init__(self, store: "StoreService", namespace: str) -> None:
            self._store = store
            self._namespace = namespace
            self._lease_id: str | None = None
            self._task_group: asyncio.TaskGroup | None = None
            self._enter_depth = 0

        async def __aenter__(self) -> "StoreService._ExclusiveNamespaceLock":
            lease_id, owns_lease = await self._store._acquire_exclusive(
                self._namespace
            )
            task_group: asyncio.TaskGroup | None = None
            try:
                if owns_lease:
                    task_group = asyncio.TaskGroup()
                    await task_group.__aenter__()
            except Exception:
                self._store._release_exclusive(self._namespace)
                raise

            if self._enter_depth == 0:
                self._lease_id = lease_id
                if task_group is not None:
                    self._task_group = task_group
            elif self._lease_id != lease_id:
                # Defensive: this should not happen when release/acquire is balanced.
                self._store._release_exclusive(self._namespace)
                raise RuntimeError(
                    f"Exclusive lease switched unexpectedly for namespace '{self._namespace}'."
                )
            self._enter_depth += 1
            return self

        def create_task(
            self,
            coro: Awaitable[Any],
            *,
            name: str | None = None,
        ) -> asyncio.Task[Any]:
            if self._task_group is None:
                raise RuntimeError(
                    "This lock instance does not own the active lease task group. "
                    "Create child tasks from the lock object that acquired the lease."
                )
            return self._task_group.create_task(coro, name=name)

        def start_soon(
            self,
            fn: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any,
        ) -> asyncio.Task[Any]:
            return self.create_task(fn(*args, **kwargs))

        async def __aexit__(self, exc_type, exc, tb) -> None:
            if self._enter_depth <= 0:
                logger.warning(
                    "[STORE] Exclusive namespace lock exited without enter",
                    namespace=self._namespace,
                )
                return

            self._enter_depth -= 1

            task_group_error: BaseException | None = None
            if self._enter_depth == 0 and self._task_group is not None:
                try:
                    await self._task_group.__aexit__(exc_type, exc, tb)
                except BaseException as error:
                    task_group_error = error
                finally:
                    self._task_group = None

            self._store._release_exclusive(self._namespace)
            if self._enter_depth == 0:
                self._lease_id = None

            if task_group_error is not None:
                raise task_group_error

    def create_namespace_lock(self, namespace: str) -> "StoreService._ExclusiveNamespaceLock":
        self._encode_namespace(namespace)
        with self._exclusive_registry_lock:
            if namespace not in self._exclusive_locks:
                self._exclusive_locks[namespace] = threading.Lock()
        return StoreService._ExclusiveNamespaceLock(self, namespace)

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
            self._internal_namespaces.add(namespace)
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
    ) -> bool:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self.wait_for_callbacks,
                timeout=timeout,
                raise_on_incomplete=raise_on_incomplete,
            )
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be >= 0")

        def _incomplete(reason: str, *, pending: int, elapsed: float) -> bool:
            logger.warning(
                "[STORE] Callback queue not fully drained",
                reason=reason,
                pending_jobs=pending,
                elapsed_seconds=round(elapsed, 3),
                timeout_seconds=timeout,
            )
            if raise_on_incomplete:
                raise TimeoutError(
                    f"Callback queue not drained: {reason} "
                    f"(pending_jobs={pending}, elapsed={elapsed:.3f}s)"
                )
            return False

        started = time.monotonic()
        worker_available = await self._ensure_callback_worker()
        while True:
            pending = await self._run_in_executor(self._count_callback_jobs)
            if pending == 0 and not self._callback_active.is_set():
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
        retention: Optional[int] = None,
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
                    self._put,
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

    async def get(self, namespace: str = "default", key: str = "") -> Optional[object]:
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
                self._get, namespace, namespace_escaped, key_bytes
            )
            if value is None and event is None and delete_ts is None:
                return None
            if delete_ts is not None and not self._callback_guard.get():
                async with self._write_lock:
                    await self._run_in_executor(
                        self._delete_key,
                        namespace,
                        namespace_escaped,
                        key_bytes,
                        delete_ts,
                    )
            if event is not None:
                await self._enqueue_callbacks([event])
            return value
        except Exception:
            logger.exception("[STORE] Failed to get key",
                             namespace=namespace, key=key)
            raise

    async def _namespace_exists(self, namespace: str) -> bool:
        async with self._namespace_lock:
            if namespace in self._namespaces:
                return True

        namespaces = await self._run_in_executor(self._list_namespaces)
        if namespace not in namespaces:
            return False

        async with self._namespace_lock:
            self._namespaces.add(namespace)
        return True

    async def cleanup_expired(self, now: Optional[float] = None) -> None:
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
                expired_events = await self._run_in_executor(
                    self._cleanup_env, namespaces, now
                )
            if expired_events:
                await self._enqueue_callbacks(expired_events)
        except Exception:
            logger.exception("[STORE] Cleanup expired records failed")
            raise

    async def _register_namespace(self, namespace: str, namespace_escaped: str) -> None:
        if not self._is_runtime_thread():
            return await self._run_on_runtime_thread(
                self._register_namespace,
                namespace,
                namespace_escaped,
            )
        # Keep namespace_lock across the registration sequence so callers cannot
        # observe "registered in memory" before meta persistence succeeds.
        async with self._namespace_lock:
            if namespace in self._namespaces:
                return
            self._ensure_data_db_capacity(namespace)
            self._get_db(namespace_escaped)
            if self._callback_guard.get():
                raise RuntimeError(
                    "Expiry callbacks cannot register new namespaces during callback execution"
                )
            async with self._write_lock:
                await self._run_in_executor(self._register_namespace_in_meta, namespace)
            self._namespaces.add(namespace)

    def _get_db(self, namespace: str) -> lmdb._Database:
        db = self._db_cache.get(namespace)
        if db is None:
            with self._db_cache_lock:
                db = self._db_cache.get(namespace)
                if db is None:
                    db = self._open_data_db(
                        namespace.encode("utf-8"), create=True)
                    self._db_cache[namespace] = db
        return db

    def _put(
        self,
        namespace: str,
        namespace_escaped: str,
        key_bytes: bytes,
        value: object,
        expire_ts: int,
        callback_name: str | None,
    ) -> None:
        db = self._get_db(namespace_escaped)
        exp_db = self._get_exp_db(namespace)
        expmeta_db = self._get_expmeta_db(namespace)
        callback_entry_db = self._get_callback_entry_db(namespace)
        value_bytes = cbor2.dumps(value)
        if len(value_bytes) > self._config.max_value_bytes:
            raise ValueError(
                f"Store value exceeds {self._config.max_value_bytes} bytes")

        payload = _EXPIRY_STRUCT.pack(expire_ts) + value_bytes
        estimated = len(payload) + len(key_bytes) + 64
        if expire_ts:
            estimated += len(key_bytes) + _BUCKET_STRUCT.size + 64
        meta_estimated = len(key_bytes) + 32
        if expire_ts:
            meta_estimated += len(key_bytes) + _BUCKET_STRUCT.size + 32
        if callback_name:
            meta_estimated += len(callback_name.encode("utf-8")) + 32
        old_expire = 0
        with self._meta_env.begin(write=False, buffers=True) as meta_txn:
            raw = meta_txn.get(key_bytes, db=expmeta_db)
            if raw is not None and len(raw) >= _EXPIRY_STRUCT.size:
                old_expire = _EXPIRY_STRUCT.unpack_from(raw, 0)[0]
        if not old_expire:
            with self._env.begin(write=False, buffers=True) as data_txn:
                old_payload = data_txn.get(key_bytes, db=db)
                if old_payload is not None and len(old_payload) >= _EXPIRY_STRUCT.size:
                    old_expire = _EXPIRY_STRUCT.unpack_from(old_payload, 0)[0]

        with self._meta_env.begin(write=False) as meta_txn:
            old_callback_raw = meta_txn.get(key_bytes, db=callback_entry_db)

        def _write_meta(txn) -> None:
            if old_expire:
                old_bucket = _BUCKET_STRUCT.pack(old_expire // 60)
                txn.delete(old_bucket, key_bytes, db=exp_db)

            if expire_ts:
                bucket = _BUCKET_STRUCT.pack(expire_ts // 60)
                txn.put(bucket, key_bytes, db=exp_db)
                txn.put(
                    key_bytes,
                    _EXPIRY_STRUCT.pack(expire_ts),
                    db=expmeta_db,
                )
            else:
                txn.delete(key_bytes, db=expmeta_db)

            if callback_name is None:
                txn.delete(key_bytes, db=callback_entry_db)
            else:
                txn.put(
                    key_bytes,
                    callback_name.encode("utf-8"),
                    db=callback_entry_db,
                )

        self._write_meta_txn_with_resize(_write_meta, meta_estimated)

        try:
            def _write_data(txn) -> None:
                txn.put(key_bytes, payload, db=db)

            self._write_data_txn_with_resize(_write_data, estimated)
        except Exception:
            def _restore_meta(txn) -> None:
                if expire_ts:
                    bucket = _BUCKET_STRUCT.pack(expire_ts // 60)
                    txn.delete(bucket, key_bytes, db=exp_db)
                if old_expire:
                    bucket = _BUCKET_STRUCT.pack(old_expire // 60)
                    txn.put(bucket, key_bytes, db=exp_db)
                    txn.put(
                        key_bytes,
                        _EXPIRY_STRUCT.pack(old_expire),
                        db=expmeta_db,
                    )
                else:
                    txn.delete(key_bytes, db=expmeta_db)

                if old_callback_raw is None:
                    txn.delete(key_bytes, db=callback_entry_db)
                else:
                    txn.put(key_bytes, old_callback_raw, db=callback_entry_db)

            self._write_meta_txn_with_resize(_restore_meta, meta_estimated)
            raise

    def _get(
        self, namespace: str, namespace_escaped: str, key_bytes: bytes
    ) -> tuple[Optional[object], Optional[ExpiryCallbackEvent], Optional[int]]:
        db = self._get_db(namespace_escaped)
        callback_entry_db = self._get_callback_entry_db(namespace)
        callback_name: str | None = None

        with self._env.begin(write=False, buffers=True) as data_txn:
            payload = data_txn.get(key_bytes, db=db)

        if payload is not None:
            with self._meta_env.begin(write=False) as meta_txn:
                callback_raw = meta_txn.get(key_bytes, db=callback_entry_db)
                if callback_raw:
                    callback_name = self._decode_callback_name(callback_raw)

        if payload is None:
            return None, None, None

        if len(payload) < _EXPIRY_STRUCT.size:
            logger.error("[STORE] Corrupt payload header; deleting key")
            return None, None, 0

        expire_ts = _EXPIRY_STRUCT.unpack_from(payload, 0)[0]
        if expire_ts < 0:
            logger.error("[STORE] Corrupt expiry timestamp; deleting key")
            return None, None, 0

        if expire_ts and expire_ts < int(time.time()):
            event = None
            if callback_name:
                event = self._build_expiry_event(
                    namespace,
                    key_bytes,
                    payload,
                    expire_ts,
                    callback_name,
                )
            return None, event, expire_ts

        try:
            return cbor2.loads(payload[_EXPIRY_STRUCT.size:]), None, None
        except Exception:
            logger.exception("[STORE] Corrupt payload; deleting key")
            return None, None, expire_ts

    def _encode_namespace(self, namespace: str) -> str:
        if not namespace:
            raise ValueError("Namespace must be non-empty")
        raw = namespace.encode("utf-8")
        if len(raw) > self._config.max_namespace_bytes:
            raise ValueError(
                f"Namespace exceeds {self._config.max_namespace_bytes} bytes")
        escaped = quote(namespace, safe="")
        if len(escaped.encode("utf-8")) > self._config.max_namespace_bytes:
            raise ValueError(
                f"Escaped namespace exceeds {self._config.max_namespace_bytes} bytes"
            )
        return escaped

    def _encode_key(self, key: str) -> bytes:
        raw = key.encode("utf-8")
        if len(raw) > self._config.max_key_bytes:
            raise ValueError(
                f"Store key exceeds {self._config.max_key_bytes} bytes")
        escaped = quote(key, safe="")
        escaped_bytes = escaped.encode("utf-8")
        if len(escaped_bytes) > self._config.max_key_bytes:
            raise ValueError(
                f"Escaped key exceeds {self._config.max_key_bytes} bytes"
            )
        return escaped_bytes

    def _decode_key(self, key_bytes: bytes) -> str:
        return unquote(key_bytes.decode("utf-8"))

    def _get_exp_db(self, namespace: str) -> lmdb._Database:
        db = self._exp_cache.get(namespace)
        if db is None:
            with self._db_cache_lock:
                db = self._exp_cache.get(namespace)
                if db is None:
                    db = self._open_meta_db(
                        _EXP_DB_PREFIX + namespace.encode("utf-8"),
                        create=True,
                        dupsort=True,
                    )
                    self._exp_cache[namespace] = db
        return db

    def _get_expmeta_db(self, namespace: str) -> lmdb._Database:
        db = self._expmeta_cache.get(namespace)
        if db is None:
            with self._db_cache_lock:
                db = self._expmeta_cache.get(namespace)
                if db is None:
                    db = self._open_meta_db(
                        _EXPMETA_DB_PREFIX + namespace.encode("utf-8"),
                        create=True,
                    )
                    self._expmeta_cache[namespace] = db
        return db

    def _get_callback_entry_db(self, namespace: str) -> lmdb._Database:
        db = self._callback_entry_cache.get(namespace)
        if db is None:
            with self._db_cache_lock:
                db = self._callback_entry_cache.get(namespace)
                if db is None:
                    db = self._open_meta_db(
                        _CALLBACK_ENTRY_DB_PREFIX + namespace.encode("utf-8"),
                        create=True,
                    )
                    self._callback_entry_cache[namespace] = db
        return db

    def _cleanup_env(
        self, namespaces: list[str], now: float
    ) -> list[ExpiryCallbackEvent]:
        now_bucket = int(now // 60)
        bucket_limit = _BUCKET_STRUCT.pack(now_bucket)
        deletes_left = self._config.cleanup_max_deletes
        # (namespace, key_bytes, bucket_key, expire_ts, callback_name)
        expired_items: list[tuple[str, bytes, bytes, int, str | None]] = []
        # (namespace, bucket_key, key_bytes)
        stale_exp_index_entries: list[tuple[str, bytes, bytes]] = []
        escaped_namespaces = {
            namespace: self._encode_namespace(namespace) for namespace in namespaces
        }
        data_dbs: dict[str, lmdb._Database] = {}
        data_txns: dict[str, lmdb._Transaction] = {}
        exp_dbs: dict[str, lmdb._Database] = {}
        expmeta_dbs: dict[str, lmdb._Database] = {}
        callback_dbs: dict[str, lmdb._Database] = {}

        def _resolve_data(namespace: str) -> tuple[lmdb._Transaction, lmdb._Database]:
            data_txn = data_txns.get(namespace)
            db = data_dbs.get(namespace)
            if data_txn is not None and db is not None:
                return data_txn, db
            namespace_escaped = escaped_namespaces.get(namespace)
            if namespace_escaped is None:
                namespace_escaped = self._encode_namespace(namespace)
                escaped_namespaces[namespace] = namespace_escaped
            db = self._get_db(namespace_escaped)
            data_txn = self._env.begin(write=False, buffers=True)
            data_dbs[namespace] = db
            data_txns[namespace] = data_txn
            return data_txn, db

        def _resolve_meta(
            namespace: str,
        ) -> tuple[lmdb._Database, lmdb._Database, lmdb._Database]:
            exp_db = exp_dbs.get(namespace)
            expmeta_db = expmeta_dbs.get(namespace)
            callback_db = callback_dbs.get(namespace)
            if exp_db is not None and expmeta_db is not None and callback_db is not None:
                return exp_db, expmeta_db, callback_db
            exp_db = self._get_exp_db(namespace)
            expmeta_db = self._get_expmeta_db(namespace)
            callback_db = self._get_callback_entry_db(namespace)
            exp_dbs[namespace] = exp_db
            expmeta_dbs[namespace] = expmeta_db
            callback_dbs[namespace] = callback_db
            return exp_db, expmeta_db, callback_db

        for namespace in namespaces:
            _resolve_meta(namespace)

        try:
            with self._meta_env.begin(write=False) as meta_txn:
                for namespace in namespaces:
                    if deletes_left <= 0:
                        break
                    exp_db = exp_dbs[namespace]
                    expmeta_db = expmeta_dbs[namespace]
                    callback_db = callback_dbs[namespace]
                    with meta_txn.cursor(db=exp_db) as cursor:
                        if not cursor.first():
                            continue

                        while True:
                            bucket_key = cursor.key()
                            if bucket_key is None or bucket_key > bucket_limit:
                                break

                            bucket_key_bytes = bytes(bucket_key)
                            dup_iter = cursor.iternext_dup()
                            dup_entries = list(
                                itertools.islice(dup_iter, deletes_left))
                            for raw_key in dup_entries:
                                key_bytes = bytes(raw_key)
                                expmeta_ts = 0
                                raw = meta_txn.get(key_bytes, db=expmeta_db)
                                if raw is not None and len(raw) >= _EXPIRY_STRUCT.size:
                                    expmeta_ts = _EXPIRY_STRUCT.unpack_from(raw, 0)[
                                        0]

                                if expmeta_ts and expmeta_ts > now:
                                    expire_ts = expmeta_ts
                                else:
                                    payload_ts = 0
                                    data_txn, data_db = _resolve_data(
                                        namespace)
                                    payload = data_txn.get(
                                        key_bytes, db=data_db)
                                    if payload is not None and len(payload) >= _EXPIRY_STRUCT.size:
                                        payload_ts = _EXPIRY_STRUCT.unpack_from(payload, 0)[
                                            0]
                                    if expmeta_ts and payload_ts:
                                        expire_ts = max(expmeta_ts, payload_ts)
                                    else:
                                        expire_ts = expmeta_ts or payload_ts
                                if expire_ts == 0 or expire_ts > now:
                                    stale_exp_index_entries.append(
                                        (namespace, bucket_key_bytes, key_bytes)
                                    )
                                    continue

                                callback_raw = meta_txn.get(
                                    key_bytes, db=callback_db)
                                callback_name = None
                                if callback_raw:
                                    callback_name = self._decode_callback_name(
                                        callback_raw
                                    )

                                deletes_left -= 1

                                expired_items.append(
                                    (
                                        namespace,
                                        key_bytes,
                                        bucket_key_bytes,
                                        expire_ts,
                                        callback_name,
                                    )
                                )

                                if deletes_left <= 0:
                                    break

                            if not cursor.next_nodup():
                                break

            expired_events: list[ExpiryCallbackEvent] = []
            if any(item[4] for item in expired_items):
                for namespace, key_bytes, _, expire_ts, callback_name in expired_items:
                    if not callback_name:
                        continue
                    data_txn, data_db = _resolve_data(namespace)
                    payload = data_txn.get(key_bytes, db=data_db)
                    event = self._build_expiry_event(
                        namespace,
                        key_bytes,
                        payload,
                        expire_ts,
                        callback_name,
                    )
                    if event is not None:
                        expired_events.append(event)
        finally:
            for txn in data_txns.values():
                txn.abort()

        if expired_items:
            def _delete_data(txn) -> None:
                for namespace, key_bytes, _, _, _ in expired_items:
                    db = data_dbs.get(namespace)
                    if db is None:
                        continue
                    txn.delete(key_bytes, db=db)

            self._write_data_txn_with_resize(_delete_data, 0)

        if stale_exp_index_entries or expired_items:
            def _delete_meta(txn) -> None:
                for namespace, bucket_key, key_bytes in stale_exp_index_entries:
                    exp_db = exp_dbs[namespace]
                    txn.delete(bucket_key, key_bytes, db=exp_db)

                for namespace, key_bytes, bucket_key, _, _ in expired_items:
                    exp_db = exp_dbs[namespace]
                    expmeta_db = expmeta_dbs[namespace]
                    callback_db = callback_dbs[namespace]
                    txn.delete(bucket_key, key_bytes, db=exp_db)
                    txn.delete(key_bytes, db=expmeta_db)
                    txn.delete(key_bytes, db=callback_db)

            self._write_meta_txn_with_resize(_delete_meta, 0)

        return expired_events

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
        growth_factor = max(self._config.map_size_growth_factor, 1)
        headroom = max(int(projected * 0.1), 16 * 4096)
        min_target = projected + headroom
        new_size = max(map_size * growth_factor, min_target)
        if force and new_size <= map_size:
            new_size = map_size + max(headroom, projected - used_bytes)
        return new_size

    def _delete_key(
        self,
        namespace: str,
        namespace_escaped: str,
        key_bytes: bytes,
        expire_ts: int = 0,
    ) -> None:
        db = self._get_db(namespace_escaped)
        exp_db = self._get_exp_db(namespace)
        expmeta_db = self._get_expmeta_db(namespace)
        callback_entry_db = self._get_callback_entry_db(namespace)

        def _delete_data(txn) -> None:
            txn.delete(key_bytes, db=db)

        self._write_data_txn_with_resize(_delete_data, 0)

        def _delete_meta(txn) -> None:
            if expire_ts:
                bucket = _BUCKET_STRUCT.pack(expire_ts // 60)
                txn.delete(bucket, key_bytes, db=exp_db)
            txn.delete(key_bytes, db=expmeta_db)
            txn.delete(key_bytes, db=callback_entry_db)

        self._write_meta_txn_with_resize(_delete_meta, 0)

    def _register_namespace_in_meta(self, namespace: str) -> None:
        meta_key = _META_NS_PREFIX + namespace.encode("utf-8")

        def _write(txn) -> None:
            txn.put(meta_key, b"1", db=self._meta_db, overwrite=False)

        self._write_meta_txn_with_resize(_write, 0)

    def _list_namespaces(self) -> list[str]:
        namespaces: list[str] = []
        with self._meta_env.begin(write=False) as txn, txn.cursor(db=self._meta_db) as cursor:
            if cursor.set_range(_META_NS_PREFIX):
                for key, _ in cursor:
                    if not key.startswith(_META_NS_PREFIX):
                        break
                    namespaces.append(
                        key[len(_META_NS_PREFIX):].decode("utf-8"))
        return namespaces

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

    def _ensure_data_db_capacity(self, namespace: str) -> None:
        if self._config.max_dbs == 0:
            return
        if namespace in self._internal_namespaces:
            return
        current = 0
        for name in self._namespaces:
            if name in self._internal_namespaces:
                continue
            current += 1
        if current + 1 > self._config.max_dbs:
            raise RuntimeError(
                "LMDB namespace quota reached; increase store_lmdb.max_dbs or set 0 to disable the quota."
            )

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

    def _assert_namespace_access(self, namespace: str) -> None:
        with self._exclusive_registry_lock:
            lock = self._exclusive_locks.get(namespace)
            active_lease_id = self._exclusive_active_leases.get(namespace)
        if lock is None:
            return
        state = self._exclusive_guard.get() or {}
        lease_entry = state.get(namespace)
        if lease_entry is None:
            raise RuntimeError(
                f"Namespace '{namespace}' is exclusive; "
                f"use store.create_namespace_lock('{namespace}') to access it."
            )
        lease_id, _depth = lease_entry
        if active_lease_id != lease_id:
            raise RuntimeError(
                f"Namespace '{namespace}' exclusive lease is no longer active. "
                f"Reacquire via store.create_namespace_lock('{namespace}')."
            )

    async def _acquire_exclusive(self, namespace: str) -> tuple[str, bool]:
        with self._exclusive_registry_lock:
            lock = self._exclusive_locks.get(namespace)
            if lock is None:
                lock = threading.Lock()
                self._exclusive_locks[namespace] = lock

        state = self._exclusive_guard.get() or {}
        lease_entry = state.get(namespace)
        with self._exclusive_registry_lock:
            active_lease_id = self._exclusive_active_leases.get(namespace)
        if lease_entry is not None:
            lease_id, depth = lease_entry
            if depth > 0 and active_lease_id == lease_id:
                new_state = dict(state)
                new_state[namespace] = (lease_id, depth + 1)
                self._exclusive_guard.set(new_state)
                return lease_id, False
            # Drop stale inherited state and reacquire a fresh lease.
            new_state = dict(state)
            new_state.pop(namespace, None)
            self._exclusive_guard.set(new_state)
            state = new_state

        acquired = await self._await_cancellable_thread_call(
            partial(self._acquire_thread_lock_cancellable, lock)
        )
        if not acquired:
            raise asyncio.CancelledError()
        lease_id = uuid4().hex
        with self._exclusive_registry_lock:
            self._exclusive_active_leases[namespace] = lease_id
        new_state = dict(state)
        new_state[namespace] = (lease_id, 1)
        self._exclusive_guard.set(new_state)
        return lease_id, True

    def _release_exclusive(self, namespace: str) -> None:
        state = self._exclusive_guard.get() or {}
        lease_entry = state.get(namespace)
        if lease_entry is None:
            logger.warning(
                "[STORE] Exclusive namespace release without acquisition",
                namespace=namespace,
            )
            return
        lease_id, depth = lease_entry

        if depth > 1:
            new_state = dict(state)
            new_state[namespace] = (lease_id, depth - 1)
            self._exclusive_guard.set(new_state)
            return

        new_state = dict(state)
        new_state.pop(namespace, None)
        self._exclusive_guard.set(new_state)

        with self._exclusive_registry_lock:
            active_lease_id = self._exclusive_active_leases.get(namespace)
        if active_lease_id != lease_id:
            logger.warning(
                "[STORE] Exclusive namespace lease mismatch during release",
                namespace=namespace,
                expected_lease=active_lease_id,
                local_lease=lease_id,
            )
            return

        with self._exclusive_registry_lock:
            lock = self._exclusive_locks.get(namespace)
            self._exclusive_active_leases.pop(namespace, None)
        if lock is None:
            logger.warning(
                "[STORE] Exclusive namespace lock missing during release",
                namespace=namespace,
            )
            return
        if lock.locked():
            lock.release()
            return
        logger.warning(
            "[STORE] Exclusive namespace lock already released",
            namespace=namespace,
        )

    async def _run_in_executor(self, fn, *args, **kwargs):
        if kwargs:
            fn = partial(fn, **kwargs)
        return fn(*args)

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
                logger.exception("[STORE] Cancellation cleanup for thread wait failed")
            raise

    @staticmethod
    def _acquire_thread_lock_cancellable(
        lock: threading.Lock,
        cancel_event: threading.Event,
    ) -> bool:
        while not cancel_event.is_set():
            acquired = lock.acquire(timeout=_RUNTIME_BLOCKING_WAIT_POLL_SECONDS)
            if not acquired:
                continue
            if cancel_event.is_set():
                lock.release()
                return False
            return True
        return False

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

    @staticmethod
    def _try_acquire_file_lock(path: Path) -> Optional[TextIO]:
        return _runtime.try_acquire_file_lock(path)

    @staticmethod
    def _release_file_lock(handle: Optional[TextIO]) -> None:
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
        cleanup_fn,
        stop_event: asyncio.Event,
        interval_seconds: int = 60,
    ) -> None:
        await _runtime.cleanup_loop(name, cleanup_fn, stop_event, interval_seconds)

    async def _ensure_callback_worker(self) -> bool:
        if self._callback_future is not None:
            if not self._callback_future.done():
                return True
            try:
                self._callback_future.result()
            except Exception:
                logger.exception("[STORE] Callback worker exited with exception")
            finally:
                self._callback_future = None
        self._callback_lock_handle = await self._run_in_executor(
            self._try_acquire_file_lock,
            self._base_dir / ".store_callbacks.lock",
        )
        if self._callback_lock_handle is None:
            logger.info("[STORE] Callback worker skipped (lock not acquired)")
            return False
        self._callback_dispatch_stop = threading.Event()
        self._callback_wakeup = threading.Event()
        self._callback_active.clear()
        self._callback_future = self._callback_executor.submit(
            self._callback_worker_main
        )
        return True

    async def _enqueue_callbacks(self, events: list[ExpiryCallbackEvent]) -> None:
        if not events:
            return
        await self._ensure_callback_worker()
        await self._run_in_executor(self._persist_callback_jobs, events)
        self._callback_wakeup.set()

    def _callback_worker_main(self) -> None:
        try:
            while not self._callback_dispatch_stop.is_set():
                job = self._peek_due_callback_job(int(time.time()))
                if job is None:
                    self._callback_wakeup.wait(timeout=_CALLBACK_POLL_INTERVAL_SECONDS)
                    self._callback_wakeup.clear()
                    continue

                should_complete = False
                self._callback_active.set()
                try:
                    should_complete = self._run_callback(job.event)
                finally:
                    self._callback_active.clear()

                if should_complete:
                    self._complete_callback_job(
                        job.due_ts,
                        job.job_id,
                    )
                else:
                    self._retry_callback_job(job)
        except Exception:
            logger.exception("[STORE] Expiry callback worker failed")
        finally:
            self._release_file_lock(self._callback_lock_handle)
            self._callback_lock_handle = None
            logger.debug("[STORE] Expiry callback worker stopped")

    async def _stop_callback_worker(self) -> bool:
        if self._callback_future is None:
            await self._run_in_executor(
                self._release_file_lock, self._callback_lock_handle
            )
            self._callback_lock_handle = None
            return True

        self._callback_dispatch_stop.set()
        self._callback_wakeup.set()
        future = self._callback_future
        try:
            await asyncio.wrap_future(future)
        except Exception:
            logger.exception("[STORE] Callback worker stopped with exception")
            return False
        finally:
            if future.done():
                self._callback_future = None
        return future.done()

    def _run_callback(self, event: ExpiryCallbackEvent) -> bool:
        with self._callback_registry_lock:
            registration = self._callback_registry.get(event.callback)
        if registration is None:
            logger.error(
                "[STORE] Expiry callback not registered; skipping",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
            )
            return True
        callback = registration.fn
        timeout_seconds = registration.timeout_seconds
        future = self._callback_runner_executor.submit(
            self._execute_callback_in_runner,
            callback,
            event,
        )
        try:
            future.result(timeout=timeout_seconds)
            return True
        except FutureTimeoutError:
            future.cancel()
            logger.warning(
                "[STORE] Expiry callback timed out",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
                timeout_seconds=timeout_seconds,
            )
            return False
        except Exception:
            logger.exception(
                "[STORE] Expiry callback failed",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
            )
            return False

    def _execute_callback_in_runner(
        self,
        callback: ExpiryCallback,
        event: ExpiryCallbackEvent,
    ) -> None:
        token = self._callback_guard.set(True)
        try:
            asyncio.run(callback(event))
        finally:
            self._callback_guard.reset(token)

    def _is_runtime_thread(self) -> bool:
        return self._runtime_thread_id == threading.get_ident()

    def _start_runtime_thread(self) -> None:
        if self._runtime_thread is not None:
            return
        self._runtime_ready.clear()
        self._runtime_thread = threading.Thread(
            target=self._runtime_thread_main,
            name="store-runtime",
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
                    "StoreService runtime thread failed to start and could not be stopped"
                )
            self._runtime_thread = None
            self._runtime_loop = None
            self._runtime_thread_id = None
            raise RuntimeError(
                "StoreService runtime thread did not start within timeout"
            )
        if self._runtime_loop is None:
            raise RuntimeError("StoreService runtime thread failed to start")

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
                raise RuntimeError("StoreService is closed")
            raise RuntimeError("StoreService runtime loop is unavailable")
        acquired = await self._await_cancellable_thread_call(
            self._acquire_runtime_submit_slot
        )
        if not acquired:
            raise TimeoutError("StoreService runtime queue is saturated")

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
            raise RuntimeError("StoreService runtime thread did not stop in time")
        self._runtime_thread = None

    @staticmethod
    def _encode_callback_job_payload(event: ExpiryCallbackEvent, attempts: int) -> bytes:
        return cbor2.dumps(
            {
                "namespace": event.namespace,
                "key": event.key,
                "value": event.value,
                "expire_ts": event.expire_ts,
                "callback": event.callback,
                "attempts": attempts,
            }
        )

    def _persist_callback_jobs(self, events: list[ExpiryCallbackEvent]) -> None:
        now_ts = int(time.time())
        due_key = _EXPIRY_STRUCT.pack(now_ts)
        estimated = 0
        serialized_events: list[tuple[bytes, bytes]] = []
        for event in events:
            job_id = uuid4().hex.encode("ascii")
            payload = self._encode_callback_job_payload(event, attempts=0)
            serialized_events.append((job_id, payload))
            estimated += len(job_id) + len(payload) + len(due_key) + 128

        def _write(txn) -> None:
            for job_id, payload in serialized_events:
                txn.put(job_id, payload, db=self._callback_job_db)
                txn.put(due_key, job_id, db=self._callback_schedule_db)

        self._write_meta_txn_with_resize(_write, estimated)

    def _peek_due_callback_job(self, now_ts: int) -> Optional[_CallbackJob]:
        with self._meta_env.begin(write=True) as txn, txn.cursor(db=self._callback_schedule_db) as cursor:
            if not cursor.first():
                return None

            while True:
                due_key = cursor.key()
                if due_key is None:
                    return None
                if len(due_key) < _EXPIRY_STRUCT.size:
                    cursor.delete()
                    if not cursor.next():
                        return None
                    continue

                due_ts = _EXPIRY_STRUCT.unpack_from(due_key, 0)[0]
                if due_ts > now_ts:
                    return None

                job_id = bytes(cursor.value())
                payload = txn.get(job_id, db=self._callback_job_db)
                if payload is None:
                    cursor.delete()
                    if not cursor.next():
                        return None
                    continue

                decoded = self._decode_callback_job(payload)
                if decoded is None:
                    cursor.delete()
                    txn.delete(job_id, db=self._callback_job_db)
                    if not cursor.next():
                        return None
                    continue

                event, attempts = decoded
                return _CallbackJob(
                    job_id=job_id,
                    due_ts=due_ts,
                    event=event,
                    attempts=attempts,
                )

    def _complete_callback_job(self, due_ts: int, job_id: bytes) -> None:
        due_key = _EXPIRY_STRUCT.pack(due_ts)

        def _write(txn) -> None:
            txn.delete(due_key, job_id, db=self._callback_schedule_db)
            txn.delete(job_id, db=self._callback_job_db)

        self._write_meta_txn_with_resize(_write, 0)

    def _retry_callback_job(self, job: _CallbackJob) -> None:
        next_attempt = job.attempts + 1
        if next_attempt > _CALLBACK_MAX_RETRIES:
            self._dead_letter_callback_job(job)
            return

        delay_seconds = min(
            _CALLBACK_RETRY_BASE_SECONDS * (2 ** job.attempts),
            _CALLBACK_RETRY_MAX_SECONDS,
        )
        next_due_ts = int(time.time()) + delay_seconds
        old_due_key = _EXPIRY_STRUCT.pack(job.due_ts)
        next_due_key = _EXPIRY_STRUCT.pack(next_due_ts)
        payload = self._encode_callback_job_payload(job.event, attempts=next_attempt)
        estimated = len(job.job_id) + len(payload) + len(next_due_key) + 128

        def _write(txn) -> None:
            txn.delete(old_due_key, job.job_id, db=self._callback_schedule_db)
            txn.put(job.job_id, payload, db=self._callback_job_db)
            txn.put(next_due_key, job.job_id, db=self._callback_schedule_db)

        self._write_meta_txn_with_resize(_write, estimated)

    def _dead_letter_callback_job(self, job: _CallbackJob) -> None:
        now_ts = int(time.time())
        old_due_key = _EXPIRY_STRUCT.pack(job.due_ts)
        dlq_key = f"{now_ts}:{uuid4().hex}".encode("ascii")
        payload = cbor2.dumps(
            {
                "job_id": job.job_id.decode("ascii", errors="replace"),
                "namespace": job.event.namespace,
                "key": job.event.key,
                "value": job.event.value,
                "expire_ts": job.event.expire_ts,
                "callback": job.event.callback,
                "attempts": job.attempts + 1,
                "failed_at": now_ts,
                "reason": "callback_failed_max_retries",
            }
        )
        estimated = len(dlq_key) + len(payload) + len(job.job_id) + len(old_due_key) + 128

        def _write(txn) -> None:
            txn.delete(old_due_key, job.job_id, db=self._callback_schedule_db)
            txn.delete(job.job_id, db=self._callback_job_db)
            txn.put(dlq_key, payload, db=self._callback_dead_letter_db)

        self._write_meta_txn_with_resize(_write, estimated)

    def _count_callback_jobs(self) -> int:
        with self._meta_env.begin(write=False) as txn:
            return txn.stat(db=self._callback_job_db)["entries"]

    def _count_dead_letter_callback_jobs(self) -> int:
        with self._meta_env.begin(write=False) as txn:
            return txn.stat(db=self._callback_dead_letter_db)["entries"]

    def _decode_callback_job(
        self, payload: bytes
    ) -> Optional[tuple[ExpiryCallbackEvent, int]]:
        try:
            data = cbor2.loads(payload)
        except Exception:
            logger.exception("[STORE] Corrupt callback job payload; dropping")
            return None
        try:
            attempts_raw = int(data.get("attempts", 0))
            attempts = attempts_raw if attempts_raw >= 0 else 0
            event = ExpiryCallbackEvent(
                namespace=str(data["namespace"]),
                key=str(data["key"]),
                value=data.get("value"),
                expire_ts=int(data["expire_ts"]),
                callback=str(data["callback"]),
            )
            return event, attempts
        except Exception:
            logger.exception("[STORE] Invalid callback job fields; dropping")
            return None

    def _normalize_callback_name(self, name: str) -> str:
        if not name or name.strip() == "":
            raise ValueError("Callback name must be non-empty")
        return name.strip()

    def _decode_callback_name(self, raw: bytes) -> Optional[str]:
        try:
            return raw.decode("utf-8")
        except Exception:
            logger.exception("[STORE] Corrupt callback name; skipping")
            return None

    def _register_callback_name(self, name: str) -> None:
        def _write(txn) -> None:
            key = _CALLBACK_REGISTRY_PREFIX + name.encode("utf-8")
            txn.put(key, b"1", db=self._callback_db)
        self._write_meta_txn_with_resize(_write, 0)

    def _unregister_callback_name(self, name: str) -> None:
        def _write(txn) -> None:
            key = _CALLBACK_REGISTRY_PREFIX + name.encode("utf-8")
            txn.delete(key, db=self._callback_db)
        self._write_meta_txn_with_resize(_write, 0)

    def _build_expiry_event(
        self,
        namespace: str,
        key_bytes: bytes,
        payload,
        expire_ts: int,
        callback_name: str,
    ) -> Optional[ExpiryCallbackEvent]:
        if payload is None or len(payload) < _EXPIRY_STRUCT.size:
            return ExpiryCallbackEvent(
                namespace=namespace,
                key=self._decode_key(key_bytes),
                value=None,
                expire_ts=expire_ts,
                callback=callback_name,
            )
        try:
            value = cbor2.loads(payload[_EXPIRY_STRUCT.size:])
        except Exception:
            logger.exception(
                "[STORE] Failed to decode expired payload for callback",
                namespace=namespace,
            )
            value = None
        return ExpiryCallbackEvent(
            namespace=namespace,
            key=self._decode_key(key_bytes),
            value=value,
            expire_ts=expire_ts,
            callback=callback_name,
        )
