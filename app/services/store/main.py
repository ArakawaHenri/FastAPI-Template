from __future__ import annotations

import asyncio
import contextvars
import inspect
import itertools
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Awaitable, Callable, Optional, TextIO
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
_CALLBACK_POLL_INTERVAL_SECONDS = 1.0
_CALLBACK_WAIT_POLL_SECONDS = 0.05
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
        self._executor = ThreadPoolExecutor(
            max_workers=config.worker_threads,
            thread_name_prefix="store-lmdb",
        )

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
        self._tighten_file_permissions(self._data_path.with_name(f"{self._data_path.name}-lock"))
        self._tighten_file_permissions(self._meta_path.with_name(f"{self._meta_path.name}-lock"))

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
        self._meta_db: lmdb._Database = self._open_meta_db(
            _META_DB_NAME, create=True)
        self._db_cache_lock = threading.Lock()
        self._namespace_lock = asyncio.Lock()
        self._resize_lock = threading.Lock()
        self._meta_resize_lock = threading.Lock()
        self._write_lock = asyncio.Lock()
        self._exclusive_locks: dict[str, asyncio.Lock] = {}
        self._exclusive_guard = contextvars.ContextVar(
            "store_exclusive_guard", default=None)
        self._namespaces: set[str] = set()
        self._internal_namespaces: set[str] = set()
        self._callback_registry: dict[str, ExpiryCallback] = {}
        self._builtin_callbacks: set[str] = set()
        self._callback_registry_lock = asyncio.Lock()
        self._callback_task: Optional[asyncio.Task] = None
        self._callback_guard = contextvars.ContextVar(
            "store_callback_guard", default=False)
        self._callback_dispatch_stop = asyncio.Event()
        self._callback_wakeup = asyncio.Event()
        self._callback_active = False
        self._callback_lock_handle = None

        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_lock_handle = None
        self._closed = False
        self._namespaces.update(self._list_namespaces())

        logger.debug("[STORE] StoreService initialized (zLMDB)")

    class _ExclusiveNamespaceLock:
        def __init__(self, store: "StoreService", namespace: str) -> None:
            self._store = store
            self._namespace = namespace

        async def __aenter__(self) -> "StoreService._ExclusiveNamespaceLock":
            await self._store._acquire_exclusive(self._namespace)
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            self._store._release_exclusive(self._namespace)

    def create_namespace_lock(self, namespace: str) -> "StoreService._ExclusiveNamespaceLock":
        self._encode_namespace(namespace)
        if namespace not in self._exclusive_locks:
            self._exclusive_locks[namespace] = asyncio.Lock()
        return StoreService._ExclusiveNamespaceLock(self, namespace)

    async def mark_internal_namespace(self, namespace: str) -> None:
        self._assert_open()
        self._assert_not_in_callback()
        self._encode_namespace(namespace)
        async with self._namespace_lock:
            self._internal_namespaces.add(namespace)

    async def start_cleanup(self) -> None:
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

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._cleanup_stop.set()
        if self._cleanup_task is not None:
            await self._cleanup_task
        await self._stop_callback_worker()
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
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            partial(self._executor.shutdown, wait=True, cancel_futures=True),
        )
        self._closed = True
        logger.debug("[STORE] StoreService shutdown completed")

    async def register_expiry_callback(self, name: str, fn: ExpiryCallback) -> None:
        self._assert_open()
        self._assert_not_in_callback()
        callback_name = self._normalize_callback_name(name)
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("Expiry callback must be an async function")
        async with self._callback_registry_lock:
            if callback_name not in self._builtin_callbacks:
                raise ValueError(
                    f"Expiry callback '{callback_name}' is not a builtin callback"
                )
            self._callback_registry[callback_name] = fn
        async with self._write_lock:
            await self._run_in_executor(
                self._register_callback_name,
                callback_name,
            )

    async def register_builtin_callback(self, name: str) -> None:
        self._assert_open()
        self._assert_not_in_callback()
        callback_name = self._normalize_callback_name(name)
        async with self._callback_registry_lock:
            self._builtin_callbacks.add(callback_name)

    async def unregister_expiry_callback(self, name: str) -> None:
        self._assert_open()
        self._assert_not_in_callback()
        callback_name = self._normalize_callback_name(name)
        async with self._callback_registry_lock:
            self._callback_registry.pop(callback_name, None)
        async with self._write_lock:
            await self._run_in_executor(
                self._unregister_callback_name,
                callback_name,
            )

    async def wait_for_callbacks(self) -> None:
        await self._ensure_callback_worker()
        while True:
            pending = await self._run_in_executor(self._count_callback_jobs)
            if pending == 0 and not self._callback_active:
                return
            await asyncio.sleep(_CALLBACK_WAIT_POLL_SECONDS)

    async def set(
        self,
        namespace: str = "default",
        key: str = "",
        value: object = "",
        retention: Optional[int] = None,
        on_expire: str | None = None,
    ) -> None:
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
            async with self._callback_registry_lock:
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
        self._assert_open()
        self._assert_namespace_access(namespace)
        if not key:
            raise ValueError("Store key must be non-empty")

        try:
            namespace_escaped = self._encode_namespace(namespace)
            key_bytes = self._encode_key(key)
            exists = await self._namespace_exists(namespace)
            if not exists:
                logger.warning(
                    "[STORE] Namespace not found",
                    namespace=namespace,
                    key=key,
                )
                return None
            value, event, delete_ts = await self._run_in_executor(
                self._get, namespace, namespace_escaped, key_bytes
            )
            if value is None and event is None and delete_ts is None:
                logger.warning(
                    "[STORE] Key not found",
                    namespace=namespace,
                    key=key,
                )
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
        async with self._namespace_lock:
            if namespace in self._namespaces:
                return
            self._ensure_data_db_capacity(namespace)
            self._namespaces.add(namespace)

        self._get_db(namespace_escaped)
        if self._callback_guard.get():
            raise RuntimeError(
                "Expiry callbacks cannot register new namespaces during callback execution"
            )
        async with self._write_lock:
            await self._run_in_executor(self._register_namespace_in_meta, namespace)

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
        expired_items: list[tuple[str, bytes, int, str | None]] = []
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
            def _cleanup(meta_txn) -> None:
                nonlocal deletes_left
                for namespace in namespaces:
                    if deletes_left <= 0:
                        return
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
                                    meta_txn.delete(
                                        bucket_key, key_bytes, db=exp_db)
                                    continue

                                callback_raw = meta_txn.get(
                                    key_bytes, db=callback_db)
                                callback_name = None
                                if callback_raw:
                                    callback_name = self._decode_callback_name(
                                        callback_raw
                                    )

                                meta_txn.delete(
                                    bucket_key, key_bytes, db=exp_db)
                                meta_txn.delete(key_bytes, db=expmeta_db)
                                meta_txn.delete(key_bytes, db=callback_db)
                                deletes_left -= 1

                                expired_items.append(
                                    (namespace, key_bytes, expire_ts, callback_name)
                                )

                                if deletes_left <= 0:
                                    return

                            if not cursor.next_nodup():
                                break

            self._write_meta_txn_with_resize(_cleanup, 0)

            if not expired_items:
                return []

            expired_events: list[ExpiryCallbackEvent] = []
            if any(item[3] for item in expired_items):
                for namespace, key_bytes, expire_ts, callback_name in expired_items:
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

        def _delete_data(txn) -> None:
            for namespace, key_bytes, _, _ in expired_items:
                db = data_dbs.get(namespace)
                if db is None:
                    continue
                txn.delete(key_bytes, db=db)

        self._write_data_txn_with_resize(_delete_data, 0)
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
        with self._meta_env.begin(write=False) as txn:
            with txn.cursor(db=self._meta_db) as cursor:
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
        lock = self._exclusive_locks.get(namespace)
        if lock is None:
            return
        state = self._exclusive_guard.get()
        if not state or state.get(namespace, 0) <= 0:
            raise RuntimeError(
                f"Namespace '{namespace}' is exclusive; use store.exclusive('{namespace}') to access it."
            )

    async def _acquire_exclusive(self, namespace: str) -> None:
        lock = self._exclusive_locks.get(namespace)
        if lock is None:
            lock = asyncio.Lock()
            self._exclusive_locks[namespace] = lock
        state = self._exclusive_guard.get()
        if state and state.get(namespace, 0) > 0:
            new_state = dict(state)
            new_state[namespace] = state.get(namespace, 0) + 1
            self._exclusive_guard.set(new_state)
            return
        await lock.acquire()
        state = self._exclusive_guard.get()
        new_state = dict(state) if state else {}
        new_state[namespace] = 1
        self._exclusive_guard.set(new_state)

    def _release_exclusive(self, namespace: str) -> None:
        state = self._exclusive_guard.get() or {}
        count = state.get(namespace, 0)
        if count <= 0:
            logger.warning(
                "[STORE] Exclusive namespace release without acquisition",
                namespace=namespace,
            )
            return
        lock = self._exclusive_locks.get(namespace)
        if lock is None:
            logger.warning(
                "[STORE] Exclusive namespace lock missing during release",
                namespace=namespace,
            )
            return
        if count == 1:
            new_state = dict(state)
            new_state.pop(namespace, None)
            self._exclusive_guard.set(new_state)
            lock.release()
        else:
            new_state = dict(state)
            new_state[namespace] = count - 1
            self._exclusive_guard.set(new_state)

    async def _run_in_executor(self, fn, *args, **kwargs):
        return await _runtime.run_in_executor(self._executor, fn, *args, **kwargs)

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

    async def _ensure_callback_worker(self) -> None:
        if self._callback_task is not None and not self._callback_task.done():
            return
        self._callback_lock_handle = await self._run_in_executor(
            self._try_acquire_file_lock,
            self._base_dir / ".store_callbacks.lock",
        )
        if self._callback_lock_handle is None:
            logger.info("[STORE] Callback worker skipped (lock not acquired)")
            return
        self._callback_dispatch_stop = asyncio.Event()
        self._callback_task = asyncio.create_task(self._callback_worker())

    async def _enqueue_callbacks(self, events: list[ExpiryCallbackEvent]) -> None:
        if not events:
            return
        await self._ensure_callback_worker()
        await self._run_in_executor(self._persist_callback_jobs, events)
        self._callback_wakeup.set()

    async def _callback_worker(self) -> None:
        try:
            while not self._callback_dispatch_stop.is_set():
                job = await self._run_in_executor(
                    self._peek_due_callback_job, int(time.time())
                )
                if job is None:
                    self._callback_wakeup.clear()
                    try:
                        await asyncio.wait_for(
                            self._callback_wakeup.wait(),
                            timeout=_CALLBACK_POLL_INTERVAL_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        continue
                    continue

                self._callback_active = True
                try:
                    await self._run_callback(job.event)
                finally:
                    self._callback_active = False
                    await self._run_in_executor(
                        self._complete_callback_job,
                        job.due_ts,
                        job.job_id,
                    )
        finally:
            self._release_file_lock(self._callback_lock_handle)
            self._callback_lock_handle = None
            logger.debug("[STORE] Expiry callback worker stopped")

    async def _stop_callback_worker(self) -> None:
        if self._callback_task is None:
            await self._run_in_executor(
                self._release_file_lock, self._callback_lock_handle
            )
            self._callback_lock_handle = None
            return
        self._callback_dispatch_stop.set()
        self._callback_wakeup.set()
        await self._callback_task
        self._callback_task = None
        await self._run_in_executor(
            self._release_file_lock, self._callback_lock_handle
        )
        self._callback_lock_handle = None

    async def _run_callback(self, event: ExpiryCallbackEvent) -> None:
        callback = self._callback_registry.get(event.callback)
        if callback is None:
            logger.error(
                "[STORE] Expiry callback not registered; skipping",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
            )
            return
        token = self._callback_guard.set(True)
        try:
            async with self._write_lock:
                await callback(event)
        except Exception:
            logger.exception(
                "[STORE] Expiry callback failed",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
            )
        finally:
            self._callback_guard.reset(token)

    def _persist_callback_jobs(self, events: list[ExpiryCallbackEvent]) -> None:
        now_ts = int(time.time())
        due_key = _EXPIRY_STRUCT.pack(now_ts)
        estimated = 0
        serialized_events: list[tuple[bytes, bytes]] = []
        for event in events:
            job_id = uuid4().hex.encode("ascii")
            payload = cbor2.dumps(
                {
                    "namespace": event.namespace,
                    "key": event.key,
                    "value": event.value,
                    "expire_ts": event.expire_ts,
                    "callback": event.callback,
                }
            )
            serialized_events.append((job_id, payload))
            estimated += len(job_id) + len(payload) + len(due_key) + 128

        def _write(txn) -> None:
            for job_id, payload in serialized_events:
                txn.put(job_id, payload, db=self._callback_job_db)
                txn.put(due_key, job_id, db=self._callback_schedule_db)

        self._write_meta_txn_with_resize(_write, estimated)

    def _peek_due_callback_job(self, now_ts: int) -> Optional[_CallbackJob]:
        with self._meta_env.begin(write=True) as txn:
            with txn.cursor(db=self._callback_schedule_db) as cursor:
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

                    event = self._decode_callback_job(payload)
                    if event is None:
                        cursor.delete()
                        txn.delete(job_id, db=self._callback_job_db)
                        if not cursor.next():
                            return None
                        continue

                    return _CallbackJob(job_id=job_id, due_ts=due_ts, event=event)

    def _complete_callback_job(self, due_ts: int, job_id: bytes) -> None:
        due_key = _EXPIRY_STRUCT.pack(due_ts)

        def _write(txn) -> None:
            txn.delete(due_key, job_id, db=self._callback_schedule_db)
            txn.delete(job_id, db=self._callback_job_db)

        self._write_meta_txn_with_resize(_write, 0)

    def _count_callback_jobs(self) -> int:
        with self._meta_env.begin(write=False) as txn:
            return txn.stat(db=self._callback_job_db)["entries"]

    def _decode_callback_job(self, payload: bytes) -> Optional[ExpiryCallbackEvent]:
        try:
            data = cbor2.loads(payload)
        except Exception:
            logger.exception("[STORE] Corrupt callback job payload; dropping")
            return None
        try:
            return ExpiryCallbackEvent(
                namespace=str(data["namespace"]),
                key=str(data["key"]),
                value=data.get("value"),
                expire_ts=int(data["expire_ts"]),
                callback=str(data["callback"]),
            )
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
