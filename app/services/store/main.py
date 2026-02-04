from __future__ import annotations

import asyncio
import itertools
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import cbor2
from loguru import logger
import zlmdb.lmdb as lmdb

from app.core.shared.store_cleanup import (
    cleanup_loop,
    release_file_lock,
    try_acquire_file_lock,
)
from app.services import BaseService


_EXPIRY_STRUCT = struct.Struct(">q")  # int64, epoch seconds; 0 means no expiry
_BUCKET_STRUCT = struct.Struct(">q")  # int64, epoch minutes
_INT64_MAX = 2 ** 63 - 1
_RESERVED_NAMESPACE_PREFIXES = ("__exp__", "__expmeta__", "__meta__")
_META_DB_NAME = b"__meta__"
_META_NS_PREFIX = b"ns:"

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
                )
            )

        @staticmethod
        async def dtor(instance: "StoreService") -> None:
            await instance.shutdown()

    def __init__(self, config: StoreConfig) -> None:
        self._validate_config(config)
        self._config = config
        self._path = Path(config.path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

        self._env = lmdb.open(
            str(self._path),
            map_size=config.map_size_mb * 1024 * 1024,
            max_dbs=config.max_dbs,
            max_readers=config.max_readers,
            sync=config.sync,
            metasync=config.metasync,
            writemap=config.writemap,
            map_async=config.map_async,
            lock=True,
            readahead=False,
            meminit=False,
        )

        self._db_cache: dict[str, lmdb._Database] = {}
        self._exp_cache: dict[str, lmdb._Database] = {}
        self._expmeta_cache: dict[str, lmdb._Database] = {}
        self._meta_db: lmdb._Database = self._env.open_db(
            _META_DB_NAME, create=True)
        self._db_cache_lock = threading.Lock()
        self._namespace_lock = asyncio.Lock()
        self._resize_lock = threading.Lock()
        self._namespaces: set[str] = set()

        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_lock_handle = None
        self._closed = False

        logger.debug("[STORE] StoreService initialized (zLMDB)")

    async def start_cleanup(self) -> None:
        self._assert_open()
        if self._cleanup_task is not None:
            return
        lock_path = self._cleanup_lock_path()
        self._cleanup_lock_handle = try_acquire_file_lock(lock_path)
        if self._cleanup_lock_handle is None:
            logger.info("[STORE] Cleanup loop skipped (lock not acquired)")
            return
        self._cleanup_task = asyncio.create_task(
            cleanup_loop("store", self.cleanup_expired, self._cleanup_stop)
        )

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._cleanup_stop.set()
        if self._cleanup_task is not None:
            await self._cleanup_task
        release_file_lock(self._cleanup_lock_handle)
        self._cleanup_lock_handle = None
        try:
            self._env.sync()
        finally:
            self._env.close()
        self._closed = True
        logger.debug("[STORE] StoreService shutdown completed")

    async def set(
        self,
        namespace: str = "default",
        key: str = "",
        value: object = "",
        retention: Optional[int] = None,
    ) -> None:
        self._assert_open()
        if not key:
            raise ValueError("Store key must be non-empty")
        if retention is not None and retention < 1:
            raise ValueError("Retention must be a positive integer (minutes)")

        namespace_escaped = self._escape_namespace(namespace)
        key_bytes = self._escape_key(key)

        expire_ts = 0
        if retention is not None:
            expire_ts = int(time.time() + retention * 60)
            if expire_ts > _INT64_MAX:
                raise ValueError(
                    "Retention is too large; expiry timestamp exceeds int64")

        try:
            await self._register_namespace(namespace_escaped)

            await asyncio.to_thread(
                self._put,
                namespace_escaped,
                key_bytes,
                value,
                expire_ts,
            )
        except Exception:
            logger.exception("[STORE] Failed to set key",
                             namespace=namespace, key=key)
            raise

    async def get(self, namespace: str = "default", key: str = "") -> Optional[object]:
        self._assert_open()
        if not key:
            raise ValueError("Store key must be non-empty")

        try:
            namespace_escaped = self._escape_namespace(namespace)
            key_bytes = self._escape_key(key)
            await self._register_namespace(namespace_escaped)
            return await asyncio.to_thread(self._get, namespace_escaped, key_bytes)
        except Exception:
            logger.exception("[STORE] Failed to get key",
                             namespace=namespace, key=key)
            raise

    async def cleanup_expired(self, now: Optional[float] = None) -> None:
        self._assert_open()
        if now is None:
            now = time.time()

        async with self._namespace_lock:
            local_namespaces = list(self._namespaces)
        namespaces = await asyncio.to_thread(self._list_namespaces)
        if local_namespaces:
            for ns in local_namespaces:
                if ns not in namespaces:
                    namespaces.append(ns)

        if not namespaces:
            return

        try:
            await asyncio.to_thread(self._cleanup_env, namespaces, now)
        except Exception:
            logger.exception("[STORE] Cleanup expired records failed")
            raise

    async def _register_namespace(self, namespace: str) -> None:
        async with self._namespace_lock:
            if namespace in self._namespaces:
                return
            self._namespaces.add(namespace)

        self._ensure_db_capacity(additional=3)
        self._get_db(namespace)
        self._get_exp_db(namespace)
        self._get_expmeta_db(namespace)
        await asyncio.to_thread(self._register_namespace_in_meta, namespace)

    def _get_db(self, namespace: str) -> lmdb._Database:
        db = self._db_cache.get(namespace)
        if db is None:
            with self._db_cache_lock:
                db = self._db_cache.get(namespace)
                if db is None:
                    self._ensure_db_capacity()
                    db = self._open_db(namespace.encode("utf-8"), create=True)
                    self._db_cache[namespace] = db
        return db

    def _get_exp_db(self, namespace: str) -> lmdb._Database:
        db = self._exp_cache.get(namespace)
        if db is None:
            with self._db_cache_lock:
                db = self._exp_cache.get(namespace)
                if db is None:
                    self._ensure_db_capacity()
                    db = self._open_db(
                        f"__exp__{namespace}".encode("utf-8"),
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
                    self._ensure_db_capacity()
                    db = self._open_db(
                        f"__expmeta__{namespace}".encode("utf-8"),
                        create=True,
                    )
                    self._expmeta_cache[namespace] = db
        return db

    def _put(
        self,
        namespace: str,
        key_bytes: bytes,
        value: object,
        expire_ts: int,
    ) -> None:
        db = self._get_db(namespace)
        exp_db = self._get_exp_db(namespace)
        expmeta_db = self._get_expmeta_db(namespace)
        value_bytes = cbor2.dumps(value)
        if len(value_bytes) > self._config.max_value_bytes:
            raise ValueError(
                f"Store value exceeds {self._config.max_value_bytes} bytes")

        payload = _EXPIRY_STRUCT.pack(expire_ts) + value_bytes
        estimated = self._estimate_write_bytes(
            len(payload), len(key_bytes), expire_ts)

        def _write(txn) -> None:
            old_expire = 0
            old_expire_raw = txn.get(key_bytes, db=expmeta_db)
            if old_expire_raw is not None and len(old_expire_raw) >= _EXPIRY_STRUCT.size:
                old_expire = _EXPIRY_STRUCT.unpack_from(old_expire_raw, 0)[0]
            else:
                # Backward compatibility: fall back to payload header if expmeta missing.
                old_payload = txn.get(key_bytes, db=db)
                if old_payload is not None and len(old_payload) >= _EXPIRY_STRUCT.size:
                    old_expire = _EXPIRY_STRUCT.unpack_from(old_payload, 0)[0]

            if old_expire:
                old_bucket = _BUCKET_STRUCT.pack(old_expire // 60)
                txn.delete(old_bucket, key_bytes, db=exp_db)

            txn.put(key_bytes, payload, db=db)

            if expire_ts:
                bucket = _BUCKET_STRUCT.pack(expire_ts // 60)
                txn.put(bucket, key_bytes, db=exp_db)
                txn.put(key_bytes, _EXPIRY_STRUCT.pack(expire_ts), db=expmeta_db)
            else:
                txn.delete(key_bytes, db=expmeta_db)

        self._write_txn_with_resize(_write, estimated)

    def _get(self, namespace: str, key_bytes: bytes) -> Optional[object]:
        db = self._get_db(namespace)
        exp_db = self._get_exp_db(namespace)
        expmeta_db = self._get_expmeta_db(namespace)

        with self._env.begin(write=False, buffers=True) as txn:
            payload = txn.get(key_bytes, db=db)

        if payload is None:
            return None

        if len(payload) < _EXPIRY_STRUCT.size:
            logger.warning("[STORE] Corrupt payload header; deleting key")
            self._delete_key(db, exp_db, expmeta_db, key_bytes)
            return None

        expire_ts = _EXPIRY_STRUCT.unpack_from(payload, 0)[0]
        if expire_ts < 0:
            logger.warning("[STORE] Corrupt expiry timestamp; deleting key")
            self._delete_key(db, exp_db, expmeta_db, key_bytes)
            return None

        if expire_ts and expire_ts < int(time.time()):
            self._delete_key(db, exp_db, expmeta_db, key_bytes, expire_ts)
            return None

        try:
            return cbor2.loads(payload[_EXPIRY_STRUCT.size:])
        except Exception:
            logger.exception("[STORE] Corrupt payload; deleting key")
            self._delete_key(db, exp_db, expmeta_db, key_bytes, expire_ts)
            return None

    def _escape_namespace(self, namespace: str) -> str:
        if not namespace:
            raise ValueError("Namespace must be non-empty")
        if namespace.startswith(_RESERVED_NAMESPACE_PREFIXES):
            raise ValueError(
                f"Namespace cannot start with reserved prefix: {namespace}"
            )
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

    def _escape_key(self, key: str) -> bytes:
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

    def _cleanup_env(self, namespaces: list[str], now: float) -> None:
        now_bucket = int(now // 60)
        bucket_limit = _BUCKET_STRUCT.pack(now_bucket)
        deletes_left = self._config.cleanup_max_deletes

        db_pairs = [
            (
                namespace,
                self._get_db(namespace),
                self._get_exp_db(namespace),
                self._get_expmeta_db(namespace),
            )
            for namespace in namespaces
        ]

        def _cleanup(txn) -> None:
            nonlocal deletes_left
            for _, db, exp_db, expmeta_db in db_pairs:
                with txn.cursor(db=exp_db) as cursor:
                    if not cursor.first():
                        continue

                    while True:
                        bucket_key = cursor.key()
                        if bucket_key is None or bucket_key > bucket_limit:
                            break

                        dup_iter = cursor.iternext_dup()
                        dup_keys = list(itertools.islice(
                            dup_iter, deletes_left))
                        for key_bytes in dup_keys:
                            txn.delete(key_bytes, db=db)
                            txn.delete(bucket_key, key_bytes, db=exp_db)
                            txn.delete(key_bytes, db=expmeta_db)
                            deletes_left -= 1
                            if deletes_left <= 0:
                                return

                        if not cursor.next_nodup():
                            break

        self._write_txn_with_resize(_cleanup, 0)

    def _cleanup_lock_path(self) -> Path:
        if self._path.is_dir():
            return self._path / ".store_cleanup.lock"
        return self._path.with_suffix(".cleanup.lock")

    def _ensure_capacity(self, estimated_write_bytes: int) -> None:
        if estimated_write_bytes <= 0:
            return

        info = self._env.info()
        stat = self._env.stat()
        map_size = info["map_size"]
        used_bytes = (info["last_pgno"] + 1) * stat["psize"]
        projected = used_bytes + estimated_write_bytes
        if projected < map_size * self._config.map_high_watermark:
            return

        with self._resize_lock:
            info = self._env.info()
            stat = self._env.stat()
            map_size = info["map_size"]
            used_bytes = (info["last_pgno"] + 1) * stat["psize"]
            projected = used_bytes + estimated_write_bytes
            if projected < map_size * self._config.map_high_watermark:
                return

            new_size = self._calc_new_map_size(map_size, used_bytes, projected)
            logger.info(
                f"[STORE] LMDB map size increased, old_size: {map_size}, new_size: {new_size}, used_bytes: {used_bytes}."
            )
            self._env.set_mapsize(new_size)

    def _write_txn_with_resize(self, fn, estimated_write_bytes: int) -> None:
        attempt = 0
        while True:
            try:
                if estimated_write_bytes:
                    self._ensure_capacity(estimated_write_bytes)
                with self._env.begin(write=True) as txn:
                    return fn(txn)
            except Exception as exc:
                if _MapFullError is not None and isinstance(exc, _MapFullError):
                    attempt += 1
                    if attempt > 3:
                        logger.exception("[STORE] LMDB map full after retries")
                        raise
                    self._force_resize(estimated_write_bytes)
                    continue
                if _BadValsizeError is not None and isinstance(exc, _BadValsizeError):
                    raise ValueError(
                        "Value or key exceeds LMDB limits") from exc
                raise

    def _force_resize(self, estimated_write_bytes: int) -> None:
        if estimated_write_bytes <= 0:
            estimated_write_bytes = 1
        with self._resize_lock:
            info = self._env.info()
            stat = self._env.stat()
            map_size = info["map_size"]
            used_bytes = (info["last_pgno"] + 1) * stat["psize"]
            projected = used_bytes + estimated_write_bytes
            new_size = self._calc_new_map_size(
                map_size, used_bytes, projected, force=True)
            logger.warning(
                f"[STORE] LMDB map size forced to grow, old_size: {map_size}, new_size: {new_size}, used_bytes: {used_bytes}."
            )
            self._env.set_mapsize(new_size)

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

    def _estimate_write_bytes(
        self,
        payload_bytes: int,
        key_bytes: int,
        expire_ts: int,
    ) -> int:
        extra = 64  # rough overhead
        estimate = payload_bytes + key_bytes + extra
        if expire_ts:
            estimate += key_bytes + _BUCKET_STRUCT.size + extra
        return estimate

    def _delete_key(
        self,
        db: lmdb._Database,
        exp_db: lmdb._Database,
        expmeta_db: lmdb._Database,
        key_bytes: bytes,
        expire_ts: int = 0,
    ) -> None:
        def _delete(txn) -> None:
            txn.delete(key_bytes, db=db)
            if expire_ts:
                bucket = _BUCKET_STRUCT.pack(expire_ts // 60)
                txn.delete(bucket, key_bytes, db=exp_db)
            txn.delete(key_bytes, db=expmeta_db)

        self._write_txn_with_resize(_delete, 0)

    def _register_namespace_in_meta(self, namespace: str) -> None:
        meta_key = _META_NS_PREFIX + namespace.encode("utf-8")

        def _write(txn) -> None:
            txn.put(meta_key, b"1", db=self._meta_db, overwrite=False)

        self._write_txn_with_resize(_write, 0)

    def _list_namespaces(self) -> list[str]:
        namespaces: list[str] = []
        with self._env.begin(write=False) as txn:
            with txn.cursor(db=self._meta_db) as cursor:
                if cursor.set_range(_META_NS_PREFIX):
                    for key, _ in cursor:
                        if not key.startswith(_META_NS_PREFIX):
                            break
                        namespaces.append(
                            key[len(_META_NS_PREFIX):].decode("utf-8"))
        return namespaces

    def _open_db(self, name: bytes, **kwargs) -> lmdb._Database:
        try:
            return self._env.open_db(name, **kwargs)
        except Exception as exc:
            if _DbsFullError is not None and isinstance(exc, _DbsFullError):
                raise RuntimeError(
                    "LMDB max_dbs limit reached; increase store_lmdb.max_dbs."
                ) from exc
            raise

    def _ensure_db_capacity(self, additional: int = 1) -> None:
        if self._config.max_dbs < 3:
            raise RuntimeError("store_lmdb.max_dbs must be >= 3")
        current = 1 + len(self._db_cache) + len(self._exp_cache) + len(self._expmeta_cache)
        if current + additional > self._config.max_dbs:
            raise RuntimeError(
                "LMDB max_dbs limit reached; increase store_lmdb.max_dbs."
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
        if config.max_dbs < 3:
            raise ValueError("max_dbs must be >= 3")
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

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("StoreService is closed")
