from __future__ import annotations

import asyncio
import contextvars
import itertools
import struct
import threading
import time
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import quote, unquote

import cbor2
import zlmdb.lmdb as lmdb
from loguru import logger

from ._types import ExpiryCallbackEvent

_EXPIRY_STRUCT = struct.Struct(">q")  # int64, epoch seconds; 0 means no expiry
_BUCKET_STRUCT = struct.Struct(">q")  # int64, epoch minutes
_META_NS_PREFIX = b"ns:"
_META_NS_INTERNAL_PREFIX = b"nsi:"
_EXP_DB_PREFIX = b"__exp__:"
_EXPMETA_DB_PREFIX = b"__expmeta__:"
_CALLBACK_ENTRY_DB_PREFIX = b"__cbentry__:"
_T = TypeVar("_T")


class StoreStorageDataPathMixin:
    _namespace_lock: asyncio.Lock
    _namespaces: set[str]
    _internal_namespaces: set[str]
    _write_lock: asyncio.Lock
    _callback_guard: contextvars.ContextVar[bool]
    _config: Any
    _db_cache: dict[str, lmdb._Database]
    _db_cache_lock: threading.Lock
    _exp_cache: dict[str, lmdb._Database]
    _expmeta_cache: dict[str, lmdb._Database]
    _callback_entry_cache: dict[str, lmdb._Database]
    _meta_env: Any
    _env: Any
    _meta_db: lmdb._Database

    if TYPE_CHECKING:
        async def _run_in_executor(
            self,
            fn: Callable[..., _T],
            *args: object,
            **kwargs: object,
        ) -> _T:
            ...

        async def _run_on_runtime_thread(
            self,
            fn: Callable[..., Coroutine[object, object, _T]],
            *args: object,
            **kwargs: object,
        ) -> _T:
            ...

        def _is_runtime_thread(self) -> bool:
            ...

        def _write_meta_txn_with_resize(
            self,
            fn: Callable[[Any], None],
            estimated_write_bytes: int,
        ) -> None:
            ...

        def _write_data_txn_with_resize(
            self,
            fn: Callable[[Any], None],
            estimated_write_bytes: int,
        ) -> None:
            ...

        def _open_data_db(self, name: bytes, **kwargs: object) -> lmdb._Database:
            ...

        def _open_meta_db(self, name: bytes, **kwargs: object) -> lmdb._Database:
            ...

        def _decode_callback_name(self, raw: bytes) -> str | None:
            ...

        def _serialize_new_callback_jobs(
            self,
            events: list[ExpiryCallbackEvent],
            *,
            due_ts: int | None = None,
        ) -> tuple[bytes, list[tuple[bytes, bytes, str]], int]:
            ...

        def _persist_serialized_callback_jobs_in_txn(
            self,
            txn: Any,
            due_key: bytes,
            serialized_events: list[tuple[bytes, bytes, str]],
        ) -> None:
            ...

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
            if self._callback_guard.get():
                raise RuntimeError(
                    "Expiry callbacks cannot register new namespaces during callback execution"
                )
            async with self._write_lock:
                await self._run_in_executor(self._register_namespace_in_meta, namespace)
            self._get_db(namespace_escaped)
            self._namespaces.add(namespace)

    def _get_db(self, namespace: str) -> lmdb._Database:
        db = self._db_cache.get(namespace)
        if db is None:
            with self._db_cache_lock:
                db = self._db_cache.get(namespace)
                if db is None:
                    db = self._open_data_db(namespace.encode("utf-8"), create=True)
                    self._db_cache[namespace] = db
        return db

    def _write_value_and_metadata(
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
            raise ValueError(f"Store value exceeds {self._config.max_value_bytes} bytes")

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

    def _read_value_with_expiry_state(
        self, namespace: str, namespace_escaped: str, key_bytes: bytes
    ) -> tuple[object | None, ExpiryCallbackEvent | None, int | None]:
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
            raise ValueError(f"Namespace exceeds {self._config.max_namespace_bytes} bytes")
        escaped = quote(namespace, safe="")
        if len(escaped.encode("utf-8")) > self._config.max_namespace_bytes:
            raise ValueError(
                f"Escaped namespace exceeds {self._config.max_namespace_bytes} bytes"
            )
        return escaped

    def _encode_key(self, key: str) -> bytes:
        raw = key.encode("utf-8")
        if len(raw) > self._config.max_key_bytes:
            raise ValueError(f"Store key exceeds {self._config.max_key_bytes} bytes")
        escaped = quote(key, safe="")
        escaped_bytes = escaped.encode("utf-8")
        if len(escaped_bytes) > self._config.max_key_bytes:
            raise ValueError(f"Escaped key exceeds {self._config.max_key_bytes} bytes")
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

    def _cleanup_expired_entries_and_schedule_callbacks(
        self, namespaces: list[str], now: float
    ) -> int:
        now_bucket = int(now // 60)
        bucket_limit = _BUCKET_STRUCT.pack(now_bucket)
        deletes_left = self._config.cleanup_max_deletes
        expired_items: list[tuple[str, bytes, bytes, int, str | None]] = []
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
                    try:
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
                                dup_entries = list(itertools.islice(dup_iter, deletes_left))
                                for raw_key in dup_entries:
                                    key_bytes = bytes(raw_key)
                                    expmeta_ts = 0
                                    raw = meta_txn.get(key_bytes, db=expmeta_db)
                                    if raw is not None and len(raw) >= _EXPIRY_STRUCT.size:
                                        expmeta_ts = _EXPIRY_STRUCT.unpack_from(raw, 0)[0]

                                    if expmeta_ts and expmeta_ts > now:
                                        expire_ts = expmeta_ts
                                    else:
                                        payload_ts = 0
                                        data_txn, data_db = _resolve_data(namespace)
                                        payload = data_txn.get(key_bytes, db=data_db)
                                        if payload is not None and len(payload) >= _EXPIRY_STRUCT.size:
                                            payload_ts = _EXPIRY_STRUCT.unpack_from(payload, 0)[0]
                                        if expmeta_ts and payload_ts:
                                            expire_ts = max(expmeta_ts, payload_ts)
                                        else:
                                            expire_ts = expmeta_ts or payload_ts
                                    if expire_ts == 0 or expire_ts > now:
                                        stale_exp_index_entries.append(
                                            (namespace, bucket_key_bytes, key_bytes)
                                        )
                                        continue

                                    callback_raw = meta_txn.get(key_bytes, db=callback_db)
                                    callback_name = None
                                    if callback_raw:
                                        callback_name = self._decode_callback_name(callback_raw)

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
                    except Exception:
                        logger.exception(
                            "[STORE] Failed to cleanup namespace during iteration",
                            namespace=namespace,
                        )
                        # Continue to next namespace instead of failing entire cleanup
                        continue

            callback_events: list[ExpiryCallbackEvent] = []
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
                        callback_events.append(event)
        finally:
            for txn in data_txns.values():
                try:
                    txn.abort()
                except Exception:
                    logger.exception(
                        "[STORE] Failed to abort read transaction during cleanup"
                    )

        if expired_items:
            def _delete_data(txn) -> None:
                for namespace, key_bytes, _, _, _ in expired_items:
                    db = data_dbs.get(namespace)
                    if db is None:
                        continue
                    txn.delete(key_bytes, db=db)

            self._write_data_txn_with_resize(_delete_data, 0)

        due_key: bytes | None = None
        serialized_callback_jobs: list[tuple[bytes, bytes, str]] = []
        callback_estimated = 0
        if callback_events:
            due_key, serialized_callback_jobs, callback_estimated = self._serialize_new_callback_jobs(
                callback_events
            )

        if stale_exp_index_entries or expired_items or serialized_callback_jobs:
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

                if due_key is not None:
                    self._persist_serialized_callback_jobs_in_txn(
                        txn,
                        due_key,
                        serialized_callback_jobs,
                    )

            self._write_meta_txn_with_resize(_delete_meta, callback_estimated)

        return len(serialized_callback_jobs)

    def _delete_entry_and_schedule_callback(
        self,
        namespace: str,
        namespace_escaped: str,
        key_bytes: bytes,
        expire_ts: int = 0,
        callback_event: ExpiryCallbackEvent | None = None,
    ) -> int:
        db = self._get_db(namespace_escaped)
        exp_db = self._get_exp_db(namespace)
        expmeta_db = self._get_expmeta_db(namespace)
        callback_entry_db = self._get_callback_entry_db(namespace)
        due_key: bytes | None = None
        serialized_callback_jobs: list[tuple[bytes, bytes, str]] = []
        callback_estimated = 0
        if callback_event is not None:
            due_key, serialized_callback_jobs, callback_estimated = self._serialize_new_callback_jobs(
                [callback_event]
            )

        def _delete_data(txn) -> None:
            txn.delete(key_bytes, db=db)

        self._write_data_txn_with_resize(_delete_data, 0)

        def _delete_meta(txn) -> None:
            if expire_ts:
                bucket = _BUCKET_STRUCT.pack(expire_ts // 60)
                txn.delete(bucket, key_bytes, db=exp_db)
            txn.delete(key_bytes, db=expmeta_db)
            txn.delete(key_bytes, db=callback_entry_db)
            if due_key is not None:
                self._persist_serialized_callback_jobs_in_txn(
                    txn,
                    due_key,
                    serialized_callback_jobs,
                )

        self._write_meta_txn_with_resize(_delete_meta, callback_estimated)
        return len(serialized_callback_jobs)

    def _register_namespace_in_meta(self, namespace: str) -> None:
        namespace_raw = namespace.encode("utf-8")
        meta_key = _META_NS_PREFIX + namespace_raw
        namespace_internal_key = _META_NS_INTERNAL_PREFIX + namespace_raw
        namespace_is_internal = namespace in self._internal_namespaces

        def _write(txn) -> None:
            exists = txn.get(meta_key, db=self._meta_db) is not None
            if not exists and self._config.max_dbs != 0 and not namespace_is_internal:
                current_user_namespaces = 0
                with txn.cursor(db=self._meta_db) as cursor:
                    if cursor.set_range(_META_NS_PREFIX):
                        for key, _ in cursor:
                            if not key.startswith(_META_NS_PREFIX):
                                break
                            existing_raw = bytes(key[len(_META_NS_PREFIX):])
                            internal_key = _META_NS_INTERNAL_PREFIX + existing_raw
                            if txn.get(internal_key, db=self._meta_db):
                                continue
                            current_user_namespaces += 1
                if current_user_namespaces + 1 > self._config.max_dbs:
                    raise RuntimeError(
                        "LMDB namespace quota reached; increase store_lmdb.max_dbs or set 0 to disable the quota."
                    )

            if not exists:
                txn.put(meta_key, b"1", db=self._meta_db, overwrite=False)
            if namespace_is_internal:
                txn.put(namespace_internal_key, b"1", db=self._meta_db, overwrite=True)

        self._write_meta_txn_with_resize(_write, 0)

    def _mark_namespace_internal_in_meta(self, namespace: str) -> None:
        namespace_raw = namespace.encode("utf-8")
        meta_key = _META_NS_PREFIX + namespace_raw
        internal_key = _META_NS_INTERNAL_PREFIX + namespace_raw

        def _write(txn) -> None:
            if txn.get(meta_key, db=self._meta_db) is None:
                return
            txn.put(internal_key, b"1", db=self._meta_db, overwrite=True)

        self._write_meta_txn_with_resize(_write, 0)

    def _list_namespace_metadata(self) -> tuple[list[str], set[str]]:
        namespaces: list[str] = []
        internal_namespaces: set[str] = set()
        with self._meta_env.begin(write=False) as txn, txn.cursor(db=self._meta_db) as cursor:
            if cursor.set_range(_META_NS_PREFIX):
                for key, _ in cursor:
                    if not key.startswith(_META_NS_PREFIX):
                        break
                    raw_namespace = bytes(key[len(_META_NS_PREFIX):])
                    namespace = raw_namespace.decode("utf-8")
                    namespaces.append(namespace)
                    if txn.get(_META_NS_INTERNAL_PREFIX + raw_namespace, db=self._meta_db):
                        internal_namespaces.add(namespace)
        return namespaces, internal_namespaces

    def _list_namespaces(self) -> list[str]:
        namespaces, _ = self._list_namespace_metadata()
        return namespaces

    def _build_expiry_event(
        self,
        namespace: str,
        key_bytes: bytes,
        payload,
        expire_ts: int,
        callback_name: str,
    ) -> ExpiryCallbackEvent | None:
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
