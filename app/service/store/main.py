from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

import cbor2
import numpy as np
import zlmdb
from fastapiex.di import BaseService, Service
from fastapiex.settings import BaseSettings, GetSettings, Settings

from app.service.shared.cleanup import cleanup_loop, release_file_lock, try_acquire_file_lock

logger = logging.getLogger(__name__)

_VALUE_SLOT = 100
_EXPIRY_SLOT = 101
_INT64_MAX = 2**63 - 1
_MIN_EXPIRY_DT = np.datetime64(0, "ns")
_MAX_STRING = "\uffff"


@Settings("store")
class StoreSettings(BaseSettings):
    path: str = "./data/store.zlmdb"
    max_size_mb: int = 1024
    sync: bool = False
    writemap: bool = False
    max_key_bytes: int = 1024
    max_namespace_bytes: int = 256
    max_value_bytes: int = 100 * 1024 * 1024
    cleanup_interval_seconds: int = 60
    cleanup_max_deletes: int = 100_000


@dataclass(frozen=True)
class StoreConfig:
    path: str = "./data/store.zlmdb"
    max_size_mb: int = 1024
    sync: bool = False
    writemap: bool = False
    max_key_bytes: int = 1024
    max_namespace_bytes: int = 256
    max_value_bytes: int = 100 * 1024 * 1024
    cleanup_interval_seconds: int = 60
    cleanup_max_deletes: int = 100_000

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("store.path must be non-empty")
        if self.max_size_mb < 1:
            raise ValueError("store.max_size_mb must be >= 1")
        if self.max_key_bytes < 1:
            raise ValueError("store.max_key_bytes must be >= 1")
        if self.max_namespace_bytes < 1:
            raise ValueError("store.max_namespace_bytes must be >= 1")
        if self.max_value_bytes < 1:
            raise ValueError("store.max_value_bytes must be >= 1")
        if self.cleanup_interval_seconds < 1:
            raise ValueError("store.cleanup_interval_seconds must be >= 1")
        if self.cleanup_max_deletes < 1:
            raise ValueError("store.cleanup_max_deletes must be >= 1")


@Service("store_service", eager=True)
class StoreService(BaseService):
    """zlmdb-backed key-value store with namespace isolation and TTL support."""

    @classmethod
    async def create(cls) -> StoreService:
        cfg: StoreSettings = GetSettings(StoreSettings)
        service = cls(
            StoreConfig(
                path=cfg.path,
                max_size_mb=cfg.max_size_mb,
                sync=cfg.sync,
                writemap=cfg.writemap,
                max_key_bytes=cfg.max_key_bytes,
                max_namespace_bytes=cfg.max_namespace_bytes,
                max_value_bytes=cfg.max_value_bytes,
                cleanup_interval_seconds=cfg.cleanup_interval_seconds,
                cleanup_max_deletes=cfg.cleanup_max_deletes,
            )
        )
        await service.start_cleanup()
        return service

    @classmethod
    async def destroy(cls, instance: StoreService) -> None:
        await instance.shutdown()

    def __init__(self, config: StoreConfig | None = None) -> None:
        self._config = config or StoreConfig()
        self._path = Path(self._config.path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._db = zlmdb.Database(
            str(self._path),
            maxsize=self._config.max_size_mb * 1024 * 1024,
            sync=self._config.sync,
            writemap=self._config.writemap,
        )
        self._values = zlmdb.MapStringCbor(
            _VALUE_SLOT,
            marshal=lambda value: value,
            unmarshal=lambda value: value,
        )
        self._expiry = zlmdb.MapTimestampStringCbor(
            _EXPIRY_SLOT,
            marshal=lambda value: value,
            unmarshal=lambda value: value,
        )
        self._values.attach_index(
            "expires_at",
            self._expiry,
            self._expiry_index_key,
            nullable=True,
        )
        self._cleanup_stop = asyncio.Event()
        self._cleanup_task: asyncio.Task[object] | None = None
        self._cleanup_lock_handle: TextIO | None = None
        self._closed = False
        logger.debug("StoreService initialized path=%s", self._path)

    @property
    def config(self) -> StoreConfig:
        return self._config

    async def start_cleanup(self) -> None:
        self._assert_open()
        if self._cleanup_task is not None:
            return
        self._cleanup_lock_handle = try_acquire_file_lock(self._path / ".store_cleanup.lock")
        if self._cleanup_lock_handle is None:
            logger.info("StoreService cleanup loop skipped; lock is held")
            return
        self._cleanup_task = asyncio.create_task(
            cleanup_loop(
                "store",
                self.cleanup_expired,
                self._cleanup_stop,
                interval_seconds=self._config.cleanup_interval_seconds,
            ),
            name="store-cleanup",
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
            await asyncio.to_thread(self._db.sync, True)
        finally:
            self._db.__exit__(None, None, None)
        self._closed = True
        logger.debug("StoreService shutdown completed")

    async def set(
        self,
        namespace: str,
        key: str,
        value: object,
        retention_seconds: int | None = None,
    ) -> None:
        expires_at = self._expiry_ts(retention_seconds)
        data_key = self._data_key(namespace, key)
        self._validate_value_size(value)
        await asyncio.to_thread(self._put_sync, data_key, value, expires_at, False)

    async def set_if_absent(
        self,
        namespace: str,
        key: str,
        value: object,
        retention_seconds: int | None = None,
    ) -> bool:
        expires_at = self._expiry_ts(retention_seconds)
        data_key = self._data_key(namespace, key)
        self._validate_value_size(value)
        return await asyncio.to_thread(self._put_sync, data_key, value, expires_at, True)

    async def get(self, namespace: str, key: str) -> object | None:
        data_key = self._data_key(namespace, key)
        return await asyncio.to_thread(self._get_sync, data_key)

    async def delete(self, namespace: str, key: str) -> None:
        data_key = self._data_key(namespace, key)
        await asyncio.to_thread(self._delete_sync, data_key)

    async def cleanup_expired(self, now: float | None = None) -> int:
        self._assert_open()
        return await asyncio.to_thread(self._cleanup_expired_sync, int(now or time.time()))

    async def sync(self, force: bool = True) -> None:
        self._assert_open()
        await asyncio.to_thread(self._db.sync, force)

    async def count(self, namespace: str | None = None) -> int:
        self._assert_open()
        prefix = None if namespace is None else f"{self._validate_namespace(namespace)}\0"
        return await asyncio.to_thread(self._count_sync, prefix)

    async def stats(self, include_slots: bool = True) -> dict[str, Any]:
        self._assert_open()
        return await asyncio.to_thread(self._stats_sync, include_slots)

    def _put_sync(
        self,
        data_key: str,
        value: object,
        expires_at: int,
        only_if_absent: bool,
    ) -> bool:
        now = int(time.time())
        with self._db.begin(write=True) as txn:
            old_record = self._values[txn, data_key]
            old_expires_at = self._record_expires_at(old_record)
            if only_if_absent and isinstance(old_record, dict):
                if not old_expires_at or old_expires_at > now:
                    return False

            self._values[txn, data_key] = {
                "data_key": data_key,
                "expires_at": expires_at,
                "value": value,
            }
        return True

    def _get_sync(self, data_key: str) -> object | None:
        self._assert_open()
        with self._db.begin() as txn:
            record = self._values[txn, data_key]
        if not isinstance(record, dict):
            return None
        expires_at = self._record_expires_at(record)
        if expires_at and expires_at <= int(time.time()):
            return None
        return record.get("value")

    def _delete_sync(self, data_key: str) -> None:
        self._assert_open()
        with self._db.begin(write=True) as txn:
            del self._values[txn, data_key]

    def _cleanup_expired_sync(self, now: int) -> int:
        to_delete: list[tuple[tuple[np.datetime64, str], str]] = []
        with self._db.begin() as txn:
            for expiry_key, _marker in self._expiry.select(
                txn,
                from_key=(_MIN_EXPIRY_DT, ""),
                to_key=(self._expiry_dt(now), _MAX_STRING),
                limit=self._config.cleanup_max_deletes,
            ):
                _expires_at_dt, data_key = expiry_key
                to_delete.append((expiry_key, data_key))

        if not to_delete:
            return 0

        deleted = 0
        with self._db.begin(write=True) as txn:
            for expiry_key, data_key in to_delete:
                record = self._values[txn, data_key]
                expires_at = self._record_expires_at(record)
                if isinstance(record, dict) and expires_at and expires_at <= now:
                    del self._values[txn, data_key]
                    deleted += 1
                else:
                    del self._expiry[txn, expiry_key]
        return deleted

    def _count_sync(self, prefix: str | None) -> int:
        with self._db.begin() as txn:
            return self._values.count(txn, prefix=prefix)

    def _stats_sync(self, include_slots: bool) -> dict[str, Any]:
        stats = self._db.stats(False)
        if include_slots:
            with self._db.begin() as txn:
                slots = [
                    {
                        "slot": _VALUE_SLOT,
                        "name": "store.values",
                        "records": self._values.count(txn),
                    },
                    {
                        "slot": _EXPIRY_SLOT,
                        "name": "store.expiry",
                        "records": self._expiry.count(txn),
                    },
                ]
            stats["num_slots"] = len(slots)
            stats["slots"] = slots
        return stats

    def _data_key(self, namespace: str, key: str) -> str:
        self._assert_open()
        namespace = self._validate_namespace(namespace)
        key = self._validate_key(key)
        return f"{namespace}\0{key}"

    def _validate_namespace(self, namespace: str) -> str:
        if not namespace or not namespace.strip():
            raise ValueError("Store namespace must be non-empty")
        namespace_bytes = namespace.encode("utf-8")
        if b"\x00" in namespace_bytes:
            raise ValueError("Store namespace cannot contain null bytes")
        if len(namespace_bytes) > self._config.max_namespace_bytes:
            raise ValueError("Store namespace exceeds max_namespace_bytes")
        return namespace

    def _validate_key(self, key: str) -> str:
        if not key or not key.strip():
            raise ValueError("Store key must be non-empty")
        key_bytes = key.encode("utf-8")
        if b"\x00" in key_bytes:
            raise ValueError("Store key cannot contain null bytes")
        if len(key_bytes) > self._config.max_key_bytes:
            raise ValueError("Store key exceeds max_key_bytes")
        return key

    def _validate_value_size(self, value: object) -> None:
        if len(cbor2.dumps(value)) > self._config.max_value_bytes:
            raise ValueError("Store value exceeds max_value_bytes")

    @staticmethod
    def _expiry_ts(retention_seconds: int | None) -> int:
        if retention_seconds is None:
            return 0
        if retention_seconds < 1:
            raise ValueError("retention_seconds must be >= 1")
        expires_at = int(time.time() + retention_seconds)
        if expires_at > _INT64_MAX:
            raise ValueError("retention_seconds too large; expiry overflows int64")
        return expires_at

    @staticmethod
    def _record_expires_at(record: Any) -> int:
        if not isinstance(record, dict):
            return 0
        try:
            return int(record.get("expires_at") or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _expiry_index_key(record: Any) -> tuple[np.datetime64 | None, str | None]:
        expires_at = StoreService._record_expires_at(record)
        if not expires_at or not isinstance(record, dict):
            return None, None
        data_key = record.get("data_key")
        if not isinstance(data_key, str):
            return None, None
        return StoreService._expiry_dt(expires_at), data_key

    @staticmethod
    def _expiry_dt(expires_at: int) -> np.datetime64:
        return np.datetime64(expires_at, "s").astype("datetime64[ns]")

    def _assert_open(self) -> None:
        if self._closed:
            raise RuntimeError("StoreService is closed")
