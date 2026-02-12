from __future__ import annotations

import asyncio
import struct
import threading
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Optional
from uuid import uuid4

import cbor2
from loguru import logger

from ._callback_engine import decode_job_payload, encode_job_payload, extract_callback_name
from ._types import (
    ExpiryCallback,
    ExpiryCallbackDeferred,
    ExpiryCallbackEvent,
    _CallbackExecutionResult,
    _CallbackJob,
)

_EXPIRY_STRUCT = struct.Struct(">q")  # int64, epoch seconds; 0 means no expiry
_CALLBACK_REGISTRY_PREFIX = b"cb:"
_CALLBACK_POLL_INTERVAL_SECONDS = 1.0
_CALLBACK_MAX_RETRIES = 1
_CALLBACK_RETRY_BASE_SECONDS = 1
_CALLBACK_RETRY_MAX_SECONDS = 30

type _SerializedCallbackJob = tuple[bytes, bytes, str]


class StoreCallbackPipelineMixin:
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

                callback_result = _CallbackExecutionResult.RETRY
                self._callback_active.set()
                try:
                    callback_result = self._run_callback(job.event)
                finally:
                    self._callback_active.clear()

                if callback_result is _CallbackExecutionResult.COMPLETE:
                    self._complete_callback_job(
                        job.due_ts,
                        job.job_id,
                        job.event.callback,
                    )
                elif callback_result is _CallbackExecutionResult.DEFER:
                    self._defer_callback_job(job)
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

    def _run_callback(self, event: ExpiryCallbackEvent) -> _CallbackExecutionResult:
        with self._callback_registry_lock:
            registration = self._callback_registry.get(event.callback)
        if registration is None:
            self._log_callback_with_throttle(
                level="error",
                reason="missing_registration",
                message="[STORE] Expiry callback not registered; deferring",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
            )
            return _CallbackExecutionResult.DEFER
        callback = registration.fn
        timeout_seconds = registration.timeout_seconds
        future = self._callback_runner_executor.submit(
            self._execute_callback_in_runner,
            callback,
            event,
        )
        try:
            future.result(timeout=timeout_seconds)
            return _CallbackExecutionResult.COMPLETE
        except ExpiryCallbackDeferred:
            self._log_callback_with_throttle(
                level="info",
                reason="deferred",
                message="[STORE] Expiry callback deferred",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
            )
            return _CallbackExecutionResult.DEFER
        except FutureTimeoutError:
            future.cancel()
            logger.warning(
                "[STORE] Expiry callback timed out",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
                timeout_seconds=timeout_seconds,
            )
            return _CallbackExecutionResult.RETRY
        except Exception:
            logger.exception(
                "[STORE] Expiry callback failed",
                callback=event.callback,
                namespace=event.namespace,
                key=event.key,
            )
            return _CallbackExecutionResult.RETRY

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

    @staticmethod
    def _encode_callback_job_payload(event: ExpiryCallbackEvent, attempts: int) -> bytes:
        return encode_job_payload(
            namespace=event.namespace,
            key=event.key,
            value=event.value,
            expire_ts=event.expire_ts,
            callback=event.callback,
            attempts=attempts,
        )

    def _persist_callback_jobs(self, events: list[ExpiryCallbackEvent]) -> None:
        due_key, serialized_events, estimated = self._serialize_new_callback_jobs(events)

        def _write(txn) -> None:
            self._persist_serialized_callback_jobs_in_txn(
                txn,
                due_key,
                serialized_events,
            )

        self._write_meta_txn_with_resize(_write, estimated)

    def _serialize_new_callback_jobs(
        self,
        events: list[ExpiryCallbackEvent],
        *,
        due_ts: int | None = None,
    ) -> tuple[bytes, list[_SerializedCallbackJob], int]:
        if due_ts is None:
            due_ts = int(time.time())
        due_key = _EXPIRY_STRUCT.pack(due_ts)
        estimated = 0
        serialized_events: list[_SerializedCallbackJob] = []
        for event in events:
            job_id = uuid4().hex.encode("ascii")
            payload = self._encode_callback_job_payload(event, attempts=0)
            serialized_events.append((job_id, payload, event.callback))
            estimated += (
                len(job_id)
                + len(payload)
                + len(due_key)
                + len(event.callback.encode("utf-8"))
                + 192
            )
        return due_key, serialized_events, estimated

    def _persist_serialized_callback_jobs_in_txn(
        self,
        txn,
        due_key: bytes,
        serialized_events: list[_SerializedCallbackJob],
    ) -> None:
        for job_id, payload, callback_name in serialized_events:
            txn.put(job_id, payload, db=self._callback_job_db)
            txn.put(due_key, job_id, db=self._callback_schedule_db)
            self._adjust_callback_count_in_txn(txn, callback_name, 1)

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
                    callback_name = self._extract_callback_name_from_payload(payload)
                    cursor.delete()
                    txn.delete(job_id, db=self._callback_job_db)
                    if callback_name is not None:
                        self._adjust_callback_count_in_txn(txn, callback_name, -1)
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

    def _complete_callback_job(self, due_ts: int, job_id: bytes, callback_name: str) -> None:
        due_key = _EXPIRY_STRUCT.pack(due_ts)
        estimated = len(job_id) + len(due_key) + len(callback_name.encode("utf-8")) + 128

        def _write(txn) -> None:
            txn.delete(due_key, job_id, db=self._callback_schedule_db)
            txn.delete(job_id, db=self._callback_job_db)
            self._adjust_callback_count_in_txn(txn, callback_name, -1)

        self._write_meta_txn_with_resize(_write, estimated)

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

    def _defer_callback_job(self, job: _CallbackJob) -> None:
        next_due_ts = max(int(time.time()) + _CALLBACK_RETRY_BASE_SECONDS, job.due_ts + 1)
        old_due_key = _EXPIRY_STRUCT.pack(job.due_ts)
        next_due_key = _EXPIRY_STRUCT.pack(next_due_ts)
        estimated = len(job.job_id) + len(next_due_key) + 128

        def _write(txn) -> None:
            txn.delete(old_due_key, job.job_id, db=self._callback_schedule_db)
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
        estimated = (
            len(dlq_key)
            + len(payload)
            + len(job.job_id)
            + len(old_due_key)
            + len(job.event.callback.encode("utf-8"))
            + 160
        )

        def _write(txn) -> None:
            txn.delete(old_due_key, job.job_id, db=self._callback_schedule_db)
            txn.delete(job.job_id, db=self._callback_job_db)
            txn.put(dlq_key, payload, db=self._callback_dead_letter_db)
            self._adjust_callback_count_in_txn(txn, job.event.callback, -1)

        self._write_meta_txn_with_resize(_write, estimated)

    def _count_callback_jobs(self) -> int:
        with self._meta_env.begin(write=False) as txn:
            return txn.stat(db=self._callback_job_db)["entries"]

    def _count_callback_jobs_for(self, callback_name: str) -> int:
        if self._callback_count_index_needs_rebuild:
            self._rebuild_callback_count_index()
        callback_key = callback_name.encode("utf-8")
        with self._meta_env.begin(write=False) as txn:
            raw = txn.get(callback_key, db=self._callback_count_db)
            if raw is None or len(raw) < _EXPIRY_STRUCT.size:
                return 0
            try:
                count = _EXPIRY_STRUCT.unpack_from(raw, 0)[0]
            except Exception:
                logger.exception(
                    "[STORE] Invalid callback count index payload",
                    callback_name=callback_name,
                )
                return 0
            if count <= 0:
                return 0
            total_jobs = txn.stat(db=self._callback_job_db)["entries"]
            if count > total_jobs:
                return total_jobs
            return count

    def _count_dead_letter_callback_jobs(self) -> int:
        with self._meta_env.begin(write=False) as txn:
            return txn.stat(db=self._callback_dead_letter_db)["entries"]

    def _decode_callback_job(
        self, payload: bytes
    ) -> Optional[tuple[ExpiryCallbackEvent, int]]:
        try:
            data = decode_job_payload(payload)
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

    def _log_callback_with_throttle(
        self,
        *,
        level: str,
        reason: str,
        callback: str,
        namespace: str,
        key: str,
        message: str,
    ) -> None:
        suppressed = self._callback_log_throttler.poll(reason, callback)
        if suppressed is None:
            return
        log_fn = getattr(logger, level, logger.info)
        log_fn(
            message,
            callback=callback,
            namespace=namespace,
            key=key,
            suppressed_repeats=suppressed,
        )

    def _extract_callback_name_from_payload(self, payload: bytes) -> str | None:
        return extract_callback_name(payload)

    def _is_callback_count_index_stale(self) -> bool:
        with self._meta_env.begin(write=False) as txn:
            jobs = txn.stat(db=self._callback_job_db)["entries"]
            if jobs == 0:
                return False
            counts = txn.stat(db=self._callback_count_db)["entries"]
            if counts == 0:
                return True
            indexed_total = 0
            with txn.cursor(db=self._callback_count_db) as cursor:
                if cursor.first():
                    for _, raw in cursor:
                        if len(raw) < _EXPIRY_STRUCT.size:
                            continue
                        try:
                            value = _EXPIRY_STRUCT.unpack_from(raw, 0)[0]
                        except Exception:
                            continue
                        if value > 0:
                            indexed_total += value
            return indexed_total != jobs

    def _rebuild_callback_count_index(self) -> None:
        counts: dict[str, int] = {}
        with self._meta_env.begin(write=False) as txn, txn.cursor(db=self._callback_job_db) as cursor:
            if cursor.first():
                for _, payload in cursor:
                    decoded = self._decode_callback_job(payload)
                    if decoded is None:
                        callback_name = self._extract_callback_name_from_payload(payload)
                        if callback_name is None:
                            continue
                    else:
                        event, _attempts = decoded
                        callback_name = event.callback
                    counts[callback_name] = counts.get(callback_name, 0) + 1

        estimated = sum(
            len(name.encode("utf-8")) + _EXPIRY_STRUCT.size + 128 for name in counts
        )

        def _write(txn) -> None:
            existing_keys: list[bytes] = []
            with txn.cursor(db=self._callback_count_db) as cursor:
                if cursor.first():
                    for key, _ in cursor:
                        existing_keys.append(bytes(key))
            for key in existing_keys:
                txn.delete(key, db=self._callback_count_db)
            for callback_name, count in counts.items():
                txn.put(
                    callback_name.encode("utf-8"),
                    _EXPIRY_STRUCT.pack(count),
                    db=self._callback_count_db,
                )

        self._write_meta_txn_with_resize(_write, estimated)
        self._callback_count_index_needs_rebuild = False

    def _adjust_callback_count_in_txn(self, txn, callback_name: str, delta: int) -> None:
        if delta == 0:
            return
        key = callback_name.encode("utf-8")
        current = 0
        raw = txn.get(key, db=self._callback_count_db)
        if raw is not None and len(raw) >= _EXPIRY_STRUCT.size:
            try:
                current = _EXPIRY_STRUCT.unpack_from(raw, 0)[0]
            except Exception:
                current = 0
        next_count = current + delta
        if next_count <= 0:
            txn.delete(key, db=self._callback_count_db)
            return
        txn.put(key, _EXPIRY_STRUCT.pack(next_count), db=self._callback_count_db)

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
