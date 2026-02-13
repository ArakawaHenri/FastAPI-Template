from __future__ import annotations

import threading
import time

import cbor2


class CallbackLogThrottler:
    def __init__(self, window_seconds: float) -> None:
        self._window_seconds = window_seconds
        self._lock = threading.Lock()
        self._state: dict[tuple[str, str], tuple[float, int]] = {}

    def poll(self, reason: str, callback: str) -> int | None:
        now = time.monotonic()
        bucket = (reason, callback)
        with self._lock:
            next_emit, suppressed_count = self._state.get(bucket, (0.0, 0))
            if now >= next_emit:
                self._state[bucket] = (now + self._window_seconds, 0)
                return suppressed_count
            self._state[bucket] = (next_emit, suppressed_count + 1)
            return None


def encode_job_payload(
    *,
    namespace: str,
    key: str,
    value: object,
    expire_ts: int,
    callback: str,
    attempts: int,
) -> bytes:
    return cbor2.dumps(
        {
            "namespace": namespace,
            "key": key,
            "value": value,
            "expire_ts": expire_ts,
            "callback": callback,
            "attempts": attempts,
        }
    )


def decode_job_payload(payload: bytes) -> dict[str, object]:
    return cbor2.loads(payload)


def extract_callback_name(payload: bytes) -> str | None:
    try:
        data = decode_job_payload(payload)
    except Exception:
        return None
    callback = data.get("callback")
    if not isinstance(callback, str):
        return None
    callback_name = callback.strip()
    if callback_name == "":
        return None
    return callback_name
