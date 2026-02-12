from __future__ import annotations

import asyncio
import os
import stat
import threading
import time
import traceback
from multiprocessing import get_context
from pathlib import Path

import pytest

import app.services.store.main as store_main
from app.services.store.main import ExpiryCallbackDeferred, StoreConfig, StoreService


def _make_store(
    tmp_path: Path,
    cleanup_max_deletes: int = 10_000,
    max_dbs: int = 128,
) -> StoreService:
    return StoreService(
        StoreConfig(
            path=str(tmp_path / "store_lmdb"),
            map_size_mb=16,
            map_size_growth_factor=2,
            map_high_watermark=0.9,
            max_dbs=max_dbs,
            max_readers=256,
            sync=False,
            metasync=False,
            writemap=True,
            map_async=True,
            max_key_bytes=256,
            max_namespace_bytes=256,
            max_value_bytes=2 * 1024 * 1024,
            cleanup_max_deletes=cleanup_max_deletes,
            worker_threads=4,
        )
    )


@pytest.mark.asyncio
async def test_store_lifespan_ctor_reuses_shared_instance(tmp_path):
    path = str(tmp_path / "store_lmdb")
    store1 = await StoreService.LifespanTasks.ctor(path=path, map_size_mb=16)
    store2 = await StoreService.LifespanTasks.ctor(path=path, map_size_mb=16)

    assert store1 is store2
    assert not store1._closed

    await StoreService.LifespanTasks.dtor(store1)
    assert not store1._closed

    await StoreService.LifespanTasks.dtor(store2)
    assert store1._closed


@pytest.mark.asyncio
async def test_store_lifespan_ctor_rejects_shared_config_mismatch(tmp_path):
    path = str(tmp_path / "store_lmdb")
    store = await StoreService.LifespanTasks.ctor(path=path, map_size_mb=16)
    try:
        with pytest.raises(RuntimeError, match="config mismatch"):
            await StoreService.LifespanTasks.ctor(path=path, map_size_mb=32)
    finally:
        await StoreService.LifespanTasks.dtor(store)


@pytest.mark.asyncio
async def test_store_lifespan_dtor_closes_non_shared_instance(tmp_path):
    store = _make_store(tmp_path)
    await StoreService.LifespanTasks.dtor(store)
    assert store._closed


@pytest.mark.asyncio
async def test_store_lifespan_dtor_closes_instance_when_shared_registry_is_missing(tmp_path):
    path = str(tmp_path / "store_lmdb")
    store = await StoreService.LifespanTasks.ctor(path=path, map_size_mb=16)
    key = store._shared_instance_key

    with StoreService._shared_instances_lock:
        StoreService._shared_instances.pop(key, None)

    await StoreService.LifespanTasks.dtor(store)
    assert store._closed


@pytest.mark.asyncio
async def test_store_acquire_shared_waits_for_inflight_shutdown(tmp_path):
    path = str(tmp_path / "store_lmdb")
    store = await StoreService.LifespanTasks.ctor(path=path, map_size_mb=16)

    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()
    original_shutdown = store.shutdown

    async def delayed_shutdown() -> None:
        shutdown_started.set()
        await release_shutdown.wait()
        await original_shutdown()

    store.shutdown = delayed_shutdown  # type: ignore[method-assign]

    release_task = asyncio.create_task(StoreService.release_shared(store))
    await asyncio.wait_for(shutdown_started.wait(), timeout=2)

    acquire_task = asyncio.create_task(StoreService.acquire_shared(store._config))
    await asyncio.sleep(0.05)
    assert not acquire_task.done()

    release_shutdown.set()
    await release_task
    replacement = await asyncio.wait_for(acquire_task, timeout=3)

    assert replacement is not store
    assert store._closed

    await StoreService.release_shared(replacement)


def _worker_main(db_path: str, worker_id: int, start_event, error_queue):
    async def _run():
        store = StoreService(
            StoreConfig(
                path=str(Path(db_path)),
                map_size_mb=16,
                map_size_growth_factor=2,
                map_high_watermark=0.9,
                max_dbs=128,
                max_readers=256,
                sync=False,
                metasync=False,
                writemap=True,
                map_async=True,
                max_key_bytes=256,
                max_namespace_bytes=256,
                max_value_bytes=2 * 1024 * 1024,
                cleanup_max_deletes=10_000,
                worker_threads=4,
            )
        )
        await store._register_namespace("shared", store._encode_namespace("shared"))

        start_event.wait()

        for i in range(50):
            key = f"w{worker_id}_k{i}"
            await store.set("shared", key, {"v": i}, retention=10)
            value = await store.get("shared", key)
            if value != {"v": i}:
                raise AssertionError(f"value mismatch for {key}")

        await store.shutdown()

    try:
        asyncio.run(_run())
    except Exception:
        error_queue.put(traceback.format_exc())


@pytest.mark.asyncio
async def test_store_roundtrip_types(tmp_path):
    store = _make_store(tmp_path)
    await store.set("default", "k1", {"a": 1, "b": [1, 2, 3]}, retention=10)
    await store.set("default", "k2", ["x", 1, True, None], retention=10)
    await store.set("default", "k3", "hello", retention=10)

    assert await store.get("default", "k1") == {"a": 1, "b": [1, 2, 3]}
    assert await store.get("default", "k2") == ["x", 1, True, None]
    assert await store.get("default", "k3") == "hello"

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiration_and_cleanup(tmp_path):
    store = _make_store(tmp_path)
    await store.set("default", "k1", "v1", retention=1)
    await store.set("default", "k2", "v2", retention=1)

    assert await store.get("default", "k1") == "v1"

    await store.cleanup_expired(now=time.time() + 120)

    assert await store.get("default", "k1") is None
    assert await store.get("default", "k2") is None

    await store.shutdown()


@pytest.mark.asyncio
async def test_get_missing_namespace_returns_none_without_namespace_creation(tmp_path):
    store = _make_store(tmp_path, max_dbs=1)
    assert await store.get("missing_ns", "missing_key") is None

    assert "missing_ns" not in store._list_namespaces()

    await store.set("real_ns", "k", "v", retention=10)
    assert await store.get("real_ns", "k") == "v"

    await store.shutdown()


@pytest.mark.asyncio
async def test_get_missing_key_returns_none(tmp_path):
    store = _make_store(tmp_path)
    await store.set("default", "exists", "v", retention=10)

    assert await store.get("default", "missing") is None
    await store.shutdown()


@pytest.mark.asyncio
async def test_get_many_returns_values_and_none_for_missing(tmp_path):
    store = _make_store(tmp_path)
    await store.set("default", "k1", "v1", retention=10)
    await store.set("default", "k2", {"ok": True}, retention=10)

    values = await store.get_many("default", ["k1", "k2", "missing"])

    assert values == {"k1": "v1", "k2": {"ok": True}, "missing": None}
    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_callback(tmp_path):
    store = _make_store(tmp_path)
    events: list[tuple[str, str, object | None, int]] = []
    callback_ran = threading.Event()
    write_blocked = threading.Event()

    async def on_expire(event):
        events.append((event.namespace, event.key, event.value, event.expire_ts))
        try:
            await store.set("default", "blocked", "v", retention=1)
        except RuntimeError:
            write_blocked.set()
        callback_ran.set()

    await store.register_builtin_callback("on_expire")
    await store.register_expiry_callback("on_expire", on_expire)
    await store.set("default", "k1", "v1", retention=1, on_expire="on_expire")

    await store.cleanup_expired(now=time.time() + 120)
    await asyncio.wait_for(asyncio.to_thread(callback_ran.wait), timeout=2)
    await store.wait_for_callbacks()

    assert write_blocked.is_set()
    assert events
    ns, key, value, expire_ts = events[0]
    assert ns == "default"
    assert key == "k1"
    assert value == "v1"
    assert expire_ts > 0

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_callback_retries_then_succeeds(tmp_path):
    store = _make_store(tmp_path)
    attempts = 0
    callback_done = threading.Event()

    async def flaky(event):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient callback failure")
        assert event.key == "k1"
        callback_done.set()

    await store.register_builtin_callback("flaky")
    await store.register_expiry_callback("flaky", flaky)
    await store.set("default", "k1", "v1", retention=1, on_expire="flaky")

    await store.cleanup_expired(now=time.time() + 120)
    await asyncio.wait_for(asyncio.to_thread(callback_done.wait), timeout=5)
    await asyncio.wait_for(store.wait_for_callbacks(), timeout=5)

    assert attempts == 2
    assert store._count_callback_jobs() == 0
    assert store._count_dead_letter_callback_jobs() == 0

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_callback_timeout_retries_then_dead_letters(tmp_path):
    store = _make_store(tmp_path)
    attempts = 0

    async def slow_callback(_event):
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.2)

    await store.register_builtin_callback("slow")
    await store.register_expiry_callback("slow", slow_callback, timeout_seconds=0.05)
    await store.set("default", "k1", "v1", retention=1, on_expire="slow")

    await store.cleanup_expired(now=time.time() + 120)
    drained = await store.wait_for_callbacks(timeout=5)

    assert drained is True
    assert attempts == 2
    assert store._count_callback_jobs() == 0
    assert store._count_callback_jobs_for("slow") == 0
    assert store._count_dead_letter_callback_jobs() == 1

    await store.shutdown()


@pytest.mark.asyncio
async def test_register_expiry_callback_rejects_non_positive_timeout(tmp_path):
    store = _make_store(tmp_path)

    async def callback(_event):
        return None

    await store.register_builtin_callback("invalid_timeout")
    with pytest.raises(ValueError, match="timeout_seconds must be > 0"):
        await store.register_expiry_callback(
            "invalid_timeout",
            callback,
            timeout_seconds=0,
        )

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_callback_moves_to_dead_letter_after_retry_limit(tmp_path):
    store = _make_store(tmp_path)
    attempts = 0

    async def always_fail(_event):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("permanent callback failure")

    await store.register_builtin_callback("always_fail")
    await store.register_expiry_callback("always_fail", always_fail)
    await store.set("default", "k1", "v1", retention=1, on_expire="always_fail")

    await store.cleanup_expired(now=time.time() + 120)
    await asyncio.wait_for(store.wait_for_callbacks(), timeout=5)

    assert attempts == 2
    assert store._count_callback_jobs() == 0
    assert store._count_callback_jobs_for("always_fail") == 0
    assert store._count_dead_letter_callback_jobs() == 1

    await store.shutdown()


@pytest.mark.asyncio
async def test_wait_for_callbacks_returns_false_when_worker_unavailable(tmp_path):
    store = _make_store(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    async def blocked_callback(_event):
        return None

    await store.register_builtin_callback("blocked")
    await store.register_expiry_callback("blocked", blocked_callback)

    await store.set("default", "k1", "v1", retention=1, on_expire="blocked")
    await store.cleanup_expired(now=time.time() + 120)

    drained = await store.wait_for_callbacks(timeout=1, raise_on_incomplete=False)
    assert drained is False
    assert store._count_callback_jobs() > 0

    StoreService._release_file_lock(callback_lock)
    await store.shutdown()


@pytest.mark.asyncio
async def test_wait_for_callbacks_raises_when_worker_unavailable_by_default(tmp_path):
    store = _make_store(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    async def blocked_callback(_event):
        return None

    await store.register_builtin_callback("blocked")
    await store.register_expiry_callback("blocked", blocked_callback)
    await store.set("default", "k1", "v1", retention=1, on_expire="blocked")
    await store.cleanup_expired(now=time.time() + 120)

    with pytest.raises(TimeoutError, match="worker_unavailable"):
        await store.wait_for_callbacks(timeout=1)

    StoreService._release_file_lock(callback_lock)
    await store.shutdown()


@pytest.mark.asyncio
async def test_shutdown_is_bounded_when_callback_worker_unavailable(tmp_path):
    store = _make_store(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    async def blocked_callback(_event):
        return None

    await store.register_builtin_callback("blocked")
    await store.register_expiry_callback("blocked", blocked_callback)
    await store.set("default", "k1", "v1", retention=1, on_expire="blocked")
    await store.cleanup_expired(now=time.time() + 120)

    started = time.monotonic()
    await store.shutdown()
    elapsed = time.monotonic() - started
    assert elapsed < 1.5

    StoreService._release_file_lock(callback_lock)


@pytest.mark.asyncio
async def test_callback_execution_does_not_hold_global_write_lock(tmp_path):
    store = _make_store(tmp_path)
    entered = threading.Event()
    release = threading.Event()

    async def slow_callback(_event):
        entered.set()
        await asyncio.to_thread(release.wait)

    await store.register_builtin_callback("slow")
    await store.register_expiry_callback("slow", slow_callback)
    await store.set("default", "k1", "v1", retention=1, on_expire="slow")
    await store.cleanup_expired(now=time.time() + 120)

    await asyncio.wait_for(asyncio.to_thread(entered.wait), timeout=2)
    await asyncio.wait_for(
        store.set("default", "normal", "ok", retention=10),
        timeout=0.2,
    )

    release.set()
    await store.wait_for_callbacks(timeout=2)
    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_callback_survives_restart(tmp_path):
    db_path = tmp_path / "store_lmdb"
    callback_lock = StoreService._try_acquire_file_lock(
        db_path / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    blocked_callback = threading.Event()

    async def on_expire_blocked(_event):
        blocked_callback.set()

    try:
        store1 = _make_store(tmp_path)
        await store1.register_builtin_callback("on_expire")
        await store1.register_expiry_callback("on_expire", on_expire_blocked)
        await store1.set("default", "k1", "v1", retention=1, on_expire="on_expire")
        await store1.cleanup_expired(now=time.time() + 120)
        await store1.shutdown()
    finally:
        StoreService._release_file_lock(callback_lock)

    assert not blocked_callback.is_set()

    restored_events: list[tuple[str, str, object | None, int]] = []
    callback_restored = threading.Event()

    async def on_expire_restored(event):
        restored_events.append((event.namespace, event.key, event.value, event.expire_ts))
        callback_restored.set()

    store2 = _make_store(tmp_path)
    await store2.register_builtin_callback("on_expire")
    await store2.register_expiry_callback("on_expire", on_expire_restored)
    await asyncio.wait_for(store2.wait_for_callbacks(), timeout=2)
    await asyncio.wait_for(asyncio.to_thread(callback_restored.wait), timeout=2)

    assert restored_events
    ns, key, value, expire_ts = restored_events[0]
    assert ns == "default"
    assert key == "k1"
    assert value == "v1"
    assert expire_ts > 0

    await store2.shutdown()


@pytest.mark.asyncio
async def test_missing_callback_handler_is_deferred_until_re_registered(tmp_path):
    store = _make_store(tmp_path)
    await store.register_builtin_callback("on_expire")

    async def initial_handler(_event):
        return None

    await store.register_expiry_callback("on_expire", initial_handler)
    await store.set("default", "k1", "v1", retention=1, on_expire="on_expire")
    await store.unregister_expiry_callback("on_expire")

    await store.cleanup_expired(now=time.time() + 120)
    await asyncio.sleep(1.2)

    assert store._count_callback_jobs() == 1
    assert store._count_dead_letter_callback_jobs() == 0

    restored_events: list[tuple[str, str, object | None, int]] = []
    restored_done = threading.Event()

    async def restored_handler(event):
        restored_events.append((event.namespace, event.key, event.value, event.expire_ts))
        restored_done.set()

    await store.register_expiry_callback("on_expire", restored_handler)
    await asyncio.wait_for(store.wait_for_callbacks(timeout=5), timeout=5)
    await asyncio.wait_for(asyncio.to_thread(restored_done.wait), timeout=2)

    assert store._count_callback_jobs() == 0
    assert restored_events
    assert restored_events[0][1] == "k1"

    await store.shutdown()


@pytest.mark.asyncio
async def test_callback_can_explicitly_defer_and_then_resume(tmp_path):
    store = _make_store(tmp_path)
    try:
        await store.register_builtin_callback("on_expire")

        callback_done = threading.Event()
        attempts = 0

        async def on_expire(_event):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ExpiryCallbackDeferred("service not ready")
            callback_done.set()

        await store.register_expiry_callback("on_expire", on_expire)
        await store.set("default", "k1", "v1", retention=1, on_expire="on_expire")
        await store.cleanup_expired(now=time.time() + 120)

        await asyncio.wait_for(asyncio.to_thread(callback_done.wait), timeout=5)
        await asyncio.wait_for(store.wait_for_callbacks(timeout=5), timeout=5)

        assert attempts >= 2
        assert store._count_callback_jobs() == 0
        assert store._count_dead_letter_callback_jobs() == 0
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_wait_for_callbacks_can_scope_to_specific_callback(tmp_path):
    store = _make_store(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None
    try:
        callback_done = threading.Event()

        async def on_c1(_event):
            callback_done.set()

        async def on_c2(_event):
            raise ExpiryCallbackDeferred("keep pending for scoped wait test")

        await store.register_builtin_callback("c1")
        await store.register_builtin_callback("c2")
        await store.register_expiry_callback("c1", on_c1)
        await store.register_expiry_callback("c2", on_c2)
        await store.set("default", "k1", "v1", retention=1, on_expire="c1")
        await store.set("default", "k2", "v2", retention=1, on_expire="c2")

        await store.cleanup_expired(now=time.time() + 120)
        assert store._count_callback_jobs() == 2
        assert store._count_callback_jobs_for("c1") == 1
        assert store._count_callback_jobs_for("c2") == 1

        StoreService._release_file_lock(callback_lock)
        callback_lock = None

        assert await store.wait_for_callbacks(timeout=5, callback_name="c1")
        assert callback_done.is_set()
        assert store._count_callback_jobs() == 1
        assert store._count_callback_jobs_for("c1") == 0
        assert store._count_callback_jobs_for("c2") == 1
    finally:
        if callback_lock is not None:
            StoreService._release_file_lock(callback_lock)
        await store.shutdown()


@pytest.mark.asyncio
async def test_get_expired_key_queues_callback_without_enqueue_helper(tmp_path, monkeypatch):
    store = _make_store(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    callback_done = threading.Event()
    callback_keys: list[str] = []

    async def on_expire(event):
        callback_keys.append(event.key)
        callback_done.set()

    await store.register_builtin_callback("on_expire")
    await store.register_expiry_callback("on_expire", on_expire)
    await store.set("default", "k1", "v1", retention=1, on_expire="on_expire")

    async def fail_enqueue(_events):
        raise RuntimeError("_enqueue_callbacks should not be called by get() delete path")

    real_time = time.time
    monkeypatch.setattr(store, "_enqueue_callbacks", fail_enqueue)
    monkeypatch.setattr(store_main.time, "time", lambda: real_time() + 120)

    try:
        assert await store.get("default", "k1") is None
        assert store._count_callback_jobs() == 1
    finally:
        StoreService._release_file_lock(callback_lock)

    await asyncio.wait_for(store.wait_for_callbacks(timeout=5), timeout=5)
    await asyncio.wait_for(asyncio.to_thread(callback_done.wait), timeout=2)
    assert callback_keys == ["k1"]

    await store.shutdown()


@pytest.mark.asyncio
async def test_get_many_expired_key_queues_callback_without_enqueue_helper(
    tmp_path, monkeypatch
):
    store = _make_store(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    callback_done = threading.Event()
    callback_keys: list[str] = []

    async def on_expire(event):
        callback_keys.append(event.key)
        callback_done.set()

    await store.register_builtin_callback("on_expire")
    await store.register_expiry_callback("on_expire", on_expire)
    await store.set("default", "k1", "v1", retention=1, on_expire="on_expire")

    async def fail_enqueue(_events):
        raise RuntimeError(
            "_enqueue_callbacks should not be called by get_many() delete path"
        )

    real_time = time.time
    monkeypatch.setattr(store, "_enqueue_callbacks", fail_enqueue)
    monkeypatch.setattr(store_main.time, "time", lambda: real_time() + 120)

    try:
        values = await store.get_many("default", ["k1", "missing"])
        assert values == {"k1": None, "missing": None}
        assert store._count_callback_jobs() == 1
        assert store._count_callback_jobs_for("on_expire") == 1
    finally:
        StoreService._release_file_lock(callback_lock)

    await asyncio.wait_for(store.wait_for_callbacks(timeout=5), timeout=5)
    await asyncio.wait_for(asyncio.to_thread(callback_done.wait), timeout=2)
    assert callback_keys == ["k1"]
    assert store._count_callback_jobs_for("on_expire") == 0

    await store.shutdown()


@pytest.mark.asyncio
async def test_cleanup_expired_queues_callback_without_enqueue_helper(tmp_path, monkeypatch):
    store = _make_store(tmp_path)
    callback_lock = StoreService._try_acquire_file_lock(
        tmp_path / "store_lmdb" / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    callback_done = threading.Event()

    async def on_expire(_event):
        callback_done.set()

    await store.register_builtin_callback("on_expire")
    await store.register_expiry_callback("on_expire", on_expire)
    await store.set("default", "k1", "v1", retention=1, on_expire="on_expire")

    async def fail_enqueue(_events):
        raise RuntimeError("_enqueue_callbacks should not be called by cleanup_expired()")

    monkeypatch.setattr(store, "_enqueue_callbacks", fail_enqueue)

    try:
        await store.cleanup_expired(now=time.time() + 120)
        assert store._count_callback_jobs() == 1
    finally:
        StoreService._release_file_lock(callback_lock)

    await asyncio.wait_for(store.wait_for_callbacks(timeout=5), timeout=5)
    await asyncio.wait_for(asyncio.to_thread(callback_done.wait), timeout=2)
    assert store._count_callback_jobs() == 0

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_callback_runs_on_dedicated_thread(tmp_path):
    store = _make_store(tmp_path)
    callback_done = threading.Event()
    callback_thread_id: int | None = None
    main_thread_id = threading.get_ident()

    async def on_expire(_event):
        nonlocal callback_thread_id
        callback_thread_id = threading.get_ident()
        callback_done.set()

    await store.register_builtin_callback("thread_check")
    await store.register_expiry_callback("thread_check", on_expire)
    await store.set("default", "k1", "v1", retention=1, on_expire="thread_check")

    await store.cleanup_expired(now=time.time() + 120)
    await asyncio.wait_for(asyncio.to_thread(callback_done.wait), timeout=2)
    await store.wait_for_callbacks(timeout=2)

    assert callback_thread_id is not None
    assert callback_thread_id != main_thread_id

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_key_namespace_escape(tmp_path):
    store = _make_store(tmp_path)
    namespace = "ns/with:weird chars"
    key = "k/space:中文"

    await store.set(namespace, key, {"ok": True}, retention=10)
    assert await store.get(namespace, key) == {"ok": True}

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_dir_and_files_permissions_are_restricted(tmp_path):
    if os.name != "posix":
        pytest.skip("permission mode checks require posix")

    store = _make_store(tmp_path)
    base_dir = tmp_path / "store_lmdb"

    dir_mode = stat.S_IMODE(base_dir.stat().st_mode)
    assert dir_mode & 0o077 == 0

    for name in ("data.mdb", "meta.mdb", "data.mdb-lock", "meta.mdb-lock"):
        path = base_dir / name
        if not path.exists():
            continue
        file_mode = stat.S_IMODE(path.stat().st_mode)
        assert file_mode & 0o077 == 0

    await store.start_cleanup()
    cleanup_lock = base_dir / ".store_cleanup.lock"
    if cleanup_lock.exists():
        lock_mode = stat.S_IMODE(cleanup_lock.stat().st_mode)
        assert lock_mode & 0o077 == 0

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_size_limits(tmp_path):
    store = _make_store(tmp_path)

    long_key = "k" * 300
    with pytest.raises(ValueError):
        await store.set("default", long_key, "v", retention=10)

    long_ns = "n" * 300
    with pytest.raises(ValueError):
        await store.set(long_ns, "k", "v", retention=10)

    big_value = "x" * (3 * 1024 * 1024)
    with pytest.raises(ValueError):
        await store.set("default", "k", big_value, retention=10)

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_index_dedup(tmp_path):
    store = _make_store(tmp_path)

    await store.set("default", "k1", "v1", retention=5)
    await store.set("default", "k1", "v2", retention=10)

    # Force cleanup past first expiry but before second
    await store.cleanup_expired(now=time.time() + 6 * 60)

    assert await store.get("default", "k1") == "v2"

    await store.shutdown()


@pytest.mark.asyncio
async def test_cleanup_limit(tmp_path):
    store = _make_store(tmp_path, cleanup_max_deletes=10)

    for i in range(25):
        await store.set("default", f"k{i}", "v", retention=1)

    await store.cleanup_expired(now=time.time() + 120)

    remaining = 0
    for i in range(25):
        if await store.get("default", f"k{i}") is not None:
            remaining += 1

    assert remaining >= 10

    await store.shutdown()


@pytest.mark.asyncio
async def test_max_dbs_zero_disables_user_namespace_quota(tmp_path):
    store = _make_store(tmp_path, max_dbs=0)
    for i in range(50):
        await store.set(f"ns{i}", "k", i, retention=10)
    for i in range(50):
        assert await store.get(f"ns{i}", "k") == i
    await store.shutdown()


@pytest.mark.asyncio
async def test_internal_namespace_not_counted_for_user_quota(tmp_path):
    store = _make_store(tmp_path, max_dbs=1)
    await store.mark_internal_namespace("tmp_files")
    await store.set("tmp_files", "k", True, retention=10)
    await store.set("user_ns", "k", "v", retention=10)

    with pytest.raises(RuntimeError, match="namespace quota"):
        await store.set("another_user_ns", "k", "v", retention=10)

    await store.shutdown()


@pytest.mark.asyncio
async def test_internal_namespace_migration_after_registration(tmp_path):
    """Test that mark_internal_namespace can migrate an already-registered namespace"""
    store = _make_store(tmp_path, max_dbs=2)

    # First, use a namespace (registers it as user namespace)
    await store.set("tmp_files", "k1", "v1", retention=10)
    await store.set("user_ns", "k", "v", retention=10)

    # Now mark tmp_files as internal - should migrate it
    await store.mark_internal_namespace("tmp_files")

    # Should still be able to create another user namespace since tmp_files no longer counts
    await store.set("another_user_ns", "k", "v", retention=10)

    # Verify the data is still accessible
    assert await store.get("tmp_files", "k1") == "v1"
    assert await store.get("user_ns", "k") == "v"
    assert await store.get("another_user_ns", "k") == "v"

    await store.shutdown()


@pytest.mark.asyncio
async def test_namespace_quota_enforced_across_store_instances(tmp_path):
    config = StoreConfig(
        path=str(tmp_path / "store_lmdb"),
        map_size_mb=16,
        map_size_growth_factor=2,
        map_high_watermark=0.9,
        max_dbs=1,
        max_readers=256,
        sync=False,
        metasync=False,
        writemap=True,
        map_async=True,
        max_key_bytes=256,
        max_namespace_bytes=256,
        max_value_bytes=2 * 1024 * 1024,
        cleanup_max_deletes=10_000,
        worker_threads=4,
    )
    store1 = StoreService(config)
    store2 = StoreService(config)
    try:
        await store1.set("user_ns_1", "k", "v", retention=10)
        with pytest.raises(RuntimeError, match="namespace quota"):
            await store2.set("user_ns_2", "k", "v", retention=10)
    finally:
        await store1.shutdown()
        await store2.shutdown()


@pytest.mark.asyncio
async def test_internal_namespace_marker_persists_across_store_instances(tmp_path):
    config = StoreConfig(
        path=str(tmp_path / "store_lmdb"),
        map_size_mb=16,
        map_size_growth_factor=2,
        map_high_watermark=0.9,
        max_dbs=1,
        max_readers=256,
        sync=False,
        metasync=False,
        writemap=True,
        map_async=True,
        max_key_bytes=256,
        max_namespace_bytes=256,
        max_value_bytes=2 * 1024 * 1024,
        cleanup_max_deletes=10_000,
        worker_threads=4,
    )
    store1 = StoreService(config)
    try:
        await store1.mark_internal_namespace("tmp_files")
        await store1.set("tmp_files", "k", True, retention=10)
    finally:
        await store1.shutdown()

    store2 = StoreService(config)
    try:
        await store2.set("user_ns", "k", "v", retention=10)
        with pytest.raises(RuntimeError, match="namespace quota"):
            await store2.set("another_user_ns", "k", "v", retention=10)
    finally:
        await store2.shutdown()


@pytest.mark.asyncio
async def test_namespace_registration_failure_does_not_pollute_memory_state(
    tmp_path, monkeypatch
):
    store = _make_store(tmp_path)

    original_register = store._register_namespace_in_meta

    def fail_register(_namespace: str) -> None:
        raise RuntimeError("meta registration failed")

    monkeypatch.setattr(store, "_register_namespace_in_meta", fail_register)

    with pytest.raises(RuntimeError, match="meta registration failed"):
        await store.set("ns_fail", "k", "v", retention=10)

    assert "ns_fail" not in store._namespaces

    monkeypatch.setattr(store, "_register_namespace_in_meta", original_register)
    await store.set("ns_fail", "k", "v", retention=10)
    assert await store.get("ns_fail", "k") == "v"

    await store.shutdown()


@pytest.mark.asyncio
async def test_cleanup_failure_keeps_metadata_for_retry(tmp_path, monkeypatch):
    store = _make_store(tmp_path)
    await store.set("default", "k1", "v1", retention=1)

    original_write_data = store._write_data_txn_with_resize
    failed_once = False

    def fail_once(fn, estimated_write_bytes: int) -> None:
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise RuntimeError("simulated data txn failure")
        original_write_data(fn, estimated_write_bytes)

    monkeypatch.setattr(store, "_write_data_txn_with_resize", fail_once)

    with pytest.raises(RuntimeError, match="simulated data txn failure"):
        await store.cleanup_expired(now=time.time() + 120)

    monkeypatch.setattr(store, "_write_data_txn_with_resize", original_write_data)
    await store.cleanup_expired(now=time.time() + 240)

    assert await store.get("default", "k1") is None

    await store.shutdown()


@pytest.mark.asyncio
async def test_namespace_lock_rejects_inherited_stale_lease_after_parent_exit(tmp_path):
    store = _make_store(tmp_path)
    await store.set("default", "seed", "ok", retention=10)

    release_child = asyncio.Event()

    async def leaked_child() -> None:
        await release_child.wait()
        with pytest.raises(RuntimeError, match="lease is no longer active"):
            await store.set("default", "child", "v", retention=10)

    async with store.create_namespace_lock("default"):
        child = asyncio.create_task(leaked_child())

    release_child.set()
    await child
    await store.shutdown()


@pytest.mark.asyncio
async def test_namespace_lock_create_task_waits_children_before_release(tmp_path):
    store = _make_store(tmp_path)
    await store.set("default", "seed", "ok", retention=10)

    marks: dict[str, float] = {}
    contender_acquired = asyncio.Event()

    async def child_writer() -> None:
        await asyncio.sleep(0.05)
        await store.set("default", "child", "v", retention=10)
        marks["child_done"] = time.monotonic()

    async def contender() -> None:
        await asyncio.sleep(0.01)
        async with store.create_namespace_lock("default"):
            marks["contender_acquired"] = time.monotonic()
            contender_acquired.set()

    contender_task = asyncio.create_task(contender())
    async with store.create_namespace_lock("default") as lease:
        lease.create_task(child_writer())
        await asyncio.sleep(0.02)
        assert not contender_acquired.is_set()

    await contender_task
    assert "child_done" in marks
    assert "contender_acquired" in marks
    assert marks["child_done"] <= marks["contender_acquired"]

    async with store.create_namespace_lock("default"):
        assert await store.get("default", "child") == "v"

    await store.shutdown()


@pytest.mark.asyncio
async def test_store_multiworker_stability(tmp_path):
    db_path = tmp_path / "store_lmdb"
    ctx = get_context("spawn")
    start_event = ctx.Event()
    error_queue = ctx.Queue()

    workers = [
        ctx.Process(target=_worker_main, args=(str(db_path), i, start_event, error_queue))
        for i in range(3)
    ]

    for proc in workers:
        proc.start()

    start_event.set()

    for proc in workers:
        proc.join(timeout=30)

    errors = []
    while not error_queue.empty():
        errors.append(error_queue.get())

    assert not errors, "\n".join(errors)

    store = _make_store(tmp_path)
    await store._register_namespace("shared", store._encode_namespace("shared"))

    for worker_id in range(3):
        for i in range(50):
            key = f"w{worker_id}_k{i}"
            value = await store.get("shared", key)
            assert value == {"v": i}

    await store.cleanup_expired(now=time.time() + 1200)

    for worker_id in range(3):
        for i in range(50):
            key = f"w{worker_id}_k{i}"
            value = await store.get("shared", key)
            assert value is None

    await store.shutdown()
