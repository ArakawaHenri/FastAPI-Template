from __future__ import annotations

import asyncio
import os
import stat
import time
import traceback
from multiprocessing import get_context
from pathlib import Path

import pytest
from loguru import logger

from app.services.store.main import StoreConfig, StoreService


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
async def test_get_missing_namespace_returns_none_warning_without_namespace_creation(tmp_path):
    store = _make_store(tmp_path, max_dbs=1)
    warnings: list[str] = []
    sink_id = logger.add(
        lambda msg: warnings.append(msg.record["message"]),
        level="WARNING",
    )
    try:
        assert await store.get("missing_ns", "missing_key") is None
    finally:
        logger.remove(sink_id)

    assert any("Namespace not found" in msg for msg in warnings)
    assert "missing_ns" not in store._list_namespaces()

    await store.set("real_ns", "k", "v", retention=10)
    assert await store.get("real_ns", "k") == "v"

    await store.shutdown()


@pytest.mark.asyncio
async def test_get_missing_key_returns_none_and_warning(tmp_path):
    store = _make_store(tmp_path)
    await store.set("default", "exists", "v", retention=10)

    warnings: list[str] = []
    sink_id = logger.add(
        lambda msg: warnings.append(msg.record["message"]),
        level="WARNING",
    )
    try:
        assert await store.get("default", "missing") is None
    finally:
        logger.remove(sink_id)

    assert any("Key not found" in msg for msg in warnings)
    await store.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_callback(tmp_path):
    store = _make_store(tmp_path)
    events: list[tuple[str, str, object | None, int]] = []
    callback_ran = asyncio.Event()
    write_blocked = asyncio.Event()

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
    await asyncio.wait_for(callback_ran.wait(), timeout=2)
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
async def test_store_expiry_callback_survives_restart(tmp_path):
    db_path = tmp_path / "store_lmdb"
    callback_lock = StoreService._try_acquire_file_lock(
        db_path / ".store_callbacks.lock"
    )
    assert callback_lock is not None

    blocked_callback = asyncio.Event()

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
    callback_restored = asyncio.Event()

    async def on_expire_restored(event):
        restored_events.append((event.namespace, event.key, event.value, event.expire_ts))
        callback_restored.set()

    store2 = _make_store(tmp_path)
    await store2.register_builtin_callback("on_expire")
    await store2.register_expiry_callback("on_expire", on_expire_restored)
    await asyncio.wait_for(store2.wait_for_callbacks(), timeout=2)
    await asyncio.wait_for(callback_restored.wait(), timeout=2)

    assert restored_events
    ns, key, value, expire_ts = restored_events[0]
    assert ns == "default"
    assert key == "k1"
    assert value == "v1"
    assert expire_ts > 0

    await store2.shutdown()

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
