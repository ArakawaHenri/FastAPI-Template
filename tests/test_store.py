from __future__ import annotations

import asyncio
import time
import traceback
from multiprocessing import get_context
from pathlib import Path

import pytest

from app.services.store.main import StoreConfig, StoreService


def _make_store(tmp_path: Path, cleanup_max_deletes: int = 10_000) -> StoreService:
    return StoreService(
        StoreConfig(
            path=str(tmp_path / "store_lmdb"),
            map_size_mb=64,
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
            max_value_bytes=10 * 1024 * 1024,
            cleanup_max_deletes=cleanup_max_deletes,
        )
    )


def _worker_main(db_path: str, worker_id: int, start_event, error_queue):
    async def _run():
        store = StoreService(
            StoreConfig(
                path=str(Path(db_path)),
                map_size_mb=64,
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
                max_value_bytes=10 * 1024 * 1024,
                cleanup_max_deletes=10_000,
            )
        )
        await store._register_namespace("shared")

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
async def test_store_key_namespace_escape(tmp_path):
    store = _make_store(tmp_path)
    namespace = "ns/with:weird chars"
    key = "k/space:中文"

    await store.set(namespace, key, {"ok": True}, retention=10)
    assert await store.get(namespace, key) == {"ok": True}

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

    big_value = "x" * (11 * 1024 * 1024)
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
    await store._register_namespace("shared")

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
