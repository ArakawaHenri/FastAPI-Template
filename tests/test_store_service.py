from __future__ import annotations

import time

import pytest

from app.service.store.main import StoreConfig, StoreService


@pytest.mark.asyncio
async def test_store_ttl_and_set_if_absent(tmp_path) -> None:
    store = StoreService(
        StoreConfig(
            path=str(tmp_path / "store.zlmdb"),
            max_size_mb=16,
            cleanup_interval_seconds=3600,
        )
    )
    try:
        assert await store.set_if_absent("refresh:dedupe", "a", {"ok": True}, retention_seconds=1)
        assert not await store.set_if_absent("refresh:dedupe", "a", {"ok": False}, retention_seconds=1)
        assert await store.get("refresh:dedupe", "a") == {"ok": True}
        assert await store.count("refresh:dedupe") == 1
        assert await store.count() == 1
        stats = await store.stats()
        assert stats["slots"][0]["records"] == 1
        assert stats["slots"][1]["records"] == 1

        deleted = await store.cleanup_expired(now=time.time() + 120)
        assert deleted == 1
        assert await store.get("refresh:dedupe", "a") is None
        assert await store.count("refresh:dedupe") == 0
        assert await store.set_if_absent("refresh:dedupe", "a", {"ok": False}, retention_seconds=1)
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_store_overwrite_removes_expiry_index(tmp_path) -> None:
    store = StoreService(
        StoreConfig(
            path=str(tmp_path / "store.zlmdb"),
            max_size_mb=16,
            cleanup_interval_seconds=3600,
        )
    )
    try:
        await store.set("ns", "a", "old", retention_seconds=1)
        await store.set("ns", "a", "new")

        deleted = await store.cleanup_expired(now=time.time() + 120)

        assert deleted == 0
        assert await store.get("ns", "a") == "new"
    finally:
        await store.shutdown()


@pytest.mark.asyncio
async def test_store_validates_namespace_key_and_value_size(tmp_path) -> None:
    store = StoreService(
        StoreConfig(
            path=str(tmp_path / "store.zlmdb"),
            max_size_mb=16,
            max_value_bytes=8,
            cleanup_interval_seconds=3600,
        )
    )
    try:
        with pytest.raises(ValueError, match="namespace"):
            await store.set("", "a", "b")
        with pytest.raises(ValueError, match="key"):
            await store.set("ns", "", "b")
        with pytest.raises(ValueError, match="max_value_bytes"):
            await store.set("ns", "a", {"too": "large"})
    finally:
        await store.shutdown()
