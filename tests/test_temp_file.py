from __future__ import annotations

import os
import stat
import time
from pathlib import Path

import pytest

from app.services.store.main import StoreConfig, StoreService
from app.services.temp_file.main import TempFileConfig, TempFileService


def _make_store(tmp_path: Path) -> StoreService:
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
            cleanup_max_deletes=10_000,
            worker_threads=2,
        )
    )


async def _make_service(
    tmp_path: Path,
    retention_days: int = 1,
    max_file_size_mb: int = 1024,
    max_total_size_mb: int = 0,
) -> tuple[TempFileService, StoreService]:
    store = _make_store(tmp_path)
    service = TempFileService(
        TempFileConfig(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=retention_days,
            cleanup_interval_seconds=60,
            worker_threads=2,
            max_file_size_mb=max_file_size_mb,
            max_total_size_mb=max_total_size_mb,
        ),
        store,
    )
    await service._bootstrap()
    return service, store


@pytest.mark.asyncio
async def test_temp_file_callback_name_is_namespace_scoped(tmp_path: Path):
    store = _make_store(tmp_path)
    service1 = TempFileService(
        TempFileConfig(
            base_dir=str(tmp_path / "tmp_files_1"),
            retention_days=1,
            cleanup_interval_seconds=60,
            worker_threads=2,
            namespace="tmp_files_1",
        ),
        store,
    )
    service2 = TempFileService(
        TempFileConfig(
            base_dir=str(tmp_path / "tmp_files_2"),
            retention_days=1,
            cleanup_interval_seconds=60,
            worker_threads=2,
            namespace="tmp_files_2",
        ),
        store,
    )

    await service1._bootstrap()
    await service2._bootstrap()

    assert service1._callback_name != service2._callback_name
    assert service1._callback_name.startswith("tmp_file_cleanup:")
    assert service2._callback_name.startswith("tmp_file_cleanup:")
    assert service1._callback_name in store._callback_registry
    assert service2._callback_name in store._callback_registry

    await service2.shutdown()
    await service1.shutdown()
    await store.shutdown()


def test_temp_file_callback_name_is_stable_for_same_namespace():
    assert (
        TempFileService._build_callback_name("tmp_files")
        == TempFileService._build_callback_name("tmp_files")
    )
    assert (
        TempFileService._build_callback_name("tmp_files")
        != TempFileService._build_callback_name("tmp_files_2")
    )


@pytest.mark.asyncio
async def test_save_and_read_text(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name = await service.save("note.txt", "hello")
    assert name == "note.txt"
    data = await service.read("note.txt")
    assert data == "hello"
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_and_read_binary(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    payload = b"\x00\x01\x02"
    name = await service.save("blob.bin", payload)
    assert name == "blob.bin"
    data = await service.read("blob.bin")
    assert isinstance(data, (bytes, bytearray))
    assert data == payload
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_rejects_oversized_payload(tmp_path: Path):
    service, store = await _make_service(tmp_path, max_file_size_mb=1)
    payload = b"x" * (2 * 1024 * 1024)
    with pytest.raises(ValueError, match="exceeds max size"):
        await service.save("too_large.bin", payload)
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_save_allows_large_payload_when_file_limit_zero(tmp_path: Path):
    service, store = await _make_service(tmp_path, max_file_size_mb=0)
    payload = b"x" * (2 * 1024 * 1024)
    name = await service.save("large.bin", payload)
    assert name == "large.bin"
    assert await service.read("large.bin") == payload
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_read_rejects_oversized_file(tmp_path: Path):
    service, store = await _make_service(tmp_path, max_file_size_mb=1)
    raw_path = Path(service._config.base_dir) / "big.bin"
    raw_path.write_bytes(b"x" * (2 * 1024 * 1024))
    with pytest.raises(ValueError, match="exceeds max size"):
        await service.read("big.bin")
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_total_size_cap_rejects_write_but_service_keeps_running(tmp_path: Path):
    service, store = await _make_service(
        tmp_path,
        max_file_size_mb=0,
        max_total_size_mb=1,
    )

    await service.save("a.bin", b"x" * (800 * 1024))
    with pytest.raises(ValueError, match="total size exceeds max"):
        await service.save("b.bin", b"x" * (400 * 1024))

    # Service remains operational after rejection.
    await service.save("c.bin", b"x" * (100 * 1024))
    assert isinstance(await service.read("c.bin"), (bytes, bytearray))

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_duplicate_filename_suffix(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name1 = await service.save("dup.txt", "a")
    name2 = await service.save("dup.txt", "b")
    name3 = await service.save("dup.txt", "c")

    assert name1 == "dup.txt"
    assert name2 == "dup.1.txt"
    assert name3 == "dup.2.txt"

    assert await service.read("dup.txt") == "a"
    assert await service.read("dup.1.txt") == "b"
    assert await service.read("dup.2.txt") == "c"
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_leading_dot_is_escaped(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name = await service.save(".env", "secret")
    assert name.startswith("%2E")

    # Read using the original name; sanitize should map to the stored filename
    data = await service.read(".env")
    assert data == "secret"

    on_disk = Path(service._config.base_dir) / name
    assert on_disk.exists()
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_temp_dir_and_files_permissions_are_restricted(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    name = await service.save("perm.txt", "secret")

    dir_mode = stat.S_IMODE(Path(service._config.base_dir).stat().st_mode)
    file_mode = stat.S_IMODE((Path(service._config.base_dir) / name).stat().st_mode)
    assert dir_mode & 0o077 == 0
    assert file_mode & 0o077 == 0

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_read_symlink_is_rejected(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    target = tmp_path / "outside.txt"
    target.write_text("outside", encoding="utf-8")
    link = Path(service._config.base_dir) / "link.txt"
    try:
        os.symlink(target, link)
    except (NotImplementedError, OSError):
        pytest.skip("symlink not supported on this platform")

    warnings: list[str] = []
    from loguru import logger
    sink_id = logger.add(
        lambda msg: warnings.append(msg.record["message"]),
        level="WARNING",
    )
    try:
        assert await service.read("link.txt") is None
    finally:
        logger.remove(sink_id)

    assert any("Refusing to read symlink temp file" in msg for msg in warnings)

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_overwrite_symlink_replaces_with_regular_file(tmp_path: Path):
    service, store = await _make_service(tmp_path)
    target = tmp_path / "outside.txt"
    target.write_text("outside", encoding="utf-8")
    link = Path(service._config.base_dir) / "swap.txt"
    try:
        os.symlink(target, link)
    except (NotImplementedError, OSError):
        pytest.skip("symlink not supported on this platform")

    await service.save_overwrite("swap.txt", "inside")
    path = Path(service._config.base_dir) / "swap.txt"
    assert not path.is_symlink()
    assert await service.read("swap.txt") == "inside"
    assert target.read_text(encoding="utf-8") == "outside"

    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_cleanup_expired_removes_files(tmp_path: Path):
    service, store = await _make_service(tmp_path, retention_days=1)
    name = await service.save("old.txt", "stale")
    path = Path(service._config.base_dir) / name

    # Simulate time passing beyond retention
    await store.cleanup_expired(now=time.time() + 2 * 24 * 60 * 60)
    await store.wait_for_callbacks()

    assert not path.exists()
    await service.shutdown()
    await store.shutdown()


@pytest.mark.asyncio
async def test_cleanup_untracked_files(tmp_path: Path):
    service, store = await _make_service(tmp_path, retention_days=1)
    base_dir = Path(service._config.base_dir)

    # Create an untracked file with old mtime
    untracked = base_dir / "stale.bin"
    untracked.write_bytes(b"x")
    old = time.time() - 2 * 24 * 60 * 60
    os.utime(untracked, (old, old))

    await service.cleanup_expired(now=time.time())

    assert not untracked.exists()
    await service.shutdown()
    await store.shutdown()
