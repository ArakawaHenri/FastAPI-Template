from __future__ import annotations

import time
from pathlib import Path

import pytest

from app.services.temp_file.main import TempFileConfig, TempFileService


def _make_service(tmp_path: Path, retention_days: int = 1) -> TempFileService:
    return TempFileService(
        TempFileConfig(
            base_dir=str(tmp_path / "tmp_files"),
            retention_days=retention_days,
            cleanup_interval_seconds=60,
        )
    )


@pytest.mark.asyncio
async def test_save_and_read_text(tmp_path: Path):
    service = _make_service(tmp_path)
    name = await service.save("note.txt", "hello")
    assert name == "note.txt"
    data = await service.read("note.txt")
    assert data == "hello"
    await service.shutdown()


@pytest.mark.asyncio
async def test_save_and_read_binary(tmp_path: Path):
    service = _make_service(tmp_path)
    payload = b"\x00\x01\x02"
    name = await service.save("blob.bin", payload)
    assert name == "blob.bin"
    data = await service.read("blob.bin")
    assert isinstance(data, (bytes, bytearray))
    assert data == payload
    await service.shutdown()


@pytest.mark.asyncio
async def test_duplicate_filename_suffix(tmp_path: Path):
    service = _make_service(tmp_path)
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


@pytest.mark.asyncio
async def test_leading_dot_is_escaped(tmp_path: Path):
    service = _make_service(tmp_path)
    name = await service.save(".env", "secret")
    assert name.startswith("%2E")

    # Read using the original name; sanitize should map to the stored filename
    data = await service.read(".env")
    assert data == "secret"

    on_disk = Path(service._config.base_dir) / name
    assert on_disk.exists()
    await service.shutdown()


@pytest.mark.asyncio
async def test_cleanup_expired_removes_files(tmp_path: Path):
    service = _make_service(tmp_path, retention_days=1)
    name = await service.save("old.txt", "stale")
    path = Path(service._config.base_dir) / name

    # Simulate time passing beyond retention
    await service.cleanup_expired(now=time.time() + 2 * 24 * 60 * 60)

    assert not path.exists()
    assert name not in service._table
    await service.shutdown()


@pytest.mark.asyncio
async def test_cleanup_untracked_files(tmp_path: Path):
    service = _make_service(tmp_path, retention_days=1)
    base_dir = Path(service._config.base_dir)

    # Create an untracked file with old mtime
    untracked = base_dir / "stale.bin"
    untracked.write_bytes(b"x")
    old = time.time() - 2 * 24 * 60 * 60
    import os
    os.utime(untracked, (old, old))

    await service.cleanup_expired(now=time.time())

    assert not untracked.exists()
    await service.shutdown()
