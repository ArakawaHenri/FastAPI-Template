from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from app.service.temp_file.main import TempFileConfig, TempFileService


@pytest.mark.asyncio
async def test_temp_file_atomic_overwrite_and_unique_save(tmp_path) -> None:
    service = TempFileService(
        TempFileConfig(base_dir=str(tmp_path), retention_days=7, buckets=("crawl", "logs"))
    )
    try:
        await service.bootstrap()
        first = await service.save("page.md", "one", bucket="crawl")
        second = await service.save("page.md", "two", bucket="crawl")
        assert first == "page.md"
        assert second == "page.1.md"

        await service.save_overwrite("latest.md", "old", bucket="crawl")
        await service.save_overwrite("latest.md", "new", bucket="crawl")
        assert (Path(tmp_path) / "crawl" / "latest.md").read_text() == "new"
        assert not list((Path(tmp_path) / "crawl").glob(".*.tmp"))
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_temp_file_read_preserves_text_and_binary(tmp_path) -> None:
    service = TempFileService(
        TempFileConfig(base_dir=str(tmp_path), retention_days=7, buckets=("logs", "exports"))
    )
    try:
        await service.bootstrap()
        await service.save_overwrite("note.txt", "hello", bucket="logs")
        await service.save_overwrite("blob.bin", b"\xff\x00", bucket="exports")

        assert await service.read("note.txt", bucket="logs") == "hello"
        assert await service.read("blob.bin", bucket="exports") == b"\xff\x00"
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_temp_file_cleanup_removes_expired_tracked_and_untracked(tmp_path) -> None:
    service = TempFileService(
        TempFileConfig(base_dir=str(tmp_path), retention_days=1, buckets=("logs",))
    )
    try:
        await service.bootstrap()
        await service.save_overwrite("tracked.log", "old", bucket="logs")
        untracked = Path(tmp_path) / "logs" / "untracked.log"
        untracked.write_text("old")
        old = time.time() - 3 * 24 * 60 * 60
        os.utime(service.path_for("tracked.log", bucket="logs"), (old, old))
        os.utime(untracked, (old, old))

        deleted = await service.cleanup_expired(now=time.time() + 2 * 24 * 60 * 60)
        assert deleted == 2
        assert not service.path_for("tracked.log", bucket="logs").exists()
        assert not untracked.exists()
    finally:
        await service.shutdown()

